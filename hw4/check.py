"""Public local entry point for HW4 project checks.

The complete checker is course-owned and runs on the server. This launcher
performs the same Compose preflight the course worker applies, then starts the
project and walks the observable path of the ``hw4-serving-v1`` contract on
the small local history:

- C1  Compose contract (services, ports, one-shot commands, mounts);
- C2  input schema and the ``history.csv.gz`` SHA-256;
- B1  ``feature-build`` and ``feature-loader`` completed successfully;
- B3  Redis keys ``hw4:features:<client_id>:<as_of>`` exist;
- B4  Redis values match the independent feature fixture;
- S1  ``GET /health`` names the loaded active model;
- S2  ``GET /features`` on ``feature-service`` returns the same vector;
- S3  ``POST /predict`` answers with probability, version and feature key;
- E1  unknown client -> 404 ``client_not_found``;
- E2  known client without a snapshot key -> 404 ``feature_not_found``;
- E3  malformed request -> 400 ``invalid_request``.

Version switches, restart persistence, dependency failures, the report and the
independent probability cross-check (V/R/D/O) stay on the server: a local
PASS never replaces the server verdict. Student code is never imported.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from checks.validate import (  # noqa: E402
    FEATURE_NAMES,
    HISTORY_NAME,
    InvalidProject,
    history_digest,
    validate_config,
    validate_history,
    validate_repo,
    validate_request,
)

INPUT_DIR = ROOT / "data"
REQUEST = INPUT_DIR / "request.json"
HISTORY = INPUT_DIR / HISTORY_NAME
EXPECTED = INPUT_DIR / "expected_features.json"
BASE_URL = "http://127.0.0.1:8080"
GATEWAY_TIMEOUT = 15
HEALTH_TIMEOUT = 300
ALLOWED_MODELS = frozenset({"hw4-logistic", "hw4-boosting"})
_TIMESTAMP = "%Y-%m-%dT%H:%M:%S"

_INT_FEATURES = frozenset(
    {
        "order_count_90d",
        "days_since_last_order",
        "distinct_stock_codes_90d",
        "order_count_30d",
    }
)
_DECIMAL_TOLERANCE = Decimal("0.000001")

_PROBE_SERVICE = """\
services:
  probe:
    image: {image}
    volumes:
      - type: bind
        source: {source}
        target: /probe/features_probe.py
        read_only: true
"""

# NOTE: plain script text, never str.format-ed; single braces are intentional.
_PROBE_SCRIPT = """\
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

BASE = "http://feature-service:8081/features"

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    client_id, as_of = json.loads(line)
    url = BASE + "?" + urllib.parse.urlencode({"client_id": client_id, "as_of": as_of})
    record = {"client_id": client_id, "as_of": as_of}
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            record["status"] = response.status
            record["body"] = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        record["status"] = error.code
        try:
            record["body"] = json.loads(error.read().decode("utf-8"))
        except (ValueError, UnicodeError):
            record["body"] = None
    except (OSError, ValueError) as error:
        record["status"] = None
        record["error"] = type(error).__name__
    print(json.dumps(record, ensure_ascii=False), flush=True)
"""


class SystemUnavailable(RuntimeError):
    """The local environment cannot run the check (Docker, probes, Redis CLI)."""


@dataclass
class Finding:
    code: str
    passed: bool
    message: str


class Report:
    def __init__(self) -> None:
        self.items: list[Finding] = []

    def passed_one(self, code: str, message: str) -> None:
        self.items.append(Finding(code, True, message))
        print("PASS " + code + ": " + message)

    def failed_one(self, code: str, message: str) -> None:
        self.items.append(Finding(code, False, message))
        print("FAIL " + code + ": " + message, file=sys.stderr)

    def skip(self, code: str, message: str) -> None:
        print("SKIP " + code + ": " + message)

    @property
    def failed(self) -> bool:
        return any(not item.passed for item in self.items)


def _environment(input_dir: Path) -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if key
        in {
            "HOME",
            "LANG",
            "PATH",
            "TMPDIR",
            "DOCKER_HOST",
            "DOCKER_CONTEXT",
            "XDG_RUNTIME_DIR",
        }
    }
    environment["HW4_INPUT_DIR"] = str(input_dir)
    return environment


def _compose(project: Path, input_dir: Path, *extra: Path) -> list[str]:
    command = [
        "docker",
        "compose",
        "--project-directory",
        str(project),
        "-f",
        str(project / "compose.yaml"),
    ]
    for path in extra:
        command += ["-f", str(path)]
    return command


def _run(
    command: list[str],
    environment: dict[str, str],
    timeout: int,
    *,
    cwd: Path = ROOT,
    stdin: str | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            cwd=cwd,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
            input=stdin,
        )
    except subprocess.TimeoutExpired as error:
        raise SystemUnavailable("docker_timeout: " + command[2]) from error
    except OSError as error:
        raise SystemUnavailable("docker_unavailable") from error


def _http(
    method: str,
    url: str,
    payload: object | None = None,
    timeout: int = GATEWAY_TIMEOUT,
) -> tuple[int | None, dict[str, Any] | None]:
    request = urllib.request.Request(url, method=method)
    if payload is not None:
        request.data = json.dumps(payload).encode("utf-8")
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return response.status, _json(body)
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", "replace")
        return error.code, _json(body)
    except (OSError, urllib.error.URLError) as error:
        raise SystemUnavailable("gateway_unreachable") from error


def _json(body: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


def preflight(
    report: Report, project: Path = ROOT, input_dir: Path = INPUT_DIR
) -> None:
    """C1: the Compose contract, plus repository hygiene."""
    if not (project / "compose.yaml").is_file():
        raise InvalidProject(
            "compose_missing: создайте hw4/compose.yaml с обязательными сервисами "
            "по разделу 2 TECHNICAL.md"
        )
    compose = _compose(project, input_dir)
    result = _run(
        [*compose, "config", "--format", "json"],
        _environment(input_dir),
        60,
        cwd=project,
    )
    if result.returncode:
        raise InvalidProject("compose_config_invalid: " + result.stderr.strip()[:400])
    config = _json(result.stdout)
    if config is None:
        raise InvalidProject("compose_config_invalid")
    validate_config(config, project, input_dir)
    report.passed_one(
        "C1",
        "compose-контракт: обязательные сервисы, порт gateway 8080, "
        "одноразовые feature-build/feature-loader, read-only /input",
    )
    for finding in validate_repo(project):
        report.failed_one("repo_hygiene", finding)


def check_input(
    report: Report, input_dir: Path = INPUT_DIR
) -> tuple[list[dict[str, str]], dict[str, dict[str, Any]]]:
    """C2: readable input whose declared hash matches the actual file."""
    digest = history_digest(input_dir / HISTORY)
    rows = validate_history(input_dir / HISTORY)
    try:
        payload = json.loads((input_dir / "request.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise InvalidProject("request_unreadable") from error
    snapshots = validate_request(payload, digest)
    report.passed_one(
        "C2",
        f"вход: {rows} строк истории, sha256 совпадает, снимков: {len(snapshots)}",
    )
    try:
        expected = json.loads(EXPECTED.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise SystemUnavailable("expected_fixture_invalid") from error
    if not isinstance(expected, dict) or not expected:
        raise SystemUnavailable("expected_fixture_invalid")
    declared = {snapshot["as_of"] for snapshot in snapshots}
    for key, value in expected.items():
        client_id, _, as_of = str(key).partition(":")
        if not client_id or as_of not in declared or not isinstance(value, dict):
            raise SystemUnavailable("expected_fixture_invalid: " + str(key))
    return snapshots, expected


def _up(environment: dict[str, str], timeout: int) -> None:
    result = _run(
        [*_compose(ROOT, INPUT_DIR), "up", "-d", "--build", "--wait"],
        environment,
        timeout,
    )
    if result.returncode:
        raise InvalidProject(
            "compose_up_failed: " + (result.stderr.strip()[-600:] or "без вывода")
        )


def _exit_code(service: str, environment: dict[str, str]) -> int | None:
    result = _run(
        [*_compose(ROOT, INPUT_DIR), "ps", "--all", "--format", "json", service],
        environment,
        60,
    )
    if result.returncode:
        return None
    stripped = result.stdout.strip()
    entries: list[Any] = []
    if stripped:
        try:
            parsed = json.loads(stripped)
            entries = parsed if isinstance(parsed, list) else [parsed]
        except ValueError:
            for line in stripped.splitlines():
                try:
                    entries.append(json.loads(line))
                except ValueError:
                    continue
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("Service") != service:
            continue
        state = entry.get("State")
        if state in {"running", "paused"}:
            return 0
        exit_code = entry.get("ExitCode")
        return int(exit_code) if isinstance(exit_code, int) else None
    return None


def check_offline(report: Report, environment: dict[str, str]) -> None:
    """B1: one-shot feature services completed with exit code 0."""
    for service in ("feature-build", "feature-loader"):
        code = _exit_code(service, environment)
        if code == 0:
            report.passed_one("B1", service + " завершился успешно (exit 0)")
        else:
            report.failed_one(
                "B1",
                f"{service} не завершился успешно (exit={code}); "
                "смотрите docker compose logs " + service,
            )


def _redis_get(key: str, environment: dict[str, str]) -> str | None:
    result = _run(
        [
            *_compose(ROOT, INPUT_DIR),
            "exec",
            "-T",
            "redis",
            "redis-cli",
            "--raw",
            "GET",
            key,
        ],
        environment,
        30,
    )
    if result.returncode:
        raise SystemUnavailable("redis_cli_unavailable: " + result.stderr.strip()[:200])
    value = result.stdout.rstrip("\n")
    return value if value else None


def _number(actual: object) -> Decimal | None:
    if isinstance(actual, bool) or not isinstance(actual, (int, float, str)):
        return None
    try:
        return Decimal(str(actual))
    except Exception:  # noqa: BLE001 - Decimal accepts many shapes
        return None


def features_mismatch(actual: object, expected: dict[str, Any]) -> str | None:
    """Return a mismatch description, or None when all eight features match."""
    if not isinstance(actual, dict):
        return "значение не является JSON-объектом с признаками"
    for name in FEATURE_NAMES:
        if name not in actual:
            return f"нет признака {name}"
        got = _number(actual[name])
        want = _number(expected.get(name))
        if got is None or want is None:
            return f"{name}: нечисловое значение {actual[name]!r}"
        if name in _INT_FEATURES:
            if got != want:
                return f"{name}: ожидалось {want}, получено {got}"
        elif abs(got - want) > _DECIMAL_TOLERANCE:
            return f"{name}: ожидалось {want}, получено {got}"
    return None


def _snapshot_keys(expected: dict[str, Any]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for key in expected:
        client_id, _, as_of = str(key).partition(":")
        pairs.append((client_id, as_of))
    return sorted(pairs)


def check_redis(
    report: Report, environment: dict[str, str], expected: dict[str, Any]
) -> None:
    """B3/B4: declared snapshot keys exist and carry the expected vector."""
    missing: list[str] = []
    mismatched: list[str] = []
    for client_id, as_of in _snapshot_keys(expected):
        key = f"hw4:features:{client_id}:{as_of}"
        value = _redis_get(key, environment)
        if value is None:
            missing.append(key)
            continue
        try:
            payload = json.loads(value)
        except ValueError:
            mismatched.append(key + ": значение не JSON")
            continue
        if not isinstance(payload, dict) or payload.get("client_id") != client_id:
            mismatched.append(key + ": нет echo client_id")
            continue
        problem = features_mismatch(payload, expected[f"{client_id}:{as_of}"])
        if problem is not None:
            mismatched.append(f"{key}: {problem}")
    total = len(expected)
    if missing:
        report.failed_one("B3", "нет ключей в Redis: " + ", ".join(missing))
    else:
        report.passed_one("B3", f"все {total} ключей признаков появились в Redis")
    if mismatched:
        report.failed_one("B4", "; ".join(mismatched))
    else:
        report.passed_one(
            "B4", "значения Redis совпадают с независимым расчётом fixture"
        )


def _wait_health() -> tuple[str, str] | None:
    deadline = time.monotonic() + HEALTH_TIMEOUT
    answered = False
    while time.monotonic() < deadline:
        try:
            status, body = _http("GET", BASE_URL + "/health", timeout=5)
        except SystemUnavailable:
            status, body = None, None
        if status is not None:
            answered = True
            if (
                status == 200
                and isinstance(body, dict)
                and body.get("status") == "ok"
                and isinstance(body.get("model_name"), str)
                and body["model_name"] in ALLOWED_MODELS
                and body.get("model_version") not in (None, "")
            ):
                return body["model_name"], str(body["model_version"])
        time.sleep(3)
    if not answered:
        raise SystemUnavailable("gateway_not_ready: сервис gateway не отвечает")
    return None


def check_health(report: Report) -> tuple[str, str] | None:
    """S1: /health names the loaded active model."""
    active = _wait_health()
    if active is None:
        report.failed_one(
            "S1",
            "/health не назвал загруженную модель: нужен 200 со статусом ok, "
            "model_name из {hw4-logistic, hw4-boosting} и model_version",
        )
        return None
    report.passed_one("S1", f"/health ok: активная модель {active[0]} v{active[1]}")
    return active


def _probe_image() -> str:
    try:
        versions = json.loads((ROOT / "versions.json").read_text(encoding="utf-8"))
        image = versions["images"]["python"]
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise SystemUnavailable("probe_image_unknown") from error
    if not isinstance(image, str) or not image:
        raise SystemUnavailable("probe_image_unknown")
    return image


def check_feature_service(
    report: Report, environment: dict[str, str], expected: dict[str, Any]
) -> None:
    """S2: feature-service returns the same vector straight from the store."""
    pairs = _snapshot_keys(expected)
    with tempfile.TemporaryDirectory(prefix="hw4-probe-") as temporary:
        probe_dir = Path(temporary)
        script = probe_dir / "features_probe.py"
        script.write_text(_PROBE_SCRIPT.lstrip("\n"), encoding="utf-8")
        override = probe_dir / "probe.yaml"
        override.write_text(
            _PROBE_SERVICE.format(image=_probe_image(), source=script),
            encoding="utf-8",
        )
        stdin = "".join(
            json.dumps([client_id, as_of[:10]]) + "\n" for client_id, as_of in pairs
        )
        result = _run(
            [
                *_compose(ROOT, INPUT_DIR, override),
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "probe",
                "python",
                "/probe/features_probe.py",
            ],
            environment,
            900,
            stdin=stdin,
        )
    if result.returncode:
        raise SystemUnavailable("feature_probe_failed: " + result.stderr.strip()[-300:])
    answers: dict[tuple[str, str], dict[str, Any]] = {}
    for line in result.stdout.splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record, dict) and {"client_id", "as_of", "status"} <= set(record):
            answers[(str(record["client_id"]), str(record["as_of"]))] = record
    mismatched: list[str] = []
    for client_id, as_of in pairs:
        record = answers.get((client_id, as_of[:10]))
        if record is None:
            mismatched.append(f"{client_id}@{as_of}: проба не вернула ответ")
            continue
        if record.get("status") != 200:
            mismatched.append(f"{client_id}@{as_of}: HTTP {record.get('status')}")
            continue
        body = record.get("body")
        if not isinstance(body, dict) or body.get("client_id") != client_id:
            mismatched.append(f"{client_id}@{as_of}: нет echo client_id")
            continue
        problem = features_mismatch(
            body.get("features"), expected[f"{client_id}:{as_of}"]
        )
        if problem is not None:
            mismatched.append(f"{client_id}@{as_of}: {problem}")
    if mismatched:
        report.failed_one("S2", "; ".join(mismatched))
    else:
        report.passed_one("S2", "feature-service отдал векторы, совпадающие с fixture")


def check_predict(
    report: Report, expected: dict[str, Any], active: tuple[str, str] | None
) -> None:
    """S3: /predict answers with probability, version and the feature key."""
    if active is None:
        report.skip("S3", "пропущено: активная модель не названа")
        return
    model_name, model_version = active
    problems: list[str] = []
    for client_id, as_of in _snapshot_keys(expected):
        date = as_of[:10]
        status, body = _http(
            "POST", BASE_URL + "/predict", {"client_id": client_id, "as_of": date}
        )
        if status != 200 or not isinstance(body, dict):
            problems.append(f"{client_id}@{date}: HTTP {status}")
            continue
        probability = body.get("probability")
        if (
            isinstance(probability, bool)
            or not isinstance(probability, (int, float))
            or not 0.0 <= float(probability) <= 1.0
        ):
            problems.append(f"{client_id}@{date}: probability {probability!r}")
            continue
        if body.get("model_name") != model_name or str(body.get("model_version")) != (
            model_version
        ):
            problems.append(
                f"{client_id}@{date}: модель {body.get('model_name')!r} "
                f"v{body.get('model_version')!r} не совпадает с /health"
            )
            continue
        if body.get("feature_key") != f"hw4:features:{client_id}:{date}":
            problems.append(
                f"{client_id}@{date}: feature_key {body.get('feature_key')!r}"
            )
    if problems:
        report.failed_one("S3", "; ".join(problems))
    else:
        report.passed_one(
            "S3",
            "прогнозы в [0,1], версия совпадает с /health, ключи канонические "
            "(сверка вероятности с моделью — на сервере)",
        )


def _error_code(body: dict[str, Any] | None) -> object:
    if isinstance(body, dict):
        error = body.get("error")
        if isinstance(error, dict):
            return error.get("code")
    return None


def check_errors(
    report: Report, snapshots: list[dict[str, str]], expected: dict[str, Any]
) -> None:
    """E1-E3: the declared error contract of the public endpoint."""
    known_clients = {client for client, _ in _snapshot_keys(expected)}
    unknown_client = "NO_SUCH_CLIENT_HW4"
    if unknown_client in known_clients:
        unknown_client = "STILL_NO_SUCH_CLIENT_HW4"
    date = snapshots[0]["as_of"][:10]
    status, body = _http(
        "POST", BASE_URL + "/predict", {"client_id": unknown_client, "as_of": date}
    )
    if status == 404 and _error_code(body) == "client_not_found":
        report.passed_one(
            "E1", f"неизвестный клиент: 404 client_not_found ({unknown_client})"
        )
    else:
        report.failed_one(
            "E1",
            "неизвестный клиент: ожидался 404 client_not_found, получено "
            f"HTTP {status}, код {_error_code(body)!r}",
        )

    missing_date = _date_without_snapshot(snapshots)
    probe_client = sorted(known_clients)[0] if known_clients else None
    if missing_date is None or probe_client is None:
        report.skip("E2", "пропущено: в окне наблюдения нет даты без снимка")
    else:
        status, body = _http(
            "POST",
            BASE_URL + "/predict",
            {"client_id": probe_client, "as_of": missing_date},
        )
        if status == 404 and _error_code(body) == "feature_not_found":
            report.passed_one(
                "E2", f"нет ключа на дату: 404 feature_not_found ({missing_date})"
            )
        else:
            report.failed_one(
                "E2",
                "нет ключа на дату: ожидался 404 feature_not_found, получено "
                f"HTTP {status}, код {_error_code(body)!r}",
            )

    invalid_payloads = (
        {"client_id": probe_client or "A"},
        {"client_id": 7, "as_of": date},
    )
    bad: list[str] = []
    for payload in invalid_payloads:
        status, body = _http("POST", BASE_URL + "/predict", payload)
        if status != 400 or _error_code(body) != "invalid_request":
            bad.append(f"{payload}: HTTP {status}, код {_error_code(body)!r}")
    if bad:
        report.failed_one("E3", "; ".join(bad))
    else:
        report.passed_one(
            "E3", "неверный вход: 400 invalid_request (нет поля и неверный тип)"
        )


def _date_without_snapshot(snapshots: list[dict[str, str]]) -> str | None:
    try:
        request = json.loads(REQUEST.read_text(encoding="utf-8"))
        observed_from = datetime.strptime(str(request["observed_from"]), _TIMESTAMP)
        observed_until = datetime.strptime(str(request["observed_until"]), _TIMESTAMP)
        first = datetime.strptime(snapshots[0]["as_of"], _TIMESTAMP)
    except (OSError, ValueError, KeyError, TypeError, IndexError):
        return None
    dates = {snapshot["as_of"][:10] for snapshot in snapshots}
    middle = observed_from + (observed_until - observed_from) / 2
    for candidate in (first - timedelta(days=1), middle):
        text = candidate.strftime("%Y-%m-%d")
        if (
            observed_from.strftime("%Y-%m-%d")
            <= text
            <= observed_until.strftime("%Y-%m-%d")
            and text not in dates
        ):
            return text
    return None


def smoke(
    report: Report,
    timeout: int,
    snapshots: list[dict[str, str]],
    expected: dict[str, Any],
) -> None:
    environment = _environment(INPUT_DIR)
    try:
        _up(environment, timeout)
    except BaseException:
        _run(
            [*_compose(ROOT, INPUT_DIR), "down", "--remove-orphans"],
            environment,
            300,
        )
        raise
    try:
        check_offline(report, environment)
        check_redis(report, environment, expected)
        active = check_health(report)
        check_feature_service(report, environment, expected)
        check_predict(report, expected, active)
        check_errors(report, snapshots, expected)
    finally:
        _run(
            [*_compose(ROOT, INPUT_DIR), "down", "--remove-orphans"],
            environment,
            300,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Локальная проверка HW4")
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="проверить Compose-границы и вход без сборки и запуска",
    )
    parser.add_argument("--timeout", type=int, default=5400)
    args = parser.parse_args()
    if not 60 <= args.timeout <= 5400:
        parser.error("--timeout должен быть от 60 до 5400 секунд")
    report = Report()
    try:
        preflight(report)
        snapshots, expected = check_input(report)
    except InvalidProject as error:
        report.failed_one("PREFLIGHT", error.code)
        print("FAIL: проект HW4 отклонён до запуска", file=sys.stderr)
        return 1
    except SystemUnavailable as error:
        print("SYSTEM_ERROR: " + str(error), file=sys.stderr)
        return 2
    if args.preflight_only:
        if report.failed:
            return 1
        print(
            "PASS: Compose-конфигурация и вход HW4 соответствуют границам "
            "локальной проверки"
        )
        return 0
    try:
        smoke(report, args.timeout, snapshots, expected)
    except InvalidProject as error:
        report.failed_one("RUN", error.code)
    except SystemUnavailable as error:
        print("SYSTEM_ERROR: " + str(error), file=sys.stderr)
        return 2
    if report.failed:
        print(
            "FAIL: локальная проверка HW4 не пройдена; серверная проверка "
            "независимо сверит признаки, вероятности, версии и перезапуск",
            file=sys.stderr,
        )
        return 1
    print("PASS: локальная проверка HW4 пройдена по всем пунктам")
    print(
        "Серверная проверка дополнительно проверит V/R/D/O: переключение версий, "
        "перезапуск, отказы зависимостей и отчёт."
    )
    print("Тома проекта сохранены; полный сброс: docker compose down -v")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
