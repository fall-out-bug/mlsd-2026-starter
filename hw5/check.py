"""Public local entry point for HW5 project checks.

The course owns the full behavioral check and runs it on the server.  This
launcher is the student's local preflight for the ``hw5-stream-v1`` contract.
It never imports student code; it starts the project, talks to it through its
public interfaces (gateway on 8080) and internal handles via
``docker compose exec`` (stream-worker 8083, monitor 8084, redis-cli), then
cleans up afterwards:

- C1  Compose contract: all sixteen services, only the gateway publishes
      8080, one-shot feature-build/feature-loader/airflow-init, read-only
      ``/input``, healthchecks on long-lived services, stream-worker and
      monitor gated on redis+postgres;
- C2  project tree and runbook sections (Алерты / Реакция / Эскалация);
- C3  input schema: ``history.csv.gz`` and the hw5-stream-v1 ``request.json``;
- T0  consumer group ``hw5-workers`` exists on both streams after startup;
- T1  identity counters on a toy flow (accept / duplicate);
- T2  queued prediction ``ok``: features and probability match the
      independent fixture from ``data/expected.json`` (tolerance 1e-9);
- T3  ``no_history`` outcome with null fields;
- T4  unknown request -> 404 ``prediction_not_found``;
- T5  monitor report sanity (time echo, counters, predictions split).

The check always starts from clean volumes (``docker compose down -v``) and
warns about it.  A local PASS never replaces the server verdict.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
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
    validate_config,
    validate_history,
    validate_repo,
    validate_request,
    validate_tree,
)

INPUT_DIR = ROOT / "input"
REQUEST = INPUT_DIR / "request.json"
HISTORY = INPUT_DIR / HISTORY_NAME
EXPECTED = ROOT / "data" / "expected.json"
BASE_URL = "http://127.0.0.1:8080"
GATEWAY_TIMEOUT = 15
HEALTH_TIMEOUT = 900
GROUP_TIMEOUT = 300
BOUNDARY_TIMEOUT = 300
TOLERANCE = 1e-9

_INT_FEATURES = frozenset(
    {
        "order_count_90d",
        "days_since_last_order",
        "distinct_stock_codes_90d",
        "order_count_30d",
    }
)
_DECIMAL_FEATURES = frozenset(set(FEATURE_NAMES) - _INT_FEATURES)

EVENT_PAYLOAD = {
    "event_id": "e-chk-1",
    "invoice_id": "I9900",
    "stock_code": "SC1",
    "description": "item",
    "quantity": 2,
    "invoice_at": "2011-01-10T10:00:00",
    "unit_price": "10.000000",
    "customer_id": "A",
    "country": "GB",
}


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
            "COMPOSE_PROJECT_NAME",
        }
    }
    environment["HW4_INPUT_DIR"] = str(input_dir)
    return environment


def _compose(*extra: Path) -> list[str]:
    command = [
        "docker",
        "compose",
        "--project-directory",
        str(ROOT),
        "-f",
        str(ROOT / "compose.yaml"),
    ]
    for path in extra:
        command += ["-f", str(path)]
    return command


def _run(
    command: list[str],
    environment: dict[str, str],
    timeout: int,
    *,
    stdin: str | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            cwd=ROOT,
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


_PY_HTTP = (
    "import sys,json,urllib.request,urllib.error\n"
    "method, path, body = sys.argv[1], sys.argv[2], sys.argv[3]\n"
    "data = body.encode() if body else None\n"
    "req = urllib.request.Request('http://127.0.0.1:%s' + path, data=data, method=method)\n"
    "if data: req.add_header('Content-Type','application/json')\n"
    "try:\n"
    "    r = urllib.request.urlopen(req, timeout=30)\n"
    "    print(json.dumps([r.status, json.loads(r.read().decode() or 'null')]))\n"
    "except urllib.error.HTTPError as e:\n"
    "    try: print(json.dumps([e.code, json.loads(e.read().decode() or 'null')]))\n"
    "    except Exception: print(json.dumps([e.code, None]))\n"
    "except Exception as e:\n"
    "    print(json.dumps([0, {'error': {'code': 'unreachable', 'message': str(e)}}]))\n"
)


def _worker_http(
    environment: dict[str, str], method: str, path: str, payload: dict | None = None
) -> tuple[int | None, Any]:
    result = _run(
        [
            *_compose(),
            "exec",
            "-T",
            "stream-worker",
            "python",
            "-c",
            _PY_HTTP % 8083,
            method,
            path,
            json.dumps(payload) if payload is not None else "",
        ],
        environment,
        60,
    )
    if result.returncode != 0:
        return None, {
            "error": {"code": "exec_failed", "message": result.stderr.strip()[-300:]}
        }
    try:
        return json.loads(result.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return None, {"error": {"code": "body_not_json"}}


def _monitor_http(
    environment: dict[str, str], method: str, path: str
) -> tuple[int | None, Any]:
    result = _run(
        [
            *_compose(),
            "exec",
            "-T",
            "monitor",
            "python",
            "-c",
            _PY_HTTP % 8084,
            method,
            path,
            "",
        ],
        environment,
        60,
    )
    if result.returncode != 0:
        return None, {
            "error": {"code": "exec_failed", "message": result.stderr.strip()[-300:]}
        }
    try:
        return json.loads(result.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return None, {"error": {"code": "body_not_json"}}


def _redis(environment: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return _run(
        [*_compose(), "exec", "-T", "redis", "redis-cli", *args], environment, 60
    )


def preflight(report: Report, input_dir: Path = INPUT_DIR) -> None:
    """C1: the Compose contract, plus repository hygiene."""
    if not (ROOT / "compose.yaml").is_file():
        raise InvalidProject(
            "compose_missing: создайте hw5/compose.yaml с обязательными сервисами "
            "по разделу 2 TECHNICAL.md"
        )
    result = _run(
        [*_compose(), "config", "--format", "json"],
        _environment(input_dir),
        60,
    )
    if result.returncode:
        raise InvalidProject("compose_config_invalid: " + result.stderr.strip()[:400])
    config = _json(result.stdout)
    if config is None:
        raise InvalidProject("compose_config_invalid")
    validate_config(config, ROOT, input_dir)
    report.passed_one(
        "C1",
        "compose-контракт: шестнадцать обязательных сервисов, порт gateway 8080, "
        "одноразовые feature-build/feature-loader/airflow-init, read-only /input, "
        "healthchecks, stream-worker и monitor после redis+postgres",
    )
    for finding in validate_repo(ROOT):
        if finding.startswith("repo_not_git"):
            report.skip("repo_hygiene", finding)
        else:
            report.failed_one("repo_hygiene", finding)


def check_tree(report: Report) -> None:
    """C2: §1 tree and runbook sections."""
    findings = validate_tree(ROOT)
    if findings:
        report.failed_one("C2", "; ".join(findings))
    else:
        report.passed_one(
            "C2",
            "дерево hw5/ и runbook.md с разделами Алерты/Реакция/Эскалация на месте",
        )


def check_input(report: Report, input_dir: Path = INPUT_DIR) -> dict[str, Any]:
    """C3: readable input per the hw5-stream-v1 schema."""
    rows = validate_history(input_dir / HISTORY)
    try:
        payload = json.loads((input_dir / "request.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise InvalidProject("request_unreadable") from error
    snapshots = validate_request(payload)
    report.passed_one(
        "C3",
        f"вход: {rows} строк истории, схема request.json hw5-stream-v1 корректна, "
        f"снимков: {len(snapshots)}",
    )
    try:
        expected = json.loads(EXPECTED.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise SystemUnavailable("expected_fixture_invalid") from error
    if not isinstance(expected, dict) or not isinstance(
        expected.get("check_toy"), dict
    ):
        raise SystemUnavailable("expected_fixture_invalid: check_toy")
    return expected


def _up(environment: dict[str, str], timeout: int) -> None:
    result = _run([*_compose(), "up", "-d", "--build", "--wait"], environment, timeout)
    if result.returncode:
        raise InvalidProject(
            "compose_up_failed: " + (result.stderr.strip()[-600:] or "без вывода")
        )


def _wait_gateway() -> None:
    deadline = time.monotonic() + HEALTH_TIMEOUT
    answered = False
    while time.monotonic() < deadline:
        try:
            status, body = _http("GET", BASE_URL + "/health", timeout=5)
        except SystemUnavailable:
            status, body = None, None
        if status is not None:
            answered = True
            if status == 200 and isinstance(body, dict) and body.get("status") == "ok":
                return
        time.sleep(3)
    if not answered:
        raise SystemUnavailable("gateway_not_ready: сервис gateway не отвечает")
    raise InvalidProject(
        "gateway_unhealthy: /health не вернул 200 ok за отведённое время"
    )


def _entry_key(entry_id: str) -> tuple[int, int]:
    ms, _, seq = (entry_id or "0-0").partition("-")
    try:
        return (int(ms), int(seq or 0))
    except ValueError:
        return (0, 0)


def _xadd(environment: dict[str, str], stream: str, payload: dict) -> str:
    result = _redis(
        environment,
        "XADD",
        stream,
        "*",
        "payload",
        json.dumps(payload, separators=(",", ":")),
    )
    if result.returncode:
        raise InvalidProject("xadd_failed: " + result.stderr.strip()[:300])
    return result.stdout.strip()


def _groups_ready(environment: dict[str, str]) -> bool:
    for stream in ("hw5:events", "hw5:requests"):
        result = _redis(environment, "XINFO", "GROUPS", stream)
        if result.returncode or "hw5-workers" not in result.stdout:
            return False
    return True


def _wait_boundary(
    environment: dict[str, str],
    max_event: tuple[int, int],
    max_request: tuple[int, int],
    timeout: int = BOUNDARY_TIMEOUT,
) -> tuple[bool, str]:
    deadline = time.monotonic() + timeout
    last_note = ""
    while time.monotonic() < deadline:
        status, body = _worker_http(environment, "GET", "/stream/status")
        if status == 200 and isinstance(body, dict):
            last_note = json.dumps(
                {
                    k: body.get(k)
                    for k in (
                        "last_event_id",
                        "last_request_id",
                        "events_pending",
                        "requests_pending",
                    )
                }
            )
            if (
                _entry_key(body.get("last_event_id", "0-0")) >= max_event
                and _entry_key(body.get("last_request_id", "0-0")) >= max_request
                and body.get("events_pending") == 0
                and body.get("requests_pending") == 0
            ):
                return True, last_note
        time.sleep(2)
    return False, last_note


def _features_mismatch(actual: Any, expected: dict[str, Any]) -> str | None:
    if not isinstance(actual, dict):
        return "признаки не являются JSON-объектом"
    for name in FEATURE_NAMES:
        if name not in actual:
            return f"нет признака {name}"
        got, want = actual[name], expected.get(name)
        if name in _INT_FEATURES:
            if isinstance(got, bool) or not isinstance(got, int) or got != want:
                return f"{name}: ожидалось {want}, получено {got!r}"
        else:
            try:
                if Decimal(str(got)) != Decimal(str(want)):
                    return f"{name}: ожидалось {want}, получено {got!r}"
            except Exception:  # noqa: BLE001
                return f"{name}: нечисловое значение {got!r}"
    return None


def smoke(report: Report, expected_fixture: dict[str, Any], timeout: int) -> None:
    environment = _environment(INPUT_DIR)
    toy = expected_fixture["check_toy"]
    requests_expected = toy.get("requests", {})

    print(
        "внимание: локальная проверка всегда стартует с чистых томов "
        "(docker compose down -v); сохранённое состояние потока будет удалено"
    )
    try:
        _run([*_compose(), "down", "-v", "--remove-orphans"], environment, 300)
        _up(environment, timeout)
        _wait_gateway()
    except BaseException:
        _run([*_compose(), "down", "--remove-orphans"], environment, 300)
        raise
    try:
        # primary activation (the server check does it too, §1)
        status, body = _http(
            "POST",
            BASE_URL + "/admin/activate",
            {
                "model_name": "hw4-logistic",
                "model_version": "1",
                "activation_id": "hw5-check-init",
            },
        )
        if status != 200:
            report.failed_one(
                "RUN", f"POST /admin/activate вернул HTTP {status}: {body}"
            )
            return
        report.passed_one(
            "RUN", "стек поднят, gateway здоров, первичная активация выполнена"
        )

        # T0: groups on both streams
        deadline = time.monotonic() + GROUP_TIMEOUT
        ready = False
        while time.monotonic() < deadline and not ready:
            ready = _groups_ready(environment)
            if not ready:
                time.sleep(3)
        if ready:
            report.passed_one(
                "T0", "consumer group hw5-workers есть на hw5:events и hw5:requests"
            )
        else:
            report.failed_one(
                "T0",
                "группа hw5-workers не появилась на обоих потоках за "
                f"{GROUP_TIMEOUT}s; stream-worker должен создавать группы при старте "
                "(XGROUP CREATE ... MKSTREAM, BUSYGROUP игнорируется); "
                "диагностика: docker compose logs stream-worker",
            )
            return

        # T1: identity on a toy flow
        status, body = _worker_http(
            environment,
            "POST",
            "/admin/time",
            {"now": "2011-02-01", "closed_until": "2011-01-01"},
        )
        if status != 200:
            report.failed_one("T1", f"POST /admin/time вернул HTTP {status}: {body}")
            return
        first = _xadd(environment, "hw5:events", EVENT_PAYLOAD)
        duplicate = _xadd(environment, "hw5:events", EVENT_PAYLOAD)
        ok, note = _wait_boundary(environment, _entry_key(duplicate), (0, 0))
        status, body = _worker_http(environment, "GET", "/stream/status")
        counters = body or {}
        problems = []
        if not ok:
            problems.append("граница обработки не достигнута: " + note)
        for field, want in (
            ("received_total", 2),
            ("attributed_new", 1),
            ("duplicates", 1),
            ("invalid_events", 0),
            ("conflicts", 0),
            ("closed_past_rejected", 0),
        ):
            if counters.get(field) != want:
                problems.append(
                    f"{field}: ожидалось {want}, получено {counters.get(field)!r}"
                )
        if problems:
            report.failed_one(
                "T1",
                "; ".join(problems)
                + "; диагностика: docker compose logs stream-worker, "
                "docker compose exec redis redis-cli XPENDING hw5:events hw5-workers",
            )
        else:
            report.passed_one("T1", "идентичность: приём + duplicate, счётчики совпали")

        # T2/T3: queued predictions
        request_ok = _xadd(
            environment,
            "hw5:requests",
            {"request_id": "r-chk-1", "client_id": "A", "as_of": "2011-02-01"},
        )
        request_no = _xadd(
            environment,
            "hw5:requests",
            {"request_id": "r-chk-2", "client_id": "D", "as_of": "2011-02-01"},
        )
        ok, note = _wait_boundary(
            environment, _entry_key(duplicate), _entry_key(request_no)
        )
        if not ok:
            report.failed_one("T2", "граница обработки заданий не достигнута: " + note)
            return

        expected_ok = requests_expected.get("r-chk-1", {})
        status, body = _http("GET", BASE_URL + "/predictions/r-chk-1")
        problems = []
        if status != 200 or not isinstance(body, dict):
            problems.append(f"HTTP {status}")
        else:
            if body.get("status") != "ok":
                problems.append(f"status: ожидался ok, получен {body.get('status')!r}")
            problem = _features_mismatch(
                body.get("features"), expected_ok.get("features", {})
            )
            if problem:
                problems.append(problem)
            if body.get("model_name") != expected_ok.get("model_name") or str(
                body.get("model_version")
            ) != str(expected_ok.get("model_version")):
                problems.append(
                    f"модель: ожидалась {expected_ok.get('model_name')} v"
                    f"{expected_ok.get('model_version')}, получена "
                    f"{body.get('model_name')!r} v{body.get('model_version')!r}"
                )
            probability = body.get("probability")
            if (
                not isinstance(probability, (int, float))
                or isinstance(probability, bool)
                or abs(float(probability) - float(expected_ok.get("probability")))
                > TOLERANCE
            ):
                problems.append(
                    f"probability: ожидалась {expected_ok.get('probability')!r}, "
                    f"получена {probability!r} (допуск 1e-9)"
                )
        if problems:
            report.failed_one(
                "T2",
                "; ".join(problems)
                + "; диагностика: docker compose logs stream-worker model-service, "
                "curl -s http://127.0.0.1:8080/predictions/r-chk-1",
            )
        else:
            report.passed_one(
                "T2",
                "прогноз ok: признаки и вероятность совпали с независимым расчётом "
                "(допуск 1e-9)",
            )

        status, body = _http("GET", BASE_URL + "/predictions/r-chk-2")
        problems = []
        if status != 200 or not isinstance(body, dict):
            problems.append(f"HTTP {status}")
        else:
            if body.get("status") != "no_history":
                problems.append(
                    f"status: ожидался no_history, получен {body.get('status')!r}"
                )
            for field in ("features", "model_name", "model_version", "probability"):
                if body.get(field) is not None:
                    problems.append(
                        f"{field}: ожидался null, получен {body.get(field)!r}"
                    )
        if problems:
            report.failed_one("T3", "; ".join(problems))
        else:
            report.passed_one("T3", "no_history: объявленный исход с null-полями")

        status, body = _http("GET", BASE_URL + "/predictions/r-chk-nope")
        code = (
            (body or {}).get("error", {}).get("code")
            if isinstance(body, dict)
            else None
        )
        if status == 404 and code == "prediction_not_found":
            report.passed_one("T4", "непринятый request_id: 404 prediction_not_found")
        else:
            report.failed_one(
                "T4",
                f"ожидался 404 prediction_not_found, получено HTTP {status}, код {code!r}",
            )

        # T5: monitor report sanity
        status, body = _monitor_http(environment, "GET", "/monitor/report")
        problems = []
        if status != 200 or not isinstance(body, dict):
            problems.append(f"HTTP {status}")
        else:
            if (
                body.get("now") != "2011-02-01"
                or body.get("closed_until") != "2011-01-01"
            ):
                problems.append(
                    f"время: ожидалось now=2011-02-01 closed_until=2011-01-01, "
                    f"получено {body.get('now')!r}/{body.get('closed_until')!r}"
                )
            quality = body.get("input_quality") or {}
            for field, want in (
                ("received_total", 2),
                ("duplicates", 1),
                ("attributed_new", 1),
            ):
                if quality.get(field) != want:
                    problems.append(
                        f"input_quality.{field}: ожидалось {want}, "
                        f"получено {quality.get(field)!r}"
                    )
            predictions = body.get("predictions") or {}
            if (
                predictions.get("total") != 2
                or predictions.get("ok") != 1
                or (predictions.get("no_history")) != 1
            ):
                problems.append(
                    "predictions: ожидалось total=2 ok=1 no_history=1, получено "
                    + json.dumps(predictions)
                )
            stream_health = body.get("stream_health") or {}
            if (
                stream_health.get("events_pending") != 0
                or (stream_health.get("requests_pending")) != 0
            ):
                problems.append(
                    "stream_health: ожидалось 0 pending, получено "
                    + json.dumps(stream_health)
                )
        if problems:
            report.failed_one(
                "T5",
                "; ".join(problems) + "; диагностика: docker compose logs monitor, "
                'docker compose exec monitor python -c "... GET /monitor/report"',
            )
        else:
            report.passed_one(
                "T5",
                "monitor report: время, счётчики, predictions и stream_health корректны",
            )
    finally:
        _run([*_compose(), "down", "--remove-orphans"], environment, 300)


def main() -> int:
    parser = argparse.ArgumentParser(description="Локальная проверка HW5")
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="проверить структуру и вход без сборки и запуска",
    )
    parser.add_argument("--timeout", type=int, default=5400)
    args = parser.parse_args()
    if not 60 <= args.timeout <= 7200:
        parser.error("--timeout должен быть от 60 до 7200 секунд")
    report = Report()
    try:
        preflight(report)
        check_tree(report)
        expected_fixture = check_input(report)
    except InvalidProject as error:
        report.failed_one("PREFLIGHT", error.code)
        print("FAIL: проект HW5 отклонён до запуска", file=sys.stderr)
        return 1
    except SystemUnavailable as error:
        print("SYSTEM_ERROR: " + str(error), file=sys.stderr)
        return 2
    if args.preflight_only:
        if report.failed:
            return 1
        print("PASS: структура и вход HW5 соответствуют границам локальной проверки")
        return 0
    try:
        smoke(report, expected_fixture, args.timeout)
    except InvalidProject as error:
        report.failed_one("RUN", error.code)
    except SystemUnavailable as error:
        print("SYSTEM_ERROR: " + str(error), file=sys.stderr)
        return 2
    if report.failed:
        print(
            "FAIL: локальная проверка HW5 не пройдена; смотрите сообщения выше",
            file=sys.stderr,
        )
        return 1
    print("PASS: локальная проверка HW5 пройдена по всем пунктам")
    print("Локальный PASS не заменяет серверный зачёт.")
    print(
        "Тома текущего прогона сохранены для осмотра; следующий запуск начнёт "
        "с чистых томов (проверка сама выполняет docker compose down -v)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
