"""Public local entry point for HW3 project checks.

The complete checker is course-owned.  This launcher always performs the same
Compose preflight that the course worker will apply, then hands the public toy
case to the packaged guest controller when that controller is present.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from checks.validate import InvalidProject, validate_config


CHECKS = ROOT / "checks"
TOY_INPUT = ROOT / "data"
TOY_REQUEST = TOY_INPUT / "request.json"
TOY_EXPECTED = TOY_INPUT / "expected.json"


def _submission_sha() -> str:
    result = subprocess.run(
        ["git", "-C", str(ROOT.parent), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    value = result.stdout.strip()
    if result.returncode == 0 and re.fullmatch(r"[0-9a-f]{40}", value):
        return value
    return "0" * 40


def _compose(project: Path, input_dir: Path) -> tuple[list[str], dict[str, str]]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if key in {"HOME", "LANG", "PATH", "TMPDIR"}
    }
    environment["HW3_INPUT_DIR"] = str(input_dir)
    return (
        [
            "docker",
            "compose",
            "--project-directory",
            str(project),
            "-f",
            str(project / "compose.yaml"),
        ],
        environment,
    )


def preflight(project: Path = ROOT, input_dir: Path = TOY_INPUT) -> None:
    if not (project / "compose.yaml").is_file():
        raise InvalidProject(
            "compose_missing: создайте hw3/compose.yaml со всеми обязательными "
            "сервисами по разделу 2 TECHNICAL.md"
        )
    compose, environment = _compose(project, input_dir)
    try:
        result = subprocess.run(
            [*compose, "config", "--format", "json"],
            cwd=project,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RuntimeError("docker_compose_unavailable") from error
    if result.returncode:
        raise InvalidProject("compose_config_invalid")
    try:
        config = json.loads(result.stdout)
    except ValueError as error:
        raise InvalidProject("compose_config_invalid") from error
    validate_config(config, project, input_dir)


def main() -> int:
    parser = argparse.ArgumentParser(description="Локальная проверка HW3")
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="проверить Compose-границы без сборки и запуска",
    )
    parser.add_argument("--timeout", type=int, default=2700)
    args = parser.parse_args()
    if not 60 <= args.timeout <= 2700:
        parser.error("--timeout должен быть от 60 до 2700 секунд")
    try:
        preflight()
    except InvalidProject as error:
        print("FAIL: Compose-проект HW3 отклонён: " + error.code, file=sys.stderr)
        return 1
    except RuntimeError:
        print("SYSTEM_ERROR: Docker Compose недоступен", file=sys.stderr)
        return 2
    if args.preflight_only:
        print("PASS: Compose-конфигурация HW3 соответствует границам локальной проверки")
        return 0

    controller = CHECKS / "guest_check.py"
    if not controller.is_file():
        print(
            "SYSTEM_ERROR: полный локальный checker HW3 ещё не установлен; "
            "выполните обновление стартера",
            file=sys.stderr,
        )
        return 2
    output = ROOT / "output" / ("local-" + uuid.uuid4().hex)
    output.parent.mkdir(mode=0o700, exist_ok=True)
    command = [
        sys.executable,
        "-I",
        str(controller),
        "--project",
        str(ROOT),
        "--submission-sha",
        _submission_sha(),
        "--input-dir",
        str(TOY_INPUT),
        "--request",
        str(TOY_REQUEST),
        "--out",
        str(output),
        "--timeout",
        str(args.timeout),
        "--expected",
        str(TOY_EXPECTED),
    ]
    return subprocess.run(command, cwd=ROOT.parent, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
