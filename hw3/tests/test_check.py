"""Public launcher tests; no reference implementation is imported here."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest


def _module():
    path = Path(__file__).parents[1] / "check.py"
    spec = importlib.util.spec_from_file_location("hw3_check", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


check = _module()


def test_preflight_accepts_rendered_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    project = tmp_path / "hw3"
    source = Path(__file__).parents[1]
    project.mkdir()
    input_dir = project / "data"
    input_dir.mkdir()
    rendered = {
        "name": "local_hw3",
        "services": {
            name: {
                "image": "example.invalid/x@sha256:" + "a" * 64,
                "networks": {"default": {}},
                "healthcheck": {"test": ["CMD", "true"]},
                "volumes": [{"type": "volume", "source": "state", "target": "/state"}],
            }
            for name in check.validate_config.__globals__["REQUIRED_SERVICES"]
        },
        "volumes": {"state": {"name": "local_hw3_state"}},
        "networks": {"default": {"name": "local_hw3_default"}},
    }
    rendered["services"]["airflow-init"].pop("healthcheck")
    (project / "compose.yaml").write_text("services: {}\n")
    source_file = project / "Dockerfile"
    source_file.write_text("FROM scratch\n")
    rendered["services"]["airflow-scheduler"]["build"] = {
        "context": str(project),
        "dockerfile": str(source_file),
    }
    rendered["services"]["airflow-scheduler"]["image"] = "local:latest"
    rendered["services"]["airflow-scheduler"]["volumes"].append(
        {"type": "bind", "source": str(input_dir), "target": "/input", "read_only": True}
    )

    def run(*_args, **_kwargs):
        return subprocess.CompletedProcess([], 0, json.dumps(rendered), "")

    monkeypatch.setattr(check.subprocess, "run", run)
    check.preflight(project, input_dir)


def test_placeholder_controller_is_explicit(tmp_path: Path) -> None:
    """The shipped controller runs with explicit arguments like check.py and
    reports an unfilled project as FAILED (compose_missing), never a crash."""
    root = Path(__file__).parents[1]
    out = tmp_path / "out"
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            str(root / "checks" / "guest_check.py"),
            "--project",
            str(root),
            "--submission-sha",
            "0" * 40,
            "--input-dir",
            str(root / "data"),
            "--request",
            str(root / "data" / "request.json"),
            "--out",
            str(out),
            "--timeout",
            "120",
            "--expected",
            str(root / "data" / "expected.json"),
        ],
        capture_output=True,
        text=True,
        check=False,
        cwd=root.parent,
    )
    assert completed.returncode == 1
    report = json.loads((out / "result.json").read_text(encoding="utf-8"))
    assert report["status"] == "FAILED"
    assert report["diagnostic"] == "compose_missing"
