"""Public launcher tests; no reference implementation and no Docker needed."""

from __future__ import annotations

import gzip
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

SOURCE = Path(__file__).parents[1]


def _module(name: str, relative: str):
    path = SOURCE / relative
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


check = _module("hw4_check", "check.py")
validate = _module("hw4_validate", "checks/validate.py")
DATA = SOURCE / "data"


def _project(tmp_path: Path) -> tuple[Path, Path]:
    project = tmp_path / "hw4"
    (project / "data").mkdir(parents=True)
    return project, project / "data"


def _config(project: Path, input_dir: Path) -> dict[str, Any]:
    # docker compose config renders build contexts as absolute paths.
    return {
        "services": {
            "gateway": {
                "build": {"context": str(project)},
                "ports": [{"published": 8080, "target": 8080, "protocol": "tcp"}],
                "depends_on": {
                    "feature-build": {"condition": "service_completed_successfully"},
                    "feature-loader": {"condition": "service_completed_successfully"},
                },
            },
            "feature-service": {
                "build": {"context": str(project / "service" / "feature-service")}
            },
            "model-service": {
                "build": {"context": str(project / "service" / "model-service")}
            },
            "feature-build": {
                "build": {"context": str(project / "offline" / "feature-build")},
                "volumes": [
                    {
                        "type": "bind",
                        "source": str(input_dir),
                        "target": "/input",
                        "read_only": True,
                    }
                ],
            },
            "feature-loader": {
                "build": {"context": str(project / "offline" / "feature-loader")},
                "depends_on": {
                    "feature-build": {"condition": "service_completed_successfully"}
                },
            },
            "redis": {"image": "redis:7.4.2-alpine"},
            "postgres": {"image": "postgres:16.6-bookworm"},
            "mlflow": {"image": "ghcr.io/mlflow/mlflow:v2.22.0"},
            "seaweedfs": {"image": "chrislusf/seaweedfs:4.48"},
            "airflow-init": {"image": "apache/airflow:2.10.5-python3.12"},
            "airflow-webserver": {"image": "apache/airflow:2.10.5-python3.12"},
            "airflow-scheduler": {"image": "apache/airflow:2.10.5-python3.12"},
            "spark-master": {"image": "bitnami/spark:3.5.4"},
            "spark-worker": {"image": "bitnami/spark:3.5.4"},
        }
    }


def test_config_accepts_contract_compose(tmp_path: Path) -> None:
    project, input_dir = _project(tmp_path)
    validate.validate_config(_config(project, input_dir), project, input_dir)


def test_config_requires_all_fourteen_services(tmp_path: Path) -> None:
    project, input_dir = _project(tmp_path)
    for service in ("redis", "airflow-scheduler", "spark-worker", "feature-loader"):
        config = json.loads(json.dumps(_config(project, input_dir)))
        del config["services"][service]
        with pytest.raises(
            validate.InvalidProject, match="compose_required_service_missing"
        ):
            validate.validate_config(config, project, input_dir)


def test_config_requires_gateway_port_8080(tmp_path: Path) -> None:
    project, input_dir = _project(tmp_path)
    no_port = json.loads(json.dumps(_config(project, input_dir)))
    no_port["services"]["gateway"]["ports"] = []
    with pytest.raises(validate.InvalidProject, match="compose_gateway_port_missing"):
        validate.validate_config(no_port, project, input_dir)

    exposed = json.loads(json.dumps(_config(project, input_dir)))
    exposed["services"]["redis"]["ports"] = [{"published": 6379, "target": 6379}]
    with pytest.raises(validate.InvalidProject, match="compose_unexpected_host_port"):
        validate.validate_config(exposed, project, input_dir)


def test_config_requires_read_only_input_mount(tmp_path: Path) -> None:
    project, input_dir = _project(tmp_path)
    writable = json.loads(json.dumps(_config(project, input_dir)))
    writable["services"]["feature-build"]["volumes"][0]["read_only"] = False
    with pytest.raises(
        validate.InvalidProject, match="compose_input_mount_not_read_only"
    ):
        validate.validate_config(writable, project, input_dir)

    missing = json.loads(json.dumps(_config(project, input_dir)))
    missing["services"]["feature-build"]["volumes"] = []
    with pytest.raises(validate.InvalidProject, match="compose_input_mount_missing"):
        validate.validate_config(missing, project, input_dir)


def test_config_requires_one_shot_ordering(tmp_path: Path) -> None:
    project, input_dir = _project(tmp_path)
    no_dependency = json.loads(json.dumps(_config(project, input_dir)))
    no_dependency["services"]["gateway"]["depends_on"].pop("feature-loader")
    with pytest.raises(
        validate.InvalidProject, match="compose_gateway_dependency_missing"
    ):
        validate.validate_config(no_dependency, project, input_dir)

    no_loader_order = json.loads(json.dumps(_config(project, input_dir)))
    no_loader_order["services"]["feature-loader"].pop("depends_on")
    with pytest.raises(
        validate.InvalidProject, match="compose_loader_dependency_missing"
    ):
        validate.validate_config(no_loader_order, project, input_dir)

    looping = json.loads(json.dumps(_config(project, input_dir)))
    looping["services"]["feature-build"]["restart"] = "always"
    with pytest.raises(
        validate.InvalidProject, match="compose_oneshot_restart_forbidden"
    ):
        validate.validate_config(looping, project, input_dir)


def test_config_rejects_forbidden_options_and_mounts(tmp_path: Path) -> None:
    project, input_dir = _project(tmp_path)
    privileged = json.loads(json.dumps(_config(project, input_dir)))
    privileged["services"]["gateway"]["privileged"] = True
    with pytest.raises(validate.InvalidProject, match="compose_forbidden_option"):
        validate.validate_config(privileged, project, input_dir)

    sensitive = json.loads(json.dumps(_config(project, input_dir)))
    sensitive["services"]["gateway"]["volumes"] = [
        {"type": "bind", "source": str(project), "target": "/var/run"}
    ]
    with pytest.raises(validate.InvalidProject, match="compose_sensitive_mount"):
        validate.validate_config(sensitive, project, input_dir)

    outside = json.loads(json.dumps(_config(project, input_dir)))
    outside["services"]["feature-build"]["volumes"][0]["source"] = str(
        tmp_path / "elsewhere"
    )
    with pytest.raises(validate.InvalidProject, match="compose_bind_outside_project"):
        validate.validate_config(outside, project, input_dir)


def test_history_accepts_and_rejects_schema(tmp_path: Path) -> None:
    assert validate.validate_history(DATA / "history.csv.gz") >= 5

    bad = tmp_path / "history.csv.gz"
    bad.write_bytes(gzip.compress("invoice_id,stock_code\nI1,S1\n".encode("utf-8")))
    with pytest.raises(validate.InvalidProject, match="history_columns_invalid"):
        validate.validate_history(bad)

    wrong_types = tmp_path / "types.csv.gz"
    wrong_types.write_bytes(
        gzip.compress(
            (
                "invoice_id,stock_code,description,quantity,invoice_at,"
                "unit_price,customer_id,country\n"
                "I1,S1,item,many,2011-01-01T10:00:00,1.0,A,GB\n"
            ).encode("utf-8")
        )
    )
    with pytest.raises(validate.InvalidProject, match="history_row_types_invalid"):
        validate.validate_history(wrong_types)


def test_request_accepts_fixture_and_rejects_violations() -> None:
    payload = json.loads((DATA / "request.json").read_text(encoding="utf-8"))
    digest = validate.history_digest(DATA / "history.csv.gz")
    assert len(digest) == 64
    snapshots = validate.validate_request(payload, digest)
    assert [item["as_of"] for item in snapshots] == [
        "2011-03-01T00:00:00",
        "2011-04-01T00:00:00",
    ]

    wrong_version = dict(payload, contract_version="hw3-serving-v1")
    with pytest.raises(validate.InvalidProject, match="request_contract_version"):
        validate.validate_request(wrong_version, digest)

    wrong_hash = dict(payload, input_sha256="0" * 64)
    with pytest.raises(validate.InvalidProject, match="sha256_mismatch"):
        validate.validate_request(wrong_hash, digest)

    outside = json.loads(json.dumps(payload))
    outside["snapshots"][0]["as_of"] = "2010-01-01T00:00:00"
    with pytest.raises(validate.InvalidProject, match="outside_window"):
        validate.validate_request(outside, digest)

    duplicate = json.loads(json.dumps(payload))
    duplicate["snapshots"][1]["as_of"] = duplicate["snapshots"][0]["as_of"]
    with pytest.raises(validate.InvalidProject, match="request_snapshot_duplicate"):
        validate.validate_request(duplicate, digest)


def test_features_mismatch_compares_types_tolerantly() -> None:
    expected = {
        "order_count_90d": 3,
        "total_amount_90d": "55.000000",
        "average_order_amount_90d": "18.33",
        "days_since_last_order": 9,
        "distinct_stock_codes_90d": 3,
        "return_rate_90d": "0.250000",
        "order_count_30d": 2,
        "total_amount_30d": "35.000000",
    }
    assert check.features_mismatch(expected, expected) is None
    numeric = dict(expected, total_amount_90d=55.0, return_rate_90d=0.25)
    assert check.features_mismatch(numeric, expected) is None
    rounded = dict(expected, average_order_amount_90d=18.3300004)
    assert check.features_mismatch(rounded, expected) is None
    assert check.features_mismatch(dict(expected, order_count_90d=4), expected)
    assert check.features_mismatch(dict(expected, days_since_last_order=-1), expected)
    assert check.features_mismatch({"order_count_90d": 3}, expected)
    assert check.features_mismatch([1, 2], expected)


@pytest.mark.skipif(
    subprocess.run(["git", "--version"], capture_output=True).returncode != 0,
    reason="git is not available",
)
def test_repo_hygiene_flags_artifacts_and_secrets(tmp_path: Path) -> None:
    repo = tmp_path / "submission"
    repo.mkdir()
    project = repo / "hw4"
    (project / "service").mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(repo)], check=True)

    def commit() -> None:
        subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "-c",
                "user.email=tests@example.invalid",
                "-c",
                "user.name=tests",
                "commit",
                "-q",
                "-m",
                "init",
            ],
            check=True,
        )

    clean = project / "README.md"
    clean.write_text("# hw4\n", encoding="utf-8")
    (project / "data").mkdir()
    (project / "data" / "history.csv.gz").write_bytes(
        (DATA / "history.csv.gz").read_bytes()
    )
    commit()
    assert [
        item for item in validate.validate_repo(project) if "repo_not_git" not in item
    ] == []

    tracked_parquet = project / "features.parquet"
    tracked_parquet.write_bytes(b"PAR1")
    commit()
    findings = validate.validate_repo(project)
    assert any("repo_artifact_committed" in item for item in findings)

    tracked_parquet.unlink()
    leaked = project / "service" / "settings.py"
    leaked.write_text('TOKEN = "ghp_' + "a" * 36 + '"\n', encoding="utf-8")
    commit()
    findings = validate.validate_repo(project)
    assert any("repo_secret_exposed" in item for item in findings)
