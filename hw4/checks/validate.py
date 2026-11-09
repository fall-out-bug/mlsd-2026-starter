"""Static safety and schema checks for an HW4 Compose project.

Three groups, all without starting anything:

- the rendered Compose configuration (required services, ports, commands,
  mounts and dependencies of the ``hw4-serving-v1`` contract);
- the checker input schema (``history.csv.gz`` and ``request.json``);
- repository hygiene (no committed full history, Parquet, models, secrets).

This is a preflight, not a sandbox. It prevents the Compose file from changing
the VM/Docker boundary before build or up, and rejects obviously invalid
input before any container is started.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import re
import subprocess
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

CONTRACT_VERSION = "hw4-serving-v1"
HISTORY_NAME = "history.csv.gz"
REQUEST_NAME = "request.json"

REQUIRED_SERVICES = frozenset(
    {
        "gateway",
        "feature-service",
        "feature-build",
        "feature-loader",
        "model-service",
        "redis",
        "postgres",
        "mlflow",
        "seaweedfs",
    }
)

HISTORY_COLUMNS = (
    "invoice_id",
    "stock_code",
    "description",
    "quantity",
    "invoice_at",
    "unit_price",
    "customer_id",
    "country",
)

FEATURE_NAMES = (
    "order_count_90d",
    "total_amount_90d",
    "average_order_amount_90d",
    "days_since_last_order",
    "distinct_stock_codes_90d",
    "return_rate_90d",
    "order_count_30d",
    "total_amount_30d",
)

_TIMESTAMP = "%Y-%m-%dT%H:%M:%S"
_HEX64 = re.compile(r"[0-9a-f]{64}")

_FORBIDDEN_SERVICE_OPTIONS = frozenset(
    {
        "container_name",
        "devices",
        "device_cgroup_rules",
        "cap_add",
        "pid",
        "ipc",
        "uts",
        "userns_mode",
        "cgroup",
        "cgroup_parent",
        "cgroupns",
        "network_mode",
        "volumes_from",
        "external_links",
        "runtime",
        "gpus",
        "use_api_socket",
        "privileged",
    }
)

_SENSITIVE_MOUNT_TARGETS = (
    "/dev",
    "/etc",
    "/opt",
    "/proc",
    "/run",
    "/submission",
    "/sys",
    "/var/run",
)

# One-shot services must terminate on their own; a restart policy would turn
# a failed run into an infinite loop inside the checker budget.
_ONESHOT_SERVICES = ("feature-build", "feature-loader")
_LOOPING_RESTART_POLICIES = frozenset({"always", "on-failure", "unless-stopped"})

# Files that must never be tracked in Git: generated artifacts, volumes of
# the full dataset and anything that looks like a real credential.
_FORBIDDEN_TRACKED_PATTERNS = (
    re.compile(r"\.parquet$"),
    re.compile(r"\.joblib$"),
    re.compile(r"\.env$"),
    re.compile(r"(^|/)output/"),
    re.compile(r"(^|/)runtime/"),
    re.compile(r"(^|/)state/"),
    re.compile(r"online-retail.*\.csv\.gz$"),
)
_SECRET_PATTERNS = (
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"gh[pousr]_[A-Za-z0-9]{20,}"),
    re.compile(r"xox[baprs]-[A-Za-z0-9-]{10,}"),
    re.compile(r"sk-[A-Za-z0-9]{20,}"),
)
_TOY_HISTORY = "data/" + HISTORY_NAME
_SECRET_SCAN_LIMIT = 2 * 1024 * 1024

_SERVICE_NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}")


class InvalidProject(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _inside(path: str, root: Path) -> bool:
    try:
        Path(path).resolve().relative_to(root.resolve())
        return True
    except (OSError, ValueError):
        return False


def _volumes(service: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[str | dict[str, Any]] = []
    for item in service.get("volumes") or []:
        if isinstance(item, str):
            parts = item.split(":")
            target = parts[1] if len(parts) > 1 else parts[0]
            result.append(
                {
                    "source": parts[0],
                    "target": target,
                    "read_only": len(parts) > 2 and "ro" in parts[2],
                }
            )
        elif isinstance(item, dict):
            result.append(item)
    return [item for item in result if isinstance(item, dict)]


def _dependency_condition(service: dict[str, Any], dependency: str) -> object:
    depends = service.get("depends_on") or {}
    if isinstance(depends, dict):
        entry = depends.get(dependency)
        if isinstance(entry, dict):
            return entry.get("condition")
    return None


def validate_config(config: dict[str, Any], project: Path, input_dir: Path) -> None:
    services = config.get("services")
    if not isinstance(services, dict):
        raise InvalidProject("compose_services_missing")
    missing = REQUIRED_SERVICES - set(services)
    if missing:
        raise InvalidProject(
            "compose_required_service_missing: " + ", ".join(sorted(missing))
        )
    extra = set(services) - REQUIRED_SERVICES
    # Extra services are allowed only if they are clearly part of the project.
    for name in extra:
        if not _SERVICE_NAME.fullmatch(name):
            raise InvalidProject("compose_service_name_invalid")

    for name, raw in services.items():
        if not isinstance(raw, dict):
            raise InvalidProject("compose_service_invalid")
        forbidden = _FORBIDDEN_SERVICE_OPTIONS & set(raw)
        if forbidden:
            raise InvalidProject(
                f"compose_forbidden_option: {name}:{sorted(forbidden)[0]}"
            )
        build = raw.get("build")
        if isinstance(build, dict):
            context = build.get("context")
            if context and not _inside(str(context), project):
                raise InvalidProject(f"compose_build_outside_project: {name}")
        ports = raw.get("ports") or []
        if ports and name != "gateway":
            raise InvalidProject(f"compose_unexpected_host_port: {name}")
        networks = raw.get("networks") or {}
        if isinstance(networks, dict):
            if set(networks) - {"default"}:
                raise InvalidProject(f"compose_network_not_default: {name}")
        restart = raw.get("restart")
        if name in _ONESHOT_SERVICES and restart in _LOOPING_RESTART_POLICIES:
            raise InvalidProject(f"compose_oneshot_restart_forbidden: {name}")
        for volume in _volumes(raw):
            target = str(volume.get("target", ""))
            if any(
                target == prefix or target.startswith(prefix + "/")
                for prefix in _SENSITIVE_MOUNT_TARGETS
            ):
                raise InvalidProject(f"compose_sensitive_mount: {name}:{target}")
            source = str(volume.get("source", ""))
            if source.startswith("/") and not _inside(source, project):
                raise InvalidProject(f"compose_bind_outside_project: {name}:{source}")

    gateway = services["gateway"]
    gateway_ports = gateway.get("ports") or []
    published = {
        str(port.get("published"))
        for port in gateway_ports
        if isinstance(port, dict) and port.get("published")
    }
    if "8080" not in published:
        raise InvalidProject("compose_gateway_port_missing")

    for dependency in ("feature-build", "feature-loader"):
        if (
            _dependency_condition(gateway, dependency)
            != "service_completed_successfully"
        ):
            raise InvalidProject(f"compose_gateway_dependency_missing: {dependency}")
    if _dependency_condition(services["feature-loader"], "feature-build") != (
        "service_completed_successfully"
    ):
        raise InvalidProject("compose_loader_dependency_missing")

    build_service = services["feature-build"]
    input_mounts = [
        volume
        for volume in _volumes(build_service)
        if str(volume.get("target", "")) == "/input"
    ]
    if not input_mounts:
        raise InvalidProject("compose_input_mount_missing")
    mount = input_mounts[0]
    if not mount.get("read_only"):
        raise InvalidProject("compose_input_mount_not_read_only")
    if not _inside(str(mount.get("source", "")), project):
        # The input directory may be outside the project; it is supplied by the checker.
        if Path(str(mount.get("source", ""))).resolve() != input_dir.resolve():
            raise InvalidProject("compose_input_mount_unexpected")


def _timestamp(value: object, code: str) -> datetime:
    if not isinstance(value, str):
        raise InvalidProject(code)
    try:
        return datetime.strptime(value, _TIMESTAMP)
    except ValueError as error:
        raise InvalidProject(code) from error


def validate_history(path: Path) -> int:
    """Check the normalized HW3 history schema; return the number of data rows."""
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise InvalidProject("history_unreadable") from error
    try:
        text = gzip.decompress(raw).decode("utf-8")
    except (OSError, UnicodeError) as error:
        raise InvalidProject("history_not_gzip_utf8") from error
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration as error:
        raise InvalidProject("history_empty") from error
    if tuple(header) != HISTORY_COLUMNS:
        raise InvalidProject("history_columns_invalid")
    rows = 0
    for row in reader:
        if not row:
            continue
        rows += 1
        if len(row) != len(HISTORY_COLUMNS):
            raise InvalidProject(f"history_row_width_invalid:{rows}")
        try:
            int(row[3])
            Decimal(row[5])
        except (ValueError, InvalidOperation) as error:
            raise InvalidProject(f"history_row_types_invalid:{rows}") from error
        try:
            datetime.strptime(row[4], _TIMESTAMP)
        except ValueError as error:
            raise InvalidProject(f"history_row_timestamp_invalid:{rows}") from error
    if rows == 0:
        raise InvalidProject("history_no_rows")
    return rows


def validate_request(payload: object, history_sha256: str) -> list[dict[str, str]]:
    """Check ``request.json`` against ``hw4-serving-v1``; return the snapshots."""
    if not isinstance(payload, dict):
        raise InvalidProject("request_not_object")
    if payload.get("contract_version") != CONTRACT_VERSION:
        raise InvalidProject("request_contract_version_invalid")
    for field in ("execution_id", "dataset_version"):
        value = payload.get(field)
        if not isinstance(value, str) or not value:
            raise InvalidProject(f"request_field_invalid:{field}")
    if payload.get("input_path") != HISTORY_NAME:
        raise InvalidProject("request_input_path_invalid")
    declared = payload.get("input_sha256")
    if not isinstance(declared, str) or not _HEX64.fullmatch(declared):
        raise InvalidProject("request_input_sha256_invalid")
    if declared != history_sha256:
        raise InvalidProject("request_input_sha256_mismatch")
    observed_from = _timestamp(
        payload.get("observed_from"), "request_observed_from_invalid"
    )
    observed_until = _timestamp(
        payload.get("observed_until"), "request_observed_until_invalid"
    )
    if observed_from >= observed_until:
        raise InvalidProject("request_observed_window_invalid")
    days = payload.get("history_days")
    if not isinstance(days, int) or isinstance(days, bool) or days < 1:
        raise InvalidProject("request_history_days_invalid")
    snapshots = payload.get("snapshots")
    if not isinstance(snapshots, list) or not snapshots:
        raise InvalidProject("request_snapshots_missing")
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for index, snapshot in enumerate(snapshots):
        if not isinstance(snapshot, dict):
            raise InvalidProject(f"request_snapshot_invalid:{index}")
        as_of = _timestamp(snapshot.get("as_of"), f"request_snapshot_invalid:{index}")
        if not observed_from <= as_of <= observed_until:
            raise InvalidProject(f"request_snapshot_outside_window:{index}")
        split = snapshot.get("split")
        if not isinstance(split, str) or not split:
            raise InvalidProject(f"request_snapshot_split_invalid:{index}")
        key = as_of.strftime(_TIMESTAMP)
        if key in seen:
            raise InvalidProject(f"request_snapshot_duplicate:{index}")
        seen.add(key)
        result.append({"as_of": key, "split": split})
    return result


def history_digest(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as error:
        raise InvalidProject("history_unreadable") from error


def _repo_root(project: Path) -> Path | None:
    current = project.resolve()
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def validate_repo(project: Path) -> list[str]:
    """Return findings for tracked artifacts/secrets; never runs student code."""
    root = _repo_root(project)
    if root is None:
        return ["repo_not_git: проверка гигиены пропущена (каталог не в Git)"]
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "ls-files", "--", project.name],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ["repo_not_git: проверка гигиены пропущена (git недоступен)"]
    if result.returncode:
        return ["repo_not_git: проверка гигиены пропущена (git ls-files не удался)"]
    findings: list[str] = []
    tracked = [line for line in result.stdout.splitlines() if line]
    prefix = project.name + "/"
    for item in tracked:
        relative = item[len(prefix) :] if item.startswith(prefix) else item
        if any(pattern.search(relative) for pattern in _FORBIDDEN_TRACKED_PATTERNS):
            findings.append(f"repo_artifact_committed: {item}")
    for item in tracked:
        path = root / item
        try:
            if path.stat().st_size > _SECRET_SCAN_LIMIT:
                continue
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            continue
        for pattern in _SECRET_PATTERNS:
            if pattern.search(text):
                findings.append(f"repo_secret_exposed: {item}")
                break
    return findings
