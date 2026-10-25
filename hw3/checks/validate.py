"""Static safety checks for a rendered HW3 Compose configuration.

This module is deliberately a preflight, not a sandbox.  The caller must run
the resulting project in its disposable VM and still apply the VM-wide CPU,
memory, deadline and egress controls.  Its job is narrower: prevent a student
Compose file from changing the VM/Docker boundary before ``build`` or ``up``.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

REQUIRED_SERVICES = frozenset(
    {
        "postgres",
        "airflow-init",
        "airflow-webserver",
        "airflow-scheduler",
        "spark-master",
        "spark-worker",
        "seaweedfs",
        "mlflow",
    }
)

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
    }
)
_SENSITIVE_MOUNT_TARGETS = (
    "/dev",
    "/etc/docker",
    "/opt/hw3-controller",
    "/proc",
    "/run",
    "/submission",
    "/sys",
    "/var/run",
)
_SERVICE_NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}")


class InvalidProject(ValueError):
    """A submission-owned Compose setting violates the project boundary."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _mapping(value: object, code: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise InvalidProject(code)
    return value


def _project_name(config: dict[str, Any]) -> str:
    name = config.get("name")
    if not isinstance(name, str) or _SERVICE_NAME.fullmatch(name) is None:
        raise InvalidProject("project_name_invalid")
    return name


def _existing_path(value: object, root: Path, code: str) -> Path:
    if not isinstance(value, str) or not value:
        raise InvalidProject(code)
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = root / candidate
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError):
        raise InvalidProject(code) from None
    return resolved


def _path_beneath(value: object, root: Path, code: str) -> Path:
    """Resolve an existing path, interpreting relative values from ``root``."""
    if not isinstance(value, str) or not value:
        raise InvalidProject(code)
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = root / candidate
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError):
        raise InvalidProject(code) from None
    return resolved


def _project_scoped(name: object, project_name: str, code: str) -> None:
    if not isinstance(name, str) or not name.startswith(project_name + "_"):
        raise InvalidProject(code)


def _is_readonly(mount: dict[str, Any]) -> bool:
    return mount.get("read_only") is True or mount.get("readonly") is True


def _sensitive_target(target: object) -> bool:
    if not isinstance(target, str) or not target.startswith("/"):
        return True
    return any(
        target == prefix or target.startswith(prefix + "/") for prefix in _SENSITIVE_MOUNT_TARGETS
    )


def _validate_top_level_objects(
    config: dict[str, Any], project_name: str, project_dir: Path
) -> None:
    for kind in ("volumes", "networks"):
        values = config.get(kind, {})
        values = _mapping(values, f"{kind}_invalid")
        for name, definition in values.items():
            if not isinstance(name, str):
                raise InvalidProject(f"{kind}_invalid")
            data = _mapping(definition, f"{kind}_invalid") if definition else {}
            if data.get("external"):
                raise InvalidProject(f"{kind}_external")
            if data.get("driver_opts"):
                raise InvalidProject(f"{kind}_driver_opts_forbidden")
            if kind == "volumes" and data.get("driver") not in (None, "local"):
                raise InvalidProject("volumes_driver_forbidden")
            if kind == "networks" and data.get("driver") not in (None, "bridge"):
                raise InvalidProject("networks_driver_forbidden")
            if "name" in data:
                _project_scoped(data["name"], project_name, f"{kind}_name_unscoped")
    for kind in ("secrets", "configs"):
        values = _mapping(config.get(kind, {}), f"{kind}_invalid")
        for definition in values.values():
            data = _mapping(definition, f"{kind}_invalid") if definition else {}
            if data.get("external"):
                raise InvalidProject(f"{kind}_external")
            if "file" in data:
                _existing_path(data["file"], project_dir, f"{kind}_path_invalid")


def _validate_build(value: object, project_dir: Path, service: str) -> None:
    if isinstance(value, str):
        _existing_path(value, project_dir, f"{service}_build_context_invalid")
        return
    build = _mapping(value, f"{service}_build_invalid")
    context = _existing_path(build.get("context"), project_dir, f"{service}_build_context_invalid")
    if "dockerfile" in build:
        _path_beneath(build["dockerfile"], context, f"{service}_dockerfile_invalid")
    for key in ("additional_contexts", "secrets", "ssh", "privileged", "entitlements"):
        if build.get(key):
            raise InvalidProject(f"{service}_build_{key}_forbidden")
    # A named Docker network can be an existing host-controlled bridge.  The
    # build needs package access only through Docker's ordinary default, or no
    # network at all; it must not select an arbitrary network.
    if build.get("network") not in (None, "default", "none"):
        raise InvalidProject(f"{service}_build_network_forbidden")


def _validate_mount(
    mount: object,
    *,
    project_dir: Path,
    input_dir: Path,
    declared_volumes: set[str],
    service: str,
) -> None:
    data = _mapping(mount, f"{service}_mount_invalid")
    kind = data.get("type")
    target = data.get("target")
    if _sensitive_target(target):
        raise InvalidProject(f"{service}_mount_target_forbidden")
    if kind == "bind":
        source = data.get("source")
        if not isinstance(source, str) or not source:
            raise InvalidProject(f"{service}_bind_source_invalid")
        raw = Path(source)
        try:
            candidate = raw if raw.is_absolute() else project_dir / raw
            resolved = candidate.resolve(strict=True)
        except OSError:
            raise InvalidProject(f"{service}_bind_source_invalid") from None
        if resolved == input_dir:
            if target != "/input" or not _is_readonly(data):
                raise InvalidProject(f"{service}_trusted_input_mount_invalid")
            return
        try:
            resolved.relative_to(project_dir)
        except ValueError:
            raise InvalidProject(f"{service}_bind_source_invalid") from None
        if target == "/input":
            raise InvalidProject(f"{service}_trusted_input_mount_invalid")
        if not _is_readonly(data):
            raise InvalidProject(f"{service}_bind_not_readonly")
        return
    if kind == "volume":
        source = data.get("source")
        if not isinstance(source, str) or source not in declared_volumes:
            raise InvalidProject(f"{service}_volume_source_invalid")
        return
    if kind == "tmpfs":
        return
    raise InvalidProject(f"{service}_mount_type_invalid")


def _validate_security(service: str, definition: dict[str, Any]) -> None:
    if definition.get("privileged") is True:
        raise InvalidProject(f"{service}_privileged_forbidden")
    if definition.get("ports"):
        raise InvalidProject(f"{service}_ports_forbidden")
    for option in _FORBIDDEN_SERVICE_OPTIONS:
        if definition.get(option):
            raise InvalidProject(f"{service}_{option}_forbidden")
    security = definition.get("security_opt", [])
    if security not in (None, [], ["no-new-privileges:true"]):
        raise InvalidProject(f"{service}_security_opt_forbidden")
    deploy = definition.get("deploy")
    if deploy:
        deploy = _mapping(deploy, f"{service}_deploy_invalid")
        reservations = _mapping(deploy.get("resources", {}), f"{service}_resources_invalid").get(
            "reservations", {}
        )
        if reservations and _mapping(reservations, f"{service}_reservations_invalid").get(
            "devices"
        ):
            raise InvalidProject(f"{service}_reserved_devices_forbidden")


def validate_config(config: dict, project_dir: Path, input_dir: Path) -> None:
    """Validate rendered Compose JSON before building an untrusted HW3 project.

    ``project_dir`` and ``input_dir`` are controller-owned, existing absolute
    directories.  The latter is the only host path that may enter a service
    outside the submitted project, and only as ``/input`` read-only.
    """
    if not isinstance(config, dict):
        raise InvalidProject("compose_invalid")
    try:
        root = project_dir.resolve(strict=True)
        trusted_input = input_dir.resolve(strict=True)
    except OSError:
        raise InvalidProject("controller_path_invalid") from None
    if not root.is_dir() or not trusted_input.is_dir() or trusted_input == root:
        raise InvalidProject("controller_path_invalid")
    project_name = _project_name(config)
    _validate_top_level_objects(config, project_name, root)
    services = _mapping(config.get("services"), "services_invalid")
    missing = REQUIRED_SERVICES - services.keys()
    if missing:
        raise InvalidProject("required_services_missing")
    declared_volumes = set(_mapping(config.get("volumes", {}), "volumes_invalid"))
    declared_networks = set(_mapping(config.get("networks", {}), "networks_invalid")) | {"default"}
    for name, value in services.items():
        if not isinstance(name, str):
            raise InvalidProject("services_invalid")
        service = _mapping(value, f"{name}_service_invalid")
        _validate_security(name, service)
        if "build" in service:
            _validate_build(service["build"], root, name)
        image = service.get("image")
        if "build" not in service and (not isinstance(image, str) or "@sha256:" not in image):
            raise InvalidProject(f"{name}_image_unpinned")
        mounts = service.get("volumes", [])
        if not isinstance(mounts, list):
            raise InvalidProject(f"{name}_mounts_invalid")
        for mount in mounts:
            _validate_mount(
                mount,
                project_dir=root,
                input_dir=trusted_input,
                declared_volumes=declared_volumes,
                service=name,
            )
        networks = service.get("networks", {"default": {}})
        names = set(networks) if isinstance(networks, (dict, list)) else set()
        if not names or not names <= declared_networks:
            raise InvalidProject(f"{name}_networks_invalid")
        if isinstance(networks, dict) and any(
            isinstance(settings, dict) and settings.get("aliases")
            for settings in networks.values()
        ):
            # DNS names of course-owned endpoints (for example ``mlflow``)
            # must not be shared with an extra student service.  Compose
            # already exposes every service under its service name, while the
            # permitted ``hostname`` setting covers Spark's own identity.
            raise InvalidProject(f"{name}_network_aliases_forbidden")
        if name in REQUIRED_SERVICES:
            if service.get("profiles") or "default" not in names:
                raise InvalidProject(f"{name}_required_runtime_invalid")
            healthcheck = service.get("healthcheck")
            if name != "airflow-init" and (
                not isinstance(healthcheck, dict) or healthcheck.get("disable") is True
            ):
                raise InvalidProject(f"{name}_healthcheck_missing")
