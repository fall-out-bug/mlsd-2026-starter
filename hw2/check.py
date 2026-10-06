"""Local HW2 project check. Production must use an isolated Docker daemon."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

SERVICES = {
    "postgres",
    "airflow-init",
    "airflow-webserver",
    "airflow-scheduler",
    "spark-master",
    "spark-worker",
    "seaweedfs",
    "redis",
}
FORBIDDEN = {
    "container_name",
    "privileged",
    "devices",
    "cap_add",
    "pid",
    "ipc",
    "network_mode",
    "volumes_from",
    "external_links",
}

EXPLANATIONS = {
    "dag_failed": "DAG завершился с ошибкой; прочитайте логи задач в каталоге проверки",
    "dag_deadline_exceeded": "DAG не завершился за отведённое время",
    "dag_or_api_not_found": "DAG online_retail_features или API Airflow не найден",
    "required_tasks_missing": "В DAG отсутствуют обязательные задачи",
    "required_task_dependency_missing": "Обязательные задачи не соединены в нужном порядке",
    "required_task_not_successful": "Одна из обязательных задач не завершилась успешно",
    "spark_worker_not_registered": "У Spark master нет зарегистрированного worker",
    "spark_application_not_observed": "После DAG не найдено нового завершённого Spark application",
    "s3_input_mismatch": "Исходный CSV в S3 не совпадает с предоставленным входом",
    "s3_output_missing": "В S3 отсутствует обязательный bucket или файл",
    "parquet_missing": "В S3 нет Parquet с признаками",
    "parquet_invalid": "Сохранённый файл не читается как Parquet",
    "parquet_schema_invalid": "Поля или типы Parquet отличаются от условия",
    "parquet_customer_invalid": "В Parquet пустые значения или повторяющиеся клиенты",
    "s3_parquet_mismatch": "Клиенты или суммы в Parquet отличаются от ожидаемых",
    "redis_mismatch": "Клиенты или суммы в Redis отличаются от ожидаемых",
    "redis_extra_customers": "В Redis остались лишние клиенты; заменяйте результат при запуске",
    "redis_value_invalid": "Некорректное значение в Redis",
    "redis_json_invalid": "Значение в Redis не является корректным JSON",
    "redis_order_count_invalid": "order_count в Redis должен быть целым числом",
    "storage_observation_unavailable": "Не удалось прочитать S3/Redis; результат не установлен",
}


class InvalidProject(ValueError):
    pass


def contained(path: str, root: Path) -> bool:
    return Path(path).resolve().is_relative_to(root.resolve())


def validate_config(config: dict, root: Path) -> None:
    services = config.get("services", {})
    missing = SERVICES - services.keys()
    if missing:
        raise InvalidProject("Добавьте сервисы: " + ", ".join(sorted(missing)))
    versions = json.loads((Path(__file__).parent / "versions.json").read_text())
    images = versions["images"]
    for kind in ("volumes", "networks"):
        if any(
            value
            and (
                value.get("external")
                or value.get("name")
                and not value["name"].startswith(config["name"] + "_")
            )
            for value in config.get(kind, {}).values()
        ):
            raise InvalidProject("Используйте volumes и networks текущего Compose-проекта")
    for kind in ("secrets", "configs"):
        for value in config.get(kind, {}).values():
            if value.get("external") or value.get("file") and not contained(value["file"], root):
                raise InvalidProject("Файлы secrets/configs должны находиться в hw2/")
    for name, service in services.items():
        if any(service.get(field) for field in FORBIDDEN):
            raise InvalidProject(f"{name}: настройка мешает изолированному запуску проекта")
        if service.get("security_opt"):
            raise InvalidProject(f"{name}: собственный security_opt не поддерживается")
        build = service.get("build")
        if build:
            context = build if isinstance(build, str) else build.get("context", "")
            if not contained(context, root):
                raise InvalidProject(f"{name}: build context должен находиться в hw2/")
            if isinstance(build, dict):
                if build.get("additional_contexts") or build.get("secrets") or build.get("ssh"):
                    raise InvalidProject(f"{name}: сборка должна использовать файлы проекта")
                dockerfile = Path(context) / build.get("dockerfile", "Dockerfile")
                if not contained(str(dockerfile), root):
                    raise InvalidProject(f"{name}: Dockerfile должен находиться в hw2/")
        elif not re.search(r"@sha256:[0-9a-f]{64}$", service.get("image", "")):
            raise InvalidProject(f"{name}: закрепите внешний image по digest из versions.json")
        if name in {"postgres", "redis", "seaweedfs"} and (
            build or service.get("image") != images[name]
        ):
            raise InvalidProject(f"{name}: используйте образ из versions.json")
        for mount in service.get("volumes", []):
            if mount.get("type") == "bind" and not contained(mount["source"], root):
                raise InvalidProject(f"{name}: bind mount должен находиться в hw2/")
        if name in SERVICES and service.get("profiles"):
            raise InvalidProject(f"{name}: обязательный сервис запускается без profiles")
        if name in SERVICES - {"airflow-init"}:
            if "default" not in service.get("networks", {}):
                raise InvalidProject(f"{name}: подключите сервис к сети default")
            if not service.get("healthcheck") or service["healthcheck"].get("disable"):
                raise InvalidProject(f"{name}: добавьте healthcheck готовности")
    for name in ("airflow-webserver", "airflow-scheduler", "spark-master", "spark-worker"):
        if not services[name].get("build"):
            raise InvalidProject(f"{name}: требуется собственный Dockerfile")
    for name in ("airflow-webserver", "airflow-scheduler"):
        depends = services[name].get("depends_on", {})
        if depends.get("airflow-init", {}).get("condition") != "service_completed_successfully":
            raise InvalidProject(f"{name}: дождитесь успешного airflow-init")


def command(argv: list[str], *, timeout: int, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(argv, timeout=timeout, check=False, **kwargs)


def cleanup(base: list[str], project: str) -> bool:
    """Remove only this invocation's observer and Compose resources."""
    success = True
    try:
        inspected = command(
            [
                "docker",
                "inspect",
                "--format",
                '{{ index .Config.Labels "org.mlsd.local-check" }}',
                project + "-observer",
            ],
            timeout=15,
            capture_output=True,
            text=True,
        )
        if inspected.returncode == 0:
            if inspected.stdout.strip() != project:
                success = False
            else:
                removed = command(
                    ["docker", "rm", "--force", project + "-observer"],
                    timeout=30,
                    capture_output=True,
                )
                success = removed.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        success = False
    try:
        removed = command(
            [*base, "down", "--volumes", "--remove-orphans"], timeout=120, capture_output=True
        )
        return success and removed.returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Проверить полный Compose-проект HW2")
    parser.add_argument("--project", type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    root = args.project.resolve()
    trusted_root = Path(__file__).resolve().parent
    compose = root / "compose.yaml"
    project = "mlsd_hw2_check_" + uuid4().hex[:12]
    base = ["docker", "compose", "--project-name", project, "--file", str(compose)]
    try:
        config_result = command(
            [*base, "config", "--format", "json"], timeout=30, capture_output=True, text=True
        )
        if config_result.returncode:
            raise InvalidProject(
                "Compose-конфигурация не разобрана; проверьте docker compose config"
            )
        config = json.loads(config_result.stdout)
        validate_config(config, root)
    except (InvalidProject, ValueError) as error:
        print("FAIL: " + str(error), file=sys.stderr)
        return 1
    except (OSError, subprocess.TimeoutExpired):
        print("SYSTEM_ERROR: Docker Compose недоступен", file=sys.stderr)
        return 2
    print("Проверка запускает отдельный проект " + project, flush=True)
    outcome = 2
    receipt = {}
    temp = trusted_root / "output" / project
    temp.mkdir(parents=True, mode=0o700)
    temp.chmod(0o700)
    try:
        with (temp / "compose.log").open("wb") as log:
            started = command(
                [*base, "up", "--build", "--wait", "--wait-timeout", "180"],
                timeout=1200,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        if started.returncode:
            raise InvalidProject(
                "проект не достиг готовности; проверьте healthcheck и логи Compose"
            )
        image_id_file = temp / "observer-image-id"
        with (temp / "observer-build.log").open("wb") as log:
            built = command(
                ["docker", "build", "--iidfile", str(image_id_file), str(trusted_root / "checks")],
                timeout=600,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        if built.returncode:
            raise RuntimeError("observer_build_failed")
        image_id = image_id_file.read_text().strip()
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
            raise RuntimeError("observer_image_id_invalid")
        init_env = config["services"]["airflow-init"].get("environment", {})
        s3_env = config["services"]["seaweedfs"].get("environment", {})
        values = {
            "AIRFLOW_API_USER": init_env.get("_AIRFLOW_WWW_USER_USERNAME", "airflow"),
            "AIRFLOW_API_PASSWORD": init_env.get("_AIRFLOW_WWW_USER_PASSWORD", "airflow"),
            "S3_ACCESS_KEY": s3_env.get("AWS_ACCESS_KEY_ID", "hw2-local"),
            "S3_SECRET_KEY": s3_env.get("AWS_SECRET_ACCESS_KEY", "hw2-local-only"),
        }
        env_file = temp / "observer.env"
        fd = os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            for key, value in values.items():
                if not isinstance(value, str) or "\n" in value or "\r" in value:
                    raise InvalidProject("Параметры доступа имеют неверный формат")
                stream.write(key + "=" + value + "\n")
        observed = command(
            [
                "docker",
                "run",
                "--rm",
                "--name",
                project + "-observer",
                "--label",
                "org.mlsd.local-check=" + project,
                "--network",
                project + "_default",
                "--read-only",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--memory=512m",
                "--pids-limit=64",
                "--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=64m",
                "--env-file",
                str(env_file),
                "--mount",
                "type=bind,src="
                + str(trusted_root / "data/sample.csv")
                + ",dst=/check/input.csv,readonly",
                image_id,
            ],
            timeout=1300,
            capture_output=True,
            text=True,
        )
        (temp / "observation.json").write_text(observed.stdout)
        try:
            receipt = json.loads(observed.stdout)
        except ValueError:
            raise RuntimeError("observer_result_invalid") from None
        if not isinstance(receipt, dict):
            raise RuntimeError("observer_result_invalid")
        details = receipt.get("details", {})
        if isinstance(details, dict) and isinstance(details.get("logs"), dict):
            for task_id, log in details["logs"].items():
                if task_id in {
                    "get_dataset",
                    "put_dataset",
                    "features_engineering",
                    "load_features",
                } and isinstance(log, str):
                    (temp / (task_id + ".log")).write_text(log)
        if observed.returncode == 0 and receipt.get("status") == "PASSED":
            outcome = 0
        else:
            diagnostic = receipt.get("diagnostic", "observer_failed")
            if not isinstance(diagnostic, str) or not re.fullmatch(r"[a-z_]+", diagnostic):
                diagnostic = "observer_failed"
            outcome = 1 if receipt.get("status") == "FAILED" else 2
            explanation = EXPLANATIONS.get(diagnostic, diagnostic)
            print(("FAIL: " if outcome == 1 else "SYSTEM_ERROR: ") + explanation)
            if diagnostic == "dag_failed" and isinstance(details, dict):
                states = details.get("tasks", {})
                if isinstance(states, dict):
                    failed = [
                        task_id
                        for task_id in (
                            "get_dataset",
                            "put_dataset",
                            "features_engineering",
                            "load_features",
                        )
                        if states.get(task_id) == "failed"
                    ]
                    if failed:
                        print("Задачи с ошибкой: " + ", ".join(failed))
    except (OSError, subprocess.TimeoutExpired):
        print("SYSTEM_ERROR: запуск проверки не завершён")
        outcome = 2
    except RuntimeError:
        print("SYSTEM_ERROR: проверяющий контейнер недоступен или не вернул результат")
        outcome = 2
    except InvalidProject as error:
        print("FAIL: " + str(error))
        outcome = 1
    finally:
        try:
            with (temp / "services.log").open("wb") as log:
                command(
                    [*base, "logs", "--no-color", "--tail", "300"],
                    timeout=30,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
        except (OSError, subprocess.TimeoutExpired):
            pass
        if not cleanup(base, project):
            print("SYSTEM_ERROR: очистка собственного проекта не подтверждена")
            outcome = 2
        # Secrets are transient; build/project logs remain local in a private directory.
        (temp / "observer.env").unlink(missing_ok=True)
    if outcome == 0:
        print(f"PASS: два DAG, два Spark application, S3/Redis, клиентов {receipt['customers']}")
    print("Логи проверки: " + str(temp))
    return outcome


if __name__ == "__main__":
    raise SystemExit(main())
