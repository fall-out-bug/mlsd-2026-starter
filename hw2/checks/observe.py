"""Course-owned observation of a running HW2 Compose project; no student imports."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import os
import tempfile
import time
import urllib.error
import urllib.request
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from pathlib import Path
from uuid import uuid4

DAG_ID = "online_retail_features"
TASK_IDS = ("get_dataset", "put_dataset", "features_engineering", "load_features")
COLUMNS = ("customer_id", "order_count", "total_amount", "average_order_amount")
MAX_HTTP_BYTES = 2 * 1024 * 1024
MAX_PARQUET_BYTES = 32 * 1024 * 1024


class ProjectFailure(ValueError):
    """A confirmed violation of the project contract."""

    def __init__(self, diagnostic: str, details: dict | None = None):
        super().__init__(diagnostic)
        self.details = details or {}


class ObservationUnavailable(RuntimeError):
    """The observer could not establish a result."""


def expected_features(raw: bytes) -> dict[str, dict[str, object]]:
    amounts: dict[str, Decimal] = {}
    orders: dict[str, set[str]] = {}
    with localcontext() as ctx:
        ctx.prec = 50
        for row in csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))):
            customer = row["customer_id"]
            quantity, price = int(row["quantity"]), Decimal(row["unit_price"])
            if not price.is_finite():
                raise ObservationUnavailable("trusted_input_invalid")
            if not customer or row["invoice_id"].casefold().startswith("c"):
                continue
            if quantity <= 0 or price <= 0:
                continue
            amounts[customer] = amounts.get(customer, Decimal(0)) + quantity * price
            orders.setdefault(customer, set()).add(row["invoice_id"])
        return {
            customer: {
                "order_count": len(orders[customer]),
                "total_amount": format(amount.quantize(Decimal("0.000001")), "f"),
                "average_order_amount": format(
                    (amount / len(orders[customer])).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_EVEN
                    ),
                    "f",
                ),
            }
            for customer, amount in amounts.items()
        }


def strict_json(raw: bytes | str) -> object:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def constant(_):
        raise ValueError("non-finite number")

    return json.loads(raw, object_pairs_hook=unique, parse_constant=constant)


def http_json(
    url: str, *, method: str = "GET", payload: object = None, airflow: bool = False
) -> dict:
    headers = {"Content-Type": "application/json"}
    if airflow:
        credentials = (
            os.getenv("AIRFLOW_API_USER", "airflow")
            + ":"
            + os.getenv("AIRFLOW_API_PASSWORD", "airflow")
        )
        headers["Authorization"] = "Basic " + base64.b64encode(credentials.encode()).decode()
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read(MAX_HTTP_BYTES + 1)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            raise ProjectFailure("dag_or_api_not_found") from None
        raise ObservationUnavailable("api_request_failed") from None
    except (OSError, urllib.error.URLError):
        raise ObservationUnavailable("api_unavailable") from None
    if len(raw) > MAX_HTTP_BYTES:
        raise ProjectFailure("api_response_too_large")
    try:
        value = strict_json(raw)
    except (ValueError, UnicodeError, RecursionError):
        raise ProjectFailure("api_response_invalid") from None
    if not isinstance(value, dict):
        raise ProjectFailure("api_response_invalid")
    return value


def airflow_api(path: str, **kwargs) -> dict:
    return http_json("http://airflow-webserver:8080/api/v1" + path, airflow=True, **kwargs)


def check_dependencies(tasks: list[dict]) -> None:
    if not isinstance(tasks, list) or any(
        not isinstance(task, dict)
        or not isinstance(task.get("task_id"), str)
        or not isinstance(task.get("downstream_task_ids"), list)
        or any(not isinstance(value, str) for value in task["downstream_task_ids"])
        for task in tasks
    ):
        raise ProjectFailure("dag_tasks_invalid")
    graph = {task["task_id"]: task["downstream_task_ids"] for task in tasks}
    if len(graph) != len(tasks):
        raise ProjectFailure("dag_tasks_invalid")
    if not set(TASK_IDS).issubset(graph):
        raise ProjectFailure("required_tasks_missing")
    for start, target in zip(TASK_IDS, TASK_IDS[1:], strict=False):
        pending, seen = list(graph[start]), set()
        while pending:
            node = pending.pop()
            if node in seen:
                continue
            seen.add(node)
            pending.extend(graph.get(node, []))
        if target not in seen:
            raise ProjectFailure("required_task_dependency_missing")


def spark_apps() -> dict[str, dict]:
    value = http_json("http://spark-master:8080/json/")
    if not any(worker.get("state") == "ALIVE" for worker in value.get("workers", [])):
        raise ProjectFailure("spark_worker_not_registered")
    return {app["id"]: app for app in value.get("completedapps", [])}


def run_dag(timeout: float = 600) -> dict:
    run_id = "course_check_" + uuid4().hex
    before = spark_apps()
    airflow_api(f"/dags/{DAG_ID}", method="PATCH", payload={"is_paused": False})
    airflow_api(f"/dags/{DAG_ID}/dagRuns", method="POST", payload={"dag_run_id": run_id})
    deadline = time.monotonic() + timeout
    while True:
        run = airflow_api(f"/dags/{DAG_ID}/dagRuns/{run_id}")
        if run.get("state") in {"success", "failed"}:
            break
        if time.monotonic() >= deadline:
            raise ProjectFailure("dag_deadline_exceeded")
        time.sleep(2)
    if run["state"] != "success":
        tasks = airflow_api(f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances").get(
            "task_instances", []
        )
        logs = {}
        states = {}
        for task in tasks:
            task_id = task.get("task_id")
            if task_id not in TASK_IDS:
                continue
            states[task_id] = task.get("state")
            if task.get("state") == "failed":
                attempt = task.get("try_number", 1)
                try:
                    response = airflow_api(
                        f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{attempt}"
                    )
                    content = response.get("content", "")
                    if isinstance(content, list):
                        content = "\n".join(
                            str(item[1])
                            for item in content
                            if isinstance(item, (list, tuple)) and len(item) == 2
                        )
                    logs[task_id] = str(content)[-120000:]
                except (ProjectFailure, ObservationUnavailable):
                    logs[task_id] = "task_log_unavailable"
        raise ProjectFailure("dag_failed", {"dag_run_id": run_id, "tasks": states, "logs": logs})
    tasks = airflow_api(f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")["task_instances"]
    for task_id in TASK_IDS:
        matches = [task for task in tasks if task.get("task_id") == task_id]
        if not matches or any(task.get("state") != "success" for task in matches):
            raise ProjectFailure("required_task_not_successful")
    deadline = time.monotonic() + 30
    while True:
        after = spark_apps()
        fresh = [
            app
            for key, app in after.items()
            if key not in before and app.get("name") == DAG_ID and app.get("state") == "FINISHED"
        ]
        if fresh:
            return {
                "dag_run_id": run_id,
                "spark_application_id": fresh[0]["id"],
                "tasks": {task_id: "success" for task_id in TASK_IDS},
            }
        if time.monotonic() >= deadline:
            raise ProjectFailure("spark_application_not_observed")
        time.sleep(1)


def require_equal(actual: dict, expected: dict, location: str) -> None:
    if actual != expected:
        raise ProjectFailure(location + "_mismatch")


def parquet_features(directory: Path) -> dict[str, dict[str, object]]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    try:
        table = pq.ParquetDataset(directory).read()
    except (pa.ArrowInvalid, pa.ArrowTypeError, OSError):
        raise ProjectFailure("parquet_invalid") from None
    types = (pa.string(), pa.int64(), pa.decimal128(20, 6), pa.decimal128(20, 2))
    if tuple(table.column_names) != COLUMNS or any(
        table.schema.field(name).type != kind for name, kind in zip(COLUMNS, types, strict=True)
    ):
        raise ProjectFailure("parquet_schema_invalid")
    result = {}
    for row in table.to_pylist():
        customer = row.pop("customer_id")
        if not customer or customer in result or any(value is None for value in row.values()):
            raise ProjectFailure("parquet_customer_invalid")
        result[customer] = {
            "order_count": row["order_count"],
            "total_amount": str(row["total_amount"]),
            "average_order_amount": str(row["average_order_amount"]),
        }
    return result


def storage_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url="http://seaweedfs:8333",
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "hw2-local"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "hw2-local-only"),
        region_name="us-east-1",
        config=Config(connect_timeout=5, read_timeout=10, retries={"total_max_attempts": 1}),
    )


def seed_stale_outputs() -> None:
    """The second DAG must replace output, including data absent from this input."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import redis

    schema = pa.schema(
        [
            ("customer_id", pa.string()),
            ("order_count", pa.int64()),
            ("total_amount", pa.decimal128(20, 6)),
            ("average_order_amount", pa.decimal128(20, 2)),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {
                "customer_id": "__course_check_stale__",
                "order_count": 1,
                "total_amount": Decimal("1.000000"),
                "average_order_amount": Decimal("1.00"),
            }
        ],
        schema=schema,
    )
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    try:
        storage_client().put_object(
            Bucket="hw2", Key="features/course-check-stale.parquet", Body=buffer.getvalue()
        )
        redis.Redis(host="redis", socket_connect_timeout=5, socket_timeout=5).set(
            "hw2:customer:__course_check_stale__",
            '{"order_count":1,"total_amount":"1.000000","average_order_amount":"1.00"}',
        )
    except Exception:
        raise ObservationUnavailable("replay_setup_unavailable") from None


def inspect_outputs(raw: bytes, expected: dict) -> dict:
    import redis
    from botocore.exceptions import ClientError

    client = storage_client()
    try:
        source = client.get_object(Bucket="hw2", Key="input/sample.csv")["Body"].read(len(raw) + 1)
        if source != raw:
            raise ProjectFailure("s3_input_mismatch")
        pages = client.get_paginator("list_objects_v2").paginate(
            Bucket="hw2", Prefix="features/", PaginationConfig={"PageSize": 128}
        )
        objects = (
            item
            for page in pages
            for item in page.get("Contents", [])
            if item["Key"].endswith(".parquet")
        )
        with tempfile.TemporaryDirectory() as temp:
            total_size = 0
            found = False
            for index, item in enumerate(objects):
                found = True
                total_size += item["Size"]
                if total_size > MAX_PARQUET_BYTES:
                    raise ProjectFailure("parquet_total_size_exceeded")
                if item["Size"] > MAX_PARQUET_BYTES:
                    raise ProjectFailure("parquet_file_too_large")
                data = client.get_object(Bucket="hw2", Key=item["Key"])["Body"].read(
                    MAX_PARQUET_BYTES + 1
                )
                if len(data) > MAX_PARQUET_BYTES:
                    raise ProjectFailure("parquet_file_too_large")
                (Path(temp) / f"part-{index}.parquet").write_bytes(data)
            if not found:
                raise ProjectFailure("parquet_missing")
            actual_parquet = parquet_features(Path(temp))
        store = redis.Redis(
            host="redis", decode_responses=True, socket_connect_timeout=5, socket_timeout=5
        )
        actual_redis = {}
        for key in store.scan_iter("hw2:customer:*"):
            customer = key[len("hw2:customer:") :]
            value = store.get(key)
            if value is None or len(value) > 4096:
                raise ProjectFailure("redis_value_invalid")
            try:
                record = strict_json(value)
            except (ValueError, UnicodeError, RecursionError):
                raise ProjectFailure("redis_json_invalid") from None
            if not isinstance(record, dict):
                raise ProjectFailure("redis_json_invalid")
            if "customer_id" in record and record.pop("customer_id") != customer:
                raise ProjectFailure("redis_customer_mismatch")
            if type(record.get("order_count")) is not int:
                raise ProjectFailure("redis_order_count_invalid")
            actual_redis[customer] = record
            if len(actual_redis) > len(expected):
                raise ProjectFailure("redis_extra_customers")
    except ProjectFailure:
        raise
    except (redis.exceptions.ResponseError, UnicodeError):
        raise ProjectFailure("redis_value_invalid") from None
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code in {"NoSuchBucket", "NoSuchKey", "404"}:
            raise ProjectFailure("s3_output_missing") from None
        raise ObservationUnavailable("storage_observation_unavailable") from None
    except Exception:
        raise ObservationUnavailable("storage_observation_unavailable") from None
    require_equal(actual_parquet, expected, "s3_parquet")
    require_equal(actual_redis, expected, "redis")
    return {
        "s3_parquet": actual_parquet,
        "redis": actual_redis,
        "input_sha256": hashlib.sha256(source).hexdigest(),
    }


def main() -> int:
    raw = Path("/check/input.csv").read_bytes()
    expected = expected_features(raw)
    check_dependencies(airflow_api(f"/dags/{DAG_ID}/tasks")["tasks"])
    runs = []
    for number in range(2):
        if number == 1:
            seed_stale_outputs()
        execution = run_dag()
        execution.update(inspect_outputs(raw, expected))
        runs.append(execution)
    if runs[0]["spark_application_id"] == runs[1]["spark_application_id"]:
        raise ProjectFailure("spark_application_reused")
    require_equal(runs[0]["s3_parquet"], runs[1]["s3_parquet"], "replay")
    print(
        json.dumps(
            {
                "schema_version": "hw2-project-v2",
                "status": "PASSED",
                "customers": len(expected),
                "runs": runs,
            },
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProjectFailure as error:
        print(json.dumps({"status": "FAILED", "diagnostic": str(error), "details": error.details}))
        raise SystemExit(1) from None
    except ObservationUnavailable as error:
        print(json.dumps({"status": "SYSTEM_ERROR", "diagnostic": str(error)}))
        raise SystemExit(2) from None
    except Exception:
        print('{"status":"SYSTEM_ERROR","diagnostic":"observer_internal_error"}')
        raise SystemExit(2) from None
