"""Course-owned HW3 artifact oracle.

This module deliberately reads only data formats (CSV, JSON and Parquet).  It
does not import a submission, execute a bundle or deserialize ``joblib``.  A
bundle reload is a separate probe that runs in the disposable student VM.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

CONTRACT = "hw3-temporal-v1"
FEATURES = ("order_count", "total_amount", "average_order_amount")
EXAMPLE_COLUMNS = ("customer_id", "as_of", *FEATURES, "label", "split")
PREDICTION_COLUMNS = (
    "experiment_id",
    "customer_id",
    "as_of",
    "split",
    "label",
    "probability",
    "in_slice",
)
EPSILON = 1e-12
TOLERANCE = 1e-9


class ContractFailure(ValueError):
    """A submission artifact violates the published HW3 contract."""


class ObservationUnavailable(RuntimeError):
    """The course-owned observation cannot establish a result."""


@dataclass(frozen=True)
class Example:
    customer_id: str
    as_of: str
    order_count: int
    total_amount: Decimal
    average_order_amount: Decimal
    label: int
    split: str


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _strict_json(path: Path) -> dict[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ContractFailure("json_duplicate_key")
            result[key] = value
        return result

    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
    except (OSError, UnicodeError, ValueError, RecursionError) as error:
        raise ContractFailure("json_invalid") from error
    if not isinstance(value, dict):
        raise ContractFailure("json_object_required")
    return value


def _date(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ContractFailure("timestamp_invalid")
    try:
        result = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
    except ValueError as error:
        raise ContractFailure("timestamp_invalid") from error
    if result.strftime("%Y-%m-%dT%H:%M:%S") != value:
        raise ContractFailure("timestamp_invalid")
    return result


def _decimal(value: Any) -> Decimal:
    try:
        result = Decimal("" if value is None else str(value).strip())
    except InvalidOperation as error:
        raise ContractFailure("decimal_invalid") from error
    if not result.is_finite():
        raise ContractFailure("decimal_invalid")
    return result


def _headers(names: list[str] | None) -> dict[str, str]:
    aliases = {
        "invoice_id": ("invoice_id", "Invoice", "invoice"),
        "customer_id": ("customer_id", "Customer ID", "customer"),
        "invoice_at": ("invoice_at", "InvoiceDate", "invoice_date"),
        "quantity": ("quantity", "Quantity"),
        "unit_price": ("unit_price", "Price", "price"),
    }
    available = {item.strip(): item for item in names or []}
    result = {
        key: next((available[x] for x in options if x in available), "")
        for key, options in aliases.items()
    }
    if not all(result.values()):
        raise ObservationUnavailable("trusted_input_schema_invalid")
    return result


def build_expected(request: dict[str, Any], input_path: Path) -> dict[str, list[Example]]:
    """Independent Decimal implementation of temporal population and labels."""
    if (
        request.get("contract_version") != CONTRACT
        or request.get("history_days") != 90
        or request.get("horizon_days") != 30
    ):
        raise ObservationUnavailable("trusted_request_invalid")
    if not input_path.is_file() or _sha(input_path) != request.get("input_sha256"):
        raise ObservationUnavailable("trusted_input_hash_invalid")
    events: list[tuple[str, str, datetime, int, Decimal]] = []
    try:
        with gzip.open(input_path, "rt", encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            names = _headers(reader.fieldnames)
            for row in reader:
                try:
                    customer = str(row.get(names["customer_id"], "")).strip()
                    invoice = str(row.get(names["invoice_id"], "")).strip()
                    when = _date(str(row.get(names["invoice_at"], "")).strip())
                    quantity = _decimal(row.get(names["quantity"]))
                    price = _decimal(row.get(names["unit_price"]))
                    if quantity != quantity.to_integral_value():
                        continue
                    if (
                        customer
                        and not invoice.upper().startswith("C")
                        and quantity > 0
                        and price > 0
                    ):
                        events.append((customer, invoice, when, int(quantity), price))
                except ContractFailure:
                    continue
    except (OSError, csv.Error, UnicodeError) as error:
        raise ObservationUnavailable("trusted_input_unavailable") from error
    observed_from, observed_until = (
        _date(request.get("observed_from")),
        _date(request.get("observed_until")),
    )
    result: dict[str, list[Example]] = {}
    snapshots = request.get("snapshots")
    if not isinstance(snapshots, list):
        raise ObservationUnavailable("trusted_request_invalid")
    for snapshot in snapshots:
        if not isinstance(snapshot, dict) or snapshot.get("split") not in {
            "train",
            "validation",
            "test",
        }:
            raise ObservationUnavailable("trusted_request_invalid")
        split, as_of = snapshot["split"], _date(snapshot.get("as_of"))
        start, end = as_of - timedelta(days=90), as_of + timedelta(days=30)
        if start < observed_from or end > observed_until:
            raise ObservationUnavailable("trusted_request_incomplete")
        invoices: dict[str, set[str]] = defaultdict(set)
        totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        future: set[str] = set()
        for customer, invoice, when, quantity, price in events:
            if start <= when < as_of:
                invoices[customer].add(invoice)
                totals[customer] += quantity * price
            elif as_of <= when < end:
                future.add(customer)
        rows: list[Example] = []
        for customer in sorted(invoices):
            count = len(invoices[customer])
            total = totals[customer].quantize(Decimal("0.000001"), rounding=ROUND_HALF_EVEN)
            rows.append(
                Example(
                    customer,
                    as_of.strftime("%Y-%m-%dT%H:%M:%S"),
                    count,
                    total,
                    (total / count).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN),
                    int(customer in future),
                    split,
                )
            )
        result[split] = rows
    if set(result) != {"train", "validation", "test"}:
        raise ObservationUnavailable("trusted_request_splits_invalid")
    return result


def _parquet(path: Path, columns: tuple[str, ...]) -> list[dict[str, Any]]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        files = sorted(path.rglob("*.parquet")) if path.is_dir() else [path]
        if not files:
            raise ContractFailure("parquet_missing")
        table = pq.read_table([str(item) for item in files])
        if tuple(table.column_names) != columns:
            raise ContractFailure("parquet_columns")
        wanted = {
            EXAMPLE_COLUMNS: (
                pa.string(),
                pa.string(),
                pa.int64(),
                pa.decimal128(20, 6),
                pa.decimal128(20, 2),
                pa.int64(),
                pa.string(),
            ),
            PREDICTION_COLUMNS: (
                pa.string(),
                pa.string(),
                pa.string(),
                pa.string(),
                pa.int64(),
                pa.float64(),
                pa.bool_(),
            ),
        }[columns]
        if tuple(table.schema.types) != wanted:
            raise ContractFailure("parquet_types")
        return table.to_pylist()
    except ContractFailure:
        raise
    except Exception as error:
        raise ContractFailure("parquet_invalid") from error


def _probability(value: Any) -> float:
    try:
        value = float(value)
    except (TypeError, ValueError) as error:
        raise ContractFailure("probability_invalid") from error
    if not math.isfinite(value) or not 0 <= value <= 1:
        raise ContractFailure("probability_invalid")
    return value


def _loss(labels: list[int], values: list[float]) -> float | None:
    if not labels:
        return None
    return sum(
        -(
            y * math.log(min(max(p, EPSILON), 1 - EPSILON))
            + (1 - y) * math.log(1 - min(max(p, EPSILON), 1 - EPSILON))
        )
        for y, p in zip(labels, values, strict=True)
    ) / len(labels)


def _auc(labels: list[int], values: list[float]) -> float | None:
    positive, negative = sum(labels), len(labels) - sum(labels)
    if not positive or not negative:
        return None
    ordered = sorted(zip(values, labels, strict=True))
    total, index = 0.0, 0
    while index < len(ordered):
        stop = index + 1
        while stop < len(ordered) and ordered[stop][0] == ordered[index][0]:
            stop += 1
        total += ((index + 1 + stop) / 2) * sum(label for _, label in ordered[index:stop])
        index = stop
    return (total - positive * (positive + 1) / 2) / (positive * negative)


def _metric(
    experiment_id: str, role: str, rows: list[dict[str, Any]], group: str
) -> dict[str, Any]:
    labels, probabilities = (
        [int(row["label"]) for row in rows],
        [_probability(row["probability"]) for row in rows],
    )
    empty = not rows
    return {
        "experiment_id": experiment_id,
        "role": role,
        "split": "validation" if role == "candidate" else "test",
        "group": group,
        "n": len(rows),
        "n_positive": sum(labels),
        "log_loss": _loss(labels, probabilities),
        "roc_auc": _auc(labels, probabilities),
        "log_loss_reason": "empty_group" if empty else None,
        "roc_auc_reason": "empty_group"
        if empty
        else (None if 0 < sum(labels) < len(labels) else "single_class"),
    }


def _same_metrics(payload: dict[str, Any], expected: list[dict[str, Any]]) -> bool:
    got = payload.get("records")
    if not isinstance(got, list):
        return False

    def key(row: dict[str, Any]) -> tuple[Any, Any, Any, Any]:
        return (
            row.get("experiment_id"),
            row.get("role"),
            row.get("split"),
            row.get("group"),
        )

    indexed = {key(row): row for row in got if isinstance(row, dict)}
    if len(indexed) != len(got) or set(indexed) != {key(row) for row in expected}:
        return False
    for wanted in expected:
        actual = indexed[key(wanted)]
        for name in ("n", "n_positive", "log_loss_reason", "roc_auc_reason"):
            if actual.get(name) != wanted[name]:
                return False
        for name in ("log_loss", "roc_auc"):
            if wanted[name] is None:
                if actual.get(name) is not None:
                    return False
            elif (
                not isinstance(actual.get(name), (float, int))
                or not math.isfinite(float(actual[name]))
                or abs(float(actual[name]) - wanted[name]) > TOLERANCE
            ):
                return False
    return True


def _examples_match(expected: dict[str, list[Example]], root: Path) -> bool:
    for split, wanted_rows in expected.items():
        actual = _parquet(root / "examples" / split, EXAMPLE_COLUMNS)
        actual_by_key = {(str(x["customer_id"]), str(x["as_of"])): x for x in actual}
        wanted = {(x.customer_id, x.as_of): x for x in wanted_rows}
        if len(actual_by_key) != len(actual) or set(actual_by_key) != set(wanted):
            return False
        for key, value in wanted.items():
            row = actual_by_key[key]
            if (
                row.get("split") != split
                or int(row.get("order_count", -1)) != value.order_count
                or Decimal(str(row.get("total_amount"))) != value.total_amount
                or Decimal(str(row.get("average_order_amount"))) != value.average_order_amount
                or int(row.get("label", -1)) != value.label
            ):
                return False
    return True


def _prediction_groups(
    rows: list[dict[str, Any]], examples: list[Example], allowed: set[str], split: str
) -> dict[str, list[dict[str, Any]]]:
    keys = {(item.customer_id, item.as_of): item for item in examples}
    grouped: dict[str, list[dict[str, Any]]] = {identifier: [] for identifier in allowed}
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        identity = (
            str(row.get("experiment_id")),
            str(row.get("customer_id")),
            str(row.get("as_of")),
        )
        item = keys.get(identity[1:])
        if (
            identity[0] not in allowed
            or item is None
            or identity in seen
            or row.get("split") != split
            or int(row.get("label", -1)) != item.label
            or bool(row.get("in_slice")) != (item.order_count == 1)
        ):
            raise ContractFailure("prediction_contract")
        _probability(row.get("probability"))
        seen.add(identity)
        grouped[identity[0]].append(row)
    if (
        any(
            {(str(x["customer_id"]), str(x["as_of"])) for x in rows} != set(keys)
            for rows in grouped.values()
        )
        or set(grouped) != allowed
    ):
        raise ContractFailure("prediction_coverage")
    return grouped


def _bundle_manifest(root: Path, selected: dict[str, Any]) -> bool:
    bundle = root / "selection" / "bundle"
    manifest, schema = _strict_json(bundle / "manifest.json"), _strict_json(bundle / "schema.json")
    files = ["pipeline.joblib", "requirements.lock", "schema.json"]
    entries = [
        {"path": name, "sha256": _sha(bundle / name)} for name in files if (bundle / name).is_file()
    ]
    canonical = sorted(entries, key=lambda item: item["path"])
    model_id = hashlib.sha256(
        json.dumps(canonical, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return (
        manifest.get("contract_version") == CONTRACT
        and manifest.get("model_id") == model_id == selected.get("model_id")
        and len(entries) == 3
        and manifest.get("files") == canonical
        and manifest.get("family") == selected.get("family")
        and schema.get("schema_version") == "hw3-features-v1"
        and schema.get("positive_class") == 1
        and [
            (x.get("name"), x.get("source_type"), x.get("model_type"))
            for x in schema.get("features", [])
            if isinstance(x, dict)
        ]
        == [
            ("order_count", "int64", "float64"),
            ("total_amount", "decimal(20,6)", "float64"),
            ("average_order_amount", "decimal(20,2)", "float64"),
        ]
    )


def verify_mlflow(
    root: Path,
    request: dict[str, Any],
    candidates: dict[str, dict[str, Any]],
    selected: dict[str, Any],
) -> bool:
    """Check remote MLflow byte identity.  It never loads a model artifact."""

    def same_scalar(actual: Any, expected: Any) -> bool:
        return (
            isinstance(actual, (int, float))
            and isinstance(expected, (int, float))
            and math.isfinite(float(actual))
            and math.isfinite(float(expected))
            and abs(float(actual) - float(expected)) <= TOLERANCE
        )

    try:
        from mlflow.tracking import MlflowClient

        result = _strict_json(root / "result.json")
        parent_id = result.get("parent_mlflow_run_id")
        client = MlflowClient()
        parent = client.get_run(parent_id)
        if parent.info.status != "FINISHED" or any(
            parent.data.tags.get(key) != request.get(key)
            for key in (
                "contract_version",
                "execution_id",
                "submission_sha",
                "dataset_version",
                "input_sha256",
            )
        ):
            return False
        experiment = client.get_experiment_by_name("hw3")
        if experiment is None:
            return False
        parents = client.search_runs(
            [experiment.experiment_id],
            filter_string=(
                "tags.run_kind = 'parent' and tags.execution_id = '"
                + str(request["execution_id"])
                + "' and tags.submission_sha = '"
                + str(request["submission_sha"])
                + "'"
            ),
        )
        if [run.info.run_id for run in parents] != [parent_id]:
            return False
        children = client.search_runs(
            [experiment.experiment_id],
            filter_string="tags.mlflow.parentRunId = '" + str(parent_id) + "'",
        )
        if len(children) != len(candidates) or {
            run.data.tags.get("experiment_id") for run in children
        } != set(candidates):
            return False
        parent_files = {
            "request.json": root / "input/request.json",
            "experiments.json": root / "selection/experiments.json",
            "selected.json": root / "selection/selected.json",
            "validation_metrics.json": root / "selection/validation_metrics.json",
            "test_metrics.json": root / "evaluation/test_metrics.json",
            "test_predictions.parquet": root / "evaluation/test_predictions.parquet",
        }
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            for name, local in parent_files.items():
                remote = Path(client.download_artifacts(parent_id, name, directory))
                if not remote.is_file() or not local.is_file() or _sha(remote) != _sha(local):
                    return False
        for identifier, candidate in candidates.items():
            run_id = candidate.get("mlflow_run_id")
            run = client.get_run(run_id)
            if (
                run.info.status != "FINISHED"
                or run.data.tags.get("experiment_id") != identifier
                or run.data.tags.get("execution_id") != request.get("execution_id")
                or run.data.tags.get("family") != candidate.get("family")
                or run.data.tags.get("seed") != "2026"
                or any(
                    run.data.tags.get(key) != request.get(key)
                    for key in (
                        "contract_version",
                        "submission_sha",
                        "dataset_version",
                        "input_sha256",
                    )
                )
                or run.data.params.get("candidate_json")
                != json.dumps(candidate.get("config"), sort_keys=True)
                or run.data.params.get("seed") != "2026"
            ):
                return False
            with tempfile.TemporaryDirectory() as directory:
                remote = Path(client.download_artifacts(run_id, "config.json", directory))
                if not remote.is_file() or _strict_json(remote) != candidate.get("config"):
                    return False
                metrics = Path(
                    client.download_artifacts(run_id, "validation_metrics.json", directory)
                )
                predictions = Path(
                    client.download_artifacts(run_id, "validation_predictions.parquet", directory)
                )
                local_rows = [
                    item
                    for item in _parquet(
                        root / "selection" / "validation_predictions.parquet",
                        PREDICTION_COLUMNS,
                    )
                    if item["experiment_id"] == identifier
                ]
                expected_metric = _metric(identifier, "candidate", local_rows, "all")
                if (
                    not metrics.is_file()
                    or not predictions.is_file()
                    or not _same_metrics({"records": [_strict_json(metrics)]}, [expected_metric])
                    or _parquet(predictions, PREDICTION_COLUMNS) != local_rows
                    or not same_scalar(
                        run.data.metrics.get("validation.all.log_loss"),
                        expected_metric["log_loss"],
                    )
                    or (
                        expected_metric["roc_auc"] is not None
                        and not same_scalar(
                            run.data.metrics.get("validation.all.roc_auc"),
                            expected_metric["roc_auc"],
                        )
                    )
                ):
                    return False
        winner = candidates.get(selected.get("experiment_id"))
        if not winner:
            return False
        with tempfile.TemporaryDirectory() as directory:
            for name in ("manifest.json", "pipeline.joblib", "requirements.lock", "schema.json"):
                remote = Path(
                    client.download_artifacts(winner["mlflow_run_id"], "bundle/" + name, directory)
                )
                local = root / "selection/bundle" / name
                if not remote.is_file() or not local.is_file() or _sha(remote) != _sha(local):
                    return False
        test_metrics = _strict_json(root / "evaluation" / "test_metrics.json").get("records")
        if not isinstance(test_metrics, list):
            return False
        for record in test_metrics:
            if not isinstance(record, dict):
                return False
            for metric_name in ("log_loss", "roc_auc"):
                value = record.get(metric_name)
                name = f"test.{record.get('role')}.{record.get('group')}.{metric_name}"
                if value is None:
                    if name in parent.data.metrics:
                        return False
                elif not same_scalar(parent.data.metrics.get(name), value):
                    return False
        return True
    except Exception:
        return False


def verify_s3(root: Path, request: dict[str, Any]) -> bool:
    """Verify ``current`` and the complete S3 manifest against downloaded bytes."""
    try:
        import boto3

        client = boto3.client(
            "s3", endpoint_url=__import__("os").environ.get("MLFLOW_S3_ENDPOINT_URL")
        )
        execution_id = request["execution_id"]
        result_key = f"runs/{execution_id}/result.json"
        current_bytes = client.get_object(Bucket="hw3", Key="current.json")["Body"].read()
        result_bytes = client.get_object(Bucket="hw3", Key=result_key)["Body"].read()
        current, result = json.loads(current_bytes), json.loads(result_bytes)
        if (
            current.get("execution_id") != execution_id
            or current.get("result_uri") != "s3://hw3/" + result_key
            or current.get("sha256") != hashlib.sha256(result_bytes).hexdigest()
            or result.get("status") != "ready"
        ):
            return False
        if not (root / "result.json").is_file() or _strict_json(root / "result.json") != result:
            return False
        declared = result.get("files")
        if not isinstance(declared, list):
            return False
        mapped = {
            item.get("path"): item.get("sha256")
            for item in declared
            if isinstance(item, dict)
            and isinstance(item.get("path"), str)
            and isinstance(item.get("sha256"), str)
        }
        if len(mapped) != len(declared) or any(
            Path(path).is_absolute() or ".." in Path(path).parts for path in mapped
        ):
            return False
        local = {
            str(path.relative_to(root)): _sha(path)
            for path in root.rglob("*")
            if path.is_file() and path.name != "result.json" and not path.name.startswith(".")
        }
        if mapped != local:
            return False
        for path, digest in mapped.items():
            remote = client.get_object(Bucket="hw3", Key=f"runs/{execution_id}/{path}")[
                "Body"
            ].read()
            if hashlib.sha256(remote).hexdigest() != digest:
                return False
        return True
    except Exception:
        return False


def verify_factory_report(root: Path, report_path: Path) -> bool:
    """Compare VM-only factory fit probabilities to published validation rows.

    ``report_path`` is course-collected output from :mod:`probes`, never an
    artifact selected by the student project.  No estimator object crosses this
    boundary.
    """
    try:
        report = _strict_json(report_path)
        if report.get("status") != "PASSED" or report.get("probe") != "factory":
            return False
        experiments = _strict_json(root / "selection/experiments.json")
        declared = {
            item.get("experiment_id"): item
            for item in experiments.get("candidates", [])
            if isinstance(item, dict) and isinstance(item.get("experiment_id"), str)
        }
        learnable = {
            identifier: item.get("family")
            for identifier, item in declared.items()
            if item.get("family") in {"logistic", "tree"}
        }
        observed = report.get("candidates")
        if not isinstance(observed, list) or {
            item.get("experiment_id") for item in observed if isinstance(item, dict)
        } != set(learnable):
            return False
        published = _parquet(root / "selection/validation_predictions.parquet", PREDICTION_COLUMNS)
        expected = {
            (str(row["experiment_id"]), str(row["customer_id"]), str(row["as_of"])): _probability(
                row["probability"]
            )
            for row in published
            if row.get("experiment_id") in learnable
        }
        actual: dict[tuple[str, str, str], float] = {}
        for candidate in observed:
            if (
                not isinstance(candidate, dict)
                or candidate.get("experiment_id") not in declared
                or candidate.get("family") != learnable.get(candidate.get("experiment_id"))
                or candidate.get("config") != declared[candidate["experiment_id"]].get("config")
                or candidate.get("resolved_params")
                != declared[candidate["experiment_id"]].get("resolved_params")
                or not isinstance(candidate.get("predictions"), list)
            ):
                return False
            for row in candidate["predictions"]:
                if not isinstance(row, dict):
                    return False
                key = (
                    str(candidate["experiment_id"]),
                    str(row.get("customer_id")),
                    str(row.get("as_of")),
                )
                if key in actual:
                    return False
                actual[key] = _probability(row.get("probability"))
        return set(actual) == set(expected) and all(
            abs(actual[key] - expected[key]) <= TOLERANCE for key in expected
        )
    except (ContractFailure, KeyError, OSError, TypeError, ValueError):
        return False


def check_artifacts(root: Path, request: dict[str, Any], *, online: bool = False) -> dict[str, Any]:
    """Validate a downloaded run directory; returns only aggregate, JSON-safe facts."""
    try:
        if not isinstance(request, dict) or request.get("contract_version") != CONTRACT:
            raise ObservationUnavailable("trusted_request_invalid")
        input_path = root / "input" / "input.csv.gz"
        if not input_path.is_file():
            candidates = list((root / "input").glob("*.csv.gz"))
            input_path = candidates[0] if len(candidates) == 1 else input_path
        expected = build_expected(request, input_path)
        checks = {"temporal_examples": _examples_match(expected, root)}
        experiments = _strict_json(root / "selection" / "experiments.json")
        candidates = {
            x.get("experiment_id"): x
            for x in experiments.get("candidates", [])
            if isinstance(x, dict) and isinstance(x.get("experiment_id"), str)
        }
        families = [x.get("family") for x in candidates.values()]
        checks["candidates"] = (
            len(candidates) == len(experiments.get("candidates", []))
            and set(families) <= {"constant", "logistic", "tree"}
            and candidates.get("constant", {}).get("family") == "constant"
            and families.count("constant") == 1
            and "logistic" in families
            and "tree" in families
            and all(
                isinstance(item.get("config"), dict)
                and isinstance(item.get("resolved_params"), dict)
                and item.get("status") == "FINISHED"
                and isinstance(item.get("mlflow_run_id"), str)
                for item in candidates.values()
            )
        )
        meta = (
            "contract_version",
            "execution_id",
            "submission_sha",
            "dataset_version",
            "input_sha256",
        )
        checks["run_identity"] = all(experiments.get(name) == request.get(name) for name in meta)
        constant_config = candidates.get("constant", {}).get("config")
        checks["constant_config"] = constant_config == {
            "experiment_id": "constant",
            "family": "constant",
            "preprocessing": [],
            "estimator_params": {},
        }
        val = _prediction_groups(
            _parquet(root / "selection" / "validation_predictions.parquet", PREDICTION_COLUMNS),
            expected["validation"],
            set(candidates),
            "validation",
        )
        validation_metrics_payload = _strict_json(root / "selection" / "validation_metrics.json")
        checks["validation_metrics"] = (
            validation_metrics_payload.get("contract_version") == CONTRACT
            and validation_metrics_payload.get("execution_id") == request.get("execution_id")
            and _same_metrics(
                validation_metrics_payload,
                [_metric(identifier, "candidate", rows, "all") for identifier, rows in val.items()],
            )
        )
        scores = {
            identifier: _loss(
                [int(x["label"]) for x in rows], [_probability(x["probability"]) for x in rows]
            )
            for identifier, rows in val.items()
        }
        rank = {"constant": 0, "logistic": 1, "tree": 2}
        best = min(scores.values())
        chosen = min(
            (key for key, value in scores.items() if value <= best + EPSILON),
            key=lambda key: (rank.get(candidates[key].get("family"), 99), key),
        )
        selected = _strict_json(root / "selection" / "selected.json")
        selected_id = selected.get("experiment_id")
        checks["selected_identity"] = (
            all(selected.get(name) == request.get(name) for name in meta)
            and selected_id in candidates
            and selected.get("family") == candidates[selected_id].get("family")
            and selected.get("mlflow_run_id") == candidates[selected_id].get("mlflow_run_id")
            and isinstance(selected.get("model_id"), str)
            and isinstance(selected.get("p_train"), (float, int))
            and math.isfinite(float(selected["p_train"]))
            and 0 <= float(selected["p_train"]) <= 1
        )
        checks["selection"] = (
            selected_id == chosen
            and selected.get("selection_rule") == "validation_log_loss_v1"
            and isinstance(selected.get("validation_log_loss"), (float, int))
            and math.isfinite(float(selected["validation_log_loss"]))
            and scores[chosen] is not None
            and abs(float(selected["validation_log_loss"]) - float(scores[chosen])) <= TOLERANCE
        )
        constant = next(
            key for key, value in candidates.items() if value.get("family") == "constant"
        )
        reported_selected = selected.get("experiment_id")
        required = {
            identifier for identifier in (reported_selected, constant) if identifier in candidates
        }
        if not required or reported_selected not in candidates:
            raise ContractFailure("selected_candidate_invalid")
        test = _prediction_groups(
            _parquet(root / "evaluation" / "test_predictions.parquet", PREDICTION_COLUMNS),
            expected["test"],
            required,
            "test",
        )
        metrics = []
        for identifier, role in ((reported_selected, "selected"), (constant, "baseline")):
            metrics += [
                _metric(identifier, role, test[identifier], "all"),
                _metric(
                    identifier,
                    role,
                    [x for x in test[identifier] if bool(x["in_slice"])],
                    "order_count_eq_1",
                ),
            ]
        test_metrics_payload = _strict_json(root / "evaluation" / "test_metrics.json")
        checks["test_metrics"] = (
            test_metrics_payload.get("contract_version") == CONTRACT
            and test_metrics_payload.get("execution_id") == request.get("execution_id")
            and _same_metrics(test_metrics_payload, metrics)
        )
        p_train = sum(item.label for item in expected["train"]) / len(expected["train"])
        constant_validation = val.get("constant", [])
        constant_test = test.get("constant", [])
        checks["constant_probability"] = (
            abs(float(selected.get("p_train", float("nan"))) - p_train) <= TOLERANCE
            and all(
                abs(_probability(row["probability"]) - p_train) <= TOLERANCE
                for row in constant_validation
            )
            and all(
                abs(_probability(row["probability"]) - p_train) <= TOLERANCE
                for row in constant_test
            )
        )
        checks["bundle_manifest"] = _bundle_manifest(root, selected)
        if online:
            checks["mlflow"] = verify_mlflow(root, request, candidates, selected)
            checks["s3"] = verify_s3(root, request)
        population = {
            split: {"n": len(rows), "n_positive": sum(row.label for row in rows)}
            for split, rows in expected.items()
        }
        failed = sorted(name for name, passed in checks.items() if not passed)
        return {
            "schema_version": "hw3-check-observation-v1",
            "status": "PASSED" if not failed else "FAILED",
            "diagnostic": "artifact_contract_mismatch" if failed else None,
            "checks": checks,
            "population": population,
        }
    except ObservationUnavailable as error:
        return {
            "schema_version": "hw3-check-observation-v1",
            "status": "SYSTEM_ERROR",
            "diagnostic": str(error),
            "checks": {},
        }
    except (ContractFailure, InvalidOperation, KeyError, TypeError, ValueError, OSError) as error:
        return {
            "schema_version": "hw3-check-observation-v1",
            "status": "FAILED",
            "diagnostic": str(error) or type(error).__name__,
            "checks": {},
        }
