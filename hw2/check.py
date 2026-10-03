"""Локальная проверка результата HW2; не серверная приёмка."""
import csv
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from decimal import Decimal, ROUND_HALF_EVEN
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import redis
from pipeline import ROOT, BUCKET, PREFIX, storage

COLUMNS = ["customer_id", "order_count", "total_amount", "average_order_amount"]


def expected_features(path):
    amounts, orders = {}, {}
    with Path(path).open(newline="") as source:
        for row in csv.DictReader(source):
            customer = row["customer_id"]
            quantity, price = int(row["quantity"]), Decimal(row["unit_price"])
            if not customer or row["invoice_id"].upper().startswith("C") or quantity <= 0 or price <= 0:
                continue
            amounts[customer] = amounts.get(customer, Decimal(0)) + quantity * price
            orders.setdefault(customer, set()).add(row["invoice_id"])
    return {key: {"order_count": len(orders[key]),
                  "total_amount": str(amount.quantize(Decimal("0.000001"))),
                  "average_order_amount": str((amount / len(orders[key])).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN))}
            for key, amount in amounts.items()}


def parquet_features(directory):
    table = pq.ParquetDataset(directory).read()
    expected_schema = pa.schema([("customer_id", pa.string()), ("order_count", pa.int64()),
                                ("total_amount", pa.decimal128(20, 6)),
                                ("average_order_amount", pa.decimal128(20, 2))])
    if table.column_names != COLUMNS or any(
            table.schema.field(name).type != expected_schema.field(name).type for name in COLUMNS):
        raise ValueError("Parquet: неверные поля или типы; проверьте четыре поля из условия")
    result = {}
    for row in table.to_pylist():
        key = row["customer_id"]
        if not key or key in result or any(row[name] is None for name in COLUMNS):
            raise ValueError("Parquet: пустой ключ, повтор клиента или NULL в агрегатах")
        result[key] = {"order_count": row["order_count"], "total_amount": str(row["total_amount"]),
                       "average_order_amount": str(row["average_order_amount"])}
    return result


def require_equal(actual, expected, location):
    if actual != expected:
        missing = len(set(expected) - set(actual))
        extra = len(set(actual) - set(expected))
        changed = sum(actual[key] != expected[key] for key in set(actual) & set(expected))
        raise ValueError(f"{location}: отсутствуют {missing}, лишние {extra}, неверные агрегаты {changed}")


def inspect_outputs(expected):
    local = parquet_features(ROOT / "output/features.parquet")
    require_equal(local, expected, "Локальный Parquet")
    client = storage()
    objects = client.list_objects_v2(Bucket=BUCKET, Prefix="features/").get("Contents", [])
    if not objects or any(not item["Key"].endswith(".parquet") for item in objects):
        raise ValueError("MinIO: ожидаются файлы Parquet в features/")
    with tempfile.TemporaryDirectory() as temporary:
        for index, item in enumerate(objects):
            client.download_file(BUCKET, item["Key"], str(Path(temporary) / f"part-{index}.parquet"))
        require_equal(parquet_features(temporary), expected, "Parquet в MinIO")
    source = client.get_object(Bucket=BUCKET, Key="input/sample.csv")["Body"].read()
    if hashlib.sha256(source).hexdigest() != hashlib.sha256((ROOT / "data/sample.csv").read_bytes()).hexdigest():
        raise ValueError("MinIO: входной файл отличается от среза")
    store = redis.Redis(host=os.environ["REDIS_HOST"], decode_responses=True)
    actual = {key[len(PREFIX):]: json.loads(store.get(key)) for key in store.scan_iter(PREFIX + "*")}
    require_equal(actual, expected, "Redis")
    return local


def main():
    (ROOT / "output/check-result.json").unlink(missing_ok=True)
    expected = expected_features(ROOT / "data/sample.csv")
    previous = None
    for attempt in range(2):
        subprocess.run([sys.executable, "run.py"], cwd=ROOT, check=True)
        actual = inspect_outputs(expected)
        if previous is not None:
            require_equal(actual, previous, "Повторный запуск")
        previous = actual
        print(f"PASS: запуск {attempt + 1}, Parquet и Redis, клиентов {len(actual)}", flush=True)
    receipt = {"status": "PASS", "runs": 2, "customers": len(expected),
               "input_sha256": hashlib.sha256((ROOT / "data/sample.csv").read_bytes()).hexdigest(),
               "parquet_minio_redis_verified": True}
    (ROOT / "output/check-result.json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, subprocess.CalledProcessError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
