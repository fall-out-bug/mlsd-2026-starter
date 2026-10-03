"""Предоставленная инфраструктура: вход, Spark, Parquet и Redis."""
import hashlib
import json
import os
import time
from pathlib import Path

import boto3
import pyarrow.parquet as pq
import redis
from botocore.exceptions import ClientError, EndpointConnectionError
from pyspark.sql import SparkSession

ROOT = Path(__file__).parent
BUCKET = "hw2"
PREFIX = "hw2:customer:"


def storage():
    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"],
                        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
                        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
                        region_name="us-east-1")


def prepare():
    source = ROOT / "data/sample.csv"
    expected = (ROOT / "data/SHA256SUMS").read_text().split()[0]
    if hashlib.sha256(source.read_bytes()).hexdigest() != expected:
        raise ValueError("Изменён исходный срез: SHA256SUMS не совпадает")
    client = storage()
    for attempt in range(60):
        try:
            client.list_buckets()
            break
        except (ClientError, EndpointConnectionError):
            if attempt == 59:
                raise
            time.sleep(1)
    try:
        client.head_bucket(Bucket=BUCKET)
    except ClientError as error:
        if error.response["Error"]["Code"] not in {"404", "NoSuchBucket"}:
            raise
        client.create_bucket(Bucket=BUCKET)
    client.upload_file(str(source), BUCKET, "input/sample.csv")
    (ROOT / "output").mkdir(exist_ok=True)
    client.download_file(BUCKET, "input/sample.csv", str(ROOT / "output/input.csv"))


def build():
    from customer_features import customer_features
    spark = (SparkSession.builder.master("local[2]").appName("HW2")
             .config("spark.driver.memory", "512m")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        rows = spark.read.option("header", True).csv(str(ROOT / "output/input.csv"))
        features = customer_features(rows)
        features.coalesce(1).write.mode("overwrite").parquet(str(ROOT / "output/features.parquet"))
    finally:
        spark.stop()


def publish():
    directory = ROOT / "output/features.parquet"
    table = pq.ParquetDataset(directory).read()
    records = table.to_pylist()
    client = storage()
    # Удаляем только старый результат этой учебной работы.
    old = client.list_objects_v2(Bucket=BUCKET, Prefix="features/").get("Contents", [])
    if old:
        client.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": x["Key"]} for x in old]})
    for part in directory.glob("*.parquet"):
        client.upload_file(str(part), BUCKET, "features/" + part.name)
    store = redis.Redis(host=os.environ["REDIS_HOST"], decode_responses=True)
    old_keys = list(store.scan_iter(PREFIX + "*"))
    with store.pipeline(transaction=True) as transaction:
        if old_keys:
            transaction.delete(*old_keys)
        for row in records:
            value = {"order_count": row["order_count"],
                     "total_amount": str(row["total_amount"]),
                     "average_order_amount": str(row["average_order_amount"])}
            transaction.set(PREFIX + row["customer_id"], json.dumps(value, sort_keys=True))
        transaction.execute()
    print(f"Сохранено клиентских агрегатов: {len(records)}")
