"""Реализуйте DAG online_retail_features.

get_dataset → put_dataset → features_engineering → load_features.
Обработка выполняется в Spark master/worker; данные хранятся в S3/Redis.
"""
