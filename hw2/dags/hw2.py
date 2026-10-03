from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline import prepare, build, publish

with DAG("hw2_customer_features", start_date=datetime(2026, 1, 1),
         schedule=None, catchup=False) as dag:
    source = PythonOperator(task_id="prepare_input", python_callable=prepare)
    features = PythonOperator(task_id="build_features", python_callable=build)
    result = PythonOperator(task_id="publish_features", python_callable=publish)
    source >> features >> result
