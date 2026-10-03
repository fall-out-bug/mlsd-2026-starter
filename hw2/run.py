import subprocess
import sys

subprocess.run(["airflow", "db", "migrate"], check=True)
subprocess.run(["airflow", "dags", "test", "hw2_customer_features",
                "2026-01-01", "--subdir", "/workspace/dags/hw2.py"], check=True)
# dags test может завершиться с кодом 0 при failed task: проверяем DagRun.
from airflow.models import DagRun
from airflow.utils.session import create_session
with create_session() as session:
    run = session.query(DagRun).filter(DagRun.dag_id == "hw2_customer_features").one()
    if run.state != "success":
        sys.exit("FAIL: Airflow DAG не завершился успешно")
