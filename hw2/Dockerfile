FROM python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
COPY constraints.txt /opt/constraints.txt
RUN pip install --no-cache-dir --constraint /opt/constraints.txt apache-airflow==2.10.5 pyspark==3.5.4 pyarrow==19.0.0 boto3==1.36.3 redis==5.2.1
ARG STUDENT_UID=1000
ARG STUDENT_GID=1000
RUN groupadd --non-unique --gid "$STUDENT_GID" student && useradd --non-unique --uid "$STUDENT_UID" --gid student --create-home student
ENV AIRFLOW_HOME=/tmp/airflow AIRFLOW__CORE__LOAD_EXAMPLES=False AIRFLOW__CORE__EXECUTOR=SequentialExecutor AIRFLOW__CORE__DAGS_FOLDER=/workspace/dags PYTHONPATH=/workspace SPARK_LOCAL_IP=127.0.0.1
WORKDIR /workspace
