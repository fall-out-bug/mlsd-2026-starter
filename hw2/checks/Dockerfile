FROM python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3
COPY requirements.txt /check/requirements.txt
RUN pip install --no-cache-dir -r /check/requirements.txt
COPY observe.py /check/observe.py
USER 65532:65532
ENV HOME=/tmp PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["python", "/check/observe.py"]
