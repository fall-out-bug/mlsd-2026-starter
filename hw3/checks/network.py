"""Course-owned API observer, also used with explicit toy expectations locally."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path, PurePosixPath

import boto3
import requests

OUT = Path("/out")
INPUT = Path("/input")
TASKS = ["stage_input", "build_examples", "train_select", "evaluate", "publish"]
LIMIT = 256 * 1024 * 1024


class Failed(Exception):
    pass


def read_json(path):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise Failed("duplicate_json_key")
            result[key] = value
        return result

    try:
        return json.loads(
            path.read_text(),
            object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(Failed("nonfinite_json")),
        )
    except (OSError, ValueError, UnicodeError) as exc:
        raise Failed("artifact_json_invalid:" + path.name) from exc


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def ensure(condition, code):
    if not condition:
        raise Failed(code)


class Observer:
    def __init__(self, args):
        self.args = args
        self.deadline = time.monotonic() + args.timeout
        self.session = requests.Session()
        self.session.auth = ("hw3", "local-hw3-only")
        self.s3 = boto3.client("s3", endpoint_url="http://seaweedfs:8333")
        self.result = {"status": "SYSTEM_ERROR", "checks": {}}

    def remaining(self):
        if time.monotonic() >= self.deadline:
            raise TimeoutError("observer_deadline")

    def _task_provenance(self, route, event):
        """Record exact run/task/try facts before resources change.

        Uses the pinned Airflow API boundary only. The snapshot is taken
        before ``clearTaskInstances`` replaces task attempts and when a DAG
        run fails, so cleared or failed attempts stay provable from the
        observer report even after retry renumbers them.
        """
        entry = {"execution_id": route.rsplit("/", 1)[-1], "event": event}
        try:
            instances = self.api(route + "/taskInstances")["task_instances"]
            entry["tasks"] = [
                {
                    "task_id": item.get("task_id"),
                    "try_number": item.get("try_number"),
                    "state": item.get("state"),
                }
                for item in instances
            ]
        except (requests.RequestException, KeyError, TypeError) as exc:
            entry["unavailable"] = type(exc).__name__
        self.result.setdefault("task_attempt_provenance", []).append(entry)

    def api(self, route, method="GET", payload=None):
        self.remaining()
        response = self.session.request(
            method, "http://airflow-webserver:8080/api/v1/" + route, json=payload, timeout=10
        )
        response.raise_for_status()
        return response.json()

    def _capture_preclear_logs(self, route):
        """Raw task-attempt bytes of this generation before clearTaskInstances.

        ``_task_provenance`` records task/try facts, not bytes: the exact
        attempt output is fetched through the pinned Airflow log endpoint
        and staged to /out before the destructive clear, bounded by the
        course log budget share passed by the controller. A failed fetch of
        one attempt is an explicit loss fact with task/try provenance, and
        the byte counters are the actual raw durable bytes (F4/F5).
        """
        name = "airflow-preclear.run.log"
        cap = getattr(self.args, "log_bytes_limit", None)
        if isinstance(cap, bool) or not isinstance(cap, int) or cap <= 0:
            return {"lost": "preclear_budget_unavailable"}
        try:
            instances = self.api(route + "/taskInstances")["task_instances"]
        except (requests.RequestException, KeyError, TypeError):
            return {"lost": "preclear_instances_unavailable"}
        written = 0
        complete = True
        stop_reason = None
        lost_attempts = []
        try:
            with (OUT / name).open("wb") as sink:
                for item in instances:
                    self.remaining()
                    task = item.get("task_id")
                    try_number = item.get("try_number")
                    if not isinstance(task, str) or not isinstance(try_number, int):
                        continue
                    header = f"===== {task} try={try_number} =====\n".encode()
                    if written + len(header) >= cap:
                        complete = False
                        stop_reason = "aggregate_log_limit"
                        break
                    sink.write(header)
                    written += len(header)
                    attempt_lost = None
                    try:
                        response = self.session.get(
                            "http://airflow-webserver:8080/api/v1/"
                            + route
                            + f"/taskInstances/{task}/logs/{try_number}",
                            stream=True,
                            timeout=10,
                        )
                        response.raise_for_status()
                        # Raw bytes are read bounded by the shared budget
                        # as they arrive: the body is never fully loaded
                        # before truncation, and the reported counter is
                        # the actual durable byte count, not characters
                        # (F5).
                        for block in response.iter_content(chunk_size=4096):
                            self.remaining()
                            if not block:
                                continue
                            room = cap - written
                            if room <= 0:
                                complete = False
                                stop_reason = "aggregate_log_limit"
                                break
                            piece = block if len(block) <= room else block[:room]
                            sink.write(piece)
                            written += len(piece)
                            if len(block) > room:
                                complete = False
                                stop_reason = "aggregate_log_limit"
                                break
                    except requests.RequestException:
                        # A failed fetch of this attempt is an explicit
                        # loss with task/try provenance; the already
                        # captured prefix stays, the replacement text is
                        # never presented as the complete raw output (F4).
                        attempt_lost = "fetch_failed"
                    if attempt_lost is not None:
                        complete = False
                        lost_attempts.append(
                            {"task": task, "try_number": try_number, "lost": attempt_lost}
                        )
        except OSError:
            return {"lost": "preclear_capture_unavailable"}
        fact = {
            "file": name,
            "bytes": written,
            "complete": complete,
            "stop_reason": stop_reason,
        }
        if lost_attempts:
            fact["lost_attempts"] = lost_attempts
        return fact

    def spark(self):
        response = requests.get("http://spark-master:8080/json/", timeout=10)
        response.raise_for_status()
        value = response.json()
        ensure(any(w["state"] == "ALIVE" for w in value.get("workers", [])), "spark_worker_missing")
        return {x["id"]: x for x in value.get("completedapps", [])}

    def ready(self):
        last = "services_not_ready"
        for _ in range(120):
            self.remaining()
            try:
                dag = self.api("dags/online_retail_training/details")
                self.s3.list_buckets()
                response = requests.get("http://mlflow:5000/health", timeout=3)
                response.raise_for_status()
                self.spark()
                ensure(dag["max_active_runs"] == 1 and not dag["catchup"], "dag_configuration")
                ensure(dag.get("schedule_interval") is None, "dag_schedule")
                errors = self.api("importErrors")
                ensure(errors["total_entries"] == 0, "dag_import_error")
                self.api("dags/online_retail_training", "PATCH", {"is_paused": False})
                tasks = self.api("dags/online_retail_training/tasks")["tasks"]
                by_id = {t["task_id"]: t for t in tasks}
                ensure(set(TASKS) <= by_id.keys(), "dag_tasks_missing")

                def reaches(start, end, seen=None, by_id=by_id):
                    seen = set() if seen is None else seen
                    if start in seen:
                        return False
                    seen.add(start)
                    return start == end or any(
                        reaches(x, end, seen) for x in by_id[start].get("downstream_task_ids", [])
                    )

                ensure(
                    all(reaches(a, b) for a, b in zip(TASKS, TASKS[1:], strict=False)),
                    "dag_dependencies",
                )
                return
            except (requests.RequestException, KeyError, Failed) as exc:
                last = str(exc) if isinstance(exc, Failed) else type(exc).__name__
                time.sleep(2)
            except Exception as exc:
                # S3 startup is independently retried; no student-generated PASS is used.
                last = type(exc).__name__
                time.sleep(2)
        raise Failed("service_readiness:" + last)

    def run(self, request, filename, retry=False):
        before = self.spark()
        route = "dags/online_retail_training/dagRuns/" + request["execution_id"]
        if retry:
            # Preserve the exact producer/run/task/try facts of the attempts
            # about to be cleared and re-run — and their raw output bytes:
            # metadata alone is not byte preservation.
            self._task_provenance(route, "before_clear_retry")
            self.result.setdefault("log_capture", {})["airflow_preclear"] = (
                self._capture_preclear_logs(route)
            )
            # Public Airflow API; no import of student orchestration functions.
            self.api(
                "dags/online_retail_training/clearTaskInstances",
                "POST",
                {
                    "dag_run_id": request["execution_id"],
                    "dry_run": False,
                    "only_failed": False,
                    "only_running": False,
                    "reset_dag_runs": True,
                    "task_ids": TASKS,
                },
            )
        else:
            self.api(
                "dags/online_retail_training/dagRuns",
                "POST",
                {
                    "dag_run_id": request["execution_id"],
                    "conf": {"request_path": "/input/" + filename},
                },
            )
        while True:
            state = self.api(route)["state"]
            if state in {"success", "failed"}:
                if state == "failed":
                    self._task_provenance(route, "dag_run_failed")
                break
            time.sleep(2)
        ensure(state == "success", "dag_failed")
        tasks = self.api(route + "/taskInstances")["task_instances"]
        ensure(set(TASKS) <= {x["task_id"] for x in tasks}, "dag_tasks_missing")
        ensure(all(x["state"] == "success" for x in tasks), "dag_task_failed")
        after = self.spark()
        fresh = [value for key, value in after.items() if key not in before]
        ensure(
            any(x["state"] == "FINISHED" and x["name"] == "online_retail_training" for x in fresh),
            "spark_application_missing",
        )
        return {
            "execution_id": request["execution_id"],
            "spark_application_ids": [x["id"] for x in fresh],
        }

    def rejected_run(self, previous):
        request = read_json(INPUT / "rejected.json")
        before = self.s3.get_object(Bucket="hw3", Key="current.json")["Body"].read()
        self.api(
            "dags/online_retail_training/dagRuns",
            "POST",
            {
                "dag_run_id": request["execution_id"],
                "conf": {"request_path": "/input/rejected.json"},
            },
        )
        route = "dags/online_retail_training/dagRuns/" + request["execution_id"]
        while True:
            status = self.api(route)["state"]
            if status in {"failed", "success"}:
                if status == "failed":
                    self._task_provenance(route, "rejected_run_failed")
                break
            time.sleep(2)
        ensure(status == "failed", "incomplete_dag_succeeded")
        after = self.s3.get_object(Bucket="hw3", Key="current.json")["Body"].read()
        ensure(before == after, "failed_run_changed_current")
        objects = self.s3.list_objects_v2(
            Bucket="hw3", Prefix="runs/" + request["execution_id"] + "/result.json"
        )
        ensure(not objects.get("Contents"), "failed_run_published_ready")
        self.result["checks"]["failure_before_publish"] = True

    def download(self, request):
        prefix = "runs/" + request["execution_id"] + "/"
        root = OUT / "runs" / request["execution_id"]
        root.mkdir(parents=True, exist_ok=True)
        objects = []
        total = 0
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket="hw3", Prefix=prefix):
            objects += page.get("Contents", [])
            ensure(len(objects) <= 4096, "artifact_count_limit")
        ensure(objects, "s3_result_missing")
        seen = set()
        for obj in objects:
            relative = obj["Key"][len(prefix) :]
            path = PurePosixPath(relative)
            ensure(
                relative
                and not path.is_absolute()
                and ".." not in path.parts
                and str(path) == relative,
                "artifact_path_invalid",
            )
            total += obj["Size"]
            ensure(total <= LIMIT, "artifact_bytes_limit")
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            self.s3.download_file("hw3", obj["Key"], str(target))
            ensure(target.stat().st_size == obj["Size"], "artifact_download_size")
            seen.add(target)
        for target in root.rglob("*"):
            if target.is_file() and target not in seen:
                target.unlink()
        result = read_json(root / "result.json")
        ensure(result.get("status") == "ready", "result_not_ready")
        for key in ("contract_version", "execution_id", "submission_sha", "input_sha256"):
            ensure(result.get(key) == request[key], "result_identity_" + key)
        pointer_body = self.s3.get_object(Bucket="hw3", Key="current.json")["Body"].read(
            1024 * 1024 + 1
        )
        ensure(len(pointer_body) <= 1024 * 1024, "pointer_size")
        pointer = json.loads(pointer_body)
        ensure(
            pointer.get("execution_id") == request["execution_id"]
            and pointer.get("result_uri") == "s3://hw3/" + prefix + "result.json"
            and pointer.get("sha256") == sha(root / "result.json"),
            "current_pointer",
        )
        manifest = result.get("files", [])
        ensure(isinstance(manifest, list), "result_files_invalid")
        expected = {str(p.relative_to(root)): sha(p) for p in seen if p != root / "result.json"}
        ensure(
            len(manifest) == len(expected)
            and {v["path"]: v["sha256"] for v in manifest} == expected,
            "result_file_hashes",
        )
        ensure(read_json(root / "input/request.json") == request, "input_request_changed")
        ensure(sha(root / "input/input.csv.gz") == request["input_sha256"], "input_copy_changed")
        return root

    def audit(self, request):
        root = self.download(request)
        if self.args.expected:
            from decimal import Decimal

            import pyarrow.parquet as pq

            expected = read_json(Path(self.args.expected))["examples"]
            for split, rows in expected.items():
                actual = pq.read_table(root / "examples" / split).to_pylist()

                def normalize(row):
                    return {k: str(v) if isinstance(v, Decimal) else v for k, v in row.items()}

                ensure(
                    sorted(map(normalize, actual), key=lambda r: (r["as_of"], r["customer_id"]))
                    == sorted(rows, key=lambda r: (r["as_of"], r["customer_id"])),
                    "public_examples_" + split,
                )
            report = {
                "status": "PASSED",
                "checks": {"public_examples": True, "artifact_hashes": True},
            }
            # The public expected examples are only a subset of the server
            # contract. Run the same course-owned artifact oracle locally so a
            # local PASS cannot disagree with the server on artifact shape.
            import oracle

            artifact = oracle.check_artifacts(root, request, online=True)
            self.result["checks"]["artifact"] = artifact
            failed = sorted(
                name for name, passed in artifact.get("checks", {}).items() if not passed
            )
            diagnostic = artifact.get("diagnostic", "artifact_contract")
            if failed:
                diagnostic = f"{diagnostic}:{','.join(failed)}"
            ensure(artifact["status"] == "PASSED", diagnostic)
        else:
            import oracle

            report = oracle.check_artifacts(root, request, online=True)
            self.result["checks"]["artifact_" + request["execution_id"]] = report
            ensure(report["status"] == "PASSED", report.get("diagnostic", "artifact_contract"))
        return root, report

    @staticmethod
    def identities(root):
        return {
            "parent": read_json(root / "result.json")["parent_mlflow_run_id"],
            "children": {
                v["experiment_id"]: v["mlflow_run_id"]
                for v in read_json(root / "selection/experiments.json")["candidates"]
            },
        }

    def execute(self):
        self.ready()
        a, b = [read_json(INPUT / (key + ".json")) for key in ("a", "b")]
        if self.args.phase == "run":
            self.result["runs"] = [self.run(a, "a.json")]
            root, report = self.audit(a)
            identity = self.identities(root)
            self.result["checks"]["first"] = report
            # Deliberately leave a stale S3 part; publish must remove its own leftovers.
            stale = "runs/" + a["execution_id"] + "/examples/train/stale-part.parquet"
            self.s3.put_object(Bucket="hw3", Key=stale, Body=b"stale")
            self.result["runs"].append(self.run(a, "a.json", retry=True))
            root, report = self.audit(a)
            ensure(self.identities(root) == identity, "retry_run_ids_changed")
            ensure(
                not self.s3.list_objects_v2(Bucket="hw3", Prefix=stale).get("Contents"),
                "retry_stale_part",
            )
            self.result["checks"]["retry"] = report
            self.result["runs"].append(self.run(b, "b.json"))
            root, report = self.audit(b)
            self.result["checks"]["new_execution"] = report
            # The previous execution must remain available, and retain its run IDs.
            prior = OUT / "runs" / a["execution_id"]
            remote = self.s3.get_object(
                Bucket="hw3", Key="runs/" + a["execution_id"] + "/result.json"
            )["Body"].read(LIMIT + 1)
            ensure(
                hashlib.sha256(remote).hexdigest() == sha(prior / "result.json"),
                "previous_execution_changed",
            )
            self.result["identities"] = self.identities(root)
            self.rejected_run(b)
        else:
            root, report = self.audit(b)
            previous = read_json(OUT / "run.json")
            ensure(
                self.identities(root) == previous["identities"], "store_recreation_lost_identity"
            )
            self.result["checks"]["persistence"] = report
        self.result["status"] = "PASSED"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["run", "persist"], required=True)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--expected")
    parser.add_argument(
        "--log-bytes-limit",
        type=int,
        default=16 * 1024 * 1024,
        help="remaining course aggregate raw-byte budget for the pre-clear capture",
    )
    args = parser.parse_args()
    observer = Observer(args)
    try:
        observer.execute()
    except Failed as exc:
        observer.result.update(status="FAILED", diagnostic=str(exc))
    except (requests.RequestException, KeyError, ValueError, OSError) as exc:
        observer.result.update(
            status="FAILED", diagnostic="submission_artifact_or_endpoint:" + type(exc).__name__
        )
    except Exception as exc:
        observer.result.update(status="SYSTEM_ERROR", diagnostic="observer:" + type(exc).__name__)
    (OUT / (args.phase + ".json")).write_text(
        json.dumps(observer.result, indent=2, sort_keys=True) + "\n"
    )
    print(
        json.dumps(
            {"status": observer.result["status"], "diagnostic": observer.result.get("diagnostic")}
        )
    )
    return {"PASSED": 0, "FAILED": 1, "SYSTEM_ERROR": 2}[observer.result["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
