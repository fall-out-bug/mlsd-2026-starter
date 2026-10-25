#!/usr/bin/env python3
"""Course controller. Run only in a disposable VM with its own Docker daemon."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import select
import shutil
import subprocess
import sys
import tarfile
import threading
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# One shared raw-byte budget across every required capture stream comes from
# the course calibration (--log-bytes-limit, hw1_course_config.log_bytes_limit):
# it counts pre-masking bytes across all streams and both resource
# generations. Per-file caps never replace it. CAPTURE_STREAM_LIMIT_BYTES is
# only the transport cap that keeps one stream collectable through the VM
# boundary (hw3_project_vm.MAX_LOG_BYTES).
CAPTURE_STREAM_LIMIT_BYTES = 4 * 1024 * 1024
CAPTURE_BUDGET_SECONDS = 45
CAPTURE_DEADLINE_RESERVE = 30
DEFAULT_LOG_BYTES_LIMIT = 16 * 1024 * 1024
AGGREGATE_LOG_LIMIT = "aggregate_log_limit"
CAPTURE_KIND_FILES = {
    "student_containers": "student-containers",
    "airflow_tasks": "airflow-tasks",
}
# Resource generations whose required streams are captured separately: the
# "run" generation disappears when stores are force-recreated and cleared
# task attempts are retried, so it must be captured before that point.
CAPTURE_PHASES = ("run", "persist")

from validate import InvalidProject, validate_config  # noqa: E402


class CheckFailure(Exception):
    pass


class SystemFailure(Exception):
    pass


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def tree_digest(root):
    files = sorted(
        p
        for p in root.rglob("*")
        if p.is_file()
        and not p.is_symlink()
        and not ({".git", "output", "__pycache__", ".data"} & set(p.relative_to(root).parts))
    )
    return hashlib.sha256(
        b"".join(
            str(p.relative_to(root)).encode() + b"\0" + bytes.fromhex(digest(p)) for p in files
        )
    ).hexdigest()


def checker_digest(root):
    files = sorted(
        p
        for p in root.iterdir()
        if p.is_file()
        and (p.suffix == ".py" or p.name in {"Dockerfile", "requirements.txt", "requirements.lock"})
    )
    return hashlib.sha256(
        b"".join(p.name.encode() + b"\0" + bytes.fromhex(digest(p)) for p in files)
    ).hexdigest()


def read_json(path):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    return json.loads(
        path.read_text(),
        object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")),
    )


class AggregateCaptureBudget:
    """One shared raw-byte budget across every required capture stream.

    The limit is the course ``log_bytes_limit`` (pre-masking, pre-compression
    bytes of the required streams of this execution), created before the
    first command so build and control output consumes it as the bytes
    arrive. Streams are capped in capture order; when the shared budget
    cuts a stream the honest stop reason is ``aggregate_log_limit``.
    Reaching the course limit is a technical block (SYSTEM_ERROR,
    workflows/result-delivery.md): the affected execution stops safely
    with cleanup and an honest partial archive — never an educational
    verdict, never an automatic retry, never an attempt spent.
    """

    def __init__(self, limit_bytes):
        if isinstance(limit_bytes, bool) or not isinstance(limit_bytes, int):
            raise SystemFailure("log_bytes_limit_invalid")
        if limit_bytes <= 0:
            raise SystemFailure("log_bytes_limit_invalid")
        self.limit_bytes = limit_bytes
        self.consumed = 0
        self.exceeded = False

    def stream_cap(self):
        """Return (cap, overflow_stop_reason) for the next stream."""
        remaining = self.limit_bytes - self.consumed
        if remaining <= 0:
            self.exceeded = True
            return 0, AGGREGATE_LOG_LIMIT
        if remaining >= CAPTURE_STREAM_LIMIT_BYTES:
            return CAPTURE_STREAM_LIMIT_BYTES, "log_limit"
        return remaining, AGGREGATE_LOG_LIMIT

    def consume(self, raw_bytes):
        self.consumed += raw_bytes

    def mark_exceeded(self):
        self.exceeded = True



class _PipeDeadlineReached(Exception):
    pass


class _DeadlinePipeReader:
    """Select-based pipe wrapper: every read is bounded by the deadline.

    ``tarfile`` in stream mode issues blocking header and body reads; a
    stalled producer (a hung ``docker cp``) would otherwise hold the final
    capture indefinitely. This reader never blocks past its deadline and
    raises instead, so the caller can kill and reap the owned subprocess
    and keep the captured prefix.
    """

    def __init__(self, stream, deadline):
        self._stream = stream
        self._deadline = deadline

    def read(self, size=-1):
        if size is None or size <= 0:
            size = 1 << 20
        chunks = bytearray()
        while True:
            remaining = self._deadline - time.monotonic()
            if remaining <= 0:
                raise _PipeDeadlineReached()
            ready = select.select([self._stream], [], [], min(remaining, 0.25))[0]
            if not ready:
                continue
            try:
                chunk = os.read(self._stream.fileno(), min(size - len(chunks), 1 << 16))
            except InterruptedError:
                continue
            if not chunk:
                if chunks:
                    return bytes(chunks)
                return b""
            chunks += chunk
            if len(chunks) >= size:
                return bytes(chunks)


class Controller:
    failure = CheckFailure
    system_failure = SystemFailure

    def __init__(self, args):
        self.args = args
        self.project = args.project.resolve()
        self.output = args.out.resolve()
        if not args.expected and self.output.is_relative_to(self.project):
            raise SystemFailure("private_output_must_be_outside_submission")
        self.output.mkdir(parents=True, mode=0o700, exist_ok=False)
        self.started = time.monotonic()
        self.deadline = self.started + args.timeout
        self.name = "hw3_check_" + uuid.uuid4().hex[:16]
        self.input = self.output / "input"
        self.input.mkdir()
        self.artifacts = self.output / "artifacts"
        self.artifacts.mkdir(mode=0o777)
        self.artifacts.chmod(0o777)
        self.log_index = 0
        self.runtime_started = False
        self.containers = []
        limit = getattr(args, "log_bytes_limit", None)
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise SystemFailure("log_bytes_limit_invalid")
        self.log_bytes_limit = limit
        # Eager shared budget: build and control output consumes it as the
        # bytes arrive, before any masking or compression.
        self._capture_budget = AggregateCaptureBudget(limit)
        self.scheduler_container_id = None
        self.report = {
            "schema_version": "hw3-project-v1",
            "status": "SYSTEM_ERROR",
            "submission_sha": args.submission_sha,
            "contract_version": "hw3-temporal-v1",
            "checker_sha256": checker_digest(ROOT),
            "project_sha256": tree_digest(self.project),
            # Identity restatement must survive fast failures raised before
            # materialize() verifies the trusted bytes; the course request
            # carries the expected hash from the start.
            "input_sha256": read_json(args.request).get("input_sha256"),
            "stages": [],
            "checks": {},
        }
        self.env = {k: v for k, v in os.environ.items() if k in {"PATH", "HOME", "LANG", "TMPDIR"}}
        self.env.update(
            HW3_INPUT_DIR=str(self.input),
            HW3_AIRFLOW_USERNAME="hw3",
            HW3_AIRFLOW_PASSWORD="local-hw3-only",
            COMPOSE_PARALLEL_LIMIT="1",
            COMPOSE_PROJECT_NAME=self.name,
            DOCKER_HOST="unix:///var/run/docker.sock",
        )
        self.compose = [
            "docker",
            "compose",
            "--project-directory",
            str(self.project),
            "-p",
            self.name,
            "-f",
            str(self.project / "compose.yaml"),
        ]
        self.built = False

    def prepare_snapshot(self):
        if self.args.expected:
            self.report["source_mode"] = "local_working_tree"
            return
        if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", self.args.submission_sha):
            raise SystemFailure("invalid_submission_sha")
        try:
            repo = Path(
                subprocess.check_output(
                    [
                        "git",
                        "-c",
                        "safe.directory=" + str(self.project.parent),
                        "-C",
                        str(self.project),
                        "rev-parse",
                        "--show-toplevel",
                    ],
                    env=self.env,
                    timeout=15,
                    text=True,
                ).strip()
            )
            relative = self.project.relative_to(repo)
            archive = self.output / "submission.tar"
            with archive.open("wb") as stream:
                result = subprocess.run(
                    [
                        "git",
                        "-c",
                        "safe.directory=" + str(repo),
                        "-C",
                        str(repo),
                        "archive",
                        self.args.submission_sha + ":" + str(relative),
                    ],
                    env=self.env,
                    stdout=stream,
                    stderr=subprocess.PIPE,
                    timeout=30,
                )
            if result.returncode or archive.stat().st_size > 128 * 1024**2:
                raise SystemFailure("immutable_snapshot_unavailable")
            destination = self.output / "snapshot" / "hw3"
            destination.mkdir(parents=True)
            with tarfile.open(archive) as stream:
                members = stream.getmembers()
                if any(
                    not (m.isfile() or m.isdir())
                    or Path(m.name).is_absolute()
                    or ".." in Path(m.name).parts
                    for m in members
                ):
                    raise SystemFailure("immutable_snapshot_unsafe")
                stream.extractall(destination, members=members, filter="data")
            archive.unlink()
        except (OSError, subprocess.SubprocessError, ValueError, tarfile.TarError) as exc:
            raise SystemFailure("immutable_snapshot_unavailable") from exc
        self.project = destination
        self.compose = [
            "docker",
            "compose",
            "--project-directory",
            str(destination),
            "-p",
            self.name,
            "-f",
            str(destination / "compose.yaml"),
        ]
        self.report["project_sha256"] = tree_digest(destination)
        self.report["source_mode"] = "git_archive_exact_sha"

    def start_monitor(self):
        def memory():
            values = {}
            for line in Path("/proc/meminfo").read_text().splitlines():
                key, value = line.split(":", 1)
                values[key] = int(value.strip().split()[0]) * 1024
            return values

        def oom():
            return int(
                dict(line.split() for line in Path("/proc/vmstat").read_text().splitlines()).get(
                    "oom_kill", "0"
                )
            )

        first = memory()
        self.oom_before = oom()
        self.oom_count = oom
        self.report["guest"] = {
            "cpus": os.cpu_count(),
            "mem_total_bytes": first["MemTotal"],
            "swap_total_bytes": first["SwapTotal"],
            "minimum_mem_available_bytes": first["MemAvailable"],
        }
        if not self.args.expected:
            result = subprocess.run(
                ["systemd-detect-virt", "--vm"], capture_output=True, timeout=10
            )
            if (
                result.returncode
                or os.cpu_count() != 4
                or not 7 * 1024**3 < first["MemTotal"] <= 8 * 1024**3
                or first["SwapTotal"] != 0
            ):
                raise SystemFailure("disposable_vm_profile_required")
        self.monitor_stop = threading.Event()

        def sample():
            with (self.output / "memory-samples.jsonl").open("w") as stream:
                while not self.monitor_stop.is_set():
                    values = memory()
                    self.report["guest"]["minimum_mem_available_bytes"] = min(
                        self.report["guest"]["minimum_mem_available_bytes"], values["MemAvailable"]
                    )
                    stream.write(
                        json.dumps(
                            {
                                "elapsed_seconds": round(time.monotonic() - self.started, 2),
                                "mem_available_bytes": values["MemAvailable"],
                            }
                        )
                        + "\n"
                    )
                    stream.flush()
                    self.monitor_stop.wait(1)

        self.monitor = threading.Thread(target=sample, daemon=True)
        self.monitor.start()

    def command(self, label, argv, *, allowed=False, system=False, cap=1200):
        remaining = min(cap, self.deadline - time.monotonic() - 60)
        if remaining <= 0:
            raise SystemFailure("deadline_exceeded")
        budget_cap, overflow = self._capture_budget.stream_cap()
        if budget_cap <= 0:
            # The course budget is already spent: the output this command
            # owes cannot be collected. A technical block, never a verdict.
            self._capture_budget.mark_exceeded()
            raise SystemFailure(AGGREGATE_LOG_LIMIT)
        self.log_index += 1
        path = self.output / f"{self.log_index:03d}-{label}.log"
        start = time.monotonic()
        end = start + remaining
        written = 0
        stop_reason = None
        # Output goes to a private local file through the shared course
        # budget, counted as raw bytes arrive — never an unbounded pipe,
        # never a late size check.
        try:
            process = subprocess.Popen(
                argv,
                cwd=self.project,
                env=self.env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        except OSError as exc:
            raise SystemFailure(label + "_unavailable") from exc
        try:
            with path.open("wb") as log:
                while True:
                    room = budget_cap - written
                    if room <= 0:
                        stop_reason = overflow
                        break
                    left = end - time.monotonic()
                    if left <= 0:
                        stop_reason = "deadline_or_step_timeout:" + label
                        break
                    ready = select.select([process.stdout], [], [], min(left, 0.5))[0]
                    if not ready:
                        continue
                    chunk = os.read(process.stdout.fileno(), min(room, 1 << 16))
                    if not chunk:
                        break
                    log.write(chunk)
                    written += len(chunk)
                    self._capture_budget.consume(len(chunk))
                    if len(chunk) >= room:
                        stop_reason = overflow
                        break
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)
        self.report["stages"].append(
            {
                "stage": label,
                "seconds": round(time.monotonic() - start, 3),
                "exit_code": process.returncode,
                "bytes": written,
                "complete": stop_reason is None,
                "stop_reason": stop_reason,
            }
        )
        if stop_reason == AGGREGATE_LOG_LIMIT:
            # Safe stop of the affected execution with an honest partial
            # log (workflows/result-delivery.md): technical block, no retry.
            self._capture_budget.mark_exceeded()
            raise SystemFailure(AGGREGATE_LOG_LIMIT)
        if stop_reason is not None:
            raise SystemFailure("deadline_or_step_timeout:" + label)
        output = path.read_text(errors="replace")
        if process.returncode and not allowed:
            infrastructure = (
                "cannot connect to the docker daemon",
                "error during connect",
                "no space left on device",
                "tls handshake timeout",
                "i/o timeout",
                "temporary failure",
                "failed to resolve source metadata",
                "network is unreachable",
            )
            if any(value in output.lower() for value in infrastructure):
                raise SystemFailure(label + "_infrastructure")
            raise (SystemFailure if system else CheckFailure)(label + "_failed")
        return process.returncode, output

    def materialize(self):
        if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", self.args.submission_sha):
            raise SystemFailure("invalid_submission_sha")
        request = read_json(self.args.request)
        raw = (self.args.input_dir / request["input_path"]).resolve()
        if not raw.is_relative_to(self.args.input_dir.resolve()) or not raw.is_file():
            raise SystemFailure("trusted_input_missing")
        if digest(raw) != request["input_sha256"]:
            raise SystemFailure("trusted_input_hash_mismatch")
        shutil.copyfile(raw, self.input / "input.csv.gz")
        self.report["input_sha256"] = request["input_sha256"]
        self.requests = []
        for suffix in ("a", "b"):
            value = {
                **request,
                "execution_id": self.name + "_" + suffix,
                "submission_sha": self.args.submission_sha,
                "input_path": "input.csv.gz",
            }
            target = self.input / (suffix + ".json")
            target.write_text(json.dumps(value, sort_keys=True))
            target.chmod(0o444)
            self.requests.append(value)
        rejected = {
            **self.requests[1],
            "execution_id": self.name + "_rejected",
            "observed_until": self.requests[1]["snapshots"][-1]["as_of"],
        }
        (self.input / "rejected.json").write_text(json.dumps(rejected, sort_keys=True))
        (self.input / "input.csv.gz").chmod(0o444)
        self.report["execution_ids"] = [r["execution_id"] for r in self.requests]
        if self.args.expected:
            shutil.copyfile(self.args.expected, self.input / "expected.json")

    def preflight(self):
        if not (self.project / "compose.yaml").is_file():
            raise CheckFailure("compose_missing")
        _, raw = self.command("compose-config", self.compose + ["config", "--format", "json"])
        try:
            config = json.loads(raw)
        except ValueError as exc:
            raise CheckFailure("compose_config_invalid") from exc
        validate_config(config, self.project, self.input)
        # All runtime networks are internal; build still has dependency access.
        override = {
            "networks": {
                name: {"internal": True} for name in config.get("networks", {"default": {}})
            }
        }
        self.build_services = []
        image_builds = {}
        override["services"] = {}
        for service, definition in config["services"].items():
            if "build" not in definition:
                continue
            identity = json.dumps(definition["build"], sort_keys=True)
            if identity not in image_builds:
                image_builds[identity] = self.name + "_build_" + str(len(image_builds))
                self.build_services.append(service)
            override["services"][service] = {"image": image_builds[identity]}
        override_path = self.output / "isolation.json"
        override_path.write_text(json.dumps(override))
        self.compose += ["-f", str(override_path)]
        self.report["checks"]["compose_policy"] = True

    def observer(self, phase):
        container = self.name + "_observer_" + phase
        self.containers.append(container)
        argv = [
            "docker",
            "run",
            "--name",
            container,
            "--network",
            self.network,
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--pids-limit=128",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,size=512m",
            "-v",
            f"{self.input}:/input:ro",
            "-v",
            f"{self.artifacts}:/out",
            "-e",
            "MLFLOW_TRACKING_URI=http://mlflow:5000",
            "-e",
            "MLFLOW_S3_ENDPOINT_URL=http://seaweedfs:8333",
            "-e",
            "AWS_ACCESS_KEY_ID=local-hw3",
            "-e",
            "AWS_SECRET_ACCESS_KEY=local-hw3-only",
            "-e",
            "AWS_DEFAULT_REGION=us-east-1",
            "-e",
            "AWS_EC2_METADATA_DISABLED=true",
            self.observer_image,
            "--phase",
            phase,
            "--timeout",
            str(max(1, int(self.deadline - time.monotonic() - 45))),
        ]
        if self.args.expected:
            argv += ["--expected", "/input/expected.json"]
        # The observer's pre-clear capture shares the course budget: it is
        # capped by whatever remains at this point in the execution.
        remaining_cap, _overflow = self._capture_budget.stream_cap()
        argv += ["--log-bytes-limit", str(max(0, remaining_cap))]
        rc, _ = self.command("observer-" + phase, argv, allowed=True, cap=2400)
        report_path = self.artifacts / (phase + ".json")
        if not report_path.is_file():
            raise SystemFailure("observer_result_missing")
        result = read_json(report_path)
        self.report["checks"][phase] = result
        if result.get("status") != "PASSED":
            raise (CheckFailure if result.get("status") == "FAILED" else SystemFailure)(
                result.get("diagnostic", "observer_failed")
            )
        if rc:
            raise SystemFailure("observer_exit_mismatch")

    def capture_runtime_logs(self, phase):
        """Collect the required runtime streams of one resource generation.

        ``phase="run"`` runs before ``--force-recreate`` replaces the store
        containers and before cleared task attempts are retried, so that
        generation's streams survive their resources; ``phase="persist"`` runs
        in the final ``finally`` before compose down. Each attempt records
        honest facts with provenance in ``report["log_capture"][phase]``. A
        capture error never masks the verdict, but the shared course budget
        is not cosmetic: reaching it is the technical block defined by
        workflows/result-delivery.md (safe stop, honest partial archive,
        no educational verdict, no automatic retry).
        """
        if not self.runtime_started:
            return
        if phase not in CAPTURE_PHASES:
            raise SystemFailure("capture_phase_invalid")
        facts = self.report.setdefault("log_capture", {}).setdefault(phase, {})
        deadline = min(
            self.deadline - CAPTURE_DEADLINE_RESERVE,
            time.monotonic() + CAPTURE_BUDGET_SECONDS,
        )
        provenance = {
            "phase": phase,
            "compose_project": self.name,
            "scheduler_container_id": self.scheduler_container_id,
            "execution_ids": self.report.get("execution_ids", []),
            "aggregate_limit_bytes": self._capture_budget.limit_bytes,
        }
        for key, attempt in (
            ("student_containers", self._capture_containers_log),
            ("airflow_tasks", self._capture_airflow_task_logs),
        ):
            cap, overflow = self._capture_budget.stream_cap()
            if cap <= 0:
                facts[key] = {"lost": AGGREGATE_LOG_LIMIT, "provenance": provenance}
                continue
            try:
                fact = attempt(deadline, phase, cap, overflow)
            except Exception as exc:  # a capture error is an honest loss
                fact = {"lost": "capture_error:" + type(exc).__name__}
            fact["provenance"] = provenance
            facts[key] = fact
            if isinstance(fact.get("bytes"), int):
                self._capture_budget.consume(fact["bytes"])
            if fact.get("stop_reason") == AGGREGATE_LOG_LIMIT:
                self._capture_budget.mark_exceeded()
        self._enforce_aggregate_block()

    def _enforce_aggregate_block(self):
        """Reaching the course limit is a technical block, not a verdict.

        workflows/result-delivery.md: safe stop, honest partial archive,
        escalation as SYSTEM_ERROR — no educational outcome, no automatic
        retry, no spent attempt.
        """
        if self._capture_budget.exceeded:
            self.report.update(status="SYSTEM_ERROR", diagnostic=AGGREGATE_LOG_LIMIT)

    def _adopt_preclear_capture(self, phase):
        """Stage the observer's pre-clear capture of raw task-attempt bytes.

        The observer fetches each attempt's exact log bytes through the
        pinned Airflow API before ``clearTaskInstances`` destroys the
        attempts (metadata alone is not byte preservation) and stages them
        under /out. The controller adopts the file into the capture layout
        and charges its bytes to the same shared course budget.
        """
        result = self.report["checks"].get(phase)
        fact = None
        if isinstance(result, dict):
            captured = result.get("log_capture")
            if isinstance(captured, dict) and isinstance(
                captured.get("airflow_preclear"), dict
            ):
                fact = captured["airflow_preclear"]
        name = "airflow-preclear." + phase + ".log"
        source = self.artifacts / name
        facts = self.report.setdefault("log_capture", {}).setdefault(phase, {})
        if isinstance(fact, dict) and fact.get("file") == name and source.is_file():
            target = self.output / name
            if not target.exists():
                shutil.copyfile(source, target)
            entry = dict(fact)
            entry["provenance"] = {
                "phase": phase,
                "compose_project": self.name,
                "scheduler_container_id": self.scheduler_container_id,
                "execution_ids": self.report.get("execution_ids", []),
                "source": "observer_preclear",
            }
            facts["airflow_preclear"] = entry
            if isinstance(entry.get("bytes"), int):
                self._capture_budget.consume(entry["bytes"])
                if entry.get("stop_reason") == AGGREGATE_LOG_LIMIT:
                    self._capture_budget.mark_exceeded()
        elif self.runtime_started:
            facts["airflow_preclear"] = {
                "lost": "preclear_capture_unavailable",
                "provenance": {"phase": phase, "source": "observer_preclear"},
            }

    def _capture_containers_log(self, deadline, phase, cap, overflow):
        path = self.output / (CAPTURE_KIND_FILES["student_containers"] + "." + phase + ".log")
        return self._run_bounded(
            self.compose + ["logs", "--no-color", "--timestamps"], path, deadline, cap, overflow
        )

    def _capture_airflow_task_logs(self, deadline, phase, cap, overflow):
        if time.monotonic() >= deadline:
            return {"lost": "capture_budget_exhausted"}
        try:
            listed = subprocess.run(
                self.compose + ["ps", "-q", "airflow-scheduler"],
                env=self.env,
                cwd=self.project,
                capture_output=True,
                timeout=max(1, int(deadline - time.monotonic())),
            )
        except (OSError, subprocess.SubprocessError):
            return {"lost": "scheduler_container_unknown"}
        cid = listed.stdout.decode(errors="replace").strip()
        if listed.returncode or not cid:
            return {"lost": "scheduler_container_missing"}
        try:
            inspected = subprocess.run(
                ["docker", "inspect", cid, "--format", "{{json .Config.Env}}"],
                env=self.env,
                cwd=self.project,
                capture_output=True,
                timeout=max(1, int(deadline - time.monotonic())),
            )
            environment = json.loads(inspected.stdout)
        except (OSError, ValueError, subprocess.SubprocessError):
            return {"lost": "scheduler_container_unknown"}
        home = "/opt/airflow"
        if isinstance(environment, list):
            for item in environment:
                if isinstance(item, str) and item.startswith("AIRFLOW_HOME="):
                    home = item.split("=", 1)[1]
        path = self.output / (CAPTURE_KIND_FILES["airflow_tasks"] + "." + phase + ".log")
        return self._run_tar_bounded(
            ["docker", "cp", cid + ":" + home + "/logs", "-"], path, deadline, cap, overflow
        )

    def _run_bounded(self, argv, path, deadline, cap, overflow):
        """Stream one command's stdout into ``path`` up to ``cap`` bytes.

        ``complete`` is true only when the source ended by itself under the
        byte cap (``overflow`` names whether the transport cap or the shared
        aggregate budget cut it) and the capture budget; otherwise the
        partial file is kept with the honest stop reason.
        """
        written = 0
        stop_reason = None
        try:
            process = subprocess.Popen(
                argv,
                env=self.env,
                cwd=self.project,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
        except OSError:
            return {"lost": "capture_failed"}
        try:
            with path.open("wb") as sink:
                while stop_reason is None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        stop_reason = "capture_timeout"
                        break
                    ready = select.select([process.stdout], [], [], min(remaining, 1.0))[0]
                    if not ready:
                        continue
                    chunk = os.read(process.stdout.fileno(), 1 << 16)
                    if not chunk:
                        break
                    room = cap - written
                    if len(chunk) > room:
                        sink.write(chunk[:room])
                        written += room
                        stop_reason = overflow
                        break
                    sink.write(chunk)
                    written += len(chunk)
                    if written == cap:
                        # The source is not proven drained at the boundary.
                        stop_reason = overflow
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)
        returncode = process.returncode
        if stop_reason is None and returncode not in (0, -9):
            stop_reason = "capture_failed:" + str(returncode)
        if written == 0 and stop_reason is not None:
            # Nothing was captured: report the lost stream, not an empty file.
            path.unlink(missing_ok=True)
            return {"lost": stop_reason}
        if stop_reason is None:
            return {"file": path.name, "bytes": written, "complete": True, "stop_reason": None}
        return {
            "file": path.name,
            "bytes": written,
            "complete": False,
            "stop_reason": stop_reason,
        }

    def _run_tar_bounded(self, argv, path, deadline, cap, overflow):
        """Materialize a streamed tar of log files into one bounded text log.

        Every regular archive member becomes a ``===== <name> =====`` section.
        Header and body reads go through a deadline-bounded pipe reader, so
        a stalled producer (no first header, or a hang mid-file) is cut at
        the deadline — not after an unbounded blocking read. The byte cap
        and capture budget apply to the written file; losing the tail keeps
        the prefix with an honest stop reason (``overflow`` distinguishes
        the aggregate budget from the transport cap). The owned subprocess
        is killed and reaped by its exact handle in the finally.
        """
        written = 0
        stop_reason = None
        try:
            process = subprocess.Popen(
                argv,
                env=self.env,
                cwd=self.project,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
        except OSError:
            return {"lost": "capture_failed"}
        reader = _DeadlinePipeReader(process.stdout, deadline)
        try:
            with path.open("wb") as sink, tarfile.open(fileobj=reader, mode="r|") as archive:
                for member in archive:
                    if not member.isfile():
                        continue
                    budget = cap - written
                    if budget <= 0:
                        stop_reason = overflow
                        break
                    header = ("===== " + member.name + " =====\n").encode("utf-8", "replace")
                    if len(header) > budget:
                        stop_reason = overflow
                        break
                    if time.monotonic() >= deadline:
                        stop_reason = "capture_timeout"
                        break
                    sink.write(header)
                    written += len(header)
                    source = archive.extractfile(member)
                    if source is None:
                        continue
                    while True:
                        room = cap - written
                        if room <= 0:
                            stop_reason = overflow
                            break
                        chunk = source.read(min(1 << 16, room))
                        if not chunk:
                            break
                        sink.write(chunk)
                        written += len(chunk)
                    if stop_reason:
                        break
        except _PipeDeadlineReached:
            stop_reason = "capture_timeout"
        except (OSError, ValueError, tarfile.TarError):
            if stop_reason is None:
                stop_reason = "capture_failed"
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)
        if stop_reason is None and process.returncode not in (0, -9):
            stop_reason = "capture_failed:" + str(process.returncode)
        if written == 0 and stop_reason is not None:
            # Nothing was captured: report the lost stream, not an empty file.
            path.unlink(missing_ok=True)
            return {"lost": stop_reason}
        if stop_reason is None:
            return {"file": path.name, "bytes": written, "complete": True, "stop_reason": None}
        return {
            "file": path.name,
            "bytes": written,
            "complete": False,
            "stop_reason": stop_reason,
        }

    def execute(self):
        try:
            self.start_monitor()
            self.prepare_snapshot()
            self.preflight()
            self.command(
                "docker-ready",
                ["docker", "info", "--format", "{{.ServerVersion}}"],
                system=True,
                cap=30,
            )
            self.command(
                "observer-build",
                ["docker", "build", "-t", self.name + "_observer", str(ROOT)],
                system=True,
            )
            _, raw = self.command(
                "observer-image",
                ["docker", "image", "inspect", self.name + "_observer", "--format", "{{.Id}}"],
                system=True,
            )
            self.observer_image = raw.strip()
            self.report["observer_image_id"] = self.observer_image
            for service in self.build_services:
                self.command("student-build-" + service, self.compose + ["build", service])
            self.built = True
            self.materialize()
            # From here the runtime may hold required student streams; the
            # finally block must capture them before cleanup even when
            # compose-up itself fails partway.
            self.runtime_started = True
            self.command("compose-up", self.compose + ["up", "-d"], cap=300)
            _, raw = self.command("scheduler-id", self.compose + ["ps", "-q", "airflow-scheduler"])
            cid = raw.strip()
            self.scheduler_container_id = cid
            _, raw = self.command(
                "scheduler-inspect",
                ["docker", "inspect", cid, "--format", "{{json .NetworkSettings.Networks}}"],
            )
            networks = json.loads(raw)
            if len(networks) != 1:
                raise CheckFailure("single_runtime_network_required")
            self.network = next(iter(networks))
            _, raw = self.command(
                "student-image", ["docker", "inspect", cid, "--format", "{{.Image}}"]
            )
            self.student_image = raw.strip()
            self.report["student_image_id"] = self.student_image
            self.verify_environment()
            self.observer("run")
            # The observer captured the raw bytes of the attempts that
            # clearTaskInstances (inside that observer run) is about to
            # destroy: adopt them before this generation's other streams.
            self._adopt_preclear_capture("run")
            # The store containers are about to be replaced and the cleared
            # task attempts retried: capture this generation's required
            # streams first, or they stop existing with their resources.
            self.capture_runtime_logs("run")
            self.command(
                "recreate-stores",
                self.compose + ["up", "-d", "--no-deps", "--force-recreate", "seaweedfs", "mlflow"],
            )
            self.observer("persist")
            self.verify_candidates()
            if tree_digest(self.project) != self.report["project_sha256"]:
                raise CheckFailure("project_bytes_changed")
            self.report["status"] = "PASSED"
        except (InvalidProject, CheckFailure) as exc:
            self.report.update(status="FAILED", diagnostic=str(exc))
        except SystemFailure as exc:
            self.report.update(status="SYSTEM_ERROR", diagnostic=str(exc))
        except Exception as exc:
            self.report.update(status="SYSTEM_ERROR", diagnostic="controller:" + type(exc).__name__)
        finally:
            self.capture_runtime_logs("persist")
            # The aggregate block supersedes an earlier PASSED/FAILED: the
            # full diagnostics obligation is unmet (see _enforce_aggregate_block).
            self._enforce_aggregate_block()
            self.cleanup()
            if hasattr(self, "monitor_stop"):
                self.monitor_stop.set()
                self.monitor.join(timeout=2)
                delta = self.oom_count() - self.oom_before
                self.report["guest"]["oom_kill_delta"] = delta
                if delta:
                    self.report.update(status="SYSTEM_ERROR", diagnostic="guest_oom")
            self.report["elapsed_seconds"] = round(time.monotonic() - self.started, 3)
            (self.output / "result.json").write_text(
                json.dumps(self.report, indent=2, sort_keys=True) + "\n"
            )
        return self.report

    def verify_environment(self):
        expected = {
            "apache-airflow": "2.10.5",
            "pyspark": "3.5.4",
            "mlflow": "2.22.0",
            "scikit-learn": "1.5.2",
            "numpy": "1.26.4",
            "scipy": "1.14.1",
            "pandas": "2.1.4",
            "pyarrow": "19.0.0",
            "joblib": "1.4.2",
            "boto3": "1.36.3",
        }
        code = (
            "import json,sys,importlib.metadata as m; "
            "print(json.dumps({'python':list(sys.version_info[:2]),"
            "'packages':{name:m.version(name) for name in " + repr(list(expected)) + "}}))"
        )
        _, raw = self.command(
            "runtime-versions",
            self.compose + ["exec", "-T", "airflow-scheduler", "python", "-c", code],
        )
        try:
            actual = json.loads(raw)
        except ValueError as exc:
            raise CheckFailure("runtime_versions_invalid") from exc
        if actual.get("python") != [3, 12] or actual.get("packages") != expected:
            raise CheckFailure("runtime_versions_mismatch")
        self.report["checks"]["versions"] = actual

    def verify_candidates(self):
        """Declared candidates must match the run artifacts byte-for-byte.

        Artifact-level check in the stage-contract model: the submission
        declares its comparison set in config/candidates.json, and
        selection/experiments.json of the first run must carry exactly the
        declared configs.
        """
        declared_path = self.project / "config" / "candidates.json"
        if not declared_path.is_file():
            raise CheckFailure("candidates_missing")
        try:
            declared = json.loads(declared_path.read_text(encoding="utf-8"))
        except ValueError as exc:
            raise CheckFailure("candidates_invalid") from exc
        if not isinstance(declared, list) or not declared or not all(
            isinstance(item, dict) and isinstance(item.get("experiment_id"), str)
            for item in declared
        ):
            raise CheckFailure("candidates_invalid")
        families = [item.get("family") for item in declared]
        if (
            families.count("constant") != 1
            or "logistic" not in families
            or "tree" not in families
        ):
            raise CheckFailure("candidates_invalid")
        experiments_path = (
            self.artifacts
            / "runs"
            / self.requests[0]["execution_id"]
            / "selection"
            / "experiments.json"
        )
        if not experiments_path.is_file():
            raise SystemFailure("experiments_artifact_missing")
        try:
            experiments = json.loads(experiments_path.read_text(encoding="utf-8"))
            recorded = [item["config"] for item in experiments["candidates"]]
        except (ValueError, KeyError, TypeError) as exc:
            raise CheckFailure("experiments_invalid") from exc

        def canonical(value):
            return json.dumps(value, sort_keys=True, separators=(",", ":"))

        if {canonical(item) for item in recorded} != {canonical(item) for item in declared}:
            raise CheckFailure("candidates_declared_mismatch")
        self.report["checks"]["candidates"] = True

    def cleanup(self):
        # A single cleanup budget, including all probe containers.
        cleanup_deadline = min(self.deadline, time.monotonic() + 45)
        failed = False

        def run(argv, cap):
            remaining = min(cap, cleanup_deadline - time.monotonic())
            if remaining <= 0:
                return None
            try:
                return subprocess.run(
                    argv, env=self.env, cwd=self.project, capture_output=True, timeout=remaining
                )
            except (OSError, subprocess.TimeoutExpired):
                return None

        if self.containers:
            # Some failed starts may not have created a container; inspect only our exact names.
            existing = []
            for name in self.containers:
                result = run(["docker", "ps", "-aq", "--filter", "name=^/" + name + "$"], 2)
                if result is None or result.returncode:
                    failed = True
                    break
                existing.extend(result.stdout.decode().split())
            if existing:
                result = run(["docker", "rm", "-f", *existing], 15)
                failed |= result is None or result.returncode != 0
        if self.built:
            result = run(
                self.compose + ["down", "--volumes", "--remove-orphans", "--timeout", "3"], 20
            )
            failed |= result is None or result.returncode != 0
            for kind in ("container", "volume", "network"):
                result = run(
                    [
                        "docker",
                        kind,
                        "ls",
                        "-q",
                        "--filter",
                        "label=com.docker.compose.project=" + self.name,
                    ],
                    3,
                )
                failed |= result is None or result.returncode != 0 or bool(result.stdout.strip())
        self.report["checks"]["cleanup"] = not failed
        if failed:
            self.report.update(status="SYSTEM_ERROR", diagnostic="cleanup_unconfirmed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project", type=Path, required=True, help="hw3 directory in the immutable snapshot"
    )
    parser.add_argument("--submission-sha", required=True)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True, help="new private output directory")
    parser.add_argument(
        "--expected", type=Path, help="public toy expected examples; local check only"
    )
    parser.add_argument("--timeout", type=int, default=2700)
    parser.add_argument(
        "--log-bytes-limit",
        type=int,
        default=DEFAULT_LOG_BYTES_LIMIT,
        help="course aggregate raw-byte budget across required capture streams",
    )
    args = parser.parse_args()
    if not 60 <= args.timeout <= 2700:
        parser.error("timeout must be between 60 and 2700 seconds")
    if args.log_bytes_limit <= 0:
        parser.error("log-bytes-limit must be positive")
    result = Controller(args).execute()
    print(json.dumps({k: v for k, v in result.items() if k not in {"stages", "checks"}}))
    return {"PASSED": 0, "FAILED": 1, "SYSTEM_ERROR": 2}[result["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
