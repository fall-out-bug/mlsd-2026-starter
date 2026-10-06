"""Behavioral tests for the supplied checker, not a student Spark solution."""

import importlib.util
import json
import subprocess
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


controller = module("hw2_local_check", ROOT / "check.py")
observer = module("hw2_project_observer", ROOT / "checks/observe.py")


def config():
    images = json.loads((ROOT / "versions.json").read_text())["images"]
    services = {
        name: {
            "networks": {"default": {}},
            "healthcheck": {"test": ["CMD", "true"]},
            "image": images.get(name, images["airflow"]),
        }
        for name in controller.SERVICES
    }
    for name in (
        "airflow-init",
        "airflow-webserver",
        "airflow-scheduler",
        "spark-master",
        "spark-worker",
    ):
        services[name]["build"] = {"context": str(ROOT), "dockerfile": "airflow/Dockerfile"}
    for name in ("airflow-webserver", "airflow-scheduler"):
        services[name]["depends_on"] = {
            "airflow-init": {"condition": "service_completed_successfully"}
        }
    return {"name": "localcheck", "services": services}


def graph():
    return [
        {
            "task_id": name,
            "downstream_task_ids": [observer.TASK_IDS[index + 1]]
            if index + 1 < len(observer.TASK_IDS)
            else [],
        }
        for index, name in enumerate(observer.TASK_IDS)
    ]


class ProjectConfigurationTests(unittest.TestCase):
    def test_standard_config_is_accepted(self):
        controller.validate_config(config(), ROOT)

    def test_missing_service_rejected(self):
        value = config()
        del value["services"]["spark-worker"]
        with self.assertRaises(controller.InvalidProject):
            controller.validate_config(value, ROOT)

    def test_missing_readiness_rejected(self):
        for mutation in (
            {"healthcheck": {"disable": True}},
            {"networks": {}},
            {"depends_on": {"airflow-init": {"condition": "service_started"}}},
        ):
            value = config()
            value["services"]["airflow-webserver"].update(mutation)
            with self.subTest(mutation=mutation), self.assertRaises(controller.InvalidProject):
                controller.validate_config(value, ROOT)

    def test_global_container_name_and_host_access_rejected(self):
        for setting in ("container_name", "privileged", "network_mode", "pid"):
            value = config()
            value["services"]["redis"][setting] = "host"
            with self.subTest(setting=setting), self.assertRaises(controller.InvalidProject):
                controller.validate_config(value, ROOT)

    def test_bind_mount_outside_project_rejected(self):
        value = config()
        value["services"]["redis"]["volumes"] = [
            {"type": "bind", "source": "/etc", "target": "/outside"}
        ]
        with self.assertRaises(controller.InvalidProject):
            controller.validate_config(value, ROOT)

    def test_external_resources_and_build_context_rejected(self):
        for kind in ("volumes", "networks"):
            value = config()
            value[kind] = {"shared": {"external": True}}
            with self.subTest(kind=kind), self.assertRaises(controller.InvalidProject):
                controller.validate_config(value, ROOT)
        value = config()
        value["services"]["spark-master"]["build"]["context"] = "/tmp"
        with self.assertRaises(controller.InvalidProject):
            controller.validate_config(value, ROOT)

    def test_wrong_infrastructure_version_rejected(self):
        value = config()
        value["services"]["redis"]["image"] = "redis:7@sha256:" + "f" * 64
        with self.assertRaises(controller.InvalidProject):
            controller.validate_config(value, ROOT)

    def test_cleanup_runs_down_after_observer_timeout(self):
        with patch.object(
            controller,
            "command",
            side_effect=[
                subprocess.TimeoutExpired("inspect", 15),
                subprocess.CompletedProcess([], 0),
            ],
        ) as command:
            self.assertFalse(controller.cleanup(["docker", "compose", "-p", "owned"], "owned"))
        self.assertEqual(command.call_count, 2)
        self.assertIn("down", command.call_args.args[0])

    def test_cleanup_never_removes_another_observer(self):
        with patch.object(
            controller,
            "command",
            side_effect=[
                subprocess.CompletedProcess([], 0, "different\n"),
                subprocess.CompletedProcess([], 0),
            ],
        ) as command:
            self.assertFalse(controller.cleanup(["docker", "compose", "-p", "owned"], "owned"))
        self.assertEqual(command.call_count, 2)
        self.assertNotIn("rm", command.call_args.args[0])

    def test_cleanup_failure_cannot_be_success(self):
        with patch.object(
            controller,
            "command",
            side_effect=[
                subprocess.CompletedProcess([], 1, ""),
                subprocess.CompletedProcess([], 1),
            ],
        ):
            self.assertFalse(controller.cleanup(["docker", "compose", "-p", "owned"], "owned"))


class OutputTests(unittest.TestCase):
    def test_oracle_filters_counts_distinct_orders_and_keeps_duplicate_lines(self):
        raw = (
            b"customer_id,invoice_id,quantity,unit_price\nx,one,1,1.005\nx,one,1,1.005\n"
            b"x,two,1,0.005\nx,cancel,1,999\nx,C123,1,999\nx,no,-1,999\n"
            b"x,free,1,0\n,anon,1,999\n"
        )
        self.assertEqual(
            observer.expected_features(raw),
            {"x": {"order_count": 2, "total_amount": "2.015000", "average_order_amount": "1.01"}},
        )

    def test_half_even_and_decimal_precision(self):
        raw = b"customer_id,invoice_id,quantity,unit_price\nx,one,1,1.005000\ny,two,1,1.015000\n"
        expected = observer.expected_features(raw)
        self.assertEqual(expected["x"]["average_order_amount"], "1.00")
        self.assertEqual(expected["y"]["average_order_amount"], "1.02")

    def test_strict_json_rejects_duplicate_keys_and_nonfinite_numbers(self):
        for raw in ('{"x":1,"x":2}', '{"x":NaN}', '{"x":Infinity}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                observer.strict_json(raw)

    def test_required_tasks_must_be_connected_in_order(self):
        observer.check_dependencies(graph())
        value = graph()
        value[1]["downstream_task_ids"] = []
        with self.assertRaisesRegex(observer.ProjectFailure, "dependency"):
            observer.check_dependencies(value)

    def test_extra_tasks_are_allowed_in_chain(self):
        value = graph()
        value[0]["downstream_task_ids"] = ["helper"]
        value.append({"task_id": "helper", "downstream_task_ids": ["put_dataset"]})
        observer.check_dependencies(value)

    def test_missing_malformed_or_duplicate_tasks_are_rejected(self):
        for value in (graph()[1:], None, [{"task_id": "get_dataset"}], graph() + [graph()[0]]):
            with self.subTest(value=value), self.assertRaises(observer.ProjectFailure):
                observer.check_dependencies(value)

    def test_successful_dag_without_spark_does_not_pass(self):
        responses = [
            {},
            {},
            {"state": "success"},
            {
                "task_instances": [
                    {"task_id": name, "state": "success"} for name in observer.TASK_IDS
                ]
            },
        ]
        with (
            patch.object(observer, "airflow_api", side_effect=responses),
            patch.object(observer, "spark_apps", return_value={}),
            patch.object(observer.time, "monotonic", side_effect=[0, 0, 31]),
            self.assertRaisesRegex(observer.ProjectFailure, "spark_application_not_observed"),
        ):
            observer.run_dag()

    def test_old_spark_application_cannot_replace_a_new_one(self):
        old = {"app-old": {"id": "app-old", "name": observer.DAG_ID, "state": "FINISHED"}}
        responses = [
            {},
            {},
            {"state": "success"},
            {
                "task_instances": [
                    {"task_id": name, "state": "success"} for name in observer.TASK_IDS
                ]
            },
        ]
        with (
            patch.object(observer, "airflow_api", side_effect=responses),
            patch.object(observer, "spark_apps", return_value=old),
            patch.object(observer.time, "monotonic", side_effect=[0, 0, 31]),
            self.assertRaisesRegex(observer.ProjectFailure, "spark_application_not_observed"),
        ):
            observer.run_dag()

    def test_skipped_task_is_not_success(self):
        responses = [
            {},
            {},
            {"state": "success"},
            {
                "task_instances": [
                    {"task_id": name, "state": "skipped" if index == 2 else "success"}
                    for index, name in enumerate(observer.TASK_IDS)
                ]
            },
        ]
        with (
            patch.object(observer, "airflow_api", side_effect=responses),
            patch.object(observer, "spark_apps", return_value={}),
            self.assertRaisesRegex(observer.ProjectFailure, "required_task_not_successful"),
        ):
            observer.run_dag()

    def test_completed_spark_and_all_successful_tasks_accepted(self):
        responses = [
            {},
            {},
            {"state": "success"},
            {
                "task_instances": [
                    {"task_id": name, "state": "success"} for name in observer.TASK_IDS
                ]
            },
        ]
        new = {"app-new": {"id": "app-new", "name": observer.DAG_ID, "state": "FINISHED"}}
        with (
            patch.object(observer, "airflow_api", side_effect=responses),
            patch.object(observer, "spark_apps", side_effect=[{}, new]),
        ):
            value = observer.run_dag()
        self.assertEqual(value["spark_application_id"], "app-new")
        self.assertEqual(value["tasks"], dict.fromkeys(observer.TASK_IDS, "success"))

    def test_wrong_customer_or_amount_rejected(self):
        expected = {
            "x": {"order_count": 1, "total_amount": "1.000000", "average_order_amount": "1.00"}
        }
        for value in (
            {},
            dict(expected, y=expected["x"]),
            {"x": dict(expected["x"], total_amount="2.000000")},
        ):
            with self.subTest(value=value), self.assertRaises(observer.ProjectFailure):
                observer.require_equal(value, expected, "redis")


@unittest.skipUnless(importlib.util.find_spec("pyarrow"), "observer container provides pyarrow")
class ParquetTests(unittest.TestCase):
    @staticmethod
    def table():
        import pyarrow as pa

        return pa.Table.from_pylist(
            [
                {
                    "customer_id": "x",
                    "order_count": 1,
                    "total_amount": Decimal("1.000000"),
                    "average_order_amount": Decimal("1.00"),
                }
            ],
            schema=pa.schema(
                [
                    ("customer_id", pa.string()),
                    ("order_count", pa.int64()),
                    ("total_amount", pa.decimal128(20, 6)),
                    ("average_order_amount", pa.decimal128(20, 2)),
                ]
            ),
        )

    def test_exact_schema_and_decimal_strings_accepted(self):
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as temporary:
            pq.write_table(self.table(), Path(temporary) / "part.parquet")
            self.assertEqual(
                observer.parquet_features(Path(temporary)),
                {
                    "x": {
                        "order_count": 1,
                        "total_amount": "1.000000",
                        "average_order_amount": "1.00",
                    },
                },
            )

    def test_wrong_schema_rejected(self):
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = self.table().set_column(1, "order_count", pa.array([1], type=pa.int32()))
        with tempfile.TemporaryDirectory() as temporary:
            pq.write_table(table, Path(temporary) / "part.parquet")
            with self.assertRaisesRegex(observer.ProjectFailure, "parquet_schema_invalid"):
                observer.parquet_features(Path(temporary))

    def test_duplicate_customer_in_different_files_rejected(self):
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as temporary:
            for index in range(2):
                pq.write_table(self.table(), Path(temporary) / f"part-{index}.parquet")
            with self.assertRaisesRegex(observer.ProjectFailure, "parquet_customer_invalid"):
                observer.parquet_features(Path(temporary))

    def test_corrupt_parquet_is_project_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            (Path(temporary) / "part.parquet").write_bytes(b"not parquet")
            with self.assertRaisesRegex(observer.ProjectFailure, "parquet_invalid"):
                observer.parquet_features(Path(temporary))


if __name__ == "__main__":
    unittest.main()
