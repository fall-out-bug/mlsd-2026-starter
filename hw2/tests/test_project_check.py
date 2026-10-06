"""Behavioral tests for the supplied checker, not a student Spark solution."""

import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import types
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

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
        services[name]["build"] = {
            "context": str(ROOT),
            "dockerfile": "airflow/Dockerfile",
        }
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
            with (
                self.subTest(mutation=mutation),
                self.assertRaises(controller.InvalidProject),
            ):
                controller.validate_config(value, ROOT)

    def test_global_container_name_and_host_access_rejected(self):
        for setting in ("container_name", "privileged", "network_mode", "pid"):
            value = config()
            value["services"]["redis"][setting] = "host"
            with (
                self.subTest(setting=setting),
                self.assertRaises(controller.InvalidProject),
            ):
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
            self.assertFalse(
                controller.cleanup(["docker", "compose", "-p", "owned"], "owned")
            )
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
            self.assertFalse(
                controller.cleanup(["docker", "compose", "-p", "owned"], "owned")
            )
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
            self.assertFalse(
                controller.cleanup(["docker", "compose", "-p", "owned"], "owned")
            )


class RuntimeVersionTests(unittest.TestCase):
    @staticmethod
    def probe(argv, **kwargs):
        if "java" in argv:
            return subprocess.CompletedProcess(
                argv, 0, b"", b'openjdk version "17.0.12"'
            )
        version = "2.10.5" if argv[3].startswith("airflow-") else "3.5.4"
        return subprocess.CompletedProcess(
            argv, 0, json.dumps([[3, 12], version]).encode(), b""
        )

    def test_pinned_runtimes_are_accepted(self):
        self.assertTrue(controller.runtime_versions_match(["docker"], self.probe))

    def test_wrong_python_airflow_spark_and_java_are_rejected(self):
        for wrong in ("python", "airflow", "spark", "java"):

            def probe(argv, wrong=wrong, **kwargs):
                result = self.probe(argv, **kwargs)
                service = argv[3]
                if wrong == "java" and "java" in argv:
                    result.stderr = b'openjdk version "21.0.1"'
                elif "java" not in argv:
                    value = json.loads(result.stdout)
                    if wrong == "python":
                        value[0] = [3, 11]
                    elif wrong == "airflow" and service.startswith("airflow-"):
                        value[1] = "2.9.0"
                    elif wrong == "spark" and service.startswith("spark-"):
                        value[1] = "3.5.3"
                    result.stdout = json.dumps(value).encode()
                return result

            with self.subTest(wrong=wrong):
                self.assertFalse(controller.runtime_versions_match(["docker"], probe))

    def test_local_main_rejects_versions_before_observing_successful_data(self):
        with tempfile.TemporaryDirectory() as temp:
            project = Path(temp)
            (project / "compose.yaml").write_text("services: {}")

            def command(argv, **kwargs):
                if "config" in argv:
                    return subprocess.CompletedProcess(
                        argv, 0, json.dumps(config()), ""
                    )
                return subprocess.CompletedProcess(argv, 0, "", "")

            with (
                patch.object(sys, "argv", ["check.py", "--project", temp]),
                patch.object(controller, "__file__", str(project / "check.py")),
                patch.object(controller, "validate_config"),
                patch.object(controller, "command", side_effect=command) as called,
                patch.object(controller, "runtime_versions_match", return_value=False),
                patch.object(controller, "cleanup", return_value=True),
                patch("sys.stdout", new_callable=io.StringIO),
            ):
                self.assertEqual(controller.main(), 1)
            self.assertFalse(
                any("build" in call.args[0] for call in called.call_args_list)
            )


class DiagnosticOutputTests(unittest.TestCase):
    def run_failed_observation(self, diagnostic):
        with tempfile.TemporaryDirectory() as temp:
            project = Path(temp)
            (project / "compose.yaml").write_text("services: {}")
            receipt = {"status": "FAILED", "diagnostic": diagnostic}

            def command(argv, **kwargs):
                if "config" in argv:
                    return subprocess.CompletedProcess(
                        argv, 0, json.dumps(config()), ""
                    )
                if argv[:2] == ["docker", "build"]:
                    Path(argv[argv.index("--iidfile") + 1]).write_text(
                        "sha256:" + "f" * 64
                    )
                if argv[:2] == ["docker", "run"]:
                    return subprocess.CompletedProcess(
                        argv,
                        1,
                        json.dumps(receipt),
                        "private exception text from process",
                    )
                return subprocess.CompletedProcess(argv, 0, "", "")

            with (
                patch.object(sys, "argv", ["check.py", "--project", temp]),
                patch.object(controller, "__file__", str(project / "check.py")),
                patch.object(controller, "validate_config"),
                patch.object(controller, "command", side_effect=command),
                patch.object(controller, "runtime_versions_match", return_value=True),
                patch.object(controller, "cleanup", return_value=True),
                patch("sys.stdout", new_callable=io.StringIO) as output,
                patch("sys.stderr", new_callable=io.StringIO) as errors,
            ):
                self.assertEqual(controller.main(), 1)
            self.assertEqual(errors.getvalue(), "")
            self.assertNotIn("private exception text", output.getvalue())
            return output.getvalue()

    def test_wrong_s3_sum_prints_russian_explanation(self):
        output = self.run_failed_observation("s3_parquet_mismatch")
        self.assertIn(
            "FAIL: Клиенты или суммы в Parquet отличаются от ожидаемых", output
        )
        self.assertNotIn("observer_failed", output)

    def test_malformed_diagnostics_are_generic_and_do_not_leak_text(self):
        for diagnostic in (
            "private exception text",
            "private\nexception",
            "private://exception",
            "a" * 129,
            None,
        ):
            with self.subTest(diagnostic=diagnostic):
                output = self.run_failed_observation(diagnostic)
                self.assertIn("FAIL: observer_failed", output)
                if isinstance(diagnostic, str):
                    self.assertNotIn(diagnostic, output)


class PaginatedStorageTests(unittest.TestCase):
    def inspect(self, pages, *, expected_size=129):
        expected = {
            "x": {
                "order_count": 1,
                "total_amount": "1.000000",
                "average_order_amount": "1.00",
            }
        }
        client = Mock()
        client.get_object.side_effect = lambda **kw: {
            "Body": io.BytesIO(b"input" if kw["Key"] == "input/sample.csv" else b"part")
        }
        client.get_paginator.return_value.paginate.return_value = iter(pages)
        store = Mock()
        store.scan_iter.return_value = ["hw2:customer:x"]
        store.get.return_value = json.dumps(expected["x"])
        redis = types.ModuleType("redis")
        redis.Redis = Mock(return_value=store)
        redis.exceptions = types.SimpleNamespace(
            ResponseError=type("ResponseError", (Exception,), {})
        )
        botocore = types.ModuleType("botocore")
        errors = types.ModuleType("botocore.exceptions")
        errors.ClientError = type("ClientError", (Exception,), {})

        def parquet(directory):
            self.assertEqual(len(list(directory.iterdir())), expected_size)
            return expected

        with (
            patch.dict(
                sys.modules,
                {"redis": redis, "botocore": botocore, "botocore.exceptions": errors},
            ),
            patch.object(observer, "storage_client", return_value=client),
            patch.object(observer, "parquet_features", side_effect=parquet),
        ):
            result = observer.inspect_outputs(b"input", expected)
        self.assertEqual(result["s3_parquet"], expected)
        self.assertEqual(result["redis"], expected)
        client.get_paginator.assert_called_once_with("list_objects_v2")

    def test_more_than_128_parts_are_read_across_pages(self):
        self.inspect(
            [
                {
                    "Contents": [
                        {"Key": f"features/{i}.parquet", "Size": 4} for i in range(128)
                    ],
                    "IsTruncated": True,
                },
                {
                    "Contents": [
                        {"Key": "features/128.parquet", "Size": 4},
                        {"Key": "features/_SUCCESS", "Size": 0},
                    ]
                },
            ]
        )

    def test_total_size_limit_still_applies_across_pages(self):
        pages = [
            {"Contents": [{"Key": "features/first.parquet", "Size": 4}]},
            {
                "Contents": [
                    {
                        "Key": "features/second.parquet",
                        "Size": observer.MAX_PARQUET_BYTES,
                    }
                ]
            },
        ]
        with self.assertRaisesRegex(
            observer.ProjectFailure, "parquet_total_size_exceeded"
        ):
            self.inspect(pages)


class OutputTests(unittest.TestCase):
    def test_oracle_filters_counts_distinct_orders_and_keeps_duplicate_lines(self):
        raw = (
            b"customer_id,invoice_id,quantity,unit_price\nx,one,1,1.005\nx,one,1,1.005\n"
            b"x,two,1,0.005\nx,cancel,1,999\nx,C123,1,999\nx,no,-1,999\n"
            b"x,free,1,0\n,anon,1,999\n"
        )
        self.assertEqual(
            observer.expected_features(raw),
            {
                "x": {
                    "order_count": 2,
                    "total_amount": "2.015000",
                    "average_order_amount": "1.01",
                }
            },
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
        for value in (
            graph()[1:],
            None,
            [{"task_id": "get_dataset"}],
            graph() + [graph()[0]],
        ):
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
            self.assertRaisesRegex(
                observer.ProjectFailure, "spark_application_not_observed"
            ),
        ):
            observer.run_dag()

    def test_old_spark_application_cannot_replace_a_new_one(self):
        old = {
            "app-old": {"id": "app-old", "name": observer.DAG_ID, "state": "FINISHED"}
        }
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
            self.assertRaisesRegex(
                observer.ProjectFailure, "spark_application_not_observed"
            ),
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
            self.assertRaisesRegex(
                observer.ProjectFailure, "required_task_not_successful"
            ),
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
        new = {
            "app-new": {"id": "app-new", "name": observer.DAG_ID, "state": "FINISHED"}
        }
        with (
            patch.object(observer, "airflow_api", side_effect=responses),
            patch.object(observer, "spark_apps", side_effect=[{}, new]),
        ):
            value = observer.run_dag()
        self.assertEqual(value["spark_application_id"], "app-new")
        self.assertEqual(value["tasks"], dict.fromkeys(observer.TASK_IDS, "success"))

    def test_wrong_customer_or_amount_rejected(self):
        expected = {
            "x": {
                "order_count": 1,
                "total_amount": "1.000000",
                "average_order_amount": "1.00",
            }
        }
        for value in (
            {},
            dict(expected, y=expected["x"]),
            {"x": dict(expected["x"], total_amount="2.000000")},
        ):
            with self.subTest(value=value), self.assertRaises(observer.ProjectFailure):
                observer.require_equal(value, expected, "redis")


@unittest.skipUnless(
    importlib.util.find_spec("pyarrow"), "observer container provides pyarrow"
)
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

        table = self.table().set_column(
            1, "order_count", pa.array([1], type=pa.int32())
        )
        with tempfile.TemporaryDirectory() as temporary:
            pq.write_table(table, Path(temporary) / "part.parquet")
            with self.assertRaisesRegex(
                observer.ProjectFailure, "parquet_schema_invalid"
            ):
                observer.parquet_features(Path(temporary))

    def test_duplicate_customer_in_different_files_rejected(self):
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as temporary:
            for index in range(2):
                pq.write_table(self.table(), Path(temporary) / f"part-{index}.parquet")
            with self.assertRaisesRegex(
                observer.ProjectFailure, "parquet_customer_invalid"
            ):
                observer.parquet_features(Path(temporary))

    def test_corrupt_parquet_is_project_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            (Path(temporary) / "part.parquet").write_bytes(b"not parquet")
            with self.assertRaisesRegex(observer.ProjectFailure, "parquet_invalid"):
                observer.parquet_features(Path(temporary))


class SuppliedInputTests(unittest.TestCase):
    def test_observer_uses_supplied_csv_with_a_new_customer(self):
        raw = (
            b"invoice_id,stock_code,description,quantity,invoice_at,unit_price,customer_id,country\n"
            b"N,P,,2,2026-01-01T00:00:00,3.00,999999999,RU\n"
        )
        expected = {
            "999999999": {
                "order_count": 1,
                "total_amount": "6.000000",
                "average_order_amount": "6.00",
            }
        }
        self.assertNotIn("999999999", observer.expected_features((ROOT / "data/sample.csv").read_bytes()))

        def outputs(supplied, calculated):
            self.assertEqual(supplied, raw)
            self.assertEqual(calculated, expected)
            return {"s3_parquet": expected, "redis": expected}

        with (
            patch.object(Path, "read_bytes", return_value=raw),
            patch.object(observer, "airflow_api", return_value={"tasks": graph()}),
            patch.object(observer, "seed_stale_outputs"),
            patch.object(observer, "run_dag", side_effect=[
                {"spark_application_id": "app-1"},
                {"spark_application_id": "app-2"},
            ]),
            patch.object(observer, "inspect_outputs", side_effect=outputs) as inspect,
            patch("sys.stdout", new_callable=io.StringIO) as output,
        ):
            self.assertEqual(observer.main(), 0)
            receipt = json.loads(output.getvalue())
        self.assertEqual(receipt["status"], "PASSED")
        self.assertEqual(receipt["customers"], 1)
        self.assertEqual(inspect.call_count, 2)


if __name__ == "__main__":
    unittest.main()
