import csv
import tempfile
import unittest
from pathlib import Path
from check import expected_features, require_equal, parquet_features
from decimal import Decimal
import pyarrow as pa
import pyarrow.parquet as pq


class FeatureChecks(unittest.TestCase):
    def test_order_is_not_item_and_missing_text_is_allowed(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "input.csv"
            with path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["invoice_id", "customer_id", "quantity", "unit_price", "description"])
                writer.writerows([["A", "U", 2, 3, "X"], ["A", "U", 1, 4, "Y"],
                                  ["B", "U", 1, 3, ""], ["cA", "U", -1, 3, "X"],
                                  ["D", "", 1, 5, "Z"], ["E", "V", 1, 0, "Z"]])
            self.assertEqual(expected_features(path), {"U": {"order_count": 2,
                             "total_amount": "13.000000", "average_order_amount": "6.50"}})

    def test_rejects_missing_extra_and_wrong_values(self):
        expected = {"U": {"order_count": 2}}
        for actual in [{}, {"U": {"order_count": 2}, "V": {}}, {"U": {"order_count": 3}}]:
            with self.assertRaises(ValueError):
                require_equal(actual, expected, "fixture")

    def test_parquet_accepts_nonnullable_count_and_rejects_duplicate(self):
        schema = pa.schema([pa.field("customer_id", pa.string()),
                            pa.field("order_count", pa.int64(), nullable=False),
                            pa.field("total_amount", pa.decimal128(20, 6)),
                            pa.field("average_order_amount", pa.decimal128(20, 2))])
        row = {"customer_id": "U", "order_count": 2, "total_amount": Decimal("13.000000"),
               "average_order_amount": Decimal("6.50")}
        with tempfile.TemporaryDirectory() as tmp:
            pq.write_table(pa.Table.from_pylist([row], schema=schema), Path(tmp) / "part.parquet")
            self.assertEqual(parquet_features(tmp)["U"]["order_count"], 2)
            pq.write_table(pa.Table.from_pylist([row, row], schema=schema), Path(tmp) / "part.parquet")
            with self.assertRaisesRegex(ValueError, "повтор клиента"):
                parquet_features(tmp)

    def test_parquet_rejects_float_money(self):
        with tempfile.TemporaryDirectory() as tmp:
            pq.write_table(pa.table({"customer_id": ["U"], "order_count": [2],
                                     "total_amount": [13.0], "average_order_amount": [6.5]}),
                           Path(tmp) / "part.parquet")
            with self.assertRaisesRegex(ValueError, "типы"):
                parquet_features(tmp)

    def test_bankers_rounding(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "input.csv"
            path.write_text("invoice_id,customer_id,quantity,unit_price\nA,U,1,1.005\n")
            self.assertEqual(expected_features(path)["U"]["average_order_amount"], "1.00")


if __name__ == "__main__":
    unittest.main()
