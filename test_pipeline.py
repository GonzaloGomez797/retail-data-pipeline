import csv
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from pipeline import FIELDS, build_warehouse, generate_orders


class RetailPipelineTest(unittest.TestCase):
    def test_generated_orders_reconcile_and_rebuild(self):
        with TemporaryDirectory() as directory:
            source = Path(directory) / "orders.csv"
            database = source.with_name("retail.duckdb")
            generate_orders(source, count=100, seed=42)
            original = source.read_bytes()
            generate_orders(source, count=100, seed=42)
            self.assertEqual(source.read_bytes(), original)

            for _ in range(2):
                build_warehouse(source, database)
                with duckdb.connect(str(database), read_only=True) as connection:
                    self.assertEqual(
                        dict(connection.execute("SELECT * FROM quality_summary").fetchall()),
                        {"accepted": 100, "duplicate_id": 5, "invalid_quantity": 4},
                    )
                    raw_count = connection.execute("SELECT COUNT(*) FROM raw_orders").fetchone()
                    self.assertEqual(raw_count, (109,))

    def test_monthly_kpis_use_delivered_sales_and_deadline(self):
        with TemporaryDirectory() as directory:
            source = Path(directory) / "orders.csv"
            database = source.with_name("retail.duckdb")
            with source.open("w", encoding="utf-8", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(FIELDS)
                writer.writerows([
                    (1, "2025-01-28", "Hogar", 2, "10.50", "delivered", "2025-02-01", "2025-02-01"),
                    (2, "2025-01-29", "Moda", 1, "30.00", "delivered", "2025-02-02", "2025-02-03"),
                    (3, "2025-01-30", "Hogar", 1, "80.00", "cancelled", "2025-02-03", ""),
                    (4, "2025-02-01", "Hogar", 1, "80.00", "pending", "2025-02-03", ""),
                ])

            build_warehouse(source, database)
            with duckdb.connect(str(database), read_only=True) as connection:
                rows = connection.execute(
                    """SELECT orders, delivered_orders, delivered_sales_pen, on_time_pct
                       FROM monthly_kpis ORDER BY month"""
                ).fetchall()
                self.assertEqual(rows, [(3, 2, Decimal("51.00"), 50.0),
                                        (1, 0, Decimal("0.00"), None)])


if __name__ == "__main__":
    unittest.main()
