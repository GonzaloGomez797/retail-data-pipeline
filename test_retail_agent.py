import io
import json
import unittest
from datetime import date
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import duckdb

from pipeline import build_warehouse, generate_orders
from retail_agent import classify_question, parse_month, query_warehouse


class RetailAgentTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        directory = TemporaryDirectory()
        cls.addClassCleanup(directory.cleanup)
        source = Path(directory.name) / "orders.csv"
        cls.database = source.with_name("retail.duckdb")
        generate_orders(source, count=1000, seed=42)
        build_warehouse(source, cls.database)

    def test_metrics_match_the_pipeline_and_leave_the_database_unchanged(self):
        month = date(2025, 3, 1)
        before = sha256(self.database.read_bytes()).digest()
        with duckdb.connect(str(self.database), read_only=True) as connection:
            sales, count, on_time = connection.execute(
                """
                SELECT SUM(order_value_pen), COUNT(*),
                    ROUND(100.0 * COUNT(*) FILTER (WHERE delivered_on <= promised_on) / COUNT(*), 2)
                FROM fact_orders
                WHERE status = 'delivered' AND ordered_on >= ? AND ordered_on < DATE '2025-04-01'
                """,
                [month],
            ).fetchone()
        self.assertIn(f"S/ {sales:,.2f} ({count} pedidos", query_warehouse(self.database, "monthly_sales", month).answer)
        self.assertIn(f"{on_time:.2f}% sobre {count}", query_warehouse(self.database, "on_time", month).answer)
        result = query_warehouse(self.database, "top_category", month)
        self.assertIn("Categoría líder: Hogar con S/ 34,989.50", result.answer)
        self.assertIn("WHERE month = ?", result.sql)
        self.assertEqual(sha256(self.database.read_bytes()).digest(), before)

    def test_latest_month_and_unavailable_data(self):
        result = query_warehouse(self.database, "monthly_sales", None)
        self.assertEqual(result.month, date(2025, 6, 1))
        missing = query_warehouse(self.database, "on_time", date(2026, 11, 1))
        self.assertIn("Sin pedidos", missing.answer)

    def test_rejects_invalid_metrics_and_periods(self):
        with self.assertRaisesRegex(ValueError, "Métrica desconocida"):
            query_warehouse(self.database, "DROP TABLE fact_orders", None)
        for month in ("2025-13", "2025-3", "2025-03-01"):
            with self.subTest(month=month), self.assertRaises(ValueError):
                parse_month(month)

    @patch("retail_agent.urlopen")
    def test_local_model_only_supplies_the_metric_and_month(self, urlopen):
        urlopen.return_value = io.BytesIO(json.dumps({
            "message": {"content": json.dumps({"metric": "top_category", "month": "2025-03"})}
        }).encode())
        selection = classify_question("¿Qué categoría vendió más en marzo de 2025?", "local")
        self.assertEqual(selection, ("top_category", date(2025, 3, 1)))
        self.assertIn("Categoría líder: Hogar", query_warehouse(self.database, *selection).answer)
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "http://127.0.0.1:11434/api/chat")
        payload = json.loads(request.data)
        self.assertEqual(set(payload["format"]["properties"]), {"metric", "month"})
        self.assertFalse(payload["format"]["additionalProperties"])

    @patch("retail_agent.urlopen")
    def test_rejects_extra_fields_from_the_model(self, urlopen):
        urlopen.return_value = io.BytesIO(json.dumps({
            "message": {"content": json.dumps({
                "metric": "monthly_sales", "month": "2025-03", "sql": "DROP TABLE fact_orders"
            })}
        }).encode())
        with self.assertRaisesRegex(ValueError, "selección de métrica inválida"):
            classify_question("Cambia la base de datos", "local")


if __name__ == "__main__":
    unittest.main()
