import argparse
import csv
import random
import sys
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb


FIELDS = (
    "order_id", "ordered_on", "category", "quantity", "unit_price_pen",
    "status", "promised_on", "delivered_on",
)
SQL_PATH = Path(__file__).resolve().parent / "warehouse.sql"


def generate_orders(path: Path, count: int, seed: int) -> None:
    rng = random.Random(seed)
    first_day = date(2025, 1, 1)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(FIELDS)
        for order_id in range(1, count + 1):
            ordered = first_day + timedelta(days=rng.randrange(180))
            promised = ordered + timedelta(days=rng.randint(2, 7))
            status = rng.choices(
                ("delivered", "pending", "cancelled"), weights=(85, 10, 5)
            )[0]
            delivered = ""
            if status == "delivered":
                delivered = promised + timedelta(days=rng.randint(-2, 4))
            category = rng.choice(("Hogar", "Tecnología", "Moda"))
            price = f"{rng.randint(15, 400)}.{rng.randrange(100):02d}"
            row = (order_id, ordered, category, rng.randint(1, 5), price,
                   status, promised, delivered)
            writer.writerow(row)
            if order_id % 20 == 0:
                writer.writerow(row)
            if order_id % 25 == 0:
                writer.writerow((count + order_id, ordered, category, "",
                                 price, status, promised, delivered))


def build_warehouse(source: Path, destination: Path) -> None:
    # Build in the destination directory so the final replacement stays atomic.
    with TemporaryDirectory(dir=destination.parent) as staging:
        database = Path(staging) / destination.name
        with duckdb.connect(str(database)) as connection:
            connection.execute(
                """CREATE TABLE raw_orders AS
                   SELECT * FROM read_csv(?, header=true, all_varchar=true,
                                            delim=',')""",
                [str(source)],
            )
            connection.execute(SQL_PATH.read_text(encoding="utf-8"))
        database.replace(destination)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline local de pedidos")
    parser.add_argument("--orders", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path(".data"))
    args = parser.parse_args()
    if args.orders < 1:
        parser.error("--orders debe ser mayor que cero")

    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    warehouse = output / "retail.duckdb"
    with TemporaryDirectory(dir=output) as staging:
        source = Path(staging) / "orders.csv"
        generate_orders(source, args.orders, args.seed)
        build_warehouse(source, warehouse)
        source.replace(output / source.name)

    with duckdb.connect(str(warehouse), read_only=True) as connection:
        writer = csv.writer(sys.stdout, lineterminator="\n")
        for report, query in (
            ("Control de calidad", "SELECT * FROM quality_summary ORDER BY result"),
            ("Indicadores mensuales", "SELECT * FROM monthly_kpis ORDER BY month"),
        ):
            results = connection.sql(query)
            print(report)
            writer.writerow(results.columns)
            writer.writerows(results.fetchall())
            print()
    print(f"Resultados en {output}")


if __name__ == "__main__":
    main()
