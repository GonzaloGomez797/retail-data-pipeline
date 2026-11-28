"""Consulta métricas del almacén de pedidos por CLI o con un modelo local."""

import argparse
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from textwrap import dedent
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import duckdb


QUERIES = {
    "top_category": """
        SELECT category, delivered_sales_pen
        FROM monthly_category_sales
        WHERE month = ?
        QUALIFY DENSE_RANK() OVER (ORDER BY delivered_sales_pen DESC) = 1
        ORDER BY category
    """,
    "monthly_sales": """
        SELECT delivered_sales_pen, delivered_orders
        FROM monthly_kpis
        WHERE month = ?
    """,
    "on_time": """
        SELECT on_time_pct, delivered_orders
        FROM monthly_kpis
        WHERE month = ?
    """,
}
SCHEMA = {
    "type": "object",
    "properties": {
        "metric": {"type": "string", "enum": [*QUERIES, "unsupported"]},
        "month": {"type": ["string", "null"]},
    },
    "required": ["metric", "month"],
    "additionalProperties": False,
}
SYSTEM_PROMPT = (
    "Selecciona exactamente una métrica: top_category para la categoría líder "
    "por importe de ventas entregadas, monthly_sales para el importe total "
    "de ventas entregadas y on_time para el porcentaje de entregas puntuales. "
    "Si hay filtros adicionales, se pide otra métrica o varios períodos, "
    "usa unsupported. Escribe month como AAAA-MM. Para fechas relativas usa "
    "la fecha de referencia indicada. Si no se indica mes usa null. "
    "Si el período es ambiguo usa unsupported. No escribas SQL ni calcules valores."
)


@dataclass(frozen=True)
class AuditResult:
    answer: str
    sql: str
    month: date


def parse_month(value: str | None) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}", value):
        raise ValueError("Indica un mes con el formato AAAA-MM, por ejemplo 2025-03.")
    try:
        return date.fromisoformat(f"{value}-01")
    except ValueError as error:
        raise ValueError(f"Mes inválido: {value}.") from error


def classify_question(question: str, model: str) -> tuple[str, date | None]:
    if not question.strip() or len(question) > 500:
        raise ValueError("Escribe una pregunta de entre 1 y 500 caracteres.")
    payload = {
        "model": model,
        "stream": False,
        "format": SCHEMA,
        "options": {"temperature": 0},
        "messages": [
            {"role": "system", "content": f"{SYSTEM_PROMPT} Fecha de referencia: {date.today()}."},
            {"role": "user", "content": question},
        ],
    }
    request = Request(
        "http://127.0.0.1:11434/api/chat",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=90) as response:
            result = json.load(response)
        selection = json.loads(result["message"]["content"])
    except HTTPError as error:
        raise ValueError(f"Ollama devolvió HTTP {error.code}. Revisa el modelo con ollama list.") from error
    except (URLError, TimeoutError) as error:
        raise ValueError("No pude conectar con Ollama local. Verifica que esté ejecutándose.") from error
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError("Ollama devolvió una respuesta con formato inesperado.") from error

    if not isinstance(selection, dict) or selection.keys() != {"metric", "month"}:
        raise ValueError("El modelo devolvió una selección de métrica inválida.")
    metric = selection["metric"]
    if metric == "unsupported":
        raise ValueError("Pregunta por ventas mensuales, categoría líder o puntualidad en un mes.")
    if not isinstance(metric, str) or metric not in QUERIES:
        raise ValueError("El modelo devolvió una métrica desconocida.")
    return metric, parse_month(selection["month"])


def query_warehouse(database: Path, metric: str, month: date | None) -> AuditResult:
    if metric not in QUERIES:
        raise ValueError("Métrica desconocida.")
    if not database.is_file():
        raise ValueError(f"No existe {database}. Ejecuta primero python pipeline.py.")
    sql = dedent(QUERIES[metric]).strip()
    selected_month = month
    with duckdb.connect(str(database.resolve()), read_only=True) as connection:
        if selected_month is None:
            selected_month = connection.execute("SELECT MAX(month) FROM monthly_kpis").fetchone()[0]
        if selected_month is None:
            raise ValueError("El almacén no contiene meses con pedidos.")
        records = connection.execute(sql, [selected_month]).fetchall()

    period = selected_month.strftime("%Y-%m")
    if not records:
        subject = "ventas entregadas por categoría" if metric == "top_category" else "pedidos"
        answer = f"{period} · Sin {subject} en los datos de la demo."
    elif metric == "top_category":
        leaders = ", ".join(category for category, _ in records)
        sales = records[0][1]
        if len(records) == 1:
            answer = f"{period} · Categoría líder: {leaders} con S/ {sales:,.2f} en ventas entregadas."
        else:
            answer = f"{period} · Empate entre {leaders}: S/ {sales:,.2f} por categoría."
    elif metric == "monthly_sales":
        sales, delivered = records[0]
        answer = f"{period} · Ventas entregadas: S/ {sales:,.2f} ({delivered} pedidos entregados)."
    else:
        percent, delivered = records[0]
        if percent is None:
            answer = f"{period} · Sin pedidos entregados para calcular la puntualidad."
        else:
            answer = f"{period} · Entregas puntuales: {percent:.2f}% sobre {delivered} pedidos entregados."
    return AuditResult(answer, sql, selected_month)


def main() -> None:
    parser = argparse.ArgumentParser(description="Asistente de ventas del Retail Data Pipeline")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--demo", choices=QUERIES, help="elige una métrica sin usar un modelo")
    mode.add_argument("--question", help="pregunta en lenguaje natural mediante Ollama local")
    parser.add_argument("--month", help="mes AAAA-MM para --demo (por defecto, último con pedidos)")
    parser.add_argument("--model", help="modelo local de Ollama para --question")
    parser.add_argument("--db", type=Path, default=Path(".data/retail.duckdb"))
    args = parser.parse_args()

    if args.question is not None and (not args.model or args.month is not None):
        parser.error("--question requiere --model y el mes debe ir en la pregunta")
    if args.demo is not None and args.model is not None:
        parser.error("--model se usa con --question")
    try:
        if args.demo is not None:
            metric, month = args.demo, parse_month(args.month)
            source = "Modo demo · métrica seleccionada"
        else:
            metric, month = classify_question(args.question, args.model)
            source = f"Modo IA local · Ollama ({args.model})"
        result = query_warehouse(args.db, metric, month)
    except (ValueError, duckdb.Error) as error:
        parser.exit(1, f"Error: {error}\n")

    print(source)
    print(result.answer)
    if month is None:
        print("Período: último mes con pedidos.")
    print(f"\nConsulta ejecutada (parámetro ?: {result.month}):\n{result.sql}")


if __name__ == "__main__":
    main()
