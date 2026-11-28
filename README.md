# Portafolio de Ingeniería de Datos

Soy Gonzalo Gómez, Data Engineer. Este portafolio muestra un flujo de datos completo, desde la ingesta y los controles de calidad hasta las métricas que puede consultar un asistente de ventas. También incluye dos prácticas de streaming.

| Proyecto | Qué puedes revisar | Herramientas |
| --- | --- | --- |
| [Retail Data Pipeline](#retail-data-pipeline) | Generación de pedidos sintéticos, validación, deduplicación y métricas de ventas y entregas. | Python, DuckDB, SQL |
| [Asistente de ventas](#asistente-de-ventas-con-ia-local) | Preguntas en español sobre ventas, categorías y puntualidad. Cada resultado muestra su consulta y período. | Python, DuckDB, Ollama |
| [Sesión 11 · Kafka con Redpanda](redpanda-python/README.md#clase-11--redpanda-con-python) | Creación de un topic, publicación de mensajes y lectura por partición y offset. | Python, Kafka, Redpanda |
| [Sesión 12 · Streaming de vuelos](redpanda-python/README.md#sesión-12--aviationstack--redpanda) | Consulta de vuelos en Aviationstack y publicación en Redpanda, verificada desde Colab. Incluye consumo hacia MySQL, Parquet en S3 y una tabla Bronze en Databricks. | Aviationstack, Python, Redpanda, MySQL, Spark, Databricks, Delta Lake, S3 |

## Retail Data Pipeline

Pipeline de ingeniería de datos para un e-commerce ficticio. Genera pedidos sintéticos, registra los motivos de rechazo y calcula ventas y puntualidad de entregas con Python y DuckDB SQL. Todos los montos se expresan en soles peruanos (PEN).

```mermaid
flowchart LR
    A[Generador Python] --> B[CSV de pedidos]
    B --> C[DuckDB · raw_orders]
    C --> D[Validación y deduplicación SQL]
    D --> E[fact_orders]
    D --> F[rejected_orders]
    E --> G[Ventas y entregas]
```

### Ejecutar en Windows

Desde la raíz del repositorio, con Python 3.12 disponible en PowerShell:

```powershell
python -m venv "$env:LOCALAPPDATA/retail-pipeline-venv"
$retailPython = "$env:LOCALAPPDATA/retail-pipeline-venv/Scripts/python.exe"
& $retailPython -m pip install -r requirements.txt
& $retailPython pipeline.py --orders 1000 --seed 42 --output "$env:LOCALAPPDATA/retail-pipeline-data"
& $retailPython -m unittest -v
```

La ubicación de salida del ejemplo mantiene el CSV y la base fuera de OneDrive. Si omites `--output`, la salida se guarda en `.data`, excluida de Git. Usa una carpeta de salida distinta para cada ejecución simultánea.

### Datos y resultados

El grano es un registro por pedido válido. El generador cubre 180 días desde el 1 de enero de 2025 y recrea los mismos pedidos con la misma semilla. Cada 20 pedidos incorpora una copia exacta y cada 25 introduce un registro adicional sin cantidad. Con 1.000 pedidos, el resultado esperado es 1.090 filas de entrada, 1.000 aceptadas, 50 duplicadas y 40 rechazadas por cantidad inválida.

| Objeto | Contenido |
| --- | --- |
| `raw_orders` | Filas originales del CSV |
| `fact_orders` | Pedidos válidos con identificador único y monto calculado |
| `rejected_orders` | Filas originales rechazadas y su primer motivo de rechazo |
| `quality_summary` | Conteo por motivo o aceptación |
| `monthly_kpis` | Pedidos, ventas entregadas y porcentaje de entregas puntuales |
| `monthly_category_sales` | Ventas entregadas por mes y categoría |

El mes corresponde a la fecha del pedido. Las ventas se calculan sobre pedidos entregados. Una entrega es puntual si ocurre en la fecha prometida o antes. Cuando el mes no tiene entregas, su porcentaje de puntualidad es nulo.

Puedes abrir `retail.duckdb` con DuckDB y ejecutar estas consultas:

```sql
SELECT * FROM quality_summary ORDER BY result;
SELECT * FROM monthly_kpis ORDER BY month;
SELECT * FROM monthly_category_sales ORDER BY month, delivered_sales_pen DESC;
```

GitHub Actions ejecuta las pruebas de reconciliación, repetibilidad y cálculo de métricas en cada push y pull request.

## Asistente de ventas con IA local

**Pregunta de negocio** — ¿Qué categoría vendió más en marzo de 2025?

Con los pedidos sintéticos generados por el pipeline (1.000 pedidos y semilla 42), el asistente devuelve **Hogar, con S/ 34,989.50 en ventas entregadas**. La respuesta incluye el período, la consulta ejecutada y el valor del parámetro.

```mermaid
flowchart LR
    Q[Pregunta en español] --> M[Ollama local]
    M --> V[Validación de métrica y mes]
    D[Selección de demo] --> V
    V --> S[Consulta SQL parametrizada]
    S --> W[DuckDB de solo lectura]
    W --> R[Respuesta con SQL y período]
```

### Ejecutar la demo

Desde la raíz, con las dependencias de `requirements.txt` instaladas:

```powershell
python pipeline.py --orders 1000 --seed 42
python retail_agent.py --demo top_category --month 2025-03
python retail_agent.py --demo on_time --month 2025-03
```

Usa `--db RUTA/retail.duckdb` si generaste los datos con una carpeta de salida distinta. El modo demo funciona sin modelo ni cuenta externa.

### Hacer una pregunta con IA

Con Ollama ejecutándose localmente y un modelo compatible con salidas estructuradas, por ejemplo `qwen2.5:3b`:

```powershell
ollama pull qwen2.5:3b
python retail_agent.py --question "¿Qué categoría vendió más en marzo de 2025?" --model qwen2.5:3b
```

Ollama recibe la pregunta y selecciona una métrica y un mes. Python valida ambos campos y ejecuta una de tres consultas predefinidas sobre las vistas del pipeline. Las cifras salen de DuckDB. La base se abre en solo lectura y el modelo carece de acceso a la base y a los registros.

El asistente cubre ventas entregadas, categoría líder y puntualidad para un mes. Si omites el período, muestra el último mes con pedidos. Los pedidos de la demo corresponden a enero–junio de 2025. Se informan períodos sin datos y empates entre categorías.

Para verificarlo, ejecuta `python -m unittest -v`. Las pruebas comparan los importes y la puntualidad con `fact_orders`, comprueban que la base permanezca intacta y validan la selección que devuelve Ollama.
