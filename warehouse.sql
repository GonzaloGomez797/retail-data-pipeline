CREATE TABLE assessed_orders AS
WITH typed AS (
    SELECT
        raw_orders AS source_row,
        TRY_CAST(order_id AS BIGINT) AS order_id,
        TRY_CAST(ordered_on AS DATE) AS ordered_on,
        NULLIF(TRIM(category), '') AS category,
        TRY_CAST(quantity AS INTEGER) AS quantity,
        TRY_CAST(unit_price_pen AS DECIMAL(12, 2)) AS unit_price_pen,
        status,
        TRY_CAST(promised_on AS DATE) AS promised_on,
        TRY_CAST(delivered_on AS DATE) AS delivered_on
    FROM raw_orders
),
validated AS (
    SELECT *,
        CASE
            WHEN order_id IS NULL OR order_id <= 0 THEN 'invalid_id'
            WHEN category IS NULL THEN 'missing_category'
            WHEN quantity IS NULL OR quantity <= 0 THEN 'invalid_quantity'
            WHEN unit_price_pen IS NULL OR unit_price_pen <= 0 THEN 'invalid_price'
            WHEN status IS NULL OR status NOT IN ('delivered', 'pending', 'cancelled')
                THEN 'invalid_status'
            WHEN ordered_on IS NULL OR promised_on IS NULL OR promised_on < ordered_on
                THEN 'invalid_order_dates'
            WHEN status = 'delivered' AND (delivered_on IS NULL OR delivered_on < ordered_on)
                THEN 'invalid_delivery_date'
            WHEN status <> 'delivered' AND source_row.delivered_on IS NOT NULL
                THEN 'unexpected_delivery_date'
        END AS validation_error
    FROM typed
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY validation_error NULLS FIRST, ordered_on, category,
            quantity, unit_price_pen, status, promised_on, delivered_on
    ) AS order_rank
    FROM validated
)
SELECT * EXCLUDE (validation_error, order_rank),
    COALESCE(validation_error, CASE WHEN order_rank > 1 THEN 'duplicate_id' END) AS reason
FROM ranked;

CREATE TABLE fact_orders AS
SELECT * EXCLUDE (source_row, reason),
    quantity * unit_price_pen AS order_value_pen
FROM assessed_orders
WHERE reason IS NULL;

CREATE VIEW rejected_orders AS
SELECT source_row.*, reason
FROM assessed_orders
WHERE reason IS NOT NULL;

CREATE VIEW quality_summary AS
SELECT COALESCE(reason, 'accepted') AS result, COUNT(*) AS rows
FROM assessed_orders
GROUP BY 1;

CREATE VIEW monthly_kpis AS
SELECT
    CAST(DATE_TRUNC('month', ordered_on) AS DATE) AS month,
    COUNT(*) AS orders,
    COUNT(*) FILTER (WHERE status = 'delivered') AS delivered_orders,
    COALESCE(SUM(order_value_pen) FILTER (WHERE status = 'delivered'), 0)
        AS delivered_sales_pen,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'delivered' AND delivered_on <= promised_on)
        / NULLIF(COUNT(*) FILTER (WHERE status = 'delivered'), 0), 2) AS on_time_pct
FROM fact_orders
GROUP BY 1;

CREATE VIEW monthly_category_sales AS
SELECT CAST(DATE_TRUNC('month', ordered_on) AS DATE) AS month,
    category, COUNT(*) AS delivered_orders, SUM(order_value_pen) AS delivered_sales_pen
FROM fact_orders
WHERE status = 'delivered'
GROUP BY 1, 2;
