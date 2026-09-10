CREATE OR REFRESH MATERIALIZED VIEW farmia_gold.customer_360
AS WITH orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(order_total_eur) AS total_spent_eur,
        AVG(order_total_eur) AS avg_order_value_eur,
        MAX(op_ts) AS last_order_ts
    FROM farmia_silver.orders_curated
    WHERE __END_AT IS NULL
    GROUP BY customer_id
),
events AS (
    SELECT
        customer_id,
        COUNT(*) AS total_events,
        MAX(event_ts) AS last_event_ts
    FROM farmia_silver.events_curated
    GROUP BY customer_id
)

SELECT
    COALESCE(o.customer_id, e.customer_id) AS customer_id,
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.total_spent_eur, 0) AS total_spent_eur,
    COALESCE(o.avg_order_value_eur, 0) AS avg_order_value_eur,
    o.last_order_ts,
    COALESCE(e.total_events, 0) AS total_events,
    e.last_event_ts
FROM orders o
FULL OUTER JOIN events e
    ON o.customer_id = e.customer_id;