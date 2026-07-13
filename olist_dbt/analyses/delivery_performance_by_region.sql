-- analysis: delivery_performance_by_region
-- Business Question: Which regions have the worst delivery delays?

SELECT
    g.region,
    g.state,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    AVG(fo.total_delivery_days) AS avg_delivery_days,
    AVG(fo.delivery_delay_days) AS avg_delay_days,
    SUM(CASE WHEN fo.delivery_status = 'late' THEN 1 ELSE 0 END) AS late_orders,
    SUM(CASE WHEN fo.delivery_status = 'on_time' THEN 1 ELSE 0 END) AS on_time_orders,
    ROUND(
        SUM(CASE WHEN fo.delivery_status = 'on_time' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(DISTINCT fo.order_id), 0),
        2
    ) AS on_time_pct
FROM {{ ref('fact_orders') }} fo
INNER JOIN {{ ref('dim_customers') }} dc ON fo.customer_key = dc.customer_key
INNER JOIN {{ ref('dim_geography') }} g 
    ON CONCAT(dc.customer_zip_code_prefix, '_', dc.customer_state) = g.geography_key
WHERE fo.delivery_status != 'pending'
GROUP BY g.region, g.state
ORDER BY avg_delay_days DESC
