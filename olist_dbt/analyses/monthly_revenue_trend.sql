-- analysis: monthly_revenue_trend
-- Business Question: What is the monthly revenue trend over time?
-- This query is NOT materialized — it's a reusable analysis template.

SELECT
    DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS order_month,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(total_order_value) AS total_revenue,
    SUM(total_freight_value) AS total_freight,
    AVG(total_order_value) AS avg_order_value,
    AVG(avg_review_score) AS avg_satisfaction_score
FROM {{ ref('fact_orders') }}
WHERE order_status NOT IN ('canceled', 'unavailable')
GROUP BY DATE_FORMAT(order_purchase_timestamp, '%Y-%m')
ORDER BY order_month
