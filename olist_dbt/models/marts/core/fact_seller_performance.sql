{{ config(
    materialized='table',
    schema='gold'
) }}

/*
    fact_seller_performance — Seller Performance Aggregated Fact Table
    
    Grain: One row per seller.
    Pre-aggregated KPIs for seller scorecard dashboards.
    This is a "derived fact" (aggregate fact table) built from the granular facts.
*/

WITH seller_orders AS (
    SELECT
        i.seller_id,
        i.order_id,
        i.price,
        i.freight_value,
        i.total_item_value,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date
    FROM {{ ref('stg_order_items') }} i
    INNER JOIN {{ ref('stg_orders') }} o ON i.order_id = o.order_id
),

seller_reviews AS (
    SELECT
        i.seller_id,
        AVG(r.review_score) AS avg_review_score,
        COUNT(*) AS total_reviews
    FROM {{ ref('stg_order_items') }} i
    INNER JOIN {{ ref('stg_order_reviews') }} r ON i.order_id = r.order_id
    GROUP BY i.seller_id
)

SELECT
    so.seller_id AS seller_key,
    COUNT(DISTINCT so.order_id) AS total_orders,
    SUM(so.price) AS total_revenue,
    SUM(so.freight_value) AS total_freight_collected,
    SUM(so.total_item_value) AS total_gmv,
    AVG(so.price) AS avg_order_value,
    
    -- Delivery performance
    SUM(CASE 
        WHEN so.order_delivered_customer_date <= so.order_estimated_delivery_date THEN 1 
        ELSE 0 
    END) AS on_time_deliveries,
    SUM(CASE 
        WHEN so.order_delivered_customer_date > so.order_estimated_delivery_date THEN 1 
        ELSE 0 
    END) AS late_deliveries,
    ROUND(
        SUM(CASE WHEN so.order_delivered_customer_date <= so.order_estimated_delivery_date THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(CASE WHEN so.order_delivered_customer_date IS NOT NULL THEN 1 END), 0),
        2
    ) AS on_time_delivery_rate_pct,

    -- Cancellation rate
    SUM(CASE WHEN so.order_status = 'canceled' THEN 1 ELSE 0 END) AS canceled_orders,
    ROUND(
        SUM(CASE WHEN so.order_status = 'canceled' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(DISTINCT so.order_id), 0),
        2
    ) AS cancellation_rate_pct,

    -- Review metrics (from seller_reviews CTE)
    COALESCE(sr.avg_review_score, 0) AS avg_review_score,
    COALESCE(sr.total_reviews, 0) AS total_reviews,

    -- Activity period
    MIN(so.order_purchase_timestamp) AS first_order_date,
    MAX(so.order_purchase_timestamp) AS last_order_date

FROM seller_orders so
LEFT JOIN seller_reviews sr ON so.seller_id = sr.seller_id
GROUP BY so.seller_id, sr.avg_review_score, sr.total_reviews
