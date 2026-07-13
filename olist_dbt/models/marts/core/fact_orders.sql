{{ config(
    materialized='table',
    unique_key='order_id',
    schema='gold'
) }}

/*
    fact_orders — Order Fact Table (Header Level)
    
    Grain: One row per order.
    This is the primary fact table connecting all dimensions.
    Contains pre-aggregated metrics from order_items and payments for fast querying.
*/

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items_agg AS (
    SELECT
        order_id,
        COUNT(DISTINCT product_id) AS total_unique_products,
        COUNT(*) AS total_items,
        SUM(price) AS total_product_value,
        SUM(freight_value) AS total_freight_value,
        SUM(total_item_value) AS total_order_value,
        AVG(price) AS avg_item_price
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),

payments_agg AS (
    SELECT
        order_id,
        COUNT(DISTINCT payment_type) AS distinct_payment_methods,
        SUM(payment_value) AS total_payment_value,
        MAX(payment_installments) AS max_installments
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
),

reviews_agg AS (
    SELECT
        order_id,
        AVG(review_score) AS avg_review_score,
        MIN(review_score) AS min_review_score,
        COUNT(*) AS review_count
    FROM {{ ref('stg_order_reviews') }}
    GROUP BY order_id
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    o.order_id,
    c.customer_unique_id AS customer_key,
    CAST(o.order_purchase_timestamp AS DATE) AS order_date_key,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- Delivery performance metrics
    -- Delivery performance metrics
    hours_diff(
        CAST(o.order_approved_at AS DATETIME), CAST(o.order_purchase_timestamp AS DATETIME)
    ) AS approval_time_hours,
    days_diff(
        CAST(o.order_delivered_carrier_date AS DATETIME), CAST(o.order_approved_at AS DATETIME)
    ) AS processing_time_days,
    days_diff(
        CAST(o.order_delivered_customer_date AS DATETIME), CAST(o.order_delivered_carrier_date AS DATETIME)
    ) AS shipping_time_days,
    days_diff(
        CAST(o.order_delivered_customer_date AS DATETIME), CAST(o.order_purchase_timestamp AS DATETIME)
    ) AS total_delivery_days,
    days_diff(
        CAST(o.order_delivered_customer_date AS DATETIME), CAST(o.order_estimated_delivery_date AS DATETIME)
    ) AS delivery_delay_days,
    CASE
        WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 'on_time'
        WHEN o.order_delivered_customer_date IS NULL THEN 'pending'
        ELSE 'late'
    END AS delivery_status,

    -- Order items metrics (from pre-aggregation)
    COALESCE(oi.total_unique_products, 0) AS total_unique_products,
    COALESCE(oi.total_items, 0) AS total_items,
    COALESCE(oi.total_product_value, 0) AS total_product_value,
    COALESCE(oi.total_freight_value, 0) AS total_freight_value,
    COALESCE(oi.total_order_value, 0) AS total_order_value,
    COALESCE(oi.avg_item_price, 0) AS avg_item_price,

    -- Payment metrics
    COALESCE(pa.distinct_payment_methods, 0) AS distinct_payment_methods,
    COALESCE(pa.total_payment_value, 0) AS total_payment_value,
    COALESCE(pa.max_installments, 0) AS max_installments,

    -- Review metrics
    COALESCE(rv.avg_review_score, 0) AS avg_review_score,
    COALESCE(rv.review_count, 0) AS review_count

FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN order_items_agg oi ON o.order_id = oi.order_id
LEFT JOIN payments_agg pa ON o.order_id = pa.order_id
LEFT JOIN reviews_agg rv ON o.order_id = rv.order_id
