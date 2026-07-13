{{ config(
    materialized='table',
    unique_key='order_item_surrogate_key',
    schema='gold'
) }}

/*
    fact_order_items — Order Line Items Fact Table (Detail Level)
    
    Grain: One row per order line item (order_id + order_item_id).
    This is the most granular fact table — connects orders, products, and sellers.
    Use this for product-level and seller-level revenue analysis.
*/

WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp
    FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT customer_id, customer_unique_id
    FROM {{ ref('stg_customers') }}
)

SELECT
    CONCAT(i.order_id, '_', CAST(i.order_item_id AS VARCHAR)) AS order_item_surrogate_key,
    i.order_id,
    i.order_item_id,
    i.product_id AS product_key,
    i.seller_id AS seller_key,
    c.customer_unique_id AS customer_key,
    CAST(o.order_purchase_timestamp AS DATE) AS order_date_key,
    o.order_status,
    i.shipping_limit_date,
    i.price,
    i.freight_value,
    i.total_item_value,
    -- Revenue contribution percentage within the order
    CASE 
        WHEN SUM(i.total_item_value) OVER (PARTITION BY i.order_id) > 0
        THEN ROUND(
            i.total_item_value * 100.0 / SUM(i.total_item_value) OVER (PARTITION BY i.order_id),
            2
        )
        ELSE 0
    END AS revenue_pct_of_order

FROM items i
INNER JOIN orders o ON i.order_id = o.order_id
LEFT JOIN customers c ON o.customer_id = c.customer_id
