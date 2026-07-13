{{ config(
    materialized='view',
    schema='silver'
) }}

WITH raw_orders AS (
    SELECT * FROM `hdfs_catalog`.`bronze`.`olist_orders_dataset`
)

SELECT DISTINCT
    order_id,
    customer_id,
    order_status,
    -- Handle timestamp conversions
    CAST(order_purchase_timestamp AS DATETIME) AS order_purchase_timestamp,
    CAST(order_approved_at AS DATETIME) AS order_approved_at,
    CAST(order_delivered_carrier_date AS DATETIME) AS order_delivered_carrier_date,
    CAST(order_delivered_customer_date AS DATETIME) AS order_delivered_customer_date,
    CAST(order_estimated_delivery_date AS DATETIME) AS order_estimated_delivery_date
FROM raw_orders
WHERE order_id IS NOT NULL
