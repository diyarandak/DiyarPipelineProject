{{ config(
    materialized='view',
    schema='silver'
) }}

WITH raw_order_items AS (
    SELECT * FROM `hdfs_catalog`.`bronze`.`olist_order_items_dataset`
)

SELECT DISTINCT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    CAST(shipping_limit_date AS DATETIME) AS shipping_limit_date,
    CAST(price AS DECIMAL(10,2)) AS price,
    CAST(freight_value AS DECIMAL(10,2)) AS freight_value,
    -- Derived metric: Total item cost (price + freight)
    CAST(price AS DECIMAL(10,2)) + CAST(freight_value AS DECIMAL(10,2)) AS total_item_value
FROM raw_order_items
WHERE order_id IS NOT NULL
  AND product_id IS NOT NULL
