{{ config(
    materialized='view',
    schema='silver'
) }}

WITH raw_payments AS (
    SELECT * FROM `hdfs_catalog`.`bronze`.`olist_order_payments_dataset`
)

SELECT DISTINCT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    CAST(payment_value AS DECIMAL(10,2)) AS payment_value
FROM raw_payments
WHERE order_id IS NOT NULL
  AND payment_value > 0
