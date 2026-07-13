{{ config(
    materialized='view',
    schema='silver'
) }}

WITH raw_customers AS (
    SELECT * FROM `hdfs_catalog`.`bronze`.`olist_customers_dataset`
)

SELECT DISTINCT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM raw_customers
-- Drop rows where primary key is null (basic data quality filter)
WHERE customer_id IS NOT NULL
