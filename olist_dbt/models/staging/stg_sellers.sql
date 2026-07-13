{{ config(
    materialized='view',
    schema='silver'
) }}

WITH raw_sellers AS (
    SELECT * FROM `hdfs_catalog`.`bronze`.`olist_sellers_dataset`
)

SELECT DISTINCT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM raw_sellers
WHERE seller_id IS NOT NULL
