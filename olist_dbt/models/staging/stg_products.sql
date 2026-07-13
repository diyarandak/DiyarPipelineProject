{{ config(
    materialized='view',
    schema='silver'
) }}

WITH raw_products AS (
    SELECT * FROM `hdfs_catalog`.`bronze`.`olist_products_dataset`
),
translations AS (
    SELECT * FROM `hdfs_catalog`.`bronze`.`product_category_name_translation`
)

SELECT DISTINCT
    p.product_id,
    COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS product_category,
    p.product_category_name AS product_category_original,
    CAST(p.product_name_lenght AS INT) AS product_name_length,
    CAST(p.product_description_lenght AS INT) AS product_description_length,
    CAST(p.product_photos_qty AS INT) AS product_photos_qty,
    CAST(p.product_weight_g AS DECIMAL(10,2)) AS product_weight_g,
    CAST(p.product_length_cm AS DECIMAL(10,2)) AS product_length_cm,
    CAST(p.product_height_cm AS DECIMAL(10,2)) AS product_height_cm,
    CAST(p.product_width_cm AS DECIMAL(10,2)) AS product_width_cm,
    -- Derived: Volume in cm³
    CAST(p.product_length_cm AS DECIMAL(10,2))
        * CAST(p.product_height_cm AS DECIMAL(10,2))
        * CAST(p.product_width_cm AS DECIMAL(10,2)) AS product_volume_cm3
FROM raw_products p
LEFT JOIN translations t
    ON p.product_category_name = t.product_category_name
WHERE p.product_id IS NOT NULL
