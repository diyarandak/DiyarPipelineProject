{{ config(
    materialized='table',
    schema='gold'
) }}

/*
    dim_products — Product Dimension Table
    
    Grain: One row per product.
    Enriched with English category names and size classification.
*/

SELECT
    product_id AS product_key,
    product_category,
    product_category_original,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_volume_cm3,
    -- Size classification based on volume
    CASE
        WHEN product_volume_cm3 IS NULL THEN 'unknown'
        WHEN product_volume_cm3 < 1000 THEN 'small'
        WHEN product_volume_cm3 < 10000 THEN 'medium'
        WHEN product_volume_cm3 < 50000 THEN 'large'
        ELSE 'extra_large'
    END AS product_size_category,
    -- Weight classification
    CASE
        WHEN product_weight_g IS NULL THEN 'unknown'
        WHEN product_weight_g < 500 THEN 'lightweight'
        WHEN product_weight_g < 5000 THEN 'medium_weight'
        ELSE 'heavyweight'
    END AS product_weight_category
FROM {{ ref('stg_products') }}
