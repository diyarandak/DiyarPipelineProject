{{ config(
    materialized='table',
    schema='gold'
) }}

/*
    dim_geography — Geography Dimension Table (Degenerate)
    
    Grain: One row per unique zip_code + city + state combination.
    Combines customer and seller locations into a single geographic reference.
    Enables geographic cross-analysis between buyers and sellers.
*/

WITH all_locations AS (
    -- Customer locations
    SELECT DISTINCT
        customer_zip_code_prefix AS zip_code_prefix,
        customer_city AS city,
        customer_state AS state
    FROM {{ ref('stg_customers') }}
    
    UNION
    
    -- Seller locations
    SELECT DISTINCT
        seller_zip_code_prefix AS zip_code_prefix,
        seller_city AS city,
        seller_state AS state
    FROM {{ ref('stg_sellers') }}
)

SELECT
    CONCAT(zip_code_prefix, '_', state) AS geography_key,
    zip_code_prefix,
    city,
    state,
    CASE
        WHEN state IN ('SP', 'RJ', 'MG', 'ES') THEN 'Sudeste'
        WHEN state IN ('PR', 'SC', 'RS') THEN 'Sul'
        WHEN state IN ('BA', 'PE', 'CE', 'MA', 'PB', 'RN', 'AL', 'SE', 'PI') THEN 'Nordeste'
        WHEN state IN ('DF', 'GO', 'MT', 'MS') THEN 'Centro-Oeste'
        WHEN state IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO') THEN 'Norte'
        ELSE 'Desconhecido'
    END AS region
FROM all_locations
WHERE zip_code_prefix IS NOT NULL
