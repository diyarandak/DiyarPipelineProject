{{ config(
    materialized='table',
    schema='gold'
) }}

/*
    dim_sellers — Seller Dimension Table
    
    Grain: One row per seller.
    Enriched with regional grouping for geographic analysis.
*/

SELECT
    seller_id AS seller_key,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    -- Regional grouping (same logic as dim_customers for cross-analysis)
    CASE
        WHEN seller_state IN ('SP', 'RJ', 'MG', 'ES') THEN 'Sudeste'
        WHEN seller_state IN ('PR', 'SC', 'RS') THEN 'Sul'
        WHEN seller_state IN ('BA', 'PE', 'CE', 'MA', 'PB', 'RN', 'AL', 'SE', 'PI') THEN 'Nordeste'
        WHEN seller_state IN ('DF', 'GO', 'MT', 'MS') THEN 'Centro-Oeste'
        WHEN seller_state IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO') THEN 'Norte'
        ELSE 'Desconhecido'
    END AS seller_region
FROM {{ ref('stg_sellers') }}
