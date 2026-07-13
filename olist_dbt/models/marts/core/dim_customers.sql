{{ config(
    materialized='table',
    schema='gold'
) }}

/*
    dim_customers — Customer Dimension Table (SCD Type 2)
    
    Grain: One row per customer per location change.
    Note: Olist dataset has customer_id (per order) and customer_unique_id (per person).
    We track the customer's address over time by using their order timestamps,
    implementing Slowly Changing Dimension (SCD) Type 2 logic.
*/

WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        c.customer_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        o.order_purchase_timestamp AS valid_from
    FROM {{ ref('stg_customers') }} c
    INNER JOIN {{ ref('stg_orders') }} o ON c.customer_id = o.customer_id
),

scd_logic AS (
    SELECT
        customer_unique_id,
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        valid_from,
        LEAD(valid_from) OVER (
            PARTITION BY customer_unique_id 
            ORDER BY valid_from
        ) AS valid_to
    FROM customer_orders
)

SELECT
    MD5(CONCAT(customer_unique_id, '_', customer_id)) AS customer_sk,
    customer_unique_id AS customer_key,
    customer_id AS original_customer_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    
    -- Regional grouping
    CASE
        WHEN customer_state IN ('SP', 'RJ', 'MG', 'ES') THEN 'Sudeste'
        WHEN customer_state IN ('PR', 'SC', 'RS') THEN 'Sul'
        WHEN customer_state IN ('BA', 'PE', 'CE', 'MA', 'PB', 'RN', 'AL', 'SE', 'PI') THEN 'Nordeste'
        WHEN customer_state IN ('DF', 'GO', 'MT', 'MS') THEN 'Centro-Oeste'
        WHEN customer_state IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO') THEN 'Norte'
        ELSE 'Desconhecido'
    END AS customer_region,
    
    -- SCD Type 2 Columns
    CAST(valid_from AS DATETIME) AS valid_from,
    CAST(valid_to AS DATETIME) AS valid_to,
    CASE WHEN valid_to IS NULL THEN TRUE ELSE FALSE END AS is_active

FROM scd_logic
