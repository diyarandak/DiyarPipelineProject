{{ config(
    materialized='table',
    schema='gold'
) }}

/*
    dim_payment_type — Payment Type Dimension Table
    
    Grain: One row per payment type.
    A junk/reference dimension for payment method analysis.
*/

SELECT DISTINCT
    payment_type AS payment_type_key,
    payment_type,
    CASE
        WHEN payment_type = 'credit_card' THEN 'Kredi Kartı'
        WHEN payment_type = 'boleto' THEN 'Boleto (Banka Havalesi)'
        WHEN payment_type = 'voucher' THEN 'Kupon / İndirim'
        WHEN payment_type = 'debit_card' THEN 'Banka Kartı'
        ELSE 'Diğer'
    END AS payment_type_description,
    CASE
        WHEN payment_type = 'credit_card' THEN 1
        WHEN payment_type = 'debit_card' THEN 2
        WHEN payment_type = 'boleto' THEN 3
        WHEN payment_type = 'voucher' THEN 4
        ELSE 5
    END AS payment_type_sort_order
FROM {{ ref('stg_order_payments') }}
