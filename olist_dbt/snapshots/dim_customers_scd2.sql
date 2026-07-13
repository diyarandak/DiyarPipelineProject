{% snapshot dim_customers_scd2 %}

{{
    config(
      target_schema='gold',
      unique_key='customer_id',
      strategy='check',
      check_cols=['customer_zip_code_prefix', 'customer_city', 'customer_state']
    )
}}

SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ ref('stg_customers') }}

{% endsnapshot %}
