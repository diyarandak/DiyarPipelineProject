-- singular test: assert_delivered_orders_have_delivery_date
-- Business rule: If order_status = 'delivered', a delivery date MUST exist.

SELECT
    order_id,
    order_status,
    order_delivered_customer_date
FROM {{ ref('stg_orders') }}
WHERE order_status = 'delivered'
  AND order_delivered_customer_date IS NULL
