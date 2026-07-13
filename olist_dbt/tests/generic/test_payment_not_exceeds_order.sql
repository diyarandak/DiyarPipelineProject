-- generic test: test_payment_not_exceeds_order
-- Validates that total payment for an order does not exceed 2x the order value.
-- This catches data anomalies like duplicate payments or currency errors.
-- Usage in schema.yml:
--   tests:
--     - payment_not_exceeds_order:
--         payment_model: ref('stg_order_payments')

{% test payment_not_exceeds_order(model, payment_model) %}

WITH order_totals AS (
    SELECT
        order_id,
        SUM(total_item_value) AS total_order_value
    FROM {{ model }}
    GROUP BY order_id
),

payment_totals AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment_value
    FROM {{ payment_model }}
    GROUP BY order_id
)

SELECT
    ot.order_id,
    ot.total_order_value,
    pt.total_payment_value
FROM order_totals ot
INNER JOIN payment_totals pt ON ot.order_id = pt.order_id
WHERE pt.total_payment_value > (ot.total_order_value * 2)
-- If this query returns rows, the test FAILS (anomalous payments detected)

{% endtest %}
