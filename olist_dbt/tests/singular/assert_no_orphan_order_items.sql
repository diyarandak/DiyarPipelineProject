-- singular test: assert_no_orphan_order_items
-- Ensures every order_item has a matching parent order.
-- Orphan records indicate ETL issues or missing data.

SELECT
    oi.order_id,
    oi.order_item_id
FROM {{ ref('stg_order_items') }} oi
LEFT JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL
-- If this returns rows, the test FAILS (orphan items found)
