-- analysis: top_sellers_by_revenue
-- Business Question: Who are the top 20 sellers by GMV and how do they perform?

SELECT
    s.seller_key,
    s.seller_city,
    s.seller_state,
    s.seller_region,
    sp.total_orders,
    sp.total_gmv,
    sp.avg_order_value,
    sp.on_time_delivery_rate_pct,
    sp.cancellation_rate_pct,
    sp.avg_review_score,
    sp.first_order_date,
    sp.last_order_date
FROM {{ ref('fact_seller_performance') }} sp
INNER JOIN {{ ref('dim_sellers') }} s ON sp.seller_key = s.seller_key
ORDER BY sp.total_gmv DESC
LIMIT 20
