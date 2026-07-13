-- analysis: customer_rfm_segmentation
-- Business Question: RFM (Recency, Frequency, Monetary) customer segmentation.
-- Advanced analytics technique for customer value classification.

WITH customer_metrics AS (
    SELECT
        fo.customer_key,
        MAX(fo.order_purchase_timestamp) AS last_order_date,
        COUNT(DISTINCT fo.order_id) AS frequency,
        SUM(fo.total_order_value) AS monetary
    FROM {{ ref('fact_orders') }} fo
    WHERE fo.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY fo.customer_key
),

rfm_scores AS (
    SELECT
        customer_key,
        last_order_date,
        frequency,
        monetary,
        -- RFM scoring (1-5 quintiles)
        NTILE(5) OVER (ORDER BY last_order_date) AS recency_score,
        NTILE(5) OVER (ORDER BY frequency) AS frequency_score,
        NTILE(5) OVER (ORDER BY monetary) AS monetary_score
    FROM customer_metrics
)

SELECT
    r.customer_key,
    dc.customer_city,
    dc.customer_state,
    dc.customer_region,
    r.last_order_date,
    r.frequency,
    r.monetary,
    r.recency_score,
    r.frequency_score,
    r.monetary_score,
    (r.recency_score + r.frequency_score + r.monetary_score) AS rfm_total,
    CASE
        WHEN (r.recency_score + r.frequency_score + r.monetary_score) >= 13 THEN 'Champions'
        WHEN (r.recency_score + r.frequency_score + r.monetary_score) >= 10 THEN 'Loyal Customers'
        WHEN (r.recency_score + r.frequency_score + r.monetary_score) >= 7 THEN 'Potential Loyalists'
        WHEN (r.recency_score + r.frequency_score + r.monetary_score) >= 4 THEN 'At Risk'
        ELSE 'Hibernating'
    END AS customer_segment
FROM rfm_scores r
LEFT JOIN {{ ref('dim_customers') }} dc ON r.customer_key = dc.customer_key
ORDER BY rfm_total DESC
