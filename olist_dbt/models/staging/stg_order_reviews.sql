{{ config(
    materialized='view',
    schema='silver'
) }}

WITH raw_reviews AS (
    SELECT * FROM `hdfs_catalog`.`bronze`.`olist_order_reviews_dataset`
)

SELECT DISTINCT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    CAST(review_creation_date AS DATETIME) AS review_creation_date,
    CAST(review_answer_timestamp AS DATETIME) AS review_answer_timestamp,
    -- Derived: Response time in hours
    hours_diff(
        CAST(review_answer_timestamp AS DATETIME),
        CAST(review_creation_date AS DATETIME)
    ) AS response_time_hours
FROM raw_reviews
WHERE review_id IS NOT NULL
  AND review_score BETWEEN 1 AND 5
