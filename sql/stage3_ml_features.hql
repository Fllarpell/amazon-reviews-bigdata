-- Stage III feature layer for Spark ML.
-- Usage:
--   hive -hivevar db_name=team34_projectdb -f sql/stage3_ml_features.hql
--
-- This script creates/refreshes `${db_name}.stage3_ml_features` from
-- review facts and metadata dimensions prepared in previous stages.

USE ${hivevar:db_name};

DROP TABLE IF EXISTS stage3_ml_features;

CREATE TABLE stage3_ml_features
STORED AS PARQUET
AS
SELECT
    r.rating                                                     AS label,
    CAST(COALESCE(r.helpful_vote, 0) AS DOUBLE)                  AS helpful_vote,
    CAST(COALESCE(r.verified_purchase, false) AS INT)            AS verified_purchase,
    CAST(COALESCE(LENGTH(r.text), 0) AS DOUBLE)                  AS review_text_len,
    CAST(COALESCE(LENGTH(r.title), 0) AS DOUBLE)                 AS review_title_len,
    CAST(COALESCE(r.timestamp_ms, 0) AS DOUBLE)                  AS timestamp_ms,
    CAST(COALESCE(m.price, 0.0) AS DOUBLE)                       AS price,
    CAST(COALESCE(m.average_rating, 0.0) AS DOUBLE)              AS item_avg_rating,
    CAST(COALESCE(m.rating_number, 0) AS DOUBLE)                 AS item_rating_count
FROM reviews r
LEFT JOIN metadata m
    ON r.parent_asin = m.parent_asin
WHERE r.rating IS NOT NULL;
