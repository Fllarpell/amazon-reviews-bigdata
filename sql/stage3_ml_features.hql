-- Stage III ML feature table (partitioned Parquet).
-- Run from scripts (official: scripts/stage3.sh) with:
--   beeline ... --hiveconf hive_db_name=... --hiveconf hive_db_location=...
--   --hiveconf hive_ml_features_table=... --hiveconf hive_ml_features_path=...

CREATE DATABASE IF NOT EXISTS ${hiveconf:hive_db_name}
LOCATION '${hiveconf:hive_db_location}';

USE ${hiveconf:hive_db_name};

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=5000;
SET hive.exec.max.dynamic.partitions.pernode=2000;

DROP TABLE IF EXISTS ${hiveconf:hive_ml_features_table};
CREATE TABLE ${hiveconf:hive_ml_features_table} (
    review_id STRING,
    parent_asin STRING,
    user_id STRING,
    asin STRING,
    label INT,
    review_title STRING,
    review_text STRING,
    helpful_vote INT,
    verified_purchase BOOLEAN,
    main_category STRING,
    store STRING,
    price DECIMAL(12,2),
    average_rating DOUBLE,
    rating_number INT
)
PARTITIONED BY (review_year INT, review_month INT)
STORED AS PARQUET
LOCATION '${hiveconf:hive_ml_features_path}';

INSERT OVERWRITE TABLE ${hiveconf:hive_ml_features_table} PARTITION (review_year, review_month)
SELECT
    r.review_id,
    r.parent_asin,
    r.user_id,
    r.asin,
    r.rating AS label,
    r.review_title,
    trim(r.review_text) AS review_text,
    CASE
        WHEN r.helpful_vote IS NULL THEN 0
        WHEN r.helpful_vote < 0 THEN 0
        ELSE r.helpful_vote
    END AS helpful_vote,
    r.verified_purchase,
    m.main_category,
    m.store,
    m.price,
    m.average_rating,
    m.rating_number,
    r.review_year,
    r.review_month
FROM reviews_optimized r
LEFT JOIN metadata_bucketed m
    ON r.parent_asin = m.parent_asin
WHERE r.rating BETWEEN 1 AND 5
  AND r.review_id IS NOT NULL
  AND trim(r.review_id) <> ''
  AND r.parent_asin IS NOT NULL
  AND trim(r.parent_asin) <> ''
  AND r.user_id IS NOT NULL
  AND trim(r.user_id) <> ''
  AND r.review_text IS NOT NULL
  AND length(trim(r.review_text)) >= 5
  AND length(trim(r.review_text)) <= 5000;

SHOW TABLES;
