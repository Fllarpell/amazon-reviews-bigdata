-- Stage 2 Hive bootstrap (external -> optimized partition/bucket tables).
-- Pass variables via --hiveconf:
--   hive_db_name
--   hive_db_location
--   hdfs_warehouse_base
--   hive_reviews_optimized_path
--   hive_metadata_bucketed_path

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.enforce.bucketing=true;
SET hive.exec.max.dynamic.partitions=5000;
SET hive.exec.max.dynamic.partitions.pernode=2000;
SET hive.vectorized.execution.enabled=false;
SET hive.vectorized.execution.reduce.enabled=false;
SET hive.vectorized.input.format.excludes=org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;

CREATE DATABASE IF NOT EXISTS ${hiveconf:hive_db_name}
LOCATION '${hiveconf:hive_db_location}';

USE ${hiveconf:hive_db_name};

DROP TABLE IF EXISTS reviews_ext;
CREATE EXTERNAL TABLE reviews_ext (
    review_id STRING,
    parent_asin STRING,
    user_id STRING,
    asin STRING,
    rating INT,
    review_title STRING,
    review_text STRING,
    review_timestamp BIGINT,
    helpful_vote INT,
    verified_purchase BOOLEAN,
    images_json STRING
)
STORED AS PARQUET
LOCATION '${hiveconf:hdfs_warehouse_base}/reviews';

DROP TABLE IF EXISTS metadata_ext;
CREATE EXTERNAL TABLE metadata_ext (
    parent_asin STRING,
    main_category STRING,
    title STRING,
    average_rating DOUBLE,
    rating_number INT,
    price STRING,
    store STRING,
    features_text STRING,
    description_text STRING,
    categories_json STRING,
    details_json STRING
)
STORED AS PARQUET
LOCATION '${hiveconf:hdfs_warehouse_base}/metadata';

DROP TABLE IF EXISTS reviews_optimized;
CREATE TABLE reviews_optimized (
    review_id STRING,
    parent_asin STRING,
    user_id STRING,
    asin STRING,
    rating INT,
    review_title STRING,
    review_text STRING,
    review_timestamp TIMESTAMP,
    helpful_vote INT,
    verified_purchase BOOLEAN,
    images_json STRING
)
PARTITIONED BY (review_year INT, review_month INT)
CLUSTERED BY (parent_asin) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION '${hiveconf:hive_reviews_optimized_path}';

INSERT OVERWRITE TABLE reviews_optimized PARTITION (review_year, review_month)
SELECT
    review_id,
    parent_asin,
    user_id,
    asin,
    rating,
    review_title,
    review_text,
    review_timestamp_ts AS review_timestamp,
    helpful_vote,
    verified_purchase,
    images_json,
    YEAR(review_timestamp_ts) AS review_year,
    MONTH(review_timestamp_ts) AS review_month
FROM (
    SELECT
        review_id,
        parent_asin,
        user_id,
        asin,
        rating,
        review_title,
        review_text,
        CASE
            WHEN review_timestamp IS NULL THEN NULL
            WHEN review_timestamp > 32503680000
                THEN CAST(from_unixtime(CAST(review_timestamp / 1000 AS BIGINT)) AS TIMESTAMP)
            ELSE CAST(from_unixtime(review_timestamp) AS TIMESTAMP)
        END AS review_timestamp_ts,
        helpful_vote,
        verified_purchase,
        images_json
    FROM reviews_ext
) reviews_prepared;

DROP TABLE IF EXISTS metadata_bucketed;
CREATE TABLE metadata_bucketed (
    parent_asin STRING,
    main_category STRING,
    title STRING,
    average_rating DOUBLE,
    rating_number INT,
    price DECIMAL(12,2),
    store STRING,
    features_text STRING,
    description_text STRING,
    categories_json STRING,
    details_json STRING
)
CLUSTERED BY (parent_asin) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION '${hiveconf:hive_metadata_bucketed_path}';

INSERT OVERWRITE TABLE metadata_bucketed
SELECT
    parent_asin,
    main_category,
    title,
    average_rating,
    rating_number,
    CASE
        WHEN price IS NULL THEN NULL
        WHEN trim(price) = '' THEN NULL
        ELSE CAST(regexp_replace(price, '[^0-9\\.-]', '') AS DECIMAL(12,2))
    END AS price,
    store,
    features_text,
    description_text,
    categories_json,
    details_json
FROM metadata_ext;

-- Per checklist: keep EDA on partition/bucket tables only.
DROP TABLE IF EXISTS reviews_ext;
DROP TABLE IF EXISTS metadata_ext;

SHOW TABLES;
