# Stage 2 summary

## Implemented scope

- Added Stage 2 entrypoint: `scripts/stage2.sh`.
- Added Spark SQL runner: `scripts/stage2_spark_eda.py`.
- Added Hive DDL and EDA query files:
  - `sql/stage2_hive_init.hql`
  - `sql/q1.hql`
  - `sql/q2.hql`
  - `sql/q3.hql`
- Added Stage 2 lint coverage in `Makefile` for `scripts/stage2_spark_eda.py`.

## Stage 2 data model

- Creates Hive DB (`teamx_projectdb` by default) in a dedicated location.
- Creates temporary external tables over Stage 1 Parquet paths.
- Builds optimized tables:
  - `reviews_optimized`: partitioned by `(review_year, review_month)` and bucketed by `parent_asin`.
  - `metadata_bucketed`: bucketed by `parent_asin`.
- Drops temporary unpartitioned external tables after transfer.

## EDA outputs

- Runs `q1`, `q2`, `q3` via Spark SQL.
- For each query:
  - stores table `${qx}_results` in Hive metastore,
  - exports one-file CSV to `output/qx.csv`.
- Writes execution summary to `output/hive_results.txt`.

## Manual Superset steps (not automated)

- Create datasets in Superset from `q1_results`, `q2_results`, `q3_results`.
- Build charts for each insight and add them to dashboard.
- Export chart images to `output/q1.jpg`, `output/q2.jpg`, `output/q3.jpg`.

## Validation status

- Shell syntax checks for Stage scripts passed.
- `pylint` for `scripts/stage2_spark_eda.py` passed (10.00/10).
- Full Stage 2 runtime requires a proper Spark/Hadoop environment (Hive metastore + accessible parquet locations).
