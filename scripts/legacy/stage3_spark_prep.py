import argparse
import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException


def parse_args():
    parser = argparse.ArgumentParser(description="Stage 3 Spark data preparation")
    parser.add_argument("--hive-db-name", required=True)
    parser.add_argument("--hive-features-table", required=True)
    parser.add_argument("--hdfs-features-path", required=True)
    parser.add_argument("--train-ratio", type=float, default=0.8)
    parser.add_argument("--split-seed", type=int, default=34)
    parser.add_argument("--hdfs-train-path", required=True)
    parser.add_argument("--hdfs-test-path", required=True)
    parser.add_argument("--summary-out", required=True)
    parser.add_argument("--class-balance-out", required=True)
    return parser.parse_args()


def write_summary(summary_path, values):
    target = Path(summary_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        for key in sorted(values):
            handle.write("{0}={1}\n".format(key, values[key]))


def write_class_balance(balance_path, rows):
    target = Path(balance_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["split", "label", "count"])
        for split_name, label, count in rows:
            writer.writerow([split_name, label, count])


def collect_label_counts(df, split_name):
    result = []
    rows = (
        df.groupBy("label")
        .count()
        .orderBy(F.col("label").asc())
        .collect()
    )
    for row in rows:
        result.append((split_name, int(row["label"]), int(row["count"])))
    return result


def main():
    args = parse_args()
    if args.train_ratio <= 0.0 or args.train_ratio >= 1.0:
        raise ValueError("train-ratio must be in range (0, 1)")

    spark = (
        SparkSession.builder.appName("stage3-spark-prep")
        .enableHiveSupport()
        .getOrCreate()
    )

    source_name = "{0}.{1}".format(args.hive_db_name, args.hive_features_table)
    source_origin = "hive_table"
    try:
        source_df = spark.table(source_name)
    except AnalysisException:
        source_df = spark.read.parquet(args.hdfs_features_path)
        source_origin = "hdfs_parquet"

    required_cols = [
        "review_id",
        "parent_asin",
        "user_id",
        "asin",
        "label",
        "review_title",
        "review_text",
        "helpful_vote",
        "verified_purchase",
        "main_category",
        "store",
        "price",
        "average_rating",
        "rating_number",
        "review_year",
        "review_month",
    ]
    missing_cols = [col for col in required_cols if col not in source_df.columns]
    if missing_cols:
        raise ValueError("Missing required columns: {0}".format(",".join(missing_cols)))

    cleaned_df = (
        source_df.select(*required_cols)
        .withColumn("review_text", F.trim(F.col("review_text")))
        .withColumn("label", F.col("label").cast("int"))
        .withColumn("helpful_vote", F.coalesce(F.col("helpful_vote").cast("int"), F.lit(0)))
        .withColumn(
            "helpful_vote",
            F.when(F.col("helpful_vote") < 0, F.lit(0)).otherwise(F.col("helpful_vote")),
        )
        .withColumn(
            "verified_purchase",
            F.when(F.col("verified_purchase").isNull(), F.lit(False)).otherwise(F.col("verified_purchase")),
        )
        .filter(F.col("label").between(1, 5))
        .filter(F.col("review_id").isNotNull() & (F.length(F.trim(F.col("review_id"))) > 0))
        .filter(F.col("parent_asin").isNotNull() & (F.length(F.trim(F.col("parent_asin"))) > 0))
        .filter(F.col("user_id").isNotNull() & (F.length(F.trim(F.col("user_id"))) > 0))
        .filter(F.col("review_text").isNotNull())
        .filter(F.length(F.col("review_text")).between(5, 5000))
    )

    source_rows = source_df.count()
    cleaned_rows = cleaned_df.count()

    with_rand = cleaned_df.withColumn("_split_rand", F.rand(args.split_seed))
    train_df = with_rand.filter(F.col("_split_rand") < F.lit(args.train_ratio)).drop("_split_rand")
    test_df = with_rand.filter(F.col("_split_rand") >= F.lit(args.train_ratio)).drop("_split_rand")

    train_rows = train_df.count()
    test_rows = test_df.count()

    train_df.write.mode("overwrite").json(args.hdfs_train_path)
    test_df.write.mode("overwrite").json(args.hdfs_test_path)

    class_balance_rows = []
    class_balance_rows.extend(collect_label_counts(cleaned_df, "all"))
    class_balance_rows.extend(collect_label_counts(train_df, "train"))
    class_balance_rows.extend(collect_label_counts(test_df, "test"))

    dropped_rows = source_rows - cleaned_rows
    train_ratio_actual = float(train_rows) / float(cleaned_rows) if cleaned_rows else 0.0
    test_ratio_actual = float(test_rows) / float(cleaned_rows) if cleaned_rows else 0.0

    summary_values = {
        "source_table": source_name,
        "source_origin": source_origin,
        "hdfs_features_path": args.hdfs_features_path,
        "source_rows": source_rows,
        "cleaned_rows": cleaned_rows,
        "dropped_rows": dropped_rows,
        "train_rows": train_rows,
        "test_rows": test_rows,
        "train_ratio_target": args.train_ratio,
        "train_ratio_actual": round(train_ratio_actual, 6),
        "test_ratio_actual": round(test_ratio_actual, 6),
        "hdfs_train_path": args.hdfs_train_path,
        "hdfs_test_path": args.hdfs_test_path,
    }

    write_summary(args.summary_out, summary_values)
    write_class_balance(args.class_balance_out, class_balance_rows)

    spark.stop()


if __name__ == "__main__":
    main()
