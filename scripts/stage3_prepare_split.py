#!/usr/bin/env python3
"""Prepare Stage III train/test artifacts from Hive feature layer on YARN."""

from __future__ import annotations

import argparse
import os

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build train/test JSON from Hive features")
    parser.add_argument("--team", default="team34")
    parser.add_argument("--database", required=True)
    parser.add_argument("--feature-table", default="ml_features")
    parser.add_argument("--label-col", default="label")
    parser.add_argument("--hdfs-train-dir", default="project/data/train")
    parser.add_argument("--hdfs-test-dir", default="project/data/test")
    parser.add_argument("--local-train-json", default="data/train.json")
    parser.add_argument("--local-test-json", default="data/test.json")
    parser.add_argument("--hive-metastore-uri", default="thrift://hadoop-02.uni.innopolis.ru:9883")
    parser.add_argument("--warehouse-dir", default="project/hive/warehouse")
    return parser.parse_args()


def mirror_hdfs_json(hdfs_dir: str, local_path: str) -> None:
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    command = f'hdfs dfs -cat "{hdfs_dir}"/*.json > "{local_path}"'
    code = os.system(command)
    if code != 0:
        raise RuntimeError(f"Cannot copy {hdfs_dir} to {local_path}")


def build_spark(args: argparse.Namespace) -> SparkSession:
    return (
        SparkSession.builder.appName(f"{args.team} - Stage3 Split")
        .master("yarn")
        .config("hive.metastore.uris", args.hive_metastore_uri)
        .config("spark.sql.warehouse.dir", args.warehouse_dir)
        .config("spark.sql.avro.compression.codec", "snappy")
        .enableHiveSupport()
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()
    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")

    source = spark.table(f"{args.database}.{args.feature_table}").na.drop()
    if args.label_col not in source.columns:
        raise ValueError(f"Label column '{args.label_col}' is missing in feature table")

    numeric_feature_cols = [
        field.name
        for field in source.schema.fields
        if field.name != args.label_col and isinstance(field.dataType, NumericType)
    ]

    if not numeric_feature_cols:
        raise ValueError("No numeric columns available for VectorAssembler features")

    label_type = source.schema[args.label_col].dataType
    prepared = source
    if not isinstance(label_type, NumericType):
        prepared = StringIndexer(
            inputCol=args.label_col, outputCol="label", handleInvalid="skip"
        ).fit(source).transform(source)
        label_col = "label"
    else:
        prepared = source.withColumn("label", F.col(args.label_col).cast("double"))
        label_col = "label"

    assembled = VectorAssembler(inputCols=numeric_feature_cols, outputCol="features").transform(prepared)
    dataset = assembled.select("features", F.col(label_col).alias("label"))

    train_df, test_df = dataset.randomSplit([0.7, 0.3], seed=42)
    train_df.coalesce(1).write.mode("overwrite").json(args.hdfs_train_dir)
    test_df.coalesce(1).write.mode("overwrite").json(args.hdfs_test_dir)

    mirror_hdfs_json(args.hdfs_train_dir, args.local_train_json)
    mirror_hdfs_json(args.hdfs_test_dir, args.local_test_json)

    spark.stop()


if __name__ == "__main__":
    main()
