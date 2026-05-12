#!/usr/bin/env python3
"""Prepare Stage III train/test artifacts from Hive feature layer on YARN."""

import argparse
import json
import os

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
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
    parser.add_argument("--store-top-k", type=int, default=200)
    parser.add_argument(
        "--features-hdfs-path",
        default="",
        help="Parquet table directory (Hive LOCATION); avoids spark.table() SQL parser",
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=float(os.environ.get("STAGE3_SAMPLE_FRACTION", "1.0")),
        help="Use a random fraction of rows before train/test split (1.0 = all). Env: STAGE3_SAMPLE_FRACTION",
    )
    parser.add_argument(
        "--feature-manifest-out",
        default="",
        help="Write JSON with human-readable feature names (same order as vectors). "
        "Default: <dirname(local-train-json)>/feature_manifest.json",
    )
    return parser.parse_args()


def feature_manifest_path(args: argparse.Namespace) -> str:
    out = (args.feature_manifest_out or "").strip()
    if out:
        return os.path.abspath(out)
    return os.path.join(os.path.dirname(os.path.abspath(args.local_train_json)), "feature_manifest.json")


def build_feature_name_lists(
    numeric_feature_cols: list,
    indexer_model,
    encoder_model,
) -> tuple:
    """Align with VectorAssembler column order (full `features` and NB-only `features_nb`)."""
    labels_main = list(indexer_model.labelsArray[0])
    labels_store = list(indexer_model.labelsArray[1])
    drop_last = bool(encoder_model.getDropLast())
    sizes = list(encoder_model.categorySizes)

    def ohe_dim(cat_count: int) -> int:
        return cat_count - 1 if drop_last else cat_count

    def ohe_names(labels: list, prefix: str, cat_count: int) -> list:
        d = ohe_dim(cat_count)
        names = []
        for j in range(d):
            lab = labels[j] if j < len(labels) else f"{prefix}_idx{j}"
            names.append(f"{prefix}={lab}")
        return names

    main_ohe = ohe_names(labels_main, "main_category", sizes[0])
    store_ohe = ohe_names(labels_store, "store", sizes[1])

    tail = ["verified_purchase_num"] + main_ohe + store_ohe
    features_nb = list(tail)
    features = list(numeric_feature_cols) + tail
    return features, features_nb


def write_feature_manifest(path: str, features: list, features_nb: list) -> None:
    payload = {
        "features": features,
        "features_nb": features_nb,
        "description": (
            "Feature names in dense vector index order. "
            "OHE uses dropLast reference category (omitted from vector). "
            "NB uses features_nb (no raw numerics)."
        ),
    }
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as wf:
        json.dump(payload, wf, ensure_ascii=False, indent=2)


def mirror_hdfs_json(hdfs_dir: str, local_path: str) -> None:
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    command = f'hdfs dfs -cat "{hdfs_dir}"/*.json > "{local_path}"'
    code = os.system(command)
    if code != 0:
        raise RuntimeError(f"Cannot copy {hdfs_dir} to {local_path}")


def build_spark(args: argparse.Namespace) -> SparkSession:
    b = (
        SparkSession.builder.appName(f"{args.team} - Stage3 Split")
        .master("yarn")
        .config("spark.sql.warehouse.dir", args.warehouse_dir)
        .config("spark.sql.avro.compression.codec", "snappy")
    )
    if (args.features_hdfs_path or "").strip():
        return b.getOrCreate()
    return (
        b.config("hive.metastore.uris", args.hive_metastore_uri)
        .enableHiveSupport()
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()
    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")

    feat_path = (args.features_hdfs_path or "").strip()
    if feat_path:
        source = spark.read.parquet(feat_path)
    else:
        source = spark.table(f"{args.database}.{args.feature_table}")
    if args.label_col not in source.columns:
        raise ValueError(f"Label column '{args.label_col}' is missing in feature table")

    for required_col in ("verified_purchase", "main_category", "store"):
        if required_col not in source.columns:
            raise ValueError(f"Required feature column '{required_col}' is missing in feature table")

    numeric_feature_cols = []
    for col_name in (
        "helpful_vote",
        "price",
        "average_rating",
        "rating_number",
        "review_year",
        "review_month",
    ):
        if col_name not in source.columns:
            raise ValueError(f"Required numeric feature column '{col_name}' is missing")
        if not isinstance(source.schema[col_name].dataType, NumericType):
            raise ValueError(f"Feature column '{col_name}' must be numeric")
        numeric_feature_cols.append(col_name)

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

    normalized_main_category = F.trim(F.coalesce(F.col("main_category").cast("string"), F.lit("")))
    normalized_store = F.trim(F.coalesce(F.col("store").cast("string"), F.lit("")))

    prepared = (
        prepared.withColumn(
            "main_category_clean",
            F.when(F.length(normalized_main_category) > 0, normalized_main_category).otherwise(
                F.lit("unknown")
            ),
        )
        .withColumn(
            "store_clean",
            F.when(F.length(normalized_store) > 0, normalized_store).otherwise(F.lit("unknown")),
        )
        .withColumn(
            "verified_purchase_num",
            F.when(F.col("verified_purchase") == F.lit(True), F.lit(1.0)).otherwise(F.lit(0.0)),
        )
    )

    if args.store_top_k <= 0:
        raise ValueError("--store-top-k must be > 0")

    top_stores_rows = (
        prepared.groupBy("store_clean")
        .count()
        .orderBy(F.desc("count"), F.asc("store_clean"))
        .limit(args.store_top_k)
        .collect()
    )
    top_stores = [row["store_clean"] for row in top_stores_rows]

    prepared = prepared.withColumn(
        "store_bucketed",
        F.when(F.col("store_clean").isin(top_stores), F.col("store_clean")).otherwise(F.lit("other")),
    )

    indexer = StringIndexer(
        inputCols=["main_category_clean", "store_bucketed"],
        outputCols=["main_category_idx", "store_bucketed_idx"],
        handleInvalid="keep",
    )
    indexer_model = indexer.fit(prepared)
    indexed = indexer_model.transform(prepared)

    encoder = OneHotEncoder(
        inputCols=["main_category_idx", "store_bucketed_idx"],
        outputCols=["main_category_ohe", "store_bucketed_ohe"],
        handleInvalid="keep",
    )
    encoder_model = encoder.fit(indexed)
    encoded = encoder_model.transform(indexed)

    features_names, features_nb_names = build_feature_name_lists(
        numeric_feature_cols, indexer_model, encoder_model
    )
    manifest_out = feature_manifest_path(args)
    write_feature_manifest(manifest_out, features_names, features_nb_names)

    assembler_input_cols = (
        numeric_feature_cols + ["verified_purchase_num", "main_category_ohe", "store_bucketed_ohe"]
    )
    assembled = VectorAssembler(
        inputCols=assembler_input_cols,
        outputCol="features",
        handleInvalid="keep",
    ).transform(encoded)
    # Narrow binary + one-hot only: multinomial Naive Bayes assumes discrete/count-like
    # dimensions; continuous numerics are intentionally omitted for model2 (see stage3_ml_train).
    assembled_nb = VectorAssembler(
        inputCols=["verified_purchase_num", "main_category_ohe", "store_bucketed_ohe"],
        outputCol="features_nb",
        handleInvalid="keep",
    ).transform(assembled)
    dataset = assembled_nb.select(
        "features",
        "features_nb",
        F.col(label_col).alias("label"),
    )

    sf = float(args.sample_fraction)
    if not (0 < sf <= 1.0):
        raise ValueError("--sample-fraction must be in (0, 1]")
    if sf < 1.0:
        dataset = dataset.sample(withReplacement=False, fraction=sf, seed=42)

    train_df, test_df = dataset.randomSplit([0.7, 0.3], seed=42)
    train_df.coalesce(1).write.mode("overwrite").json(args.hdfs_train_dir)
    test_df.coalesce(1).write.mode("overwrite").json(args.hdfs_test_dir)

    mirror_hdfs_json(args.hdfs_train_dir, args.local_train_json)
    mirror_hdfs_json(args.hdfs_test_dir, args.local_test_json)

    spark.stop()


if __name__ == "__main__":
    main()
