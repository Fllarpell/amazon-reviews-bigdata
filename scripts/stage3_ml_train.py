#!/usr/bin/env python3
"""Stage III distributed Spark ML training on YARN.

This script is designed to be launched only with spark-submit on YARN.
It reads split datasets from JSON, trains/tunes multiple Spark ML models,
stores predictions and evaluation to HDFS, and mirrors CSV artifacts locally.
"""

from __future__ import annotations

import argparse
import os
from typing import Dict, List, Tuple

from pyspark.ml.classification import (
    NaiveBayes,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.functions import array_to_vector
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train Spark ML models on YARN")
    parser.add_argument("--team", default="team34", help="Team name for Spark app")
    parser.add_argument("--train-path", default="project/data/train")
    parser.add_argument("--test-path", default="project/data/test")
    parser.add_argument("--hdfs-output-base", default="project/output")
    parser.add_argument("--local-output-dir", default="output")
    parser.add_argument("--hdfs-model-base", default="project/models")
    parser.add_argument("--hive-metastore-uri", default="thrift://hadoop-02.uni.innopolis.ru:9883")
    parser.add_argument("--warehouse-dir", default="project/hive/warehouse")
    parser.add_argument("--cv-folds", type=int, default=3)
    parser.add_argument("--cv-parallelism", type=int, default=3)
    return parser.parse_args()


def build_spark_session(args: argparse.Namespace) -> SparkSession:
    return (
        SparkSession.builder.appName(f"{args.team} - Stage3 Spark ML")
        .master("yarn")
        .config("hive.metastore.uris", args.hive_metastore_uri)
        .config("spark.sql.warehouse.dir", args.warehouse_dir)
        .config("spark.sql.avro.compression.codec", "snappy")
        .enableHiveSupport()
        .getOrCreate()
    )


def normalize_frame(df: DataFrame) -> DataFrame:
    """Ensure the DataFrame has `features` as Vector and `label` as double."""
    if "label" not in df.columns or "features" not in df.columns:
        raise ValueError("Input JSON must contain 'features' and 'label' columns")

    features_type = df.schema["features"].dataType
    if isinstance(features_type, ArrayType):
        normalized = df.withColumn("features", array_to_vector(F.col("features")))
    elif hasattr(features_type, "names") and "values" in features_type.names:
        # Spark JSON export from Vector often produces struct with `values` array.
        normalized = df.withColumn("features", array_to_vector(F.col("features.values")))
    else:
        normalized = df

    return normalized.withColumn("label", F.col("label").cast("double")).select("features", "label")


def build_models() -> List[Tuple[str, str, object, List[Dict]]]:
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", seed=42)
    rf_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [40, 80])
        .addGrid(rf.maxDepth, [8, 14])
        .build()
    )

    nb = NaiveBayes(labelCol="label", featuresCol="features")
    nb_grid = ParamGridBuilder().addGrid(nb.smoothing, [0.5, 1.0, 2.0]).addGrid(
        nb.modelType, ["multinomial", "bernoulli"]
    ).build()

    return [
        ("model1", "random_forest", rf, rf_grid),
        ("model2", "naive_bayes", nb, nb_grid),
    ]


def evaluate_predictions(predictions: DataFrame) -> Dict[str, float]:
    metrics = {}
    for metric_name in ("f1", "accuracy", "weightedPrecision", "weightedRecall"):
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName=metric_name
        )
        metrics[metric_name] = float(evaluator.evaluate(predictions))
    return metrics


def mirror_hdfs_csv(hdfs_dir: str, local_file: str) -> None:
    os.makedirs(os.path.dirname(local_file), exist_ok=True)
    cmd = f'hdfs dfs -cat "{hdfs_dir}"/*.csv > "{local_file}"'
    code = os.system(cmd)
    if code != 0:
        raise RuntimeError(f"Failed to mirror HDFS CSV with command: {cmd}")


def train_and_evaluate(
    train_df: DataFrame, test_df: DataFrame, args: argparse.Namespace
) -> DataFrame:
    results = []
    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1"
    )

    for model_id, model_type, estimator, grid in build_models():
        cv = CrossValidator(
            estimator=estimator,
            estimatorParamMaps=grid,
            evaluator=f1_evaluator,
            numFolds=args.cv_folds,
            parallelism=args.cv_parallelism,
        )
        cv_model = cv.fit(train_df)
        best_model = cv_model.bestModel

        model_hdfs_path = f"{args.hdfs_model_base}/{model_id}"
        best_model.write().overwrite().save(model_hdfs_path)

        predictions = best_model.transform(test_df).select("label", "prediction")
        model_pred_hdfs_dir = f"{args.hdfs_output_base}/{model_id}_predictions"
        predictions.coalesce(1).write.mode("overwrite").option("header", "true").csv(model_pred_hdfs_dir)

        local_pred_file = os.path.join(args.local_output_dir, f"{model_id}_predictions.csv")
        mirror_hdfs_csv(model_pred_hdfs_dir, local_pred_file)

        metrics = evaluate_predictions(predictions)
        results.append(
            (
                model_id,
                model_type,
                metrics["f1"],
                metrics["accuracy"],
                metrics["weightedPrecision"],
                metrics["weightedRecall"],
            )
        )

    spark = train_df.sparkSession
    return spark.createDataFrame(
        results,
        ["model", "model_type", "f1", "accuracy", "weighted_precision", "weighted_recall"],
    )


def main() -> None:
    args = parse_args()
    spark = build_spark_session(args)
    spark.sparkContext.setLogLevel("WARN")

    train_df = normalize_frame(spark.read.json(args.train_path))
    test_df = normalize_frame(spark.read.json(args.test_path))

    evaluation_df = train_and_evaluate(train_df, test_df, args)
    evaluation_hdfs_dir = f"{args.hdfs_output_base}/evaluation"
    evaluation_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(evaluation_hdfs_dir)
    mirror_hdfs_csv(evaluation_hdfs_dir, os.path.join(args.local_output_dir, "evaluation.csv"))

    spark.stop()


if __name__ == "__main__":
    main()
