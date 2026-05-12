#!/usr/bin/env python3
"""Stage III distributed Spark ML training on YARN.

This script is designed to be launched only with spark-submit on YARN.
It reads split datasets from JSON, trains/tunes multiple Spark ML models,
stores predictions and evaluation to HDFS, and mirrors CSV artifacts locally.
"""

import argparse
import math
import os
from typing import Dict, List, Tuple

import numpy as np

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    NaiveBayes,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.functions import array_to_vector, vector_to_array
from pyspark.ml.linalg import VectorUDT, Vectors
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
    parser.add_argument(
        "--cv-folds",
        type=int,
        default=int(os.environ.get("STAGE3_CV_FOLDS", "3")),
    )
    parser.add_argument(
        "--cv-parallelism",
        type=int,
        default=int(os.environ.get("STAGE3_CV_PARALLELISM", "3")),
    )
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
        normalized = df.withColumn(
            "features",
            array_to_vector(F.expr("transform(features, x -> cast(x as double))")),
        )
    elif hasattr(features_type, "names") and "values" in features_type.names:
        normalized = df.withColumn(
            "features",
            array_to_vector(F.expr("transform(features.values, x -> cast(x as double))")),
        )
    else:
        normalized = df

    return normalized.withColumn("label", F.col("label").cast("double")).select("features", "label")


def max_feature_dimension(df: DataFrame) -> int:
    row = df.select(F.max(F.size(vector_to_array(F.col("features")))).alias("mx")).collect()[0]
    mx = row["mx"]
    if mx is None:
        raise ValueError("Cannot infer feature dimension (empty frame or all-null features)")
    return int(mx)


def pad_feature_vectors(df: DataFrame, dim: int) -> DataFrame:
    def pad(vec):
        if vec is None:
            return Vectors.dense([0.0] * dim)
        raw = np.asarray(vec.toArray(), dtype=np.float64)
        n = int(raw.shape[0])
        if n < dim:
            raw = np.concatenate([raw, np.zeros(dim - n, dtype=np.float64)])
        elif n > dim:
            raw = raw[:dim]
        cleaned = np.nan_to_num(raw, nan=0.0, posinf=0.0, neginf=0.0)
        cleaned = np.maximum(cleaned, 0.0)
        out = []
        for x in cleaned.flat:
            v = float(x)
            if not math.isfinite(v) or v < 0.0:
                v = 0.0
            out.append(v)
        return Vectors.dense(out)

    udf_pad = F.udf(pad, VectorUDT())
    return df.withColumn("features", udf_pad(F.col("features")))


def build_models() -> List[Tuple[str, str, object, List[Dict]]]:
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", seed=42)
    rf_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [40, 80])
        .addGrid(rf.maxDepth, [8, 14])
        .build()
    )

    # Multinomial NB expects nonnegative count-like magnitudes; raw mixed-scale
    # numerics + one-hot would dominate (year, price). Scale to [0,1] first.
    nb_scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
    nb = NaiveBayes(
        labelCol="label",
        featuresCol="scaledFeatures",
        modelType="multinomial",
    )
    nb_pipeline = Pipeline(stages=[nb_scaler, nb])
    nb_grid = ParamGridBuilder().addGrid(nb.smoothing, [0.25, 0.5, 1.0, 2.0]).build()

    return [
        ("model1", "random_forest", rf, rf_grid),
        ("model2", "naive_bayes", nb_pipeline, nb_grid),
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
    feature_dim = max_feature_dimension(train_df)
    train_df = pad_feature_vectors(train_df, feature_dim)
    test_df = pad_feature_vectors(normalize_frame(spark.read.json(args.test_path)), feature_dim)

    evaluation_df = train_and_evaluate(train_df, test_df, args)
    evaluation_hdfs_dir = f"{args.hdfs_output_base}/evaluation"
    evaluation_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(evaluation_hdfs_dir)
    mirror_hdfs_csv(evaluation_hdfs_dir, os.path.join(args.local_output_dir, "evaluation.csv"))

    spark.stop()


if __name__ == "__main__":
    main()
