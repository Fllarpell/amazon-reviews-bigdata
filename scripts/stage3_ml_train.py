#!/usr/bin/env python3
"""Stage III distributed Spark ML training on YARN.

This script is designed to be launched only with spark-submit on YARN.
It reads split datasets from JSON, trains/tunes multiple Spark ML models,
stores predictions and evaluation to HDFS, and mirrors CSV artifacts locally.
"""

import argparse
import csv
import json
import math
import os
from typing import Dict, List, Optional, Tuple

import numpy as np

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import (
    NaiveBayes,
    NaiveBayesModel,
    RandomForestClassifier,
    RandomForestClassificationModel,
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
    parser.add_argument(
        "--feature-manifest",
        default=os.environ.get("STAGE3_FEATURE_MANIFEST") or "",
        help="JSON with 'features' / 'features_nb' name lists from stage3_prepare_split.",
    )
    parser.add_argument(
        "--interpretability-dir",
        default="",
        help="RF feature importance + NB theta/pi tables. Default: <local-output-dir>/interpretability",
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


def _vectorize_column(df: DataFrame, col_name: str) -> DataFrame:
    col_type = df.schema[col_name].dataType
    if isinstance(col_type, ArrayType):
        return df.withColumn(
            col_name,
            array_to_vector(F.expr(f"transform({col_name}, x -> cast(x as double))")),
        )
    if hasattr(col_type, "names") and "values" in col_type.names:
        return df.withColumn(
            col_name,
            array_to_vector(F.expr(f"transform({col_name}.values, x -> cast(x as double))")),
        )
    return df


def normalize_frame(df: DataFrame) -> DataFrame:
    """Ensure `features` (and optional `features_nb`) are Vectors; `label` is double."""
    if "label" not in df.columns or "features" not in df.columns:
        raise ValueError("Input JSON must contain 'features' and 'label' columns")

    out = _vectorize_column(df, "features")
    select_cols = ["features"]
    if "features_nb" in df.columns:
        out = _vectorize_column(out, "features_nb")
        select_cols.append("features_nb")
    select_cols.append("label")
    return out.withColumn("label", F.col("label").cast("double")).select(*select_cols)


def max_feature_dimension(df: DataFrame, col: str = "features") -> int:
    row = df.select(F.max(F.size(vector_to_array(F.col(col)))).alias("mx")).collect()[0]
    mx = row["mx"]
    if mx is None:
        raise ValueError(f"Cannot infer feature dimension for '{col}' (empty frame or all-null)")
    return int(mx)


def pad_feature_vectors(df: DataFrame, dim: int, col: str = "features") -> DataFrame:
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
    return df.withColumn(col, udf_pad(F.col(col)))


def build_models(nb_features_col: str) -> List[Tuple[str, str, object, List[Dict]]]:
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", seed=42)
    rf_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [40, 80])
        .addGrid(rf.maxDepth, [8, 14])
        .build()
    )

    # Multinomial NB: nonnegative, discrete/count-like dimensions. We train on
    # `nb_features_col` (binary flag + OHE only when features_nb exists — see prepare_split).
    nb_scaler = MinMaxScaler(inputCol=nb_features_col, outputCol="scaledFeatures")
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


def resolve_interpret_dir(args: argparse.Namespace) -> str:
    raw = (args.interpretability_dir or "").strip()
    if raw:
        return os.path.abspath(raw)
    return os.path.join(os.path.abspath(args.local_output_dir), "interpretability")


def resolve_feature_manifest_path(args: argparse.Namespace) -> Optional[str]:
    explicit = (args.feature_manifest or "").strip()
    if explicit:
        ap = os.path.abspath(explicit)
        return ap if os.path.isfile(ap) else None
    cand = os.path.join(
        os.path.dirname(os.path.abspath(args.local_output_dir)),
        "data",
        "feature_manifest.json",
    )
    return cand if os.path.isfile(cand) else None


def load_feature_manifest(path: Optional[str]) -> Optional[Dict]:
    if not path:
        return None
    with open(path, encoding="utf-8") as rf:
        return json.load(rf)


def export_rf_feature_importances(
    model: RandomForestClassificationModel,
    out_csv: str,
    names: Optional[List[str]],
) -> None:
    arr = model.featureImportances.toArray()
    ranked = sorted(enumerate(arr), key=lambda x: -x[1])
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as wf:
        w = csv.writer(wf)
        w.writerow(["rank", "feature_index", "feature_name", "importance"])
        for rank, (idx, imp) in enumerate(ranked, start=1):
            label = names[idx] if names is not None and idx < len(names) else f"feature_{idx}"
            w.writerow([rank, idx, label, float(imp)])


def extract_naive_bayes_model(best_model) -> Optional[NaiveBayesModel]:
    if isinstance(best_model, PipelineModel):
        last = best_model.stages[-1]
        if isinstance(last, NaiveBayesModel):
            return last
    return None


def export_nb_tables(
    nb: NaiveBayesModel,
    theta_csv: str,
    pi_csv: str,
    feature_names: Optional[List[str]],
    class_labels: List[float],
) -> None:
    theta = nb.theta
    nr = int(theta.numRows)
    nc = int(theta.numCols)
    if hasattr(theta, "toArray"):
        mat = np.asarray(theta.toArray(), dtype=np.float64)
        if mat.ndim == 1:
            mat = mat.reshape(nr, nc)
    else:
        mat = np.asarray(theta.values, dtype=np.float64).reshape(nr, nc)
    pi_vec = np.asarray(nb.pi.toArray(), dtype=np.float64)

    os.makedirs(os.path.dirname(theta_csv), exist_ok=True)
    with open(theta_csv, "w", newline="", encoding="utf-8") as wf:
        w = csv.writer(wf)
        w.writerow(
            [
                "class_index",
                "label_value",
                "feature_index",
                "feature_name",
                "log_conditional_probability",
            ]
        )
        for i in range(nr):
            lbl = ""
            if i < len(class_labels):
                lbl = class_labels[i]
            for j in range(nc):
                fname = (
                    feature_names[j]
                    if feature_names is not None and j < len(feature_names)
                    else f"feature_nb_{j}"
                )
                w.writerow([i, lbl, j, fname, float(mat[i, j])])

    with open(pi_csv, "w", newline="", encoding="utf-8") as wf:
        w = csv.writer(wf)
        w.writerow(["class_index", "label_value", "log_prior"])
        for i, lp in enumerate(pi_vec):
            lbl = class_labels[i] if i < len(class_labels) else ""
            w.writerow([i, lbl, float(lp)])


def write_interpretability_readme(out_dir: str) -> None:
    path = os.path.join(out_dir, "README_interpretability.txt")
    body = (
        "Random Forest: rf_feature_importance.csv — Gini-based importance per tree feature; "
        "higher values mean more splits using that coordinate.\n\n"
        "Naive Bayes: nb_theta_long.csv — log P(feature_j | class_k) with Laplace smoothing "
        "(Spark multinomial NB parameterization). Compare rows across class_index for the same "
        "feature_name to see which rating level each discrete/OHE dimension favors.\n"
        "nb_class_priors.csv — log pi(class_k).\n\n"
        "NB is trained on features_nb (binary purchase flag + OHE only) when present.\n"
    )
    with open(path, "w", encoding="utf-8") as wf:
        wf.write(body)


def mirror_hdfs_csv(hdfs_dir: str, local_file: str) -> None:
    os.makedirs(os.path.dirname(local_file), exist_ok=True)
    cmd = f'hdfs dfs -cat "{hdfs_dir}"/*.csv > "{local_file}"'
    code = os.system(cmd)
    if code != 0:
        raise RuntimeError(f"Failed to mirror HDFS CSV with command: {cmd}")


def train_and_evaluate(
    train_df: DataFrame,
    test_df: DataFrame,
    args: argparse.Namespace,
    nb_features_col: str,
    feature_manifest: Optional[Dict],
    ordered_class_labels: List[float],
) -> DataFrame:
    results = []
    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1"
    )
    interpret_dir = resolve_interpret_dir(args)
    os.makedirs(interpret_dir, exist_ok=True)
    write_interpretability_readme(interpret_dir)

    for model_id, model_type, estimator, grid in build_models(nb_features_col):
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

        if model_id == "model1" and isinstance(best_model, RandomForestClassificationModel):
            names = (feature_manifest or {}).get("features") if feature_manifest else None
            export_rf_feature_importances(
                best_model,
                os.path.join(interpret_dir, "rf_feature_importance.csv"),
                names,
            )
        elif model_id == "model2":
            nb_model = extract_naive_bayes_model(best_model)
            if nb_model is not None:
                nb_names: Optional[List[str]] = None
                if feature_manifest:
                    nb_names = (
                        feature_manifest.get("features_nb")
                        if nb_features_col == "features_nb"
                        else feature_manifest.get("features")
                    )
                export_nb_tables(
                    nb_model,
                    os.path.join(interpret_dir, "nb_theta_long.csv"),
                    os.path.join(interpret_dir, "nb_class_priors.csv"),
                    nb_names,
                    ordered_class_labels,
                )

    spark = train_df.sparkSession
    return spark.createDataFrame(
        results,
        ["model", "model_type", "f1", "accuracy", "weighted_precision", "weighted_recall"],
    )


def resolve_nb_feature_column(train_df: DataFrame) -> str:
    """Prefer binary + OHE-only vector for Naive Bayes when the split step wrote it."""
    if "features_nb" in train_df.columns:
        return "features_nb"
    return "features"


def main() -> None:
    args = parse_args()
    spark = build_spark_session(args)
    spark.sparkContext.setLogLevel("WARN")

    train_df = normalize_frame(spark.read.json(args.train_path))
    feature_dim = max_feature_dimension(train_df, "features")
    train_df = pad_feature_vectors(train_df, feature_dim, "features")
    if "features_nb" in train_df.columns:
        nb_dim = max_feature_dimension(train_df, "features_nb")
        train_df = pad_feature_vectors(train_df, nb_dim, "features_nb")

    test_df = normalize_frame(spark.read.json(args.test_path))
    test_df = pad_feature_vectors(test_df, feature_dim, "features")
    if "features_nb" in test_df.columns:
        nb_dim = max_feature_dimension(test_df, "features_nb")
        test_df = pad_feature_vectors(test_df, nb_dim, "features_nb")

    nb_features_col = resolve_nb_feature_column(train_df)

    manifest_path = resolve_feature_manifest_path(args)
    feature_manifest = load_feature_manifest(manifest_path)
    ordered_labels = [
        float(r.label)
        for r in train_df.select("label").distinct().orderBy("label").collect()
    ]

    evaluation_df = train_and_evaluate(
        train_df,
        test_df,
        args,
        nb_features_col,
        feature_manifest,
        ordered_labels,
    )
    evaluation_hdfs_dir = f"{args.hdfs_output_base}/evaluation"
    evaluation_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(evaluation_hdfs_dir)
    mirror_hdfs_csv(evaluation_hdfs_dir, os.path.join(args.local_output_dir, "evaluation.csv"))

    spark.stop()


if __name__ == "__main__":
    main()
