# pylint: disable=import-error
import argparse
import logging
import shutil
import sys
from pathlib import Path
from typing import Iterable, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.constants import HDFS_WAREHOUSE_BASE, PROJECT_ROOT
from lib.logutil import setup_logging

try:
    from pyspark.sql import SparkSession as SPARK_SESSION_CLASS
except ModuleNotFoundError:
    SPARK_SESSION_CLASS = None

LOGGER = logging.getLogger(__name__)


def _split_hql_statements(content: str) -> List[str]:
    statements: List[str] = []
    chunk: List[str] = []
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--"):
            continue
        chunk.append(raw_line)
        if line.endswith(";"):
            statement = "\n".join(chunk).strip()
            if statement.endswith(";"):
                statement = statement[:-1]
            if statement:
                statements.append(statement)
            chunk = []
    if chunk:
        statement = "\n".join(chunk).strip()
        if statement:
            statements.append(statement)
    return statements


def _read_query(path: Path) -> str:
    query = path.read_text(encoding="utf-8").strip()
    if query.endswith(";"):
        query = query[:-1]
    return query


def _replace_hiveconf_tokens(content: str, mapping: dict[str, str]) -> str:
    replaced = content
    for key, value in mapping.items():
        replaced = replaced.replace(f"${{hiveconf:{key}}}", value)
    return replaced


def _copy_single_csv_part(temp_dir: Path, target_file: Path) -> None:
    part_files = sorted(temp_dir.glob("part-*.csv"))
    if not part_files:
        raise RuntimeError(f"No CSV part files found in {temp_dir}")
    target_file.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(part_files[0], target_file)


def _spark_session():
    if SPARK_SESSION_CLASS is None:
        raise RuntimeError(
            "pyspark is not installed. Install requirements and run again."
        )
    spark = (
        SPARK_SESSION_CLASS.builder.appName("stage2-hive-spark-eda")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark


def _append_hive_results(path: Path, lines: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        for line in lines:
            handle.write(f"{line}\n")


def run_hive_init(
    spark, hql_path: Path, hive_db_name: str, hive_db_location: str, output_log: Path
) -> None:
    reviews_optimized_path = f"{hive_db_location.rstrip('/')}/reviews_optimized"
    metadata_bucketed_path = f"{hive_db_location.rstrip('/')}/metadata_bucketed"
    mapping = {
        "hive_db_name": hive_db_name,
        "hive_db_location": hive_db_location,
        "hdfs_warehouse_base": HDFS_WAREHOUSE_BASE,
        "hive_reviews_optimized_path": reviews_optimized_path,
        "hive_metadata_bucketed_path": metadata_bucketed_path,
    }
    hql_content = _replace_hiveconf_tokens(
        hql_path.read_text(encoding="utf-8"),
        mapping,
    )
    statements = _split_hql_statements(hql_content)
    _append_hive_results(
        output_log,
        [
            "=== Stage 2 Hive init (Spark SQL) ===",
            f"hive_db_name={hive_db_name}",
            f"hive_db_location={hive_db_location}",
            f"hdfs_warehouse_base={HDFS_WAREHOUSE_BASE}",
        ],
    )
    for idx, statement in enumerate(statements, start=1):
        LOGGER.info("Executing Hive init statement %s/%s", idx, len(statements))
        spark.sql(statement)
    _append_hive_results(output_log, ["Hive init completed", ""])


def run_eda(
    spark,
    hive_db_name: str,
    query_paths: List[Path],
    output_dir: Path,
    output_log: Path,
) -> None:
    spark.sql(f"USE {hive_db_name}")
    _append_hive_results(output_log, ["=== Stage 2 EDA (Spark SQL) ==="])
    for query_path in query_paths:
        query_name = query_path.stem
        query = _read_query(query_path)
        LOGGER.info("Running %s", query_name)
        result_df = spark.sql(query)

        results_table = f"{query_name}_results"
        result_df.write.mode("overwrite").saveAsTable(results_table)

        temp_dir = output_dir / f"_{query_name}_csv_tmp"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            str(temp_dir)
        )
        final_csv = output_dir / f"{query_name}.csv"
        _copy_single_csv_part(temp_dir, final_csv)
        shutil.rmtree(temp_dir, ignore_errors=True)

        count = result_df.count()
        _append_hive_results(
            output_log,
            [f"{query_name}: rows={count}, table={results_table}, csv={final_csv.name}"],
        )

    _append_hive_results(output_log, ["EDA completed", ""])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 2 Spark SQL EDA runner")
    parser.add_argument(
        "--mode",
        choices=["init", "eda", "all"],
        default="all",
        help="Execution mode",
    )
    parser.add_argument(
        "--hive-db-name",
        default="team34_projectdb",
        help="Hive database name",
    )
    parser.add_argument(
        "--hive-db-location",
        default="project/hive/warehouse/team34_projectdb",
        help="Hive database location (HDFS or local path configured in cluster)",
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()

    sql_dir = PROJECT_ROOT / "sql"
    output_dir = PROJECT_ROOT / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_log = output_dir / "hive_results.txt"
    if args.mode in ("init", "all"):
        output_log.write_text("", encoding="utf-8")

    spark = _spark_session()
    try:
        if args.mode in ("init", "all"):
            run_hive_init(
                spark=spark,
                hql_path=sql_dir / "stage2_hive_init.hql",
                hive_db_name=args.hive_db_name,
                hive_db_location=args.hive_db_location,
                output_log=output_log,
            )

        if args.mode in ("eda", "all"):
            run_eda(
                spark=spark,
                hive_db_name=args.hive_db_name,
                query_paths=[sql_dir / "q1.hql", sql_dir / "q2.hql", sql_dir / "q3.hql"],
                output_dir=output_dir,
                output_log=output_log,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
