import logging
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

REFERENCE_SCHEMA_DIR = PROJECT_ROOT / "reference" / "schema"
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_STAGING_DIR = PROJECT_ROOT / "data" / "staging"

RAW_REVIEWS_FILENAME = "Appliances.jsonl"
RAW_META_FILENAME = "meta_Appliances.jsonl"
STAGING_REVIEWS_CSV = "reviews.csv"
STAGING_METADATA_CSV = "metadata.csv"

REVIEWS_JSONL_URL = (
    "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/"
    "resolve/main/raw/review_categories/Appliances.jsonl"
)
META_JSONL_URL = (
    "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/"
    "resolve/main/raw/meta_categories/meta_Appliances.jsonl"
)

DOWNLOAD_CHUNK_BYTES = 1024 * 1024
HTTP_TIMEOUT_SEC = int(os.environ.get("HTTP_TIMEOUT_SEC", "600"))

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S"
LOG_LEVEL_NAME = os.environ.get("LOG_LEVEL", "INFO")
LOG_LEVEL = getattr(logging, LOG_LEVEL_NAME.upper(), logging.INFO)

EXPECTED_MIN_REVIEW_LINES = int(os.environ.get("EXPECTED_MIN_REVIEW_LINES", "2000000"))
EXPECTED_MIN_METADATA_LINES = int(os.environ.get("EXPECTED_MIN_METADATA_LINES", "10000"))
JSONL_LINE_LIMIT = os.environ.get("JSONL_LINE_LIMIT")

PGUSER = os.environ.get("PGUSER", "pipeline_app")
PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGDATABASE = os.environ.get("PGDATABASE", "review_analytics")
PGPASSWORD_FILE = PROJECT_ROOT / "secrets" / ".psql.pass"

MIGRATIONS_DIR = PROJECT_ROOT / "migrations" / "versions"
PIPELINE_SCHEMA = "pipeline"
SCHEMA_MIGRATIONS_TABLE = "schema_migrations"

HDFS_WAREHOUSE_BASE = os.environ.get("HDFS_WAREHOUSE_BASE", "project/warehouse")
HDFS_REVIEWS_DIR = f"{HDFS_WAREHOUSE_BASE}/reviews"
HDFS_METADATA_DIR = f"{HDFS_WAREHOUSE_BASE}/metadata"

REQUIRED_REVIEW_JSON_KEYS = (
    "rating",
    "title",
    "text",
    "asin",
    "parent_asin",
    "user_id",
    "timestamp",
)
REQUIRED_META_JSON_KEYS = ("parent_asin", "title")

DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
ML_INPUT_REVIEWS_CSV = DATA_STAGING_DIR / STAGING_REVIEWS_CSV
ML_OUTPUT_PROCESSED_CSV = DATA_PROCESSED_DIR / "ml_dataset.csv"
ML_OUTPUT_TRAIN_JSON = PROJECT_ROOT / "data" / "train.json"
ML_OUTPUT_TEST_JSON = PROJECT_ROOT / "data" / "test.json"

ML_RANDOM_SEED = int(os.environ.get("ML_RANDOM_SEED", "34"))
ML_TRAIN_RATIO = float(os.environ.get("ML_TRAIN_RATIO", "0.8"))
ML_ALLOWED_RATINGS = (1, 2, 3, 4, 5)
ML_REQUIRED_COLUMNS = (
    "review_id",
    "parent_asin",
    "user_id",
    "rating",
    "review_text",
    "review_timestamp",
)
ML_MIN_REVIEW_TEXT_LEN = int(os.environ.get("ML_MIN_REVIEW_TEXT_LEN", "5"))
ML_MAX_REVIEW_TEXT_LEN = int(os.environ.get("ML_MAX_REVIEW_TEXT_LEN", "5000"))
