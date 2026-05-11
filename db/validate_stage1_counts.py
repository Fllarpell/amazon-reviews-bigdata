import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.constants import DATA_STAGING_DIR, STAGING_METADATA_CSV, STAGING_REVIEWS_CSV
from lib.dbutil import connect
from lib.logutil import setup_logging

LOGGER = logging.getLogger(__name__)


def _count_csv_rows(path: Path) -> int:
    total = 0
    with open(path, encoding="utf-8", errors="replace") as handle:
        for _ in handle:
            total += 1
    return max(0, total - 1)


def _fetch_table_count(table_name: str) -> int:
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"No count returned for table {table_name}")
            return int(row[0])
    finally:
        conn.close()


def main() -> None:
    setup_logging()
    metadata_csv = DATA_STAGING_DIR / STAGING_METADATA_CSV
    reviews_csv = DATA_STAGING_DIR / STAGING_REVIEWS_CSV

    if not metadata_csv.is_file():
        raise FileNotFoundError(metadata_csv)
    if not reviews_csv.is_file():
        raise FileNotFoundError(reviews_csv)

    metadata_csv_rows = _count_csv_rows(metadata_csv)
    reviews_csv_rows = _count_csv_rows(reviews_csv)

    metadata_db_rows = _fetch_table_count("metadata")
    reviews_db_rows = _fetch_table_count("reviews")

    LOGGER.info(
        "metadata rows: csv=%s db=%s",
        metadata_csv_rows,
        metadata_db_rows,
    )
    LOGGER.info(
        "reviews rows: csv=%s db=%s",
        reviews_csv_rows,
        reviews_db_rows,
    )

    if metadata_csv_rows != metadata_db_rows:
        raise ValueError(
            f"metadata row count mismatch: csv={metadata_csv_rows}, db={metadata_db_rows}"
        )
    if reviews_csv_rows != reviews_db_rows:
        raise ValueError(
            f"reviews row count mismatch: csv={reviews_csv_rows}, db={reviews_db_rows}"
        )

    LOGGER.info("Stage 1 row-count validation passed")


if __name__ == "__main__":
    main()
