"""Run migrations, truncate ``reviews`` / ``metadata``, and bulk-load staging CSV."""

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.constants import (
    DATA_STAGING_DIR,
    STAGING_METADATA_CSV,
    STAGING_REVIEWS_CSV,
)
from lib.dbutil import connect
from lib.logutil import setup_logging
from lib.schema_migrations import apply_pending_migrations

logger = logging.getLogger(__name__)

COPY_METADATA = """
COPY metadata (
    parent_asin,
    main_category,
    title,
    average_rating,
    rating_number,
    price,
    store,
    features_text,
    description_text,
    categories_json,
    details_json
) FROM STDIN WITH (FORMAT CSV, HEADER true, NULL '')
"""

COPY_REVIEWS = """
COPY reviews (
    review_id,
    parent_asin,
    user_id,
    asin,
    rating,
    review_title,
    review_text,
    review_timestamp,
    helpful_vote,
    verified_purchase,
    images_json
) FROM STDIN WITH (FORMAT CSV, HEADER true, NULL '')
"""

TRUNCATE_RELATIONS = "TRUNCATE TABLE reviews, metadata CASCADE"


def main() -> None:
    setup_logging()
    meta_csv = DATA_STAGING_DIR / STAGING_METADATA_CSV
    reviews_csv = DATA_STAGING_DIR / STAGING_REVIEWS_CSV
    if not meta_csv.is_file():
        raise FileNotFoundError(meta_csv)
    if not reviews_csv.is_file():
        raise FileNotFoundError(reviews_csv)
    conn_migrate = connect()
    try:
        apply_pending_migrations(conn_migrate)
    finally:
        conn_migrate.close()
    conn = connect(autocommit=False)
    try:
        with conn.cursor() as cur:
            logger.info("truncate metadata and reviews before reload")
            cur.execute(TRUNCATE_RELATIONS)
            logger.info("copy metadata")
            with open(meta_csv, encoding="utf-8") as handle:
                cur.copy_expert(COPY_METADATA, handle)
            logger.info("copy reviews")
            with open(reviews_csv, encoding="utf-8") as handle:
                cur.copy_expert(COPY_REVIEWS, handle)
        conn.commit()
        logger.info("postgres load complete")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
