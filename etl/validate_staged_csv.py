import json
import logging
import sys
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.constants import (
    DATA_RAW_DIR,
    DATA_STAGING_DIR,
    EXPECTED_MIN_METADATA_LINES,
    EXPECTED_MIN_REVIEW_LINES,
    JSONL_LINE_LIMIT,
    RAW_META_FILENAME,
    RAW_REVIEWS_FILENAME,
    REQUIRED_META_JSON_KEYS,
    REQUIRED_REVIEW_JSON_KEYS,
    STAGING_METADATA_CSV,
    STAGING_REVIEWS_CSV,
)
from lib.logutil import setup_logging

logger = logging.getLogger(__name__)


def _line_limit() -> Optional[int]:
    if JSONL_LINE_LIMIT is None or str(JSONL_LINE_LIMIT).strip() == "":
        return None
    return int(JSONL_LINE_LIMIT)


def _count_lines(path: Path) -> int:
    total = 0
    with open(path, encoding="utf-8", errors="replace") as handle:
        for _ in handle:
            total += 1
    return total


def _validate_jsonl_sample(path: Path, required: tuple, label: str, max_lines: int = 50) -> None:
    seen = 0
    with open(path, encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            missing = [k for k in required if k not in obj]
            if missing:
                raise ValueError(f"{label} missing keys {missing} in first checked rows")
            seen += 1
            if seen >= max_lines:
                break
    logger.info("%s jsonl schema sample ok (%s lines)", label, seen)


def _validate_csv_nonempty(path: Path, min_data_rows: int) -> int:
    total = _count_lines(path)
    data_rows = max(0, total - 1)
    if data_rows < min_data_rows:
        raise ValueError(f"{path} has {data_rows} data rows, expected >= {min_data_rows}")
    logger.info("%s rows=%s", path.name, data_rows)
    return data_rows


def main() -> None:
    setup_logging()
    lim = _line_limit()
    min_rev = 1 if lim is not None else EXPECTED_MIN_REVIEW_LINES
    min_meta = 1 if lim is not None else EXPECTED_MIN_METADATA_LINES
    raw_reviews = DATA_RAW_DIR / RAW_REVIEWS_FILENAME
    raw_meta = DATA_RAW_DIR / RAW_META_FILENAME
    if not raw_reviews.is_file() or raw_reviews.stat().st_size == 0:
        raise FileNotFoundError(raw_reviews)
    if not raw_meta.is_file() or raw_meta.stat().st_size == 0:
        raise FileNotFoundError(raw_meta)
    _validate_jsonl_sample(raw_reviews, REQUIRED_REVIEW_JSON_KEYS, "reviews")
    _validate_jsonl_sample(raw_meta, REQUIRED_META_JSON_KEYS, "metadata")
    rev_lines = _count_lines(raw_reviews)
    meta_lines = _count_lines(raw_meta)
    logger.info("raw line counts reviews=%s metadata=%s", rev_lines, meta_lines)
    if lim is None:
        if rev_lines < EXPECTED_MIN_REVIEW_LINES:
            raise ValueError(f"reviews jsonl too small: {rev_lines}")
        if meta_lines < EXPECTED_MIN_METADATA_LINES:
            raise ValueError(f"metadata jsonl too small: {meta_lines}")
    meta_csv = DATA_STAGING_DIR / STAGING_METADATA_CSV
    reviews_csv = DATA_STAGING_DIR / STAGING_REVIEWS_CSV
    _validate_csv_nonempty(meta_csv, min_meta)
    _validate_csv_nonempty(reviews_csv, min_rev)


if __name__ == "__main__":
    main()
