import csv
import json
import logging
import os
import sys
from pathlib import Path
from typing import Iterator, Optional, Set, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.constants import (
    DATA_RAW_DIR,
    DATA_STAGING_DIR,
    DOWNLOAD_CHUNK_BYTES,
    HTTP_TIMEOUT_SEC,
    JSONL_LINE_LIMIT,
    META_JSONL_URL,
    RAW_META_FILENAME,
    RAW_REVIEWS_FILENAME,
    REVIEWS_JSONL_URL,
    STAGING_METADATA_CSV,
    STAGING_REVIEWS_CSV,
)
from lib.logutil import setup_logging
from lib.staging import meta_csv_row, review_csv_row

logger = logging.getLogger(__name__)

META_HEADER = [
    "parent_asin",
    "main_category",
    "title",
    "average_rating",
    "rating_number",
    "price",
    "store",
    "features_text",
    "description_text",
    "categories_json",
    "details_json",
]
REVIEWS_HEADER = [
    "review_id",
    "parent_asin",
    "user_id",
    "asin",
    "rating",
    "review_title",
    "review_text",
    "review_timestamp",
    "helpful_vote",
    "verified_purchase",
    "images_json",
]


def _force_download() -> bool:
    return os.environ.get("FORCE_RAW_REDOWNLOAD", "").lower() in ("1", "true", "yes")


def _line_limit() -> Optional[int]:
    if JSONL_LINE_LIMIT is None or str(JSONL_LINE_LIMIT).strip() == "":
        return None
    return int(JSONL_LINE_LIMIT)


def _stream_download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0 and not _force_download():
        logger.info("reuse existing raw file %s", dest)
        return
    logger.info("downloading %s -> %s", url, dest)
    with requests.get(url, stream=True, timeout=HTTP_TIMEOUT_SEC) as resp:
        resp.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".partial")
        with open(tmp, "wb") as handle:
            for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if chunk:
                    handle.write(chunk)
        tmp.replace(dest)
    logger.info("saved %s bytes", dest.stat().st_size)


def _iter_jsonl(path: Path, limit: Optional[int]) -> Iterator[dict]:
    count = 0
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)
            count += 1
            if limit is not None and count >= limit:
                break


def _write_metadata_csv(
    meta_path: Path,
    out_path: Path,
    limit: Optional[int],
) -> Set[str]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    parents: Set[str] = set()
    rows = 0
    with open(out_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(META_HEADER)
        for obj in _iter_jsonl(meta_path, limit):
            pid = obj.get("parent_asin")
            if not pid:
                continue
            parents.add(str(pid))
            writer.writerow(meta_csv_row(obj))
            rows += 1
    logger.info("metadata csv rows=%s unique_parent_asin=%s", rows, len(parents))
    return parents


def _write_reviews_csv(
    reviews_path: Path,
    out_path: Path,
    parents: Set[str],
    limit: Optional[int],
) -> Tuple[int, int]:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    skipped = 0
    deduped = 0
    seen_keys: Set[Tuple[str, str]] = set()
    with open(out_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(REVIEWS_HEADER)
        for obj in _iter_jsonl(reviews_path, limit):
            pid = obj.get("parent_asin")
            if not pid or str(pid) not in parents:
                skipped += 1
                continue
            row = review_csv_row(obj)
            key = (str(row[1]), str(row[0]))
            if key in seen_keys:
                deduped += 1
                continue
            seen_keys.add(key)
            writer.writerow(row)
            written += 1
    logger.info(
        "reviews csv rows=%s skipped_missing_meta=%s deduped=%s",
        written,
        skipped,
        deduped,
    )
    return written, skipped


def main() -> None:
    setup_logging()
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATA_STAGING_DIR.mkdir(parents=True, exist_ok=True)
    lim = _line_limit()
    if lim is not None:
        logger.warning("JSONL_LINE_LIMIT=%s enabled", lim)
    raw_reviews = DATA_RAW_DIR / RAW_REVIEWS_FILENAME
    raw_meta = DATA_RAW_DIR / RAW_META_FILENAME
    _stream_download(REVIEWS_JSONL_URL, raw_reviews)
    _stream_download(META_JSONL_URL, raw_meta)
    meta_csv = DATA_STAGING_DIR / STAGING_METADATA_CSV
    reviews_csv = DATA_STAGING_DIR / STAGING_REVIEWS_CSV
    parents = _write_metadata_csv(raw_meta, meta_csv, lim)
    _write_reviews_csv(raw_reviews, reviews_csv, parents, lim)


if __name__ == "__main__":
    main()
