"""Legacy Stage III data preparation for local exploratory runs.

Production Stage III uses ``scripts/stage3.sh`` (Hive feature layer,
``spark-submit`` on YARN).
"""

# pylint: disable=import-error,too-many-locals,too-many-statements
import csv
import hashlib
import json
import logging
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.constants import (
    ML_ALLOWED_RATINGS,
    ML_INPUT_REVIEWS_CSV,
    ML_MAX_REVIEW_TEXT_LEN,
    ML_MIN_REVIEW_TEXT_LEN,
    ML_OUTPUT_PROCESSED_CSV,
    ML_OUTPUT_TEST_JSON,
    ML_OUTPUT_TRAIN_JSON,
    ML_RANDOM_SEED,
    ML_REQUIRED_COLUMNS,
    ML_TRAIN_RATIO,
    PROJECT_ROOT,
)
from lib.logutil import setup_logging

LOGGER = logging.getLogger(__name__)

OUTPUT_PROFILE_BEFORE = PROJECT_ROOT / "output" / "data_profile_before.csv"
OUTPUT_PROFILE_AFTER = PROJECT_ROOT / "output" / "data_profile_after.csv"
OUTPUT_QUALITY_REPORT = PROJECT_ROOT / "output" / "data_quality_report.txt"

PROCESSED_COLUMNS = [
    "review_id",
    "parent_asin",
    "user_id",
    "asin",
    "review_text",
    "review_title",
    "review_timestamp",
    "helpful_vote",
    "verified_purchase",
    "label",
]


def _is_empty(value: str) -> bool:
    return value is None or str(value).strip() == ""


def _safe_int(value: str) -> int:
    if _is_empty(value):
        raise ValueError("empty-int")
    return int(float(str(value).strip()))


def _safe_int_or_default(value: str, default: int = 0) -> int:
    try:
        parsed = _safe_int(value)
    except ValueError:
        return default
    return parsed


def _normalize_bool(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized in ("1", "true", "yes", "y"):
        return "true"
    return "false"


def _review_key(row: Dict[str, str]) -> str:
    return f"{row.get('parent_asin', '').strip()}|{row.get('review_id', '').strip()}"


def _split_bucket(key: str, seed: int) -> float:
    digest = hashlib.sha256(f"{seed}:{key}".encode("utf-8")).hexdigest()
    value = int(digest[:12], 16)
    return value / float(16**12 - 1)


def _profile_before(path: Path) -> Dict[str, object]:  # pylint: disable=too-many-locals
    if not path.is_file():
        raise FileNotFoundError(path)

    total_rows = 0
    null_counts: Counter = Counter()
    rating_distribution: Counter = Counter()
    seen_keys = set()
    duplicate_rows = 0

    text_nonempty_count = 0
    text_len_sum = 0
    text_len_min = None
    text_len_max = None

    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("reviews csv header is missing")

        for row in reader:
            total_rows += 1
            for col in reader.fieldnames:
                if _is_empty(row.get(col, "")):
                    null_counts[col] += 1

            row_key = _review_key(row)
            if row_key in seen_keys:
                duplicate_rows += 1
            else:
                seen_keys.add(row_key)

            try:
                rating = _safe_int(row.get("rating", ""))
                rating_distribution[str(rating)] += 1
            except ValueError:
                rating_distribution["invalid"] += 1

            review_text = (row.get("review_text") or "").strip()
            if review_text:
                text_len = len(review_text)
                text_nonempty_count += 1
                text_len_sum += text_len
                text_len_min = text_len if text_len_min is None else min(text_len_min, text_len)
                text_len_max = text_len if text_len_max is None else max(text_len_max, text_len)

    avg_text_len = 0.0
    if text_nonempty_count > 0:
        avg_text_len = text_len_sum / text_nonempty_count

    return {
        "total_rows": total_rows,
        "null_counts": dict(null_counts),
        "rating_distribution": dict(rating_distribution),
        "duplicate_rows": duplicate_rows,
        "text_nonempty_count": text_nonempty_count,
        "text_len_min": text_len_min or 0,
        "text_len_max": text_len_max or 0,
        "text_len_avg": round(avg_text_len, 4),
    }


def _write_profile_before(profile: Dict[str, object], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["section", "metric", "field", "value"])
        writer.writerow(["global", "total_rows", "", profile["total_rows"]])
        writer.writerow(["global", "duplicate_rows", "review_key", profile["duplicate_rows"]])
        writer.writerow(["text", "nonempty_count", "review_text", profile["text_nonempty_count"]])
        writer.writerow(["text", "min_len", "review_text", profile["text_len_min"]])
        writer.writerow(["text", "max_len", "review_text", profile["text_len_max"]])
        writer.writerow(["text", "avg_len", "review_text", profile["text_len_avg"]])

        total_rows = max(1, int(profile["total_rows"]))
        for col, count in sorted(profile["null_counts"].items()):
            ratio = round(float(count) / total_rows, 6)
            writer.writerow(["nulls", "count", col, count])
            writer.writerow(["nulls", "ratio", col, ratio])

        for label, count in sorted(profile["rating_distribution"].items()):
            writer.writerow(["rating_distribution", "count", label, count])


def _clean_and_split(
    source: Path,
    processed_csv: Path,
    train_json: Path,
    test_json: Path,
) -> Dict[str, object]:  # pylint: disable=too-many-locals,too-many-statements
    processed_csv.parent.mkdir(parents=True, exist_ok=True)
    train_json.parent.mkdir(parents=True, exist_ok=True)
    test_json.parent.mkdir(parents=True, exist_ok=True)

    seen_keys = set()
    drop_reasons: Counter = Counter()
    train_counts: Counter = Counter()
    test_counts: Counter = Counter()

    kept_rows = 0

    with open(source, "r", encoding="utf-8", newline="") as src_handle, open(
        processed_csv, "w", encoding="utf-8", newline=""
    ) as processed_handle, open(
        train_json, "w", encoding="utf-8", newline=""
    ) as train_handle, open(
        test_json, "w", encoding="utf-8", newline=""
    ) as test_handle:
        reader = csv.DictReader(src_handle)
        writer = csv.DictWriter(processed_handle, fieldnames=PROCESSED_COLUMNS)
        writer.writeheader()

        for row in reader:
            missing_required = [col for col in ML_REQUIRED_COLUMNS if _is_empty(row.get(col, ""))]
            if missing_required:
                drop_reasons["missing_required"] += 1
                continue

            row_key = _review_key(row)
            if row_key in seen_keys:
                drop_reasons["duplicate_review_key"] += 1
                continue
            seen_keys.add(row_key)

            try:
                rating = _safe_int(row.get("rating", ""))
            except ValueError:
                drop_reasons["invalid_rating"] += 1
                continue

            if rating not in ML_ALLOWED_RATINGS:
                drop_reasons["rating_out_of_range"] += 1
                continue

            review_text = (row.get("review_text") or "").strip()
            text_len = len(review_text)
            if text_len < ML_MIN_REVIEW_TEXT_LEN:
                drop_reasons["review_text_too_short"] += 1
                continue
            if text_len > ML_MAX_REVIEW_TEXT_LEN:
                drop_reasons["review_text_too_long"] += 1
                continue

            helpful_vote = _safe_int_or_default(row.get("helpful_vote", ""), default=0)
            helpful_vote = max(helpful_vote, 0)

            record = {
                "review_id": row.get("review_id", "").strip(),
                "parent_asin": row.get("parent_asin", "").strip(),
                "user_id": row.get("user_id", "").strip(),
                "asin": row.get("asin", "").strip(),
                "review_text": review_text,
                "review_title": (row.get("review_title") or "").strip(),
                "review_timestamp": row.get("review_timestamp", "").strip(),
                "helpful_vote": helpful_vote,
                "verified_purchase": _normalize_bool(row.get("verified_purchase", "")),
                "label": rating,
            }

            writer.writerow(record)

            split_value = _split_bucket(row_key, ML_RANDOM_SEED)
            if split_value < ML_TRAIN_RATIO:
                train_handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                train_counts[str(rating)] += 1
            else:
                test_handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                test_counts[str(rating)] += 1

            kept_rows += 1

    train_rows = sum(train_counts.values())
    test_rows = sum(test_counts.values())
    return {
        "kept_rows": kept_rows,
        "train_rows": train_rows,
        "test_rows": test_rows,
        "drop_reasons": dict(drop_reasons),
        "train_counts": dict(train_counts),
        "test_counts": dict(test_counts),
    }


def _write_profile_after(stats: Dict[str, object], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["section", "metric", "field", "value"])
        writer.writerow(["global", "kept_rows", "", stats["kept_rows"]])
        writer.writerow(["global", "train_rows", "", stats["train_rows"]])
        writer.writerow(["global", "test_rows", "", stats["test_rows"]])

        kept_rows = max(1, int(stats["kept_rows"]))
        train_ratio = round(float(stats["train_rows"]) / kept_rows, 6)
        test_ratio = round(float(stats["test_rows"]) / kept_rows, 6)
        writer.writerow(["global", "train_ratio", "", train_ratio])
        writer.writerow(["global", "test_ratio", "", test_ratio])

        for reason, count in sorted(stats["drop_reasons"].items()):
            writer.writerow(["drop_reasons", "count", reason, count])

        for label, count in sorted(stats["train_counts"].items()):
            writer.writerow(["class_balance_train", "count", label, count])

        for label, count in sorted(stats["test_counts"].items()):
            writer.writerow(["class_balance_test", "count", label, count])


def _quality_report_lines(
    profile_before: Dict[str, object],
    stats_after: Dict[str, object],
) -> Iterable[str]:
    total_before = int(profile_before["total_rows"])
    kept_rows = int(stats_after["kept_rows"])
    dropped = max(0, total_before - kept_rows)
    yield "=== ML Data Prep Quality Report ==="
    yield f"total_before={total_before}"
    yield f"kept_rows={kept_rows}"
    yield f"dropped_rows={dropped}"
    yield f"train_rows={stats_after['train_rows']}"
    yield f"test_rows={stats_after['test_rows']}"
    yield f"allowed_ratings={','.join(map(str, ML_ALLOWED_RATINGS))}"
    yield (
        "text_len_constraints="
        f"[{ML_MIN_REVIEW_TEXT_LEN},{ML_MAX_REVIEW_TEXT_LEN}]"
    )
    yield "drop_reasons:"
    for reason, count in sorted(stats_after["drop_reasons"].items()):
        yield f"  - {reason}: {count}"


def _write_quality_report(lines: Iterable[str], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", encoding="utf-8") as handle:
        for line in lines:
            handle.write(f"{line}\n")


def main() -> None:
    setup_logging()
    LOGGER.warning(
        "Running legacy local CSV prep helper. Official Stage III flow is scripts/stage3.sh (Hive + YARN)."
    )
    LOGGER.info("Starting ML data prep from %s", ML_INPUT_REVIEWS_CSV)
    profile_before = _profile_before(ML_INPUT_REVIEWS_CSV)
    _write_profile_before(profile_before, OUTPUT_PROFILE_BEFORE)

    stats_after = _clean_and_split(
        source=ML_INPUT_REVIEWS_CSV,
        processed_csv=ML_OUTPUT_PROCESSED_CSV,
        train_json=ML_OUTPUT_TRAIN_JSON,
        test_json=ML_OUTPUT_TEST_JSON,
    )
    _write_profile_after(stats_after, OUTPUT_PROFILE_AFTER)
    _write_quality_report(
        _quality_report_lines(profile_before, stats_after),
        OUTPUT_QUALITY_REPORT,
    )
    LOGGER.info(
        "ML data prep completed: kept=%s train=%s test=%s",
        stats_after["kept_rows"],
        stats_after["train_rows"],
        stats_after["test_rows"],
    )


if __name__ == "__main__":
    main()
