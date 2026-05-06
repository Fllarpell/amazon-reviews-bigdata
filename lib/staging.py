import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def review_primary_key(row: Dict[str, Any]) -> str:
    parts = "|".join(
        str(row.get(k, ""))
        for k in ("user_id", "parent_asin", "timestamp", "asin")
    )
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()


def _json_cell(value: Any) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def meta_csv_row(obj: Dict[str, Any]) -> List[Any]:
    return [
        obj.get("parent_asin"),
        obj.get("main_category"),
        obj.get("title"),
        obj.get("average_rating"),
        obj.get("rating_number"),
        obj.get("price"),
        obj.get("store"),
        _json_cell(obj.get("features")),
        _json_cell(obj.get("description")),
        _json_cell(obj.get("categories")),
        _json_cell(obj.get("details")),
    ]


def review_csv_row(obj: Dict[str, Any]) -> List[Any]:
    ts_ms = int(obj["timestamp"])
    ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    verified_str = "true" if bool(obj.get("verified_purchase")) else "false"
    rating_val = int(round(float(obj["rating"])))
    return [
        review_primary_key(obj),
        obj.get("parent_asin"),
        obj.get("user_id"),
        obj.get("asin"),
        rating_val,
        obj.get("title"),
        obj.get("text"),
        ts.isoformat(),
        obj.get("helpful_vote"),
        verified_str,
        _json_cell(obj.get("images")),
    ]
