"""Validation errors expose field names, never private transaction values."""

import json
import math
from datetime import datetime, timezone


REQUIRED_COLUMNS = (
    "transaction_time", "merch", "cat_id", "amount", "name_1", "name_2",
    "gender", "street", "one_city", "us_state", "post_code", "lat", "lon",
    "population_city", "jobs", "merchant_lat", "merchant_lon",
)
NUMERIC_COLUMNS = ("amount", "lat", "lon", "population_city", "merchant_lat", "merchant_lon")
SCORE_COLUMNS = frozenset(("transaction_id", "score", "fraud_flag", "model_version"))
SCORE_CONTEXT_COLUMNS = frozenset(("us_state", "merch", "cat_id", "amount", "transaction_time", "processed_at", "emitted_at"))


class ValidationError(ValueError):
    """A malformed event that can safely be diverted to the dead-letter topic."""


def parse_json(payload: bytes | str | None) -> dict:
    try:
        value = json.loads(payload)
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValidationError("Message must contain valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ValidationError("Message must be one JSON object")
    return value


def _transaction_id(value: object) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise ValidationError("transaction_id must be a nonempty string of at most 128 characters")
    return value


def _finite_number(value: object, field: str) -> float:
    if isinstance(value, bool):
        raise ValidationError(f"{field} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValidationError(f"{field} must be a finite number") from exc
    if not math.isfinite(number):
        raise ValidationError(f"{field} must be a finite number")
    return number


def normalize_timestamp(value: object, field: str, *, require_timezone: bool = False) -> str:
    """Normalize transport timestamps to UTC; historical CSV dates assume UTC."""
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field} must be a valid ISO date/time")
    try:
        timestamp = datetime.fromisoformat(value)
        if timestamp.tzinfo is None:
            if require_timezone:
                raise ValueError("Timezone is required")
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return timestamp.astimezone(timezone.utc).isoformat()
    except (ValueError, OverflowError) as exc:
        raise ValidationError(f"{field} must be a valid ISO date/time" + (" with timezone" if require_timezone else "")) from exc


def validate_transaction(value: object) -> dict:
    """Normalize CSV numeric strings, retaining extra columns for feature selection."""
    if not isinstance(value, dict):
        raise ValidationError("Transaction must be one JSON object")
    transaction_id = _transaction_id(value.get("transaction_id"))
    raw_data = value.get("data")
    if not isinstance(raw_data, dict):
        raise ValidationError("data must be an object")
    missing = [column for column in REQUIRED_COLUMNS if column not in raw_data]
    if missing:
        raise ValidationError("Missing required fields: " + ", ".join(missing))
    data = dict(raw_data)
    for field in REQUIRED_COLUMNS:
        if field in NUMERIC_COLUMNS:
            data[field] = _finite_number(data[field], field)
        elif field == "post_code":
            if isinstance(data[field], bool) or not isinstance(data[field], (str, int)):
                raise ValidationError("post_code must be a nonempty string or integer")
            data[field] = str(data[field])
            if not data[field].isascii() or not data[field].isdigit() or len(data[field]) > 5:
                raise ValidationError("post_code must contain 1 to 5 ASCII digits")
        elif not isinstance(data[field], str) or not data[field].strip():
            raise ValidationError(f"{field} must be a nonempty string")
    for field in ("amount", "population_city"):
        if data[field] < 0:
            raise ValidationError(f"{field} must be nonnegative")
    for field, limit in (("lat", 90), ("lon", 180), ("merchant_lat", 90), ("merchant_lon", 180)):
        if not -limit <= data[field] <= limit:
            raise ValidationError(f"{field} is outside the coordinate range")
    # Validate the same UTC conversion used by the score envelope without
    # changing the historical feature value passed to the trained pipeline.
    normalize_timestamp(data["transaction_time"], "transaction_time")
    result = {"transaction_id": transaction_id, "data": data}
    if "emitted_at" in value:
        result["emitted_at"] = normalize_timestamp(value["emitted_at"], "emitted_at", require_timezone=True)
    return result


def validate_score(value: object) -> dict:
    if not isinstance(value, dict) or not SCORE_COLUMNS <= set(value) or set(value) - SCORE_COLUMNS - SCORE_CONTEXT_COLUMNS:
        raise ValidationError("Score must contain transaction_id, score, fraud_flag, model_version and only supported optional context")
    transaction_id = _transaction_id(value["transaction_id"])
    if isinstance(value["score"], bool) or not isinstance(value["score"], (int, float)):
        raise ValidationError("score must be a finite JSON number")
    score = _finite_number(value["score"], "score")
    if not 0 <= score <= 1:
        raise ValidationError("score must be between 0 and 1")
    flag = value["fraud_flag"]
    if type(flag) is not int or flag not in (0, 1):
        raise ValidationError("fraud_flag must be integer 0 or 1")
    version = value["model_version"]
    if not isinstance(version, str) or not version.strip() or len(version) > 128:
        raise ValidationError("model_version must be a nonempty string of at most 128 characters")
    result = {"transaction_id": transaction_id, "score": score, "fraud_flag": flag, "model_version": version}
    for field in SCORE_CONTEXT_COLUMNS & value.keys():
        if field in ("processed_at", "emitted_at", "transaction_time"):
            result[field] = normalize_timestamp(value[field], field, require_timezone=field != "transaction_time")
        elif field == "amount":
            result[field] = _finite_number(value[field], field)
            if result[field] < 0:
                raise ValidationError("amount must be nonnegative")
        else:
            if not isinstance(value[field], str) or not value[field].strip():
                raise ValidationError(f"{field} must be a nonempty string")
            result[field] = value[field].strip()
            if field == "us_state":
                result[field] = result[field].upper()
    return result
