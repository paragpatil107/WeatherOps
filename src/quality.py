"""Transparent, field-level data quality calculations for weather records."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

from .validation import ValidationError, validate_observation


QUALITY_FIELDS = (
    "city", "latitude", "longitude", "observation_timestamp", "temperature_c",
    "relative_humidity", "apparent_temperature_c", "wind_speed_kmh", "weather_code",
    "pressure_hpa", "visibility_m", "data_source", "ingestion_timestamp",
)


def calculate_quality_metrics(
    records: Iterable[Mapping[str, Any]], fields: Sequence[str] = QUALITY_FIELDS,
) -> dict[str, Any]:
    """Return auditable completeness, validity, and consistency metrics.

    Formula: each field completeness is ``present / record_count * 100``;
    completeness is the mean field completeness; validity is the percentage of
    records accepted by ``validate_observation``; consistency is the percentage
    of unique city/timestamp keys. Overall quality is
    ``0.7*completeness + 0.2*validity + 0.1*consistency``.
    """
    rows = [dict(record) for record in records]
    count = len(rows)
    missing_metrics: dict[str, dict[str, float | int]] = {}
    for field in fields:
        missing = sum(1 for row in rows if row.get(field) in (None, ""))
        present = count - missing
        rate = missing / count * 100 if count else 100.0
        missing_metrics[field] = {
            "missing_count": missing, "present_count": present,
            "missing_rate": round(rate, 2), "completeness": round(100 - rate, 2),
        }
    completeness = sum(item["completeness"] for item in missing_metrics.values()) / len(fields) if fields and count else 0.0
    valid = 0
    for row in rows:
        try:
            validate_observation(row)
            valid += 1
        except (ValidationError, TypeError):
            pass
    validity = valid / count * 100 if count else 0.0
    keys = [(row.get("city"), row.get("observation_timestamp")) for row in rows]
    consistency = len(set(keys)) / count * 100 if count else 0.0
    score = 0.7 * completeness + 0.2 * validity + 0.1 * consistency
    return {
        "record_count": count,
        "valid_record_count": valid,
        "completeness_score": round(completeness, 2),
        "validity_score": round(validity, 2),
        "consistency_score": round(consistency, 2),
        "quality_score": round(score, 2),
        "missing_metrics": missing_metrics,
        "field_metrics": missing_metrics,
        "overall_score": round(score, 2),
        "score_available": bool(count),
        "score_note": "Calculated from accepted normalized records." if count else "Not calculated: no accepted normalized records were available.",
        "formula": "quality_score = 0.7*completeness + 0.2*validity + 0.1*consistency; percentages are 0-100",
    }


def build_quality_report(records: Iterable[Mapping[str, Any]], run_id: int | None = None) -> dict[str, Any]:
    report = calculate_quality_metrics(records)
    report["run_id"] = run_id
    report["generated_at"] = datetime.now(timezone.utc).isoformat()
    return report


# Compatibility aliases for callers that prefer verb-based names.
calculate_quality_score = calculate_quality_metrics
generate_quality_report = build_quality_report
