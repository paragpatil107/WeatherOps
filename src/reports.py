"""Helpers for building and exporting operational reports."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional

from .export import export_report_csv, export_report_json
from .quality import build_quality_report


def generate_report(records: Iterable[Mapping[str, Any]], run_id: int | None = None) -> dict[str, Any]:
    return build_quality_report(records, run_id=run_id)


def build_activity_report(
    database: Any,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict[str, Any]:
    """Build a persisted-data summary without presenting it as an audit."""
    observations, _ = database.list_observations(
        limit=1_000_000, start_date=start_date, end_date=end_date,
    )
    runs = database.list_runs(limit=1_000_000, start_date=start_date, end_date=end_date)
    completed = [run for run in runs if run.get("status") != "running"]
    missing_fields = [
        "temperature_c", "relative_humidity", "apparent_temperature_c", "wind_speed_kmh",
        "pressure_hpa", "visibility_m", "weather_code",
    ]
    missing_summary = {
        field: {
            "missing_count": sum(1 for row in observations if row.get(field) is None),
            "missing_rate": round(
                sum(1 for row in observations if row.get(field) is None) / len(observations) * 100, 2
            ) if observations else 100.0,
        }
        for field in missing_fields
    }
    city_counts: dict[str, int] = {}
    for row in observations:
        city_counts[row["city"]] = city_counts.get(row["city"], 0) + 1
    display_end = end_date.split("T", 1)[0] if end_date else None
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "period": {"start": start_date or "earliest persisted data", "end": display_end or "latest persisted data"},
        "scope": "Persisted SQLite records and pipeline history for the selected period.",
        "runs": {
            "total": len(runs),
            "successful": sum(run.get("status") == "success" for run in completed),
            "partial_success": sum(run.get("status") == "partial_success" for run in completed),
            "failed": sum(run.get("status") in {"failure", "failed"} for run in completed),
        },
        "records": {
            "unique_observations": len(observations),
            "fetched": sum(int(run.get("records_fetched") or 0) for run in runs),
            "accepted": sum(int(run.get("records_accepted") or 0) for run in runs),
            "inserted": sum(int(run.get("records_inserted") or 0) for run in runs),
            "rejected": sum(int(run.get("records_rejected") or 0) for run in runs),
            "duplicates_skipped": sum(int(run.get("duplicates_skipped") or 0) for run in runs),
        },
        "errors": [run.get("error_summary") for run in runs if run.get("error_summary")],
        "city_counts": city_counts,
        "missing_values": missing_summary,
        "freshness": observations[0].get("observation_timestamp") if observations else None,
    }


def export_activity_csv(report: Mapping[str, Any], path: str) -> int:
    """Export the human-readable activity summary as a compact CSV."""
    import csv
    from pathlib import Path

    rows = [
        ("generated_at", report.get("generated_at")),
        ("period_start", report.get("period", {}).get("start")),
        ("period_end", report.get("period", {}).get("end")),
        ("runs_total", report.get("runs", {}).get("total")),
        ("runs_successful", report.get("runs", {}).get("successful")),
        ("runs_partial_success", report.get("runs", {}).get("partial_success")),
        ("runs_failed", report.get("runs", {}).get("failed")),
    ]
    for key, value in report.get("records", {}).items():
        rows.append((f"records_{key}", value))
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("metric", "value"))
        writer.writerows(rows)
    return len(rows)


__all__ = [
    "generate_report", "build_activity_report", "export_activity_csv",
    "export_report_json", "export_report_csv",
]
