"""CSV export for database-backed weather observations."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - exercised only in minimal installs
    pd = None


CSV_FIELDS = [
    "city", "latitude", "longitude", "observation_timestamp", "temperature_c",
    "relative_humidity", "apparent_temperature_c", "wind_speed_kmh", "weather_code",
    "pressure_hpa", "visibility_m", "data_source", "ingestion_timestamp",
]


def export_csv(observations: Iterable[dict[str, Any]], path: str) -> int:
    """Write actual observation records to a UTF-8 CSV file."""
    rows = list(observations)
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if pd is not None:
        frame = pd.DataFrame(rows, columns=CSV_FIELDS)
        frame.to_csv(destination, index=False, encoding="utf-8", lineterminator="\n")
    else:
        # Keep the core pipeline runnable before optional dependencies are installed.
        with destination.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore", lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
    return len(rows)


def export_json(payload: Any, path: str) -> int:
    """Write a JSON-compatible report or collection and return bytes written."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    destination.write_text(text, encoding="utf-8")
    return len(text.encode("utf-8"))


def export_report_json(report: dict[str, Any], path: str) -> int:
    return export_json(report, path)


def export_report_csv(report: dict[str, Any], path: str) -> int:
    """Export field-level quality metrics as a tidy CSV report."""
    metrics = report.get("missing_metrics", {})
    rows = [
        {"field": field, **(values if isinstance(values, dict) else {"missing_count": values})}
        for field, values in metrics.items()
    ]
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fields = ["field", "missing_count", "present_count", "missing_rate", "completeness"]
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)
