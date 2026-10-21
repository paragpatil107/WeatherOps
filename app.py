"""Flask dashboard for the automated weather data pipeline."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
import json
import logging
import os
from pathlib import Path
from threading import Lock
from typing import Any, Optional

from flask import Flask, abort, flash, jsonify, redirect, render_template, request, Response, send_file, url_for

from src.air_quality import AirQualityExtractor
from src.database import WeatherDatabase
from src.extraction import WeatherExtractor
from src.export import export_csv, export_report_csv, export_json
from src.pipeline import CITY_LOCATIONS, WeatherPipeline
from src.reports import build_activity_report, export_activity_csv


BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8"), logging.StreamHandler()],
)

app = Flask(__name__)
app.config.update(
    SECRET_KEY=os.environ.get("FLASK_SECRET_KEY", "change-this-development-key"),
    DATABASE_PATH=os.environ.get("WEATHER_DB_PATH", str(BASE_DIR / "data" / "weather.db")),
    OBSERVATIONS_PER_PAGE=10,
    MAX_QUERY_ROWS=1000,
    MAX_RUN_ROWS=100,
    MAX_LOG_LINES=250,
    ENABLE_AIR_QUALITY=os.environ.get("ENABLE_AIR_QUALITY", "1") == "1",
)
database = WeatherDatabase(app.config["DATABASE_PATH"])
pipeline_lock = Lock()

ANALYTICS_METRICS: dict[str, dict[str, str]] = {
    "temperature": {"label": "Temperature", "field": "temperature_c", "unit": "°C", "color": "#73b7ff"},
    "humidity": {"label": "Humidity", "field": "relative_humidity", "unit": "%", "color": "#68e0c4"},
    "wind": {"label": "Wind speed", "field": "wind_speed_kmh", "unit": "km/h", "color": "#b4a3ff"},
    "pressure": {"label": "Pressure", "field": "pressure_hpa", "unit": "hPa", "color": "#ffba7b"},
    "visibility": {"label": "Visibility", "field": "visibility_m", "unit": "m", "color": "#f28fa8"},
    "conditions": {"label": "Conditions", "field": "weather_code", "unit": "code", "color": "#f5d27a"},
}
WEATHER_CONDITIONS = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Rime fog", 51: "Light drizzle", 53: "Drizzle",
    55: "Heavy drizzle", 61: "Light rain", 63: "Rain", 65: "Heavy rain",
    71: "Light snow", 73: "Snow", 75: "Heavy snow", 80: "Rain showers",
    81: "Rain showers", 82: "Heavy showers", 95: "Thunderstorm", 96: "Thunderstorm + hail",
    99: "Thunderstorm + hail",
}


def get_database() -> WeatherDatabase:
    """Return the shared repository and ensure tables exist."""
    database.initialize()
    return database


def _pipeline_result_payload(result: object) -> dict:
    if is_dataclass(result):
        return asdict(result)  # type: ignore[arg-type]
    if hasattr(result, "__dict__"):
        return vars(result)
    return {"result": str(result)}


@app.template_filter("pretty_date")
def pretty_date(value: object) -> str:
    if value in (None, ""):
        return "—"
    text = str(value).replace("T", " ").replace("+00:00", " UTC")
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.strftime("%b %d, %Y %H:%M UTC").replace(" 0", " ", 1)
    except (TypeError, ValueError):
        return text


@app.template_filter("status_label")
def status_label(value: object) -> str:
    return str(value or "unknown").replace("_", " ").title()


@app.template_filter("aqi_status")
def aqi_status(value: object) -> str:
    """Classify US AQI using the official six-category breakpoints."""
    try:
        score = float(str(value))
    except (TypeError, ValueError):
        return "unavailable"
    if score <= 50:
        return "good"
    if score <= 100:
        return "moderate"
    if score <= 150:
        return "unhealthy-sensitive"
    if score <= 200:
        return "unhealthy"
    if score <= 300:
        return "very-unhealthy"
    return "hazardous"


@app.template_filter("condition_name")
def condition_name(value: object) -> str:
    try:
        return WEATHER_CONDITIONS.get(int(float(str(value))), "Unknown")
    except (TypeError, ValueError):
        return "Unknown"


@app.context_processor
def inject_dashboard_context() -> dict:
    return {
        "backend_ready": True,
        "cities": list(CITY_LOCATIONS),
        "analytics_metrics": ANALYTICS_METRICS,
    }


def _validated_filters() -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Validate common list filters before they reach repository SQL."""
    city = request.args.get("city", "", type=str).strip() or None
    start_date = request.args.get("start_date", "", type=str).strip() or None
    end_date = request.args.get("end_date", "", type=str).strip() or None
    if city and city not in CITY_LOCATIONS:
        abort(400, description="Unknown city filter")
    for name, value in (("start_date", start_date), ("end_date", end_date)):
        if value:
            try:
                datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                abort(400, description=f"{name} must use YYYY-MM-DD")
    if start_date and end_date and start_date > end_date:
        abort(400, description="start_date must be before or equal to end_date")
    return city, start_date, end_date


def _validated_search() -> str:
    search = request.args.get("q", "", type=str).strip()
    if len(search) > 120:
        abort(400, description="q must be 120 characters or fewer")
    return search


def _inclusive_end_date(value: Optional[str]) -> Optional[str]:
    """Make a YYYY-MM-DD end filter include the complete UTC calendar day."""
    return f"{value}T23:59:59" if value else None


def _bounded_page() -> int:
    raw_page = request.args.get("page", "1")
    try:
        page = int(raw_page)
    except (TypeError, ValueError):
        abort(400, description="page must be a positive integer")
    if page < 1:
        abort(400, description="page must be a positive integer")
    return min(page, 100_000)


def _latest_air_quality(db: WeatherDatabase) -> list[dict[str, Any]]:
    rows = db.list_air_quality(limit=app.config["MAX_QUERY_ROWS"])
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        latest.setdefault(row["city"], row)
    return list(latest.values())


def _analytics_rows(db: WeatherDatabase, metric: str, city: Optional[str], start_date: Optional[str], end_date: Optional[str]) -> list[dict[str, Any]]:
    rows, _ = db.list_observations(
        limit=app.config["MAX_QUERY_ROWS"], city=city, start_date=start_date, end_date=end_date,
    )
    field = ANALYTICS_METRICS[metric]["field"]
    result = []
    for row in reversed(rows):
        value = row.get(field)
        if value is None:
            continue
        result.append({
            "id": row["id"], "city": row["city"], "timestamp": row["observation_timestamp"],
            "label": f"{row['city']} · {row['observation_timestamp']}", "value": value,
            "condition": WEATHER_CONDITIONS.get(int(float(str(row["weather_code"]))), "Unknown") if row.get("weather_code") is not None else "Unknown",
        })
    return result


def _pipeline_metrics(runs: list[dict[str, Any]]) -> dict[str, Any]:
    completed = [run for run in runs if run.get("status") != "running"]
    successes = [run for run in completed if run.get("status") == "success"]
    durations = [float(run["duration_ms"]) for run in completed if run.get("duration_ms") is not None]
    return {
        "total_runs": len(runs), "successful_runs": len(successes),
        "success_rate": round(len(successes) / len(completed) * 100, 1) if completed else None,
        "records_inserted": sum(int(run.get("records_inserted") or 0) for run in runs),
        "records_rejected": sum(int(run.get("records_rejected") or 0) for run in runs),
        "average_duration_ms": round(sum(durations) / len(durations), 1) if durations else None,
        "duplicates_skipped": sum(int(run.get("duplicates_skipped") or 0) for run in runs),
        "partial_runs": sum(run.get("status") == "partial_success" for run in completed),
        "failed_runs": sum(run.get("status") in {"failure", "failed"} for run in completed),
    }


def _metric_summary(rows: list[dict[str, Any]], metric: str) -> dict[str, Any]:
    field = ANALYTICS_METRICS[metric]["field"]
    values = [row.get(field) for row in rows if row.get(field) is not None]
    numeric = [float(value) for value in values if isinstance(value, (int, float)) and not isinstance(value, bool)]
    latest = rows[-1] if rows else None
    summary: dict[str, Any] = {
        "count": len(numeric),
        "latest": latest.get(field) if latest else None,
        "average": round(sum(numeric) / len(numeric), 2) if numeric else None,
        "minimum": min(numeric) if numeric else None,
        "maximum": max(numeric) if numeric else None,
        "missing": len(rows) - len(numeric),
    }
    if metric == "conditions":
        counts: dict[str, int] = {}
        for value in values:
            label = WEATHER_CONDITIONS.get(int(float(str(value))), "Unknown")
            counts[label] = counts.get(label, 0) + 1
        summary["distribution"] = counts
    return summary


def _read_log_entries(
    search: str = "", severity: str = "", run_id: str = "",
    start_date: Optional[str] = None, end_date: Optional[str] = None,
) -> list[dict[str, str]]:
    """Read a bounded, redacted log slice for the logs page."""
    import re

    path = LOG_DIR / "pipeline.log"
    if not path.exists():
        return []
    pattern = re.compile(r"^(?P<timestamp>\S+\s+\S+)\s+(?P<level>[A-Z]+)\s+(?P<logger>\S+)\s+(?P<message>.*)$")
    secret_pattern = re.compile(r"(?i)(api[_-]?key|token|secret|password)=\S+")
    entries: list[dict[str, str]] = []
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines()[-app.config["MAX_LOG_LINES"]:]:
        match = pattern.match(raw_line)
        entry = match.groupdict() if match else {"timestamp": "", "level": "INFO", "logger": "app", "message": raw_line}
        entry["message"] = secret_pattern.sub(r"\1=[REDACTED]", entry["message"])
        if search and search.lower() not in (entry["message"] + " " + entry["logger"]).lower():
            continue
        if severity and entry["level"] != severity:
            continue
        if run_id and f"run {run_id}" not in entry["message"].lower() and f"run_id={run_id}" not in entry["message"].lower():
            continue
        if start_date and entry["timestamp"][:10] < start_date:
            continue
        if end_date and entry["timestamp"][:10] > end_date:
            continue
        entries.append(entry)
    return list(reversed(entries))


@app.get("/")
def index():
    db = get_database()
    observations, _ = db.list_observations(limit=5)
    latest_run = db.latest_run()
    latest_by_city = db.latest_by_city()
    latest_quality_report = db.latest_quality_report()
    air_quality = _latest_air_quality(db)
    runs = db.list_runs(limit=app.config["MAX_RUN_ROWS"])
    pipeline_metrics = _pipeline_metrics(runs)
    overview = {
        "total_observations": db.count_observations(),
        "latest_run": latest_run,
        "cities": len(latest_by_city),
        "latest_quality_report": latest_quality_report,
        "air_quality_cities": len(air_quality),
        "pipeline_metrics": pipeline_metrics,
        "latest_timestamp": max(
            (row["observation_timestamp"] for row in latest_by_city), default=None
        ),
    }
    return render_template(
        "index.html", overview=overview, observations=observations,
        chart_rows=latest_by_city, city_summaries=latest_by_city,
        air_quality=air_quality, error=None,
    )


@app.get("/observations")
def observations():
    search = _validated_search()
    page = _bounded_page()
    city, start_date, end_date = _validated_filters()
    per_page = app.config["OBSERVATIONS_PER_PAGE"]
    rows, total = get_database().list_observations(
        search, per_page, (page - 1) * per_page, city=city, start_date=start_date,
        end_date=_inclusive_end_date(end_date),
    )
    return render_template(
        "observations.html", observations=rows, total=total, page=page,
        per_page=per_page, has_next=page * per_page < total, search=search,
        city=city, start_date=start_date, end_date=end_date, error=None,
    )


@app.get("/runs")
def runs():
    runs = get_database().list_runs(limit=app.config["MAX_RUN_ROWS"])
    return render_template(
        "runs.html", runs=runs, pipeline_metrics=_pipeline_metrics(runs),
        quality_report_id=request.args.get("quality_report", type=int), error=None,
    )


@app.get("/analytics")
@app.get("/analytics/<metric>")
def analytics(metric: str = "temperature"):
    if metric not in ANALYTICS_METRICS:
        abort(404)
    city, start_date, end_date = _validated_filters()
    db = get_database()
    rows = _analytics_rows(db, metric, city, start_date, _inclusive_end_date(end_date))
    raw_rows, _ = db.list_observations(
        limit=app.config["MAX_QUERY_ROWS"], city=city,
        start_date=start_date, end_date=_inclusive_end_date(end_date),
    )
    summary = _metric_summary(raw_rows, metric)
    return render_template(
        "analytics.html", metric=metric, metric_meta=ANALYTICS_METRICS[metric],
        analytics_rows=rows, raw_rows=raw_rows, summary=summary,
        city=city, start_date=start_date, end_date=end_date,
        error=None,
    )


@app.get("/air-quality")
def air_quality():
    city, start_date, end_date = _validated_filters()
    rows = get_database().list_air_quality(
        city=city, start_date=start_date, end_date=_inclusive_end_date(end_date), limit=app.config["MAX_QUERY_ROWS"],
    )
    latest_by_city: dict[str, dict[str, Any]] = {}
    for row in rows:
        latest_by_city.setdefault(row["city"], row)
    return render_template(
        "air_quality.html", rows=rows, latest_by_city=list(latest_by_city.values()),
        city=city, start_date=start_date, end_date=end_date,
        error=None,
    )


@app.get("/pipeline")
@app.get("/pipeline-metrics")
def pipeline_dashboard():
    runs = get_database().list_runs(limit=app.config["MAX_RUN_ROWS"])
    return render_template("pipeline.html", runs=runs, pipeline_metrics=_pipeline_metrics(runs), error=None)


@app.get("/quality")
@app.get("/data-quality")
@app.get("/quality-reports")
def quality_reports():
    reports = get_database().list_quality_reports(limit=app.config["MAX_RUN_ROWS"])
    raw_run_filter = request.args.get("run_id", "", type=str).strip()
    if raw_run_filter and not raw_run_filter.isdigit():
        abort(400, description="run_id must be a positive integer")
    run_filter = int(raw_run_filter) if raw_run_filter else None
    if run_filter is not None:
        reports = [report for report in reports if report.get("run_id") == run_filter]
    comparison = reports[:2] if request.args.get("compare") == "1" else []
    return render_template(
        "quality_reports.html", reports=reports, latest=reports[0] if reports else None,
        comparison=comparison, run_filter=run_filter, error=None,
    )


def _quality_report_by_id(report_id: int) -> Optional[dict[str, Any]]:
    db = get_database()
    report = db.get_quality_report_by_id(report_id)
    if report is not None:
        return report
    return db.get_quality_report(report_id)


@app.get("/quality-reports/<int:report_id>")
def quality_report_detail(report_id: int):
    report = _quality_report_by_id(report_id)
    if report is None:
        abort(404)
    return render_template("quality_report_detail.html", report=report, error=None)


@app.get("/data-reports")
@app.get("/reports")
def reports():
    db = get_database()
    _, start_date, end_date = _validated_filters()
    activity_report = build_activity_report(db, start_date, _inclusive_end_date(end_date))
    quality_reports = db.list_quality_reports(limit=app.config["MAX_RUN_ROWS"])
    latest = next(
        (
            report for report in quality_reports
            if (not start_date or str(report.get("generated_at", ""))[:10] >= start_date)
            and (not end_date or str(report.get("generated_at", ""))[:10] <= end_date)
        ),
        None,
    )
    metrics = db.observation_metrics(start_date=start_date, end_date=_inclusive_end_date(end_date))
    related_run = db.latest_run()
    return render_template(
        "reports.html", latest=latest, metrics=metrics, activity_report=activity_report,
        start_date=start_date, end_date=end_date, related_run=related_run, error=None,
    )


@app.post("/reports/generate")
def generate_activity_report():
    _, start_date, end_date = _validated_filters()
    report = build_activity_report(get_database(), start_date, _inclusive_end_date(end_date))
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    export_json(report, str(BASE_DIR / "data" / f"weather-report-{stamp}.json"))
    export_activity_csv(report, str(BASE_DIR / "data" / f"weather-report-{stamp}.csv"))
    flash(f"Report generated for {report['period']['start']} through {report['period']['end']}.", "success")
    return redirect(url_for("reports", start_date=start_date, end_date=end_date))


@app.get("/reports.json")
def reports_json():
    _, start_date, end_date = _validated_filters()
    report = build_activity_report(get_database(), start_date, _inclusive_end_date(end_date))
    response = Response(json.dumps(report, indent=2, default=str) + "\n", mimetype="application/json")
    response.headers["Content-Disposition"] = "attachment; filename=weatherops-report.json"
    return response


@app.get("/reports.csv")
def reports_csv():
    _, start_date, end_date = _validated_filters()
    report = build_activity_report(get_database(), start_date, _inclusive_end_date(end_date))
    destination = BASE_DIR / "data" / "weatherops-report.csv"
    export_activity_csv(report, str(destination))
    return send_file(destination, mimetype="text/csv", as_attachment=True, download_name="weatherops-report.csv")


@app.get("/logs")
def logs():
    search = _validated_search()
    severity = request.args.get("severity", "", type=str).upper().strip()
    if severity not in {"", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        abort(400, description="Invalid log severity")
    run_id = request.args.get("run_id", "", type=str).strip()
    start_date = request.args.get("start_date", "", type=str).strip() or None
    end_date = request.args.get("end_date", "", type=str).strip() or None
    for name, value in (("start_date", start_date), ("end_date", end_date)):
        if value:
            try:
                datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                abort(400, description=f"{name} must use YYYY-MM-DD")
    entries = _read_log_entries(search, severity, run_id, start_date, end_date)
    return render_template(
        "logs.html", entries=entries, log_path="logs/pipeline.log", search=search,
        severity=severity, run_id=run_id, start_date=start_date, end_date=end_date, error=None,
    )


@app.get("/logs.txt")
def logs_download():
    search = _validated_search()
    severity = request.args.get("severity", "", type=str).upper().strip()
    if severity not in {"", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        abort(400, description="Invalid log severity")
    run_id = request.args.get("run_id", "", type=str).strip()
    start_date = request.args.get("start_date", "", type=str).strip() or None
    end_date = request.args.get("end_date", "", type=str).strip() or None
    for name, value in (("start_date", start_date), ("end_date", end_date)):
        if value:
            try:
                datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                abort(400, description=f"{name} must use YYYY-MM-DD")
    entries = _read_log_entries(search, severity, run_id, start_date, end_date)
    text = "\n".join(
        f"{entry['timestamp']} {entry['level']} {entry['logger']} {entry['message']}" for entry in entries
    ) + ("\n" if entries else "")
    response = Response(text, mimetype="text/plain")
    response.headers["Content-Disposition"] = "attachment; filename=weatherops-logs.txt"
    return response


@app.get("/export")
@app.get("/data-export")
def data_export():
    city, start_date, end_date = _validated_filters()
    search = _validated_search()
    rows, total = get_database().list_observations(
        search, limit=app.config["MAX_QUERY_ROWS"], city=city,
        start_date=start_date, end_date=_inclusive_end_date(end_date),
    )
    return render_template(
        "export.html", rows=rows, total=total, search=search, city=city,
        start_date=start_date, end_date=end_date, error=None,
    )


@app.get("/observations/<int:observation_id>")
def observation_detail(observation_id: int):
    observation = get_database().get_observation(observation_id)
    if observation is None:
        abort(404)
    return render_template("observation_detail.html", observation=observation, error=None)


@app.get("/observations.csv")
def observations_csv():
    search = _validated_search()
    city, start_date, end_date = _validated_filters()
    rows, _ = get_database().list_observations(
        search, limit=app.config["MAX_QUERY_ROWS"], city=city,
        start_date=start_date, end_date=_inclusive_end_date(end_date),
    )
    destination = BASE_DIR / "data" / "weather-observations.csv"
    export_csv(rows, str(destination))
    return send_file(destination, mimetype="text/csv", as_attachment=True, download_name="weather-observations.csv")


@app.get("/export/observations.csv")
def observations_csv_alias():
    return observations_csv()


@app.get("/observations.json")
@app.get("/export/observations.json")
def observations_json():
    search = _validated_search()
    city, start_date, end_date = _validated_filters()
    rows, _ = get_database().list_observations(
        search, limit=app.config["MAX_QUERY_ROWS"], city=city,
        start_date=start_date, end_date=_inclusive_end_date(end_date),
    )
    response = Response(json.dumps(rows, indent=2, default=str) + "\n", mimetype="application/json")
    response.headers["Content-Disposition"] = "attachment; filename=weather-observations.json"
    return response


@app.get("/quality-reports/<int:report_id>.json")
def quality_report_json(report_id: int):
    report = _quality_report_by_id(report_id)
    if report is None:
        abort(404)
    response = Response(json.dumps(report["report"], indent=2, default=str) + "\n", mimetype="application/json")
    response.headers["Content-Disposition"] = f"attachment; filename=quality-report-{report['id']}.json"
    return response


@app.get("/quality-reports/<int:report_id>.csv")
def quality_report_csv(report_id: int):
    report = _quality_report_by_id(report_id)
    if report is None:
        abort(404)
    destination = BASE_DIR / "data" / f"quality-report-{report['id']}.csv"
    export_report_csv(report["report"], str(destination))
    return send_file(destination, mimetype="text/csv", as_attachment=True, download_name=f"quality-report-{report['id']}.csv")


@app.post("/pipeline/run")
def pipeline_run():
    logger = logging.getLogger("weather-dashboard")
    if not pipeline_lock.acquire(blocking=False):
        message = "A pipeline run is already in progress. Please wait for it to finish."
        if request.is_json:
            return jsonify({"ok": False, "error": message}), 409
        flash(message, "warning")
        return redirect(url_for("runs"))
    try:
        try:
            air_extractor = AirQualityExtractor(logger=logger) if app.config["ENABLE_AIR_QUALITY"] else None
            result = WeatherPipeline(
                WeatherExtractor(logger=logger), get_database(), logger=logger,
                air_quality_extractor=air_extractor,
            ).run(
                csv_path=str(BASE_DIR / "data" / "weather-observations.csv")
            )
        except Exception as exc:
            logger.exception("Unexpected pipeline failure")
            if request.is_json:
                return jsonify({"ok": False, "error": str(exc)}), 500
            flash(f"Pipeline could not complete: {exc}", "error")
            return redirect(url_for("runs"))
        payload = _pipeline_result_payload(result)
        message = (
            f"Run {payload['run_id']} finished with {str(payload['status']).replace('_', ' ')}. "
            f"Inserted {payload['records_inserted']} record(s)."
        )
        if request.is_json:
            payload["quality_report_url"] = (
                url_for("quality_report_detail", report_id=payload["quality_report_id"])
                if payload.get("quality_report_id") else None
            )
            return jsonify({"ok": payload["status"] != "failure", "result": payload}), 200
        flash(message, "success" if payload["status"] == "success" else "warning")
        return redirect(url_for("runs", quality_report=payload.get("quality_report_id")))
    finally:
        pipeline_lock.release()


@app.get("/about")
def about():
    return render_template("about.html")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", "5000")), debug=os.environ.get("FLASK_DEBUG") == "1")
