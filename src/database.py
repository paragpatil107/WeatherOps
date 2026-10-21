"""SQLite persistence, additive migrations, and analytics queries."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Iterator, Optional


class WeatherDatabase:
    """Small SQLite repository with explicit transactions and parameterized SQL."""

    def __init__(self, path: str = "data/weather.db", logger: Optional[logging.Logger] = None) -> None:
        self.path = Path(path)
        self.logger = logger or logging.getLogger(__name__)

    def connect(self) -> sqlite3.Connection:
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(self.path))
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        """Create current tables and apply only additive migrations to old DBs."""
        with self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS weather_observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT NOT NULL,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    observation_timestamp TEXT NOT NULL,
                    temperature_c REAL NOT NULL,
                    relative_humidity REAL NOT NULL,
                    apparent_temperature_c REAL NOT NULL,
                    wind_speed_kmh REAL NOT NULL,
                    weather_code INTEGER NOT NULL,
                    pressure_hpa REAL,
                    visibility_m REAL,
                    data_source TEXT NOT NULL,
                    ingestion_timestamp TEXT NOT NULL,
                    UNIQUE (city, observation_timestamp)
                );
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    records_fetched INTEGER NOT NULL DEFAULT 0,
                    records_accepted INTEGER NOT NULL DEFAULT 0,
                    records_rejected INTEGER NOT NULL DEFAULT 0,
                    records_inserted INTEGER NOT NULL DEFAULT 0,
                    duplicates_skipped INTEGER NOT NULL DEFAULT 0,
                    error_summary TEXT,
                    duration_ms REAL,
                    duration_seconds REAL,
                    stage TEXT,
                    error TEXT
                );
                CREATE TABLE IF NOT EXISTS data_quality_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER,
                    generated_at TEXT NOT NULL,
                    quality_score REAL NOT NULL,
                    completeness_score REAL,
                    validity_score REAL,
                    consistency_score REAL,
                    record_count INTEGER NOT NULL DEFAULT 0,
                    missing_metrics_json TEXT NOT NULL,
                    report_json TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES pipeline_runs(id)
                );
                CREATE TABLE IF NOT EXISTS air_quality_observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER,
                    city TEXT NOT NULL,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    observation_timestamp TEXT NOT NULL,
                    us_aqi REAL,
                    pm10 REAL,
                    pm2_5 REAL,
                    carbon_monoxide REAL,
                    nitrogen_dioxide REAL,
                    sulphur_dioxide REAL,
                    ozone REAL,
                    data_source TEXT NOT NULL,
                    units_json TEXT,
                    ingestion_timestamp TEXT NOT NULL,
                    UNIQUE (city, observation_timestamp),
                    FOREIGN KEY (run_id) REFERENCES pipeline_runs(id)
                );
                CREATE INDEX IF NOT EXISTS idx_weather_city_time
                    ON weather_observations (city, observation_timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started
                    ON pipeline_runs (started_at DESC);
                CREATE INDEX IF NOT EXISTS idx_air_quality_city_time
                    ON air_quality_observations (city, observation_timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_quality_reports_run
                    ON data_quality_reports (run_id, generated_at DESC);
                """
            )
            # Existing Phase 1 databases do not have these columns. ALTER TABLE
            # is deliberately used instead of rebuilding tables, preserving rows.
            self._ensure_column(connection, "weather_observations", "pressure_hpa", "REAL")
            self._ensure_column(connection, "weather_observations", "visibility_m", "REAL")
            self._ensure_column(connection, "pipeline_runs", "duration_ms", "REAL")
            self._ensure_column(connection, "pipeline_runs", "duration_seconds", "REAL")
            self._ensure_column(connection, "pipeline_runs", "stage", "TEXT")
            self._ensure_column(connection, "pipeline_runs", "error", "TEXT")
            self._ensure_column(connection, "air_quality_observations", "units_json", "TEXT")

    def start_run(self) -> int:
        self.initialize()
        with self._connection() as connection:
            cursor = connection.execute(
                "INSERT INTO pipeline_runs (started_at, status, stage) VALUES (?, ?, ?)",
                (self._utc_now(), "running", "initializing"),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("SQLite did not return a pipeline run id")
            return int(cursor.lastrowid)

    def finish_run(
        self,
        run_id: int,
        status: str,
        records_fetched: int,
        records_accepted: int,
        records_rejected: int,
        records_inserted: int,
        duplicates_skipped: int,
        error_summary: Optional[str] = None,
        duration_ms: Optional[float] = None,
        stage: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        self.initialize()
        with self._connection() as connection:
            connection.execute(
                """
                UPDATE pipeline_runs
                SET finished_at = ?, status = ?, records_fetched = ?, records_accepted = ?,
                    records_rejected = ?, records_inserted = ?, duplicates_skipped = ?,
                    error_summary = ?, duration_ms = ?, duration_seconds = ?, stage = ?, error = ?
                WHERE id = ?
                """,
                (
                    self._utc_now(), status, records_fetched, records_accepted,
                    records_rejected, records_inserted, duplicates_skipped, error_summary,
                    duration_ms, duration_ms / 1000.0 if duration_ms is not None else None,
                    stage, error if error is not None else error_summary, run_id,
                ),
            )

    def update_run_stage(self, run_id: int, stage: str, error: Optional[str] = None) -> None:
        """Persist the current pipeline stage without closing the run."""
        self.initialize()
        with self._connection() as connection:
            connection.execute(
                "UPDATE pipeline_runs SET stage = ?, error = ? WHERE id = ?",
                (stage, error, run_id),
            )

    def insert_observations(self, observations: Iterable[dict[str, Any]]) -> tuple[int, int]:
        """Insert records and return ``(inserted, duplicates_skipped)``."""
        self.initialize()
        rows = list(observations)
        if not rows:
            return 0, 0
        with self._connection() as connection:
            before = connection.total_changes
            connection.executemany(
                """
                INSERT OR IGNORE INTO weather_observations (
                    city, latitude, longitude, observation_timestamp, temperature_c,
                    relative_humidity, apparent_temperature_c, wind_speed_kmh,
                    weather_code, pressure_hpa, visibility_m, data_source, ingestion_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["city"], row["latitude"], row["longitude"], row["observation_timestamp"],
                        row["temperature_c"], row["relative_humidity"], row["apparent_temperature_c"],
                        row["wind_speed_kmh"], row["weather_code"], row.get("pressure_hpa"),
                        row.get("visibility_m"), row["data_source"], row["ingestion_timestamp"],
                    )
                    for row in rows
                ],
            )
            inserted = connection.total_changes - before
        return inserted, len(rows) - inserted

    def insert_air_quality_observations(self, observations: Iterable[dict[str, Any]]) -> tuple[int, int]:
        self.initialize()
        rows = list(observations)
        if not rows:
            return 0, 0
        with self._connection() as connection:
            before = connection.total_changes
            connection.executemany(
                """
                INSERT OR IGNORE INTO air_quality_observations (
                    run_id, city, latitude, longitude, observation_timestamp, us_aqi,
                    pm10, pm2_5, carbon_monoxide, nitrogen_dioxide, sulphur_dioxide,
                    ozone, data_source, units_json, ingestion_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.get("run_id"), row["city"], row["latitude"], row["longitude"],
                        row["observation_timestamp"], row.get("us_aqi"), row.get("pm10"),
                        row.get("pm2_5"), row.get("carbon_monoxide"), row.get("nitrogen_dioxide"),
                        row.get("sulphur_dioxide"), row.get("ozone"), row["data_source"],
                        json.dumps(row.get("units", {}), sort_keys=True), row["ingestion_timestamp"],
                    )
                    for row in rows
                ],
            )
            inserted = connection.total_changes - before
        return inserted, len(rows) - inserted

    def save_quality_report(self, report: dict[str, Any], run_id: Optional[int] = None) -> int:
        self.initialize()
        with self._connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO data_quality_reports (
                    run_id, generated_at, quality_score, completeness_score,
                    validity_score, consistency_score, record_count,
                    missing_metrics_json, report_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id, report.get("generated_at", self._utc_now()), report.get("quality_score", 0.0),
                    report.get("completeness_score"), report.get("validity_score"),
                    report.get("consistency_score"), report.get("record_count", 0),
                    json.dumps(report.get("missing_metrics", {}), sort_keys=True),
                    json.dumps(report, sort_keys=True),
                ),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("SQLite did not return a quality report id")
            return int(cursor.lastrowid)

    def list_quality_reports(self, limit: int = 25) -> list[dict[str, Any]]:
        self.initialize()
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT * FROM data_quality_reports ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["missing_metrics"] = json.loads(item.pop("missing_metrics_json"))
            item["report"] = json.loads(item.pop("report_json"))
            result.append(item)
        return result

    def latest_quality_report(self) -> Optional[dict[str, Any]]:
        reports = self.list_quality_reports(1)
        return reports[0] if reports else None

    def get_quality_report(self, run_id: int) -> Optional[dict[str, Any]]:
        reports = [report for report in self.list_quality_reports(1_000_000) if report.get("run_id") == run_id]
        return reports[0] if reports else None

    def list_air_quality(
        self, city: Optional[str] = None, limit: int = 25,
        start_date: Optional[str] = None, end_date: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        self.initialize()
        clauses: list[str] = []
        params: list[Any] = [city] if city else []
        if city:
            clauses.append("city = ?")
        if start_date:
            clauses.append("observation_timestamp >= ?")
            params.append(start_date)
        if end_date:
            clauses.append("observation_timestamp <= ?")
            params.append(end_date)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        with self._connection() as connection:
            rows = connection.execute(
                f"SELECT * FROM air_quality_observations {where} "
                "ORDER BY observation_timestamp DESC, id DESC LIMIT ?",
                [*params, limit],
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            if item.get("units_json"):
                item["units"] = json.loads(item["units_json"])
            result.append(item)
        return result

    def count_observations(self) -> int:
        self.initialize()
        with self._connection() as connection:
            return int(connection.execute("SELECT COUNT(*) FROM weather_observations").fetchone()[0])

    def list_observations(
        self, search: str = "", limit: int = 25, offset: int = 0,
        city: Optional[str] = None, start_date: Optional[str] = None,
        end_date: Optional[str] = None, date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> tuple[list[dict[str, Any]], int]:
        self.initialize()
        start_date = start_date or date_from
        end_date = end_date or date_to
        clauses: list[str] = []
        params: list[Any] = []
        if search:
            search_term = f"%{search}%"
            clauses.append("(city LIKE ? OR observation_timestamp LIKE ? OR data_source LIKE ?)")
            params.extend([search_term, search_term, search_term])
        if city:
            clauses.append("city = ?")
            params.append(city)
        if start_date:
            clauses.append("observation_timestamp >= ?")
            params.append(start_date)
        if end_date:
            clauses.append("observation_timestamp <= ?")
            params.append(end_date)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        with self._connection() as connection:
            total = int(connection.execute(f"SELECT COUNT(*) FROM weather_observations {where}", params).fetchone()[0])
            rows = connection.execute(
                f"SELECT * FROM weather_observations {where} ORDER BY observation_timestamp DESC, id DESC LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
        return [dict(row) for row in rows], total

    def list_runs(
        self, limit: int = 25, start_date: Optional[str] = None, end_date: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        self.initialize()
        clauses: list[str] = []
        params: list[Any] = []
        if start_date:
            clauses.append("started_at >= ?")
            params.append(start_date)
        if end_date:
            clauses.append("started_at <= ?")
            params.append(end_date)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        with self._connection() as connection:
            rows = connection.execute(
                f"SELECT * FROM pipeline_runs {where} ORDER BY id DESC LIMIT ?",
                [*params, limit],
            ).fetchall()
        return [dict(row) for row in rows]

    def latest_run(self) -> Optional[dict[str, Any]]:
        runs = self.list_runs(1)
        return runs[0] if runs else None

    def all_observations(self) -> list[dict[str, Any]]:
        rows, _ = self.list_observations(limit=1_000_000)
        return rows

    def get_observation(self, observation_id: int) -> Optional[dict[str, Any]]:
        self.initialize()
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM weather_observations WHERE id = ?", (observation_id,)).fetchone()
        return dict(row) if row else None

    def get_quality_report_by_id(self, report_id: int) -> Optional[dict[str, Any]]:
        """Read one persisted report by its report id."""
        self.initialize()
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM data_quality_reports WHERE id = ?", (report_id,)
            ).fetchone()
        return self._decode_quality_report(row)

    def latest_by_city(self, city: Optional[str] = None) -> list[dict[str, Any]]:
        self.initialize()
        params: list[Any] = [city] if city else []
        where = "WHERE city = ?" if city else ""
        with self._connection() as connection:
            rows = connection.execute(
                f"""SELECT observation.* FROM weather_observations observation
                JOIN (SELECT city, MAX(observation_timestamp) AS latest_time
                      FROM weather_observations {where} GROUP BY city) latest
                  ON latest.city = observation.city AND latest.latest_time = observation.observation_timestamp
                ORDER BY observation.city""", params,
            ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _decode_quality_report(row: Optional[sqlite3.Row]) -> Optional[dict[str, Any]]:
        if row is None:
            return None
        report = dict(row)
        report["missing_metrics"] = json.loads(report.pop("missing_metrics_json"))
        report["report"] = json.loads(report.pop("report_json"))
        return report

    # Convenience methods used by the analytics and detail pages.
    def get_latest_by_city(self, city: Optional[str] = None) -> list[dict[str, Any]]:
        return self.latest_by_city(city)

    def observation_metrics(
        self, city: Optional[str] = None, start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> dict[str, Any]:
        rows, _ = self.list_observations(limit=1_000_000, city=city, start_date=start_date, end_date=end_date)
        def average(field: str) -> Optional[float]:
            values = [row[field] for row in rows if row.get(field) is not None]
            return sum(values) / len(values) if values else None
        return {
            "count": len(rows), "cities": sorted({row["city"] for row in rows}),
            "average_temperature_c": average("temperature_c"),
            "average_humidity": average("relative_humidity"),
            "average_wind_speed_kmh": average("wind_speed_kmh"),
            "min_temperature_c": min((row["temperature_c"] for row in rows), default=None),
            "max_temperature_c": max((row["temperature_c"] for row in rows), default=None),
        }

    def get_observation_metrics(
        self, city: Optional[str] = None, start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> dict[str, Any]:
        return self.observation_metrics(city=city, start_date=start_date, end_date=end_date)

    @staticmethod
    def _ensure_column(connection: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        existing = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
        if column not in existing:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()
