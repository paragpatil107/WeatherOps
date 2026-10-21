import csv
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from src.air_quality import AirQualityExtractor, transform_air_quality_response
from src.database import WeatherDatabase
from src.export import export_report_csv, export_report_json
from src.pipeline import WeatherPipeline
from src.quality import build_quality_report
from tests.test_validation import valid_record


class Phase2BackendTests(unittest.TestCase):
    def test_initialize_migrates_old_schema_without_erasing_records(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "weather.db"
            connection = sqlite3.connect(path)
            connection.executescript(
                """
                CREATE TABLE weather_observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, city TEXT NOT NULL,
                    latitude REAL NOT NULL, longitude REAL NOT NULL,
                    observation_timestamp TEXT NOT NULL, temperature_c REAL NOT NULL,
                    relative_humidity REAL NOT NULL, apparent_temperature_c REAL NOT NULL,
                    wind_speed_kmh REAL NOT NULL, weather_code INTEGER NOT NULL,
                    data_source TEXT NOT NULL, ingestion_timestamp TEXT NOT NULL,
                    UNIQUE(city, observation_timestamp)
                );
                CREATE TABLE pipeline_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT NOT NULL,
                    finished_at TEXT, status TEXT NOT NULL,
                    records_fetched INTEGER NOT NULL DEFAULT 0,
                    records_accepted INTEGER NOT NULL DEFAULT 0,
                    records_rejected INTEGER NOT NULL DEFAULT 0,
                    records_inserted INTEGER NOT NULL DEFAULT 0,
                    duplicates_skipped INTEGER NOT NULL DEFAULT 0,
                    error_summary TEXT
                );
                """
            )
            record = valid_record()
            connection.execute(
                "INSERT INTO weather_observations (city, latitude, longitude, observation_timestamp, temperature_c, relative_humidity, apparent_temperature_c, wind_speed_kmh, weather_code, data_source, ingestion_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tuple(record[field] for field in ("city", "latitude", "longitude", "observation_timestamp", "temperature_c", "relative_humidity", "apparent_temperature_c", "wind_speed_kmh", "weather_code", "data_source", "ingestion_timestamp")),
            )
            connection.commit()
            connection.close()

            database = WeatherDatabase(str(path))
            database.initialize()
            connection = database.connect()
            columns = {row[1] for row in connection.execute("PRAGMA table_info(weather_observations)")}
            connection.close()
            self.assertTrue({"pressure_hpa", "visibility_m"}.issubset(columns))
            self.assertEqual(database.count_observations(), 1)
            connection = database.connect()
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            connection.close()
            self.assertTrue({"data_quality_reports", "air_quality_observations"}.issubset(tables))

    def test_quality_metrics_have_formula_and_field_missing_counts_and_persist(self):
        record = valid_record()
        report = build_quality_report([record], run_id=7)
        self.assertIn("formula", report)
        self.assertEqual(report["missing_metrics"]["pressure_hpa"]["missing_count"], 1)
        with tempfile.TemporaryDirectory() as temp_dir:
            database = WeatherDatabase(str(Path(temp_dir) / "weather.db"))
            report_id = database.save_quality_report(report, run_id=7)
            stored = database.latest_quality_report()
            self.assertIsNotNone(stored)
            assert stored is not None
            self.assertEqual(report_id, stored["id"])
            self.assertEqual(stored["report"]["quality_score"], report["quality_score"])

    def test_air_quality_extractor_and_transform_are_mockable(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "current": {
                "time": "2026-10-21T10:00", "us_aqi": 42, "pm10": 12.5,
                "pm2_5": 7.1, "carbon_monoxide": 210, "nitrogen_dioxide": 15,
                "sulphur_dioxide": 2, "ozone": 80,
            }
        }
        session = Mock()
        session.get.return_value = response
        payload = AirQualityExtractor(session=session, retries=1).fetch("Pune", 18.5, 73.8)
        row = transform_air_quality_response(payload, "Pune", 18.5, 73.8)
        self.assertEqual(row["us_aqi"], 42)
        self.assertEqual(row["pm2_5"], 7.1)

    def test_pipeline_air_quality_failure_does_not_block_weather_success(self):
        weather = Mock()
        weather.fetch.return_value = {"current": {
            "time": "2026-10-21T10:00", "temperature_2m": 28.5,
            "relative_humidity_2m": 62, "apparent_temperature": 29.2,
            "wind_speed_10m": 12.3, "weather_code": 1,
        }}
        air_quality = Mock()
        air_quality.fetch.side_effect = RuntimeError("AQ unavailable")
        with tempfile.TemporaryDirectory() as temp_dir:
            database = WeatherDatabase(str(Path(temp_dir) / "weather.db"))
            result = WeatherPipeline(weather, database, air_quality_extractor=air_quality).run(
                cities={"Pune": (18.5, 73.8)}
            )
            self.assertEqual(result.status, "success")
            self.assertEqual(result.records_inserted, 1)
            self.assertIn("AQ unavailable", result.air_quality_error or "")
            self.assertIsNotNone(database.latest_quality_report())
            latest_run = database.latest_run()
            self.assertIsNotNone(latest_run)
            assert latest_run is not None
            self.assertGreaterEqual(latest_run["duration_ms"], 0)

    def test_report_json_and_csv_exports(self):
        report = build_quality_report([valid_record()])
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path = Path(temp_dir) / "quality.json"
            csv_path = Path(temp_dir) / "quality.csv"
            self.assertGreater(export_report_json(report, str(json_path)), 0)
            self.assertEqual(export_report_csv(report, str(csv_path)), len(report["missing_metrics"]))
            self.assertEqual(json.loads(json_path.read_text(encoding="utf-8"))["quality_score"], report["quality_score"])
            with csv_path.open(newline="", encoding="utf-8") as handle:
                self.assertEqual(len(list(csv.DictReader(handle))), len(report["missing_metrics"]))


if __name__ == "__main__":
    unittest.main()
