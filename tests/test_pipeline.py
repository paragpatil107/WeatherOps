import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from src.database import WeatherDatabase
from src.extraction import WeatherAPIError, WeatherExtractor
from src.pipeline import CITY_LOCATIONS, WeatherPipeline


def api_payload(city: str):
    return {
        "current": {
            "time": "2026-10-21T10:00",
            "temperature_2m": 28.5,
            "relative_humidity_2m": 62,
            "apparent_temperature": 29.2,
            "wind_speed_10m": 12.3,
            "weather_code": 1,
        },
        "city": city,
    }


class PipelineTests(unittest.TestCase):
    def test_one_city_failure_does_not_discard_successful_cities(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database = WeatherDatabase(str(Path(temp_dir) / "weather.db"))
            extractor = Mock(spec=WeatherExtractor)

            def fetch(city, latitude, longitude):
                if city == "Nashik":
                    raise WeatherAPIError("service unavailable")
                return api_payload(city)

            extractor.fetch.side_effect = fetch
            pipeline = WeatherPipeline(extractor, database)
            result = pipeline.run(cities=CITY_LOCATIONS)

            self.assertEqual(result.status, "partial_success")
            self.assertEqual(result.records_fetched, 3)
            self.assertEqual(result.records_accepted, 3)
            self.assertEqual(result.records_rejected, 1)
            self.assertEqual(result.records_inserted, 3)
            self.assertEqual(database.count_observations(), 3)

    def test_all_api_failures_are_recorded_without_fabricated_data(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database = WeatherDatabase(str(Path(temp_dir) / "weather.db"))
            extractor = Mock(spec=WeatherExtractor)
            extractor.fetch.side_effect = WeatherAPIError("service unavailable")
            result = WeatherPipeline(extractor, database).run(cities={"Pune": CITY_LOCATIONS["Pune"]})
            self.assertEqual(result.status, "failure")
            self.assertEqual(result.records_inserted, 0)
            self.assertEqual(database.count_observations(), 0)
            self.assertIsNotNone(result.error_summary)
            self.assertIn("service unavailable", result.error_summary or "")


if __name__ == "__main__":
    unittest.main()
