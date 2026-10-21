import tempfile
import unittest
from pathlib import Path

from src.database import WeatherDatabase
from tests.test_validation import valid_record


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = WeatherDatabase(str(Path(self.temp_dir.name) / "weather.db"))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_initialize_creates_expected_tables_and_indexes(self):
        self.database.initialize()
        connection = self.database.connect()
        try:
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        finally:
            connection.close()
        self.assertIn("weather_observations", tables)
        self.assertIn("pipeline_runs", tables)

    def test_duplicate_record_is_skipped(self):
        self.assertEqual(self.database.insert_observations([valid_record()]), (1, 0))
        self.assertEqual(self.database.insert_observations([valid_record()]), (0, 1))
        self.assertEqual(self.database.count_observations(), 1)

    def test_run_history_keeps_all_counts(self):
        run_id = self.database.start_run()
        self.database.finish_run(run_id, "partial_success", 4, 3, 1, 2, 1, "Nagpur: timeout")
        latest = self.database.latest_run()
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest["status"], "partial_success")
        self.assertEqual(latest["records_rejected"], 1)
        self.assertEqual(latest["duplicates_skipped"], 1)


if __name__ == "__main__":
    unittest.main()
