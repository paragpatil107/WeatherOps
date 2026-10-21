import csv
import tempfile
import unittest
from pathlib import Path

from src.export import export_csv
from tests.test_validation import valid_record


class ExportTests(unittest.TestCase):
    def test_csv_contains_actual_observation_records(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "weather.csv"
            self.assertEqual(export_csv([valid_record()], str(path)), 1)
            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["city"], "Pune")
            self.assertEqual(rows[0]["temperature_c"], "28.5")


if __name__ == "__main__":
    unittest.main()
