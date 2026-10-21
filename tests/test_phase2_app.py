import unittest

from app import app


class Phase2AppTests(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()

    def test_phase2_pages_and_exports_are_reachable(self):
        paths = [
            "/", "/analytics/temperature", "/analytics/humidity", "/analytics/wind",
            "/analytics/pressure", "/analytics/visibility", "/analytics/conditions",
            "/air-quality", "/pipeline", "/quality-reports", "/reports", "/logs", "/export",
            "/reports.json", "/reports.csv", "/logs.txt",
        ]
        for path in paths:
            with self.subTest(path=path):
                response = self.client.get(path)
                try:
                    self.assertEqual(response.status_code, 200)
                finally:
                    response.close()

    def test_invalid_filters_are_rejected(self):
        self.assertEqual(self.client.get("/observations?city=Unknown").status_code, 400)
        self.assertEqual(self.client.get("/analytics/temperature?start_date=not-a-date").status_code, 400)
        self.assertEqual(self.client.get("/observations?page=zero").status_code, 400)
        self.assertEqual(self.client.get("/logs?severity=SECRET").status_code, 400)

    def test_quality_report_alias_and_observation_json_are_available(self):
        self.assertEqual(self.client.get("/data-quality").status_code, 200)
        response = self.client.get("/observations.json?city=Pune")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "application/json")


if __name__ == "__main__":
    unittest.main()
