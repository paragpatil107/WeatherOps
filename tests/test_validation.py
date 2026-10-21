import unittest

from src.validation import ValidationError, validate_observation


def valid_record():
    return {
        "city": "Pune",
        "latitude": 18.5204,
        "longitude": 73.8567,
        "observation_timestamp": "2026-10-21T10:00",
        "temperature_c": 28.5,
        "relative_humidity": 62,
        "apparent_temperature_c": 29.2,
        "wind_speed_kmh": 12.3,
        "weather_code": 1,
        "data_source": "Open-Meteo",
        "ingestion_timestamp": "2026-10-21T10:01:00+00:00",
    }


class ValidationTests(unittest.TestCase):
    def test_valid_record_is_accepted(self):
        validate_observation(valid_record())

    def test_missing_field_is_rejected(self):
        record = valid_record()
        del record["temperature_c"]
        with self.assertRaisesRegex(ValidationError, "temperature_c"):
            validate_observation(record)

    def test_invalid_type_and_range_are_rejected(self):
        record = valid_record()
        record["relative_humidity"] = "high"
        with self.assertRaises(ValidationError):
            validate_observation(record)
        record = valid_record()
        record["wind_speed_kmh"] = -1
        with self.assertRaises(ValidationError):
            validate_observation(record)


if __name__ == "__main__":
    unittest.main()
