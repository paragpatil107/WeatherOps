"""Validation for normalized weather observations and pipeline inputs."""

from __future__ import annotations

from datetime import datetime
import math
from typing import Any, Mapping, Optional, Tuple


class ValidationError(ValueError):
    """Raised when a record cannot be safely persisted."""


def validate_coordinates(latitude: float, longitude: float) -> Tuple[float, float]:
    """Validate and normalize a latitude/longitude pair."""
    for name, value in (("latitude", latitude), ("longitude", longitude)):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValidationError(f"{name} must be a number")
        if not math.isfinite(float(value)):
            raise ValidationError(f"{name} must be finite")
    latitude_value = float(latitude)
    longitude_value = float(longitude)
    if not -90 <= latitude_value <= 90:
        raise ValidationError("latitude must be between -90 and 90")
    if not -180 <= longitude_value <= 180:
        raise ValidationError("longitude must be between -180 and 180")
    return latitude_value, longitude_value


def _measurement(name: str, value: Optional[Any], minimum: float, maximum: float) -> None:
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{name} must be a numeric value")
    value_float = float(value)
    if not math.isfinite(value_float) or not minimum <= value_float <= maximum:
        raise ValidationError(f"{name} must be between {minimum} and {maximum}")


def validate_observation(observation: Mapping[str, Any]) -> None:
    """Validate every required field and its reasonable physical range."""
    required = {
        "city", "latitude", "longitude", "observation_timestamp",
        "temperature_c", "relative_humidity", "apparent_temperature_c",
        "wind_speed_kmh", "weather_code", "data_source", "ingestion_timestamp",
    }
    missing = required.difference(observation)
    if missing:
        raise ValidationError("observation is missing: " + ", ".join(sorted(missing)))
    if not isinstance(observation["city"], str) or not observation["city"].strip():
        raise ValidationError("city must be a non-empty string")
    validate_coordinates(observation["latitude"], observation["longitude"])
    for field in ("observation_timestamp", "ingestion_timestamp"):
        value = observation[field]
        if not isinstance(value, str) or not value.strip():
            raise ValidationError(f"{field} must be a non-empty string")
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValidationError(f"{field} must be an ISO timestamp") from exc
    _measurement("temperature_c", observation["temperature_c"], -100, 70)
    _measurement("apparent_temperature_c", observation["apparent_temperature_c"], -120, 80)
    _measurement("relative_humidity", observation["relative_humidity"], 0, 100)
    _measurement("wind_speed_kmh", observation["wind_speed_kmh"], 0, 500)
    if isinstance(observation["weather_code"], bool) or not isinstance(observation["weather_code"], int):
        raise ValidationError("weather_code must be an integer")
    if not 0 <= observation["weather_code"] <= 99:
        raise ValidationError("weather_code must be between 0 and 99")
    if not isinstance(observation["data_source"], str) or not observation["data_source"].strip():
        raise ValidationError("data_source must be a non-empty string")
    if "pressure_hpa" in observation and observation["pressure_hpa"] is not None:
        _measurement("pressure_hpa", observation["pressure_hpa"], 100, 1200)
    if "visibility_m" in observation and observation["visibility_m"] is not None:
        _measurement("visibility_m", observation["visibility_m"], 0, 100_000)
