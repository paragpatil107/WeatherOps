"""Transform Open-Meteo JSON into the internal observation schema."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .extraction import DATA_SOURCE
from .validation import ValidationError, validate_coordinates, validate_observation


class TransformationError(ValueError):
    """Raised when the provider payload is incomplete or malformed."""


def transform_weather_response(
    payload: dict[str, Any],
    city: str,
    latitude: float,
    longitude: float,
    data_source: str = DATA_SOURCE,
) -> dict[str, Any]:
    """Map a provider payload to one current observation record."""
    latitude, longitude = validate_coordinates(latitude, longitude)
    if not isinstance(payload, dict) or not isinstance(payload.get("current"), dict):
        raise TransformationError("weather API response does not contain a current object")
    current = payload["current"]
    fields = (
        "time", "temperature_2m", "relative_humidity_2m", "apparent_temperature",
        "wind_speed_10m", "weather_code",
    )
    missing = [field for field in fields if field not in current or current[field] is None]
    if missing:
        raise TransformationError("weather API response is missing: " + ", ".join(missing))
    observation = {
        "city": city,
        "latitude": latitude,
        "longitude": longitude,
        "observation_timestamp": str(current["time"]),
        "temperature_c": current["temperature_2m"],
        "relative_humidity": current["relative_humidity_2m"],
        "apparent_temperature_c": current["apparent_temperature"],
        "wind_speed_kmh": current["wind_speed_10m"],
        "weather_code": current["weather_code"],
        "data_source": data_source,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
    }
    # Pressure and visibility are optional provider fields. Store them whenever
    # the current response includes a value.
    if current.get("surface_pressure") is not None:
        observation["pressure_hpa"] = current["surface_pressure"]
    if current.get("visibility") is not None:
        observation["visibility_m"] = current["visibility"]
    try:
        validate_observation(observation)
    except ValidationError as exc:
        raise TransformationError(str(exc)) from exc
    return observation
