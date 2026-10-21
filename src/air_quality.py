"""Optional Open-Meteo air-quality extraction and transformation."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import time
from typing import Any, Optional

import requests

from .validation import validate_coordinates


DEFAULT_AIR_QUALITY_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
AIR_QUALITY_DATA_SOURCE = "Open-Meteo Air Quality"
POLLUTANT_FIELDS = (
    "us_aqi", "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
    "sulphur_dioxide", "ozone",
)


class AirQualityAPIError(RuntimeError):
    """Raised when the optional air-quality provider is unavailable."""


class AirQualityExtractor:
    """Fetch current US AQI and pollutant values with bounded retries."""

    def __init__(
        self, base_url: str = DEFAULT_AIR_QUALITY_API_URL, timeout: float = 10.0,
        retries: int = 3, backoff_seconds: float = 1.0,
        session: Optional[requests.Session] = None, logger: Optional[logging.Logger] = None,
    ) -> None:
        if timeout <= 0 or retries < 1 or backoff_seconds < 0:
            raise ValueError("invalid air-quality request settings")
        self.base_url, self.timeout, self.retries = base_url, timeout, retries
        self.backoff_seconds = backoff_seconds
        self.session = session or requests.Session()
        self.logger = logger or logging.getLogger(__name__)

    def fetch(self, city: str, latitude: float, longitude: float) -> dict[str, Any]:
        latitude, longitude = validate_coordinates(latitude, longitude)
        params = {"latitude": latitude, "longitude": longitude,
                  "current": ",".join(POLLUTANT_FIELDS), "timezone": "UTC"}
        last_error: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(self.base_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict) or not isinstance(payload.get("current"), dict):
                    raise ValueError("air-quality API response must contain a current object")
                return payload
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                self.logger.warning("Air quality request for %s failed on attempt %s/%s: %s", city, attempt, self.retries, exc)
                if attempt < self.retries:
                    time.sleep(self.backoff_seconds * attempt)
        raise AirQualityAPIError(
            f"air-quality API request for {city} failed after {self.retries} attempt(s): {last_error}"
        ) from last_error


def transform_air_quality_response(
    payload: dict[str, Any], city: str, latitude: float, longitude: float,
    run_id: Optional[int] = None, data_source: str = AIR_QUALITY_DATA_SOURCE,
) -> dict[str, Any]:
    """Normalize current air-quality values; absent pollutants remain nullable."""
    latitude, longitude = validate_coordinates(latitude, longitude)
    current = payload.get("current") if isinstance(payload, dict) else None
    if not isinstance(current, dict) or not current.get("time"):
        raise ValueError("air-quality API response is missing current.time")
    return {
        "run_id": run_id, "city": city, "latitude": latitude, "longitude": longitude,
        "observation_timestamp": str(current["time"]),
        **{field: current.get(field) for field in POLLUTANT_FIELDS},
        "units": payload.get("current_units", {}) if isinstance(payload, dict) else {},
        "data_source": data_source,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
    }
