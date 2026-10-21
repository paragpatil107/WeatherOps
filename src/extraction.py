"""Extraction layer for the Open-Meteo current weather endpoint."""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

import requests

from .validation import validate_coordinates


DEFAULT_API_URL = "https://api.open-meteo.com/v1/forecast"
DATA_SOURCE = "Open-Meteo"


class WeatherAPIError(RuntimeError):
    """Raised when Open-Meteo cannot return a usable response."""


class WeatherExtractor:
    """Fetch current observations with explicit timeouts and bounded retries."""

    def __init__(
        self,
        base_url: str = DEFAULT_API_URL,
        timeout: float = 10.0,
        retries: int = 3,
        backoff_seconds: float = 1.0,
        session: Optional[requests.Session] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        if retries < 1:
            raise ValueError("retries must be at least one")
        if backoff_seconds < 0:
            raise ValueError("backoff_seconds cannot be negative")
        self.base_url = base_url
        self.timeout = timeout
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.session = session or requests.Session()
        self.logger = logger or logging.getLogger(__name__)

    def fetch(self, city: str, latitude: float, longitude: float) -> dict[str, Any]:
        """Fetch the latest observation for one city.

        The response is returned unchanged so the transformation stage remains
        responsible for interpreting the provider's JSON schema.
        """
        latitude, longitude = validate_coordinates(latitude, longitude)
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": (
                "temperature_2m,relative_humidity_2m,apparent_temperature,"
                "wind_speed_10m,weather_code,surface_pressure,visibility"
            ),
            "timezone": "UTC",
        }
        last_error: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(self.base_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("weather API response must be a JSON object")
                self.logger.info("Fetched current weather for %s on attempt %s", city, attempt)
                return payload
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                self.logger.warning(
                    "Weather API request for %s failed on attempt %s/%s: %s",
                    city,
                    attempt,
                    self.retries,
                    exc,
                )
                if attempt < self.retries:
                    time.sleep(self.backoff_seconds * attempt)
        raise WeatherAPIError(
            f"weather API request for {city} failed after {self.retries} attempt(s): {last_error}"
        ) from last_error


# Re-export the optional collector for callers that use the extraction module as
# the single entry point for external data sources.
from .air_quality import AirQualityAPIError, AirQualityExtractor, transform_air_quality_response  # noqa: E402,F401
