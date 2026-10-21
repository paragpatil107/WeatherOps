"""End-to-end orchestration for weather, quality, and optional air quality."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import time
from typing import Any, Optional

from .air_quality import AirQualityExtractor, transform_air_quality_response
from .database import WeatherDatabase
from .extraction import WeatherAPIError, WeatherExtractor
from .export import export_csv, export_report_json
from .quality import build_quality_report
from .transformation import TransformationError, transform_weather_response
from .validation import ValidationError, validate_observation


CITY_LOCATIONS: dict[str, tuple[float, float]] = {
    "Pune": (18.5204, 73.8567),
    "Mumbai": (19.0760, 72.8777),
    "Nashik": (19.9975, 73.7898),
    "Nagpur": (21.1458, 79.0882),
}


@dataclass
class PipelineResult:
    run_id: int
    status: str
    records_fetched: int = 0
    records_accepted: int = 0
    records_rejected: int = 0
    records_inserted: int = 0
    duplicates_skipped: int = 0
    records_exported: int = 0
    error_summary: Optional[str] = None
    duration_ms: Optional[float] = None
    quality_score: Optional[float] = None
    quality_report_id: Optional[int] = None
    air_quality_inserted: int = 0
    air_quality_error: Optional[str] = None
    api_failures: int = 0
    transformation_errors: int = 0
    validation_errors: int = 0

    @property
    def error_message(self) -> Optional[str]:
        """Backward-compatible alias for simple callers."""
        return self.error_summary


class WeatherPipeline:
    """Run extraction, transformation, validation, loading, quality, and export."""

    def __init__(
        self, extractor: WeatherExtractor, database: WeatherDatabase,
        logger: Optional[logging.Logger] = None,
        air_quality_extractor: Optional[AirQualityExtractor] = None,
    ) -> None:
        self.extractor = extractor
        self.database = database
        self.logger = logger or logging.getLogger(__name__)
        self.air_quality_extractor = air_quality_extractor

    def run(
        self,
        cities: Optional[dict[str, tuple[float, float]]] = None,
        csv_path: Optional[str] = None,
        quality_report_path: Optional[str] = None,
        include_air_quality: Optional[bool] = None,
    ) -> PipelineResult:
        locations = cities or CITY_LOCATIONS
        started = time.perf_counter()
        run_id = self.database.start_run()
        fetched = accepted = rejected = inserted = duplicates = exported = 0
        air_quality_inserted = 0
        errors: list[str] = []
        valid_records: list[dict[str, Any]] = []
        api_failures = transformation_errors = validation_errors = 0
        self.database.update_run_stage(run_id, "extracting")
        air_quality_error: Optional[str] = None

        for city, (latitude, longitude) in locations.items():
            try:
                payload = self.extractor.fetch(city, latitude, longitude)
                fetched += 1
                record = transform_weather_response(payload, city, latitude, longitude)
                validate_observation(record)
                valid_records.append(record)
                accepted += 1
            except WeatherAPIError as exc:
                api_failures += 1
                rejected += 1
                message = f"{city}: API failure: {exc}"
                errors.append(message)
                self.logger.exception("API failure for %s", city)
            except TransformationError as exc:
                transformation_errors += 1
                rejected += 1
                message = f"{city}: transformation error: {exc}"
                errors.append(message)
                self.logger.exception("Transformation error for %s", city)
            except ValidationError as exc:
                validation_errors += 1
                rejected += 1
                message = f"{city}: validation error: {exc}"
                errors.append(message)
                self.logger.exception("Validation error for %s", city)
            except Exception as exc:  # one city must not stop other cities
                transformation_errors += 1
                rejected += 1
                message = f"{city}: {exc}"
                errors.append(message)
                self.logger.exception("Rejected weather record for %s", city)

        try:
            self.database.update_run_stage(run_id, "loading")
            inserted, duplicates = self.database.insert_observations(valid_records)
            if csv_path is not None:
                exported = export_csv(self.database.all_observations(), csv_path)
        except Exception as exc:
            errors.append(f"storage: {exc}")
            self.logger.exception("Storage/export failed for pipeline run %s", run_id)

        quality_report_id: Optional[int] = None
        quality_score: Optional[float] = None
        try:
            self.database.update_run_stage(run_id, "quality")
            report = build_quality_report(valid_records, run_id=run_id)
            report["run_counts"] = {
                "records_fetched": fetched,
                "records_accepted": accepted,
                "records_rejected": rejected,
                "records_inserted": inserted,
                "duplicates_skipped": duplicates,
                "api_failures": api_failures,
                "transformation_errors": transformation_errors,
                "validation_errors": validation_errors,
            }
            report["issues"] = errors
            quality_score = float(report["quality_score"])
            quality_report_id = self.database.save_quality_report(report, run_id=run_id)
            if quality_report_path is not None:
                export_report_json(report, quality_report_path)
        except Exception:
            # Quality telemetry must never turn a successful weather load into a failure.
            self.logger.exception("Quality report failed for pipeline run %s", run_id)

        should_collect_air = self.air_quality_extractor is not None if include_air_quality is None else include_air_quality
        if should_collect_air and self.air_quality_extractor is not None:
            self.database.update_run_stage(run_id, "air_quality")
            air_rows: list[dict[str, Any]] = []
            for city, (latitude, longitude) in locations.items():
                try:
                    payload = self.air_quality_extractor.fetch(city, latitude, longitude)
                    air_rows.append(transform_air_quality_response(payload, city, latitude, longitude, run_id=run_id))
                except Exception as exc:
                    air_quality_error = f"{city}: {exc}"
                    self.logger.warning("Optional air quality unavailable for %s: %s", city, exc)
            try:
                air_quality_inserted, _ = self.database.insert_air_quality_observations(air_rows)
            except Exception as exc:
                air_quality_error = f"storage: {exc}"
                self.logger.exception("Optional air-quality storage failed for pipeline run %s", run_id)

        status = "failure" if errors and accepted == 0 and inserted == 0 else "partial_success" if errors else "success"
        error_summary = "; ".join(errors) if errors else None
        duration_ms = round((time.perf_counter() - started) * 1000, 3)
        self.database.finish_run(
            run_id, status, fetched, accepted, rejected, inserted, duplicates, error_summary,
            duration_ms=duration_ms, stage="complete", error=error_summary,
        )
        self.logger.info(
            "Pipeline run %s finished status=%s fetched=%s accepted=%s rejected=%s inserted=%s duplicates=%s duration_ms=%s",
            run_id, status, fetched, accepted, rejected, inserted, duplicates, duration_ms,
        )
        return PipelineResult(
            run_id=run_id, status=status, records_fetched=fetched,
            records_accepted=accepted, records_rejected=rejected,
            records_inserted=inserted, duplicates_skipped=duplicates,
            records_exported=exported, error_summary=error_summary,
            duration_ms=duration_ms, quality_score=quality_score,
            quality_report_id=quality_report_id,
            air_quality_inserted=air_quality_inserted, air_quality_error=air_quality_error,
            api_failures=api_failures, transformation_errors=transformation_errors,
            validation_errors=validation_errors,
        )


def run_pipeline(
    database: Optional[WeatherDatabase] = None,
    extractor: Optional[WeatherExtractor] = None,
    csv_path: Optional[str] = None,
    air_quality_extractor: Optional[AirQualityExtractor] = None,
    include_air_quality: Optional[bool] = None,
) -> PipelineResult:
    """Convenience function used by scripts and simple integrations."""
    return WeatherPipeline(
        extractor or WeatherExtractor(), database or WeatherDatabase(),
        air_quality_extractor=air_quality_extractor,
    ).run(csv_path=csv_path, include_air_quality=include_air_quality)
