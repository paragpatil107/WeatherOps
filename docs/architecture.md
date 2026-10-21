# Architecture

WeatherOps is a small, modular ETL application with a Flask presentation layer.

```text
Open-Meteo Weather API
      │
      ▼
src/extraction.py       HTTP, timeout, retry and JSON checks
      │
      ▼
src/transformation.py   provider JSON → normalized observation
      │
      ▼
src/validation.py       required fields, types and safe ranges
      │
      ▼
src/database.py         SQLite tables, transactions and uniqueness
      │
      ├── src/quality.py  persisted quality calculations
      ├── src/reports.py activity summaries and report exports
      ├── src/export.py  database-backed CSV/JSON export
      └── Flask app      dark dashboard, analytics, logs and reports
```

## Internal observation schema

Each accepted record contains `city`, latitude/longitude, an Open-Meteo
observation timestamp, temperature, relative humidity, apparent temperature,
wind speed, WMO weather code, optional pressure and visibility, data source, and
an ingestion timestamp. Air-quality rows are stored separately with source
timestamps, US AQI, and available pollutant values.

## Duplicate prevention

SQLite enforces `UNIQUE(city, observation_timestamp)`. Inserts use parameterized
SQL and `INSERT OR IGNORE`; the pipeline records the number of ignored duplicate
rows in the run history rather than silently hiding it.

## Failure isolation

The pipeline processes each configured city independently. A timeout or malformed
response for one city is logged and counted as rejected while successful cities
are still loaded. The run is marked `partial_success` when at least one city
succeeds and another city fails, and `failure` when no city can be accepted.

## Local storage

- SQLite database: `data/weather.db`
- Generated CSV: `data/weather-observations.csv`
- Application log: `logs/pipeline.log`
- Quality reports: `data_quality_reports` SQLite table and JSON/CSV downloads
- Air quality: `air_quality_observations` SQLite table
