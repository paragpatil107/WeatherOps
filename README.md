# WeatherOps — Automated Weather Data Pipeline

WeatherOps is a Python data engineering project. It
extracts current weather observations for Pune, Mumbai, Nashik, and Nagpur from
the Open-Meteo API, validates and stores them in SQLite, exports real stored data
to CSV, records pipeline history, and presents the results in a responsive Flask
dashboard.

## Features

- Real Open-Meteo API integration with no API key for the configured endpoint.
- Separate extract, transform, validate, load, export, and orchestration modules.
- Current temperature, humidity, apparent temperature, wind speed, WMO weather
  code, location, source, observation timestamp, and ingestion timestamp.
- Explicit request timeouts, retry/backoff behavior, malformed-response handling,
  and per-city failure isolation.
- SQLite persistence with parameterized SQL and duplicate prevention using
  `UNIQUE(city, observation_timestamp)`.
- Pipeline run history with fetched, accepted, rejected, inserted, duplicate, and
  error counts.
- CSV downloads generated from database records, never hardcoded sample data.
- Flask/Jinja2 dashboard with custom responsive CSS, search, pagination, charts,
  empty states, loading states, and readable error feedback.
- A premium dark operations UI with clickable metric cards, city weather
  cards, analytics pages, pipeline metrics, data-quality reports, data reports,
  logs, JSON/CSV exports, and optional air-quality readings.
- Additive SQLite migrations preserve Phase 1 weather records while adding
  pressure, visibility, quality-report, air-quality, duration, and stage fields.
- Built-in `unittest` tests with mocked API behavior; tests do not require internet.

## Technology stack

Python · Requests · Open-Meteo · JSON · Pandas · SQLite/sqlite3 · Flask · Jinja2 ·
HTML5/CSS3 · Chart.js CDN · unittest · logging · Git-ready structure.

## Architecture and ETL workflow

1. **Extract** — `src/extraction.py` calls Open-Meteo's current weather endpoint
   with an explicit timeout and bounded retries.
2. **Transform** — `src/transformation.py` maps the provider response to one
   consistent internal record.
3. **Validate** — `src/validation.py` rejects missing fields, incorrect types,
   invalid timestamps, and unreasonable physical values.
4. **Load** — `src/database.py` inserts valid records inside SQLite transactions.
   Duplicate rows are counted rather than duplicated.
5. **Export** — `src/export.py` uses Pandas to create UTF-8 CSV output from the
   database-backed records.
6. **Monitor** — `src/pipeline.py` records a run summary and the dashboard reads
   the same database state.

See [`docs/architecture.md`](docs/architecture.md) for the component view.

## Folder structure

```text
automated-data-pipeline/
├── app.py
├── requirements.txt
├── README.md
├── .gitignore
├── src/
│   ├── extraction.py
│   ├── transformation.py
│   ├── validation.py
│   ├── database.py
│   ├── export.py
│   ├── pipeline.py
│   ├── quality.py
│   ├── reports.py
│   └── air_quality.py
├── templates/
├── static/css/style.css
├── static/js/dashboard.js
├── tests/
├── docs/architecture.md
├── data/                 # generated DB/CSV files are ignored by Git
└── logs/                 # generated logs are ignored by Git
```

## Application routes

- `/` — dark overview dashboard with metrics, city cards, charts, and air-quality summary.
- `/observations` — server-filtered observation table with city/date/search filters and pagination.
- `/observations/<id>` — observation detail with links to metric-specific analytics.
- `/analytics/<metric>` — temperature, humidity, wind, pressure, visibility, or conditions analytics.
- `/air-quality` — US AQI and pollutant readings when the optional source returns real data.
- `/runs` and `/pipeline` — pipeline history and execution metrics.
- `/quality-reports` — persisted quality reports, filtering, comparison, and exports.
- `/reports` — activity and dataset-health summary with Generate Report, CSV, and JSON actions.
- `/logs` — bounded, filterable, redacted operational log viewer.
- `/export` — filtered CSV/JSON preview and downloads.

## Database migrations

The application applies additive migrations on startup, so existing rows are kept.

- `weather_observations`: nullable `pressure_hpa` and `visibility_m`.
- `pipeline_runs`: `duration_ms`, `duration_seconds`, `stage`, and `error`.
- `data_quality_reports`: formula, score components, field-level missing metrics, and run issues.
- `air_quality_observations`: source timestamps, US AQI, pollutants, and ingestion timestamps.

The weather uniqueness rule remains `UNIQUE(city, observation_timestamp)`. Air-quality
rows use the same city/timestamp uniqueness rule.

## Data-quality methodology

Each pipeline run persists a report even when one or more cities fail. The report
uses accepted normalized weather records for field-level completeness and validity,
while its `run_counts` section separately records fetched, accepted, rejected,
inserted, duplicate, API-failure, transformation-error, and validation-error counts.

The displayed score is transparent:

```text
quality_score =
  0.70 × completeness_score +
  0.20 × validity_score +
  0.10 × consistency_score
```

Each component is a percentage from 0–100. Completeness is the mean field-presence
percentage; validity is the percentage of accepted records passing the existing
validation rules; consistency is the percentage of unique city/timestamp keys.
Duplicates are reported separately and are not automatically treated as invalid.

`records_fetched`, `records_accepted`, `records_inserted`, `duplicates_skipped`,
and `records_rejected` describe different pipeline stages and are not expected to
sum to the same value.

## Optional air-quality configuration

The Flask Run Pipeline action enables Open-Meteo Air Quality collection by default.
It is failure-isolated: an air-quality timeout does not turn a successful weather
load into a failed weather run.

Disable it for an offline or faster local run:

```powershell
$env:ENABLE_AIR_QUALITY = "0"
python app.py
```

The UI labels the index as **US AQI** and uses the six official US AQI categories:
Good, Moderate, Unhealthy for Sensitive Groups, Unhealthy, Very Unhealthy, and
Hazardous (301–500). It does not mix AQI scales or fabricate values. The air-quality
provider attribution is Open-Meteo / CAMS ENSEMBLE.

## Windows PowerShell setup

Open PowerShell in the project folder:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If PowerShell blocks activation in your environment, run the project with the
virtual environment interpreter directly:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Start the Flask application

```powershell
python app.py
```

Open `http://127.0.0.1:5000/` in a browser. The dashboard starts with an empty
state. Click **Run pipeline** to make a real API request for all four cities.

Optional environment variables:

```powershell
$env:WEATHER_DB_PATH = "data\weather.db"
$env:FLASK_SECRET_KEY = "local-development-only"
$env:FLASK_DEBUG = "1"
$env:ENABLE_AIR_QUALITY = "1"
python app.py
```

## Run the pipeline without the dashboard

```powershell
python -c "from src.pipeline import run_pipeline; print(run_pipeline())"
```

This command makes real API calls. If the API is unavailable, the result is
reported as `failure` or `partial_success`; no fabricated observations are
created.

## Run tests

The required project check is:

```powershell
python -m pytest -q
```

The suite uses mocked extraction behavior and temporary database/CSV paths, so
it does not depend on current weather or internet access.

## Database, CSV, and logs

- `data/weather.db` — SQLite database created on first app/pipeline use.
- `data/weather-observations.csv` — current export generated after a pipeline run
  or from the Data export navigation item.
- `logs/pipeline.log` — structured application and rejection logging.
- `data/weather-report-*.json` / `data/weather-report-*.csv` — generated activity reports.
- `data/quality-report-*.json` / `data/quality-report-*.csv` — quality-report exports.

Generated files are ignored by Git. The application creates the `data` and
`logs` folders when needed.

## Screenshots

To be added soon.

## Known limitations and future enhancements

- The pipeline is intentionally a manually triggered local process; it does not
  install a Windows scheduled task or external orchestrator.
- Open-Meteo's current endpoint is sampled when a run is started; it is not a
  historical time-series collector.
- The optional air-quality source is current-observation based; a failed AQ
  request leaves the weather pipeline usable and displays an honest unavailable state.
- The log viewer is intentionally bounded to the most recent configured lines;
  it is not a replacement for a centralized log platform.
- The dashboard has no authentication and is intended for local development.
- Future enhancements could add historical trend storage, configurable city
  lists, richer WMO descriptions, automated scheduling, and deployment-specific
  observability.
