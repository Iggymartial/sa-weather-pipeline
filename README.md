# South Africa Weather Data Pipeline

A batch data engineering pipeline that collects current weather conditions
for nine South African cities (one per province) on a schedule, validates
and cleans the data, stores it in a normalised relational database, and
serves it over a REST API.

Built to demonstrate the full data engineering lifecycle: extraction, raw
storage, transformation and data quality checks, relational design,
containerisation, serving, and orchestration.

## Status

- [x] **Step 1 - Extractor, raw storage and tests**
- [ ] Step 2 - MySQL schema (ERD first, then DDL)
- [ ] Step 3 - Transform (pandas: clean, validate, dedupe) and load
- [ ] Step 4 - Dockerise
- [ ] Step 5 - Java (Spring Boot) REST API
- [ ] Step 6 - Airflow DAG to schedule extract -> transform -> load
- [ ] Step 7 - Architecture diagram, demo video

## Architecture (target)

```
Open-Meteo API
      |
      v
extractor (Python)  ->  data/raw/*.json     raw payload + request context, untouched
      |
      v
transform (pandas)  ->  validated, de-duplicated records
      |
      v
MySQL               ->  locations, weather_codes, weather_observations, ingestion_runs
      |
      v
Spring Boot API     ->  serves the stored data over REST

Airflow runs extract -> transform -> load on a schedule.
```

## Data source

Weather data by [Open-Meteo.com](https://open-meteo.com/), licensed under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). The free API is
for non-commercial use (under 10,000 calls per day); this project makes one
request per run, covering all nine locations.

## Running Step 1

```
pip install -r extractor/requirements.txt
python extractor/extract.py
```

On Windows: `py -3 -m pip install -r extractor\requirements.txt` then
`py -3 extractor\extract.py`.

Each run writes one file to `data/raw/`, named `weather_<UTC timestamp>.json`.
The file holds the API's response exactly as received, plus the request that
produced it and the locations that were asked for.

The locations come from [`config/locations.json`](config/locations.json).
Adding a city is a config change, not a code change.

## Running the tests

```
pip install -r tests/requirements.txt
python -m pytest
```

No network access is needed: HTTP is faked, and retry delays are recorded
instead of slept.

## Design decisions

See [docs/decisions.md](docs/decisions.md).
