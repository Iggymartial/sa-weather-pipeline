"""
extract.py
Step 1 of the SA weather pipeline.

Pulls current conditions for every location in config/locations.json from
Open-Meteo in ONE request, and stores the response, untouched, in data/raw/.

Design notes (the longer reasoning lives in docs/decisions.md):

- Raw first. The API's JSON is stored exactly as received, wrapped in a small
  "envelope" that records WHEN it was fetched, WHAT was asked for, and WHICH
  locations were requested. Cleaning and validation are the transform step's
  job, not this one's.
- Locations are matched to results by POSITION. Open-Meteo snaps coordinates
  to its model grid, so the lat/lon in the response will not equal what we
  sent. Order is the reliable link, so the envelope keeps the request order.
- Transient failures (timeouts, 429, 5xx) are retried with backoff. Client
  errors (bad request) are not - retrying a request that is wrong can't help.
- Exit code 0 on success, 1 on failure, so Airflow can read the outcome.

Usage:
    python extractor/extract.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger("extract")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCATIONS_PATH = PROJECT_ROOT / "config" / "locations.json"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

# The base URL is configuration, not a constant buried in code: the last API
# this kind of pipeline depended on retired its old version without warning,
# and fixing that should be an environment change, not a code change.
API_URL = os.getenv("OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast")

# The "current" block updates every 15 minutes (interval = 900s).
CURRENT_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "weather_code",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
]

# 429 = rate limited, 5xx = server-side trouble. All worth another attempt.
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# (connect timeout, read timeout) in seconds. A nine-location response is
# bigger than a single-location one, so the read timeout is generous.
TIMEOUT = (5, 30)

HEADERS = {"User-Agent": "sa-weather-pipeline (student data engineering project)"}

REQUIRED_LOCATION_FIELDS = ("id", "name", "province", "latitude", "longitude")


class ExtractError(Exception):
    """Base class for anything that makes this run fail."""


class ConfigError(ExtractError):
    """The locations config is missing or invalid."""


class ApiError(ExtractError):
    """The API could not give us a usable answer."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _is_number(value) -> bool:
    # bool is a subclass of int in Python, but True is not a latitude.
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def load_locations(path: Path = None) -> list[dict]:
    """Read and validate config/locations.json. Fails loudly on bad config:
    a typo in a coordinate should stop the run, not quietly fetch the wrong
    place."""
    path = path or LOCATIONS_PATH

    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError as e:
        raise ConfigError(f"locations file not found: {path}") from e
    except json.JSONDecodeError as e:
        raise ConfigError(f"locations file is not valid JSON: {e}") from e

    locations = data.get("locations") if isinstance(data, dict) else None
    if not isinstance(locations, list) or not locations:
        raise ConfigError("locations file must contain a non-empty 'locations' list")

    seen_ids = set()
    for index, loc in enumerate(locations):
        if not isinstance(loc, dict):
            raise ConfigError(f"location #{index} is not an object")

        missing = [field for field in REQUIRED_LOCATION_FIELDS if field not in loc]
        if missing:
            raise ConfigError(f"location #{index} is missing fields: {missing}")

        if not _is_number(loc["latitude"]) or not -90 <= loc["latitude"] <= 90:
            raise ConfigError(f"location '{loc['id']}' has an invalid latitude: {loc['latitude']!r}")
        if not _is_number(loc["longitude"]) or not -180 <= loc["longitude"] <= 180:
            raise ConfigError(f"location '{loc['id']}' has an invalid longitude: {loc['longitude']!r}")

        if loc["id"] in seen_ids:
            raise ConfigError(f"duplicate location id: '{loc['id']}'")
        seen_ids.add(loc["id"])

    return locations


def build_params(locations: list[dict]) -> dict:
    """Query parameters for ONE request covering every location.

    No `timezone` parameter on purpose: the documented default is GMT, which
    is what we want (everything stored in UTC). The transform step verifies
    each response really says GMT / offset 0, so this is a checked assumption
    rather than a hopeful one.
    """
    return {
        "latitude": ",".join(str(loc["latitude"]) for loc in locations),
        "longitude": ",".join(str(loc["longitude"]) for loc in locations),
        "current": ",".join(CURRENT_VARIABLES),
    }


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def _error_reason(response) -> str:
    """Open-Meteo explains 4xx errors as {"error": true, "reason": "..."}.
    Surface that reason instead of a bare status code."""
    try:
        body = response.json()
        if isinstance(body, dict) and body.get("reason"):
            return str(body["reason"])
    except ValueError:
        pass
    return (response.text or "").strip()[:200] or "no details given"


def fetch_weather(
    params: dict,
    *,
    url: str = None,
    max_attempts: int = 3,
    backoff_seconds: float = 2.0,
    sleep=time.sleep,
    get=None,
):
    """GET the weather, retrying transient failures with exponential backoff.

    `sleep` and `get` are injectable purely so tests can run instantly and
    without a network.
    """
    url = url or API_URL
    get = get or requests.get
    last_problem = "unknown error"

    for attempt in range(1, max_attempts + 1):
        try:
            response = get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
            last_problem = f"network error: {e}"
        else:
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError:
                    # A truncated or garbled body is as likely to be transient
                    # as anything else, so it gets the same retry treatment.
                    last_problem = "200 OK but the body was not valid JSON"
            elif response.status_code in RETRYABLE_STATUS:
                last_problem = f"HTTP {response.status_code}: {_error_reason(response)}"
            else:
                # Our request is wrong (bad variable name, bad coordinates...).
                # Asking again will not change the answer.
                raise ApiError(
                    f"API rejected the request (HTTP {response.status_code}): {_error_reason(response)}"
                )

        if attempt < max_attempts:
            delay = backoff_seconds * (2 ** (attempt - 1))
            log.warning("Attempt %d/%d failed (%s). Retrying in %.0fs.", attempt, max_attempts, last_problem, delay)
            sleep(delay)

    raise ApiError(f"API unavailable after {max_attempts} attempts: {last_problem}")


# ---------------------------------------------------------------------------
# Storing
# ---------------------------------------------------------------------------

def build_envelope(locations: list[dict], params: dict, payload, extracted_at: datetime, url: str = None) -> dict:
    """Wrap the untouched API payload with the context needed to understand
    it later. `response` is exactly what the API sent; everything else is
    provenance. Keeping the requested locations IN the file means an old raw
    file can still be interpreted correctly after config/locations.json
    changes."""
    return {
        "extracted_at": extracted_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "open-meteo",
        "request": {"url": url or API_URL, "params": params},
        "locations": locations,
        "response": payload,
    }


def _unique_path(directory: Path, stem: str, suffix: str) -> Path:
    """Never overwrite raw data. If a file with this name already exists
    (two runs in the same second), add -1, -2, ..."""
    candidate = directory / f"{stem}{suffix}"
    counter = 0
    while candidate.exists():
        counter += 1
        candidate = directory / f"{stem}-{counter}{suffix}"
    return candidate


def save_raw(envelope: dict, raw_dir: Path = None, extracted_at: datetime = None) -> Path:
    """Write the envelope to data/raw/weather_<UTC timestamp>.json.

    Written to a .tmp file first and then renamed, so a crash mid-write can
    never leave a half-written .json for the transform step to choke on.
    """
    raw_dir = raw_dir or RAW_DATA_DIR
    extracted_at = extracted_at or datetime.now(timezone.utc)
    raw_dir.mkdir(parents=True, exist_ok=True)

    stamp = extracted_at.strftime("%Y%m%dT%H%M%SZ")
    final_path = _unique_path(raw_dir, f"weather_{stamp}", ".json")
    temp_path = final_path.with_name(final_path.name + ".tmp")

    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(envelope, f, indent=2, ensure_ascii=False)
    os.replace(temp_path, final_path)

    return final_path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        locations = load_locations(LOCATIONS_PATH)
        params = build_params(locations)
        log.info("Requesting current conditions for %d locations...", len(locations))
        payload = fetch_weather(params)
    except ConfigError as e:
        log.error("Configuration error: %s", e)
        return 1
    except ApiError as e:
        log.error("API error: %s", e)
        return 1

    extracted_at = datetime.now(timezone.utc)
    envelope = build_envelope(locations, params, payload, extracted_at)

    try:
        saved_path = save_raw(envelope, RAW_DATA_DIR, extracted_at)
    except OSError as e:
        log.error("Could not write the raw file: %s", e)
        return 1

    log.info("Saved raw extract (%d locations) to %s", len(locations), saved_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
