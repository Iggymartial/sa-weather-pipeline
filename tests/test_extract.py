"""
Unit tests for extractor/extract.py.

Nothing here touches the network or sleeps for real: HTTP is replaced by a
scripted fake and the retry delay is replaced by a recorder. Testing against
the REAL API is a separate, manual step (the first live run), not something
that should happen every time the suite runs.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

import extract
from helpers import FakeResponse, make_entry, make_get

REAL_CONFIG = Path(__file__).resolve().parent.parent / "config" / "locations.json"


def write_config(tmp_path, content):
    path = tmp_path / "locations.json"
    path.write_text(content if isinstance(content, str) else json.dumps(content), encoding="utf-8")
    return path


def valid_location(**overrides):
    loc = {"id": "test-city", "name": "Test City", "province": "Gauteng", "latitude": -26.2, "longitude": 28.0}
    loc.update(overrides)
    return loc


# ---------------------------------------------------------------------------
# load_locations
# ---------------------------------------------------------------------------

def test_the_real_config_file_is_valid():
    """Guards the file that actually ships with the project."""
    locations = extract.load_locations(REAL_CONFIG)

    assert len(locations) == 9
    # The selection rule is "one city per province" - the test enforces it.
    assert len({loc["province"] for loc in locations}) == 9


def test_missing_config_file_raises(tmp_path):
    with pytest.raises(extract.ConfigError, match="not found"):
        extract.load_locations(tmp_path / "nope.json")


def test_config_that_is_not_json_raises(tmp_path):
    path = write_config(tmp_path, "{ this is not json")
    with pytest.raises(extract.ConfigError, match="not valid JSON"):
        extract.load_locations(path)


@pytest.mark.parametrize("content", [{"locations": []}, {"locations": "x"}, {}, []])
def test_empty_or_malformed_location_list_raises(tmp_path, content):
    path = write_config(tmp_path, content)
    with pytest.raises(extract.ConfigError, match="non-empty"):
        extract.load_locations(path)


def test_location_missing_a_field_raises(tmp_path):
    loc = valid_location()
    del loc["province"]
    path = write_config(tmp_path, {"locations": [loc]})
    with pytest.raises(extract.ConfigError, match="missing fields"):
        extract.load_locations(path)


@pytest.mark.parametrize("field,value", [
    ("latitude", 91), ("latitude", -91), ("longitude", 181), ("longitude", -181),
    ("latitude", "26.2"), ("longitude", None), ("latitude", True),
])
def test_invalid_coordinates_raise(tmp_path, field, value):
    path = write_config(tmp_path, {"locations": [valid_location(**{field: value})]})
    with pytest.raises(extract.ConfigError, match="invalid"):
        extract.load_locations(path)


def test_duplicate_location_ids_raise(tmp_path):
    path = write_config(tmp_path, {"locations": [valid_location(), valid_location()]})
    with pytest.raises(extract.ConfigError, match="duplicate"):
        extract.load_locations(path)


# ---------------------------------------------------------------------------
# build_params
# ---------------------------------------------------------------------------

def test_params_keep_location_order_and_are_comma_separated():
    locations = [
        valid_location(id="a", latitude=-26.2, longitude=28.0),
        valid_location(id="b", latitude=-33.9, longitude=18.4),
    ]
    params = extract.build_params(locations)

    # Order matters: results are matched back to locations BY POSITION.
    assert params["latitude"] == "-26.2,-33.9"
    assert params["longitude"] == "28.0,18.4"


def test_params_request_every_current_variable():
    params = extract.build_params([valid_location()])
    assert params["current"].split(",") == extract.CURRENT_VARIABLES


def test_params_do_not_override_the_default_utc_timezone():
    """The transform step verifies the response says GMT; sending our own
    timezone could quietly break that guarantee."""
    assert "timezone" not in extract.build_params([valid_location()])


# ---------------------------------------------------------------------------
# fetch_weather: success and retry behaviour
# ---------------------------------------------------------------------------

def run_fetch(*outcomes, **kwargs):
    """Run fetch_weather against scripted outcomes with instant, recorded sleeps."""
    sleeps = []
    get = make_get(*outcomes)
    try:
        result = extract.fetch_weather({"a": "b"}, get=get, sleep=sleeps.append, **kwargs)
    except extract.ApiError as e:
        return e, get, sleeps
    return result, get, sleeps


def test_successful_fetch_returns_the_parsed_json():
    payload = [make_entry(-26.2, 28.0)]
    result, get, sleeps = run_fetch(FakeResponse(200, payload))

    assert result == payload
    assert len(get.calls) == 1
    assert sleeps == []


def test_request_is_sent_with_params_timeout_and_user_agent():
    _, get, _ = run_fetch(FakeResponse(200, []))
    call = get.calls[0]

    assert call["params"] == {"a": "b"}
    assert call["timeout"] == extract.TIMEOUT
    assert "User-Agent" in call["headers"]


def test_retries_a_503_then_succeeds():
    payload = [make_entry(-26.2, 28.0)]
    result, get, sleeps = run_fetch(FakeResponse(503, text="busy"), FakeResponse(200, payload))

    assert result == payload
    assert len(get.calls) == 2
    assert sleeps == [2.0]


def test_backoff_doubles_between_attempts():
    result, get, sleeps = run_fetch(
        FakeResponse(503), FakeResponse(503), FakeResponse(503), max_attempts=3
    )

    assert isinstance(result, extract.ApiError)
    assert sleeps == [2.0, 4.0]  # no pointless sleep after the final attempt


def test_gives_up_after_max_attempts_and_says_so():
    result, get, _ = run_fetch(FakeResponse(500), FakeResponse(500), FakeResponse(500))

    assert isinstance(result, extract.ApiError)
    assert "after 3 attempts" in str(result)
    assert len(get.calls) == 3


def test_a_429_rate_limit_is_retried():
    payload = [make_entry(-26.2, 28.0)]
    result, get, _ = run_fetch(FakeResponse(429), FakeResponse(200, payload))

    assert result == payload
    assert len(get.calls) == 2


def test_a_timeout_is_retried():
    payload = [make_entry(-26.2, 28.0)]
    result, get, _ = run_fetch(requests.Timeout("slow"), FakeResponse(200, payload))

    assert result == payload
    assert len(get.calls) == 2


def test_a_connection_error_that_never_clears_fails_cleanly():
    result, get, _ = run_fetch(
        requests.ConnectionError("down"), requests.ConnectionError("down"), requests.ConnectionError("down")
    )

    assert isinstance(result, extract.ApiError)
    assert "network error" in str(result)


def test_a_200_with_a_garbled_body_is_retried():
    payload = [make_entry(-26.2, 28.0)]
    result, get, _ = run_fetch(FakeResponse(200, invalid_json=True), FakeResponse(200, payload))

    assert result == payload
    assert len(get.calls) == 2


def test_a_400_is_not_retried_and_the_api_reason_is_reported():
    bad = FakeResponse(400, {"error": True, "reason": "Cannot initialize WeatherVariable from invalid String value"})
    result, get, sleeps = run_fetch(bad)

    assert isinstance(result, extract.ApiError)
    assert "Cannot initialize WeatherVariable" in str(result)
    assert len(get.calls) == 1   # retrying a wrong request can't help
    assert sleeps == []


# ---------------------------------------------------------------------------
# build_envelope and save_raw
# ---------------------------------------------------------------------------

NOW = datetime(2026, 10, 22, 9, 0, 5, tzinfo=timezone.utc)


def test_envelope_keeps_the_payload_exactly_as_received():
    payload = [make_entry(-26.2, 28.0), make_entry(-33.9, 18.4)]
    locations = [valid_location(id="a"), valid_location(id="b")]
    envelope = extract.build_envelope(locations, {"p": "q"}, payload, NOW)

    assert envelope["response"] == payload          # untouched
    assert envelope["locations"] == locations       # request order preserved
    assert envelope["extracted_at"] == "2026-10-22T09:00:05Z"
    assert envelope["request"]["params"] == {"p": "q"}
    assert envelope["source"] == "open-meteo"


def test_save_raw_writes_a_timestamped_file_that_round_trips(tmp_path):
    envelope = extract.build_envelope([valid_location()], {}, [make_entry(-26.2, 28.0)], NOW)
    path = extract.save_raw(envelope, tmp_path, NOW)

    assert path.name == "weather_20261022T090005Z.json"
    assert json.loads(path.read_text(encoding="utf-8")) == envelope


def test_save_raw_creates_the_directory_if_missing(tmp_path):
    target = tmp_path / "deep" / "raw"
    path = extract.save_raw({"x": 1}, target, NOW)
    assert path.exists()


def test_save_raw_never_overwrites_existing_data(tmp_path):
    first = extract.save_raw({"run": 1}, tmp_path, NOW)
    second = extract.save_raw({"run": 2}, tmp_path, NOW)   # same second

    assert first != second
    assert second.name == "weather_20261022T090005Z-1.json"
    assert json.loads(first.read_text())["run"] == 1       # original intact
    assert json.loads(second.read_text())["run"] == 2


def test_save_raw_leaves_no_temp_file_behind(tmp_path):
    extract.save_raw({"x": 1}, tmp_path, NOW)
    assert [p.name for p in tmp_path.iterdir() if p.name.endswith(".tmp")] == []


# ---------------------------------------------------------------------------
# main(): exit codes are what Airflow will read
# ---------------------------------------------------------------------------

@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Point the module at a throwaway config file and raw directory."""
    config = write_config(tmp_path, {"locations": [valid_location(id="a"), valid_location(id="b", latitude=-33.9)]})
    raw_dir = tmp_path / "raw"
    monkeypatch.setattr(extract, "LOCATIONS_PATH", config)
    monkeypatch.setattr(extract, "RAW_DATA_DIR", raw_dir)
    return raw_dir


def test_main_succeeds_and_writes_exactly_one_raw_file(sandbox, monkeypatch):
    payload = [make_entry(-26.2, 28.0), make_entry(-33.9, 28.0)]
    monkeypatch.setattr(extract, "fetch_weather", lambda params: payload)

    assert extract.main() == 0

    files = list(sandbox.glob("weather_*.json"))
    assert len(files) == 1
    saved = json.loads(files[0].read_text(encoding="utf-8"))
    assert saved["response"] == payload
    assert [loc["id"] for loc in saved["locations"]] == ["a", "b"]


def test_main_returns_1_and_writes_nothing_when_the_api_fails(sandbox, monkeypatch):
    def broken(params):
        raise extract.ApiError("API unavailable after 3 attempts")

    monkeypatch.setattr(extract, "fetch_weather", broken)

    assert extract.main() == 1
    assert not sandbox.exists() or list(sandbox.iterdir()) == []


def test_main_returns_1_on_bad_config(tmp_path, monkeypatch):
    monkeypatch.setattr(extract, "LOCATIONS_PATH", tmp_path / "missing.json")
    monkeypatch.setattr(extract, "RAW_DATA_DIR", tmp_path / "raw")

    assert extract.main() == 1


def test_main_returns_1_if_the_raw_file_cannot_be_written(sandbox, monkeypatch):
    monkeypatch.setattr(extract, "fetch_weather", lambda params: [make_entry(-26.2, 28.0)])

    def cannot_write(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(extract, "save_raw", cannot_write)

    assert extract.main() == 1
