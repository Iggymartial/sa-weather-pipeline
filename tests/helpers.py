"""
Shared test helpers.

The fake API entries below are modelled on Open-Meteo's DOCUMENTED response
format, not captured from a live call. Once the first real extraction has
run, one real raw file should be copied into data/samples/ and used as an
extra fixture - the closer tests sit to real data, the more they're worth.
"""


def make_entry(latitude, longitude, *, time="2026-10-22T09:15", **current_overrides):
    """One location's worth of API response.

    Note the returned coordinates are deliberately NOT identical to the
    requested ones: the real API snaps to the centre of its model grid, and
    the pipeline must never rely on them matching.
    """
    current = {
        "time": time,
        "interval": 900,
        "temperature_2m": 21.4,
        "relative_humidity_2m": 48,
        "apparent_temperature": 20.1,
        "precipitation": 0.0,
        "weather_code": 1,
        "cloud_cover": 25,
        "wind_speed_10m": 11.2,
        "wind_direction_10m": 140,
    }
    current.update(current_overrides)

    return {
        "latitude": round(latitude, 1) + 0.03,
        "longitude": round(longitude, 1) - 0.02,
        "generationtime_ms": 0.05,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "elevation": 1753.0,
        "current_units": {
            "time": "iso8601",
            "interval": "seconds",
            "temperature_2m": "°C",
            "relative_humidity_2m": "%",
            "apparent_temperature": "°C",
            "precipitation": "mm",
            "weather_code": "wmo code",
            "cloud_cover": "%",
            "wind_speed_10m": "km/h",
            "wind_direction_10m": "°",
        },
        "current": current,
    }


class FakeResponse:
    """Just enough of requests.Response for the extractor to use."""

    def __init__(self, status_code=200, json_data=None, text="", invalid_json=False):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text
        self._invalid_json = invalid_json

    def json(self):
        if self._invalid_json:
            raise ValueError("not json")
        return self._json_data


def make_get(*outcomes):
    """A fake replacement for requests.get that plays back scripted outcomes
    in order. Each outcome is either a response object (returned) or an
    exception (raised). Records every call so tests can inspect them."""
    remaining = list(outcomes)
    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        outcome = remaining.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    fake_get.calls = calls
    return fake_get
