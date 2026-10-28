"""Open-Meteo current hourly solar-radiation adapter (weather-model data)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from nasa_power import LOCATIONS
from storage import upsert_solar_rows


LOGGER = logging.getLogger(__name__)
API_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARIABLES = (
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "direct_normal_irradiance",
    "global_tilted_irradiance",
    "terrestrial_radiation",
    "shortwave_radiation_instant",
    "direct_radiation_instant",
    "diffuse_radiation_instant",
    "direct_normal_irradiance_instant",
    "global_tilted_irradiance_instant",
    "terrestrial_radiation_instant",
)


def _get_json(params: dict[str, str]) -> object:
    request = Request(
        f"{API_URL}?{urlencode(params)}",
        headers={"User-Agent": "weather-pipeline/1.0 (Open-Meteo forecast API)"},
    )
    try:
        with urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError) as error:
        raise RuntimeError(f"Open-Meteo request failed: {error}") from error


def collect_current(project_root: Path) -> tuple[int, int]:
    """Fetch the latest hourly model value at or before now for all cities."""
    now = datetime.now(timezone.utc)
    locations = list(LOCATIONS)
    params = {
        "latitude": ",".join(str(location.latitude) for location in locations),
        "longitude": ",".join(str(location.longitude) for location in locations),
        "hourly": ",".join(HOURLY_VARIABLES),
        "past_hours": "3",
        "forecast_hours": "1",
        "timezone": "UTC",
        "tilt": "0",
        "azimuth": "0",
    }
    payload = _get_json(params)
    responses = payload if isinstance(payload, list) else [payload]
    if len(responses) != len(locations):
        raise ValueError(f"Open-Meteo returned {len(responses)} locations; expected {len(locations)}")

    retrieved_at = now.isoformat(timespec="seconds")
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    rows: list[dict[str, object]] = []
    for location, response in zip(locations, responses):
        if not isinstance(response, dict) or "hourly" not in response:
            raise ValueError(f"Unexpected Open-Meteo response for {location.city}")
        hourly = response["hourly"]
        times = hourly.get("time", [])
        eligible = [
            index
            for index, stamp in enumerate(times)
            if datetime.fromisoformat(stamp).replace(tzinfo=timezone.utc) <= current_hour
        ]
        if not eligible:
            LOGGER.warning("No current hourly value in Open-Meteo response for %s", location.city)
            continue
        index = eligible[-1]
        timestamp = datetime.fromisoformat(times[index]).replace(tzinfo=timezone.utc)
        row: dict[str, object] = {
            "timestamp_utc": timestamp.isoformat(timespec="minutes"),
            "city": location.city,
            "state": location.state,
            "latitude": location.latitude,
            "longitude": location.longitude,
            "source": "Open-Meteo Forecast API",
            "data_type": "weather_model_estimate",
            "retrieved_at_utc": retrieved_at,
        }
        for name in HOURLY_VARIABLES:
            values = hourly.get(name)
            row[name] = values[index] if values is not None and index < len(values) else None
        rows.append(row)

    inserted, replaced = upsert_solar_rows(project_root, "open_meteo", rows)
    LOGGER.info("Open-Meteo solar processed %d rows (%d new, %d replaced)", len(rows), inserted, replaced)
    return inserted, replaced


def collect_current_weather(project_root: Path) -> tuple[int, int]:
    """Fetch the latest live atmospheric observations for each city."""
    weather_variables = (
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "snowfall",
        "weather_code",
        "surface_pressure",
        "cloud_cover",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
    )
    now = datetime.now(timezone.utc)
    locations = list(LOCATIONS)
    params = {
        "latitude": ",".join(str(location.latitude) for location in locations),
        "longitude": ",".join(str(location.longitude) for location in locations),
        "hourly": ",".join(weather_variables),
        "past_hours": "3",
        "forecast_hours": "1",
        "timezone": "UTC",
    }
    payload = _get_json(params)
    responses = payload if isinstance(payload, list) else [payload]
    if len(responses) != len(locations):
        raise ValueError(f"Open-Meteo weather returned {len(responses)} locations; expected {len(locations)}")

    rows: list[dict[str, object]] = []
    retrieved_at = now.isoformat(timespec="seconds")
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    for location, response in zip(locations, responses):
        if not isinstance(response, dict) or "hourly" not in response:
            raise ValueError(f"Unexpected Open-Meteo weather response for {location.city}")
        hourly = response["hourly"]
        times = hourly.get("time", [])
        eligible = [
            index for index, stamp in enumerate(times)
            if datetime.fromisoformat(stamp).replace(tzinfo=timezone.utc) <= current_hour
        ]
        if not eligible:
            LOGGER.warning("No current weather value in Open-Meteo response for %s", location.city)
            continue
        index = eligible[-1]
        timestamp = datetime.fromisoformat(times[index]).replace(tzinfo=timezone.utc)
        row: dict[str, object] = {
            "timestamp_utc": timestamp.isoformat(timespec="minutes"),
            "city": location.city,
            "state": location.state,
            "latitude": location.latitude,
            "longitude": location.longitude,
            "source": "Open-Meteo Forecast API",
            "data_type": "live_weather",
            "retrieved_at_utc": retrieved_at,
        }
        for name in weather_variables:
            values = hourly.get(name)
            row[name] = values[index] if values is not None and index < len(values) else None
        rows.append(row)

    from storage import upsert_weather_rows

    inserted, replaced = upsert_weather_rows(project_root, "open_meteo", rows)
    LOGGER.info("Open-Meteo weather processed %d rows (%d new, %d replaced)", len(rows), inserted, replaced)
    return inserted, replaced


def collect_forecast_weather(project_root: Path, forecast_hours: int = 48) -> tuple[int, int]:
    """Fetch a short-range weather forecast for each configured coordinate set."""
    if forecast_hours < 1:
        raise ValueError("forecast_hours must be positive")
    weather_variables = (
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "snowfall",
        "weather_code",
        "surface_pressure",
        "cloud_cover",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
    )
    now = datetime.now(timezone.utc)
    locations = list(LOCATIONS)
    params = {
        "latitude": ",".join(str(location.latitude) for location in locations),
        "longitude": ",".join(str(location.longitude) for location in locations),
        "hourly": ",".join(weather_variables),
        "forecast_hours": str(forecast_hours),
        "timezone": "UTC",
    }
    payload = _get_json(params)
    responses = payload if isinstance(payload, list) else [payload]
    if len(responses) != len(locations):
        raise ValueError(f"Open-Meteo forecast returned {len(responses)} locations; expected {len(locations)}")

    rows: list[dict[str, object]] = []
    retrieved_at = now.isoformat(timespec="seconds")
    for location, response in zip(locations, responses):
        if not isinstance(response, dict) or "hourly" not in response:
            raise ValueError(f"Unexpected Open-Meteo forecast response for {location.city}")
        hourly = response["hourly"]
        times = hourly.get("time", [])
        for index, stamp in enumerate(times):
            timestamp = datetime.fromisoformat(stamp).replace(tzinfo=timezone.utc)
            row: dict[str, object] = {
                "timestamp_utc": timestamp.isoformat(timespec="minutes"),
                "city": location.city,
                "state": location.state,
                "latitude": location.latitude,
                "longitude": location.longitude,
                "source": "Open-Meteo Forecast API",
                "data_type": "weather_forecast",
                "retrieved_at_utc": retrieved_at,
            }
            for name in weather_variables:
                values = hourly.get(name)
                row[name] = values[index] if values is not None and index < len(values) else None
            rows.append(row)

    from storage import upsert_weather_rows

    inserted, replaced = upsert_weather_rows(project_root, "open_meteo", rows, forecast=True)
    LOGGER.info("Open-Meteo forecast processed %d rows (%d new, %d replaced)", len(rows), inserted, replaced)
    return inserted, replaced
