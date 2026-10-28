"""OpenWeather Solar Irradiance current hourly adapter."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from nasa_power import LOCATIONS
from storage import upsert_solar_rows


LOGGER = logging.getLogger(__name__)
API_URL = "https://api.openweathermap.org/energy/2.0/solar/interval_data"
LOCAL_OFFSET = timezone(timedelta(hours=5, minutes=30))


def _get_json(params: dict[str, str], api_url: str = API_URL) -> dict[str, object]:
    request = Request(
        f"{api_url}?{urlencode(params)}",
        headers={"User-Agent": "weather-pipeline/1.0 (OpenWeather API)"},
    )
    try:
        with urlopen(request, timeout=60) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError) as error:
        raise RuntimeError(f"OpenWeather request failed: {error}") from error
    if not isinstance(payload, dict):
        raise ValueError("Unexpected OpenWeather response")
    return payload


def collect_current(project_root: Path) -> tuple[int, int]:
    """Fetch the latest completed hourly solar interval for each city.

    Requires OPENWEATHER_API_KEY with access to the separately subscribed Solar
    Irradiance API product. This adapter performs one API request per city.
    """
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    if not api_key:
        raise RuntimeError("Set OPENWEATHER_API_KEY to use the OpenWeather Solar Irradiance API")

    now_local = datetime.now(LOCAL_OFFSET)
    request_date = now_local.date()
    if now_local.hour == 0:
        # At midnight the current date has no completed interval yet; fetch the
        # previous day's final hour so the hourly collector does not skip it.
        request_date -= timedelta(days=1)
    day = request_date.isoformat()
    completed_before = now_local.replace(minute=0, second=0, microsecond=0)
    retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: list[dict[str, object]] = []

    for location in LOCATIONS:
        payload = _get_json(
            {
                "lat": str(location.latitude),
                "lon": str(location.longitude),
                "date": day,
                "interval": "1h",
                "tz": "+05:30",
                "appid": api_key,
            },
            API_URL,
        )
        if "intervals" not in payload:
            raise ValueError("Unexpected OpenWeather Solar Irradiance response")
        eligible = []
        for interval in payload["intervals"]:
            if not isinstance(interval, dict):
                continue
            start = datetime.combine(
                request_date,
                datetime.strptime(interval["start"], "%H:%M").time(),
                LOCAL_OFFSET,
            )
            if start < completed_before:
                eligible.append((start, interval))
        if not eligible:
            LOGGER.warning("No completed OpenWeather hourly interval for %s", location.city)
            continue

        start, interval = eligible[-1]
        row: dict[str, object] = {
            "timestamp_utc": start.astimezone(timezone.utc).isoformat(timespec="minutes"),
            "city": location.city,
            "state": location.state,
            "latitude": location.latitude,
            "longitude": location.longitude,
            "source": "OpenWeather Solar Irradiance API",
            "retrieved_at_utc": retrieved_at,
            "sunrise_local": payload.get("sunrise"),
            "sunset_local": payload.get("sunset"),
        }
        for group_name in ("avg_irradiance", "max_irradiance", "irradiation"):
            group = interval.get(group_name, {})
            for sky in ("clear_sky", "cloudy_sky"):
                metrics = group.get(sky, {}) if isinstance(group, dict) else {}
                for variable in ("ghi", "dni", "dhi"):
                    row[f"{group_name}_{sky}_{variable}"] = metrics.get(variable) if isinstance(metrics, dict) else None
        rows.append(row)

    inserted, replaced = upsert_solar_rows(project_root, "openweather_solar", rows)
    LOGGER.info("OpenWeather solar processed %d rows (%d new, %d replaced)", len(rows), inserted, replaced)
    return inserted, replaced


def collect_current_weather(project_root: Path) -> tuple[int, int]:
    """Fetch the most recent atmospheric weather reading for all locations."""
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    if not api_key:
        raise RuntimeError("Set OPENWEATHER_API_KEY to use the OpenWeather weather API")

    rows: list[dict[str, object]] = []
    retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for location in LOCATIONS:
        payload = _get_json(
            {
                "lat": str(location.latitude),
                "lon": str(location.longitude),
                "appid": api_key,
                "units": "metric",
            },
            "https://api.openweathermap.org/data/2.5/weather",
        )
        if not isinstance(payload, dict):
            continue
        timestamp = datetime.fromtimestamp(int(payload.get("dt", 0)), tz=timezone.utc)
        row: dict[str, object] = {
            "timestamp_utc": timestamp.isoformat(timespec="minutes"),
            "city": location.city,
            "state": location.state,
            "latitude": location.latitude,
            "longitude": location.longitude,
            "source": "OpenWeather Current Weather API",
            "data_type": "live_weather",
            "retrieved_at_utc": retrieved_at,
            "temperature_c": payload.get("main", {}).get("temp"),
            "feels_like_c": payload.get("main", {}).get("feels_like"),
            "humidity_pct": payload.get("main", {}).get("humidity"),
            "pressure_hpa": payload.get("main", {}).get("pressure"),
            "wind_speed_ms": payload.get("wind", {}).get("speed"),
            "wind_direction_deg": payload.get("wind", {}).get("deg"),
            "cloud_cover_pct": payload.get("clouds", {}).get("all"),
            "weather_main": (payload.get("weather") or [{}])[0].get("main"),
            "weather_description": (payload.get("weather") or [{}])[0].get("description"),
            "weather_code": (payload.get("weather") or [{}])[0].get("id"),
        }
        rows.append(row)

    from storage import upsert_weather_rows

    inserted, replaced = upsert_weather_rows(project_root, "openweather_solar", rows)
    LOGGER.info("OpenWeather weather processed %d rows (%d new, %d replaced)", len(rows), inserted, replaced)
    return inserted, replaced


def collect_forecast_weather(project_root: Path, forecast_hours: int = 48) -> tuple[int, int]:
    """Fetch hourly weather forecast rows for all configured locations."""
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    if not api_key:
        raise RuntimeError("Set OPENWEATHER_API_KEY to use the OpenWeather weather forecast API")
    if forecast_hours < 1:
        raise ValueError("forecast_hours must be positive")

    rows: list[dict[str, object]] = []
    retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for location in LOCATIONS:
        payload = _get_json(
            {
                "lat": str(location.latitude),
                "lon": str(location.longitude),
                "appid": api_key,
                "units": "metric",
                "cnt": str(min(max(forecast_hours, 1), 40)),
            },
            "https://api.openweathermap.org/data/2.5/forecast",
        )
        if not isinstance(payload, dict):
            continue
        for item in payload.get("list", []):
            if not isinstance(item, dict):
                continue
            timestamp = datetime.fromtimestamp(int(item.get("dt", 0)), tz=timezone.utc)
            row: dict[str, object] = {
                "timestamp_utc": timestamp.isoformat(timespec="minutes"),
                "city": location.city,
                "state": location.state,
                "latitude": location.latitude,
                "longitude": location.longitude,
                "source": "OpenWeather Forecast API",
                "data_type": "weather_forecast",
                "retrieved_at_utc": retrieved_at,
                "temperature_c": item.get("main", {}).get("temp"),
                "feels_like_c": item.get("main", {}).get("feels_like"),
                "humidity_pct": item.get("main", {}).get("humidity"),
                "pressure_hpa": item.get("main", {}).get("pressure"),
                "wind_speed_ms": item.get("wind", {}).get("speed"),
                "wind_direction_deg": item.get("wind", {}).get("deg"),
                "cloud_cover_pct": item.get("clouds", {}).get("all"),
                "weather_main": (item.get("weather") or [{}])[0].get("main"),
                "weather_description": (item.get("weather") or [{}])[0].get("description"),
                "weather_code": (item.get("weather") or [{}])[0].get("id"),
            }
            rows.append(row)

    from storage import upsert_weather_rows

    inserted, replaced = upsert_weather_rows(project_root, "openweather_solar", rows, forecast=True)
    LOGGER.info("OpenWeather forecast processed %d rows (%d new, %d replaced)", len(rows), inserted, replaced)
    return inserted, replaced
