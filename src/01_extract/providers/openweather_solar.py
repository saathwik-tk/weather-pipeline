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


def _get_json(params: dict[str, str]) -> dict[str, object]:
    request = Request(
        f"{API_URL}?{urlencode(params)}",
        headers={"User-Agent": "weather-pipeline/1.0 (OpenWeather Solar Irradiance API)"},
    )
    try:
        with urlopen(request, timeout=60) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError) as error:
        raise RuntimeError(f"OpenWeather Solar Irradiance request failed: {error}") from error
    if not isinstance(payload, dict) or "intervals" not in payload:
        raise ValueError("Unexpected OpenWeather Solar Irradiance response")
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
            }
        )
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
    LOGGER.info("OpenWeather processed %d rows (%d new, %d replaced)", len(rows), inserted, replaced)
    return inserted, replaced
