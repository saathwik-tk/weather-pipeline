"""NASA POWER hourly solar-radiation source adapter.

NASA POWER is public and does not require an API key. Solar values are satellite
derived, hourly, approximately 1-degree resolution, and can arrive 5–7 days late.
The extractor re-requests a rolling window and upserts by location and timestamp
so delayed values can be filled or corrected without duplicate CSV rows.
"""

from __future__ import annotations

import logging
import json
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd


LOGGER = logging.getLogger(__name__)
API_URL = "https://power.larc.nasa.gov/api/temporal/hourly/point"
SOURCE = "NASA POWER"
TIME_FORMAT = "%Y%m%d%H"
FILL_VALUE = -999

# Hourly all-sky and clear-sky solar irradiance parameters. NASA POWER's solar
# product is derived from satellite observations; these parameters are provided
# on the source grid rather than downscaled to city-level resolution.
SOLAR_PARAMETERS = (
    "ALLSKY_SFC_SW_DWN",  # Global horizontal irradiance (GHI)
    "ALLSKY_SFC_SW_DNI",  # Direct normal irradiance (DNI)
    "ALLSKY_SFC_SW_DIFF",  # Diffuse horizontal irradiance (DHI)
    "CLRSKY_SFC_SW_DWN",
    "CLRSKY_SFC_SW_DNI",
    "CLRSKY_SFC_SW_DIFF",
)


@dataclass(frozen=True)
class Location:
    city: str
    state: str
    latitude: float
    longitude: float


LOCATIONS = (
    Location("Mumbai", "Maharashtra", 19.0760, 72.8777),
    Location("Bengaluru", "Karnataka", 12.9716, 77.5946),
    Location("Agartala", "Tripura", 23.8315, 91.2868),
)


def _request_location(
    location: Location,
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    params = {
        "parameters": ",".join(SOLAR_PARAMETERS),
        "community": "RE",
        "longitude": location.longitude,
        "latitude": location.latitude,
        "start": start_date,
        "end": end_date,
        "format": "JSON",
        "time-standard": "UTC",
    }
    request_url = f"{API_URL}?{urlencode(params)}"
    request = Request(request_url, headers={"User-Agent": "weather-pipeline/1.0 (NASA POWER public API)"})
    payload: dict[str, Any] | None = None
    for attempt in range(5):
        try:
            with urlopen(request, timeout=90) as response:
                payload = json.loads(response.read().decode("utf-8"))
            break
        except HTTPError as error:
            if error.code not in {429, 500, 502, 503, 504} or attempt == 4:
                raise
            LOGGER.warning("NASA POWER returned HTTP %s; retrying", error.code)
        except (TimeoutError, URLError) as error:
            if attempt == 4:
                raise
            LOGGER.warning("NASA POWER request failed (%s); retrying", error)
        time.sleep(min(2**attempt, 30))

    if payload is None:
        raise RuntimeError(f"No response received from NASA POWER for {location.city}")

    if "properties" not in payload or "parameter" not in payload["properties"]:
        message = payload.get("messages") or payload.get("message") or payload
        raise ValueError(f"Unexpected NASA POWER response for {location.city}: {message}")

    parameter_data = payload["properties"]["parameter"]
    timestamps: set[str] = set()
    for values in parameter_data.values():
        timestamps.update(values.keys())

    retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: list[dict[str, Any]] = []
    for stamp in sorted(timestamps):
        values: dict[str, Any] = {}
        for parameter in SOLAR_PARAMETERS:
            raw_value = parameter_data.get(parameter, {}).get(stamp)
            if raw_value is None or raw_value == FILL_VALUE:
                values[parameter] = pd.NA
            else:
                values[parameter] = float(raw_value)

        # Skip timestamps for which NASA has not published any requested solar
        # value yet. Later hourly executions will pick them up in the lookback.
        if all(pd.isna(values[name]) for name in SOLAR_PARAMETERS):
            continue

        timestamp = datetime.strptime(stamp, TIME_FORMAT).replace(tzinfo=timezone.utc)
        rows.append(
            {
                "timestamp_utc": timestamp.isoformat(timespec="minutes"),
                "city": location.city,
                "state": location.state,
                "latitude": location.latitude,
                "longitude": location.longitude,
                **values,
                "source": SOURCE,
                "retrieved_at_utc": retrieved_at,
            }
        )
    return rows


def _upsert_partition(path: Path, incoming: pd.DataFrame) -> int:
    """Merge rows by city and UTC timestamp, replacing delayed/corrected values."""
    path.parent.mkdir(parents=True, exist_ok=True)
    key_columns = ["city", "timestamp_utc"]
    if path.exists() and path.stat().st_size:
        existing = pd.read_csv(path)
        combined = pd.concat([existing, incoming], ignore_index=True)
    else:
        combined = incoming.copy()

    before = len(combined)
    combined = combined.drop_duplicates(subset=key_columns, keep="last")
    combined["_sort_timestamp"] = pd.to_datetime(combined["timestamp_utc"], utc=True)
    combined = combined.sort_values(["_sort_timestamp", "city"]).drop(columns="_sort_timestamp")

    # Replace atomically so an interrupted hourly run cannot leave a truncated CSV.
    temporary_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", suffix=".csv", dir=path.parent, delete=False
        ) as temporary:
            temporary_path = temporary.name
        combined.to_csv(temporary_path, index=False, na_rep="")
        os.replace(temporary_path, path)
    finally:
        if temporary_path and os.path.exists(temporary_path):
            os.unlink(temporary_path)
    return before - len(combined)


def extract_date_range(project_root: Path, start_date: str, end_date: str) -> None:
    """Fetch and upsert an inclusive ISO date range for all configured cities."""
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError as error:
        raise ValueError("Dates must use YYYY-MM-DD format") from error
    if end < start:
        raise ValueError("end_date must be on or after start_date")

    api_start = start.strftime("%Y%m%d")
    api_end = end.strftime("%Y%m%d")
    LOGGER.info(
        "Fetching NASA POWER hourly solar data for %s through %s",
        api_start,
        api_end,
    )

    rows: list[dict[str, Any]] = []
    for location in LOCATIONS:
        LOGGER.info("Requesting %s, %s", location.city, location.state)
        rows.extend(_request_location(location, api_start, api_end))

    if not rows:
        LOGGER.warning("NASA POWER returned no published solar observations for this window")
        return

    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame["timestamp_utc"], utc=True).dt.strftime("%Y-%m-%d")
    inserted_or_updated = 0
    duplicates_replaced = 0
    for day, daily_rows in frame.groupby("date", sort=True):
        year, month, _ = day.split("-")
        output_path = (
            project_root
            / "data"
            / "nasa_power"
            / f"year={year}"
            / f"month={month}"
            / "hourly_solar_radiation.csv"
        )
        daily_rows = daily_rows.drop(columns="date")
        prior_rows = 0
        if output_path.exists() and output_path.stat().st_size:
            prior_rows = len(pd.read_csv(output_path))
        duplicates_replaced += _upsert_partition(output_path, daily_rows)
        after_rows = len(pd.read_csv(output_path))
        inserted_or_updated += max(0, after_rows - prior_rows)

    LOGGER.info(
        "Processed %d observations; added %d new rows and replaced %d duplicate keys",
        len(frame),
        inserted_or_updated,
        duplicates_replaced,
    )


def run_current_extraction(project_root: Path, lookback_days: int = 10) -> None:
    """Fetch recent available hourly observations for the configured cities."""
    if lookback_days < 1 or lookback_days > 30:
        raise ValueError("lookback_days must be between 1 and 30")

    today = datetime.now(timezone.utc).date()
    start_date = (today - timedelta(days=lookback_days)).isoformat()
    extract_date_range(project_root, start_date, today.isoformat())


def collect_current_weather(project_root: Path, lookback_days: int = 10) -> tuple[int, int]:
    """Fetch current atmospheric observations available from the NASA POWER API."""
    if lookback_days < 1 or lookback_days > 30:
        raise ValueError("lookback_days must be between 1 and 30")

    weather_parameters = (
        "T2M",
        "RH2M",
        "WS10M",
        "WS10M_MAX",
        "WS10M_MIN",
        "WD10M",
        "PRECTOTCORR",
        "PS",
        "ALLSKY_SFC_SW_DWN",
    )
    today = datetime.now(timezone.utc).date()
    start_date = (today - timedelta(days=lookback_days)).isoformat()
    rows: list[dict[str, Any]] = []
    for location in LOCATIONS:
        params = {
            "parameters": ",".join(weather_parameters),
            "community": "RE",
            "longitude": location.longitude,
            "latitude": location.latitude,
            "start": start_date.replace("-", ""),
            "end": today.isoformat().replace("-", ""),
            "format": "JSON",
            "time-standard": "UTC",
        }
        request_url = f"{API_URL}?{urlencode(params)}"
        request = Request(request_url, headers={"User-Agent": "weather-pipeline/1.0 (NASA POWER public API)"})
        payload: dict[str, Any] | None = None
        for attempt in range(5):
            try:
                with urlopen(request, timeout=90) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                break
            except HTTPError as error:
                if error.code not in {429, 500, 502, 503, 504} or attempt == 4:
                    raise
                LOGGER.warning("NASA POWER weather returned HTTP %s; retrying", error.code)
            except (TimeoutError, URLError) as error:
                if attempt == 4:
                    raise
                LOGGER.warning("NASA POWER weather request failed (%s); retrying", error)
            time.sleep(min(2**attempt, 30))
        if payload is None:
            continue
        parameter_data = payload.get("properties", {}).get("parameter", {})
        timestamps: set[str] = set()
        for values in parameter_data.values():
            timestamps.update(values.keys())
        for stamp in sorted(timestamps):
            values: dict[str, Any] = {}
            for parameter in weather_parameters:
                raw_value = parameter_data.get(parameter, {}).get(stamp)
                values[parameter] = pd.NA if raw_value is None or raw_value == FILL_VALUE else float(raw_value)
            if all(pd.isna(values[name]) for name in weather_parameters):
                continue
            timestamp = datetime.strptime(stamp, TIME_FORMAT).replace(tzinfo=timezone.utc)
            rows.append(
                {
                    "timestamp_utc": timestamp.isoformat(timespec="minutes"),
                    "city": location.city,
                    "state": location.state,
                    "latitude": location.latitude,
                    "longitude": location.longitude,
                    **values,
                    "source": SOURCE,
                    "retrieved_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                }
            )

    if not rows:
        LOGGER.warning("NASA POWER returned no atmospheric rows for %s through %s", start_date, today.isoformat())
        return 0, 0

    from storage import upsert_weather_rows

    inserted, replaced = upsert_weather_rows(project_root, "nasa_power", rows)
    LOGGER.info("NASA POWER weather processed %d rows (%d new, %d replaced)", len(rows), inserted, replaced)
    return inserted, replaced


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_current_extraction(project_root=Path(__file__).resolve().parents[3])
