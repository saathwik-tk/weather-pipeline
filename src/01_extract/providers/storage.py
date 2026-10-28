"""Shared date-partitioned CSV upsert for extraction providers."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd


def upsert_partitioned_rows(
    project_root: Path,
    provider: str,
    dataset_name: str,
    filename: str,
    rows: list[dict[str, Any]],
) -> tuple[int, int]:
    """Write provider rows by UTC date into a dataset-specific partition."""
    if not rows:
        return 0, 0

    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame["timestamp_utc"], utc=True).dt.strftime("%Y-%m-%d")
    inserted = 0
    replaced = 0
    for day, daily_rows in frame.groupby("date", sort=True):
        year, month, _ = day.split("-")
        output_path = project_root / "data" / provider / dataset_name / f"year={year}" / f"month={month}" / f"{filename}.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        daily_rows = daily_rows.drop(columns="date")
        key_columns = ["city", "timestamp_utc"]

        if output_path.exists() and output_path.stat().st_size:
            existing = pd.read_csv(output_path)
            combined = pd.concat([existing, daily_rows], ignore_index=True)
        else:
            existing = pd.DataFrame(columns=daily_rows.columns)
            combined = daily_rows.copy()

        combined = combined.drop_duplicates(subset=key_columns, keep="last")
        combined["_sort_timestamp"] = pd.to_datetime(combined["timestamp_utc"], utc=True)
        combined = combined.sort_values(["_sort_timestamp", "city"]).drop(columns="_sort_timestamp")
        new_rows = max(0, len(combined) - len(existing))
        inserted += new_rows
        replaced += max(0, len(daily_rows) - new_rows)

        temporary_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", newline="", suffix=".csv", dir=output_path.parent, delete=False
            ) as temporary:
                temporary_path = temporary.name
            combined.to_csv(temporary_path, index=False, na_rep="")
            os.replace(temporary_path, output_path)
        finally:
            if temporary_path and os.path.exists(temporary_path):
                os.unlink(temporary_path)

    return inserted, replaced


def upsert_solar_rows(project_root: Path, provider: str, rows: list[dict[str, Any]]) -> tuple[int, int]:
    """Write rows to the solar partition for a provider."""
    return upsert_partitioned_rows(project_root, provider, "solar", "hourly_solar_radiation", rows)


def upsert_weather_rows(project_root: Path, provider: str, rows: list[dict[str, Any]], *, forecast: bool = False) -> tuple[int, int]:
    """Write rows to the atmospheric weather partition for a provider."""
    dataset_name = "weather_forecast" if forecast else "weather"
    filename = "hourly_weather_forecast" if forecast else "hourly_weather"
    return upsert_partitioned_rows(project_root, provider, dataset_name, filename, rows)
