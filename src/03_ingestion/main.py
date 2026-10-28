"""Build dashboard-ready Gold tables from extracted CSVs and Silver Parquet."""

from __future__ import annotations

import argparse
import logging
import math
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

LOG = logging.getLogger("weather_gold")
ROOT = Path(__file__).resolve().parents[2]
MEASURES = {
    "temperature": ("temperature_2m", "temperature_2m_c", "temperature_c", "temperature", "temp", "t2m"),
    "apparent_temperature": ("apparent_temperature", "apparent_temperature_c", "feels_like"),
    "relative_humidity": ("relative_humidity_2m", "relative_humidity_pct", "humidity_pct", "humidity", "rh2m"),
    "precipitation": ("precipitation", "precipitation_mm", "precip_mm", "rain", "prectotcorr"),
    "ghi": ("shortwave_radiation", "solar_radiation_wm2", "ghi_wm2", "ghi", "allsky_sfc_sw_dwn"),
    "dni": ("direct_normal_irradiance", "dni_wm2", "dni"),
    "dhi": ("diffuse_radiation", "dhi_wm2", "dhi"),
    "cloud_cover": ("cloud_cover", "cloud_cover_pct", "cloudcover"),
    "wind_speed_10m": ("wind_speed_10m", "wind_speed_10m_mps", "wind_speed_ms", "ws10m", "wind_speed"),
    "wind_direction_10m": ("wind_direction_10m", "wind_direction_10m_deg", "wind_direction_deg", "wd10m"),
    "surface_pressure": ("surface_pressure", "air_pressure_hpa", "pressure_hpa", "pressure", "ps"),
}
UNIT = {"temperature": "degC or source unit", "apparent_temperature": "degC or source unit", "relative_humidity": "%", "precipitation": "mm or source unit", "ghi": "W/m2 or source unit", "dni": "W/m2", "dhi": "W/m2", "cloud_cover": "%", "wind_speed_10m": "m/s or source unit", "wind_direction_10m": "degrees", "surface_pressure": "hPa or source unit"}


def snake(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name.strip()).lower()).strip("_")


def state_code(value) -> str | None:
    normalized = snake(str(value)) if value is not None and pd.notna(value) else ""
    return {"ka": "KA", "karnataka": "KA", "mh": "MH", "maharashtra": "MH", "tr": "TR", "tripura": "TR"}.get(normalized)


def read_coordinates(spark: SparkSession, silver: Path, coordinate_file: Path | None) -> pd.DataFrame:
    if coordinate_file:
        coords = pd.read_csv(coordinate_file)
        coords.columns = [snake(c) for c in coords.columns]
        aliases = {"latitude": ("latitude", "lat"), "longitude": ("longitude", "lon", "lng"), "state_code": ("state_code", "state")}
        for target, choices in aliases.items():
            if target not in coords:
                match = next((c for c in choices if c in coords), None)
                if match:
                    coords[target] = coords[match]
        if not {"latitude", "longitude", "state_code"}.issubset(coords.columns):
            raise ValueError("Coordinate CSV must include latitude, longitude, and state_code (KA, MH, or TR)")
        coords["coordinate_id"] = coords.get("coordinate_id", pd.Series([None] * len(coords)))
        coords["label"] = coords.get("label", pd.Series([None] * len(coords)))
        coords["coordinate_id"] = coords.apply(lambda r: str(r.coordinate_id) if pd.notna(r.coordinate_id) else f"{r.state_code}_{r.latitude:.6f}_{r.longitude:.6f}", axis=1)
        coords["state_code"] = coords.state_code.astype(str).str.upper()
        if not coords.state_code.isin(["KA", "MH", "TR"]).all():
            raise ValueError("state_code must be KA, MH, or TR")
        if not coords.latitude.between(-90, 90).all() or not coords.longitude.between(-180, 180).all():
            raise ValueError("Coordinates must be valid latitude/longitude values")
        return coords[["coordinate_id", "label", "state_code", "latitude", "longitude"]].drop_duplicates("coordinate_id")

    path = silver / "regions"
    if not path.exists():
        raise FileNotFoundError(f"No coordinate CSV supplied and Silver regions table is missing: {path}")
    regions = spark.read.parquet(str(path)).select("region_id", "state_code", "region_name", "centroid_latitude", "centroid_longitude").dropDuplicates(["region_id"])
    frame = regions.toPandas().rename(columns={"region_id": "coordinate_id", "region_name": "label", "centroid_latitude": "latitude", "centroid_longitude": "longitude"})
    return frame[frame.state_code.isin(["KA", "MH", "TR"])].drop_duplicates("coordinate_id")


def source_frames(bronze: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    records: list[dict] = []
    catalog: dict[str, dict] = {}
    for file in sorted(bronze.rglob("*.csv")):
        if "raw" in file.parts and file.parent.name == "raw":
            provider, dataset = "indian_weather_dataset", "historical_weather"
        else:
            parts = file.relative_to(bronze).parts
            if len(parts) < 2:
                continue
            provider = parts[0]
            dataset = next((p for p in parts if p in {"solar", "weather", "weather_forecast"}), "source_data")
            if dataset == "source_data" and "solar" in file.name.lower():
                dataset = "solar"
            elif dataset == "source_data" and "weather" in file.name.lower():
                dataset = "weather"
        frame = pd.read_csv(file)
        if frame.empty:
            continue
        frame.columns = [snake(c) for c in frame.columns]
        stamp_col = next((c for c in ("timestamp_utc", "valid_time_utc", "timestamp", "datetime", "time") if c in frame), None)
        lat_col = next((c for c in ("latitude", "lat") if c in frame), None)
        lon_col = next((c for c in ("longitude", "lon", "lng") if c in frame), None)
        if not stamp_col or not lat_col or not lon_col:
            LOG.warning("Skipping %s: timestamp and coordinates are required for coordinate serving", file)
            continue
        frame["valid_time_utc"] = pd.to_datetime(frame[stamp_col], utc=True, errors="coerce")
        frame["latitude"] = pd.to_numeric(frame[lat_col], errors="coerce")
        frame["longitude"] = pd.to_numeric(frame[lon_col], errors="coerce")
        frame = frame.dropna(subset=["valid_time_utc", "latitude", "longitude"])
        forecast = dataset == "weather_forecast" or frame.get("data_type", pd.Series(dtype=str)).astype(str).str.contains("forecast", case=False).any()
        provider_source_id = f"{provider}:{dataset}"
        source_name = str(frame["source"].dropna().iloc[0]) if "source" in frame and frame["source"].notna().any() else provider
        retrieved = next((c for c in ("forecast_issued_at_utc", "retrieved_at_utc", "ingested_at") if c in frame), None)
        catalog[provider_source_id] = {"source_id": provider_source_id, "provider": provider, "product_dataset": dataset, "source_name": source_name,
                                       "license_cost_note": "See provider terms; verify before redistribution", "active": True}
        for measure, aliases in MEASURES.items():
            col = next((c for c in aliases if c in frame.columns), None)
            if not col:
                continue
            values = pd.to_numeric(frame[col], errors="coerce")
            for i in frame.index[values.notna()]:
                row = frame.loc[i]
                issue = pd.to_datetime(row[retrieved], utc=True, errors="coerce") if retrieved else pd.NaT
                typ = str(row.get("data_type", ""))
                scenario = "forecast" if forecast or "forecast" in typ.lower() else ("current" if "model" in typ.lower() else "actual")
                unit = UNIT[measure]
                if measure == "wind_speed_10m":
                    unit = "km/h" if provider == "open_meteo" and col in {"wind_speed_10m", "windspeed_10m"} else "m/s"
                records.append({"provider": provider, "source_id": provider_source_id, "source_name": source_name, "dataset": dataset,
                                "source_state_code": state_code(row.get("state_code", row.get("state"))), "source_latitude": float(row.latitude), "source_longitude": float(row.longitude), "valid_time_utc": row.valid_time_utc,
                                "forecast_issued_at": issue, "measure_name": measure, "value": float(values.loc[i]), "unit": unit, "source_field": col,
                                "scenario_type": scenario, "is_estimate": scenario == "current" and "model" in typ.lower(), "source_file": file.name})
    return pd.DataFrame.from_records(records), pd.DataFrame.from_records(catalog.values())


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0088 * math.asin(math.sqrt(min(1.0, a)))


def build_gold(coords: pd.DataFrame, source_data: pd.DataFrame, catalog: pd.DataFrame, max_distance_km: float, silver: Path):
    now = pd.Timestamp(datetime.now(timezone.utc))
    region_map = pd.DataFrame(columns=["region_id", "state_code", "latitude", "longitude"])
    region_file = silver / "regions"
    if region_file.exists():
        region_map = SparkSession.getActiveSession().read.parquet(str(region_file)).select("region_id", "state_code", "centroid_latitude", "centroid_longitude").toPandas().rename(columns={"centroid_latitude": "latitude", "centroid_longitude": "longitude"})
    series_rows: list[dict] = []
    coverage_rows: list[dict] = []
    if not source_data.empty:
        source_locations = source_data[["provider", "source_id", "source_name", "dataset", "source_state_code", "source_latitude", "source_longitude"]].drop_duplicates()
        nearest: dict[tuple[str, str], tuple[float, tuple[float, float]]] = {}
        for c in coords.itertuples(index=False):
            for s in source_locations.itertuples(index=False):
                if s.source_state_code and s.source_state_code != c.state_code:
                    continue
                d = haversine_km(float(c.latitude), float(c.longitude), s.source_latitude, s.source_longitude)
                k = (str(c.coordinate_id), s.source_id)
                if k not in nearest or d < nearest[k][0]:
                    nearest[k] = (d, (s.source_latitude, s.source_longitude))
        for c in coords.itertuples(index=False):
            for s in source_locations.itertuples(index=False):
                if s.source_state_code and s.source_state_code != c.state_code:
                    continue
                if (str(c.coordinate_id), s.source_id) not in nearest:
                    continue
                distance, matched_coordinate = nearest[(str(c.coordinate_id), s.source_id)]
                if (s.source_latitude, s.source_longitude) != matched_coordinate:
                    continue
                match = f"{matched_coordinate[0]:.6f},{matched_coordinate[1]:.6f}"
                covered = distance <= max_distance_km
                selected = source_data[(source_data.source_id == s.source_id) & (source_data.source_latitude == s.source_latitude) & (source_data.source_longitude == s.source_longitude)] if covered else source_data.iloc[0:0]
                if covered:
                    for r in selected.itertuples(index=False):
                        if r.valid_time_utc < now - pd.Timedelta(days=365 * 2) and r.scenario_type == "current":
                            continue
                        region_id = None
                        if not region_map.empty:
                            candidates = region_map[region_map.state_code == c.state_code]
                            if not candidates.empty:
                                dists = candidates.apply(lambda x: haversine_km(c.latitude, c.longitude, x.latitude, x.longitude), axis=1)
                                nearest_region_index = dists.idxmin()
                                if dists.loc[nearest_region_index] <= max_distance_km:
                                    region_id = candidates.loc[nearest_region_index, "region_id"]
                        series_rows.append({"coordinate_id": c.coordinate_id, "source_id": r.source_id, "provider": r.provider, "dataset": r.dataset,
                            "matched_region_id": region_id, "source_latitude": r.source_latitude, "source_longitude": r.source_longitude,
                            "distance_km": distance, "resolution_method": "exact_source_coordinate" if distance < 0.01 else "nearest_source_coordinate",
                            "valid_time_utc": r.valid_time_utc, "forecast_issued_at": r.forecast_issued_at, "measure_name": r.measure_name,
                            "value": r.value, "unit": r.unit, "scenario_type": r.scenario_type, "is_estimate": r.is_estimate,
                            "source_name": r.source_name, "source_file": r.source_file, "coverage_status": "direct" if distance < 0.01 else "nearest_within_limit"})
                source_measures = sorted(source_data.loc[source_data.source_id == s.source_id, "measure_name"].dropna().unique())
                for measure_name in source_measures:
                    measure_rows = selected[selected.measure_name == measure_name] if covered else selected
                    observations = measure_rows[measure_rows.scenario_type.isin(["actual", "current"])]
                    forecasts = measure_rows[measure_rows.scenario_type == "forecast"]
                    observed_times = observations.valid_time_utc.dropna().drop_duplicates().sort_values()
                    if len(observed_times) > 1:
                        cadence_seconds = observed_times.diff().dropna().median().total_seconds()
                        expected_hours = int(round((observed_times.iloc[-1] - observed_times.iloc[0]).total_seconds() / cadence_seconds + 1)) if cadence_seconds > 0 else len(observed_times)
                    else:
                        expected_hours = len(observed_times) if len(observed_times) else None
                    available_hours = int(len(observed_times))
                    missing_pct = (100.0 * max(0, expected_hours - available_hours) / expected_hours) if expected_hours else None
                    coverage_rows.append({"coordinate_id": c.coordinate_id, "source_id": s.source_id, "measure_name": measure_name, "checked_at": now,
                        "nearest_source_coordinate": match, "distance_km": distance, "latest_observed_time": observations.valid_time_utc.max() if covered and not observations.empty else pd.NaT,
                        "latest_forecast_issue": pd.to_datetime(forecasts.forecast_issued_at, utc=True, errors="coerce").max() if covered and not forecasts.empty else pd.NaT,
                        "expected_hour_count": expected_hours, "available_hour_count": available_hours if covered else 0, "missing_pct": missing_pct,
                        "available_record_count": int(len(measure_rows)) if covered else 0, "coverage_status": ("covered" if covered and not measure_rows.empty else "no_data_at_source") if covered else "outside_match_radius"})
        source_definitions = source_data[["source_id", "measure_name"]].drop_duplicates()
        for c in coords.itertuples(index=False):
            for definition in source_definitions.itertuples(index=False):
                if (str(c.coordinate_id), definition.source_id) not in nearest:
                    coverage_rows.append({"coordinate_id": c.coordinate_id, "source_id": definition.source_id,
                        "measure_name": definition.measure_name, "checked_at": now, "nearest_source_coordinate": None,
                        "distance_km": None, "latest_observed_time": pd.NaT, "latest_forecast_issue": pd.NaT,
                        "expected_hour_count": None, "available_hour_count": 0, "missing_pct": None,
                        "available_record_count": 0, "coverage_status": "no_source_in_state"})
    series = pd.DataFrame.from_records(series_rows)
    coverage = pd.DataFrame.from_records(coverage_rows)
    if not series.empty:
        series["lead_time_hours"] = ((series.valid_time_utc - pd.to_datetime(series.forecast_issued_at, utc=True, errors="coerce")).dt.total_seconds() / 3600).where(series.scenario_type == "forecast")
        series["valid_time_ist"] = series.valid_time_utc + pd.Timedelta(hours=5, minutes=30)
        # Explicit heuristic components; this is a quality score, not a calibrated probability.
        series["source_quality_score"] = series.is_estimate.map({True: 0.70, False: 1.0})
        temporal_keys = ["coordinate_id", "source_id", "measure_name", "scenario_type"]
        temporal_scores: dict[tuple, float] = {}
        for key, group in series.groupby(temporal_keys, dropna=False):
            times = group.valid_time_utc.dropna().drop_duplicates().sort_values()
            if len(times) < 2:
                score = 1.0
            else:
                cadence = times.diff().dropna().median().total_seconds()
                expected = ((times.iloc[-1] - times.iloc[0]).total_seconds() / cadence + 1) if cadence > 0 else len(times)
                score = min(1.0, len(times) / expected) if expected else 1.0
            temporal_scores[key if isinstance(key, tuple) else (key,)] = score
        series["temporal_completeness_score"] = series.apply(lambda r: temporal_scores[tuple(r[k] for k in temporal_keys)], axis=1)
        series["spatial_match_score"] = (1.0 - (series.distance_km / max_distance_km)).clip(0.0, 1.0) if max_distance_km else (series.distance_km == 0).astype(float)
        series["measure_availability_score"] = series.value.notna().astype(float)
        series["confidence_score"] = (0.35 * series.source_quality_score + 0.25 * series.temporal_completeness_score + 0.25 * series.spatial_match_score + 0.15 * series.measure_availability_score).clip(0, 1)
        series["confidence_method"] = "heuristic_v1; weighted source, coverage, distance, availability"
        series["confidence_reason"] = series.apply(lambda r: "model_estimate" if r.is_estimate else ("spatial_distance_penalty" if r.distance_km >= 0.01 else "direct_source_coordinate"), axis=1)
        actual = series[series.scenario_type == "actual"].copy()
        forecast = series[series.scenario_type == "forecast"].copy()
        join_keys = ["coordinate_id", "measure_name", "valid_time_utc"]
        forecast = forecast.rename(columns={"value": "forecast_value", "source_id": "forecast_source_id", "source_name": "forecast_source_name", "distance_km": "forecast_distance_km"})
        actual = actual.rename(columns={"value": "actual_value", "source_id": "actual_source_id", "provider": "actual_provider", "source_name": "actual_source_name", "distance_km": "actual_distance_km"})
        verified = forecast.merge(actual[join_keys + ["unit", "actual_value", "actual_source_id", "actual_provider", "actual_source_name", "actual_distance_km"]], on=join_keys + ["unit"], how="left")
        verified["signed_error"] = verified.forecast_value - verified.actual_value
        verified["absolute_error"] = verified.signed_error.abs()
        verified["squared_error"] = verified.signed_error ** 2
        verified["percentage_error"] = (100 * verified.signed_error / verified.actual_value.abs()).where(verified.actual_value.notna() & (verified.actual_value != 0))
        verified["actual_unit"] = verified["unit"]
        verified["match_status"] = verified.actual_value.notna()
        verified["lead_time_hours"] = ((verified.valid_time_utc - pd.to_datetime(verified.forecast_issued_at, utc=True, errors="coerce")).dt.total_seconds() / 3600)
        verified = verified[verified.match_status]
        if not verified.empty:
            verified["valid_date"] = verified.valid_time_utc.dt.strftime("%Y-%m-%d")
            verified["lead_time_bucket"] = (verified.lead_time_hours.clip(lower=0) // 6 * 6).astype("Int64").astype(str) + "-" + ((verified.lead_time_hours.clip(lower=0) // 6 + 1) * 6).astype("Int64").astype(str) + "h"
            skill = verified.groupby(["coordinate_id", "forecast_source_id", "actual_source_id", "measure_name", "lead_time_bucket", "valid_date"], as_index=False).agg(matched_count=("signed_error", "count"), mae=("absolute_error", "mean"), rmse=("squared_error", lambda x: float((x.mean()) ** 0.5)), bias=("signed_error", "mean"), mape=("percentage_error", lambda x: x.abs().mean()))
        else:
            skill = pd.DataFrame()
        comparison = series.copy()
        peer_keys = ["coordinate_id", "valid_time_utc", "measure_name", "unit", "scenario_type"]
        comparison["source_count"] = comparison.groupby(peer_keys).value.transform("count")
        comparison["peer_min"] = comparison.groupby(peer_keys).value.transform("min")
        comparison["peer_max"] = comparison.groupby(peer_keys).value.transform("max")
        comparison["peer_median"] = comparison.groupby(peer_keys).value.transform("median")
        confidence = series[["coordinate_id", "source_id", "valid_time_utc", "measure_name", "scenario_type", "confidence_score", "source_quality_score", "temporal_completeness_score", "spatial_match_score", "measure_availability_score", "confidence_method", "confidence_reason"]].copy()
    else:
        skill = comparison = confidence = pd.DataFrame()
    coords_out = coords.copy()
    coords_out["created_at_utc"] = now
    return {"coordinate_queries": coords_out, "source_catalog": catalog, "coordinate_weather_series": series,
            "forecast_verification": verified if not series.empty else pd.DataFrame(), "weather_confidence": confidence,
            "mv_coordinate_weather_hourly": series, "mv_source_comparison_hourly": comparison,
            "mv_forecast_skill_daily": skill, "mv_coordinate_coverage": coverage}


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish dashboard-ready Gold weather tables and materialized views.")
    parser.add_argument("--bronze", type=Path, default=ROOT / "data", help="Provider CSV root")
    parser.add_argument("--silver", type=Path, default=ROOT / "output", help="Silver Parquet root")
    parser.add_argument("--output", type=Path, default=ROOT / "gold", help="Gold Parquet output root")
    parser.add_argument("--coordinates", type=Path, help="CSV with coordinate_id (optional), label (optional), latitude, longitude, state_code")
    parser.add_argument("--max-distance-km", type=float, default=25.0, help="Maximum source-coordinate match distance; farther matches are reported as no coverage")
    args = parser.parse_args()
    if args.max_distance_km <= 0:
        parser.error("--max-distance-km must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = SparkSession.builder.appName("WeatherDashboardGoldIngestion").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        coords = read_coordinates(spark, args.silver.resolve(), args.coordinates.resolve() if args.coordinates else None)
        source_data, catalog = source_frames(args.bronze.resolve())
        tables = build_gold(coords, source_data, catalog, args.max_distance_km, args.silver.resolve())
        args.output.mkdir(parents=True, exist_ok=True)
        for name, frame in tables.items():
            if frame is None:
                frame = pd.DataFrame()
            if frame.empty:
                LOG.warning("Gold table %s has no rows; no Parquet dataset written", name)
                continue
            for col in frame.columns:
                if frame[col].dtype == "object":
                    frame[col] = frame[col].map(lambda x: x.to_pydatetime() if isinstance(x, pd.Timestamp) and pd.notna(x) else x)
            spark.createDataFrame(frame).write.mode("overwrite").parquet(str(args.output / name))
            LOG.info("Wrote %s (%d rows)", name, len(frame))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
