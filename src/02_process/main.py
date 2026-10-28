"""PySpark weather-to-energy feature pipeline.

Raw inputs are read-only. The pipeline writes an audit snapshot before curated
outputs and keeps unavailable source measurements NULL rather than fabricating
them. See README.md for supported field mappings and limitations.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

LOG = logging.getLogger("weather_process")
ROOT = Path(__file__).resolve().parents[2]
STATES = {"karnataka": ("KA", "Karnataka"), "maharashtra": ("MH", "Maharashtra"), "tripura": ("TR", "Tripura")}
SOURCE_URL = "https://www.kaggle.com/datasets/sandeepgupta09/indian-weather-dataset-1996-2026"
VERSION = "0.1.0"


@dataclass(frozen=True)
class ProcessingConfig:
    noct_c: float = 45.0
    gamma_pv_per_c: float = -0.004
    system_performance_ratio: float = 0.80
    hub_height_m: float = 100.0
    wind_shear_exponent: float = 0.20
    turbine_cut_in_mps: float = 3.0
    turbine_rated_mps: float = 12.0
    turbine_cut_out_mps: float = 25.0

# Canonical output name -> known source field aliases. Extend this map for new datasets.
ALIASES = {
    "timestamp": ("timestamp", "time", "datetime", "date_time", "valid_time", "valid_time_utc"),
    "state": ("state", "state_name", "admin1"), "city": ("city", "city_name", "location", "station"),
    "latitude": ("latitude", "lat"), "longitude": ("longitude", "lon", "lng"),
    "elevation": ("elevation_m", "elevation", "altitude_m"), "population": ("population_weight", "population", "population_count"),
    "urban_share": ("urban_share", "urban_share_pct"), "soil_moisture": ("soil_moisture_proxy", "soil_moisture_0_to_7cm", "soil_moisture"),
    "temperature": ("temperature_2m", "temperature_2m_c", "temperature_c", "temperature_k", "temperature", "temp", "t2m"),
    "dewpoint": ("dewpoint_2m", "dewpoint_k", "dew_point_2m", "dew_point_c", "dew_point_k", "dewpoint_c", "dewpoint", "d2m"),
    "humidity": ("relative_humidity_2m", "relative_humidity_pct", "relative_humidity", "humidity", "rh"),
    "specific_humidity": ("specific_humidity_gkg", "specific_humidity"),
    "heat_index": ("heat_index_c", "heat_index"),
    "apparent_temperature": ("apparent_temperature", "apparent_temperature_c", "apparent_temperature_2m", "feels_like"),
    "precipitation": ("precipitation", "precipitation_mm", "precip_mm", "rain", "total_precipitation", "tp"),
    "precipitation_intensity": ("precipitation_intensity_mmhr", "precipitation_intensity"),
    "cloud": ("cloud_cover", "cloud_cover_pct", "cloud_cover_fraction", "cloudcover", "tcc"),
    "wind10": ("wind_speed_10m", "wind_speed_10m_mps", "wind_speed_ms", "wind_speed_10m_kmh", "wind_speed", "windspeed_10m"),
    "winddir10": ("wind_direction_10m", "wind_direction_10m_deg", "wind_direction_deg", "wind_direction", "winddirection_10m"),
    "u10": ("wind_u_10m_mps", "u_component_of_wind_10m", "u10"), "v10": ("wind_v_10m_mps", "v_component_of_wind_10m", "v10"),
    "wind100": ("wind_speed_100m", "wind_speed_100m_mps", "windspeed_100m"),
    "winddir100": ("wind_direction_100m", "wind_direction_100m_deg", "winddirection_100m"),
    "u100": ("wind_u_100m_mps", "u_component_of_wind_100m", "u100"), "v100": ("wind_v_100m_mps", "v_component_of_wind_100m", "v100"),
    "pressure": ("surface_pressure", "pressure_msl", "air_pressure_hpa", "pressure", "sp"),
    "ghi": ("shortwave_radiation", "solar_radiation_wm2", "ghi_wm2", "ghi", "global_horizontal_irradiance", "ssrd"),
    "clear_sky_ghi": ("clear_sky_ghi_wm2", "clear_sky_ghi", "shortwave_radiation_clear_sky"),
    "solar_elevation": ("solar_elevation_deg", "solar_elevation"),
    "dni": ("direct_normal_irradiance", "dni_wm2", "dni"), "dhi": ("diffuse_radiation", "dhi_wm2", "dhi"),
}


def snake(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name.strip()).lower()).strip("_")


def discover_files(path: Path) -> list[Path]:
    return [p for p in path.rglob("*") if p.is_file() and p.suffix.lower() in {".csv", ".parquet"} and "output" not in p.parts]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_raw(spark: SparkSession, files: list[Path]):
    frames = []
    for file in files:
        frame = spark.read.option("header", True).option("inferSchema", True).option("mode", "PERMISSIVE").csv(str(file)) if file.suffix.lower() == ".csv" else spark.read.parquet(str(file))
        for col in frame.columns:
            normalized = snake(col)
            if normalized != col:
                frame = frame.withColumnRenamed(col, normalized)
        frame = frame.withColumn("_raw_file_name", F.lit(file.name)).withColumn("_source_path", F.lit(str(file)))
        frames.append(frame)
    if not frames:
        return None
    all_cols = sorted(set().union(*(set(f.columns) for f in frames)))
    aligned = [f.select(*[F.col(c) if c in f.columns else F.lit(None).alias(c) for c in all_cols]) for f in frames]
    result = aligned[0]
    for frame in aligned[1:]:
        result = result.unionByName(frame)
    return result


def schema_audit(df, out: Path) -> list[dict]:
    records = []
    for field in df.schema.fields:
        c = F.col(field.name)
        stats = df.agg(
            F.sum(F.when(c.isNull() | (c.cast("string").isNull()) | (F.trim(c.cast("string")) == ""), 1).otherwise(0)).alias("nulls"),
            F.countDistinct(c).alias("unique_count"), F.min(c).alias("min_value"), F.max(c).alias("max_value"),
        ).first()
        samples = [r.v for r in df.select(c.cast("string").alias("v")).where(F.col("v").isNotNull()).distinct().limit(5).collect()]
        records.append({"column_name": field.name, "inferred_dtype": field.dataType.simpleString(), "null_count": stats.nulls,
                        "unique_count": stats.unique_count, "sample_values": samples, "min": stats.min_value,
                        "max": stats.max_value, "suspected_unit": unit_guess(field.name)})
    out.mkdir(parents=True, exist_ok=True)
    (out / "raw_schema_snapshot.json").write_text(json.dumps(records, indent=2, default=str), encoding="utf-8")
    return records


def unit_guess(name: str) -> str:
    n = name.lower()
    if n in {"t2m", "d2m"}: return "K (ERA-style source field)"
    if n in {"wind_speed_10m", "windspeed_10m"}: return "km/h (Open-Meteo default convention unless request overrides)"
    if n == "sp": return "Pa (ERA-style surface pressure)"
    exact_suffixes = (("_c", "°C"), ("_k", "K"), ("_pct", "%"), ("_hpa", "hPa"), ("_pa", "Pa"), ("_wm2", "W/m²"), ("_mps", "m/s"), ("_ms", "m/s"), ("_kmh", "km/h"), ("_mmhr", "mm/hour"), ("_mm", "mm"), ("_deg", "degrees"), ("_gkg", "g/kg"), ("_kgm3", "kg/m³"))
    for suffix, unit in exact_suffixes:
        if n.endswith(suffix): return unit
    for token, unit in (("temperature", "°C or K; inspect source metadata"), ("dewpoint", "°C or K; inspect source metadata"), ("humidity", "%"),
                        ("precipitation", "mm or source-dependent accumulated units"), ("wind_speed", "km/h or m/s; inspect source metadata"),
                        ("radiation", "W/m² or accumulated J/m²; inspect source metadata"), ("pressure", "hPa or Pa"), ("latitude", "degrees north"), ("longitude", "degrees east")):
        if token in n: return unit
    return "unknown; see source documentation"


def pick(df, aliases):
    for name in aliases:
        if name in df.columns: return F.col(name)
    return F.lit(None).cast("double")


def selected_name(df, aliases):
    return next((name for name in aliases if name in df.columns), None)


def normalize_in_partition(value, partition):
    """5th/95th percentile scaling clamped to [0,1], null if unmeasured/constant."""
    p05 = F.percentile_approx(value, 0.05, 10000).over(partition)
    p95 = F.percentile_approx(value, 0.95, 10000).over(partition)
    return F.when(value.isNull() | p05.isNull() | p95.isNull() | (p95 <= p05), F.lit(None).cast("double")) \
        .otherwise(F.least(F.lit(1.0), F.greatest(F.lit(0.0), (value - p05) / (p95 - p05))))


def resolve_regions(points_df, regions_df, max_distance_km: float = 25.0):
    """Resolve input latitude/longitude to a containing bbox or nearby centroid.

    `points_df` must provide `latitude` and `longitude`; optional `state_code`
    narrows candidates. A containing bbox wins, then nearest centroid within
    max_distance_km. The returned frame preserves input columns and adds the
    resolved region id, distance, centroid, and bbox.
    """
    if max_distance_km <= 0:
        raise ValueError("max_distance_km must be positive")
    p = points_df.withColumn("_resolver_row_id", F.monotonically_increasing_id()).alias("p")
    r = regions_df.alias("r")
    lat1, lat2 = F.radians(F.col("p.latitude")), F.radians(F.col("r.centroid_latitude"))
    dlat = lat2 - lat1
    dlon = F.radians(F.col("r.centroid_longitude") - F.col("p.longitude"))
    distance = 2 * 6371.0088 * F.asin(F.sqrt(F.pow(F.sin(dlat / 2), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2), 2)))
    inside = F.col("p.latitude").between(F.col("r.min_latitude"), F.col("r.max_latitude")) & F.col("p.longitude").between(F.col("r.min_longitude"), F.col("r.max_longitude"))
    condition = (inside | (distance <= max_distance_km))
    if "state_code" in points_df.columns:
        condition = condition & (F.col("p.state_code") == F.col("r.state_code"))
    joined = p.join(r, condition, "left").select("p.*", F.col("r.region_id").alias("resolved_region_id"),
        distance.alias("distance_to_centroid_km"), inside.alias("inside_region_bbox"),
        F.col("r.centroid_latitude").alias("region_centroid_latitude"), F.col("r.centroid_longitude").alias("region_centroid_longitude"),
        F.col("r.min_latitude").alias("region_min_latitude"), F.col("r.max_latitude").alias("region_max_latitude"),
        F.col("r.min_longitude").alias("region_min_longitude"), F.col("r.max_longitude").alias("region_max_longitude"))
    ranking = Window.partitionBy("_resolver_row_id").orderBy(F.col("inside_region_bbox").desc_nulls_last(), F.col("distance_to_centroid_km").asc_nulls_last())
    return joined.withColumn("_match_rank", F.row_number().over(ranking)).filter(F.col("_match_rank") == 1).drop("_resolver_row_id", "_match_rank")


def numeric(expr):
    return expr.cast("double")


def write_partitioned(df, name: str, output: Path):
    # Static dimensions/catalogues have no natural observation date. Keep them
    # in the requested partition layout using stable sentinel partitions.
    for col, value, dtype in (("state_code", "ALL", "string"), ("year", 0, "int"), ("month", 0, "int"), ("region_id", "ALL", "string")):
        if col not in df.columns:
            df = df.withColumn(col, F.lit(value).cast(dtype))
    (df.write.mode("overwrite").partitionBy("state_code", "year", "month", "region_id").parquet(str(output / name)))


def run(spark: SparkSession, input_path: Path, output: Path, inspect_only: bool, config: ProcessingConfig | None = None, source_timezone: str = "UTC"):
    config = config or ProcessingConfig()
    files = discover_files(input_path)
    if not files:
        raise FileNotFoundError(f"No CSV/Parquet inputs found under {input_path}. Place the Kaggle archive there; source data is not downloaded by this pipeline.")
    raw = read_raw(spark, files).cache()
    audit_dir = output / "_audit"
    snapshot = schema_audit(raw, audit_dir)
    LOG.info("Discovered %d source files and %d columns. Raw schema snapshot: %s", len(files), len(snapshot), audit_dir / "raw_schema_snapshot.json")
    if inspect_only:
        raw.show(20, truncate=False)
        raw.unpersist()
        return

    # Rename raw source fields into a canonical intermediate. Unit assumptions are conservative:
    # Open-Meteo temperature/radiation/pressure units are known; other inputs stay source-led.
    ts = pick(raw, ALIASES["timestamp"]).cast("string")
    state = F.lower(F.trim(pick(raw, ALIASES["state"]).cast("string")))
    city = F.trim(pick(raw, ALIASES["city"]).cast("string"))
    lat, lon = numeric(pick(raw, ALIASES["latitude"])), numeric(pick(raw, ALIASES["longitude"]))
    version_field = selected_name(raw, ("ingested_at", "ingestion_time", "retrieved_at_utc", "updated_at", "source_version", "version"))
    version_priority = F.lit(None).cast("double")
    if version_field:
        version_value = F.col(version_field)
        if any(token in version_field for token in ("time", "at", "date")):
            version_priority = F.unix_timestamp(F.to_timestamp(version_value)).cast("double")
        else:
            version_priority = F.coalesce(version_value.cast("double"), F.dense_rank().over(Window.orderBy(version_value.cast("string"))).cast("double"))
    df = raw.select("*", F.to_timestamp(ts).alias("_parsed_time"), state.alias("_state_key"), city.alias("_city"), lat.alias("_lat"), lon.alias("_lon"))
    df = df.filter(F.col("_state_key").isin(*STATES.keys()) & F.col("_parsed_time").isNotNull())
    state_code = F.when(F.col("_state_key") == "karnataka", "KA").when(F.col("_state_key") == "maharashtra", "MH").otherwise("TR")
    state_name = F.when(F.col("_state_key") == "karnataka", "Karnataka").when(F.col("_state_key") == "maharashtra", "Maharashtra").otherwise("Tripura")
    # Kaggle Open-Meteo archive timestamps are local-naive only if metadata says so. Default is UTC and is explicit in lineage.
    utc = F.to_utc_timestamp(F.col("_parsed_time"), source_timezone)
    temp_source = selected_name(df, ALIASES["temperature"])
    temp_raw = numeric(pick(df, ALIASES["temperature"]))
    temp = temp_raw - 273.15 if temp_source and (temp_source.endswith("_k") or temp_source == "t2m") else temp_raw
    dew_source = selected_name(df, ALIASES["dewpoint"])
    dew_raw = numeric(pick(df, ALIASES["dewpoint"]))
    dew = dew_raw - 273.15 if dew_source and (dew_source.endswith("_k") or dew_source == "d2m") else dew_raw
    rh = numeric(pick(df, ALIASES["humidity"]))
    pressure = numeric(pick(df, ALIASES["pressure"]))
    elevation = numeric(pick(df, ALIASES["elevation"]))
    population = numeric(pick(df, ALIASES["population"]))
    urban_share = numeric(pick(df, ALIASES["urban_share"]))
    # Common Open-Meteo pressure is hPa; Pa values are scaled when evidently > 2000.
    pressure_hpa = F.coalesce(F.when(pressure > 2000, pressure / 100.0).otherwise(pressure),
        F.when(elevation.isNotNull() & (elevation >= 0) & (elevation < 9000), 1013.25 * F.pow(1 - 2.25577e-5 * elevation, 5.25588)))
    wind_source = selected_name(df, ALIASES["wind10"])
    wind = numeric(pick(df, ALIASES["wind10"]))
    if wind_source and (wind_source.endswith("_kmh") or wind_source in {"wind_speed_10m", "windspeed_10m"}):
        wind = wind / 3.6
    winddir = numeric(pick(df, ALIASES["winddir10"]))
    u, v = numeric(pick(df, ALIASES["u10"])), numeric(pick(df, ALIASES["v10"]))
    speed = F.coalesce(wind, F.sqrt(u * u + v * v))
    u_out = F.coalesce(u, -speed * F.sin(F.radians(winddir)))
    v_out = F.coalesce(v, -speed * F.cos(F.radians(winddir)))
    dew = F.coalesce(dew, F.when((rh > 0) & (temp > -243.04), (243.04 * (F.log(rh / 100.0) + 17.625 * temp / (243.04 + temp))) / (17.625 - (F.log(rh / 100.0) + 17.625 * temp / (243.04 + temp)))))
    rh = F.coalesce(rh, 100.0 * F.exp(17.625 * dew / (243.04 + dew) - 17.625 * temp / (243.04 + temp)))
    ghi_source = selected_name(df, ALIASES["ghi"])
    ghi_raw = numeric(pick(df, ALIASES["ghi"]))
    precip_source = selected_name(df, ALIASES["precipitation"])
    rain_raw = numeric(pick(df, ALIASES["precipitation"]))
    # Explicit accumulations are differenced hourly. Hourly precip_mm / Open-Meteo
    # precipitation is retained as reported. ssrd is accumulated J/m².
    derivation_window = Window.partitionBy("_state_key", "_city").orderBy("_parsed_time")
    if ghi_source == "ssrd":
        df = df.withColumn("_ghi_raw", ghi_raw).withColumn("_ghi_previous", F.lag("_ghi_raw").over(derivation_window))
        ghi = F.when(F.col("_ghi_previous").isNotNull(), F.greatest(F.lit(0.0), F.col("_ghi_raw") - F.col("_ghi_previous")) / 3600.0)
        df = df.withColumn("_ghi_negative_difference", (F.col("_ghi_previous").isNotNull()) & (F.col("_ghi_raw") < F.col("_ghi_previous")))
    else:
        ghi = ghi_raw
        df = df.withColumn("_ghi_negative_difference", F.lit(False))
    if precip_source in {"total_precipitation", "tp"}:
        df = df.withColumn("_precip_raw", rain_raw).withColumn("_precip_previous", F.lag("_precip_raw").over(derivation_window))
        rain = F.when(F.col("_precip_previous").isNotNull(), F.greatest(F.lit(0.0), F.col("_precip_raw") - F.col("_precip_previous")))
        if precip_source == "tp":
            rain = rain * 1000.0  # ERA-style total precipitation is stored in metres.
        df = df.withColumn("_precip_negative_difference", (F.col("_precip_previous").isNotNull()) & (F.col("_precip_raw") < F.col("_precip_previous")))
    else:
        rain = rain_raw
        df = df.withColumn("_precip_negative_difference", F.lit(False))
    source_value_cols = [c for c in raw.columns if not c.startswith("_")]
    row_hash = F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in source_value_cols]), 256)
    df = df.withColumn("_row_hash", row_hash)
    cloud_source = selected_name(df, ALIASES["cloud"])
    cloud = numeric(pick(df, ALIASES["cloud"]))
    if cloud_source in {"cloud_cover_fraction", "tcc"}:
        cloud = cloud * 100.0
    clear_sky_ghi = numeric(pick(df, ALIASES["clear_sky_ghi"]))
    solar_elevation = numeric(pick(df, ALIASES["solar_elevation"]))
    soil_moisture = numeric(pick(df, ALIASES["soil_moisture"]))
    wind100 = numeric(pick(df, ALIASES["wind100"]))
    winddir100 = numeric(pick(df, ALIASES["winddir100"]))
    u100, v100 = numeric(pick(df, ALIASES["u100"])), numeric(pick(df, ALIASES["v100"]))
    dni, dhi = numeric(pick(df, ALIASES["dni"])), numeric(pick(df, ALIASES["dhi"]))
    name = F.coalesce(F.col("_city"), F.lit("unknown"))
    region_id = F.concat(state_code, F.lit("_CITY_"), F.regexp_replace(F.upper(name), "[^A-Z0-9]+", "_"))
    base = df.select(
        region_id.alias("region_id"), state_code.alias("state_code"), state_name.alias("state_name"), name.alias("city"),
        F.col("_lat").alias("latitude"), F.col("_lon").alias("longitude"), utc.alias("valid_time_utc"),
        temp.alias("temperature_2m_c"), dew.alias("dewpoint_c"), rh.alias("relative_humidity_pct"),
        speed.alias("wind_speed_10m_mps"), winddir.alias("wind_direction_10m_deg"), u_out.alias("wind_u_10m_mps"), v_out.alias("wind_v_10m_mps"),
        wind100.alias("wind_speed_100m_mps"), winddir100.alias("wind_direction_100m_deg"), u100.alias("wind_u_100m_mps"), v100.alias("wind_v_100m_mps"),
        numeric(pick(df, ALIASES["wind100"])).isNotNull().alias("wind_100m_observed"),
        rain.alias("precipitation_mm"), cloud.alias("cloud_cover_pct"), ghi.alias("ghi_wm2"), dni.alias("dni_wm2"), dhi.alias("dhi_wm2"),
        pressure_hpa.alias("air_pressure_hpa"), F.col("_raw_file_name").alias("raw_file_name"),
        elevation.alias("elevation_m"), population.alias("population_weight"), urban_share.alias("urban_share"),
        soil_moisture.alias("soil_moisture_proxy"), clear_sky_ghi.alias("clear_sky_ghi_wm2"), solar_elevation.alias("solar_elevation_deg"),
        numeric(pick(df, ALIASES["specific_humidity"])).alias("specific_humidity_source_gkg"),
        numeric(pick(df, ALIASES["heat_index"])).alias("heat_index_source_c"),
        numeric(pick(df, ALIASES["apparent_temperature"])).alias("apparent_temperature_c"),
        numeric(pick(df, ALIASES["precipitation_intensity"])).alias("precipitation_intensity_source_mmhr"),
        F.lit(temp_source or "missing").alias("temperature_source_field"), F.lit(dew_source or "missing").alias("dewpoint_source_field"),
        F.lit(ghi_source or "missing").alias("ghi_source_field"), F.lit(precip_source or "missing").alias("precipitation_source_field"),
        F.lit(f"source_timezone_assumed_{source_timezone}").alias("timestamp_timezone_assumption"),
        F.col("_ghi_negative_difference").alias("solar_accumulation_reset_flag"), F.col("_precip_negative_difference").alias("precipitation_accumulation_reset_flag"),
        (pressure.isNull() & elevation.isNotNull()).alias("air_pressure_estimated_from_elevation"),
        version_priority.alias("_source_priority"),
        F.col("_row_hash"),
    ).withColumn("valid_time_ist", F.expr("valid_time_utc + INTERVAL 5 HOURS 30 MINUTES"))
    base = base.withColumn("year", F.year("valid_time_utc")).withColumn("month", F.month("valid_time_utc")).withColumn("day", F.dayofmonth("valid_time_utc"))
    base = base.withColumn("hour", F.hour("valid_time_utc")).withColumn("day_of_week", F.dayofweek("valid_time_utc")).withColumn("is_weekend", F.dayofweek("valid_time_utc").isin(1, 7))
    # Exact duplicates collapse. Conflicts are flagged and excluded at their city-hour key.
    keys = ["region_id", "valid_time_utc"]
    duplicate_keys = base.groupBy(*keys, "state_code", F.year("valid_time_utc").alias("year"), F.month("valid_time_utc").alias("month")).agg(F.count("*").alias("_rows"), F.countDistinct("_row_hash").alias("_n_hash"))
    duplicate_summary = duplicate_keys.groupBy("region_id", "state_code", "year", "month").agg(
        F.sum(F.greatest(F.col("_rows") - 1, F.lit(0))).alias("duplicate_count"),
        F.sum(F.when(F.col("_n_hash") > 1, 1).otherwise(0)).alias("conflict_count"),
    )
    conflicts = duplicate_keys.filter(F.col("_n_hash") > 1).select(*keys, "state_code").withColumn("_conflict", F.lit(True))
    base = base.dropDuplicates(keys + ["_row_hash"])
    if version_field:
        max_priority = F.max("_source_priority").over(Window.partitionBy(*keys))
        ranked = base.withColumn("_max_source_priority", max_priority)
        latest_candidates = ranked.filter(F.col("_source_priority").isNotNull() & (F.col("_source_priority") == F.col("_max_source_priority")))
        latest_conflict_candidates = latest_candidates.join(conflicts.select(*keys), keys, "inner")
        latest_hash_counts = latest_conflict_candidates.groupBy(*keys).agg(F.countDistinct("_row_hash").alias("_latest_hashes"))
        resolved_keys = latest_hash_counts.filter(F.col("_latest_hashes") == 1).select(*keys)
        non_conflict_rows = base.join(conflicts.select(*keys), keys, "left_anti")
        resolved_rows = latest_conflict_candidates.join(resolved_keys, keys, "inner").drop("_max_source_priority")
        base = non_conflict_rows.unionByName(resolved_rows)
        conflicts = conflicts.join(resolved_keys, keys, "left_anti")
    else:
        base = base.join(conflicts.select(*keys), keys, "left_anti")
    base = base.drop("_row_hash", "_source_priority")
    base = base.withColumn("is_estimate", F.col("wind_speed_100m_mps").isNull() & F.col("wind_speed_10m_mps").isNotNull())
    base = base.withColumn("estimation_method", F.when(F.col("is_estimate"), F.lit(f"wind_speed_100m_power_law_alpha_{config.wind_shear_exponent:g}")).otherwise(F.lit(None).cast("string")))
    physically_invalid = ((F.col("temperature_2m_c") < -30) | (F.col("temperature_2m_c") > 55) | (F.col("relative_humidity_pct") < 0) | (F.col("relative_humidity_pct") > 100) | (F.col("wind_speed_10m_mps") < 0) | (F.col("wind_speed_100m_mps") < 0) | (F.col("precipitation_mm") < 0) | (F.col("ghi_wm2") < 0) | (F.col("ghi_wm2") > 1500) | (F.col("cloud_cover_pct") < 0) | (F.col("cloud_cover_pct") > 100) | (F.col("dewpoint_c") > F.col("temperature_2m_c")))
    reset_clamped = F.col("solar_accumulation_reset_flag") | F.col("precipitation_accumulation_reset_flag")
    base = base.withColumn("quality_flag", F.when(physically_invalid, "outlier").when(reset_clamped, "cumulative_reset_clamped").otherwise("ok"))
    base = base.withColumn("gap_flag", F.when(F.col("temperature_2m_c").isNull() | F.col("precipitation_mm").isNull() | F.col("ghi_wm2").isNull(), "missing_fields").otherwise("none"))
    base = base.withColumn("source_dataset", F.lit("Indian_Weather_Dataset_1996-2026"))
    base = base.withColumn("source_url", F.lit(SOURCE_URL)).withColumn("source_license", F.lit("Refer to Kaggle dataset license; verify before redistribution"))
    base = base.withColumn("transformation_code_version", F.lit(VERSION)).withColumn("region_boundary_version", F.lit("city-coordinate-grid-0.25deg-v1"))
    base = base.withColumn("ingested_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())
    partition_cols = ["state_code", "year", "month", "region_id"]

    # Coordinate-derived city and grid cell regions; distinct city rows only.
    city_regions = base.groupBy("region_id", "state_code", "state_name", "city").agg(F.avg("latitude").alias("centroid_latitude"), F.avg("longitude").alias("centroid_longitude"), F.avg("population_weight").alias("population_weight"), F.avg("urban_share").alias("urban_share"), F.avg("elevation_m").alias("elevation_m"))
    city_regions = city_regions.withColumn("region_type", F.lit("city")).withColumnRenamed("city", "region_name")
    city_regions = city_regions.withColumn("min_latitude", F.col("centroid_latitude")).withColumn("max_latitude", F.col("centroid_latitude")).withColumn("min_longitude", F.col("centroid_longitude")).withColumn("max_longitude", F.col("centroid_longitude"))
    city_regions = city_regions.withColumn("bbox_resolution_km", F.lit(0.0)).withColumn("coastal_flag", F.lit(None).cast("boolean"))
    city_regions = city_regions.withColumn("boundary_version", F.lit("city-coordinate-grid-0.25deg-v1")).withColumn("source", F.lit("Kaggle city coordinates"))
    city_regions = city_regions.withColumn("license", F.lit("Refer to Kaggle dataset license")).withColumn("created_at", F.current_timestamp())
    grids = base.select("state_code", "state_name", (F.floor(F.col("latitude") * 4) / 4.0).alias("_lat_floor"), (F.floor(F.col("longitude") * 4) / 4.0).alias("_lon_floor")).distinct()
    grids = grids.withColumn("region_id", F.concat(F.lit("GRID_"), F.regexp_replace(F.col("_lat_floor").cast("string"), "\\.", "p"), F.lit("_"), F.regexp_replace(F.col("_lon_floor").cast("string"), "\\.", "p")))
    grids = grids.withColumn("region_type", F.lit("grid_cell")).withColumn("region_name", F.col("region_id"))
    grids = grids.withColumn("centroid_latitude", F.col("_lat_floor") + 0.125).withColumn("centroid_longitude", F.col("_lon_floor") + 0.125)
    grids = grids.withColumn("min_latitude", F.col("_lat_floor")).withColumn("max_latitude", F.col("_lat_floor") + 0.25).withColumn("min_longitude", F.col("_lon_floor")).withColumn("max_longitude", F.col("_lon_floor") + 0.25)
    for col, typ, val in [("bbox_resolution_km", "double", 27.8), ("population_weight", "double", None), ("urban_share", "double", None), ("elevation_m", "double", None), ("coastal_flag", "boolean", None)]: grids = grids.withColumn(col, F.lit(val).cast(typ))
    grids = grids.withColumn("boundary_version", F.lit("city-coordinate-grid-0.25deg-v1")).withColumn("source", F.lit("Derived from source coordinates")).withColumn("license", F.lit("Source dataset license applies")).withColumn("created_at", F.current_timestamp())
    regions = city_regions.unionByName(grids.select(*city_regions.columns), allowMissingColumns=True)
    coordinate_cities = base.select("region_id", "state_code", "latitude", "longitude").dropDuplicates(["region_id", "latitude", "longitude"])
    city_coordinates = coordinate_cities.withColumn("distance_to_centroid_km", F.lit(0.0)).withColumn("weight", F.lit(1.0)).withColumn("is_primary_coordinate", F.lit(True))
    grid_coordinates = coordinate_cities.withColumn("_lat_floor", F.floor(F.col("latitude") * 4) / 4.0).withColumn("_lon_floor", F.floor(F.col("longitude") * 4) / 4.0)
    grid_coordinates = grid_coordinates.withColumn("region_id", F.concat(F.lit("GRID_"), F.regexp_replace(F.col("_lat_floor").cast("string"), "\\.", "p"), F.lit("_"), F.regexp_replace(F.col("_lon_floor").cast("string"), "\\.", "p")))
    lat1, lat2 = F.radians(F.col("latitude")), F.radians(F.col("_lat_floor") + 0.125)
    dlat = lat2 - lat1
    dlon = F.radians(F.col("_lon_floor") + 0.125 - F.col("longitude"))
    haversine = 2 * 6371.0088 * F.asin(F.sqrt(F.pow(F.sin(dlat / 2), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2), 2)))
    grid_coordinates = grid_coordinates.withColumn("distance_to_centroid_km", haversine).withColumn("weight", F.lit(1.0)).withColumn("is_primary_coordinate", F.lit(False)).drop("_lat_floor", "_lon_floor")
    region_coordinates = city_coordinates.unionByName(grid_coordinates.select(*city_coordinates.columns))

    # Canonical weather facts include explicit NULLs for source fields not supplied.
    w = Window.partitionBy("region_id").orderBy("valid_time_utc")
    temporal = base.groupBy("region_id", "state_code").agg(F.min("valid_time_utc").alias("_start"), F.max("valid_time_utc").alias("_end"), F.countDistinct("valid_time_utc").alias("_observed"))
    temporal = temporal.withColumn("temporal_coverage_score", F.col("_observed") / ((F.unix_timestamp("_end") - F.unix_timestamp("_start")) / 3600.0 + 1.0))
    temp_c, rh_pct = F.col("temperature_2m_c"), F.col("relative_humidity_pct")
    saturation_hpa = 6.112 * F.exp(17.67 * temp_c / (temp_c + 243.5))
    vapor_hpa = (rh_pct / 100.0) * saturation_hpa
    weather = base.withColumn("specific_humidity_gkg", F.coalesce(F.col("specific_humidity_source_gkg"), F.when(
        F.col("air_pressure_hpa").isNotNull() & rh_pct.isNotNull() & temp_c.isNotNull() & (F.col("air_pressure_hpa") > 0),
        1000.0 * (0.622 * vapor_hpa) / (F.col("air_pressure_hpa") - 0.378 * vapor_hpa))))
    # Rothfusz heat-index regression (Fahrenheit coefficients), with NOAA low/high humidity adjustments.
    tf = temp_c * 9.0 / 5.0 + 32.0
    r = rh_pct
    hi_f = (-42.379 + 2.04901523 * tf + 10.14333127 * r - 0.22475541 * tf * r
            - 0.00683783 * F.pow(tf, 2) - 0.05481717 * F.pow(r, 2)
            + 0.00122874 * F.pow(tf, 2) * r + 0.00085282 * tf * F.pow(r, 2)
            - 0.00000199 * F.pow(tf, 2) * F.pow(r, 2))
    low_rh_adjustment = ((13.0 - r) / 4.0) * F.sqrt(F.greatest(0.0, (17.0 - F.abs(tf - 95.0)) / 17.0))
    high_rh_adjustment = ((r - 85.0) / 10.0) * ((87.0 - tf) / 5.0)
    hi_f = F.when((r < 13) & (tf.between(80, 112)), hi_f - low_rh_adjustment).when((r > 85) & (tf.between(80, 87)), hi_f + high_rh_adjustment).otherwise(hi_f)
    heat_index = F.when((temp_c >= 26.7) & rh_pct.between(0, 100), (hi_f - 32.0) * 5.0 / 9.0)
    weather = weather.withColumn("heat_index_c", F.coalesce(F.col("heat_index_source_c"), heat_index))
    weather = weather.withColumn("apparent_temperature_c", F.col("apparent_temperature_c"))
    weather = weather.withColumn("wind_shear_exponent", F.lit(config.wind_shear_exponent)).withColumn("wind_speed_100m_mps", F.coalesce("wind_speed_100m_mps", F.col("wind_speed_10m_mps") * F.pow(10.0, F.col("wind_shear_exponent"))))
    weather = weather.withColumn("wind_speed_100m_method", F.when(~F.col("wind_100m_observed"), "power_law_estimate").otherwise("observed"))
    weather = weather.withColumn("wind_u_100m_mps", F.coalesce("wind_u_100m_mps", -F.col("wind_speed_100m_mps") * F.sin(F.radians("wind_direction_100m_deg"))))
    weather = weather.withColumn("wind_v_100m_mps", F.coalesce("wind_v_100m_mps", -F.col("wind_speed_100m_mps") * F.cos(F.radians("wind_direction_100m_deg"))))
    weather = weather.withColumn("precipitation_intensity_mmhr", F.coalesce(F.col("precipitation_intensity_source_mmhr"), F.col("precipitation_mm")))
    weather = weather.withColumn("air_density_kgm3", F.when(F.col("air_pressure_hpa").isNotNull() & F.col("temperature_2m_c").isNotNull(), F.col("air_pressure_hpa") * 100 / (287.05 * (F.col("temperature_2m_c") + 273.15))))
    weather = weather.withColumn("weather_source", F.lit("Kaggle source; per-row source field retained in raw file lineage")).withColumn("source_version", F.lit(VERSION))

    # Solar outputs: clear-sky reference is retained NULL when the source lacks it.
    solar = weather.select("region_id", "state_code", "year", "month", "valid_time_utc", "day", "hour", "ghi_wm2", "dni_wm2", "dhi_wm2", "clear_sky_ghi_wm2", "solar_elevation_deg", "cloud_cover_pct", "temperature_2m_c")
    solar = solar.withColumn("daylight_flag", F.when(F.col("solar_elevation_deg").isNotNull(), F.col("solar_elevation_deg") > 0).when(F.col("ghi_wm2").isNotNull(), F.col("ghi_wm2") > 0).otherwise(F.lit(None).cast("boolean")))
    solar = solar.withColumn("clear_sky_index", F.when((F.col("daylight_flag") == True) & (F.col("clear_sky_ghi_wm2") > 0), F.col("ghi_wm2") / F.col("clear_sky_ghi_wm2")))
    # Sandia/NREL-style NOCT model; defaults: NOCT=45 C, gamma=-0.004/C, PR=0.80.
    solar = solar.withColumn("pv_cell_temperature_c", F.when(F.col("ghi_wm2").isNotNull() & F.col("temperature_2m_c").isNotNull(), F.col("temperature_2m_c") + (F.col("ghi_wm2") / 800.0) * (config.noct_c - 20.0)))
    derate = 1 + config.gamma_pv_per_c * (F.col("pv_cell_temperature_c") - 25)
    solar = solar.withColumn("pv_power_factor", derate).withColumn("pv_capacity_factor", F.when(F.col("daylight_flag") == False, 0.0).when(F.col("ghi_wm2").isNull(), F.lit(None).cast("double")).when(F.col("ghi_wm2") <= 0, 0.0).otherwise(F.least(1.0, F.greatest(0.0, F.col("ghi_wm2") / 1000.0 * derate * config.system_performance_ratio))))
    solar = solar.withColumn("solar_generation_index", F.col("pv_capacity_factor"))
    solar = solar.withColumn("_prev", F.lag("pv_capacity_factor").over(w)).withColumn("_prev_time", F.lag(F.unix_timestamp("valid_time_utc")).over(w))
    solar = solar.withColumn("solar_ramp_mw_per_mw_per_hour", F.when((F.unix_timestamp("valid_time_utc") - F.col("_prev_time")) == 3600, F.col("pv_capacity_factor") - F.col("_prev")))
    solar = solar.withColumn("solar_variability_index", F.abs("solar_ramp_mw_per_mw_per_hour"))
    solar = solar.withColumn("data_quality_flag", F.when(F.col("ghi_wm2").isNull(), "ghi_unavailable").otherwise("clear_sky_reference_unavailable"))
    solar = solar.drop("_prev", "_prev_time", "temperature_2m_c")

    # Wind table: 100m estimate marked explicitly; generic power curve is an analytical proxy.
    wind_table = weather.select("region_id", "state_code", "year", "month", "day", "hour", "valid_time_utc", "wind_speed_10m_mps", "wind_speed_100m_mps", "wind_direction_10m_deg", "wind_direction_100m_deg", "wind_u_10m_mps", "wind_v_10m_mps", "wind_u_100m_mps", "wind_v_100m_mps", "wind_shear_exponent", "temperature_2m_c", "air_pressure_hpa", "air_density_kgm3", "wind_speed_100m_method")
    hub_speed = F.col("wind_speed_100m_mps") if config.hub_height_m == 100 else F.col("wind_speed_10m_mps") * F.pow(config.hub_height_m / 10.0, F.col("wind_shear_exponent"))
    wind_table = wind_table.withColumn("wind_speed_hub_mps", hub_speed).withColumn("hub_height_m", F.lit(config.hub_height_m))
    wind_table = wind_table.withColumn("wind_power_density_wm2", 0.5 * F.col("air_density_kgm3") * F.pow("wind_speed_hub_mps", 3))
    vhub = F.col("wind_speed_hub_mps")
    ci, rated, co = config.turbine_cut_in_mps, config.turbine_rated_mps, config.turbine_cut_out_mps
    curve = F.when(vhub.isNull(), F.lit(None).cast("double")).when(vhub < ci, 0.0).when(vhub < rated, (F.pow(vhub, 3) - ci ** 3) / (rated ** 3 - ci ** 3)).when(vhub < co, 1.0).otherwise(0.0)
    wind_table = wind_table.withColumn("turbine_power_factor", curve).withColumn("wind_capacity_factor", F.least(1.0, F.greatest(0.0, curve * F.col("air_density_kgm3") / 1.225)))
    wind_table = wind_table.withColumn("estimated_wind_generation_index", F.col("wind_capacity_factor")).withColumn("cut_in_flag", vhub < ci).withColumn("rated_flag", (vhub >= rated) & (vhub < co)).withColumn("cut_out_flag", vhub >= co)
    wind_table = wind_table.withColumn("_prev", F.lag("wind_capacity_factor").over(w)).withColumn("_prev_time", F.lag(F.unix_timestamp("valid_time_utc")).over(w))
    wind_table = wind_table.withColumn("wind_ramp_mw_per_mw_per_hour", F.when((F.unix_timestamp("valid_time_utc") - F.col("_prev_time")) == 3600, F.col("wind_capacity_factor") - F.col("_prev")))
    wind_table = wind_table.withColumn("wind_ramp_flag", F.abs("wind_ramp_mw_per_mw_per_hour") > 0.3)
    wind_table = wind_table.withColumn("turbulence_proxy", F.lit(None).cast("double")).withColumn("data_quality_flag", F.when(F.col("wind_speed_100m_method") == "power_law_estimate", "estimated_hub_height_wind_generic_curve").otherwise("generic_power_curve" )).drop("_prev", "_prev_time")
    wind_table = wind_table.withColumnRenamed("temperature_2m_c", "air_temperature_c")

    # Rain table: event streaks reset across timestamp gaps; drought/hydro remain proxies.
    rain = weather.select("region_id", "state_code", "year", "month", "day", "hour", "valid_time_utc", "precipitation_mm", "precipitation_intensity_mmhr", "soil_moisture_proxy")
    rain = rain.withColumn("_epoch", F.unix_timestamp("valid_time_utc"))
    wr = Window.partitionBy("region_id").orderBy("_epoch")
    for n, label in [(3, "3h"), (6, "6h"), (24, "24h"), (72, "72h")]:
        rain = rain.withColumn(f"precipitation_{label}_mm", F.sum("precipitation_mm").over(wr.rangeBetween(-n * 3600 + 1, 0)))
    wet = F.when(F.col("precipitation_mm").isNull(), F.lit(None).cast("int")).when(F.col("precipitation_mm") >= 0.1, 1).otherwise(0)
    rain = rain.withColumn("_wet_class", wet).withColumn("_previous_class", F.lag(wet).over(wr)).withColumn("_previous_epoch", F.lag("_epoch").over(wr))
    break_segment = F.when(F.col("_wet_class").isNull() | F.col("_previous_class").isNull() | (F.col("_wet_class") != F.col("_previous_class")) | ((F.col("_epoch") - F.col("_previous_epoch")) > 3600), 1).otherwise(0)
    rain = rain.withColumn("_segment", F.sum(break_segment).over(wr.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    streak_window = Window.partitionBy("region_id", "_segment").orderBy("_epoch")
    rain = rain.withColumn("consecutive_dry_hours", F.when(F.col("_wet_class").isNull(), F.lit(None).cast("int")).when(F.col("_wet_class") == 0, F.row_number().over(streak_window)).otherwise(F.lit(0)))
    rain = rain.withColumn("consecutive_wet_hours", F.when(F.col("_wet_class").isNull(), F.lit(None).cast("int")).when(F.col("_wet_class") == 1, F.row_number().over(streak_window)).otherwise(F.lit(0)))
    rain = rain.withColumn("consecutive_dry_days", F.col("consecutive_dry_hours") / 24.0)
    rain = rain.withColumn("daily_precipitation_mm", F.sum("precipitation_mm").over(Window.partitionBy("region_id", "year", "month", "day")))
    rain = rain.withColumn("monthly_precipitation_mm", F.sum("precipitation_mm").over(Window.partitionBy("region_id", "year", "month")))
    rain = rain.withColumn("monsoon_flag", (F.col("month") >= 6) & (F.col("month") <= 9))
    rain = rain.withColumn("_avg_hourly_rain", F.avg("precipitation_mm").over(Window.partitionBy("region_id", "month")))
    rain = rain.withColumn("rolling_30day_precipitation_mm", F.sum("precipitation_mm").over(wr.rangeBetween(-30 * 86400 + 1, 0)))
    rain = rain.withColumn("rolling_90day_precipitation_mm", F.sum("precipitation_mm").over(wr.rangeBetween(-90 * 86400 + 1, 0)))
    rain = rain.withColumn("rolling_30day_precipitation_deficit", F.greatest(0.0, F.col("_avg_hourly_rain") * 24 * 30 - F.col("rolling_30day_precipitation_mm")))
    rain = rain.withColumn("rolling_90day_precipitation_deficit", F.greatest(0.0, F.col("_avg_hourly_rain") * 24 * 90 - F.col("rolling_90day_precipitation_mm")))
    rain = rain.withColumn("_flood_raw", 0.45 * F.col("precipitation_24h_mm") + 0.35 * F.col("precipitation_72h_mm") + 0.20 * F.col("precipitation_intensity_mmhr"))
    rain_norm = Window.partitionBy("region_id", "month")
    rain = rain.withColumn("flood_risk_index", normalize_in_partition(F.col("_flood_raw"), rain_norm))
    extreme_rain_cutoff = F.percentile_approx("precipitation_24h_mm", 0.99, 10000).over(rain_norm)
    rain = rain.withColumn("extreme_24h_rainfall_flag", F.col("precipitation_24h_mm") >= extreme_rain_cutoff)
    rain = rain.withColumn("_drought_raw", 0.50 * F.col("rolling_30day_precipitation_deficit") + 0.30 * F.col("consecutive_dry_days") + 0.20 * F.col("rolling_90day_precipitation_deficit"))
    rain = rain.withColumn("drought_risk_index", normalize_in_partition(F.col("_drought_raw"), rain_norm))
    soil_norm = normalize_in_partition(F.col("soil_moisture_proxy"), rain_norm)
    rainfall_only_hydro = 0.625 * F.col("precipitation_24h_mm") + 0.375 * F.col("precipitation_72h_mm")
    full_hydro = 0.50 * F.col("precipitation_24h_mm") + 0.30 * F.col("precipitation_72h_mm") + 0.20 * soil_norm
    rain = rain.withColumn("hydro_inflow_proxy", normalize_in_partition(F.when(F.col("soil_moisture_proxy").isNotNull(), full_hydro).otherwise(rainfall_only_hydro), rain_norm))
    rain = rain.withColumn("data_quality_flag", F.when(F.col("soil_moisture_proxy").isNull(), "rainfall_only_proxy_soil_moisture_unavailable_no_reservoir_inflow_claim").otherwise("proxy_not_reservoir_inflow"))
    rain = rain.drop("_epoch", "_wet_class", "_previous_class", "_previous_epoch", "_segment", "_avg_hourly_rain", "_flood_raw", "_drought_raw")

    # Weather-driven demand indexes are empirical proxies, not meter/load forecasts.
    dem = weather.select("region_id", "state_code", "year", "month", "day", "hour", "valid_time_utc", "temperature_2m_c", "apparent_temperature_c", "heat_index_c", "relative_humidity_pct", "dewpoint_c", "wind_speed_10m_mps", "ghi_wm2", "cloud_cover_pct", "precipitation_mm")
    dem = dem.join(rain.select("region_id", "valid_time_utc", "precipitation_24h_mm", "consecutive_wet_hours"), ["region_id", "valid_time_utc"], "left")
    index_window = Window.partitionBy("state_code", "region_id", "month")
    dem = dem.withColumn("cooling_degree_hour", F.greatest(0.0, F.col("temperature_2m_c") - 24.0))
    dem = dem.withColumn("heating_degree_hour", F.greatest(0.0, 18.0 - F.col("temperature_2m_c")))
    dem = dem.withColumn("_rh_norm", normalize_in_partition(F.col("relative_humidity_pct"), index_window))
    dem = dem.withColumn("_hi_norm", normalize_in_partition(F.col("heat_index_c"), index_window))
    dem = dem.withColumn("humidity_discomfort_index", F.when(F.col("_rh_norm").isNotNull() & F.col("_hi_norm").isNotNull(), 0.6 * F.col("_rh_norm") + 0.4 * F.col("_hi_norm")).otherwise(F.col("_rh_norm")))
    dem = dem.withColumn("_cool_norm", normalize_in_partition(F.col("cooling_degree_hour"), index_window))
    dem = dem.withColumn("_heat_norm", normalize_in_partition(F.col("heating_degree_hour"), index_window))
    # The population-activity proxy is unavailable, so redistribute its component weights as specified.
    dem = dem.withColumn("cooling_load_index", F.when(F.col("_cool_norm").isNotNull() & F.col("humidity_discomfort_index").isNotNull(), 0.70 * F.col("_cool_norm") + 0.30 * F.col("humidity_discomfort_index")))
    # NWS wind-chill formula is evaluated in Fahrenheit and mph under the documented conditions.
    tf = F.col("temperature_2m_c") * 9.0 / 5.0 + 32.0
    mph = F.col("wind_speed_10m_mps") * 2.2369362921
    chill_f = 35.74 + 0.6215 * tf - 35.75 * F.pow(mph, 0.16) + 0.4275 * tf * F.pow(mph, 0.16)
    dem = dem.withColumn("wind_chill_index", F.when((F.col("temperature_2m_c") < 10.0) & (mph > 3.0), (chill_f - 32.0) * 5.0 / 9.0))
    dem = dem.withColumn("_chill_norm", normalize_in_partition(F.col("wind_chill_index"), index_window))
    dem = dem.withColumn("heating_load_index", F.when(F.col("_heat_norm").isNotNull() & F.col("_chill_norm").isNotNull(), 0.80 * F.col("_heat_norm") + 0.20 * F.col("_chill_norm")).otherwise(F.col("_heat_norm")))
    dem = dem.withColumn("daylight_flag", F.when(F.col("ghi_wm2").isNotNull(), F.col("ghi_wm2") > 0).otherwise(F.lit(None).cast("boolean")))
    dem = dem.withColumn("solar_radiation_wm2", F.col("ghi_wm2"))
    dem = dem.withColumn("_precip_disruption_raw", 0.50 * F.col("precipitation_mm") + 0.30 * F.col("precipitation_24h_mm") + 0.20 * F.col("consecutive_wet_hours"))
    dem = dem.withColumn("precipitation_disruption_index", normalize_in_partition(F.col("_precip_disruption_raw"), index_window))
    dem = dem.withColumn("wind_discomfort_index", normalize_in_partition(F.col("wind_speed_10m_mps"), index_window))
    dem = dem.withColumn("_solar_norm", normalize_in_partition(F.col("ghi_wm2"), index_window))
    demand_raw = (0.60 * F.col("cooling_load_index") + 0.15 * F.col("heating_load_index") + 0.10 * F.col("_solar_norm")
                  + 0.10 * F.col("precipitation_disruption_index") + 0.05 * F.col("wind_discomfort_index"))
    dem = dem.withColumn("demand_weather_index", F.when(demand_raw.isNotNull(), F.least(1.0, F.greatest(0.0, demand_raw))))
    stress_raw = (0.45 * F.col("cooling_load_index") + 0.20 * F.col("heating_load_index")
                  + 0.15 * F.col("precipitation_disruption_index") + 0.10 * F.col("wind_discomfort_index")
                  + 0.10 * F.col("humidity_discomfort_index"))
    dem = dem.withColumn("weather_stress_index", F.when(stress_raw.isNotNull(), F.least(1.0, F.greatest(0.0, stress_raw))))
    p80 = F.percentile_approx("demand_weather_index", 0.8, 10000).over(index_window)
    dem = dem.withColumn("peak_demand_risk_flag", (F.col("demand_weather_index") >= p80) | (F.col("cooling_degree_hour") >= 6.0) | (F.col("heat_index_c") >= 40.0))
    dem = dem.withColumn("humidity_pct", F.col("relative_humidity_pct"))
    dem = dem.withColumn("data_quality_flag", F.lit("weather_driven_proxy_not_load_forecast"))
    dem = dem.drop("_rh_norm", "_hi_norm", "_cool_norm", "_heat_norm", "_chill_norm", "_precip_disruption_raw", "_solar_norm")

    # Contributions use local seasonal percentiles. They are explicit rule weights,
    # not causal coefficients. Supply terms are grouped into their corresponding factor rows.
    feature_keys = ["region_id", "state_code", "valid_time_utc", "year", "month", "day", "hour"]
    features = weather.join(temporal.select("region_id", "temporal_coverage_score"), "region_id", "left")
    features = features.join(solar.select("region_id", "valid_time_utc", "pv_power_factor", "pv_capacity_factor", "solar_ramp_mw_per_mw_per_hour", "daylight_flag"), ["region_id", "valid_time_utc"], "left")
    features = features.join(wind_table.select("region_id", "valid_time_utc", "wind_power_density_wm2", "wind_capacity_factor", "wind_ramp_mw_per_mw_per_hour"), ["region_id", "valid_time_utc"], "left")
    features = features.join(rain.select("region_id", "valid_time_utc", "precipitation_24h_mm", "precipitation_72h_mm", "rolling_30day_precipitation_mm", "drought_risk_index", "flood_risk_index", "monsoon_flag"), ["region_id", "valid_time_utc"], "left")
    features = features.join(dem.select("region_id", "valid_time_utc", "precipitation_disruption_index", "weather_stress_index"), ["region_id", "valid_time_utc"], "left")
    factor_values = {
        "temperature": (F.col("temperature_2m_c"), "increases_demand"),
        "humidity": (F.col("relative_humidity_pct"), "increases_demand"),
        "heat_index": (F.col("heat_index_c"), "increases_demand"),
        "solar_radiation": (F.col("ghi_wm2"), "increases_solar_supply"),
        "cloud_cover": (F.col("cloud_cover_pct"), "decreases_solar_supply"),
        "wind_speed": (F.col("wind_speed_10m_mps"), "increases_wind_supply"),
        "wind_direction": (F.col("wind_direction_10m_deg"), "increases_wind_supply"),
        "precipitation": (F.col("precipitation_mm"), "increases_supply_risk"),
        "air_density": (F.col("air_density_kgm3"), "increases_wind_supply"),
        "daylight": (F.col("daylight_flag").cast("double"), "increases_solar_supply"),
        "monsoon": (F.col("monsoon_flag").cast("double"), "increases_supply_risk"),
        "drought": (F.col("drought_risk_index"), "increases_supply_risk"),
        "flood": (F.col("flood_risk_index"), "increases_supply_risk"),
        "weather_stress": (F.col("weather_stress_index"), "increases_supply_risk"),
    }
    fw = Window.partitionBy("state_code", "region_id", "month", "hour")
    fact_frames = []
    for factor, (factor_expr, direction) in factor_values.items():
        one = features.select("*", factor_expr.alias("factor_value"))
        one = one.withColumn("factor_name", F.lit(factor)).withColumn("normalized_contribution", normalize_in_partition(F.col("factor_value"), fw))
        # Demand factor weights follow the supplied default; humidity and heat-index split the 0.20.
        demand_weights = {"temperature": 0.40, "humidity": 0.10, "heat_index": 0.10, "solar_radiation": 0.15, "precipitation": 0.10, "wind_speed": 0.05, "daylight": 0.10}
        weight = demand_weights.get(factor, 0.0)
        one = one.withColumn("factor_direction", F.lit(direction))
        one = one.withColumn("raw_contribution", F.col("normalized_contribution") * F.lit(weight))
        one = one.withColumn("contribution_to_demand", F.when(F.lit(weight > 0), F.col("raw_contribution")).otherwise(F.lit(0.0)))
        # Supply/risk features are computed separately with their prescribed blend weights.
        solar_norm = normalize_in_partition(F.col("ghi_wm2"), fw)
        cloud_norm = normalize_in_partition(F.col("cloud_cover_pct"), fw)
        derate_norm = normalize_in_partition(F.col("pv_power_factor"), fw)
        power_norm = normalize_in_partition(F.col("wind_power_density_wm2"), fw)
        capacity_norm = normalize_in_partition(F.col("wind_capacity_factor"), fw)
        density_norm = normalize_in_partition(F.col("air_density_kgm3"), fw)
        rain24_norm = normalize_in_partition(F.col("precipitation_24h_mm"), fw)
        rain72_norm = normalize_in_partition(F.col("precipitation_72h_mm"), fw)
        rain30_norm = normalize_in_partition(F.col("rolling_30day_precipitation_mm"), fw)
        solar_ramp_norm = normalize_in_partition(F.abs(F.col("solar_ramp_mw_per_mw_per_hour")), fw)
        wind_ramp_norm = normalize_in_partition(F.abs(F.col("wind_ramp_mw_per_mw_per_hour")), fw)
        disruption_norm = normalize_in_partition(F.col("precipitation_disruption_index"), fw)
        heat_norm = normalize_in_partition(F.col("heat_index_c"), fw)
        risk_cloud_norm = normalize_in_partition(F.col("cloud_cover_pct"), fw)
        solar_contrib = F.when(F.lit(factor == "solar_radiation"), 0.70 * solar_norm) \
            .when(F.lit(factor == "cloud_cover"), -0.20 * cloud_norm) \
            .when(F.lit(factor == "temperature"), 0.10 * derate_norm).otherwise(0.0)
        wind_contrib = F.when(F.lit(factor == "wind_speed"), 0.70 * power_norm + 0.20 * capacity_norm) \
            .when(F.lit(factor == "air_density"), 0.10 * density_norm).otherwise(0.0)
        hydro_contrib = F.when(F.lit(factor == "precipitation"), 0.60 * rain24_norm + 0.25 * rain72_norm + 0.15 * rain30_norm).otherwise(0.0)
        risk_contrib = F.when(F.lit(factor == "wind_speed"), 0.35 * wind_ramp_norm) \
            .when(F.lit(factor == "solar_radiation"), 0.25 * solar_ramp_norm) \
            .when(F.lit(factor == "precipitation"), 0.20 * disruption_norm) \
            .when(F.lit(factor == "heat_index"), 0.10 * heat_norm) \
            .when(F.lit(factor == "cloud_cover"), 0.10 * risk_cloud_norm).otherwise(0.0)
        one = one.withColumn("contribution_to_solar_supply", solar_contrib).withColumn("contribution_to_wind_supply", wind_contrib)
        one = one.withColumn("contribution_to_hydro_supply", hydro_contrib).withColumn("contribution_to_supply_risk", risk_contrib)
        one = one.withColumn("method", F.lit("rule_based_normalized_contribution"))
        source_quality_score = F.when(F.col("quality_flag") == "ok", 1.0).otherwise(0.5)
        temporal_coverage = F.col("temporal_coverage_score")
        spatial_representativeness = F.lit(0.5)  # City observations are not statewide coverage.
        variable_availability = F.col("factor_value").isNotNull().cast("double")
        one = one.withColumn("confidence", 0.40 * source_quality_score + 0.25 * temporal_coverage + 0.20 * spatial_representativeness + 0.15 * variable_availability)
        one = one.withColumn("data_quality_flag", F.when(F.col("factor_value").isNull(), "source_field_unavailable").otherwise("proxy_not_causal"))
        fact_frames.append(one.select(*feature_keys, "population_weight", "factor_name", "factor_value", "factor_direction", "raw_contribution", "normalized_contribution", "contribution_to_demand", "contribution_to_solar_supply", "contribution_to_wind_supply", "contribution_to_hydro_supply", "contribution_to_supply_risk", "method", "confidence", "data_quality_flag"))
    factors_df = fact_frames[0]
    for one in fact_frames[1:]: factors_df = factors_df.unionByName(one)
    state_group_keys = ["state_code", "valid_time_utc", "year", "month", "day", "hour", "factor_name"]
    measure_pairs = [("factor_value", "state_factor_value"), ("normalized_contribution", "state_normalized_factor"),
                     ("contribution_to_demand", "state_contribution_to_demand"), ("contribution_to_solar_supply", "state_contribution_to_solar_supply"),
                     ("contribution_to_wind_supply", "state_contribution_to_wind_supply"), ("contribution_to_hydro_supply", "state_contribution_to_hydro_supply"),
                     ("contribution_to_supply_risk", "state_contribution_to_supply_risk")]
    state_aggs = [F.countDistinct(F.when(F.col("factor_value").isNotNull(), F.col("region_id"))).alias("contributing_region_count"),
                  F.countDistinct(F.when(F.col("factor_value").isNotNull() & (F.col("population_weight") > 0), F.col("region_id"))).alias("_weighted_region_count"),
                  F.avg("confidence").alias("confidence")]
    for source_col, _ in measure_pairs:
        state_aggs.extend([F.avg(source_col).alias(f"_avg_{source_col}"),
            (F.sum(F.when(F.col(source_col).isNotNull() & (F.col("population_weight") > 0), F.col(source_col) * F.col("population_weight"))) /
             F.sum(F.when(F.col(source_col).isNotNull() & (F.col("population_weight") > 0), F.col("population_weight")))).alias(f"_weighted_{source_col}")])
    state_factors = factors_df.groupBy(*state_group_keys).agg(*state_aggs)
    all_population_available = (F.col("_weighted_region_count") == F.col("contributing_region_count")) & (F.col("contributing_region_count") > 0)
    for source_col, target_col in measure_pairs:
        state_factors = state_factors.withColumn(target_col, F.when(all_population_available, F.col(f"_weighted_{source_col}")).otherwise(F.col(f"_avg_{source_col}")))
    state_factors = state_factors.withColumn("weight_method", F.when(all_population_available, "population").otherwise("equal_city"))
    state_factors = state_factors.drop(*[f"_{kind}_{col}" for col, _ in measure_pairs for kind in ("avg", "weighted")], "_weighted_region_count")
    state_factors = state_factors.withColumn("state_name", F.when(F.col("state_code") == "KA", "Karnataka").when(F.col("state_code") == "MH", "Maharashtra").otherwise("Tripura"))
    state_factors = state_factors.withColumn("data_quality_flag", F.lit("state_city_composite_not_statewide_observation"))

    # Missing cells and absent hourly records are emitted with gap duration.
    quality_cols = ["temperature_2m_c", "dewpoint_c", "relative_humidity_pct", "specific_humidity_gkg", "apparent_temperature_c", "wind_speed_10m_mps", "wind_direction_10m_deg", "wind_u_10m_mps", "wind_v_10m_mps", "wind_speed_100m_mps", "wind_direction_100m_deg", "precipitation_mm", "cloud_cover_pct", "ghi_wm2", "dni_wm2", "dhi_wm2", "air_pressure_hpa", "air_density_kgm3"]
    value_gap_frames = []
    conflict_log = conflicts.select("region_id", "state_code", "valid_time_utc", F.year("valid_time_utc").alias("year"), F.month("valid_time_utc").alias("month"), F.lit("*").alias("variable"), F.lit("conflicting_duplicate_excluded").alias("gap_reason"), F.lit(None).cast("long").alias("gap_duration_hours"))
    value_gap_frames.append(conflict_log)
    for col in quality_cols:
        series = weather.select("region_id", "state_code", "valid_time_utc", F.lit(col).alias("variable"), F.col(col).isNull().alias("_missing"))
        series = series.withColumn("_epoch", F.unix_timestamp("valid_time_utc"))
        sw = Window.partitionBy("region_id", "variable").orderBy("_epoch")
        series = series.withColumn("_prev_missing", F.lag("_missing").over(sw)).withColumn("_prev_epoch", F.lag("_epoch").over(sw))
        series = series.withColumn("_break", F.when(F.col("_missing") & (~F.coalesce(F.col("_prev_missing"), F.lit(False)) | ((F.col("_epoch") - F.col("_prev_epoch")) > 3600)), 1).otherwise(0))
        series = series.withColumn("_segment", F.sum("_break").over(sw.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        streak = Window.partitionBy("region_id", "variable", "_segment").orderBy("_epoch")
        series = series.withColumn("gap_duration_hours", F.when(F.col("_missing"), F.row_number().over(streak)).otherwise(F.lit(0)))
        value_gap_frames.append(series.filter("_missing").select("region_id", "state_code", "valid_time_utc", F.year("valid_time_utc").alias("year"), F.month("valid_time_utc").alias("month"), "variable", F.lit("source_value_null").alias("gap_reason"), "gap_duration_hours"))
    expected_hours = base.groupBy("region_id", "state_code").agg(F.min("valid_time_utc").alias("_start"), F.max("valid_time_utc").alias("_end"))
    expected_hours = expected_hours.withColumn("_hours", F.expr("sequence(_start, _end, interval 1 hour)"))
    expected_hours = expected_hours.select("region_id", "state_code", F.explode("_hours").alias("valid_time_utc")).withColumn("year", F.year("valid_time_utc")).withColumn("month", F.month("valid_time_utc"))
    known_hours = base.select("region_id", "state_code", "valid_time_utc").unionByName(conflicts.select("region_id", "state_code", "valid_time_utc")).dropDuplicates()
    missing_hour = expected_hours.join(known_hours, ["region_id", "state_code", "valid_time_utc"], "left_anti")
    gap_window = Window.partitionBy("region_id").orderBy("valid_time_utc")
    missing_hour = missing_hour.withColumn("_epoch", F.unix_timestamp("valid_time_utc")).withColumn("_prev", F.lag(F.unix_timestamp("valid_time_utc")).over(gap_window))
    missing_hour = missing_hour.withColumn("_segment", F.sum(F.when(F.col("_prev").isNull() | ((F.col("_epoch") - F.col("_prev")) > 3600), 1).otherwise(0)).over(gap_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    missing_hour = missing_hour.withColumn("gap_duration_hours", F.row_number().over(Window.partitionBy("region_id", "_segment").orderBy("valid_time_utc")))
    value_gap_frames.append(missing_hour.select("region_id", "state_code", "valid_time_utc", "year", "month", F.lit("*").alias("variable"), F.lit("missing_timestamp_row").alias("gap_reason"), "gap_duration_hours"))
    gap_log = value_gap_frames[0]
    for frame in value_gap_frames[1:]: gap_log = gap_log.unionByName(frame)

    # Per-variable quality summary: expected calendar hours, missingness, duplicates, conflicts, outliers, and quantiles.
    qframes = []
    for col in quality_cols:
        outlier = {
            "temperature_2m_c": (F.col(col) < -30) | (F.col(col) > 55), "dewpoint_c": F.col(col) > F.col("temperature_2m_c"),
            "relative_humidity_pct": (F.col(col) < 0) | (F.col(col) > 100), "wind_speed_10m_mps": F.col(col) < 0,
            "wind_speed_100m_mps": F.col(col) < 0, "precipitation_mm": F.col(col) < 0,
            "ghi_wm2": (F.col(col) < 0) | (F.col(col) > 1500), "cloud_cover_pct": (F.col(col) < 0) | (F.col(col) > 100),
        }.get(col, F.lit(False))
        q = weather.groupBy("region_id", "state_code", "year", "month").agg(
            F.min("valid_time_utc").alias("period_start"), F.max("valid_time_utc").alias("period_end"),
            F.count(F.col(col)).alias("observed_hour_count"), F.sum(F.col(col).isNull().cast("long")).alias("null_value_count"),
            F.avg(col).alias("mean_value"), F.percentile_approx(col, 0.50, 10000).alias("median_value"),
            F.percentile_approx(col, 0.05, 10000).alias("p05"), F.percentile_approx(col, 0.95, 10000).alias("p95"),
            F.min(col).alias("min_value"), F.max(col).alias("max_value"), F.sum(outlier.cast("long")).alias("outlier_count"),
        )
        q = q.join(expected_hours.groupBy("region_id", "state_code", "year", "month").count().withColumnRenamed("count", "expected_hour_count"), ["region_id", "state_code", "year", "month"], "left")
        q = q.join(duplicate_summary, ["region_id", "state_code", "year", "month"], "left")
        q = q.withColumn("variable", F.lit(col)).withColumn("missing_hour_count", F.greatest(F.lit(0), F.col("expected_hour_count") - F.col("observed_hour_count")))
        q = q.withColumn("missing_pct", F.when(F.col("expected_hour_count") > 0, 100 * F.col("missing_hour_count") / F.col("expected_hour_count")))
        estimated_count = F.sum(F.when(F.col("wind_speed_100m_method") == "power_law_estimate", 1).otherwise(0)) if col == "wind_speed_100m_mps" else F.lit(0)
        if col == "wind_speed_100m_mps":
            est = weather.groupBy("region_id", "state_code").agg(estimated_count.alias("estimated_count"))
            q = q.join(est, ["region_id", "state_code"], "left")
        else:
            q = q.withColumn("estimated_count", F.lit(0))
        q = q.withColumn("interpolated_count", F.lit(0))
        q = q.withColumn("gap_reasons", F.when(F.col("null_value_count") > 0, "source null values and/or absent timestamps").when(F.col("missing_hour_count") > 0, "absent timestamps").otherwise("none"))
        q = q.drop("null_value_count")
        qframes.append(q)
    quality = qframes[0]
    for q in qframes[1:]: quality = quality.unionByName(q)
    outputs = {"regions": regions, "region_coordinates": region_coordinates, "weather_hourly": weather, "solar_table": solar, "wind_table": wind_table, "rain_table": rain, "demand_weather_table": dem, "factor_contribution_table": factors_df, "state_factor_contribution_table": state_factors, "data_quality_report": quality, "gap_log": gap_log}
    formula_docs = {
        "dewpoint_c": "Magnus: gamma=ln(RH/100)+17.625*T/(243.04+T); Td=243.04*gamma/(17.625-gamma)",
        "relative_humidity_pct": "100*exp(17.625*Td/(243.04+Td)-17.625*T/(243.04+T)) when absent",
        "specific_humidity_gkg": "es=6.112*exp(17.67*T/(T+243.5)); e=(RH/100)*es; q=1000*(0.622*e)/(pressure_hpa-0.378*e)",
        "air_pressure_hpa": "source pressure converted Pa to hPa when >2000; else altitude formula 1013.25*(1-2.25577e-5*elevation)^5.25588 if elevation is available",
        "clear_sky_index": "GHI/clear_sky_GHI only for daylight and positive clear-sky GHI; NULL without reference",
        "daylight_flag": "solar_elevation_deg>0 when available; otherwise GHI>0",
        "wind_u_10m_mps": "-speed*sin(meteorological_direction_rad)", "wind_v_10m_mps": "-speed*cos(meteorological_direction_rad)",
        "wind_speed_100m_mps": f"observed when present; otherwise speed_10m*(100/10)^{config.wind_shear_exponent:g} estimate",
        "air_density_kgm3": "pressure_hpa*100/(287.05*(temperature_2m_c+273.15)); NULL if pressure unavailable",
        "heat_index_c": "Rothfusz heat-index regression with low/high humidity adjustment; only T>=26.7 C and RH 0..100",
        "ghi_wm2": "retained when source reports W/m2; ssrd accumulated J/m2 is max(0,delta)/3600",
        "precipitation_mm": "retained when hourly; cumulative source tp/total_precipitation uses max(0,current-previous)",
        "pv_cell_temperature_c": f"T_air+(GHI/800)*(NOCT-20), NOCT={config.noct_c:g} C",
        "pv_power_factor": f"1 + gamma_pv*(pv_cell_temperature_c-25), gamma_pv={config.gamma_pv_per_c:g}/C",
        "pv_capacity_factor": f"clamp((GHI/1000)*pv_power_factor*{config.system_performance_ratio:g},0,1); 0 when GHI<=0; NULL when GHI is missing",
        "solar_ramp_mw_per_mw_per_hour": "pv_capacity_factor_t - pv_capacity_factor_(t-1), only for consecutive hours",
        "wind_power_density_wm2": "0.5*air_density_kgm3*wind_speed_hub_mps^3",
        "turbine_power_factor": f"generic cubic curve: 0 below {config.turbine_cut_in_mps:g}, cubic ramp to {config.turbine_rated_mps:g}, 1 to {config.turbine_cut_out_mps:g}, 0 at/above cut-out m/s",
        "wind_capacity_factor": "clamp(turbine_power_factor*(air_density_kgm3/1.225),0,1)",
        "precipitation_24h_mm": "time-based 24-hour precipitation sum; analogous 3h/6h/72h windows",
        "flood_risk_index": "region-month 5th/95th percentile normalized 0.45*P24+0.35*P72+0.20*intensity",
        "drought_risk_index": "region-month normalized 0.50*30d deficit+0.30*consecutive dry days+0.20*90d deficit; deficits use same-region/month mean hourly rain baseline",
        "hydro_inflow_proxy": "normalized rainfall-only proxy with weights renormalized to 0.625*P24+0.375*P72; not reservoir inflow",
        "cooling_degree_hour": "max(0,temperature_2m_c-24)", "heating_degree_hour": "max(0,18-temperature_2m_c)",
        "humidity_discomfort_index": "0.6*normalized RH + 0.4*normalized heat index; if heat index unavailable uses RH only",
        "wind_chill_index": "NWS Fahrenheit/mph formula; only T<10 C and wind>4.8 km/h",
        "cooling_load_index": "0.70*normalized cooling degree hour + 0.30*humidity discomfort; population activity unavailable",
        "heating_load_index": "0.80*normalized heating degree hour + 0.20*normalized wind chill; renormalized when chill unavailable",
        "demand_weather_index": "0.60*cooling+0.15*heating+0.10*normalized GHI+0.10*precip disruption+0.05*wind discomfort",
        "weather_stress_index": "normalized weighted blend: 0.45 cooling+0.20 heating+0.15 precip disruption+0.10 wind discomfort+0.10 humidity discomfort",
        "normalized_contribution": "clamp((factor_value-p05)/(p95-p05),0,1) within state,region,month,hour",
        "state_factor_value": "equal-city mean by state and hour; population weights unavailable",
    }
    formula_docs["state_factor_value"] = "population-weighted mean if every contributing city has positive population; otherwise equal-city mean"
    metadata_rows = []
    for table_name, frame in outputs.items():
        metadata_fields = [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]
        lineage_descriptions = {"source_dataset": "string", "source_url": "string", "source_license": "string", "source_download_date": "date", "raw_file_name": "string", "raw_file_checksum": "string", "transformation_code_version": "string", "region_boundary_version": "string", "ingested_at": "timestamp", "updated_at": "timestamp", "is_estimate": "boolean", "estimation_method": "string", "confidence": "double"}
        metadata_fields.extend((name, dtype) for name, dtype in lineage_descriptions.items() if name not in frame.columns)
        for column_name, data_type in metadata_fields:
            mapping = next((source for aliases in ALIASES.values() for source in aliases if snake(source) == column_name), None)
            metadata_rows.append((table_name, column_name, data_type, unit_guess(column_name),
                                  f"{table_name}.{column_name}", formula_docs.get(column_name, lineage_descriptions.get(column_name, "Source-mapped value; NULL if unavailable")), mapping,
                                  "Indian_Weather_Dataset_1996-2026", f"transformation_code_version={VERSION}", "Kaggle source license; verify license terms", "See limitations.md"))
    metadata = spark.createDataFrame(metadata_rows, ["table_name", "column_name", "data_type", "unit", "description", "formula", "source_field", "source_dataset", "transformation_rule", "license", "limitations"])
    outputs["metadata_catalog"] = metadata

    # Apply common lineage fields to every output table, retaining specialized values already computed.
    file_digests = {p.name: sha256_file(p) for p in files}
    combined_digest = hashlib.sha256(json.dumps(file_digests, sort_keys=True).encode("utf-8")).hexdigest()
    raw_names = ",".join(sorted(file_digests))
    for table_name, frame in list(outputs.items()):
        defaults = {
            "source_dataset": F.lit("Indian_Weather_Dataset_1996-2026"), "source_url": F.lit(SOURCE_URL),
            "source_license": F.lit("Kaggle dataset license; verify terms before redistribution"),
            "source_download_date": F.lit(None).cast("date"), "raw_file_name": F.lit(raw_names),
            "raw_file_checksum": F.lit(combined_digest), "transformation_code_version": F.lit(VERSION),
            "region_boundary_version": F.lit("city-coordinate-grid-0.25deg-v1"),
            "ingested_at": F.current_timestamp(), "updated_at": F.current_timestamp(),
            "is_estimate": F.lit(table_name in {"solar_table", "wind_table", "rain_table", "demand_weather_table", "factor_contribution_table", "state_factor_contribution_table"}),
            "estimation_method": F.lit("rule_based_proxy" if table_name in {"solar_table", "wind_table", "rain_table", "demand_weather_table", "factor_contribution_table", "state_factor_contribution_table"} else None),
            "confidence": F.lit(0.5),
        }
        for col, value in defaults.items():
            if col not in frame.columns:
                frame = frame.withColumn(col, value)
        outputs[table_name] = frame
    # Preview and validation summary are materialized before curated writes for user review/audit.
    preview = base.orderBy("valid_time_utc", "region_id").limit(20).toJSON().collect()
    (audit_dir / "first_20_cleaned_rows.json").write_text("[\n" + ",\n".join(json.dumps(json.loads(row), indent=2, default=str) for row in preview) + "\n]", encoding="utf-8")
    solar_hour_profile = solar.filter(F.col("ghi_wm2").isNotNull()).groupBy("region_id", "hour").agg(F.avg("ghi_wm2").alias("mean_ghi"))
    solar_hour_rank = Window.partitionBy("region_id").orderBy(F.col("mean_ghi").desc(), F.col("hour"))
    solar_peak = solar_hour_profile.withColumn("_rank", F.row_number().over(solar_hour_rank)).filter(F.col("_rank") == 1).join(city_regions.select("region_id", "centroid_longitude"), "region_id", "left")
    expected_noon = F.pmod(F.round(12.0 - F.col("centroid_longitude") / 15.0), 24)
    solar_peak = solar_peak.withColumn("_expected_noon", expected_noon).withColumn("_hour_distance", F.least(F.abs(F.col("hour") - F.col("_expected_noon")), 24 - F.abs(F.col("hour") - F.col("_expected_noon"))))
    state_range = factors_df.groupBy("state_code", "valid_time_utc", "factor_name").agg(F.min("factor_value").alias("_min_region"), F.max("factor_value").alias("_max_region"))
    state_range = state_range.join(state_factors.select("state_code", "valid_time_utc", "factor_name", "state_factor_value"), ["state_code", "valid_time_utc", "factor_name"], "inner")
    cloud_means = solar.agg(F.avg(F.when(F.col("cloud_cover_pct") >= 75, F.col("clear_sky_index")).otherwise(None)).alias("cloudy_mean"),
                            F.avg(F.when(F.col("cloud_cover_pct") < 25, F.col("clear_sky_index")).otherwise(None)).alias("clear_mean")).first()
    cloudy_clear_comparison = None if cloud_means.cloudy_mean is None or cloud_means.clear_mean is None else bool(cloud_means.cloudy_mean <= cloud_means.clear_mean)
    validation = {
        "hourly_missing_timestamp_rows": gap_log.filter(F.col("variable") == "*").count(),
        "ghi_invalid_range_rows": weather.filter((F.col("ghi_wm2") < 0) | (F.col("ghi_wm2") > 1500)).count(),
        "pv_capacity_factor_out_of_bounds": solar.filter((F.col("pv_capacity_factor") < 0) | (F.col("pv_capacity_factor") > 1)).count(),
        "pv_night_nonzero_rows": solar.filter((F.col("daylight_flag") == False) & (F.col("pv_capacity_factor") != 0)).count(),
        "solar_noon_peak_deviation_over_2_hours": solar_peak.filter(F.col("_hour_distance") > 2).count(),
        "solar_elevation_night_positive_ghi_rows": solar.filter((F.col("solar_elevation_deg") <= 0) & (F.col("ghi_wm2") > 0)).count(),
        "cloudy_hour_clear_sky_index_mean": cloud_means.cloudy_mean,
        "clear_hour_clear_sky_index_mean": cloud_means.clear_mean,
        "cloudy_hours_lower_clear_sky_index": cloudy_clear_comparison,
        "negative_wind_speed_rows": weather.filter((F.col("wind_speed_10m_mps") < 0) | (F.col("wind_speed_100m_mps") < 0)).count(),
        "wind_below_cut_in_nonzero_rows": wind_table.filter((F.col("wind_speed_hub_mps") < config.turbine_cut_in_mps) & (F.col("turbine_power_factor") != 0)).count(),
        "wind_above_cut_out_nonzero_rows": wind_table.filter((F.col("wind_speed_hub_mps") >= config.turbine_cut_out_mps) & (F.col("turbine_power_factor") != 0)).count(),
        "wind_ramps_over_0_3_capacity_factor": wind_table.filter(F.col("wind_ramp_flag") == True).count(),
        "negative_precipitation_rows": weather.filter(F.col("precipitation_mm") < 0).count(),
        "daily_rainfall_sum_mismatch_days": rain.groupBy("region_id", "year", "month", "day").agg(F.sum("precipitation_mm").alias("sum_h"), F.max("daily_precipitation_mm").alias("daily_total")).filter(F.abs(F.col("sum_h") - F.col("daily_total")) > 1e-6).count(),
        "cooling_degree_rule_violations": dem.filter((F.col("temperature_2m_c") <= 24) & (F.col("cooling_degree_hour") != 0)).count(),
        "heating_degree_rule_violations": dem.filter((F.col("temperature_2m_c") >= 18) & (F.col("heating_degree_hour") != 0)).count(),
        "demand_index_out_of_bounds": dem.filter((F.col("demand_weather_index") < 0) | (F.col("demand_weather_index") > 1)).count(),
        "weather_stress_index_out_of_bounds": dem.filter((F.col("weather_stress_index") < 0) | (F.col("weather_stress_index") > 1)).count(),
        "state_factor_outside_region_minmax": state_range.filter((F.col("state_factor_value") < F.col("_min_region")) | (F.col("state_factor_value") > F.col("_max_region"))).count(),
        "demand_risk_flags_by_hour": [r.asDict() for r in dem.groupBy("hour").agg(F.avg(F.col("peak_demand_risk_flag").cast("double")).alias("flag_rate")).orderBy("hour").collect()],
        "demand_risk_flags_by_month": [r.asDict() for r in dem.groupBy("month").agg(F.avg(F.col("peak_demand_risk_flag").cast("double")).alias("flag_rate")).orderBy("month").collect()],
    }
    validation["core_checks_passed"] = all(validation[k] == 0 for k in (
        "ghi_invalid_range_rows", "pv_capacity_factor_out_of_bounds", "pv_night_nonzero_rows",
        "negative_wind_speed_rows", "wind_below_cut_in_nonzero_rows", "wind_above_cut_out_nonzero_rows",
        "negative_precipitation_rows", "daily_rainfall_sum_mismatch_days", "cooling_degree_rule_violations",
        "heating_degree_rule_violations", "demand_index_out_of_bounds", "weather_stress_index_out_of_bounds", "state_factor_outside_region_minmax",
    ))
    summary = {
        "source_files": [{"name": name, "sha256": digest} for name, digest in file_digests.items()],
        "rows_after_state_filter": base.count(),
        "regions_by_state": [r.asDict() for r in base.groupBy("state_code", "state_name").agg(F.countDistinct("region_id").alias("city_count"), F.sort_array(F.collect_set("city")).alias("cities")).collect()],
        "raw_schema_snapshot": snapshot,
        "column_mapping": {canonical: selected_name(raw, aliases) for canonical, aliases in ALIASES.items()},
        "duplicate_resolution_field": version_field,
        "timezone": f"source values interpreted as {source_timezone}; canonical UTC and valid_time_ist=UTC+05:30",
        "model_configuration": config.__dict__,
        "missing_value_strategy": "No forward-fill or interpolation; missing source values remain NULL and are logged; only formulas explicitly listed in metadata are derived.",
        "validation_checks": validation,
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
        "assumptions": ["timestamps interpreted as UTC unless source metadata establishes another zone", "units are mapped from explicit column suffixes for this archive; unknown aliases require source metadata", "population weights/elevation/clear-sky radiation/soil moisture/actual load are absent; equal-city state composites and nulls/proxies are documented", "state values describe the cities present in the filtered input, not complete-state coverage"],
    }
    (audit_dir / "validation_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    base.show(20, truncate=False)
    LOG.info("Audit preview and validation summary written to %s before curated outputs", audit_dir)
    for companion in ("sample_queries.sql", "pipeline_architecture.md", "limitations.md"):
        shutil.copy2(Path(__file__).with_name(companion), output / companion)
    for name, frame in outputs.items():
        write_partitioned(frame, name, output)
    raw.unpersist()


def main():
    parser = argparse.ArgumentParser(description="Inspect and process the Indian hourly weather dataset with PySpark.")
    parser.add_argument("--input", type=Path, default=ROOT / "data" / "raw", help="Directory containing downloaded Kaggle CSV/Parquet files")
    parser.add_argument("--output", type=Path, default=ROOT / "output", help="Curated output directory")
    parser.add_argument("--inspect-only", action="store_true", help="Write raw schema snapshot and print up to 20 raw rows; do not create curated tables")
    parser.add_argument("--source-timezone", default="UTC", choices=("UTC", "Asia/Kolkata"), help="Timezone represented by naive source timestamps; UTC by default")
    parser.add_argument("--noct-c", type=float, default=45.0, help="PV module NOCT in Celsius")
    parser.add_argument("--gamma-pv-per-c", type=float, default=-0.004, help="PV temperature coefficient per Celsius")
    parser.add_argument("--system-performance-ratio", type=float, default=0.80, help="PV performance ratio in [0,1]")
    parser.add_argument("--hub-height-m", type=float, default=100.0, help="Wind turbine hub height in metres")
    parser.add_argument("--wind-shear-exponent", type=float, default=0.20, help="Power-law shear exponent; defaults to mixed/urban terrain")
    parser.add_argument("--turbine-cut-in-mps", type=float, default=3.0)
    parser.add_argument("--turbine-rated-mps", type=float, default=12.0)
    parser.add_argument("--turbine-cut-out-mps", type=float, default=25.0)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = SparkSession.builder.appName("IndianWeatherFeatureWarehouse").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    config = ProcessingConfig(args.noct_c, args.gamma_pv_per_c, args.system_performance_ratio, args.hub_height_m,
                              args.wind_shear_exponent, args.turbine_cut_in_mps, args.turbine_rated_mps, args.turbine_cut_out_mps)
    if not 0 <= config.system_performance_ratio <= 1:
        parser.error("--system-performance-ratio must be between 0 and 1")
    if not 0 < config.hub_height_m or not 0 < config.turbine_cut_in_mps < config.turbine_rated_mps < config.turbine_cut_out_mps:
        parser.error("Hub height and turbine speeds must be positive with cut-in < rated < cut-out")
    try: run(spark, args.input.resolve(), args.output.resolve(), args.inspect_only, config, args.source_timezone)
    finally: spark.stop()


if __name__ == "__main__":
    main()
