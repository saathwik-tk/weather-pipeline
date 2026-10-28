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
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

LOG = logging.getLogger("weather_process")
ROOT = Path(__file__).resolve().parents[2]
STATES = {"karnataka": ("KA", "Karnataka"), "maharashtra": ("MH", "Maharashtra"), "tripura": ("TR", "Tripura")}
SOURCE_URL = "https://www.kaggle.com/datasets/sandeepgupta09/indian-weather-dataset-1996-2026"
VERSION = "0.1.0"

# Canonical output name -> known source field aliases. Extend this map for new datasets.
ALIASES = {
    "timestamp": ("timestamp", "time", "datetime", "date_time", "valid_time", "valid_time_utc"),
    "state": ("state", "state_name", "admin1"), "city": ("city", "city_name", "location", "station"),
    "latitude": ("latitude", "lat"), "longitude": ("longitude", "lon", "lng"),
    "temperature": ("temperature_2m", "temperature_2m_c", "temperature_c", "temperature", "temp", "t2m"),
    "dewpoint": ("dewpoint_2m", "dew_point_2m", "dew_point_c", "dewpoint_c", "dewpoint", "d2m"),
    "humidity": ("relative_humidity_2m", "relative_humidity_pct", "relative_humidity", "humidity", "rh"),
    "precipitation": ("precipitation", "precipitation_mm", "rain", "total_precipitation", "tp"),
    "cloud": ("cloud_cover", "cloud_cover_pct", "cloudcover", "tcc"),
    "wind10": ("wind_speed_10m", "wind_speed_10m_mps", "wind_speed_ms", "wind_speed", "windspeed_10m"),
    "winddir10": ("wind_direction_10m", "wind_direction_10m_deg", "wind_direction_deg", "wind_direction", "winddirection_10m"),
    "u10": ("wind_u_10m_mps", "u_component_of_wind_10m", "u10"), "v10": ("wind_v_10m_mps", "v_component_of_wind_10m", "v10"),
    "wind100": ("wind_speed_100m", "wind_speed_100m_mps", "windspeed_100m"),
    "winddir100": ("wind_direction_100m", "wind_direction_100m_deg", "winddirection_100m"),
    "pressure": ("surface_pressure", "pressure_msl", "air_pressure_hpa", "pressure", "sp"),
    "ghi": ("shortwave_radiation", "solar_radiation_wm2", "ghi_wm2", "ghi", "global_horizontal_irradiance", "ssrd"),
    "dni": ("direct_normal_irradiance", "dni_wm2", "dni"), "dhi": ("diffuse_radiation", "dhi_wm2", "dhi"),
}


def snake(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name.strip()).lower()).strip("_")


def discover_files(path: Path) -> list[Path]:
    return [p for p in path.rglob("*") if p.is_file() and p.suffix.lower() in {".csv", ".parquet"} and "output" not in p.parts]


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
        stats = df.select(c.cast("string").alias("v")).agg(
            F.sum(F.when(F.col("v").isNull() | (F.trim("v") == ""), 1).otherwise(0)).alias("nulls"),
            F.countDistinct("v").alias("unique_count"), F.min("v").alias("min_value"), F.max("v").alias("max_value"),
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
    for token, unit in (("temperature", "°C or K; inspect source metadata"), ("dewpoint", "°C or K; inspect source metadata"), ("humidity", "%"),
                        ("precipitation", "mm or source-dependent accumulated units"), ("wind_speed", "km/h or m/s; inspect source metadata"),
                        ("radiation", "W/m² or accumulated J/m²; inspect source metadata"), ("pressure", "hPa or Pa"), ("latitude", "degrees north"), ("longitude", "degrees east")):
        if token in n: return unit
    return "unknown; see source documentation"


def pick(df, aliases):
    for name in aliases:
        if name in df.columns: return F.col(name)
    return F.lit(None).cast("double")


def numeric(expr):
    return expr.cast("double")


def normalized(col, window):
    lo = F.percentile_approx(col, 0.05).over(window)
    hi = F.percentile_approx(col, 0.95).over(window)
    return F.when(col.isNull() | (hi <= lo), None).otherwise(F.least(F.lit(1.0), F.greatest(F.lit(0.0), (col - lo) / (hi - lo))))


def write_partitioned(df, name: str, output: Path):
    # Static dimensions/catalogues have no natural observation date. Keep them
    # in the requested partition layout using stable sentinel partitions.
    for col, value, dtype in (("state_code", "ALL", "string"), ("year", 0, "int"), ("month", 0, "int"), ("region_id", "ALL", "string")):
        if col not in df.columns:
            df = df.withColumn(col, F.lit(value).cast(dtype))
    (df.write.mode("overwrite").partitionBy("state_code", "year", "month", "region_id").parquet(str(output / name)))


def run(spark: SparkSession, input_path: Path, output: Path, inspect_only: bool):
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
    df = raw.select("*", F.to_timestamp(ts).alias("_parsed_time"), state.alias("_state_key"), city.alias("_city"), lat.alias("_lat"), lon.alias("_lon"))
    df = df.filter(F.col("_state_key").isin(*STATES.keys()) & F.col("_parsed_time").isNotNull())
    state_code = F.when(F.col("_state_key") == "karnataka", "KA").when(F.col("_state_key") == "maharashtra", "MH").otherwise("TR")
    state_name = F.when(F.col("_state_key") == "karnataka", "Karnataka").when(F.col("_state_key") == "maharashtra", "Maharashtra").otherwise("Tripura")
    # Kaggle Open-Meteo archive timestamps are local-naive only if metadata says so. Default is UTC and is explicit in lineage.
    utc = F.to_utc_timestamp(F.col("_parsed_time"), "UTC")
    k = F.col("_raw_file_name")
    temp_raw = numeric(pick(df, ALIASES["temperature"]))
    # Open-Meteo temperature_2m is Celsius; generic temperature columns remain numeric and flagged as source-mapped.
    temp = temp_raw
    dew = numeric(pick(df, ALIASES["dewpoint"]))
    rh = numeric(pick(df, ALIASES["humidity"]))
    pressure = numeric(pick(df, ALIASES["pressure"]))
    # Common Open-Meteo pressure is hPa; Pa values are scaled when evidently > 2000.
    pressure_hpa = F.when(pressure > 2000, pressure / 100.0).otherwise(pressure)
    wind = numeric(pick(df, ALIASES["wind10"]))
    winddir = numeric(pick(df, ALIASES["winddir10"]))
    u, v = numeric(pick(df, ALIASES["u10"])), numeric(pick(df, ALIASES["v10"]))
    speed = F.coalesce(wind, F.sqrt(u * u + v * v))
    u_out = F.coalesce(u, -speed * F.sin(F.radians(winddir)))
    v_out = F.coalesce(v, -speed * F.cos(F.radians(winddir)))
    dew = F.coalesce(dew, F.when((rh > 0) & (temp > -243.04), (243.04 * (F.log(rh / 100.0) + 17.625 * temp / (243.04 + temp))) / (17.625 - (F.log(rh / 100.0) + 17.625 * temp / (243.04 + temp)))))
    rh = F.coalesce(rh, 100.0 * F.exp(17.625 * dew / (243.04 + dew) - 17.625 * temp / (243.04 + temp)))
    ghi = numeric(pick(df, ALIASES["ghi"]))
    cloud = numeric(pick(df, ALIASES["cloud"]))
    rain = numeric(pick(df, ALIASES["precipitation"]))
    wind100 = numeric(pick(df, ALIASES["wind100"]))
    winddir100 = numeric(pick(df, ALIASES["winddir100"]))
    dni, dhi = numeric(pick(df, ALIASES["dni"])), numeric(pick(df, ALIASES["dhi"]))
    name = F.coalesce(F.col("_city"), F.lit("unknown"))
    region_id = F.concat(F.lit("CITY_"), F.regexp_replace(F.upper(name), "[^A-Z0-9]+", "_"))
    base = df.select(
        region_id.alias("region_id"), state_code.alias("state_code"), state_name.alias("state_name"), name.alias("city"),
        F.col("_lat").alias("latitude"), F.col("_lon").alias("longitude"), utc.alias("valid_time_utc"),
        temp.alias("temperature_2m_c"), dew.alias("dewpoint_c"), rh.alias("relative_humidity_pct"),
        speed.alias("wind_speed_10m_mps"), winddir.alias("wind_direction_10m_deg"), u_out.alias("wind_u_10m_mps"), v_out.alias("wind_v_10m_mps"),
        wind100.alias("wind_speed_100m_mps"), winddir100.alias("wind_direction_100m_deg"),
        numeric(pick(df, ALIASES["wind100"])).isNotNull().alias("wind_100m_observed"),
        rain.alias("precipitation_mm"), cloud.alias("cloud_cover_pct"), ghi.alias("ghi_wm2"), dni.alias("dni_wm2"), dhi.alias("dhi_wm2"),
        pressure_hpa.alias("air_pressure_hpa"), F.col("_raw_file_name").alias("raw_file_name"),
    ).withColumn("valid_time_ist", F.expr("valid_time_utc + INTERVAL 5 HOURS 30 MINUTES"))
    base = base.withColumn("year", F.year("valid_time_utc")).withColumn("month", F.month("valid_time_utc")).withColumn("day", F.dayofmonth("valid_time_utc"))
    base = base.withColumn("hour", F.hour("valid_time_utc")).withColumn("day_of_week", F.dayofweek("valid_time_utc")).withColumn("is_weekend", F.dayofweek("valid_time_utc").isin(1, 7))
    # Exact duplicates collapse. Conflicts are flagged and excluded at their city-hour key.
    source_value_cols = [c for c in raw.columns if not c.startswith("_")]
    base = base.withColumn("_row_hash", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in source_value_cols]), 256))
    keys = ["region_id", "valid_time_utc"]
    conflicts = base.groupBy(*keys).agg(F.countDistinct("_row_hash").alias("_n_hash")).filter(F.col("_n_hash") > 1).select(*keys).withColumn("_conflict", F.lit(True))
    base = base.dropDuplicates(keys + ["_row_hash"]).join(conflicts, keys, "left")
    base = base.filter(F.col("_conflict").isNull()).drop("_conflict", "_row_hash")
    base = base.withColumn("is_estimate", F.lit(False)).withColumn("estimation_method", F.lit(None).cast("string"))
    base = base.withColumn("quality_flag", F.when((F.col("temperature_2m_c") < -30) | (F.col("temperature_2m_c") > 55) | (F.col("relative_humidity_pct") < 0) | (F.col("relative_humidity_pct") > 100) | (F.col("wind_speed_10m_mps") < 0) | (F.col("precipitation_mm") < 0) | (F.col("ghi_wm2") < 0) | (F.col("ghi_wm2") > 1500) | (F.col("cloud_cover_pct") < 0) | (F.col("cloud_cover_pct") > 100) | (F.col("dewpoint_c") > F.col("temperature_2m_c")), "outlier").otherwise("ok"))
    base = base.withColumn("gap_flag", F.when(F.col("temperature_2m_c").isNull() | F.col("precipitation_mm").isNull() | F.col("ghi_wm2").isNull(), "missing_fields").otherwise("none"))
    base = base.withColumn("source_dataset", F.lit("Indian_Weather_Dataset_1996-2026"))
    base = base.withColumn("source_url", F.lit(SOURCE_URL)).withColumn("source_license", F.lit("Refer to Kaggle dataset license; verify before redistribution"))
    base = base.withColumn("transformation_code_version", F.lit(VERSION)).withColumn("region_boundary_version", F.lit("city-coordinate-grid-0.25deg-v1"))
    base = base.withColumn("ingested_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())
    partition_cols = ["state_code", "year", "month", "region_id"]

    # Coordinate-derived city and grid cell regions; distinct city rows only.
    city_regions = base.groupBy("region_id", "state_code", "state_name", "city").agg(F.avg("latitude").alias("centroid_latitude"), F.avg("longitude").alias("centroid_longitude"))
    city_regions = city_regions.withColumn("region_type", F.lit("city")).withColumnRenamed("city", "region_name")
    city_regions = city_regions.withColumn("min_latitude", F.col("centroid_latitude")).withColumn("max_latitude", F.col("centroid_latitude")).withColumn("min_longitude", F.col("centroid_longitude")).withColumn("max_longitude", F.col("centroid_longitude"))
    city_regions = city_regions.withColumn("bbox_resolution_km", F.lit(0.0)).withColumn("population_weight", F.lit(None).cast("double")).withColumn("urban_share", F.lit(None).cast("double"))
    city_regions = city_regions.withColumn("elevation_m", F.lit(None).cast("double")).withColumn("coastal_flag", F.lit(None).cast("boolean"))
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

    # Canonical weather facts include explicit NULLs for source fields not supplied.
    w = Window.partitionBy("region_id").orderBy("valid_time_utc")
    weather = base.withColumn("specific_humidity_gkg", F.when(F.col("air_pressure_hpa").isNotNull() & F.col("relative_humidity_pct").isNotNull() & F.col("temperature_2m_c").isNotNull(),
        1000 * (0.622 * ((F.col("relative_humidity_pct") / 100) * 6.112 * F.exp(17.67 * F.col("temperature_2m_c") / (F.col("temperature_2m_c") + 243.5)))) / (F.col("air_pressure_hpa") - 0.378 * ((F.col("relative_humidity_pct") / 100) * 6.112 * F.exp(17.67 * F.col("temperature_2m_c") / (F.col("temperature_2m_c") + 243.5))))))
    weather = weather.withColumn("heat_index_c", F.lit(None).cast("double")).withColumn("apparent_temperature_c", F.lit(None).cast("double"))
    weather = weather.withColumn("wind_shear_exponent", F.lit(0.20)).withColumn("wind_speed_100m_mps", F.coalesce("wind_speed_100m_mps", F.col("wind_speed_10m_mps") * F.pow(10.0, F.col("wind_shear_exponent"))))
    weather = weather.withColumn("wind_speed_100m_method", F.when(~F.col("wind_100m_observed"), "power_law_estimate").otherwise("observed"))
    weather = weather.withColumn("precipitation_intensity_mmhr", F.col("precipitation_mm"))
    weather = weather.withColumn("air_density_kgm3", F.when(F.col("air_pressure_hpa").isNotNull() & F.col("temperature_2m_c").isNotNull(), F.col("air_pressure_hpa") * 100 / (287.05 * (F.col("temperature_2m_c") + 273.15))))
    weather = weather.withColumn("weather_source", F.lit("Kaggle source; per-row source field retained in raw file lineage")).withColumn("source_version", F.lit(VERSION))

    # Solar table: no clear-sky radiation available => clear-sky index stays NULL.
    solar = weather.select("region_id", *partition_cols[:3], "valid_time_utc", "day", "hour", "ghi_wm2", "dni_wm2", "dhi_wm2").withColumn("clear_sky_ghi_wm2", F.lit(None).cast("double"))
    solar = solar.withColumn("daylight_flag", F.col("ghi_wm2") > 0).withColumn("clear_sky_index", F.lit(None).cast("double"))
    solar = solar.join(weather.select("region_id", "valid_time_utc", "temperature_2m_c", "ghi_wm2"), ["region_id", "valid_time_utc", "ghi_wm2"], "left")
    solar = solar.withColumn("pv_cell_temperature_c", F.when(F.col("ghi_wm2").isNotNull() & F.col("temperature_2m_c").isNotNull(), F.col("temperature_2m_c") + F.col("ghi_wm2") / 800 * 25))
    derate = 1 + (-0.004) * (F.col("pv_cell_temperature_c") - 25)
    solar = solar.withColumn("pv_power_factor", derate).withColumn("pv_capacity_factor", F.when(~F.col("daylight_flag"), 0.0).when(F.col("ghi_wm2").isNotNull(), F.least(1.0, F.greatest(0.0, F.col("ghi_wm2") / 1000 * derate * 0.8))))
    solar = solar.withColumn("solar_generation_index", F.col("pv_capacity_factor"))
    solar = solar.withColumn("_prev", F.lag("pv_capacity_factor").over(w)).withColumn("solar_ramp_mw_per_mw_per_hour", F.col("pv_capacity_factor") - F.col("_prev"))
    solar = solar.withColumn("solar_variability_index", F.abs("solar_ramp_mw_per_mw_per_hour")).withColumn("cloud_cover_pct", F.lit(None).cast("double")).withColumn("data_quality_flag", F.lit("clear_sky_reference_unavailable" )).drop("_prev", "temperature_2m_c")

    # Wind table: 100m estimate marked explicitly; generic power curve is an analytical proxy.
    wind_table = weather.select("region_id", "state_code", "year", "month", "day", "hour", "valid_time_utc", "wind_speed_10m_mps", "wind_speed_100m_mps", "wind_direction_10m_deg", "wind_direction_100m_deg", "wind_u_10m_mps", "wind_v_10m_mps", "temperature_2m_c", "air_pressure_hpa", "air_density_kgm3", "wind_speed_100m_method")
    wind_table = wind_table.withColumn("wind_speed_hub_mps", F.col("wind_speed_100m_mps")).withColumn("wind_u_100m_mps", F.lit(None).cast("double")).withColumn("wind_v_100m_mps", F.lit(None).cast("double"))
    wind_table = wind_table.withColumn("wind_power_density_wm2", 0.5 * F.col("air_density_kgm3") * F.pow("wind_speed_hub_mps", 3))
    vhub = F.col("wind_speed_hub_mps")
    curve = F.when(vhub < 3, 0.0).when(vhub < 12, (F.pow(vhub, 3) - 27) / (1728 - 27)).when(vhub < 25, 1.0).otherwise(0.0)
    wind_table = wind_table.withColumn("turbine_power_factor", curve).withColumn("wind_capacity_factor", F.least(1.0, F.greatest(0.0, curve * F.col("air_density_kgm3") / 1.225)))
    wind_table = wind_table.withColumn("estimated_wind_generation_index", F.col("wind_capacity_factor")).withColumn("cut_in_flag", vhub < 3).withColumn("rated_flag", (vhub >= 12) & (vhub < 25)).withColumn("cut_out_flag", vhub >= 25)
    wind_table = wind_table.withColumn("_prev", F.lag("wind_capacity_factor").over(w)).withColumn("wind_ramp_mw_per_mw_per_hour", F.col("wind_capacity_factor") - F.col("_prev"))
    wind_table = wind_table.withColumn("turbulence_proxy", F.lit(None).cast("double")).withColumn("data_quality_flag", F.when(F.col("wind_speed_100m_method") == "power_law_estimate", "estimated_hub_height_wind").otherwise("generic_power_curve")).drop("_prev")
    wind_table = wind_table.withColumnRenamed("temperature_2m_c", "air_temperature_c")

    # Rain table: rolling accumulations and calendar monsoon only; no soil-moisture/hydrologic claims.
    rain = weather.select("region_id", "state_code", "year", "month", "day", "hour", "valid_time_utc", "precipitation_mm", "precipitation_intensity_mmhr")
    wr = Window.partitionBy("region_id").orderBy("valid_time_utc")
    for n, label in [(3, "3h"), (6, "6h"), (24, "24h"), (72, "72h")]: rain = rain.withColumn(f"precipitation_{label}_mm", F.sum("precipitation_mm").over(wr.rowsBetween(-n + 1, 0)))
    wet = F.coalesce(F.col("precipitation_mm"), F.lit(0.0)) >= 0.1
    rain = rain.withColumn("_dry_start", F.sum(F.when(wet, 1).otherwise(0)).over(wr)).withColumn("_wet_start", F.sum(F.when(~wet, 1).otherwise(0)).over(wr))
    runw = Window.partitionBy("region_id", "_dry_start").orderBy("valid_time_utc")
    rain = rain.withColumn("consecutive_dry_hours", F.when(~wet, F.row_number().over(runw)).otherwise(0)).withColumn("consecutive_wet_hours", F.when(wet, F.row_number().over(Window.partitionBy("region_id", "_wet_start").orderBy("valid_time_utc"))).otherwise(0))
    rain = rain.withColumn("daily_precipitation_mm", F.sum("precipitation_mm").over(Window.partitionBy("region_id", "year", "month", "day")))
    rain = rain.withColumn("monthly_precipitation_mm", F.sum("precipitation_mm").over(Window.partitionBy("region_id", "year", "month")))
    rain = rain.withColumn("monsoon_flag", F.col("month").between(6, 9)).withColumn("flood_risk_index", F.least(1.0, (0.45 * F.col("precipitation_24h_mm") + 0.35 * F.col("precipitation_72h_mm") + 0.20 * F.col("precipitation_intensity_mmhr")) / 500))
    rain = rain.withColumn("drought_risk_index", F.lit(None).cast("double")).withColumn("hydro_inflow_proxy", F.when(F.col("precipitation_24h_mm").isNotNull(), F.least(1.0, F.col("precipitation_24h_mm") / 250)))
    rain = rain.withColumn("data_quality_flag", F.lit("rainfall_only_proxy_soil_moisture_unavailable")).drop("_dry_start", "_wet_start")

    # Demand-weather indices are proxies. Region/month empirical normalization is bounded [0,1].
    dem = weather.select("region_id", "state_code", "year", "month", "day", "hour", "valid_time_utc", "temperature_2m_c", "apparent_temperature_c", "heat_index_c", "relative_humidity_pct", "dewpoint_c", "wind_speed_10m_mps", "ghi_wm2", "cloud_cover_pct", "precipitation_mm")
    dem = dem.withColumn("cooling_degree_hour", F.greatest(0.0, F.col("temperature_2m_c") - 24)).withColumn("heating_degree_hour", F.greatest(0.0, 18 - F.col("temperature_2m_c")))
    dem = dem.withColumn("humidity_discomfort_index", F.when(F.col("relative_humidity_pct").isNotNull(), F.col("relative_humidity_pct") / 100))
    wn = Window.partitionBy("state_code", "region_id", "month")
    dem = dem.withColumn("_cool_norm", normalized(F.col("cooling_degree_hour"), wn)).withColumn("_heat_norm", normalized(F.col("heating_degree_hour"), wn))
    dem = dem.withColumn("cooling_load_index", 0.7 * F.coalesce("_cool_norm", F.lit(0.0)) + 0.3 * F.coalesce("humidity_discomfort_index", F.lit(0.0)))
    dem = dem.withColumn("heating_load_index", F.coalesce("_heat_norm", F.lit(0.0))).withColumn("wind_chill_index", F.lit(None).cast("double"))
    dem = dem.withColumn("daylight_flag", F.col("ghi_wm2") > 0).withColumn("solar_radiation_wm2", F.col("ghi_wm2"))
    dem = dem.withColumn("demand_weather_index", F.least(1.0, 0.60 * F.col("cooling_load_index") + 0.15 * F.col("heating_load_index") + 0.10 * F.coalesce(F.col("ghi_wm2") / 1000, F.lit(0.0)) + 0.15 * F.coalesce(F.col("precipitation_mm") / 50, F.lit(0.0))))
    p80 = F.percentile_approx("demand_weather_index", 0.8).over(wn)
    dem = dem.withColumn("peak_demand_risk_flag", (F.col("demand_weather_index") >= p80) | (F.col("cooling_degree_hour") >= 6) | (F.col("heat_index_c") >= 40))
    dem = dem.withColumn("weather_stress_index", F.col("demand_weather_index")).withColumn("precipitation_disruption_index", F.lit(None).cast("double")).withColumn("wind_discomfort_index", F.lit(None).cast("double")).withColumn("data_quality_flag", F.lit("weather_driven_proxy_not_load_forecast" )).drop("_cool_norm", "_heat_norm")

    # Factor contribution rows with region-season empirical normalization; rule-based, not causal.
    factors = [("temperature", "temperature_2m_c", "increases_demand", 0.40), ("humidity", "relative_humidity_pct", "increases_demand", 0.20), ("solar_radiation", "ghi_wm2", "increases_solar_supply", 0.15), ("cloud_cover", "cloud_cover_pct", "decreases_solar_supply", 0.10), ("wind_speed", "wind_speed_10m_mps", "increases_wind_supply", 0.10), ("precipitation", "precipitation_mm", "increases_supply_risk", 0.05)]
    fact_frames = []
    for factor, col, direction, weight in factors:
        fw = Window.partitionBy("state_code", "region_id", "month", "hour")
        one = weather.select("region_id", "state_code", "valid_time_utc", "year", "month", "day", "hour", F.lit(factor).alias("factor_name"), F.col(col).alias("factor_value"))
        one = one.withColumn("normalized_contribution", normalized(F.col("factor_value"), fw)).withColumn("factor_direction", F.lit(direction))
        one = one.withColumn("raw_contribution", F.col("normalized_contribution") * weight).withColumn("contribution_to_demand", F.when(direction == "increases_demand", F.col("raw_contribution")).otherwise(0.0))
        one = one.withColumn("contribution_to_solar_supply", F.when(direction == "increases_solar_supply", F.col("raw_contribution")).when(direction == "decreases_solar_supply", -F.col("raw_contribution")).otherwise(0.0))
        one = one.withColumn("contribution_to_wind_supply", F.when(direction == "increases_wind_supply", F.col("raw_contribution")).otherwise(0.0)).withColumn("contribution_to_hydro_supply", F.lit(0.0))
        one = one.withColumn("contribution_to_supply_risk", F.when(direction == "increases_supply_risk", F.col("raw_contribution")).otherwise(0.0)).withColumn("method", F.lit("rule_based_normalized_contribution"))
        one = one.withColumn("confidence", F.when(F.col("factor_value").isNotNull(), 0.4).otherwise(0.0)).withColumn("data_quality_flag", F.when(F.col("factor_value").isNull(), "source_field_unavailable").otherwise("proxy_not_causal"))
        fact_frames.append(one)
    factors_df = fact_frames[0]
    for one in fact_frames[1:]: factors_df = factors_df.unionByName(one)
    state_factors = factors_df.groupBy("state_code", "valid_time_utc", "year", "month", "day", "hour", "factor_name").agg(
        F.avg("factor_value").alias("state_factor_value"), F.avg("normalized_contribution").alias("state_normalized_factor"), F.avg("contribution_to_demand").alias("state_contribution_to_demand"),
        F.avg("contribution_to_solar_supply").alias("state_contribution_to_solar_supply"), F.avg("contribution_to_wind_supply").alias("state_contribution_to_wind_supply"),
        F.avg("contribution_to_hydro_supply").alias("state_contribution_to_hydro_supply"), F.avg("contribution_to_supply_risk").alias("state_contribution_to_supply_risk"),
        F.countDistinct("region_id").alias("contributing_region_count"), F.avg("confidence").alias("confidence"))
    state_factors = state_factors.withColumn("state_name", F.when(F.col("state_code") == "KA", "Karnataka").when(F.col("state_code") == "MH", "Maharashtra").otherwise("Tripura"))
    state_factors = state_factors.withColumn("weight_method", F.lit("equal_city; population data unavailable")).withColumn("data_quality_flag", F.lit("state_city_composite_not_statewide_observation"))

    # Basic column-level coverage report and machine-readable metadata catalogue.
    quality_cols = ["temperature_2m_c", "relative_humidity_pct", "precipitation_mm", "ghi_wm2", "wind_speed_10m_mps", "cloud_cover_pct"]
    qframes = []
    for col in quality_cols:
        q = base.groupBy("region_id", "state_code").agg(F.min("valid_time_utc").alias("period_start"), F.max("valid_time_utc").alias("period_end"), F.count("*").alias("observed_hour_count"), F.sum(F.col(col).isNull().cast("int")).alias("missing_hour_count"), F.avg(col).alias("mean_value"), F.expr(f"percentile_approx({col}, 0.5)").alias("median_value"), F.min(col).alias("min_value"), F.max(col).alias("max_value"))
        q = q.withColumn("variable", F.lit(col)).withColumn("expected_hour_count", F.lit(None).cast("long")).withColumn("missing_pct", F.when(F.col("observed_hour_count") > 0, 100 * F.col("missing_hour_count") / F.col("observed_hour_count")))
        q = q.withColumn("duplicate_count", F.lit(None).cast("long")).withColumn("conflict_count", F.lit(None).cast("long")).withColumn("outlier_count", F.lit(None).cast("long")).withColumn("interpolated_count", F.lit(0)).withColumn("estimated_count", F.lit(None).cast("long"))
        q = q.withColumn("gap_reasons", F.lit("source field absent or null; expected-hour continuity not inferred without complete calendar metadata"))
        qframes.append(q)
    quality = qframes[0]
    for q in qframes[1:]: quality = quality.unionByName(q)
    metadata_rows = [("weather_hourly", c, "source mapped", unit_guess(c), "Canonical hourly weather field; unavailable fields remain NULL", "source mapping or documented formula", c, "Kaggle dataset", "No imputation", "Dataset license applies", "See limitations.md") for c in weather.columns]
    metadata = spark.createDataFrame(metadata_rows, ["table_name", "column_name", "data_type", "unit", "description", "formula", "source_field", "source_dataset", "transformation_rule", "license", "limitations"])
    metadata = metadata.unionByName(spark.createDataFrame([(n, "*", "parquet", "varies", f"Derived {n} output", "Documented formulas in pipeline README", None, "Kaggle dataset", VERSION, "Dataset license applies", "See limitations.md") for n in ["regions", "solar_table", "wind_table", "rain_table", "demand_weather_table", "factor_contribution_table", "state_factor_contribution_table", "data_quality_report"]], metadata.schema))

    outputs = {"regions": regions, "weather_hourly": weather, "solar_table": solar, "wind_table": wind_table, "rain_table": rain, "demand_weather_table": dem, "factor_contribution_table": factors_df, "state_factor_contribution_table": state_factors, "data_quality_report": quality, "metadata_catalog": metadata}
    # Preview and validation summary are materialized before curated writes for user review/audit.
    preview = base.orderBy("valid_time_utc", "region_id").limit(20).toJSON().collect()
    (audit_dir / "first_20_cleaned_rows.json").write_text("[\n" + ",\n".join(json.dumps(json.loads(row), indent=2, default=str) for row in preview) + "\n]", encoding="utf-8")
    summary = {"source_files": [{"name": p.name, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in files], "rows_after_state_filter": base.count(), "regions_by_state": [r.asDict() for r in base.groupBy("state_code", "state_name").agg(F.countDistinct("region_id").alias("city_count")).collect()], "validated_at_utc": datetime.now(timezone.utc).isoformat(), "assumptions": ["timestamps interpreted as UTC unless source metadata establishes another zone", "source field units require verification; no generic unit conversion is inferred", "population weights and elevation are unavailable; equal-city rollups and missing pressure estimates are documented", "this local input contained only files discovered under the selected input directory"]}
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
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = SparkSession.builder.appName("IndianWeatherFeatureWarehouse").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try: run(spark, args.input.resolve(), args.output.resolve(), args.inspect_only)
    finally: spark.stop()


if __name__ == "__main__":
    main()
