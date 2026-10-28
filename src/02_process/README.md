# PySpark weather processing

This stage reads local Kaggle CSV/Parquet files without changing them and
creates a partitioned Parquet warehouse for Karnataka (`KA`), Maharashtra
(`MH`), and Tripura (`TR`). It does not download the Kaggle dataset. Put the
downloaded and extracted files under `data/raw/`, or pass another directory.
For a large archive, filter the desired date range and states first with the
streaming helper, which does not load the CSV into memory:

```powershell
python src/02_process/filter_recent.py --input D:\Engineer\arborum-labs\Indian_Weather_Dataset.csv --output data/raw/indian_weather_recent.csv --start-date 2024-03-18 --end-date 2026-03-18
```

## Run

Install dependencies and inspect the raw source first:

```powershell
python -m pip install -r requirements.txt
python src/02_process/main.py --input data/raw --inspect-only
```

The inspection writes `output/_audit/raw_schema_snapshot.json`, prints up to 20
raw rows, and creates no curated tables. Review the inferred schema and units
against the Kaggle file documentation. Then run processing:

```powershell
python src/02_process/main.py --input data/raw --output output
```

The processing run writes the schema snapshot, a cleaned-row preview, and a
validation/assumption summary under `output/_audit/` before writing the curated
tables. The preview and summary are also printed/logged where appropriate.
No data is fetched from the network and no source files are modified.

## Outputs

The run creates Parquet datasets under `output/`:

* `regions/` and `region_coordinates/` — deterministic city regions, observed
  0.25° grid cells, city-to-grid coordinate mapping, and centroid distances.
  `resolve_regions(points_df, regions_df, max_distance_km=25)` resolves a point
  by bbox first and then nearest centroid within the configured radius.
* `weather_hourly/` — canonical hourly weather, retaining source rows and
  derived fields.
* `solar_table/`, `wind_table/`, `rain_table/`, `demand_weather_table/` — hourly
  resource and demand proxies.
* `factor_contribution_table/` and `state_factor_contribution_table/` —
  normalized rule-based contributions and equal-city rollups.
* `data_quality_report/`, `gap_log/`, and `metadata_catalog/` — availability,
  missing-hour/variable gaps, mappings, formulas, units, lineage, and limits.
* `sample_queries.sql`, `pipeline_architecture.md`, and `limitations.md` —
  companion documentation.

Every Parquet dataset is partitioned by `state_code`, `year`, `month`, and
`region_id`. The state rollups are composites of included cities; they do not
represent complete state coverage.

## Mapping and transformation policy

Source headers are normalized to snake_case. `ALIASES` includes the delivered
CSV fields (`temperature_C`, `humidity_pct`, `pressure_hPa`, `dew_point_C`,
`solar_radiation_Wm2`, `wind_speed_ms`, `cloud_cover_pct`, `precip_mm`, and
`wind_direction_deg`) and common Open-Meteo fields. Units with explicit source
suffixes are mapped directly; pressure above 2000 is treated as Pa. `ssrd` is
converted from accumulated J/m² by hourly differencing and `tp` is differenced
as accumulated metres and converted to millimetres. No generic numeric-value
unit guessing is used.

The transformations implement the prompt's Magnus dewpoint/RH and specific
humidity equations; meteorological wind components; 100 m power-law wind
estimate; air density; Rothfusz heat index and conditional wind chill; PV NOCT,
temperature derating, capacity, ramps and variability; generic wind power
curve, density adjustment, ramps and cut-in/rated/cut-out flags; time-based
rainfall windows, dry/wet streaks, calendar monsoon, normalized flood/drought
and rainfall-only hydro proxies; cooling/heating degree hours, discomfort,
weather stress, demand index and peak-risk flag; and local seasonal normalized
factor contributions with equal-city state rollups. Each formula and its
parameters are also written to `metadata_catalog/`.

Unsupported inputs remain NULL: clear-sky GHI/solar elevation, DNI/DHI when not
provided, elevation, population/activity, soil moisture, turbulence, and actual
electricity demand/generation. Estimated 100 m wind is tagged with its method.
GHI is not plane-of-array irradiance; output indexes are proxies, not forecasts
or causal coefficients. Source timezone defaults to UTC and is recorded; verify
that convention against the source before production. `valid_time_ist` is UTC
plus 05:30. See [limitations.md](limitations.md).

The full schema snapshot includes column type, null and unique counts, sample
values, min/max, and a deliberately tentative unit hint. Use it together with
the actual data dictionary before production use.
