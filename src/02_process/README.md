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

* `regions/` — deterministic city regions and 0.25° cells containing source
  city coordinates.
* `weather_hourly/` — canonical hourly weather, retaining source rows and
  derived fields.
* `solar_table/`, `wind_table/`, `rain_table/`, `demand_weather_table/` — hourly
  resource and demand proxies.
* `factor_contribution_table/` and `state_factor_contribution_table/` —
  normalized rule-based contributions and equal-city rollups.
* `data_quality_report/` and `metadata_catalog/` — availability metrics,
  mappings, units, and limitations.
* `sample_queries.sql`, `pipeline_architecture.md`, and `limitations.md` —
  companion documentation.

Every Parquet dataset is partitioned by `state_code`, `year`, `month`, and
`region_id`. The state rollups are composites of included cities; they do not
represent complete state coverage.

## Mapping and transformation policy

Source headers are normalized to snake_case. `main.py` contains the explicit
`ALIASES` map; add source-specific aliases there when the Kaggle schema is
known. Common Open-Meteo archive field names are recognized. Fields absent from
the source remain NULL. Temperature and radiation units are not guessed from
numeric values; verify them from the source metadata before relying on those
outputs. Pressure values above 2000 are treated as Pa and converted to hPa.
Timestamps are treated as UTC by default; the source timezone must be confirmed
from dataset metadata. `valid_time_ist` is UTC plus 05:30.

Documented derivations include dewpoint/RH via Magnus, wind components from
meteorological direction, humidity, air density, 100 m wind via the stated
power law, generic turbine curve, PV cell temperature and derating, rolling
rainfall, degree hours, and empirical percentile normalization. Wind estimates
are tagged. DNI/DHI, clear-sky GHI, heat index, wind chill, soil moisture,
population weights, elevation, and other unsupported measurements are not
invented. Some requested scores therefore remain NULL or are explicitly
rainfall-only / rule-based proxies. See [limitations.md](limitations.md).

The full schema snapshot includes column type, null and unique counts, sample
values, min/max, and a deliberately tentative unit hint. Use it together with
the actual data dictionary before production use.
