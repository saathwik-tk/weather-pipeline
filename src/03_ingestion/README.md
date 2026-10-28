# Gold ingestion and dashboard serving

This stage reads provider-shaped Bronze CSV files and the Silver `regions`
Parquet dataset, then writes dashboard-ready Parquet tables and materialized
views under `gold/`. It does not fetch from provider APIs.

## Run

First run extraction and Silver processing, then publish Gold:

```powershell
python src/03_ingestion/main.py --bronze data --silver output --output gold
```

By default, `coordinate_queries` is built from the city and observed grid
regions available in Silver. To serve user-selected coordinates (including
forest and other non-city points), pass a CSV with `latitude`, `longitude`,
and `state_code` (`KA`, `MH`, or `TR`). Optional fields are `coordinate_id`
and `label`:

```powershell
python src/03_ingestion/main.py --coordinates data/coordinate_queries.csv --max-distance-km 25
```

Coordinates are accepted independently of city membership. Source data is
matched to the nearest extracted source coordinate within the distance limit.
The result records the match method and distance. If a source has no extracted
point within that limit, coverage is emitted with `outside_match_radius` and
no weather value is fabricated. To populate forest coordinates with values,
the extractors must also collect those coordinates or a gridded source covering
them; Gold does not create a spatial observation from a distant city.

## Outputs

| Gold dataset | Contents |
| --- | --- |
| `coordinate_queries` | Requested or Silver-derived dashboard coordinates. |
| `source_catalog` | Provider/product/dataset identity and source metadata. |
| `coordinate_weather_series` | Long-form weather/solar measure series, retaining provider, dataset, scenario, time, match distance and lineage. |
| `forecast_verification` | Forecast vintages matched to available actual reference sources by coordinate, compatible unit, measure and valid time; includes signed, absolute, squared and percentage errors and both source IDs. |
| `weather_confidence` | Heuristic score with source, temporal, spatial and measure-availability components and reason. It is not a calibrated probability. |
| `mv_coordinate_weather_hourly` | Chart-ready source-specific hourly values with UTC and IST timestamps. |
| `mv_source_comparison_hourly` | Source-specific values plus per-coordinate/time/measure peer count, min, max and median. Provider rows remain separate. |
| `mv_forecast_skill_daily` | Matched count, MAE, RMSE, bias and MAPE by coordinate, provider, measure, lead-time bucket and date. |
| `mv_coordinate_coverage` | Per coordinate/source/measure nearest match, distance, latest available times, record count and coverage status. |

All outputs are Parquet directories. Re-running replaces each Gold dataset.
Measures with source-specific units are labeled conservatively; convert or
filter units before comparing providers if their source conventions differ.

## Forecast vintage retention

Forecast CSV upserts retain `forecast_issued_at_utc` in the natural key along
with city and valid timestamp. This preserves successive runs for future
verification. Existing forecast CSV rows created before this change have no
issue timestamp and cannot be reconstructed as historical vintages. The
current Silver processor still creates a canonical weather series and does
not retain provider-specific forecast facts, so this Gold stage reads forecast
vintages directly from Bronze.
