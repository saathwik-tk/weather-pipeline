# Processing pipeline architecture

```text
Local immutable CSV/Parquet inputs
              |
              v
PySpark discovery + raw schema snapshot (output/_audit)
              |
              v
Header normalization -> state/time filtering -> mappings and documented derivations
              |
              v
Duplicate/conflict and range-quality flags -> cleaned preview + assumptions report
              |
              v
regions + region_coordinates | weather_hourly | solar | wind | rain | demand proxies
              |
              v
factor contributions | state composites | gap log | quality + metadata catalog
              |
              v
Partitioned Parquet: state_code/year/month/region_id
```

The processing CLI has an inspection-only mode so a schema snapshot can be
reviewed before transformation. Transformations are deterministic for fixed
input files and code. Input file names and SHA-256 checksums are recorded in
the audit summary. A change in source timezone, unit convention, or aliases
must be reflected in the mapping and pipeline version.
