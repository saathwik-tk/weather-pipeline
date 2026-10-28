# Processing limitations and assumptions

* Source timezone is interpreted as UTC unless source documentation confirms
  another basis. This must be checked before treating hourly continuity or IST
  conversion as authoritative.
* The schema aliases are an initial compatibility map. Inspect the generated
  raw snapshot and extend mappings to match actual Kaggle field names.
* Units are not reliably inferable from values. Temperature and solar
  radiation are retained numerically; accumulated J/m² radiation is not
  differenced without identifying that source convention. Confirm source units.
* Conflicting city-hour rows are excluded because ingestion/version precedence
  is not available. Exact duplicate values collapse. The source does not have
  an ingestion version unless supplied by input.
* No interpolation or forward-fill is applied. Gaps remain visible; the
  per-variable report records null counts. Full expected-hour continuity
  metrics require a defined source coverage calendar.
* The initial range checks flag physically implausible values but do not
  silently delete or repair them. Range and missing-data reports should be
  expanded after the actual source schema is confirmed.
* Grid cells are produced only for observed city coordinates. They do not
  imply full state coverage or spatially interpolated weather. City and grid
  region identification is deterministic; no districts or boundaries are
  inferred.
* Population weights, elevation, urban share, clear-sky irradiance, soil
  moisture, and actual load/generation are not present in the expected inputs.
  State rollups use equal city weights. Rainfall is not reservoir inflow.
* PV and wind outputs are generic feasibility proxies. GHI is not plane-of-
  array irradiance; the generic turbine curve is not a manufacturer's curve.
  100 m wind extrapolation uses an explicitly estimated 0.20 shear exponent
  unless observed 100 m wind is available.
* The requested heat-index, apparent-temperature, wind-chill and some
  precipitation-disruption outputs remain NULL unless source fields/formulas
  are added with verified requirements. Rule-based contributions are not
  causal estimates. Demand-weather values are not electricity forecasts.
* Confirm the Kaggle dataset's license before redistributing source or derived
  data. The metadata catalog intentionally calls out that the license must be
  verified.
