# Processing limitations and assumptions

* Source timezone is interpreted as UTC unless source documentation confirms
  another basis. This must be checked before treating hourly continuity or IST
  conversion as authoritative.
* The schema aliases are an initial compatibility map. Inspect the generated
  raw snapshot and extend mappings to match actual Kaggle field names.
* The delivered CSV advertises units in the field names. The code maps those
  units and known accumulation conventions; new/unlabeled source fields need
  explicit mapping updates. It does not infer units from numeric ranges.
* Conflicting city-hour rows are excluded because ingestion/version precedence
  is not available. Exact duplicate values collapse. The source does not have
  an ingestion version unless supplied by input.
* No interpolation or forward-fill is applied. Gaps remain visible in
  `gap_log/`; the report records expected hours, missingness, duplicates,
  conflicts, outliers, and source-derived estimates. Interpolation is optional
  in the supplied spec and was intentionally not used.
* Physically impossible values are flagged and retained for audit. The job
  reports checks for temperature, humidity, dewpoint, wind, precipitation,
  GHI, and cloud-cover limits.
* Grid cells are produced only for observed city coordinates. They do not
  imply full state coverage or spatially interpolated weather. City and grid
  region identification is deterministic; no districts or boundaries are
  inferred.
* Population weights, elevation, urban share, clear-sky irradiance, solar
  elevation, soil moisture, and actual load/generation are not present in this
  input. State rollups use equal city weights. Rainfall is not reservoir inflow.
* PV and wind outputs are generic feasibility proxies. GHI is not plane-of-
  array irradiance; the generic turbine curve is not a manufacturer's curve.
  100 m wind extrapolation uses an explicitly estimated 0.20 shear exponent
  unless observed 100 m wind is available.
* Heat index and wind chill are calculated only under their stated validity
  conditions. Apparent temperature remains NULL unless supplied. Clear-sky
  index remains NULL without clear-sky GHI. Rule-based contributions are not
  causal estimates; demand-weather values are not electricity forecasts.
* Demand scores use a daylight indicator as the available calendar/activity
  proxy because no population-by-hour profile is included. The 0.10 calendar
  weight is therefore illustrative, not measured activity.
* The three state composites contain only the cities present in the filtered
  file. Karnataka's coastal/interior variation, Maharashtra's coastal/inland
  variation, and Tripura's hilly terrain are not spatially resolved by city
  observations. Treat city wind as especially weak evidence for wind-farm
  siting and Tripura weather as especially weak evidence for whole-state
  representation.
* The default wind shear exponent is 0.20 for mixed/urban terrain. The CLI
  allows 0.14 (open), 0.20 (mixed/urban), or 0.25 (complex terrain) to be
  selected explicitly. Turbine hub height, power curve cut-in/rated/cut-out,
  PV NOCT, temperature coefficient, and system performance ratio are also
  configurable; defaults are generic assumptions rather than plant data.
* The rainfall deficit baseline uses the same region and calendar month's mean
  hourly precipitation because no independent climatology is supplied. The
  resulting drought and flood values are relative screening proxies.
* Confirm the Kaggle dataset's license before redistributing source or derived
  data. The metadata catalog intentionally calls out that the license must be
  verified.
