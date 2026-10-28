-- Run from Spark SQL after registering each Parquet directory as a table.
-- Example: CREATE OR REPLACE TEMP VIEW weather_hourly AS SELECT * FROM parquet.`output/weather_hourly`;

-- Latest available weather per city region.
SELECT region_id, state_code, valid_time_utc, temperature_2m_c,
       relative_humidity_pct, precipitation_mm, ghi_wm2
FROM weather_hourly
WHERE state_code IN ('KA', 'MH', 'TR')
ORDER BY valid_time_utc DESC, state_code, region_id
LIMIT 100;

-- Monthly mean solar proxy by state (city composite, not a state-wide observation).
SELECT state_code, year, month, AVG(pv_capacity_factor) AS mean_pv_capacity_factor
FROM solar_table
GROUP BY state_code, year, month
ORDER BY year, month, state_code;

-- Hours flagged for high weather-driven demand risk.
SELECT state_code, region_id, valid_time_utc, demand_weather_index
FROM demand_weather_table
WHERE peak_demand_risk_flag = true
ORDER BY valid_time_utc, state_code, region_id;

-- Rainfall and its 24-hour accumulation.
SELECT state_code, region_id, valid_time_utc, precipitation_mm, precipitation_24h_mm
FROM rain_table
WHERE precipitation_24h_mm >= 100
ORDER BY precipitation_24h_mm DESC;

-- Missing source values, absent hours, and excluded conflicting duplicates.
SELECT state_code, region_id, valid_time_utc, variable, gap_reason, gap_duration_hours
FROM gap_log
ORDER BY valid_time_utc, state_code, region_id, variable;
