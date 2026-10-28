

# Weather and solar extraction

This folder contains the current-data collectors and the existing historical
collector. Historical extraction remains as designed; see
[historical/README.md](historical/README.md) for its date-range command and
behavior.

## Current data

Run from the repository root:

```powershell
python main.py
```

This runs all configured providers for Mumbai, Bengaluru, and Agartala. To run
one provider, pass `--provider nasa_power`, `--provider open_meteo`, or
`--provider openweather_solar`. Schedule the command hourly in production.
The pipeline now stores both solar and atmospheric data in date partitions under
`data/<provider>/`.

The extraction code is intentionally coordinate-driven. Each location is defined
with a city, state, latitude, and longitude, so the same pattern scales to any
additional grid point or state-level coordinate list without changing the
provider logic.

### Data types collected

- Solar radiation: actual/current irradiation values from each provider, where the API exposes them.
- Live weather: actual or near-real-time atmospheric conditions, including temperature, humidity, pressure, rainfall, and wind.
- Forecast weather: hourly weather forecast rows for the next 48 hours using the Open-Meteo forecast endpoint.

### Output layout

```text
data/
  nasa_power/
    solar/year=YYYY/month=MM/hourly_solar_radiation.csv
    weather/year=YYYY/month=MM/hourly_weather.csv
  open_meteo/
    solar/year=YYYY/month=MM/hourly_solar_radiation.csv
    weather/year=YYYY/month=MM/hourly_weather.csv
    weather_forecast/year=YYYY/month=MM/hourly_weather_forecast.csv
  openweather_solar/
    solar/year=YYYY/month=MM/hourly_solar_radiation.csv
    weather/year=YYYY/month=MM/hourly_weather.csv
    weather_forecast/year=YYYY/month=MM/hourly_weather_forecast.csv
```

| Provider | Current-data source | Forecast source | API cost / access |
| --- | --- | --- | --- |
| NASA POWER | Hourly satellite-derived solar observations and atmospheric variables from the point API; current collection revisits a rolling window to account for source delay. | Not provided by NASA POWER; the project uses its live weather values when available. | **Free; no API key.** Data is delayed, typically several days. [NASA POWER hourly API](https://power.larc.nasa.gov/docs/services/api/temporal/hourly/) |
| Open-Meteo | Current solar model values and live atmospheric variables from the forecast endpoint. | Hourly weather forecast for the next 48 hours from the Open-Meteo API. | **Free for non-commercial use; no API key.** Usage limits apply. [Open-Meteo docs](https://open-meteo.com/en/docs) |
| OpenWeather Solar Irradiance | Current solar irradiance measures and live weather metrics from OpenWeather's APIs. | Short-range hourly forecast from the OpenWeather forecast/weather API. | **Paid for solar product access** and requires `OPENWEATHER_API_KEY`; weather data also depends on API access and usage policy. [OpenWeather Solar Irradiance](https://openweathermap.org/api/solar-irradiance) · [Pricing](https://openweathermap.org/price) |

Provider columns and units remain source-specific. For the full extraction
behavior, timing, and coordinate model, see [current/README.md](current/README.md).

## Scaling up for state-level coverage

To extend this pipeline beyond the current city set, the extraction layer should
be expanded around a coordinate-first design:

- enumerate all relevant latitude/longitude points for the target states
- fetch data asynchronously per coordinate and provider
- group payloads by source, state, and timestamp
- push the extracted records into a durable messaging layer such as a Kafka topic
- let the ingestion layer consume and persist them in the warehouse or storage
  partitioning model

This keeps the extraction process decoupled from downstream storage and makes it
scalable to statewide or region-wide coverage without changing the core provider
logic.

A practical production pattern is:

1. build a coordinate registry for each state
2. fan out provider requests asynchronously
3. normalize payloads into a common event schema
4. publish to Kafka for ingestion and storage
5. retain the same `data/<provider>/...` partition layout for local or batch
   processing

The ingestion layer will handle downstream consumption, validation, and
persistence of the queued records.