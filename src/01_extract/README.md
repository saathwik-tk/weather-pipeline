# Solar radiation extraction

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
Current collectors write/upsert CSV rows by city and UTC hour under
`data/<provider>/year=YYYY/month=MM/hourly_solar_radiation.csv`.

| Provider | Current-data source | API cost / access |
| --- | --- | --- |
| NASA POWER | Hourly satellite-derived solar observations; current collection revisits a rolling window to account for source delay. | **Free; no API key.** Data is delayed, typically several days. [NASA POWER hourly API](https://power.larc.nasa.gov/docs/services/api/temporal/hourly/) |
| Open-Meteo | Hourly solar estimates from the Forecast API weather models; these are not satellite observations. | **Free for non-commercial use; no API key.** Usage limits apply. The Satellite Radiation API is a separate offering and should not be assumed to be free. [Open-Meteo pricing](https://open-meteo.com/en/pricing) |
| OpenWeather Solar Irradiance | Current solar irradiance measures from OpenWeather's solar product. | **Paid: $0.12 per API call** at the listed rate, and requires product access plus `OPENWEATHER_API_KEY`. The collector makes one request per city (three calls per run, about $0.36 per run). [OpenWeather Solar Irradiance](https://openweathermap.org/api/solar-irradiance) · [Pricing](https://openweathermap.org/price) |

Provider columns and units remain source-specific. The current collector
README has additional field and scheduling details:
[current/README.md](current/README.md).
