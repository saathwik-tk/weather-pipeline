# Current weather and solar extraction

The current orchestrator supports NASA POWER, Open-Meteo, and OpenWeather for
Mumbai, Bengaluru, and Agartala. Each source writes its own date-partitioned
CSVs under `data/<provider>/`, and each row is keyed by city and UTC timestamp
for safe upserts.

The location model is coordinate-driven. A location is defined by city,
state, latitude, and longitude. To scale beyond the current three cities, add
more coordinates to the `LOCATIONS` list so the pipeline can work for any point
inside Maharashtra, Karnataka, or Tripura or for other state-level coordinate
sets.

## Run once

From the repository root, run all configured sources (OpenWeather is skipped
when its key is not set):

```powershell
python main.py
```

Choose one source explicitly:

```powershell
python main.py --provider nasa_power
python main.py --provider open_meteo
python main.py --provider openweather_solar
```

### What this collects

- Solar radiation: current actual/available solar values from each provider.
- Live weather: actual atmospheric fields such as temperature, humidity, precipitation, pressure, and wind.
- Forecast weather: hourly weather forecast rows for the next 48 hours from Open-Meteo.

Open-Meteo is the no-key, non-commercial model option. Its latest hourly solar
and weather values are model-led estimates. OpenWeather requires a separate
product subscription and an API key in `OPENWEATHER_API_KEY`; do not commit the
key.

NASA POWER's current run revisits the rolling 10-day window to capture delayed
or corrected satellite-derived data. Change that window with:

```powershell
python main.py --provider nasa_power --lookback-days 10
```

## Schedule hourly in production

Schedule `python main.py` to run once per hour from the repository root. The
rolling window makes reruns safe; the collected timestamps are UTC and may be
several days behind the run time. NASA POWER reports hourly solar data at its
source resolution (about 1° for solar parameters), so city coordinates resolve
to coarse grid cells rather than rooftop or neighborhood measurements.

The CSV partitions have the form:

```text
data/<provider>/solar/year=YYYY/month=MM/hourly_solar_radiation.csv
data/<provider>/weather/year=YYYY/month=MM/hourly_weather.csv
data/<provider>/weather_forecast/year=YYYY/month=MM/hourly_weather_forecast.csv
```

Provider payload columns remain source-specific. Every row includes
`timestamp_utc`, city/state/coordinates, source, and retrieval time. Open-Meteo
values use W/m² for solar values and metric weather quantities for atmospheric
fields; NASA POWER's hourly solar values use Wh/m² and meteorological values
vary by parameter. OpenWeather keeps the average, maximum, and irradiation
measures for clear-sky and cloudy-sky GHI/DNI/DHI. NASA missing fill-values are
stored as blank cells.
