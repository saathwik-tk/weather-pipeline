# Current solar radiation extraction

The current orchestrator supports NASA POWER, Open-Meteo, and OpenWeather Solar
Irradiance for Mumbai, Bengaluru, and Agartala. Each source writes its own
date-partitioned CSVs under `data/<provider>/` and upserts by city and UTC hour.

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

Open-Meteo is the no-key, non-commercial forecast-model option. Its latest
hourly solar values are estimates, not satellite observations. OpenWeather
Solar Irradiance requires a separate product subscription and an API key in
`OPENWEATHER_API_KEY`; do not commit the key.

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
data/<provider>/year=YYYY/month=MM/hourly_solar_radiation.csv
```

Provider payload columns remain source-specific. Every row includes
`timestamp_utc`, city/state/coordinates, source, and retrieval time. Open-Meteo
values use W/m²; NASA POWER's hourly solar values use Wh/m². OpenWeather keeps
the average, maximum, and irradiation measures for clear-sky and cloudy-sky
GHI/DNI/DHI. NASA missing fill-values are stored as blank cells.
