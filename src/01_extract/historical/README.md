# Historical solar radiation extraction

The historical entry point retrieves an inclusive date range from the same
public NASA POWER hourly solar API used by the current collector. It does not
need an API key. Data is written to the same year/month CSV partitions under
`data/nasa_power/`; overlapping city-hour keys are updated instead of duplicated.

The alternative window checked against the API was July 1–7, 2026. NASA returned
valid GHI values for all three configured cities during that period. September
2026 and August 1–7, 2026 returned missing-value markers. To fetch the checked
July window, run from the repository root:

```powershell
python src/01_extract/historical/main.py --start-date 2026-07-01 --end-date 2026-07-07
```

The script fetches all configured all-sky and clear-sky solar variables for
Mumbai, Bengaluru, and Agartala. NASA's hourly solar values are reported in
Wh/m² on an approximately 1° source grid. The timestamp column is UTC.

The command above is provided for use when you choose to fetch the data; this
implementation turn did not run it.
