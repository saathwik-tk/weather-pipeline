"""Orchestrator and main entry point for the weather pipeline."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PROVIDER_DIR = ROOT / "src" / "01_extract" / "providers"
sys.path.insert(0, str(PROVIDER_DIR))

from nasa_power import collect_current_weather as collect_nasa_weather  # noqa: E402
from nasa_power import run_current_extraction  # noqa: E402
from open_meteo import collect_current as collect_open_meteo  # noqa: E402
from open_meteo import collect_current_weather as collect_open_meteo_weather  # noqa: E402
from open_meteo import collect_forecast_weather as collect_open_meteo_forecast  # noqa: E402
from openweather_solar import collect_current as collect_openweather  # noqa: E402
from openweather_solar import collect_current_weather as collect_openweather_weather  # noqa: E402
from openweather_solar import collect_forecast_weather as collect_openweather_forecast  # noqa: E402


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Run current solar-radiation extractors.")
    parser.add_argument(
        "--provider",
        choices=("all", "nasa_power", "open_meteo", "openweather_solar"),
        default="all",
        help="Source to collect (default: all; OpenWeather is skipped without its API key).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=10,
        help="Days to re-fetch to capture NASA POWER's delayed solar data (default: 10).",
    )
    args = parser.parse_args()
    selected = {"nasa_power", "open_meteo", "openweather_solar"} if args.provider == "all" else {args.provider}
    if "nasa_power" in selected:
        run_current_extraction(project_root=ROOT, lookback_days=args.lookback_days)
        collect_nasa_weather(project_root=ROOT, lookback_days=args.lookback_days)
    if "open_meteo" in selected:
        collect_open_meteo(ROOT)
        collect_open_meteo_weather(ROOT)
        collect_open_meteo_forecast(ROOT, forecast_hours=48)
    if "openweather_solar" in selected:
        if os.environ.get("OPENWEATHER_API_KEY"):
            collect_openweather(ROOT)
            collect_openweather_weather(ROOT)
            collect_openweather_forecast(ROOT, forecast_hours=48)
        elif args.provider == "all":
            logging.warning("Skipping OpenWeather Solar and weather: OPENWEATHER_API_KEY is not set")
        else:
            collect_openweather(ROOT)
            collect_openweather_weather(ROOT)
            collect_openweather_forecast(ROOT, forecast_hours=48)


if __name__ == "__main__":
    main()
