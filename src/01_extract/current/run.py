"""Command-line entry point for the rolling current-data extraction."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PROVIDER_DIR = ROOT / "src" / "01_extract" / "providers"
sys.path.insert(0, str(PROVIDER_DIR))

from nasa_power import run_current_extraction  # noqa: E402


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Collect recent NASA POWER hourly solar data.")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=10,
        help="Days to re-fetch for delayed/corrected observations (default: 10).",
    )
    args = parser.parse_args()
    run_current_extraction(ROOT, lookback_days=args.lookback_days)


if __name__ == "__main__":
    main()
