"""Command-line entry point for explicit NASA POWER historical date ranges."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PROVIDER_DIR = ROOT / "src" / "01_extract" / "providers"
sys.path.insert(0, str(PROVIDER_DIR))

from nasa_power import extract_date_range  # noqa: E402


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Collect NASA POWER hourly solar data for an inclusive date range."
    )
    parser.add_argument("--start-date", required=True, help="First date (YYYY-MM-DD).")
    parser.add_argument("--end-date", required=True, help="Last date (YYYY-MM-DD).")
    args = parser.parse_args()
    extract_date_range(ROOT, args.start_date, args.end_date)


if __name__ == "__main__":
    main()
