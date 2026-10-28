"""Stream-filter a large source CSV without loading it into memory."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Keep a recent date window and selected states from a weather CSV.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--states", nargs="+", default=["Karnataka", "Maharashtra", "Tripura"])
    args = parser.parse_args()
    if args.end_date < args.start_date:
        parser.error("--end-date must be on or after --start-date")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    allowed = {state.casefold() for state in args.states}
    total = kept = 0
    with args.input.open("r", encoding="utf-8-sig", newline="") as source, args.output.open("w", encoding="utf-8", newline="") as target:
        header_line = source.readline()
        header = [part.strip().casefold() for part in header_line.rstrip("\r\n").split(",")]
        if "datetime" not in header or "state" not in header:
            raise ValueError("Expected 'datetime' and 'state' columns in CSV")
        time_index, state_index = header.index("datetime"), header.index("state")
        # This archive stores datetime/state as the first plain-text fields.
        # Splitting only through those columns avoids parsing every 7+ GB row.
        if (time_index, state_index) != (0, 1):
            raise ValueError("Streaming fast path expects datetime,state as the first two columns")
        target.write(header_line)
        for line in source:
            total += 1
            first = line.find(",")
            second = line.find(",", first + 1) if first >= 0 else -1
            if first < 0 or second < 0:
                continue
            state = line[first + 1 : second].strip().casefold()
            if state not in allowed:
                continue
            stamp = line[:first].strip()
            try:
                row_date = date.fromisoformat(stamp[:10])
            except ValueError:
                continue
            if args.start_date <= row_date <= args.end_date:
                target.write(line)
                kept += 1
    print(f"Read {total:,} rows; retained {kept:,} rows for {', '.join(args.states)} from {args.start_date} through {args.end_date}.")
    print(f"Filtered CSV: {args.output}")


if __name__ == "__main__":
    main()
