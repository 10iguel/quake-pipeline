#!/usr/bin/env python3
"""
Historical seed, part 1: download the USGS earthquake catalog as raw CSV files.

- Uses the USGS FDSN Event API (no API key needed).
- The API returns at most 20,000 events per request, so we:
    1. ask "how many events are in this time window?" (the /count endpoint)
    2. if it's more than 20,000, split the window in half and try again
    3. otherwise download the window as one CSV file
- Every window is saved to data/raw/ as its own file. If the script crashes or
  you stop it (Ctrl+C), just run it again: finished windows are skipped.
  This is the "seeding etiquette" from the assignment: download once, keep the files.

Usage:
    python seed/download.py --start 2026-10-01 --end 2026-11-01   # quick test (1 month)
    python seed/download.py                                       # full catalog, 1900 -> now
"""

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1"
MAX_PER_REQUEST = 20_000
RAW_DIR = Path("data/raw")
PAUSE_SECONDS = 1.0  # be polite to a free public service


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def fetch(endpoint: str, params: dict, retries: int = 5) -> str:
    """GET a USGS URL and return the body as text. Retries on network errors."""
    url = f"{BASE_URL}/{endpoint}?{urllib.parse.urlencode(params)}"
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=180) as resp:
                # USGS answers "204 No Content" when a window has zero events.
                return resp.read().decode("utf-8")
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            wait = 10 * attempt
            print(f"    ! {err} -- retrying in {wait}s ({attempt}/{retries})")
            time.sleep(wait)
    raise RuntimeError(f"Gave up after {retries} tries: {url}")


def count_events(start: datetime, end: datetime) -> int:
    body = fetch("count", {"format": "geojson", "starttime": iso(start), "endtime": iso(end)})
    time.sleep(PAUSE_SECONDS)
    return int(json.loads(body)["count"])


def download_window(start: datetime, end: datetime) -> int:
    """Download one window; split it in half if it's too big. Returns rows saved."""
    out_file = RAW_DIR / f"quakes_{start:%Y%m%dT%H%M%S}_{end:%Y%m%dT%H%M%S}.csv"
    if out_file.exists():
        print(f"  skip   {out_file.name} (already downloaded)")
        return 0

    n = count_events(start, end)
    if n == 0:
        print(f"  empty  {iso(start)} -> {iso(end)}")
        return 0
    if n > MAX_PER_REQUEST:
        middle = start + (end - start) / 2
        print(f"  split  {iso(start)} -> {iso(end)} has {n:,} events (> {MAX_PER_REQUEST:,})")
        return download_window(start, middle) + download_window(middle, end)

    body = fetch("query", {
        "format": "csv",
        "starttime": iso(start),
        "endtime": iso(end),
        "orderby": "time-asc",
        "limit": MAX_PER_REQUEST,
    })
    time.sleep(PAUSE_SECONDS)

    # Write to a temp file first, then rename. A half-written file never looks "done".
    tmp_file = out_file.with_suffix(".part")
    tmp_file.write_text(body, encoding="utf-8")
    tmp_file.rename(out_file)
    print(f"  saved  {out_file.name}  ({n:,} events)")
    return n


def main() -> None:
    parser = argparse.ArgumentParser(description="Download the USGS earthquake catalog.")
    parser.add_argument("--start", default="1900-01-01", help="UTC date, e.g. 2000-01-01")
    parser.add_argument("--end", default=None, help="UTC date (default: now)")
    args = parser.parse_args()

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    if args.end:
        end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    else:
        end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {iso(start)} -> {iso(end)} into {RAW_DIR}/")

    # Walk forward one calendar year at a time; download_window splits further if needed.
    total = 0
    cursor = start
    while cursor < end:
        next_year = cursor.replace(year=cursor.year + 1, month=1, day=1,
                                   hour=0, minute=0, second=0)
        window_end = min(next_year, end)
        print(f"{cursor.year}:")
        total += download_window(cursor, window_end)
        cursor = window_end

    print(f"\nDone. {total:,} new events saved this run. Files are in {RAW_DIR}/")
    print("Tip: windows touch at their edges, so a few events may appear twice. "
          "That's fine: the loader dedups on the earthquake id.")


if __name__ == "__main__":
    main()