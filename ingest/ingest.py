#!/usr/bin/env python3
"""
Live trickle: the hourly job that GitHub Actions runs with nobody pressing a button.

Each run:
    1. downloads the USGS "all earthquakes, past day" CSV feed
    2. upserts it into `earthquakes` (same db.upsert_csv the seed uses)
    3. writes one row to `ingest_runs`

Why the PAST DAY every HOUR: every run overlaps the previous 23 hours. If a run
fails (USGS down at 3 a.m., GitHub hiccup), the next run still covers the missed
quakes, so short gaps heal themselves. The overlap means most rows are repeats,
and that is fine: the upsert on `id` absorbs them, and picks up USGS revisions
(magnitude updates) at the same time.

A run with 0 new rows is normal (a quiet hour), not a bug. It is still logged.

Usage:
    python ingest/ingest.py                         # MotherDuck (needs MOTHERDUCK_TOKEN)
    python ingest/ingest.py --db data/test.duckdb   # local test, no account needed
"""

import argparse
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

# db.py lives one folder up (the project root), so add that folder to the import path.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import db  # noqa: E402

FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.csv"
EXPECTED_HEADER_START = "time,latitude,longitude,depth,mag,"


def fetch_feed(url: str, retries: int = 4) -> str:
    """Download the feed as text. Retries with a growing wait, then gives up."""
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=60) as resp:
                body = resp.read().decode("utf-8")
            # Guard against an error page that still answers "200 OK".
            if not body.startswith(EXPECTED_HEADER_START):
                raise ValueError(f"unexpected feed content: {body[:80]!r}")
            return body
        except (urllib.error.URLError, TimeoutError, ConnectionError, ValueError) as err:
            last_error = err
            if attempt < retries:
                wait = 15 * attempt
                print(f"  ! {err} -- retrying in {wait}s ({attempt}/{retries})")
                time.sleep(wait)
    raise RuntimeError(f"feed download failed after {retries} tries: {last_error}")


def run_label() -> str:
    """Says where this run happened, so the run log links back to the GitHub run."""
    run_id = os.environ.get("GITHUB_RUN_ID")
    return f"github run {run_id}" if run_id else "manual run"


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest the USGS past-day feed.")
    parser.add_argument("--db", default="md:quakes",
                        help="md:quakes (default) or a local file such as data/test.duckdb")
    parser.add_argument("--url", default=FEED_URL, help="feed URL (override only for testing)")
    args = parser.parse_args()

    # If we can't even connect, there is nowhere to log to: let it crash (exit code 1).
    con = db.connect(args.db)

    try:
        body = fetch_feed(args.url)
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_file = Path(tmp_dir) / "all_day.csv"
            tmp_file.write_text(body, encoding="utf-8")
            rows_fetched, rows_new = db.upsert_csv(con, tmp_file.as_posix(), source="live")
    except Exception as err:
        # Record the failure in the run log if we can, then exit non-zero so
        # GitHub marks the run as failed (red X) and sends an email.
        print(f"ERROR: {err}")
        try:
            db.log_run(con, "live", 0, 0, status="error", message=f"{run_label()}: {err}")
        except Exception as log_err:
            print(f"(could not write the error to ingest_runs: {log_err})")
        sys.exit(1)

    db.log_run(con, "live", rows_fetched, rows_new, message=run_label())
    print(f"ok: fetched {rows_fetched:,} rows, {rows_new:,} new, "
          f"{db.count_rows(con):,} total in {args.db}")


if __name__ == "__main__":
    main()
