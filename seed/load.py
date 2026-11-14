#!/usr/bin/env python3
"""
Historical seed, part 2: bulk-load the downloaded CSV files into the database.

Reads the files that seed/download.py saved in data/raw/ (it never calls the
USGS API), so it can be re-run as often as needed without re-downloading.

Usage:
    python seed/load.py                    # local test database: data/test.duckdb
    python seed/load.py --db md:quakes     # MotherDuck (run `source .env` first)

Idempotency proof: run it twice. The second run must print "new rows inserted: 0".
"""

import argparse
import sys
import time
from pathlib import Path

# db.py lives one folder up (the project root), so add that folder to the import path.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import db  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk-load data/raw/*.csv into the database.")
    parser.add_argument("--db", default="data/test.duckdb",
                        help="local file path, or md:quakes for MotherDuck")
    parser.add_argument("--files", default="data/raw/*.csv",
                        help="CSV file or glob to load")
    args = parser.parse_args()

    n_files = len(list(Path().glob(args.files)))
    if n_files == 0:
        sys.exit(f"No files match {args.files}. Run seed/download.py first.")

    print(f"Loading {n_files} file(s) from {args.files} into {args.db} ...")
    started = time.time()
    con = db.connect(args.db)

    try:
        rows_fetched, rows_new = db.upsert_csv(con, args.files, source="seed")
    except Exception as err:
        db.log_run(con, "seed", 0, 0, status="error", message=str(err))
        raise
    db.log_run(con, "seed", rows_fetched, rows_new, message=f"{n_files} files")

    # Format the timestamps as text inside SQL (UTC), so Python doesn't need
    # the extra `pytz` package just to print them.
    total, oldest, newest = con.execute(
        """
        SELECT count(*),
               strftime(min(event_time) AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S UTC'),
               strftime(max(event_time) AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M:%S UTC')
        FROM earthquakes
        """
    ).fetchone()

    print(f"  rows read from files : {rows_fetched:,}")
    print(f"  new rows inserted    : {rows_new:,}")
    print(f"  total rows in table  : {total:,}")
    print(f"  oldest event         : {oldest}")
    print(f"  newest event         : {newest}")
    print(f"  time taken           : {time.time() - started:.1f}s")
    if not args.db.startswith("md:"):
        con.close()
        print(f"  database file size   : {Path(args.db).stat().st_size / 1e6:,.1f} MB")


if __name__ == "__main__":
    main()
