"""
Shared database code, used by BOTH paths of the pipeline:

    seed/load.py      (one-time bulk load of the downloaded history)
    ingest/ingest.py  (hourly live job on GitHub Actions)

Keeping the schema and the upsert in one place means the seed and the live job
cannot drift apart: same table, same dedup rule, same run log.
"""

import os
from pathlib import Path

import duckdb

# CSV column -> (our column name, type). The order matches the USGS CSV header.
# Both the history API and the live feed use exactly these columns.
COLUMNS = [
    ("time",            "event_time",       "TIMESTAMPTZ"),
    ("latitude",        "latitude",         "DOUBLE"),
    ("longitude",       "longitude",        "DOUBLE"),
    ("depth",           "depth_km",         "DOUBLE"),
    ("mag",             "mag",              "DOUBLE"),
    ("magType",         "mag_type",         "VARCHAR"),
    ("nst",             "nst",              "INTEGER"),
    ("gap",             "gap",              "DOUBLE"),
    ("dmin",            "dmin",             "DOUBLE"),
    ("rms",             "rms",              "DOUBLE"),
    ("net",             "net",              "VARCHAR"),
    ("id",              "id",               "VARCHAR"),
    ("updated",         "updated_at",       "TIMESTAMPTZ"),
    ("place",           "place",            "VARCHAR"),
    ("type",            "event_type",       "VARCHAR"),
    ("horizontalError", "horizontal_error", "DOUBLE"),
    ("depthError",      "depth_error",      "DOUBLE"),
    ("magError",        "mag_error",        "DOUBLE"),
    ("magNst",          "mag_nst",          "INTEGER"),
    ("status",          "status",           "VARCHAR"),
    ("locationSource",  "location_source",  "VARCHAR"),
    ("magSource",       "mag_source",       "VARCHAR"),
]

SCHEMA_SQL = [
    """
    CREATE TABLE IF NOT EXISTS earthquakes (
        id                VARCHAR PRIMARY KEY,   -- USGS event id: the dedup key
        event_time        TIMESTAMPTZ,
        latitude          DOUBLE,
        longitude         DOUBLE,
        depth_km          DOUBLE,
        mag               DOUBLE,
        mag_type          VARCHAR,
        nst               INTEGER,
        gap               DOUBLE,
        dmin              DOUBLE,
        rms               DOUBLE,
        net               VARCHAR,
        updated_at        TIMESTAMPTZ,           -- when USGS last revised the event
        place             VARCHAR,
        event_type        VARCHAR,
        horizontal_error  DOUBLE,
        depth_error       DOUBLE,
        mag_error         DOUBLE,
        mag_nst           INTEGER,
        status            VARCHAR,
        location_source   VARCHAR,
        mag_source        VARCHAR,
        source            VARCHAR,               -- 'seed' or 'live': who loaded it FIRST
        first_ingested_at TIMESTAMPTZ,           -- when OUR pipeline first saw it (never overwritten)
        last_ingested_at  TIMESTAMPTZ            -- when our pipeline last refreshed it
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ingest_runs (
        run_at        TIMESTAMPTZ,
        source        VARCHAR,                   -- 'seed' or 'live'
        rows_fetched  BIGINT,                    -- rows in the file(s) we read
        rows_new      BIGINT,                    -- rows that were not in the table before
        rows_total    BIGINT,                    -- table size after the run
        status        VARCHAR,                   -- 'ok' or 'error'
        message       VARCHAR
    )
    """,
]


def connect(target: str) -> duckdb.DuckDBPyConnection:
    """
    Open the database and make sure the tables exist.

    target = "data/test.duckdb"  -> a local file (testing, no account needed)
    target = "md:quakes"         -> MotherDuck database `quakes`
                                    (needs the MOTHERDUCK_TOKEN env var)
    """
    if target.startswith("md:"):
        if not os.environ.get("MOTHERDUCK_TOKEN"):
            raise RuntimeError(
                "MOTHERDUCK_TOKEN is not set. Run `source .env` first "
                "(or add the secret in GitHub Actions)."
            )
        db_name = target[3:] or "quakes"
        con = duckdb.connect("md:")  # the token is read from the env var
        con.execute(f'CREATE DATABASE IF NOT EXISTS "{db_name}"')
        con.execute(f'USE "{db_name}"')
    else:
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(target)

    for statement in SCHEMA_SQL:
        con.execute(statement)
    return con


def count_rows(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute("SELECT count(*) FROM earthquakes").fetchone()[0]


def upsert_csv(con: duckdb.DuckDBPyConnection, path_or_glob: str, source: str) -> tuple[int, int]:
    """
    Load one CSV file (or a glob like data/raw/*.csv) into `earthquakes`.

    Safe to run any number of times with the same data (idempotent):
    a quake that is already in the table is UPDATED, never duplicated.

    Returns (rows_fetched, rows_new).
    """
    before = count_rows(con)

    # 1. Read everything as text into a temporary staging table.
    #    all_varchar = no type guessing, so one odd value can't break a 4M-row load.
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE staging AS
        SELECT * FROM read_csv(?, header = true, all_varchar = true)
        """,
        [path_or_glob],
    )
    rows_fetched = con.execute("SELECT count(*) FROM staging").fetchone()[0]

    # 2. TRY_CAST each column: a value that doesn't parse becomes NULL instead of an error.
    select_list = ",\n            ".join(
        f'"{csv_name}" AS {name}' if col_type == "VARCHAR"
        else f'TRY_CAST("{csv_name}" AS {col_type}) AS {name}'
        for csv_name, name, col_type in COLUMNS
    )
    insert_columns = ", ".join(name for _, name, _ in COLUMNS)

    # 4. On a duplicate id: refresh every data column, but keep `source` and
    #    `first_ingested_at` exactly as they were the first time.
    update_list = ",\n            ".join(
        f"{name} = EXCLUDED.{name}" for _, name, _ in COLUMNS if name != "id"
    )

    con.execute(
        f"""
        INSERT INTO earthquakes ({insert_columns}, source, first_ingested_at, last_ingested_at)
        SELECT {insert_columns}, ? AS source, now(), now()
        FROM (
            SELECT
            {select_list},
            -- 3. The same quake can appear twice in one batch (download windows
            --    touch at their edges). Keep only its newest version, because
            --    one INSERT can't upsert the same key twice.
            row_number() OVER (
                PARTITION BY "id" ORDER BY "updated" DESC NULLS LAST
            ) AS rn
            FROM staging
            WHERE "id" IS NOT NULL AND "id" <> ''
        )
        WHERE rn = 1
        ON CONFLICT (id) DO UPDATE SET
            {update_list},
            last_ingested_at = EXCLUDED.last_ingested_at
        """,
        [source],
    )
    con.execute("DROP TABLE staging")

    rows_new = count_rows(con) - before
    return rows_fetched, rows_new


def log_run(con: duckdb.DuckDBPyConnection, source: str, rows_fetched: int,
            rows_new: int, status: str = "ok", message: str = "") -> None:
    """Write one row to `ingest_runs`. Every run is logged, even 0-new-row runs and errors."""
    con.execute(
        "INSERT INTO ingest_runs VALUES (now(), ?, ?, ?, ?, ?, ?)",
        [source, rows_fetched, rows_new, count_rows(con), status, message[:500]],
    )
