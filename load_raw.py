"""Load source parquet files from S3 landing zone into DuckLake RAW layer using dlt.

This is the "ingestion" step — analogous to Fivetran landing data in
Snowflake's RAW schema.  It reads parquet files from s3://<bucket>/events_landing/
and writes them to the DuckLake using dlt's native ducklake destination.

Supports two modes:
  - Multiplayer (default): Postgres catalog + S3 storage
  - Single-player (SINGLE_PLAYER=true): SQLite catalog + S3 storage

Incremental: dlt tracks which files have already been loaded by modification_date.
Only new files are loaded on subsequent runs. Users are always fully replaced.

Usage:
    uv run python load_raw.py
"""

import os

import boto3
import dlt
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials
from dlt.sources.filesystem import filesystem, read_parquet
from dotenv import load_dotenv

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────
AWS_PROFILE = os.environ["AWS_PROFILE"]
AWS_REGION = os.environ["AWS_REGION"]
S3_BUCKET = os.environ["S3_BUCKET"]
SINGLE_PLAYER = os.environ.get("SINGLE_PLAYER", "false").lower() == "true"
LANDING_PATH = f"s3://{S3_BUCKET}/events_landing"

S3_PATH = os.environ.get("S3_PATH", "ducklake-sp" if SINGLE_PLAYER else "ducklake")

if SINGLE_PLAYER:
    CATALOG_URL = f"sqlite:///{os.path.join(os.path.dirname(__file__), 'ducklake_catalog.db')}"
    CATALOG_DISPLAY = "sqlite:///ducklake_catalog.db (single-player)"
else:
    CATALOG_URL = os.environ["PG_URL"]
    CATALOG_DISPLAY = CATALOG_URL.split("@")[1] if "@" in CATALOG_URL else CATALOG_URL

# ── Resolve AWS SSO creds via boto3 ──────────────────────────────────
session = boto3.Session(profile_name=AWS_PROFILE)
creds = session.get_credentials().get_frozen_credentials()

# Creds for the ducklake destination (writing to S3)
os.environ["DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_ACCESS_KEY_ID"] = creds.access_key
os.environ["DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = creds.secret_key
os.environ["DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__REGION_NAME"] = AWS_REGION
if creds.token:
    os.environ["DESTINATION__DUCKLAKE__CREDENTIALS__STORAGE__CREDENTIALS__AWS_SESSION_TOKEN"] = creds.token

# Creds for the filesystem source (reading from S3)
os.environ["SOURCES__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID"] = creds.access_key
os.environ["SOURCES__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY"] = creds.secret_key
os.environ["SOURCES__FILESYSTEM__CREDENTIALS__REGION_NAME"] = AWS_REGION
if creds.token:
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__AWS_SESSION_TOKEN"] = creds.token

# ── DuckLake destination ─────────────────────────────────────────────
# Single-player: one catalog, so data_path is the root; schema "raw" creates the raw/ prefix.
# Multiplayer: separate catalog per layer with its own data_path; schema stays "main".
STORAGE_URL = f"s3://{S3_BUCKET}/{S3_PATH}/" if SINGLE_PLAYER else f"s3://{S3_BUCKET}/{S3_PATH}/raw/"
DATASET_NAME = "raw_data" if SINGLE_PLAYER else "main"

dl_credentials = DuckLakeCredentials(
    "raw",
    catalog=CATALOG_URL,
    storage=STORAGE_URL,
)

pipeline = dlt.pipeline(
    pipeline_name=f"raw_load_{S3_PATH}",
    destination=dlt.destinations.ducklake(credentials=dl_credentials),
    dataset_name=DATASET_NAME,
)

# ── Users: always replace (stable dimension) ─────────────────────────
users_fs = filesystem(
    bucket_url=f"{LANDING_PATH}/users",
    file_glob="*.parquet",
)
users_source = (users_fs | read_parquet()).with_name("users")
users_source.apply_hints(write_disposition="replace")

# ── Activity tables: incremental append by file modification_date ────
ACTIVITY_TABLES = ["events", "invoices", "support_tickets"]

activity_sources = []
for table in ACTIVITY_TABLES:
    table_fs = filesystem(
        bucket_url=f"{LANDING_PATH}/{table}",
        file_glob="batch_*.parquet",
        incremental=dlt.sources.incremental("modification_date"),
    )
    source = (table_fs | read_parquet()).with_name(table)
    source.apply_hints(write_disposition="append")
    activity_sources.append(source)

# ── Run ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mode = "single-player (SQLite)" if SINGLE_PLAYER else "multiplayer (Postgres)"
    print(f"Loading source parquets into DuckLake RAW layer via dlt [{mode}]...")
    print(f"  Source  : {LANDING_PATH}/")
    print(f"  Catalog : {CATALOG_DISPLAY}")
    print(f"  Storage : {STORAGE_URL}")
    print()

    info = pipeline.run(
        [users_source] + activity_sources,
    )

    print(info)
    print("\nDone! Data is in the DuckLake RAW layer.")
    print(f"  Catalog: {CATALOG_DISPLAY}")
    print(f"  Data files: {STORAGE_URL}")
