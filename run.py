"""Run the full DuckLake demo pipeline.

Usage:
    uv run python run.py          # 1 batch (default)
    uv run python run.py 10       # 10 batches (10x activity)

Set SINGLE_PLAYER=true in .env to use SQLite catalog instead of Postgres.
"""

import logging
import os
import subprocess
import sys

from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

load_dotenv()

SCALE_FACTOR = sys.argv[1] if len(sys.argv) > 1 else "1"
AWS_PROFILE = os.environ["AWS_PROFILE"]
SINGLE_PLAYER = os.environ.get("SINGLE_PLAYER", "false").lower() == "true"


def run(cmd: list[str], **kwargs) -> None:
    log.info("> %s", " ".join(cmd))
    subprocess.run(cmd, check=True, **kwargs)


if SINGLE_PLAYER:
    log.info("Running in SINGLE-PLAYER mode (SQLite catalog)")
else:
    log.info("Running in MULTIPLAYER mode (Postgres catalog)")

# Step 1: Generate source data to S3 landing zone
log.info("Step 1: Generate source data (scale factor: %s)", SCALE_FACTOR)
run(["uv", "run", "python", "generate_data.py", SCALE_FACTOR])

# Step 2: Load into DuckLake RAW via dlt
log.info("Step 2: Load into DuckLake RAW via dlt")
run(["uv", "run", "python", "load_raw.py"])

# Step 3: Export temp AWS creds for dbt, then run dbt
log.info("Step 3: dbt run")
result = subprocess.run(
    ["aws", "configure", "export-credentials", "--profile", AWS_PROFILE, "--format", "env"],
    check=True,
    capture_output=True,
    text=True,
)
dbt_env = os.environ.copy()
for line in result.stdout.strip().splitlines():
    line = line.removeprefix("export ")
    key, _, value = line.partition("=")
    dbt_env[key] = value

if SINGLE_PLAYER:
    dbt_env["DUCKLAKE_CATALOG_PATH"] = os.path.join(os.path.dirname(__file__), "ducklake_catalog.db")

dbt_cmd = ["uv", "run", "dbt", "run", "--project-dir", "ducklake_dbt"]
if SINGLE_PLAYER:
    dbt_cmd += ["--target", "sp"]

run(dbt_cmd, env=dbt_env)

log.info("Done!")
