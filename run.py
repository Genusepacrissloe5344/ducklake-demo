"""Run the full DuckLake demo pipeline.

Usage:
    uv run python run.py          # 1 batch (default)
    uv run python run.py 10       # 10 batches (10x activity)
"""

import os
import subprocess
import sys

from dotenv import load_dotenv

load_dotenv()

SCALE_FACTOR = sys.argv[1] if len(sys.argv) > 1 else "1"
AWS_PROFILE = os.environ["AWS_PROFILE"]


def run(cmd: list[str], **kwargs) -> None:
    print(f"\n> {' '.join(cmd)}")
    subprocess.run(cmd, check=True, **kwargs)


# Step 1: Generate source data to S3 landing zone
print(f"=== Step 1: Generate source data (scale factor: {SCALE_FACTOR}) ===")
run(["uv", "run", "python", "generate_data.py", SCALE_FACTOR])

# Step 2: Load into DuckLake RAW via dlt
print("\n=== Step 2: Load into DuckLake RAW via dlt ===")
run(["uv", "run", "python", "load_raw.py"])

# Step 3: Export temp AWS creds for dbt, then run dbt
print("\n=== Step 3: dbt run ===")
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

run(["uv", "run", "dbt", "run", "--project-dir", "ducklake_dbt"], env=dbt_env)

print("\n=== Done! ===")
