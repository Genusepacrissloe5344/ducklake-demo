```
     ____             _    _          _
    |  _ \ _   _  ___| | _| |    __ _| | _____
    | | | | | | |/ __| |/ / |   / _` | |/ / _ \
    | |_| | |_| | (__|   <| |__| (_| |   <  __/
    |____/ \__,_|\___|_|\_\_____\__,_|_|\_\___|
         +  dlt  +  dbt  +  S3  +  Neon
    ─────────────────────────────────────────────
     The Modern Open Lakehouse on Your Laptop
```

# DuckLake + dlt + dbt: The Open Lakehouse Stack

A complete, runnable demo of a **modern lakehouse architecture** using entirely open-source tools — no Spark cluster, no Databricks account, no warehouse license required.

This project demonstrates a realistic ELT pipeline where raw data lands as parquet files in S3, gets ingested into a [DuckLake](https://ducklake.select/) catalog via [dlt](https://dlthub.com/), and is transformed into analytics-ready tables with [dbt](https://www.getdbt.com/).

## Architecture

```
                         ┌──────────────────────────────────────────────┐
                         │              S3 (Object Storage)             │
                         │                                              │
                         │   events_landing/         ducklake/raw/      │
                         │   ├── users.parquet       ├── (parquet)      │
  generate_data.py ────► │   ├── events.parquet      ducklake/dev/      │
     (source sim)        │   ├── invoices.parquet    ├── (parquet)      │
                         │   └── support_tickets     └── ...            │
                         │          .parquet                            │
                         └─────────┬────────────────────────┬────────────┘
                                   │                        │
                                   │ read                   │ write
                                   ▼                        │
                         ┌─────────────────────┐  ┌─────────┴─────────┐
                         │   dlt (ingestion)    │  │  dbt (transform)  │
                         │                      │  │                   │
                         │  load_raw.py         │  │  staging (views)  │
                         │  • reads landing zone│  │  intermediate     │
                         │  • writes to DuckLake│  │  marts (tables)   │
                         │  • adds _dlt metadata│  │                   │
                         └──────────┬───────────┘  └─────────▲─────────┘
                                    │                        │
                                    │ catalog ops            │ catalog ops
                                    ▼                        │
                         ┌───────────────────────────────────┐
                         │    Neon Postgres (Metadata DB)     │
                         │                                    │
                         │  schema: raw          (DuckLake)  │
                         │  schema: dev_analytics (DuckLake) │
                         └───────────────────────────────────┘
```

**Data flows through three distinct stages:**

| Step | Tool | What happens |
|------|------|-------------|
| **Generate** | `generate_data.py` | Simulates source system data, writes parquet files to S3 landing zone |
| **Ingest** | `load_raw.py` (dlt) | Reads from landing zone, loads into DuckLake RAW layer with lineage metadata |
| **Transform** | `dbt run` | Builds staging views, intermediate aggregations, and mart tables in DuckLake |

## What Makes This Interesting

**DuckLake** is a new open lakehouse format that separates compute (DuckDB), metadata (any SQL database), and storage (any object store). Unlike Iceberg or Delta, the catalog is just Postgres — no HMS, no Unity Catalog, no REST catalog server.

**dlt** handles ingestion with its native `ducklake` destination — you get automatic schema evolution, load metadata (`_dlt_load_id`, `_dlt_id`), and state tracking out of the box.

**dbt-duckdb** attaches to DuckLake catalogs natively, reading from `raw` and writing transformations to a separate `dev_analytics` (or `prod_analytics`) DuckLake — all in-process, no server.

**The whole pipeline runs on your laptop** but hits real cloud infrastructure (S3 for storage, Neon Postgres for metadata). Swap Neon for RDS, swap S3 for GCS — the architecture stays the same.

## Tech Stack

| Component | Role | Why |
|-----------|------|-----|
| [DuckDB](https://duckdb.org/) | Query engine | In-process OLAP, zero infrastructure |
| [DuckLake](https://ducklake.select/) | Lakehouse format | Open catalog, Postgres-native metadata |
| [dlt](https://dlthub.com/) | Data ingestion | Python-native EL with built-in ducklake destination |
| [dbt](https://www.getdbt.com/) | Transformation | SQL-first modeling, staging → intermediate → marts |
| [Neon](https://neon.tech/) | Metadata catalog | Serverless Postgres for DuckLake metadata |
| [S3](https://aws.amazon.com/s3/) | Object storage | Parquet file storage for both landing zone and lakehouse |
| [uv](https://docs.astral.sh/uv/) | Package manager | Fast Python dependency management |

## Project Structure

```
ducklake/
├── generate_data.py              # Step 1: Generate source parquet → S3 landing zone
├── load_raw.py                   # Step 2: dlt ingestion → DuckLake RAW
├── ducklake_dbt/                 # Step 3: dbt transformations
│   ├── dbt_project.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── staging/              # 1:1 views on raw tables
│       │   ├── _sources.yml
│       │   ├── stg_users.sql
│       │   ├── stg_events.sql
│       │   ├── stg_invoices.sql
│       │   └── stg_tickets.sql
│       ├── intermediate/         # Business logic aggregations
│       │   ├── int_revenue_by_user.sql
│       │   └── int_user_events_sessionized.sql
│       └── marts/                # Analytics-ready tables
│           ├── dim_users.sql
│           ├── fct_daily_active_users.sql
│           └── fct_revenue.sql
├── .env.example                  # Environment variable template
├── run.py                        # Run the full pipeline (generate → load → transform)
├── pyproject.toml                # Python dependencies (managed by uv)
└── uv.lock
```

## Data Model

The demo generates a realistic SaaS product dataset:

**Raw tables** (loaded via dlt):
- `users` — 2,000 user accounts with plan tiers and geography
- `events` — 50,000 product events (page views, signups, upgrades, feature usage)
- `invoices` — 5,000 billing records with payment status
- `support_tickets` — 1,500 support requests with priority and resolution

**Mart tables** (built by dbt):
- `dim_users` — Enriched user dimension with lifetime revenue and support metrics
- `fct_daily_active_users` — Daily active users with event type breakdowns
- `fct_revenue` — Revenue facts by date, plan, country, and invoice status

## Getting Started

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- AWS CLI with SSO configured (`aws configure sso`)
- A Postgres database ([Neon](https://neon.tech/) free tier works great)
- An S3 bucket

### Setup

**1. Clone and install dependencies**

```bash
git clone <this-repo>
cd ducklake
uv sync
```

**2. Configure environment**

```bash
cp .env.example .env
# Edit .env with your Postgres URL, S3 bucket, and connection details
```

**3. Authenticate AWS**

```bash
aws sso login --profile $AWS_PROFILE
```

### Run the Pipeline

The easiest way to run the full pipeline is with `run.py`:

```bash
uv run python run.py        # 1 batch of source data (default)
uv run python run.py 10     # 10 batches (10x activity data)
```

This runs all three steps in order:
1. **Generate** source parquet files to the S3 landing zone
2. **Load** into DuckLake RAW via dlt (incremental — only new files are loaded)
3. **Transform** with dbt (staging views, intermediate views, mart tables)

You can also run each step individually:

<details>
<summary>Individual steps</summary>

**Step 1 — Generate source data to S3 landing zone**

```bash
uv run python generate_data.py       # 1 batch
uv run python generate_data.py 10    # 10 batches
```

Run this as many times as you want — each run adds new activity data for the same stable set of 2,000 users.

**Step 2 — Ingest into DuckLake RAW via dlt**

```bash
uv run python load_raw.py
```

This reads parquet files from the S3 landing zone and loads them into the DuckLake RAW layer. dlt tracks which files have already been loaded — only new files are picked up on subsequent runs.

**Step 3 — Transform with dbt**

```bash
# Export temp AWS creds (dbt can't use SSO directly)
eval "$(aws configure export-credentials --profile $AWS_PROFILE --format env)"

# Source env vars and run dbt
set -a && source .env && set +a
uv run dbt run --project-dir ducklake_dbt
```

</details>

### Verify

Query the results directly with DuckDB:

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL ducklake; LOAD ducklake;")
# ... attach your DuckLake and query away
con.execute("SELECT * FROM dev_analytics.marts.dim_users LIMIT 5").show()
```

## How DuckLake Works

DuckLake splits a traditional data warehouse into three independent layers:

```
┌─────────────────────────────────┐
│         DuckDB (Compute)        │  ← Runs anywhere: laptop, CI, Lambda
├─────────────────────────────────┤
│      Postgres (Metadata)        │  ← Schema, partitions, snapshots
├─────────────────────────────────┤
│         S3 (Storage)            │  ← Parquet files, append-only
└─────────────────────────────────┘
```

- **No vendor lock-in** — metadata is Postgres tables, data is Parquet on S3
- **Time travel** — DuckLake tracks snapshots, so you can query any prior state
- **Multi-writer safe** — Postgres handles catalog concurrency
- **Zero servers** — DuckDB runs in-process, Neon and S3 are serverless

## Why This Stack

The traditional modern data stack (Fivetran + Snowflake + dbt) costs real money and requires managed services. This stack achieves the same architecture pattern — **extract → load → transform** — with:

- **$0 compute** — DuckDB runs locally
- **~$0 metadata** — Neon free tier (0.5 GB storage)
- **~$0 storage** — S3 costs pennies for demo-scale data
- **Full ELT lineage** — dlt tracks every load, dbt tracks every transform
- **Production-grade patterns** — The same separation of concerns scales up

## License

MIT
