"""Generate realistic SaaS source data as parquet files on S3.

Users are written once (stable base of 2,000). Each batch adds new
activity data (events, invoices, tickets) that references those users.
Run repeatedly or with a scale factor to accumulate more activity.

Usage:
    uv run python generate_data.py          # 1 batch (default)
    uv run python generate_data.py 10       # 10 batches (10x activity)

Files land at:
    s3://<bucket>/events_landing/users/users.parquet           (written once)
    s3://<bucket>/events_landing/<table>/batch_<ts>_<n>.parquet (per batch)
"""

import os
import random
import sys
from datetime import datetime, timezone

import boto3
import duckdb

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

SCALE_FACTOR = int(sys.argv[1]) if len(sys.argv) > 1 else 1
NUM_USERS = 2000

AWS_PROFILE = os.environ["AWS_PROFILE"]
AWS_REGION = os.environ["AWS_REGION"]
S3_BUCKET = os.environ["S3_BUCKET"]
LANDING_PATH = f"s3://{S3_BUCKET}/events_landing"

batch_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")

con = duckdb.connect()

# ── S3 access via boto3 SSO ──────────────────────────────────────────
for ext in ["httpfs"]:
    con.execute(f"INSTALL {ext}; LOAD {ext};")

session = boto3.Session(profile_name=AWS_PROFILE)
creds = session.get_credentials().get_frozen_credentials()
con.execute(f"""
    CREATE SECRET s3_secret (
        TYPE s3,
        KEY_ID '{creds.access_key}',
        SECRET '{creds.secret_key}',
        SESSION_TOKEN '{creds.token}',
        REGION '{AWS_REGION}'
    );
""")

# ── Users (stable, written once — overwrites each run) ───────────────
con.execute(f"""
    CREATE TABLE users AS
    WITH base AS (
        SELECT i AS id FROM generate_series(1, {NUM_USERS}) t(i)
    )
    SELECT
        id,
        'user' || id || '@' ||
            CASE id % 5
                WHEN 0 THEN 'gmail.com'
                WHEN 1 THEN 'outlook.com'
                WHEN 2 THEN 'company.io'
                WHEN 3 THEN 'fastmail.com'
                ELSE 'proton.me'
            END AS email,
        CASE id % 12
            WHEN 0  THEN 'Alice Johnson'
            WHEN 1  THEN 'Bob Smith'
            WHEN 2  THEN 'Carlos Garcia'
            WHEN 3  THEN 'Diana Chen'
            WHEN 4  THEN 'Erik Larsson'
            WHEN 5  THEN 'Fatima Al-Rashid'
            WHEN 6  THEN 'Grace Kim'
            WHEN 7  THEN 'Hiroshi Tanaka'
            WHEN 8  THEN 'Isabella Rossi'
            WHEN 9  THEN 'James O''Brien'
            WHEN 10 THEN 'Katarina Novak'
            ELSE 'Liam Patel'
        END || ' ' || (id % 200)::TEXT AS full_name,
        CASE
            WHEN id % 10 < 5 THEN 'free'
            WHEN id % 10 < 8 THEN 'pro'
            ELSE 'enterprise'
        END AS plan,
        DATE '2025-07-01' + INTERVAL (-(id * 97 % 730)) DAY AS created_at,
        CASE id % 8
            WHEN 0 THEN 'US'  WHEN 1 THEN 'GB'  WHEN 2 THEN 'DE'
            WHEN 3 THEN 'CA'  WHEN 4 THEN 'AU'  WHEN 5 THEN 'FR'
            WHEN 6 THEN 'JP'  ELSE 'BR'
        END AS country_code
    FROM base;
""")

users_path = f"{LANDING_PATH}/users/users.parquet"
con.execute(f"COPY users TO '{users_path}' (FORMAT PARQUET)")
print(f"  users/users.parquet: {NUM_USERS} rows (stable)")

# ── Activity batches (accumulate over runs) ──────────────────────────
print(f"\nGenerating {SCALE_FACTOR} activity batch(es)...\n")

for batch_num in range(SCALE_FACTOR):
    seed = random.randint(0, 1_000_000)
    ID_OFFSET = seed * 100  # unique IDs per batch, user_id always 1-2000

    for t in ["events", "invoices", "support_tickets"]:
        con.execute(f"DROP TABLE IF EXISTS {t}")

    # ── events (~50,000 rows) ────────────────────────────────────────
    con.execute(f"""
        CREATE TABLE events AS
        WITH base AS (
            SELECT i AS id FROM generate_series(1, 50000) t(i)
        )
        SELECT
            id + {ID_OFFSET}::BIGINT * 500 AS id,
            (id % {NUM_USERS}) + 1 AS user_id,
            CASE (id + {seed}) % 6
                WHEN 0 THEN 'page_view'
                WHEN 1 THEN 'signup'
                WHEN 2 THEN 'upgrade'
                WHEN 3 THEN 'downgrade'
                WHEN 4 THEN 'feature_use'
                ELSE 'logout'
            END AS event_name,
            TIMESTAMP '2025-10-01' + INTERVAL ((id * 317 + {seed}) % (180 * 86400)) SECOND AS event_timestamp,
            CASE (id + {seed}) % 6
                WHEN 0 THEN '{{"page": "/' ||
                    CASE (id + {seed}) % 5
                        WHEN 0 THEN 'home'   WHEN 1 THEN 'pricing'
                        WHEN 2 THEN 'dashboard' WHEN 3 THEN 'settings'
                        ELSE 'docs'
                    END || '", "referrer": "' ||
                    CASE (id + {seed}) % 4
                        WHEN 0 THEN 'https://google.com'
                        WHEN 1 THEN 'https://twitter.com'
                        WHEN 2 THEN 'direct'
                        ELSE 'https://github.com'
                    END || '"}}'
                WHEN 1 THEN '{{"source": "' ||
                    CASE (id + {seed}) % 3 WHEN 0 THEN 'organic' WHEN 1 THEN 'referral' ELSE 'paid' END || '"}}'
                WHEN 2 THEN '{{"from_plan": "free", "to_plan": "' ||
                    CASE (id + {seed}) % 2 WHEN 0 THEN 'pro' ELSE 'enterprise' END || '"}}'
                WHEN 3 THEN '{{"from_plan": "' ||
                    CASE (id + {seed}) % 2 WHEN 0 THEN 'pro' ELSE 'enterprise' END || '", "to_plan": "free"}}'
                WHEN 4 THEN '{{"feature": "' ||
                    CASE (id + {seed}) % 4
                        WHEN 0 THEN 'export_csv'  WHEN 1 THEN 'create_report'
                        WHEN 2 THEN 'invite_member' ELSE 'api_call'
                    END || '"}}'
                ELSE '{{"session_duration_sec": ' || ((id + {seed}) % 3600)::TEXT || '}}'
            END AS properties
        FROM base;
    """)

    # ── invoices (~5,000 rows) ───────────────────────────────────────
    con.execute(f"""
        CREATE TABLE invoices AS
        WITH base AS (
            SELECT i AS id FROM generate_series(1, 5000) t(i)
        )
        SELECT
            id + {ID_OFFSET}::BIGINT * 5 AS id,
            (id % {NUM_USERS}) + 1 AS user_id,
            ROUND(
                CASE
                    WHEN (id + {seed}) % 5 < 3 THEN 9.99 + ((id + {seed}) % 20) * 0.5
                    WHEN (id + {seed}) % 5 < 4 THEN 49.99 + ((id + {seed}) % 50)
                    ELSE 199.99 + ((id + {seed}) % 100) * 2
                END, 2
            )::DECIMAL(10,2) AS amount,
            'USD' AS currency,
            CASE (id + {seed}) % 10
                WHEN 0 THEN 'pending'
                WHEN 1 THEN 'failed'
                WHEN 2 THEN 'refunded'
                ELSE 'paid'
            END AS status,
            DATE '2025-10-01' + INTERVAL ((id * 131 + {seed}) % 180) DAY AS issued_at,
            CASE
                WHEN (id + {seed}) % 10 IN (0, 1) THEN NULL
                ELSE DATE '2025-10-01' + INTERVAL ((id * 131 + {seed}) % 180 + (id + {seed}) % 14) DAY
            END AS paid_at
        FROM base;
    """)

    # ── support_tickets (~1,500 rows) ────────────────────────────────
    con.execute(f"""
        CREATE TABLE support_tickets AS
        WITH base AS (
            SELECT i AS id FROM generate_series(1, 1500) t(i)
        )
        SELECT
            id + {ID_OFFSET}::BIGINT AS id,
            (id % {NUM_USERS}) + 1 AS user_id,
            CASE (id + {seed}) % 8
                WHEN 0 THEN 'Cannot export data'
                WHEN 1 THEN 'Billing discrepancy'
                WHEN 2 THEN 'Login issues after upgrade'
                WHEN 3 THEN 'Feature request: dark mode'
                WHEN 4 THEN 'API rate limit too low'
                WHEN 5 THEN 'Dashboard not loading'
                WHEN 6 THEN 'Team invitation failed'
                ELSE 'Data sync delay'
            END AS subject,
            CASE (id + {seed}) % 4
                WHEN 0 THEN 'low'    WHEN 1 THEN 'medium'
                WHEN 2 THEN 'high'   ELSE 'critical'
            END AS priority,
            CASE (id + {seed}) % 4
                WHEN 0 THEN 'open'       WHEN 1 THEN 'in_progress'
                WHEN 2 THEN 'resolved'   ELSE 'closed'
            END AS status,
            DATE '2025-10-01' + INTERVAL ((id * 73 + {seed}) % 180) DAY AS created_at,
            CASE
                WHEN (id + {seed}) % 4 IN (0, 1) THEN NULL
                ELSE DATE '2025-10-01' + INTERVAL ((id * 73 + {seed}) % 180 + 1 + (id + {seed}) % 7) DAY
            END AS resolved_at
        FROM base;
    """)

    # ── Export activity to parquet on S3 ─────────────────────────────
    file_tag = f"{batch_ts}_{batch_num:04d}"
    for table in ["events", "invoices", "support_tickets"]:
        s3_path = f"{LANDING_PATH}/{table}/batch_{file_tag}.parquet"
        con.execute(f"COPY {table} TO '{s3_path}' (FORMAT PARQUET)")
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}/batch_{file_tag}.parquet: {count} rows")

    print(f"  [batch {batch_num + 1}/{SCALE_FACTOR} done]\n")

print(f"Wrote {SCALE_FACTOR} batch(es) to {LANDING_PATH}/")
print("Run again to add more data!")
