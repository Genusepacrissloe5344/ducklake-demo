"""DuckLake experiment: connect to Postgres catalog + S3 storage, load realistic SaaS data."""

import os
from urllib.parse import urlparse

import boto3
import duckdb

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Config from env ---
AWS_PROFILE = os.environ["AWS_PROFILE"]
AWS_REGION = os.environ["AWS_REGION"]
S3_BUCKET = os.environ["S3_BUCKET"]

pg = urlparse(os.environ["PG_URL"])
PG_HOST = pg.hostname
PG_PORT = pg.port or 5432
PG_DB = pg.path.lstrip("/")
PG_USER = pg.username
PG_PASS = pg.password

DATA_PATH = f"s3://{S3_BUCKET}/ducklake/raw/"
PG_CONN = f"dbname={PG_DB} host={PG_HOST} port={PG_PORT} user={PG_USER} password={PG_PASS} sslmode=require"

con = duckdb.connect()

for ext in ["ducklake", "postgres", "httpfs"]:
    con.execute(f"INSTALL {ext}; LOAD {ext};")

# Resolve AWS SSO creds via boto3 (run `aws sso login --profile $AWS_PROFILE` first)
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

con.execute(f"""
    CREATE SECRET pg_secret (
        TYPE postgres,
        HOST '{PG_HOST}',
        PORT {PG_PORT},
        DATABASE '{PG_DB}',
        USER '{PG_USER}',
        PASSWORD '{PG_PASS}'
    );
""")

# Attach DuckLake with METADATA_SCHEMA: raw
con.execute(f"""
    ATTACH 'ducklake:postgres:{PG_CONN}' AS raw (
        DATA_PATH '{DATA_PATH}',
        METADATA_SCHEMA 'raw'
    );
""")
con.execute("USE raw;")

print("Connected! Generating realistic SaaS data...")

# --- users (~2,000 rows) ---
con.execute("""
    CREATE OR REPLACE TABLE users AS
    WITH base AS (
        SELECT i AS id FROM generate_series(1, 2000) t(i)
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
            WHEN 0 THEN 'Alice Johnson'
            WHEN 1 THEN 'Bob Smith'
            WHEN 2 THEN 'Carlos Garcia'
            WHEN 3 THEN 'Diana Chen'
            WHEN 4 THEN 'Erik Larsson'
            WHEN 5 THEN 'Fatima Al-Rashid'
            WHEN 6 THEN 'Grace Kim'
            WHEN 7 THEN 'Hiroshi Tanaka'
            WHEN 8 THEN 'Isabella Rossi'
            WHEN 9 THEN 'James O''Brien'
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
            WHEN 0 THEN 'US'
            WHEN 1 THEN 'GB'
            WHEN 2 THEN 'DE'
            WHEN 3 THEN 'CA'
            WHEN 4 THEN 'AU'
            WHEN 5 THEN 'FR'
            WHEN 6 THEN 'JP'
            ELSE 'BR'
        END AS country_code
    FROM base;
""")

# --- events (~50,000 rows, spanning ~6 months) ---
con.execute("""
    CREATE OR REPLACE TABLE events AS
    WITH base AS (
        SELECT i AS id FROM generate_series(1, 50000) t(i)
    )
    SELECT
        id,
        (id % 2000) + 1 AS user_id,
        CASE id % 6
            WHEN 0 THEN 'page_view'
            WHEN 1 THEN 'signup'
            WHEN 2 THEN 'upgrade'
            WHEN 3 THEN 'downgrade'
            WHEN 4 THEN 'feature_use'
            ELSE 'logout'
        END AS event_name,
        TIMESTAMP '2025-10-01' + INTERVAL (id * 317 % (180 * 86400)) SECOND AS event_timestamp,
        CASE id % 6
            WHEN 0 THEN '{"page": "/' ||
                CASE id % 5
                    WHEN 0 THEN 'home'
                    WHEN 1 THEN 'pricing'
                    WHEN 2 THEN 'dashboard'
                    WHEN 3 THEN 'settings'
                    ELSE 'docs'
                END || '", "referrer": "' ||
                CASE id % 4
                    WHEN 0 THEN 'https://google.com'
                    WHEN 1 THEN 'https://twitter.com'
                    WHEN 2 THEN 'direct'
                    ELSE 'https://github.com'
                END || '"}'
            WHEN 1 THEN '{"source": "' ||
                CASE id % 3 WHEN 0 THEN 'organic' WHEN 1 THEN 'referral' ELSE 'paid' END ||
                '"}'
            WHEN 2 THEN '{"from_plan": "free", "to_plan": "' ||
                CASE id % 2 WHEN 0 THEN 'pro' ELSE 'enterprise' END || '"}'
            WHEN 3 THEN '{"from_plan": "' ||
                CASE id % 2 WHEN 0 THEN 'pro' ELSE 'enterprise' END || '", "to_plan": "free"}'
            WHEN 4 THEN '{"feature": "' ||
                CASE id % 4
                    WHEN 0 THEN 'export_csv'
                    WHEN 1 THEN 'create_report'
                    WHEN 2 THEN 'invite_member'
                    ELSE 'api_call'
                END || '"}'
            ELSE '{"session_duration_sec": ' || (id % 3600)::TEXT || '}'
        END AS properties
    FROM base;
""")

# --- invoices (~5,000 rows) ---
con.execute("""
    CREATE OR REPLACE TABLE invoices AS
    WITH base AS (
        SELECT i AS id FROM generate_series(1, 5000) t(i)
    )
    SELECT
        id,
        (id % 2000) + 1 AS user_id,
        ROUND(
            CASE
                WHEN id % 5 < 3 THEN 9.99 + (id % 20) * 0.5
                WHEN id % 5 < 4 THEN 49.99 + (id % 50)
                ELSE 199.99 + (id % 100) * 2
            END, 2
        )::DECIMAL(10,2) AS amount,
        'USD' AS currency,
        CASE id % 10
            WHEN 0 THEN 'pending'
            WHEN 1 THEN 'failed'
            WHEN 2 THEN 'refunded'
            ELSE 'paid'
        END AS status,
        DATE '2025-10-01' + INTERVAL (id * 131 % 180) DAY AS issued_at,
        CASE
            WHEN id % 10 IN (0, 1) THEN NULL
            ELSE DATE '2025-10-01' + INTERVAL (id * 131 % 180 + id % 14) DAY
        END AS paid_at
    FROM base;
""")

# --- support_tickets (~1,500 rows) ---
con.execute("""
    CREATE OR REPLACE TABLE support_tickets AS
    WITH base AS (
        SELECT i AS id FROM generate_series(1, 1500) t(i)
    )
    SELECT
        id,
        (id % 2000) + 1 AS user_id,
        CASE id % 8
            WHEN 0 THEN 'Cannot export data'
            WHEN 1 THEN 'Billing discrepancy'
            WHEN 2 THEN 'Login issues after upgrade'
            WHEN 3 THEN 'Feature request: dark mode'
            WHEN 4 THEN 'API rate limit too low'
            WHEN 5 THEN 'Dashboard not loading'
            WHEN 6 THEN 'Team invitation failed'
            ELSE 'Data sync delay'
        END AS subject,
        CASE id % 4
            WHEN 0 THEN 'low'
            WHEN 1 THEN 'medium'
            WHEN 2 THEN 'high'
            ELSE 'critical'
        END AS priority,
        CASE id % 4
            WHEN 0 THEN 'open'
            WHEN 1 THEN 'in_progress'
            WHEN 2 THEN 'resolved'
            ELSE 'closed'
        END AS status,
        DATE '2025-10-01' + INTERVAL (id * 73 % 180) DAY AS created_at,
        CASE
            WHEN id % 4 IN (0, 1) THEN NULL
            ELSE DATE '2025-10-01' + INTERVAL (id * 73 % 180 + 1 + id % 7) DAY
        END AS resolved_at
    FROM base;
""")

for table in ["users", "events", "invoices", "support_tickets"]:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table}: {count} rows")

print("\nSnapshots:")
for row in con.execute("FROM raw.snapshots();").fetchall():
    print(f"  {row}")

print("\nDone! Data is on S3, metadata is in Postgres (schema: raw).")
