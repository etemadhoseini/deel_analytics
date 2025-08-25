import os
import subprocess
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector as sf

load_dotenv()

ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
USER      = os.environ["SNOWFLAKE_USER"]
PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
ROLE      = os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER")
DB        = os.environ.get("SNOWFLAKE_DATABASE", "DEEL_ANALYTICS")
WH        = os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORMING")
SUFFIX    = os.environ.get("SCHEMA_SUFFIX", "")
SCHEMA_RAW= os.environ.get("SNOWFLAKE_SCHEMA_RAW", "RAW") + SUFFIX

ORG_CSV  = Path(os.environ.get("LOCAL_ORGANIZATIONS_CSV","./data/organizations.csv")).resolve()
INV1_CSV = Path(os.environ.get("LOCAL_INVOICES_CSV_1","./data/invoices.csv")).resolve()
INV2_ENV = os.environ.get("LOCAL_INVOICES_CSV_2","").strip()
INV2_CSV = Path(INV2_ENV).resolve() if INV2_ENV else None

SNOWSQL = os.environ.get("SNOWSQL_PATH","snowsql")  # path to snowsql.exe on Windows if not on PATH

# --- Connect as TRANSFORMER and USE existing objects (don't CREATE DATABASE) ---
con = sf.connect(
    account=ACCOUNT,
    user=USER,
    password=PASSWORD,
    role=ROLE,
    warehouse=WH,
    database=DB,
)
cur = con.cursor()

print("Setting context…")
# Use existing DB & SCHEMA provisioned by bootstrap
cur.execute(f'USE DATABASE "{DB}"')
cur.execute(f'USE SCHEMA "{DB}"."{SCHEMA_RAW}"')

# Create convenience objects if missing (you have CREATE on RAW_*)
cur.execute("""
    create file format if not exists csv_ff
      type = csv
      field_optionally_enclosed_by = '\"'
      skip_header = 1
      null_if = ('', 'NULL')
""")
cur.execute("create stage if not exists raw_stage")

print("Ensuring RAW tables…")
cur.execute(f"""
    create table if not exists "{DB}"."{SCHEMA_RAW}".ORGANIZATIONS (
        ORGANIZATION_ID number,
        FIRST_PAYMENT_DATE string,
        LAST_PAYMENT_DATE string,
        LEGAL_ENTITY_COUNTRY_CODE string,
        COUNT_TOTAL_CONTRACTS_ACTIVE number,
        CREATED_DATE string
    )
""")
cur.execute(f"""
    create table if not exists "{DB}"."{SCHEMA_RAW}".INVOICES (
        INVOICE_ID string,
        PARENT_INVOICE_ID string,
        TRANSACTION_ID string,
        ORGANIZATION_ID number,
        TYPE string,
        STATUS string,
        CURRENCY string,
        PAYMENT_CURRENCY string,
        PAYMENT_METHOD string,
        AMOUNT float,
        PAYMENT_AMOUNT float,
        FX_RATE float,
        FX_RATE_PAYMENT float,
        CREATED_AT string
    )
""")

cur.execute("create or replace table TMP_ORGANIZATIONS like ORGANIZATIONS")
cur.execute("create or replace table TMP_INVOICES like INVOICES")

def run_put(p: Path):
    print(f"PUT: {p}")
    if not p.exists():
        raise SystemExit(f"Not found: {p}")

    cmd = [
        SNOWSQL,
        "-a", ACCOUNT,
        "-u", USER,
        "-r", ROLE,
        "-w", WH,
        "-d", DB,
        "-s", SCHEMA_RAW,
        "-q",
        f'PUT file://{p.as_posix()} @"{DB}"."{SCHEMA_RAW}".raw_stage auto_compress=true overwrite=true'
    ]

    # pass password via environment (supported by SnowSQL)
    env = os.environ.copy()
    env["SNOWSQL_PWD"] = PASSWORD

    subprocess.check_call(cmd, env=env)

# Upload local CSVs to the stage
run_put(ORG_CSV)
run_put(INV1_CSV)
if INV2_CSV and INV2_CSV.exists():
    run_put(INV2_CSV)

print("COPY to TMP tables…")
cur.execute("truncate table TMP_ORGANIZATIONS")
cur.execute("truncate table TMP_INVOICES")
cur.execute("copy into TMP_ORGANIZATIONS from @raw_stage pattern='.*organizations.*(csv|csv.gz)$' file_format=csv_ff")
cur.execute("copy into TMP_INVOICES     from @raw_stage pattern='.*invoices.*(csv|csv.gz)$'     file_format=csv_ff")

print("MERGE into base tables…")
cur.execute(f"""
    merge into "{DB}"."{SCHEMA_RAW}".ORGANIZATIONS t
    using (select * from TMP_ORGANIZATIONS) s
    on t.ORGANIZATION_ID = s.ORGANIZATION_ID
    when matched then update set
      FIRST_PAYMENT_DATE=s.FIRST_PAYMENT_DATE,
      LAST_PAYMENT_DATE=s.LAST_PAYMENT_DATE,
      LEGAL_ENTITY_COUNTRY_CODE=s.LEGAL_ENTITY_COUNTRY_CODE,
      COUNT_TOTAL_CONTRACTS_ACTIVE=s.COUNT_TOTAL_CONTRACTS_ACTIVE,
      CREATED_DATE=s.CREATED_DATE
    when not matched then insert values
      (s.ORGANIZATION_ID,s.FIRST_PAYMENT_DATE,s.LAST_PAYMENT_DATE,s.LEGAL_ENTITY_COUNTRY_CODE,s.COUNT_TOTAL_CONTRACTS_ACTIVE,s.CREATED_DATE)
""")
cur.execute(f"""
    merge into "{DB}"."{SCHEMA_RAW}".INVOICES t
    using (select * from TMP_INVOICES) s
    on coalesce(t.INVOICE_ID,'')=coalesce(s.INVOICE_ID,'')
       and coalesce(t.TRANSACTION_ID,'')=coalesce(s.TRANSACTION_ID,'')
    when matched then update set
      PARENT_INVOICE_ID=s.PARENT_INVOICE_ID,
      ORGANIZATION_ID=s.ORGANIZATION_ID,
      TYPE=s.TYPE,
      STATUS=s.STATUS,
      CURRENCY=s.CURRENCY,
      PAYMENT_CURRENCY=s.PAYMENT_CURRENCY,
      PAYMENT_METHOD=s.PAYMENT_METHOD,
      AMOUNT=s.AMOUNT,
      PAYMENT_AMOUNT=s.PAYMENT_AMOUNT,
      FX_RATE=s.FX_RATE,
      FX_RATE_PAYMENT=s.FX_RATE_PAYMENT,
      CREATED_AT=s.CREATED_AT
    when not matched then insert values
      (s.INVOICE_ID,s.PARENT_INVOICE_ID,s.TRANSACTION_ID,s.ORGANIZATION_ID,s.TYPE,s.STATUS,s.CURRENCY,s.PAYMENT_CURRENCY,s.PAYMENT_METHOD,s.AMOUNT,s.PAYMENT_AMOUNT,s.FX_RATE,s.FX_RATE_PAYMENT,s.CREATED_AT)
""")

cur.close()
con.close()
print("RAW ingestion completed.")
