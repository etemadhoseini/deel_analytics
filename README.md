# Deel Analytics (dbt + Snowflake + Airflow + Slack)

A compact analytics engineering setup for Deel: Snowflake as the warehouse, dbt for transforms, Airflow for orchestration, and optional Slack alerts.

---

## Layers & Schemas

* **RAW** – CSV landing (internal stage → COPY → MERGE) for a single file: `data/invoices.csv`
* **CURATED** – typed staging + dimensions/facts (dbt materialized tables)
* **MARTS** – daily organization payment mart used for alerting/BI (`MART_DAILY_ORG_PAYMENTS`)

> Schemas can use an optional suffix via `SCHEMA_SUFFIX` (e.g., `_DEV` or empty for prod).

---


## Prerequisites

* Docker Desktop (Windows/macOS/Linux)
* Snowflake account (user/role/warehouse/database; see bootstrap SQL in repo)
* (Optional) Slack Incoming Webhook URL if you want Slack alerts

---

## Quick Start

### 1) Configure environment

```bash
cp .env.example .env
# Open .env and fill in SNOWFLAKE_* values and other settings you use.
```

### 2) Start services & bootstrap Airflow

> If you change anything under `airflow/bootstrap/`, re-run the init step.

```bash
# Pull images (or build if you customized Dockerfile)
docker compose pull

# Initialize Airflow DB, variables, and connections via bootstrap scripts
docker compose run --rm airflow-init

# Start services
docker compose up -d postgres airflow-scheduler airflow-webserver

# Airflow UI → http://localhost:8080  (default admin/admin unless overridden)
```

If you aren’t importing variables automatically, set these in **Airflow → Admin → Variables**:

`SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE, SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_SCHEMA_RAW, SNOWFLAKE_SCHEMA_CURATED, SNOWFLAKE_SCHEMA_MARTS, SCHEMA_SUFFIX`.

For Slack alerts, create a **Connection** named `slack_webhook` with your Incoming Webhook URL.

---

## Load data to RAW

Place your files at:

```
data/invoices.csv
data/organizations.csv
```

Then load it:

```bash
python scripts/load_csv_to_snowflake.py
```

---

## Build dbt models & tests

Run locally (with your `profiles.yml`) **or** inside the Airflow container.

**Inside Airflow container**

```bash
docker compose exec airflow-scheduler bash -lc '
  cd /opt/project
  dbt deps
  dbt build --target prod
'
```

**Locally**

```bash
dbt deps
dbt build
```

---

## Daily DAG & Alerts

* **DAG**: `deel_analytics_daily`

  * Runs `dbt deps` → `dbt build`
  * Computes alert candidates from `MART_DAILY_ORG_PAYMENTS` and posts via Slack (optional).
* Ensure Snowflake roles/privileges allow reading mart tables from Airflow.

Trigger from the UI or let it run on schedule.

---


## Full-refresh a specific model

DAG provided to full-refresh `FACT_ORGANIZATIONS_PAYMENT`:

```bash
# In Airflow UI, trigger: dbt_refresh_fact_payment
# It runs (inside the container):
#   dbt deps
#   dbt run --full-refresh --select FACT_ORGANIZATIONS_PAYMENT --target prod
```

---

## Docs

```bash
dbt docs generate
dbt docs serve
```

---

## Troubleshooting

* **Missing env/vars**: Airflow logs showing “Env var required but not provided” → set the corresponding Airflow Variable.
* **Permissions**: Ensure Snowflake roles (e.g., `TRANSFORMER`) have USAGE on warehouse/database and CREATE/SELECT on schemas per the bootstrap SQL.
* **Schema suffix**: If using `SCHEMA_SUFFIX`, confirm your dbt and Airflow settings both include it so model/schema names match.
