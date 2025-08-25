from datetime import datetime
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from callbacks import on_task_fail
import snowflake.connector as sf

# ---- Environment for dbt shell tasks ----
ENV = {
    "SNOWFLAKE_ACCOUNT": Variable.get("SNOWFLAKE_ACCOUNT"),
    "SNOWFLAKE_USER": Variable.get("SNOWFLAKE_USER"),
    "SNOWFLAKE_PASSWORD": Variable.get("SNOWFLAKE_PASSWORD"),
    "SNOWFLAKE_ROLE": Variable.get("SNOWFLAKE_ROLE", default_var="TRANSFORMER"),
    "SNOWFLAKE_DATABASE": Variable.get("SNOWFLAKE_DATABASE", default_var="DEEL_ANALYTICS"),
    "SNOWFLAKE_WAREHOUSE": Variable.get("SNOWFLAKE_WAREHOUSE", default_var="TRANSFORMING"),
    "SNOWFLAKE_SCHEMA_RAW": Variable.get("SNOWFLAKE_SCHEMA_RAW", default_var="RAW"),
    "SNOWFLAKE_SCHEMA_CURATED": Variable.get("SNOWFLAKE_SCHEMA_CURATED", default_var="CURATED"),
    "SNOWFLAKE_SCHEMA_MARTS": Variable.get("SNOWFLAKE_SCHEMA_MARTS", default_var="MARTS"),
    "SCHEMA_SUFFIX": Variable.get("SCHEMA_SUFFIX", default_var=os.getenv("SCHEMA_SUFFIX", "")),
    "ALERT_THRESHOLD_PCT": Variable.get("ALERT_THRESHOLD_PCT", default_var="0.5"),
    "ALERT_MIN_ABSOLUTE_DELTA": Variable.get("ALERT_MIN_ABSOLUTE_DELTA", default_var="0"),
    "DBT_PROFILES_DIR": "/opt/project",
    "DBT_QUERY_TAG": "deel_analytics",
    "DBT_LOG_PATH": "/opt/airflow/dbt/dbt_logs",
    "DBT_TARGET_PATH": "/opt/airflow/dbt/dbt_target",
    "DBT_PACKAGES_PATH": "/opt/airflow/dbt/dbt_packages",
    "PATH": "/home/airflow/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
}

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": 300,
    "on_failure_callback": on_task_fail,
}

with DAG(
    dag_id="deel_analytics_daily",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["deel", "analytics"],
) as dag:

    # dbt deps -> ensures packages (e.g., dbt_utils) are available
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command='mkdir -p "$DBT_LOG_PATH" "$DBT_TARGET_PATH" "$DBT_PACKAGES_PATH" && cd /opt/project && dbt deps',
        env=ENV,
    )

    # dbt build -> runs all models/tests (produces MART_DAILY_ORG_BALANCE)
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command='mkdir -p "$DBT_LOG_PATH" "$DBT_TARGET_PATH" "$DBT_PACKAGES_PATH" && cd /opt/project && dbt build --target prod',
        env=ENV,
    )

    @task(task_id="compute_alert_messages")
    def compute_alert_messages() -> list[str]:
        # read settings
        account   = Variable.get("SNOWFLAKE_ACCOUNT")
        user      = Variable.get("SNOWFLAKE_USER")
        password  = Variable.get("SNOWFLAKE_PASSWORD")
        role      = Variable.get("SNOWFLAKE_ROLE", default_var="TRANSFORMER")
        database  = Variable.get("SNOWFLAKE_DATABASE", default_var="DEEL_ANALYTICS")
        warehouse = Variable.get("SNOWFLAKE_WAREHOUSE", default_var="TRANSFORMING")
        schema_marts = Variable.get("SNOWFLAKE_SCHEMA_MARTS", default_var="MARTS") + \
                       Variable.get("SCHEMA_SUFFIX", default_var="")

        th = float(Variable.get("ALERT_THRESHOLD_PCT", default_var="0.5"))
        min_abs = float(Variable.get("ALERT_MIN_ABSOLUTE_DELTA", default_var="0"))

        con = sf.connect(
            account=account, user=user, password=password,
            role=role, warehouse=warehouse, database=database,
        )
        cur = con.cursor()
        try:
            # Use latest date already materialized by dbt in the mart
            cur.execute(f'''
                SELECT MAX("DATE")
                FROM "{database}"."{schema_marts}"."MART_DAILY_ORG_BALANCE"
            ''')
            row = cur.fetchone()
            if not row or not row[0]:
                return []

            target_date = row[0]  # Snowflake DATE

            # Filter directly on the mart's own columns
            sql = f"""
                with cfg as (
                  select
                    to_date('{target_date}') as alert_date,
                    {th}::float  as threshold_pct,
                    {min_abs}::float as min_abs
                )
                select
                  m."ORGANIZATION_ID",
                  m."DATE",
                  m."DAILY_PAYMENT_AMOUNT",
                  m."PREV_DAILY_PAYMENT_AMOUNT",
                  m."DOD_CHANGE_PCT" as PCT_CHANGE,
                  (m."DAILY_PAYMENT_AMOUNT" - coalesce(m."PREV_DAILY_PAYMENT_AMOUNT", 0)) as ABS_CHANGE
                from "{database}"."{schema_marts}"."MART_DAILY_ORG_BALANCE" m, cfg
                where m."DATE" = cfg.alert_date
                  and (
                        (ABS_CHANGE >= cfg.min_abs and (PCT_CHANGE is null or PCT_CHANGE >= cfg.threshold_pct))
                     or (ABS_CHANGE <= -cfg.min_abs and (PCT_CHANGE is null or PCT_CHANGE <= -cfg.threshold_pct))
                  )
                order by abs(ABS_CHANGE) desc, m."ORGANIZATION_ID"
            """
            cur.execute(sql)
            rows = cur.fetchall()

            msgs: list[str] = []
            for org_id, dt, today_amt, prev_amt, pct, abs_change in rows:
                pct_str = "n/a" if pct is None else f"{pct:.1%}"
                msgs.append(
                    ":rotating_light: *Balance Change Alert*\n"
                    f"• Org: `{org_id}`\n"
                    f"• Date: {dt}\n"
                    f"• Today's Amount: {today_amt}\n"
                    f"• Previous Day: {prev_amt}\n"
                    f"• DoD Change: *{pct_str}* (Δ={abs_change:,.2f})"
                )
            return msgs
        finally:
            try:
                cur.close(); con.close()
            except Exception:
                pass

    alert_msgs = compute_alert_messages()

    post_alerts = SlackWebhookOperator.partial(
        task_id="post_alerts",
        slack_webhook_conn_id="slack_webhook",
    ).expand(message=alert_msgs)

    dbt_deps >> dbt_build >> alert_msgs >> post_alerts
