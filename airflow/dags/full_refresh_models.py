# dags/dbt_refresh_fact_payment.py
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

ENV = {
    "SNOWFLAKE_ACCOUNT":   Variable.get("SNOWFLAKE_ACCOUNT"),
    "SNOWFLAKE_USER":      Variable.get("SNOWFLAKE_USER"),
    "SNOWFLAKE_PASSWORD":  Variable.get("SNOWFLAKE_PASSWORD"),
    "SNOWFLAKE_ROLE":      Variable.get("SNOWFLAKE_ROLE", default_var="TRANSFORMER"),
    "SNOWFLAKE_DATABASE":  Variable.get("SNOWFLAKE_DATABASE", default_var="DEEL_ANALYTICS"),
    "SNOWFLAKE_WAREHOUSE": Variable.get("SNOWFLAKE_WAREHOUSE", default_var="TRANSFORMING"),
    # 🔽 add the schema envs your dbt config reads via env_var()
    "SNOWFLAKE_SCHEMA_RAW":     Variable.get("SNOWFLAKE_SCHEMA_RAW", default_var="RAW"),
    "SNOWFLAKE_SCHEMA_CURATED": Variable.get("SNOWFLAKE_SCHEMA_CURATED", default_var="CURATED"),
    "SNOWFLAKE_SCHEMA_MARTS":   Variable.get("SNOWFLAKE_SCHEMA_MARTS", default_var="MARTS"),
    "SCHEMA_SUFFIX":            Variable.get("SCHEMA_SUFFIX", default_var=""),

    "DBT_PROFILES_DIR": "/opt/project",
    "DBT_LOG_PATH":     "/opt/airflow/dbt/dbt_logs",
    "DBT_TARGET_PATH":  "/opt/airflow/dbt/dbt_target",
    "DBT_PACKAGES_PATH":"/opt/airflow/dbt/dbt_packages",
    "PATH": "/home/airflow/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
}

with DAG(
    dag_id="full_refresh_models",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # manual
    catchup=False,
    params={
        "full_refresh": Param(default=True, type="boolean", description="Use --full-refresh"),
        "select": Param(default="fact_organizations_payment+", type="string"),
    },
    tags=["dbt", "maintenance"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            'mkdir -p "$DBT_LOG_PATH" "$DBT_TARGET_PATH" "$DBT_PACKAGES_PATH" && '
            'cd /opt/project && dbt deps'
        ),
        env=ENV,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_fact",
        bash_command=(
            'mkdir -p "$DBT_LOG_PATH" "$DBT_TARGET_PATH" "$DBT_PACKAGES_PATH" && '
            'cd /opt/project && '
            'dbt run --target prod '
            '--select "{{ params.select }}" '
            '{% if params.full_refresh %} --full-refresh {% endif %}'
        ),
        env=ENV,
    )

    dbt_deps >> dbt_run
