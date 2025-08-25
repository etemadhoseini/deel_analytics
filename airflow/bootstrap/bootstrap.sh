#!/usr/bin/env bash
set -euo pipefail

echo "[bootstrap] migrating Airflow DB…"
airflow db migrate
airflow connections create-default-connections || true

echo "[bootstrap] creating admin user…"
airflow users create \
  --username "${_AIRFLOW_WWW_USER_USERNAME}" \
  --password "${_AIRFLOW_WWW_USER_PASSWORD}" \
  --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME}" \
  --lastname "${_AIRFLOW_WWW_USER_LASTNAME}" \
  --role Admin \
  --email "${_AIRFLOW_WWW_USER_EMAIL}" \
  || true

if [ -f "/opt/airflow/bootstrap/variables.json" ]; then
  echo "[bootstrap] importing variables…"
  airflow variables import /opt/airflow/bootstrap/variables.json
fi

if [ -f "/opt/airflow/bootstrap/pools.json" ]; then
  echo "[bootstrap] importing pools…"
  airflow pools import /opt/airflow/bootstrap/pools.json
fi

if [ -f "/opt/airflow/bootstrap/connections.yaml" ]; then
  echo "[bootstrap] importing connections.yaml…"
  airflow connections import /opt/airflow/bootstrap/connections.yaml
fi

if [ -n "${SLACK_WEBHOOK_TOKEN:-}" ]; then
  airflow connections delete slack_webhook || true
  airflow connections add slack_webhook \
    --conn-type slackwebhook \
    --conn-password "${SLACK_INCOMING_WEBHOOK_URL}"
fi

echo "[bootstrap] done."
