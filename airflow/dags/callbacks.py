"""
Failure callback that posts to the same Slack Incoming Webhook connection
used by the DAG (`slack_webhook`)
"""
from airflow.hooks.base import BaseHook
import requests


def on_task_fail(context) -> None:
    try:
        conn = BaseHook.get_connection("slack_webhook")
        webhook = (conn.extra_dejson or {}).get("webhook_token")
        if not webhook:
            return

        ti = context.get("task_instance")
        dag = context.get("dag")
        dag_id = getattr(dag, "dag_id", "unknown_dag")
        task_id = getattr(ti, "task_id", "task")
        run_id = context.get("run_id", "unknown_run")
        log_url = getattr(ti, "log_url", "")

        text = (
            f":x: *Airflow Failure* — {dag_id}.{task_id} run {run_id}\n"
            f"{log_url}"
        )
        requests.post(webhook, json={"text": text}, timeout=10)
    except Exception:
        pass
