from airflow import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime
from airflow.models import Variable

with DAG(
    dag_id="test_slack_webhook",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
) as dag:
    SlackWebhookOperator(
        task_id="hello_slack",
        slack_webhook_conn_id="slack_webhook",
        message=":white_check_mark: Slack webhook works from Airflow!",
    )
