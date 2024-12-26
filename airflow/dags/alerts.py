from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_alerts():
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
    )

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM analytics.alerts_model;")
    alert_count = cursor.fetchone()[0]
    conn.close()

    if alert_count > 0:
        return True
    else:
        return False


with DAG(
    "alert_pipeline",
    default_args=default_args,
    description="Check and notify on alerts",
    schedule_interval="@hourly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Check if alerts exist
    check_alerts_task = PythonOperator(
        task_id="check_alerts", python_callable=check_alerts
    )

    # Notify if alerts exist
    notify_alerts = SlackWebhookOperator(
        task_id="notify_alerts",
        slack_webhook_conn_id="slack_webhook",
        message="New alerts detected in dbt's alerts_model.",
        channel="#alerts",
    )

    check_alerts_task >> notify_alerts
