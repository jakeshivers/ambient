from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
import subprocess


def check_for_blank_postal_codes():
    script_path = "/path/to/src/processing/postal_code_enrichment.py"
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Script failed with error: {result.stderr}")
    else:
        print(f"Script output: {result.stdout}")


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "postal_code_enrichment_pipeline",
    default_args=default_args,
    description="Check and notify on alerts",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=True,
) as dag:

    # Check if empty postal codes exist
    check_for_blank_postal_codes_task = PythonOperator(
        task_id="check_unmapped_postal_codes",
        python_callable=check_for_blank_postal_codes,
    )

    check_for_blank_postal_codes_task
