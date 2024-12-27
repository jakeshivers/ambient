import json
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DBT_PATH = "/home/shivers/data_engineering/Ambient/dbt_project/iot_data_project/target/"


# Function to parse manifest.json and extract model dependencies
def get_dbt_tasks(manifest_path):
    with open(manifest_path) as f:
        manifest = json.load(f)

    # Filter models
    models = {
        node: details
        for node, details in manifest["nodes"].items()
        if details["resource_type"] == "model"
    }
    return models


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dbt_manifest_rebuild_pipeline",
    default_args=default_args,
    description="Rebuild dbt manifest and run models dynamically",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Step 1: Rebuild the manifest.json
    rebuild_manifest = BashOperator(
        task_id="rebuild_manifest",
        bash_command=f"cd {DBT_PATH} && dbt compile",
    )

    # Step 2: Dynamically create tasks from manifest.json
    manifest_path = f"{DBT_PATH}/manifest.json"
    dbt_tasks = get_dbt_tasks(manifest_path)

    # Dictionary to hold Airflow tasks for dependency resolution
    airflow_tasks = {}

    for model_name, model_details in dbt_tasks.items():
        task = BashOperator(
            task_id=f"dbt_run_{model_name}",
            bash_command=f"cd {DBT_PATH} && dbt run --select {model_name}",
        )
        airflow_tasks[model_name] = task

        # Ensure the manifest is rebuilt before any dbt tasks
        rebuild_manifest >> task

    # Step 3: Define dependencies between dbt tasks
    for model_name, model_details in dbt_tasks.items():
        dependencies = model_details["depends_on"]["nodes"]
        for dep in dependencies:
            if dep in airflow_tasks:
                airflow_tasks[dep] >> airflow_tasks[model_name]
