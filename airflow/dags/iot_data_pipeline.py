from airflow import DAG
from datetime import datetime
import subprocess
from airflow.operators.python import PythonOperator
import boto3


# Function to start the Glue Crawler
def run_glue_crawler(crawler_name):
    print("Starting Glue Crawler...")
    client = boto3.client(
        "glue", region_name="us-west-2"
    )  # Update the region as needed
    try:
        response = client.start_crawler(Name=crawler_name)
        print(f"Crawler {crawler_name} started successfully. Response: {response}")
    except Exception as e:
        print(f"Error starting Glue Crawler: {e}")
        raise


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 20),
    "retries": 1,
}


# Function to generate IoT data
def generate_data():
    print("Starting data generation process...")
    try:
        subprocess.run(["python3", "scripts/generate_data.py"], check=True)
        print("Data generation completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error generating data: {e}")
        raise


# Define the DAG
with DAG(
    "iot_data_pipeline",
    default_args=default_args,
    schedule="@daily",  # Fixed the parameter name
    catchup=False,
    tags=["IoT", "data-pipeline", "glue"],
) as dag:

    # Task to generate data
    generate_data_task = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data,
    )

    # Task to start the Glue Crawler
    run_glue_crawler_task = PythonOperator(
        task_id="run_glue_crawler",
        python_callable=run_glue_crawler,
        op_kwargs={"crawler_name": "iot-data-crawler"},
    )

    # Set task dependencies
    generate_data_task >> run_glue_crawler_task
