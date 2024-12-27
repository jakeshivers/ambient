from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

file_path = "../src/kafka"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "kafka_producer_consumer_shutdown_pipeline",
    default_args=default_args,
    description="Start Kafka producer, consumer and shut it down after a timeout",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Step 1: Start Kafka producer
    start_kafka_producer = BashOperator(
        task_id="start_kafka_producer",
        bash_command=f"python {file_path}/kafka_producer.py & echo $! > /tmp/kafka_producer.pid",
    )

    # Step 2: Start Kafka consumer
    start_kafka_consumer = BashOperator(
        task_id="start_kafka_consumer",
        bash_command=f"python {file_path}/kafka_consumer.py & echo $! > /tmp/kafka_consumer.pid",
    )

    # Step 3: Wait for a specified timeout (e.g., 10 minutes)
    wait_task = BashOperator(
        task_id="wait",
        bash_command="sleep 60",  # Wait for 10 minutes
    )

    # Step 4: Kill the Kafka producer process
    stop_kafka_producer = BashOperator(
        task_id="stop_kafka_producer",
        bash_command="kill -9 $(cat /tmp/kafka_producer.pid) && rm /tmp/kafka_producer.pid",
    )

    # Step 4: Kill the Kafka producer process
    stop_kafka_consumer = BashOperator(
        task_id="stop_kafka_consumer",
        bash_command="kill -9 $(cat /tmp/kafka_consumer.pid) && rm /tmp/kafka_consumer.pid",
    )

    # Task dependencies
    (
        [start_kafka_producer, start_kafka_consumer]
        >> wait_task
        >> [stop_kafka_producer, stop_kafka_consumer]
    )
