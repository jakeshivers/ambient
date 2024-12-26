import json
import os
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer
import boto3
import snowflake.connector
from dotenv import load_dotenv


load_dotenv()


def read_config():
    """
    Reads Kafka client configuration from a properties file.
    """
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.split("=", 1)
                config[parameter.strip()] = value.strip()
    return config


def save_to_parquet(data_list, filepath="../../data/raw"):
    """
    Saves processed IoT data to a Parquet file.
    """
    table = pa.Table.from_pylist(data_list)
    pq.write_table(table, filepath)


def upload_to_s3(filepath, bucket_name, object_name):
    """
    Uploads a file to S3.
    """
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_file(filepath, bucket_name, object_name)
        print(f"Uploaded {filepath} to s3://{bucket_name}/{object_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")


def insert_into_snowflake(data_list, table_name):
    """
    Inserts processed IoT data into Snowflake.
    """
    try:
        conn = snowflake.connector.connect(
            user=os.environ.get("SNOWFLAKE_USER"),
            password=os.environ.get("SNOWFLAKE_PASSWORD"),
            account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            database=os.environ.get("SNOWFLAKE_DATABASE"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA"),
        )
        cursor = conn.cursor()

        # Updated query to include LATITUDE and LONGITUDE
        insert_query = f"""
        INSERT INTO {table_name} (
            SENSOR_ID, SENSOR_TYPE, VALUE, TIMESTAMP, 
            LATITUDE, LONGITUDE, CITY, STATE, DEVICE_MODEL,
            MANUFACTURER, FIRMWARE_VERSION, ENERGY_USAGE,
            DURATION, SEVERITY
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Transform data list into tuples for Snowflake insertion
        tupled_data = [
            (
                record["sensor_id"],
                record["sensor_type"],
                record["value"],
                datetime.fromtimestamp(record["timestamp"]),
                record.get("latitude"),
                record.get("longitude"),
                record.get("city"),
                record.get("state"),
                record.get("device_model"),
                record.get("manufacturer"),
                record.get("firmware_version"),
                record.get("energy_usage"),
                record.get("duration"),
                record.get("severity"),
            )
            for record in data_list
        ]

        # Execute batch insert
        cursor.executemany(insert_query, tupled_data)
        conn.commit()

        print(f"Inserted {len(data_list)} records into Snowflake table {table_name}")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error inserting into Snowflake: {e}")


def normalize_data(record):
    """
    Normalizes a single IoT data record to ensure consistent data types.
    """
    return {
        "sensor_id": record["sensor_id"],
        "sensor_type": record["sensor_type"],
        "value": (
            float(record.get("value"))
            if isinstance(record.get("value"), (int, bool))
            else record.get("value")
        ),
        "timestamp": record["timestamp"],
        "latitude": record.get("latitude"),
        "longitude": record.get("longitude"),
        "city": record.get("city", "Unknown"),
        "state": record.get("state", "Unknown"),
        "device_model": record.get("device_model", "Unknown"),
        "manufacturer": record.get("manufacturer", "Unknown"),
        "firmware_version": record.get("firmware_version", "v1.0"),
        "energy_usage": record.get("energy_usage"),
        "weather_conditions": record.get("weather_conditions", "Unknown"),
        "duration": record.get("duration"),
        "severity": record.get("severity"),
    }


def consume_and_process(topic, config, table_name, s3_bucket=None, s3_folder=None):
    """
    Consumes messages from Kafka, saves locally, uploads to S3, and sends to Snowflake in batches.
    """
    config["group.id"] = "iot_consumer_group"
    config["auto.offset.reset"] = "latest"

    consumer = Consumer(config)
    consumer.subscribe([topic])

    processed_data = []  # Buffer for batch processing
    batch_size = 2000  # Number of records per batch

    try:
        print(f"Starting consumer for topic '{topic}'...")
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            # Parse the Kafka message
            try:
                data = json.loads(msg.value().decode("utf-8"))
                processed_data.append(normalize_data(data))

                # Once we reach the batch size, process the batch
                if len(processed_data) >= batch_size:
                    # Save and upload to S3 (optional)
                    if s3_bucket and s3_folder:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        parquet_file = f"iot_data_{timestamp}.parquet"
                        save_to_parquet(processed_data, parquet_file)

                        s3_key = f"{s3_folder}/{parquet_file}"
                        # upload_to_s3(parquet_file, s3_bucket, s3_key)

                    # Insert into Snowflake
                    # print(processed_data)
                    insert_into_snowflake(processed_data, table_name)

                    # Clear the buffer
                    processed_data = []

            except json.JSONDecodeError:
                print("Malformed message received and skipped.")
    except KeyboardInterrupt:
        print("Consumer interrupted. Exiting...")
    finally:
        consumer.close()


def main():
    """
    Main function for the IoT consumer.
    """
    # Kafka Configuration
    config = read_config()
    topic = "iot_data"

    table_name = "iot_sensor_data"

    # Optional S3 Configuration (if needed)
    s3_bucket = "ambient-iot-data-bucket"
    s3_folder = "raw"

    consume_and_process(topic, config, table_name, s3_bucket, s3_folder)


if __name__ == "__main__":
    main()
