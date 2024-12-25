import io
import polars as pl
import logging
import boto3
import shutil
import os
import uuid
import random
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

s3 = boto3.client("s3")
BUCKET_NAME = "ambient-iot-data-bucket"  # Base bucket name


def generate_iot_data(file_path, sensor_count=100, records=1000):
    """Generate synthetic IoT sensor data and save to parquet."""
    logger.info("Starting IoT data generation...")

    sensor_ids = [f"sensor_{i}" for i in range(1, sensor_count + 1)]
    sensor_types = ["motion", "temperature", "water_leak", "doorbell", "air_quality"]

    data = []
    air_quality_range = range(500)

    for _ in range(records):
        for sensor_id in sensor_ids:
            timestamp = datetime.now()
            sensor_type = random.choice(sensor_types)

            if random.random() < 0.02:  # Generate bad data with a 2% chance
                data.append(
                    [
                        timestamp,
                        sensor_id,
                        sensor_type,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        "BAD",
                    ]
                )
                continue

            if sensor_type == "motion":
                motion_detected = random.choice([0, 1])
                data.append(
                    [
                        timestamp,
                        sensor_id,
                        sensor_type,
                        motion_detected,
                        None,
                        None,
                        None,
                        None,
                        None,
                        "OK",
                    ]
                )
            elif sensor_type == "temperature":
                temperature = round(random.uniform(20, 25), 2)
                humidity = round(random.uniform(30, 60), 2)
                data.append(
                    [
                        timestamp,
                        sensor_id,
                        sensor_type,
                        None,
                        temperature,
                        humidity,
                        None,
                        None,
                        None,
                        "OK",
                    ]
                )
            elif sensor_type == "water_leak":
                leak_detected = random.choice([0, 1])
                data.append(
                    [
                        timestamp,
                        sensor_id,
                        sensor_type,
                        None,
                        None,
                        None,
                        leak_detected,
                        None,
                        None,
                        "OK",
                    ]
                )
            elif sensor_type == "doorbell":
                doorbell_rings = random.randint(0, 5)
                data.append(
                    [
                        timestamp,
                        sensor_id,
                        sensor_type,
                        None,
                        None,
                        None,
                        None,
                        doorbell_rings,
                        None,
                        "OK",
                    ]
                )
            elif sensor_type == "air_quality":
                air_quality_index = random.choice(air_quality_range)
                data.append(
                    [
                        timestamp,
                        sensor_id,
                        sensor_type,
                        None,
                        None,
                        None,
                        None,
                        None,
                        air_quality_index,
                        "OK",
                    ]
                )

    df = pl.DataFrame(
        data,
        schema=[
            "timestamp",
            "sensor_id",
            "sensor_type",
            "motion_detected",
            "temperature",
            "humidity",
            "water_leak_detected",
            "doorbell_rings",
            "air_quality_index",
            "anomaly_status",
        ],
    )

    df.write_parquet(file_path)
    logger.info(f"IoT data generated and saved to {file_path}")


def upload_data_to_s3(file_path):
    """Upload data directly to S3 bucket."""
    try:
        df = pl.read_parquet(file_path)

        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)

        s3_key = f"raw/iot_data_{int(time.time())}.parquet"
        s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=buffer)
        logger.info(f"Uploaded data to S3 bucket '{BUCKET_NAME}' with key '{s3_key}'")
    except Exception as e:
        logger.error(f"Error uploading data to S3: {e}", exc_info=True)


def move_file_to_processed(src_file_path, dest_file_path):
    """Move file from raw to processed directory."""
    try:
        os.makedirs(os.path.dirname(dest_file_path), exist_ok=True)
        shutil.move(src_file_path, dest_file_path)
        logger.info(f"Moved file from {src_file_path} to {dest_file_path}")
    except Exception as e:
        logger.error(f"Error moving file to processed: {e}", exc_info=True)


if __name__ == "__main__":
    RAW_DIR = "../data/raw"
    PROCESSED_DIR = "../data/processed"

    raw_file_name = f"iot_data_{uuid.uuid4().hex[:8]}.parquet"
    raw_file_path = os.path.join(RAW_DIR, raw_file_name)
    processed_file_path = os.path.join(PROCESSED_DIR, raw_file_name)

    try:
        logger.info("Starting IoT data pipeline...")

        generate_iot_data(raw_file_path, sensor_count=200, records=500)
        upload_data_to_s3(raw_file_path)
        move_file_to_processed(raw_file_path, processed_file_path)

        logger.info("IoT data pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
