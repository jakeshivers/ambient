import random
import time
import json
from confluent_kafka import Producer


def read_config():
    """
    Reads Kafka client configuration from a properties file.
    """
    config = {}
    with open("client.properties", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.split("=", 1)
                config[parameter.strip()] = value.strip()
    return config


def generate_iot_data():
    """Generate synthetic IoT sensor data."""
    sensors = [f"sensor_{i}" for i in range(1, 500)]  # Simulate 500 sensors
    sensor_types = [
        "motion",
        "temperature",
        "air_quality",
        "doorbell",
        "water_leak",
        "humidity",
        "light",
        "excessive_airconditioning",
    ]

    data = {
        "sensor_id": random.choice(sensors),
        "timestamp": time.time(),
        "sensor_type": random.choice(sensor_types),
    }

    if data["sensor_type"] == "motion":
        data["value"] = random.choice([0, 1])
        data["duration"] = random.uniform(0, 5) if data["value"] else 0
    elif data["sensor_type"] == "temperature":
        data["value"] = round(random.uniform(15, 35), 2)
        if random.random() < 0.05:  # 5% chance of faulty temperature
            data["value"] = random.choice([-10, 60])  # Extreme outliers
    elif data["sensor_type"] == "air_quality":
        data["value"] = round(random.uniform(0, 500), 2)
    elif data["sensor_type"] == "doorbell":
        data["value"] = random.randint(0, 10)
    elif data["sensor_type"] == "water_leak":
        data["value"] = random.choice([0, 1])
        data["severity"] = random.uniform(0, 1) if data["value"] else 0
    elif data["sensor_type"] == "humidity":
        data["value"] = round(random.uniform(20, 80), 2)
        if random.random() < 0.02:  # 2% chance of faulty humidity
            data["value"] = random.choice([0, 120])  # Unrealistic values
    elif data["sensor_type"] == "light":
        data["value"] = random.randint(0, 1000)
    elif data["sensor_type"] == "excessive_airconditioning":
        if random.randint(0, 100) >= 95:  # 5% chance of False
            data["value"] = False
        else:
            data["value"] = True

    return data


def produce(topic, config):
    """
    Produces real-time IoT data to the specified Kafka topic.
    """
    producer = Producer(config)
    try:
        print(f"Starting producer for topic '{topic}'...")
        while True:
            iot_data = generate_iot_data()
            key = iot_data["sensor_id"]
            value = json.dumps(iot_data)

            producer.produce(topic, key=key, value=value)
            print(f"Produced message to topic {topic}: {value}")

            # Simulate real-time streaming with a short delay
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("Producer interrupted. Exiting...")
    finally:
        producer.flush()


def main():
    """
    Main function for the IoT producer.
    """
    config = read_config()
    topic = "iot_data"
    produce(topic, config)


if __name__ == "__main__":
    main()
