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


def generate_random_coordinates():
    """
    Generates random latitude and longitude within North America.
    """
    # Define city ranges
    cities = {
        "San Francisco, CA": {
            "lat_range": (37.3, 37.8),
            "lon_range": (-122.5, -122.32),
        },
        "Miami, FL": {"lat_range": (25.7, 25.8), "lon_range": (-80.3, -80.1)},
        "Denver, CO": {"lat_range": (39.6, 39.9), "lon_range": (-105.1, -104.8)},
        "Salt Lake City, UT": {
            "lat_range": (40.6, 40.8),
            "lon_range": (-112.0, -111.7),
        },
        "New York, NY": {"lat_range": (40.7, 40.8), "lon_range": (-74.0, -73.9)},
    }

    # Randomly pick a city
    city = random.choice(list(cities.keys()))
    lat_range = cities[city]["lat_range"]
    lon_range = cities[city]["lon_range"]

    # Generate random lat/lon within the selected city's range
    lat = random.uniform(lat_range[0], lat_range[1])
    lon = random.uniform(lon_range[0], lon_range[1])

    return round(lat, 6), round(lon, 6)


def generate_iot_data():
    """Generate enhanced IoT sensor data."""
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
        "door_movement",
        "lock_status",
        "co2_sensor",
    ]

    lat, lon = generate_random_coordinates()
    manufacturers = [
        "GE",
        "Wiliot",
        "Samsung",
        "Philips",
        "Benji",
        "Bastille",
        "Honeywell",
    ]  # Example device models
    models = [
        "Model S",
        "Sleep Easy",
        "Night Watch",
        "v2",
        "Model P",
    ]  # Example manufacturers

    data = {
        "sensor_id": random.choice(sensors),
        "timestamp": time.time(),
        "sensor_type": random.choice(sensor_types),
        "latitude": lat,
        "longitude": lon,
        "city": "Unknown",  # Placeholder for city info (can be enhanced with APIs)
        "state": "Unknown",  # Placeholder for state info
        "device_model": random.choice(models),
        "manufacturer": random.choice(manufacturers),
        "firmware_version": f"v{random.randint(1, 5)}.{random.randint(0, 9)}",
    }

    # Sensor-specific value generation
    value_generators = {
        "motion": lambda: {
            "value": random.choice([0, 1]),
            "duration": random.uniform(0, 5) if random.choice([0, 1]) else 0,
        },
        "door_movement": lambda: {"value": 1 if random.random() < 0.1 else 0},
        "lock_status": lambda: {"value": 1 if random.random() < 0.05 else 0},
        "temperature": lambda: {"value": round(random.uniform(15, 35), 2)},
        "air_quality": lambda: {"value": round(random.uniform(0, 500), 2)},
        "doorbell": lambda: {"value": random.randint(0, 10)},
        "water_leak": lambda: {
            "value": random.choice([0, 1]),
            "severity": random.uniform(0, 1) if random.choice([0, 1]) else 0,
        },
        "humidity": lambda: {"value": round(random.uniform(20, 80), 2)},
        "light": lambda: {"value": random.randint(0, 1000)},
        "excessive_airconditioning": lambda: {
            "value": True if random.choice([True, False]) else False,
            "energy_usage": (
                round(random.uniform(750, 1000), 2)
                if random.choice([True, False])
                else 0
            ),
            "duration": random.uniform(18, 24) if random.choice([True, False]) else 0,
        },
        "co2_sensor": lambda: {"value": round(random.uniform(300, 2000), 2)},
    }

    # Default to an empty dictionary if sensor_type isn't explicitly handled
    sensor_specific_data = value_generators.get(data["sensor_type"], lambda: {})()
    data.update(sensor_specific_data)

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
