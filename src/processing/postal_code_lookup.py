# https://radar.com/dashboard/maps/api-explorer/geocoding?project=676cb9aaad2192071df692c1&live=false
# curl "https://api.radar.io/v1/geocode/reverse?coordinates=30.792029%2C+-115.891996&layers=postalCode" \
#  -H "Authorization: prj_test_pk_f7a87c401fed08ac0e7470a8bea45fc4ba4e519c"


30.792029, -115.891996


import os
from dotenv import load_dotenv
import requests
import snowflake.connector

# Load environment variables
load_dotenv()

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
)

# Fetch records from Snowflake
select_query = """
    SELECT iot.latitude, iot.longitude
    from iot_sensor_data iot
    left join tbl_postal_code_mapping pc
    on round(iot.latitude, 2) = round(pc.latitude, 2)
    and round(iot.longitude, 2) = round(pc.longitude, 2)
    where iot.postal_code is null
    limit 30
"""
cursor = conn.cursor()
cursor.execute("USE DATABASE ambient_db;")
cursor.execute("USE SCHEMA RAW;")
cursor.execute(select_query)
records = cursor.fetchall()
cursor.close()

# API setup
url = "https://api.radar.io/v1/geocode/reverse"
headers = {"Authorization": os.environ["TEST_RADAR_KEY"]}

# Batch processing
batch_size = 100  # Define batch size
results = []

for i in range(0, len(records), batch_size):
    batch = records[i : i + batch_size]
    for latitude, longitude in batch:
        params = {"coordinates": f"{latitude},{longitude}", "layers": "postalCode"}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            print(f"Response for ({latitude}, {longitude}): {data}")
            postal_code = (
                data.get("addresses", [{}])[0].get("postalCode")
                if data.get("addresses")
                else None
            )
            if postal_code:
                results.append((latitude, longitude, postal_code))
            else:
                print(f"No postal code found for ({latitude}, {longitude}).")
        else:
            print(
                f"API request failed for coordinates ({latitude}, {longitude}) with status {response.status_code}."
            )


# Insert data back into Snowflake in batch
if results:
    cursor = conn.cursor()
    cursor.execute("USE DATABASE ambient_db;")
    cursor.execute("USE SCHEMA RAW;")

    insert_query = """
        INSERT INTO tbl_postal_code_mapping (latitude, longitude, postal_code)
        VALUES (%s, %s, %s)
        """
    print(insert_query)
    cursor.executemany(insert_query, results)
    conn.commit()

    update_query = """
    update iot_sensor_data
    set postal_code = pc.postal_code
    from iot_sensor_data iot
    left join tbl_postal_code_mapping pc
    on round(iot.latitude, 2) = round(pc.latitude, 2)
    and round(iot.longitude, 2) = round(pc.longitude, 2)
    where iot.postal_code is null
    and pc.postal_code is not null
    
    """
    cursor.executemany(update_query)
    conn.commit()
    cursor.close()


# Close Snowflake connection
conn.close()

print(f"Processed and inserted {len(results)} records into Snowflake.")
