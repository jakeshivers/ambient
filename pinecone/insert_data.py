from pinecone import Pinecone, ServerlessSpec
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Validate API key
pinecone = Pinecone(api_key=os.environ.get("PINECONE_API_KEY"))

# Create or connect to an index
index_name = "iot-sensors"
if index_name in pinecone.list_indexes():
    pinecone.delete_index(index_name)  # Delete if existing with wrong dimension
pinecone.create_index(
    name=index_name,
    dimension=3,
    metric="cosine",  # Adjust metric if needed (e.g., "euclidean", "dotproduct")
    spec=ServerlessSpec(cloud="aws", region="us-east-1"),
)
index = pinecone.Index(index_name)

# Load data from a file
data_file = "data.jsonl"  # Replace with your file path
data_list = []
with open(data_file, "r", encoding="utf-8") as file:
    for line_number, line in enumerate(file, start=1):
        try:
            data = json.loads(line.strip())  # Validate and parse JSON
            data_list.append(data)  # Add to the data list
        except json.JSONDecodeError as e:
            print(f"Invalid JSON on line {line_number}: {e}")

# Validate that the data_list is not empty
if not data_list:
    raise ValueError("No valid data found in the file. Please check the file content.")

# Prepare the data for bulk upsert
vectors = [
    {"id": data["ID"], "values": data["STREAM_VALUES"], "metadata": data["METADATA"]}
    for data in data_list
]

# Perform the upsert operation
try:
    index.upsert(vectors=vectors)
    print("Data successfully uploaded to Pinecone!")
except Exception as e:
    print(f"Error during upsert: {e}")
