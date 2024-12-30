import os
from pinecone import Pinecone, ServerlessSpec
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Validate API key
api_key = os.environ.get("PINECONE_API_KEY")
if not api_key:
    raise ValueError("PINECONE_API_KEY is not set in the environment.")

# Initialize Pinecone instance
pc = Pinecone(api_key=api_key)

# Define index name
index_name = "iot-sensors"

# Sample data
data = [
    {
        "sensor_id": "sensor_432",
        "timestamp": 1735261445.2160692,
        "sensor_type": "air_quality",
        "latitude": 40.631483,
        "longitude": -111.940353,
        "city": "Unknown",
        "state": "Unknown",
        "device_model": "Model S",
        "manufacturer": "Honeywell",
        "firmware_version": "v4.2",
        "value": 419.53,
    },
    # Add more data as needed...
]

# Combine fields into descriptive strings for embeddings
texts = [
    f"Sensor {d.get('sensor_id', 'unknown')} of type {d.get('sensor_type', 'unknown')} located at "
    f"latitude {d.get('latitude', 'unknown')} and longitude {d.get('longitude', 'unknown')} reported a value of {d.get('value', 'unknown')} "
    f"on timestamp {d.get('timestamp', 'unknown')}."
    for d in data
]

# Generate test embedding to determine dimension
try:
    print("Testing embedding generation...")
    response = pc.inference.embed(
        model="multilingual-e5-large",
        inputs=["This is a test sentence."],
        parameters={"input_type": "passage", "truncate": "END"},
    )
    if not response or not response.data:
        raise ValueError("Test embedding generation returned no data.")

    dimension = len(response.data[0].values)
    print(f"Detected embedding dimension: {dimension}")

except Exception as e:
    raise RuntimeError(f"Error generating test embedding: {e}")

# Check existing indexes
try:
    existing_indexes = pc.list_indexes()
    print(f"Existing indexes: {existing_indexes}")

    # Convert to list of index names
    index_names = [i.name for i in existing_indexes.indexes]

    # Check for dimension mismatch
    if index_name in index_names:
        index_info = next(i for i in existing_indexes.indexes if i.name == index_name)
        if index_info.dimension != dimension:
            print(f"Index '{index_name}' dimension mismatch. Deleting and recreating.")
            pc.delete_index(index_name)
            while index_name in [i.name for i in pc.list_indexes().indexes]:
                print(f"Waiting for index '{index_name}' to be deleted...")
        else:
            print(f"Index '{index_name}' exists with matching dimension.")
    else:
        print(f"Index '{index_name}' does not exist. Creating a new index.")

    # Create the index if necessary
    if index_name not in [i.name for i in pc.list_indexes().indexes]:
        pc.create_index(
            name=index_name,
            dimension=dimension,
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1"),
        )
        print(f"Index '{index_name}' created with dimension {dimension}.")

except Exception as e:
    raise RuntimeError(f"Error managing index: {e}")


# Generate embeddings for the dataset
try:
    print("Generating embeddings for dataset...")
    response = pc.inference.embed(
        model="multilingual-e5-large",
        inputs=texts,
        parameters={"input_type": "passage", "truncate": "END"},
    )
    if not response or not response.data:
        raise ValueError("Embedding generation returned no data.")

    embeddings = [item.values for item in response.data]
    print(f"Generated {len(embeddings)} embeddings successfully.")

except Exception as e:
    raise RuntimeError(f"Error generating embeddings: {e}")

# Upsert data into Pinecone
try:
    index = pc.Index(index_name)
    pinecone_vectors = [
        {
            "id": f"{d['sensor_id']}-{d['timestamp']}",
            "values": embeddings[i],
            "metadata": {key: d.get(key, "unknown") for key in d},
        }
        for i, d in enumerate(data)
    ]
    index.upsert(vectors=pinecone_vectors)
    print("Vectors upserted successfully.")
except Exception as e:
    raise RuntimeError(f"Error upserting vectors into Pinecone: {e}")


# Define a query text
query_text = "Sensor near 40.73739 latitude reported value 0."

try:
    # Generate query embedding
    print("Generating query embedding...")
    query_embedding = pc.inference.embed(
        model="multilingual-e5-large",
        inputs=[query_text],
        parameters={"input_type": "query", "truncate": "END"},
    )
    query_vector = query_embedding.data[0].values
    print("Query embedding generated successfully.")

    # Query the index
    print("Querying the index...")
    results = index.query(vector=query_vector, top_k=5, include_metadata=True)

    # Process and print results
    if results.matches:
        print("Query Results:")
        for match in results.matches:
            print(f"ID: {match.id}, Score: {match.score}, Metadata: {match.metadata}")
    else:
        print("No matches found.")
except Exception as e:
    raise RuntimeError(f"Error querying the index: {e}")
