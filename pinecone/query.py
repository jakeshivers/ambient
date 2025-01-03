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
    results = index.query(
        vector=query_vector,
        top_k=5,
        include_metadata=True
    )

    # Process and print results
    if results.matches:
        print("Query Results:")
        for match in results.matches:
            print(f"ID: {match.id}, Score: {match.score}, Metadata: {match.metadata}")
    else:
        print("No matches found.")
except Exception as e:
    raise RuntimeError(f"Error querying the index: {e}")
