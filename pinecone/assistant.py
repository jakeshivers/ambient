from pinecone import Pinecone
import os
from dotenv import load_dotenv
from pinecone_plugins.assistant.models.chat import Message


load_dotenv()

pc = Pinecone(api_key=os.environ.get("PINECONE_API_KEY"))

assistant = pc.assistant.Assistant(assistant_name="example-assistant")

msg = Message(
    role="user", content="What is the inciting incident of Pride and Prejudice?"
)

chunks = assistant.chat(messages=[msg], stream=True)

for chunk in chunks:
    if chunk:
        print(chunk)
