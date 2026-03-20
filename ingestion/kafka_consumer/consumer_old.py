from processing.x12_parser.parser import parse_x12
import json
from kafka import KafkaConsumer
import os

# Create output folder if not exists
output_dir = "../output"
os.makedirs(output_dir, exist_ok=True)

# Initialize consumer
consumer = KafkaConsumer(
    'healthcare-claims',
    bootstrap_servers='127.0.0.1:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='healthcare-group',
    value_deserializer=lambda x: x.decode('utf-8'),
    api_version=(0, 10, 1)
)

print("Listening for messages...")

# Read messages
for i, message in enumerate(consumer):
    file_path = f"{output_dir}/claim_{i}.txt"

    with open(file_path, 'w') as f:
        f.write(message.value)

    print(f"Saved message to {file_path}")