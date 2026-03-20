from kafka import KafkaConsumer
import os
import json

# Import parser
from processing.x12_parser.parser import parse_x12

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(BASE_DIR, "../output")
output_dir = os.path.abspath(output_dir)

os.makedirs(output_dir, exist_ok=True)

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'healthcare-claims',
    bootstrap_servers='127.0.0.1:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='healthcare-group',
    value_deserializer=lambda x: x.decode('utf-8'),
    api_version=(0, 10, 1)
)

print("Listening for messages...\n")

# Consume messages
for i, message in enumerate(consumer):
    try:
        # Raw X12 message
        x12_data = message.value
        print("RAW MESSAGE:\n", message.value)
        # Parse X12 → structured data
        parsed_data = parse_x12(x12_data)

        # Define output file path
        file_path = f"{output_dir}/claim_{i}.json"

        # Write JSON output
        with open(file_path, 'w') as f:
            json.dump(parsed_data, f, indent=2)

        print(f"✅ Saved parsed data to {file_path}")

    except Exception as e:
        print(f"❌ Error processing message: {e}")