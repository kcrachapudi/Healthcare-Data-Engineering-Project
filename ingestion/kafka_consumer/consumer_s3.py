import boto3
from kafka import KafkaConsumer
import os
import json
import datetime
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# Import parser
from processing.x12_parser.parser import parse_x12

s3 = boto3.client('s3')
s3bucket = 'healthcare-data-lake-kalyan'


# -----------------------------
# Setup Paths (Robust)
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

data_lake_dir = os.path.abspath(os.path.join(BASE_DIR, "../data_lake"))
bronze_dir = os.path.join(data_lake_dir, "bronze")
silver_dir = os.path.join(data_lake_dir, "silver")

os.makedirs(bronze_dir, exist_ok=True)
os.makedirs(silver_dir, exist_ok=True)

print(f"📁 Data Lake Location: {data_lake_dir}")

# -----------------------------
# Kafka Consumer Setup
# -----------------------------
consumer = KafkaConsumer(
    'healthcare-claims',
    bootstrap_servers='127.0.0.1:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='healthcare-group',
    value_deserializer=lambda x: x.decode('utf-8'),
    api_version=(0, 10, 1)
)

print("🚀 Listening for Kafka messages...\n")

# -----------------------------
# Consume Messages
# -----------------------------
for i, message in enumerate(consumer):
    try:
        x12_data = message.value

        print("\n===== RAW MESSAGE =====")
        print(x12_data)
        print("=======================\n")

        # Parse X12 → JSON
        parsed_data = parse_x12(x12_data)
        print(f"📊 Parsed Data: {parsed_data}\n")

        # Timestamp for uniqueness
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        # -----------------------------
        # Bronze Layer (Raw X12)
        # -----------------------------
        bronze_path = os.path.join(bronze_dir, f"claim_{i}_{timestamp}.txt")
        with open(bronze_path, 'w') as f:
            f.write(x12_data)
        print(f"✅ Bronze saved: {bronze_path}")
        # Upload Bronze to S3
        s3.upload_file(
            bronze_path,
            s3bucket,
            f"bronze/{os.path.basename(bronze_path)}"
        )
        print("☁️ Uploaded Bronze to S3")

        silver_path = os.path.join(silver_dir, f"claim_{i}_{timestamp}.json")
        with open(silver_path, 'w') as f:
            json.dump(parsed_data, f, indent=2)
        print(f"✅ Silver saved: {silver_path}")
        # Upload Silver to S3
        s3.upload_file(
            silver_path,
            s3bucket,
            f"silver/{os.path.basename(silver_path)}"
        )
        print("☁️ Uploaded Silver to S3")

    except Exception as e:
        print(f"❌ Error processing message: {e}")