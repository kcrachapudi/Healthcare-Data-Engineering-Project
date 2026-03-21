from kafka import KafkaConsumer
import psycopg2
import json

import os
import sys
import dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# Import parser
from processing.x12_parser.parser import parse_x12


dotenv.load_dotenv()

# -------------------------
# RDS CONNECTION
# -------------------------
conn = psycopg2.connect(
    host=os.getenv("AWS_Database_Endpoint"),
    database=os.getenv("AWS_Database_Name"),
    user=os.getenv("AWS_Database_User"),
    password=os.getenv("AWS_Database_Password")
)

cur = conn.cursor()

# -------------------------
# KAFKA CONSUMER
# -------------------------
consumer = KafkaConsumer(
    'healthcare-claims',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

# -------------------------
# CONSUME + INSERT
# -------------------------
for msg in consumer:
    raw_message = msg.value
    print("\nRAW MESSAGE:\n", raw_message)
    
    parsed = parse_x12(raw_message)
    print("PARSED:", parsed)
    
    if parsed:
        try:
            cur.execute(
                "INSERT INTO silver_claims VALUES (%s, %s, %s, %s)",
                (
                    parsed.get("patient_first_name"),
                    parsed.get("patient_last_name"),
                    parsed.get("claim_id"),
                    parsed.get("claim_amount")
                )
            )
            conn.commit()
            print("Inserted into RDS ✅")
        
        except Exception as e:
            print("DB ERROR:", e)
            conn.rollback()