import os
import json
from collections import defaultdict

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(BASE_DIR, "../../"))

silver_dir = os.path.join(project_root, "ingestion/data_lake/silver")
gold_dir = os.path.join(project_root, "ingestion/data_lake/gold")

os.makedirs(gold_dir, exist_ok=True)

# Aggregations
total_claim_amount = 0
claims_per_patient = defaultdict(int)

print("Processing Silver data...\n")

# Read all silver files
for file in os.listdir(silver_dir):
    if file.endswith(".json"):
        file_path = os.path.join(silver_dir, file)

        with open(file_path, 'r') as f:
            data = json.load(f)

            if not data:
                continue

            # Extract fields
            patient = f"{data.get('patient_first_name', '')} {data.get('patient_last_name', '')}"
            claim_amount = float(data.get("claim_amount", 0))

            # Aggregate
            total_claim_amount += claim_amount
            claims_per_patient[patient] += 1

# Prepare Gold output
gold_data = {
    "total_claim_amount": total_claim_amount,
    "claims_per_patient": dict(claims_per_patient)
}

# Write Gold file
output_path = os.path.join(gold_dir, "gold_metrics.json")

with open(output_path, 'w') as f:
    json.dump(gold_data, f, indent=2)

print("✅ Gold layer created:")
print(output_path)
print("\n📊 Metrics:")
print(json.dumps(gold_data, indent=2))