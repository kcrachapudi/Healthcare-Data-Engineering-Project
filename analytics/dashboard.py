import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import os
import sys
import dotenv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("AWS_Database_Endpoint"),
    database=os.getenv("AWS_Database_Name"),
    user=os.getenv("AWS_Database_User"),
    password=os.getenv("AWS_Database_Password")
)

# Load data
df = pd.read_sql("SELECT * FROM gold_claims", conn)

print("\n=== GOLD DATA ===")
print(df)

# -------------------------
# Chart 1: Cost by Diagnosis
# -------------------------
df.groupby("diagnosis_code")["total_cost"].sum().plot(kind="bar")
plt.title("Total Cost by Diagnosis")
plt.xlabel("Diagnosis Code")
plt.ylabel("Total Cost")
plt.show()

# -------------------------
# Chart 2: Claims by Provider
# -------------------------
df.groupby("provider_id")["total_claims"].sum().plot(kind="bar")
plt.title("Claims by Provider")
plt.xlabel("Provider")
plt.ylabel("Total Claims")
plt.show()