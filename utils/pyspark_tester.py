import pyspark
print(f"✅ PySpark Version: {pyspark.__version__}")

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PySparkTester").getOrCreate()
print("✅ SparkSession created successfully")

data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
print("✅ Spark DataFrame created successfully")

df.show()