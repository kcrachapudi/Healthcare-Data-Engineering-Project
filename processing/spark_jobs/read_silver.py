from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file

#Get AWS credentials from environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

# Create SparkSession with S3 Support
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read S3 Silver Data") \
    .getOrCreate()

# AWS S3 Configuration
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)  # Add your AWS Access Key
hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)  # Add your AWS Secret Key
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")  # Add your AWS S3 Endpoint
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# Read Silver Data from S3
silver_df = spark.read.json("s3a://healthcare-data-lake/silver/")
# Show the Silver Data
silver_df.show(truncate=False)
print("\n Silver Data Schema:")
silver_df.printSchema()
