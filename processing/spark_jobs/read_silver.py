from pyspark.sql import SparkSession

import os
#Get AWS credentials from environment variables
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

# Create SparkSession with S3 Support
spark = SparkSession.builder \
    .appName("Read S3 Silver Data") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

# AWS S3 Configuration
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)  # Add your AWS Access Key
hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)  # Add your AWS Secret Key
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")  # Add your AWS S3 Endpoint

# Read Silver Data from S3
#silver_df = spark.read.json("s3a://healthcare-data-lake/silver/*).