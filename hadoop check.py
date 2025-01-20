import os

# Set Hadoop environment variables
os.environ["HADOOP_HOME"] = r"C:\hadoop\hadoop-3.4.0"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\hadoop-3.4.0\bin"

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerifyWinutils").getOrCreate()

print("Spark session initialized successfully!")
