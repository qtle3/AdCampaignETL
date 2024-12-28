from pyspark.sql import SparkSession
from pyspark.sql.functions import *


#Initialize Spark Session
spark = SparkSession.builder \
    .appName("AdCampaignPerfomance") \
    .config("spark.sql.shuffle.partitions", 10) \
    .getOrCreate()

#File Path to your CSV file
file_path = (
    "C:/Users/Q/Documents/Important Stuff/Job Stuff/2025 Projects/advertising.csv"
)
#Read the CSV File
df = spark.read.csv(file_path, header=True, inferSchema=True)

# df.printSchema()
df.show(5)


