from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("AdCampaignPerfomance") \
    .config("spark.sql.shuffle.partitions", 10) \
    .getOrCreate()

# File Path to your CSV file
file_path = (
    "C:/Users/Q/Documents/Important Stuff/Job Stuff/2025 Projects/advertising.csv"
)
# Read the CSV File
df = spark.read.csv(file_path, header=True, inferSchema=True)

# df.printSchema()
# df.show(5)

# Transformed Dataset
df_transformed = (
    df.withColumn(
        "Site Engagement Ratio",
        round((col("Daily Time Spent on Site") / col("Daily Internet Usage")) * 100, 2),
    )
    .withColumn(
        "Clicked on Ad",
        F.when(col("Clicked on Ad") == 1, F.lit("Yes")).otherwise(F.lit("No")),
    )
    .withColumn(
        "Gender", when(col("Male") == 1, F.lit("Male")).otherwise(F.lit("Female"))
    )
    .withColumn(
        "Age Group",
        F.when(col("Age") < 18, F.lit("Under 18"))
        .when((col("Age") >= 18) & (col("Age") < 35), F.lit("18-34"))
        .when((col("Age") >= 35) & (col("Age") < 50), F.lit("35-49"))
        .otherwise(F.lit("50+")),
    )
)

df_transformed.show(15)
