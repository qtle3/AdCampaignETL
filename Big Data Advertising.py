import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pandas as pd


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
    .withColumn(
        "Income Bracket",
        F.when(col("Area Income") < 35000, F.lit("Lower Income"))
        .when(
            (col("Area Income") >= 35000) & (col("Area Income") < 65000),
            F.lit("Middle Income"),
        )
        .otherwise(F.lit("High Income")),
    )
    .withColumn(
        "Time Spent Category",
        F.when(col("Site Engagement Ratio") < 20, F.lit("Low Engagement"))
        .when(
            (col("Site Engagement Ratio") >= 20) & (col("Site Engagement Ratio") < 50),
            F.lit("Medium Engagement"),
        )
        .otherwise(F.lit("High Engagement")),
    )
    
)

# df_transformed.show(15)

# Summarized user engagement by age group and gender
age_gender_df = df_transformed.groupBy("Age Group", "Gender").agg(
    F.round(F.avg("Site Engagement Ratio"),2).alias("Avg Engagement Ratio"),
    F.count(F.when(F.col("Clicked on Ad") == "Yes", 1)).alias("Total Clicks")
)
age_gender_df.show()

# Summarize user engagement by income levels
income_level_df = df_transformed.groupBy("Income Bracket").agg(
    F.round(F.avg("Daily Internet Usage"),2).alias("Avg Daily Internet Usage"),
    F.count(F.when(F.col("Clicked on Ad") == "Yes",1)).alias("Total Clicks")
)
income_level_df.show()

# Analyze the performance of ad topics:
ad_topic_df = df_transformed.groupBy("Ad Topic Line").agg(
    F.avg("Site Engagement Ratio").alias("Avg Engagement Ratio"),
    F.count(F.when(F.col("Clicked on Ad") == "Yes", 1)).alias("Total Clicks"),
)
ad_topic_df.show()
# Engagement levels by different demographics
time_spent_df = df_transformed.groupBy("Time Spent Category", "Gender").agg(
    F.count("*").alias("Total Users"), 
    F.round(F.avg("Age"),1).alias("Avg Age")
)
time_spent_df.show()

# Summarize Engagement Levels by country
country_engagement_df = df_transformed.groupBy("Country").agg(
    F.round(F.avg("Site Engagement Ratio"),2).alias("Avg Engagement Ratio"),
    F.count(F.when(F.col("Clicked on Ad") == "Yes", 1)).alias("Total Clicks"),
)
country_engagement_df.show()

# Define a dictionary of DataFrames and their respective filenames
dataframes = {
    "age_gender_summary": age_gender_df,
    "income_level_summary": income_level_df,
    "ad_topic_summary": ad_topic_df,
    "time_spent_summary": time_spent_df,
    "country_engagement_summary": country_engagement_df,
}

# Base output directory
base_output_path = r"C:\Users\Q\Documents\Important Stuff\Python Work\Projects\2025 Python Projects\AdCampaign"

# Export each DataFrame to CSV
for name, df in dataframes.items():
    # Convert PySpark DataFrame to Pandas
    df_pd = df.toPandas()

    # Define the output file path
    output_path = f"{base_output_path}\\{name}.csv"

    if os.path.exists(output_path):
        # If the file exists, read it and append the new data
        existing_df = pd.read.csv(output_path)
        combined_df = pd.concat([existing_df, df_pd], ignore_index=True)

        # Remove duplicates (optional, based on the use case)
        combined_df.drop_duplicates(inplace=True)
        print(f"dropped duplicate csv {combined_df}")
        # save back to the same file
        combined_df.to_csv(output_path, index=False)
        print(f"Appended data to {output_path}")
    else:
        # Save as CSV
        df_pd.to_csv(output_path, index=False)
        print(f"Created new CSV file at {output_path}")
