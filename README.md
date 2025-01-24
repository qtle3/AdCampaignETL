This project leverages PySpark for scalable data processing and analysis of advertising campaign performance. It provides actionable insights into user engagement by demographics, income levels, and ad topics.

Features
Data Transformation:
Calculate Site Engagement Ratio.
Categorize users by age, gender, income, and engagement levels.
Analytical Summaries:
Engagement trends by age group and gender.
Click-through analysis by income bracket.
Ad topic performance evaluation.
Demographic-based engagement analysis.
Integration:
Export analytical results to local CSV files.
Automatic uploads to AWS S3 for storage.
Technologies Used
PySpark for data transformation and analysis.
Pandas for data handling.
Boto3 for AWS S3 interaction.
How to Use
Prerequisites:
Python with PySpark and Pandas installed.
AWS credentials configured for S3 access.
Setup:
Ensure your advertising CSV file is placed in the correct path.
Update the file path and S3 bucket details in the script.
Run the Script:
Execute the script to generate and upload analytical summaries.
Outputs
The project generates the following summaries as CSV files:

Age and Gender Summary
Income Level Summary
Ad Topic Performance
Time Spent by Demographics
Country-level Engagement
These summaries are stored locally and uploaded to AWS S3.
