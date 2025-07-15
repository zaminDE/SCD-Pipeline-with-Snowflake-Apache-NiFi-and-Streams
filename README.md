# Slowly Changing Dimensions Pipeline with Snowflake, Apache NiFi, and Streams

## Introduction  
This project builds a real-time data pipeline for continuous ingestion and transformation into a Snowflake data warehouse. It implements Change Data Capture (CDC) and Slowly Changing Dimensions (SCD) to maintain historical data accuracy.

## Tech Stack  
- **Languages:** Python3, JavaScript, SQL  
- **Services:** Apache NiFi, Amazon S3, Snowflake, Amazon EC2, Docker  

## Dataset  
Synthetic user data generated using Python Faker library and saved as CSV files with fields:  
`Customer_id`, `First_name`, `Last_name`, `Email`, `Street`, `State`, `Country`.

## Process Flow  
1. Data generation on Amazon EC2 using Python Faker scripts.  
2. Apache NiFi monitors local folder and uploads new CSV files to Amazon S3.  
3. Snowpipe ingests files from S3 into Snowflake staging tables automatically.  
4. Scheduled Snowflake tasks execute stored procedures for CDC via MERGE commands.  
5. Snowflake Streams track changes to support Slowly Changing Dimensions (SCD) Type-1 and Type-2.

## Key Features  
- Real-time data ingestion pipeline using NiFi and Snowpipe.  
- CDC implementation with Snowflake streams and tasks.  
- SCD Types 1 and 2 for historical data management.  
- Automated data flow from generation to transformation.

## Benefits  
- Near real-time updates in Snowflake for analytics.  
- Maintains historical integrity of dimension data.  
- Scalable architecture using cloud-native tools.

## FAQs

### What are Slowly Changing Dimensions (SCD)?  
SCD techniques track changes in dimension data over time, preserving historical context.  
- **Type 0:** No changes tracked.  
- **Type 1:** Overwrites old data.  
- **Type 2:** Preserves full history with multiple records.  
- **Type 3:** Stores current and previous values.

### Technologies Used  
- **Apache NiFi:** Automates and manages data flows.  
- **Docker:** Containerizes applications for consistency.  
- **Amazon EC2:** Provides scalable virtual machines.  
- **Amazon S3:** Object storage service for files.  
- **Snowflake:** Cloud data warehouse with components like Snowpipe, Streams, and Tasks.

---

Feel free to customize or ask if you want me to add sections like Installation, Usage, or Contribution!
