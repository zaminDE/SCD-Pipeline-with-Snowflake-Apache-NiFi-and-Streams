Project: Slowly Changing Dimensions in Snowflake Using Streams and Tasks
Overview
This project builds a real-time data pipeline for continuous ingestion and transformation into Snowflake, implementing Change Data Capture (CDC) and Slowly Changing Dimensions (SCD) to manage historical data effectively.

Tech Stack
Languages: Python3, JavaScript, SQL

Services: Apache NiFi, Amazon S3, Snowflake, Amazon EC2, Docker

Dataset
Synthetic user data is generated using Python’s Faker library and stored as CSV files, including fields like Customer_id, First_name, Last_name, Email, Street, State, and Country.

Process Flow
Data Generation: Python scripts on EC2 generate data and save it locally.

Data Movement: Apache NiFi monitors and uploads files to S3 automatically.

Data Ingestion: Snowpipe ingests data from S3 into a Snowflake staging table.

Data Transformation: Scheduled Snowflake tasks run stored procedures every minute:

Merge updates target table with CDC logic.

Truncate staging table for next batch.

Historical Data: Snowflake Streams track changes to maintain SCD-compliant history.

Key Learnings
Fundamentals of SCD types and CDC.

Using AWS EC2, NiFi, Docker, and Snowflake components.

Building scalable, real-time data pipelines in the cloud.

Benefits
Enables near real-time data updates in Snowflake.

Maintains accurate historical records with SCD techniques.

Ensures data integrity via CDC merges.

Scalable architecture handles large data volumes efficiently.

FAQs
What is Slowly Changing Dimensions (SCD)?
SCD tracks and manages changes in dimension data over time in a data warehouse, preserving historical accuracy. Common types include:

Type 0: No changes stored; original data remains.

Type 1: Overwrites old data with new values (no history).

Type 2: Preserves full history by creating new records on changes.

Type 3: Stores previous and current values in the same record.

Apache NiFi: Automates and manages data flow between systems with real-time control.

Docker: Containerizes applications to ensure consistent execution across environments.

Amazon EC2: Scalable virtual servers for running applications in the cloud.

Amazon S3: Secure, scalable object storage for any volume of data.

Snowflake: Cloud-native data warehouse platform supporting storage, processing, and analytics, featuring components like Warehouses, Snowpipe, Streams, and Tasks.
