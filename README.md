# Project Overview

This data pipeline brings in data from two seperate sources: Customer Purchase data from an OLTP database, and Movie Review data from a 3rd party data vendor. Using Apache Airflow, we can extract the data from both sources on a daily basis, perform processing using big data tools (AWS EMR, AWS Spark), and combine the two sources into a Redshift database (data warehouse) for analytics. 

Tech Stack
Languages: Python, PostgreSQL

Frameworks: Apache Airflow

Services: AWS EMR (Spark), AWS Redshift, Docker, PostgreSQL, 

# Finished Project

What you see below is the full data pipeline. On the bottom section, airflow is reading in the entire postgreSQL database daily, moving the data to an S3 bucket and immediately deleting it. Once the data is in the S3 bucket, it is staged in the redshift database (the final product of the data pipelien). 

On the top side of the data pipeline, movie review data is brought in from a csv. You'll notice that airflow simultaneously moves the data to an S3 bucket while also moving the EMR steps to the S3 bucket. Next, the EMR steps are added to EMR where the data is cleaned using a naive classification model in Apache Spark.
![alt text](https://github.com/AndrewSLowe/airflow_batch_project/blob/main/images/airflow_UI.png?raw=true)

# Beginner DE Project - Batch Edition

This project was inspired by a blog post at Start Data Engineering. (https://www.startdataengineering.com/post/data-engineering-project-for-beginners-batch-edition)
