# Project Overview

This data pipeline brings in data from two seperate sources: Customer Purchase data from an OLTP database, and Movie Review data from a 3rd party data vendor. Using Apache Airflow, we can extract the data from both sources on a daily basis, perform processing using big data tools (AWS EMR, AWS Spark), and combine the two sources into a Redshift database (data warehouse) for analytics. 

Tech Stack
Languages: Python, PostgreSQL

Frameworks: Apache Airflow

Services: AWS EMR (Spark), AWS Redshift, Docker, PostgreSQL, 









# Beginner DE Project - Batch Edition

This project was inspired by a blog post at Start Data Engineering. (https://www.startdataengineering.com/post/data-engineering-project-for-beginners-batch-edition)