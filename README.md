# airflow_data_pipelines
ETL pipelines using Apache Airflow, that transforms data from various sources into a star schema.

## How to run
```
docker-compose up airflow-init
docker-compose up
```

- Create a Redshift cluster in us-west-2 region, enable public accessibility
- Create a IAM role that has full accesibility to S3 and Redshift
- Add proper connections to your Airflow Admin page (including aws_credentials and redshift connection)

## Data sources
Data resides in two directories that contain files in JSON format:

Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song_data

## Project Files
etl.py - The DAG configuration file to run in Airflow
create_tables.sql - Contains the DDL for all tables used in this projecs
stage_redshift.py - Operator to read files from S3 and load into Redshift staging tables
load_fact.py - Operator to load the fact table in Redshift
load_dimension.py - Operator to read from staging tables and load the dimension tables in Redshift
data_quality.py - Operator for data quality checking

## DAG view