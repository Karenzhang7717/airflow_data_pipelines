# airflow_data_pipelines
ETL pipelines using Apache Airflow, that transforms data from various sources into a star schema.

## How to run
```
docker-compose up airflow-init
docker-compose up
```

- Create a Redshift cluster in us-west-2 region, enable public accessibility
- Create a IAM role that has full accesibility to S3 and Redshift
