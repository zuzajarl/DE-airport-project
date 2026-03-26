# Airflow Orchestration

This directory contains the Airflow DAGs and dbt profile used to orchestrate the warehouse refresh layer.

Current DAG flow:

1. Load raw arrivals files from GCS into the BigQuery raw table
2. Run `dbt run`
3. Run `dbt test`

Schedule:

- `krk_pipeline` currently runs every 5 minutes

The Airflow layer does not orchestrate Kafka or Flink. Those services continue running continuously in Docker.
