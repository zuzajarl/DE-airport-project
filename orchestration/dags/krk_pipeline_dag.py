from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery


PROJECT_ID = "axial-trail-485514-p5"
DATASET_ID = "krk_flights"
TABLE_ID = "krk_arrivals_raw"
GCS_URI = "gs://krk-flights-bucket/krk_arrivals_raw/*"


def load_raw_arrivals_to_bigquery() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("flight_number", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("airline_name", "STRING"),
            bigquery.SchemaField("origin_name", "STRING"),
            bigquery.SchemaField("scheduled_arrival_utc", "TIMESTAMP"),
            bigquery.SchemaField("revised_arrival_utc", "TIMESTAMP"),
            bigquery.SchemaField("origin_icao", "STRING"),
            bigquery.SchemaField("airline_icao", "STRING"),
            bigquery.SchemaField("delay_minutes", "FLOAT"),
            bigquery.SchemaField("status_bucket", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ],
    )

    load_job = client.load_table_from_uri(
        GCS_URI,
        table_ref,
        job_config=job_config,
    )
    load_job.result()


with DAG(
    dag_id="krk_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["krk", "airport", "streaming"],
) as dag:
    load_raw = PythonOperator(
        task_id="load_raw_arrivals_to_bigquery",
        python_callable=load_raw_arrivals_to_bigquery,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt/krk_flights_dbt && "
            "dbt run --profiles-dir /opt/airflow/dbt_profiles"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt/krk_flights_dbt && "
            "dbt test --profiles-dir /opt/airflow/dbt_profiles"
        ),
    )

    load_raw >> dbt_run >> dbt_test
