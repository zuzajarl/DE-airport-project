# Flink Jobs

This module contains the PyFlink jobs used in the streaming layer.

## Current job

- `src/write_to_gcs_job.py`: reads Kafka messages from `krk_arrivals` and writes raw JSON files to GCS

## Runtime role

The Flink layer currently acts as the streaming transport from Kafka to the raw data lake:

- source: Redpanda/Kafka
- sink: GCS filesystem path under `gs://krk-flights-bucket/krk_arrivals_raw`

Deduplication is handled later in dbt through the `int_latest_arrivals` model.
