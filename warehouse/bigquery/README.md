# BigQuery Layer

This layer stores the warehouse copy of the raw arrivals data and the datasets consumed by dbt.

## Current raw table

- dataset: `krk_flights`
- raw table: `krk_arrivals_raw`
- partitioning: `ingested_at` by day
- source format: newline-delimited JSON loaded from GCS
