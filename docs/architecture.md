# Architecture Notes

## Core flow

`AeroDataBox -> publisher -> Kafka -> Flink -> GCS -> Airflow -> BigQuery -> dbt -> Looker Studio`

## Canonical Event Schema

This is the normalized arrival event shape produced by `ingestion/producer.py`.

Each row represents one flight arrival for Krakow Airport (`EPKK` / `KRK`) at a specific ingestion time.

### Event grain

- One row per flight in the AeroDataBox `arrivals` response
- Snapshot-style event captured at poll time
- Primary business key for now: `flight_number + scheduled_arrival_utc + ingested_at`

### Event fields

| Field | Type | Description |
|---|---|---|
| `flight_number` | string | Airline flight number from AeroDataBox, for example `FR 5144` |
| `status` | string | Normalized source status from AeroDataBox: `arrived`, `expected`, `cancelled`, `delayed`, `unknown` |
| `status_bucket` | string | Analytics bucket derived from delay threshold, currently `on_time`, `delayed`, `unknown` |
| `airline_name` | string | Airline display name |
| `airline_icao` | string | Airline ICAO code used for grouping and joins |
| `origin_name` | string | Origin airport display name |
| `origin_icao` | string | Origin airport ICAO code used for grouping and joins |
| `scheduled_arrival_utc` | timestamp | Scheduled arrival timestamp in UTC |
| `revised_arrival_utc` | timestamp | Revised arrival timestamp in UTC from AeroDataBox |
| `delay_minutes` | float | Difference between revised and scheduled arrival in minutes |
| `ingested_at` | timestamp | UTC timestamp when the record was pulled from the API |

### Current business rules

- `delay_minutes = revised_arrival_utc - scheduled_arrival_utc`
- `status` is mapped from AeroDataBox raw statuses
- `status_bucket = delayed` when `delay_minutes > 15`
- `status_bucket = on_time` when `delay_minutes <= 15`
- `status_bucket = unknown` when delay cannot be computed

### Notes

- Keep `status` and `status_bucket` as separate fields
- `status` preserves the provider view of the flight
- `status_bucket` supports dashboard logic and consistent aggregations
- The next schema revision should add `origin_iata` and `airline_iata` for easier reporting in BigQuery and Looker Studio

## Processing layers

- Raw layer: append-only flight snapshots in GCS and the raw BigQuery table
- Staging layer: cleaned source-aligned records in dbt
- Intermediate layer: latest snapshot per flight in `int_latest_arrivals`
- Mart layer: dashboard-facing aggregates and latest-arrival datasets

## Dashboard datasets

- `mart_recent_arrivals`
- `mart_delay_distribution`
- `mart_delay_trend`

## Dashboard Mapping

- `mart_recent_arrivals`: latest arrival records with `flight_number`, `status`, `status_bucket`, `airline_name`, `origin_name`, `scheduled_arrival_utc`, `revised_arrival_utc`, `delay_minutes`
- `mart_delay_distribution`: aggregate `delay_minutes` by `origin_icao`
- `mart_delay_trend`: aggregate average delay over time using scheduled arrival timestamps and derived day/hour fields

## Recommended warehouse strategy

- Raw events table partitioned by ingestion date
- Raw table loaded from GCS by Airflow
- dbt staging and marts rebuilt on a schedule by Airflow
- Deduplication done in dbt using latest `ingested_at` per `flight_number + scheduled_arrival_utc`
