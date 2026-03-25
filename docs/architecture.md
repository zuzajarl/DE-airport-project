# Architecture Notes

## Core flow

`AeroDataBox -> Producer -> Kafka -> Flink -> GCS + BigQuery -> dbt -> Looker Studio`

## Canonical Event Schema

This is the normalized arrival event shape produced by `ingestion/producer/producer.py`.

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

## Main entities

- flight arrival event
- latest flight status snapshot
- delay metric by route
- hourly and daily delay aggregates

## Dashboard datasets

- `mart_recent_arrivals`
- `mart_delay_distribution`
- `mart_delay_trend`

## Dashboard Mapping

- `mart_recent_arrivals`: latest arrival records with `flight_number`, `status`, `status_bucket`, `airline_name`, `origin_name`, `scheduled_arrival_utc`, `revised_arrival_utc`, `delay_minutes`
- `mart_delay_distribution`: aggregate `delay_minutes` by `origin_icao` and later by `origin_iata`
- `mart_delay_trend`: aggregate average and median `delay_minutes` by hour and day based on `ingested_at` and scheduled arrival timestamps

## Recommended warehouse strategy

- Raw events table partitioned by ingestion date
- Curated facts partitioned by service date
- Cluster curated tables by `airport_code`, `arrival_status`, `origin_iata`, `destination_iata`, `airline_code`
