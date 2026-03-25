# KRK Arrivals Streaming Pipeline

Real-time data engineering project for monitoring arrivals and delays at Krakow Airport (KRK / Balice).

The project uses AeroDataBox as the flight-status source and is designed around a streaming architecture with Kafka, Flink, GCS, BigQuery, dbt, and Looker Studio.

## Dashboard scope

- Tile 1: Recent arrivals by status (`landed`, `expected`, `cancelled`, `delayed`)
- Tile 2: Delay distribution by source/destination
- Tile 3: Delay trend over time by hour/day

## Proposed architecture

1. `ingestion/producer` polls AeroDataBox for recent KRK arrivals
2. Raw events are published to Kafka
3. `streaming/flink-jobs` parses, deduplicates, enriches, and computes delay metrics
4. Raw and curated data are written to GCS and BigQuery
5. `warehouse/dbt` builds analytics-ready models
6. `dashboards/looker` stores dashboard definitions and screenshots

## Repository structure

```text
.
├── dashboards/
│   └── looker/               # Looker Studio notes, assets, screenshots
├── docker/
│   └── docker-compose.yml    # Local Kafka/Flink stack
├── docs/
│   └── architecture.md       # Design notes and data flow
├── infra/
│   └── terraform/
│       ├── environments/
│       │   └── dev/          # Environment-specific Terraform
│       └── modules/
│           ├── bigquery/     # Reusable BQ resources
│           ├── gcs/          # Reusable bucket resources
│           └── iam/          # Service accounts and roles
├── ingestion/
│   ├── common/               # Shared schemas, utilities, config loading
│   └── producer/             # AeroDataBox polling producer
├── orchestration/            # Optional scheduled jobs / runner scripts
├── streaming/
│   ├── contracts/            # Event schemas and topic contracts
│   └── flink-jobs/           # Flink stream processing code
├── tests/                    # Unit/integration tests
└── warehouse/
    ├── bigquery/             # DDL, table design, partitioning/clustering notes
    └── dbt/                  # Transformations for marts and dashboard models
```

<!-- ## Suggested ownership by layer

- `ingestion/`: source extraction and Kafka publishing
- `streaming/`: event-time logic, deduplication, delay calculation, sinks
- `warehouse/`: warehouse design and dbt models
- `infra/`: GCP infrastructure as code
- `dashboards/`: final dashboard assets and documentation -->

<!-- ## Suggested next implementation steps

1. Add the AeroDataBox producer in `ingestion/producer`
2. Define Kafka topic contracts in `streaming/contracts`
3. Set up local Kafka + Flink in `docker/docker-compose.yml`
4. Create BigQuery raw and curated tables in `warehouse/bigquery`
5. Add dbt staging and marts models in `warehouse/dbt`
6. Build the Looker Studio dashboard on top of the marts layer -->
