# KRK Arrivals Streaming Pipeline

Real-time data engineering project for monitoring arrivals and delays at Krakow Airport (KRK / Balice).

The project uses AeroDataBox as the flight-status source and is built as a streaming pipeline with Redpanda/Kafka, Flink, GCS, BigQuery, dbt, Airflow, Looker Studio, and Terraform.

---

## Problem statement

The goal is to monitor inbound flights to Krakow Airport in near real time and answer questions such as:

- which arrivals are currently delayed
- which origin airports generate the highest delays
- how delay patterns change over time

The raw source is polled repeatedly, so the pipeline stores append-only snapshots in the raw layer and deduplicates to the latest flight state in dbt for dashboarding.

---

## Architecture

```
AeroDataBox → publisher → Redpanda/Kafka → Flink → GCS → Airflow → BigQuery → dbt → Looker Studio
```

1. `ingestion/producer.py` fetches KRK arrivals from the AeroDataBox API and normalises them into a canonical event schema.
2. `ingestion/publisher.py` runs continuously in Docker and publishes batches to the Kafka topic `krk_arrivals` every 5 minutes.
3. `streaming/flink-jobs/src/write_to_gcs_job.py` consumes the Kafka topic and writes raw JSON files to GCS (append-only).
4. `orchestration/dags/krk_pipeline_dag.py` runs every 5 minutes: loads all GCS files into the raw BigQuery table, then runs `dbt run` and `dbt test`.
5. `warehouse/dbt/krk_flights_dbt` builds staging, a deduplicated intermediate model (`int_latest_arrivals`), and three dashboard mart tables.
6. Looker Studio reads the mart tables.

### Event schema

Each row is a flight arrival snapshot captured at poll time.

| Field | Type | Description |
|---|---|---|
| `flight_number` | STRING | Airline flight number, e.g. `FR 5144` |
| `status` | STRING | Normalised status: `arrived`, `expected`, `cancelled`, `delayed`, `unknown` |
| `status_bucket` | STRING | Analytics bucket: `on_time`, `delayed`, `unknown` |
| `airline_name` | STRING | Airline display name |
| `airline_icao` | STRING | Airline ICAO code |
| `origin_name` | STRING | Origin airport display name |
| `origin_icao` | STRING | Origin airport ICAO code |
| `scheduled_arrival_utc` | TIMESTAMP | Scheduled arrival in UTC |
| `revised_arrival_utc` | TIMESTAMP | Revised arrival in UTC |
| `delay_minutes` | FLOAT64 | `revised_arrival_utc − scheduled_arrival_utc` in minutes |
| `ingested_at` | TIMESTAMP | UTC timestamp when the record was pulled from the API |

**Business rules**
- `delay_minutes = revised_arrival_utc − scheduled_arrival_utc`
- `status_bucket = delayed` when `delay_minutes > 15`
- `status_bucket = on_time` when `delay_minutes ≤ 15`
- `status_bucket = unknown` when delay cannot be computed

### Processing layers

| Layer | Location | Description |
|---|---|---|
| Raw | GCS + BigQuery `krk_arrivals_raw` | Append-only snapshots; partitioned by `ingested_at` date, clustered by `origin_icao` and `status_bucket` |
| Staging | dbt `stg_raw_arrivals_events` | Cleaned, typed, source-aligned records |
| Intermediate | dbt `int_latest_arrivals` | Latest snapshot per `flight_number + scheduled_arrival_utc` (dedup via `ingested_at DESC`) |
| Marts | dbt `mart_*` | Dashboard-facing aggregates |

---

## Dashboard

![KRK Balice Arrivals Monitoring dashboard](dashboards/looker/image.png)

The dashboard is built in Looker Studio and connects directly to the three mart tables in BigQuery:

- `mart_recent_arrivals` — live arrivals table with status and delay
- `mart_delay_distribution` — delay aggregated by origin airport
- `mart_delay_trend` — average delay over time by scheduled hour

To recreate the dashboard in your own GCP project: open [Looker Studio](https://lookerstudio.google.com), add a BigQuery data source, select your project → `krk_flights` dataset, and connect each mart table to a chart.

---

## Repository structure

```text
.
├── .env.example                   # Template for required environment variables
├── Makefile                       # Top-level commands: infra, docker, flink
├── docker-compose.yml             # Main local runtime stack
├── Dockerfile.airflow
├── Dockerfile.flink
├── Dockerfile.publisher
├── dashboards/
│   └── looker/                    # Dashboard screenshot
├── docs/
│   └── architecture.md            # Detailed schema and architecture notes
├── infra/
│   └── terraform/
│       ├── environments/
│       │   └── dev/               # Terraform entrypoint — GCP resources
│       │       └── terraform.tfvars.example
│       └── modules/
│           ├── bigquery/          # BigQuery dataset + partitioned raw table
│           └── gcs/               # GCS bucket
├── ingestion/
│   ├── producer.py                # AeroDataBox extraction and normalisation
│   └── publisher.py               # Continuous Kafka publisher
├── orchestration/
│   ├── dags/                      # Airflow DAGs
│   └── dbt_profiles/              # dbt profile mounted into Airflow
├── streaming/
│   └── flink-jobs/                # PyFlink Kafka→GCS job
└── warehouse/
    └── dbt/
        └── krk_flights_dbt/       # dbt project: staging / intermediate / marts
```

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop | ≥ 4.x | Runs all pipeline services |
| Terraform | ≥ 1.5.0 | Provisions GCP infrastructure |
| GCP account | — | Free tier is sufficient for dev |
| AeroDataBox API key | — | Free plan via RapidAPI (see Setup) |

---

## Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd DE-airport-project
```

### 2. Get an AeroDataBox API key

1. Go to [RapidAPI — AeroDataBox](https://rapidapi.com/aedbx-aedbx/api/aerodatabox).
2. Sign up or log in and subscribe to the **Basic (free)** plan.
3. Copy your `X-RapidAPI-Key` from the **Header Parameters** panel.

### 3. Create a GCP service account

1. In the [GCP Console](https://console.cloud.google.com) open **IAM & Admin → Service Accounts**.
2. Create a new service account and grant it the following roles:
   - **Storage Admin** (create/read/write GCS bucket)
   - **BigQuery Data Editor** (create/truncate tables)
   - **BigQuery Job User** (run load jobs and dbt queries)
3. Create a JSON key and save it to `creds/gcp-service-account.json` (this path is already in `.gitignore`).

### 4. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in:

```
AERODATABOX_API_KEY=<your RapidAPI key>
GCP_PROJECT_ID=<your GCP project ID>
GCS_BUCKET_NAME=<globally unique bucket name>
```

The remaining variables (`BQ_DATASET_ID`, `BQ_TABLE_ID`, Kafka settings) have sensible defaults and usually do not need changing.

### 5. Configure Terraform variables

```bash
cp infra/terraform/environments/dev/terraform.tfvars.example \
   infra/terraform/environments/dev/terraform.tfvars
```

Edit `terraform.tfvars` and set the same `project_id` and `bucket_name` you used in `.env`. The `credentials_file` path defaults to `../../../../creds/gcp-service-account.json` (relative to the Terraform working directory), which resolves to the file you placed in step 3.

---

## How to run

The `Makefile` is the single entry point. Run `make help` to see all targets.

### Step 1 — Provision GCP infrastructure

```bash
make infra-apply
```

This runs `terraform init` + `terraform apply` and creates:
- A GCS bucket for raw JSON files
- A BigQuery dataset (`krk_flights`)
- The `krk_arrivals_raw` table — partitioned by `ingested_at` date, clustered by `origin_icao` and `status_bucket`

### Step 2 — Start all Docker services

```bash
make up
```

Starts Redpanda, the publisher, Flink job manager and task manager, and Airflow. All services receive their configuration from `.env` and the mounted `creds/` directory — no GCP credentials are baked into the images.

### Step 3 — Submit the Flink streaming job

```bash
make flink-submit
```

Submits `write_to_gcs_job.py` to the running Flink cluster. The job reads from the `krk_arrivals` Kafka topic and writes JSON files to GCS continuously.

### Step 4 — Open Airflow

```
http://localhost:8085
```

Default credentials: `admin` / `admin`.

Enable or manually trigger the `krk_pipeline` DAG. It runs every 5 minutes and executes:
1. Load all GCS files → `krk_arrivals_raw` (BigQuery)
2. `dbt run` — rebuilds staging, intermediate, and mart tables
3. `dbt test` — validates the models

### Step 5 — (Optional) run dbt locally

```bash
cd warehouse/dbt/krk_flights_dbt
GCP_PROJECT_ID=<your-project> BQ_DATASET_ID=krk_flights dbt run --profiles-dir ../../../orchestration/dbt_profiles
```

### Full one-command setup

```bash
make setup   # infra-apply → up → flink-submit
```

### Tear down

```bash
make down            # stop Docker services
make infra-destroy   # destroy GCP resources
```

---

## Injecting credentials into Docker

**GCP service account** — the `creds/` directory is bind-mounted into each container that needs it:

```yaml
volumes:
  - ./creds:/opt/airflow/creds:ro   # Airflow / dbt
  - ./creds:/opt/creds:ro           # Flink job manager and task manager
```

The `GOOGLE_APPLICATION_CREDENTIALS` environment variable is set inside each service to point at the mounted JSON file. No credentials are copied into Docker images.

**AeroDataBox API key and GCP config** — passed via `.env` through Docker Compose `env_file`:

```yaml
env_file:
  - .env
```

This makes `AERODATABOX_API_KEY`, `GCP_PROJECT_ID`, `GCS_BUCKET_NAME`, and the BigQuery variables available inside every service that needs them.

---

## Notes

- **Raw layer is append-only in GCS.** Flink continuously writes new JSON files. The BigQuery raw table is rebuilt from all GCS files on every Airflow run (`WRITE_TRUNCATE`). This is intentional: GCS is the source of truth and the full history is always recoverable. At production scale, date-partitioned GCS paths and `WRITE_APPEND` per partition would be more efficient.
- **Deduplication happens in dbt.** `int_latest_arrivals` keeps the latest `ingested_at` record per `flight_number + scheduled_arrival_utc`. Mart tables are built on top of this deduplicated view.
- **BigQuery table is pre-created by Terraform** with DAY partitioning on `ingested_at` and clustering on `origin_icao`, `status_bucket`. This optimises query cost on the mart tables that filter or group by these columns.
