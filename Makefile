## Load .env so all variables are available to make targets
ifneq (,$(wildcard .env))
  include .env
  export
endif

TF_DIR = infra/terraform/environments/dev

.PHONY: help infra-init infra-apply infra-destroy up flink-submit down setup

help:
	@echo ""
	@echo "KRK Arrivals Streaming Pipeline — available targets"
	@echo "----------------------------------------------------"
	@echo "  make infra-init     Initialise Terraform (run once)"
	@echo "  make infra-apply    Provision GCS bucket + BigQuery dataset/table"
	@echo "  make infra-destroy  Tear down GCP infrastructure"
	@echo "  make up             Build and start all Docker services"
	@echo "  make flink-submit   Submit the PyFlink Kafka→GCS streaming job"
	@echo "  make down           Stop all Docker services"
	@echo "  make setup          Full setup: infra-apply → up → flink-submit"
	@echo ""

## ── Infrastructure (Terraform) ───────────────────────────────────────────────

infra-init:
	terraform -chdir=$(TF_DIR) init

infra-apply: infra-init
	terraform -chdir=$(TF_DIR) apply

infra-destroy:
	terraform -chdir=$(TF_DIR) destroy

## ── Docker services ──────────────────────────────────────────────────────────

up:
	docker compose up --build -d redpanda publisher jobmanager taskmanager airflow

down:
	docker compose down

## ── Flink job ────────────────────────────────────────────────────────────────

flink-submit:
	docker compose exec jobmanager flink run -py /opt/flink/jobs/write_to_gcs_job.py

## ── Full setup ───────────────────────────────────────────────────────────────

setup: infra-apply up flink-submit
