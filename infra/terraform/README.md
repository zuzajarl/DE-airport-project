# Terraform

Use reusable modules for GCS, BigQuery, and IAM, then compose them in `environments/dev`.

Recommended first resources:

- GCS raw bucket
- BigQuery dataset
- service account for ingestion
- service account for Flink sinks

## Usage

From `infra/terraform/environments/dev`:

```bash
terraform init
cp terraform.tfvars.example terraform.tfvars
terraform plan
terraform apply
```

This setup provisions:

- GCS bucket for raw arrivals files
- BigQuery dataset for warehouse tables
- one service account for pipeline access
- IAM bindings for GCS and BigQuery
