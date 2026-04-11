resource "google_bigquery_dataset" "this" {
  dataset_id                 = var.dataset_id
  location                   = var.location
  delete_contents_on_destroy = var.delete_contents_on_destroy
}

resource "google_bigquery_table" "krk_arrivals_raw" {
  dataset_id          = google_bigquery_dataset.this.dataset_id
  table_id            = "krk_arrivals_raw"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "ingested_at"
  }

  clustering = ["origin_icao", "status_bucket"]

  schema = jsonencode([
    { name = "flight_number",         type = "STRING",    mode = "NULLABLE" },
    { name = "status",                type = "STRING",    mode = "NULLABLE" },
    { name = "airline_name",          type = "STRING",    mode = "NULLABLE" },
    { name = "origin_name",           type = "STRING",    mode = "NULLABLE" },
    { name = "scheduled_arrival_utc", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "revised_arrival_utc",   type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "origin_icao",           type = "STRING",    mode = "NULLABLE" },
    { name = "airline_icao",          type = "STRING",    mode = "NULLABLE" },
    { name = "delay_minutes",         type = "FLOAT64",   mode = "NULLABLE" },
    { name = "status_bucket",         type = "STRING",    mode = "NULLABLE" },
    { name = "ingested_at",           type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}
