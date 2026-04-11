output "dataset_id" {
  value = google_bigquery_dataset.this.dataset_id
}

output "raw_table_id" {
  value = google_bigquery_table.krk_arrivals_raw.table_id
}
