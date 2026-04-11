output "bucket_name" {
  value = module.gcs.bucket_name
}

output "dataset_id" {
  value = module.bigquery.dataset_id
}

output "raw_table_id" {
  value = module.bigquery.raw_table_id
}
