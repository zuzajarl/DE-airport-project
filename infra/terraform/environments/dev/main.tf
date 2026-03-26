module "gcs" {
  source = "../../modules/gcs"

  bucket_name        = var.bucket_name
  location           = var.location
  force_destroy      = false
  versioning_enabled = false
}

module "bigquery" {
  source = "../../modules/bigquery"

  dataset_id                 = var.dataset_id
  location                   = var.location
  delete_contents_on_destroy = false
}
