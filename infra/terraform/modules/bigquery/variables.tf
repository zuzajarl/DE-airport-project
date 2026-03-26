variable "dataset_id" {
  type = string
}

variable "location" {
  type = string
}

variable "delete_contents_on_destroy" {
  type    = bool
  default = false
}
