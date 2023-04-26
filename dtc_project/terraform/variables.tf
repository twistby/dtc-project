locals {
  data_lake_bucket = "dtc-de-chicago"
}

locals {
  data_lake_bucket_dev = "dtc-de-chicago-dev"
}
variable "project" {
  description = "GCP project ID"
  default = "project-id-777888"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "resource-location6"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "chicago"
}
variable "BQ_DATASET_PROD" {
  description = "BigQuery Dataset for production tables"
  type = string
  default = "chicago_prod"
}