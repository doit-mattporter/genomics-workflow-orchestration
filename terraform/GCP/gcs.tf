resource "random_id" "genomic_inputs_bucket_suffix" {
  byte_length = 8
}

resource "google_storage_bucket" "genomic_inputs_bucket" {
  name                        = "genomic-inputs-${random_id.genomic_inputs_bucket_suffix.hex}"
  location                    = local.env_configs[terraform.workspace]["region"]
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "genomic_reference_bucket" {
  name                        = "genomic-reference-${random_id.genomic_inputs_bucket_suffix.hex}"
  location                    = local.env_configs[terraform.workspace]["region"]
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
}

data "google_compute_default_service_account" "default" {
  project = local.env_configs[terraform.workspace]["project_id"]
}

resource "google_storage_bucket" "genomic_outputs_bucket" {
  name                        = "genomic-outputs-${random_id.genomic_inputs_bucket_suffix.hex}"
  location                    = local.env_configs[terraform.workspace]["region"]
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
}

resource "random_id" "cf_bucket_suffix" {
  byte_length = 8
}

resource "google_storage_bucket" "cloud_function_bucket" {
  name     = "genomics-gcf-source-${random_id.cf_bucket_suffix.hex}"
  location = local.env_configs[terraform.workspace]["region"]
}

data "google_storage_project_service_account" "gcs_account" {
  project = local.env_configs[terraform.workspace]["project_id"]
}

resource "google_project_iam_member" "gcs_pubsub_publisher" {
  project = local.env_configs[terraform.workspace]["project_id"]
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"
}
