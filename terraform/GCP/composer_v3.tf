resource "google_composer_environment" "genomics_composer_v3" {
  name   = "my-composer-env"
  region = local.env_configs[terraform.workspace].region

  config {
    software_config {
      image_version = "composer-3-airflow-2"
      airflow_config_overrides = {
        "core-dags_are_paused_at_creation" = "False"
      }
      env_variables = {
        GCP_PROJECT_ID = local.env_configs[terraform.workspace]["project_id"]
        GCP_PROJECT_NUMBER = local.env_configs[terraform.workspace]["project_number"]
        GENOMICS_PROCESSING_REGION = local.env_configs[terraform.workspace].region
        GENOMICS_PROCESSING_ZONE = local.env_configs[terraform.workspace].zone
        GENOMICS_REFERENCE_BUCKET = google_storage_bucket.genomic_reference_bucket.name
        GENOMICS_OUTPUT_BUCKET = google_storage_bucket.genomic_outputs_bucket.name
      }
    }

    workloads_config {
      scheduler {
        cpu        = 1
        memory_gb  = 4
        storage_gb = 10
      }
      web_server {
        cpu        = 1
        memory_gb  = 4
        storage_gb = 10
      }
      worker {
        cpu        = 1
        memory_gb  = 4
        storage_gb = 10
        min_count  = 1
        max_count  = 2
      }
    }
  }
}
