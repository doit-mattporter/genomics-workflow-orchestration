data "archive_file" "genomic_dag_kickoff_source_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../../functions/gcf_genomic_dag_kickoff/"
  output_path = "${path.module}/../tmp/genomic-dag-kickoff.zip"
}

resource "google_storage_bucket_object" "genomic_dag_kickoff_function_source" {
  name   = "genomic-dag-kickoff-${data.archive_file.genomic_dag_kickoff_source_zip.output_md5}.zip"
  bucket = google_storage_bucket.cloud_function_bucket.name
  source = data.archive_file.genomic_dag_kickoff_source_zip.output_path
}

resource "google_service_account" "genomic_dag_kickoff_cf_service_account" {
  account_id   = "genomic-dag-kickoff-cf-sa"
  display_name = "Genomic DAG Kickoff Cloud Function service account"
  project      = local.env_configs[terraform.workspace]["project_id"]
}

resource "google_cloudfunctions2_function" "genomic_dag_kickoff" {
  name        = "genomic-dag-kickoff"
  location    = local.env_configs[terraform.workspace]["region"]
  description = "Kick off the DAG for processing genomic data"

  build_config {
    runtime     = "python312"
    entry_point = "gcs_message"
    source {
      storage_source {
        bucket = google_storage_bucket.cloud_function_bucket.name
        object = google_storage_bucket_object.genomic_dag_kickoff_function_source.name
      }
    }
  }

  service_config {
    available_memory      = "512Mi"
    available_cpu         = 1
    timeout_seconds       = 540
    max_instance_count    = 20
    min_instance_count    = 1
    service_account_email = google_service_account.genomic_dag_kickoff_cf_service_account.email
    environment_variables = {
      REFERENCE_BUCKET       = google_storage_bucket.genomic_reference_bucket.name
      AIRFLOW_WEB_SERVER_URL = google_composer_environment.genomics_composer_v3.config[0].airflow_uri
      LOG_EXECUTION_ID       = true # LOG_EXECUTION_ID = true is added after every Terraform apply anyway
    }
  }

  event_trigger {
    event_type            = "google.cloud.storage.object.v1.finalized"
    retry_policy          = "RETRY_POLICY_RETRY"
    service_account_email = google_service_account.genomic_dag_kickoff_cf_service_account.email
    event_filters {
      attribute = "bucket"
      value     = google_storage_bucket.genomic_inputs_bucket.name
    }
  }
}

resource "google_storage_bucket_iam_member" "genomic_dag_kickoff_gcs_viewer" {
  bucket = google_storage_bucket.genomic_inputs_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.genomic_dag_kickoff_cf_service_account.email}"
}

resource "google_project_iam_member" "genomic_dag_kickoff_event_receiver" {
  project = local.env_configs[terraform.workspace]["project_id"]
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.genomic_dag_kickoff_cf_service_account.email}"
}

data "google_iam_policy" "genomic_dag_kickoff_invoker" {
  binding {
    role = "roles/run.invoker"
    members = [
      "serviceAccount:${google_service_account.genomic_dag_kickoff_cf_service_account.email}",
    ]
  }
}

resource "google_cloud_run_service_iam_policy" "genomic_dag_kickoff_invoker_policy" {
  location = local.env_configs[terraform.workspace]["region"]
  project  = local.env_configs[terraform.workspace]["project_id"]
  service  = google_cloudfunctions2_function.genomic_dag_kickoff.name

  policy_data = data.google_iam_policy.genomic_dag_kickoff_invoker.policy_data
}

resource "google_project_iam_member" "genomic_dag_kickoff_composer_user" {
  project = local.env_configs[terraform.workspace]["project_id"]
  role    = "roles/composer.user"
  member  = "serviceAccount:${google_service_account.genomic_dag_kickoff_cf_service_account.email}"
}
