resource "google_storage_bucket_object" "genomic_processing_dag" {
  name   = "dags/genomic_processing_dag.py"
  bucket = replace(replace(google_composer_environment.genomics_composer_v3.config[0].dag_gcs_prefix, "gs://", ""), "/dags", "")
  source = "${path.module}/../../dags/genomic_processing_dag.py"
}
