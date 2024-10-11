import os
from google.auth import default
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage


AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = default(scopes=[AUTH_SCOPE])

REFERENCE_BUCKET = os.environ.get("REFERENCE_BUCKET")
AIRFLOW_WEB_SERVER_URL = os.getenv("AIRFLOW_WEB_SERVER_URL")

GENOMES = [
    "GCF_000181335.3_Felis_catus_9.0_genomic.fna",
    "GCF_018350175.1_F.catus_Fca126_mat1.0_genomic.fna",
]


def make_composer2_web_server_request(url, method="GET", **kwargs):
    """Make a request to Cloud Composer environment's web server."""
    authed_session = AuthorizedSession(CREDENTIALS)
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90
    return authed_session.request(method, url, **kwargs)


def trigger_dag(dag_id, job_details):
    """Make a request to trigger a DAG using the Airflow REST API."""
    endpoint = f"api/v1/dags/{dag_id}/dagRuns"
    request_url = f"{AIRFLOW_WEB_SERVER_URL}/{endpoint}"
    json_data = {"conf": job_details}
    response = make_composer2_web_server_request(
        request_url, method="POST", json=json_data
    )
    if response.status_code == 403:
        raise Exception(f"Permission denied: {response.text}")
    elif response.status_code != 200:
        response.raise_for_status()
    else:
        print(f"DAG triggered successfully: {response.text}")


def read_ready_file(bucket_name, object_name):
    """Read the 'ready.txt' file and return the list of FASTQ file paths."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    fastq_files = blob.download_as_text().splitlines()
    cleaned_files = [
        x.removeprefix("gs://").removeprefix(f"{bucket_name}/")
        for x in fastq_files
        if x.strip()
    ]
    return cleaned_files


def gcs_message(data, context):
    """Cloud Function triggered by Cloud Storage that will publish a message to a Pub/Sub topic to kickoff Cloud Composer DAG execution of genomics workloads when a 'ready.txt' file containing FASTQ GCS filepaths is uploaded."""
    fastq_bucket = data["bucket"]
    object_path = data["name"]
    if object_path.endswith("ready.txt"):
        fastq_file_paths = read_ready_file(fastq_bucket, object_path)
        for genome in GENOMES:
            job_details = {
                "pipeline": "genomic_processing",
                "reference_bucket": REFERENCE_BUCKET,
                "genome": genome,
                "fastq_bucket": fastq_bucket,
                "fastq_file_paths": fastq_file_paths,
            }
            trigger_dag("genomic_processing_dag", job_details)
