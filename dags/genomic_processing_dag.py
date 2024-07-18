from airflow import DAG
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineInsertInstanceOperator,
)
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago
from google.cloud import storage
from math import ceil

import os
import re

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
PROJECT_NUMBER = os.getenv("GCP_PROJECT_NUMBER")
REGION = os.getenv("GENOMICS_PROCESSING_REGION")
ZONE = os.getenv("GENOMICS_PROCESSING_ZONE")
GENOMICS_REFERENCE_BUCKET = os.getenv("GENOMICS_REFERENCE_BUCKET")
GENOMICS_OUTPUT_BUCKET = os.getenv("GENOMICS_OUTPUT_BUCKET")
PUBSUB_SUBSCRIPTION = os.getenv("PUBSUB_SUBSCRIPTION")


default_args = {"owner": "airflow", "start_date": days_ago(1), "retries": 0}

dag = DAG(
    "genomic_processing_dag",
    default_args=default_args,
    description="A DAG to process cat genomic data. The Pub/Sub messages that kick this off are generated when sequencing objects are uploaded to the data source bucket.",
    schedule_interval=None,
    catchup=False,
)


# Establish the startup script and metadata for the instance that will perform genomic analyses
def prepare_genomic_analysis_instance(**context):
    job_details = context["dag_run"].conf
    if not job_details:
        print("No configuration data received")
        return

    print(f"job_details: {job_details}")
    genome = job_details["genome"].split(".fna")[0].split(".fa")[0]
    genome_sanitized = re.sub(
        r"[^a-zA-Z0-9-]", "", genome
    )  # For use in naming instance & disk
    reference_bucket = job_details["reference_bucket"]
    fastq_bucket = job_details["fastq_bucket"]
    fastq_file_paths = job_details["fastq_file_paths"]
    fastq_file_paths_bash_compatible = " ".join(job_details["fastq_file_paths"])

    sample_ids = ""
    for fastq_file in fastq_file_paths:
        sample_id = fastq_file.split(".")[3]
        sample_ids += f"{sample_id}_"
    sample_ids = sample_ids.rstrip("_")
    sample_ids_sanitized = re.sub(
        r"[^a-zA-Z0-9-]", "", sample_ids
    )  # For use in naming instance & disk

    results_bam_uri = f"gs://{GENOMICS_OUTPUT_BUCKET}/{sample_ids}_{genome}.bam"
    results_vcf_uri = f"gs://{GENOMICS_OUTPUT_BUCKET}/{sample_ids}_{genome}.vcf.gz"
    results_gvcf_uri = f"gs://{GENOMICS_OUTPUT_BUCKET}/{sample_ids}_{genome}.g.vcf.gz"
    results_bam_object_path = f"{sample_ids}_{genome}.bam"
    results_vcf_object_path = f"{sample_ids}_{genome}.vcf.gz"
    results_gvcf_object_path = f"{sample_ids}_{genome}.g.vcf.gz"

    # Fetch the size of the FASTQ files in order to size the disk to 12X their total size
    storage_client = storage.Client()
    fastq_bucket_client = storage_client.bucket(fastq_bucket)
    total_size_bytes = 0
    for fastq_file in job_details["fastq_file_paths"]:
        blob = fastq_bucket_client.get_blob(fastq_file)
        total_size_bytes += blob.size

    total_size_gb = total_size_bytes / (1024**3)
    disk_size_gb = max(50, ceil(total_size_gb * 12))

    # Construct the startup script
    instance_name = (
        f"sample{sample_ids_sanitized}-{genome_sanitized}"[:62].lower().rstrip("-")
    )

    # # Run BWA MEM
    # bwa-mem2 mem -t $NUM_CORES -R "$read_group" $genome $fastq > aligned_${{sample_id}}_${{genome_name}}.sam

    # # Convert SAM to BAM
    # samtools view -Sb aligned_${{sample_id}}_${{genome_name}}.sam > aligned_${{sample_id}}_${{genome_name}}.bam

    # # Sort the BAM file
    # samtools sort aligned_${{sample_id}}_${{genome_name}}.bam -o ${{sample_id}}_${{genome_name}}.bam
    startup_script = f"""#!/bin/bash
# trap 'gcloud compute instances delete {instance_name} --zone=${ZONE} --quiet' ERR EXIT

# Install software required to run alignment against a reference genome
apt-get install -y python3-pip htop docker.io bzip2
python3 -m pip install google-cloud-storage --break-system-packages
sudo usermod -aG docker $USER
newgrp docker
NUM_CORES=$(nproc)

# Prepare GATK
docker pull broadinstitute/gatk:latest &

# Install bwa-mem2
curl -L https://github.com/bwa-mem2/bwa-mem2/releases/download/v2.2.1/bwa-mem2-2.2.1_x64-linux.tar.bz2 -o bwa-mem2-2.2.1_x64-linux.tar.bz2
tar -xjf bwa-mem2-2.2.1_x64-linux.tar.bz2
sudo mv bwa-mem2-2.2.1_x64-linux/bwa-mem2* /usr/bin/
rm -rf bwa-mem2-2.2.1_x64-linux bwa-mem2-2.2.1_x64-linux.tar.bz2

# Install samtools
sudo apt-get install -y autoconf automake make gcc perl zlib1g-dev libbz2-dev liblzma-dev libcurl4-gnutls-dev libssl-dev libncurses5-dev libdeflate-dev
wget https://github.com/samtools/samtools/releases/download/1.20/samtools-1.20.tar.bz2
tar -xjf samtools-1.20.tar.bz2
cd samtools-1.20
autoheader
autoconf -Wno-syntax
./configure
make
sudo make install
sudo cp samtools /usr/bin/
cd ../
rm -f samtools-1.20.tar.bz2

# Download reference genome files
reference_blobs=$(gcloud storage ls gs://{reference_bucket}/{genome}*)
for blob in $reference_blobs; do
    gcloud storage cp $blob /tmp/ &
done

# Download FASTQ files
for fastq_file_path in {fastq_file_paths_bash_compatible}; do
    gcloud storage cp gs://{fastq_bucket}/$fastq_file_path /tmp/ &
done

wait

# Align sequencing data against reference
cd /tmp/
genome=$(ls *.fna)
genome_name=${{genome%.fna}}

# Define a function to send each FASTQ file through alignment
process_fastq() {{
    fastq=$1
    sample_id=$(echo $fastq | awk -F. '{{print $4"_"$5}}')

    # Construct the read group information
    read_group="@RG\\tID:${{sample_id}}\\tSM:sample\\tPL:ILLUMINA"

    # Run BWA MEM, Convert SAM to BAM, and sort the BAM
    bwa-mem2 mem -t $NUM_CORES -R "$read_group" $genome $fastq | samtools view -Sb - | samtools sort -o ${{sample_id}}_${{genome_name}}.bam

    # Index the sorted BAM file
    samtools index ${{sample_id}}_${{genome_name}}.bam
}}

# Collect all sample IDs
unique_sample_ids=$(for fastq in *.fastq.gz; do echo $fastq | awk -F. '{{print $4}}'; done | sort | uniq | tr '\\n' '_')
unique_sample_ids=${{unique_sample_ids%_}}

# Run the process_fastq function in parallel for each FASTQ file
for fastq in *.fastq.gz; do
    process_fastq $fastq &
done

# Wait for all alignment processes to complete
wait

# Merge BAM Files
bam_files=$(ls *_${{genome_name}}.bam | tr '\\n' ' ')

samtools merge merged_tmp_${{genome_name}}_${{unique_sample_ids}}.bam $bam_files
samtools sort -o merged_${{genome_name}}_${{unique_sample_ids}}.bam merged_tmp_${{genome_name}}_${{unique_sample_ids}}.bam
samtools index merged_${{genome_name}}_${{unique_sample_ids}}.bam

gcloud storage cp merged_${{genome_name}}_${{unique_sample_ids}}.bam {results_bam_uri} &
gcloud storage cp merged_${{genome_name}}_${{unique_sample_ids}}.bam.bai {results_bam_uri}.bai &

# Perform variant calling with GATK
docker run --rm -v /tmp/:/data broadinstitute/gatk:latest \\
    gatk --java-options "-Xmx600G" HaplotypeCaller \\
    -R /data/${{genome}} \\
    -I /data/merged_${{genome_name}}_${{unique_sample_ids}}.bam \\
    -O /data/${{unique_sample_ids}}_${{genome_name}}.vcf.gz && gcloud storage cp /tmp/${{unique_sample_ids}}_${{genome_name}}.vcf.gz {results_vcf_uri} &

# Also create a gVCF
docker run --rm -v /tmp/:/data broadinstitute/gatk:latest \\
    gatk --java-options "-Xmx600G" HaplotypeCaller \\
    -R /data/${{genome}} \\
    -I /data/merged_${{genome_name}}_${{unique_sample_ids}}.bam \\
    -O /data/${{unique_sample_ids}}_${{genome_name}}.g.vcf.gz \\
    -ERC GVCF && gcloud storage cp /tmp/${{unique_sample_ids}}_${{genome_name}}.g.vcf.gz {results_gvcf_uri} &

wait
"""
    # Send to XCom the details required to spin up a genomic analysis instance and monitor GCS for its output
    context["task_instance"].xcom_push(
        key="genome_results_bucket", value=GENOMICS_OUTPUT_BUCKET
    )
    context["task_instance"].xcom_push(
        key="results_bam_object_path", value=results_bam_object_path
    )
    context["task_instance"].xcom_push(
        key="results_vcf_object_path", value=results_vcf_object_path
    )
    context["task_instance"].xcom_push(
        key="results_gvcf_object_path", value=results_gvcf_object_path
    )
    context["task_instance"].xcom_push(key="instance_name", value=instance_name)
    context["task_instance"].xcom_push(
        # key="machine_type", value=f"zones/{ZONE}/machineTypes/c3-standard-8"
        key="machine_type", value=f"zones/{ZONE}/machineTypes/c4-standard-192"
    )
    context["task_instance"].xcom_push(
        key="subnetwork", value=f"regions/{REGION}/subnetworks/default"
    )
    context["task_instance"].xcom_push(
        key="disk_type", value=f"zones/{ZONE}/diskTypes/hyperdisk-balanced"
    )
    context["task_instance"].xcom_push(key="disk_size_gb", value=int(disk_size_gb))
    context["task_instance"].xcom_push(key="startup_script", value=startup_script)
    context["task_instance"].xcom_push(
        key="sa_email", value=f"{PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
    )


# Task to process job details message and create the startup script for a genomic processing GCE instance
prepare_genomic_analysis_instance_task = PythonOperator(
    task_id="prepare_genomic_analysis_instance",
    python_callable=prepare_genomic_analysis_instance,
    dag=dag,
)

start_alignment_instance_task = ComputeEngineInsertInstanceOperator(
    task_id="start_alignment_instance",
    project_id=PROJECT_ID,
    zone=ZONE,
    body={
        "name": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='instance_name') }}",
        "machine_type": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='machine_type') }}",
        "disks": [
            {
                "boot": True,
                "auto_delete": True,
                "device_name": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='instance_name') }}",
                "initialize_params": {
                    "disk_size_gb": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='disk_size_gb') | int }}",
                    "disk_type": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='disk_type') }}",
                    "source_image": "projects/debian-cloud/global/images/family/debian-12",
                },
            }
        ],
        "metadata": {
            "items": [
                {
                    "key": "startup-script",
                    "value": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='startup_script') }}",
                }
            ]
        },
        "network_interfaces": [
            {
                "network": "global/networks/default",
                "subnetwork": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='subnetwork') }}",
                "nic_type": "GVNIC",
                "stack_type": "IPV4_ONLY",
                "access_configs": [{"name": "External NAT", "network_tier": "PREMIUM"}],
            }
        ],
        "service_accounts": [
            {
                "email": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='sa_email') }}",
                "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
            }
        ],
    },
    dag=dag,
)

wait_for_bam_gcs_file = GCSObjectExistenceSensor(
    task_id="wait_for_bam_gcs_file",
    bucket="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='genome_results_bucket') }}",
    object="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='results_bam_object_path') }}",
    google_cloud_conn_id="google_cloud_default",
    timeout=10800,
    poke_interval=30,
    mode="poke",
    dag=dag,
)

wait_for_vcf_gcs_file = GCSObjectExistenceSensor(
    task_id="wait_for_vcf_gcs_file",
    bucket="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='genome_results_bucket') }}",
    object="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='results_vcf_object_path') }}",
    google_cloud_conn_id="google_cloud_default",
    timeout=10800,
    poke_interval=30,
    mode="poke",
    dag=dag,
)

wait_for_gvcf_gcs_file = GCSObjectExistenceSensor(
    task_id="wait_for_gvcf_gcs_file",
    bucket="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='genome_results_bucket') }}",
    object="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis_instance', key='results_gvcf_object_path') }}",
    google_cloud_conn_id="google_cloud_default",
    timeout=10800,
    poke_interval=30,
    mode="poke",
    dag=dag,
)

# Define DAG task dependencies
(
    prepare_genomic_analysis_instance_task
    >> start_alignment_instance_task
    >> wait_for_bam_gcs_file
    >> wait_for_vcf_gcs_file
    >> wait_for_gvcf_gcs_file
)
