from airflow import DAG
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineInsertInstanceOperator,
    ComputeEngineDeleteInstanceOperator,
)
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from google.cloud import storage
from math import ceil

import os
import re
import uuid

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
def prepare_genomic_analysis_func(**context):
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
    fastq_file_paths_bash_list = " ".join(job_details["fastq_file_paths"])

    sample_ids_set = set()
    for fastq_file in fastq_file_paths:
        sample_ids_set.add(fastq_file.split(".")[3])

    sample_ids = "_".join(sample_ids_set)
    sample_ids_sanitized = re.sub(
        r"[^a-zA-Z0-9-]", "", sample_ids
    )  # For use in naming instance & disk

    results_bam_filename = f"{sample_ids}_{genome}.bam"
    results_bam_uri = f"gs://{GENOMICS_OUTPUT_BUCKET}/{sample_ids}_{genome}.bam"
    results_vcf_filename = f"{sample_ids}_{genome}.vcf.gz"
    results_vcf_uri = f"gs://{GENOMICS_OUTPUT_BUCKET}/{sample_ids}_{genome}.vcf.gz"
    results_gvcf_filename = f"{sample_ids}_{genome}.g.vcf.gz"
    results_gvcf_uri = f"gs://{GENOMICS_OUTPUT_BUCKET}/{sample_ids}_{genome}.g.vcf.gz"
    results_bam_object_path = f"{sample_ids}_{genome}.bam"
    results_bai_object_path = f"{sample_ids}_{genome}.bam.bai"
    results_vcf_object_path = f"{sample_ids}_{genome}.vcf.gz"
    results_gvcf_object_path = f"{sample_ids}_{genome}.g.vcf.gz"
    results_vcf_object_prefix = f"{sample_ids}_{genome}"

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
    random_uuid = str(uuid.uuid4())[:5]
    alignment_instance_name = (
        f"al{random_uuid}-sample{sample_ids_sanitized}-{genome_sanitized}"[:62]
        .lower()
        .rstrip("-")
    )
    variant_calling_instance_name = (
        f"vc{random_uuid}-sample{sample_ids_sanitized}-{genome_sanitized}"[:62]
        .lower()
        .rstrip("-")
    )
    alignment_startup_script = f"""#!/bin/bash
# trap 'gcloud compute instances delete {alignment_instance_name} --zone={ZONE} --quiet' ERR EXIT

# Install software required to run alignment against a reference genome
apt-get install -y python3-pip htop bzip2 bc openjdk-17-jre
python3 -m pip install google-cloud-storage --break-system-packages
NUM_CORES=$(nproc)

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

# Install Picard
wget https://github.com/broadinstitute/picard/releases/download/3.2.0/picard.jar -O /usr/bin/picard.jar

# Download reference genome files
reference_blobs=$(gcloud storage ls gs://{reference_bucket}/{genome}*)
for blob in $reference_blobs; do
    gcloud storage cp $blob /tmp/ &
done

# Download FASTQ files
for fastq_file_path in {fastq_file_paths_bash_list}; do
    gcloud storage cp gs://{fastq_bucket}/$fastq_file_path /tmp/ &
done

# Define a function to send each FASTQ file through alignment
process_fastq() {{
    fastq=$1
    tmp_unique_id=$(echo $fastq | awk -F. '{{print $4"_"$5"_"$6}}')

    # Construct the read group information
    read_group="@RG\\tID:${{tmp_unique_id}}\\tSM:sample\\tPL:ILLUMINA"

    # Adjust core usage based on the number of parallel jobs being run (in order to limit 'samtools sort' memory usage)
    NUM_JOBS=$(ls *.fastq.gz | wc -l)
    CORES_PER_JOB=$((NUM_CORES / NUM_JOBS))

    # Run BWA MEM, Convert SAM to BAM, and sort the BAM
    bwa-mem2 mem -t $NUM_CORES -R "$read_group" $genome $fastq | samtools sort -@ $CORES_PER_JOB -m 2048M -o ${{tmp_unique_id}}_${{genome_name}}.bam

    # Index the sorted BAM file
    samtools index ${{tmp_unique_id}}_${{genome_name}}.bam
}}

# Calculate 90% of total memory; we'll use this for the MarkDuplicates step later
total_mem_kb=$(grep MemTotal /proc/meminfo | awk '{{print $2}}')
mem_for_md=$(echo "scale=2; (${{total_mem_kb}} * 0.9) / 1024 / 1024" | bc)
rounded_mem_for_md=$(printf "%.0f" "$mem_for_md")

# Wait for FASTQs and FASTA files to finish downloading
cd /tmp/
wait

# Prepare for aligning sequencing data against reference
genome=$(ls *.fna)
genome_name=${{genome%.fna}}

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
samtools merge -@ $NUM_CORES -u - ${{bam_files}} | samtools sort -@ $NUM_CORES -m 3G -o merged_${{genome_name}}_${{unique_sample_ids}}.bam
samtools index merged_${{genome_name}}_${{unique_sample_ids}}.bam

# Mark duplicates
java -Xmx${{rounded_mem_for_md}}g -jar /usr/bin/picard.jar MarkDuplicates I=merged_${{genome_name}}_${{unique_sample_ids}}.bam O=dedup_merged_${{genome_name}}_${{unique_sample_ids}}.bam M=marked_dup_metrics.txt
samtools index dedup_merged_${{genome_name}}_${{unique_sample_ids}}.bam

# Upload final BAM file to Cloud Storage
gcloud storage cp dedup_merged_${{genome_name}}_${{unique_sample_ids}}.bam.bai {results_bam_uri}.bai &
gcloud storage cp dedup_merged_${{genome_name}}_${{unique_sample_ids}}.bam {results_bam_uri}
wait
"""
    # Variant calling isn't very parallelizable and it takes ~1 day to run, so let's run it on a smaller instance
    variant_calling_startup_script = f"""#!/bin/bash
# trap 'gcloud compute instances delete {variant_calling_instance_name} --zone={ZONE} --quiet' ERR EXIT

# Install software required to run alignment against a reference genome
apt-get install -y python3-pip htop docker.io bzip2 bc
python3 -m pip install google-cloud-storage --break-system-packages
sudo usermod -aG docker $USER
newgrp docker
NUM_CORES=$(nproc)

# Prepare GATK
docker pull broadinstitute/gatk:latest &

# Download reference genome files
reference_blobs=$(gcloud storage ls gs://{reference_bucket}/{genome}*)
for blob in $reference_blobs; do
    gcloud storage cp $blob /tmp/ &
done

# Download BAM file
gcloud storage cp {results_bam_uri} /tmp/ &
gcloud storage cp {results_bam_uri}.bai /tmp/ &

wait

cd /tmp/
genome=$(ls *.fna)
genome_name=${{genome%.fna}}

# Calculate 90% of total memory, then half of that value, and round up.
# Split this memory across 2 GATK jobs (VCF and gVCF production).
total_mem_kb=$(grep MemTotal /proc/meminfo | awk '{{print $2}}')
mem_for_gatk=$(echo "scale=2; ((${{total_mem_kb}} * 0.9) / 2) / 1024 / 1024" | bc)
rounded_mem_for_gatk=$(printf "%.0f" "$mem_for_gatk")

# Perform variant calling with GATK
docker run --rm -v /tmp/:/data broadinstitute/gatk:latest \\
    gatk --java-options "-Xmx${{rounded_mem_for_gatk}}G" HaplotypeCaller \\
    -R /data/${{genome}} \\
    -I /data/{results_bam_filename} \\
    -O /data/{results_vcf_filename} && gcloud storage cp /tmp/{results_vcf_filename} {results_vcf_uri} &

# Also create a gVCF
docker run --rm -v /tmp/:/data broadinstitute/gatk:latest \\
    gatk --java-options "-Xmx${{rounded_mem_for_gatk}}G" HaplotypeCaller \\
    -R /data/${{genome}} \\
    -I /data/{results_bam_filename} \\
    -O /data/{results_gvcf_filename} \\
    -ERC GVCF && gcloud storage cp /tmp/{results_gvcf_filename} {results_gvcf_uri} &

wait
"""
    # Send to XCom the details required to spin up genomic analysis instances and monitor GCS for their outputs
    context["task_instance"].xcom_push(
        key="genome_results_bucket", value=GENOMICS_OUTPUT_BUCKET
    )
    context["task_instance"].xcom_push(
        key="results_bam_object_path", value=results_bam_object_path
    )
    context["task_instance"].xcom_push(
        key="results_bai_object_path", value=results_bai_object_path
    )
    context["task_instance"].xcom_push(
        key="results_vcf_object_path", value=results_vcf_object_path
    )
    context["task_instance"].xcom_push(
        key="results_gvcf_object_path", value=results_gvcf_object_path
    )
    context["task_instance"].xcom_push(
        key="results_vcf_object_prefix", value=results_vcf_object_prefix
    )
    context["task_instance"].xcom_push(
        key="alignment_instance_name", value=alignment_instance_name
    )
    context["task_instance"].xcom_push(
        key="variant_calling_instance_name", value=variant_calling_instance_name
    )
    context["task_instance"].xcom_push(
        key="alignment_machine_type", value=f"zones/{ZONE}/machineTypes/c3-standard-88"
    )
    context["task_instance"].xcom_push(
        key="variant_calling_machine_type",
        value=f"zones/{ZONE}/machineTypes/c3-standard-8",
    )
    context["task_instance"].xcom_push(
        key="subnetwork", value=f"regions/{REGION}/subnetworks/default"
    )
    context["task_instance"].xcom_push(
        key="disk_type", value=f"zones/{ZONE}/diskTypes/hyperdisk-balanced"
    )
    context["task_instance"].xcom_push(key="disk_size_gb", value=int(disk_size_gb))
    context["task_instance"].xcom_push(
        key="alignment_startup_script", value=alignment_startup_script
    )
    context["task_instance"].xcom_push(
        key="variant_calling_startup_script", value=variant_calling_startup_script
    )
    context["task_instance"].xcom_push(
        key="sa_email", value=f"{PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
    )


# Task to process job details message and create the startup script for a genomic processing GCE instance
prepare_genomic_analysis = PythonOperator(
    task_id="prepare_genomic_analysis",
    python_callable=prepare_genomic_analysis_func,
    dag=dag,
)

# Start running alignment of FASTQs against the reference genome
start_alignment_instance = ComputeEngineInsertInstanceOperator(
    task_id="start_alignment_instance",
    trigger_rule="none_failed_min_one_success",  # Allows for upstream branching DAG logic
    project_id=PROJECT_ID,
    zone=ZONE,
    body={
        "name": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='alignment_instance_name') }}",
        "machine_type": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='alignment_machine_type') }}",
        "disks": [
            {
                "boot": True,
                "auto_delete": True,
                "device_name": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='alignment_instance_name') }}",
                "initialize_params": {
                    "disk_size_gb": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='disk_size_gb') | int }}",
                    "disk_type": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='disk_type') }}",
                    "source_image": "projects/debian-cloud/global/images/family/debian-12",
                },
            }
        ],
        "metadata": {
            "items": [
                {
                    "key": "startup-script",
                    "value": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='alignment_startup_script') }}",
                }
            ]
        },
        "network_interfaces": [
            {
                "network": "global/networks/default",
                "subnetwork": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='subnetwork') }}",
                "nic_type": "GVNIC",
                "stack_type": "IPV4_ONLY",
                "access_configs": [{"name": "External NAT", "network_tier": "PREMIUM"}],
            }
        ],
        "service_accounts": [
            {
                "email": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='sa_email') }}",
                "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
            }
        ],
    },
    dag=dag,
)


# Returns a list of objects present in a bucket with the provided prefix
def check_gcs_prefix(bucket_name, prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    return [blob.name for blob in blobs]


# Check for BAM file in GCS
def check_for_bam_gcs_file_func(**context):
    bucket_name = context["task_instance"].xcom_pull(
        task_ids="prepare_genomic_analysis", key="genome_results_bucket"
    )
    prefix = context["task_instance"].xcom_pull(
        task_ids="prepare_genomic_analysis", key="results_bam_object_path"
    )
    return check_gcs_prefix(bucket_name, prefix)


# Check if BAM file exists in GCS
check_for_bam_gcs_file = PythonOperator(
    task_id="check_for_bam_gcs_file",
    python_callable=check_for_bam_gcs_file_func,
    provide_context=True,
    dag=dag,
)


# Determine the DAG branch to take based on whether a BAM file is present in GCS
def bam_bai_present_branch_func(**context):
    bam_file_present = context["task_instance"].xcom_pull(
        task_ids="check_for_bam_gcs_file", key="return_value"
    )
    if len(bam_file_present) >= 2:
        return "bam_bai_present"
    else:
        return "bam_bai_not_present"


# Skip the alignment step and go straight to variant calling if the BAM is already present from a previous alignment run
bam_bai_present_branch = BranchPythonOperator(
    task_id="bam_bai_present_branch",
    python_callable=bam_bai_present_branch_func,
    dag=dag,
)

bam_bai_present = EmptyOperator(task_id="bam_bai_present", dag=dag)

bam_bai_not_present = EmptyOperator(task_id="bam_bai_not_present", dag=dag)

# Wait for alignment of FASTQs against the reference genome to complete by monitoring for a BAI file in GCS
wait_for_bai_gcs_file = GCSObjectExistenceSensor(
    task_id="wait_for_bai_gcs_file",
    bucket="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='genome_results_bucket') }}",
    object="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='results_bai_object_path') }}",
    google_cloud_conn_id="google_cloud_default",
    timeout=28800,
    poke_interval=30,
    mode="poke",
    dag=dag,
)

# Wait for alignment of FASTQs against the reference genome to complete by monitoring for a BAM file in GCS
wait_for_bam_gcs_file = GCSObjectExistenceSensor(
    task_id="wait_for_bam_gcs_file",
    bucket="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='genome_results_bucket') }}",
    object="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='results_bam_object_path') }}",
    google_cloud_conn_id="google_cloud_default",
    timeout=28800,
    poke_interval=30,
    mode="poke",
    dag=dag,
)

# Since we perform alignment with high cost compute machines, even though they should self-terminate we want to be sure they are terminated by the time the output files are in GCS
terminate_alignment_instance = ComputeEngineDeleteInstanceOperator(
    task_id="terminate_alignment_instance",
    project_id=PROJECT_ID,
    zone=ZONE,
    resource_id="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='alignment_instance_name') }}",
    dag=dag,
)


# Check for VCF and gVCF files in GCS
def check_for_vcf_gvcf_gcs_files_func(**context):
    bucket_name = context["task_instance"].xcom_pull(
        task_ids="prepare_genomic_analysis", key="genome_results_bucket"
    )
    prefix = context["task_instance"].xcom_pull(
        task_ids="prepare_genomic_analysis", key="results_vcf_object_prefix"
    )
    return check_gcs_prefix(bucket_name, prefix)


# Check if VCF and gVCF files exist in GCS
check_for_vcf_gvcf_gcs_files = PythonOperator(
    task_id="check_for_vcf_gvcf_gcs_files",
    python_callable=check_for_vcf_gvcf_gcs_files_func,
    trigger_rule="none_failed_min_one_success",  # Allows for upstream branching DAG logic
    provide_context=True,
    dag=dag,
)


# Determine the DAG branch to take based on whether VCF files are present in GCS
def vcf_gvcf_present_branch_func(**context):
    vcf_gvcf_prefix = context["task_instance"].xcom_pull(
        task_ids="prepare_genomic_analysis", key="results_vcf_object_prefix"
    )
    prefix_files = context["task_instance"].xcom_pull(
        task_ids="check_for_vcf_gvcf_gcs_files", key="return_value"
    )
    if (
        f"{vcf_gvcf_prefix}.vcf.gz" in prefix_files
        and f"{vcf_gvcf_prefix}.g.vcf.gz" in prefix_files
    ):
        return "vcf_gvcf_present"
    else:
        return "vcf_gvcf_not_present"


# Skip the variant calling step if the VCF and gVCF are already present from a previous run
vcf_gvcf_present_branch = BranchPythonOperator(
    task_id="vcf_gvcf_present_branch",
    python_callable=vcf_gvcf_present_branch_func,
    dag=dag,
)

vcf_gvcf_present = EmptyOperator(task_id="vcf_gvcf_present", dag=dag)

vcf_gvcf_not_present = EmptyOperator(task_id="vcf_gvcf_not_present", dag=dag)

# Start variant calling after alignment has finished
start_variant_calling_instance = ComputeEngineInsertInstanceOperator(
    task_id="start_variant_calling_instance",
    trigger_rule="none_failed_min_one_success",  # Allows for upstream branching DAG logic
    project_id=PROJECT_ID,
    zone=ZONE,
    body={
        "name": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='variant_calling_instance_name') }}",
        "machine_type": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='variant_calling_machine_type') }}",
        "disks": [
            {
                "boot": True,
                "auto_delete": True,
                "device_name": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='variant_calling_instance_name') }}",
                "initialize_params": {
                    "disk_size_gb": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='disk_size_gb') | int }}",
                    "disk_type": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='disk_type') }}",
                    "source_image": "projects/debian-cloud/global/images/family/debian-12",
                },
            }
        ],
        "metadata": {
            "items": [
                {
                    "key": "startup-script",
                    "value": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='variant_calling_startup_script') }}",
                }
            ]
        },
        "network_interfaces": [
            {
                "network": "global/networks/default",
                "subnetwork": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='subnetwork') }}",
                "nic_type": "GVNIC",
                "stack_type": "IPV4_ONLY",
                "access_configs": [{"name": "External NAT", "network_tier": "PREMIUM"}],
            }
        ],
        "service_accounts": [
            {
                "email": "{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='sa_email') }}",
                "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
            }
        ],
    },
    dag=dag,
)

# Check for successful variant calling completion by monitoring for a VCF file in GCS
wait_for_vcf_gcs_file = GCSObjectExistenceSensor(
    task_id="wait_for_vcf_gcs_file",
    bucket="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='genome_results_bucket') }}",
    object="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='results_vcf_object_path') }}",
    google_cloud_conn_id="google_cloud_default",
    timeout=172800,
    poke_interval=120,
    mode="poke",
    dag=dag,
)

# Check for successful variant calling completion by monitoring for a gVCF file in GCS
wait_for_gvcf_gcs_file = GCSObjectExistenceSensor(
    task_id="wait_for_gvcf_gcs_file",
    bucket="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='genome_results_bucket') }}",
    object="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='results_gvcf_object_path') }}",
    google_cloud_conn_id="google_cloud_default",
    timeout=172800,
    poke_interval=120,
    mode="poke",
    dag=dag,
)

# Since we perform VC with long-running machines, even though they should self-terminate we want to be sure they are terminated by the time the output files are in GCS
terminate_variant_calling_instance = ComputeEngineDeleteInstanceOperator(
    task_id="terminate_variant_calling_instance",
    project_id=PROJECT_ID,
    zone=ZONE,
    resource_id="{{ task_instance.xcom_pull(task_ids='prepare_genomic_analysis', key='variant_calling_instance_name') }}",
    dag=dag,
)

# Define DAG task dependencies
(prepare_genomic_analysis >> check_for_bam_gcs_file >> bam_bai_present_branch)

(
    bam_bai_present_branch
    >> bam_bai_present
    >> check_for_vcf_gvcf_gcs_files
    >> vcf_gvcf_present_branch
)

(
    bam_bai_present_branch
    >> bam_bai_not_present
    >> start_alignment_instance
    >> wait_for_bam_gcs_file
    >> wait_for_bai_gcs_file
    >> terminate_alignment_instance
    >> check_for_vcf_gvcf_gcs_files
    >> vcf_gvcf_present_branch
)

vcf_gvcf_present_branch >> vcf_gvcf_present

(
    vcf_gvcf_present_branch
    >> vcf_gvcf_not_present
    >> start_variant_calling_instance
    >> wait_for_vcf_gcs_file
    >> wait_for_gvcf_gcs_file
    >> terminate_variant_calling_instance
)
