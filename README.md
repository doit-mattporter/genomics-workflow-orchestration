# genomics-workflow-orchestration

Real-world examples of how to utilize Apache Airflow / Google Cloud Composer to perform bioinformatics workflow orchestration using cat whole genome sequencing data from Basepaws.

## Overview

This repo is the companion piece to [this cat genomics blog post](medium.com/here) and provides a functional demo on GCP of how to implement scalable, fault-tolerant bioinformatics workflow orchestration using Apache Airflow / Google Cloud Composer v3.

The DAG this repo executes on Composer performs the following:

1. Alignment, duplicate marking, and BAM indexing of cat FASTQ sequencing data against a reference genome, which must be either Felis_catus_9.0 or F.catus_Fca126_mat1.0. This process yields an indexed BAM file.
   * Given that this step is highly parallelizable, this is performed on a high-performance c3-standard-88 machine.

2. Variant calling is performed, yielding both a VCF and a gVCF.
   * Given that variant calling is poorly parallelizable, this long-running process is performed on a cost-effective c3-standard-8 machine.

3. Annotation of the VCFs is performed with snpEFF, yielding an annotated VCF and a TSV file.
   * This is performed on a cost-effective c3-standard-8 machine.

4. All DAG outputs are uploaded to bucket `genomic-outputs-<random_id>` as they are produced from each step
   * When re-running a DAG on the same input FASTQ files, steps 1, 2, and 3 will be skipped if the expected output files from each step are already located in this bucket.

When following the DAG kickoff instructions in section 'DAG Execution and Prep Work', two instances of the DAG described above will be kicked off per batch of FASTQs, with each DAG running secondary analysis against one of the two supported reference genomes.

## Infrastructure

All the cloud infrastructure required to run the demo is provisioned with Terraform.

To apply all Terraform-based infrastructure changes, first set the sensitive variable values in `terraform/variables.tf`, then run the following from within `terraform/`:

```bash
gcloud config set project <YOUR_PLAYGROUND_PROJECT_ID>
terraform workspace select dev
terraform init -upgrade
terraform apply
```

## DAG Execution and Prep Work

Once your Terraform-provisioned cloud infrastructure has spun up, simply upload the FASTQ files you wish the Cloud Composer DAG to process into bucket `genomic-inputs-<random_id>`, then upload `ready.txt` to this bucket, where the contents of `ready.txt` are the GCS URI paths for the FASTQ files that correspond to the sample you're processing. For example, your `ready.txt` file should look like the following if you have 3 FASTQ files for the one sample you're processing:

```bash
gs://genomic-inputs-3aaed91a8d2e0971/AC.BE.96.31220812116969.LP.1417.H12.L3.R289.WGS.fastq.gz
gs://genomic-inputs-3aaed91a8d2e0971/AC.BE.96.31220812116969.SP.442.A5.L5.R289.WGS.fastq.gz
gs://genomic-inputs-3aaed91a8d2e0971/AC.BE.96.31220812116969.SP.449.A6.L6.R299.WGS.fastq.gz
```

Once `ready.txt` is uploaded, the Google Cloud Function 'Genomic DAG Kickoff' will be triggered and send a notification to the Cloud Composer environment that kicks off secondary analysis against two reference genome versions: Felis_catus_9.0 and F.catus_Fca126_mat1.0.
