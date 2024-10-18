# genomics-workflow-orchestration

A functional example of how to utilize Apache Airflow / Google Cloud Composer v3 to perform bioinformatics workflow orchestration using cat whole genome sequencing data provided by [Basepaws](https://basepaws.com/products/whole-genome-sequencing).

## Overview

This repo is the companion piece to [this blog post on running cat genome analytics with Cloud Composer and Claude](https://medium.com/p/8ab5c5e57d92) and provides a functional demo on GCP of how to implement scalable, fault-tolerant bioinformatics workflow orchestration using a DAG deployed to Google Cloud Composer v3 (Google's fully managed Apache Airflow service).

When the cloud infrastructure powering this app is spun up by following the directions under the 'Infrastructure' section, you will be left with a Cloud Composer v3 instance that is ready to accept work. To begin analyzing a cat genome, simply follow the DAG kickoff instructions in section 'DAG Execution and Prep Work', which essentially entails uploading your BasePaws-provided FASTQ files containing genomic sequencing data to a bucket, along with a `ready.txt` file containing the GCS URIs for those FASTQs. Upload of the `ready.txt` file will kick off two instances of the genomic anlaytics DAG described below for the provided batch of FASTQ files, where each DAG runs secondary and tertiary analysis using one of the two supported cat reference genomes, `Felis_catus_9.0` and `Felis_catus_Fca126_mat1.0`.

The DAG this repo executes on Composer performs the following:

1. Alignment, duplicate marking, and BAM indexing of cat FASTQ sequencing data against a reference genome. Alignment is run against both Felis_catus_9.0 and F.catus_Fca126_mat1.0. This process yields an indexed BAM file.
   * Given that this step is highly parallelizable, this is performed on a high-performance c4-standard-96 machine.

2. Variant calling is performed, yielding both a VCF and a gVCF.
   * Given that variant calling is poorly parallelizable, this long-running process is performed on a cost-effective c4-standard-8 machine.

3. Annotation of the VCFs is performed with snpEFF, yielding an annotated VCF and a TSV file.
   * This is performed on a cost-effective c4-standard-8 machine.

4. All DAG outputs (BAM, VCF, and annotation files) are uploaded to bucket `genomic-outputs-<random_id>` as they are produced from each step
   * When re-running a DAG on the same input FASTQ files, steps 1, 2, and 3 will be skipped if the expected output files from each step are already located in this bucket.

## Infrastructure

All the cloud infrastructure required to run the demo is provisioned with Terraform. Note that secondary and tertiary genomic analysis are compute- and memory-intensive and therefore expensive operations, so make sure you dive into and understand the Terraform scripts and Cloud Composer DAG before executing them. Be prepared to budget appropriately for the resources being spun up, and terminate cloud infrastructure when it is no longer being used.

To create all the required cloud infrastructure, first set the sensitive variable values in `terraform/variables.tf`, then run the following from within `terraform/`:

```bash
gcloud config set project <YOUR_PLAYGROUND_PROJECT_ID>
terraform workspace select dev
terraform init -upgrade
terraform apply
```

### upload_reference_genomes.tf

One particular terraform file, `upload_reference_genomes.tf`, has its contents commented out by default. This file defines the creation of a high-powered, expensive c4-standard-96 Compute Engine instance whose bootstrap script downloads two reference genome versions, prepares them for later execution in the DAG by using GATK's HaplotypeCaller to create sequence dictionaries, and then uploads these DAG-ready reference genome sequences to a GCS bucket. The secondary analysis portion of the DAG will use these files.

You should uncomment this file and let this instance run the first time you run `terraform apply`. It should self-terminate when it either completes successfully or encounters an error. It is only commented out as a precaution due to it being a costly machine to spin up, and because it only needs to be run once. All subsequent runs of `terraform apply` will not require this file's resource to be spun up again unless you remove the reference genome sequences, so feel free to comment it back out after you've run this once.

## DAG Execution

Once your Terraform-provisioned cloud infrastructure has spun up, simply upload your BasePaws-provided FASTQ files you want the Cloud Composer DAG to process into bucket `genomic-inputs-<random_id>`, then upload `ready.txt` to this bucket, where the contents of `ready.txt` are the GCS URI paths for the FASTQ files that correspond to the sample you're processing. For example, your `ready.txt` file should look like the following if you have 3 FASTQ files for the one sample you're processing (BasePaws should have sent you 3 FASTQs):

```bash
gs://genomic-inputs-3aaed91a8d2e0971/AC.BE.96.31220812116969.LP.1417.H12.L3.R289.WGS.fastq.gz
gs://genomic-inputs-3aaed91a8d2e0971/AC.BE.96.31220812116969.SP.442.A5.L5.R289.WGS.fastq.gz
gs://genomic-inputs-3aaed91a8d2e0971/AC.BE.96.31220812116969.SP.449.A6.L6.R299.WGS.fastq.gz
```

Once `ready.txt` is uploaded, the Google Cloud Function 'Genomic DAG Kickoff' will be triggered and send a notification to the Cloud Composer environment that kicks off secondary analysis against two reference genome versions: `Felis_catus_9.0` and `F.catus_Fca126_mat1.0`. All DAG outputs (BAM, VCF, and annotation files) are uploaded to bucket `genomic-outputs-<random_id>` as they are produced from each step.
