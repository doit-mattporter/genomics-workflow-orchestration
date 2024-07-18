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

# resource "google_compute_instance" "upload_indexed_reference_genome_instance" {
#   name         = "grabbing-reference-genome"
#   machine_type = "c4-standard-96"
#   zone         = local.env_configs[terraform.workspace]["zone"]

#   boot_disk {
#     initialize_params {
#       image = "debian-12-bookworm-v20240617"
#       size  = 50
#     }
#   }

#   network_interface {
#     network = "default"
#     access_config {}
#   }

#   service_account {
#     email  = data.google_compute_default_service_account.default.email
#     scopes = ["https://www.googleapis.com/auth/cloud-platform"]
#   }

#   metadata_startup_script = <<-EOT
#     #!/bin/bash
#     set -e
#     trap 'gcloud compute instances delete grabbing-reference-genome --zone=${local.env_configs[terraform.workspace]["zone"]} --quiet' ERR EXIT

#     # Install dependencies
#     sudo apt-get install -y docker.io bzip2 wget curl
#     sudo usermod -aG docker $USER
#     newgrp docker

#     # Prepare GATK
#     docker pull broadinstitute/gatk:latest &

#     # Establish cat reference genome sources and destination
#     REFERENCE_BUCKET_NAME=${google_storage_bucket.genomic_reference_bucket.name}
#     URL1="https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/181/335/GCF_000181335.3_Felis_catus_9.0/GCF_000181335.3_Felis_catus_9.0_genomic.fna.gz"
#     URL2="https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/018/350/175/GCF_018350175.1_F.catus_Fca126_mat1.0/GCF_018350175.1_F.catus_Fca126_mat1.0_genomic.fna.gz"

#     UNZIPPED_FILENAME1=$(basename $URL1 .gz)
#     UNZIPPED_FILENAME2=$(basename $URL2 .gz)

#     # Install bwa-mem2
#     wget -O bwa-mem2-2.2.1_x64-linux.tar.bz2 https://github.com/bwa-mem2/bwa-mem2/releases/download/v2.2.1/bwa-mem2-2.2.1_x64-linux.tar.bz2
#     tar -xjf bwa-mem2-2.2.1_x64-linux.tar.bz2
#     sudo mv bwa-mem2-2.2.1_x64-linux/bwa-mem2* /usr/bin/
#     rm -rf bwa-mem2-2.2.1_x64-linux bwa-mem2-2.2.1_x64-linux.tar.bz2

#     # Install samtools
#     sudo apt-get install -y autoconf automake make gcc perl zlib1g-dev libbz2-dev liblzma-dev libcurl4-gnutls-dev libssl-dev libncurses5-dev libdeflate-dev
#     wget https://github.com/samtools/samtools/releases/download/1.20/samtools-1.20.tar.bz2
#     tar -xjf samtools-1.20.tar.bz2
#     cd samtools-1.20
#     autoheader
#     autoconf -Wno-syntax
#     ./configure
#     make
#     sudo make install
#     sudo cp samtools /usr/bin/
#     cd ../
#     rm -f samtools-1.20.tar.bz2

#     download_and_unzip_fasta() {
#       url=$1
#       local_filename=$(basename $url)
#       unzipped_filename=$${local_filename%.gz}
#       wget $url || { echo "Failed to download $url"; exit 1; }
#       gunzip $local_filename || { echo "Failed to unzip $local_filename"; exit 1; }
#       echo $unzipped_filename
#     }

#     index_and_upload() {
#       filename=$1
#       base_filename=basename
#       bwa-mem2 index $filename &
#       samtools faidx $filename &
#       docker run --rm -v /tmp/:/data broadinstitute/gatk:latest gatk CreateSequenceDictionary -R /data/$filename &
#       wait
#       for file in $filename*; do
#         gcloud storage cp "$file" gs://$REFERENCE_BUCKET_NAME/$file || { echo "Failed to upload $file to $REFERENCE_BUCKET_NAME"; exit 1; }
#       done
#     }

#     check_and_process() {
#       filename=$1
#       url=$2
#       if ! gcloud storage ls gs://$REFERENCE_BUCKET_NAME/$filename; then
#         unzipped_filename=$(download_and_unzip_fasta $url)
#         index_and_upload $unzipped_filename
#       else
#         echo "$filename already exists in $REFERENCE_BUCKET_NAME, skipping download and processing."
#       fi
#     }

#     # Wait for GATK to finish pulling
#     wait

#     # Check to see if the FASTA file and its indexes exist in GCS; if not, create those files
#     cd /tmp/
#     check_and_process $UNZIPPED_FILENAME1 $URL1 &
#     check_and_process $UNZIPPED_FILENAME2 $URL2 &
#     wait
#   EOT

#   depends_on = [google_storage_bucket.genomic_reference_bucket]
# }

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
