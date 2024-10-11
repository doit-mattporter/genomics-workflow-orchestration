terraform {
  required_version = "~> 1.9.1"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.28.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.28.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6.3"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4.2"
    }
    external = {
      source  = "hashicorp/external"
      version = "~> 2.3.4"
    }
    null = {
      source = "hashicorp/null"
      version = "~> 3.2.3"
    }
  }
}

provider "google" {
  project = local.env_configs[terraform.workspace]["project_id"]
  region  = local.env_configs[terraform.workspace]["region"]
  zone    = local.env_configs[terraform.workspace]["zone"]
}

provider "google-beta" {
  project = local.env_configs[terraform.workspace]["project_id"]
  region  = local.env_configs[terraform.workspace]["region"]
  zone    = local.env_configs[terraform.workspace]["zone"]
}

provider "random" {}

provider "archive" {}

provider "external" {}

provider "null" {}