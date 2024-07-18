locals {
  env_configs = {
    dev = {
      region         = "us-central1"
      zone           = "us-central1-c"
      project_name   = ""
      project_id     = ""
      project_number = 123456789012
    }
  }
}

# To use the local definitions, create and select the corresponding workspace:
# terraform workspace new dev
# terraform workspace select dev
# terraform init -upgrade
# terraform apply
