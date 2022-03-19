provider "aws" {
  region = var.aws_region
}

#Centralizar oarquivo de controle de estado do Terraform
terraform {
  backend "s3" {
    bucket = "terraform-state-igti-gustavo"
    key    = "state/igti/edc/mod1/terraform.tfstate"
    region = "sa-east-1"
  }
}