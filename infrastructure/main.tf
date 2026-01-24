# Configure Terraform AWS provider
terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "6.20.0"
    }
  }

  # Remote state backend
  backend "s3" {
    bucket         = "investment-analytics-terraform-state"
    key            = "prd/terraform.tfstate"
    region         = "ap-southeast-1"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}

# Configure AWS credentials
provider "aws" {
  region = var.aws_region
  access_key = var.aws_access_key_id
  secret_key = var.aws_secret_access_key

  default_tags {
    tags = {
        Environment = "prd"
        Application = "investment_analytics_data_warehouse"
        ManagedBy = "Terraform"
    }
  }
}