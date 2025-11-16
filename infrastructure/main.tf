# Configure Terraform AWS provider
terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "6.20.0"
    }
  }
}

# Configure AWS credentials
provider "aws" {
  region = var.aws_region
  access_key = var.aws_access_key_id
  secret_key = var.aws_secret_key

  default_tags {
    tags = {
        Environment = "prd"
        Application = "investment_analytics_data_warehouse"
        ManagedBy = "Terraform"
    }
  }
}