terraform {
  required_version = ">= 1.0"

  cloud {
    organization = "gtis"

    workspaces {
      tags = ["app:mfa-backup-dynamodb"]
    }
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "aws" {
  region = var.aws_region
}
