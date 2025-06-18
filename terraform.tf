terraform {
  required_version = ">= 1.0"

  cloud {
    organization = "gtis"

    workspaces {
      tags = ["mfa-backup"]
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
