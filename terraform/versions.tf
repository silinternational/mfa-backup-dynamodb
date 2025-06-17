terraform {
  required_version = ">= 1.0"

  cloud {
    organization = "gtis"

    workspaces {
      name = "mfa-backup-system"
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
    random = {
      source  = "hashicorp/random"
      version = "~> 3.4"
    }
  }
}
