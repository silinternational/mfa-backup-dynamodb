locals {
  common_tags = {
    itse_app_env      = "dev"
    workspace         = "mfa-backup-system"
    itse_app_customer = "shared"
    managed_by        = "terraform"
    itse_app_name     = "mfa-api"
  }
  # S3 bucket naming with randomization for uniqueness
  backup_bucket_name = var.backup_bucket_name != "" ? var.backup_bucket_name : "${var.company_prefix}-mfa-backups-${var.environment}-${random_id.bucket_suffix.hex}"

}

# Random suffix for bucket uniqueness
resource "random_id" "bucket_suffix" {
  byte_length = 4
}
