variable "environment" {
  description = "Environment name (prod, staging, dev)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "company_prefix" {
  description = "Company prefix for resource naming"
  type        = string
  default     = "sil"
}

variable "backup_bucket_name" {
  description = "S3 bucket name for MFA backups (leave empty for auto-generated)"
  type        = string
  default     = ""
}

variable "backup_retention_days" {
  description = "Number of days to retain backups in S3"
  type        = number
  default     = 31
}

variable "dynamodb_table_prefix" {
  description = "Prefix for DynamoDB table names"
  type        = string
  default     = "mfa-api_dev_"
}

variable "dynamodb_tables" {
  description = "List of DynamoDB table suffixes to backup"
  type        = list(string)
  default     = ["u2f_global", "totp_global", "api-key_global"]
}

variable "backup_schedule" {
  description = "Cron expression for backup schedule (UTC)"
  type        = string
  default     = "cron(0 2 * * ? *)"  # 2 AM UTC daily
}

variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 900  # 15 minutes
}
