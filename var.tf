variable "environment" {
  description = "Environment name (dev or prod) - Set in Terraform Cloud workspace"
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "Environment must be either 'dev' or 'prod'."
  }
}

variable "itse_app_env" {
  description = "ITSE app environment"
  type        = string
  default     = ""
}

variable "aws_region" {
  description = "AWS region - Set in Terraform Cloud workspace"
  type        = string
  default     = "us-east-1"
}

variable "aws_access_key_id" {
  default = ""
}

variable "aws_secret_access_key" {
  default = ""
}

variable "dynamodb_tables" {
  description = "List of DynamoDB table suffixes to backup"
  type        = list(string)
  default     = ["u2f", "totp", "api-key"]
}

variable "backup_retention_days" {
  description = "Number of days to retain backups - Set in Terraform Cloud workspace"
  type        = number
  default     = 31
}

variable "lambda_timeout" {
  description = "Lambda function timeout in seconds - Set in Terraform Cloud workspace"
  type        = number
  default     = 900
}

variable "backup_schedule" {
  description = "Cron expression for backup schedule"
  type        = string
  default     = "cron(0 2 * *? *)" # 2 AM daily
}

variable "backup_schedule_enabled" {
  description = "Enable or disable the automatic backup schedule (useful for maintenance or cost control)"
  type        = bool
  default     = true
}
