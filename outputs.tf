# Lambda Function Information
output "daily_backup_function_name" {
  description = "Name of the daily backup Lambda function"
  value       = aws_lambda_function.daily_backup.function_name
}

output "daily_backup_function_arn" {
  description = "ARN of the daily backup Lambda function"
  value       = aws_lambda_function.daily_backup.arn
}

output "disaster_recovery_function_name" {
  description = "Name of the disaster recovery Lambda function"
  value       = aws_lambda_function.disaster_recovery.function_name
}

output "disaster_recovery_function_arn" {
  description = "ARN of the disaster recovery Lambda function"
  value       = aws_lambda_function.disaster_recovery.arn
}

# Table Information
output "dynamodb_tables_monitored" {
  description = "List of DynamoDB tables being backed up"
  value       = local.table_names
}

# Backup Schedule Information
output "backup_schedule_enabled" {
  description = "Whether automatic backup scheduling is enabled"
  value       = var.backup_schedule_enabled
}

output "backup_schedule" {
  description = "Cron expression for the backup schedule (if enabled)"
  value       = var.backup_schedule_enabled ? var.backup_schedule : "DISABLED"
}

# CloudWatch Log Groups
output "daily_backup_log_group" {
  description = "CloudWatch log group for daily backup Lambda"
  value       = aws_cloudwatch_log_group.daily_backup_logs.name
}

output "disaster_recovery_log_group" {
  description = "CloudWatch log group for disaster recovery Lambda"
  value       = aws_cloudwatch_log_group.disaster_recovery_logs.name
}
