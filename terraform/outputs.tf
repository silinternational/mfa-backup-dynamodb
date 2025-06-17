output "backup_bucket_name" {
  description = "Name of the S3 backup bucket"
  value       = aws_s3_bucket.mfa_backups.bucket
}

output "daily_backup_function_name" {
  description = "Name of the daily backup Lambda function"
  value       = aws_lambda_function.daily_backup.function_name
}

output "disaster_recovery_function_name" {
  description = "Name of the disaster recovery Lambda function"
  value       = aws_lambda_function.disaster_recovery.function_name
}

output "backup_schedule" {
  description = "Daily backup schedule expression"
  value       = aws_cloudwatch_event_rule.daily_backup_schedule.schedule_expression
}
