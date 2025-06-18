output "backup_bucket_name" {
  description = "Name of the S3 bucket for backups"
  value       = aws_s3_bucket.mfa_backups.bucket
}

output "backup_lambda_arn" {
  description = "ARN of the backup Lambda function"
  value       = aws_lambda_function.daily_backup.arn
}

output "restore_lambda_arn" {
  description = "ARN of the disaster recovery Lambda function"
  value       = aws_lambda_function.disaster_recovery.arn
}

output "table_names" {
  description = "List of DynamoDB table names being backed up"
  value       = local.table_names
}
