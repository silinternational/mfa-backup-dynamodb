locals {
  common_tags = {
    itse_app_env      = var.itse_app_env
    workspace         = var.environment  # Will be set in Terraform Cloud workspace variables
    itse_app_customer = "shared"
    managed_by        = "terraform"
    itse_app_name     = "mfa-api"
    environment       = var.environment
  }

  # Environment-specific S3 bucket naming
  backup_bucket_name = "silidp-mfa-${var.environment}-dynamodb-backups"

  # Table names matching your actual pattern: mfa-api_ENV_TABLE_global
  table_names = [
    for table in var.dynamodb_tables : "mfa-api_${var.environment}_${table}_global"
  ]
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Archive Lambda functions
data "archive_file" "daily_backup" {
  type        = "zip"
  source_dir  = "../lambda/daily_backup"
  output_path = "/tmp/daily_backup_lambda_${var.environment}.zip"
  excludes    = ["*.pyc", "__pycache__"]
}

data "archive_file" "disaster_recovery" {
  type        = "zip"
  source_dir  = "../lambda/disaster_recovery"
  output_path = "/tmp/disaster_recovery_lambda_${var.environment}.zip"
  excludes    = ["*.pyc", "__pycache__"]
}

# S3 Bucket for backups (environment-specific)
resource "aws_s3_bucket" "mfa_backups" {
  bucket = local.backup_bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "mfa_backups" {
  bucket = aws_s3_bucket.mfa_backups.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mfa_backups" {
  bucket = aws_s3_bucket.mfa_backups.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "mfa_backups" {
  bucket = aws_s3_bucket.mfa_backups.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Lifecycle policy - Environment-specific retention
resource "aws_s3_bucket_lifecycle_configuration" "mfa_backups" {
  bucket = aws_s3_bucket.mfa_backups.id

  rule {
    id     = "mfa_backup_lifecycle_${var.environment}"
    status = "Enabled"

    # Different retention for dev vs prod
    expiration {
      days = var.environment == "prod" ? var.backup_retention_days : 14  # Shorter retention for dev
    }

    # Delete old versions after 7 days
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

# IAM Role for Daily Backup Lambda
resource "aws_iam_role" "daily_backup_lambda_role" {
  name = "mfa-daily-backup-lambda-role-${var.environment}"
  tags = local.common_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "daily_backup_lambda_policy" {
  name = "mfa-daily-backup-lambda-policy-${var.environment}"
  role = aws_iam_role.daily_backup_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:ExportTableToPointInTime",
          "dynamodb:DescribeExport",
          "dynamodb:DescribeTable",
          "dynamodb:ListExports"
        ]
        Resource = [
          for table_name in local.table_names :
          "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${table_name}"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_bucket.mfa_backups.arn,
          "${aws_s3_bucket.mfa_backups.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "sts:GetCallerIdentity"
        ]
        Resource = "*"
      }
    ]
  })
}

# Daily Backup Lambda Function
resource "aws_lambda_function" "daily_backup" {
  filename         = data.archive_file.daily_backup.output_path
  function_name    = "mfa-daily-backup-${var.environment}"
  role            = aws_iam_role.daily_backup_lambda_role.arn
  handler         = "lambda_function.lambda_handler"
  runtime         = "python3.11"
  timeout         = var.lambda_timeout
  memory_size     = 512  # Increased for better performance
  source_code_hash = data.archive_file.daily_backup.output_base64sha256
  tags            = local.common_tags

  environment {
    variables = {
      BACKUP_BUCKET      = aws_s3_bucket.mfa_backups.bucket
      ENVIRONMENT        = var.environment
      # Pass the actual table names, not constructed ones
      DYNAMODB_TABLES    = jsonencode(local.table_names)
      AWS_REGION         = data.aws_region.current.name
      # Remove TABLE_PREFIX since we're using full table names
    }
  }

  depends_on = [
    aws_iam_role_policy.daily_backup_lambda_policy,
    aws_cloudwatch_log_group.daily_backup_logs,
  ]
}

# CloudWatch Log Group for Daily Backup
resource "aws_cloudwatch_log_group" "daily_backup_logs" {
  name              = "/aws/lambda/mfa-daily-backup-${var.environment}"
  retention_in_days = var.environment == "prod" ? 30 : 14  # Shorter retention for dev
  tags              = local.common_tags
}

# EventBridge Rule for Daily Backup (different schedule for dev vs prod)
resource "aws_cloudwatch_event_rule" "daily_backup_schedule" {
  name                = "mfa-daily-backup-schedule-${var.environment}"
  description         = "Trigger MFA backup daily for ${var.environment}"
  # Different schedules: prod daily at 2 AM, dev at 3 AM
  schedule_expression = var.environment == "prod" ? "cron(0 2 * * ? *)" : "cron(0 3 * * ? *)"
  tags                = local.common_tags
}

resource "aws_cloudwatch_event_target" "daily_backup_target" {
  rule      = aws_cloudwatch_event_rule.daily_backup_schedule.name
  target_id = "MFADailyBackupTarget"
  arn       = aws_lambda_function.daily_backup.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.daily_backup.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_backup_schedule.arn
}

# IAM Role for Disaster Recovery Lambda
resource "aws_iam_role" "disaster_recovery_lambda_role" {
  name = "mfa-disaster-recovery-lambda-role-${var.environment}"
  tags = local.common_tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "disaster_recovery_lambda_policy" {
  name = "mfa-disaster-recovery-lambda-policy-${var.environment}"
  role = aws_iam_role.disaster_recovery_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:CreateTable",
          "dynamodb:DescribeTable",
          "dynamodb:PutItem",
          "dynamodb:BatchWriteItem",
          "dynamodb:UpdateTable",
          "dynamodb:DeleteTable",
          "dynamodb:ImportTable",
          "dynamodb:DescribeImport"
        ]
        Resource = [
          "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/mfa-api_${var.environment}_*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.mfa_backups.arn,
          "${aws_s3_bucket.mfa_backups.arn}/*"
        ]
      }
    ]
  })
}

# Disaster Recovery Lambda Function
resource "aws_lambda_function" "disaster_recovery" {
  filename         = data.archive_file.disaster_recovery.output_path
  function_name    = "mfa-disaster-recovery-${var.environment}"
  role            = aws_iam_role.disaster_recovery_lambda_role.arn
  handler         = "lambda_function.lambda_handler"
  runtime         = "python3.11"
  timeout         = var.lambda_timeout
  memory_size     = 1024
  source_code_hash = data.archive_file.disaster_recovery.output_base64sha256
  tags            = local.common_tags

  environment {
    variables = {
      BACKUP_BUCKET   = aws_s3_bucket.mfa_backups.bucket
      ENVIRONMENT     = var.environment
      # Pass the actual table names
      DYNAMODB_TABLES = jsonencode(local.table_names)
      AWS_REGION      = data.aws_region.current.name
    }
  }

  depends_on = [
    aws_iam_role_policy.disaster_recovery_lambda_policy,
    aws_cloudwatch_log_group.disaster_recovery_logs,
  ]
}

# CloudWatch Log Group for Disaster Recovery
resource "aws_cloudwatch_log_group" "disaster_recovery_logs" {
  name              = "/aws/lambda/mfa-disaster-recovery-${var.environment}"
  retention_in_days = var.environment == "prod" ? 30 : 14
  tags              = local.common_tags
}
