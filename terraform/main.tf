data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Archive Lambda functions
data "archive_file" "daily_backup" {
  type        = "zip"
  source_dir  = "../lambda/daily_backup"
  output_path = "/tmp/daily_backup_lambda.zip"
  excludes    = ["*.pyc", "__pycache__"]
}

data "archive_file" "disaster_recovery" {
  type        = "zip"
  source_dir  = "../lambda/disaster_recovery"
  output_path = "/tmp/disaster_recovery_lambda.zip"
  excludes    = ["*.pyc", "__pycache__"]
}

# S3 Bucket for backups
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

# S3 Lifecycle policy - Simple 31-day retention
resource "aws_s3_bucket_lifecycle_configuration" "mfa_backups" {
  bucket = aws_s3_bucket.mfa_backups.id

  rule {
    id     = "mfa_backup_lifecycle"
    status = "Enabled"

    # Delete backups after retention period
    expiration {
      days = var.backup_retention_days
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
          "dynamodb:DescribeTable"
        ]
        Resource = [
          for table in var.dynamodb_tables :
          "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.dynamodb_table_prefix}_${var.environment}_${table}"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:GetObject",
          "s3:ListBucket"
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
  memory_size     = 256
  source_code_hash = data.archive_file.daily_backup.output_base64sha256
  tags            = local.common_tags

  environment {
    variables = {
      BACKUP_BUCKET      = aws_s3_bucket.mfa_backups.bucket
      ENVIRONMENT        = var.environment
      TABLE_PREFIX       = "${var.dynamodb_table_prefix}_${var.environment}_"
      DYNAMODB_TABLES    = jsonencode(var.dynamodb_tables)
      AWS_REGION         = data.aws_region.current.name
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
  retention_in_days = 30
  tags              = local.common_tags
}

# EventBridge Rule for Daily Backup
resource "aws_cloudwatch_event_rule" "daily_backup_schedule" {
  name                = "mfa-daily-backup-schedule-${var.environment}"
  description         = "Trigger MFA backup daily"
  schedule_expression = var.backup_schedule
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
          "dynamodb:DeleteTable"
        ]
        Resource = [
          "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.dynamodb_table_prefix}_${var.environment}_*"
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
      TABLE_PREFIX    = "${var.dynamodb_table_prefix}_${var.environment}_"
      DYNAMODB_TABLES = jsonencode(var.dynamodb_tables)
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
  retention_in_days = 30
  tags              = local.common_tags
}
