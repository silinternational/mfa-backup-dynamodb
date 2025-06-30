locals {
  common_tags = {
    itse_app_env      = var.itse_app_env
    workspace         = var.environment # Will be set in Terraform Cloud workspace variables
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
  type             = "zip"
  source_dir       = "${path.module}/lambda/daily_backup"
  output_file_mode = "0666"
  output_path      = "daily_backup_${var.environment}.zip"
  excludes         = ["*.pyc", "__pycache__"]
}

data "archive_file" "disaster_recovery" {
  type             = "zip"
  source_dir       = "${path.module}/lambda/disaster_recovery"
  output_file_mode = "0666"
  output_path      = "disaster_recovery_${var.environment}.zip"
  excludes         = ["*.pyc", "__pycache__"]
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
  bucket                  = aws_s3_bucket.mfa_backups.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 bucket policy to allow DynamoDB service access for imports
resource "aws_s3_bucket_policy" "mfa_backups" {
  bucket = aws_s3_bucket.mfa_backups.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Allow DynamoDB service to read objects for import
      {
        Sid    = "DynamoDBImportAccess"
        Effect = "Allow"
        Principal = {
          Service = "dynamodb.amazonaws.com"
        }
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.mfa_backups.arn,
          "${aws_s3_bucket.mfa_backups.arn}/*"
        ]
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      },
      # Allow Lambda functions to access the bucket
      {
        Sid    = "LambdaAccess"
        Effect = "Allow"
        Principal = {
          AWS = [
            aws_iam_role.daily_backup_lambda_role.arn,
            aws_iam_role.disaster_recovery_lambda_role.arn
          ]
        }
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:DeleteObject",
          "s3:GetObjectVersion",
          "s3:ListBucketVersions"
        ]
        Resource = [
          aws_s3_bucket.mfa_backups.arn,
          "${aws_s3_bucket.mfa_backups.arn}/*"
        ]
      }
    ]
  })
}

# S3 Lifecycle policy - Environment-specific retention
resource "aws_s3_bucket_lifecycle_configuration" "mfa_backups" {
  bucket = aws_s3_bucket.mfa_backups.id

  rule {
    id     = "mfa_backup_lifecycle_${var.environment}"
    status = "Enabled"

    filter {
      prefix = "exports/"
    }

    # Different retention for dev vs prod
    expiration {
      days = var.environment == "prod" ? var.backup_retention_days : 14 # Shorter retention for dev
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
          "dynamodb:ListExports",
          "dynamodb:DescribeContinuousBackups"
        ]
        Resource = concat(
          # Table permissions
          [for table_name in local.table_names :
            "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${table_name}"
          ],
          # Export permissions - pattern for export ARNs
          [for table_name in local.table_names :
            "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${table_name}/export/*"
          ]
        )
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
  description      = "Daily MFA Backup Lambda for ${var.environment}"
  role             = aws_iam_role.daily_backup_lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.11"
  timeout          = var.lambda_timeout
  memory_size      = 128
  source_code_hash = data.archive_file.daily_backup.output_base64sha256
  tags             = local.common_tags

  environment {
    variables = {
      # Required environment variables (no fallbacks)
      BACKUP_BUCKET = aws_s3_bucket.mfa_backups.bucket
      ENVIRONMENT   = var.environment
      # Table names constructed from Terraform variables
      DYNAMODB_TABLES = jsonencode(local.table_names)

      # Sentry configuration for failure notifications
      SENTRY_DSN     = var.sentry_dsn
      LAMBDA_VERSION = var.lambda_version

      # Service identification for Sentry
      SERVICE_NAME   = "mfa-backup-system"
      COMPONENT_NAME = "daily-backup"
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
  retention_in_days = var.environment == "prod" ? 30 : 14 # Shorter retention for dev
  tags              = local.common_tags
}

# EventBridge Rule for Daily Backup (using your configured schedule)
resource "aws_cloudwatch_event_rule" "daily_backup_schedule" {
  count               = var.backup_schedule_enabled ? 1 : 0
  name                = "mfa-daily-backup-schedule-${var.environment}"
  description         = "Trigger MFA backup daily for ${var.environment}"
  schedule_expression = var.backup_schedule
  tags                = local.common_tags
}

resource "aws_cloudwatch_event_target" "daily_backup_target" {
  count     = var.backup_schedule_enabled ? 1 : 0
  rule      = aws_cloudwatch_event_rule.daily_backup_schedule[0].name
  target_id = "MFADailyBackupTarget"
  arn       = aws_lambda_function.daily_backup.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  count         = var.backup_schedule_enabled ? 1 : 0
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.daily_backup.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_backup_schedule[0].arn
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

# Enhanced disaster recovery Lambda policy with import permissions
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
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
      },
      # Table-specific DynamoDB permissions
      {
        Effect = "Allow"
        Action = [
          "dynamodb:CreateTable",
          "dynamodb:DescribeTable",
          "dynamodb:PutItem",
          "dynamodb:BatchWriteItem",
          "dynamodb:UpdateTable",
          "dynamodb:DeleteTable",
          "dynamodb:ListTables",
          "dynamodb:DescribeContinuousBackups",
          "dynamodb:UpdateContinuousBackups",
          "dynamodb:ListTagsOfResource",
          "dynamodb:TagResource",
          "dynamodb:UntagResource"

        ]
        Resource = [
          "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/mfa-api_${var.environment}_*"
        ]
      },
      # DynamoDB Import Operations (requires Resource = "*")
      {
        Effect = "Allow"
        Action = [
          "dynamodb:ImportTable",
          "dynamodb:DescribeImport",
          "dynamodb:ListImports"
        ]
        Resource = "*"
      },
      # Global Table Operations
      {
        Effect = "Allow"
        Action = [
          "dynamodb:DescribeGlobalTable",
          "dynamodb:CreateGlobalTable",
          "dynamodb:UpdateGlobalTable"
        ]
        Resource = "*"
      },

      # S3 permissions for reading backups
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetObjectVersion",
          "s3:ListBucketVersions"
        ]
        Resource = [
          aws_s3_bucket.mfa_backups.arn,
          "${aws_s3_bucket.mfa_backups.arn}/*"
        ]
      },
      # CloudWatch for monitoring import progress
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = "MFA/DisasterRecovery"
          }
        }
      }
    ]
  })
}

# Disaster Recovery Lambda Function
resource "aws_lambda_function" "disaster_recovery" {
  filename         = data.archive_file.disaster_recovery.output_path
  function_name    = "mfa-disaster-recovery-${var.environment}"
  description      = "MFA Disaster Recovery Lambda for ${var.environment}"
  role             = aws_iam_role.disaster_recovery_lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.11"
  timeout          = var.lambda_timeout
  memory_size      = 128
  source_code_hash = data.archive_file.disaster_recovery.output_base64sha256
  tags             = local.common_tags

  environment {
    variables = {
      BACKUP_BUCKET = aws_s3_bucket.mfa_backups.bucket
      ENVIRONMENT   = var.environment
      # Pass the actual table names
      DYNAMODB_TABLES = jsonencode(local.table_names)
      TABLE_PREFIX    = "mfa-api_${var.environment}_"

      # Sentry configuration for failure notifications
      SENTRY_DSN     = var.sentry_dsn
      LAMBDA_VERSION = var.lambda_version

      # Service identification for Sentry
      SERVICE_NAME   = "mfa-backup-system"
      COMPONENT_NAME = "disaster-recovery"
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
