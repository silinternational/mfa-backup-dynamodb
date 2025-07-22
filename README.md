# DynamoDB Backup and Restore System

AWS Lambda-based solution for backing up and restoring DynamoDB tables using native DynamoDB export functionality and S3 storage.

## Overview

This system consists of two main Lambda functions:

1. **Daily Backup Lambda**: Creates native DynamoDB exports to S3 with monitoring and manifest generation
2. **Restore Lambda**: Restores data from S3 exports back to DynamoDB tables using batch write operations

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   DynamoDB      │    │   S3 Bucket     │    │   DynamoDB      │
│   Tables        │───▶│   Backups       │───▶│   Tables        │
│   (Source)      │    │                 │    │   (Target)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                       ▲
        │                       │                       │
        ▼                       ▼                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Backup Lambda  │    │   Manifest      │    │ Restore Lambda  │
│                 │    │   Files         │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Features

### Backup Lambda Features
- **Native DynamoDB Exports**: Uses AWS DynamoDB's native export functionality
- **Parallel Processing**: Starts multiple exports simultaneously
- **Export Monitoring**: Waits for exports to complete with status tracking
- **Manifest Generation**: Creates detailed backup manifests with metadata
- **Error Handling**: Comprehensive error handling and logging
- **Terraform Integration**: Reads table names from Terraform environment variables

### Restore Lambda Features
- **Batch Write Operations**: Efficiently writes data using DynamoDB batch operations
- **Multi-threaded Processing**: Configurable worker threads for optimal performance
- **Data Validation**: Validates export data before restoration
- **Flexible Target**: Restores to existing tables with same names
- **Dry Run Mode**: Test restore operations without writing data
- **Progress Tracking**: Detailed progress reporting and statistics

## Environment Variables

### Backup Lambda
| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `BACKUP_BUCKET` | Yes | S3 bucket for storing backups | `my-backup-bucket` |
| `ENVIRONMENT` | Yes | Environment identifier | `production` |
| `DYNAMODB_TABLES` | Yes | JSON array of table names from Terraform | `["table1", "table2"]` |

### Restore Lambda
| Variable | Required | Description | Default | Example |
|----------|----------|-------------|---------|---------|
| `BACKUP_BUCKET` | Yes | S3 bucket containing backups | - | `my-backup-bucket` |
| `ENVIRONMENT` | Yes | Environment identifier | - | `production` |
| `S3_EXPORTS_PREFIX` | No | S3 prefix for exports | `native-exports` | `backups/dynamodb` |

## S3 Structure

The backup system creates the following S3 structure:

```
s3://backup-bucket/
└── native-exports/
    └── YYYY-MM-DD/                    # Backup date
        ├── manifest.json              # Backup manifest
        ├── table1/                    # Table-specific export
        │   └── AWSDynamoDB/
        │       └── {export-id}/
        │           └── data/
        │               ├── file1.json.gz
        │               └── file2.json.gz
        └── table2/
            └── AWSDynamoDB/
                └── {export-id}/
                    └── data/
                        └── file1.json.gz
```

## Usage

### Running Backup Lambda

The backup Lambda can be triggered manually or via scheduled events:

```json
{
  "version": "0",
  "id": "backup-trigger",
  "detail-type": "Scheduled Event",
  "source": "aws.events"
}
```

**Response Format:**
```json
{
  "statusCode": 200,
  "body": {
    "backup_date": "2025-07-22",
    "environment": "production", 
    "backup_type": "DYNAMODB_NATIVE_EXPORT",
    "total_tables_processed": 3,
    "successful_exports": 3,
    "failed_exports": 0,
    "total_items_exported": 150000,
    "total_size_mb": 45.67,
    "manifest_s3_key": "native-exports/2025-07-22/manifest.json",
    "s3_bucket": "my-backup-bucket"
  }
}
```

### Running Restore Lambda

#### Basic Restore (Latest Backup)
```json
{
  "backup_date": "latest",
  "clear_existing_data": false,
  "max_workers": 5
}
```

#### Specific Date Restore
```json
{
  "backup_date": "2025-07-20",
  "tables": ["mfa-api_production_u2f_global"],
  "clear_existing_data": true,
  "max_workers": 10
}
```

#### Dry Run Mode
```json
{
  "backup_date": "latest",
  "dry_run": true,
  "clear_existing_data": false
}
```

### Event Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `backup_date` | string | No | `"latest"` | Backup date (YYYY-MM-DD) or "latest" |
| `tables` | array | No | all available | Specific tables to restore |
| `dry_run` | boolean | No | `false` | Validate without writing data |
| `clear_existing_data` | boolean | No | `false` | Clear existing data before restore |
| `max_workers` | integer | No | `5` | Number of worker threads |

## Table Naming Convention

The system expects tables to follow the pattern:
```
mfa-api_{environment}_{type}_global
```

Where:
- `{environment}`: Environment identifier (e.g., `production`, `staging`)
- `{type}`: Table type (e.g., `u2f`, `totp`, `api-key`)


## Response Status Codes

| Code | Meaning | Description |
|------|---------|-------------|
| 200 | Success | All operations completed successfully |
| 207 | Multi-Status | Some operations succeeded, others failed |
| 500 | Error | Complete failure or critical error |
