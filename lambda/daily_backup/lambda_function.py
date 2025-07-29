# Daily backup lambda function with Backblaze offsite backup
import json
import boto3
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
import logging

# Constants
CONTENT_TYPE_JSON = 'application/json'

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')


def decimal_default(obj):
    """JSON serializer for objects not serializable by default"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def get_account_id():
    """Get AWS account ID"""
    try:
        sts = boto3.client('sts')
        return sts.get_caller_identity()['Account']
    except Exception as e:
        logger.error(f"Failed to get account ID: {str(e)}")
        raise


def get_region():
    """Get AWS region"""
    try:
        # Region is automatically available in Lambda context
        session = boto3.Session()
        return session.region_name
    except Exception as e:
        logger.error(f"Failed to get region: {str(e)}")
        # Fallback to us-east-1 if region detection fails
        return 'us-east-1'


def get_tables_to_backup():
    """Get the list of tables to backup from Terraform environment variables"""
    # Parse table names directly from Terraform
    try:
        tables_json = os.environ['DYNAMODB_TABLES']
        tables = json.loads(tables_json)

        logger.info(f"Tables from Terraform: {tables}")
        return tables

    except KeyError:
        logger.error("DYNAMODB_TABLES environment variable not found")
        raise Exception("DYNAMODB_TABLES environment variable is required")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse DYNAMODB_TABLES: {e}")
        raise Exception(f"Invalid DYNAMODB_TABLES format: {e}")


def generate_export_prefix(table_name, backup_date):
    """Generate S3 prefix for the export"""
    return f"native-exports/{backup_date}/{table_name}/"


def start_table_export(table_name, s3_bucket, backup_date):
    """Start DynamoDB export to S3 for a single table"""
    logger.info(f"Starting native export for table: {table_name}")

    try:
        # Get table ARN
        account_id = get_account_id()
        region = get_region()
        table_arn = f"arn:aws:dynamodb:{region}:{account_id}:table/{table_name}"

        # Generate S3 prefix for this export
        s3_prefix = generate_export_prefix(table_name, backup_date)

        # Start the export
        response = dynamodb.export_table_to_point_in_time(
            TableArn=table_arn,
            S3Bucket=s3_bucket,
            S3Prefix=s3_prefix,
            ExportFormat='DYNAMODB_JSON',
            ExportType='FULL_EXPORT'
        )

        export_arn = response['ExportDescription']['ExportArn']
        export_time = response['ExportDescription']['ExportTime']

        logger.info(f"Export started for {table_name}: {export_arn}")

        return {
            'table_name': table_name,
            'export_arn': export_arn,
            'export_time': export_time.isoformat(),
            's3_prefix': s3_prefix,
            'status': 'IN_PROGRESS'
        }

    except Exception as e:
        logger.error(f"Failed to start export for table {table_name}: {str(e)}")
        return {
            'table_name': table_name,
            'error': str(e),
            'status': 'FAILED'
        }


def check_export_status(export_arn):
    """Check the status of a DynamoDB export"""
    try:
        response = dynamodb.describe_export(ExportArn=export_arn)
        export_desc = response['ExportDescription']

        result = {
            'export_arn': export_arn,
            'status': export_desc['ExportStatus'],
            'table_arn': export_desc.get('TableArn', ''),
            'exported_record_count': export_desc.get('ExportedRecordCount', 0),
            'item_count': export_desc.get('ItemCount', 0),
            's3_bucket': export_desc.get('S3Bucket', ''),
            's3_prefix': export_desc.get('S3Prefix', ''),
            'failure_message': export_desc.get('FailureMessage', ''),
            'billing_size_bytes': export_desc.get('BillingSizeBytes', 0)
        }

        # Add timestamps if available
        if export_desc.get('StartTime'):
            result['start_time'] = export_desc['StartTime'].isoformat()
        if export_desc.get('EndTime'):
            result['end_time'] = export_desc['EndTime'].isoformat()

        return result

    except Exception as e:
        logger.error(f"Failed to check export status for {export_arn}: {str(e)}")
        return {
            'export_arn': export_arn,
            'status': 'UNKNOWN',
            'error': str(e)
        }


def wait_for_exports_completion(export_arns, max_wait_time=840):
    """Monitor multiple exports until completion or timeout"""
    logger.info(f"Monitoring {len(export_arns)} exports for completion...")

    start_time = time.time()
    completed_exports = []

    while export_arns and (time.time() - start_time) < max_wait_time:
        remaining_exports = []

        for export_arn in export_arns:
            status_info = check_export_status(export_arn)

            if status_info['status'] in ['COMPLETED', 'FAILED']:
                completed_exports.append(status_info)
                if status_info['status'] == 'COMPLETED':
                    logger.info(f"Export completed successfully: {export_arn}")
                else:
                    logger.error(f"Export failed: {export_arn} - {status_info.get('failure_message', 'Unknown error')}")
            else:
                remaining_exports.append(export_arn)
                logger.info(f"Export in progress: {export_arn} - Status: {status_info['status']}")

        export_arns = remaining_exports

        if export_arns:
            time.sleep(30)  # Wait 30 seconds before checking again

    # Handle any remaining exports that didn't complete
    for export_arn in export_arns:
        status_info = check_export_status(export_arn)
        status_info['timeout'] = True
        completed_exports.append(status_info)
        logger.warning(f"Export monitoring timed out: {export_arn}")

    return completed_exports


def create_export_manifest(completed_exports, backup_date, s3_bucket, environment):
    """Create a manifest file with export details"""
    total_exports = len(completed_exports)
    successful_exports = len([e for e in completed_exports if e['status'] == 'COMPLETED'])
    failed_exports = len([e for e in completed_exports if e['status'] in ['FAILED', 'UNKNOWN']])
    total_items = sum(e.get('item_count', 0) for e in completed_exports if e.get('item_count'))
    total_size_bytes = sum(e.get('billing_size_bytes', 0) for e in completed_exports if e.get('billing_size_bytes'))

    manifest = {
        'backup_date': backup_date,
        'environment': environment,
        'backup_type': 'DYNAMODB_NATIVE_EXPORT',
        'created_at': datetime.now(timezone.utc).isoformat(),
        'total_exports': total_exports,
        'successful_exports': successful_exports,
        'failed_exports': failed_exports,
        'total_items_exported': total_items,
        'total_size_bytes': total_size_bytes,
        's3_bucket': s3_bucket,
        'exports': completed_exports
    }

    # Upload manifest to S3
    manifest_key = f"native-exports/{backup_date}/manifest.json"

    try:
        s3.put_object(
            Bucket=s3_bucket,
            Key=manifest_key,
            Body=json.dumps(manifest, default=decimal_default, indent=2),
            ContentType=CONTENT_TYPE_JSON,
            Metadata={
                'backup_date': backup_date,
                'environment': environment,
                'backup_type': 'DYNAMODB_NATIVE_EXPORT',
                'total_exports': str(total_exports),
                'successful_exports': str(successful_exports)
            }
        )

        logger.info(f"Export manifest created: s3://{s3_bucket}/{manifest_key}")
        return manifest_key

    except Exception as e:
        logger.error(f"Failed to create export manifest: {str(e)}")
        return None


def get_backblaze_config():
    """Get Backblaze configuration from environment variables"""
    required_vars = {
        'b2_application_key_id': 'key_id',
        'b2_application_key': 'app_key',
        'b2_bucket': 'bucket',
        'b2_endpoint': 'endpoint'
    }
    config = {}

    for env_var, config_key in required_vars.items():
        value = os.environ.get(env_var.upper())
        if not value:
            raise Exception(f"Missing required Backblaze environment variable: {env_var.upper()}")
        config[config_key] = value

    return config


def list_s3_objects(bucket, prefix):
    """List all objects in S3 with given prefix"""
    objects = []
    paginator = s3.get_paginator('list_objects_v2')

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                objects.extend(page['Contents'])

        logger.info(f"Found {len(objects)} objects in s3://{bucket}/{prefix}")
        return objects

    except Exception as e:
        logger.error(f"Failed to list S3 objects: {str(e)}")
        return []


def copy_to_backblaze(s3_bucket, backup_date, backblaze_config, environment):
    """Copy backup files from S3 to Backblaze"""
    logger.info("Starting copy to Backblaze...")

    try:
        logger.info(f"Using Backblaze endpoint: {backblaze_config['endpoint']}")

        # Initialize Backblaze S3-compatible client with specific configuration
        backblaze_client = boto3.client(
            's3',
            endpoint_url=backblaze_config['endpoint'],
            aws_access_key_id=backblaze_config['key_id'],
            aws_secret_access_key=backblaze_config['app_key'],
            region_name='us-east-1',  # Backblaze uses us-east-1 for S3 compatibility
            config=boto3.session.Config(
                signature_version='s3v4',
                s3={
                    'addressing_style': 'path'
                }
            )
        )

        # Get list of all backup files for this date
        backup_prefix = f"native-exports/{backup_date}/"
        s3_objects = list_s3_objects(s3_bucket, backup_prefix)

        if not s3_objects:
            logger.warning(f"No objects found to copy for prefix: {backup_prefix}")
            return {
                'status': 'SUCCESS',
                'files_copied': 0,
                'total_size_bytes': 0,
                'errors': []
            }

        copy_results = {
            'status': 'SUCCESS',
            'files_copied': 0,
            'total_size_bytes': 0,
            'errors': []
        }

        # Copy each file to Backblaze
        for obj in s3_objects:
            s3_key = obj['Key']
            file_size = obj['Size']

            # Create the same key structure in Backblaze with environment prefix
            backblaze_key = f"{environment}/{s3_key}"

            try:
                # Get object from S3
                s3_response = s3.get_object(Bucket=s3_bucket, Key=s3_key)

                # Read the content into memory to avoid streaming issues
                file_content = s3_response['Body'].read()

                # Upload to Backblaze (overwrite if exists)
                # Use put_object with explicit content length and type
                put_kwargs = {
                    'Bucket': backblaze_config['bucket'],
                    'Key': backblaze_key,
                    'Body': file_content,
                    'ContentLength': len(file_content),
                    'Metadata': {
                        'original-bucket': s3_bucket,
                        'original-key': s3_key,
                        'backup-date': backup_date,
                        'environment': environment,
                        'copied-at': datetime.now(timezone.utc).isoformat()
                    }
                }

                # Set content type based on file extension
                if s3_key.endswith('.json'):
                    put_kwargs['ContentType'] = CONTENT_TYPE_JSON
                elif s3_key.endswith('.gz'):
                    put_kwargs['ContentType'] = 'application/gzip'
                elif s3_key.endswith('.md5'):
                    put_kwargs['ContentType'] = 'text/plain'
                else:
                    put_kwargs['ContentType'] = 'binary/octet-stream'

                backblaze_client.put_object(**put_kwargs)

                copy_results['files_copied'] += 1
                copy_results['total_size_bytes'] += file_size

                logger.info(f"Successfully copied to Backblaze: {s3_key} -> {backblaze_key} ({file_size} bytes)")

            except Exception as e:
                error_msg = f"Failed to copy {s3_key}: {str(e)}"
                logger.error(error_msg)
                copy_results['errors'].append(error_msg)
                copy_results['status'] = 'PARTIAL_SUCCESS' if copy_results['files_copied'] > 0 else 'FAILED'

        # Create summary log
        total_size_mb = copy_results['total_size_bytes'] / (1024 * 1024)
        logger.info(f"Backblaze copy completed: {copy_results['files_copied']} files copied, "
                    f"{len(copy_results['errors'])} errors, {total_size_mb:.2f}MB transferred")

        # Create a copy manifest in Backblaze
        copy_manifest = {
            'backup_date': backup_date,
            'environment': environment,
            'copy_completed_at': datetime.now(timezone.utc).isoformat(),
            'source_bucket': s3_bucket,
            'destination_bucket': backblaze_config['bucket'],
            'copy_results': copy_results
        }

        manifest_content = json.dumps(copy_manifest, default=decimal_default, indent=2)
        manifest_key = f"{environment}/native-exports/{backup_date}/backblaze-copy-manifest.json"

        backblaze_client.put_object(
            Bucket=backblaze_config['bucket'],
            Key=manifest_key,
            Body=manifest_content.encode('utf-8'),
            ContentLength=len(manifest_content.encode('utf-8')),
            ContentType=CONTENT_TYPE_JSON
        )

        logger.info(f"Backblaze copy manifest created: {manifest_key}")

        return copy_results

    except Exception as e:
        logger.error(f"Critical error in Backblaze copy: {str(e)}")
        return {
            'status': 'FAILED',
            'error': str(e),
            'files_copied': 0,
            'total_size_bytes': 0,
            'errors': [str(e)]
        }


def lambda_handler(event, context):
    """Main Lambda handler for DynamoDB native exports with Backblaze copy"""
    logger.info("Starting MFA daily backup using DynamoDB native export")

    try:
        # Get configuration from environment
        backup_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        s3_bucket = os.environ['BACKUP_BUCKET']
        environment = os.environ['ENVIRONMENT']

        logger.info(f"Backup configuration: environment={environment}, bucket={s3_bucket}, date={backup_date}")

        # Get tables from Terraform (no fallback)
        tables_to_backup = get_tables_to_backup()

        logger.info(f"Starting exports for {len(tables_to_backup)} tables: {tables_to_backup}")

        # Phase 1: Start all exports
        export_results = []
        export_arns = []

        for table_name in tables_to_backup:
            try:
                # Check if table exists first
                logger.info(f"Checking if table {table_name} exists...")
                dynamodb.describe_table(TableName=table_name)
                logger.info(f"Table {table_name} exists, proceeding with export")

                # Start the export
                result = start_table_export(table_name, s3_bucket, backup_date)
                export_results.append(result)

                # Collect ARNs for monitoring
                if result.get('export_arn'):
                    export_arns.append(result['export_arn'])

            except dynamodb.exceptions.ResourceNotFoundException:
                error_msg = f"Table {table_name} not found"
                logger.error(error_msg)
                export_results.append({
                    'table_name': table_name,
                    'error': error_msg,
                    'status': 'FAILED'
                })
            except Exception as e:
                logger.error(f"Failed to start export for table {table_name}: {str(e)}")
                export_results.append({
                    'table_name': table_name,
                    'error': str(e),
                    'status': 'FAILED'
                })

        # Phase 2: Monitor exports for completion
        if export_arns:
            logger.info(f"Monitoring {len(export_arns)} exports...")
            completed_exports = wait_for_exports_completion(export_arns)

            # Update results with completion status
            for i, result in enumerate(export_results):
                if result.get('export_arn'):
                    # Find corresponding completion status
                    for completed in completed_exports:
                        if completed['export_arn'] == result['export_arn']:
                            export_results[i].update(completed)
                            break

        # Phase 3: Create export manifest
        manifest_key = create_export_manifest(export_results, backup_date, s3_bucket, environment)

        # Generate summary
        successful_exports = len([r for r in export_results if r.get('status') == 'COMPLETED'])
        failed_exports = len([r for r in export_results if r.get('status') in ['FAILED', 'UNKNOWN']])
        total_items = sum(r.get('item_count', 0) for r in export_results if r.get('item_count'))
        total_size_mb = sum(r.get('billing_size_bytes', 0) for r in export_results if r.get('billing_size_bytes')) / (
                    1024 * 1024)

        # Phase 4: Copy to Backblaze (ONLY if there were successful exports)
        backblaze_copy_results = None
        if successful_exports > 0:
            logger.info(f"Starting Backblaze copy for {successful_exports} successful exports...")
            try:
                backblaze_config = get_backblaze_config()
                backblaze_copy_results = copy_to_backblaze(s3_bucket, backup_date, backblaze_config, environment)
                logger.info(f"Backblaze copy completed with status: {backblaze_copy_results['status']}")
            except Exception as e:
                logger.error(f"Backblaze copy failed: {str(e)}")
                backblaze_copy_results = {
                    'status': 'FAILED',
                    'error': str(e),
                    'files_copied': 0,
                    'total_size_bytes': 0,
                    'skipped_files': 0,
                    'errors': [str(e)]
                }
        else:
            logger.warning("Skipping Backblaze copy due to no successful exports")
            backblaze_copy_results = {
                'status': 'SKIPPED',
                'reason': 'No successful exports to copy',
                'files_copied': 0,
                'total_size_bytes': 0,
                'errors': []
            }

        summary = {
            'backup_date': backup_date,
            'environment': environment,
            'backup_type': 'DYNAMODB_NATIVE_EXPORT',
            'total_tables_processed': len(export_results),
            'successful_exports': successful_exports,
            'failed_exports': failed_exports,
            'total_items_exported': total_items,
            'total_size_mb': round(total_size_mb, 2),
            'manifest_s3_key': manifest_key,
            's3_bucket': s3_bucket,
            'backblaze_copy': backblaze_copy_results,
            'export_results': export_results
        }

        logger.info(f"Backup completed: {successful_exports} successful, {failed_exports} failed")
        if backblaze_copy_results:
            logger.info(
                f"Backblaze copy: {backblaze_copy_results['files_copied']} files copied, status: {backblaze_copy_results['status']}")

        # Determine response status
        if failed_exports > 0 and successful_exports == 0:
            status_code = 500  # Complete failure
        elif failed_exports > 0 or (backblaze_copy_results and backblaze_copy_results['status'] == 'FAILED'):
            status_code = 207  # Multi-status (partial success)
        else:
            status_code = 200  # Success

        return {
            'statusCode': status_code,
            'body': json.dumps(summary, default=decimal_default)
        }

    except Exception as e:
        logger.error(f"Critical error in backup process: {str(e)}")

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'environment': os.environ.get('ENVIRONMENT', 'unknown'),
                'backup_date': datetime.now(timezone.utc).strftime('%Y-%m-%d')
            })
        }
