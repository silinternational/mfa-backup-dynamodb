import json
import boto3
import os
import re
import time
from datetime import datetime, timezone
from decimal import Decimal
import logging

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
        session = boto3.Session()
        return session.region_name
    except Exception as e:
        logger.error(f"Failed to get region: {str(e)}")
        return 'us-east-1'


def get_tables_to_restore():
    """Get the list of tables to restore from environment variables"""
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


def enable_pitr_on_table(table_name, max_retries=3):
    """Enable Point-in-Time Recovery on a table with retries"""
    logger.info(f"Enabling PITR on table: {table_name}")

    for attempt in range(max_retries):
        try:
            # Check current PITR status first
            response = dynamodb.describe_continuous_backups(TableName=table_name)
            pitr_status = response['ContinuousBackupsDescription']['PointInTimeRecoveryDescription'][
                'PointInTimeRecoveryStatus']

            if pitr_status == 'ENABLED':
                logger.info(f"PITR already enabled on {table_name}")
                return True, "PITR already enabled"

            # Enable PITR
            dynamodb.update_continuous_backups(
                TableName=table_name,
                PointInTimeRecoverySpecification={
                    'PointInTimeRecoveryEnabled': True
                }
            )

            logger.info(f"Successfully enabled PITR on {table_name}")
            return True, "PITR enabled successfully"

        except dynamodb.exceptions.ResourceNotFoundException:
            logger.warning(f"Table {table_name} not found for PITR enablement (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(5)  # Brief wait before retry
                continue
            return False, "Table not found"

        except dynamodb.exceptions.ResourceInUseException:
            logger.warning(
                f"Table {table_name} is busy, retrying PITR enablement (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(10)  # Wait before retry
                continue
            return False, "Table is busy, could not enable PITR"

        except Exception as e:
            logger.error(f"Failed to enable PITR on {table_name}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(5)  # Brief wait before retry
                continue
            return False, f"PITR enablement failed: {str(e)}"

    return False, "Max retries exceeded"


def enable_pitr_on_completed_tables(import_results):
    """Enable PITR on all successfully imported tables as operational policy"""
    pitr_results = []

    for result in import_results:
        if result.get('import_status') == 'COMPLETED' and result.get('target_table_name'):
            table_name = result['target_table_name']

            # Table is already ACTIVE since import completed, enable PITR directly
            # This applies operational policy (PITR) even though it's not in backup data
            success, message = enable_pitr_on_table(table_name)
            pitr_results.append({
                'table_name': table_name,
                'original_table': result.get('table_name'),
                'pitr_enabled': success,
                'pitr_message': message
            })

    return pitr_results


def get_available_backups(s3_bucket):
    """Get list of available backup dates"""
    try:
        response = s3.list_objects_v2(
            Bucket=s3_bucket,
            Prefix='native-exports/',
            Delimiter='/'
        )

        dates = []
        for prefix in response.get('CommonPrefixes', []):
            match = re.search(r'native-exports/(\d{4}-\d{2}-\d{2})/', prefix['Prefix'])
            if match:
                dates.append(match.group(1))

        return sorted(dates, reverse=True)  # Most recent first
    except Exception as e:
        logger.error(f"Failed to get available backups: {str(e)}")
        return []


def get_backup_manifest(s3_bucket, backup_date):
    """Get backup manifest for a specific date"""
    try:
        manifest_key = f"native-exports/{backup_date}/manifest.json"
        response = s3.get_object(Bucket=s3_bucket, Key=manifest_key)
        manifest_data = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"Retrieved manifest for {backup_date}: {len(manifest_data.get('exports', []))} exports")
        return manifest_data
    except Exception as e:
        logger.error(f"Failed to get backup manifest for {backup_date}: {str(e)}")
        return None


def validate_export_data_integrity(s3_bucket, export_info):
    """Validate the integrity of export data before import"""
    try:
        s3_prefix = export_info['s3_prefix']

        # Look for export directory structure
        response = s3.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=f"{s3_prefix}AWSDynamoDB/",
            MaxKeys=10
        )

        if not response.get('Contents'):
            return False, "No export data found"

        # Check for manifest and data files
        manifest_files = []
        data_files = []

        for obj in response['Contents']:
            key = obj['Key']
            if 'manifest-summary.json' in key:
                manifest_files.append(key)
            elif key.endswith('.json.gz') and '/data/' in key:
                data_files.append(key)

        if not manifest_files:
            return False, "Missing manifest files"
        if not data_files:
            return False, "Missing data files"

        logger.info(f"Export validation successful for {export_info['table_name']}")
        return True, "Export data appears valid"

    except Exception as e:
        logger.error(f"Export validation failed for {export_info['table_name']}: {str(e)}")
        return False, f"Validation error: {str(e)}"


def get_table_schema(table_name):
    """Get the schema of the original table for reference"""
    try:
        response = dynamodb.describe_table(TableName=table_name)
        table_info = response['Table']

        schema = {
            'AttributeDefinitions': table_info['AttributeDefinitions'],
            'KeySchema': table_info['KeySchema'],
            'BillingMode': table_info.get('BillingMode', 'PAY_PER_REQUEST')
        }

        # Add GSI if present
        if 'GlobalSecondaryIndexes' in table_info:
            schema['GlobalSecondaryIndexes'] = []
            for gsi in table_info['GlobalSecondaryIndexes']:
                gsi_def = {
                    'IndexName': gsi['IndexName'],
                    'KeySchema': gsi['KeySchema'],
                    'Projection': gsi['Projection']
                }
                if schema['BillingMode'] == 'PROVISIONED':
                    gsi_def['ProvisionedThroughput'] = gsi.get('ProvisionedThroughput', {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    })
                schema['GlobalSecondaryIndexes'].append(gsi_def)

        # Add provisioned throughput if not PAY_PER_REQUEST
        if schema['BillingMode'] == 'PROVISIONED':
            schema['ProvisionedThroughput'] = table_info.get('ProvisionedThroughput', {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            })

        return schema
    except Exception as e:
        logger.error(f"Could not get schema for {table_name}: {str(e)}")
        return None


def construct_s3_import_path(s3_bucket, export_info):
    """Construct the S3 path for DynamoDB import"""
    s3_prefix = export_info['s3_prefix']

    try:
        response = s3.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=f"{s3_prefix}AWSDynamoDB/",
            Delimiter='/'
        )

        export_dirs = []
        for prefix in response.get('CommonPrefixes', []):
            export_dirs.append(prefix['Prefix'])

        if not export_dirs:
            raise Exception(f"No export directories found under {s3_prefix}")

        # Use the first export directory
        export_dir = export_dirs[0]
        import_path = f"s3://{s3_bucket}/{export_dir}data/"
        logger.info(f"Constructed import path: {import_path}")
        return import_path
    except Exception as e:
        logger.error(f"Failed to construct S3 import path: {str(e)}")
        raise


def start_table_import(export_info, target_table_name, original_table_schema, s3_bucket):
    """Start DynamoDB import from S3 for a single table"""
    logger.info(f"Starting import for table: {export_info['table_name']} → {target_table_name}")

    try:
        # Construct S3 import path
        s3_import_path = construct_s3_import_path(s3_bucket, export_info)

        # Prepare table creation parameters
        table_creation_params = {
            'TableName': target_table_name,
            'AttributeDefinitions': original_table_schema['AttributeDefinitions'],
            'KeySchema': original_table_schema['KeySchema'],
            'BillingMode': original_table_schema['BillingMode']
        }

        # Add provisioned throughput if needed
        if original_table_schema['BillingMode'] == 'PROVISIONED':
            table_creation_params['ProvisionedThroughput'] = original_table_schema['ProvisionedThroughput']

        # Add GSI if present
        if original_table_schema.get('GlobalSecondaryIndexes'):
            table_creation_params['GlobalSecondaryIndexes'] = original_table_schema['GlobalSecondaryIndexes']

        # Start the import
        response = dynamodb.import_table(
            S3BucketSource={
                'S3Bucket': s3_bucket,
                'S3KeyPrefix': s3_import_path.replace(f"s3://{s3_bucket}/", "").rstrip('/')
            },
            InputFormat='DYNAMODB_JSON',
            InputCompressionType='GZIP',
            TableCreationParameters=table_creation_params
        )

        import_arn = response['ImportTableDescription']['ImportArn']
        import_status = response['ImportTableDescription']['ImportStatus']

        logger.info(f"Import started successfully for {target_table_name}: {import_arn}")

        return {
            'table_name': export_info['table_name'],
            'target_table_name': target_table_name,
            'import_arn': import_arn,
            'import_status': import_status,
            's3_import_path': s3_import_path,
            'expected_items': export_info.get('item_count', 0),
            'status': 'IMPORT_STARTED'
        }

    except Exception as e:
        logger.error(f"Failed to start import for table {export_info['table_name']}: {str(e)}")
        return {
            'table_name': export_info['table_name'],
            'target_table_name': target_table_name,
            'error': str(e),
            'status': 'FAILED'
        }


def check_import_status(import_arn):
    """Check the status of a DynamoDB import"""
    try:
        response = dynamodb.describe_import(ImportArn=import_arn)
        import_desc = response['ImportTableDescription']

        result = {
            'import_arn': import_arn,
            'import_status': import_desc['ImportStatus'],
            'table_arn': import_desc.get('TableArn', ''),
            'imported_item_count': import_desc.get('ImportedItemCount', 0),
            'processed_item_count': import_desc.get('ProcessedItemCount', 0),
            'processed_size_bytes': import_desc.get('ProcessedSizeBytes', 0),
            'table_name': import_desc.get('TableId', '').split('/')[-1] if import_desc.get('TableId') else '',
            'failure_code': import_desc.get('FailureCode', ''),
            'failure_message': import_desc.get('FailureMessage', ''),
            'error_count': import_desc.get('ErrorCount', 0)
        }

        # Add timestamps if available
        if import_desc.get('StartTime'):
            result['start_time'] = import_desc['StartTime'].isoformat()
        if import_desc.get('EndTime'):
            result['end_time'] = import_desc['EndTime'].isoformat()

        return result

    except Exception as e:
        logger.error(f"Failed to check import status for {import_arn}: {str(e)}")
        return {
            'import_arn': import_arn,
            'import_status': 'UNKNOWN',
            'error': str(e)
        }


def wait_for_imports_completion(import_arns, max_wait_time=720):  # 12 minutes max
    """Monitor multiple imports until completion or timeout"""
    if not import_arns:
        return []

    logger.info(f"Monitoring {len(import_arns)} imports for completion (max {max_wait_time / 60:.1f} minutes)...")

    start_time = time.time()
    completed_imports = []

    while import_arns and (time.time() - start_time) < max_wait_time:
        remaining_imports = []

        for import_arn in import_arns:
            status_info = check_import_status(import_arn)

            if status_info['import_status'] in ['COMPLETED', 'FAILED', 'CANCELLED']:
                completed_imports.append(status_info)

                if status_info['import_status'] == 'COMPLETED':
                    logger.info(f"Import completed: {import_arn}")
                    logger.info(f"Items imported: {status_info.get('imported_item_count', 0)}")
                    logger.info(f"Table: {status_info.get('table_name', 'unknown')}")
                else:
                    logger.error(f"Import failed: {import_arn}")
                    logger.error(f"Error: {status_info.get('failure_message', 'Unknown error')}")
                    if status_info.get('error_count', 0) > 0:
                        logger.error(f"Error count: {status_info['error_count']}")
            else:
                remaining_imports.append(import_arn)
                progress_msg = f"Import in progress: {status_info['import_status']}"
                if status_info.get('processed_item_count', 0) > 0:
                    progress_msg += f" - {status_info['processed_item_count']} items processed"
                logger.info(progress_msg)

        import_arns = remaining_imports

        if import_arns:
            # Wait 30 seconds before checking again, but check remaining time
            elapsed = time.time() - start_time
            if elapsed + 30 < max_wait_time:
                time.sleep(30)
            else:
                logger.info("Approaching timeout, stopping monitoring")
                break

    # Handle any remaining imports that didn't complete
    for import_arn in import_arns:
        status_info = check_import_status(import_arn)
        status_info['timeout'] = True
        completed_imports.append(status_info)
        logger.warning(
            f"Import monitoring timed out: {import_arn} - Status: {status_info.get('import_status', 'UNKNOWN')}")

    return completed_imports


def lambda_handler(event, context):
    """Main disaster recovery handler using DynamoDB import from S3"""
    logger.info("Starting MFA disaster recovery process using DynamoDB import from S3")

    try:
        # Parse input parameters
        backup_date = event.get('backup_date', 'latest')
        specific_tables = event.get('tables', [])
        dry_run = event.get('dry_run', False)
        wait_for_completion = event.get('wait_for_completion', True)  # Default to waiting
        enable_pitr = event.get('enable_pitr', True)  # Apply PITR as operational policy

        s3_bucket = os.environ['BACKUP_BUCKET']
        environment = os.environ['ENVIRONMENT']

        logger.info(f"Configuration: environment={environment}, bucket={s3_bucket}, backup_date={backup_date}")
        logger.info(f"Options: wait_for_completion={wait_for_completion}, enable_pitr={enable_pitr}")
        logger.info("Note: PITR is applied as operational policy, not restored from backup data")

        # Get tables from Terraform environment variables
        all_available_tables = get_tables_to_restore()

        # Use specific tables if provided, otherwise use all available tables
        if specific_tables:
            tables_to_restore = [table for table in specific_tables if table in all_available_tables]
            if not tables_to_restore:
                raise Exception("None of the specified tables are available for restoration")
        else:
            tables_to_restore = all_available_tables

        logger.info(f"Disaster recovery for tables: {tables_to_restore}")

        # If backup_date is 'latest', find the most recent backup
        if backup_date == 'latest':
            available_backups = get_available_backups(s3_bucket)
            if not available_backups:
                raise Exception("No backups found in S3")
            backup_date = available_backups[0]
            logger.info(f"Using latest backup from: {backup_date}")

        # Get backup manifest
        manifest = get_backup_manifest(s3_bucket, backup_date)
        if not manifest:
            raise Exception(f"Could not find backup manifest for {backup_date}")

        # Filter exports for requested tables - only successful exports
        available_exports = {}
        for export in manifest.get('exports', []):
            if export.get('status') == 'COMPLETED' and export.get('table_name') in tables_to_restore:
                available_exports[export['table_name']] = export

        logger.info(f"Available exports for {backup_date}: {list(available_exports.keys())}")

        if dry_run:
            logger.info("DRY RUN MODE - No actual restoration will be performed")

            validation_results = []
            for table_name in tables_to_restore:
                if table_name in available_exports:
                    export_info = available_exports[table_name]
                    is_valid, validation_message = validate_export_data_integrity(s3_bucket, export_info)

                    validation_results.append({
                        'table_name': table_name,
                        'target_table_name': f"{table_name}_restored",
                        'export_arn': export_info.get('export_arn', 'unknown'),
                        'item_count': export_info.get('item_count', 0),
                        'export_valid': is_valid,
                        'validation_message': validation_message
                    })

            dry_run_summary = {
                'dry_run': True,
                'backup_date': backup_date,
                'environment': environment,
                'tables_requested': tables_to_restore,
                'validation_results': validation_results,
                'tables_missing_exports': [t for t in tables_to_restore if t not in available_exports],
                'valid_tables_count': len([r for r in validation_results if r['export_valid']]),
                'total_items_to_restore': sum(r.get('item_count', 0) for r in validation_results if r['export_valid']),
                'wait_for_completion': wait_for_completion,
                'enable_pitr': enable_pitr,
                'note': "Tables will be created with '_restored' suffix. PITR will be enabled as operational policy (not from backup data). Set 'wait_for_completion': false to return immediately."
            }

            return {
                'statusCode': 200,
                'body': json.dumps(dry_run_summary, default=decimal_default)
            }

        # Start imports for all tables
        import_results = []
        import_arns = []

        for table_name in tables_to_restore:
            if table_name not in available_exports:
                logger.warning(f"No successful export found for table {table_name}")
                import_results.append({
                    'table_name': table_name,
                    'target_table_name': f"{table_name}_restored",
                    'error': 'No successful export found for this table',
                    'status': 'FAILED'
                })
                continue

            # Create target table name with _restored suffix
            target_table_name = f"{table_name}_restored"

            # Validate export data
            export_info = available_exports[table_name]
            is_valid, validation_message = validate_export_data_integrity(s3_bucket, export_info)

            if not is_valid:
                import_results.append({
                    'table_name': table_name,
                    'target_table_name': target_table_name,
                    'error': f'Export validation failed: {validation_message}',
                    'status': 'FAILED'
                })
                continue

            # Get schema from original table
            original_schema = get_table_schema(table_name)
            if not original_schema:
                import_results.append({
                    'table_name': table_name,
                    'target_table_name': target_table_name,
                    'error': 'Could not get original table schema',
                    'status': 'FAILED'
                })
                continue

            logger.info(f"Starting import {table_name} → {target_table_name}")

            # Start the import
            result = start_table_import(export_info, target_table_name, original_schema, s3_bucket)
            import_results.append(result)

            # Collect import ARNs for monitoring
            if result.get('import_arn'):
                import_arns.append(result['import_arn'])

        # Monitor imports for completion if requested and we have imports running
        pitr_results = []
        if wait_for_completion and import_arns:
            logger.info(f"Waiting for {len(import_arns)} imports to complete (timeout: 12 minutes)...")
            completed_imports = wait_for_imports_completion(import_arns)

            # Update results with completion status
            for i, result in enumerate(import_results):
                if result.get('import_arn'):
                    # Find corresponding completion status
                    for completed in completed_imports:
                        if completed['import_arn'] == result['import_arn']:
                            # Update with final status
                            import_results[i].update(completed)
                            import_results[i]['items_restored'] = completed.get('imported_item_count', 0)
                            break

            # Enable PITR on successfully imported tables if requested (operational policy)
            if enable_pitr:
                logger.info("Enabling PITR on successfully imported tables as operational policy...")
                pitr_results = enable_pitr_on_completed_tables(import_results)

                # Log PITR results
                for pitr_result in pitr_results:
                    if pitr_result['pitr_enabled']:
                        logger.info(f"PITR enabled on {pitr_result['table_name']}")
                    else:
                        logger.warning(
                            f"Failed to enable PITR on {pitr_result['table_name']}: {pitr_result['pitr_message']}")
        else:
            logger.info("Imports started, not waiting for completion")
            if enable_pitr:
                logger.info("PITR will not be enabled since wait_for_completion=False")

        # Generate summary
        successful_imports = len([r for r in import_results if r.get('import_status') == 'COMPLETED'])
        failed_imports = len(
            [r for r in import_results if r.get('import_status') in ['FAILED', 'CANCELLED'] or 'error' in r])
        in_progress_imports = len([r for r in import_results if r.get('import_status') == 'IN_PROGRESS'])
        import_jobs_started = len([r for r in import_results if r.get('status') == 'IMPORT_STARTED'])
        total_items = sum(r.get('items_restored', 0) for r in import_results)

        # List completed tables
        completed_tables = []
        for result in import_results:
            if result.get('import_status') == 'COMPLETED':
                # Find corresponding PITR status
                pitr_status = "not_attempted"
                for pitr_result in pitr_results:
                    if pitr_result['table_name'] == result.get('target_table_name'):
                        pitr_status = "enabled" if pitr_result['pitr_enabled'] else "failed"
                        break

                completed_tables.append({
                    'table_name': result.get('target_table_name'),
                    'original_table': result.get('table_name'),
                    'items': result.get('items_restored', 0),
                    'pitr_enabled': pitr_status
                })

        summary = {
            'backup_date': backup_date,
            'environment': environment,
            'restore_type': 'DYNAMODB_IMPORT_FROM_S3',
            'tables_requested': len(tables_to_restore),
            'import_jobs_started': import_jobs_started,
            'successful_imports': successful_imports,
            'failed_imports': failed_imports,
            'in_progress_imports': in_progress_imports,
            'total_items_restored': total_items,
            'waited_for_completion': wait_for_completion,
            'pitr_enabled': enable_pitr,
            'completed_tables': completed_tables,
            'import_arns': import_arns,
            'import_results': import_results,
            'pitr_results': pitr_results,
            'manifest_info': {
                'backup_type': manifest.get('backup_type'),
                'created_at': manifest.get('created_at')
            }
        }

        # Add monitoring info if imports are still running
        if in_progress_imports > 0:
            summary['note'] = "Some imports are still running. Check DynamoDB console to monitor progress."
            summary['monitoring_command'] = "aws dynamodb describe-import --import-arn <import-arn>"
            if enable_pitr:
                summary['note'] += " PITR will need to be enabled manually after imports complete."

        # PITR summary
        if pitr_results:
            pitr_enabled_count = len([r for r in pitr_results if r['pitr_enabled']])
            pitr_failed_count = len([r for r in pitr_results if not r['pitr_enabled']])
            summary['pitr_summary'] = {
                'pitr_enabled_count': pitr_enabled_count,
                'pitr_failed_count': pitr_failed_count,
                'total_pitr_attempts': len(pitr_results)
            }

        logger.info(
            f"Disaster recovery summary: {successful_imports} completed, {failed_imports} failed, {in_progress_imports} in progress")

        if completed_tables:
            logger.info("Completed restored tables:")
            for table in completed_tables:
                pitr_msg = f" (PITR: {table['pitr_enabled']})" if table['pitr_enabled'] != "not_attempted" else ""
                logger.info(f"  - {table['table_name']} ({table['items']} items){pitr_msg}")

        if in_progress_imports > 0:
            logger.info(f"{in_progress_imports} imports still running in background")

        # Determine response status
        if failed_imports > 0 and successful_imports == 0:
            status_code = 500  # Complete failure
        elif failed_imports > 0:
            status_code = 207  # Multi-status (partial success)
        elif in_progress_imports > 0:
            status_code = 202  # Accepted (imports still running)
        else:
            status_code = 200  # Success

        return {
            'statusCode': status_code,
            'body': json.dumps(summary, default=decimal_default)
        }

    except Exception as e:
        logger.error(f"Critical error in disaster recovery: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'environment': os.environ.get('ENVIRONMENT', 'unknown')
            })
        }
