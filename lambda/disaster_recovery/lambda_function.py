import json
import boto3
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
from decimal import Decimal
import logging
from datetime import datetime
import re

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
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def validate_environment():
    """Validate required environment variables"""
    required_vars = ['BACKUP_BUCKET', 'ENVIRONMENT']
    missing_vars = []

    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)

    if missing_vars:
        raise Exception(f"Missing required environment variables: {', '.join(missing_vars)}")

    return {
        'backup_bucket': os.environ['BACKUP_BUCKET'],
        'environment': os.environ['ENVIRONMENT']
    }


def get_tables_to_restore():
    """
    Get list of MFA tables that can be restored
    """
    try:
        env_config = validate_environment()
        environment = env_config['environment']

        tables = [
            f"mfa-api_{environment}_u2f_global",
            f"mfa-api_{environment}_totp_global",
            f"mfa-api_{environment}_api-key_global"
        ]

        logger.info(f"Available tables for restore: {tables}")
        return tables

    except Exception as e:
        logger.error(f"Failed to get tables to restore: {str(e)}")
        raise


def get_available_backups(s3_bucket):
    """
    Get list of available backup dates from S3
    Returns dates in descending order (newest first)
    """
    try:
        logger.info(f"Scanning for backups in s3://{s3_bucket}/exports/")

        response = s3.list_objects_v2(
            Bucket=s3_bucket,
            Prefix='exports/',
            Delimiter='/'
        )

        backup_dates = []
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')  # YYYY-MM-DD format

        for prefix in response.get('CommonPrefixes', []):
            # Extract date from prefix like 'exports/2025-01-15/'
            path_parts = prefix['Prefix'].rstrip('/').split('/')
            if len(path_parts) >= 2:
                date_part = path_parts[-1]
                if date_pattern.match(date_part):
                    backup_dates.append(date_part)

        if not backup_dates:
            logger.warning("No backup dates found in expected format (YYYY-MM-DD)")

            # Try to list what's actually there for debugging
            response = s3.list_objects_v2(
                Bucket=s3_bucket,
                Prefix='exports/',
                MaxKeys=10
            )

            logger.info("Available objects under exports/:")
            for obj in response.get('Contents', []):
                logger.info(f"  {obj['Key']}")

        # Return sorted by date descending (newest first)
        sorted_dates = sorted(backup_dates, reverse=True)
        logger.info(f"Found {len(sorted_dates)} backup dates: {sorted_dates[:5]}")  # Show first 5

        return sorted_dates

    except Exception as e:
        logger.error(f"Failed to get available backups: {str(e)}")
        raise


def get_backup_manifest(s3_bucket, backup_date):
    """
    Get backup manifest for a specific date
    """
    try:
        manifest_key = f"exports/{backup_date}/manifest.json"

        logger.info(f"Fetching manifest: s3://{s3_bucket}/{manifest_key}")

        try:
            response = s3.get_object(Bucket=s3_bucket, Key=manifest_key)
            manifest_content = response['Body'].read().decode('utf-8')
            manifest = json.loads(manifest_content)

            # Validate basic manifest structure
            if not isinstance(manifest, dict):
                raise ValueError("Manifest is not a valid JSON object")

            if 'exports' not in manifest:
                raise ValueError("Manifest missing 'exports' field")

            if not isinstance(manifest['exports'], list):
                raise ValueError("Manifest 'exports' field is not a list")

            logger.info(f"Found valid manifest with {len(manifest.get('exports', []))} exports")
            return manifest

        except s3.exceptions.NoSuchKey:
            logger.error(f"Manifest not found: {manifest_key}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in manifest: {str(e)}")
            return None

    except Exception as e:
        logger.error(f"Failed to get backup manifest: {str(e)}")
        return None


def validate_export_info(export_info):
    """
    Validate that export_info has required fields and correct status
    """
    if not isinstance(export_info, dict):
        raise ValueError("Export info is not a dictionary")

    required_fields = ['table_name', 's3_prefix', 'status']

    for field in required_fields:
        if field not in export_info:
            raise ValueError(f"Export info missing required field: {field}")

        if not export_info[field]:
            raise ValueError(f"Export info field '{field}' is empty")

    if export_info['status'] != 'COMPLETED':
        raise ValueError(f"Export status is '{export_info['status']}', expected 'COMPLETED'")

    return True


def get_export_data_files(s3_bucket, export_info):
    """
    Get list of all data files for an export with robust S3 structure detection
    """
    try:
        # Validate export info first
        validate_export_info(export_info)

        s3_prefix = export_info['s3_prefix'].rstrip('/')
        table_name = export_info['table_name']

        logger.info(f"Looking for data files for {table_name} under: {s3_prefix}")

        # Strategy 1: Standard DynamoDB export structure
        # s3_prefix/AWSDynamoDB/{export-id}/data/*.json.gz
        data_prefix = f"{s3_prefix}/AWSDynamoDB/"

        try:
            logger.info(f"Trying standard structure: {data_prefix}")

            response = s3.list_objects_v2(
                Bucket=s3_bucket,
                Prefix=data_prefix,
                Delimiter='/'
            )

            export_dirs = [prefix['Prefix'] for prefix in response.get('CommonPrefixes', [])]

            if export_dirs:
                # Use the most recent export directory (highest timestamp)
                export_dirs.sort(reverse=True)
                export_dir = export_dirs[0]

                logger.info(f"Found export directory: {export_dir}")

                # Look for data files in multiple possible locations
                possible_data_paths = [
                    f"{export_dir}data/",  # Standard location
                    export_dir,  # Files directly in export dir
                ]

                for data_path in possible_data_paths:
                    logger.info(f"Checking for data files in: {data_path}")

                    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=data_path)

                    data_files = []
                    for obj in response.get('Contents', []):
                        if obj['Key'].endswith('.json.gz') or obj['Key'].endswith('.json'):
                            data_files.append(obj['Key'])

                    if data_files:
                        logger.info(f"Found {len(data_files)} data files in {data_path}")
                        return data_files

        except Exception as e:
            logger.warning(f"Standard structure failed: {str(e)}")

        # Strategy 2: Files directly under s3_prefix
        logger.info(f"Trying direct files under: {s3_prefix}")

        try:
            response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=f"{s3_prefix}/")

            data_files = []
            for obj in response.get('Contents', []):
                if obj['Key'].endswith('.json.gz') or obj['Key'].endswith('.json'):
                    data_files.append(obj['Key'])

            if data_files:
                logger.info(f"Found {len(data_files)} data files directly under {s3_prefix}")
                return data_files

        except Exception as e:
            logger.warning(f"Direct file search failed: {str(e)}")

        # Strategy 3: Recursive search under s3_prefix
        logger.info(f"Trying recursive search under: {s3_prefix}")

        try:
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=s3_bucket, Prefix=f"{s3_prefix}/")

            data_files = []
            for page in pages:
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('.json.gz') or obj['Key'].endswith('.json'):
                        data_files.append(obj['Key'])

            if data_files:
                logger.info(f"Found {len(data_files)} data files via recursive search")
                return data_files

        except Exception as e:
            logger.warning(f"Recursive search failed: {str(e)}")

        # If we get here, no files were found
        logger.error(f"No data files found for {table_name} under any search strategy")

        # List what's actually there for debugging
        try:
            response = s3.list_objects_v2(
                Bucket=s3_bucket,
                Prefix=f"{s3_prefix}/",
                MaxKeys=20
            )

            logger.info(f"Debug: Contents under {s3_prefix}/:")
            for obj in response.get('Contents', []):
                logger.info(f" {obj['Key']}")

        except Exception:
            pass

        raise Exception(f"No data files found for export {table_name}")

    except Exception as e:
        logger.error(f"Failed to get export data files for {export_info.get('table_name', 'unknown')}: {str(e)}")
        raise


def parse_dynamodb_json_file(s3_bucket, s3_key):
    """Parse a single DynamoDB JSON export file from S3"""
    try:
        logger.debug(f"Parsing file: {s3_key}")

        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)

        # Handle gzipped files
        if s3_key.endswith('.gz'):
            content = gzip.decompress(response['Body'].read()).decode('utf-8')
        else:
            content = response['Body'].read().decode('utf-8')

        items = []
        line_count = 0
        error_count = 0

        for line in content.strip().split('\n'):
            line_count += 1
            if line.strip():
                try:
                    item_data = json.loads(line)
                    if 'Item' in item_data:
                        items.append(item_data['Item'])
                    elif isinstance(item_data, dict):
                        # Handle case where the line is already the item
                        items.append(item_data)
                except json.JSONDecodeError as e:
                    error_count += 1
                    if error_count <= 5:  # Log first 5 errors only
                        logger.warning(f"JSON decode error on line {line_count}: {str(e)}")

        if error_count > 0:
            logger.warning(f"File {s3_key}: {error_count} JSON decode errors out of {line_count} lines")

        logger.debug(f"Parsed {len(items)} items from {s3_key}")
        return items

    except Exception as e:
        logger.error(f"Error parsing file {s3_key}: {str(e)}")
        return []


def clear_existing_table_data(table_name, preserve_schema=True):
    """
    Clear existing table data before restore
    WARNING: This deletes all existing data!
    """
    try:
        logger.warning(f"CLEARING ALL DATA from table: {table_name}")

        # Get table schema
        response = dynamodb.describe_table(TableName=table_name)
        table_info = response['Table']
        key_schema = table_info['KeySchema']

        # Get partition key and sort key names
        partition_key = None
        sort_key = None

        for key in key_schema:
            if key['KeyType'] == 'HASH':
                partition_key = key['AttributeName']
            elif key['KeyType'] == 'RANGE':
                sort_key = key['AttributeName']

        if not partition_key:
            raise Exception("Could not determine partition key")

        logger.info(f"Table schema - Partition key: {partition_key}, Sort key: {sort_key}")

        # Scan and delete all items
        scan_kwargs = {'TableName': table_name}
        items_deleted = 0
        batch_count = 0

        while True:
            response = dynamodb.scan(**scan_kwargs)
            items = response.get('Items', [])

            if not items:
                break

            # Delete items in batches
            with ThreadPoolExecutor(max_workers=10) as executor:
                delete_futures = []

                for i in range(0, len(items), 25):  # DynamoDB batch limit
                    batch = items[i:i + 25]
                    delete_requests = []

                    for item in batch:
                        key = {partition_key: item[partition_key]}
                        if sort_key and sort_key in item:
                            key[sort_key] = item[sort_key]

                        delete_requests.append({
                            'DeleteRequest': {'Key': key}
                        })

                    future = executor.submit(
                        dynamodb.batch_write_item,
                        RequestItems={table_name: delete_requests}
                    )
                    delete_futures.append(future)
                    batch_count += 1

                # Wait for all delete batches to complete
                for future in as_completed(delete_futures):
                    try:
                        result = future.result()
                        items_deleted += 25

                        # Handle unprocessed items
                        unprocessed = result.get('UnprocessedItems', {}).get(table_name, [])
                        if unprocessed:
                            logger.warning(f"Batch delete had {len(unprocessed)} unprocessed items")

                    except Exception as e:
                        logger.error(f"Error in delete batch: {str(e)}")

            # Handle pagination
            if 'LastEvaluatedKey' in response:
                scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            else:
                break

            if batch_count % 10 == 0:
                logger.info(f"Deletion progress: ~{items_deleted} items deleted...")

        logger.info(f"Successfully cleared {items_deleted} items from {table_name}")
        return True, items_deleted

    except Exception as e:
        logger.error(f"Failed to clear table data: {str(e)}")
        return False, 0


def batch_write_items_to_table(table_name, items, max_workers=5):
    """Write items to DynamoDB table using batch_write_item with threading"""
    if not items:
        return 0, 0

    total_items = len(items)
    items_written = 0
    failed_items = 0

    logger.info(f"Writing {total_items} items to {table_name} using {max_workers} threads")

    def write_batch(batch_items):
        """Write a single batch of items"""
        batch_successful = 0
        batch_failed = 0
        max_retries = 3

        try:
            put_requests = []
            for item in batch_items:
                put_requests.append({
                    'PutRequest': {'Item': item}
                })

            for attempt in range(max_retries):
                try:
                    response = dynamodb.batch_write_item(
                        RequestItems={table_name: put_requests}
                    )

                    batch_successful = len(batch_items)

                    # Handle unprocessed items
                    unprocessed = response.get('UnprocessedItems', {}).get(table_name, [])
                    if unprocessed and attempt < max_retries - 1:
                        logger.debug(
                            f"Batch had {len(unprocessed)} unprocessed items, retrying... (attempt {attempt + 1})")
                        time.sleep(min(2 ** attempt, 10))  # Exponential backoff
                        put_requests = unprocessed
                        batch_successful = len(batch_items) - len(unprocessed)
                        continue
                    elif unprocessed:
                        # Final attempt still has unprocessed items
                        batch_failed = len(unprocessed)
                        batch_successful = len(batch_items) - batch_failed
                        logger.warning(f"Final attempt: {batch_failed} items failed after {max_retries} retries")

                    break  # Success

                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Batch write attempt {attempt + 1} failed: {str(e)}, retrying...")
                        time.sleep(min(2 ** attempt, 10))
                        continue
                    else:
                        raise  # Final attempt failed

        except Exception as e:
            logger.error(f"Batch write failed after all retries: {str(e)}")
            batch_failed = len(batch_items)
            batch_successful = 0

        return batch_successful, batch_failed

    # Process items in batches of 25 (DynamoDB limit) using threads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for i in range(0, total_items, 25):
            batch = items[i:i + 25]
            future = executor.submit(write_batch, batch)
            futures.append(future)

        # Collect results
        batch_count = 0
        for future in as_completed(futures):
            try:
                successful, failed = future.result()
                items_written += successful
                failed_items += failed
                batch_count += 1

                if batch_count % 20 == 0:  # Progress every 500 items (20 batches)
                    logger.info(f"Progress: {items_written + failed_items}/{total_items} items processed")

            except Exception as e:
                logger.error(f"Thread execution failed: {str(e)}")
                failed_items += 25  # Assume whole batch failed

    success_rate = (items_written / total_items * 100) if total_items > 0 else 0
    logger.info(f"Batch write completed: {items_written}/{total_items} successful ({success_rate:.1f}%)")

    if failed_items > 0:
        logger.warning(f" {failed_items} items failed to write")

    return items_written, failed_items


def restore_table_from_s3_export(table_name, export_info, s3_bucket, clear_existing=False, max_workers=5):
    """
    Restore table data from S3 export using batch write operations
    """
    logger.info(f" Starting batch write restore for table: {table_name}")

    try:
        # Optional: Clear existing data first
        items_cleared = 0
        if clear_existing:
            logger.warning(f" Clearing existing data as requested")
            success, items_cleared = clear_existing_table_data(table_name)
            if not success:
                raise Exception("Failed to clear existing table data")

        # Get all data files for this export
        data_files = get_export_data_files(s3_bucket, export_info)
        if not data_files:
            raise Exception("No data files found for export")

        total_items_processed = 0
        total_items_written = 0
        total_items_failed = 0

        # Process each data file
        for i, data_file in enumerate(data_files):
            logger.info(f" Processing file {i + 1}/{len(data_files)}: {data_file}")

            # Parse items from this file
            items = parse_dynamodb_json_file(s3_bucket, data_file)
            if not items:
                logger.warning(f"No items found in {data_file}")
                continue

            logger.info(f"Found {len(items)} items in {data_file}")

            # Write items to table
            written, failed = batch_write_items_to_table(table_name, items, max_workers)

            total_items_processed += len(items)
            total_items_written += written
            total_items_failed += failed

            # Brief pause between files to avoid overwhelming DynamoDB
            if i < len(data_files) - 1:
                time.sleep(1)

        # Calculate success rate
        success_rate = (total_items_written / total_items_processed * 100) if total_items_processed > 0 else 0

        # Determine status
        if total_items_failed == 0:
            status = 'COMPLETED'
        elif total_items_written > 0:
            status = 'PARTIAL_SUCCESS'
        else:
            status = 'FAILED'

        result = {
            'table_name': table_name,
            'restore_type': 'BATCH_WRITE_FROM_S3',
            'status': status,
            'total_files_processed': len(data_files),
            'items_cleared': items_cleared,
            'items_processed': total_items_processed,
            'items_written': total_items_written,
            'items_failed': total_items_failed,
            'success_rate': f"{success_rate:.2f}%",
            'expected_items': export_info.get('item_count', 0),
            'export_arn': export_info.get('export_arn', 'unknown')
        }

        if total_items_failed > 0:
            result['warning'] = f"{total_items_failed} items failed to write"

        logger.info(f" Batch write restore completed for {table_name}")
        logger.info(
            f"Results: {total_items_written}/{total_items_processed} items written ({success_rate:.2f}% success)")

        return result

    except Exception as e:
        logger.error(f" Batch write restore failed for {table_name}: {str(e)}")
        return {
            'table_name': table_name,
            'restore_type': 'BATCH_WRITE_FROM_S3',
            'status': 'FAILED',
            'error': str(e)
        }


def lambda_handler(event, context):
    """
    Main handler for batch write restoration from S3 exports
    """
    start_time = datetime.now()
    logger.info(f" Starting MFA disaster recovery from S3 exports at {start_time}")

    try:
        # Validate environment
        env_config = validate_environment()
        s3_bucket = env_config['backup_bucket']
        environment = env_config['environment']

        # Parse input parameters
        backup_date = event.get('backup_date', 'latest')
        specific_tables = event.get('tables', [])
        dry_run = event.get('dry_run', False)
        clear_existing_data = event.get('clear_existing_data', False)
        max_workers = event.get('max_workers', 5)

        logger.info(f"📋 Configuration:")
        logger.info(f"  Environment: {environment}")
        logger.info(f"  S3 Bucket: {s3_bucket}")
        logger.info(f"  Backup Date: {backup_date}")
        logger.info(f"  Specific Tables: {specific_tables or 'All available'}")
        logger.info(f"  Dry Run: {dry_run}")
        logger.info(f"  Clear Existing Data: {clear_existing_data}")
        logger.info(f"  Max Workers: {max_workers}")

        if clear_existing_data:
            logger.warning(" WARNING: clear_existing_data=True will DELETE ALL existing data before restore!")

        # Get tables to restore
        all_available_tables = get_tables_to_restore()

        if specific_tables:
            # Validate requested tables
            invalid_tables = [t for t in specific_tables if t not in all_available_tables]
            if invalid_tables:
                logger.warning(f"Invalid tables requested: {invalid_tables}")

            tables_to_restore = [table for table in specific_tables if table in all_available_tables]
            if not tables_to_restore:
                raise Exception(f"None of the specified tables are available. Available: {all_available_tables}")
        else:
            tables_to_restore = all_available_tables

        logger.info(f" Tables to restore: {tables_to_restore}")

        # Get backup date if 'latest'
        if backup_date == 'latest':
            available_backups = get_available_backups(s3_bucket)
            if not available_backups:
                raise Exception("No backups found in S3")
            backup_date = available_backups[0]
            logger.info(f" Using latest backup from: {backup_date}")

        # Get and validate backup manifest
        manifest = get_backup_manifest(s3_bucket, backup_date)
        if not manifest:
            raise Exception(f"Could not find or parse backup manifest for {backup_date}")

        # Build export lookup
        available_exports = {}
        invalid_exports = []

        for export in manifest.get('exports', []):
            try:
                validate_export_info(export)
                table_name = export['table_name']
                if table_name in tables_to_restore:
                    available_exports[table_name] = export
            except ValueError as e:
                invalid_exports.append(f"{export.get('table_name', 'unknown')}: {str(e)}")

        if invalid_exports:
            logger.warning(f" Invalid exports found: {invalid_exports}")

        logger.info(f"📊 Available exports for {backup_date}: {list(available_exports.keys())}")

        # Check for missing exports
        missing_exports = [t for t in tables_to_restore if t not in available_exports]
        if missing_exports:
            logger.warning(f" No valid exports found for tables: {missing_exports}")

        if not available_exports:
            raise Exception("No valid exports found for any requested tables")

        # Dry run mode
        if dry_run:
            logger.info(" DRY RUN MODE - Validating restore capability without writing data")

            validation_results = []
            for table_name in tables_to_restore:
                if table_name in available_exports:
                    export_info = available_exports[table_name]
                    try:
                        data_files = get_export_data_files(s3_bucket, export_info)

                        # Calculate estimated restore size
                        total_file_size = 0
                        for file_key in data_files[:5]:  # Sample first 5 files
                            try:
                                response = s3.head_object(Bucket=s3_bucket, Key=file_key)
                                total_file_size += response['ContentLength']
                            except Exception:
                                pass

                        validation_results.append({
                            'table_name': table_name,
                            'status': 'READY',
                            'export_arn': export_info.get('export_arn', 'unknown'),
                            'expected_items': export_info.get('item_count', 0),
                            'data_files_count': len(data_files),
                            'estimated_size_mb': round(total_file_size / 1024 / 1024, 2),
                            'restore_options': {
                                'clear_existing_data': clear_existing_data,
                                'max_workers': max_workers
                            }
                        })
                    except Exception as e:
                        validation_results.append({
                            'table_name': table_name,
                            'status': 'ERROR',
                            'error': str(e)
                        })
                else:
                    validation_results.append({
                        'table_name': table_name,
                        'status': 'NO_EXPORT',
                        'error': 'No valid export found for this table'
                    })

            dry_run_summary = {
                'dry_run': True,
                'backup_date': backup_date,
                'environment': environment,
                'restore_type': 'BATCH_WRITE_FROM_S3_TO_EXISTING_TABLES',
                'tables_requested': len(tables_to_restore),
                'validation_results': validation_results,
                'configuration': {
                    'clear_existing_data': clear_existing_data,
                    'max_workers': max_workers
                },
                'warnings': [
                    'This approach writes directly to existing tables with the same names',
                    'Set clear_existing_data=true to clear existing data first',
                    'Restore will merge with existing data if clear_existing_data=false'
                ]
            }

            return {
                'statusCode': 200,
                'body': json.dumps(dry_run_summary, default=decimal_default, indent=2)
            }

        # Perform actual restore
        logger.info(f" Starting batch write restore for {len(available_exports)} tables")
        restore_results = []

        for table_name in tables_to_restore:
            if table_name not in available_exports:
                logger.warning(f" Skipping {table_name} - no valid export found")
                restore_results.append({
                    'table_name': table_name,
                    'restore_type': 'BATCH_WRITE_FROM_S3',
                    'status': 'SKIPPED',
                    'error': 'No valid export found for this table'
                })
                continue

            export_info = available_exports[table_name]
            logger.info(f" Starting restore for {table_name}")

            # Start batch write restore
            result = restore_table_from_s3_export(
                table_name,
                export_info,
                s3_bucket,
                clear_existing=clear_existing_data,
                max_workers=max_workers
            )
            restore_results.append(result)

            # Brief pause between tables
            if len(available_exports) > 1:
                time.sleep(2)

        # Generate summary
        end_time = datetime.now()
        duration = end_time - start_time

        successful_restores = len([r for r in restore_results if r.get('status') == 'COMPLETED'])
        partial_restores = len([r for r in restore_results if r.get('status') == 'PARTIAL_SUCCESS'])
        failed_restores = len([r for r in restore_results if r.get('status') == 'FAILED'])
        skipped_restores = len([r for r in restore_results if r.get('status') == 'SKIPPED'])

        total_items_written = sum(r.get('items_written', 0) for r in restore_results)
        total_items_processed = sum(r.get('items_processed', 0) for r in restore_results)

        summary = {
            'backup_date': backup_date,
            'environment': environment,
            'restore_type': 'BATCH_WRITE_FROM_S3_TO_EXISTING_TABLES',
            'duration_seconds': int(duration.total_seconds()),
            'tables_requested': len(tables_to_restore),
            'successful_restores': successful_restores,
            'partial_restores': partial_restores,
            'failed_restores': failed_restores,
            'skipped_restores': skipped_restores,
            'total_items_written': total_items_written,
            'total_items_processed': total_items_processed,
            'configuration': {
                'clear_existing_data': clear_existing_data,
                'max_workers': max_workers
            },
            'restore_results': restore_results,
            'completed_at': end_time.isoformat()
        }

        # Log summary
        logger.info(f" Batch write restore completed in {duration}")
        logger.info(
            f"📊 Results: {successful_restores} completed, {partial_restores} partial, {failed_restores} failed, {skipped_restores} skipped")
        logger.info(f"📈 Total items: {total_items_written}/{total_items_processed} written")

        # Determine response status
        if failed_restores > 0 and successful_restores == 0:
            status_code = 500
        elif failed_restores > 0 or partial_restores > 0:
            status_code = 207  # Multi-status
        else:
            status_code = 200

        return {
            'statusCode': status_code,
            'body': json.dumps(summary, default=decimal_default, indent=2)
        }

    except Exception as e:
        end_time = datetime.now()
        duration = end_time - start_time

        logger.error(f" Critical error in batch write restore after {duration}: {str(e)}")

        error_response = {
            'error': str(e),
            'restore_type': 'BATCH_WRITE_FROM_S3',
            'environment': os.environ.get('ENVIRONMENT', 'unknown'),
            'duration_seconds': int(duration.total_seconds()),
            'failed_at': end_time.isoformat()
        }

        return {
            'statusCode': 500,
            'body': json.dumps(error_response, default=decimal_default, indent=2)
        }
