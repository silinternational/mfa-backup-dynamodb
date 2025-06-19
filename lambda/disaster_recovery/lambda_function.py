import json
import boto3
import gzip
import os
import re
from datetime import datetime, timezone
from decimal import Decimal
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')
s3 = boto3.client('s3')

def decimal_default(obj):
    """JSON serializer for objects not serializable by default"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

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
            # Extract date from prefix like 'native-exports/2025-06-19/'
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

def get_table_schema(table_name):
    """Get the schema of the original table for recreation"""
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

        # Add LSI if present
        if 'LocalSecondaryIndexes' in table_info:
            schema['LocalSecondaryIndexes'] = table_info['LocalSecondaryIndexes']

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

def create_table_from_schema(table_name, schema):
    """Create a new table with the given schema"""
    try:
        create_params = {
            'TableName': table_name,
            'AttributeDefinitions': schema['AttributeDefinitions'],
            'KeySchema': schema['KeySchema'],
            'BillingMode': schema['BillingMode']
        }

        # Add provisioned throughput if needed
        if schema['BillingMode'] == 'PROVISIONED':
            create_params['ProvisionedThroughput'] = schema['ProvisionedThroughput']

        # Add GSI if present
        if schema.get('GlobalSecondaryIndexes'):
            create_params['GlobalSecondaryIndexes'] = schema['GlobalSecondaryIndexes']

        # Add LSI if present
        if schema.get('LocalSecondaryIndexes'):
            create_params['LocalSecondaryIndexes'] = schema['LocalSecondaryIndexes']

        dynamodb.create_table(**create_params)

        # Wait for table to be active
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(TableName=table_name, WaiterConfig={'Delay': 5, 'MaxAttempts': 60})

        logger.info(f"Successfully created table: {table_name}")
        return True

    except Exception as e:
        logger.error(f"Failed to create table {table_name}: {str(e)}")
        return False

def list_export_data_files(s3_bucket, s3_prefix):
    """List all data files from a DynamoDB export"""
    try:
        # DynamoDB exports create files under: s3_prefix/AWSDynamoDB/{export-id}/data/
        response = s3.list_objects_v2(
            Bucket=s3_bucket,
            Prefix=f"{s3_prefix}AWSDynamoDB/",
            Delimiter='/'
        )

        export_dirs = []
        for prefix in response.get('CommonPrefixes', []):
            export_dirs.append(prefix['Prefix'])

        if not export_dirs:
            logger.error(f"No export directories found under {s3_prefix}")
            return []

        # Use the first export directory (there should only be one)
        export_dir = export_dirs[0]
        data_prefix = f"{export_dir}data/"

        # List all data files
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=s3_bucket,
            Prefix=data_prefix
        )

        data_files = []
        for page in page_iterator:
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.json.gz'):
                    data_files.append(obj['Key'])

        logger.info(f"Found {len(data_files)} data files for export")
        return data_files

    except Exception as e:
        logger.error(f"Failed to list export data files: {str(e)}")
        return []

def restore_data_from_export_file(s3_bucket, file_key, table_name, batch_size=25):
    """Restore data from a single export file"""
    try:
        # Download and decompress the file
        response = s3.get_object(Bucket=s3_bucket, Key=file_key)
        compressed_data = response['Body'].read()
        json_data = gzip.decompress(compressed_data).decode('utf-8')

        # Parse JSON lines (each line is a separate item)
        table = dynamodb_resource.Table(table_name)
        items_processed = 0

        with table.batch_writer() as batch:
            for line in json_data.strip().split('\n'):
                if line.strip():
                    try:
                        # Parse the DynamoDB JSON format
                        item_data = json.loads(line)

                        # Extract the Item from DynamoDB export format
                        if 'Item' in item_data:
                            ddb_item = item_data['Item']
                        else:
                            ddb_item = item_data

                        # Convert DynamoDB JSON format to regular format
                        converted_item = convert_ddb_json_to_item(ddb_item)

                        # Write to table
                        batch.put_item(Item=converted_item)
                        items_processed += 1

                        if items_processed % 100 == 0:
                            logger.info(f"Processed {items_processed} items from {file_key}")

                    except json.JSONDecodeError as e:
                        logger.warning(f"Skipping invalid JSON line in {file_key}: {str(e)}")
                        continue
                    except Exception as e:
                        logger.warning(f"Failed to process item in {file_key}: {str(e)}")
                        continue

        logger.info(f"Successfully restored {items_processed} items from {file_key}")
        return items_processed

    except Exception as e:
        logger.error(f"Failed to restore data from {file_key}: {str(e)}")
        return 0

def convert_ddb_json_to_item(ddb_item):
    """Convert DynamoDB JSON format to regular Python dict"""
    converted_item = {}

    for key, value in ddb_item.items():
        if isinstance(value, dict) and len(value) == 1:
            type_key = list(value.keys())[0]
            type_value = value[type_key]

            if type_key == 'S':  # String
                converted_item[key] = type_value
            elif type_key == 'N':  # Number
                converted_item[key] = Decimal(type_value)
            elif type_key == 'B':  # Binary
                converted_item[key] = type_value
            elif type_key == 'BOOL':  # Boolean
                converted_item[key] = type_value
            elif type_key == 'NULL':  # Null
                converted_item[key] = None
            elif type_key == 'M':  # Map
                converted_item[key] = convert_ddb_json_to_item(type_value)
            elif type_key == 'L':  # List
                converted_item[key] = [convert_ddb_json_to_item({'temp': item})['temp'] for item in type_value]
            elif type_key == 'SS':  # String Set
                converted_item[key] = set(type_value)
            elif type_key == 'NS':  # Number Set
                converted_item[key] = set(Decimal(n) for n in type_value)
            elif type_key == 'BS':  # Binary Set
                converted_item[key] = set(type_value)
            else:
                converted_item[key] = type_value
        else:
            converted_item[key] = value

    return converted_item

def restore_table_from_export(s3_bucket, export_info, target_table_name, original_table_name):
    """Restore a complete table from DynamoDB export"""
    try:
        s3_prefix = export_info['s3_prefix']

        # Get schema from original table
        original_schema = get_table_schema(original_table_name)
        if not original_schema:
            logger.error(f"Could not get schema for {original_table_name}")
            return {
                'table_name': original_table_name,
                'target_table_name': target_table_name,
                'error': 'Could not get original table schema',
                'items_restored': 0
            }

        # Delete target table if it exists
        try:
            dynamodb.describe_table(TableName=target_table_name)
            logger.info(f"Deleting existing table: {target_table_name}")
            dynamodb.delete_table(TableName=target_table_name)

            # Wait for deletion
            waiter = dynamodb.get_waiter('table_not_exists')
            waiter.wait(TableName=target_table_name, WaiterConfig={'Delay': 5, 'MaxAttempts': 60})

        except dynamodb.exceptions.ResourceNotFoundException:
            pass  # Table doesn't exist, which is fine

        # Create new table
        if not create_table_from_schema(target_table_name, original_schema):
            return {
                'table_name': original_table_name,
                'target_table_name': target_table_name,
                'error': 'Failed to create target table',
                'items_restored': 0
            }

        # Get list of data files
        data_files = list_export_data_files(s3_bucket, s3_prefix)
        if not data_files:
            return {
                'table_name': original_table_name,
                'target_table_name': target_table_name,
                'error': 'No export data files found',
                'items_restored': 0
            }

        # Restore data from all files
        total_items_restored = 0
        for file_key in data_files:
            items_count = restore_data_from_export_file(s3_bucket, file_key, target_table_name)
            total_items_restored += items_count

        logger.info(f"Successfully restored {total_items_restored} items to {target_table_name}")

        return {
            'table_name': original_table_name,
            'target_table_name': target_table_name,
            'items_restored': total_items_restored,
            'data_files_processed': len(data_files),
            'export_date': export_info.get('export_time', 'unknown'),
            'export_arn': export_info.get('export_arn', 'unknown')
        }

    except Exception as e:
        logger.error(f"Failed to restore table {original_table_name}: {str(e)}")
        return {
            'table_name': original_table_name,
            'target_table_name': target_table_name,
            'error': str(e),
            'items_restored': 0
        }

def lambda_handler(event, context):
    """Main disaster recovery handler"""
    logger.info("Starting MFA disaster recovery process")

    try:
        # Parse input parameters
        backup_date = event.get('backup_date', 'latest')
        specific_tables = event.get('tables', [])  # Specific tables to restore
        target_suffix = event.get('target_suffix', '_restored')
        dry_run = event.get('dry_run', False)

        s3_bucket = os.environ['BACKUP_BUCKET']
        environment = os.environ['ENVIRONMENT']

        logger.info(f"Configuration: environment={environment}, bucket={s3_bucket}, backup_date={backup_date}")

        # Get tables from Terraform environment variables
        all_available_tables = get_tables_to_restore()

        # Use specific tables if provided, otherwise use all available tables
        if specific_tables:
            # Validate that specified tables are in our available list
            tables_to_restore = []
            for table in specific_tables:
                if table in all_available_tables:
                    tables_to_restore.append(table)
                else:
                    logger.warning(f"Requested table {table} not in available tables list")

            if not tables_to_restore:
                raise Exception("None of the specified tables are available for restoration")
        else:
            tables_to_restore = all_available_tables

        logger.info(f"📋 Disaster recovery for tables: {tables_to_restore}")

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
            logger.info("🔍 DRY RUN MODE - No actual restoration will be performed")

            # Check which tables would be restored
            tables_that_can_be_restored = []
            tables_missing_exports = []

            for table_name in tables_to_restore:
                if table_name in available_exports:
                    tables_that_can_be_restored.append({
                        'table_name': table_name,
                        'target_table_name': f"{table_name}{target_suffix}",
                        'export_arn': available_exports[table_name].get('export_arn', 'unknown'),
                        'item_count': available_exports[table_name].get('item_count', 0)
                    })
                else:
                    tables_missing_exports.append(table_name)

            dry_run_summary = {
                'dry_run': True,
                'backup_date': backup_date,
                'environment': environment,
                'tables_requested': tables_to_restore,
                'tables_that_can_be_restored': tables_that_can_be_restored,
                'tables_missing_exports': tables_missing_exports,
                'target_suffix': target_suffix,
                'total_items_to_restore': sum(t.get('item_count', 0) for t in tables_that_can_be_restored),
                'manifest_summary': {
                    'backup_type': manifest.get('backup_type'),
                    'total_exports': manifest.get('total_exports'),
                    'successful_exports': manifest.get('successful_exports')
                }
            }

            return {
                'statusCode': 200,
                'body': json.dumps(dry_run_summary, default=decimal_default)
            }

        # Perform actual restoration
        restore_results = []

        for table_name in tables_to_restore:
            if table_name not in available_exports:
                logger.warning(f"No successful export found for table {table_name}")
                restore_results.append({
                    'table_name': table_name,
                    'error': 'No successful export found for this table',
                    'items_restored': 0
                })
                continue

            # Determine target table name
            target_table_name = f"{table_name}{target_suffix}"

            logger.info(f"🔄 Restoring {table_name} → {target_table_name}")

            # Restore the table
            export_info = available_exports[table_name]
            result = restore_table_from_export(s3_bucket, export_info, target_table_name, table_name)
            restore_results.append(result)

        # Generate summary
        successful_restores = len([r for r in restore_results if 'items_restored' in r and r['items_restored'] > 0])
        failed_restores = len([r for r in restore_results if 'error' in r])
        total_items = sum(r.get('items_restored', 0) for r in restore_results if 'items_restored' in r)

        summary = {
            'backup_date': backup_date,
            'environment': environment,
            'restore_type': 'DYNAMODB_NATIVE_EXPORT',
            'tables_requested': len(tables_to_restore),
            'successful_restores': successful_restores,
            'failed_restores': failed_restores,
            'total_items_restored': total_items,
            'target_suffix': target_suffix,
            'restore_results': restore_results,
            'manifest_info': {
                'backup_type': manifest.get('backup_type'),
                'created_at': manifest.get('created_at')
            }
        }

        logger.info(
            f"Disaster recovery completed: {successful_restores} successful, {failed_restores} failed, {total_items} items restored")

        # Determine response status
        if failed_restores > 0 and successful_restores == 0:
            status_code = 500  # Complete failure
        elif failed_restores > 0:
            status_code = 207  # Multi-status (partial success)
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
