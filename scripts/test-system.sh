#!/bin/bash

echo "Testing Simple MFA Backup System..."

ENVIRONMENT=${1:-prod}
REGION=${2:-us-east-1}

# Function names
BACKUP_FUNCTION="mfa-daily-backup-${ENVIRONMENT}"
RECOVERY_FUNCTION="mfa-disaster-recovery-${ENVIRONMENT}"

test_lambda() {
    local function_name=$1
    local payload=$2
    local test_name=$3

    echo "Testing $test_name..."

    if aws lambda invoke \
        --region $REGION \
        --function-name $function_name \
        --payload "$payload" \
        test_output.json > /dev/null 2>&1; then

        if [ -f test_output.json ]; then
            STATUS=$(jq -r '.statusCode // empty' test_output.json)
            ERROR=$(jq -r '.errorMessage // empty' test_output.json)

            if [ "$ERROR" != "" ] && [ "$ERROR" != "null" ]; then
                echo "$test_name failed: $ERROR"
                return 1
            else
                echo "$test_name passed"

                # Show key results
                if jq -e '.body' test_output.json > /dev/null 2>&1; then
                    BODY=$(jq -r '.body' test_output.json)
                    echo "$BODY" | jq -r '
                        if .successful_exports then "  Successful exports: \(.successful_exports)" else empty end,
                        if .successful_restores then "  Successful restores: \(.successful_restores)" else empty end,
                        if .total_items_exported then "  Items exported: \(.total_items_exported)" else empty end,
                        if .total_items_restored then "  Items restored: \(.total_items_restored)" else empty end
                    ' 2>/dev/null || true
                fi
                return 0
            fi
        fi
    else
        echo "$test_name failed: Lambda invocation failed"
        return 1
    fi
}

# Test 1: Daily Backup
test_lambda $BACKUP_FUNCTION '{}' "Daily Backup"
BACKUP_RESULT=$?

echo ""

# Test 2: Disaster Recovery (Dry Run)
test_lambda $RECOVERY_FUNCTION '{
    "backup_date": "latest",
    "target_suffix": "_test",
    "dry_run": true
}' "Disaster Recovery (Dry Run)"
RECOVERY_RESULT=$?

echo ""

# Test Summary
if [ $BACKUP_RESULT -eq 0 ] && [ $RECOVERY_RESULT -eq 0 ]; then
    echo "All tests passed! System is working correctly."
else
    echo "Some tests failed. Check logs and fix issues."
    exit 1
fi

# Cleanup
rm -f test_output.json
