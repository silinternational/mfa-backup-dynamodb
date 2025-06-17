#!/bin/bash

echo "Deploying Simple MFA Backup System..."
echo "========================================"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "terraform/versions.tf" ]; then
    echo "terraform/versions.tf not found. Run this from project root."
    exit 1
fi

# Validate Python syntax
print_status "Validating Python syntax..."
find lambda -name "*.py" -exec python3 -m py_compile {} \; 2>/dev/null || {
    echo "Python syntax validation failed"
    exit 1
}
print_success "Python syntax validation passed"

# Check git status
print_status "Checking git status..."
if [ -n "$(git status --porcelain)" ]; then
    print_warning "Uncommitted changes detected"
    git status --short
    echo ""
    read -p "Commit changes? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git add .
        read -p "Commit message: " COMMIT_MESSAGE
        git commit -m "${COMMIT_MESSAGE:-'Deploy MFA backup system'}"
        print_success "Changes committed"
    else
        echo "Please commit changes first"
        exit 1
    fi
fi

# Push to trigger Terraform Cloud
print_status "Pushing to trigger Terraform Cloud deployment..."
git push origin main

print_success "🎉 Deployment initiated!"
echo ""
echo "🔗 Monitor at: https://app.terraform.io/app/YOUR_ORG/workspaces/mfa-backup-system"
echo ""
echo "📋 What happens next:"
echo "1. Terraform Cloud runs plan"
echo "2. Review and approve"
echo "3. Lambda functions deployed"
echo "4. Backup schedule activated"
