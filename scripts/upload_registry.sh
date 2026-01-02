#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
: "${REGISTRY_BUCKET:?Set REGISTRY_BUCKET}"
REGISTRY_KEY="${REGISTRY_KEY:-configs/tables.json}"

aws s3 cp "configs/tables.json" "s3://$REGISTRY_BUCKET/$REGISTRY_KEY" --region "$AWS_REGION"
echo "Uploaded registry to s3://$REGISTRY_BUCKET/$REGISTRY_KEY"
