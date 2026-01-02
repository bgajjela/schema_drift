#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
: "${CONTRACT_BUCKET:?Set CONTRACT_BUCKET}"
: "${CONTRACT_PATH:?Set CONTRACT_PATH (e.g., contracts/chicago_public/cpd_parks/v1_2_1.json)}"

aws s3 cp "$CONTRACT_PATH" "s3://$CONTRACT_BUCKET/$CONTRACT_PATH" --region "$AWS_REGION"
echo "Uploaded $CONTRACT_PATH to s3://$CONTRACT_BUCKET/$CONTRACT_PATH"
