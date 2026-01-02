#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
: "${CONTRACT_BUCKET:?Set CONTRACT_BUCKET}"

aws s3 cp "contracts/chicago_public/cpd_parks/v1_2_1.json" "s3://$CONTRACT_BUCKET/contracts/chicago_public/cpd_parks/v1_2_1.json" --region "$AWS_REGION"
aws s3 cp "contracts/chicago_public/cps_schools/v1_0_0.json" "s3://$CONTRACT_BUCKET/contracts/chicago_public/cps_schools/v1_0_0.json" --region "$AWS_REGION"

echo "Uploaded contracts to s3://$CONTRACT_BUCKET/contracts/..."
