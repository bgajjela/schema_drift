# IAM & Prerequisites (Outside the bundle)

This project is deployed with **CloudFormation** and runs on **Lambda + EventBridge + Glue + S3**.
The CloudFormation template in this repo creates the **runtime IAM roles** for the Lambdas, but your *deployment identity*
(you, your CI role, etc.) must have permissions to create/update those resources.

## Required prerequisites

- AWS account with access to the target region (example: `us-east-1`)
- AWS CLI v2 configured (`aws sts get-caller-identity` works)
- One S3 bucket for CloudFormation packaging artifacts (**required**)  
  You can reuse the same bucket for contracts/reports/registry/data for a PoC.

### Required S3 objects (at runtime)

- Contracts (JSON): `s3://<contract-bucket>/<contract-key>`
- Registry (JSON): `s3://<registry-bucket>/<registry-key>`
- Data prefixes: `s3://<data-bucket>/<prefix>/...` (at least 1 object per prefix to avoid `NO_DATA`)

## IAM: deployment identity permissions

For a quick PoC, the easiest path is using an identity with:

- `AdministratorAccess` (temporary), **or**
- a combination of AWS managed policies:
  - `AWSCloudFormationFullAccess`
  - `AWSLambda_FullAccess`
  - `AmazonS3FullAccess` (or bucket-scoped)
  - `AWSGlueConsoleFullAccess`
  - `AmazonEventBridgeFullAccess`
  - `CloudWatchLogsFullAccess`
  - plus the ability to create/pass roles (see below)

In stricter environments (SCPs / permission boundaries), ensure you are allowed to:
- create IAM roles and inline policies
- pass the created roles to Lambda (`iam:PassRole`)
- create EventBridge rules/targets
- create Glue databases/tables

### Example: minimal-ish *custom* deploy policy (starting point)

This is not perfectly minimal, but it’s a practical starting point you can tighten after deployment.
Replace placeholders like `<ACCOUNT_ID>`, `<REGION>`, `<ARTIFACT_BUCKET>`, and `<STACK_NAME>`.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CloudFormation",
      "Effect": "Allow",
      "Action": [
        "cloudformation:CreateChangeSet",
        "cloudformation:CreateStack",
        "cloudformation:DeleteChangeSet",
        "cloudformation:DeleteStack",
        "cloudformation:Describe*",
        "cloudformation:ExecuteChangeSet",
        "cloudformation:Get*",
        "cloudformation:List*",
        "cloudformation:UpdateStack",
        "cloudformation:ValidateTemplate"
      ],
      "Resource": "*"
    },
    {
      "Sid": "S3ForPackaging",
      "Effect": "Allow",
      "Action": [
        "s3:AbortMultipartUpload",
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:PutObject",
        "s3:PutObjectTagging"
      ],
      "Resource": [
        "arn:aws:s3:::<ARTIFACT_BUCKET>",
        "arn:aws:s3:::<ARTIFACT_BUCKET>/*"
      ]
    },
    {
      "Sid": "LambdaAndLogs",
      "Effect": "Allow",
      "Action": [
        "lambda:*",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EventBridge",
      "Effect": "Allow",
      "Action": [
        "events:*"
      ],
      "Resource": "*"
    },
    {
      "Sid": "Glue",
      "Effect": "Allow",
      "Action": [
        "glue:*"
      ],
      "Resource": "*"
    },
    {
      "Sid": "IAMForStackRoles",
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole",
        "iam:DeleteRole",
        "iam:GetRole",
        "iam:PassRole",
        "iam:PutRolePolicy",
        "iam:DeleteRolePolicy",
        "iam:AttachRolePolicy",
        "iam:DetachRolePolicy",
        "iam:TagRole",
        "iam:UntagRole"
      ],
      "Resource": "*"
    }
  ]
}
```

> If your org requires a **permissions boundary** for created roles, update the template to apply it (common in enterprises).

## IAM: runtime (Lambda execution) S3 access

The template creates the Lambda execution roles with permissions to:
- read contracts + registry from S3
- read Glue catalog
- write diffs and HTML reports to the report bucket
- (optional) create Glue DB/table if missing

If your S3 buckets have restrictive **bucket policies** (or are cross-account), you may also need to add a bucket policy that allows the Lambda roles to access required prefixes.

### Example bucket policy fragment (same account; adjust role ARN)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowDriftLambdas",
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::<ACCOUNT_ID>:role/<STACK_NAME>-*" },
      "Action": ["s3:GetObject","s3:ListBucket","s3:PutObject"],
      "Resource": [
        "arn:aws:s3:::<BUCKET>",
        "arn:aws:s3:::<BUCKET>/*"
      ]
    }
  ]
}
```

## Troubleshooting quick checks

- Confirm credentials:
  - `aws sts get-caller-identity`
- Confirm required objects exist:
  - `aws s3 ls s3://<contract-bucket>/<contract-key>`
  - `aws s3 ls s3://<registry-bucket>/<registry-key>`
- Confirm data exists (avoids `NO_DATA`):
  - `aws s3 ls s3://<data-bucket>/<prefix>/ --recursive | head`
