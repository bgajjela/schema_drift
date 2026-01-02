# AWS Schema Drift Monitor (PoC)

Deterministic schema-drift monitor that compares contract schemas (JSON in S3) against AWS Glue Data Catalog schemas, writes diff artifacts to S3, and produces HTML + Markdown reports. This pattern scales to larger environments because it operates on metadata, not data files.

## Table of contents

- [Overview](#overview)
- [How it works](#how-it-works)
- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Deployment (AWS CLI only)](#deployment-aws-cli-only)
- [Upload contracts and registry](#upload-contracts-and-registry)
- [Run on-demand](#run-on-demand)
- [Outputs](#outputs)
- [Requirements](#requirements)
- [IAM and permissions](#iam-and-permissions)
- [Developer workflow](#developer-workflow)

## Overview

This project monitors drift by comparing a versioned contract to the current Glue schema per table. It supports single-table mode and registry-driven multi-table monitoring.

## How it works

1. Load contract JSON from S3.
2. Guardrail: check `data_location` prefix in S3. If no files exist, mark `NO_DATA` (not drift) and write a report.
3. Ensure Glue DB/table exist (create if missing) using the contract.
4. Read Glue schema and compute drift vs contract.
5. Write a diff JSON to S3.
6. Invoke `ReportGeneratorFunction` asynchronously to render:
   - `reports/<db>.<table>/<run>.report.md`
   - `reports/<db>.<table>/<run>.report.html`
   - `reports/<db>.<table>/latest.html`
   - `index.html`

## Architecture

- Contracts stored in S3 (JSON).
- `SchemaDiffFunction` Lambda reads contracts, compares with Glue, and writes diffs.
- `ReportGeneratorFunction` Lambda renders deterministic HTML/Markdown from diff artifacts.
- S3 hosts diffs and reports for audit and review.

## Repository layout

- `src/schema_diff/` - Lambda that computes diffs and writes artifacts.
- `src/report_generator/` - Lambda that renders deterministic HTML reports.
- `src/shared/` - Shared logic (single source of truth for diff rules).
- `contracts/` - Example contracts (parks + schools).
- `configs/tables.json` - Registry (Pattern A) for multi-table monitoring.
- `docs/` - Deployment and IAM prerequisites.

## Prerequisites

- AWS CLI configured with credentials for your account.
- S3 bucket(s) for contracts and reports (can be the same bucket).
- Permissions required for CloudFormation, IAM, Lambda, EventBridge, Glue, and S3 (see `docs/IAM_PREREQS.md`).

## Configuration

Single-table mode (defaults):
- `ContractBucket`
- `ReportBucket`

Multi-table mode (registry-driven):
- `RegistryBucket`
- `RegistryKey` (defaults to `configs/tables.json`)
- `MaxTablesPerRun`

## Deployment (AWS CLI only)

1) Package

```bash
aws cloudformation package --region us-east-1 --template-file template.yaml --s3-bucket YOUR_BUCKET --output-template-file packaged.yaml
```

2) Deploy

Single-table mode:

```bash
aws cloudformation deploy --region us-east-1 --stack-name schema-drift-stack --template-file packaged.yaml --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM --parameter-overrides ContractBucket=YOUR_BUCKET ReportBucket=YOUR_BUCKET
```

Multi-table mode:

```bash
aws cloudformation deploy --region us-east-1 --stack-name schema-drift-stack --template-file packaged.yaml --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM --parameter-overrides ContractBucket=YOUR_BUCKET ReportBucket=YOUR_BUCKET RegistryBucket=YOUR_BUCKET RegistryKey=configs/tables.json MaxTablesPerRun=50
```

## Upload contracts and registry

Upload all included contracts:

```bash
./scripts/upload_contracts_all.sh
```

Upload registry:

```bash
./scripts/upload_registry.sh
```

## Run on-demand

```bash
aws lambda invoke --region us-east-1 --function-name <SchemaDiffFunctionPhysicalName> --payload '{}' /tmp/schema_out.json
cat /tmp/schema_out.json
```

## Outputs

- Diffs: `diffs/<db>.<table>/*.diff.json`
- Reports: `reports/<db>.<table>/*.report.html` and `reports/<db>.<table>/latest.html`
- Index: `index.html`

## Requirements

Functional:
- Compare Glue catalog schema to a versioned contract per table.
- Classify changes into SAFE, RISKY, BREAKING.
- Produce human-readable reports in HTML/Markdown.
- Registry-driven multi-table monitoring (single scheduled run).

Non-functional:
- Deterministic output (no dependency on access/quotas).
- Low cost (metadata-only; no data scans).
- Clear failure modes (`ERROR` and `NO_DATA` reports).
- Least-privilege IAM for S3, Glue, Lambda invoke.

## IAM and permissions

See `docs/IAM_PREREQS.md` for required AWS services, deployment permissions, and bucket policy guidance.

## Developer workflow

```bash
make build
make deploy
make localtest
```
