"""S3 helper utilities."""

# pylint: disable=import-error

import json
from typing import Any, Dict

import boto3

s3 = boto3.client("s3")


def read_json(bucket: str, key: str) -> Dict[str, Any]:
    """Read a JSON object from S3."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))
