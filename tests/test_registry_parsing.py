"""Tests for S3 URI parsing."""

from schema_diff.app import _parse_s3_uri


def test_parse_s3_uri_basic() -> None:
    """Parse bucket and prefix from S3 URIs."""
    bucket, prefix = _parse_s3_uri("s3://my-bucket/path/to/data")
    assert bucket == "my-bucket"
    assert prefix == "path/to/data/"


def test_parse_s3_uri_root() -> None:
    """Parse root bucket URIs without a prefix."""
    bucket, prefix = _parse_s3_uri("s3://my-bucket")
    assert bucket == "my-bucket"
    assert prefix == ""
