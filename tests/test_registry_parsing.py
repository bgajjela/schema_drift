import json
from schema_diff.app import _parse_s3_uri

def test_parse_s3_uri_basic():
    b, p = _parse_s3_uri("s3://my-bucket/path/to/data")
    assert b == "my-bucket"
    assert p == "path/to/data/"

def test_parse_s3_uri_root():
    b, p = _parse_s3_uri("s3://my-bucket")
    assert b == "my-bucket"
    assert p == ""
