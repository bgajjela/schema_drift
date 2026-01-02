"""Tests for NO_DATA payload generation."""

from schema_diff.app import _no_data_payload


def test_no_data_payload_has_status_and_safe_diff() -> None:
    """Ensure NO_DATA payload has safe, empty diffs."""
    contract = {"contract_version": "1.0.0", "columns": []}
    refs = {
        "contract_bucket": "cb",
        "contract_key": "ck",
        "report_bucket": "rb",
        "data_location": "s3://b/prefix/",
    }
    payload = _no_data_payload(
        glue_db="db",
        glue_table="t",
        contract=contract,
        refs=refs,
    )
    assert payload["status"] == "NO_DATA"
    assert payload["diff"]["overall_severity"] == "SAFE"
    assert not payload["diff"]["changes"]
