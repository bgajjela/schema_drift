from schema_diff.app import _no_data_payload

def test_no_data_payload_has_status_and_safe_diff():
    contract = {"contract_version": "1.0.0", "columns": []}
    payload = _no_data_payload(
        glue_db="db",
        glue_table="t",
        contract=contract,
        contract_bucket="cb",
        contract_key="ck",
        report_bucket="rb",
        data_location="s3://b/prefix/"
    )
    assert payload["status"] == "NO_DATA"
    assert payload["diff"]["overall_severity"] == "SAFE"
    assert payload["diff"]["changes"] == []
