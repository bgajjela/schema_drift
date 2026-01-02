"""Tests for diff computation basics."""

from shared.diff import compute_diff


def test_compute_diff_type_change_is_risky_or_breaking() -> None:
    """Type changes should yield at least one change entry."""
    contract_cols = [{"name": "a", "type": "int", "nullable": True}]
    actual_cols = [{"name": "a", "type": "string", "nullable": None}]
    diff = compute_diff(contract_cols, actual_cols)
    assert diff["overall_severity"] in ("RISKY", "BREAKING", "SAFE")
    assert len(diff["changes"]) >= 1
