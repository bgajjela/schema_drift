"""Local smoke test for drift computation."""

import json
from pathlib import Path

from shared.diff import compute_diff


def main() -> None:
    """Run a local diff example against a sample contract."""
    contract_path = Path("../../contracts/chicago_public/cpd_parks/v1_2_1.json")
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    contract_cols = contract["columns"]

    # Simulate drift
    actual_cols = [col.copy() for col in contract_cols]
    # Narrow a type to trigger BREAKING example
    for col in actual_cols:
        if col["name"].lower() == "acres":
            col["type"] = "int"  # pretend narrowing from double -> int
    # Add a new column
    actual_cols.append({"name": "new_field", "type": "string", "nullable": None})

    diff = compute_diff(contract_cols, actual_cols)
    print(json.dumps(diff, indent=2))


if __name__ == "__main__":
    main()
