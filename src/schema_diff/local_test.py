\
import json
from pathlib import Path
from shared.diff import compute_diff

def main():
    contract_path = Path("../../contracts/chicago_public/cpd_parks/v1_2.json")
    contract = json.loads(contract_path.read_text())
    contract_cols = contract["columns"]

    # Simulate drift
    actual_cols = [c.copy() for c in contract_cols]
    # Narrow a type to trigger BREAKING example
    for c in actual_cols:
        if c["name"].lower() == "acres":
            c["type"] = "int"  # pretend narrowing from double -> int
    # Add a new column
    actual_cols.append({"name": "new_field", "type": "string", "nullable": None})

    diff = compute_diff(contract_cols, actual_cols)
    print(json.dumps(diff, indent=2))

if __name__ == "__main__":
    main()
