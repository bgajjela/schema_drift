"""Schema diff helpers for contract and actual column comparisons."""

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

_NUMERIC_ORDER = [
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "float",
    "double",
    "decimal",
    "string",
]


def _base_type(t: str) -> Tuple[str, Optional[Tuple[int, int]]]:
    """Return base type and decimal precision/scale if present."""
    raw = (t or "").strip().lower()
    match = re.match(r"^(decimal)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$", raw)
    if match:
        return "decimal", (int(match.group(2)), int(match.group(3)))
    return raw, None


def _numeric_rank(base: str) -> Optional[int]:
    """Return numeric type order index if known."""
    if base in _NUMERIC_ORDER:
        return _NUMERIC_ORDER.index(base)
    return None


def type_change_severity(old: str, new: str) -> Tuple[str, str]:
    """Classify type changes into SAFE, RISKY, or BREAKING."""
    old_base, old_dec = _base_type(old)
    new_base, new_dec = _base_type(new)

    if old_base == new_base == "decimal" and old_dec and new_dec:
        old_p, old_s = old_dec
        new_p, new_s = new_dec
        if new_p >= old_p and new_s >= old_s:
            return "RISKY", f"Decimal widened from {old} to {new}."
        return "BREAKING", f"Decimal narrowed from {old} to {new}."

    if old_base == new_base:
        return "SAFE", f"Type unchanged base '{old_base}'."

    old_rank = _numeric_rank(old_base)
    new_rank = _numeric_rank(new_base)
    if old_rank is not None and new_rank is not None:
        if new_rank > old_rank:
            if new_base == "string":
                rationale = f"Changed from numeric '{old}' to string '{new}'."
            else:
                rationale = f"Widened type from '{old}' to '{new}'."
            return "RISKY", rationale
        return "BREAKING", f"Narrowed type from '{old}' to '{new}'."

    return "RISKY", f"Type changed from '{old}' to '{new}' (unknown compatibility)."


@dataclass
class Column:
    """Normalized column metadata."""

    name: str
    type: str
    nullable: Optional[bool] = None
    comment: Optional[str] = None
    tags: Optional[List[str]] = None


def columns_by_name(cols: List[Dict[str, Any]]) -> Dict[str, Column]:
    """Build a name-keyed column mapping."""
    out: Dict[str, Column] = {}
    for col in cols:
        out[col["name"].lower()] = Column(
            name=col["name"],
            type=col.get("type", ""),
            nullable=col.get("nullable"),
            comment=col.get("comment"),
            tags=col.get("tags"),
        )
    return out


def compute_diff(
    contract_cols: List[Dict[str, Any]],
    actual_cols: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Compute drift between contract columns and actual columns."""
    contract_map = columns_by_name(contract_cols)
    actual_map = columns_by_name(actual_cols)

    changes: List[Dict[str, Any]] = []

    for name_lc, contract_col in contract_map.items():
        if name_lc not in actual_map:
            changes.append(
                {
                    "kind": "REMOVE_COLUMN",
                    "column": contract_col.name,
                    "before": {
                        "type": contract_col.type,
                        "nullable": contract_col.nullable,
                    },
                    "after": None,
                    "severity": "BREAKING",
                    "rationale": (
                        "Column present in contract but missing in actual schema."
                    ),
                }
            )
            continue

        actual_col = actual_map[name_lc]
        contract_type = (contract_col.type or "").strip().lower()
        actual_type = (actual_col.type or "").strip().lower()

        if contract_type != actual_type:
            severity, rationale = type_change_severity(
                contract_col.type, actual_col.type
            )
            changes.append(
                {
                    "kind": "TYPE_CHANGE",
                    "column": contract_col.name,
                    "before": {
                        "type": contract_col.type,
                        "nullable": contract_col.nullable,
                    },
                    "after": {
                        "type": actual_col.type,
                        "nullable": actual_col.nullable,
                    },
                    "severity": severity,
                    "rationale": rationale,
                }
            )

        nullable_changed = (
            contract_col.nullable is not None
            and actual_col.nullable is not None
            and contract_col.nullable != actual_col.nullable
        )
        if nullable_changed:
            if contract_col.nullable and (actual_col.nullable is False):
                changes.append(
                    {
                        "kind": "NULLABILITY_CHANGE",
                        "column": contract_col.name,
                        "before": {"nullable": contract_col.nullable},
                        "after": {"nullable": actual_col.nullable},
                        "severity": "BREAKING",
                        "rationale": "Column became non-nullable.",
                    }
                )
            else:
                changes.append(
                    {
                        "kind": "NULLABILITY_CHANGE",
                        "column": contract_col.name,
                        "before": {"nullable": contract_col.nullable},
                        "after": {"nullable": actual_col.nullable},
                        "severity": "RISKY",
                        "rationale": "Nullability changed.",
                    }
                )

    for name_lc, actual_col in actual_map.items():
        if name_lc not in contract_map:
            is_nullable = actual_col.nullable is True or actual_col.nullable is None
            severity = "SAFE" if is_nullable else "RISKY"
            rationale = "New column added (nullable/unknown)."
            if actual_col.nullable is False:
                rationale = "New non-nullable column added."
            changes.append(
                {
                    "kind": "ADD_COLUMN",
                    "column": actual_col.name,
                    "before": None,
                    "after": {"type": actual_col.type, "nullable": actual_col.nullable},
                    "severity": severity,
                    "rationale": rationale,
                }
            )

    counts = {"SAFE": 0, "RISKY": 0, "BREAKING": 0}
    for change in changes:
        counts[change["severity"]] += 1

    overall = "SAFE"
    if counts["BREAKING"] > 0:
        overall = "BREAKING"
    elif counts["RISKY"] > 0:
        overall = "RISKY"

    return {"overall_severity": overall, "counts": counts, "changes": changes}
