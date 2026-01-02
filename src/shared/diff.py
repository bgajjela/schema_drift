import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any

_NUMERIC_ORDER = ["tinyint", "smallint", "int", "bigint", "float", "double", "decimal", "string"]

def _base_type(t: str) -> Tuple[str, Optional[Tuple[int,int]]]:
    t = (t or "").strip().lower()
    m = re.match(r"^(decimal)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$", t)
    if m:
        return "decimal", (int(m.group(2)), int(m.group(3)))
    return t, None

def _numeric_rank(base: str) -> Optional[int]:
    if base in _NUMERIC_ORDER:
        return _NUMERIC_ORDER.index(base)
    return None

def type_change_severity(old: str, new: str) -> Tuple[str, str]:
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

    old_r = _numeric_rank(old_base)
    new_r = _numeric_rank(new_base)
    if old_r is not None and new_r is not None:
        if new_r > old_r:
            if new_base == "string":
                return "RISKY", f"Changed from numeric '{old}' to string '{new}'."
            return "RISKY", f"Widened type from '{old}' to '{new}'."
        return "BREAKING", f"Narrowed type from '{old}' to '{new}'."

    return "RISKY", f"Type changed from '{old}' to '{new}' (unknown compatibility)."

@dataclass
class Column:
    name: str
    type: str
    nullable: Optional[bool] = None
    comment: Optional[str] = None
    tags: Optional[List[str]] = None

def columns_by_name(cols: List[Dict[str, Any]]) -> Dict[str, Column]:
    out: Dict[str, Column] = {}
    for c in cols:
        out[c["name"].lower()] = Column(
            name=c["name"],
            type=c.get("type", ""),
            nullable=c.get("nullable", None),
            comment=c.get("comment", None),
            tags=c.get("tags", None),
        )
    return out

def compute_diff(contract_cols: List[Dict[str, Any]], actual_cols: List[Dict[str, Any]]) -> Dict[str, Any]:
    c_map = columns_by_name(contract_cols)
    a_map = columns_by_name(actual_cols)

    changes: List[Dict[str, Any]] = []

    for name_lc, ccol in c_map.items():
        if name_lc not in a_map:
            changes.append({
                "kind": "REMOVE_COLUMN",
                "column": ccol.name,
                "before": {"type": ccol.type, "nullable": ccol.nullable},
                "after": None,
                "severity": "BREAKING",
                "rationale": "Column present in contract but missing in actual schema."
            })
            continue

        acol = a_map[name_lc]

        if (ccol.type or "").strip().lower() != (acol.type or "").strip().lower():
            sev, why = type_change_severity(ccol.type, acol.type)
            changes.append({
                "kind": "TYPE_CHANGE",
                "column": ccol.name,
                "before": {"type": ccol.type, "nullable": ccol.nullable},
                "after": {"type": acol.type, "nullable": acol.nullable},
                "severity": sev,
                "rationale": why
            })

        if ccol.nullable is not None and acol.nullable is not None and ccol.nullable != acol.nullable:
            if ccol.nullable and (acol.nullable is False):
                changes.append({
                    "kind": "NULLABILITY_CHANGE",
                    "column": ccol.name,
                    "before": {"nullable": ccol.nullable},
                    "after": {"nullable": acol.nullable},
                    "severity": "BREAKING",
                    "rationale": "Column became non-nullable."
                })
            else:
                changes.append({
                    "kind": "NULLABILITY_CHANGE",
                    "column": ccol.name,
                    "before": {"nullable": ccol.nullable},
                    "after": {"nullable": acol.nullable},
                    "severity": "RISKY",
                    "rationale": "Nullability changed."
                })

    for name_lc, acol in a_map.items():
        if name_lc not in c_map:
            sev = "SAFE" if (acol.nullable is True or acol.nullable is None) else "RISKY"
            rationale = "New column added (nullable/unknown)."
            if acol.nullable is False:
                rationale = "New non-nullable column added."
            changes.append({
                "kind": "ADD_COLUMN",
                "column": acol.name,
                "before": None,
                "after": {"type": acol.type, "nullable": acol.nullable},
                "severity": sev,
                "rationale": rationale
            })

    counts = {"SAFE": 0, "RISKY": 0, "BREAKING": 0}
    for ch in changes:
        counts[ch["severity"]] += 1

    overall = "SAFE"
    if counts["BREAKING"] > 0:
        overall = "BREAKING"
    elif counts["RISKY"] > 0:
        overall = "RISKY"

    return {"overall_severity": overall, "counts": counts, "changes": changes}
