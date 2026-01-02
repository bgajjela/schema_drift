import json
import os
import html
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


def _read_json(bucket: str, key: str) -> Dict[str, Any]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def _write_text(bucket: str, key: str, text: str, content_type: str = "text/plain; charset=utf-8") -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=(text or "").encode("utf-8"), ContentType=content_type)


def _list_recent_reports(bucket: str, prefix: str, limit: int = 10) -> List[str]:
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        objs = resp.get("Contents", []) or []
        objs.sort(key=lambda o: o.get("LastModified"), reverse=True)
        keys = [o["Key"] for o in objs if o.get("Key", "").endswith(".report.html")]
        return keys[:limit]
    except Exception:
        logger.exception("Failed to list recent reports")
        return []


def _render_report_html(title: str, markdown: str, latest_key: str) -> str:
    safe_md = html.escape(markdown or "")
    safe_title = html.escape(title or "Schema Drift Report")
    safe_latest = html.escape(latest_key or "")
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>{safe_title}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; padding: 24px; max-width: 1000px; margin: 0 auto; }}
    .top {{ display:flex; gap: 12px; align-items:center; flex-wrap: wrap; }}
    .badge {{ border: 1px solid #e5e7eb; border-radius: 999px; padding: 4px 10px; font-size: 12px; }}
    pre {{ white-space: pre-wrap; word-break: break-word; background: #0b1020; color: #e6edf3; padding: 16px; border-radius: 12px; overflow:auto; }}
    a {{ text-decoration: none; }}
  </style>
</head>
<body>
  <div class="top">
    <h1 style="margin:0;">Schema Drift Report</h1>
    <span class="badge"><a href="/{safe_latest}">latest</a></span>
  </div>
  <p style="color:#6b7280;">Deterministic report.</p>
  <pre>{safe_md}</pre>
</body>
</html>
"""


def _render_index_html(latest_href: str, recent_items: List[str]) -> str:
    latest_href = html.escape(latest_href or "")
    li = "\n".join(
        [f'<li><a href="/{html.escape(x)}">{html.escape(x.split("/")[-1])}</a></li>' for x in (recent_items or [])]
    )
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Schema Drift Reports</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; padding: 24px; max-width: 900px; margin: 0 auto; }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 12px; padding: 16px; }}
    code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 6px; }}
  </style>
</head>
<body>
  <h1>Schema Drift Reports</h1>
  <div class="card">
    <p><strong>Latest report:</strong> <a href="/{latest_href}">{latest_href}</a></p>
    <p style="color:#6b7280;">Enable <code>S3 static website hosting</code> (or CloudFront) on this bucket to browse.</p>
    <h3>Recent reports</h3>
    <ol>
      {li if li else "<li>No recent reports yet.</li>"}
    </ol>
  </div>
</body>
</html>
"""


def _md_from_payload(payload: Dict[str, Any]) -> str:
    ts = payload.get("timestamp") or datetime.now(timezone.utc).isoformat()
    table = payload.get("table", {})
    table_name = f"{table.get('database','unknown')}.{table.get('name','unknown')}"
    status = (payload.get("status") or "OK").upper()
    data_location = payload.get("data_location")
    contract_version = payload.get("contract_version")
    diff = payload.get("diff") or {}
    overall = diff.get("overall_severity", "UNKNOWN")
    counts = diff.get("counts", {}) or {}
    changes = diff.get("changes", []) or []
    error = payload.get("error")

    lines: List[str] = []
    lines.append("# Overview")
    lines.append(f"- **Table:** `{table_name}`")
    lines.append(f"- **Timestamp:** `{ts}`")
    lines.append(f"- **Status:** `{status}`")
    if contract_version:
        lines.append(f"- **Contract version:** `{contract_version}`")
    if data_location:
        lines.append(f"- **DataLocation:** `{data_location}`")
    lines.append("")

    if status == "NO_DATA":
        lines.append("# Result")
        lines.append("No files were found under the configured DataLocation prefix. Drift check was skipped to avoid confusion.")
        lines.append("")
        lines.append("## Next steps")
        lines.append("1. Verify the S3 prefix is correct (bucket/prefix).")
        lines.append("2. Upload at least one data file to that prefix.")
        lines.append("3. Re-run the drift check.")
        return "\n".join(lines)

    if status == "ERROR":
        lines.append("# Result")
        lines.append("An error occurred while running schema drift checks.")
        if error:
            lines.append("")
            lines.append("## Error")
            lines.append(f"- `{error}`")
        lines.append("")
        lines.append("## Next steps")
        lines.append("1. Check CloudWatch Logs for the failing Lambda.")
        lines.append("2. Verify ContractKey/RegistryKey exist in S3 and IAM permissions allow access.")
        return "\n".join(lines)

    lines.append("# Drift summary")
    lines.append(f"- **Overall severity:** `{overall}`")
    lines.append(f"- **Counts:** SAFE={counts.get('SAFE',0)}, RISKY={counts.get('RISKY',0)}, BREAKING={counts.get('BREAKING',0)}")
    lines.append("")

    lines.append("# Changes")
    if not changes:
        lines.append("- No schema changes detected.")
    else:
        for ch in changes:
            kind = ch.get("kind")
            col = ch.get("column")
            sev = ch.get("severity")
            rationale = ch.get("rationale", "")
            before = ch.get("before")
            after = ch.get("after")
            lines.append(f"- **{sev}** `{kind}` on `{col}`")
            if before is not None:
                lines.append(f"  - before: `{before}`")
            if after is not None:
                lines.append(f"  - after: `{after}`")
            if rationale:
                lines.append(f"  - rationale: {rationale}")

    lines.append("")
    lines.append("# Recommended actions")
    if overall == "BREAKING":
        lines.append("1. Treat as breaking: notify downstream owners and pause dependent pipelines if needed.")
        lines.append("2. Add a compatibility layer (view/CTAS) or dual-write during migration.")
        lines.append("3. Version the contract and communicate a deprecation window.")
    elif overall == "RISKY":
        lines.append("1. Review the change and confirm compatibility (especially type changes).")
        lines.append("2. Update the contract if the change is expected, or fix ingestion/catalog if not.")
        lines.append("3. Monitor downstream jobs for warnings/failures.")
    else:
        lines.append("1. No action required beyond routine monitoring.")

    return "\n".join(lines)


def lambda_handler(event, context):
    report_bucket = os.environ["REPORT_BUCKET"]

    diff_bucket = event.get("diff_s3", {}).get("bucket", report_bucket)
    diff_key = event.get("diff_s3", {}).get("key")
    if not diff_key:
        return {"statusCode": 400, "message": "Missing event.diff_s3.key (diff file to analyze)."}

    payload = _read_json(diff_bucket, diff_key)

    md = _md_from_payload(payload)

    table = payload.get("table", {})
    safe_table = f"{table.get('database','unknown')}.{table.get('name','unknown')}".replace("/", "_")
    run_id = diff_key.split("/")[-1].replace(".diff.json", "")

    report_md_key = f"reports/{safe_table}/{run_id}.report.md"
    report_html_key = f"reports/{safe_table}/{run_id}.report.html"
    latest_key = f"reports/{safe_table}/latest.html"

    _write_text(report_bucket, report_md_key, md, content_type="text/markdown; charset=utf-8")
    html_doc = _render_report_html(title=f"{safe_table} drift report", markdown=md, latest_key=latest_key)
    _write_text(report_bucket, report_html_key, html_doc, content_type="text/html; charset=utf-8")
    _write_text(report_bucket, latest_key, html_doc, content_type="text/html; charset=utf-8")

    recent = _list_recent_reports(report_bucket, prefix=f"reports/{safe_table}/")
    index_html = _render_index_html(latest_href=latest_key, recent_items=recent)
    _write_text(report_bucket, "index.html", index_html, content_type="text/html; charset=utf-8")

    return {
        "statusCode": 200,
        "report_s3": {"bucket": report_bucket, "key": report_md_key},
        "latest_html": {"bucket": report_bucket, "key": latest_key},
    }
