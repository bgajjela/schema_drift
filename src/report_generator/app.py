"""Render deterministic HTML/Markdown schema drift reports."""

# pylint: disable=import-error

import html
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from shared.s3_utils import read_json
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


def _write_text(
    bucket: str,
    key: str,
    text: str,
    content_type: str = "text/plain; charset=utf-8",
) -> None:
    """Write text content to S3."""
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=(text or "").encode("utf-8"),
        ContentType=content_type,
    )


def _list_recent_reports(bucket: str, prefix: str, limit: int = 10) -> List[str]:
    """List recent HTML report keys under a prefix."""
    try:
        objs: List[Dict[str, Any]] = []
        token: str = ""
        while True:
            params = {"Bucket": bucket, "Prefix": prefix}
            if token:
                params["ContinuationToken"] = token
            resp = s3.list_objects_v2(**params)
            objs.extend(resp.get("Contents", []) or [])
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken", "")
            if not token:
                break
        objs.sort(key=lambda obj: obj.get("LastModified"), reverse=True)
        keys = [
            obj["Key"]
            for obj in objs
            if obj.get("Key", "").endswith(".report.html")
        ]
        return keys[:limit]
    except (BotoCoreError, ClientError):
        logger.exception("Failed to list recent reports")
        return []


def _render_report_html(title: str, markdown: str, latest_key: str) -> str:
    """Wrap markdown content in a simple HTML shell."""
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
    body {{
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      padding: 24px;
      max-width: 1000px;
      margin: 0 auto;
    }}
    .top {{ display:flex; gap: 12px; align-items:center; flex-wrap: wrap; }}
    .badge {{
      border: 1px solid #e5e7eb;
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 12px;
    }}
    pre {{
      white-space: pre-wrap;
      word-break: break-word;
      background: #0b1020;
      color: #e6edf3;
      padding: 16px;
      border-radius: 12px;
      overflow: auto;
    }}
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
    """Render the HTML index of reports."""
    latest_href = html.escape(latest_href or "")
    items = []
    for key in recent_items or []:
        safe_key = html.escape(key)
        label = html.escape(key.split("/")[-1])
        items.append(f'<li><a href="/{safe_key}">{label}</a></li>')
    li = "\n".join(items)
    list_markup = li if li else "<li>No recent reports yet.</li>"
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Schema Drift Reports</title>
  <style>
    body {{
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      padding: 24px;
      max-width: 900px;
      margin: 0 auto;
    }}
    .card {{ border: 1px solid #e5e7eb; border-radius: 12px; padding: 16px; }}
    code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 6px; }}
  </style>
</head>
<body>
  <h1>Schema Drift Reports</h1>
  <div class="card">
    <p><strong>Latest report:</strong> <a href="/{latest_href}">{latest_href}</a></p>
    <p style="color:#6b7280;">
      Enable <code>S3 static website hosting</code> (or CloudFront) on this bucket
      to browse.
    </p>
    <h3>Recent reports</h3>
    <ol>
      {list_markup}
    </ol>
  </div>
</body>
</html>
"""


def _payload_context(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize payload fields used for markdown rendering."""
    table = payload.get("table", {})
    db_name = table.get("database", "unknown")
    table_name = table.get("name", "unknown")
    return {
        "timestamp": payload.get("timestamp") or datetime.now(timezone.utc).isoformat(),
        "table_name": f"{db_name}.{table_name}",
        "status": (payload.get("status") or "OK").upper(),
        "data_location": payload.get("data_location"),
        "contract_version": payload.get("contract_version"),
        "diff": payload.get("diff") or {},
        "error": payload.get("error"),
    }


def _overview_lines(ctx: Dict[str, Any]) -> List[str]:
    """Build the overview section lines."""
    lines = [
        "# Overview",
        f"- **Table:** `{ctx['table_name']}`",
        f"- **Timestamp:** `{ctx['timestamp']}`",
        f"- **Status:** `{ctx['status']}`",
    ]
    contract_version = ctx.get("contract_version")
    if contract_version:
        lines.append(f"- **Contract version:** `{contract_version}`")
    data_location = ctx.get("data_location")
    if data_location:
        lines.append(f"- **DataLocation:** `{data_location}`")
    lines.append("")
    return lines


def _md_no_data(ctx: Dict[str, Any]) -> str:
    """Render markdown for a NO_DATA payload."""
    lines = _overview_lines(ctx)
    lines.extend(
        [
            "# Result",
            "No files were found under the configured DataLocation prefix. "
            "Drift check was skipped to avoid confusion.",
            "",
            "## Next steps",
            "1. Verify the S3 prefix is correct (bucket/prefix).",
            "2. Upload at least one data file to that prefix.",
            "3. Re-run the drift check.",
        ]
    )
    return "\n".join(lines)


def _md_error(ctx: Dict[str, Any]) -> str:
    """Render markdown for an ERROR payload."""
    lines = _overview_lines(ctx)
    lines.append("# Result")
    lines.append("An error occurred while running schema drift checks.")
    error = ctx.get("error")
    if error:
        lines.extend(["", "## Error", f"- `{error}`"])
    lines.extend(
        [
            "",
            "## Next steps",
            "1. Check CloudWatch Logs for the failing Lambda.",
            "2. Verify ContractKey/RegistryKey exist in S3 and IAM permissions allow access.",
        ]
    )
    return "\n".join(lines)


def _md_drift(ctx: Dict[str, Any]) -> str:
    """Render markdown for a standard drift payload."""
    lines = _overview_lines(ctx)
    diff = ctx.get("diff") or {}
    overall = diff.get("overall_severity", "UNKNOWN")
    counts = diff.get("counts", {}) or {}
    changes = diff.get("changes", []) or []

    counts_line = (
        f"- **Counts:** SAFE={counts.get('SAFE', 0)}, "
        f"RISKY={counts.get('RISKY', 0)}, "
        f"BREAKING={counts.get('BREAKING', 0)}"
    )

    lines.extend(["# Drift summary", f"- **Overall severity:** `{overall}`", counts_line, ""])

    lines.append("# Changes")
    if not changes:
        lines.append("- No schema changes detected.")
    else:
        for change in changes:
            kind = change.get("kind")
            col = change.get("column")
            sev = change.get("severity")
            rationale = change.get("rationale", "")
            before = change.get("before")
            after = change.get("after")
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
        lines.extend(
            [
                "1. Treat as breaking: notify downstream owners and pause dependent "
                "pipelines if needed.",
                "2. Add a compatibility layer (view/CTAS) or dual-write during migration.",
                "3. Version the contract and communicate a deprecation window.",
            ]
        )
    elif overall == "RISKY":
        lines.extend(
            [
                "1. Review the change and confirm compatibility (especially type changes).",
                "2. Update the contract if the change is expected, or fix ingestion/catalog "
                "if not.",
                "3. Monitor downstream jobs for warnings/failures.",
            ]
        )
    else:
        lines.append("1. No action required beyond routine monitoring.")

    return "\n".join(lines)


def _md_from_payload(payload: Dict[str, Any]) -> str:
    """Render markdown from the report payload."""
    ctx = _payload_context(payload)
    status = ctx["status"]
    if status == "NO_DATA":
        return _md_no_data(ctx)
    if status == "ERROR":
        return _md_error(ctx)
    return _md_drift(ctx)

def _diff_location(event: Dict[str, Any], default_bucket: str) -> Dict[str, str]:
    """Extract diff S3 location from the event."""
    diff_info = event.get("diff_s3", {})
    return {
        "bucket": diff_info.get("bucket", default_bucket),
        "key": diff_info.get("key", ""),
    }


def _report_keys(payload: Dict[str, Any], diff_key: str) -> Dict[str, str]:
    """Build report output keys based on payload and diff key."""
    table = payload.get("table", {})
    db_name = table.get("database", "unknown")
    table_name = table.get("name", "unknown")
    safe_table = f"{db_name}.{table_name}".replace("/", "_")
    run_id = diff_key.split("/")[-1].replace(".diff.json", "")
    return {
        "safe_table": safe_table,
        "report_md_key": f"reports/{safe_table}/{run_id}.report.md",
        "report_html_key": f"reports/{safe_table}/{run_id}.report.html",
        "latest_key": f"reports/{safe_table}/latest.html",
        "prefix": f"reports/{safe_table}/",
    }


def _write_report_artifacts(
    report_bucket: str,
    keys: Dict[str, str],
    markdown: str,
) -> None:
    """Write report markdown/html plus index to S3."""
    _write_text(
        report_bucket,
        keys["report_md_key"],
        markdown,
        content_type="text/markdown; charset=utf-8",
    )
    html_doc = _render_report_html(
        title=f"{keys['safe_table']} drift report",
        markdown=markdown,
        latest_key=keys["latest_key"],
    )
    _write_text(
        report_bucket,
        keys["report_html_key"],
        html_doc,
        content_type="text/html; charset=utf-8",
    )
    _write_text(
        report_bucket,
        keys["latest_key"],
        html_doc,
        content_type="text/html; charset=utf-8",
    )

    recent = _list_recent_reports(report_bucket, prefix=keys["prefix"])
    index_html = _render_index_html(
        latest_href=keys["latest_key"],
        recent_items=recent,
    )
    _write_text(
        report_bucket,
        "index.html",
        index_html,
        content_type="text/html; charset=utf-8",
    )


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """Lambda entry point for generating reports."""
    report_bucket = os.environ["REPORT_BUCKET"]

    diff_loc = _diff_location(event, report_bucket)
    diff_bucket = diff_loc["bucket"]
    diff_key = diff_loc["key"]
    if not diff_key:
        return {
            "statusCode": 400,
            "message": "Missing event.diff_s3.key (diff file to analyze).",
        }

    payload = read_json(diff_bucket, diff_key)
    md = _md_from_payload(payload)
    keys = _report_keys(payload, diff_key)
    _write_report_artifacts(report_bucket, keys, md)

    return {
        "statusCode": 200,
        "report_s3": {"bucket": report_bucket, "key": keys["report_md_key"]},
        "latest_html": {"bucket": report_bucket, "key": keys["latest_key"]},
    }
