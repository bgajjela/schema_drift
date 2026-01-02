"""Compare contract schemas with Glue and emit drift artifacts."""

# pylint: disable=import-error

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from shared.diff import compute_diff

s3 = boto3.client("s3")
glue = boto3.client("glue")
lambda_client = boto3.client("lambda")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _read_json_s3(bucket: str, key: str) -> Dict[str, Any]:
    """Read a JSON document from S3."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def _write_json_s3(bucket: str, key: str, data: Dict[str, Any]) -> None:
    """Write a JSON document to S3."""
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    """Parse an s3:// bucket/prefix URI."""
    # returns (bucket, prefix). Prefix always ends with "/" if non-empty.
    if not uri or not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri!r}")
    without = uri[len("s3://") :]
    parts = without.split("/", 1)
    bucket = parts[0]
    prefix = ""
    if len(parts) == 2:
        prefix = parts[1]
    prefix = prefix.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix


def _s3_prefix_has_any_objects(s3_uri: str) -> bool:
    """Check if an S3 prefix contains any objects."""
    bucket, prefix = _parse_s3_uri(s3_uri)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return bool(resp.get("Contents"))


def _cols_from_contract(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert contract column definitions to Glue column format."""
    cols: List[Dict[str, Any]] = []
    for col in contract.get("columns", []):
        cols.append(
            {
                "Name": col["name"],
                "Type": col["type"],
                "Comment": col.get("comment", ""),
            }
        )
    return cols


def _ensure_glue_database(database: str) -> None:
    """Create the Glue database if it does not exist."""
    try:
        glue.get_database(Name=database)
        return
    except glue.exceptions.EntityNotFoundException:
        pass
    glue.create_database(DatabaseInput={"Name": database})


def _serde_for_format(file_format: str) -> Tuple[str, str, Dict[str, Any]]:
    """Return input/output formats and SerDe for the file format."""
    ff = (file_format or "csv").lower().strip()
    if ff == "parquet":
        serde = {
            "SerializationLibrary": (
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            ),
            "Parameters": {},
        }
        return (
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            serde,
        )
    serde = {
        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        "Parameters": {"field.delim": ",", "serialization.format": ","},
    }
    return (
        "org.apache.hadoop.mapred.TextInputFormat",
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        serde,
    )


def _ensure_glue_table(
    database: str,
    table: str,
    location: str,
    file_format: str,
    contract: Dict[str, Any],
) -> None:
    """Create the Glue table if it does not exist."""
    try:
        glue.get_table(DatabaseName=database, Name=table)
        return
    except glue.exceptions.EntityNotFoundException:
        pass

    _ensure_glue_database(database)

    input_fmt, output_fmt, serde = _serde_for_format(file_format)
    cols = _cols_from_contract(contract)
    classification = (file_format or "csv").lower().strip() or "csv"

    glue.create_table(
        DatabaseName=database,
        TableInput={
            "Name": table,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": classification},
            "StorageDescriptor": {
                "Columns": cols,
                "Location": location,
                "InputFormat": input_fmt,
                "OutputFormat": output_fmt,
                "SerdeInfo": serde,
                "Compressed": False,
                "NumberOfBuckets": -1,
                "StoredAsSubDirectories": False,
            },
        },
    )


def _load_glue_schema(database: str, table: str) -> List[Dict[str, Any]]:
    """Load Glue table columns as generic column dictionaries."""
    table_def = glue.get_table(DatabaseName=database, Name=table)
    cols = table_def["Table"]["StorageDescriptor"]["Columns"]
    return [
        {"name": col["Name"], "type": col["Type"], "nullable": None}
        for col in cols
    ]


def _write_diff(
    report_bucket: str,
    glue_db: str,
    glue_table: str,
    payload: Dict[str, Any],
) -> str:
    """Write diff payload to S3 and return the key."""
    ts = int(time.time())
    key = f"diffs/{glue_db}.{glue_table}/{ts}.diff.json"
    _write_json_s3(report_bucket, key, payload)
    return key


def _invoke_report_generator(function_name: str, bucket: str, key: str) -> None:
    """Invoke the report generator Lambda asynchronously."""
    payload = {"diff_s3": {"bucket": bucket, "key": key}}
    try:
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="Event",  # async
            Payload=json.dumps(payload).encode("utf-8"),
        )
    except (BotoCoreError, ClientError):
        logger.exception("Failed to invoke report generator")


def _no_data_payload(
    glue_db: str,
    glue_table: str,
    contract: Dict[str, Any],
    refs: Dict[str, str],
) -> Dict[str, Any]:
    """Create a NO_DATA payload for empty data prefixes."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "NO_DATA",
        "table": {"database": glue_db, "name": glue_table},
        "contract_version": contract.get("contract_version"),
        "contract_key": refs["contract_key"],
        "contract_bucket": refs["contract_bucket"],
        "report_bucket": refs["report_bucket"],
        "data_location": refs["data_location"],
        "actual_source": "glue",
        "diff": {
            "overall_severity": "SAFE",
            "counts": {"SAFE": 0, "RISKY": 0, "BREAKING": 0},
            "changes": [],
        },
    }


def _error_payload(
    glue_db: str,
    glue_table: str,
    refs: Dict[str, str],
    error: str,
) -> Dict[str, Any]:
    """Create an ERROR payload for reporting failures."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "ERROR",
        "table": {"database": glue_db, "name": glue_table},
        "contract_key": refs["contract_key"],
        "contract_bucket": refs["contract_bucket"],
        "report_bucket": refs["report_bucket"],
        "data_location": refs["data_location"],
        "error": error,
        "diff": {
            "overall_severity": "SAFE",
            "counts": {"SAFE": 0, "RISKY": 0, "BREAKING": 0},
            "changes": [],
        },
    }


def _run_one(cfg: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    """Run drift detection for a single table config."""
    contract_bucket = cfg.get("contract_bucket") or defaults["contract_bucket"]
    contract_key = cfg.get("contract_key") or defaults["contract_key"]
    report_bucket = cfg.get("report_bucket") or defaults["report_bucket"]

    glue_db = cfg.get("glue_database") or defaults["glue_database"]
    glue_table = cfg.get("glue_table") or defaults["glue_table"]

    data_location = cfg.get("data_location") or defaults.get("data_location") or ""
    file_format = cfg.get("file_format") or defaults.get("file_format") or "csv"

    refs = {
        "contract_bucket": contract_bucket,
        "contract_key": contract_key,
        "report_bucket": report_bucket,
        "data_location": data_location,
    }
    table_ref = f"{glue_db}.{glue_table}"

    # Try to read contract; on failure write an ERROR payload to S3.
    try:
        contract_doc = _read_json_s3(contract_bucket, contract_key)
    except (ClientError, BotoCoreError, json.JSONDecodeError) as exc:
        payload = _error_payload(
            glue_db,
            glue_table,
            refs,
            f"{type(exc).__name__}: {exc}",
        )
        diff_key = _write_diff(report_bucket, glue_db, glue_table, payload)
        return {
            "table": table_ref,
            "status": "ERROR",
            "diff_s3": {"bucket": report_bucket, "key": diff_key},
        }

    # Guardrail: if DataLocation is configured but has no objects, skip drift.
    if data_location:
        try:
            if not _s3_prefix_has_any_objects(data_location):
                payload = _no_data_payload(glue_db, glue_table, contract_doc, refs)
                diff_key = _write_diff(report_bucket, glue_db, glue_table, payload)
                return {
                    "table": table_ref,
                    "status": "NO_DATA",
                    "diff_s3": {"bucket": report_bucket, "key": diff_key},
                }
        except (ClientError, BotoCoreError, ValueError) as exc:
            payload = _error_payload(
                glue_db,
                glue_table,
                refs,
                f"{type(exc).__name__}: {exc}",
            )
            diff_key = _write_diff(report_bucket, glue_db, glue_table, payload)
            return {
                "table": table_ref,
                "status": "ERROR",
                "diff_s3": {"bucket": report_bucket, "key": diff_key},
            }

    # Ensure Glue metadata exists if DataLocation provided; otherwise assume it exists.
    if data_location:
        _ensure_glue_table(glue_db, glue_table, data_location, file_format, contract_doc)

    try:
        actual_cols = _load_glue_schema(glue_db, glue_table)
    except (ClientError, BotoCoreError) as exc:
        payload = _error_payload(
            glue_db,
            glue_table,
            refs,
            f"{type(exc).__name__}: {exc}",
        )
        diff_key = _write_diff(report_bucket, glue_db, glue_table, payload)
        return {
            "table": table_ref,
            "status": "ERROR",
            "diff_s3": {"bucket": report_bucket, "key": diff_key},
        }

    diff_doc = compute_diff(contract_doc.get("columns", []), actual_cols)

    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "OK",
        "table": {"database": glue_db, "name": glue_table},
        "contract_version": contract_doc.get("contract_version"),
        "diff": diff_doc,
        "contract_key": contract_key,
        "contract_bucket": contract_bucket,
        "report_bucket": report_bucket,
        "data_location": data_location,
        "actual_source": "glue",
    }

    diff_key = _write_diff(report_bucket, glue_db, glue_table, payload)
    return {
        "table": table_ref,
        "status": "OK",
        "overall_severity": diff_doc.get("overall_severity"),
        "diff_s3": {"bucket": report_bucket, "key": diff_key},
        "counts": diff_doc.get("counts", {}),
    }


def _load_registry(bucket: str, key: str) -> List[Dict[str, Any]]:
    """Load registry list from S3."""
    reg = _read_json_s3(bucket, key)
    if isinstance(reg, dict) and "tables" in reg and isinstance(reg["tables"], list):
        return reg["tables"]
    if isinstance(reg, list):
        return reg
    raise ValueError(
        "Registry must be a list of table entries or an object with a 'tables' list."
    )


def lambda_handler(_event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """Lambda entry point for schema drift checks."""
    defaults = {
        "contract_bucket": os.environ["CONTRACT_BUCKET"],
        "contract_key": os.environ.get("DEFAULT_CONTRACT_KEY", ""),
        "report_bucket": os.environ["REPORT_BUCKET"],
        "glue_database": os.environ.get("DEFAULT_GLUE_DATABASE", ""),
        "glue_table": os.environ.get("DEFAULT_GLUE_TABLE", ""),
        "data_location": os.environ.get("DEFAULT_DATA_LOCATION", ""),
        "file_format": os.environ.get("DEFAULT_FILE_FORMAT", "csv"),
    }

    report_fn = os.environ.get("REPORT_GENERATOR_FUNCTION_NAME", "").strip()

    # Registry mode
    reg_bucket = os.environ.get("REGISTRY_BUCKET", "").strip()
    reg_key = os.environ.get("REGISTRY_KEY", "").strip()
    max_tables = int(os.environ.get("MAX_TABLES_PER_RUN", "50"))

    if reg_bucket and reg_key:
        results: List[Dict[str, Any]] = []
        try:
            tables = _load_registry(reg_bucket, reg_key)
        except (ClientError, BotoCoreError, json.JSONDecodeError, ValueError) as exc:
            error_msg = f"RegistryLoadError: {type(exc).__name__}: {exc}"
            table_info = {
                "database": defaults.get("glue_database") or "unknown",
                "name": defaults.get("glue_table") or "unknown",
            }
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "ERROR",
                "table": table_info,
                "error": error_msg,
                "diff": {
                    "overall_severity": "SAFE",
                    "counts": {"SAFE": 0, "RISKY": 0, "BREAKING": 0},
                    "changes": [],
                },
            }
            diff_key = _write_diff(
                defaults["report_bucket"],
                table_info["database"],
                table_info["name"],
                payload,
            )
            if report_fn:
                _invoke_report_generator(report_fn, defaults["report_bucket"], diff_key)
            error_result = {
                "status": "error",
                "error": str(exc),
                "diff_s3": {"bucket": defaults["report_bucket"], "key": diff_key},
            }
            return {
                "statusCode": 200,
                "mode": "registry",
                "processed": 0,
                "results": [error_result],
            }

        for cfg in tables[:max_tables]:
            result = _run_one(cfg, defaults)
            results.append(result)
            if report_fn:
                _invoke_report_generator(
                    report_fn,
                    result["diff_s3"]["bucket"],
                    result["diff_s3"]["key"],
                )

        return {
            "statusCode": 200,
            "mode": "registry",
            "processed": len(results),
            "results": results,
        }

    # Single-table fallback
    result = _run_one({}, defaults)
    if report_fn:
        _invoke_report_generator(
            report_fn,
            result["diff_s3"]["bucket"],
            result["diff_s3"]["key"],
        )
    return {"statusCode": 200, "mode": "single", **result}
