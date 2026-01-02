import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3

from shared.diff import compute_diff

s3 = boto3.client("s3")
glue = boto3.client("glue")
lambda_client = boto3.client("lambda")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _read_json_s3(bucket: str, key: str) -> Dict[str, Any]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def _write_json_s3(bucket: str, key: str, data: Dict[str, Any]) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
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
    bucket, prefix = _parse_s3_uri(s3_uri)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return bool(resp.get("Contents"))


def _cols_from_contract(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    cols: List[Dict[str, Any]] = []
    for c in contract.get("columns", []):
        cols.append({"Name": c["name"], "Type": c["type"], "Comment": c.get("comment", "")})
    return cols


def _ensure_glue_database(database: str) -> None:
    try:
        glue.get_database(Name=database)
        return
    except glue.exceptions.EntityNotFoundException:
        pass
    glue.create_database(DatabaseInput={"Name": database})


def _serde_for_format(file_format: str) -> Tuple[str, str, Dict[str, Any]]:
    ff = (file_format or "csv").lower().strip()
    if ff == "parquet":
        return (
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "Parameters": {}},
        )
    # default csv
    return (
        "org.apache.hadoop.mapred.TextInputFormat",
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        {
            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "Parameters": {"field.delim": ",", "serialization.format": ","},
        },
    )


def _ensure_glue_table(database: str, table: str, location: str, file_format: str, contract: Dict[str, Any]) -> None:
    try:
        glue.get_table(DatabaseName=database, Name=table)
        return
    except glue.exceptions.EntityNotFoundException:
        pass

    _ensure_glue_database(database)

    input_fmt, output_fmt, serde = _serde_for_format(file_format)
    cols = _cols_from_contract(contract)

    glue.create_table(
        DatabaseName=database,
        TableInput={
            "Name": table,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": (file_format or "csv").lower().strip() or "csv"},
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
    t = glue.get_table(DatabaseName=database, Name=table)
    cols = t["Table"]["StorageDescriptor"]["Columns"]
    return [{"name": c["Name"], "type": c["Type"], "nullable": None} for c in cols]


def _write_diff(report_bucket: str, glue_db: str, glue_table: str, payload: Dict[str, Any]) -> str:
    ts = int(time.time())
    key = f"diffs/{glue_db}.{glue_table}/{ts}.diff.json"
    _write_json_s3(report_bucket, key, payload)
    return key


def _invoke_report_generator(function_name: str, bucket: str, key: str) -> None:
    try:
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="Event",  # async
            Payload=json.dumps({"diff_s3": {"bucket": bucket, "key": key}}).encode("utf-8"),
        )
    except Exception:
        logger.exception("Failed to invoke report generator")


def _no_data_payload(glue_db: str, glue_table: str, contract: Dict[str, Any], contract_bucket: str, contract_key: str, report_bucket: str, data_location: str) -> Dict[str, Any]:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "NO_DATA",
        "table": {"database": glue_db, "name": glue_table},
        "contract_version": contract.get("contract_version"),
        "contract_key": contract_key,
        "contract_bucket": contract_bucket,
        "report_bucket": report_bucket,
        "data_location": data_location,
        "actual_source": "glue",
        "diff": {
            "overall_severity": "SAFE",
            "counts": {"SAFE": 0, "RISKY": 0, "BREAKING": 0},
            "changes": [],
        },
    }


def _error_payload(glue_db: str, glue_table: str, contract_bucket: str, contract_key: str, report_bucket: str, data_location: str, error: str) -> Dict[str, Any]:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "ERROR",
        "table": {"database": glue_db, "name": glue_table},
        "contract_key": contract_key,
        "contract_bucket": contract_bucket,
        "report_bucket": report_bucket,
        "data_location": data_location,
        "error": error,
        "diff": {
            "overall_severity": "SAFE",
            "counts": {"SAFE": 0, "RISKY": 0, "BREAKING": 0},
            "changes": [],
        },
    }


def _run_one(cfg: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    contract_bucket = cfg.get("contract_bucket") or defaults["contract_bucket"]
    contract_key = cfg.get("contract_key") or defaults["contract_key"]
    report_bucket = cfg.get("report_bucket") or defaults["report_bucket"]

    glue_db = cfg.get("glue_database") or defaults["glue_database"]
    glue_table = cfg.get("glue_table") or defaults["glue_table"]

    data_location = cfg.get("data_location") or defaults.get("data_location") or ""
    file_format = cfg.get("file_format") or defaults.get("file_format") or "csv"

    # Try to read contract; on failure write an ERROR payload to S3 so the system is observable.
    try:
        contract_doc = _read_json_s3(contract_bucket, contract_key)
    except Exception as e:
        payload = _error_payload(glue_db, glue_table, contract_bucket, contract_key, report_bucket, data_location, f"{type(e).__name__}: {e}")
        diff_key = _write_diff(report_bucket, glue_db, glue_table, payload)
        return {"table": f"{glue_db}.{glue_table}", "status": "ERROR", "diff_s3": {"bucket": report_bucket, "key": diff_key}}

    # Guardrail: if DataLocation is configured but has no objects, mark NO_DATA and skip drift comparison.
    if data_location:
        try:
            if not _s3_prefix_has_any_objects(data_location):
                payload = _no_data_payload(glue_db, glue_table, contract_doc, contract_bucket, contract_key, report_bucket, data_location)
                diff_key = _write_diff(report_bucket, glue_db, glue_table, payload)
                return {"table": f"{glue_db}.{glue_table}", "status": "NO_DATA", "diff_s3": {"bucket": report_bucket, "key": diff_key}}
        except Exception as e:
            payload = _error_payload(glue_db, glue_table, contract_bucket, contract_key, report_bucket, data_location, f"{type(e).__name__}: {e}")
            diff_key = _write_diff(report_bucket, glue_db, glue_table, payload)
            return {"table": f"{glue_db}.{glue_table}", "status": "ERROR", "diff_s3": {"bucket": report_bucket, "key": diff_key}}

    # Ensure Glue metadata exists if DataLocation provided; otherwise assume it already exists.
    if data_location:
        _ensure_glue_table(glue_db, glue_table, data_location, file_format, contract_doc)

    contract_cols = contract_doc.get("columns", [])
    try:
        actual_cols = _load_glue_schema(glue_db, glue_table)
    except Exception as e:
        payload = _error_payload(glue_db, glue_table, contract_bucket, contract_key, report_bucket, data_location, f"{type(e).__name__}: {e}")
        diff_key = _write_diff(report_bucket, glue_db, glue_table, payload)
        return {"table": f"{glue_db}.{glue_table}", "status": "ERROR", "diff_s3": {"bucket": report_bucket, "key": diff_key}}
    diff_doc = compute_diff(contract_cols, actual_cols)

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
        "table": f"{glue_db}.{glue_table}",
        "status": "OK",
        "overall_severity": diff_doc.get("overall_severity"),
        "diff_s3": {"bucket": report_bucket, "key": diff_key},
        "counts": diff_doc.get("counts", {}),
    }


def _load_registry(bucket: str, key: str) -> List[Dict[str, Any]]:
    reg = _read_json_s3(bucket, key)
    if isinstance(reg, dict) and "tables" in reg and isinstance(reg["tables"], list):
        return reg["tables"]
    if isinstance(reg, list):
        return reg
    raise ValueError("Registry must be a list of table entries or an object with a 'tables' list.")


def lambda_handler(event, context):
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
        except Exception as e:
            # Write a single error diff for visibility
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "ERROR",
                "table": {"database": defaults.get("glue_database") or "unknown", "name": defaults.get("glue_table") or "unknown"},
                "error": f"RegistryLoadError: {type(e).__name__}: {e}",
                "diff": {"overall_severity": "SAFE", "counts": {"SAFE": 0, "RISKY": 0, "BREAKING": 0}, "changes": []},
            }
            diff_key = _write_diff(defaults["report_bucket"], defaults.get("glue_database") or "unknown", defaults.get("glue_table") or "unknown", payload)
            if report_fn:
                _invoke_report_generator(report_fn, defaults["report_bucket"], diff_key)
            return {"statusCode": 200, "mode": "registry", "processed": 0, "results": [{"status": "error", "error": str(e), "diff_s3": {"bucket": defaults["report_bucket"], "key": diff_key}}]}

        for cfg in tables[:max_tables]:
            r = _run_one(cfg, defaults)
            results.append(r)
            if report_fn:
                _invoke_report_generator(report_fn, r["diff_s3"]["bucket"], r["diff_s3"]["key"])

        return {"statusCode": 200, "mode": "registry", "processed": len(results), "results": results}

    # Single-table fallback
    r = _run_one({}, defaults)
    if report_fn:
        _invoke_report_generator(report_fn, r["diff_s3"]["bucket"], r["diff_s3"]["key"])
    return {"statusCode": 200, "mode": "single", **r}
