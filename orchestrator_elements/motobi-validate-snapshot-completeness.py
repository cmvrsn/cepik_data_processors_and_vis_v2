import json
import logging
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

REPORT_BUCKET = os.getenv("REPORT_BUCKET", "motointel-cepik-raw-prod")
REPORT_PREFIX = os.getenv("REPORT_PREFIX", "reports")
APPROVAL_PREFIX = os.getenv("APPROVAL_PREFIX", "reports")
ALLOW_MANUAL_APPROVE = os.getenv("ALLOW_MANUAL_APPROVE", "true").strip().lower() in {"1", "true", "yes"}


def _read_json(bucket: str, key: str):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())


def _exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def lambda_handler(event, context):
    snapshot_date = (event or {}).get("snapshot_date") or datetime.utcnow().strftime("%Y-%m-%d-%H%M")
    bucket = (event or {}).get("s3_bucket") or REPORT_BUCKET

    completeness_key = f"{REPORT_PREFIX}/completeness-{snapshot_date}.json"
    approval_key = f"{APPROVAL_PREFIX}/completeness-approved-{snapshot_date}.json"

    logger.info(f"[COMPLETENESS] snapshot_date={snapshot_date} bucket={bucket}")
    logger.info(f"[COMPLETENESS] reading s3://{bucket}/{completeness_key}")

    try:
        completeness = _read_json(bucket, completeness_key)
    except ClientError as exc:
        raise RuntimeError(
            f"Completeness file missing or unreadable: s3://{bucket}/{completeness_key}. "
            "Worker should generate it before downstream steps."
        ) from exc

    missed_pages_count = int(completeness.get("missed_pages_count", 0) or 0)
    status = str(completeness.get("status", "")).upper().strip() or ("INCOMPLETE" if missed_pages_count > 0 else "COMPLETE")

    if status == "COMPLETE" and missed_pages_count == 0:
        result = {
            "status": "COMPLETE",
            "snapshot_date": snapshot_date,
            "missed_pages_count": 0,
            "completeness_key": completeness_key,
        }
        logger.info(f"[COMPLETENESS] pass: {result}")
        return result

    approved = ALLOW_MANUAL_APPROVE and _exists(bucket, approval_key)
    if approved:
        result = {
            "status": "COMPLETE_APPROVED_MANUALLY",
            "snapshot_date": snapshot_date,
            "missed_pages_count": missed_pages_count,
            "completeness_key": completeness_key,
            "approval_key": approval_key,
        }
        logger.warning(f"[COMPLETENESS] manual override used: {result}")
        return result

    raise RuntimeError(
        "Snapshot completeness gate failed: missing pages detected. "
        f"snapshot_date={snapshot_date}, missed_pages_count={missed_pages_count}, "
        f"completeness_key=s3://{bucket}/{completeness_key}. "
        "Backfill missing data, run repair-raw-archive for this snapshot, and then create manual approval file "
        f"s3://{bucket}/{approval_key} (if override is desired) before Step Functions redrive."
    )
