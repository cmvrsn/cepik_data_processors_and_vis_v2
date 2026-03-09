import boto3
import logging
from typing import Dict, List

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


def list_keys(bucket: str, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def delete_prefix(bucket: str, prefix: str) -> int:
    keys = list(list_keys(bucket, prefix))
    if not keys:
        logger.info(f"[CLEAR_RAW] no objects under s3://{bucket}/{prefix}")
        return 0

    deleted = 0
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        deleted += len(batch)

    logger.info(f"[CLEAR_RAW] deleted={deleted} under s3://{bucket}/{prefix}")
    return deleted


def build_partition_prefix(base_prefix: str, snapshot_date: str, part: Dict) -> str:
    year = int(part["year"])
    month = str(part["month"]).zfill(2)
    return (
        f"{base_prefix}/archive/"
        f"snapshot_date={snapshot_date}/"
        f"year={year}/month={month}/"
    )


def lambda_handler(event, context):
    bucket = event["s3_bucket"]
    base_prefix = event.get("s3_prefix", "snapshots")
    snapshot_date = event["snapshot_date"]
    partitions: List[Dict] = event.get("partitions_to_clear") or []

    logger.info(
        f"[CLEAR_RAW] snapshot_date={snapshot_date} bucket={bucket} "
        f"partitions_to_clear={len(partitions)}"
    )

    deleted_total = 0
    for part in partitions:
        prefix = build_partition_prefix(base_prefix, snapshot_date, part)
        deleted_total += delete_prefix(bucket, prefix)

    result = {
        "status": "SUCCESS",
        "snapshot_date": snapshot_date,
        "partitions_requested": len(partitions),
        "deleted_objects": deleted_total,
    }
    logger.info(f"[CLEAR_RAW] summary: {result}")
    return result
