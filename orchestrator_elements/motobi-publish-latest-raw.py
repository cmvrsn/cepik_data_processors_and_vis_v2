import boto3
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

MAX_DELETE_BATCH = 1000
COPY_WORKERS = 32  # zacznij od 16-32, zwiększ jeśli potrzeba

def chunked(iterable, size):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def list_keys(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]

def delete_prefix(bucket, prefix):
    keys = list(list_keys(bucket, prefix))
    if not keys:
        return 0

    deleted = 0
    for batch in chunked(keys, MAX_DELETE_BATCH):
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        # resp może mieć "Errors" – warto to zalogować
        deleted += len(batch)
    return deleted

def copy_one(bucket, src_key, dest_key):
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": src_key},
        Key=dest_key
    )

def copy_prefix(bucket, src_prefix, dest_prefix):
    src_keys = list(list_keys(bucket, src_prefix))
    if not src_keys:
        return 0

    copied = 0
    futures = []
    with ThreadPoolExecutor(max_workers=COPY_WORKERS) as ex:
        for src_key in src_keys:
            dest_key = src_key.replace(src_prefix, dest_prefix, 1)
            futures.append(ex.submit(copy_one, bucket, src_key, dest_key))

        for f in as_completed(futures):
            f.result()  # jeśli wyjątek, tu poleci i zobaczysz realny błąd
            copied += 1

    return copied

def lambda_handler(event, context):
    bucket = event["s3_bucket"]
    base_prefix = "snapshots"
    snapshot_date = event["snapshot_date"]

    src_prefix = f"{base_prefix}/archive/snapshot_date={snapshot_date}/"
    dest_prefix = f"{base_prefix}/latest/"

    logger.info(f"[PUBLISH] Publishing RAW snapshot {snapshot_date} → {dest_prefix}")

    logger.info(f"[PUBLISH] Cleaning folder: {dest_prefix}")
    deleted = delete_prefix(bucket, dest_prefix)
    logger.info(f"[PUBLISH] Deleted {deleted} old files from latest/")

    logger.info(f"[PUBLISH] Copying snapshot at: {src_prefix}")
    copied = copy_prefix(bucket, src_prefix, dest_prefix)
    logger.info(f"[PUBLISH] Copied {copied} files into latest/")

    return {
        "status": "SUCCESS",
        "deleted_old_latest": deleted,
        "copied_new_latest": copied,
        "snapshot_date": snapshot_date,
    }
