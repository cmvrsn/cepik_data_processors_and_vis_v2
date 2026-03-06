import json
import logging
import os
import time
from typing import List, Tuple, Dict

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("register-partitions")

s3 = boto3.client("s3")
athena = boto3.client("athena")

# ===== CONFIG =====
DATABASE = os.getenv("ATHENA_DATABASE", "motobi_cepik_hist")
RAW_TABLE = os.getenv("RAW_TABLE", "raw_archive")

S3_BUCKET = os.getenv("S3_BUCKET", "motointel-cepik-raw-prod")
S3_ARCHIVE_PREFIX = os.getenv("S3_ARCHIVE_PREFIX", "snapshots/archive/")  # no leading slash, must end with "/"
SNAPSHOT_DATE = os.getenv("SNAPSHOT_DATE")  # e.g. "2026-02-07-1410"

# Optional: if you DO set this and your WorkGroup uses "Athena managed storage", it may conflict.
# Default is empty -> rely on WorkGroup settings.
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT", "").strip()

POLL_INTERVAL_SEC = float(os.getenv("ATHENA_POLL_INTERVAL_SEC", "2"))
ATHENA_TIMEOUT_SEC = int(os.getenv("ATHENA_TIMEOUT_SEC", "3600"))

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))


def _list_common_prefixes(bucket: str, prefix: str) -> List[str]:
    """
    Returns list of child "directories" under prefix, as full prefixes ending with "/".
    Uses Delimiter='/' -> CommonPrefixes.
    """
    out: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            p = cp.get("Prefix")
            if p:
                out.append(p)
    return out


def _prefix_has_objects(bucket: str, prefix: str) -> bool:
    """
    Checks quickly if prefix contains at least 1 object (not just subfolders).
    """
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return bool(resp.get("Contents"))


def _start_query(sql: str) -> str:
    params = {
        "QueryString": sql,
        "QueryExecutionContext": {"Database": DATABASE},
    }
    # IMPORTANT: if WorkGroup enforces managed results, setting OutputLocation may cause:
    # "ManagedQueryResultsConfiguration and ResultConfiguration cannot be set together."
    if ATHENA_OUTPUT:
        params["ResultConfiguration"] = {"OutputLocation": ATHENA_OUTPUT}

    q = athena.start_query_execution(**params)
    return q["QueryExecutionId"]


def _wait_for_query(qid: str, timeout_sec: int = ATHENA_TIMEOUT_SEC) -> None:
    start = time.time()
    while True:
        res = athena.get_query_execution(QueryExecutionId=qid)
        state = res["QueryExecution"]["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if state != "SUCCEEDED":
                reason = res["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
                raise RuntimeError(f"Athena query {qid} failed: {state} ({reason})")
            return

        if time.time() - start > timeout_sec:
            raise TimeoutError(f"Athena query {qid} timed out after {timeout_sec}s")

        time.sleep(POLL_INTERVAL_SEC)


def run_athena(sql: str) -> str:
    logger.info(f"Running Athena query (len={len(sql)}):\n{sql[:1200]}{'...' if len(sql) > 1200 else ''}")
    qid = _start_query(sql)
    _wait_for_query(qid)
    logger.info(f"Athena query succeeded, qid={qid}")
    return qid


def _parse_kv(folder: str) -> Tuple[str, str]:
    """
    folder like 'year=2005' -> ('year','2005')
    """
    if "=" not in folder:
        raise ValueError(f"Invalid partition folder (expected k=v): {folder}")
    k, v = folder.split("=", 1)
    return k, v


def discover_partitions_for_snapshot(snapshot_date: str) -> List[Dict[str, str]]:
    """
    Walks:
      snapshots/archive/snapshot_date=.../year=YYYY/month=MM/type=.../wojewodztwo=XX/
    Returns list of dicts with keys: snapshot_date, year, month, type, wojewodztwo, location_prefix
    """
    base = S3_ARCHIVE_PREFIX
    if not base.endswith("/"):
        base += "/"

    snap_prefix = f"{base}snapshot_date={snapshot_date}/"
    logger.info(f"Discovering partitions under s3://{S3_BUCKET}/{snap_prefix}")

    years = _list_common_prefixes(S3_BUCKET, snap_prefix)
    if not years:
        logger.warning("No year prefixes found. Is the snapshot in archive path?")
        return []

    partitions: List[Dict[str, str]] = []

    for ypref in years:
        yfolder = ypref.rstrip("/").split("/")[-1]
        ky, vy = _parse_kv(yfolder)
        if ky != "year":
            continue

        months = _list_common_prefixes(S3_BUCKET, ypref)
        for mpref in months:
            mfolder = mpref.rstrip("/").split("/")[-1]
            km, vm = _parse_kv(mfolder)
            if km != "month":
                continue

            types = _list_common_prefixes(S3_BUCKET, mpref)
            for tpref in types:
                tfolder = tpref.rstrip("/").split("/")[-1]
                kt, vt = _parse_kv(tfolder)
                if kt != "type":
                    continue

                woj = _list_common_prefixes(S3_BUCKET, tpref)
                for wpref in woj:
                    wfolder = wpref.rstrip("/").split("/")[-1]
                    kw, vw = _parse_kv(wfolder)
                    if kw != "wojewodztwo":
                        continue

                    # Ensure there is at least 1 object under this final prefix
                    if not _prefix_has_objects(S3_BUCKET, wpref):
                        continue

                    partitions.append(
                        {
                            "snapshot_date": snapshot_date,
                            "year": vy,
                            "month": vm,
                            "type": vt,
                            "wojewodztwo": vw,
                            "location_prefix": wpref,  # s3 prefix, without bucket
                        }
                    )

    logger.info(f"Discovered partitions: {len(partitions)}")
    return partitions


def build_add_partition_sql(partitions: List[Dict[str, str]]) -> str:
    """
    Builds one ALTER TABLE ... ADD IF NOT EXISTS with multiple PARTITION clauses.
    """
    clauses = []
    for p in partitions:
        # year is int in table definition -> no quotes
        # others are strings -> quotes
        loc = f"s3://{S3_BUCKET}/{p['location_prefix']}"
        clause = (
            "PARTITION ("
            f"snapshot_date='{p['snapshot_date']}', "
            f"year={int(p['year'])}, "
            f"month='{p['month']}', "
            f"type='{p['type']}', "
            f"wojewodztwo='{p['wojewodztwo']}'"
            f") LOCATION '{loc}'"
        )
        clauses.append(clause)

    sql = f"ALTER TABLE {RAW_TABLE} ADD IF NOT EXISTS \n" + "\n".join(clauses)
    return sql


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def main() -> int:
    if not SNAPSHOT_DATE:
        raise ValueError("SNAPSHOT_DATE is required (e.g. 2026-02-07-1410)")

    parts = discover_partitions_for_snapshot(SNAPSHOT_DATE)
    if not parts:
        logger.warning("No partitions discovered. Nothing to register.")
        print(json.dumps({"status": "NO_PARTITIONS", "snapshot_date": SNAPSHOT_DATE}, ensure_ascii=False))
        return 0

    qids = []
    for batch in chunked(parts, BATCH_SIZE):
        sql = build_add_partition_sql(batch)
        qids.append(run_athena(sql))

    result = {
        "status": "REGISTERED",
        "snapshot_date": SNAPSHOT_DATE,
        "registered_partitions": len(parts),
        "batches": len(qids),
        "query_execution_ids": qids,
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
