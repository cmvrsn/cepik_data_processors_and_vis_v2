import json
import logging
import os
import time

import boto3

athena = boto3.client("athena")

# ===== CONFIG =====
DATABASE = os.getenv("ATHENA_DATABASE", "motobi_cepik_hist")
RAW_TABLE = os.getenv("RAW_TABLE", "raw_archive")
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT", "s3://motointel-cepik-raw-prod/athena/results/")
POLL_INTERVAL_SEC = float(os.getenv("ATHENA_POLL_INTERVAL_SEC", "2"))
ATHENA_TIMEOUT_SEC = int(os.getenv("ATHENA_TIMEOUT_SEC", "3600"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def _start_query(sql: str) -> str:
    params = {
        "QueryString": sql,
        "QueryExecutionContext": {"Database": DATABASE},
    }
    if ATHENA_OUTPUT:
        params["ResultConfiguration"] = {"OutputLocation": ATHENA_OUTPUT}

    q = athena.start_query_execution(**params)
    return q["QueryExecutionId"]


def wait_for_query(qid: str, timeout_sec: int = ATHENA_TIMEOUT_SEC) -> str:
    start = time.time()

    while True:
        res = athena.get_query_execution(QueryExecutionId=qid)
        state = res["QueryExecution"]["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if state != "SUCCEEDED":
                reason = res["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
                raise RuntimeError(f"Athena query {qid} failed: {state} ({reason})")
            return state

        if time.time() - start > timeout_sec:
            raise TimeoutError(f"Athena query {qid} timed out after {timeout_sec}s")

        time.sleep(POLL_INTERVAL_SEC)


def run_athena(sql: str) -> str:
    logger.info(f"Running Athena query:\n{sql}")
    qid = _start_query(sql)
    wait_for_query(qid)
    logger.info(f"Athena query succeeded, qid={qid}")
    return qid


def repair_raw_archive() -> dict:
    logger.info(f"🔧 Running MSCK REPAIR for {RAW_TABLE}")

    sql = f"MSCK REPAIR TABLE {RAW_TABLE}"
    run_athena(sql)

    logger.info("✅ raw_archive repaired")

    return {
        "status": "REPAIRED",
        "table": RAW_TABLE,
        "database": DATABASE,
    }


def lambda_handler(event, context):
    return repair_raw_archive()


def main() -> int:
    """
    ECS entrypoint.
    No required args; reads config from env.
    """
    result = repair_raw_archive()
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
