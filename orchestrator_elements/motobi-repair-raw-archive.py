import boto3
import time
import logging

athena = boto3.client("athena")

# ===== CONFIG =====
DATABASE = "motobi_cepik_hist"
RAW_TABLE = "raw_archive"
ATHENA_OUTPUT = "s3://motointel-cepik-raw-prod/athena/results/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def run_athena(sql: str) -> None:
    logger.info(f"Running Athena query:\n{sql}")

    q = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
    )

    qid = q["QueryExecutionId"]

    while True:
        res = athena.get_query_execution(QueryExecutionId=qid)
        state = res["QueryExecution"]["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break

        time.sleep(2)

    if state != "SUCCEEDED":
        reason = res["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        raise RuntimeError(f"Athena failed: {reason}")

    logger.info(f"Athena query succeeded, qid={qid}")


def lambda_handler(event, context):
    logger.info("🔧 Running MSCK REPAIR for raw_archive")

    sql = f"MSCK REPAIR TABLE {RAW_TABLE}"
    run_athena(sql)

    logger.info("✅ raw_archive repaired")

    return {
        "status": "REPAIRED",
        "table": RAW_TABLE
    }