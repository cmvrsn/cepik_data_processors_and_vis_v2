import boto3
import logging
import time
import os
from datetime import datetime

athena = boto3.client("athena")

DATABASE = "motobi_cepik"
RAW_TABLE = "motobi_raw_latest"
DIM_BRAND_TABLE = "dim_brand"
DIM_BRAND_S3 = "s3://motointel-cepik-raw-prod/dim/brand/"
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "motobi-etl")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def run_athena(sql: str) -> str:
    logger.info(f"Running Athena query on workgroup='{ATHENA_WORKGROUP}':\n{sql}")
    q = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
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

    logger.info(f"Athena state={state}, qid={qid}")
    return qid


def fetch_single_number(sql: str) -> int:
    qid = run_athena(sql)
    res = athena.get_query_results(QueryExecutionId=qid, MaxResults=1)
    rows = res.get("ResultSet", {}).get("Rows", [])
    if len(rows) < 2:
        return 0

    data_row = rows[1].get("Data", [])
    if not data_row:
        return 0

    val = data_row[0].get("VarCharValue")
    try:
        return int(val)
    except Exception:
        logger.warning(f"Cannot parse numeric result from '{val}'")
        return 0


def lambda_handler(event, context):
    snapshot_date = (event or {}).get("snapshot_date") or datetime.utcnow().strftime("%Y-%m-%d-%H%M")
    logger.info(f"🔁 Refresh DIM brand for snapshot_date={snapshot_date}")
    logger.info(f"[DIM_BRAND] external_location={DIM_BRAND_S3}")

    # Rebuild DIM table from latest RAW snapshot.
    run_athena(f"DROP TABLE IF EXISTS {DIM_BRAND_TABLE}")

    sql = f"""
    CREATE TABLE {DIM_BRAND_TABLE}
    WITH (
        format = 'PARQUET',
        external_location = '{DIM_BRAND_S3}',
        parquet_compression = 'GZIP'
    ) AS
    SELECT DISTINCT
        marka AS brand,
        model
    FROM {RAW_TABLE}
    WHERE
        marka IS NOT NULL AND marka <> ''
        AND model IS NOT NULL AND model <> ''
    """
    run_athena(sql)

    row_count = fetch_single_number(f"SELECT COUNT(*) AS cnt FROM {DIM_BRAND_TABLE}")

    result = {
        "status": "SUCCESS",
        "snapshot_date": snapshot_date,
        "dim_brand_rows": row_count,
        "athena_workgroup": ATHENA_WORKGROUP,
        "dim_brand_s3": DIM_BRAND_S3,
    }
    logger.info(f"[DIM_BRAND] Summary: {result}")
    return result
