import boto3
import json
import logging
import os
import time

athena = boto3.client("athena")

DATABASE = "motobi_cepik_hist"
RAW_TABLE = "raw_archive"
TOP_BRAND_MOM_TABLE = "top_brand_mom_snapshot"

POLL_INTERVAL_SEC = float(os.getenv("ATHENA_POLL_INTERVAL_SEC", "2"))
ATHENA_TIMEOUT_SEC = int(os.getenv("ATHENA_TIMEOUT_SEC", "3600"))

TRACKED_BRANDS = [
    "VOLKSWAGEN",
    "OPEL",
    "TOYOTA",
    "FORD",
    "AUDI",
    "SKODA",
    "RENAULT",
    "BMW",
    "PEUGEOT",
    "MERCEDES-BENZ",
    "HONDA",
    "HYUNDAI",
    "FIAT",
    "KIA",
    "CITROEN",
    "SEAT",
    "VOLVO",
    "NISSAN",
    "TESLA",
    "BYD",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


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
    q = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
    )
    qid = q["QueryExecutionId"]
    wait_for_query(qid)
    logger.info(f"Athena query succeeded, qid={qid}")
    return qid


def table_exists() -> bool:
    qid = run_athena(f"SHOW TABLES IN {DATABASE} LIKE '{TOP_BRAND_MOM_TABLE}'")
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return len(rows) > 1


def snapshot_already_processed(snapshot_date: str) -> bool:
    qid = run_athena(
        f"""
        SELECT 1
        FROM {TOP_BRAND_MOM_TABLE}
        WHERE snapshot_date = '{snapshot_date}'
        LIMIT 1
        """
    )
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return len(rows) > 1


def select_rows_sql(snapshot_date: str) -> str:
    brands_values = ",\n            ".join(f"('{brand}')" for brand in TRACKED_BRANDS)

    return f"""
    WITH
    brand_whitelist AS (
        SELECT * FROM (
            VALUES
            {brands_values}
        ) AS t(brand)
    ),
    prev_snapshot AS (
        SELECT MAX(snapshot_date) AS prev_snapshot_date
        FROM {RAW_TABLE}
        WHERE snapshot_date < '{snapshot_date}'
    ),
    current_counts AS (
        SELECT
            marka AS brand,
            COUNT(DISTINCT id) AS vehicle_count
        FROM {RAW_TABLE}
        WHERE snapshot_date = '{snapshot_date}'
          AND marka IN (SELECT brand FROM brand_whitelist)
        GROUP BY marka
    ),
    prev_counts AS (
        SELECT
            marka AS brand,
            COUNT(DISTINCT id) AS vehicle_count
        FROM {RAW_TABLE}
        WHERE snapshot_date = (SELECT prev_snapshot_date FROM prev_snapshot)
          AND marka IN (SELECT brand FROM brand_whitelist)
        GROUP BY marka
    )
    SELECT
        bw.brand,
        '{snapshot_date}' AS snapshot_date,
        COALESCE(cc.vehicle_count, 0) AS vehicle_count,
        CASE
            WHEN (SELECT prev_snapshot_date FROM prev_snapshot) IS NULL THEN NULL
            ELSE COALESCE(cc.vehicle_count, 0) - COALESCE(pc.vehicle_count, 0)
        END AS mom_delta_abs,
        CASE
            WHEN (SELECT prev_snapshot_date FROM prev_snapshot) IS NULL THEN NULL
            WHEN COALESCE(pc.vehicle_count, 0) = 0 THEN NULL
            ELSE ((COALESCE(cc.vehicle_count, 0) - pc.vehicle_count) * 100.0 / pc.vehicle_count)
        END AS mom_delta_pct
    FROM brand_whitelist bw
    LEFT JOIN current_counts cc ON cc.brand = bw.brand
    LEFT JOIN prev_counts pc ON pc.brand = bw.brand
    """


def build_top_brand_mom(snapshot_date: str) -> dict:
    if not snapshot_date:
        raise ValueError("snapshot_date is required")

    if not table_exists():
        create_sql = f"""
        CREATE TABLE {TOP_BRAND_MOM_TABLE}
        WITH (
            format = 'PARQUET'
        ) AS
        {select_rows_sql(snapshot_date)}
        """
        run_athena(create_sql)
        return {
            "status": "CREATED_AND_INSERTED",
            "snapshot_date": snapshot_date,
            "table": TOP_BRAND_MOM_TABLE,
            "brands_count": len(TRACKED_BRANDS),
        }

    if snapshot_already_processed(snapshot_date):
        return {
            "status": "SKIPPED_ALREADY_EXISTS",
            "snapshot_date": snapshot_date,
            "table": TOP_BRAND_MOM_TABLE,
        }

    insert_sql = f"""
    INSERT INTO {TOP_BRAND_MOM_TABLE}
    {select_rows_sql(snapshot_date)}
    """
    run_athena(insert_sql)

    return {
        "status": "INSERTED",
        "snapshot_date": snapshot_date,
        "table": TOP_BRAND_MOM_TABLE,
        "brands_count": len(TRACKED_BRANDS),
    }


def lambda_handler(event, context):
    snapshot_date = (event or {}).get("snapshot_date")
    return build_top_brand_mom(snapshot_date)


def main() -> int:
    snapshot_date = os.getenv("SNAPSHOT_DATE", "").strip()
    result = build_top_brand_mom(snapshot_date)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
