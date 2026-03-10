import boto3
import time
import logging
from datetime import datetime

athena = boto3.client("athena")

# ===== CONFIG =====
DATABASE  = "motobi_cepik"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def run_athena(sql: str):
    """Uruchamia zapytanie w Athenie i czeka na wynik."""
    logger.info(f"[ATHENA] Query start:\n{sql}")
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

    logger.info(f"[ATHENA] SUCCEEDED qid={qid}")
    return qid


def fetch_single_number(sql: str) -> int:
    """Uruchamia zapytanie, które zwraca 1 liczbę (COUNT lub SUM) i ją odczytuje."""
    qid = run_athena(sql)
    res = athena.get_query_results(QueryExecutionId=qid, MaxResults=1)
    rows = res.get("ResultSet", {}).get("Rows", [])
    if len(rows) < 2:
        return 0
    # pierwszy wiersz = nagłówek, drugi = dane
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
    snapshot_date = (event or {}).get("snapshot_date") or datetime.utcnow().strftime("%Y-%m")
    logger.info(f"🔎 QA Validate for snapshot_date={snapshot_date}")

    # 1) Policz rekordy RAW
    sql_count_raw = """
        SELECT COUNT(*) AS cnt
        FROM motobi_raw_latest
    """
    raw_count = fetch_single_number(sql_count_raw)
    logger.info(f"[QA] RAW total rows = {raw_count}")

    # 2) Policz sumę total_count w PROD
    sql_count_prod = """
        SELECT COALESCE(SUM(total_count), 0) AS cnt
        FROM motobi_prod_latest
    """
    prod_count = fetch_single_number(sql_count_prod)
    logger.info(f"[QA] PROD total_count sum = {prod_count}")

    # 3) Prosta różnica / ratio
    diff = raw_count - prod_count
    ratio = (prod_count / raw_count) if raw_count > 0 else 0.0

    logger.info(f"[QA] diff = RAW - PROD = {diff}")
    logger.info(f"[QA] ratio = PROD / RAW = {ratio:.4f}")

    # 4) Możesz tu dodać progi alarmowe, np.:
    # if raw_count > 0 and ratio < 0.90:
    #     logger.warning("[QA] Ratio PROD/RAW < 0.9 – potencjalny problem z agregacją")

    # ALE: nie wywalamy całego pipeline'u – zwracamy statystyki
    result = {
        "status": "OK",
        "snapshot_date": snapshot_date,
        "raw_count": raw_count,
        "prod_count": prod_count,
        "diff": diff,
        "ratio_prod_to_raw": ratio,
    }
    logger.info(f"[QA] Summary: {result}")
    return result