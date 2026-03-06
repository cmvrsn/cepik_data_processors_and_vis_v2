import json
import logging
import os
import time

import boto3

athena = boto3.client("athena")

# ===== CONFIG =====
DATABASE = os.getenv("ATHENA_DATABASE", "motobi_cepik_hist")
TREND_TABLE = os.getenv("TREND_TABLE", "motobi_prod_snapshot_trend")
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


# ----------------------------------------------------------
# ATHENA RUNNER
# ----------------------------------------------------------
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


def build_snapshot_trend(snapshot_date: str) -> dict:
    if not snapshot_date:
        raise ValueError("snapshot_date is required")

    snapshot_month = snapshot_date[:7]  # YYYY-MM

    logger.info(f"📊 Build snapshot trend for {snapshot_month}")

    # 1️⃣ IDEMPOTENCY CHECK
    check_sql = f"""
    SELECT 1
    FROM {TREND_TABLE}
    WHERE snapshot_month = '{snapshot_month}'
    LIMIT 1
    """

    qid = _start_query(check_sql)
    wait_for_query(qid)

    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]

    if len(rows) > 1:
        logger.info("⏭️ Snapshot month already exists — skipping insert")
        return {
            "status": "SKIPPED_ALREADY_EXISTS",
            "snapshot_month": snapshot_month,
        }

    # 2️⃣ INSERT SNAPSHOT TREND
    insert_sql = f"""
    INSERT INTO {TREND_TABLE}
    SELECT
        COUNT(DISTINCT id)                               AS total_count,

        marka,
        model,
        wariant,
        wersja,
        TRY_CAST("rok-produkcji" AS INTEGER)             AS rok_produkcji,

        "rodzaj-pojazdu",
        "podrodzaj-pojazdu",
        "przeznaczenie-pojazdu",
        "pochodzenie-pojazdu",
        "rodzaj-paliwa",

        "pojemnosc-skokowa-silnika",
        "moc-netto-silnika",

        COALESCE(
            "rodzaj-pierwszego-paliwa-alternatywnego",
            "rodzaj-drugiego-paliwa-alternatywnego",
            'BRAK'
        ) AS paliwo_alternatywne,

        "kierownica-po-prawej-stronie-pierwotnie",
        "kierownica-po-prawej-stronie",

        year,
        month,
        "rejestracja-wojewodztwo",
        "rejestracja-powiat",

        SUBSTR(snapshot_date, 1, 7) AS snapshot_month
    FROM {RAW_TABLE}
    WHERE
        SUBSTR(snapshot_date, 1, 7) = '{snapshot_month}'
        AND ("przeznaczenie-pojazdu" IS NULL OR "przeznaczenie-pojazdu" = '---')
        AND "sposob-produkcji" = 'FABRYCZNY'
        AND marka IS NOT NULL AND marka <> ''
        AND "rodzaj-pojazdu" IS NOT NULL AND "rodzaj-pojazdu" <> ''
        AND "rodzaj-paliwa" IS NOT NULL AND "rodzaj-paliwa" <> ''
        AND "pochodzenie-pojazdu" IN (
            'UŻYW. ZAKUPIONY W KRAJU',
            'NOWY ZAKUPIONY W KRAJU',
            'UŻYW. IMPORT INDYW',
            'NOWY IMPORT INDYW'
        )
    GROUP BY
        marka,
        model,
        wariant,
        wersja,
        TRY_CAST("rok-produkcji" AS INTEGER),

        "rodzaj-pojazdu",
        "podrodzaj-pojazdu",
        "przeznaczenie-pojazdu",
        "pochodzenie-pojazdu",
        "rodzaj-paliwa",

        "pojemnosc-skokowa-silnika",
        "moc-netto-silnika",

        COALESCE(
            "rodzaj-pierwszego-paliwa-alternatywnego",
            "rodzaj-drugiego-paliwa-alternatywnego",
            'BRAK'
        ),

        "kierownica-po-prawej-stronie-pierwotnie",
        "kierownica-po-prawej-stronie",

        year,
        month,
        "rejestracja-wojewodztwo",
        "rejestracja-powiat",

        SUBSTR(snapshot_date, 1, 7)
    """

    run_athena(insert_sql)

    logger.info("✅ Snapshot trend inserted")

    return {
        "status": "INSERTED",
        "snapshot_month": snapshot_month,
    }


# ----------------------------------------------------------
# MAIN HANDLER
# ----------------------------------------------------------
def lambda_handler(event, context):
    snapshot_date = (event or {}).get("snapshot_date")
    return build_snapshot_trend(snapshot_date)


def main() -> int:
    """
    ECS entrypoint.
    Required env: SNAPSHOT_DATE (e.g. 2025-01-15-1200)
    """
    snapshot_date = os.getenv("SNAPSHOT_DATE", "").strip()
    result = build_snapshot_trend(snapshot_date)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
