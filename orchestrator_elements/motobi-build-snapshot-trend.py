import boto3
import time
import logging

athena = boto3.client("athena")

# ===== CONFIG =====
DATABASE = "motobi_cepik_hist"
TREND_TABLE = "motobi_prod_snapshot_trend"
RAW_TABLE = "raw_archive"

ATHENA_OUTPUT = "s3://motointel-cepik-raw-prod/athena/results/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


# ----------------------------------------------------------
# ATHENA RUNNER
# ----------------------------------------------------------
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


# ----------------------------------------------------------
# MAIN HANDLER
# ----------------------------------------------------------
def lambda_handler(event, context):
    snapshot_date = event.get("snapshot_date")

    if not snapshot_date:
        raise ValueError("snapshot_date is required in event")

    snapshot_month = snapshot_date[:7]  # YYYY-MM

    logger.info(f"📊 Build snapshot trend for {snapshot_month}")

    # 1️⃣ IDEMPOTENCY CHECK
    check_sql = f"""
    SELECT 1
    FROM {TREND_TABLE}
    WHERE snapshot_month = '{snapshot_month}'
    LIMIT 1
    """

    q = athena.start_query_execution(
        QueryString=check_sql,
        QueryExecutionContext={"Database": DATABASE},
    )

    qid = q["QueryExecutionId"]

    while True:
        res = athena.get_query_execution(QueryExecutionId=qid)
        state = res["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = res["QueryExecution"]["Status"].get(
            "StateChangeReason", "Unknown reason"
        )
        raise RuntimeError(f"Idempotency check failed: {reason}")

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