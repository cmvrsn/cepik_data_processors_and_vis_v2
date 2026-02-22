import boto3
import time
import logging
from datetime import datetime

athena = boto3.client("athena")
s3 = boto3.client("s3")

# ===== CONFIG =====
DATABASE    = "motobi_cepik"

RAW_TABLE   = "motobi_raw_latest"
PROD_TABLE  = "motobi_prod_latest"

# 🚀 TO JEST WŁAŚCIWE MIEJSCE PRODUKCJI
PROD_PREFIX = "prod-data/latest/"
PROD_S3 = "s3://motointel-cepik-raw-prod/prod-data/latest/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


# ----------------------------------------------------------
#  ATHENA RUN
# ----------------------------------------------------------
def run_athena(sql: str) -> str:
    logger.info(f"Running Athena query:\n{sql}")
    
    q = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        # ❌ ResultConfiguration USUNIĘTE
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

# ----------------------------------------------------------
#  CLEANUP S3 PREFIX
# ----------------------------------------------------------
def delete_prefix(bucket: str, prefix: str) -> int:
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    to_delete = []
    for page in pages:
        for obj in page.get("Contents", []):
            to_delete.append({"Key": obj["Key"]})

    if not to_delete:
        logger.info(f"No objects to delete under prefix: {prefix}")
        return 0

    deleted = 0
    for i in range(0, len(to_delete), 1000):
        batch = to_delete[i:i + 1000]
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": batch}
        )
        deleted += len(resp.get("Deleted", []))

    logger.info(f"Deleted {deleted} objects under prefix: {prefix}")
    return deleted


# ----------------------------------------------------------
#  MAIN LAMBDA
# ----------------------------------------------------------
def lambda_handler(event, context):
    snapshot_date = (event or {}).get("snapshot_date") or datetime.utcnow().strftime("%Y-%m-%d-%H%M")

    logger.info(f"🏗️ Build PROD for snapshot_date={snapshot_date}")
    logger.info(f"RAW TABLE  : {RAW_TABLE}")
    logger.info(f"PROD TABLE : {PROD_TABLE}")
    logger.info(f"PROD PATH  : {PROD_S3}")

    # 0) DB + RAW REPAIR
    run_athena(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    run_athena(f"MSCK REPAIR TABLE {RAW_TABLE}")

    # 1) CLEAN PROD TABLE + PROD FILES
    logger.info("Dropping old PROD table (if exists)...")
    run_athena(f"DROP TABLE IF EXISTS {PROD_TABLE}")

    logger.info("Cleaning PROD prefix in S3…")
    delete_prefix("motointel-cepik-raw-prod", PROD_PREFIX)

    # 2) CTAS — BUILD PROD FROM CLEAN RAW SNAPSHOT
    sql_prod = f"""
    CREATE TABLE {PROD_TABLE}
    WITH (
        format = 'PARQUET',
        external_location = '{PROD_S3}',
        partitioned_by = ARRAY['type','wojewodztwo'],
        parquet_compression = 'GZIP'
    ) AS
    SELECT
        COUNT(DISTINCT id)                                       AS total_count,

        marka,
        model,
        wariant,
        wersja,
        TRY_CAST("rok-produkcji" AS INTEGER)                     AS rok_produkcji,

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

        -- PARTYCJE
        type,
        wojewodztwo
    FROM "{DATABASE}".{RAW_TABLE}
    WHERE
        ("przeznaczenie-pojazdu" IS NULL OR "przeznaczenie-pojazdu" = '---')
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
        type,
        wojewodztwo
    """

    run_athena(sql_prod)

    logger.info("✅ PROD build completed successfully.")
    return {
        "status": "SUCCESS",
        "snapshot_date": snapshot_date
    }