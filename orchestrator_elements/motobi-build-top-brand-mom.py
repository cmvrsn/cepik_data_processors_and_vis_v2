import boto3
import json
import logging
import os
import time
from datetime import datetime
from decimal import Decimal

athena = boto3.client("athena")
dynamodb = boto3.resource("dynamodb")

DATABASE = "motobi_cepik_hist"
RAW_TABLE = "raw_archive"

# Domyślnie używamy user-managed WG (obsługuje INSERT/CTAS dla tej lambdy).
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "motobi-etl")
# Fallback na wypadek uruchomienia z WG z Managed Results.
ATHENA_FALLBACK_WORKGROUP = os.getenv("ATHENA_FALLBACK_WORKGROUP", "motobi-etl")

DDB_TABLE = os.getenv("TOP_BRAND_MOM_DDB_TABLE", "motobi_top_brand_mom")
TOP_BRAND_REPLACE_MODE = os.getenv("TOP_BRAND_REPLACE_MODE", "true").strip().lower() in {"1", "true", "yes"}

POLL_INTERVAL_SEC = float(os.getenv("ATHENA_POLL_INTERVAL_SEC", "2"))
ATHENA_TIMEOUT_SEC = int(os.getenv("ATHENA_TIMEOUT_SEC", "3600"))

TARGET_VEHICLE_ORIGIN = "UŻYW. ZAKUPIONY W KRAJU"
TARGET_VEHICLE_TYPE = "SAMOCHÓD OSOBOWY"

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


def _start_query(sql: str, workgroup: str):
    return athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=workgroup,
    )


def run_athena(sql: str) -> str:
    workgroup = ATHENA_WORKGROUP
    logger.info(f"Running Athena query on workgroup='{workgroup}':\n{sql}")

    try:
        q = _start_query(sql, workgroup)
    except athena.exceptions.InvalidRequestException as exc:
        msg = str(exc)
        managed_results_error = "not supported for workgroups with Managed Query Results enabled"
        should_retry = managed_results_error in msg and ATHENA_FALLBACK_WORKGROUP != workgroup

        if not should_retry:
            raise

        fallback_workgroup = ATHENA_FALLBACK_WORKGROUP
        logger.warning(
            "Athena rejected query for workgroup='%s' due to Managed Results restrictions. "
            "Retrying on fallback workgroup='%s'.",
            workgroup,
            fallback_workgroup,
        )
        q = _start_query(sql, fallback_workgroup)

    qid = q["QueryExecutionId"]
    wait_for_query(qid)
    logger.info(f"Athena query succeeded, qid={qid}")
    return qid


def get_query_rows(qid: str):
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    if len(rows) <= 1:
        return []

    headers = [c.get("VarCharValue", "") for c in rows[0].get("Data", [])]
    parsed_rows = []

    for row in rows[1:]:
        vals = [c.get("VarCharValue") for c in row.get("Data", [])]
        parsed_rows.append(dict(zip(headers, vals)))

    return parsed_rows


def month_shift(month_yyyy_mm: str, delta: int) -> str:
    dt = datetime.strptime(month_yyyy_mm + "-01", "%Y-%m-%d")
    month_index = dt.year * 12 + (dt.month - 1) + delta
    year = month_index // 12
    month = (month_index % 12) + 1
    return f"{year:04d}-{month:02d}"


def get_prev_snapshot(snapshot_date: str) -> str:
    qid = run_athena(
        f"""
        SELECT MAX(snapshot_date) AS prev_snapshot_date
        FROM {RAW_TABLE}
        WHERE snapshot_date < '{snapshot_date}'
        """
    )
    rows = get_query_rows(qid)
    if not rows or not rows[0].get("prev_snapshot_date"):
        return ""
    return rows[0]["prev_snapshot_date"]


def get_brand_counts(snapshot_date: str, target_month: str) -> dict:
    year, month = target_month.split("-")
    brands_values = ",\n            ".join(f"('{brand}')" for brand in TRACKED_BRANDS)

    qid = run_athena(
        f"""
        WITH brand_whitelist AS (
            SELECT * FROM (
                VALUES
                {brands_values}
            ) AS t(brand)
        ),
        counts AS (
            SELECT
                marka AS brand,
                COUNT(DISTINCT id) AS vehicle_count
            FROM {RAW_TABLE}
            WHERE snapshot_date = '{snapshot_date}'
              AND year = {year}
              AND month = '{month}'
              AND "pochodzenie-pojazdu" = '{TARGET_VEHICLE_ORIGIN}'
              AND "rodzaj-pojazdu" = '{TARGET_VEHICLE_TYPE}'
              AND marka IN (SELECT brand FROM brand_whitelist)
            GROUP BY marka
        )
        SELECT
            bw.brand,
            COALESCE(c.vehicle_count, 0) AS vehicle_count
        FROM brand_whitelist bw
        LEFT JOIN counts c ON c.brand = bw.brand
        """
    )

    rows = get_query_rows(qid)
    out = {brand: 0 for brand in TRACKED_BRANDS}
    for row in rows:
        brand = row.get("brand")
        count = int(row.get("vehicle_count") or 0)
        if brand in out:
            out[brand] = count
    return out


def compute_payload(snapshot_date: str) -> tuple[list[dict], dict]:
    snapshot_month = snapshot_date[:7]
    current_reg_month = month_shift(snapshot_month, -1)
    prev_reg_month = month_shift(snapshot_month, -2)

    prev_snapshot_date = get_prev_snapshot(snapshot_date)
    if not prev_snapshot_date:
        raise ValueError(f"No previous snapshot found for snapshot_date={snapshot_date}")

    current_counts = get_brand_counts(snapshot_date, current_reg_month)
    prev_counts = get_brand_counts(prev_snapshot_date, prev_reg_month)

    rows = []
    for brand in TRACKED_BRANDS:
        current = current_counts.get(brand, 0)
        previous = prev_counts.get(brand, 0)
        delta_abs = current - previous
        delta_pct = None if previous == 0 else (delta_abs * 100.0 / previous)

        rows.append(
            {
                "brand": brand,
                "snapshot_date": snapshot_date,
                "previous_snapshot_date": prev_snapshot_date,
                "current_reg_month": current_reg_month,
                "previous_reg_month": prev_reg_month,
                "vehicle_count": current,
                "prev_vehicle_count": previous,
                "mom_delta_abs": delta_abs,
                "mom_delta_pct": delta_pct,
            }
        )

    summary = {
        "snapshot_date": snapshot_date,
        "previous_snapshot_date": prev_snapshot_date,
        "current_reg_month": current_reg_month,
        "previous_reg_month": prev_reg_month,
    }
    return rows, summary


def save_to_dynamodb(rows: list[dict]) -> None:
    table = dynamodb.Table(DDB_TABLE)

    if TOP_BRAND_REPLACE_MODE:
        _replace_table_content(table)

    with table.batch_writer(overwrite_by_pkeys=["snapshot_date", "brand"]) as batch:
        for row in rows:
            item = {
                "snapshot_date": row["snapshot_date"],
                "brand": row["brand"],
                "previous_snapshot_date": row["previous_snapshot_date"],
                "current_reg_month": row["current_reg_month"],
                "previous_reg_month": row["previous_reg_month"],
                "vehicle_count": row["vehicle_count"],
                "prev_vehicle_count": row["prev_vehicle_count"],
                "mom_delta_abs": row["mom_delta_abs"],
                "updated_at": int(time.time()),
            }
            if row["mom_delta_pct"] is not None:
                item["mom_delta_pct"] = Decimal(str(round(row["mom_delta_pct"], 4)))
            batch.put_item(Item=item)


def _replace_table_content(table) -> None:
    key_schema = table.key_schema
    key_names = [x["AttributeName"] for x in key_schema]
    projection = ", ".join(key_names)

    scan_kwargs = {"ProjectionExpression": projection}
    deleted = 0

    while True:
        response = table.scan(**scan_kwargs)
        items = response.get("Items", [])

        if items:
            with table.batch_writer() as batch:
                for item in items:
                    key = {k: item[k] for k in key_names}
                    batch.delete_item(Key=key)
                    deleted += 1

        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    logger.info("Top Brand MoM replace mode: deleted %s existing DDB items before write", deleted)


def build_top_brand_mom(snapshot_date: str) -> dict:
    if not snapshot_date:
        raise ValueError("snapshot_date is required")

    rows, summary = compute_payload(snapshot_date)
    save_to_dynamodb(rows)

    return {
        "status": "UPSERTED_TO_DDB",
        "snapshot_date": summary["snapshot_date"],
        "previous_snapshot_date": summary["previous_snapshot_date"],
        "current_reg_month": summary["current_reg_month"],
        "previous_reg_month": summary["previous_reg_month"],
        "rows_written": len(rows),
        "ddb_table": DDB_TABLE,
    }


def lambda_handler(event, context):
    snapshot_date = (event or {}).get("snapshot_date")
    return build_top_brand_mom(snapshot_date)


def main() -> int:
    snapshot_date = os.getenv("SNAPSHOT_DATE", "").strip()
    result = build_top_brand_mom(snapshot_date)
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
