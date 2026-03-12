import os
import ssl
import time
import json
import uuid
import random
import logging

import boto3
import requests
import pandas as pd

from datetime import datetime
from urllib.parse import urlencode

from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.poolmanager import PoolManager

# =====================================================
#                  CONFIG & CONSTANTS
# =====================================================

API_URL = "https://api.cepik.gov.pl/pojazdy"
BUCKET = os.getenv("S3_BUCKET", "motointel-cepik-raw-prod")
BASE_PREFIX = os.getenv("S3_PREFIX", "snapshots")
SNAPSHOT_DATE = os.getenv("SNAPSHOT_DATE", datetime.utcnow().strftime("%Y-%m"))

YEARS_ENV = os.getenv("YEARS", "").strip()
if YEARS_ENV:
    # formaty dozwolone:
    # "2018"
    # "2015-2020"
    # "2015,2016,2018"
    years_list = []
    for part in YEARS_ENV.split(","):
        part = part.strip()
        if "-" in part:
            start, end = part.split("-")
            years_list.extend(range(int(start), int(end) + 1))
        else:
            years_list.append(int(part))
    YEARS = sorted(set(years_list))
else:
    # kompatybilność wsteczna
    YEAR_START = int(os.getenv("START_YEAR", 2005))
    YEAR_END = int(os.getenv("END_YEAR", datetime.utcnow().year))
    YEARS = list(range(YEAR_START, YEAR_END + 1))

MONTH_START = int(os.getenv("MONTH_START", 1))
MONTH_END = int(os.getenv("MONTH_END", 12))

VEHICLE_TYPES = ["SAMOCHÓD OSOBOWY", "MOTOCYKL", "MOTOROWER"]

LIMIT = int(os.getenv("LIMIT", 450))
TIMEOUT = int(os.getenv("TIMEOUT", 25))
MAX_RETRIES = int(os.getenv("RETRIES", 15))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", 1.7))
FLUSH_EVERY_PAGES = int(os.getenv("FLUSH_EVERY_PAGES", 50))
TYP_DATY = str(os.getenv("TYP_DATY", "2"))

# delikatny throttling per request – można stroić ENV-ami
REQUEST_DELAY_MIN = float(os.getenv("REQUEST_DELAY_MIN", "0.03"))  # sekundy
REQUEST_DELAY_MAX = float(os.getenv("REQUEST_DELAY_MAX", "0.08"))

_w = os.getenv("WOJ_LIST", "")
if _w.strip():
    WOJEWODZTWA = [w.strip() for w in _w.split(",") if w.strip()]
else:
    # domyślnie 02..32 co 2
    WOJEWODZTWA = [f"{i:02}" for i in range(2, 34, 2)]

s3 = boto3.client("s3")
RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")
RUN_TAG = datetime.utcnow().strftime("%b-%Y").lower()  # np. nov-2025

# WORKER_ID zostawiamy tylko do ewentualnych logów/debugu
WORKER_ID = int(os.getenv("WORKER_ID", "0"))

# =====================================================
#        SEMANTIC RETRY (CEPIK GLITCH PROTECTION)
# =====================================================

SANITY_RETRIES = int(os.getenv("SANITY_RETRIES", "5"))
SANITY_BACKOFF_BASE = float(os.getenv("SANITY_BACKOFF_BASE", "2.0"))

PAGE1_SEMANTIC_RETRIES = int(os.getenv("PAGE1_SEMANTIC_RETRIES", "7"))
PAGE1_BACKOFF_BASE = float(os.getenv("PAGE1_BACKOFF_BASE", "2.0"))


# =====================================================
#                  LOGGING
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# =====================================================
#           PROSTE LOGOWANIE PUBLICZNEGO IP
# =====================================================

def log_public_ip():
    """Loguje publiczne IP workera wg serwisu zewnętrznego."""
    try:
        ip = requests.get("https://api.ipify.org", timeout=5).text.strip()
        logging.info(f"[NET] Public IP (ipify): {ip}")
    except Exception as e:
        logging.warning(f"[NET] Failed to get public IP: {e}")

log_public_ip()

# =====================================================
#               SSL + RETRY SESSION
# =====================================================

context = ssl.create_default_context()
context.set_ciphers("DEFAULT:@SECLEVEL=1")

class SSLAdapter(HTTPAdapter):
    """Adapter z niestandardowym kontekstem SSL"""
    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = context
        return super().init_poolmanager(*args, **kwargs)

retry_strategy = Retry(
    total=MAX_RETRIES,
    backoff_factor=BACKOFF_BASE,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)

adapter = SSLAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)
session.headers.update({"User-Agent": "motointel-cepik-worker/prod-sequential"})

# =====================================================
#               VALID FIELD LIST (from CEPIK docs)
# =====================================================

FIELDS_API = [
    "marka","typ","model","wariant","wersja",
    "rodzaj-pojazdu","podrodzaj-pojazdu","przeznaczenie-pojazdu",
    "pochodzenie-pojazdu","rodzaj-tabliczki-znamionowej","sposob-produkcji",
    "rok-produkcji","data-pierwszej-rejestracji-w-kraju","data-ostatniej-rejestracji-w-kraju",
    "data-rejestracji-za-granica","pojemnosc-skokowa-silnika","moc-netto-silnika",
    "moc-netto-silnika-hybrydowego","masa-wlasna","masa-pojazdu-gotowego-do-jazdy",
    "liczba-miejsc-ogolem","liczba-miejsc-stojacych","rodzaj-paliwa",
    "rodzaj-pierwszego-paliwa-alternatywnego","rodzaj-drugiego-paliwa-alternatywnego",
    "kierownica-po-prawej-stronie","kierownica-po-prawej-stronie-pierwotnie",
    "data-pierwszej-rejestracji","data-wyrejestrowania-pojazdu","przyczyna-wyrejestrowania-pojazdu",
    "rejestracja-wojewodztwo","rejestracja-gmina","rejestracja-powiat","wojewodztwo-kod"
]

# =====================================================
#                   HELPERS
# =====================================================

def slug_type(vehicle_type: str) -> str:
    repl = (
        ("ą","a"),("ć","c"),("ę","e"),("ł","l"),("ń","n"),
        ("ó","o"),("ś","s"),("ż","z"),("ź","z"),
        ("Ą","A"),("Ć","C"),("Ę","E"),("Ł","L"),("Ń","N"),
        ("Ó","O"),("Ś","S"),("Ż","Z"),("Ź","Z")
    )
    s = vehicle_type.strip().lower().replace(" ", "_")
    for a, b in repl:
        s = s.replace(a, b)
    return s

def save_dataframe_to_s3(df: pd.DataFrame, path: str):
    import io, pyarrow as pa, pyarrow.parquet as pq
    logging.info("dtypes_before=%s", df.dtypes.to_dict())

    def _to_str_safe(x):
        if x is None:
            return None
        try:
            import math
            if isinstance(x, float) and math.isnan(x):
                return None
        except Exception:
            pass
        return str(x)

    df_str = df.copy()
    df_str = df_str.astype("string").where(df_str.notnull(), None)

    logging.info("dtypes_after=%s", df_str.dtypes.to_dict())
    pa_fields = [pa.field(col, pa.string()) for col in df_str.columns]
    schema = pa.schema(pa_fields)
    table = pa.Table.from_pandas(df_str, preserve_index=False, schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="gzip")
    s3.put_object(Bucket=BUCKET, Key=path, Body=buffer.getvalue())
    logging.info(f"✅ saved_rows={len(df)} s3://{BUCKET}/{path}")

def build_api_url(params):
    return f"{API_URL}?{urlencode(params)}"

# =====================================================
#                   FETCH PAGE
# =====================================================

def fetch_page(vehicle_type, woj, year, month, page):
    """
    Pobiera jedną stronę danych z CEPIK dla danego:
    typu pojazdu, województwa, roku, miesiąca i numeru strony.
    Zwraca:
      - rows: lista rekordów
      - has_next: bool czy istnieje kolejna strona
      - latency: czas zapytania
      - used_limit: użyty limit
      - err_type: typ błędu (lub None)
    """
    date_od = f"{year}{month:02}01"
    date_do = f"{year}{month:02}31"
    current_limit = LIMIT

    params = {
        "wojewodztwo": woj,
        "data-od": date_od,
        "data-do": date_do,
        "typ-daty": TYP_DATY,
        "filter[rodzaj-pojazdu]": vehicle_type,
        "tylko-zarejestrowane": "true",
        "limit": str(current_limit),
        "page": str(page),
        "pokaz-wszystkie-pola": "false",
        "fields": ",".join(FIELDS_API),
    }
    url = build_api_url(params)

    for attempt in range(1, MAX_RETRIES + 1):
        resp = None
        raw_data = b""
        try:
            # delikatny jitter, żeby nie walić jak metronom
            delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
            time.sleep(delay)

            t0 = time.time()
            resp = session.get(url, timeout=TIMEOUT, stream=True)
            raw_data = resp.content
            latency = time.time() - t0

            if resp.status_code == 404:
                content_len_header = resp.headers.get("Content-Length")
                raw_len = len(raw_data)
                logging.warning(
                    f"page_404 woj={woj} type={vehicle_type} year={year} month={month} "
                    f"page={page} status=404 header_len={content_len_header} "
                    f"raw_len={raw_len} attempt={attempt}"
                )

                # retry 404 do MAX_RETRIES – CEPIK bywa niestabilny
                if attempt < MAX_RETRIES:
                    logging.warning(
                        f"retrying_404 woj={woj} type={vehicle_type} "
                        f"year={year} month={month} page={page} attempt={attempt}"
                    )
                    time.sleep(1.0 * attempt)  # 1s -> 2s
                    continue

                # dopiero po 3 nieudanych próbach traktujemy to jako "NO_DATA"
                return [], False, latency, current_limit, "404_NO_DATA"

            resp.raise_for_status()

            if attempt > 1:
                content_len_header = resp.headers.get("Content-Length")
                raw_len = len(raw_data)
                logging.info(
                    f"page_diag_retry woj={woj} type={vehicle_type} year={year} month={month} "
                    f"page={page} status={resp.status_code} header_len={content_len_header} "
                    f"raw_len={raw_len} attempt={attempt}"
                )

            data = json.loads(raw_data)

            if attempt > 1:
                logging.info(
                    f"retry_success woj={woj} type={vehicle_type} page={page} "
                    f"attempts={attempt} elapsed={round(latency, 2)}s"
                )

            rows = []
            for x in data.get("data", []):
                rec = dict(x.get("attributes", {}))
                rec["id"] = x.get("id")
                rec["month"] = f"{month:02}"
                rec["year"] = int(year)
                rec["wojewodztwo"] = woj
                rec["snapshot_date"] = SNAPSHOT_DATE
                rec["ingest_ts"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                rec["source_page"] = page
                rows.append(rec)

            has_next = "next" in (data.get("links") or {})
            return rows, has_next, latency, current_limit, None

        except json.JSONDecodeError as e:
            header_len = None
            raw_len = None
            status = None
            if resp is not None:
                header_len = resp.headers.get("Content-Length")
                status = resp.status_code
            if raw_data is not None:
                raw_len = len(raw_data)

            logging.warning(
                f"json_error woj={woj} type={vehicle_type} page={page} "
                f"attempt={attempt} status={status} header_len={header_len} "
                f"raw_len={raw_len} err={e}"
            )

            time.sleep(2)
            if attempt == MAX_RETRIES:
                return None, None, None, current_limit, "REQUEST_FAILED"

        except Exception as e:
            header_len = None
            raw_len = None
            status = None
            if resp is not None:
                header_len = resp.headers.get("Content-Length")
                status = resp.status_code
            if raw_data is not None:
                raw_len = len(raw_data)

            logging.warning(
                f"request_error woj={woj} type={vehicle_type} page={page} "
                f"attempt={attempt} status={status} header_len={header_len} "
                f"raw_len={raw_len} err={e}"
            )

            time.sleep(2)
            if attempt == MAX_RETRIES:
                return None, None, None, current_limit, "REQUEST_FAILED"

    return None, None, None, current_limit, "REQUEST_FAILED_MAX"

def fetch_api_count_for_segment(vehicle_type, woj, year, month):
    """
    Sanity call z retry semantycznym.
    NIE uznajemy api_count za None po pierwszym failu.
    """
    date_od = f"{year}{month:02}01"
    date_do = f"{year}{month:02}31"

    params = {
        "wojewodztwo": woj,
        "data-od": date_od,
        "data-do": date_do,
        "typ-daty": TYP_DATY,
        "filter[rodzaj-pojazdu]": vehicle_type,
        "tylko-zarejestrowane": "true",
        "limit": "100",
        "pokaz-wszystkie-pola": "false",
        "fields": ",".join(FIELDS_API),
    }
    url = build_api_url(params)

    for attempt in range(1, SANITY_RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            meta = data.get("meta") or {}
            api_count = (
                meta.get("count")
                or meta.get("Count")
                or meta.get("total")
                or meta.get("Total")
            )

            if api_count is not None:
                api_count = int(api_count)
                logging.info(
                    f"[SANITY_OK] woj={woj} type={vehicle_type} "
                    f"year={year} month={month} api_count={api_count}"
                )
                return api_count, url

            raise ValueError("api_count missing in meta")

        except Exception as e:
            sleep_s = (SANITY_BACKOFF_BASE ** (attempt - 1)) + random.random()
            logging.warning(
                f"[SANITY_RETRY] woj={woj} type={vehicle_type} "
                f"year={year} month={month} attempt={attempt}/{SANITY_RETRIES} err={e}"
            )
            time.sleep(sleep_s)

    logging.error(
        f"[SANITY_FAILED] woj={woj} type={vehicle_type} year={year} month={month}"
    )
    return None, url

# =====================================================
#                MAIN LOGIC PER VEHICLE TYPE
# =====================================================

def run_vehicle_type(vehicle_type):
    """
    Dla danego typu pojazdu iteruje po latach, miesiącach i województwach,
    pobiera wszystkie strony i zapisuje dane do S3 w kawałkach.
    Zwraca:
      - all_logs: statystyki
      - missed_pages: lista nieudanych stron
    """
    start_t = time.time()
    all_logs = []
    missed_pages = []
    type_safe = slug_type(vehicle_type)

    for year in YEARS:
        for month in range(MONTH_START, MONTH_END + 1):
            for woj in WOJEWODZTWA:

                total_rows = 0
                total_files = 0
                buffer = []
                page = 1

                # =========================
                # PAGINATION LOOP
                # =========================
                while True:
                    rows, has_next, latency, used_limit, err_type = fetch_page(
                        vehicle_type, woj, year, month, page
                    )

                    if rows is None:
                        # krytyczny błąd po MAX_RETRIES
                        missed_pages.append({
                            "snapshot_date": SNAPSHOT_DATE,
                            "year": year,
                            "month": f"{month:02}",
                            "vehicle_type": vehicle_type,
                            "woj": woj,
                            "page": page,
                            "limit_used": used_limit,
                            "error_type": err_type or "REQUEST_FAILED",
                            "link_do_strony": build_api_url({
                                "wojewodztwo": woj,
                                "data-od": f"{year}{month:02}01",
                                "data-do": f"{year}{month:02}31",
                                "typ-daty": TYP_DATY,
                                "filter[rodzaj-pojazdu]": vehicle_type,
                                "tylko-zarejestrowane": "true",
                                "limit": str(used_limit),
                                "page": str(page),
                                "fields": ",".join(FIELDS_API),
                            })
                        })
                        break

                    if err_type == "404_NO_DATA":
                        break

                    if not rows:
                        break

                    buffer.extend(rows)
                    total_rows += len(rows)

                    if page % FLUSH_EVERY_PAGES == 0 or not has_next:
                        df = pd.DataFrame(buffer)
                        fetch_uuid = uuid.uuid4().hex[:8]
                        key = (
                            f"{BASE_PREFIX}/archive/"
                            f"snapshot_date={SNAPSHOT_DATE}/"
                            f"year={year}/month={month:02}/type={type_safe}/wojewodztwo={woj}/"
                            f"part-{int(time.time()*1000)}-{fetch_uuid}.parquet.gz"
                        )
                        save_dataframe_to_s3(df, key)
                        total_files += 1
                        buffer.clear()

                    if not has_next:
                        break

                    page += 1

                # =========================
                # SANITY CHECK – API COUNT
                # =========================
                api_count, api_link = fetch_api_count_for_segment(
                    vehicle_type, woj, year, month
                )

                # CEPIK glitch: count == limit + 1 (np. 101, 451)
                if api_count is not None and api_count == LIMIT + 1:
                    logging.warning(
                        f"[API_COUNT_GLITCH] woj={woj} type={vehicle_type} "
                        f"year={year} month={month} api_count={api_count}"
                    )
                    api_count = None

                # =========================
                # REPORT LOGIC
                # =========================
                if api_count is None:
                    counts_match = "UNKNOWN"
                    if total_rows > 0:
                        notes = "API_COUNT_GLITCH"
                    else:
                        notes = "NO_DATA"
                else:
                    counts_match = "YES" if api_count == total_rows else "NO"
                    notes = "OK" if counts_match == "YES" else "FAILED_INCOMPLETE"

                all_logs.append({
                    "snapshot_date": SNAPSHOT_DATE,
                    "year": year,
                    "month": f"{month:02}",
                    "vehicle_type": vehicle_type,
                    "woj": woj,
                    "file_count": total_files,
                    "total_raw_rows": total_rows,
                    "runtime_sec": int(time.time() - start_t),
                    "limit_used": LIMIT,
                    "api_count": api_count,
                    "counts_match": counts_match,
                    "api_link": api_link,
                    "run_date": RUN_DATE,
                    "notes": notes,
                })

    return all_logs, missed_pages

# =====================================================
#                         MAIN
# =====================================================

def main():
    logging.info(
        f"mode=PROD-SEQUENTIAL snapshot_date={SNAPSHOT_DATE} "
        f"years={YEARS} months={MONTH_START}-{MONTH_END} "
        f"woj={','.join(WOJEWODZTWA)} worker_id={WORKER_ID}"
    )

    all_reports = []
    all_missed = []

    for vehicle_type in VEHICLE_TYPES:
        logging.info(f"=== START vehicle_type={vehicle_type} ===")
        reports, missed = run_vehicle_type(vehicle_type)
        all_reports.extend(reports)
        all_missed.extend(missed)
        logging.info(f"=== DONE vehicle_type={vehicle_type} ===")

    df_all = pd.DataFrame(all_reports)
    report_key = f"reports/report-{SNAPSHOT_DATE}.xlsx"

    # Tworzymy plik XLSX w pamięci z dwoma arkuszami
    import io
    excel_buffer = io.BytesIO()

    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        # Arkusz 1: summary
        df_all.to_excel(writer, sheet_name="summary", index=False)

        # Arkusz 2: missed_pages (jeśli są)
        if all_missed:
            df_missed = pd.DataFrame(all_missed)
            df_missed.to_excel(writer, sheet_name="missed_pages", index=False)

    excel_buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=report_key, Body=excel_buffer.getvalue())
    logging.info(f"report_xlsx s3://{BUCKET}/{report_key}")

    missed_pages_count = len(all_missed)
    completeness_key = f"reports/completeness-{SNAPSHOT_DATE}.json"
    completeness_doc = {
        "snapshot_date": SNAPSHOT_DATE,
        "status": "INCOMPLETE" if missed_pages_count > 0 else "COMPLETE",
        "missed_pages_count": missed_pages_count,
        "report_key": report_key,
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    s3.put_object(
        Bucket=BUCKET,
        Key=completeness_key,
        Body=json.dumps(completeness_doc, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    logging.info(f"completeness_json s3://{BUCKET}/{completeness_key} -> {completeness_doc['status']}")

    if all_missed:
        logging.warning(f"missed_pages_count={missed_pages_count} (zapisane w arkuszu 'missed_pages')")
    else:
        logging.info("missed_pages=0")

    logging.info("done=1 status=success")

if __name__ == "__main__":
    main()
