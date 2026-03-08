import time
from typing import Dict, List, Optional, Tuple

import boto3
import pandas as pd

# ============================================
#    KONFIGURACJA ATHENY
# ============================================

AWS_REGION = "eu-north-1"
ATHENA_DB = "motobi_cepik"
ATHENA_OUTPUT = "s3://motointel-cepik-raw-prod/athena-results/"
ATHENA_TABLE = "motobi_prod_latest"

DIM_REGION_TABLE = "motobi_cepik.dim_region"
DIM_BRAND_TABLE = "motobi_cepik.dim_brand"
DIM_METADATA_TABLE = "motobi_cepik.dim_metadata"
DIM_VEHICLE_SUBTYPE_TABLE = "motobi_cepik.dim_vehicle_subtype"


TOP_BRAND_MOM_TABLE = "motobi_cepik_hist.top_brand_mom_snapshot"

# ====== POPULACJA ======
POP_TABLE = "motobi_cepik.population"
POP_VOIV_COL = "wojewodztwo"
POP_COUNTY_COL = "powiat"
POP_VALUE_COL = "liczba_ludnosci"

# ====== POWIAT MAPPING (kanoniczny layer) ======
# MAP-CONTRACT: tabela normalizująca 3 źródła nazw + display name
POWIAT_MAPPING_TABLE = "motobi_cepik.dim_powiat_mapping"
POWIAT_MAP_COL_POP = "population_powiat_raw"
POWIAT_MAP_COL_GEO = "geojson_name_raw"
POWIAT_MAP_COL_CEPIK = "cepik_powiat_raw"
POWIAT_MAP_COL_DISPLAY = "name_to_display"

athena = boto3.client("athena", region_name=AWS_REGION)

FUEL_BUCKETS_BY_TYPE = {
    "samochod_osobowy": [
        "BENZYNA",
        "OLEJ NAPĘDOWY",
        "ENERGIA ELEKTRYCZNA",
        "INNE",
    ],
    "motocykl": [
        "BENZYNA",
        "ENERGIA ELEKTRYCZNA",
        "INNE",
    ],
    "motorower": [
        "BENZYNA",
        "MIESZANKA PALIWO-OLEJ",
        "ENERGIA ELEKTRYCZNA",
        "INNE",
    ],
}

FUEL_BUCKET_COLORS = {
    "BENZYNA": "#1F77B4",
    "OLEJ NAPĘDOWY": "#FF7F0E",
    "ENERGIA ELEKTRYCZNA": "#2CA02C",
    "MIESZANKA PALIWO-OLEJ": "#9467BD",
    "INNE": "#B0B0B0",
}

VEHICLE_TYPE_RAW_MAP = {
    "samochod_osobowy": "SAMOCHÓD OSOBOWY",
    "motocykl": "MOTOCYKL",
    "motorower": "MOTOROWER",
}

REG_DATE_EXPR = (
    "date_parse("
    "cast(year as varchar) || '-' || lpad(cast(month as varchar), 2, '0') || '-01',"
    "'%Y-%m-%d'"
    ")"
)

def map_fuel_to_bucket(vehicle_type_expr: str, raw_fuel_expr: str) -> str:
    return f"""
    CASE
        WHEN {vehicle_type_expr} = 'samochod_osobowy' THEN
            CASE
                WHEN {raw_fuel_expr} IN ('BENZYNA','OLEJ NAPĘDOWY','ENERGIA ELEKTRYCZNA')
                    THEN {raw_fuel_expr}
                ELSE 'INNE'
            END
        WHEN {vehicle_type_expr} = 'motocykl' THEN
            CASE
                WHEN {raw_fuel_expr} IN ('BENZYNA','ENERGIA ELEKTRYCZNA')
                    THEN {raw_fuel_expr}
                ELSE 'INNE'
            END
        WHEN {vehicle_type_expr} = 'motorower' THEN
            CASE
                WHEN {raw_fuel_expr} IN ('BENZYNA','MIESZANKA PALIWO-OLEJ','ENERGIA ELEKTRYCZNA')
                    THEN {raw_fuel_expr}
                ELSE 'INNE'
            END
        ELSE 'INNE'
    END
    """

FUEL_BUCKET_EXPR = map_fuel_to_bucket("type", '"rodzaj-paliwa"')

# ============================================
#    ATHENA HELPERS
# ============================================

def run_athena_query(sql: str, timeout: int = 300) -> str:
    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DB},
    )
    qid = response["QueryExecutionId"]

    start = time.time()
    while True:
        res = athena.get_query_execution(QueryExecutionId=qid)
        state = res["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = res["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {qid} failed: {state} - {reason}")
        if time.time() - start > timeout:
            raise TimeoutError(f"Athena query {qid} timed out after {timeout}s.")

        time.sleep(0.5)

    return qid

def fetch_athena_result_as_df(qid: str) -> pd.DataFrame:
    paginator = athena.get_paginator("get_query_results")
    pages = paginator.paginate(QueryExecutionId=qid)

    columns = None
    rows = []

    for page in pages:
        for row in page["ResultSet"]["Rows"]:
            data = row["Data"]
            if columns is None:
                columns = [cell.get("VarCharValue", "") for cell in data]
                continue

            values = []
            for cell in data:
                values.append(cell.get("VarCharValue") if "VarCharValue" in cell else None)
            rows.append(values)

    if columns is None:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=columns)

# ============================================
#    DIMY – DO FILTRÓW
# ============================================

def dim_voivodeships() -> List[str]:
    sql = f"""
    SELECT DISTINCT voivodeship
    FROM {DIM_REGION_TABLE}
    WHERE voivodeship IS NOT NULL
    ORDER BY voivodeship
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if "voivodeship" not in df:
        return []
    return [v for v in df["voivodeship"].dropna().astype(str).tolist() if v]

def dim_all_counties() -> List[str]:
    sql = f"""
    SELECT DISTINCT county
    FROM {DIM_REGION_TABLE}
    WHERE county IS NOT NULL
    ORDER BY county
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if "county" not in df:
        return []
    return [c for c in df["county"].dropna().astype(str).tolist() if c]

def dim_counties_for(voivs: List[str]) -> List[str]:
    if not voivs:
        return dim_all_counties()

    voiv_list = ",".join(f"'{v}'" for v in voivs)
    sql = f"""
    SELECT DISTINCT county
    FROM {DIM_REGION_TABLE}
    WHERE voivodeship IN ({voiv_list})
      AND county IS NOT NULL
    ORDER BY county
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if "county" not in df:
        return []
    return [c for c in df["county"].dropna().astype(str).tolist() if c]

def dim_brands() -> List[str]:
    sql = f"""
    SELECT DISTINCT brand
    FROM {DIM_BRAND_TABLE}
    WHERE brand IS NOT NULL
    ORDER BY brand
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if "brand" not in df:
        return []
    return [b for b in df["brand"].dropna().astype(str).tolist() if b]

def dim_brands_for_vehicle_types(vehicle_types: List[str]) -> List[str]:
    if not vehicle_types:
        return []
    vehicle_list = ",".join(f"'{v}'" for v in vehicle_types)
    sql = f"""
    SELECT DISTINCT marka AS brand
    FROM {ATHENA_TABLE}
    WHERE type IN ({vehicle_list})
      AND marka IS NOT NULL
    ORDER BY marka
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if "brand" not in df:
        return []
    return [b for b in df["brand"].dropna().astype(str).tolist() if b]

def dim_models_for(brands: List[str]) -> List[str]:
    if not brands:
        return []
    brand_list = ",".join(f"'{b}'" for b in brands)
    sql = f"""
    SELECT DISTINCT model
    FROM {DIM_BRAND_TABLE}
    WHERE brand IN ({brand_list})
      AND model IS NOT NULL
    ORDER BY model
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if "model" not in df:
        return []
    return [m for m in df["model"].dropna().astype(str).tolist() if m]

def dim_models_for_vehicle_and_brand(
    vehicle_types: List[str],
    brands: List[str],
    vehicle_subtypes: Optional[List[str]] = None,
) -> List[str]:
    if not vehicle_types or not brands:
        return []
    vehicle_list = ",".join(f"'{v}'" for v in vehicle_types)
    brand_list = ",".join(f"'{b}'" for b in brands)
    subtype_clause = ""
    if vehicle_subtypes:
        subtype_vals = ",".join(f"'{v}'" for v in vehicle_subtypes)
        raw_types = [VEHICLE_TYPE_RAW_MAP[v] for v in vehicle_types if v in VEHICLE_TYPE_RAW_MAP]
        if raw_types:
            raw_vals = ",".join(f"'{v}'" for v in raw_types)
            subtype_clause = f"""
      AND "podrodzaj-pojazdu" IN (
          SELECT subtype_raw
          FROM {DIM_VEHICLE_SUBTYPE_TABLE}
          WHERE subtype_simplified IN ({subtype_vals})
            AND vehicle_type_raw IN ({raw_vals})
            AND is_subtype_available = true
      )
            """
    sql = f"""
    SELECT DISTINCT model
    FROM {ATHENA_TABLE}
    WHERE type IN ({vehicle_list})
      AND marka IN ({brand_list})
      {subtype_clause}
      AND model IS NOT NULL
    ORDER BY model
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if "model" not in df:
        return []
    return [m for m in df["model"].dropna().astype(str).tolist() if m]

def dim_models_for_filters(filters: Dict) -> List[str]:
    filters = dict(filters)
    filters.pop("models", None)
    where_clause = _build_region_where(filters)
    sql = f"""
    SELECT DISTINCT model
    FROM {ATHENA_TABLE}
    WHERE {where_clause}
      AND model IS NOT NULL
    ORDER BY model
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if "model" not in df:
        return []
    return [m for m in df["model"].dropna().astype(str).tolist() if m]

def dim_origin() -> List[str]:
    sql = f"""
    SELECT DISTINCT origin_raw
    FROM {DIM_METADATA_TABLE}
    WHERE origin_raw IS NOT NULL
    ORDER BY origin_raw
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if "origin_raw" not in df:
        return []
    return [o for o in df["origin_raw"].dropna().astype(str).tolist() if o]

def dim_alt_fuel() -> List[str]:
    sql = f"""
    SELECT DISTINCT alt_fuel_raw
    FROM {DIM_METADATA_TABLE}
    WHERE alt_fuel_raw IS NOT NULL
    ORDER BY alt_fuel_raw
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if "alt_fuel_raw" not in df:
        return []
    return [a for a in df["alt_fuel_raw"].dropna().astype(str).tolist() if a]

def dim_vehicle_subtypes(vehicle_types: Optional[List[str]]) -> List[str]:
    if not vehicle_types:
        return []

    raw_types = [VEHICLE_TYPE_RAW_MAP[v] for v in vehicle_types if v in VEHICLE_TYPE_RAW_MAP]
    if not raw_types:
        return []

    raw_list = ",".join(f"'{v}'" for v in raw_types)
    sql = f"""
    SELECT DISTINCT subtype_simplified AS vehicle_subtype
    FROM {DIM_VEHICLE_SUBTYPE_TABLE}
    WHERE vehicle_type_raw IN ({raw_list})
      AND is_subtype_available = true
      AND subtype_simplified IS NOT NULL
    ORDER BY subtype_simplified
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if "vehicle_subtype" not in df:
        return []
    return [s for s in df["vehicle_subtype"].dropna().astype(str).tolist() if s]

def dim_fuel_buckets(vehicle_types: Optional[List[str]]) -> List[str]:
    if not vehicle_types:
        return []

    vehicle_set = set(vehicle_types)
    if vehicle_set == {"motocykl", "motorower"}:
        return FUEL_BUCKETS_BY_TYPE["motorower"]

    if vehicle_set == {"samochod_osobowy"}:
        return FUEL_BUCKETS_BY_TYPE["samochod_osobowy"]
    if vehicle_set == {"motocykl"}:
        return FUEL_BUCKETS_BY_TYPE["motocykl"]
    if vehicle_set == {"motorower"}:
        return FUEL_BUCKETS_BY_TYPE["motorower"]

    if "samochod_osobowy" in vehicle_set:
        return FUEL_BUCKETS_BY_TYPE["samochod_osobowy"]

    return FUEL_BUCKETS_BY_TYPE["motorower"]

# ============================================
#    vehicle_types helper
# ============================================

def _get_vehicle_types(filters: Dict) -> Optional[List[str]]:
    vt = filters.get("vehicle_types", None)
    if vt is None:
        vt = filters.get("vehicle_type", None)

    if vt is None:
        return None
    if isinstance(vt, list):
        return vt
    if isinstance(vt, str):
        return [vt]
    return None

# ============================================
#    WHERE BUILDER
# ============================================

def _build_region_where(filters: Dict) -> str:
    where: List[str] = []

    voivs = filters.get("voivodeships") or []
    if voivs:
        vals = ",".join(f"'{v}'" for v in voivs)
        where.append(f'"rejestracja-wojewodztwo" IN ({vals})')

    counties = filters.get("counties") or []
    if counties:
        vals = ",".join(f"'{c}'" for c in counties)
        where.append(f'"rejestracja-powiat" IN ({vals})')

    brands = filters.get("brands") or []
    if brands:
        vals = ",".join(f"'{b}'" for b in brands)
        where.append(f"marka IN ({vals})")

    models = filters.get("models") or []
    if models:
        vals = ",".join(f"'{m}'" for m in models)
        where.append(f"model IN ({vals})")

    vehicle_types = _get_vehicle_types(filters)
    if vehicle_types:
        vals = ",".join(f"'{v}'" for v in vehicle_types)
        where.append(f"type IN ({vals})")

    vehicle_subtypes = filters.get("vehicle_subtype") or []
    if vehicle_subtypes:
        subtype_vals = ",".join(f"'{v}'" for v in vehicle_subtypes)
        raw_types = [VEHICLE_TYPE_RAW_MAP[v] for v in (vehicle_types or []) if v in VEHICLE_TYPE_RAW_MAP]
        if raw_types:
            raw_vals = ",".join(f"'{v}'" for v in raw_types)
            where.append(
                f"\"podrodzaj-pojazdu\" IN ("
                f"SELECT subtype_raw FROM {DIM_VEHICLE_SUBTYPE_TABLE} "
                f"WHERE subtype_simplified IN ({subtype_vals}) "
                f"AND vehicle_type_raw IN ({raw_vals}) "
                "AND is_subtype_available = true"
                ")"
            )

    fuels = filters.get("fuel_multi") or []

    if fuels:
        vals = ",".join(f"'{f}'" for f in fuels)
        where.append(f"{FUEL_BUCKET_EXPR} IN ({vals})")

    origin = filters.get("origin")
    if origin and origin != "Wszystkie":
        where.append(f'"pochodzenie-pojazdu" = \'{origin}\'')

    alt_fuel = filters.get("alt_fuel")
    if alt_fuel and alt_fuel != "Wszystkie":
        if alt_fuel == "Brak":
            where.append(
                '('
                '"paliwo_alternatywne" IS NULL '
                "OR \"paliwo_alternatywne\" IN ('BRAK','NIE DOTYCZY','')"
                ")"
            )
        else:
            where.append(f"\"paliwo_alternatywne\" = '{alt_fuel}'")

    prod_year: Optional[Tuple[int, int]] = filters.get("prod_year")
    if prod_year:
        y1, y2 = prod_year
        where.append(f"rok_produkcji BETWEEN {int(y1)} AND {int(y2)}")

    reg_year: Optional[Tuple[int, int]] = filters.get("reg_year")
    if reg_year:
        r1, r2 = reg_year
        where.append(f"year BETWEEN {int(r1)} AND {int(r2)}")

    fuels_for_ev = fuels or []
    includes_ev = True if not fuels_for_ev else ("ENERGIA ELEKTRYCZNA" in fuels_for_ev)

    enable_power = bool(filters.get("enable_power_filter"))
    power_range: Optional[Tuple[float, float]] = filters.get("power_range")
    motorower_only = (vehicle_types == ["motorower"]) if vehicle_types else False

    if enable_power and (not motorower_only) and power_range:
        pmin, pmax = power_range
        if includes_ev:
            where.append(
                '('
                "\"rodzaj-paliwa\" = 'ENERGIA ELEKTRYCZNA' "
                "OR ("
                "\"rodzaj-paliwa\" <> 'ENERGIA ELEKTRYCZNA' "
                'AND "moc-netto-silnika" IS NOT NULL '
                f'AND CAST("moc-netto-silnika" AS DOUBLE) BETWEEN {pmin} AND {pmax}'
                ")"
                ")"
            )
        else:
            where.append(
                '"moc-netto-silnika" IS NOT NULL '
                f'AND CAST("moc-netto-silnika" AS DOUBLE) BETWEEN {pmin} AND {pmax}'
            )

    enable_capacity = bool(filters.get("enable_capacity_filter"))
    capacity_range: Optional[Tuple[float, float]] = filters.get("capacity_range")

    if enable_capacity and (not motorower_only) and capacity_range:
        cmin, cmax = capacity_range
        if includes_ev:
            where.append(
                "("
                "\"rodzaj-paliwa\" = 'ENERGIA ELEKTRYCZNA' "
                "OR ("
                "\"rodzaj-paliwa\" <> 'ENERGIA ELEKTRYCZNA' "
                "AND \"pojemnosc-skokowa-silnika\" IS NOT NULL "
                f"AND CAST(\"pojemnosc-skokowa-silnika\" AS DOUBLE) BETWEEN {cmin} AND {cmax} "
                ")"
                ")"
            )
        else:
            where.append(
                "\"pojemnosc-skokowa-silnika\" IS NOT NULL "
                f"AND CAST(\"pojemnosc-skokowa-silnika\" AS DOUBLE) BETWEEN {cmin} AND {cmax}"
            )

    include_rhd = bool(filters.get("include_rhd", False))
    if not include_rhd:
        where.append(
            '('
            '"kierownica-po-prawej-stronie" IS NULL '
            "OR \"kierownica-po-prawej-stronie\" = '' "
            "OR \"kierownica-po-prawej-stronie\" <> 'False'"
            ')'
        )

    if not where:
        return "1=1"
    return " AND ".join(where)

def _build_national_where(filters: Dict) -> str:
    f2 = dict(filters)
    f2["voivodeships"] = []
    f2["counties"] = []
    return _build_region_where(f2)

def _build_region_snapshot_trend_query_parts(filters: Dict) -> Tuple[str, str]:
    where: List[str] = []
    joins = ""

    vehicle_types = _get_vehicle_types(filters)
    if vehicle_types:
        raw_types = [VEHICLE_TYPE_RAW_MAP[v] for v in vehicle_types if v in VEHICLE_TYPE_RAW_MAP]
        if raw_types:
            vals = ",".join(f"'{v}'" for v in raw_types)
            where.append(f'f."rodzaj-pojazdu" IN ({vals})')

    voivs = filters.get("voivodeships") or []
    if voivs:
        vals = ",".join(f"'{v}'" for v in voivs)
        where.append(f'f."rejestracja-wojewodztwo" IN ({vals})')

    counties = filters.get("counties") or []
    if counties:
        vals = ",".join(f"'{c}'" for c in counties)
        where.append(f'f."rejestracja-powiat" IN ({vals})')

    brands = filters.get("brands") or []
    if brands:
        vals = ",".join(f"'{b}'" for b in brands)
        where.append(f"f.marka IN ({vals})")

    models = filters.get("models") or []
    if models:
        vals = ",".join(f"'{m}'" for m in models)
        where.append(f"f.model IN ({vals})")

    vehicle_subtypes = filters.get("vehicle_subtype") or []
    if vehicle_subtypes:
        subtype_vals = ",".join(f"'{v}'" for v in vehicle_subtypes)
        joins = f"""
    LEFT JOIN {DIM_VEHICLE_SUBTYPE_TABLE} d
      ON UPPER(TRIM(f."podrodzaj-pojazdu")) = UPPER(TRIM(d.subtype_raw))
     AND UPPER(TRIM(f."rodzaj-pojazdu")) = UPPER(TRIM(d.vehicle_type_raw))
        """
        where.append("d.is_subtype_available = true")
        where.append(f"d.subtype_simplified IN ({subtype_vals})")

    origin = filters.get("origin")
    if origin and origin != "Wszystkie":
        where.append(f'f."pochodzenie-pojazdu" = \'{origin}\'')

    prod_year: Optional[Tuple[int, int]] = filters.get("prod_year")
    if prod_year:
        y1, y2 = prod_year
        where.append(f"f.rok_produkcji BETWEEN {int(y1)} AND {int(y2)}")

    reg_year: Optional[Tuple[int, int]] = filters.get("reg_year")
    if reg_year:
        r1, r2 = reg_year
        where.append(f"f.year BETWEEN {int(r1)} AND {int(r2)}")

    fuels = filters.get("fuel_multi") or []
    if fuels:
        vals = ",".join(f"'{f}'" for f in fuels)
        where.append(f'f."rodzaj-paliwa" IN ({vals})')

    alt_fuel = filters.get("alt_fuel")
    if alt_fuel and alt_fuel != "Wszystkie":
        if alt_fuel == "Brak":
            where.append(
                '('
                'f."paliwo_alternatywne" IS NULL '
                "OR f.\"paliwo_alternatywne\" IN ('BRAK','NIE DOTYCZY','')"
                ")"
            )
        else:
            where.append(f'f."paliwo_alternatywne" = \'{alt_fuel}\'')

    fuels_for_ev = fuels or []
    includes_ev = True if not fuels_for_ev else ("ENERGIA ELEKTRYCZNA" in fuels_for_ev)

    enable_power = bool(filters.get("enable_power_filter"))
    power_range: Optional[Tuple[float, float]] = filters.get("power_range")
    motorower_only = (vehicle_types == ["motorower"]) if vehicle_types else False

    if enable_power and (not motorower_only) and power_range:
        pmin, pmax = power_range
        if includes_ev:
            where.append(
                '('
                "f.\"rodzaj-paliwa\" = 'ENERGIA ELEKTRYCZNA' "
                "OR ("
                "f.\"rodzaj-paliwa\" <> 'ENERGIA ELEKTRYCZNA' "
                'AND f."moc-netto-silnika" IS NOT NULL '
                f'AND CAST(f."moc-netto-silnika" AS DOUBLE) BETWEEN {pmin} AND {pmax}'
                ")"
                ")"
            )
        else:
            where.append(
                'f."moc-netto-silnika" IS NOT NULL '
                f'AND CAST(f."moc-netto-silnika" AS DOUBLE) BETWEEN {pmin} AND {pmax}'
            )

    enable_capacity = bool(filters.get("enable_capacity_filter"))
    capacity_range: Optional[Tuple[float, float]] = filters.get("capacity_range")

    if enable_capacity and (not motorower_only) and capacity_range:
        cmin, cmax = capacity_range
        if includes_ev:
            where.append(
                "("
                "f.\"rodzaj-paliwa\" = 'ENERGIA ELEKTRYCZNA' "
                "OR ("
                "f.\"rodzaj-paliwa\" <> 'ENERGIA ELEKTRYCZNA' "
                'AND f."pojemnosc-skokowa-silnika" IS NOT NULL '
                f'AND CAST(f."pojemnosc-skokowa-silnika" AS DOUBLE) BETWEEN {cmin} AND {cmax} '
                ")"
                ")"
            )
        else:
            where.append(
                'f."pojemnosc-skokowa-silnika" IS NOT NULL '
                f'AND CAST(f."pojemnosc-skokowa-silnika" AS DOUBLE) BETWEEN {cmin} AND {cmax}'
            )

    include_rhd = bool(filters.get("include_rhd", False))
    if not include_rhd:
        where.append(
            '('
            'f."kierownica-po-prawej-stronie" IS NULL '
            "OR f.\"kierownica-po-prawej-stronie\" = '' "
            "OR f.\"kierownica-po-prawej-stronie\" <> 'False'"
            ')'
        )

    if not where:
        return joins, "1=1"
    return joins, " AND ".join(where)

# ============================================
#    KPI
# ============================================

def load_region_kpis(filters: Dict) -> Dict[str, float]:
    where_clause = _build_region_where(filters)

    sql = f"""
    WITH base AS (
        SELECT
            total_count,
            CAST(rok_produkcji AS integer) AS prod_year,
            {REG_DATE_EXPR} AS registration_date,
            {FUEL_BUCKET_EXPR} AS fuel_bucket,
            CASE
                WHEN "kierownica-po-prawej-stronie" = 'False'
                 AND "kierownica-po-prawej-stronie-pierwotnie" = 'False'
                THEN 1
                ELSE 0
            END AS is_rhd
        FROM {ATHENA_TABLE}
        WHERE {where_clause}
    )
    SELECT
        SUM(total_count) AS total_reg,
        SUM(CASE WHEN fuel_bucket = 'ENERGIA ELEKTRYCZNA' THEN total_count ELSE 0 END) AS ev_reg,
        CAST(SUM((year(current_date) - prod_year) * total_count) AS DOUBLE)
            / NULLIF(SUM(total_count), 0) AS avg_age_years,
        SUM( date_diff('day', registration_date, current_date) / 365.25 * total_count )
            / NULLIF(SUM(total_count), 0) AS avg_time_owned_years,
        SUM(is_rhd * total_count) AS rhd_count
    FROM base
    """

    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)

    if df.empty:
        return dict(total_reg=0.0, ev_reg=0.0, avg_age_years=0.0, avg_time_owned_years=0.0, rhd_count=0.0)

    def _to_float(col: str) -> float:
        val = pd.to_numeric(df[col].iloc[0], errors="coerce")
        return float(val) if pd.notna(val) else 0.0

    return dict(
        total_reg=_to_float("total_reg"),
        ev_reg=_to_float("ev_reg"),
        avg_age_years=_to_float("avg_age_years"),
        avg_time_owned_years=_to_float("avg_time_owned_years"),
        rhd_count=_to_float("rhd_count"),
    )

# ============================================
#    MIKS / TRENDY
# ============================================

def load_region_fuel_mix(filters: Dict) -> pd.DataFrame:
    where_clause = _build_region_where(filters)
    sql = f"""
    SELECT
        {FUEL_BUCKET_EXPR} AS fuel_bucket,
        SUM(total_count) AS total_count
    FROM {ATHENA_TABLE}
    WHERE {where_clause}
    GROUP BY {FUEL_BUCKET_EXPR}
    ORDER BY total_count DESC
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if df.empty:
        return df
    df["total_count"] = pd.to_numeric(df["total_count"], errors="coerce")
    return df

def load_region_fuel_trend(filters: Dict) -> pd.DataFrame:
    where_clause = _build_region_where(filters)
    sql = f"""
    SELECT
        {REG_DATE_EXPR} AS registration_date,
        {FUEL_BUCKET_EXPR} AS fuel_bucket,
        SUM(total_count) AS total_count
    FROM {ATHENA_TABLE}
    WHERE {where_clause}
    GROUP BY {REG_DATE_EXPR}, {FUEL_BUCKET_EXPR}
    ORDER BY registration_date
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if df.empty:
        return df
    df["total_count"] = pd.to_numeric(df["total_count"], errors="coerce")
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    return df

def load_region_origin_mix(filters: Dict) -> pd.DataFrame:
    where_clause = _build_region_where(filters)
    sql = f"""
    SELECT
        COALESCE("pochodzenie-pojazdu", 'Brak danych') AS origin,
        SUM(total_count) AS total_count
    FROM {ATHENA_TABLE}
    WHERE {where_clause}
    GROUP BY COALESCE("pochodzenie-pojazdu", 'Brak danych')
    ORDER BY total_count DESC
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if df.empty:
        return df
    df["total_count"] = pd.to_numeric(df["total_count"], errors="coerce")
    return df

def load_region_origin_trend(filters: Dict) -> pd.DataFrame:
    where_clause = _build_region_where(filters)
    sql = f"""
    SELECT
        {REG_DATE_EXPR} AS registration_date,
        COALESCE("pochodzenie-pojazdu", 'Brak danych') AS origin,
        SUM(total_count) AS total_count
    FROM {ATHENA_TABLE}
    WHERE {where_clause}
    GROUP BY {REG_DATE_EXPR}, COALESCE("pochodzenie-pojazdu", 'Brak danych')
    ORDER BY registration_date
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if df.empty:
        return df
    df["total_count"] = pd.to_numeric(df["total_count"], errors="coerce")
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    return df

def load_region_snapshot_trend(filters: Dict) -> pd.DataFrame:
    join_clause, where_clause = _build_region_snapshot_trend_query_parts(filters)
    sql = f"""
    SELECT
        snapshot_month,
        SUM(total_count) AS total_count
    FROM motobi_cepik_hist.motobi_prod_snapshot_trend f
    {join_clause}
    WHERE {where_clause}
    GROUP BY snapshot_month
    ORDER BY snapshot_month
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if df.empty:
        return df
    df["total_count"] = pd.to_numeric(df["total_count"], errors="coerce")
    df["snapshot_month"] = pd.to_datetime(df["snapshot_month"], format="%Y-%m", errors="coerce")
    return df

def load_region_vehicle_subtype_mix(filters: Dict) -> pd.DataFrame:
    vehicle_types = _get_vehicle_types(filters)
    if not vehicle_types or vehicle_types != ["samochod_osobowy"]:
        return pd.DataFrame()

    raw_type = VEHICLE_TYPE_RAW_MAP.get("samochod_osobowy")
    where_clause = _build_region_where(filters)
    sql = f"""
    WITH subtype_map AS (
        SELECT subtype_raw, subtype_simplified
        FROM {DIM_VEHICLE_SUBTYPE_TABLE}
        WHERE vehicle_type_raw = '{raw_type}'
          AND is_subtype_available = true
    )
    SELECT
        m.subtype_simplified AS vehicle_subtype,
        SUM(f.total_count) AS total_count
    FROM {ATHENA_TABLE} f
    JOIN subtype_map m
      ON UPPER(TRIM(f."podrodzaj-pojazdu")) = UPPER(TRIM(m.subtype_raw))
    WHERE {where_clause}
    GROUP BY m.subtype_simplified
    ORDER BY total_count DESC
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if df.empty:
        return df
    df["total_count"] = pd.to_numeric(df["total_count"], errors="coerce")
    return df

def load_region_vehicle_subtype_trend(filters: Dict) -> pd.DataFrame:
    vehicle_types = _get_vehicle_types(filters)
    if not vehicle_types or vehicle_types != ["samochod_osobowy"]:
        return pd.DataFrame()

    raw_type = VEHICLE_TYPE_RAW_MAP.get("samochod_osobowy")
    where_clause = _build_region_where(filters)
    sql = f"""
    WITH subtype_map AS (
        SELECT subtype_raw, subtype_simplified
        FROM {DIM_VEHICLE_SUBTYPE_TABLE}
        WHERE vehicle_type_raw = '{raw_type}'
          AND is_subtype_available = true
    )
    SELECT
        {REG_DATE_EXPR} AS registration_date,
        m.subtype_simplified AS vehicle_subtype,
        SUM(f.total_count) AS total_count
    FROM {ATHENA_TABLE} f
    JOIN subtype_map m
      ON UPPER(TRIM(f."podrodzaj-pojazdu")) = UPPER(TRIM(m.subtype_raw))
    WHERE {where_clause}
    GROUP BY {REG_DATE_EXPR}, m.subtype_simplified
    ORDER BY registration_date
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if df.empty:
        return df
    df["total_count"] = pd.to_numeric(df["total_count"], errors="coerce")
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    return df

# ============================================
#    Ranking (v2: modele = marka+model)
# ============================================

def load_region_top_brands(filters: Dict) -> pd.DataFrame:
    where_clause = _build_region_where(filters)
    sql = f"""
    SELECT
        marka AS brand,
        SUM(total_count) AS total_count
    FROM {ATHENA_TABLE}
    WHERE {where_clause}
    GROUP BY marka
    ORDER BY total_count DESC
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if df.empty:
        return df
    df["total_count"] = pd.to_numeric(df["total_count"], errors="coerce")
    return df

def load_region_top_models(filters: Dict) -> pd.DataFrame:
    where_clause = _build_region_where(filters)
    sql = f"""
    SELECT
        marka AS brand,
        model AS model,
        SUM(total_count) AS total_count
    FROM {ATHENA_TABLE}
    WHERE {where_clause}
    GROUP BY marka, model
    ORDER BY total_count DESC
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if df.empty:
        return df
    df["total_count"] = pd.to_numeric(df["total_count"], errors="coerce")
    return df

def load_top_brands_mom_latest() -> pd.DataFrame:
    sql = f"""
    WITH latest AS (
        SELECT MAX(snapshot_date) AS snapshot_date
        FROM {TOP_BRAND_MOM_TABLE}
    )
    SELECT
        t.brand,
        t.snapshot_date,
        t.vehicle_count,
        t.mom_delta_abs,
        t.mom_delta_pct
    FROM {TOP_BRAND_MOM_TABLE} t
    JOIN latest l
      ON t.snapshot_date = l.snapshot_date
    ORDER BY t.vehicle_count DESC, t.brand ASC
    """
    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if df.empty:
        return df

    for col in ("vehicle_count", "mom_delta_abs", "mom_delta_pct"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ============================================
#    INDEX vs PL (100=PL) – trend
# ============================================

def load_region_representation_index(filters: Dict) -> pd.DataFrame:
    where_region = _build_region_where(filters)
    where_pl = _build_national_where(filters)

    selected_voivs = filters.get("voivodeships") or []
    if selected_voivs:
        vals = ",".join(f"'{v}'" for v in selected_voivs)
        pop_region_sql = f"""
            SELECT SUM(CAST({POP_VALUE_COL} AS DOUBLE)) AS pop_region
            FROM {POP_TABLE}
            WHERE UPPER(TRIM({POP_VOIV_COL})) IN ({vals})
        """
    else:
        pop_region_sql = f"""
            SELECT SUM(CAST({POP_VALUE_COL} AS DOUBLE)) AS pop_region
            FROM {POP_TABLE}
        """

    pop_pl_sql = f"""
        SELECT SUM(CAST({POP_VALUE_COL} AS DOUBLE)) AS pop_pl
        FROM {POP_TABLE}
    """

    sql = f"""
    WITH
    pop_region AS ({pop_region_sql}),
    pop_pl AS ({pop_pl_sql}),
    region AS (
        SELECT
            {REG_DATE_EXPR} AS registration_date,
            SUM(total_count) AS region_count
        FROM {ATHENA_TABLE}
        WHERE {where_region}
        GROUP BY {REG_DATE_EXPR}
    ),
    pl AS (
        SELECT
            {REG_DATE_EXPR} AS registration_date,
            SUM(total_count) AS pl_count
        FROM {ATHENA_TABLE}
        WHERE {where_pl}
        GROUP BY {REG_DATE_EXPR}
    )
    SELECT
        COALESCE(region.registration_date, pl.registration_date) AS registration_date,
        CAST(region.region_count AS DOUBLE) AS region_count,
        CAST(pl.pl_count AS DOUBLE) AS pl_count,
        100.0
        * (
            (CAST(region.region_count AS DOUBLE) / NULLIF((SELECT pop_region FROM pop_region), 0.0))
            /
            (CAST(pl.pl_count AS DOUBLE) / NULLIF((SELECT pop_pl FROM pop_pl), 0.0))
          ) AS index_vs_pl
    FROM region
    FULL OUTER JOIN pl
        ON region.registration_date = pl.registration_date
    ORDER BY registration_date
    """

    qid = run_athena_query(sql)
    df = fetch_athena_result_as_df(qid)
    if df.empty:
        return df

    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    df["region_count"] = pd.to_numeric(df.get("region_count"), errors="coerce")
    df["pl_count"] = pd.to_numeric(df.get("pl_count"), errors="coerce")
    df["index_vs_pl"] = pd.to_numeric(df.get("index_vs_pl"), errors="coerce")
    return df

# ============================================
#    MAPA — woj + powiat (kanoniczne dane)
# ============================================

def load_map_region_summary(filters: dict, level: str) -> pd.DataFrame:
    """
    # ARCH-DECISION
    Jedyny punkt poboru danych pod mapę.
    app.py renderuje mapę, ale NIE liczy nic.

    # MAP-CONTRACT
    Zwraca DF z kolumnami:
      - region_name   (lowercase)  -> klucz do geojson "nazwa"
      - display_name  (string)     -> tooltip/UI (powiaty: name_to_display)
      - total_reg     (double)
      - reg_per_1000  (double)
      - index_vs_pl   (double)
    """
    assert level in ("county", "voivodeship")

    map_filters = dict(filters)
    map_filters["voivodeships"] = []
    map_filters["counties"] = []

    where_region = _build_region_where(map_filters)
    where_pl = _build_region_where(map_filters)

    if level == "voivodeship":
        # VOIV: region_name = LOWER(rejestracja-wojewodztwo)
        sql = f"""
        WITH
        pop_pl AS (
            SELECT SUM(CAST({POP_VALUE_COL} AS DOUBLE)) AS pop_pl
            FROM {POP_TABLE}
        ),
        pl_total AS (
            SELECT SUM(total_count) AS pl_total_reg
            FROM {ATHENA_TABLE}
            WHERE {where_pl}
        ),
        region_total AS (
            SELECT
                LOWER(TRIM(f."rejestracja-wojewodztwo")) AS region_name,
                SUM(f.total_count) AS total_reg
            FROM {ATHENA_TABLE} f
            WHERE {where_region}
            GROUP BY 1
        ),
        pop_region AS (
            SELECT
                LOWER(TRIM({POP_VOIV_COL})) AS region_name,
                SUM(CAST({POP_VALUE_COL} AS DOUBLE)) AS population
            FROM {POP_TABLE}
            GROUP BY 1
        )
        SELECT
            COALESCE(rt.region_name, pr.region_name) AS region_name,
            COALESCE(rt.region_name, pr.region_name) AS display_name,
            CAST(COALESCE(rt.total_reg, 0.0) AS DOUBLE) AS total_reg,
            CAST(COALESCE(rt.total_reg, 0.0) AS DOUBLE) / NULLIF(CAST(pr.population AS DOUBLE), 0.0) * 1000.0 AS reg_per_1000,
            100.0
              * (
                    (CAST(COALESCE(rt.total_reg, 0.0) AS DOUBLE) / NULLIF(CAST(pr.population AS DOUBLE), 0.0))
                    /
                    (CAST((SELECT pl_total_reg FROM pl_total) AS DOUBLE) / NULLIF(CAST((SELECT pop_pl FROM pop_pl) AS DOUBLE), 0.0))
                ) AS index_vs_pl
        FROM pop_region pr
        LEFT JOIN region_total rt
          ON pr.region_name = rt.region_name
        """
        qid = run_athena_query(sql)
        df = fetch_athena_result_as_df(qid)

    else:
        # COUNTY: kanonizacja przez dim_powiat_mapping + merge (sum reg + sum population) per (cepik_powiat_raw, name_to_display)
        sql = f"""
        WITH
        pop_pl AS (
            SELECT SUM(CAST({POP_VALUE_COL} AS DOUBLE)) AS pop_pl
            FROM {POP_TABLE}
        ),
        pl_total AS (
            SELECT SUM(total_count) AS pl_total_reg
            FROM {ATHENA_TABLE}
            WHERE {where_pl}
        ),
        mapping AS (
            SELECT
                TRIM({POWIAT_MAP_COL_POP})     AS pop_raw,
                TRIM({POWIAT_MAP_COL_GEO})     AS geo_raw,
                TRIM({POWIAT_MAP_COL_CEPIK})   AS cepik_raw,
                TRIM({POWIAT_MAP_COL_DISPLAY}) AS display_name
            FROM {POWIAT_MAPPING_TABLE}
        ),
        reg_by_group AS (
            SELECT
                UPPER(TRIM(m.cepik_raw)) AS group_key,
                m.display_name           AS display_name,
                SUM(f.total_count)       AS total_reg
            FROM {ATHENA_TABLE} f
            JOIN mapping m
              ON UPPER(TRIM(f."rejestracja-powiat")) = UPPER(TRIM(m.cepik_raw))
            WHERE {where_region}
            GROUP BY 1,2
        ),
        pop_by_group AS (
            SELECT
                UPPER(TRIM(m.cepik_raw)) AS group_key,
                m.display_name           AS display_name,
                SUM(CAST(p.{POP_VALUE_COL} AS DOUBLE)) AS population
            FROM mapping m
            JOIN {POP_TABLE} p
              ON LOWER(TRIM(p.{POP_COUNTY_COL})) = LOWER(TRIM(m.pop_raw))
            GROUP BY 1,2
        ),
        group_metrics AS (
            SELECT
                COALESCE(r.group_key, p.group_key) AS group_key,
                COALESCE(r.display_name, p.display_name) AS display_name,
                CAST(COALESCE(r.total_reg, 0.0) AS DOUBLE) AS total_reg,
                CAST(COALESCE(p.population, 0.0) AS DOUBLE) AS population
            FROM pop_by_group p
            LEFT JOIN reg_by_group r
              ON p.group_key = r.group_key
             AND p.display_name = r.display_name
        ),
        geo_rows AS (
            SELECT
                LOWER(TRIM(m.geo_raw)) AS region_name,
                m.display_name         AS display_name,
                UPPER(TRIM(m.cepik_raw)) AS group_key
            FROM mapping m
        )
        SELECT
            g.region_name,
            g.display_name,
            CAST(COALESCE(m.total_reg, 0.0) AS DOUBLE) AS total_reg,
            CAST(COALESCE(m.total_reg, 0.0) AS DOUBLE) / NULLIF(CAST(m.population AS DOUBLE), 0.0) * 1000.0 AS reg_per_1000,
            100.0
              * (
                    (CAST(COALESCE(m.total_reg, 0.0) AS DOUBLE) / NULLIF(CAST(m.population AS DOUBLE), 0.0))
                    /
                    (CAST((SELECT pl_total_reg FROM pl_total) AS DOUBLE) / NULLIF(CAST((SELECT pop_pl FROM pop_pl) AS DOUBLE), 0.0))
                ) AS index_vs_pl
        FROM geo_rows g
        LEFT JOIN group_metrics m
          ON g.group_key = m.group_key
         AND g.display_name = m.display_name
        """
        qid = run_athena_query(sql)
        df = fetch_athena_result_as_df(qid)

    if df.empty:
        return df

    # Typy i normalizacja
    for c in ["total_reg", "reg_per_1000", "index_vs_pl"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["region_name"] = df["region_name"].astype(str).str.strip().str.lower()
    df["display_name"] = df["display_name"].astype(str).fillna("")

    # Bezpieczeństwo: index może dać inf/NaN przy 0 populacji — ujednolicamy do 0.
    if "index_vs_pl" in df.columns:
        df["index_vs_pl"] = pd.to_numeric(df["index_vs_pl"], errors="coerce").fillna(0.0)

    return df

# ============================================
#    GENERIC QUERY (do dim / util)
# ============================================

def run_query(sql: str) -> pd.DataFrame:
    qid = run_athena_query(sql)
    return fetch_athena_result_as_df(qid)
