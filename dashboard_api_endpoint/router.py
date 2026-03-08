import base64
import json
from typing import Any, Dict, List, Tuple

from athena_client import (
    dim_voivodeships,
    dim_all_counties,
    dim_counties_for,
    dim_brands,
    dim_models_for,
    dim_origin,
    dim_alt_fuel,
    dim_fuel_buckets,
    load_region_kpis,
    load_region_fuel_mix,
    load_region_fuel_trend,
    load_region_origin_mix,
    load_region_origin_trend,
    load_region_top_brands,
    load_region_top_models,
    load_top_brands_mom_latest,
    load_region_representation_index,
    load_map_region_summary,
    run_athena_query,
    fetch_athena_result_as_df,
    run_query,
    map_fuel_to_bucket,
)
from serializers import dataframe_to_records


def _parse_json_body(event: dict) -> dict:
    body = event.get("body")
    if not body:
        return {}
    if event.get("isBase64Encoded"):
        body = json.loads(base64.b64decode(body).decode("utf-8"))
        return body
    return json.loads(body)


def _get_query_list(event: dict, key: str) -> List[str]:
    multi = event.get("multiValueQueryStringParameters") or {}
    if key in multi and multi[key]:
        values = multi[key]
        if len(values) == 1 and "," in values[0]:
            return [v for v in values[0].split(",") if v]
        return [v for v in values if v]

    single = event.get("queryStringParameters") or {}
    raw = single.get(key)
    if raw is None:
        return []
    return [v for v in raw.split(",") if v]


def _bad_request(message: str) -> Tuple[int, dict]:
    return 400, {"message": message}

def _normalize_path(event: dict) -> str:
    # HTTP API – najbardziej wiarygodne
    path = (
        event.get("requestContext", {})
             .get("http", {})
             .get("path")
        or event.get("rawPath")
        or event.get("path")
        or ""
    )

    # Usuń stage (np. /dev, /prod)
    stage = event.get("requestContext", {}).get("stage")
    if stage and path.startswith(f"/{stage}/"):
        path = path[len(stage) + 1:]

    # Usuń prefix API (/cepik)
    API_PREFIX = "/cepik"
    if path == API_PREFIX:
        return "/"
    if path.startswith(API_PREFIX + "/"):
        path = path[len(API_PREFIX):]

    return path

def route_request(event: dict) -> Tuple[int, Any]:
    raw_path = _normalize_path(event)
    method = event.get("requestContext", {}).get("http", {}).get("method", "")

    if method == "OPTIONS":
        return 200, {}

    if method == "GET" and raw_path == "/dims/voivodeships":
        return 200, dim_voivodeships()

    if method == "GET" and raw_path == "/dims/counties/all":
        return 200, dim_all_counties()

    if method == "GET" and raw_path == "/dims/counties/by-voivodeships":
        voivs = _get_query_list(event, "voivodeships")
        if not voivs:
            return _bad_request("Missing voivodeships query param.")
        return 200, dim_counties_for(voivs)

    if method == "GET" and raw_path == "/dims/brands":
        return 200, dim_brands()

    if method == "GET" and raw_path == "/dims/models":
        brands = _get_query_list(event, "brands")
        if not brands:
            return _bad_request("Missing brands query param.")
        return 200, dim_models_for(brands)

    if method == "GET" and raw_path == "/dims/origins":
        return 200, dim_origin()

    if method == "GET" and raw_path == "/dims/alt-fuels":
        return 200, dim_alt_fuel()

    if method == "POST" and raw_path == "/dims/fuel-buckets":
        body = _parse_json_body(event)
        vehicle_types = body.get("vehicle_types")
        if vehicle_types is None or vehicle_types == []:
            return _bad_request("vehicle_types is required.")
        return 200, dim_fuel_buckets(vehicle_types)

    if method == "POST" and raw_path == "/region/kpis":
        filters = _parse_json_body(event)
        return 200, load_region_kpis(filters)

    if method == "POST" and raw_path == "/region/fuel-mix":
        filters = _parse_json_body(event)
        df = load_region_fuel_mix(filters)
        return 200, dataframe_to_records(df)

    if method == "POST" and raw_path == "/region/fuel-trend":
        filters = _parse_json_body(event)
        df = load_region_fuel_trend(filters)
        return 200, dataframe_to_records(df)

    if method == "POST" and raw_path == "/region/origin-mix":
        filters = _parse_json_body(event)
        df = load_region_origin_mix(filters)
        return 200, dataframe_to_records(df)

    if method == "POST" and raw_path == "/region/origin-trend":
        filters = _parse_json_body(event)
        df = load_region_origin_trend(filters)
        return 200, dataframe_to_records(df)

    if method == "POST" and raw_path == "/region/top-brands":
        filters = _parse_json_body(event)
        df = load_region_top_brands(filters)
        return 200, dataframe_to_records(df)

    if method == "POST" and raw_path == "/region/top-models":
        filters = _parse_json_body(event)
        df = load_region_top_models(filters)
        return 200, dataframe_to_records(df)

    if method == "GET" and raw_path == "/region/top-brands/mom/latest":
        df = load_top_brands_mom_latest()
        return 200, dataframe_to_records(df)

    if method == "POST" and raw_path == "/region/representation-index":
        filters = _parse_json_body(event)
        df = load_region_representation_index(filters)
        return 200, dataframe_to_records(df)

    if method == "POST" and raw_path.startswith("/map/summary/"):
        level = raw_path.split("/map/summary/", 1)[1]
        if level not in ("voivodeship", "county"):
            return _bad_request("Invalid level. Use voivodeship or county.")
        filters = _parse_json_body(event)
        df = load_map_region_summary(filters, level=level)
        return 200, dataframe_to_records(df)

    if method == "POST" and raw_path == "/athena/query":
        body = _parse_json_body(event)
        sql = body.get("sql")
        if not sql:
            return _bad_request("Missing sql in request body.")
        timeout = body.get("timeout", 300)
        query_id = run_athena_query(sql, timeout=timeout)
        return 200, {"query_id": query_id}

    if method == "GET" and raw_path.startswith("/athena/query/") and raw_path.endswith("/results"):
        query_id = raw_path.split("/athena/query/")[1].split("/results")[0]
        if not query_id:
            return _bad_request("Missing query_id in path.")
        df = fetch_athena_result_as_df(query_id)
        return 200, dataframe_to_records(df)

    if method == "POST" and raw_path == "/athena/sql":
        body = _parse_json_body(event)
        sql = body.get("sql")
        if not sql:
            return _bad_request("Missing sql in request body.")
        df = run_query(sql)
        return 200, dataframe_to_records(df)

    if method == "POST" and raw_path == "/athena/map-fuel-to-bucket":
        body = _parse_json_body(event)
        vehicle_type_expr = body.get("vehicle_type_expr")
        raw_fuel_expr = body.get("raw_fuel_expr")
        if not vehicle_type_expr or not raw_fuel_expr:
            return _bad_request("Missing vehicle_type_expr or raw_fuel_expr.")
        sql = map_fuel_to_bucket(vehicle_type_expr, raw_fuel_expr)
        return 200, {"sql": sql}

    return 404, {"message": "Not Found"}