import copy
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np
import plotly.express as px
import pydeck as pdk
import streamlit as st
import requests

APP_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(APP_ROOT / "dashboard_api_endpoint"))

from athena_client import (  # noqa: E402
    dim_voivodeships,
    dim_all_counties,
    dim_counties_for,
    dim_brands,
    dim_brands_for_vehicle_types,
    dim_models_for,
    dim_models_for_vehicle_and_brand,
    dim_models_for_filters,
    dim_origin,
    dim_alt_fuel,
    dim_fuel_buckets,
    dim_vehicle_subtypes,
    load_region_kpis,
    load_region_fuel_mix,
    load_region_fuel_trend,
    load_region_origin_mix,
    load_region_origin_trend,
    load_region_snapshot_trend,
    load_region_vehicle_subtype_mix,
    load_region_vehicle_subtype_trend,
    load_region_top_brands,
    load_region_top_models,
    load_region_representation_index,
    load_map_region_summary,
)

# ============================================
#    CONFIG
# ============================================

st.set_page_config(page_title="motointel – CEPIK dashboards", layout="wide")

VEHICLE_TYPE_LABELS = {
    "Samochód osobowy": ["samochod_osobowy"],
    "Motocykl": ["motocykl"],
    "Motorower": ["motorower"],
    "Motocykl + motorower": ["motocykl", "motorower"],
}

COMPARE_VEHICLE_TYPES = [
    "Samochód osobowy",
    "Motocykl",
    "Motorower",
]

COMPARE_SERIES_ORDER = ["A", "B", "C", "D"]

FUEL_COLOR_PALETTE = {
    "BENZYNA": "#1f77b4",
    "OLEJ NAPĘDOWY": "#ff7f0e",
    "MIESZANKA PALIWO-OLEJ": "#9467bd",
    "ENERGIA ELEKTRYCZNA": "#2ca02c",
    "INNE": "#c7c7c7",
}

ORIGIN_COLOR_PALETTE = {
    "NOWY ZAKUPIONY W KRAJU": "#1f77b4",
    "UŻYW. ZAKUPIONY W KRAJU": "#aec7e8",
    "NOWY IMPORT INDYW": "#ff7f0e",
    "UŻYW. IMPORT INDYW": "#d62728",
}

MAPTILER_KEY = "skNK3M68jpv8JVuXSM0H"

GEOJSON_PATHS = {
    "voivodeship": "https://motointel-cepik-raw-prod.s3.eu-north-1.amazonaws.com/geo/voivodeships.geojson",
    "county": "https://motointel-cepik-raw-prod.s3.eu-north-1.amazonaws.com/geo/counties.geojson",
}

# ============================================
#    CACHE DIMENSIONS / GEOJSON
# ============================================

@st.cache_data
def get_voivodeships() -> List[str]:
    return dim_voivodeships()


@st.cache_data
def get_all_counties() -> List[str]:
    return dim_all_counties()


@st.cache_data
def get_counties_for_voivs(voivs: Tuple[str, ...]) -> List[str]:
    if not voivs:
        return dim_all_counties()
    return dim_counties_for(list(voivs))


@st.cache_data
def get_brands() -> List[str]:
    return dim_brands()


@st.cache_data
def get_brands_for_vehicle_types(vehicle_types: Optional[List[str]]) -> List[str]:
    if not vehicle_types:
        return []
    return dim_brands_for_vehicle_types(vehicle_types)


@st.cache_data
def get_models_for_brands(brands: Tuple[str, ...]) -> List[str]:
    if not brands:
        return []
    return dim_models_for(list(brands))


@st.cache_data
def get_models_for_vehicle_and_brands(
    vehicle_types: Optional[List[str]],
    brands: Tuple[str, ...],
    vehicle_subtypes: Optional[List[str]] = None,
) -> List[str]:
    if not vehicle_types or not brands:
        return []
    return dim_models_for_vehicle_and_brand(vehicle_types, list(brands), vehicle_subtypes)


@st.cache_data
def get_models_for_filters(filters: Dict[str, object]) -> List[str]:
    return dim_models_for_filters(filters)


@st.cache_data
def get_origins() -> List[str]:
    return dim_origin()


@st.cache_data
def get_alt_fuels() -> List[str]:
    return dim_alt_fuel()


@st.cache_data
def get_fuel_buckets(vehicle_types: Optional[List[str]]) -> List[str]:
    return dim_fuel_buckets(vehicle_types if vehicle_types else None)

@st.cache_data
def get_vehicle_subtypes(vehicle_types: Optional[List[str]]) -> List[str]:
    return dim_vehicle_subtypes(vehicle_types if vehicle_types else None)


@st.cache_data
def get_region_snapshot_trend(filters: Dict[str, object]) -> pd.DataFrame:
    return load_region_snapshot_trend(filters)


@st.cache_data
def load_geojson(path: str) -> dict:
    if path.startswith("http"):
        resp = requests.get(path, timeout=20)
        resp.raise_for_status()
        return resp.json()
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


# ============================================
#    HELPERS
# ============================================

def normalize_region_key(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def format_int_pl(value) -> str:
    try:
        return f"{int(round(float(value))):,}".replace(",", " ")
    except Exception:
        return "0"


def as_df(data) -> pd.DataFrame:
    if data is None:
        return pd.DataFrame()

    if isinstance(data, pd.DataFrame):
        return data

    if isinstance(data, list):
        return pd.DataFrame(data) if len(data) > 0 else pd.DataFrame()

    try:
        return pd.DataFrame(data)
    except Exception:
        return pd.DataFrame()


def prepare_share_ranking(df: pd.DataFrame, label_builder) -> pd.DataFrame:
    if df is None or df.empty or "total_count" not in df:
        return pd.DataFrame()

    data = df.copy()
    data["total_count"] = pd.to_numeric(data["total_count"], errors="coerce").fillna(0)
    data["label"] = data.apply(label_builder, axis=1).astype(str)
    total = data["total_count"].sum()
    if total > 0:
        data["share_pct"] = data["total_count"] / total * 100.0
    else:
        data["share_pct"] = 0.0
    return data


def _parse_float(value: str | float | int | None, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def is_personal_car_only(vehicle_types: Optional[List[str]]) -> bool:
    return bool(vehicle_types) and vehicle_types == ["samochod_osobowy"]


def is_motorower_only(vehicle_types: Optional[List[str]]) -> bool:
    return bool(vehicle_types) and vehicle_types == ["motorower"]


# ============================================
#    PLOTS
# ============================================

def donut(df: pd.DataFrame, category_col: str, title: str, color_map: dict | None = None):
    if df is None or df.empty or category_col not in df:
        fig = px.pie(values=[1], names=["Brak danych"], hole=0.5, title=title)
    else:
        dff = df.sort_values("total_count", ascending=True)
        order = dff[category_col].tolist()
        fig = px.pie(
            dff,
            values="total_count",
            names=category_col,
            hole=0.5,
            title=title,
            color=category_col,
            category_orders={category_col: order},
            color_discrete_map=color_map,
        )
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    return fig


def stacked_area(df: pd.DataFrame, category_col: str, title: str, color_map: dict | None = None):
    if df is None or df.empty or category_col not in df or "registration_date" not in df:
        fig = px.area(title=title)
    else:
        order = (
            df.groupby(category_col)["total_count"]
            .sum()
            .sort_values(ascending=True)
            .index
            .tolist()
        )
        fig = px.area(
            df,
            x="registration_date",
            y="total_count",
            color=category_col,
            category_orders={category_col: order},
            title=title,
            color_discrete_map=color_map,
        )
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    return fig


# ============================================
#    MAP HELPERS (MAP-CONTRACT)
# ============================================

FILL_ALPHA = 210

MAP_COLOR_PALETTE = [
    [165, 185, 215, FILL_ALPHA],
    [125, 155, 200, FILL_ALPHA],
    [90, 125, 180, FILL_ALPHA],
    [60, 95, 155, FILL_ALPHA],
    [35, 65, 130, FILL_ALPHA],
]


def _compute_quintile_thresholds(values: pd.Series) -> list[float]:
    clean = pd.Series(values).dropna().astype(float)
    if clean.empty:
        return [0.0, 0.0, 0.0, 0.0]
    return clean.quantile([0.2, 0.4, 0.6, 0.8]).tolist()


def _assign_quintile_bucket(value: float, thresholds: list[float]) -> int:
    for idx, threshold in enumerate(thresholds):
        if value <= threshold:
            return idx
    return 4


def _ensure_map_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    data = df.copy()
    required = {"region_name", "display_name", "total_reg", "reg_per_1000"}
    missing = required - set(data.columns)
    if missing:
        return pd.DataFrame()

    data["region_name"] = data["region_name"].astype(str).map(normalize_region_key)
    data["display_name"] = data["display_name"].astype(str).fillna("")

    for col in ["total_reg", "reg_per_1000"]:
        data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0.0)

    data = (
        data.groupby("region_name", as_index=False)
        .agg({"display_name": "first", "total_reg": "sum", "reg_per_1000": "mean"})
    )

    return data


def _estimate_baseline_reg_per_1000(df: pd.DataFrame) -> float | None:
    if df is None or df.empty:
        return None

    dff = df.copy()
    dff = dff[(dff["reg_per_1000"] > 0) & (dff["total_reg"] >= 0)]
    if dff.empty:
        return None

    population = dff["total_reg"] / (dff["reg_per_1000"] / 1000.0)
    population_sum = population.sum()
    if population_sum <= 0:
        return None
    return float(dff["total_reg"].sum() / population_sum * 1000.0)


def _estimate_baseline_reg_per_1000_for_keys(df: pd.DataFrame, keys: list[str]) -> float | None:
    if df is None or df.empty or not keys:
        return None

    dff = df[df["region_name"].isin(keys)].copy()
    dff = dff[(dff["reg_per_1000"] > 0) & (dff["total_reg"] >= 0)]
    if dff.empty:
        return None

    population = dff["total_reg"] / (dff["reg_per_1000"] / 1000.0)
    population_sum = population.sum()
    if population_sum <= 0:
        return None
    return float(dff["total_reg"].sum() / population_sum * 1000.0)


def prepare_map_geojson(
    geojson: dict,
    map_df: pd.DataFrame,
    metric: str,
    thresholds: list[float],
    baseline_value: float | None = None,
    baseline_keys: set[str] | None = None,
    baseline_label: str = "Polska",
) -> dict:
    if geojson is None or "features" not in geojson:
        return geojson

    geojson = copy.deepcopy(geojson)

    df = _ensure_map_columns(map_df)
    lookup = {}
    if not df.empty:
        lookup = df.set_index("region_name").to_dict(orient="index")

    index_label = f"Indeks vs {baseline_label}"

    for feature in geojson.get("features", []):
        props = feature.setdefault("properties", {})
        name_raw = props.get("nazwa", "")
        key = normalize_region_key(name_raw)
        is_baseline = bool(baseline_keys) and key in baseline_keys

        row = lookup.get(key, None)
        if row is None:
            display_name = name_raw
            total_reg = 0.0
            reg_per_1000 = 0.0
        else:
            display_name = row.get("display_name") or name_raw
            total_reg = float(row.get("total_reg", 0.0))
            reg_per_1000 = float(row.get("reg_per_1000", 0.0))

        props["name"] = name_raw
        props["display_name"] = display_name
        props["total_reg"] = total_reg
        props["reg_per_1000"] = reg_per_1000
        props["is_baseline"] = is_baseline

        total_k = f"{total_reg / 1000:.1f}"
        per_1000_round = f"{reg_per_1000:.0f}"
        if baseline_value is not None and baseline_value > 0:
            index_value = (reg_per_1000 / baseline_value) * 100.0
        else:
            index_value = 0.0
        idx_round = f"{index_value:.1f}"

        line_total = f"Rejestracje: {total_k} tys."
        line_per = f"Rejestracje na 1 tys. mieszkańców: {per_1000_round}"
        if is_baseline and baseline_value is not None:
            line_idx = f"{index_label}: Region bazowy"
            idx_round = "100.0"
        else:
            line_idx = f"{index_label}: {idx_round}"

        if metric == "total":
            line_total = f"<b>{line_total}</b>"
            props["value"] = total_reg
            color_value = total_reg
        elif metric == "per_1000":
            line_per = f"<b>{line_per}</b>"
            props["value"] = reg_per_1000
            color_value = reg_per_1000
        else:
            line_idx = f"<b>{line_idx}</b>"
            props["value"] = index_value
            color_value = abs(index_value - 100.0)

        props["line_total"] = line_total
        props["line_per_1000"] = line_per
        props["line_index"] = line_idx
        bucket = _assign_quintile_bucket(color_value, thresholds)
        props["fill_color"] = [int(c) for c in MAP_COLOR_PALETTE[bucket]]

    return geojson


def render_region_map(
    map_df: pd.DataFrame | None,
    level: str,
    color_mode: str,
    baseline_region: dict | None = None,
):
    if level not in GEOJSON_PATHS:
        st.error("Błędny level mapy.")
        return

    if color_mode not in ("total", "per_1000", "index"):
        color_mode = "total"

    base_geo = load_geojson(GEOJSON_PATHS[level])

    voiv_geo = None
    if level == "county":
        voiv_geo = load_geojson(GEOJSON_PATHS["voivodeship"])

    df = _ensure_map_columns(map_df) if map_df is not None else pd.DataFrame()
    baseline_label = "Polska"
    baseline_value = None
    baseline_keys = None
    if baseline_region:
        baseline_keys = baseline_region.get("keys")
        baseline_label = baseline_region.get("label") or baseline_label
        baseline_value = baseline_region.get("value")

    if baseline_value is None and not df.empty:
        baseline_value = _estimate_baseline_reg_per_1000(df)
    if baseline_value is not None and baseline_value <= 0:
        baseline_value = None
        baseline_label = "Polska"
        baseline_keys = None

    if df.empty:
        color_values = pd.Series(dtype=float)
    elif color_mode == "index":
        if baseline_value is not None and baseline_value > 0:
            idx_series = (df["reg_per_1000"] / baseline_value) * 100.0
            color_values = (idx_series - 100.0).abs()
        else:
            color_values = pd.Series(0.0, index=df.index)
    else:
        color_values = df["total_reg"] if color_mode == "total" else df["reg_per_1000"]

    thresholds = _compute_quintile_thresholds(color_values)
    base_geo = prepare_map_geojson(
        base_geo,
        df,
        metric=color_mode,
        thresholds=thresholds,
        baseline_value=baseline_value,
        baseline_keys=baseline_keys,
        baseline_label=baseline_label,
    )

    voiv_line_px = 1.2
    county_line_px = 0.7

    fill_layer = pdk.Layer(
        "GeoJsonLayer",
        data=base_geo,
        id=f"{level}-fill",
        pickable=True,
        auto_highlight=True,
        highlight_color=[0, 102, 204, 140],
        filled=True,
        stroked=False,
        extruded=False,
        get_fill_color="properties.fill_color",
        opacity=0.95,
        parameters={"depthTest": False, "blend": False},
    )

    outline_layer = pdk.Layer(
        "GeoJsonLayer",
        data=base_geo,
        id=f"{level}-outline",
        pickable=False,
        filled=False,
        stroked=True,
        get_line_color=[40, 40, 40],
        line_width_min_pixels=(voiv_line_px if level == "voivodeship" else county_line_px),
        opacity=0.7,
        parameters={"depthTest": False, "blend": False},
    )

    layers = [fill_layer, outline_layer]

    if level == "county" and voiv_geo is not None:
        voiv_outline_layer = pdk.Layer(
            "GeoJsonLayer",
            data=voiv_geo,
            id="voiv-outline-overlay",
            pickable=False,
            filled=False,
            stroked=True,
            get_line_color=[0, 0, 0],
            line_width_min_pixels=voiv_line_px,
            opacity=1.0,
            parameters={"depthTest": False, "blend": False},
        )
        layers.append(voiv_outline_layer)

    baseline_features = [
        feature
        for feature in base_geo.get("features", [])
        if feature.get("properties", {}).get("is_baseline")
    ]
    if baseline_features:
        baseline_geo = {"type": "FeatureCollection", "features": baseline_features}
        baseline_outline_layer = pdk.Layer(
            "GeoJsonLayer",
            data=baseline_geo,
            id=f"{level}-baseline-outline",
            pickable=False,
            filled=False,
            stroked=True,
            get_line_color=[0, 0, 0],
            line_width_min_pixels=(
                (voiv_line_px if level == "voivodeship" else county_line_px) + 1.2
            ),
            opacity=1.0,
            parameters={"depthTest": False, "blend": False},
        )
        layers.append(baseline_outline_layer)

    footer_label = baseline_label
    tooltip = {
        "html": f"""
            <b>{{display_name}}</b><br/><br/>
            {{line_total}}<br/>
            {{line_per_1000}}<br/>
            {{line_index}}<br/>
            <span style="font-size:11px;color:#888">100 = {footer_label}</span>
        """,
        "style": {"backgroundColor": "white"},
    }

    deck = pdk.Deck(
        map_style=f"https://api.maptiler.com/maps/dataviz-light/style.json?key={MAPTILER_KEY}",
        initial_view_state=pdk.ViewState(
            latitude=52.1,
            longitude=19.4,
            zoom=4.7 if level == "voivodeship" else 5.6,
            pitch=0,
        ),
        layers=layers,
        tooltip=tooltip,
    )

    st.pydeck_chart(deck, use_container_width=True)


# ============================================
#    REGION VIEW
# ============================================

def render_region_view():
    st.title("Analiza regionu")
    st.caption("Układ zgodny z /dashboard/region (frontend produkcyjny).")

    voivodeships = get_voivodeships()
    origins = get_origins()
    alt_fuels = get_alt_fuels()
    st.markdown("## Filtry")

    vehicle_type_label = st.selectbox(
        "Rodzaj pojazdu *",
        options=list(VEHICLE_TYPE_LABELS.keys()),
        index=None,
        placeholder="Wybierz rodzaj pojazdu",
        help="Wybór rodzaju pojazdu jest wymagany.",
        key="region_vehicle_type",
    )
    vehicle_types = VEHICLE_TYPE_LABELS.get(vehicle_type_label)

    if vehicle_types:
        prev_vehicle_type = st.session_state.get("region_prev_vehicle_type")
        if prev_vehicle_type != vehicle_type_label:
            st.session_state["region_models_loaded"] = False
            st.session_state["region_model_options"] = []
            st.session_state["region_selected_subtypes"] = []
            st.session_state["region_prev_vehicle_type"] = vehicle_type_label

        st.markdown("### Lokalizacja")
        loc_cols = st.columns(2)
        with loc_cols[0]:
            st.markdown("**Lokalizacja**")
            st.session_state["region_prev_vehicle_type"] = vehicle_type_label
            selected_voivs = st.multiselect("Województwo", voivodeships, default=[])
        with loc_cols[1]:
            counties_options = (
                get_counties_for_voivs(tuple(selected_voivs))
                if selected_voivs
                else get_all_counties()
            )
            selected_counties = st.multiselect("Powiat", counties_options, default=[])

        st.markdown("### Pojazd")
        st.caption(
            "Użyj tych filtrów, aby zawęzić liczbę dostępnych modeli i uniknąć niejednoznacznych wyników."
        )
        vehicle_cols = st.columns(3)

        brands_key = f"region_selected_brands_{vehicle_type_label}"
        with vehicle_cols[0]:
            st.markdown("**Pojazd**")
            brands = get_brands_for_vehicle_types(vehicle_types)
            selected_brands = st.multiselect(
                "Marka",
                brands,
                default=st.session_state.get(brands_key, []),
                key=brands_key,
            )
            prev_brands = st.session_state.get("region_prev_brands", [])
            if selected_brands != prev_brands:
                st.session_state["region_models_loaded"] = False
                st.session_state["region_model_options"] = []
                st.session_state["region_prev_brands"] = list(selected_brands)

            load_models = st.button(
                "Załaduj modele",
                disabled=not selected_brands,
            )
            models_loaded = st.session_state.get("region_models_loaded", False)
            if load_models:
                models_loaded = True
                st.session_state["region_models_loaded"] = True

            if models_loaded and selected_brands:
                model_filters = {
                    "voivodeships": [],
                    "counties": [],
                    "brands": selected_brands,
                    "vehicle_type": vehicle_types,
                    "vehicle_subtype": st.session_state.get("region_selected_subtypes", []),
                    "fuel_multi": st.session_state.get("region_selected_fuels", []),
                    "origin": st.session_state.get("region_selected_origin", "Wszystkie"),
                    "alt_fuel": st.session_state.get("region_selected_alt_fuel", "Wszystkie"),
                    "prod_year": st.session_state.get("region_prod_year", (2000, 2025)),
                    "reg_year": st.session_state.get("region_reg_year", (2018, 2025)),
                    "enable_power_filter": st.session_state.get("region_enable_power_filter", False),
                    "power_range": st.session_state.get("region_power_range", (0, 0)),
                    "enable_capacity_filter": st.session_state.get("region_enable_capacity_filter", False),
                    "capacity_range": st.session_state.get("region_capacity_range", (0, 0)),
                    "include_rhd": st.session_state.get("region_include_rhd", False),
                }
                model_options = get_models_for_filters(model_filters)
                st.session_state["region_model_options"] = model_options
            else:
                model_options = st.session_state.get("region_model_options", [])

            models_key = f"region_selected_models_{vehicle_type_label}_{hash(tuple(selected_brands))}"
            selected_models = st.multiselect(
                "Model (opcjonalnie)",
                model_options,
                default=st.session_state.get(models_key, []),
                key=models_key,
                disabled=(not selected_brands) or not models_loaded,
            )
            st.caption(
                "Nazwy modeli w CEPiK mogą być niespójne. Zalecamy najpierw zawęzić wyniki innymi filtrami "
                "(np. paliwo, moc), a dopiero potem wybrać model."
            )

        with vehicle_cols[1]:
            fuel_options = get_fuel_buckets(vehicle_types)
            selected_fuels = st.multiselect(
                "Paliwo",
                fuel_options,
                default=st.session_state.get("region_selected_fuels", []),
                key="region_selected_fuels",
            )
            origin_options = ["Wszystkie"] + origins
            origin_default = st.session_state.get("region_selected_origin", "Wszystkie")
            origin_index = origin_options.index(origin_default) if origin_default in origin_options else 0
            origin = st.selectbox(
                "Pochodzenie pojazdu",
                origin_options,
                index=origin_index,
                key="region_selected_origin",
            )
            alt_fuel_options = ["Wszystkie", "Brak"] + alt_fuels
            alt_fuel_default = st.session_state.get("region_selected_alt_fuel", "Wszystkie")
            alt_fuel_index = alt_fuel_options.index(alt_fuel_default) if alt_fuel_default in alt_fuel_options else 0
            alt_fuel = st.selectbox(
                "Paliwo alternatywne",
                alt_fuel_options,
                index=alt_fuel_index,
                key="region_selected_alt_fuel",
            )

        with vehicle_cols[2]:
            st.markdown("**Czas i pochodzenie**")
            prod_year = st.slider(
                "Rok produkcji",
                1950,
                2025,
                st.session_state.get("region_prod_year", (2000, 2025)),
                key="region_prod_year",
            )
            reg_year = st.slider(
                "Rok rejestracji (ostatnia)",
                2005,
                2025,
                st.session_state.get("region_reg_year", (2018, 2025)),
                key="region_reg_year",
            )
            st.caption(
                "Dane CEPiK sprzed 2018 r. są niepełne. "
                "Starsze dane pozostawiamy dla transparentności, ale nie rekomendujemy ich używania. "
                "[Sprawdź komunikat CEPiK]"
                "(https://www.gov.pl/web/cepik/ruszyla-nowa-centralna-ewidencja-pojazdow---informacje-dla-uzytkownikow)."
            )

            is_personal_car = is_personal_car_only(vehicle_types)
            subtype_help = None
            subtype_disabled = not is_personal_car
            if subtype_disabled:
                subtype_help = "Brak dostępnych podrodzajów dla tego typu pojazdu"
            subtype_options = get_vehicle_subtypes(vehicle_types) if is_personal_car else []
            selected_subtypes = st.multiselect(
                "Podrodzaj pojazdu",
                subtype_options,
                default=st.session_state.get("region_selected_subtypes", []),
                key="region_selected_subtypes",
                disabled=subtype_disabled,
                help=subtype_help,
            )

            include_rhd = False
            if is_personal_car:
                include_rhd = st.checkbox(
                    "Uwzględnij RHD",
                    value=st.session_state.get("region_include_rhd", False),
                    key="region_include_rhd",
                )
            else:
                st.caption("RHD dotyczy tylko samochodów osobowych.")

            only_motorower = is_motorower_only(vehicle_types)
            if only_motorower:
                st.caption("Filtry mocy i pojemności nie dotyczą motorowerów.")
                enable_power_filter = False
                enable_capacity_filter = False
                power_range = (0, 0)
                capacity_range = (0, 0)
                st.session_state["region_power_range"] = power_range
                st.session_state["region_capacity_range"] = capacity_range
            else:
                enable_power_filter = st.checkbox(
                    "Filtruj po mocy [kW]",
                    value=st.session_state.get("region_enable_power_filter", False),
                    key="region_enable_power_filter",
                )
                if enable_power_filter:
                    pmin = st.number_input("Moc od [kW]", min_value=0.0, value=40.0)
                    pmax = st.number_input("Moc do [kW]", min_value=pmin, value=250.0)
                else:
                    pmin = pmax = 0.0

                enable_capacity_filter = st.checkbox(
                    "Filtruj po pojemności [cm³]",
                    value=st.session_state.get("region_enable_capacity_filter", False),
                    key="region_enable_capacity_filter",
                )
                if enable_capacity_filter:
                    cmin = st.number_input("Pojemność od [cm³]", min_value=0.0, value=1000.0)
                    cmax = st.number_input("Pojemność do [cm³]", min_value=cmin, value=3000.0)
                else:
                    cmin = cmax = 0.0

                power_range = (pmin, pmax)
                capacity_range = (cmin, cmax)
                st.session_state["region_power_range"] = power_range
                st.session_state["region_capacity_range"] = capacity_range

        model_signature = (
            tuple(vehicle_types),
            tuple(selected_brands),
            tuple(selected_fuels),
            origin,
            alt_fuel,
            tuple(selected_subtypes),
            prod_year,
            reg_year,
            enable_power_filter,
            power_range,
            enable_capacity_filter,
            capacity_range,
            include_rhd,
        )
        if st.session_state.get("region_model_signature") != model_signature:
            st.session_state["region_models_loaded"] = False
            st.session_state["region_model_options"] = []
            st.session_state["region_model_signature"] = model_signature

        filters = {
            "voivodeships": selected_voivs,
            "counties": selected_counties,
            "brands": selected_brands,
            "models": selected_models,
            "vehicle_type": vehicle_types,
            "vehicle_subtype": selected_subtypes,
            "fuel_multi": selected_fuels,
            "origin": origin,
            "alt_fuel": alt_fuel,
            "prod_year": prod_year,
            "reg_year": reg_year,
            "enable_power_filter": enable_power_filter,
            "power_range": power_range,
            "enable_capacity_filter": enable_capacity_filter,
            "capacity_range": capacity_range,
            "include_rhd": include_rhd,
        }

        st.markdown("---")
        run = st.button("Wykonaj analizę")
        if run:
            with st.spinner("Pobieram dane z Atheny..."):
                kpis = load_region_kpis(filters)
                fuel_mix = as_df(load_region_fuel_mix(filters))
                fuel_trend = as_df(load_region_fuel_trend(filters))
                origin_mix = as_df(load_region_origin_mix(filters))
                origin_trend = as_df(load_region_origin_trend(filters))
                snapshot_trend = as_df(get_region_snapshot_trend(filters))
                vehicle_subtype_mix = as_df(load_region_vehicle_subtype_mix(filters))
                vehicle_subtype_trend = as_df(load_region_vehicle_subtype_trend(filters))
                top_brands = as_df(load_region_top_brands(filters))
                top_models = as_df(load_region_top_models(filters))
                idx_df = as_df(load_region_representation_index(filters))
                map_voiv = as_df(load_map_region_summary(filters, level="voivodeship"))
                map_county = as_df(load_map_region_summary(filters, level="county"))

            st.session_state.region_data = dict(
                kpis=kpis,
                fuel_mix=fuel_mix,
                fuel_trend=fuel_trend,
                origin_mix=origin_mix,
                origin_trend=origin_trend,
                snapshot_trend=snapshot_trend,
                vehicle_subtype_mix=vehicle_subtype_mix,
                vehicle_subtype_trend=vehicle_subtype_trend,
                top_brands=top_brands,
                top_models=top_models,
                index_trend=idx_df,
                map_voiv=map_voiv,
                map_county=map_county,
                filters=filters,
            )
    else:
        st.info("Wybierz rodzaj pojazdu, aby odblokować pozostałe filtry.")
        return

    data = st.session_state.get("region_data")
    if data is None:
        st.info("Ustaw filtry i kliknij **Wykonaj analizę**.")
        return

    kpis = data["kpis"] or {}
    if float(kpis.get("total_reg", 0)) == 0:
        st.warning("Brak wyników dla wybranych filtrów.")
        return

    st.markdown("## Wyniki")

    # KPI + MAP layout
    left, right = st.columns([1.05, 1.35], gap="large")

    with left:
        st.markdown("### KPI")

        total_reg = float(kpis.get("total_reg", 0))
        ev_reg = float(kpis.get("ev_reg", 0))
        ev_share = (ev_reg / total_reg * 100.0) if total_reg > 0 else 0.0
        avg_age = float(kpis.get("avg_age_years", 0))
        avg_time_owned = float(kpis.get("avg_time_owned_years", 0))
        rhd_count = float(kpis.get("rhd_count", 0))

        k1, k2 = st.columns(2)
        with k1:
            st.metric("Łączna Ilość Pojazdów", format_int_pl(total_reg))
        with k2:
            st.metric("Udział Pojazdów Elektrycznych", f"{ev_share:.2f} %")

        k3, k4 = st.columns(2)
        with k3:
            st.metric("Średni wiek", f"{avg_age:.1f} lat")
        with k4:
            st.metric("Średni czas posiadania", f"{avg_time_owned:.1f} lat")

        k5, k6 = st.columns(2)
        with k5:
            idx_df = data.get("index_trend")
            if idx_df is None or idx_df.empty:
                last_index = None
            else:
                idx_df = idx_df.dropna(subset=["registration_date", "index_vs_pl"]).sort_values(
                    "registration_date"
                )
                last_index = float(idx_df.iloc[-1]["index_vs_pl"]) if not idx_df.empty else None
            st.metric("Indeks vs Polska", "—" if last_index is None else f"{last_index:.1f}")
            st.caption("Indeks porównuje rejestracje na 1 tys. mieszkańców do średniej krajowej.")

        with k6:
            filters = data.get("filters", {})
            is_personal_car = is_personal_car_only(filters.get("vehicle_type"))
            include_rhd = bool(filters.get("include_rhd", False))
            if not is_personal_car:
                st.metric("Udział RHD", "Nie dotyczy")
            elif include_rhd:
                st.metric("Udział RHD", format_int_pl(rhd_count))
            else:
                st.metric("Udział RHD", "Nie uwzgl.")

    with right:
        st.markdown("### Mapa")

        c1, c2 = st.columns([1, 2])
        with c1:
            map_level_label = st.radio(
                "Widok",
                options=["Województwa", "Powiaty"],
                horizontal=True,
                index=0,
            )
        with c2:
            color_mode_label = st.radio(
                "Koloruj według",
                options=["Liczba rejestracji", "Rejestracje na 1 tys. mieszkańców"],
                horizontal=True,
                index=0,
            )

        level_map = {"Województwa": "voivodeship", "Powiaty": "county"}
        color_map = {
            "Liczba rejestracji": "total",
            "Rejestracje na 1 tys. mieszkańców": "per_1000",
        }

        level = level_map[map_level_label]
        color_mode = color_map[color_mode_label]

        map_df = data.get("map_voiv") if level == "voivodeship" else data.get("map_county")
        map_voiv = data.get("map_voiv")
        map_county = data.get("map_county")
        df_voiv = _ensure_map_columns(map_voiv) if map_voiv is not None else pd.DataFrame()
        df_county = _ensure_map_columns(map_county) if map_county is not None else pd.DataFrame()

        selected_counties = data["filters"].get("counties", [])
        selected_voivs = data["filters"].get("voivodeships", [])
        county_keys = [normalize_region_key(c) for c in selected_counties]
        voiv_keys = [normalize_region_key(v) for v in selected_voivs]

        baseline_region = {
            "keys": None,
            "label": "Polska",
            "value": _estimate_baseline_reg_per_1000(
                _ensure_map_columns(map_df) if map_df is not None else pd.DataFrame()
            ),
        }

        if selected_counties:
            baseline_region["label"] = selected_counties[0] if len(selected_counties) == 1 else "wybrane regiony"
            baseline_region["value"] = _estimate_baseline_reg_per_1000_for_keys(df_county, county_keys)
            if level == "county":
                baseline_region["keys"] = set(county_keys)
        elif selected_voivs:
            baseline_region["label"] = selected_voivs[0] if len(selected_voivs) == 1 else "wybrane regiony"
            baseline_region["value"] = _estimate_baseline_reg_per_1000_for_keys(df_voiv, voiv_keys)
            if level == "voivodeship":
                baseline_region["keys"] = set(voiv_keys)

        render_region_map(
            map_df=map_df,
            level=level,
            color_mode=color_mode,
            baseline_region=baseline_region,
        )

        st.caption("Zmiana poziomu lub koloru nie odpytuje Atheny.")

    st.markdown("---")
    st.markdown("## Struktura paliw i pochodzenia")

    mix1, mix2 = st.columns(2)
    with mix1:
        st.plotly_chart(
            donut(data["fuel_mix"], "fuel_bucket", "Miks paliw", color_map=FUEL_COLOR_PALETTE),
            use_container_width=True,
        )
        st.plotly_chart(
            donut(data["origin_mix"], "origin", "Pochodzenie", color_map=ORIGIN_COLOR_PALETTE),
            use_container_width=True,
        )

    with mix2:
        st.caption(
            "CEPiK nie zawiera każdego historycznego zdarzenia rejestracji. "
            "Jeśli pojazd był rejestrowany wielokrotnie, widoczna jest tylko ostatnia rejestracja. "
            "Wykresy pokazują rozkład rejestracji w czasie, a nie realny trend."
        )
        st.plotly_chart(
            stacked_area(
                data["fuel_trend"],
                "fuel_bucket",
                "Rozkład rejestracji paliw w czasie",
                color_map=FUEL_COLOR_PALETTE,
            ),
            use_container_width=True,
        )
        st.plotly_chart(
            stacked_area(
                data["origin_trend"],
                "origin",
                "Rozkład rejestracji pochodzenia w czasie",
                color_map=ORIGIN_COLOR_PALETTE,
            ),
            use_container_width=True,
        )

    filters = data.get("filters", {})
    if is_personal_car_only(filters.get("vehicle_type")):
        st.markdown("---")
        st.markdown("## Struktura podrodzaju pojazdu")

        subtype1, subtype2 = st.columns(2)
        with subtype1:
            st.plotly_chart(
                donut(data["vehicle_subtype_mix"], "vehicle_subtype", "Struktura podrodzaju pojazdu"),
                use_container_width=True,
            )

        with subtype2:
            st.caption(
                "CEPiK nie zawiera każdego historycznego zdarzenia rejestracji. "
                "Jeśli pojazd był rejestrowany wielokrotnie, widoczna jest tylko ostatnia rejestracja. "
                "Wykresy pokazują rozkład rejestracji w czasie, a nie realny trend."
            )
            st.plotly_chart(
                stacked_area(
                    data["vehicle_subtype_trend"],
                    "vehicle_subtype",
                    "Rozkład rejestracji podrodzaju pojazdu w czasie",
                ),
                use_container_width=True,
            )

    st.markdown("---")
    st.markdown("## Top marki i modele")

    t1, t2 = st.columns(2, gap="large")

    with t1:
        st.markdown("### Udział marek")
        brand_query = st.text_input("Wyszukaj markę", key="brand_share_search")
        dfb = prepare_share_ranking(
            data["top_brands"],
            lambda row: row.get("brand", "") or "",
        )
        if dfb is None or dfb.empty:
            st.info("Brak danych.")
        else:
            if brand_query:
                dfb = dfb[dfb["label"].str.contains(brand_query, case=False, na=False)]
            if dfb.empty:
                st.info("Brak danych dla podanego wyszukiwania.")
            else:
                dfb = dfb.sort_values("total_count", ascending=False)
                total_rows = len(dfb)
                ranges = [
                    (start, min(start + 19, total_rows))
                    for start in range(1, total_rows + 1, 20)
                ]
                range_labels = [f"{start}\u2013{end}" for start, end in ranges]
                selected_range = st.selectbox(
                    "Zakres pozycji",
                    range_labels,
                    key="brand_share_range",
                )
                selected_idx = range_labels.index(selected_range)
                start, end = ranges[selected_idx]
                dfb_view = dfb.iloc[start - 1 : end]
                height = max(360, 24 * len(dfb_view) + 120)
                fig = px.bar(
                    dfb_view,
                    x="total_count",
                    y="label",
                    orientation="h",
                    title="Udział marek",
                    color_discrete_sequence=["#1f77b4"],
                )
                fig.update_traces(
                    customdata=dfb_view[["share_pct"]],
                    hovertemplate=(
                        "%{y}<br>"
                        "Rejestracje: %{x:,}<br>"
                        "Udział: %{customdata[0]:.2f}%<extra></extra>"
                    ),
                )
                fig.update_layout(
                    xaxis_title="Liczba rejestracji",
                    yaxis_title="Marka",
                    yaxis=dict(
                        categoryorder="array",
                        categoryarray=list(reversed(dfb_view["label"].tolist())),
                    ),
                    margin=dict(l=0, r=0, t=60, b=0),
                    height=height,
                )
                st.plotly_chart(fig, use_container_width=True)

    with t2:
        st.markdown("### Udział modeli")
        filters = data.get("filters", {})
        selected_brands = filters.get("brands") or []
        model_query = st.text_input("Wyszukaj model", key="model_share_search")
        dfm = prepare_share_ranking(
            data["top_models"],
            lambda row: f"{row.get('brand', '')} {row.get('model', '')}".strip(),
        )
        if dfm is None or dfm.empty:
            st.info("Brak danych.")
        else:
            if model_query:
                dfm = dfm[dfm["label"].str.contains(model_query, case=False, na=False)]
            if dfm.empty:
                st.info("Brak danych dla podanego wyszukiwania.")
            else:
                dfm = dfm.sort_values("total_count", ascending=False)
                total_rows = len(dfm)
                ranges = [
                    (start, min(start + 19, total_rows))
                    for start in range(1, total_rows + 1, 20)
                ]
                range_labels = [f"{start}\u2013{end}" for start, end in ranges]
                selected_range = st.selectbox(
                    "Zakres pozycji",
                    range_labels,
                    key="model_share_range",
                )
                selected_idx = range_labels.index(selected_range)
                start, end = ranges[selected_idx]
                dfm_view = dfm.iloc[start - 1 : end]
                height = max(360, 24 * len(dfm_view) + 120)
                chart_context = (
                    f"Udział modeli — {', '.join(selected_brands)}"
                    if selected_brands
                    else "Udział modeli — globalnie"
                )
                fig = px.bar(
                    dfm_view,
                    x="total_count",
                    y="label",
                    orientation="h",
                    title=chart_context,
                    color_discrete_sequence=["#1f77b4"],
                )
                fig.update_traces(
                    customdata=dfm_view[["share_pct"]],
                    hovertemplate=(
                        "%{y}<br>"
                        "Rejestracje: %{x:,}<br>"
                        "Udział: %{customdata[0]:.2f}%<extra></extra>"
                    ),
                )
                fig.update_layout(
                    xaxis_title="Liczba rejestracji",
                    yaxis_title="Model",
                    yaxis=dict(
                        categoryorder="array",
                        categoryarray=list(reversed(dfm_view["label"].tolist())),
                    ),
                    margin=dict(l=0, r=0, t=60, b=0),
                    height=height,
                )
                st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.markdown("## Funkcjonalności testowe")

    snapshot_trend = data.get("snapshot_trend")
    if snapshot_trend is None or snapshot_trend.empty or "snapshot_month" not in snapshot_trend:
        st.info("Brak danych.")
    else:
        left_chart, right_chart = st.columns(2)
        with left_chart:
            fig = px.line(
                snapshot_trend,
                x="snapshot_month",
                y="total_count",
                title="Trend miesięczny rejestracji",
            )
            fig.update_layout(margin=dict(l=0, r=0, t=60, b=0))
            st.plotly_chart(fig, use_container_width=True)

        with right_chart:
            net_flow_df = snapshot_trend.sort_values("snapshot_month").copy()
            net_flow_df["net_flow"] = net_flow_df["total_count"].diff().fillna(0)
            net_flow_df["flow_direction"] = np.where(net_flow_df["net_flow"] >= 0, "Wzrost", "Spadek")
            flow_fig = px.bar(
                net_flow_df,
                x="snapshot_month",
                y="net_flow",
                color="flow_direction",
                title="Miesięczny przepływ netto",
                color_discrete_map={"Wzrost": "#2ca02c", "Spadek": "#d62728"},
            )
            flow_fig.add_hline(y=0, line_width=1, line_color="#666", opacity=0.8)
            flow_fig.update_layout(margin=dict(l=0, r=0, t=60, b=0), showlegend=False)
            st.plotly_chart(flow_fig, use_container_width=True)


# ============================================
#    COMPARE VIEW
# ============================================

def _create_compare_card(series_id: str, card_id: int) -> Dict[str, object]:
    return {
        "id": card_id,
        "series": series_id,
        "brand": "",
        "models": [],
        "vehicle_subtypes": [],
        "fuel_bucket": "Wszystkie",
        "origin": "Wszystkie",
        "alt_fuel": "Wszystkie",
        "enable_prod_year": False,
        "prod_year": (2000, 2025),
        "enable_power_filter": False,
        "power_min": "",
        "power_max": "",
        "enable_capacity_filter": False,
        "capacity_min": "",
        "capacity_max": "",
        "include_rhd": False,
    }


def _init_compare_state() -> None:
    if "compare_cards" not in st.session_state:
        st.session_state.compare_cards = [
            _create_compare_card("A", 1),
            _create_compare_card("B", 2),
        ]
        st.session_state.compare_next_id = 3
        st.session_state.compare_results = None
        st.session_state.compare_summary = None
    else:
        cards = st.session_state.compare_cards
        for idx, card in enumerate(cards):
            if "series" not in card:
                series_id = COMPARE_SERIES_ORDER[idx] if idx < len(COMPARE_SERIES_ORDER) else ""
                card["series"] = series_id
        existing_series = {card.get("series") for card in cards}
        missing_series = [s for s in ("A", "B") if s not in existing_series]
        next_id = st.session_state.get("compare_next_id")
        if next_id is None:
            max_id = max((card.get("id", 0) for card in cards), default=0)
            next_id = max_id + 1
        for series_id in missing_series:
            cards.append(_create_compare_card(series_id, next_id))
            next_id += 1
        st.session_state.compare_next_id = next_id

    if "compare_shared" not in st.session_state:
        st.session_state.compare_shared = {
            "vehicle_type": "Samochód osobowy",
            "voivodeships": [],
            "counties": [],
            "reg_year": (2018, 2025),
        }


def _format_compare_models(models: List[str]) -> str:
    if not models:
        return "wszystkie modele"
    if len(models) == 1:
        return models[0]
    return ", ".join(models)


def _format_compare_subtypes(subtypes: List[str]) -> str:
    if not subtypes:
        return "wszystkie podrodzaje"
    if len(subtypes) == 1:
        return subtypes[0]
    return ", ".join(subtypes)


def _build_compare_label(card: Dict) -> str:
    series_id = card.get("series", "")
    brand = card.get("brand", "")
    model_label = _format_compare_models(card.get("models", []))
    subtype_label = _format_compare_subtypes(card.get("vehicle_subtypes", []))
    return f"[{series_id}] {brand} · {model_label} · {subtype_label}".strip()


def _build_compare_signature(
    card: Dict,
    vehicle_types: Optional[List[str]],
    reg_year: Tuple[int, int],
    is_personal_car: bool,
) -> Tuple[object, ...]:
    models = tuple(sorted(card.get("models", [])))
    subtypes = tuple(sorted(card.get("vehicle_subtypes", [])))
    prod_year = card.get("prod_year") if card.get("enable_prod_year") else None

    power_enabled = bool(card.get("enable_power_filter")) and (
        card.get("power_min") or card.get("power_max")
    )
    power_range = None
    if power_enabled:
        power_range = _normalize_compare_numeric_range(
            card.get("power_min"),
            card.get("power_max"),
            9999.0,
        )

    capacity_enabled = bool(card.get("enable_capacity_filter")) and (
        card.get("capacity_min") or card.get("capacity_max")
    )
    capacity_range = None
    if capacity_enabled:
        capacity_range = _normalize_compare_numeric_range(
            card.get("capacity_min"),
            card.get("capacity_max"),
            99999.0,
        )

    include_rhd = bool(card.get("include_rhd")) if is_personal_car else False

    return (
        tuple(vehicle_types or []),
        subtypes,
        card.get("brand", ""),
        models,
        card.get("fuel_bucket"),
        card.get("origin"),
        card.get("alt_fuel"),
        prod_year,
        reg_year,
        power_enabled,
        power_range,
        capacity_enabled,
        capacity_range,
        include_rhd,
    )


def _normalize_compare_numeric_range(
    minimum: str | float | int | None,
    maximum: str | float | int | None,
    default_max: float,
) -> Tuple[float, float]:
    min_val = _parse_float(minimum, 0.0)
    max_val = _parse_float(maximum, default_max)
    if max_val < min_val:
        max_val = min_val
    return min_val, max_val


def render_compare_view():
    _init_compare_state()

    st.title("Porównywarka")
    st.caption("Układ zgodny z /dashboard/compare (frontend produkcyjny).")

    shared = st.session_state.compare_shared
    cards = st.session_state.compare_cards
    cards_by_series = {card.get("series"): card for card in cards}
    cards = [cards_by_series[s] for s in COMPARE_SERIES_ORDER if s in cards_by_series]
    st.session_state.compare_cards = cards

    shared_col, cards_col = st.columns([1.0, 2.0], gap="large")

    with shared_col:
        st.markdown("## Wspólny kontekst")

        shared_vehicle_label = st.radio(
            "Rodzaj pojazdu *",
            COMPARE_VEHICLE_TYPES,
            index=COMPARE_VEHICLE_TYPES.index(shared["vehicle_type"]),
        )
        prev_vehicle_type = st.session_state.get("compare_prev_vehicle_type", shared["vehicle_type"])
        if shared_vehicle_label != prev_vehicle_type:
            for card in cards:
                card["brand"] = ""
                card["models"] = []
                card["vehicle_subtypes"] = []
                card["alt_fuel"] = "Wszystkie"
                card["include_rhd"] = False
                st.session_state[f"compare_brand_{card['id']}"] = ""
                st.session_state[f"compare_models_{card['id']}"] = []
                st.session_state[f"compare_vehicle_subtypes_{card['id']}"] = []
                st.session_state[f"compare_alt_fuel_{card['id']}"] = "Wszystkie"
                st.session_state[f"compare_include_rhd_{card['id']}"] = False
            st.session_state.compare_results = None
            st.session_state.compare_summary = None
            st.session_state.compare_prev_vehicle_type = shared_vehicle_label

        shared["vehicle_type"] = shared_vehicle_label

        voivodeships = get_voivodeships()
        selected_voivs = st.multiselect(
            "Województwo",
            voivodeships,
            default=shared.get("voivodeships", []),
        )
        counties_options = (
            get_counties_for_voivs(tuple(selected_voivs))
            if selected_voivs
            else get_all_counties()
        )
        selected_counties = st.multiselect(
            "Powiat",
            counties_options,
            default=shared.get("counties", []),
        )

        reg_year = st.slider(
            "Rok rejestracji (ostatnia)",
            2005,
            2025,
            shared.get("reg_year", (2018, 2025)),
        )
        st.caption(
            "Dane CEPiK sprzed 2018 r. są niepełne. "
            "Starsze dane pozostawiamy dla transparentności, ale nie rekomendujemy ich używania. "
            "[Sprawdź komunikat CEPiK]"
            "(https://www.gov.pl/web/cepik/ruszyla-nowa-centralna-ewidencja-pojazdow---informacje-dla-uzytkownikow)."
        )

        shared.update(
            {
                "vehicle_type": shared_vehicle_label,
                "voivodeships": selected_voivs,
                "counties": selected_counties,
                "reg_year": reg_year,
            }
        )

    with cards_col:
        st.markdown("## Porównywane auta")
        existing_series = {card.get("series") for card in cards}
        available_series = [s for s in COMPARE_SERIES_ORDER if s not in existing_series]
        can_add = len(cards) < 4 and bool(available_series)
        add_col, info_col = st.columns([1, 2])
        with add_col:
            if st.button("Dodaj auto", disabled=not can_add):
                cards.append(
                    _create_compare_card(
                        available_series[0],
                        st.session_state.compare_next_id,
                    )
                )
                st.session_state.compare_next_id += 1
                st.session_state.compare_results = None
                st.session_state.compare_summary = None

        with info_col:
            st.caption("Dodaj maksymalnie 2 dodatkowe auta (C i D).")

        vehicle_types = VEHICLE_TYPE_LABELS.get(shared_vehicle_label)
        is_only_motorower = is_motorower_only(vehicle_types)
        is_personal_car = is_personal_car_only(vehicle_types)

        brands = get_brands_for_vehicle_types(vehicle_types)
        origins = get_origins()
        alt_fuels = get_alt_fuels()
        fuel_options = get_fuel_buckets(vehicle_types) or []

        for card in list(cards):
            label = f"Pojazd {card.get('series')}"
            with st.expander(label, expanded=True):
                header_cols = st.columns([3, 1])
                with header_cols[0]:
                    st.markdown(f"### {label}")
                with header_cols[1]:
                    is_fixed_series = card.get("series") in ("A", "B")
                    if (
                        st.button(
                            "Usuń",
                            key=f"compare_remove_{card['id']}",
                            disabled=is_fixed_series,
                        )
                        and not is_fixed_series
                        and len(cards) > 2
                    ):
                        st.session_state.compare_cards = [
                            c for c in cards if c["id"] != card["id"]
                        ]
                        st.session_state.compare_results = None
                        st.session_state.compare_summary = None
                        st.rerun()

                c1, c2 = st.columns(2)
                with c1:
                    brand = st.selectbox(
                        "Marka *",
                        options=[""] + brands,
                        index=([""] + brands).index(card["brand"]) if card["brand"] in brands else 0,
                        format_func=lambda v: "Wybierz markę" if v == "" else v,
                        key=f"compare_brand_{card['id']}",
                    )
                    current_subtypes = card.get("vehicle_subtypes", [])
                    if is_personal_car:
                        subtype_options = get_vehicle_subtypes(vehicle_types)
                        selected_subtypes = st.multiselect(
                            "Podrodzaj pojazdu",
                            subtype_options,
                            default=[s for s in current_subtypes if s in subtype_options],
                            key=f"compare_vehicle_subtypes_{card['id']}",
                        )
                    else:
                        selected_subtypes = []
                        if current_subtypes:
                            st.session_state[f"compare_vehicle_subtypes_{card['id']}"] = []
                            card["vehicle_subtypes"] = []
                        if card.get("include_rhd"):
                            st.session_state[f"compare_include_rhd_{card['id']}"] = False
                            card["include_rhd"] = False

                with c2:
                    fuel_bucket_state = st.session_state.get(
                        f"compare_fuel_{card['id']}",
                        card["fuel_bucket"],
                    )
                    origin_state = st.session_state.get(
                        f"compare_origin_{card['id']}",
                        card["origin"],
                    )
                    alt_fuel_state = st.session_state.get(
                        f"compare_alt_fuel_{card['id']}",
                        card.get("alt_fuel", "Wszystkie"),
                    )
                    prod_year_state = st.session_state.get(
                        f"compare_prod_year_{card['id']}",
                        card["prod_year"],
                    )
                    enable_prod_year_state = st.session_state.get(
                        f"compare_prod_enable_{card['id']}",
                        card["enable_prod_year"],
                    )
                    power_min_state = st.session_state.get(
                        f"compare_power_min_{card['id']}",
                        card["power_min"],
                    )
                    power_max_state = st.session_state.get(
                        f"compare_power_max_{card['id']}",
                        card["power_max"],
                    )
                    enable_power_state = st.session_state.get(
                        f"compare_power_enable_{card['id']}",
                        card["enable_power_filter"],
                    )
                    capacity_min_state = st.session_state.get(
                        f"compare_capacity_min_{card['id']}",
                        card["capacity_min"],
                    )
                    capacity_max_state = st.session_state.get(
                        f"compare_capacity_max_{card['id']}",
                        card["capacity_max"],
                    )
                    enable_capacity_state = st.session_state.get(
                        f"compare_capacity_enable_{card['id']}",
                        card["enable_capacity_filter"],
                    )
                    include_rhd_state = st.session_state.get(
                        f"compare_include_rhd_{card['id']}",
                        card.get("include_rhd", False),
                    )

                    if enable_prod_year_state:
                        prod_year_range = prod_year_state
                    else:
                        prod_year_range = (2000, 2025)

                    selected_fuels = []
                    if fuel_bucket_state and fuel_bucket_state != "Wszystkie":
                        selected_fuels = [fuel_bucket_state]

                    power_range = (0.0, 0.0)
                    if enable_power_state:
                        power_range = _normalize_compare_numeric_range(
                            power_min_state,
                            power_max_state,
                            9999.0,
                        )

                    capacity_range = (0.0, 0.0)
                    if enable_capacity_state:
                        capacity_range = _normalize_compare_numeric_range(
                            capacity_min_state,
                            capacity_max_state,
                            99999.0,
                        )

                    model_signature = (
                        tuple(vehicle_types or []),
                        brand,
                        tuple(selected_subtypes),
                        tuple(selected_fuels),
                        origin_state,
                        alt_fuel_state,
                        prod_year_range,
                        shared.get("reg_year", (2018, 2025)),
                        bool(enable_power_state),
                        power_range,
                        bool(enable_capacity_state),
                        capacity_range,
                        bool(include_rhd_state) if is_personal_car else False,
                    )
                    signature_key = f"compare_model_signature_{card['id']}"
                    if st.session_state.get(signature_key) != model_signature:
                        card["models"] = []
                        st.session_state[f"compare_models_{card['id']}"] = []
                        st.session_state[signature_key] = model_signature

                    model_filters = {
                        "voivodeships": [],
                        "counties": [],
                        "brands": [brand] if brand else [],
                        "vehicle_type": vehicle_types,
                        "vehicle_subtype": selected_subtypes,
                        "fuel_multi": selected_fuels,
                        "origin": origin_state,
                        "alt_fuel": alt_fuel_state,
                        "prod_year": prod_year_range,
                        "reg_year": shared.get("reg_year", (2018, 2025)),
                        "enable_power_filter": bool(enable_power_state),
                        "power_range": power_range,
                        "enable_capacity_filter": bool(enable_capacity_state),
                        "capacity_range": capacity_range,
                        "include_rhd": bool(include_rhd_state) if is_personal_car else False,
                    }

                    models_options = get_models_for_filters(model_filters) if brand else []
                    current_models = [m for m in card["models"] if m in models_options]
                    models = st.multiselect(
                        "Model (opcjonalnie)",
                        models_options,
                        default=current_models,
                        key=f"compare_models_{card['id']}",
                    )

                c3, c4 = st.columns(2)
                with c3:
                    fuel_bucket = st.selectbox(
                        "Paliwo",
                        options=["Wszystkie"] + fuel_options,
                        index=(
                            (["Wszystkie"] + fuel_options).index(card["fuel_bucket"])
                            if card["fuel_bucket"] in fuel_options
                            else 0
                        ),
                        key=f"compare_fuel_{card['id']}",
                    )
                with c4:
                    origin = st.selectbox(
                        "Pochodzenie",
                        options=["Wszystkie"] + origins,
                        index=(
                            (["Wszystkie"] + origins).index(card["origin"])
                            if card["origin"] in origins
                            else 0
                        ),
                        key=f"compare_origin_{card['id']}",
                    )
                    alt_fuel_options = ["Wszystkie", "Brak"] + alt_fuels
                    alt_fuel_current = card.get("alt_fuel", "Wszystkie")
                    alt_fuel_index = (
                        alt_fuel_options.index(alt_fuel_current)
                        if alt_fuel_current in alt_fuel_options
                        else 0
                    )
                    alt_fuel = st.selectbox(
                        "Paliwo alternatywne",
                        options=alt_fuel_options,
                        index=alt_fuel_index,
                        key=f"compare_alt_fuel_{card['id']}",
                    )

                c5, c6, c7 = st.columns(3)
                with c5:
                    enable_prod_year = st.checkbox(
                        "Rok produkcji",
                        value=card["enable_prod_year"],
                        key=f"compare_prod_enable_{card['id']}",
                    )
                    if enable_prod_year:
                        prod_year = st.slider(
                            "Zakres",
                            1950,
                            2025,
                            card["prod_year"],
                            key=f"compare_prod_year_{card['id']}",
                        )
                    else:
                        prod_year = card["prod_year"]

                with c6:
                    if is_only_motorower:
                        st.caption("Moc nie dotyczy motorowerów.")
                        enable_power_filter = False
                        power_min = ""
                        power_max = ""
                    else:
                        enable_power_filter = st.checkbox(
                            "Moc [kW]",
                            value=card["enable_power_filter"],
                            key=f"compare_power_enable_{card['id']}",
                        )
                        power_min = st.text_input(
                            "Od",
                            value=card["power_min"],
                            key=f"compare_power_min_{card['id']}",
                        )
                        power_max = st.text_input(
                            "Do",
                            value=card["power_max"],
                            key=f"compare_power_max_{card['id']}",
                        )

                with c7:
                    if is_only_motorower:
                        st.caption("Pojemność nie dotyczy motorowerów.")
                        enable_capacity_filter = False
                        capacity_min = ""
                        capacity_max = ""
                    else:
                        enable_capacity_filter = st.checkbox(
                            "Pojemność [cm³]",
                            value=card["enable_capacity_filter"],
                            key=f"compare_capacity_enable_{card['id']}",
                        )
                        capacity_min = st.text_input(
                            "Od",
                            value=card["capacity_min"],
                            key=f"compare_capacity_min_{card['id']}",
                        )
                        capacity_max = st.text_input(
                            "Do",
                            value=card["capacity_max"],
                            key=f"compare_capacity_max_{card['id']}",
                        )
                    include_rhd = False
                    if is_personal_car:
                        include_rhd = st.checkbox(
                            "Uwzględnij RHD",
                            value=card.get("include_rhd", False),
                            key=f"compare_include_rhd_{card['id']}",
                        )
                    else:
                        st.caption("RHD dotyczy tylko samochodów osobowych.")

                card.update(
                    {
                        "brand": brand,
                        "models": models,
                        "vehicle_subtypes": selected_subtypes,
                        "fuel_bucket": fuel_bucket,
                        "origin": origin,
                        "alt_fuel": alt_fuel,
                        "enable_prod_year": enable_prod_year,
                        "prod_year": prod_year,
                        "enable_power_filter": enable_power_filter,
                        "power_min": power_min,
                        "power_max": power_max,
                        "enable_capacity_filter": enable_capacity_filter,
                        "capacity_min": capacity_min,
                        "capacity_max": capacity_max,
                        "include_rhd": include_rhd,
                    }
                )

        st.markdown("---")
        if st.button("Porównaj"):
            empty_cards = [card for card in cards if not card["brand"]]

            if empty_cards and len(empty_cards) == len(cards):
                st.warning("Uzupełnij markę w pojazdach A i B, aby uruchomić porównanie.")
                st.session_state.compare_results = None
                st.session_state.compare_summary = None
            elif empty_cards:
                st.warning("Uzupełnij markę we wszystkich kartach lub usuń puste.")
                st.session_state.compare_results = None
                st.session_state.compare_summary = None
            else:
                signatures = [
                    _build_compare_signature(
                        card,
                        vehicle_types,
                        shared.get("reg_year", (2018, 2025)),
                        is_personal_car,
                    )
                    for card in cards
                ]
                if len(signatures) != len(set(signatures)):
                    st.warning(
                        "Wybrane pojazdy mają identyczną konfigurację filtrów – "
                        "porównanie nie ma sensu."
                    )
                    st.session_state.compare_results = None
                    st.session_state.compare_summary = None
                else:
                    results = []
                    summaries = []
                    with st.spinner("Pobieram dane z Atheny..."):
                        for card in cards:
                            filters: Dict[str, object] = {
                                "voivodeships": shared.get("voivodeships", []),
                                "counties": shared.get("counties", []),
                                "brands": [card["brand"]],
                                "vehicle_type": vehicle_types,
                                "reg_year": shared.get("reg_year"),
                            }
                            if card["models"]:
                                filters["models"] = card["models"]
                            if card["fuel_bucket"] and card["fuel_bucket"] != "Wszystkie":
                                filters["fuel_multi"] = [card["fuel_bucket"]]
                            if card["origin"] and card["origin"] != "Wszystkie":
                                filters["origin"] = card["origin"]
                            if card.get("alt_fuel") and card["alt_fuel"] != "Wszystkie":
                                filters["alt_fuel"] = card["alt_fuel"]
                            if card.get("vehicle_subtypes"):
                                filters["vehicle_subtype"] = card["vehicle_subtypes"]
                            if card["enable_prod_year"]:
                                filters["prod_year"] = card["prod_year"]
                            if card["enable_power_filter"] and (card["power_min"] or card["power_max"]):
                                pmin, pmax = _normalize_compare_numeric_range(
                                    card["power_min"],
                                    card["power_max"],
                                    9999.0,
                                )
                                filters["enable_power_filter"] = True
                                filters["power_range"] = (pmin, pmax)
                            if card["enable_capacity_filter"] and (card["capacity_min"] or card["capacity_max"]):
                                cmin, cmax = _normalize_compare_numeric_range(
                                    card["capacity_min"],
                                    card["capacity_max"],
                                    99999.0,
                                )
                                filters["enable_capacity_filter"] = True
                                filters["capacity_range"] = (cmin, cmax)
                            if card.get("include_rhd") and is_personal_car:
                                filters["include_rhd"] = True

                            # TODO: No dedicated compare trend endpoint; reuse region fuel trend for series.
                            kpis = load_region_kpis(filters) or {}
                            total_reg = float(kpis.get("total_reg", 0))
                            avg_age = float(kpis.get("avg_age_years", 0))
                            model_label = _format_compare_models(card.get("models", []))
                            subtype_label = _format_compare_subtypes(card.get("vehicle_subtypes", []))
                            summaries.append(
                                {
                                    "series": card.get("series", ""),
                                    "brand": card["brand"],
                                    "model": model_label,
                                    "subtype": subtype_label,
                                    "total": total_reg,
                                    "avg_age": avg_age,
                                }
                            )
                            trend_df = as_df(load_region_fuel_trend(filters))
                            if trend_df is None or trend_df.empty:
                                continue

                            trend_df = (
                                trend_df.groupby("registration_date", as_index=False)["total_count"]
                                .sum()
                                .sort_values("registration_date")
                            )
                            label = _build_compare_label(card)
                            trend_df["label"] = label
                            trend_df["series"] = card.get("series", "")
                            trend_df["brand"] = card["brand"]
                            trend_df["model"] = model_label
                            trend_df["subtype"] = subtype_label
                            results.append(trend_df)

                    if results:
                        combined = pd.concat(results, ignore_index=True)
                        st.session_state.compare_results = combined
                    else:
                        st.session_state.compare_results = pd.DataFrame()

                    summary_df = pd.DataFrame(summaries)
                    if not summary_df.empty:
                        total_group = summary_df["total"].sum()
                        summary_df["share_pct"] = (
                            summary_df["total"] / total_group * 100.0 if total_group > 0 else 0.0
                        )
                        summary_df["series_order"] = summary_df["series"].apply(
                            lambda s: COMPARE_SERIES_ORDER.index(s)
                            if s in COMPARE_SERIES_ORDER
                            else len(COMPARE_SERIES_ORDER)
                        )
                        summary_df = summary_df.sort_values("series_order").reset_index(drop=True)
                        st.session_state.compare_summary = summary_df
                    else:
                        st.session_state.compare_summary = pd.DataFrame()

    results = st.session_state.compare_results
    if results is None:
        st.info("Ustaw filtry i kliknij **Porównaj**.")
        return

    if results.empty:
        st.warning("Brak danych dla wybranych pojazdów i filtrów.")
        return

    st.markdown("## Rozkład rejestracji w czasie (porównanie)")
    st.caption(
        "CEPiK nie zawiera każdego historycznego zdarzenia rejestracji. "
        "Jeśli pojazd był rejestrowany wielokrotnie, widoczna jest tylko ostatnia rejestracja. "
        "Wykres pokazuje rozkład rejestracji w czasie, a nie realny trend."
    )
    fig = px.line(
        results,
        x="registration_date",
        y="total_count",
        color="label",
        markers=False,
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=40, b=0),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
    )
    st.plotly_chart(fig, use_container_width=True)

    summary = st.session_state.compare_summary
    if summary is None or summary.empty:
        return

    st.markdown("## Podsumowanie porównania")
    table = summary.rename(
        columns={
            "series": "Identyfikator",
            "brand": "Marka",
            "model": "Model",
            "subtype": "Podrodzaj",
            "total": "Liczba pojazdów",
            "avg_age": "Śr. wiek",
            "share_pct": "% grupy",
        }
    )
    table["Liczba pojazdów"] = table["Liczba pojazdów"].map(format_int_pl)
    table["Śr. wiek"] = table["Śr. wiek"].map(lambda x: f"{x:.1f}")
    table["% grupy"] = table["% grupy"].map(lambda x: f"{x:.2f} %")
    st.dataframe(
        table[
            [
                "Identyfikator",
                "Marka",
                "Model",
                "Podrodzaj",
                "Liczba pojazdów",
                "Śr. wiek",
                "% grupy",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )


# ============================================
#    ROUTING
# ============================================

view = st.sidebar.radio(
    "Widok",
    options=["Region", "Compare"],
    index=0,
)

if view == "Region":
    render_region_view()
else:
    render_compare_view()
