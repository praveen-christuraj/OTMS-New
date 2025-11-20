# oil_app_ui.py
import streamlit as st
import pandas as pd
from datetime import datetime, date, time, timedelta
dt_time = time
from pathlib import Path
from ui import header
import sys
import os
import shutil
import asyncio
from io import BytesIO
import base64
import streamlit.components.v1 as components
from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm, mm
from reportlab.lib import colors
from reportlab.lib.utils import ImageReader
import math
CONDENSATE_M3_TO_BBL = 6.289
WAT60_CONST = 999.012
from db import get_session, init_db, engine
from material_balance_calculator import MaterialBalanceCalculator as MBC
from sqlalchemy import text, func, and_, or_
from sqlalchemy.orm import joinedload
from sqlalchemy.exc import IntegrityError
import plotly.graph_objects as go
from logger import log_info, log_error, log_warning, log_debug
from security import SecurityManager
from task_manager import TaskManager
from typing import Any, Dict, Optional, List, Tuple
from collections import defaultdict
from functools import lru_cache
import re
import builtins
# Models (aligned with models.py)
from models import (
    Tank, TankStatus, YadeBarge,
    CalibrationTank, YadeCalibration, Table11,
    TankTransaction, Operation, MeterTransaction,
    YadeVoyage, YadeDip,
    CargoKind, DestinationKind, LoadingBerthKind,
    OTRRecord, GPPProductionRecord, RiverDraftRecord, ProducedWaterRecord,
    TOAYadeStage, TOAYadeSummary, YadeSealDetail, YadeSampleParam, AuditLog, User, LoginAttempt,
    Task, TaskActivity, TaskStatus, TaskType,
    OFSProductionEvacuationRecord, LocationTankerEntry, RecycleBinEntry,
)
from uuid import uuid4
import hashlib
import html
from timezone_utils import format_local_datetime, get_local_time
from recycle_bin import RecycleBinManager
# --- paths for assets ---
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent
ASSETS   = BASE_DIR / "assets"
LOGOS    = ASSETS / "logos"
ICONS    = ASSETS / "icons"
OUTPUT   = BASE_DIR / "output"
OUTPUT.mkdir(exist_ok=True)
import reportlab
init_db()
st.set_page_config(page_title="OTMS", page_icon="🛢️", layout="wide")
st.session_state.setdefault("auth_user", None)
PAGE_OPTIONS = [
    "Home",
    "Manage Locations",
    "Manage Users",
    "Tank Transactions",
    "Yade Transactions",
    "Yade Tracking",
    "Tanker Transactions",
    "TOA-Yade",
    "View Transactions",
    "OTR",
    "BCCR",
    "Material Balance",
    "Add Asset",
    "Location Settings",
    "Recycle Bin",
    "Audit Log",
    "Backup & Recovery",
    "2FA Verify",
    "My Tasks",
    "2FA Settings",
    "Login History",
    "OTR-Vessel",
    "Convoy Status",
    "Reporting",
    "Yade-Vessel Mapping",
    "FSO-Operations",
]
_initial_page = st.session_state.get("page") or PAGE_OPTIONS[0]
page = st.sidebar.selectbox("Page", PAGE_OPTIONS, index=PAGE_OPTIONS.index(_initial_page), key="_nav_page_select")
st.session_state["page"] = page

# --- Convoy Status constants ---
CONVOY_STATUS_ALLOWED_LOCATIONS = {"agge", "utapate", "lagos ho", "lagos (ho)"}
CONVOY_STATUS_YADE_STATUS_OPTIONS = [
    "AT JETTY",
    "AT AGGE",
    "TOWARDS JETTY",
    "TOWARDS AGGE",
    "SALVAGE",
    "MAINTENANCE",
    "GROUNDED",
    "TANK CLEANING",
]
CONVOY_STATUS_VESSEL_NAMES = [
    "MT VEDMATA",
    "MT VISHWAMATA",
    "MT SUNDARI",
    "MT VARUNI",
    "MT VAMIKA",
    "MT SURBHI",
    "MT SATYA",
    "MT SIDDHI",
    "MT RADHIKA",
    "MT TULJA TANVI",
    "MT TULJA KALYANI",
]
CONVOY_STATUS_SPECIAL_VESSEL_LOCATIONS = {
    "MT TULJA TANVI": ("agge",),
    "MT TULJA KALYANI": ("utapate", "oml-13"),
}
CONVOY_STATUS_SPECIAL_VESSEL_ORDER = ["MT TULJA TANVI", "MT TULJA KALYANI"]
CONVOY_STATUS_SPECIAL_VESSELS = set(CONVOY_STATUS_SPECIAL_VESSEL_LOCATIONS.keys())
CONVOY_STATUS_VESSEL_STATUS_OPTIONS = [
    "ANCHOR POSITION",
    "O/A EMPTY",
    "O/A LOADED",
    "I/A EMPTY",
    "I/A LOADED",
    "RECEIVING YADE",
    "EXPORT",
    "DRY DOCK",
    "MAINTENANCE",
    "TOWARDS FSO",
    "TOWARDS AGGE",
    "DISCHARGING",
    "BUNKERING",
]
FSO_MATERIAL_BALANCE_STATE_KEYS = [
    "fso_mb_df",
    "fso_mb_table",
    "fso_material_balance_df",
    "fso_mb_daily",
    "fso_mb_cache",
    "fso_mb_summary_df",
    "fso_mb_pivot",
    "fso_mb_records",
]

# --- detect / load reportlab at runtime (avoids "install reportlab" loop) ---
import importlib

# --- Report Builder Source Metadata ------------------------------------------------------------
REPORT_DAY_START_TIME = time(6, 1)
REPORT_SOURCE_DEFINITIONS = [
    {
        "key": "otr_vessel",
        "table": "otr_vessel",
        "label": "OTR Vessel",
        "class_name": "OTRVessel",
        "date_field": "date",
        "time_field": "time",
        "location_field": "location_id",
        "sort": ("date", "time"),
        "aliases": ["OTRVessel"],
    },
    {
        "key": "convoy_status_vessel",
        "table": "convoy_status_vessel",
        "label": "Convoy Status (Vessel)",
        "class_name": "ConvoyStatusVessel",
        "date_field": "date",
        "time_field": None,
        "location_field": "location_id",
        "sort": ("date", "vessel_name"),
        "aliases": ["ConvoyStatusVessel"],
    },
    {
        "key": "convoy_status_yade",
        "table": "convoy_status_yade",
        "label": "Convoy Status (YADE)",
        "class_name": "ConvoyStatusYade",
        "date_field": "date",
        "time_field": None,
        "location_field": "location_id",
        "sort": ("date", "yade_barge_id"),
        "aliases": ["ConvoyStatusYade"],
    },
    {
        "key": "fso_operations",
        "table": "fso_operations",
        "label": "FSO Operations",
        "class_name": "FSOOperation",
        "date_field": "date",
        "time_field": "time",
        "location_field": "location_id",
        "sort": ("date", "time"),
        "aliases": ["FSOOperation"],
    },
    {
        "key": "gpp_production_records",
        "table": "gpp_production_records",
        "label": "GPP Production",
        "class_name": "GPPProductionRecord",
        "date_field": "date",
        "time_field": None,
        "location_field": "location_id",
        "sort": ("date",),
        "aliases": ["GPPProductionRecord"],
    },
    {
        "key": "tank_transactions",
        "table": "tank_transactions",
        "label": "Tank Transactions",
        "class_name": "TankTransaction",
        "date_field": "date",
        "time_field": "time",
        "location_field": "location_id",
        "sort": ("date", "time"),
        "aliases": ["TankTransaction"],
    },
    {
        "key": "location_tanker_entries",
        "table": "location_tanker_entries",
        "label": "Location Tanker Entries",
        "class_name": "LocationTankerEntry",
        "date_field": "date",
        "time_field": None,
        "location_field": "location_id",
        "sort": ("date", "serial_no"),
        "aliases": ["LocationTankerEntry"],
    },
    {
        "key": "meter_transactions",
        "table": "meter_transactions",
        "label": "Meter Transactions",
        "class_name": "MeterTransaction",
        "date_field": "date",
        "time_field": None,
        "location_field": "location_id",
        "sort": ("date",),
        "aliases": ["MeterTransaction"],
    },
    {
        "key": "ofs_production_evacuation_records",
        "table": "ofs_production_evacuation_records",
        "label": "OFS Production & Evacuation",
        "class_name": "OFSProductionEvacuationRecord",
        "date_field": "date",
        "time_field": None,
        "location_field": "location_id",
        "sort": ("date", "serial_no"),
        "aliases": ["OFSProductionEvacuationRecord"],
    },
    {
        "key": "produced_water_records",
        "table": "produced_water_records",
        "label": "Produced Water Records",
        "class_name": "ProducedWaterRecord",
        "date_field": "date",
        "time_field": None,
        "location_field": "location_id",
        "sort": ("date",),
        "aliases": ["ProducedWaterRecord"],
    },
    {
        "key": "river_draft_records",
        "table": "river_draft_records",
        "label": "River Draft Records",
        "class_name": "RiverDraftRecord",
        "date_field": "date",
        "time_field": None,
        "location_field": "location_id",
        "sort": ("date",),
        "aliases": ["RiverDraftRecord"],
    },
    {
        "key": "tanker_transactions",
        "table": "tanker_transactions",
        "label": "Tanker Transactions",
        "class_name": "TankerTransaction",
        "date_field": "transaction_date",
        "time_field": "transaction_time",
        "location_field": "location_id",
        "sort": ("transaction_date", "transaction_time"),
        "aliases": ["TankerTransaction"],
    },
    {
        "key": "toa_yade_summary",
        "table": "toa_yade_summary",
        "label": "TOA YADE Summary",
        "class_name": "TOAYadeSummary",
        "date_field": "date",
        "time_field": "time",
        "location_field": None,
        "sort": ("date", "time"),
        "aliases": ["TOAYadeSummary"],
    },
    {
        "key": "otr_records",
        "table": "otr_records",
        "label": "OTR Records",
        "class_name": "OTRRecord",
        "date_field": "date",
        "time_field": "time",
        "location_field": "location_id",
        "sort": ("date", "time"),
        "aliases": ["OTRRecord"],
        "extra_fields": [
            "Net Rece/Disp (bbls)",
            "Net Water Rece/Disp (bbls)",
        ],
    },
    {
        "key": "toa_tanker",
        "table": "toa_tanker",
        "label": "TOA Tanker",
        "class_name": "TOATanker",
        "date_field": "transaction_date",
        "time_field": None,
        "location_field": "location_id",
        "sort": ("transaction_date", "tanker_name"),
        "aliases": ["TOATanker"],
    },
    {
        "key": "material_balance",
        "table": "material_balance",
        "label": "Material Balance",
        "class_name": None,
        "date_field": "Date",
        "time_field": None,
        "location_field": None,
        "sort": ("Date",),
        "aliases": ["MaterialBalance"],
        "type": "material_balance",
    },
    {
        "key": "fso_material_balance",
        "table": "fso_material_balance",
        "label": "FSO Material Balance",
        "class_name": None,
        "date_field": "Date",
        "time_field": None,
        "location_field": None,
        "sort": ("Date",),
        "aliases": ["FSO-MaterialBalance"],
        "type": "fso_material_balance",
    },
    {
        "key": "condensate",
        "table": "tank_transactions",
        "label": "Condensate Receipts",
        "class_name": "TankTransaction",
        "date_field": "date",
        "time_field": "time",
        "location_field": "location_id",
        "sort": ("date", "time"),
        "aliases": ["Condensate"],
        "filter_mode": "condensate_only",
    },
]
REPORT_SOURCE_MAP = {entry["key"]: entry for entry in REPORT_SOURCE_DEFINITIONS}
REPORT_SOURCE_ALIAS_MAP = {}
for entry in REPORT_SOURCE_DEFINITIONS:
    REPORT_SOURCE_ALIAS_MAP[entry["key"].lower()] = entry["key"]
    for alias in entry.get("aliases", []):
        REPORT_SOURCE_ALIAS_MAP[alias.lower()] = entry["key"]

PRIMARY_SOURCE_ORDER = [
    "otr_vessel",
    "convoy_status_vessel",
    "convoy_status_yade",
    "fso_operations",
    "gpp_production_records",
    "tank_transactions",
    "location_tanker_entries",
    "meter_transactions",
    "ofs_production_evacuation_records",
    "produced_water_records",
    "river_draft_records",
    "tanker_transactions",
    "toa_yade_summary",
    "otr_records",
    "toa_tanker",
    "material_balance",
    "fso_material_balance",
    "condensate",
]

@lru_cache(maxsize=None)
def _get_model_class(class_name: str):
    models_module = importlib.import_module("models")
    return getattr(models_module, class_name)

def _resolve_source_key(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    key = str(value).strip()
    if not key:
        return None
    if key in REPORT_SOURCE_MAP:
        return key
    lowered = key.lower()
    return REPORT_SOURCE_ALIAS_MAP.get(lowered, None)

def _get_source_meta(value: Optional[str]) -> Optional[dict]:
    key = _resolve_source_key(value)
    if not key:
        return None
    return REPORT_SOURCE_MAP.get(key)

@lru_cache(maxsize=None)
def _get_source_fields(value: Optional[str]) -> list[str]:
    meta = _get_source_meta(value)
    if not meta:
        return []
    model_name = meta.get("class_name")
    if not model_name:
        return []
    try:
        model_cls = _get_model_class(model_name)
        fields = list(model_cls.__table__.columns.keys())
    except Exception:
        fields = []
    if meta.get("extra_fields"):
        fields.extend(meta["extra_fields"])
    return fields

@lru_cache(maxsize=None)
def _get_model_columns(value: Optional[str]) -> list[str]:
    meta = _get_source_meta(value)
    if not meta:
        return []
    try:
        model_cls = _get_model_class(meta["class_name"])
        return list(model_cls.__table__.columns.keys())
    except Exception:
        return []

def _available_primary_source_keys() -> list[str]:
    ordered_keys = []
    for key in PRIMARY_SOURCE_ORDER:
        if key in REPORT_SOURCE_MAP and key not in ordered_keys:
            ordered_keys.append(key)
    for key in REPORT_SOURCE_MAP.keys():
        if key not in ordered_keys:
            ordered_keys.append(key)
    return ordered_keys

def _format_source_label(key: str) -> str:
    meta = REPORT_SOURCE_MAP.get(key)
    if not meta:
        return key
    return f"{meta['label']} ({meta['table']})"

def _format_source_option(value: str) -> str:
    meta = _get_source_meta(value)
    if meta:
        return _format_source_label(meta["key"])
    if value:
        return f"{value} (legacy)"
    return "(legacy)"

def _derive_report_date(base_date: Optional[date], maybe_time) -> Optional[date]:
    if not base_date:
        return None
    if maybe_time is None:
        return base_date
    t_obj = convert_to_time_object(maybe_time)
    if not isinstance(t_obj, time):
        return base_date
    if t_obj < REPORT_DAY_START_TIME:
        return base_date - timedelta(days=1)
    return base_date

def _combine_for_bucket(base_date: Optional[date], maybe_time) -> Optional[datetime]:
    if not base_date:
        return None
    t_obj = convert_to_time_object(maybe_time) if maybe_time is not None else REPORT_DAY_START_TIME
    if not isinstance(t_obj, time):
        t_obj = REPORT_DAY_START_TIME
    return datetime.combine(base_date, t_obj)

def _pluck_value(row: Any, field: Optional[str]):
    if not field:
        return None
    try:
        return getattr(row, field)
    except Exception:
        if isinstance(row, dict):
            return row.get(field)
    return None

def _matches_operation_filter(row: Any, expected: str) -> bool:
    if not expected:
        return True
    op_val = _pluck_value(row, "operation")
    if op_val is None:
        return False
    op_text = getattr(op_val, "value", op_val)
    return str(op_text) == expected

def _get_location_obj(session, loc_id: Optional[int]):
    if not loc_id:
        return None
    try:
        from models import Location
        return session.query(Location).filter(Location.id == int(loc_id)).one_or_none()
    except Exception:
        return None

def _calculate_material_balance_rows(session, location_id: Optional[int], date_from: date, date_to: date):
    loc_obj = _get_location_obj(session, location_id)
    if not loc_obj:
        return []
    try:
        rows = MBC.calculate_material_balance(
            None,
            getattr(loc_obj, "code", ""),
            date_from,
            date_to,
            location_id=location_id,
        )
    except Exception:
        rows = []
    return rows or []

def _material_balance_columns_for_location(location_id: Optional[int]) -> list[str]:
    if not location_id:
        return []
    try:
        with get_session() as s_loc:
            sample_rows = _calculate_material_balance_rows(s_loc, location_id, date.today() - timedelta(days=7), date.today())
    except Exception:
        sample_rows = []
    if not sample_rows:
        return ["Date"]
    try:
        df_tmp = pd.DataFrame(sample_rows)
        return list(df_tmp.columns)
    except Exception:
        sample = sample_rows[0]
        if isinstance(sample, dict):
            return list(sample.keys())
    return ["Date"]

_COL_INDEX_PATTERN = re.compile(r"\bcolumn\s*(\d+)\b", re.IGNORECASE)
_BRACKETED_COL_PATTERN = re.compile(r"\[(.+?)\]")

def _normalize_calc_expression(expr: str, columns: list[str]) -> str:
    def _idx_repl(match):
        idx = int(match.group(1)) - 1
        if 0 <= idx < len(columns):
            return f"`{columns[idx]}`"
        return match.group(0)
    result = _COL_INDEX_PATTERN.sub(_idx_repl, expr)
    def _bracket_repl(match):
        name = match.group(1).strip()
        return f"`{name}`" if name else match.group(0)
    result = _BRACKETED_COL_PATTERN.sub(_bracket_repl, result)
    return result

def _fetch_source_rows(
    source_key: str,
    session,
    location_id: Optional[int],
    date_from: date,
    date_to: date,
):
    meta = REPORT_SOURCE_MAP.get(source_key)
    if not meta:
        return []
    special_type = meta.get("type")
    if special_type == "material_balance":
        return _calculate_material_balance_rows(session, location_id, date_from, date_to)
    if special_type == "fso_material_balance":
        return []
    try:
        model_cls = _get_model_class(meta["class_name"])
    except Exception:
        return []
    query = session.query(model_cls)
    location_field = meta.get("location_field")
    if location_field and location_id:
        try:
            query = query.filter(getattr(model_cls, location_field) == location_id)
        except Exception:
            pass
    date_attr = getattr(model_cls, meta["date_field"], None)
    if date_attr is not None:
        range_start = date_from - timedelta(days=1) if meta.get("time_field") else date_from
        range_end = date_to + timedelta(days=1) if meta.get("time_field") else date_to
        query = query.filter(date_attr >= range_start, date_attr <= range_end)
    sort_fields = meta.get("sort") or (meta["date_field"],)
    for field_name in sort_fields:
        sort_attr = getattr(model_cls, field_name, None)
        if sort_attr is not None:
            query = query.order_by(sort_attr.asc())
    try:
        rows = query.all()
    except Exception:
        rows = []
    if meta.get("filter_mode") == "condensate_only":
        rows = [r for r in rows if getattr(r, "condensate_qty_m3", None) not in (None, 0)]
    return rows

def _load_rows_for_any_source(
    source_name: Optional[str],
    session,
    location_id: Optional[int],
    date_from: date,
    date_to: date,
) -> tuple[list[Any], Optional[str]]:
    canonical = _resolve_source_key(source_name)
    if canonical:
        return _fetch_source_rows(canonical, session, location_id, date_from, date_to), canonical
    if not source_name:
        return [], None
    normalized = str(source_name).strip()
    legacy_key = normalized.lower().replace("-", "").replace("_", "")
    legacy_map = {
        "materialbalance": "material_balance",
        "fsomaterialbalance": "fso_material_balance",
        "condensate": "condensate",
    }
    mapped = legacy_map.get(legacy_key)
    if mapped:
        return _fetch_source_rows(mapped, session, location_id, date_from, date_to), mapped
    return [], None

def _discover_source_fields(source_value: Optional[str], src_location_id: Optional[int]) -> list[str]:
    key = _resolve_source_key(source_value) or str(source_value or "")
    if key == "material_balance":
        return _material_balance_columns_for_location(src_location_id)
    if key == "fso_material_balance":
        return [
            "Date",
            "Opening Stock",
            "Opening Water",
            "Receipts",
            "Exports",
            "Closing Stock",
            "Closing Water",
            "Loss/Gain",
        ]
    if key == "condensate":
        return _get_source_fields("tank_transactions")
    try:
        return _get_source_fields(key)
    except Exception:
        return []

# --- YADE tracking helpers ----------------------------------------------------
_YADE_TRACKING_TARGETS = {
    "ASEMOKU": {
        "label": "Asemoku Jetty",
        "tokens": {"ASEMOKU", "ASEMOKUJETTY", "JETTY"},
    },
    "NDONI": {
        "label": "Ndoni",
        "tokens": {"NDONI"},
    },
    "AGGE": {
        "label": "Agge",
        "tokens": {"AGGE"},
    },
}


def _canonical_location_tokens(value: str | None) -> set[str]:
    """Normalize a location name/code into comparable tokens."""
    tokens: set[str] = set()
    if value is None:
        return tokens
    try:
        raw = str(value).strip().upper()
    except Exception:
        return tokens
    if not raw:
        return tokens
    cleaned = raw.replace(".", "")
    variants = {
        cleaned,
        cleaned.replace(" ", ""),
        cleaned.replace("-", ""),
        cleaned.replace("_", ""),
    }
    variants.add(cleaned.replace("JETTY", "").strip())
    tokens.update({v for v in variants if v})
    return tokens


def _resolve_yade_tracking_locations(session) -> Dict[str, Optional[Dict[str, Any]]]:
    """Return database metadata for the locations we need to stitch together."""
    from models import Location

    matches: Dict[str, Optional[Dict[str, Any]]] = {k: None for k in _YADE_TRACKING_TARGETS}
    try:
        all_locations = session.query(Location).all()
    except Exception:
        return matches

    for loc in all_locations:
        loc_tokens = _canonical_location_tokens(loc.code)
        loc_tokens.update(_canonical_location_tokens(loc.name))
        if not loc_tokens:
            continue
        for key, meta in _YADE_TRACKING_TARGETS.items():
            if matches[key]:
                continue
            if loc_tokens & meta["tokens"]:
                matches[key] = {"id": loc.id, "name": loc.name, "code": loc.code}
    return matches


def _load_yade_tracking_rows(session, location_ids: List[int]) -> List[Dict[str, Any]]:
    """Load voyage rows + NSV quantities for the supplied locations."""
    if not location_ids:
        return []
    voyages = (
        session.query(YadeVoyage)
        .filter(YadeVoyage.location_id.in_(location_ids))
        .order_by(YadeVoyage.date.desc(), YadeVoyage.time.desc())
        .all()
    )
    if not voyages:
        return []
    voyage_ids = [v.id for v in voyages]
    stage_rows = session.query(TOAYadeStage).filter(TOAYadeStage.voyage_id.in_(voyage_ids)).all()
    stage_map: Dict[int, Dict[str, TOAYadeStage]] = {}
    for stage_row in stage_rows:
        stage_key = (stage_row.stage or "").strip().lower()
        stage_map.setdefault(stage_row.voyage_id, {})[stage_key] = stage_row

    def _stage_nsv(stage_obj: Optional[TOAYadeStage]) -> Optional[float]:
        if not stage_obj:
            return None
        try:
            return round(float(getattr(stage_obj, "nsv_bbl", 0.0) or 0.0), 2)
        except Exception:
            return None

    rows: List[Dict[str, Any]] = []
    for voyage in voyages:
        per_stage = stage_map.get(voyage.id, {})
        loading_berth = voyage.loading_berth
        if hasattr(loading_berth, "value"):
            loading_berth = loading_berth.value
        rows.append(
            {
                "Date": voyage.date,
                "Convoy No": voyage.convoy_no or "",
                "Yade No": voyage.yade_name or "",
                "ROB qty": _stage_nsv(per_stage.get("before")),
                "TOB qty": _stage_nsv(per_stage.get("after")),
                "Loading berth": loading_berth or "",
                "_location_id": voyage.location_id,
                "_voyage_id": voyage.id,
            }
        )
    return rows
# ---- Streamlit safe rerun helper (prevents 'DeltaGenerator.rerun' errors) ----
def _st_safe_rerun():
    try:
        import streamlit as _stmod
        _stmod.rerun()
    except Exception:
        import streamlit as _stmod
        _stmod.experimental_rerun()

def _open_pdf_blob(pdf_bytes: bytes, filename: str = "OTMS.pdf") -> None:
    """Open a PDF blob in a new browser tab via base64 injection."""
    b64 = base64.b64encode(pdf_bytes).decode("ascii")
    components.html(
        f"""
        <script>
        (function(){{
            const b64="{b64}";
            const bytes=atob(b64);
            const len=bytes.length;
            const out=new Uint8Array(len);
            for(let i=0;i<len;i++){{out[i]=bytes.charCodeAt(i);}}
            const blob=new Blob([out],{{type:"application/pdf"}});
            const url=URL.createObjectURL(blob);
            const win=window.open(url,"_blank");
            if(!win){{alert("Please allow pop-ups for OTMS to display the PDF.");}}
            setTimeout(()=>URL.revokeObjectURL(url),120000);
        }})();
        </script>
        """,
        height=0,
    )

def _current_user_audit_context():
    """Return (username, user_id, location_id) from session safely."""
    u = st.session_state.get("auth_user") or {}
    username = u.get("username", "unknown")
    user_id = u.get("id")
    location_id = st.session_state.get("active_location_id")
    return username, user_id, location_id


def _list_supervisors(location_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """Return active supervisors (optionally filtered by location)."""
    with get_session() as s:
        supervisors = (
            s.query(User)
            .filter(
                User.role == "supervisor",
                User.is_active == True,  # noqa: E712
            )
        )
        if location_id:
            supervisors = supervisors.filter(
                or_(User.location_id == location_id, User.location_id.is_(None))
            )
        supervisors = supervisors.order_by(User.full_name, User.username).all()
        return [
            {
                "username": sup.username,
                "full_name": sup.full_name or sup.username,
                "location_id": sup.location_id,
            }
            for sup in supervisors
        ]


def _supervisor_dropdown(label: str, key: str, location_id: Optional[int]) -> tuple[Optional[str], Optional[str]]:
    """Render a supervisor selector and return (username, display_label)."""
    supervisors = _list_supervisors(location_id)
    if not supervisors:
        st.warning("No supervisors available for approval.")
        return None, None
    options = {
        f"{sup['full_name']} ({sup['username']})": sup["username"] for sup in supervisors
    }
    display = st.selectbox(label, list(options.keys()), key=key)
    return options.get(display), display


def _convoy_canonical_fso_code(value: Optional[str]) -> str:
    """Normalize FSO location codes (AGGE / OML-13)."""
    if not value:
        return ""
    text = str(value).strip().upper()
    simplified = text.replace(" ", "").replace("-", "")
    aliases = {
        "UTAPATE": "OML-13",
        "OML13": "OML-13",
        "OML 13": "OML-13",
        "OML-13": "OML-13",
        "AGGE": "AGGE",
    }
    if text in aliases:
        return aliases[text]
    return aliases.get(simplified, text)


def _convoy_fetch_mb_closing_value(
    selected_date: date, location_code: Optional[str]
) -> Tuple[Optional[str], Optional[float]]:
    """Fetch closing stock display/value from cached Material Balance artifacts."""
    canon_code = _convoy_canonical_fso_code(location_code)
    if not canon_code:
        return None, None
    target_norm = canon_code.replace("-", "").replace(" ", "")

    for state_key in FSO_MATERIAL_BALANCE_STATE_KEYS:
        if state_key not in st.session_state:
            continue
        raw_obj = st.session_state.get(state_key)
        try:
            df = raw_obj.copy() if isinstance(raw_obj, pd.DataFrame) else pd.DataFrame(raw_obj)
        except Exception:
            continue
        if df is None or df.empty:
            continue

        date_col = next(
            (c for c in df.columns if str(c).strip().lower() in {"date", "mb date", "as of", "asof"}),
            None,
        )
        if not date_col:
            continue

        dfx = df.copy()
        dfx[date_col] = pd.to_datetime(dfx[date_col], errors="coerce").dt.date
        dfx = dfx[dfx[date_col] == selected_date]
        if dfx.empty:
            continue

        loc_col = next(
            (c for c in dfx.columns if str(c).strip().lower() in {"location", "loc", "site", "code"}),
            None,
        )
        if loc_col:
            dfx["_convoy_loc_norm"] = (
                dfx[loc_col]
                .astype(str)
                .str.upper()
                .str.replace(" ", "", regex=False)
                .str.replace("-", "", regex=False)
            )
            dfx = dfx[dfx["_convoy_loc_norm"] == target_norm]
            if dfx.empty:
                continue

        close_candidates = [
            "closing stock (bbls)",
            "closing stock",
            "closing_stock",
            "closing_stock_bbl",
        ]
        close_col = None
        columns_lower = {str(col).strip().lower(): col for col in dfx.columns}
        for cand in close_candidates:
            if cand in columns_lower:
                close_col = columns_lower[cand]
                break
        if not close_col:
            continue

        display_value = None
        numeric_value = None
        for raw_val in dfx[close_col]:
            if raw_val is None:
                continue
            raw_str = str(raw_val).strip()
            if not raw_str:
                continue
            numeric_series = pd.to_numeric(pd.Series([raw_val]), errors="coerce").dropna()
            if numeric_series.empty:
                continue
            display_value = raw_str
            numeric_value = float(numeric_series.iloc[-1])
        if display_value is not None or numeric_value is not None:
            return display_value, numeric_value

    return None, None


def _convoy_fallback_closing_from_ops(
    selected_date: date, location_id: Optional[int], vessel_name: Optional[str]
) -> Optional[float]:
    """Fallback closing stock lookup from FSOOperation entries."""
    if not location_id or not vessel_name:
        return None
    from models import FSOOperation  # local import to avoid circular deps

    ext_from = selected_date - timedelta(days=1)
    ext_to = selected_date + timedelta(days=1)
    with get_session() as s:
        rows = (
            s.query(FSOOperation)
            .filter(
                FSOOperation.location_id == location_id,
                FSOOperation.fso_vessel == vessel_name,
                FSOOperation.date >= ext_from,
                FSOOperation.date <= ext_to,
            )
            .order_by(FSOOperation.date, FSOOperation.time)
            .all()
        )
    if not rows:
        return None

    win_start = datetime.combine(selected_date, dt_time(6, 1))
    win_end = datetime.combine(selected_date + timedelta(days=1), dt_time(6, 0))

    def _safe_time(val):
        if isinstance(val, dt_time):
            return val
        try:
            return datetime.strptime(str(val), "%H:%M").time()
        except Exception:
            return dt_time(0, 0)

    period_rows = []
    for row in rows:
        try:
            ts = datetime.combine(row.date, _safe_time(row.time))
        except Exception:
            continue
        if win_start <= ts <= win_end:
            period_rows.append(row)
    if not period_rows:
        return None
    period_rows.sort(key=lambda r: datetime.combine(r.date, _safe_time(r.time)))
    last_entry = period_rows[-1]
    try:
        return float(last_entry.closing_stock or 0.0)
    except Exception:
        return None


def _normalize_date_value(val: Any) -> Optional[date]:
    """Convert supported date/datetime values to date objects."""
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return None


def _derive_filter_bounds(date_values: List[Any], fallback_days: int = 14) -> tuple[date, date]:
    """
    Return (min_date, max_date) bounds for live filters.
    When there are no valid dates fall back to a recent rolling window ending today.
    """
    today_local = date.today()
    normalized = [
        d for d in (_normalize_date_value(v) for v in date_values) if d is not None
    ]
    if normalized:
        min_date = min(normalized)
        max_date = min(max(normalized), today_local)
        if min_date > max_date:
            min_date = max_date
    else:
        max_date = today_local
        min_date = today_local - timedelta(days=fallback_days)
    return min_date, max_date


def _ensure_date_key_in_bounds(
    key: str,
    min_value: date,
    max_value: date,
    default_value: Optional[date] = None,
) -> date:
    """Clamp st.session_state[key] within [min_value, max_value] and return it."""
    if default_value is None:
        default_value = min_value
    current = st.session_state.get(key, default_value)
    normalized = _normalize_date_value(current) or default_value
    if normalized < min_value:
        normalized = min_value
    if normalized > max_value:
        normalized = max_value
    st.session_state[key] = normalized
    return normalized


EDIT_LOCK_HOURS = 24


def _record_created_timestamp(record: Any) -> Optional[datetime]:
    """Best-effort extraction of the datetime a record was created."""
    ts = getattr(record, "created_at", None) or getattr(record, "timestamp", None)
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=None)
    date_val = getattr(record, "date", None)
    time_val = getattr(record, "time", None)
    if date_val:
        if isinstance(time_val, time):
            return datetime.combine(date_val, time_val)
        return datetime.combine(date_val, datetime.min.time())
    return None


def _is_edit_lock_active(record: Any, hours: int = EDIT_LOCK_HOURS) -> tuple[bool, Optional[datetime]]:
    """Return (locked, created_at) tuple for a record."""
    created_at = _record_created_timestamp(record)
    if not created_at:
        return False, None
    return (datetime.utcnow() - created_at) > timedelta(hours=hours), created_at


def _deny_edit_for_lock(record: Any, resource_type: str, label: Optional[str] = None) -> bool:
    """Check and block editing if record is older than the configured window."""
    locked, created_at = _is_edit_lock_active(record)
    if not locked:
        return False
    label = label or getattr(record, "ticket_id", None) or getattr(record, "id", "record")
    ts_display = format_local_datetime(created_at) if created_at else "unknown time"
    message = (
        f"Editing locked - {resource_type} {label} was created on {ts_display}. "
        f"Records older than {EDIT_LOCK_HOURS} hours cannot be modified."
    )
    st.warning(message)
    try:
        username, user_id, location_id = _current_user_audit_context()
        SecurityManager.log_audit(
            None,
            username,
            "UPDATE_BLOCKED",
            resource_type=resource_type,
            resource_id=str(getattr(record, "id", label)),
            details=message,
            user_id=user_id,
            location_id=location_id,
            success=False,
        )
    except Exception:
        pass
    return True


def _archive_record_for_delete(
    session,
    record: Any,
    resource_type: str,
    reason: Optional[str] = None,
    label: Optional[str] = None,
    extra_payload: Optional[Dict[str, Any]] = None,
):
    """Push the record into the recycle bin and log the action."""
    username, user_id, location_id = _current_user_audit_context()
    payload = RecycleBinManager.snapshot_record(record)
    if extra_payload:
        payload.update(extra_payload)
    entry = RecycleBinManager.archive_payload(
        session,
        resource_type=resource_type,
        resource_id=str(payload.get("id") or label or resource_type),
        payload=payload,
        username=username,
        user_id=user_id,
        location_id=location_id,
        reason=reason,
        label=label or payload.get("ticket_id") or payload.get("id"),
    )
    session.delete(record)
    try:
        SecurityManager.log_audit(
            session,
            username,
            "DELETE",
            resource_type=resource_type,
            resource_id=entry.resource_id,
            details=reason or f"Moved {resource_type} {entry.resource_id} to recycle bin.",
            user_id=user_id,
            location_id=location_id,
        )
    except Exception:
        pass
    return entry


def _archive_payload_for_delete(
    session,
    resource_type: str,
    resource_id: str,
    payload: Dict[str, Any],
    reason: Optional[str] = None,
    label: Optional[str] = None,
):
    """Archive raw payloads (for bulk deletes)."""
    username, user_id, location_id = _current_user_audit_context()
    entry = RecycleBinManager.archive_payload(
        session,
        resource_type=resource_type,
        resource_id=resource_id,
        payload=payload,
        username=username,
        user_id=user_id,
        location_id=location_id,
        reason=reason,
        label=label,
    )
    try:
        SecurityManager.log_audit(
            session,
            username,
            "DELETE",
            resource_type=resource_type,
            resource_id=resource_id,
            details=reason or f"Archived {resource_type} {resource_id} payload to recycle bin.",
            user_id=user_id,
            location_id=location_id,
        )
    except Exception:
        pass
    return entry


def _persist_toa_from_current_inputs(
    session,
    voyage_obj,
    yade_name: str,
    tank_ids: list,
    num_samples: int,
):
    """
    Persist TOA (Transfer of Account) data from current YADE voyage inputs.
    Creates TOA summary and stage records with ACTUAL calculations.
    """
    from models import TOAYadeSummary, TOAYadeStage, YadeDip, YadeSampleParam

    try:
        try:
            from yade_toa_calculator import calculate_yade_toa
        except ImportError:
            st.warning("⚠️ yade_toa_calculator not found. Using placeholder TOA values.")
            summary = TOAYadeSummary(
                voyage_id=voyage_obj.id,
                ticket_id=f"YADE-{voyage_obj.voyage_no}",
                date=voyage_obj.date,
                time=voyage_obj.time,
                yade_name=yade_name,
                convoy_no=voyage_obj.convoy_no,
                destination=voyage_obj.destination,
                loading_berth=voyage_obj.loading_berth,
                gsv_before_bbl=0.0,
                gsv_after_bbl=0.0,
                gsv_loaded_bbl=0.0,
            )
            session.add(summary)
            for stage_name in ["before", "after"]:
                stage = TOAYadeStage(
                    voyage_id=voyage_obj.id,
                    stage=stage_name,
                    gov_bbl=0.0,
                    gsv_bbl=0.0,
                    bsw_pct=0.0,
                    bsw_bbl=0.0,
                    nsv_bbl=0.0,
                    lt=0.0,
                    mt=0.0,
                    fw_bbl=0.0,
                )
                session.add(stage)
            return

        session.query(TOAYadeStage).filter(TOAYadeStage.voyage_id == voyage_obj.id).delete(
            synchronize_session=False
        )
        session.query(TOAYadeSummary).filter(TOAYadeSummary.voyage_id == voyage_obj.id).delete(
            synchronize_session=False
        )
        session.flush()

        before_dips_db = (
            session.query(YadeDip)
            .filter(YadeDip.voyage_id == voyage_obj.id, YadeDip.stage == "before")
            .all()
        )
        after_dips_db = (
            session.query(YadeDip)
            .filter(YadeDip.voyage_id == voyage_obj.id, YadeDip.stage == "after")
            .all()
        )

        before_params_db = (
            session.query(YadeSampleParam)
            .filter(YadeSampleParam.voyage_id == voyage_obj.id, YadeSampleParam.stage == "before")
            .first()
        )
        after_params_db = (
            session.query(YadeSampleParam)
            .filter(YadeSampleParam.voyage_id == voyage_obj.id, YadeSampleParam.stage == "after")
            .first()
        )

        dip_data = {"before": {}, "after": {}}
        for dip in before_dips_db:
            dip_data["before"][dip.tank_id] = {
                "total_cm": float(dip.total_cm or 0.0),
                "water_cm": float(dip.water_cm or 0.0),
            }
        for dip in after_dips_db:
            dip_data["after"][dip.tank_id] = {
                "total_cm": float(dip.total_cm or 0.0),
                "water_cm": float(dip.water_cm or 0.0),
            }

        sample_data = {
            "before": {
                "obs_mode": before_params_db.obs_mode if before_params_db else "Observed API",
                "obs_val": float(before_params_db.obs_val or 0.0) if before_params_db else 0.0,
                "sample_temp": float(before_params_db.sample_temp or 60.0) if before_params_db else 60.0,
                "tank_temp": float(before_params_db.tank_temp or 60.0) if before_params_db else 60.0,
                "bsw_pct": float(before_params_db.bsw_pct or 0.0) if before_params_db else 0.0,
                "ccf": float(before_params_db.ccf or 1.0) if before_params_db else 1.0,
            },
            "after": {
                "obs_mode": after_params_db.obs_mode if after_params_db else "Observed API",
                "obs_val": float(after_params_db.obs_val or 0.0) if after_params_db else 0.0,
                "sample_temp": float(after_params_db.sample_temp or 60.0) if after_params_db else 60.0,
                "tank_temp": float(after_params_db.tank_temp or 60.0) if after_params_db else 60.0,
                "bsw_pct": float(after_params_db.bsw_pct or 0.0) if after_params_db else 0.0,
                "ccf": float(after_params_db.ccf or 1.0) if after_params_db else 1.0,
            },
        }

        toa_result = calculate_yade_toa(
            yade_name=yade_name,
            dip_data=dip_data,
            sample_data=sample_data,
            session=session,
        )

        if not toa_result:
            toa_result = {
                "before": {"gov_bbl": 0, "gsv_bbl": 0, "bsw_pct": 0, "bsw_bbl": 0, "nsv_bbl": 0, "lt": 0, "mt": 0, "fw_bbl": 0},
                "after": {"gov_bbl": 0, "gsv_bbl": 0, "bsw_pct": 0, "bsw_bbl": 0, "nsv_bbl": 0, "lt": 0, "mt": 0, "fw_bbl": 0},
                "loaded": {"gsv_bbl": 0},
            }

        summary = TOAYadeSummary(
            voyage_id=voyage_obj.id,
            ticket_id=f"YADE-{voyage_obj.voyage_no}",
            date=voyage_obj.date,
            time=voyage_obj.time,
            yade_name=yade_name,
            convoy_no=voyage_obj.convoy_no,
            destination=voyage_obj.destination,
            loading_berth=voyage_obj.loading_berth,
            gsv_before_bbl=toa_result.get("before", {}).get("gsv_bbl", 0.0),
            gsv_after_bbl=toa_result.get("after", {}).get("gsv_bbl", 0.0),
            gsv_loaded_bbl=toa_result.get("loaded", {}).get("gsv_bbl", 0.0),
        )
        session.add(summary)

        for stage_name in ["before", "after"]:
            stage_data = toa_result.get(stage_name, {})
            stage = TOAYadeStage(
                voyage_id=voyage_obj.id,
                stage=stage_name,
                gov_bbl=stage_data.get("gov_bbl", 0.0),
                gsv_bbl=stage_data.get("gsv_bbl", 0.0),
                bsw_pct=stage_data.get("bsw_pct", 0.0),
                bsw_bbl=stage_data.get("bsw_bbl", 0.0),
                nsv_bbl=stage_data.get("nsv_bbl", 0.0),
                lt=stage_data.get("lt", 0.0),
                mt=stage_data.get("mt", 0.0),
                fw_bbl=stage_data.get("fw_bbl", 0.0),
            )
            session.add(stage)

        log_info(f"TOA data calculated and saved for voyage {voyage_obj.id}")
    except Exception as ex:
        log_error(f"Failed to persist TOA inputs for voyage {getattr(voyage_obj, 'id', '?')}: {ex}", exc_info=True)


TEMP_LIMITS = {
    "C": (0.0, 60.0),
    "F": (32.0, 120.0),
}
API_MIN, API_MAX = 15.0, 70.0
DENSITY_MIN, DENSITY_MAX = 600.0, 1000.0


def _normalize_temp_unit(unit: Optional[str]) -> str:
    """Normalize temperature units to either "C" or "F".

    The original implementation attempted to strip an unknown replacement
    character (�) that appeared when the degree symbol (°) was lost during
    encoding. In the corrected version we explicitly remove the degree
    symbol to ensure that inputs like "°C" and "°F" are handled properly.
    """
    label = (unit or "").strip().upper().replace("°", "")
    return "F" if label.startswith("F") else "C"


def _temperature_bounds(unit: Optional[str]) -> tuple[float, float]:
    norm = _normalize_temp_unit(unit)
    return TEMP_LIMITS.get(norm, TEMP_LIMITS["C"])


def _clamp_value(value: Optional[float], min_value: float, max_value: float) -> float:
    if value is None:
        return min_value
    try:
        numeric = float(value)
    except Exception:
        return min_value
    return max(min(numeric, max_value), min_value)


def _session_state_proxy():
    try:
        state = st.session_state
        if hasattr(state, "__contains__"):
            return state
    except Exception:
        return None


def _coerce_numeric_state(key: str, min_value: float, max_value: float):
    state_key = str(key)
    state = _session_state_proxy()
    if state is not None and state_key in state:
        state[state_key] = _clamp_value(state[state_key], min_value, max_value)


def _bounded_number_input(
    label: str,
    key: str,
    min_value: float,
    max_value: float,
    *,
    value: Optional[float] = None,
    **kwargs,
):
    state_key = str(key)
    state = _session_state_proxy()
    if state is not None:
        _coerce_numeric_state(state_key, min_value, max_value)
    params = dict(kwargs)
    params["min_value"] = min_value
    params["max_value"] = max_value
    params["key"] = state_key
    if "step" not in params:
        params["step"] = 0.1
    if value is not None:
        params["value"] = _clamp_value(value, min_value, max_value)
    return st.number_input(label, **params)


def _temperature_input(
    label: str,
    unit: Optional[str],
    key: str,
    *,
    value: Optional[float] = None,
    **kwargs,
):
    min_value, max_value = _temperature_bounds(unit)
    return _bounded_number_input(label, key, min_value, max_value, value=value, **kwargs)


def _observed_value_bounds(mode: str) -> tuple[float, float]:
    return (
        (DENSITY_MIN, DENSITY_MAX)
        if "density" in (mode or "").lower()
        else (API_MIN, API_MAX)
    )


_original_streamlit_error = st.error


def _otms_error(message, *args, **kwargs):
    """Wrap Streamlit error to auto-create admin tasks for critical failures."""
    result = _original_streamlit_error(message, *args, **kwargs)
    try:
        text_message = str(message)
        if TaskManager.should_capture_error(text_message):
            TaskManager.log_ui_error_task(
                text_message,
                st.session_state.get("auth_user"),
                st.session_state.get("active_location_id"),
            )
    except Exception:
        log_error("Unable to record UI error as task", exc_info=True)
    return result


st.error = _otms_error


def _format_task_timestamp(value):
    try:
        return format_local_datetime(value) if value else "-"
    except Exception:
        return "-"


# ------------------ OFS Production & Evacuation Helpers ------------------
def _next_ofs_serial(session, location_id: int) -> int:
    """
    Compute the next serial number for OFS production records for a location.
    Serial numbers increment by 1 per location and never reset unless records
    are purged. If no records exist, start from 1.
    """
    try:
        max_serial = session.query(func.max(OFSProductionEvacuationRecord.serial_no)).filter(
            OFSProductionEvacuationRecord.location_id == int(location_id)
        ).scalar() or 0
        return int(max_serial) + 1
    except Exception:
        return 1


def _next_tanker_serial(session, location_id: int) -> int:
    """Return next serial number for tanker entries per location."""
    try:
        max_serial = (
            session.query(func.max(LocationTankerEntry.serial_no))
            .filter(LocationTankerEntry.location_id == int(location_id))
            .scalar()
            or 0
        )
        return int(max_serial) + 1
    except Exception:
        return 1

def render_ofs_production_page(active_location_id: int, loc: Any, user: Dict[str, Any]):
    """
    Render the OFS Production & Evacuation tab for a given location.

    This page allows users to add, edit and view daily production and evacuation
    entries for OFS locations (e.g. OML-157). A simple form captures metrics
    for Oguali and Ukpichi production, other locations, evacuation and tanker
    counts. Records are displayed below with edit/delete controls and live
    date filters. Deletion is disabled for operator role.
    """
    import pandas as pd  # local import to avoid polluting top-level namespace
    from permission_manager import PermissionManager
    from security import SecurityManager

    user_role = (user.get("role") or "").lower()
    # Determine permissions: allow creation/edit if user can make entries
    with get_session() as _sess:
        can_make_entries = PermissionManager.can_make_entries(_sess, user_role, active_location_id)
    # Deletion is disabled for operators
    can_delete = can_make_entries and user_role != "operator"

    today = date.today()

    # Ensure session state defaults for form values
    def _ensure_ofs_defaults():
        ss = st.session_state
        ss.setdefault("ofs_form_date", today)
        ss.setdefault("ofs_form_oguali", 0.0)
        ss.setdefault("ofs_form_ukpichi", 0.0)
        ss.setdefault("ofs_form_other_locations", 0.0)
        ss.setdefault("ofs_form_evacuation", 0.0)
        ss.setdefault("ofs_form_tankers_oguali", 0.0)
        ss.setdefault("ofs_form_tankers_ukpichi", 0.0)
        ss.setdefault("ofs_form_other_tankers", 0.0)
        ss.setdefault("ofs_edit_id", None)

    def _reset_ofs_form():
        ss = st.session_state
        ss["ofs_edit_id"] = None
        ss["ofs_form_date"] = today
        ss["ofs_form_oguali"] = 0.0
        ss["ofs_form_ukpichi"] = 0.0
        ss["ofs_form_other_locations"] = 0.0
        ss["ofs_form_evacuation"] = 0.0
        ss["ofs_form_tankers_oguali"] = 0.0
        ss["ofs_form_tankers_ukpichi"] = 0.0
        ss["ofs_form_other_tankers"] = 0.0

    _ensure_ofs_defaults()
    is_editing = st.session_state.get("ofs_edit_id") is not None

    st.markdown("#### OFS Production & Evacuation")
    if is_editing:
        st.info(f"Editing record for {st.session_state.get('ofs_form_date')}")

    # ---- Input form ----
    with st.container(border=True):
        entry_cols = st.columns([0.15, 0.15, 0.15, 0.15, 0.1, 0.1, 0.1, 0.1])
        # Date
        with entry_cols[0]:
            date_val = st.date_input(
                "Date",
                value=st.session_state.get("ofs_form_date", today),
                key="ofs_prod_date_input",
            )
            st.session_state["ofs_form_date"] = date_val
        # Oguali Production
        with entry_cols[1]:
            oguali_val = st.number_input(
                "Oguali Production",
                min_value=0.0,
                step=1.0,
                value=float(st.session_state.get("ofs_form_oguali", 0.0)),
                key="ofs_prod_oguali_input",
            )
            st.session_state["ofs_form_oguali"] = oguali_val
        # Ukpichi Production
        with entry_cols[2]:
            ukpichi_val = st.number_input(
                "Ukpichi Production",
                min_value=0.0,
                step=1.0,
                value=float(st.session_state.get("ofs_form_ukpichi", 0.0)),
                key="ofs_prod_ukpichi_input",
            )
            st.session_state["ofs_form_ukpichi"] = ukpichi_val
        # Other Locations Production
        with entry_cols[3]:
            other_loc_val = st.number_input(
                "Other Locations",
                min_value=0.0,
                step=1.0,
                value=float(st.session_state.get("ofs_form_other_locations", 0.0)),
                key="ofs_prod_other_loc_input",
            )
            st.session_state["ofs_form_other_locations"] = other_loc_val
        # Evacuation
        with entry_cols[4]:
            evacuation_val = st.number_input(
                "Evacuation",
                min_value=0.0,
                step=1.0,
                value=float(st.session_state.get("ofs_form_evacuation", 0.0)),
                key="ofs_prod_evac_input",
            )
            st.session_state["ofs_form_evacuation"] = evacuation_val
        # Tankers - Oguali
        with entry_cols[5]:
            t_oguali_val = st.number_input(
                "Tankers - Oguali",
                min_value=0.0,
                step=1.0,
                value=float(st.session_state.get("ofs_form_tankers_oguali", 0.0)),
                key="ofs_prod_tankers_oguali_input",
            )
            st.session_state["ofs_form_tankers_oguali"] = t_oguali_val
        # Tankers - Ukpichi
        with entry_cols[6]:
            t_ukpichi_val = st.number_input(
                "Tankers - Ukpichi",
                min_value=0.0,
                step=1.0,
                value=float(st.session_state.get("ofs_form_tankers_ukpichi", 0.0)),
                key="ofs_prod_tankers_ukpichi_input",
            )
            st.session_state["ofs_form_tankers_ukpichi"] = t_ukpichi_val
        # Other Tankers
        with entry_cols[7]:
            t_other_val = st.number_input(
                "Other Tankers",
                min_value=0.0,
                step=1.0,
                value=float(st.session_state.get("ofs_form_other_tankers", 0.0)),
                key="ofs_prod_tankers_other_input",
            )
            st.session_state["ofs_form_other_tankers"] = t_other_val

        # Action buttons
        action_cols = st.columns([0.2, 0.2, 0.6])
        save_label = "Update" if is_editing else "Save"
        save_clicked = action_cols[0].button(
            save_label,
            type="primary",
            disabled=not can_make_entries,
            key="ofs_save_btn",
        )
        cancel_clicked = False
        if is_editing:
            cancel_clicked = action_cols[1].button("Cancel", key="ofs_cancel_btn")

        if cancel_clicked:
            _reset_ofs_form()
            _st_safe_rerun()

        if save_clicked:
            errors: List[str] = []
            if date_val is None:
                errors.append("Date is required.")
            # At least one production or evacuation or tankers should have positive value
            total_sum = (
                float(oguali_val or 0)
                + float(ukpichi_val or 0)
                + float(other_loc_val or 0)
                + float(evacuation_val or 0)
                + float(t_oguali_val or 0)
                + float(t_ukpichi_val or 0)
                + float(t_other_val or 0)
            )
            if total_sum <= 0:
                errors.append("At least one entry must be greater than zero.")
            if errors:
                for err in errors:
                    st.error(err)
            else:
                try:
                    with get_session() as sess:
                        if is_editing:
                            rec = (
                                sess.query(OFSProductionEvacuationRecord)
                                .filter(
                                    OFSProductionEvacuationRecord.id == int(st.session_state["ofs_edit_id"]),
                                    OFSProductionEvacuationRecord.location_id == active_location_id,
                                )
                                .one_or_none()
                            )
                            if not rec:
                                st.error("Selected record no longer exists.")
                            else:
                                rec.date = date_val
                                rec.oguali_production = float(oguali_val or 0.0)
                                rec.ukpichi_production = float(ukpichi_val or 0.0)
                                rec.other_locations = float(other_loc_val or 0.0)
                                rec.evacuation = float(evacuation_val or 0.0)
                                rec.tankers_oguali = float(t_oguali_val or 0.0)
                                rec.tankers_ukpichi = float(t_ukpichi_val or 0.0)
                                rec.other_tankers = float(t_other_val or 0.0)
                                rec.updated_by = user.get("username", "unknown")
                                SecurityManager.log_audit(
                                    sess,
                                    user.get("username", "unknown"),
                                    "UPDATE",
                                    resource_type="OFSProductionEvacuationRecord",
                                    resource_id=rec.id,
                                    details=f"Updated OFS record for {rec.date}",
                                    user_id=user.get("id"),
                                    location_id=active_location_id,
                                )
                                sess.commit()
                                st.success("Record updated successfully.")
                                _reset_ofs_form()
                                _st_safe_rerun()
                        else:
                            # Create new record with next serial
                            next_serial = _next_ofs_serial(sess, active_location_id)
                            rec = OFSProductionEvacuationRecord(
                                location_id=active_location_id,
                                serial_no=next_serial,
                                date=date_val,
                                oguali_production=float(oguali_val or 0.0),
                                ukpichi_production=float(ukpichi_val or 0.0),
                                other_locations=float(other_loc_val or 0.0),
                                evacuation=float(evacuation_val or 0.0),
                                tankers_oguali=float(t_oguali_val or 0.0),
                                tankers_ukpichi=float(t_ukpichi_val or 0.0),
                                other_tankers=float(t_other_val or 0.0),
                                created_by=user.get("username", "unknown"),
                            )
                            sess.add(rec)
                            sess.flush()
                            SecurityManager.log_audit(
                                sess,
                                user.get("username", "unknown"),
                                "CREATE",
                                resource_type="OFSProductionEvacuationRecord",
                                resource_id=rec.id,
                                details=f"Created OFS record for {date_val}",
                                user_id=user.get("id"),
                                location_id=active_location_id,
                            )
                            sess.commit()
                            st.success("Record saved successfully.")
                            _reset_ofs_form()
                            _st_safe_rerun()
                except Exception as ex:
                    st.error(f"Failed to save record: {ex}")

    with get_session() as sess:
        rows = (
            sess.query(OFSProductionEvacuationRecord)
            .filter(OFSProductionEvacuationRecord.location_id == active_location_id)
            .order_by(
                OFSProductionEvacuationRecord.date.desc(),
                OFSProductionEvacuationRecord.serial_no.desc(),
            )
            .all()
        )

    ofs_dates = [r.date for r in rows if isinstance(r.date, date)]
    ofs_min_date, ofs_max_date = _derive_filter_bounds(ofs_dates)
    ofs_from_default = _ensure_date_key_in_bounds(
        "ofs_filter_from", ofs_min_date, ofs_max_date, ofs_min_date
    )
    ofs_to_default = _ensure_date_key_in_bounds(
        "ofs_filter_to", ofs_min_date, ofs_max_date, ofs_max_date
    )

    # ---- Filters ----
    st.markdown("##### Live Filters")
    filter_cols = st.columns([0.3, 0.3, 0.4])
    with filter_cols[0]:
        filt_from = st.date_input(
            "From date",
            value=ofs_from_default,
            min_value=ofs_min_date,
            max_value=ofs_max_date,
            key="ofs_filter_from",
        )
    with filter_cols[1]:
        filt_to = st.date_input(
            "To date",
            value=ofs_to_default,
            min_value=ofs_min_date,
            max_value=ofs_max_date,
            key="ofs_filter_to",
        )
    with filter_cols[2]:
        search_term = st.text_input(
            "Search (not used yet)",
            key="ofs_filter_search",
        ).strip().lower()

    # ---- Load & display records (single table with actions) ----
    records_list = []
    for r in rows:
        records_list.append(
            {
                "id": r.id,
                "S.No": r.serial_no,
                "Date": r.date,
                "Oguali Production": r.oguali_production,
                "Ukpichi Production": r.ukpichi_production,
                "Other Locations": r.other_locations,
                "Evacuation": r.evacuation,
                "Tankers - Oguali": r.tankers_oguali,
                "Tankers - Ukpichi": r.tankers_ukpichi,
                "Other Tankers": r.other_tankers,
                "Created By": r.created_by,
                "Updated By": r.updated_by,
                "Updated At": r.updated_at,
            }
        )

    df = pd.DataFrame(records_list)
    if not df.empty:
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        if filt_from:
            df = df[df["Date"] >= filt_from]
        if filt_to:
            df = df[df["Date"] <= filt_to]
        # Note: search_term reserved for future use

    st.caption(f"{len(df)} record(s) shown")

    if df.empty:
        st.info("No records found for the selected filters.")
        return

    # ---------- Single table with header + rows + Actions ----------
    display_cols = [
        "S.No",
        "Date",
        "Oguali Production",
        "Ukpichi Production",
        "Other Locations",
        "Evacuation",
        "Tankers - Oguali",
        "Tankers - Ukpichi",
        "Other Tankers",
    ]

    st.markdown("##### Saved OFS Records")

    # Header row
    header_cols = st.columns([0.06, 0.11, 0.11, 0.11, 0.11, 0.11, 0.11, 0.11, 0.11, 0.12])
    header_labels = display_cols + ["Actions"]
    for col, label in zip(header_cols, header_labels):
        col.markdown(f"**{label}**")

    # Sort by date descending then serial descending
    manage_records = (
        df.sort_values(by=["Date", "S.No"], ascending=[False, False])
        .to_dict("records")
    )

    for rec in manage_records:
        cols = st.columns([0.06, 0.11, 0.11, 0.11, 0.11, 0.11, 0.11, 0.11, 0.11, 0.12])
        cols[0].write(int(rec["S.No"]))
        cols[1].write(str(rec["Date"]))
        cols[2].write(f"{rec['Oguali Production']:,}")
        cols[3].write(f"{rec['Ukpichi Production']:,}")
        cols[4].write(f"{rec['Other Locations']:,}")
        cols[5].write(f"{rec['Evacuation']:,}")
        cols[6].write(f"{rec['Tankers - Oguali']:,}")
        cols[7].write(f"{rec['Tankers - Ukpichi']:,}")
        cols[8].write(f"{rec['Other Tankers']:,}")

        # Actions column: place edit and delete buttons side by side
        with cols[9]:
            e_col, d_col = st.columns(2)
            edit_btn = e_col.button(
                "✏️", key=f"ofs_edit_{rec['id']}", disabled=not can_make_entries
            )
            delete_btn = d_col.button(
                "🗑️", key=f"ofs_delete_{rec['id']}", disabled=not can_delete
            )

        if edit_btn:
            allow_edit = True
            with get_session() as _lock_s:
                obj = (
                    _lock_s.query(OFSProductionEvacuationRecord)
                    .filter(
                        OFSProductionEvacuationRecord.id == int(rec["id"]),
                        OFSProductionEvacuationRecord.location_id == active_location_id,
                    )
                    .one_or_none()
                )
                if obj and _deny_edit_for_lock(obj, "OFSProductionEvacuationRecord", f"{obj.date}"):
                    allow_edit = False
            if allow_edit:
                st.session_state["ofs_edit_id"] = rec["id"]
                # populate form values
                st.session_state["ofs_form_date"] = rec["Date"]
                st.session_state["ofs_form_oguali"] = rec["Oguali Production"]
                st.session_state["ofs_form_ukpichi"] = rec["Ukpichi Production"]
                st.session_state["ofs_form_other_locations"] = rec["Other Locations"]
                st.session_state["ofs_form_evacuation"] = rec["Evacuation"]
                st.session_state["ofs_form_tankers_oguali"] = rec["Tankers - Oguali"]
                st.session_state["ofs_form_tankers_ukpichi"] = rec["Tankers - Ukpichi"]
                st.session_state["ofs_form_other_tankers"] = rec["Other Tankers"]
                _st_safe_rerun()

        if delete_btn:
            try:
                with get_session() as dsess:
                    row = (
                        dsess.query(OFSProductionEvacuationRecord)
                        .filter(
                            OFSProductionEvacuationRecord.id == int(rec["id"]),
                            OFSProductionEvacuationRecord.location_id == active_location_id,
                        )
                        .one_or_none()
                    )
                    if not row:
                        st.warning("Record already removed.")
                    else:
                        _archive_record_for_delete(
                            dsess,
                            row,
                            "OFSProductionEvacuationRecord",
                            reason=f"Marked OFS record for {row.date} for deletion.",
                            label=f"{row.date}",
                        )
                        dsess.commit()
                        st.success("Record moved to recycle bin.")
                        _reset_ofs_form()
                        _st_safe_rerun()
            except Exception as ex:
                st.error(f"Failed to delete record: {ex}")

def render_ofs_reports_tab(active_location_id: int, loc: Any, user: Optional[Dict[str, Any]] = None) -> None:
    """
    Render OFS Production & Evacuation reporting tab. This report is only shown for locations
    whose code normalizes to OML-157. It provides live date filters and allows export to
    CSV/XLSX/PDF or viewing the PDF inline.
    """
    import base64
    from io import BytesIO
    from datetime import date, timedelta

    import pandas as pd
    import streamlit as st
    import streamlit.components.v1 as components
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    from db import get_session
    from models import OFSProductionEvacuationRecord

    st.subheader("OFS Production & Evacuation Report")

    with get_session() as s:
        rows = (
            s.query(OFSProductionEvacuationRecord)
            .filter(OFSProductionEvacuationRecord.location_id == active_location_id)
            .order_by(OFSProductionEvacuationRecord.date, OFSProductionEvacuationRecord.id)
            .all()
        )

    data = []
    for r in rows:
        data.append(
            {
                "S.No": r.serial_no,
                "Date": r.date,
                "Oguali Production": r.oguali_production,
                "Ukpichi Production": r.ukpichi_production,
                "Other Locations": r.other_locations,
                "Evacuation": r.evacuation,
                "Tankers - Oguali": r.tankers_oguali,
                "Tankers - Ukpichi": r.tankers_ukpichi,
                "Other Tankers": r.other_tankers,
            }
        )

    df = pd.DataFrame(data)
    ofs_report_dates = df["Date"].tolist() if not df.empty else []
    rpt_min, rpt_max = _derive_filter_bounds(ofs_report_dates)
    rpt_from_default = _ensure_date_key_in_bounds(
        "ofs_rpt_from", rpt_min, rpt_max, rpt_min
    )
    rpt_to_default = _ensure_date_key_in_bounds(
        "ofs_rpt_to", rpt_min, rpt_max, rpt_max
    )

    # ---- Live Filters ----
    with st.container(border=True):
        c1, c2 = st.columns([0.5, 0.5])
        with c1:
            f_from: date = st.date_input(
                "From",
                value=rpt_from_default,
                min_value=rpt_min,
                max_value=rpt_max,
                key="ofs_rpt_from",
            )
        with c2:
            f_to: date = st.date_input(
                "To",
                value=rpt_to_default,
                min_value=rpt_min,
                max_value=rpt_max,
                key="ofs_rpt_to",
            )

    display_cols = [
        "S.No",
        "Date",
        "Oguali Production",
        "Ukpichi Production",
        "Other Locations",
        "Evacuation",
        "Tankers - Oguali",
        "Tankers - Ukpichi",
        "Other Tankers",
    ]

    if not df.empty:
        # Normalize Date
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        # Apply date filters
        if f_from:
            df = df[df["Date"] >= f_from]
        if f_to:
            df = df[df["Date"] <= f_to]

    st.caption(f"{len(df)} record(s) found")

    if df.empty:
        st.info("No records found for the selected date range.")
        return

    # Show table
    st.dataframe(df[display_cols], use_container_width=True, hide_index=True)

    # ---------- PDF builder (conventional OTMS style, A4 portrait, 0.5 cm margins) ----------
    def build_pdf(dataframe: pd.DataFrame) -> bytes:
        # Work on a copy to avoid mutating original df
        _df = dataframe.copy()

        # Convert numeric columns safely
        for col in _df.columns:
            if col not in ("S.No", "Date"):
                _df[col] = pd.to_numeric(_df[col], errors="coerce").fillna(0.0)

        buffer = BytesIO()

        # A4 portrait, 0.5 cm margins on all sides
        page_w, _page_h = A4
        lm = rm = tm = bm = 0.5 * cm

        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            leftMargin=lm,
            rightMargin=rm,
            topMargin=tm,
            bottomMargin=bm,
        )

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            "OFS_TITLE",
            parent=styles["Heading1"],
            fontSize=15,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#1f4788"),
        )
        sub_style = ParagraphStyle(
            "OFS_SUBTITLE",
            parent=styles["Normal"],
            fontSize=10,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#666666"),
        )

        elements = []
        # Title & subtitle (same convention as other reports)
        elements.append(Paragraph("<b>OFS PRODUCTION & EVACUATION</b>", title_style))
        elements.append(
            Paragraph(
                f"{loc.name} ({loc.code}) � Period: <b>{str(f_from)}</b> to <b>{str(f_to)}</b>",
                sub_style,
            )
        )
        elements.append(Spacer(1, 0.3 * cm))

        # Table headers & rows
        headers = display_cols
        table_data = [headers]

        for _, row in _df.iterrows():
            d = row["Date"]
            if hasattr(d, "strftime"):
                d_str = d.strftime("%Y-%m-%d")
            else:
                d_str = str(d)

            table_data.append(
                [
                    int(row["S.No"]) if pd.notna(row["S.No"]) else "",
                    d_str,
                    f"{float(row['Oguali Production']):,.2f}",
                    f"{float(row['Ukpichi Production']):,.2f}",
                    f"{float(row['Other Locations']):,.2f}",
                    f"{float(row['Evacuation']):,.2f}",
                    f"{float(row['Tankers - Oguali']):,.2f}",
                    f"{float(row['Tankers - Ukpichi']):,.2f}",
                    f"{float(row['Other Tankers']):,.2f}",
                ]
            )

        # Column widths scaled to perfectly fit A4 width within margins
        available_width = doc.width
        # S.No + Date slightly smaller, production/evac slightly wider
        weights = [0.6, 1.0, 1.2, 1.2, 1.2, 1.2, 1.0, 1.0, 1.0]
        scale = available_width / sum(weights)
        col_widths = [w * scale for w in weights]

        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        table.setStyle(
            TableStyle(
                [
                    # Header row
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4788")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 5),
                    ("TOPPADDING", (0, 0), (-1, 0), 5),
                    # Body rows
                    ("ALIGN", (0, 1), (-1, -1), "CENTER"),
                    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 1), (-1, -1), 7),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor("#f8f9fa")]),
                    # Grid & border
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ]
            )
        )

        elements.append(table)
        doc.build(elements)
        pdf_data = buffer.getvalue()
        buffer.close()
        return pdf_data

    # Build PDF bytes once (for download + view)
    pdf_bytes = build_pdf(df[display_cols]) if not df.empty else b""

    # ---- Export buttons (single row: CSV, XLSX, PDF, View PDF) ----
    st.markdown("---")
    col_csv, col_xlsx, col_pdf, col_view = st.columns(4)

    # CSV
    csv_data = df[display_cols].to_csv(index=False).encode("utf-8")
    with col_csv:
        st.download_button(
            "📥 CSV",
            data=csv_data,
            file_name="ofs_production_evacuation.csv",
            mime="text/csv",
            use_container_width=True,
        )

    # XLSX
    xlsx_buffer = BytesIO()
    with pd.ExcelWriter(xlsx_buffer, engine="xlsxwriter") as writer:
        df[display_cols].to_excel(writer, index=False, sheet_name="OFS_Report")
    xlsx_bytes = xlsx_buffer.getvalue()
    with col_xlsx:
        st.download_button(
            "📥 XLSX",
            data=xlsx_bytes,
            file_name="ofs_production_evacuation.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    # PDF Download
    with col_pdf:
        st.download_button(
            "📥 PDF",
            data=pdf_bytes,
            file_name="ofs_production_evacuation.pdf",
            mime="application/pdf",
            use_container_width=True,
            disabled=(not pdf_bytes),
        )

    # View PDF in new tab (same JS pattern as other reports)
    with col_view:
        if st.button("👁️ View PDF", use_container_width=True, disabled=(not pdf_bytes)):
            if pdf_bytes:
                b64 = base64.b64encode(pdf_bytes).decode("utf-8")
                components.html(
                    f"""
                    <script>
                        const pdfData = "{b64}";
                        const byteCharacters = atob(pdfData);
                        const byteNumbers = new Array(byteCharacters.length);
                        for (let i = 0; i < byteCharacters.length; i++) {{
                            byteNumbers[i] = byteCharacters.charCodeAt(i);
                        }}
                        const byteArray = new Uint8Array(byteNumbers);
                        const file = new Blob([byteArray], {{ type: "application/pdf" }});
                        const fileURL = URL.createObjectURL(file);
                        window.open(fileURL, "_blank");
                    </script>
                    """,
                    height=0,
                )


def _render_remote_delete_request_ui(
    resource_type: str,
    resource_id: str,
    resource_label: str,
    page_name: str,
    metadata: Optional[Dict[str, Any]] = None,
):
    """Display helper UI for operators to request remote delete approvals."""
    user = st.session_state.get("auth_user") or {}
    if user.get("role") != "operator":
        return None

    approved = TaskManager.get_task_for_resource(
        resource_type,
        resource_id,
        statuses=[TaskStatus.APPROVED.value],
    )
    pending = TaskManager.get_task_for_resource(
        resource_type,
        resource_id,
        statuses=[TaskStatus.PENDING.value],
    )

    if approved:
        approved_by = approved.get("approved_by") or "Supervisor"
        approved_at = _format_task_timestamp(approved.get("approved_at"))
        st.success(
            f"Remote approval granted by {approved_by} on {approved_at}. "
            "You may proceed with deletion."
        )
        return approved

    if pending:
        st.info(
            f"Remote approval requested on "
            f"{_format_task_timestamp(pending.get('raised_at'))}. "
            "Awaiting supervisor action."
        )
        return None

    key = f"remote_delete_req_{resource_type}_{resource_id}"
    if st.button("📤 Request remote approval", key=key):
        merged_meta = metadata.copy() if metadata else {}
        merged_meta.setdefault("page", page_name)
        TaskManager.create_delete_request(
            resource_type=resource_type,
            resource_id=resource_id,
            resource_label=resource_label,
            raised_by=user.get("username", "operator"),
            raised_by_role=user.get("role"),
            location_id=st.session_state.get("active_location_id"),
            metadata=merged_meta,
        )
        st.success("Request sent to the supervisors for this location.")
        import time

        time.sleep(1)
        _st_safe_rerun()
    return None

def _normalized_temp_unit(unit: str | None) -> str:
    """
    Collapse messy unit labels (e.g., '°C', '°F') into a simple token.
    """
    if unit is None:
        return ""
    try:
        raw = str(unit).strip().upper()
    except Exception:
        raw = ""
    if not raw:
        return ""
    letters = "".join(ch for ch in raw if ch.isalpha())
    if not letters:
        return ""
    if "C" in letters and "F" not in letters:
        return "C"
    if "F" in letters and "C" not in letters:
        return "F"
    if letters.endswith("C"):
        return "C"
    if letters.endswith("F"):
        return "F"
    return letters


def _temp_to_f(val: float | None, unit: str | None) -> float:
    """Normalize arbitrary input into degrees Fahrenheit."""
    try:
        num = float(val if val is not None else 0.0)
    except Exception:
        num = 0.0
    unit_norm = _normalized_temp_unit(unit)
    if unit_norm == "C":
        return (num * 1.8) + 32.0
    return num


def _temp_to_c(val: float | None, unit: str | None) -> float:
    """Normalize arbitrary input into degrees Celsius."""
    try:
        num = float(val if val is not None else 0.0)
    except Exception:
        num = 0.0
    unit_norm = _normalized_temp_unit(unit)
    if unit_norm == "F":
        return (num - 32.0) / 1.8
    return num


def convert_api_to_60_from_api(api_obs: float, sample_temp_val: float, temp_unit: str = "°F") -> float:
    """Iterative API@60 calculator using observed API + sample temperature."""
    if not api_obs or api_obs <= 0:
        return 0.0
    tf = _temp_to_f(sample_temp_val, temp_unit)
    temp_diff = tf - 60.0
    rho_obs = (141.5 * WAT60_CONST / (131.5 + float(api_obs))) * (
        (1.0 - 0.00001278 * temp_diff) - (0.0000000062 * temp_diff * temp_diff)
    )
    rho = rho_obs
    for _ in range(10):
        alfa = 341.0957 / (rho * rho)
        vcf = math.exp(-alfa * temp_diff - 0.8 * alfa * alfa * temp_diff * temp_diff)
        rho = rho_obs / vcf
    api60 = 141.5 * WAT60_CONST / rho - 131.5
    return round(api60, 2)


def convert_api_to_60_from_density(dens_obs_kgm3: float, sample_temp_val: float, temp_unit: str = "°C") -> float:
    """Iterative API@60 calculator using observed density + sample temperature."""
    if not dens_obs_kgm3 or dens_obs_kgm3 <= 0:
        return 0.0
    tc = _temp_to_c(sample_temp_val, temp_unit)
    temp_diff = tc - 15.0
    hyc = 1.0 - 0.000023 * temp_diff - 0.00000002 * temp_diff * temp_diff
    rho_obs_corrected = float(dens_obs_kgm3) * hyc
    rho15 = rho_obs_corrected
    for _ in range(17):
        K = 613.9723 / (rho15 * rho15)
        vcf = math.exp(-K * temp_diff * (1.0 + 0.8 * K * temp_diff))
        rho15 = rho_obs_corrected / vcf
    sg60 = rho15 / WAT60_CONST
    if sg60 <= 0:
        return 0.0
    api60 = 141.5 / sg60 - 131.5
    return round(api60, 2)


def vcf_from_api60_and_temp(api60: float, tank_temp: float, tank_temp_unit: str = "°F", input_mode: str = "api") -> float:
    """VCF helper shared across tabs (input_mode kept for API parity with legacy helpers)."""
    if not api60 or api60 <= 0:
        return 1.0
    tank_temp_f = _temp_to_f(tank_temp, tank_temp_unit)
    delta_t = tank_temp_f - 60.0
    if abs(delta_t) < 0.01:
        return 1.0
    sg60 = 141.5 / (api60 + 131.5)
    rho60 = sg60 * WAT60_CONST
    K0 = 341.0957
    alpha = K0 / (rho60 * rho60)
    vcf = math.exp(-alpha * delta_t * (1.0 + 0.8 * alpha * delta_t))
    return round(float(vcf), 5)

def ensure_reportlab():
    """
    Import reportlab modules lazily. Returns (ok, err, mods_dict_or_None).
    This lets the app pick up a freshly installed package without restarting Streamlit.
    """
    # Force reportlab to be detected as installed
    return True, None, {
        "canvas": importlib.import_module("reportlab.pdfgen").canvas,
        "A4": importlib.import_module("reportlab.lib.pagesizes").A4,
        "ImageReader": importlib.import_module("reportlab.lib.utils").ImageReader,
    }
def clear_2fa_session_states():
    """Clear all 2FA-related session states"""
    keys_to_clear = [
        "pending_2fa_user",
        "backup_codes_visible",
        "new_backup_codes",
        "2fa_setup",
        "2fa_backup_codes_ready",
        "show_backup_codes"
    ]
    
    for key in keys_to_clear:
        st.session_state.pop(key, None)

# ---- OTR operation helpers ---------------------------------------------------
def _normalize_operation(loc_code: str, op_label: str) -> str | None:
    """Return canonical operation label for DB, per location."""
    if not op_label:
        return None
    label = " ".join(str(op_label).strip().lower().split())
    loc = (loc_code or "").strip().upper()

    jetty_map = {
        "okw receipt": "OKW Receipt",
        "anz receipt": "ANZ Receipt",
        "other receipts": "Other Receipts",
        "dispatch to barge": "Dispatch to barge",
        "other dispatch": "Other dispatch",
    }
    bfs_map = {
        "receipt - commingled": "Receipt - Commingled",
        "receipt commingled": "Receipt - Commingled",
        # legacy aliases
        "receipt - crude": "Receipt - Commingled",
        "receipt crude": "Receipt - Commingled",
        "receipt - condensate": "Receipt - Condensate",
        "receipt condensate": "Receipt - Condensate",
        "dispatch to jetty": "Dispatch to Jetty",
    }
    ndoni_map = {
        "receipt from agu": "Receipt from Agu",
        "receipt from ofs": "Receipt from OFS",
        "other receipts": "Other Receipts",
        "dispatch to barge": "Dispatch to barge",
    }
    generic_map = {
        "receipt": "Receipt",
        "dispatch": "Dispatch",
        "closing stock": "Closing Stock",
    }

    if loc in {"JETTY", "ASEMOKU"}:
        m = jetty_map
    elif loc in {"BFS", "BENEKU"}:
        m = bfs_map
    elif loc in {"NDONI"}:
        m = ndoni_map
    else:
        m = generic_map

    if label in m:
        return m[label]
    for k, v in m.items():
        if k in label:
            return v
    return op_label.strip() or None


def _coerce_operation_for_db(op_canonical: str):
    """
    Convert canonical human label (e.g., 'Opening Stock') to models.Operation enum.
    Returning the Enum instance makes SAEnum persist the enum NAME (OPENING_STOCK),
    which is what your column expects.
    """
    from models import Operation  # this Enum is defined in models.py

    if not op_canonical:
        return None

    # 1) try by value (labels like "Opening Stock")
    try:
        return Operation(op_canonical)
    except Exception:
        pass

    # 2) try by name (e.g., "OPENING_STOCK")
    key = (
        op_canonical.upper()
        .replace("-", "_")
        .replace(" ", "_")
    )
    try:
        return Operation[key]
    except Exception:
        return None  # force the caller to handle unsupported ops

def _is_condensate_tx(tx) -> bool:
    if tx is None:
        return False
    op_obj = getattr(tx, "operation", None)
    if hasattr(op_obj, "value"):
        label = str(op_obj.value or "")
    else:
        label = str(op_obj or "")
    tank_name = str(getattr(tx, "tank_name", "") or "")
    has_meter = (
        getattr(tx, "opening_meter_reading", None) is not None
        and getattr(tx, "closing_meter_reading", None) is not None
    )
    return (
        "condensate" in label.lower()
        or "condensate" in tank_name.lower()
        or has_meter
    )

def load_condensate_transactions(location_id: int | None, limit: int = 1000):
    """Return (records, meta_map) for condensate tank transactions (most recent first)."""
    if not location_id:
        return [], {}
    from models import TankTransaction, Table11

    with get_session() as s:
        rows = (
            s.query(TankTransaction)
            .filter(TankTransaction.location_id == int(location_id))
            .order_by(TankTransaction.date.desc(), TankTransaction.time.desc())
            .limit(limit)
            .all()
        )

    try:
        with get_session() as s_lt:
            lt_rows = s_lt.query(Table11).order_by(Table11.api60).all()
    except Exception:
        lt_rows = []
    lt_xs = [float(getattr(r, "api60", 0.0) or 0.0) for r in lt_rows]
    lt_ys = [float(getattr(r, "lt_factor", 0.0) or 0.0) for r in lt_rows]

    def _lt_lookup(api60: float) -> float:
        if not lt_xs or api60 <= 0:
            return 0.0
        if api60 <= lt_xs[0]:
            return lt_ys[0]
        if api60 >= lt_xs[-1]:
            return lt_ys[-1]
        import bisect
        idx = bisect.bisect_left(lt_xs, api60)
        x1, y1 = lt_xs[idx - 1], lt_ys[idx - 1]
        x2, y2 = lt_xs[idx], lt_ys[idx]
        if x2 == x1:
            return y1
        frac = (api60 - x1) / (x2 - x1)
        return y1 + frac * (y2 - y1)

    records = []
    meta_map: dict[str, tuple[str | None, str | None, datetime | None]] = {}
    for row in rows:
        if not _is_condensate_tx(row):
            continue

        opening_val = float(row.opening_meter_reading or 0.0)
        closing_val = float(row.closing_meter_reading or opening_val)
        qty_m3 = (
            float(row.condensate_qty_m3)
            if row.condensate_qty_m3 is not None
            else max(closing_val - opening_val, 0.0)
        )
        gov_bbl = float(row.qty_bbls or (qty_m3 * CONDENSATE_M3_TO_BBL))

        api_obs = float(getattr(row, "api_observed", 0.0) or 0.0)
        dens_obs = float(getattr(row, "density_observed", 0.0) or 0.0)
        sample_temp_c_val = float(getattr(row, "sample_temp_c", 0.0) or 0.0)
        sample_temp_f_val = float(getattr(row, "sample_temp_f", 0.0) or 0.0)
        tank_temp_c_val = float(getattr(row, "tank_temp_c", 0.0) or 0.0)
        tank_temp_f_val = float(getattr(row, "tank_temp_f", 0.0) or 0.0)

        if not sample_temp_f_val and sample_temp_c_val:
            sample_temp_f_val = (sample_temp_c_val * 1.8) + 32.0
        if not sample_temp_c_val and sample_temp_f_val:
            sample_temp_c_val = (sample_temp_f_val - 32.0) / 1.8

        if api_obs > 0:
            sample_unit = "°F" if sample_temp_f_val else "°C"
            sample_temp_for_api = sample_temp_f_val if sample_unit == "°F" else sample_temp_c_val
            api60_val = convert_api_to_60_from_api(api_obs, sample_temp_for_api or 0.0, sample_unit)
        elif dens_obs > 0:
            sample_temp_for_density = sample_temp_c_val or ((sample_temp_f_val - 32.0) / 1.8 if sample_temp_f_val else 15.0)
            api60_val = convert_api_to_60_from_density(dens_obs, sample_temp_for_density or 0.0, "°C")
        else:
            api60_val = 0.0

        tank_temp_c_for_vcf = tank_temp_c_val or ((tank_temp_f_val - 32.0) / 1.8 if tank_temp_f_val else 0.0)
        input_mode = "api" if api_obs > 0 else ("density" if dens_obs > 0 else "api")
        vcf_val = vcf_from_api60_and_temp(api60_val, tank_temp_c_for_vcf, "°C", input_mode)
        gsv_val = round(gov_bbl * vcf_val, 2)
        lt_factor = _lt_lookup(api60_val)
        lt_val = round(gsv_val * lt_factor, 2)
        mt_val = round(lt_val * 1.01605, 2)

        records.append({
            "Ticket ID": row.ticket_id,
            "Date": row.date,
            "Opening (m3)": opening_val,
            "Closing (m3)": closing_val,
            "Net Receipt (m3)": qty_m3,
            "GOV (bbls)": gov_bbl,
            "API @ 60": api60_val,
            "VCF": vcf_val,
            "GSV (bbls)": gsv_val,
            "LT": lt_val,
            "MT": mt_val,
            "Created By": getattr(row, "created_by", None),
            "Updated By": getattr(row, "updated_by", None),
            "Updated At": getattr(row, "updated_at", None),
        })
        meta_map[row.ticket_id] = (
            getattr(row, "created_by", None),
            getattr(row, "updated_by", None),
            getattr(row, "updated_at", None),
        )
    return records, meta_map


def _ensure_okw_column_exists():
    """Add okw_production column to gpp_production_records if missing (idempotent)."""
    if st.session_state.get("_okw_column_checked"):
        return
    try:
        with engine.begin() as conn:
            cols = {row[1] for row in conn.execute(text("PRAGMA table_info('gpp_production_records')"))}
            if "okw_production" not in cols:
                conn.execute(
                    text(
                        "ALTER TABLE gpp_production_records "
                        "ADD COLUMN okw_production REAL NOT NULL DEFAULT 0.0"
                    )
                )
    except Exception as exc:
        st.warning(f"Could not extend gpp_production_records: {exc}")
    finally:
        st.session_state["_okw_column_checked"] = True


def _ensure_gpp_closing_column_exists():
    if st.session_state.get("_gpp_closing_column_checked"):
        return
    try:
        with engine.begin() as conn:
            cols = {row[1] for row in conn.execute(text("PRAGMA table_info('gpp_production_records')"))}
            if "gpp_closing_stock" not in cols:
                conn.execute(
                    text(
                        "ALTER TABLE gpp_production_records "
                        "ADD COLUMN gpp_closing_stock REAL NOT NULL DEFAULT 0.0"
                    )
                )
    except Exception as exc:
        st.warning(f"Could not extend gpp_production_records (closing): {exc}")
    finally:
        st.session_state["_gpp_closing_column_checked"] = True


def load_gpp_production_records(location_id: int | None, limit: int = 1000) -> list[dict[str, Any]]:
    """Fetch latest GPP production entries for a location."""
    _ensure_okw_column_exists()
    _ensure_gpp_closing_column_exists()
    if not location_id:
        return []

    with get_session() as s:
        rows = (
            s.query(GPPProductionRecord)
            .filter(GPPProductionRecord.location_id == int(location_id))
            .order_by(GPPProductionRecord.date.desc(), GPPProductionRecord.id.desc())
            .limit(limit)
            .all()
        )

    records: list[dict[str, Any]] = []
    import re
    for row in rows:
        # ? Just use the dedicated GPP closing stock field; don't touch remarks.
        try:
            closing_stock_val = float(getattr(row, "gpp_closing_stock", 0.0) or 0.0)
        except Exception:
            closing_stock_val = 0.0

        # ? Remarks are whatever you typed. If empty, they remain empty.
        remarks_str = row.remarks or ""

        records.append(
            {
                "id": row.id,
                "Date": row.date,
                "OKW Production": round(float(row.okw_production or 0.0), 2),
                "GPP1 Production": round(float(row.gpp1_production or 0.0), 2),
                "GPP2 Production": round(float(row.gpp2_production or 0.0), 2),
                "GPP Closing Stock": round(float(closing_stock_val or 0.0), 2),
                "Total GPP Production": round(float(row.total_production or 0.0), 2),
                "Remarks": remarks_str or "",
                "Created By": row.created_by or "-",
                "Updated By": row.updated_by or "",
                "Updated At": row.updated_at,
            }
        )
    return records

def load_river_draft_records(location_id: int | None, limit: int = 1000) -> list[dict[str, Any]]:
    """Load recent river draft entries for convenience functions."""
    if not location_id:
        return []

    with get_session() as s:
        rows = (
            s.query(RiverDraftRecord)
            .filter(RiverDraftRecord.location_id == int(location_id))
            .order_by(RiverDraftRecord.date.desc(), RiverDraftRecord.id.desc())
            .limit(limit)
            .all()
        )

    records: list[dict[str, Any]] = []
    for row in rows:
        records.append(
            {
                "id": row.id,
                "Date": row.date,
                "River Draft (m)": round(float(row.river_draft_m or 0.0), 2),
                "Rainfall (cm)": round(float(row.rainfall_cm or 0.0), 2),
                "Created By": row.created_by or "-",
                "Updated By": row.updated_by or "",
                "Updated At": row.updated_at,
            }
        )
    return records


def load_produced_water_records(location_id: int | None, limit: int = 1000) -> list[dict[str, Any]]:
    """Load recent produced water entries for convenience functions."""
    if not location_id:
        return []

    with get_session() as s:
        rows = (
            s.query(ProducedWaterRecord)
            .filter(ProducedWaterRecord.location_id == int(location_id))
            .order_by(ProducedWaterRecord.date.desc(), ProducedWaterRecord.id.desc())
            .limit(limit)
            .all()
        )

    records: list[dict[str, Any]] = []
    for row in rows:
        records.append(
            {
                "id": row.id,
                "Date": row.date,
                "Produced Water (bbls)": round(float(row.produced_water_bbl or 0.0), 2),
                "Created By": row.created_by or "-",
                "Updated By": row.updated_by or "",
                "Updated At": row.updated_at,
            }
        )
    return records

def _fix_legacy_operation_enums():
    """
    One-time data repair:
    Convert legacy human-readable strings in tank_transactions.operation
    to the enum NAMES expected by SAEnum(Operation).
    Safe to run multiple times (idempotent).
    """
    from sqlalchemy import text
    from models import Operation
    from db import get_session

    label_to_name = {m.value: m.name for m in Operation}  # "Opening Stock" -> "OPENING_STOCK"
    if not label_to_name:
        return

    with get_session() as s:
        # use raw SQL so we don't trip over enum hydration
        rows = s.execute(text("SELECT DISTINCT operation FROM tank_transactions")).fetchall()
        for (op_val,) in rows:
            if op_val in label_to_name:  # legacy label stored
                s.execute(
                    text("UPDATE tank_transactions SET operation = :new WHERE operation = :old"),
                    {"new": label_to_name[op_val], "old": op_val},
                )
        s.commit()


def _ensure_ops_enum_fixed_once():
    """Run the legacy enum repair only once per Streamlit session."""
    if st.session_state.get("_ops_enum_fixed"):
        return
    try:
        _fix_legacy_operation_enums()
        st.session_state["_ops_enum_fixed"] = True
    except Exception:
        # stay non-blocking � if there is no table yet, etc.
        pass


_ensure_ops_enum_fixed_once()
def _get_selected_operation_from_state() -> str | None:
    """Try common widget keys so we don�t depend on one exact name."""
    for k in ("otr_form_operation", "otr_operation", "operation", "otr_op"):
        v = st.session_state.get(k)
        if v not in (None, "", "(Select)", "(All)"):
            return v
    return None

def user_with_caution(created_by: str | None, updated_by: str | None, updated_at) -> str:
    """
    Returns a small HTML badge:
      - shows creator
      - if edited, shows ✏️ with hover tooltip "Edited by X on YYYY-MM-DD HH:MM"
    """
    creator = (created_by or "").strip()
    if updated_by and updated_at:
        try:
            ts = pd.to_datetime(updated_at).strftime("%Y-%m-%d %H:%M")
        except Exception:
            ts = str(updated_at)
        tip = f"Edited by {updated_by} on {ts}"
        return f"<span title='{tip}'>✏️ {creator}</span>"
    return creator or "-"


# -----------------------------------------------------------------------------

import os
import re
# ---------------- Reporting Page (Asemoku Jetty ? BENEKU DISPATCH VS JETTY RECEIPT) ----------------
import json
from pathlib import Path
from datetime import date, timedelta
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib import colors

REPORT_NOTES_PATH = OUTPUT / "reporting_notes.json"
REPORT_NOTES_PATH.parent.mkdir(exist_ok=True)
if not REPORT_NOTES_PATH.exists():
    REPORT_NOTES_PATH.write_text("{}", encoding="utf-8")

def _load_reporting_notes():
    try:
        return json.loads(REPORT_NOTES_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_reporting_notes(notes: dict):
    REPORT_NOTES_PATH.write_text(json.dumps(notes, indent=2), encoding="utf-8")

def _date_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def _generate_bfs_vs_jetty_pdf(df: pd.DataFrame, loc_name: str, loc_code: str, dfrom: date, dto: date) -> bytes:
    bio = BytesIO()
    doc = SimpleDocTemplate(bio, pagesize=landscape(A4), leftMargin=0.5*cm, rightMargin=0.5*cm, topMargin=0.6*cm, bottomMargin=0.6*cm)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("T", parent=styles["Heading1"], fontSize=15, alignment=TA_CENTER, textColor=colors.HexColor("#1f4788"))
    sub_style   = ParagraphStyle("S", parent=styles["Normal"],   fontSize=10, alignment=TA_CENTER, textColor=colors.HexColor("#666666"))

    elements = []
    elements.append(Paragraph("<b>BENEKU DISPATCH VS JETTY RECEIPT</b>", title_style))
    elements.append(Paragraph(f"{loc_name} ({loc_code}) &nbsp;�&nbsp; Period: <b>{_date_str(dfrom)}</b> to <b>{_date_str(dto)}</b>", sub_style))
    elements.append(Spacer(1, 0.3*cm))

    # Table
    headers = ["Date","BFS Dispatch","Jetty Receipt","Water Received","Loss/Gain","Loss/Gain %","Remarks"]
    data = [headers]
    for _, r in df.iterrows():
        data.append([
            r["Date"],
            f"{r['BFS Dispatch']:,.2f}",
            f"{r['Jetty Receipt']:,.2f}",
            f"{r['Water Received']:,.2f}",
            f"{r['Loss/Gain']:,.2f}",
            f"{r['Loss/Gain %']:,.4f}",
            (r.get("Remarks") or "")[:70]
        ])

    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#1f4788")),
        ("TEXTCOLOR",(0,0),(-1,0), colors.white),
        ("ALIGN",(0,0),(-1,-1), "CENTER"),
        ("FONTNAME",(0,0),(-1,0), "Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,0), 8),
        ("GRID",(0,0),(-1,-1), 0.25, colors.grey),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.whitesmoke, colors.HexColor("#f8f9fa")]),
        ("FONTSIZE",(0,1),(-1,-1), 8)
    ]))
    elements.append(table)
    doc.build(elements)
    return bio.getvalue()


def _generate_production_pdf(df: pd.DataFrame, loc_name: str, loc_code: str, dfrom: date, dto: date) -> bytes:
    bio = BytesIO()
    doc = SimpleDocTemplate(
        bio,
        pagesize=landscape(A4),
        leftMargin=0.5 * cm,
        rightMargin=0.5 * cm,
        topMargin=0.6 * cm,
        bottomMargin=0.6 * cm,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "GPPTitle",
        parent=styles["Heading1"],
        fontSize=15,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#1f4788"),
    )
    sub_style = ParagraphStyle(
        "GPPSub",
        parent=styles["Normal"],
        fontSize=10,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#666666"),
    )

    df_sorted = df.sort_values(by="Date")
    headers = [
        "Date",
        "OKW Production",
        "GPP1 Production",
        "GPP2 Production",
        "GPP Closing Stock",
        "Total GPP Production",
        "Remarks",
    ]
    data = [headers]
    for _, row in df_sorted.iterrows():
        date_val = row["Date"]
        if isinstance(date_val, pd.Timestamp):
            date_val = date_val.date()
        data.append(
            [
                str(date_val),
                f"{row['OKW Production']:,.2f}",
                f"{row['GPP1 Production']:,.2f}",
                f"{row['GPP2 Production']:,.2f}",
                f"{row.get('GPP Closing Stock', 0.0):,.2f}",
                f"{row['Total GPP Production']:,.2f}",
                (row.get('Remarks') or '')[:80],
            ]
        )

    totals = df_sorted[
        [
            "OKW Production",
            "GPP1 Production",
            "GPP2 Production",
            "Total GPP Production",
        ]
    ].sum(numeric_only=True)
    data.append(
        [
            "TOTAL",
            f"{totals.get('OKW Production', 0.0):,.2f}",
            f"{totals.get('GPP1 Production', 0.0):,.2f}",
            f"{totals.get('GPP2 Production', 0.0):,.2f}",
            "",
            f"{totals.get('Total GPP Production', 0.0):,.2f}",
            "",
        ]
    )

    # Adjust column widths for the additional 'GPP Closing Stock' column
    table = Table(
        data,
        repeatRows=1,
        colWidths=[
            4.0 * cm,
            3.5 * cm,
            3.5 * cm,
            3.5 * cm,
            3.5 * cm,
            4.0 * cm,
            7.0 * cm,
        ],
    )
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4788")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("ALIGN", (1, 1), (-2, -2), "RIGHT"),
                ("BACKGROUND", (0, 1), (-1, -1), colors.whitesmoke),
                ("ROWBACKGROUNDS", (0, 1), (-1, -2), [colors.whitesmoke, colors.HexColor("#f8f9fa")]),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#dfe6f1")),
                ("TEXTCOLOR", (0, -1), (-1, -1), colors.HexColor("#1f4788")),
            ]
        )
    )

    elements = [
        Paragraph("<b>PRODUCTION SUMMARY</b>", title_style),
        Paragraph(
            f"{loc_name} ({loc_code}) &nbsp;�&nbsp; Period: <b>{_date_str(dfrom)}</b> to <b>{_date_str(dto)}</b>",
            sub_style,
        ),
        Paragraph(
            f"Totals � OKW: <b>{totals.get('OKW Production', 0.0):,.2f}</b> bbls | GPP: <b>{totals.get('Total GPP Production', 0.0):,.2f}</b> bbls",
            sub_style,
        ),
        Spacer(1, 0.3 * cm),
        table,
    ]

    doc.build(elements)
    return bio.getvalue()

# --------------------------------------------------------------------
# PDF generator for tanker details (Aggu Dispatched & Ndoni Receipt)
def _generate_tanker_details_pdf(
    df: pd.DataFrame,
    title: str,
    dfrom: date,
    dto: date,
    summary_row: Optional[list[Any]] = None,
    summary_pairs: Optional[list[tuple[str, Any]]] = None,
) -> bytes:
    """
    Build a simple PDF summarizing tanker details for Aggu or Ndoni.

    The DataFrame columns should already be ordered as desired for the report.
    The title parameter will be used as the main heading (e.g. "Aggu Dispatched" or "Ndoni Receipt").
    dfrom and dto are used for the date range subtitle.
    """
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib import colors
    from reportlab.lib.units import cm
    # Create buffer for PDF data
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=0.5 * cm,
        rightMargin=0.5 * cm,
        topMargin=0.6 * cm,
        bottomMargin=0.6 * cm,
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "TankerTitle",
        parent=styles["Heading1"],
        fontSize=15,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#1f4788"),
    )
    sub_style = ParagraphStyle(
        "TankerSub",
        parent=styles["Normal"],
        fontSize=10,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#666666"),
    )
    elements: list[Any] = []
    # Title
    elements.append(Paragraph(f"<b>{html.escape(title)}</b>", title_style))
    try:
        df_str = _date_str(dfrom) if dfrom else ""
        dt_str = _date_str(dto) if dto else ""
        subtitle = f"Period: <b>{df_str}</b> to <b>{dt_str}</b>"
    except Exception:
        subtitle = f"Period: {dfrom} to {dto}"
    elements.append(Paragraph(subtitle, sub_style))
    elements.append(Spacer(1, 0.3 * cm))
    # Build table data from DataFrame
    header_count = 0
    if df.empty:
        table_data = [["No records found"]]
        col_widths = [doc.width]
    else:
        headers = list(df.columns)
        header_count = len(headers)
        table_data = [headers]
        for _, r in df.iterrows():
            row = []
            for c in headers:
                v = r[c]
                if isinstance(v, float):
                    if float(v).is_integer():
                        row.append(f"{int(v):,}")
                    else:
                        row.append(f"{v:,.2f}")
                else:
                    if isinstance(v, (datetime, date)):
                        try:
                            row.append(v.strftime("%d-%b-%Y"))
                        except Exception:
                            row.append(str(v))
                    else:
                        row.append(str(v))
            table_data.append(row)
        # Determine column widths: small for serial/date, large for remarks, medium for others
        weights = []
        for h in headers:
            h_lower = h.lower()
            if "s.no" in h_lower or "serial" in h_lower:
                weights.append(0.6)
            elif "date" in h_lower:
                weights.append(1.3)
            elif "remark" in h_lower:
                weights.append(3.0)
            else:
                weights.append(1.3)
        if summary_row:
            padded_row = list(summary_row)
            if len(padded_row) < header_count:
                padded_row.extend([""] * (header_count - len(padded_row)))
            table_data.append(padded_row[:header_count])
        available_width = doc.width
        scale = available_width / sum(weights)
        col_widths = [w * scale for w in weights]
    tbl = Table(table_data, colWidths=col_widths, repeatRows=1)
    tbl_style_cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4788")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("ALIGN", (0, 1), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor("#f8f9fa")]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
    ]
    if summary_row and header_count:
        tbl_style_cmds.append(
            ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#dfe6f1"))
        )
        tbl_style_cmds.append(
            ("TEXTCOLOR", (0, -1), (-1, -1), colors.HexColor("#1f4788"))
        )
        tbl_style_cmds.append(
            ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold")
        )
    tbl.setStyle(TableStyle(tbl_style_cmds))
    elements.append(tbl)
    if summary_pairs:
        elements.append(Spacer(1, 0.3 * cm))
        grid_items = summary_pairs[:]
        target_cells = 10  # 2 rows x 5 columns
        while len(grid_items) < target_cells:
            grid_items.append(("", ""))
        cell_paragraphs: list[list[Any]] = []
        cols = 5
        col_width = doc.width / cols
        for row_idx in range(2):
            row_cells: list[Any] = []
            for col_idx in range(cols):
                name, val = grid_items[row_idx * cols + col_idx]
                if name:
                    if isinstance(val, (int, float)):
                        val_str = f"{float(val):,.2f}"
                    else:
                        val_str = str(val)
                    cell_html = (
                        f"<font size=9 color='#0f172a'>{html.escape(str(name))}:</font><br/>"
                        f"<font size=10 color='#0f172a'><b>{html.escape(val_str)}</b></font>"
                    )
                else:
                    cell_html = ""
                row_cells.append(Paragraph(cell_html, styles["Normal"]))
            cell_paragraphs.append(row_cells)
        summary_table = Table(cell_paragraphs, colWidths=[col_width] * cols)
        summary_table.setStyle(
            TableStyle(
                [
                    ("BOX", (0, 0), (-1, -1), 1.2, colors.HexColor("#1f4788")),
                    ("INNERGRID", (0, 0), (-1, -1), 0, colors.white),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ]
            )
        )
        elements.append(Spacer(1, 0.3 * cm))
        elements.append(Paragraph("<b>Summary Totals</b>", styles["Heading4"]))
        elements.append(summary_table)
    doc.build(elements)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes

def render_tanker_details_tab() -> None:
    """
    Render the 'Tankers Details' tab on the Reporting page.

    This tab displays two tables: one for Aggu dispatches and another for Ndoni receipts.
    Each table provides date range filters and download/export options (CSV, XLSX, PDF)
    along with a 'View PDF' button that opens the PDF in a new browser tab.
    """
    import pandas as pd  # local import to avoid polluting global namespace
    import streamlit as st
    import base64
    from io import BytesIO
    from datetime import date, timedelta
    from sqlalchemy import or_
    from models import Location, LocationTankerEntry
    from db import get_session
    # Fetch Aggu and Ndoni locations
    with get_session() as _sess:
        aggu_loc = (
            _sess.query(Location)
            .filter(or_(Location.code.ilike("%AGGU%"), Location.name.ilike("%AGGU%")))
            .first()
        )
        ndoni_loc = (
            _sess.query(Location)
            .filter(or_(Location.code.ilike("%NDONI%"), Location.name.ilike("%NDONI%")))
            .first()
        )

    col_aggu, col_ndoni = st.columns(2)

    # Render Aggu table
    with col_aggu:
        if aggu_loc:
            st.markdown("#### Aggu Dispatched")
            aggu_cols = st.columns(2)
            today = date.today()
            with aggu_cols[0]:
                aggu_from = st.date_input(
                    "From (Aggu)",
                    value=today - timedelta(days=14),
                    key="tanker_rpt_aggu_from",
                )
            with aggu_cols[1]:
                aggu_to = st.date_input(
                    "To (Aggu)",
                    value=today,
                    key="tanker_rpt_aggu_to",
                )
            with get_session() as _sess:
                aggu_rows = (
                    _sess.query(LocationTankerEntry)
                    .filter(LocationTankerEntry.location_id == aggu_loc.id)
                    .order_by(LocationTankerEntry.date.asc(), LocationTankerEntry.serial_no.asc())
                    .all()
                )
            aggu_filtered = []
            for rec in aggu_rows:
                if aggu_from and rec.date < aggu_from:
                    continue
                if aggu_to and rec.date > aggu_to:
                    continue
                aggu_filtered.append(rec)
            aggu_data = []
            for rec in aggu_filtered:
                aggu_data.append(
                    {
                        "S.No": rec.serial_no,
                        "Date": rec.date.strftime("%d-%b-%Y") if rec.date else "",
                        "Tankers Dispatched": float(rec.tankers_dispatched or 0.0),
                        "Remarks": rec.remarks or "",
                    }
                )
            aggu_df = pd.DataFrame(aggu_data)
            st.dataframe(aggu_df, use_container_width=True, hide_index=True)
            # Export buttons
            st.markdown("---")
            col_csv, col_xlsx, col_pdf, col_view = st.columns(4)
            # CSV
            aggu_csv = aggu_df.to_csv(index=False).encode("utf-8")
            col_csv.download_button(
                "📥 CSV",
                data=aggu_csv,
                file_name=f"aggu_tankers_{aggu_from}_{aggu_to}.csv",
                mime="text/csv",
                use_container_width=True,
            )
            # XLSX
            aggu_xbio = BytesIO()
            with pd.ExcelWriter(aggu_xbio, engine="xlsxwriter") as writer:
                aggu_df.to_excel(writer, index=False, sheet_name="Aggu")
            col_xlsx.download_button(
                "📥 XLSX",
                data=aggu_xbio.getvalue(),
                file_name=f"aggu_tankers_{aggu_from}_{aggu_to}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
            # PDF
            aggu_pdf = b""
            if not aggu_df.empty:
                try:
                    aggu_pdf = _generate_tanker_details_pdf(
                        aggu_df, "Aggu Dispatched", aggu_from, aggu_to
                    )
                except Exception:
                    aggu_pdf = b""
            col_pdf.download_button(
                "📥 PDF",
                data=aggu_pdf,
                file_name=f"aggu_tankers_{aggu_from}_{aggu_to}.pdf",
                mime="application/pdf",
                use_container_width=True,
                disabled=(len(aggu_data) == 0),
            )
            # View PDF
            if col_view.button(
                "👁️ View PDF",
                key="aggu_pdf_view_btn",
                use_container_width=True,
                disabled=(len(aggu_data) == 0),
            ):
                if aggu_pdf:
                    b64 = base64.b64encode(aggu_pdf).decode("utf-8")
                    import streamlit.components.v1 as components

                    components.html(
                        f"""
                                <script>
                                    const pdfData = "{b64}";
                                    const byteCharacters = atob(pdfData);
                                    const byteNumbers = new Array(byteCharacters.length);
                                    for (let i = 0; i < byteCharacters.length; i++) {{
                                        byteNumbers[i] = byteCharacters.charCodeAt(i);
                                    }}
                                    const byteArray = new Uint8Array(byteNumbers);
                                    const file = new Blob([byteArray], {{ type: "application/pdf" }});
                                    const fileURL = URL.createObjectURL(file);
                                    window.open(fileURL, "_blank");
                                </script>
                            """,
                        height=0,
                    )
        else:
            st.info("Aggu location not configured.")

    # Render Ndoni table
    with col_ndoni:
        if ndoni_loc:
            st.markdown("#### Ndoni Receipt")
            nd_cols = st.columns(2)
            today = date.today()
            with nd_cols[0]:
                nd_from = st.date_input(
                    "From (Ndoni)",
                    value=today - timedelta(days=14),
                    key="tanker_rpt_ndoni_from",
                )
            with nd_cols[1]:
                nd_to = st.date_input(
                    "To (Ndoni)",
                    value=today,
                    key="tanker_rpt_ndoni_to",
                )
            with get_session() as _sess:
                nd_rows = (
                    _sess.query(LocationTankerEntry)
                    .filter(LocationTankerEntry.location_id == ndoni_loc.id)
                    .order_by(LocationTankerEntry.date.asc(), LocationTankerEntry.serial_no.asc())
                    .all()
                )
            nd_filtered: list[Any] = []
            for rec in nd_rows:
                if nd_from and rec.date < nd_from:
                    continue
                if nd_to and rec.date > nd_to:
                    continue
                nd_filtered.append(rec)
            nd_data = []
            for rec in nd_filtered:
                nd_data.append(
                    {
                        "S.No": rec.serial_no,
                        "Date": rec.date.strftime("%d-%b-%Y") if rec.date else "",
                        "Tankers from Aggu": float(rec.tankers_from_aggu or 0.0),
                        "Tankers from OFS": float(rec.tankers_from_ofs or 0.0),
                        "Other Tankers": float(rec.other_tankers or 0.0),
                        "Remarks": rec.remarks or "",
                    }
                )
            nd_df = pd.DataFrame(nd_data)
            st.dataframe(nd_df, use_container_width=True, hide_index=True)
            st.markdown("---")
            d_csv, d_xlsx, d_pdf, d_view = st.columns(4)
            # Ndoni CSV
            nd_csv = nd_df.to_csv(index=False).encode("utf-8")
            d_csv.download_button(
                "📥 CSV",
                data=nd_csv,
                file_name=f"ndoni_tankers_{nd_from}_{nd_to}.csv",
                mime="text/csv",
                use_container_width=True,
            )
            # Ndoni XLSX
            nd_xbio = BytesIO()
            with pd.ExcelWriter(nd_xbio, engine="xlsxwriter") as writer:
                nd_df.to_excel(writer, index=False, sheet_name="Ndoni")
            d_xlsx.download_button(
                "📥 XLSX",
                data=nd_xbio.getvalue(),
                file_name=f"ndoni_tankers_{nd_from}_{nd_to}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
            nd_pdf = b""
            if not nd_df.empty:
                try:
                    nd_pdf = _generate_tanker_details_pdf(
                        nd_df, "Ndoni Receipt", nd_from, nd_to
                    )
                except Exception:
                    nd_pdf = b""
            d_pdf.download_button(
                "📥 PDF",
                data=nd_pdf,
                file_name=f"ndoni_tankers_{nd_from}_{nd_to}.pdf",
                mime="application/pdf",
                use_container_width=True,
                disabled=(len(nd_data) == 0),
            )
            if d_view.button(
                "👁️ View PDF",
                key="ndoni_pdf_view_btn",
                use_container_width=True,
                disabled=(len(nd_data) == 0),
            ):
                if nd_pdf:
                    b64 = base64.b64encode(nd_pdf).decode("utf-8")
                    import streamlit.components.v1 as components

                    components.html(
                        f"""
                                <script>
                                    const pdfData = "{b64}";
                                    const byteCharacters = atob(pdfData);
                                    const byteNumbers = new Array(byteCharacters.length);
                                    for (let i = 0; i < byteCharacters.length; i++) {{
                                        byteNumbers[i] = byteCharacters.charCodeAt(i);
                                    }}
                                    const byteArray = new Uint8Array(byteNumbers);
                                    const file = new Blob([byteArray], {{ type: "application/pdf" }});
                                    const fileURL = URL.createObjectURL(file);
                                    window.open(fileURL, "_blank");
                                </script>
                            """,
                        height=0,
                    )
        else:
            st.info("Ndoni location not configured.")

def render_reports_page():
    """
    Reporting page � Location-aware.

    Tab 1: BENEKU DISPATCH VS JETTY RECEIPT (Asemoku Jetty)
      - Live filters (date + remark search)
      - Inline editable Remarks (double-click)
      - CSV/XLSX/PDF exports + View PDF in new tab
      - Portrait PDF, 0.5 cm margins, full-width table, dynamic TOTAL row

    Tab 2: JETTY METER READING (Asemoku Jetty)
      - Live date filter
      - Columns: Date, Opening (M1), Closing (M1), Opening (M2), Closing (M2), Net Receipt/Dispatch, Remarks
      - CSV/XLSX/PDF + View PDF in new tab
      - PDF: A4 portrait, 0.5 cm margins, same colours as other reports
    """
    import json, base64
    from io import BytesIO
    from datetime import date, timedelta, datetime, time
    from pathlib import Path

    import pandas as pd
    import streamlit as st
    import streamlit.components.v1 as components

    from db import get_session
    from models import Location
    from material_balance_calculator import MaterialBalanceCalculator as MBC

    # PDF libs (kept inside the function)
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4  # portrait
    from reportlab.lib.units import cm

    st.title("Reporting")

    # -------- Ensure active location / show context --------
    active_location_id = st.session_state.get("active_location_id")
    if not active_location_id:
        st.error("⚠️ No active location selected. Please select a location first.")
        st.stop()

    with get_session() as s:
        loc = s.query(Location).filter(Location.id == active_location_id).first()
    if not loc:
        st.error("? Location not found.")
        st.stop()

    st.caption(f"📍 **Active Location:** {loc.name} ({loc.code})")

    try:
        from sqlalchemy import or_
        from models import ReportDefinition
        with get_session() as s:
            custom_reports = (
                s.query(ReportDefinition)
                .filter(
                    ReportDefinition.is_active == True,
                    or_(ReportDefinition.location_id == active_location_id, ReportDefinition.location_id.is_(None)),
                )
                .order_by(ReportDefinition.name)
                .all()
            )
        if custom_reports:
            st.markdown("### Custom Reports")
            cr_tabs = st.tabs([r.name for r in custom_reports])
            for i, r in enumerate(custom_reports):
                with cr_tabs[i]:
                    try:
                        cfg = json.loads(r.config_json or "{}")
                    except Exception:
                        cfg = {}
                    mode = str(cfg.get("mode") or "").strip()
                    src = str(cfg.get("primary_source") or "").strip()
                    columns = cfg.get("columns") or []
                    filter_cols = st.columns(2)
                    d_from = filter_cols[0].date_input("From date", value=date.today() - timedelta(days=30), key=f"cr_from_{r.id}")
                    d_to = filter_cols[1].date_input("To date", value=date.today(), key=f"cr_to_{r.id}")
                    loc_id_for_src = cfg.get("source_location_id") or active_location_id
                    # Date-Merge mode: build per-date columns from multiple sources/locations
                    if mode == "date_merge":
                        mappings = cfg.get("mappings") or []
                        cur = d_from
                        date_list = []
                        while cur <= d_to:
                            date_list.append(cur)
                            cur = cur + timedelta(days=1)
                        df = pd.DataFrame({"Date": [d.strftime("%Y-%m-%d") for d in date_list]})
                        def _load_rows_for_mapping(s2, mapping):
                            src_name = mapping.get("source")
                            loc_id = mapping.get("location_id") or active_location_id
                            rows, resolved_key = _load_rows_for_any_source(src_name, s2, loc_id, d_from, d_to)
                            op_filter = (mapping.get("operation_filter") or "").strip()
                            if op_filter:
                                rows = [row for row in rows if _matches_operation_filter(row, op_filter)]
                            return rows, resolved_key

                        with get_session() as s2:
                            for m in mappings:
                                label = m.get("label")
                                field = m.get("field")
                                dt_field = m.get("date_field") or "date"
                                agg = (m.get("aggregate") or "sum").lower()
                                rows, resolved_key = _load_rows_for_mapping(s2, m)
                                meta = _get_source_meta(resolved_key) if resolved_key else None
                                time_field = meta.get("time_field") if meta else None
                                grouped = defaultdict(list)
                                for r_obj in rows:
                                    d_val = _pluck_value(r_obj, dt_field)
                                    base_date = None
                                    t_val = None
                                    if isinstance(d_val, datetime):
                                        base_date = d_val.date()
                                        t_val = d_val.time()
                                    else:
                                        base_date = d_val
                                        if time_field:
                                            t_val = _pluck_value(r_obj, time_field)
                                        else:
                                            t_val = _pluck_value(r_obj, "time") or _pluck_value(r_obj, "transaction_time")
                                    report_date = _derive_report_date(base_date, t_val)
                                    if report_date is None or report_date < d_from or report_date > d_to:
                                        continue
                                    grouped[report_date].append(r_obj)
                                values = []
                                for d_cur in date_list:
                                    bucket_rows = grouped.get(d_cur, [])
                                    if not bucket_rows:
                                        values.append(None)
                                        continue
                                    agg_val = None
                                    if agg == "sum":
                                        total = 0.0
                                        for r_obj in bucket_rows:
                                            v_raw = _pluck_value(r_obj, field)
                                            try:
                                                total += float(v_raw or 0.0)
                                            except Exception:
                                                try:
                                                    total += float(str(v_raw)) if v_raw is not None else 0.0
                                                except Exception:
                                                    pass
                                        agg_val = total
                                    elif agg == "last":
                                        agg_val = _pluck_value(bucket_rows[-1], field)
                                    elif agg == "first":
                                        agg_val = _pluck_value(bucket_rows[0], field)
                                    else:
                                        agg_val = _pluck_value(bucket_rows[-1], field)
                                    values.append(agg_val)
                                df[label] = values
                        calcs = cfg.get("calculations") or []
                        if calcs:
                            for c in calcs:
                                lbl = str(c.get("label") or "").strip()
                                expr = str(c.get("expression") or "").strip()
                                if lbl and expr:
                                    try:
                                        normalized_expr = _normalize_calc_expression(expr, df.columns.tolist())
                                        df[lbl] = df.eval(normalized_expr)
                                    except Exception:
                                        df[lbl] = None
                        st.dataframe(df, use_container_width=True, hide_index=True)
                        col_dl, col_pdf = st.columns(2)
                        with col_dl:
                            xbio = BytesIO()
                            with pd.ExcelWriter(xbio, engine="xlsxwriter") as writer:
                                df.to_excel(writer, sheet_name=(r.slug or "Report")[:31], index=False)
                            st.download_button(
                                "Download",
                                data=xbio.getvalue(),
                                file_name=f"{r.slug or 'report'}_{d_from}_{d_to}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True,
                                key=f"cr_xlsx_{r.id}",
                            )
                        with col_pdf:
                            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
                            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
                            from reportlab.lib.enums import TA_CENTER
                            from reportlab.lib.pagesizes import A4
                            from reportlab.lib import colors
                            pdf_buf = BytesIO()
                            doc = SimpleDocTemplate(pdf_buf, pagesize=A4)
                            styles = getSampleStyleSheet()
                            title_style = ParagraphStyle('CRT', parent=styles['Heading1'], alignment=TA_CENTER)
                            elems = [Paragraph(r.name, title_style), Paragraph(f"{d_from} to {d_to}", styles['Normal']), Spacer(1, 8)]
                            tbl = Table([list(df.columns)] + df.astype(str).values.tolist(), repeatRows=1)
                            tbl.setStyle(TableStyle([
                                ('BACKGROUND',(0,0),(-1,0),colors.lightgrey),
                                ('GRID',(0,0),(-1,-1),0.25,colors.grey),
                                ('ALIGN',(0,0),(-1,-1),'CENTER')
                            ]))
                            elems.append(tbl)
                            doc.build(elems)
                            st.download_button(
                                "View PDF",
                                data=pdf_buf.getvalue(),
                                file_name=f"{r.slug or 'report'}_{d_from}_{d_to}.pdf",
                                mime="application/pdf",
                                use_container_width=True,
                                key=f"cr_pdf_{r.id}",
                            )
                        continue
                    # Single-source mode: build row-wise payload
                    objs = []
                    with get_session() as s2:
                        if src_key == "fso_material_balance":
                            from models import FSOOperation
                            vessels = [row[0] for row in s2.query(FSOOperation.fso_vessel).filter(FSOOperation.location_id == loc_id_for_src).distinct().all()]
                            sel_vessel = st.selectbox("FSO Vessel", options=vessels or ["(none)"] , key=f"cr_vessel_{r.id}")
                            if vessels and sel_vessel:
                                q = s2.query(FSOOperation).filter(FSOOperation.location_id == loc_id_for_src, FSOOperation.fso_vessel == sel_vessel)
                                q = q.filter(FSOOperation.date >= d_from, FSOOperation.date <= d_to)
                                items = q.order_by(FSOOperation.date.asc(), FSOOperation.time.asc()).all()
                                mb_rows = []
                                cur = d_from
                                to = d_to
                                from datetime import time as dt_time
                                while cur <= to:
                                    ps = datetime.combine(cur, dt_time(6,1))
                                    pe = datetime.combine(cur + timedelta(days=1), dt_time(6,0))
                                    period = [e for e in items if ps <= datetime.combine(e.date, e.time if isinstance(e.time, dt_time) else convert_to_time_object(e.time)) <= pe]
                                    if period:
                                        first = period[0]; last = period[-1]
                                        opening_stock = float(getattr(first, 'opening_stock', 0.0) or 0.0)
                                        opening_water = float(getattr(first, 'opening_water', 0.0) or 0.0)
                                        receipts = sum(float(getattr(e, 'net_receipt_dispatch', 0.0) or 0.0) for e in period if getattr(e, 'operation', '') == 'Receipt' and float(getattr(e, 'net_receipt_dispatch', 0.0) or 0.0) > 0)
                                        exports = sum(abs(float(getattr(e, 'net_receipt_dispatch', 0.0) or 0.0)) for e in period if getattr(e, 'operation', '') == 'Export' and float(getattr(e, 'net_receipt_dispatch', 0.0) or 0.0) < 0)
                                        closing_stock = float(getattr(last, 'closing_stock', 0.0) or 0.0)
                                        closing_water = float(getattr(last, 'closing_water', 0.0) or 0.0)
                                        loss_gain = closing_stock - (opening_stock + receipts - exports)
                                        mb_rows.append({
                                            "Date": cur.strftime("%Y-%m-%d"),
                                            "Opening Stock": opening_stock,
                                            "Opening Water": opening_water,
                                            "Receipts": receipts,
                                            "Exports": exports,
                                            "Closing Stock": closing_stock,
                                            "Closing Water": closing_water,
                                            "Loss/Gain": loss_gain,
                                        })
                                    cur = cur + timedelta(days=1)
                                objs = mb_rows
                        else:
                            rows, resolved_key = _load_rows_for_any_source(src, s2, loc_id_for_src, d_from, d_to)
                            meta = _get_source_meta(resolved_key) if resolved_key else None
                            if meta:
                                filtered = []
                                for obj in rows:
                                    d_val = _pluck_value(obj, meta["date_field"])
                                    base_date = None
                                    t_val = None
                                    if isinstance(d_val, datetime):
                                        base_date = d_val.date()
                                        t_val = d_val.time()
                                    else:
                                        base_date = d_val
                                        t_val = _pluck_value(obj, meta.get("time_field"))
                                    report_date = _derive_report_date(base_date, t_val)
                                    if report_date is None or report_date < d_from or report_date > d_to:
                                        continue
                                    filtered.append(obj)
                                objs = filtered
                            else:
                                objs = rows
                    payload = []
                    for obj in objs:
                        row = {}
                        for c in columns:
                            label = str(c.get("label") or "")
                            field = str(c.get("field") or "")
                            if (src_key == "otr_records" or src == "OTRRecord") and field in ("Net Rece/Disp (bbls)", "Net Water Rece/Disp (bbls)"):
                                try:
                                    op = getattr(obj, "operation")
                                    t = str(op or "").lower()
                                    sign = 1.0 if ("rece" in t) else (-1.0 if ("disp" in t or "export" in t) else 0.0)
                                    base = getattr(obj, "nsv_bbl") if field == "Net Rece/Disp (bbls)" else getattr(obj, "free_water_bbl")
                                    val = sign * float(base or 0.0)
                                except Exception:
                                    val = None
                            else:
                                try:
                                    val = getattr(obj, field)
                                except Exception:
                                    try:
                                        val = obj.get(field) if isinstance(obj, dict) else None
                                    except Exception:
                                        val = None
                            if isinstance(val, (datetime, date)):
                                try:
                                    val = val.strftime("%Y-%m-%d")
                                except Exception:
                                    val = str(val)
                            row[label or field] = val
                        payload.append(row)
                    df = pd.DataFrame(payload)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    col_dl, col_pdf = st.columns(2)
                    with col_dl:
                        xbio = BytesIO()
                        with pd.ExcelWriter(xbio, engine="xlsxwriter") as writer:
                            df.to_excel(writer, sheet_name=(r.slug or "Report")[:31], index=False)
                        st.download_button(
                            "Download",
                            data=xbio.getvalue(),
                            file_name=f"{r.slug or 'report'}_{d_from}_{d_to}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                            key=f"cr_xlsx_{r.id}",
                        )
                    with col_pdf:
                        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
                        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
                        from reportlab.lib.enums import TA_CENTER
                        from reportlab.lib.pagesizes import A4
                        from reportlab.lib import colors
                        pdf_buf = BytesIO()
                        doc = SimpleDocTemplate(pdf_buf, pagesize=A4)
                        styles = getSampleStyleSheet()
                        title_style = ParagraphStyle('CRT', parent=styles['Heading1'], alignment=TA_CENTER)
                        elems = [Paragraph(r.name, title_style), Paragraph(f"{d_from} to {d_to}", styles['Normal']), Spacer(1, 8)]
                        tbl = Table([list(df.columns)] + df.astype(str).values.tolist(), repeatRows=1)
                        tbl.setStyle(TableStyle([
                            ('BACKGROUND',(0,0),(-1,0),colors.lightgrey),
                            ('GRID',(0,0),(-1,-1),0.25,colors.grey),
                            ('ALIGN',(0,0),(-1,-1),'CENTER')
                        ]))
                        elems.append(tbl)
                        doc.build(elems)
                        st.download_button(
                            "View PDF",
                            data=pdf_buf.getvalue(),
                            file_name=f"{r.slug or 'report'}_{d_from}_{d_to}.pdf",
                            mime="application/pdf",
                            use_container_width=True,
                            key=f"cr_pdf_{r.id}",
                        )
    except Exception:
        pass

    # Special handling: if this location is OML-157, show OFS reports and skip other tabs
    try:
        loc_code_norm = (loc.code or "").replace(" ", "").replace("-", "").upper()
    except Exception:
        loc_code_norm = ""
    if loc_code_norm == "OML157":
        # Render OFS report tab only
        user = st.session_state.get("auth_user") or {}
        render_ofs_reports_tab(active_location_id, loc, user)
        return
    loc_name_upper = (loc.name or "").upper()
    loc_name_norm = (loc.name or "").strip().lower()
    loc_code_upper = (loc.code or "").upper()
    is_beneku_location = (
        "BFS" in loc_code_upper
        or "BFS" in loc_name_upper
        or "BENEKU" in loc_name_upper
    )
    is_asemoku_location = loc_name_norm == "asemoku jetty" or "ASEMOKU" in loc_code_upper
    is_ndoni_location = "NDONI" in loc_name_upper or "NDONI" in loc_code_upper
    show_river_reports = is_asemoku_location or is_ndoni_location

    def _canon_token(value: str | None) -> str:
        return "".join(ch for ch in str(value or "").upper() if ch.isalnum())

    loc_token_set = {
        _canon_token(loc.code),
        _canon_token(loc.name),
        _canon_token(f"{loc.name}{loc.code}"),
    }
    lagos_tokens = {"LAGOSHO", "HO", "LAGOS"}
    is_lagos_ho = bool(loc_token_set & lagos_tokens)

    # -------- Tabs --------
    # Added "TANKER DETAILS" as a fifth tab for tanker movement comparisons (Aggu & Ndoni)
    tab_labels = [
        "BFS VS JETTY",
        "JETTY METER RECORDS",
        "CONDENSATE RECEIPT",
        "BFS PRODUCTION",
        "TANKER DETAILS",
    ]
    river_tab = None
    produced_tab = None
    daily_tab = None
    river_index = produced_index = daily_index = None
    if show_river_reports:
        river_index = len(tab_labels)
        tab_labels.append("River Draft")
        produced_index = len(tab_labels)
        tab_labels.append("Produced Water")
    if is_lagos_ho:
        daily_index = len(tab_labels)
        tab_labels.append("DAILY PRODUCTION & EVACUATION")
    tabs = st.tabs(tab_labels)
    tab1, tab2, tab3, tab4, tab5 = tabs[:5]
    if river_index is not None:
        river_tab = tabs[river_index]
    if produced_index is not None:
        produced_tab = tabs[produced_index]
    if daily_index is not None:
        daily_tab = tabs[daily_index]

    # Immediately render tanker details tab (tab5) content.  This ensures that the Tanker Details
    # tab is properly registered and can be displayed alongside other report tabs.
    with tab5:
        try:
            render_tanker_details_tab()
        except Exception as ex:
            st.error(f"Failed to render tanker details: {ex}")

    if river_tab is not None:
        with river_tab:
            st.subheader("River Draft Records")
            river_df = pd.DataFrame(load_river_draft_records(active_location_id, limit=3000))
            if not river_df.empty:
                river_df["Date"] = pd.to_datetime(river_df["Date"]).dt.date
            river_dates = river_df["Date"].tolist() if not river_df.empty else []
            rpt_river_min, rpt_river_max = _derive_filter_bounds(river_dates)
            rpt_river_from_default = _ensure_date_key_in_bounds(
                f"report_river_from_{active_location_id}",
                rpt_river_min,
                rpt_river_max,
                rpt_river_min,
            )
            rpt_river_to_default = _ensure_date_key_in_bounds(
                f"report_river_to_{active_location_id}",
                rpt_river_min,
                rpt_river_max,
                rpt_river_max,
            )

            filters = st.columns(2)
            with filters[0]:
                river_from = st.date_input(
                    "From date",
                    value=rpt_river_from_default,
                    min_value=rpt_river_min,
                    max_value=rpt_river_max,
                    key=f"report_river_from_{active_location_id}",
                )
            with filters[1]:
                river_to = st.date_input(
                    "To date",
                    value=rpt_river_to_default,
                    min_value=rpt_river_min,
                    max_value=rpt_river_max,
                    key=f"report_river_to_{active_location_id}",
                )

            if not river_df.empty:
                if river_from:
                    river_df = river_df[river_df["Date"] >= river_from]
                if river_to:
                    river_df = river_df[river_df["Date"] <= river_to]

            display_cols = ["Date", "River Draft (m)", "Rainfall (cm)"]
            river_display_df = (
                river_df[display_cols].copy()
                if not river_df.empty
                else pd.DataFrame(columns=display_cols)
            )
            st.caption(f"{len(river_display_df)} record(s) shown")

            if river_display_df.empty:
                st.info("No river draft entries for the selected range.")
            else:
                st.dataframe(river_display_df, use_container_width=True, hide_index=True)

                river_csv = river_display_df.to_csv(index=False).encode("utf-8")
                river_xlsx = BytesIO()
                with pd.ExcelWriter(river_xlsx, engine="xlsxwriter") as writer:
                    river_display_df.to_excel(writer, index=False, sheet_name="RiverDraft")
                river_pdf = _generate_tanker_details_pdf(
                    river_display_df,
                    "River Draft Report",
                    river_from,
                    river_to,
                )
                file_stub = f"river_draft_{loc.code}_{river_from}_{river_to}".replace(" ", "_")
                downloads = st.columns(4)
                downloads[0].download_button(
                    "Download CSV",
                    data=river_csv,
                    file_name=f"{file_stub}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
                downloads[1].download_button(
                    "Download XLSX",
                    data=river_xlsx.getvalue(),
                    file_name=f"{file_stub}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
                downloads[2].download_button(
                    "Download PDF",
                    data=river_pdf,
                    file_name=f"{file_stub}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )
                if downloads[3].button(
                    "View PDF",
                    key=f"river_pdf_view_report_{active_location_id}",
                    use_container_width=True,
                ):
                    _open_pdf_blob(river_pdf)

    if produced_tab is not None:
        with produced_tab:
            st.subheader("Produced Water Records")
            pw_df = pd.DataFrame(load_produced_water_records(active_location_id, limit=3000))
            if not pw_df.empty:
                pw_df["Date"] = pd.to_datetime(pw_df["Date"]).dt.date
            report_pw_dates = pw_df["Date"].tolist() if not pw_df.empty else []
            rpt_pw_min, rpt_pw_max = _derive_filter_bounds(report_pw_dates)
            rpt_pw_from_default = _ensure_date_key_in_bounds(
                f"report_pw_from_{active_location_id}",
                rpt_pw_min,
                rpt_pw_max,
                rpt_pw_min,
            )
            rpt_pw_to_default = _ensure_date_key_in_bounds(
                f"report_pw_to_{active_location_id}",
                rpt_pw_min,
                rpt_pw_max,
                rpt_pw_max,
            )

            filters = st.columns(2)
            with filters[0]:
                pw_from = st.date_input(
                    "From date",
                    value=rpt_pw_from_default,
                    min_value=rpt_pw_min,
                    max_value=rpt_pw_max,
                    key=f"report_pw_from_{active_location_id}",
                )
            with filters[1]:
                pw_to = st.date_input(
                    "To date",
                    value=rpt_pw_to_default,
                    min_value=rpt_pw_min,
                    max_value=rpt_pw_max,
                    key=f"report_pw_to_{active_location_id}",
                )

            if not pw_df.empty:
                if pw_from:
                    pw_df = pw_df[pw_df["Date"] >= pw_from]
                if pw_to:
                    pw_df = pw_df[pw_df["Date"] <= pw_to]

            pw_display_cols = ["Date", "Produced Water (bbls)"]
            pw_display_df = (
                pw_df[pw_display_cols].copy()
                if not pw_df.empty
                else pd.DataFrame(columns=pw_display_cols)
            )
            st.caption(f"{len(pw_display_df)} record(s) shown")

            if pw_display_df.empty:
                st.info("No produced water entries for the selected range.")
            else:
                st.dataframe(pw_display_df, use_container_width=True, hide_index=True)

                pw_csv = pw_display_df.to_csv(index=False).encode("utf-8")
                pw_xlsx = BytesIO()
                with pd.ExcelWriter(pw_xlsx, engine="xlsxwriter") as writer:
                    pw_display_df.to_excel(writer, index=False, sheet_name="ProducedWater")
                pw_pdf = _generate_tanker_details_pdf(
                    pw_display_df,
                    "Produced Water Report",
                    pw_from,
                    pw_to,
                )
                file_stub = f"produced_water_{loc.code}_{pw_from}_{pw_to}".replace(" ", "_")
                downloads = st.columns(4)
                downloads[0].download_button(
                    "Download CSV",
                    data=pw_csv,
                    file_name=f"{file_stub}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
                downloads[1].download_button(
                    "Download XLSX",
                    data=pw_xlsx.getvalue(),
                    file_name=f"{file_stub}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
                downloads[2].download_button(
                    "Download PDF",
                    data=pw_pdf,
                    file_name=f"{file_stub}.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )
                if downloads[3].button(
                    "View PDF",
                    key=f"pw_pdf_view_report_{active_location_id}",
                    use_container_width=True,
                ):
                    _open_pdf_blob(pw_pdf)

    if daily_tab is not None:
        with daily_tab:
            st.subheader("DAILY PRODUCTION & EVACUATION")
            st.caption("Date-wise production, evacuation, and FSO receipt snapshot (Lagos HO view).")

            default_daily_to = date.today()
            default_daily_from = default_daily_to - timedelta(days=14)
            filter_cols = st.columns(2)
            daily_from = filter_cols[0].date_input(
                "From date",
                value=default_daily_from,
                key="lagos_daily_from",
            )
            daily_to = filter_cols[1].date_input(
                "To date",
                value=default_daily_to,
                key="lagos_daily_to",
            )

            if daily_from > daily_to:
                st.error("From date cannot be after To date.")
            else:
                with get_session() as s_locations:
                    location_entries = [
                        {"id": entry.id, "code": entry.code or "", "name": entry.name or ""}
                        for entry in s_locations.query(Location).all()
                    ]

                def _resolve_location(token_set: set[str]) -> dict | None:
                    canon_targets = {_canon_token(tok) for tok in token_set}
                    for entry in location_entries:
                        entry_tokens = {_canon_token(entry["code"]), _canon_token(entry["name"])}
                        if entry_tokens & canon_targets:
                            return entry
                    return None

                loc_aggu = _resolve_location({"AGGU"})
                loc_asemoku = _resolve_location({"JETTY", "ASEMOKU", "ASEMOKUJETTY"})
                loc_bfs = _resolve_location({"BFS", "BENEKU"})
                loc_oguali = _resolve_location({"OGUALI", "OML157", "OGUALIOML157"})
                loc_utapate = _resolve_location({"UTAPATE", "OML13", "OML-13"})
                loc_agge = _resolve_location({"AGGE"})

                missing_locations = []
                if not loc_aggu:
                    missing_locations.append("Aggu")
                if not loc_asemoku:
                    missing_locations.append("Asemoku Jetty")
                if not loc_bfs:
                    missing_locations.append("Beneku (BFS)")
                if not loc_oguali:
                    missing_locations.append("Oguali (OML-157)")
                if not loc_utapate:
                    missing_locations.append("Utapate (OML-13)")
                if not loc_agge:
                    missing_locations.append("Agge (FSO)")
                if missing_locations:
                    st.warning("Location mapping missing for: " + ", ".join(missing_locations))

                def _find_col_case(df: pd.DataFrame, candidates: list[str]) -> str | None:
                    if df is None or df.empty:
                        return None
                    for cand in candidates:
                        if cand in df.columns:
                            return cand
                    lower_map = {str(col).strip().lower(): col for col in df.columns}
                    for cand in candidates:
                        key = cand.strip().lower()
                        if key in lower_map:
                            return lower_map[key]
                    return None

                def _mb_series(loc_entry: dict | None, candidates: list[str], label: str) -> dict[date, float]:
                    if not loc_entry:
                        return {}
                    try:
                        rows = MBC.calculate_material_balance(
                            None,
                            (loc_entry["code"] or "").upper(),
                            daily_from,
                            daily_to,
                            location_id=loc_entry["id"],
                            debug=False,
                        ) or []
                    except Exception as ex:
                        st.warning(f"Unable to load {label}: {ex}")
                        return {}
                    if not rows:
                        return {}
                    df = pd.DataFrame(rows)
                    if df.empty or "Date" not in df.columns:
                        return {}
                    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
                    column = _find_col_case(df, candidates)
                    if not column:
                        return {}
                    df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
                    return {
                        row["Date"]: float(row[column])
                        for _, row in df.iterrows()
                        if isinstance(row["Date"], date)
                    }

                def _gpp_series(loc_entry: dict | None, column_name: str, label: str) -> dict[date, float]:
                    if not loc_entry:
                        return {}
                    try:
                        records = load_gpp_production_records(loc_entry["id"], limit=2000)
                    except Exception as ex:
                        st.warning(f"Unable to load {label}: {ex}")
                        return {}
                    if not records:
                        return {}
                    df = pd.DataFrame(records)
                    if df.empty or column_name not in df.columns:
                        return {}
                    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
                    df[column_name] = pd.to_numeric(df[column_name], errors="coerce").fillna(0.0)
                    df = df[(df["Date"] >= daily_from) & (df["Date"] <= daily_to)]
                    return {
                        row["Date"]: float(row[column_name])
                        for _, row in df.iterrows()
                        if isinstance(row["Date"], date)
                    }

                def _ofs_series(loc_entry: dict | None, label: str) -> tuple[dict[date, float], dict[date, float]]:
                    oguali_map: dict[date, float] = {}
                    ukpichi_map: dict[date, float] = {}
                    if not loc_entry:
                        return oguali_map, ukpichi_map
                    try:
                        with get_session() as s_ofs:
                            rows = (
                                s_ofs.query(OFSProductionEvacuationRecord)
                                .filter(
                                    OFSProductionEvacuationRecord.location_id == loc_entry["id"],
                                    OFSProductionEvacuationRecord.date >= daily_from,
                                    OFSProductionEvacuationRecord.date <= daily_to,
                                )
                                .all()
                            )
                    except Exception as ex:
                        st.warning(f"Unable to load {label}: {ex}")
                        return oguali_map, ukpichi_map
                    for row in rows:
                        oguali_map[row.date] = float(row.oguali_production or 0.0)
                        uk_val = float(row.ukpichi_production or 0.0)
                        other_val = float(row.other_locations or 0.0)
                        ukpichi_map[row.date] = uk_val + other_val
                    return oguali_map, ukpichi_map

                def _fso_receipt_series(loc_entry: dict | None, vessel_name: str, label: str) -> dict[date, float]:
                    if not loc_entry:
                        return {}
                    try:
                        from models import FSOOperation
                        with get_session() as s_fso:
                            rows = (
                                s_fso.query(FSOOperation.date, FSOOperation.operation, FSOOperation.net_receipt_dispatch)
                                .filter(
                                    FSOOperation.location_id == loc_entry["id"],
                                    FSOOperation.fso_vessel == vessel_name,
                                    FSOOperation.date >= daily_from,
                                    FSOOperation.date <= daily_to,
                                )
                                .all()
                            )
                    except Exception as ex:
                        st.warning(f"Unable to load {label}: {ex}")
                        return {}
                    result: dict[date, float] = defaultdict(float)
                    for dt_val, op_val, qty_val in rows:
                        op_text = str(op_val or "").strip().lower()
                        if op_text.startswith("receipt"):
                            result[dt_val] += float(qty_val or 0.0)
                    return dict(result)

                aggu_series = _mb_series(loc_aggu, ["Receipt", "Receipts"], "Aggu receipt")
                anz_series = _mb_series(loc_asemoku, ["ANZ Receipt"], "ANZ receipt")
                gpp_series = _gpp_series(loc_bfs, "Total GPP Production", "Total GPP production")
                okw_series = _gpp_series(loc_bfs, "OKW Production", "OKW production")
                oguali_series, ukpichi_series = _ofs_series(loc_oguali, "OFS production")
                utapate_series = _mb_series(loc_utapate, ["Receipt", "Receipts"], "Utapate receipt")
                tanvi_series = _fso_receipt_series(loc_agge, "MT TULJA TANVI", "Tanvi receipts")
                kalyani_series = _fso_receipt_series(loc_utapate, "MT TULJA KALYANI", "Kalyani receipts")

                date_range = pd.date_range(daily_from, daily_to)
                rows: list[dict[str, Any]] = []
                for dt_val in date_range:
                    day = dt_val.date()
                    aggu_val = aggu_series.get(day, 0.0)
                    anz_val = anz_series.get(day, 0.0)
                    gpp_val = gpp_series.get(day, 0.0)
                    oguali_val = oguali_series.get(day, 0.0)
                    okw_val = okw_series.get(day, 0.0)
                    ukpichi_val = ukpichi_series.get(day, 0.0)
                    total_psc = aggu_val + anz_val + gpp_val + oguali_val + okw_val + ukpichi_val
                    row = {
                        "Date": day,
                        "AGGU": aggu_val,
                        "ANIEZE & ENYIE": anz_val,
                        "GPP": gpp_val,
                        "OGUALI": oguali_val,
                        "OKW": okw_val,
                        "UKPICHI": ukpichi_val,
                        "TOTAL PSC PRODUCTION": total_psc,
                        "TANVI RECEIPT": tanvi_series.get(day, 0.0),
                        "UTAPATE": utapate_series.get(day, 0.0),
                        "KALYANI RECEIPT": kalyani_series.get(day, 0.0),
                    }
                    rows.append(row)

                daily_df = pd.DataFrame(rows)
                if daily_df.empty:
                    st.info("No production/evacuation data found for the selected period.")
                else:
                    daily_df["Date"] = pd.to_datetime(daily_df["Date"])
                    numeric_columns = [col for col in daily_df.columns if col != "Date"]
                    totals = {col: float(daily_df[col].sum()) for col in numeric_columns}
                    st.markdown("##### Totals (Selected Range)")
                    totals_df = pd.DataFrame([totals])
                    st.dataframe(
                        totals_df.style.format({col: "{:,.2f}" for col in totals_df.columns}),
                        use_container_width=True,
                        hide_index=True,
                    )
                    display_df = daily_df.copy()
                    display_df["Date"] = display_df["Date"].dt.strftime("%d-%b-%Y")
                    st.dataframe(
                        display_df.style.format({col: "{:,.2f}" for col in numeric_columns}),
                        use_container_width=True,
                        hide_index=True,
                    )

                    export_df = daily_df.copy()
                    export_df["Date"] = export_df["Date"].dt.strftime("%Y-%m-%d")
                    csv_bytes = export_df.to_csv(index=False).encode("utf-8")
                    xlsx_buffer = BytesIO()
                    with pd.ExcelWriter(xlsx_buffer, engine="xlsxwriter") as writer:
                        export_df.to_excel(writer, index=False, sheet_name="DailyProduction")
                    xlsx_bytes = xlsx_buffer.getvalue()
                    pdf_title = "DATE-WISE PRODUCTION, EVACUATION & FSO RECEIPT"
                    pdf_df = export_df.copy()
                    pdf_columns = list(export_df.columns)
                    pdf_summary_row: list[Any] = []
                    for col in pdf_columns:
                        if col == "Date":
                            pdf_summary_row.append("TOTAL")
                        else:
                            pdf_summary_row.append(totals.get(col, 0.0))
                    pdf_summary_pairs = [(col, totals.get(col, 0.0)) for col in numeric_columns]
                    pdf_bytes = _generate_tanker_details_pdf(
                        pdf_df,
                        pdf_title,
                        daily_from,
                        daily_to,
                        summary_row=pdf_summary_row,
                        summary_pairs=pdf_summary_pairs,
                    )

                    file_stub = f"daily_production_{daily_from}_{daily_to}".replace(" ", "_")
                    btn_cols = st.columns(4)
                    btn_cols[0].download_button(
                        "📥 CSV",
                        data=csv_bytes,
                        file_name=f"{file_stub}.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
                    btn_cols[1].download_button(
                        "📥 XLSX",
                        data=xlsx_bytes,
                        file_name=f"{file_stub}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )
                    btn_cols[2].download_button(
                        "📥 PDF",
                        data=pdf_bytes,
                        file_name=f"{file_stub}.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                    )
                    if btn_cols[3].button("👁️ View PDF", key="daily_production_pdf_view", use_container_width=True):
                        _open_pdf_blob(pdf_bytes, filename=f"{file_stub}.pdf")


    # ======================================================================
    # TAB 1: BENEKU DISPATCH VS JETTY RECEIPT
    # ======================================================================
    with tab1:
        st.subheader("BENEKU DISPATCH VS JETTY RECEIPT")

        # ----- Live filters -----
        with st.container(border=True):
            c1, c2, c3 = st.columns([0.33, 0.33, 0.34])
            with c1:
                f_from = st.date_input("From", value=date.today() - timedelta(days=14), key="rpt_bj_from")
            with c2:
                f_to = st.date_input("To", value=date.today(), key="rpt_bj_to")
            with c3:
                search_remark = st.text_input("Search in remarks", key="rpt_bj_search")

        # ----- Resolve BFS & JETTY locations -----
        with get_session() as s:
            bfs = s.query(Location).filter(Location.code.in_(["BFS", "Beneku", "BENEKU"])).first()
            jet = s.query(Location).filter(Location.code.in_(["JETTY", "Asemoku", "ASEMOKU"])).first()

        if not bfs or not jet:
            st.error("Could not find BFS or JETTY locations. Ensure Location.code contains 'BFS' and 'JETTY'.")
            st.stop()

        # ----- Pull daily MB rows via calculator -----
        bfs_rows = MBC.calculate_material_balance(None, "BFS", f_from, f_to, location_id=bfs.id, debug=False)
        jet_rows = MBC.calculate_material_balance(None, "JETTY", f_from, f_to, location_id=jet.id, debug=False)

        bfs_df = pd.DataFrame(bfs_rows)
        jet_df = pd.DataFrame(jet_rows)

        if "Dispatch to Jetty" not in bfs_df.columns:
            bfs_df["Dispatch to Jetty"] = 0.0
        if "OKW Receipt" not in jet_df.columns:
            jet_df["OKW Receipt"] = 0.0

        bfs_df = bfs_df[["Date", "Dispatch to Jetty"]].rename(columns={"Dispatch to Jetty": "BFS Dispatch"})
        jet_df = jet_df[["Date", "OKW Receipt"]].rename(columns={"OKW Receipt": "Jetty Receipt"})

        # Join by Date and compute metrics
        df = pd.merge(jet_df, bfs_df, on="Date", how="outer").sort_values("Date")

        # Optional: derive Jetty free-water received from OTR-like model when available
        from sqlalchemy import and_, or_

        with get_session() as s:
            try:
                from models import OTR  # your preferred model name
                OTRModel = OTR
            except Exception:
                try:
                    from models import OTRTransaction as OTRModel
                except Exception:
                    OTRModel = None

        water_by_report_date = {}

        if OTRModel is not None:
            # time window [f_from 06:01, f_to+1 06:00] to map early morning to previous day
            fetch_start = datetime.combine(f_from, time(hour=6, minute=1))
            fetch_end   = datetime.combine(f_to + timedelta(days=1), time(hour=6, minute=0))

            def col(obj, *names):
                for n in names:
                    if hasattr(obj, n):
                        return getattr(obj, n)
                raise AttributeError(f"Missing column candidates {names} on {obj}")

            try:
                q = s.query(OTRModel).filter(
                    and_(
                        col(OTRModel, "location_id", "LocationId") == jet.id,
                        or_(
                            col(OTRModel, "operation", "operation_name", "Operation").ilike("%OKW Receipt%"),
                            col(OTRModel, "operation", "operation_name", "Operation") == "OKW Receipt",
                        ),
                        col(OTRModel, "timestamp", "dt", "created_at", "TxnDateTime").between(fetch_start, fetch_end),
                    )
                ).order_by(col(OTRModel, "timestamp", "dt", "created_at", "TxnDateTime").asc())
                rows = q.all()
            except Exception:
                rows = []

            prev_fw = None
            for r in rows:
                ts = None
                for nm in ("timestamp", "dt", "created_at", "TxnDateTime"):
                    if hasattr(r, nm):
                        ts = getattr(r, nm)
                        break
                if ts is None:
                    continue

                fw = None
                for nm in ("free_water_bbl", "FreeWater_bbl", "Free_Water_bbl", "free_water", "Free_Water"):
                    if hasattr(r, nm):
                        fw = getattr(r, nm)
                        break
                if fw is None:
                    continue

                try:
                    fw = float(fw or 0.0)
                except Exception:
                    fw = 0.0

                if prev_fw is None:
                    net_water = 0.0
                else:
                    net_water = fw - prev_fw
                prev_fw = fw

                # map to OTMS "report date"
                cutoff = time(hour=6, minute=0)
                report_date = (ts.date() - timedelta(days=1)) if ts.time() <= cutoff else ts.date()

                if report_date < f_from or report_date > f_to:
                    continue

                water_by_report_date[report_date] = water_by_report_date.get(report_date, 0.0) + float(net_water or 0.0)

        df["Water Received"] = df["Date"].map(lambda d: float(water_by_report_date.get(pd.to_datetime(d).date(), 0.0)))

        # Loss/Gain and Loss/Gain %
        df["Loss/Gain"] = (df["Jetty Receipt"].fillna(0) + df["Water Received"].fillna(0)) - df["BFS Dispatch"].fillna(0)
        denom = (df["Jetty Receipt"].fillna(0) + df["Water Received"].fillna(0))
        df["Loss/Gain %"] = df["Loss/Gain"].where(denom == 0, df["Loss/Gain"] / denom) * 100.0

        # ----- Inline Remarks persisted to OUTPUT/reporting_notes.json -----
        OUTPUT = Path("OUTPUT"); OUTPUT.mkdir(exist_ok=True)
        REPORT_NOTES_PATH = OUTPUT / "reporting_notes.json"
        if not REPORT_NOTES_PATH.exists():
            REPORT_NOTES_PATH.write_text("{}", encoding="utf-8")
        try:
            notes = json.loads(REPORT_NOTES_PATH.read_text(encoding="utf-8"))
        except Exception:
            notes = {}

        key_space = f"BFS_JETTY::{loc.code}"
        remarks_map = notes.get(key_space, {})
        df["Remarks"] = df["Date"].map(lambda d: remarks_map.get(str(d), ""))

        if search_remark.strip():
            df = df[df["Remarks"].str.contains(search_remark.strip(), case=False, na=False)]

        # ----- Table (Remarks editable only) -----
        st.caption("Double-click �Remarks� to edit, then click **Save Remarks**.")
        cfg = {
            "Date": st.column_config.TextColumn("Date", width="small"),
            "BFS Dispatch": st.column_config.NumberColumn("BFS Dispatch (bbls)", format="%.2f"),
            "Jetty Receipt": st.column_config.NumberColumn("Jetty Receipt (bbls)", format="%.2f"),
            "Water Received": st.column_config.NumberColumn("Water Received (bbls)", format="%.2f"),
            "Loss/Gain": st.column_config.NumberColumn("Loss/Gain (bbls)", format="%.2f"),
            "Loss/Gain %": st.column_config.NumberColumn("Loss/Gain %", format="%.4f"),
            "Remarks": st.column_config.TextColumn("Remarks", width="large"),
        }
        edited = st.data_editor(
            df,
            use_container_width=True,
            hide_index=True,
            column_config=cfg,
            disabled=["Date", "BFS Dispatch", "Jetty Receipt", "Water Received", "Loss/Gain", "Loss/Gain %"],
        )

        # Normalize dtypes
        num_cols = ["BFS Dispatch", "Jetty Receipt", "Water Received", "Loss/Gain", "Loss/Gain %"]
        for c in num_cols:
            edited[c] = pd.to_numeric(edited[c], errors="coerce").fillna(0.0)
        edited["Date"] = edited["Date"].astype(str)

        # Save Remarks
        if st.button("💾 Save Remarks", type="primary", key="bj_save_remarks_btn"):
            new_map = {}
            for _, r in edited.iterrows():
                if str(r["Date"]).upper() == "TOTAL":
                    continue
                new_map[str(r["Date"])] = r.get("Remarks", "") or ""
            notes[key_space] = new_map
            REPORT_NOTES_PATH.write_text(json.dumps(notes, indent=2), encoding="utf-8")
            st.success("Remarks saved.")
            try:
                import streamlit as _stmod
                _stmod.rerun()
            except Exception:
                import streamlit as _stmod
                _stmod.experimental_rerun()

        # Totals
        sum_cols = ["BFS Dispatch", "Jetty Receipt", "Water Received", "Loss/Gain"]
        totals = {c: float(edited[c].sum()) for c in sum_cols}
        denom_total = totals["Jetty Receipt"] + totals["Water Received"]
        totals_pct = 0.0 if denom_total == 0 else (totals["Loss/Gain"] / denom_total) * 100.0

        totals_row = {
            "Date": "TOTAL",
            "BFS Dispatch": totals["BFS Dispatch"],
            "Jetty Receipt": totals["Jetty Receipt"],
            "Water Received": totals["Water Received"],
            "Loss/Gain": totals["Loss/Gain"],
            "Loss/Gain %": totals_pct,
            "Remarks": "",
        }

        st.markdown("**Totals (filtered):**")
        totals_df = pd.DataFrame([totals_row])
        st.dataframe(
            totals_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "BFS Dispatch": st.column_config.NumberColumn(format="%.2f"),
                "Jetty Receipt": st.column_config.NumberColumn(format="%.2f"),
                "Water Received": st.column_config.NumberColumn(format="%.2f"),
                "Loss/Gain": st.column_config.NumberColumn(format="%.2f"),
                "Loss/Gain %": st.column_config.NumberColumn(format="%.4f"),
            },
        )

        edited_with_total = pd.concat([edited, totals_df], ignore_index=True)

        # Exports
        st.markdown("---")
        col_a, col_b, col_c, col_d = st.columns(4)

        # CSV
        col_a.download_button(
            "📥 Download CSV",
            data=edited_with_total.to_csv(index=False).encode("utf-8"),
            file_name=f"bfs_vs_jetty_{loc.code}_{str(f_from)}_{str(f_to)}.csv",
            mime="text/csv",
            key="bj_csv_dl",
        )

        # XLSX
        xbio = BytesIO()
        with pd.ExcelWriter(xbio, engine="xlsxwriter") as writer:
            edited_with_total.to_excel(writer, index=False, sheet_name="BFS_vs_Jetty")
        col_b.download_button(
            "📥 Download XLSX",
            data=xbio.getvalue(),
            file_name=f"bfs_vs_jetty_{loc.code}_{str(f_from)}_{str(f_to)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="bj_xlsx_dl",
        )

        # PDF generator for BFS vs JETTY
        def _pdf_bytes(_df: pd.DataFrame) -> bytes:
            for c in ["BFS Dispatch", "Jetty Receipt", "Water Received", "Loss/Gain", "Loss/Gain %"]:
                _df[c] = pd.to_numeric(_df[c], errors="coerce").fillna(0.0)

            if len(_df) == 0 or str(_df.iloc[-1]["Date"]).upper() != "TOTAL":
                _sum_cols = ["BFS Dispatch", "Jetty Receipt", "Water Received", "Loss/Gain"]
                _totals = {c: float(_df[c].sum()) for c in _sum_cols} if len(_df) else {c: 0.0 for c in _sum_cols}
                _denom_total = _totals["Jetty Receipt"] + _totals["Water Received"]
                _totals_pct = 0.0 if _denom_total == 0 else (_totals["Loss/Gain"] / _denom_total) * 100.0
                _total_row = {
                    "Date": "TOTAL",
                    "BFS Dispatch": _totals["BFS Dispatch"],
                    "Jetty Receipt": _totals["Jetty Receipt"],
                    "Water Received": _totals["Water Received"],
                    "Loss/Gain": _totals["Loss/Gain"],
                    "Loss/Gain %": _totals_pct,
                    "Remarks": "",
                }
                _df = pd.concat([_df, pd.DataFrame([_total_row])], ignore_index=True)

            bio = BytesIO()

            page_w, _page_h = A4
            lm = rm = tm = bm = 0.5 * cm
            avail_w = page_w - lm - rm

            doc = SimpleDocTemplate(
                bio, pagesize=A4,
                leftMargin=lm, rightMargin=rm, topMargin=tm, bottomMargin=bm
            )

            styles = getSampleStyleSheet()
            title_style = ParagraphStyle("T", parent=styles["Heading1"], fontSize=15,
                                         alignment=TA_CENTER, textColor=colors.HexColor("#1f4788"))
            sub_style = ParagraphStyle("S", parent=styles["Normal"], fontSize=10,
                                       alignment=TA_CENTER, textColor=colors.HexColor("#666666"))

            elements = []
            elements.append(Paragraph("<b>BENEKU DISPATCH VS JETTY RECEIPT</b>", title_style))
            elements.append(Paragraph(f"{loc.name} ({loc.code}) � Period: <b>{str(f_from)}</b> to <b>{str(f_to)}</b>", sub_style))
            elements.append(Spacer(1, 0.3 * cm))

            headers = ["Date", "BFS Dispatch", "Jetty Receipt", "Water Received", "Loss/Gain", "Loss/Gain %", "Remarks"]
            data = [headers]
            for _, r in _df.iterrows():
                is_total = str(r["Date"]).upper() == "TOTAL"
                data.append([
                    r["Date"],
                    f"{float(r['BFS Dispatch']):,.2f}",
                    f"{float(r['Jetty Receipt']):,.2f}",
                    f"{float(r['Water Received']):,.2f}",
                    f"{float(r['Loss/Gain']):,.2f}",
                    f"{float(r['Loss/Gain %']):,.4f}",
                    ("TOTAL" if is_total else (str(r.get("Remarks") or "")[:90])),
                ])

            weights = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.2]
            scale = avail_w / sum(weights)
            col_widths = [w * scale for w in weights]

            table = Table(data, repeatRows=1, colWidths=col_widths)
            table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4788")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 8),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("ROWBACKGROUNDS", (0, 1), (-1, -2), [colors.whitesmoke, colors.HexColor("#f8f9fa")]),
                ("FONTSIZE", (0, 1), (-1, -1), 8),
                ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#e8eefc")),
                ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
            ]))

            elements.append(table)
            doc.build(elements)
            return bio.getvalue()

        pdf_bytes = _pdf_bytes(edited_with_total)

        col_c.download_button(
            "📥 Download PDF",
            data=pdf_bytes,
            file_name=f"bfs_vs_jetty_{loc.code}_{str(f_from)}_{str(f_to)}.pdf",
            mime="application/pdf",
            key="bj_pdf_dl",
        )

        if col_d.button("👁️ View PDF", key=f"view_bfs_pdf_{loc.code}"):
            b64 = base64.b64encode(pdf_bytes).decode("utf-8")
            components.html(
                f"""
                <script>
                (function(){{
                const b64="{b64}";
                const byteChars=atob(b64);
                const byteNums=new Array(byteChars.length);
                for (let i=0;i<byteChars.length;i++) byteNums[i]=byteChars.charCodeAt(i);
                const blob=new Blob([new Uint8Array(byteNums)],{{type:'application/pdf'}});
                const url=URL.createObjectURL(blob);
                window.open(url,'_blank');
                setTimeout(()=>URL.revokeObjectURL(url),60000);
                }})();
                </script>
                """,
                height=0
            )

    # ======================================================================
    # TAB 2: JETTY METER READING
    # ======================================================================
    with tab2:
        st.subheader("JETTY METER RECORDS")

        with st.container(border=True):
            f_col1, f_col2 = st.columns(2)
            with f_col1:
                jmr_from = st.date_input("From (Jetty Meter)", value=date.today() - timedelta(days=14), key="jmr_from")
            with f_col2:
                jmr_to = st.date_input("To (Jetty Meter)", value=date.today(), key="jmr_to")

        # Fetch meter transactions for active Jetty location within the range
        with get_session() as s:
            try:
                from models import MeterTransaction  # added Meter-2 fields earlier
            except Exception:
                MeterTransaction = None

            entries = []
            if MeterTransaction is not None:
                entries = (
                    s.query(MeterTransaction)
                    .filter(
                        MeterTransaction.location_id == active_location_id,
                        MeterTransaction.date >= jmr_from,
                        MeterTransaction.date <= jmr_to,
                    )
                    .order_by(MeterTransaction.date.asc())
                    .all()
                )

        # Build DataFrame with SHORT column names expected by the PDF
        jmr_cols = [
            "Date",
            "Opening (M1)", "Closing (M1)",
            "Opening (M2)", "Closing (M2)",
            "Net Receipt/Dispatch",
            "Net Tank dispatch",   # NEW
            "Variance",            # NEW
            "Remarks",
        ]

        data_rows = []
        for e in entries:
            try:
                dte = e.date.strftime("%Y-%m-%d") if isinstance(e.date, date) else str(e.date)
            except Exception:
                dte = str(e.date)

            om1 = float(e.opening_meter_reading or 0.0)
            cm1 = float(e.closing_meter_reading or 0.0)
            om2 = float(getattr(e, "opening_meter2_reading", 0.0) or 0.0)
            cm2 = float(getattr(e, "closing_meter2_reading", 0.0) or 0.0)
            net = (cm1 - om1) + (cm2 - om2)

            data_rows.append({
                "Date": dte,
                "Opening (M1)": om1,
                "Closing (M1)": cm1,
                "Opening (M2)": om2,
                "Closing (M2)": cm2,
                "Net Receipt/Dispatch": net,
                "Net Tank dispatch": 0.0,  # will be filled from MB
                "Variance": 0.0,            # will be computed
                "Remarks": e.remarks or "",
            })

        # Ensure columns exist even if no entries
        jmr_df = pd.DataFrame(data_rows, columns=jmr_cols)

        # --- Bring Net Tank dispatch from Material Balance (JETTY) and compute Variance ---
        # Normalize JMR dates and ensure target cols exist
        if not jmr_df.empty:
            jmr_df["Date"] = pd.to_datetime(jmr_df["Date"]).dt.strftime("%Y-%m-%d")
        else:
            jmr_df["Date"] = pd.Series(dtype=str)

        for col in ["Net Tank dispatch", "Variance"]:
            if col not in jmr_df.columns:
                jmr_df[col] = 0.0

        # Pull MB and compute Net Tank dispatch (case-insensitive, robust)
        try:
            mb_rows = MBC.calculate_material_balance(
                None, "JETTY", jmr_from, jmr_to, location_id=loc.id, debug=False
            )
            mb_df = pd.DataFrame(mb_rows)
        except Exception:
            mb_df = pd.DataFrame()

        if not mb_df.empty:
            # Lowercase and trim all MB column names for flexible matching
            mb_df = mb_df.rename(columns=lambda x: str(x).strip().lower())

            # Date normalization
            if "date" in mb_df.columns:
                mb_df["date"] = pd.to_datetime(mb_df["date"]).dt.strftime("%Y-%m-%d")
            else:
                mb_df["date"] = ""  # ensures no matches if MB lacks date

            # Accept variants like "Other Dispatches" vs "Other dispatch"
            if "dispatch to barge" not in mb_df.columns:
                mb_df["dispatch to barge"] = 0.0

            if "other dispatch" not in mb_df.columns:
                if "other dispatches" in mb_df.columns:
                    mb_df["other dispatch"] = mb_df["other dispatches"]
                else:
                    mb_df["other dispatch"] = 0.0

            mb_df["net tank dispatch"] = (
                pd.to_numeric(mb_df["dispatch to barge"], errors="coerce").fillna(0.0) +
                pd.to_numeric(mb_df["other dispatch"], errors="coerce").fillna(0.0)
            )

            # Map by date instead of merge to avoid _x/_y suffixes
            tank_map = mb_df.set_index("date")["net tank dispatch"].to_dict()
            if not jmr_df.empty:
                jmr_df["Net Tank dispatch"] = jmr_df["Date"].map(tank_map).fillna(0.0)

        # Compute Variance safely
        jmr_df["Variance"] = (
            pd.to_numeric(jmr_df["Net Receipt/Dispatch"], errors="coerce").fillna(0.0) -
            pd.to_numeric(jmr_df["Net Tank dispatch"], errors="coerce").fillna(0.0)
        )

        # View table
        if not jmr_df.empty:
            st.dataframe(
                jmr_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Date": st.column_config.TextColumn("Date", width="small"),
                    "Opening (M1)": st.column_config.NumberColumn("Opening (M1)", format="%.2f"),
                    "Closing (M1)": st.column_config.NumberColumn("Closing (M1)", format="%.2f"),
                    "Opening (M2)": st.column_config.NumberColumn("Opening (M2)", format="%.2f"),
                    "Closing (M2)": st.column_config.NumberColumn("Closing (M2)", format="%.2f"),
                    "Net Receipt/Dispatch": st.column_config.NumberColumn("Net Receipt/Dispatch", format="%.2f"),
                    "Net Tank dispatch": st.column_config.NumberColumn("Net Tank dispatch", format="%.2f"),
                    "Variance": st.column_config.NumberColumn("Variance", format="%.2f"),
                    "Remarks": st.column_config.TextColumn("Remarks", width="large"),
                },
            )
        else:
            st.info("No meter transactions found for the selected date range.")

        # ----- Exports -----
        st.markdown("---")
        d_col_a, d_col_b, d_col_c, d_col_d = st.columns(4)

        # CSV
        d_col_a.download_button(
            "📥 Download CSV",
            data=jmr_df.to_csv(index=False).encode("utf-8"),
            file_name=f"jetty_meter_reading_{loc.code}_{str(jmr_from)}_{str(jmr_to)}.csv",
            mime="text/csv",
            key="jmr_csv_dl",
        )

        # XLSX
        _xlsx_io = BytesIO()
        with pd.ExcelWriter(_xlsx_io, engine="xlsxwriter") as writer:
            jmr_df.to_excel(writer, index=False, sheet_name="JettyMeterReading")
        d_col_b.download_button(
            "📥 Download XLSX",
            data=_xlsx_io.getvalue(),
            file_name=f"jetty_meter_reading_{loc.code}_{str(jmr_from)}_{str(jmr_to)}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="jmr_xlsx_dl",
        )

        # PDF helper for Jetty Meter Reading (LANDSCAPE A4)
        def _jmr_pdf_bytes(df: pd.DataFrame) -> bytes:
            # local import to ensure landscape is available even if not imported globally
            from reportlab.lib.pagesizes import A4, landscape

            df = df.copy()

            # numeric safety (works even when df is empty because columns exist)
            for col_name in [
                "Opening (M1)", "Closing (M1)",
                "Opening (M2)", "Closing (M2)",
                "Net Receipt/Dispatch",
                "Net Tank dispatch",  # NEW
                "Variance",           # NEW
            ]:
                if col_name in df.columns:
                    df[col_name] = pd.to_numeric(df[col_name], errors="coerce").fillna(0.0)

            bio = BytesIO()

            # LANDSCAPE sizing
            page_w, _page_h = landscape(A4)
            lm = rm = tm = bm = 0.5 * cm
            avail_w = page_w - lm - rm

            doc = SimpleDocTemplate(
                bio,
                pagesize=landscape(A4),  # <<< LANDSCAPE
                leftMargin=lm,
                rightMargin=rm,
                topMargin=tm,
                bottomMargin=bm,
            )

            styles = getSampleStyleSheet()
            title_style = ParagraphStyle(
                "JMRTitle", parent=styles["Heading1"], fontSize=15,
                alignment=TA_CENTER, textColor=colors.HexColor("#1f4788"),
            )
            sub_style = ParagraphStyle(
                "JMRSub", parent=styles["Normal"], fontSize=10,
                alignment=TA_CENTER, textColor=colors.HexColor("#666666"),
            )

            elements = []
            elements.append(Paragraph("<b>JETTY METER RECORDS</b>", title_style))
            elements.append(
                Paragraph(
                    f"{loc.name} ({loc.code}) � Period: <b>{str(jmr_from)}</b> to <b>{str(jmr_to)}</b>",
                    sub_style,
                )
            )
            elements.append(Spacer(1, 0.3 * cm))

            # Headers including the two new columns
            headers = [
                "Date",
                "Opening (M1)", "Closing (M1)",
                "Opening (M2)", "Closing (M2)",
                "Net Receipt/Dispatch",
                "Net Tank dispatch",   # NEW
                "Variance",            # NEW
                "Remarks",
            ]
            data = [headers]

            for _, row in df.iterrows():
                data.append([
                    str(row.get("Date", "")),
                    f"{float(row.get('Opening (M1)', 0.0)):,.2f}",
                    f"{float(row.get('Closing (M1)', 0.0)):,.2f}",
                    f"{float(row.get('Opening (M2)', 0.0)):,.2f}",
                    f"{float(row.get('Closing (M2)', 0.0)):,.2f}",
                    f"{float(row.get('Net Receipt/Dispatch', 0.0)):,.2f}",
                    f"{float(row.get('Net Tank dispatch', 0.0)):,.2f}",
                    f"{float(row.get('Variance', 0.0)):,.2f}",
                    (str(row.get("Remarks", ""))[:90] if row.get("Remarks") else "-"),
                ])

            # Column widths (remarks wider) - 9 columns total; landscape gives us more room
            weights = [1.0, 0.9, 0.9, 0.9, 0.9, 1.1, 1.1, 1.1, 2.0]
            scale = avail_w / sum(weights)
            col_widths = [w * scale for w in weights]

            table = Table(data, repeatRows=1, colWidths=col_widths)
            table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4788")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 8),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor("#f8f9fa")]),
                ("FONTSIZE", (0, 1), (-1, -1), 8),
            ]))

            elements.append(table)
            doc.build(elements)
            return bio.getvalue()

        jmr_pdf_bytes = _jmr_pdf_bytes(jmr_df)

        # PDF Download
        d_col_c.download_button(
            "📥 Download PDF",
            data=jmr_pdf_bytes,
            file_name=f"jetty_meter_reading_{loc.code}_{str(jmr_from)}_{str(jmr_to)}.pdf",
            mime="application/pdf",
            key="jmr_pdf_dl_btn",
        )

        # View PDF
        if d_col_d.button("👁️ View PDF", key="jmr_pdf_view_btn"):
            _b64 = base64.b64encode(jmr_pdf_bytes).decode("utf-8")
            components.html(
                f"""
                <script>
                (function(){{
                const b64="{_b64}";
                const byteChars=atob(b64);
                const byteNums=new Array(byteChars.length);
                for (let i=0;i<byteChars.length;i++) byteNums[i]=byteChars.charCodeAt(i);
                const blob=new Blob([new Uint8Array(byteNums)],{{type:'application/pdf'}});
                const url=URL.createObjectURL(blob);
                window.open(url,'_blank');
                setTimeout(()=>URL.revokeObjectURL(url),60000);
                }})();
                </script>
                """,
                height=0,
            )
    # ======================================================================
    # TAB 3: CONDENSATE RECEIPT
    # ======================================================================
    with tab3:
        st.subheader("CONDENSATE RECEIPT")

        from datetime import date, timedelta
        import pandas as pd
        import io, base64

        display_df = None

        # --- Filters (last 14 days default) ---
        with st.container(border=True):
            c1, c2, c3 = st.columns([1,1,1])
            with c1:
                cr_from = st.date_input("From", value=date.today() - timedelta(days=14), key="cr_from")
            with c2:
                cr_to = st.date_input("To", value=date.today(), key="cr_to")
            with c3:
                search_tid = st.text_input("Ticket Id (optional)", placeholder="e.g. BFS-TIC-...")

        # --- Load saved condensate receipts using shared helper to keep math identical ---
        raw_records, _ = load_condensate_transactions(active_location_id, limit=2000)
        df_raw = pd.DataFrame(raw_records)

        if df_raw.empty:
            st.info("No condensate receipts captured yet for this location.")
        else:
            df_raw["Date"] = pd.to_datetime(df_raw["Date"], errors="coerce")
            filtered = df_raw.dropna(subset=["Date"]).copy()
            filtered_dates = filtered["Date"].dt.date
            date_mask = (filtered_dates >= cr_from) & (filtered_dates <= cr_to)
            filtered = filtered[date_mask]

            if search_tid:
                search_term = search_tid.strip()
                if search_term:
                    filtered = filtered[
                        filtered["Ticket ID"].astype(str).str.contains(search_term, case=False, na=False)
                    ]

            if filtered.empty:
                st.info("No condensate receipts found for the selected range.")
            else:
                filtered = filtered.sort_values("Date", ascending=False)
                filtered["Date"] = filtered["Date"].dt.strftime("%Y-%m-%d")

                rename_map = {
                    "Ticket ID": "Ticket Id",
                    "Opening (m3)": "Opening meter reading",
                    "Closing (m3)": "Closing meter reading",
                    "Net Receipt (m3)": "Qty (m�)",
                    "API @ 60": "API @60°F",
                    "Created By": "Entered By",
                    "Updated By": "Edited By",
                    "Updated At": "Edited At",
                }
                display_df = filtered.rename(columns=rename_map)

                column_order = [
                    "Ticket Id",
                    "Date",
                    "Opening meter reading",
                    "Closing meter reading",
                    "Qty (m�)",
                    "GOV (bbls)",
                    "API @60°F",
                    "VCF",
                    "GSV (bbls)",
                    "LT",
                    "MT",
                ]
                # Ensure all expected columns exist before reordering
                for col in column_order:
                    if col not in display_df.columns:
                        display_df[col] = ""
                display_df = display_df[column_order].reset_index(drop=True)

                # Nice wide table (values already match View Transactions output)
                st.dataframe(display_df, hide_index=True, use_container_width=True)

        # --- Export buttons ---
        exp1, exp2, exp3, exp4 = st.columns([1,1,1,1])

        # CSV
        if display_df is not None and not display_df.empty:
            csv_bytes = display_df.to_csv(index=False).encode("utf-8")
            with exp1:
                st.download_button("Download CSV", data=csv_bytes, file_name="condensate_receipts.csv", mime="text/csv")

            # XLSX
            xbuf = io.BytesIO()
            with pd.ExcelWriter(xbuf, engine="xlsxwriter") as writer:
                display_df.to_excel(writer, index=False, sheet_name="Condensate")
            with exp2:
                st.download_button(
                    "Download XLSX",
                    data=xbuf.getvalue(),
                    file_name="condensate_receipts.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

            # PDF (A4 landscape, styled like other reporting tabs)
            def _build_cond_pdf(pdf_df: pd.DataFrame, title_text: str) -> bytes:
                from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
                from reportlab.lib.pagesizes import A4, landscape
                from reportlab.lib.units import cm
                from reportlab.lib import colors
                from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
                from reportlab.lib.enums import TA_CENTER

                df = pdf_df.copy()
                numeric_cols = ["Opening meter reading", "Closing meter reading", "Qty (m�)", "GOV (bbls)", "GSV (bbls)", "LT", "MT", "VCF", "API @60°F"]
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

                totals = {
                    "Qty (m�)": float(df.get("Qty (m�)", pd.Series(dtype=float)).sum()),
                    "GOV (bbls)": float(df.get("GOV (bbls)", pd.Series(dtype=float)).sum()),
                    "GSV (bbls)": float(df.get("GSV (bbls)", pd.Series(dtype=float)).sum()),
                    "LT": float(df.get("LT", pd.Series(dtype=float)).sum()),
                    "MT": float(df.get("MT", pd.Series(dtype=float)).sum()),
                }
                total_row = {
                    "Ticket Id": "",
                    "Date": "TOTAL",
                    "Opening meter reading": "",
                    "Closing meter reading": "",
                    "Qty (m�)": totals["Qty (m�)"],
                    "GOV (bbls)": totals["GOV (bbls)"],
                    "GSV (bbls)": totals["GSV (bbls)"],
                    "API @60°F": "",
                    "VCF": "",
                    "LT": totals["LT"],
                    "MT": totals["MT"],
                }
                df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)

                buf = io.BytesIO()
                doc = SimpleDocTemplate(
                    buf,
                    pagesize=landscape(A4),
                    leftMargin=0.5 * cm,
                    rightMargin=0.5 * cm,
                    topMargin=0.5 * cm,
                    bottomMargin=0.5 * cm,
                )
                styles = getSampleStyleSheet()
                title_style = ParagraphStyle(
                    "cond_title",
                    parent=styles["Heading1"],
                    fontSize=15,
                    alignment=TA_CENTER,
                    textColor=colors.HexColor("#1f4788"),
                )
                sub_style = ParagraphStyle(
                    "cond_sub",
                    parent=styles["Normal"],
                    fontSize=10,
                    alignment=TA_CENTER,
                    textColor=colors.HexColor("#666666"),
                )

                story = [
                    Paragraph(f"<b>{title_text}</b>", title_style),
                    Paragraph(
                        f"{loc.name} ({loc.code}) � Period: <b>{cr_from.strftime('%d-%b-%Y')}</b> to <b>{cr_to.strftime('%d-%b-%Y')}</b>",
                        sub_style,
                    ),
                    Paragraph(
                        f"Total GSV (bbls): <b>{totals['GSV (bbls)']:,.2f}</b> � Receipts: <b>{len(pdf_df):,}</b>",
                        sub_style,
                    ),
                        Spacer(1, 0.25 * cm),
                ]

                headers = [
                    "Date",
                    "Ticket Id",
                    "Opening meter reading",
                    "Closing meter reading",
                    "Qty (m�)",
                    "GOV (bbls)",
                    "GSV (bbls)",
                    "API @60°F",
                    "VCF",
                    "LT",
                    "MT",
                ]
                data = [headers]
                for _, row in df.iterrows():
                    is_total = str(row.get("Date", "")).upper() == "TOTAL"
                    data.append([
                        row.get("Date", ""),
                        row.get("Ticket Id", "") if not is_total else "TOTAL",
                        "" if is_total else f"{float(row.get('Opening meter reading') or 0.0):,.3f}",
                        "" if is_total else f"{float(row.get('Closing meter reading') or 0.0):,.3f}",
                        f"{float(row.get('Qty (m�)') or 0.0):,.3f}",
                        f"{float(row.get('GOV (bbls)') or 0.0):,.2f}",
                        f"{float(row.get('GSV (bbls)') or 0.0):,.2f}",
                        "" if is_total else f"{float(row.get('API @60°F') or 0.0):,.2f}",
                        "" if is_total else f"{float(row.get('VCF') or 0.0):,.5f}",
                        f"{float(row.get('LT') or 0.0):,.2f}",
                        f"{float(row.get('MT') or 0.0):,.2f}",
                    ])

                weights = [1.1, 1.4, 1.0, 1.0, 1.0, 0.95, 0.95, 0.8, 0.7, 0.85, 0.85]
                available_width = doc.width
                scale = available_width / sum(weights)
                col_widths = [w * scale for w in weights]

                table = Table(data, colWidths=col_widths, repeatRows=1)
                table.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4788")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 8),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -2), [colors.whitesmoke, colors.HexColor("#f8f9fa")]),
                    ("FONTSIZE", (0, 1), (-1, -1), 8),
                    ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#e8eefc")),
                    ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ]))

                story.append(table)
                doc.build(story)
                return buf.getvalue()

            pdf_source = display_df[[
                "Ticket Id","Date","Opening meter reading","Closing meter reading",
                "Qty (m�)","GOV (bbls)","API @60°F","VCF","GSV (bbls)","LT","MT"
            ]].copy()

            pdf_bytes = _build_cond_pdf(pdf_source, "Condensate Receipts")

            with exp3:
                st.download_button("Download PDF", data=pdf_bytes, file_name="condensate_receipts.pdf", mime="application/pdf")

            # Open PDF in new tab
            with exp4:
                if st.button("👁️ View PDF", key="cond_pdf_view_btn"):
                    b64 = base64.b64encode(pdf_bytes).decode("utf-8")
                    components.html(
                        f"""
                        <script>
                        (function(){{
                            const b64="{b64}";
                            const byteChars=atob(b64);
                            const byteNums=new Array(byteChars.length);
                            for (let i=0;i<byteChars.length;i++) byteNums[i]=byteChars.charCodeAt(i);
                            const blob=new Blob([new Uint8Array(byteNums)],{{type:'application/pdf'}});
                            const url=URL.createObjectURL(blob);
                            window.open(url,'_blank');
                            setTimeout(()=>URL.revokeObjectURL(url),60000);
                        }})();
                        </script>
                        """,
                        height=0,
                    )


    # ======================================================================
    # TAB 4: PRODUCTION
    # ======================================================================
    with tab4:
        st.subheader("Production")
        if not is_beneku_location:
            st.info("Production reporting is only available for Beneku/BFS locations.")
        else:
            default_from = date.today() - timedelta(days=14)
            default_to = date.today()
            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([0.2, 0.2, 0.2, 0.4])
                with c1:
                    gpp_from = st.date_input("From", value=default_from, key="rpt_gpp_from")
                with c2:
                    gpp_to = st.date_input("To", value=default_to, key="rpt_gpp_to")
                with c3:
                    gpp_min_total = st.number_input("Min GPP Total (bbls)", min_value=0.0, step=100.0, key="rpt_gpp_min_total")
                with c4:
                    gpp_search = st.text_input(
                        "Search remarks / user",
                        key="rpt_gpp_search",
                        placeholder="Type any keyword",
                    ).strip().lower()

            gpp_records = load_gpp_production_records(active_location_id, limit=2000)
            gpp_df = pd.DataFrame(gpp_records)
            if not gpp_df.empty:
                gpp_df["Date"] = pd.to_datetime(gpp_df["Date"]).dt.date
                gpp_df["Updated At"] = (
                    pd.to_datetime(gpp_df["Updated At"], errors="coerce")
                    .dt.strftime("%Y-%m-%d %H:%M:%S")
                    .fillna("")
                )

                if gpp_from:
                    gpp_df = gpp_df[gpp_df["Date"] >= gpp_from]
                if gpp_to:
                    gpp_df = gpp_df[gpp_df["Date"] <= gpp_to]
                if gpp_min_total and gpp_min_total > 0:
                    gpp_df = gpp_df[gpp_df["Total GPP Production"] >= gpp_min_total]
                if gpp_search:
                    gpp_df = gpp_df[
                        gpp_df.apply(
                            lambda r: gpp_search in str(r["Remarks"]).lower()
                            or gpp_search in str(r["Created By"]).lower()
                            or gpp_search in str(r["Updated By"]).lower(),
                            axis=1,
                        )
                    ]

            st.caption(f"{len(gpp_df)} record(s) found")
            gpp_display_cols = [
                "Date",
                "OKW Production",
                "GPP1 Production",
                "GPP2 Production",
                "GPP Closing Stock",
                "Total GPP Production",
                "Remarks",
                "Created By",
                "Updated By",
                "Updated At",
            ]

            if gpp_df.empty:
                st.info("No production records match the selected filters.")
            else:
                gpp_display_df = gpp_df[gpp_display_cols].sort_values(by="Date", ascending=False)
                st.dataframe(gpp_display_df, use_container_width=True, hide_index=True)

                total_okw = gpp_display_df["OKW Production"].sum()
                total_gpp = gpp_display_df["Total GPP Production"].sum()
                met_okw, met_gpp = st.columns(2)
                met_okw.metric("Total OKW Production", f"{total_okw:,.2f} bbls")
                met_gpp.metric("Total GPP Production", f"{total_gpp:,.2f} bbls")

                csv_bytes = gpp_display_df.to_csv(index=False).encode("utf-8")
                xlsx_buffer = BytesIO()
                with pd.ExcelWriter(xlsx_buffer, engine="xlsxwriter") as writer:
                    gpp_display_df.to_excel(writer, sheet_name="Production", index=False)
                pdf_bytes = _generate_production_pdf(
                    gpp_display_df,
                    loc.name,
                    loc.code,
                    gpp_from or gpp_display_df["Date"].min(),
                    gpp_to or gpp_display_df["Date"].max(),
                )

                exp_csv, exp_xlsx, exp_pdf, exp_view = st.columns(4)
                exp_csv.download_button(
                    "Download CSV",
                    data=csv_bytes,
                    file_name="production.csv",
                    mime="text/csv",
                )
                exp_xlsx.download_button(
                    "Download XLSX",
                    data=xlsx_buffer.getvalue(),
                    file_name="production.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                exp_pdf.download_button(
                    "Download PDF",
                    data=pdf_bytes,
                    file_name="production.pdf",
                    mime="application/pdf",
                )
                if exp_view.button("View PDF"):
                    try:
                        base64_pdf = base64.b64encode(pdf_bytes).decode("utf-8")
                        pdf_html = f"""
                            <script>
                                var pdfWindow = window.open("");
                                pdfWindow.document.write(
                                    '<html><head><title>Production Report</title></head>' +
                                    '<body style="margin:0"><iframe width="100%" height="100%" src="data:application/pdf;base64,{base64_pdf}"></iframe></body></html>'
                                );
                            </script>
                        """
                        components.html(pdf_html, height=0)
                        st.success("PDF opened in new tab!")
                    except Exception as ex:
                        st.error(f"Unable to open PDF: {ex}")

# Make helper utilities accessible to refactored page modules that still call the
# original monolith's module-level helpers without importing them explicitly.
def _register_shared_helpers() -> None:
    shared_names = [
        "_st_safe_rerun",
        "_archive_record_for_delete",
        "_archive_payload_for_delete",
        "_convoy_fallback_closing_from_ops",
        "_convoy_fetch_mb_closing_value",
        "_render_remote_delete_request_ui",
        "_supervisor_dropdown",
        "_deny_edit_for_lock",
        "_available_primary_source_keys",
        "_discover_source_fields",
        "_fetch_source_rows",
        "_format_source_label",
        "_get_model_columns",
        "_get_source_meta",
        "_resolve_source_key",
        "_format_task_timestamp",
        "_ensure_date_key_in_bounds",
        "_bounded_number_input",
        "_coerce_operation_for_db",
        "_derive_filter_bounds",
        "_next_tanker_serial",
        "_normalize_operation",
        "_temperature_input",
        "_current_user_audit_context",
        "_is_condensate_tx",
        "_observed_value_bounds",
        "_persist_toa_from_current_inputs",
        "_load_yade_tracking_rows",
        "_resolve_yade_tracking_locations",
        "render_reports_page",
    ]
    for name in shared_names:
        func = globals().get(name)
        if func:
            setattr(builtins, name, func)


_register_shared_helpers()

def _render_home_page():
    header("Home")
    st.markdown("### Oil Terminal Management System")
    st.info("Use the sidebar to navigate. Some pages are under refactor.")

if page == "Home":
    from pages.home import render as render_home
    render_home()
    st.stop()
elif page == "Manage Locations":
    # Render the 'Manage Locations' page via its dedicated module
    from pages.manage_locations import render as render_manage_locations
    render_manage_locations()
    st.stop()
elif page == "Manage Users":
    # Render the 'Manage Users' page via its dedicated module
    from pages.manage_users import render as render_manage_users
    render_manage_users()
    st.stop()
elif page == "Tank Transactions":
    # Render the 'Tank Transactions' page via its dedicated module
    from pages.tank_transactions import render as render_tank_transactions
    render_tank_transactions()
    st.stop()
elif page == "Yade Transactions":
    # Render the 'Yade Transactions' page via its dedicated module
    from pages.yade_transactions import render as render_yade_transactions
    render_yade_transactions()
    st.stop()
elif page == "Yade Tracking":
    # Render the 'Yade Tracking' page via its dedicated module
    from pages.yade_tracking import render as render_yade_tracking
    render_yade_tracking()
    st.stop()
elif page == "Tanker Transactions":
    # Render the 'Tanker Transactions' page via its dedicated module
    from pages.tanker_transactions import render as render_tanker_transactions
    render_tanker_transactions()
    st.stop()
elif page == "TOA-Yade":
    # Render the 'TOA-Yade' page via its dedicated module
    from pages.toa_yade import render as render_toa_yade
    render_toa_yade()
    st.stop()
elif page == "View Transactions":
    # Render the 'View Transactions' page via its dedicated module
    from pages.view_transactions import render as render_view_transactions
    render_view_transactions()
    st.stop()
elif page == "OTR":
    # Render the 'OTR' page via its dedicated module
    from pages.otr import render as render_otr
    render_otr()
    st.stop()
elif page == "BCCR":
    # Render the 'BCCR' page via its dedicated module
    from pages.bccr import render as render_bccr
    render_bccr()
    st.stop()
elif page == "Material Balance":
    # Render the 'Material Balance' page via its dedicated module
    from pages.material_balance import render as render_material_balance
    render_material_balance()
    st.stop()
elif page == "Add Asset":
    # Render the 'Add Asset' page via its dedicated module
    from pages.add_asset import render as render_add_asset
    render_add_asset()
    st.stop()
elif page == "Location Settings":
    # Render the 'Location Settings' page via its dedicated module
    from pages.location_settings import render as render_location_settings
    render_location_settings()
    st.stop()
elif page == "Recycle Bin":
    # Render the 'Recycle Bin' page via its dedicated module
    from pages.recycle_bin import render as render_recycle_bin
    render_recycle_bin()
    st.stop()
elif page == "Audit Log":
    # Render the 'Audit Log' page via its dedicated module
    from pages.audit_log import render as render_audit_log
    render_audit_log()
    st.stop()
elif page == "Backup & Recovery":
    # Render the 'Backup & Recovery' page via its dedicated module
    from pages.backup__recovery import render as render_backup__recovery
    render_backup__recovery()
    st.stop()
elif page == "2FA Verify":
    # Render the '2FA Verify' page via its dedicated module
    from pages.page_2fa_verify_2 import render as render_page_2fa_verify_2
    render_page_2fa_verify_2()
    st.stop()
elif page == "My Tasks":
    # Render the 'My Tasks' page via its dedicated module
    from pages.my_tasks import render as render_my_tasks
    render_my_tasks()
    st.stop()
elif page == "2FA Settings":
    # Render the '2FA Settings' page via its dedicated module
    from pages.page_2fa_settings import render as render_page_2fa_settings
    render_page_2fa_settings()
    st.stop()
elif page == "Login History":
    # Render the 'Login History' page via its dedicated module
    from pages.login_history import render as render_login_history
    render_login_history()
    st.stop()
elif page == "OTR-Vessel":
    # Render the 'OTR-Vessel' page via its dedicated module
    from pages.otr_vessel import render as render_otr_vessel
    render_otr_vessel()
    st.stop()
elif page == "Convoy Status":
    # Render the 'Convoy Status' page via its dedicated module
    from pages.convoy_status import render as render_convoy_status
    render_convoy_status()
    st.stop()
elif page == "Reporting":
    render_reports_page()
    st.stop()
elif page == "Yade-Vessel Mapping":
    # Render the 'Yade-Vessel Mapping' page via its dedicated module
    from pages.yade_vessel_mapping import render as render_yade_vessel_mapping
    render_yade_vessel_mapping()
    st.stop()
elif page == "FSO-Operations":
    # Render the 'FSO-Operations' page via its dedicated module
    from pages.fso_operations import render as render_fso_operations
    render_fso_operations()
    st.stop()
