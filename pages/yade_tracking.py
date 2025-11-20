"""
Auto-generated module for the 'Yade Tracking' page.
"""
from __future__ import annotations
import streamlit as st
import streamlit as st
import pandas as pd
from datetime import datetime, date, time, timedelta
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
from uuid import uuid4
import hashlib
import html
from timezone_utils import format_local_datetime, get_local_time
from recycle_bin import RecycleBinManager
import reportlab
import importlib
from models import Location
from ui import header
from db import get_session
from pages.helpers import st_safe_rerun, archive_payload_for_delete

def render() -> None:
        header("Yade Tracking")
        
        # Check Admin-IT access restriction
        if st.session_state.get("auth_user", {}).get("role") == "admin-it":
            st.error("🚫 Access Denied: Admin-IT users do not have access to operational pages.")
            st.stop()
        
        try:
            _user_role = st.session_state.get("auth_user", {}).get("role")
            _loc_id = st.session_state.get("active_location_id")
            if _user_role not in ["admin-operations", "manager"] and _loc_id:
                from location_config import LocationConfig
                with get_session() as _s:
                    _cfg = LocationConfig.get_config(_s, _loc_id)
                if _cfg.get("page_access", {}).get("Yade Tracking") is False:
                    st.caption("Yade Tracking Access: **? Denied**")
                    st.stop()
        except Exception:
            pass
        st.markdown("#### Jetty Departure ? Agge Arrival tracker")
        st.caption("Live comparison of YADE voyages captured at Asemoku Jetty, Ndoni, and Agge.")
    
        active_location_id = st.session_state.get("active_location_id")
        if not active_location_id:
            st.error("⚠️ No active location selected. Please pick a location on the Home page.")
            st.stop()
    
        user = st.session_state.get("auth_user")
        if user:
            from auth import AuthManager
    
            if not AuthManager.can_access_location(user, active_location_id):
                st.error("🚫 You do not have access to this location.")
                st.stop()
        else:
            user = {}
        user_role = (user or {}).get("role", "operator")
    
        from permission_manager import PermissionManager
    
        can_view_tracking = False
        allowed_tracking_locations: List[str] = []
        tracking_meta: Dict[str, Optional[Dict[str, Any]]] = {}
        jetty_rows: List[Dict[str, Any]] = []
        agge_rows: List[Dict[str, Any]] = []
        missing_targets: List[str] = []
        location_name = ""
        location_code = ""
        jetty_keys: List[str] = []
        agge_keys: List[str] = []
    
        with get_session() as s:
            from location_manager import LocationManager
    
            loc = LocationManager.get_location_by_id(s, active_location_id)
            if not loc:
                st.error("? Location not found.")
                st.stop()
    
            location_name = loc.name
            location_code = loc.code
            st.info(f"📍 **Active Location:** {loc.name} ({loc.code})")
    
            can_view_tracking = PermissionManager.can_access_feature(
                s, active_location_id, "yade_transactions", user_role
            )
    
            if not can_view_tracking:
                allowed_tracking_locations = PermissionManager.get_allowed_locations_for_feature(
                    s, "yade_transactions"
                )
            else:
                tracking_meta = _resolve_yade_tracking_locations(s)
                missing_targets = [
                    meta["label"]
                    for key, meta in _YADE_TRACKING_TARGETS.items()
                    if not tracking_meta.get(key)
                ]
                jetty_keys = [key for key in ("ASEMOKU", "NDONI") if tracking_meta.get(key)]
                jetty_ids = [tracking_meta[key]["id"] for key in jetty_keys]
                agge_keys = ["AGGE"] if tracking_meta.get("AGGE") else []
                agge_ids = [tracking_meta["AGGE"]["id"]] if agge_keys else []
                jetty_rows = _load_yade_tracking_rows(s, jetty_ids)
                agge_rows = _load_yade_tracking_rows(s, agge_ids)
    
        if not can_view_tracking:
            st.error("🚫 **Access Denied**")
            st.warning(f"**Yade Tracking** is not available at **{location_name or 'this location'}**")
            if allowed_tracking_locations:
                st.info(f"? Available at: **{', '.join(allowed_tracking_locations)}**")
            st.markdown("---")
            st.caption(f"Current Location: **{location_name} ({location_code})**")
            st.caption("Yade Tracking Access: **? Denied**")
            st.stop()
    
        if missing_targets:
            st.warning(
                "Locations missing from the database: "
                + ", ".join(missing_targets)
                + ". Their YADE voyages will not appear until the locations are configured."
            )
    
        def _source_labels(keys: List[str]) -> List[str]:
            labels: List[str] = []
            for key in keys:
                meta = tracking_meta.get(key)
                if meta:
                    labels.append(meta.get("name") or meta.get("code") or _YADE_TRACKING_TARGETS[key]["label"])
            return labels
    
        def _render_tracking_table(
            title: str,
            rows: List[Dict[str, Any]],
            key_prefix: str,
            keys: List[str],
            rename_map: Optional[Dict[str, str]] = None,
        ):
            st.markdown(f"### {title}")
            sources = _source_labels(keys)
            if sources:
                st.caption("Data sources: " + ", ".join(sources))
            if not rows:
                st.info("No YADE voyages captured yet.")
                return
    
            df = pd.DataFrame(rows)
            if df.empty:
                st.info("No YADE voyages captured yet.")
                return
    
            def _fmt_date_cell(val):
                if isinstance(val, (datetime, date)):
                    return val.strftime("%Y-%m-%d")
                if not val:
                    return ""
                try:
                    return pd.to_datetime(val).date().strftime("%Y-%m-%d")
                except Exception:
                    return str(val)
    
            df["_Date"] = df["Date"].apply(_fmt_date_cell)
            df["_Convoy"] = df["Convoy No"].fillna("").astype(str)
            df["_Yade"] = df["Yade No"].fillna("").astype(str)
            df["_Berth"] = df["Loading berth"].fillna("").astype(str)
    
            f_cols = st.columns(4)
            date_opts = sorted([d for d in df["_Date"].unique() if d])
            convoy_opts = sorted([c for c in df["_Convoy"].unique() if c])
            yade_opts = sorted([c for c in df["_Yade"].unique() if c])
            berth_opts = sorted([c for c in df["_Berth"].unique() if c])
    
            selected_dates = f_cols[0].multiselect(
                "Date", options=date_opts, default=[], key=f"{key_prefix}_date"
            )
            selected_convoys = f_cols[1].multiselect(
                "Convoy No", options=convoy_opts, default=[], key=f"{key_prefix}_convoy"
            )
            selected_yades = f_cols[2].multiselect(
                "Yade No", options=yade_opts, default=[], key=f"{key_prefix}_yade"
            )
            selected_berths = f_cols[3].multiselect(
                "Loading berth", options=berth_opts, default=[], key=f"{key_prefix}_berth"
            )
    
            if selected_dates:
                df = df[df["_Date"].isin(selected_dates)]
            if selected_convoys:
                df = df[df["_Convoy"].isin(selected_convoys)]
            if selected_yades:
                df = df[df["_Yade"].isin(selected_yades)]
            if selected_berths:
                df = df[df["_Berth"].isin(selected_berths)]
    
            display_df = df[["Date", "Convoy No", "Yade No", "ROB qty", "TOB qty", "Loading berth"]].copy()
            display_df["Date"] = df["_Date"]
            display_df["Convoy No"] = df["_Convoy"]
            display_df["Yade No"] = df["_Yade"]
            display_df["Loading berth"] = df["_Berth"]
    
            def _fmt_qty(val):
                return f"{val:,.2f}" if val is not None else "�"
    
            display_df["ROB qty"] = display_df["ROB qty"].apply(_fmt_qty)
            display_df["TOB qty"] = display_df["TOB qty"].apply(_fmt_qty)
    
            if rename_map:
                display_df.rename(columns=rename_map, inplace=True)
    
            st.dataframe(
                display_df,
                hide_index=True,
                use_container_width=True,
            )
            st.caption(f"{len(display_df)} voyage(s) shown")
    
        left, right = st.columns(2)
        with left:
            _render_tracking_table("Jetty Departure", jetty_rows, "yt_track_depart", jetty_keys)
        with right:
            _render_tracking_table(
                "Agge Arrival",
                agge_rows,
                "yt_track_agge",
                agge_keys,
                rename_map={"ROB qty": "TOB qty", "TOB qty": "ROB qty"},
            )
    
    # ========================= TANKER TRANSACTIONS PAGE =========================