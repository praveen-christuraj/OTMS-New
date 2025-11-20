"""
Auto-generated module for the 'Home' page.
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
        # ============ CUSTOM CSS ============
        st.markdown("""
            <style>
            .main-header {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 2rem;
                border-radius: 10px;
                color: white;
                text-align: center;
                margin-bottom: 2rem;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            }
            .main-header h1 {
                margin: 0;
                font-size: 2.5rem;
                font-weight: 700;
            }
            .main-header p {
                margin: 0.5rem 0 0 0;
                font-size: 1.2rem;
                opacity: 0.9;
            }
            .stat-card {
                background: white;
                padding: 1rem;                 
                border-radius: 10px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                border-left: 4px solid #667eea;
                transition: transform 0.2s;
                margin-bottom: 0.75rem;
            }
            .stat-card:hover {
                transform: translateY(-5px);
                box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            }
            .stat-card.stat-card--compact { padding: 0.75rem; border-left-width: 3px; }
            .stat-card.stat-card--compact .stat-value { font-size: 1.2rem; }
            .stat-card.stat-card--compact .stat-label { font-size: 0.75rem; }
            .psc-card {
                min-height: 90px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
            }
            .stat-label--nowrap { white-space: nowrap; }
            .stat-label--wrap { white-space: normal; word-break: break-word; }
            .mb-note { font-size: 0.7rem; color: #888; }
            .stat-card.stat-card--mini { padding: 0.6rem; border-left-width: 3px; }
            .mb-note.mb-note--avg { font-size: 0.85rem; font-weight: 700; }
            .stat-value {
                font-size: 1.6rem;             
                font-weight: bold;
                color: #667eea;
                margin: 0.5rem 0;
            }
            .stat-label {
                color: #666;
                font-size: 0.8rem;
                font-weight: bold;             
                text-transform: uppercase;
                letter-spacing: 1px;
            }
            .stat-card--brown { border-left-color: #8b5a2b; }
            .stat-card--brown .stat-value,
            .stat-card--brown .stat-label { color: #8b5a2b; }
            .stat-card--orange { border-left-color: #d97706; }
            .stat-card--orange .stat-value,
            .stat-card--orange .stat-label { color: #d97706; }
            .stat-card--jv { border-left-color: #064e3b; }
            .stat-card--jv .stat-value,
            .stat-card--jv .stat-label { color: #064e3b; }
            .total-card { min-height: 230px; }
            .total-title {
                font-size: 1.2rem;
                font-weight: 700;
                letter-spacing: 2px;
            }
    
            .tank-card {
                background: linear-gradient(to bottom, #f8f9fa 0%, #e9ecef 100%);
                border-radius: 15px;
                padding: 1.5rem;
                box-shadow: 0 4px 12px rgba(0,0,0,0.1);
                margin-bottom: 1.5rem;
                transition: all 0.3s;
            }
            .tank-card:hover {
                transform: scale(1.02);
                box-shadow: 0 6px 20px rgba(0,0,0,0.15);
            }
            .tank-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 1rem;
                padding-bottom: 0.5rem;
                border-bottom: 2px solid #dee2e6;
            }
            .tank-name {
                font-size: 1.3rem;
                font-weight: bold;
                color: #2c3e50;
            }
            .tank-status {
                padding: 0.3rem 0.8rem;
                border-radius: 20px;
                font-size: 0.85rem;
                font-weight: 600;
            }
            .tank-info-row {
                display: flex;
                justify-content: space-between;
                margin-bottom: 0.5rem;
            }
            .tank-info-label {
                color: #6c757d;
                font-size: 0.9rem;
            }
            .tank-info-value {
                font-weight: bold;
            }
            .tank-visual {
                position: relative;
                width: 100%;
                height: 200px;
                background: linear-gradient(to bottom, #e9ecef 0%, #dee2e6 100%);
                border-radius: 10px;
                overflow: hidden;
                border: 3px solid #adb5bd;
                margin: 1rem 0;
            }
            .tank-fill {
                position: absolute;
                bottom: 0;
                width: 100%;
                transition: height 0.5s ease;
            }
            .tank-percentage {
                position: absolute;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                color: white;
                font-weight: bold;
                font-size: 1.5rem;
                text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            }
            .tank-stock-badge {
                position: absolute;
                top: 10px;
                right: 10px;
                background: rgba(255,255,255,0.9);
                padding: 0.3rem 0.6rem;
                border-radius: 5px;
                font-size: 0.8rem;
                font-weight: bold;
            }
            .tank-footer {
                margin-top: 1rem;
                padding-top: 1rem;
                border-top: 1px solid #dee2e6;
                display: flex;
                justify-content: space-between;
                font-size: 0.85rem;
                color: #6c757d;
            }
            .activity-item {
                padding: 1rem;
                border-left: 3px solid #667eea;
                margin-bottom: 1rem;
                background: #f8f9fa;
                border-radius: 5px;
            }
            </style>
        """, unsafe_allow_html=True)
        
        def _coerce_time(value) -> dt_time | None:
            """Normalize stored time inputs (strings/datetime) into datetime.time."""
            if isinstance(value, dt_time):
                return value
            if isinstance(value, datetime):
                return value.time()
            if isinstance(value, str):
                txt = value.strip()
                if not txt:
                    return None
                parts = txt.split(":")
                try:
                    hour = int(parts[0])
                    minute = int(parts[1]) if len(parts) > 1 else 0
                    second = int(parts[2]) if len(parts) > 2 else 0
                    return dt_time(hour, minute, second)
                except Exception:
                    return None
            return None
    
        # ============ GET LOCATION AND USER ============
        active_location_id = st.session_state.get("active_location_id")
        user = st.session_state.get("auth_user")
        
        if not user:
            st.error("🚫 User not authenticated")
            st.stop()
        
        # ============ ADMIN-IT SPECIAL HOME PAGE ============
        if user.get("role") == "admin-it":
            st.markdown("""
            <div style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 2rem; border-radius: 10px; margin-bottom: 2rem;'>
                <h1 style='color: white; margin: 0;'>🔧 System Administration</h1>
                <p style='color: rgba(255,255,255,0.9); margin: 0.5rem 0 0 0;'>Admin-IT Dashboard</p>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown(f"### Welcome, {user.get('full_name') or user.get('username')}!")
            st.caption("You have system administration access")
            
            st.markdown("---")
            st.markdown("### 🔐 Your Access")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("""
                **? You Can Access:**
                - 👥 Manage Users
                - 📜 Audit Log
                - 🕒 Login History
                - 💾 Backup & Recovery
                - ✅ My Tasks (Password Reset Requests)
                - 🔐 2FA Settings
                """)
            
            with col2:
                st.markdown("""
                **? Restricted Access:**
                - All Operational Pages (Tank, Yade, Tanker Transactions)
                - All Reports (OTR, BCCR, Material Balance)
                - Location-specific Operations
                
                *Admin-IT is for system administration only*
                """)
            
            st.markdown("---")
            st.markdown("### 📌 Quick Links")
            
            quick_col1, quick_col2, quick_col3 = st.columns(3)
            
            with quick_col1:
                if st.button("👥 Manage Users", use_container_width=True):
                    st.session_state.page = "Manage Users"
                    st.rerun()
            
            with quick_col2:
                if st.button("📜 View Audit Log", use_container_width=True):
                    st.session_state.page = "Audit Log"
                    st.rerun()
            
            with quick_col3:
                if st.button("✅ My Tasks", use_container_width=True):
                    st.session_state.page = "My Tasks"
                    st.rerun()
            
            st.stop()
        
        # ============ NORMAL OPERATIONAL DASHBOARD FOR OTHER ROLES ============
        if not active_location_id:
            st.warning("⚠️ Please select a location after login")
            st.stop()
        
        # ============ GET LOCATION DETAILS ============
        is_agge_location = False
        with get_session() as s:
            from location_manager import LocationManager
            from permission_manager import PermissionManager
            
            loc = LocationManager.get_location_by_id(s, active_location_id)
            if not loc:
                st.error("? Location not found.")
                st.stop()
            
            def _dash_canon(txt: str) -> str:
                return str(txt or "").upper().replace(" ", "").replace("-", "")
            
            loc_code_value = getattr(loc, "code", "") or ""
            loc_name_value = getattr(loc, "name", "") or ""
            dash_tokens = {_dash_canon(loc_code_value), _dash_canon(loc_name_value)}
            is_agge_location = bool(dash_tokens & {"AGGE"})
            
            # Check permissions
            can_view_tanks = PermissionManager.can_access_feature(s, active_location_id, "tank_transactions", user["role"])
            can_view_yade = PermissionManager.can_access_feature(s, active_location_id, "yade_transactions", user["role"])
            can_view_tanker = PermissionManager.can_access_feature(s, active_location_id, "tanker_transactions", user["role"])
        
        # ============ HEADER ============
    
        # Decide dashboard title based on location
        def _canon_loc(text: str) -> str:
            return str(text or "").upper().replace(" ", "").replace("-", "")
    
        loc_name_safe = getattr(loc, "name", "") or ""
        loc_code_safe = getattr(loc, "code", "") or ""
    
        _name_c = _canon_loc(loc_name_safe)
        _code_c = _canon_loc(loc_code_safe)
    
        # Lagos ? special title, others ? "<Location> Dashboard"
        is_lagos_location = ("LAGOS" in _name_c) or ("LAGOS" in _code_c)
        header_title = (
            "CRUDE OPERATIONS DASHBOARD"
            if is_lagos_location
            else f"{loc_name_safe or loc_code_safe or 'Location'} Dashboard"
        )
    
        st.markdown(
            f"""
            <div class="main-header">
                <h1>{header_title}</h1>
                <p>MANAGEMENT INFORMATION SYSTEM</p>
                <p>Welcome back, <strong>{user['username']}</strong> | {datetime.now().strftime('%A, %B %d, %Y - %I:%M %p')}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if is_lagos_location:
            from models import Location, OFSProductionEvacuationRecord
            from material_balance_calculator import MaterialBalanceCalculator as MBC
            from datetime import date as _date
    
            # Reuse the global dashboard date instead of creating a new widget
            dp_date = st.session_state.get("dash_date_all_sites", _date.today())
            tabs = st.tabs(["Production & Evacuation Summary", "Stock Positions"])
    
            # ---------- Helper functions (PSC labels, MB helpers, FSO helper) ----------
            def _psc_label_html(text: str) -> str:
                tokens = [t for t in str(text).split() if t]
                wrap_cls = "stat-label--nowrap" if len(tokens) <= 1 else "stat-label--wrap"
                return f'<div class="stat-label {wrap_cls}">{html.escape(text)}</div>'
    
            def _psc_card_class(text: str) -> str:
                canon = str(text or "").strip().upper()
                if canon in {"AGGU", "NDONI RECEIPT", "NDONI EVACUATION"}:
                    return "stat-card--brown"
                if canon in {"OGUALI", "UKPICHI"}:
                    return "stat-card--orange"
                return ""
    
            def _mb_sum_range(loc_entry: dict | None, start_date, end_date, candidates: list[str]) -> float:
                if not loc_entry:
                    return 0.0
                try:
                    rows = MBC.calculate_material_balance(
                        None,
                        (loc_entry["code"] or "").upper(),
                        start_date,
                        end_date,
                        location_id=loc_entry["id"],
                        debug=False,
                    ) or []
                    df = pd.DataFrame(rows)
                    if df.empty:
                        return 0.0
                    lower_map = {str(col).strip().lower(): col for col in df.columns}
                    target_col = None
                    for cand in candidates:
                        if cand in df.columns:
                            target_col = cand
                            break
                        cand_lower = str(cand).strip().lower()
                        if cand_lower in lower_map:
                            target_col = lower_map[cand_lower]
                            break
                    if not target_col:
                        return 0.0
                    return float(pd.to_numeric(df[target_col], errors="coerce").fillna(0.0).sum())
                except Exception:
                    return 0.0
    
            def _fso_receipt_sum(loc_entry: dict | None, start_date, end_date, vessel_name: str) -> float:
                if not loc_entry:
                    return 0.0
                total = 0.0
                try:
                    from models import FSOOperation
                    with get_session() as s_fso:
                        rows = (
                            s_fso.query(FSOOperation)
                            .filter(
                                FSOOperation.location_id == loc_entry["id"],
                                FSOOperation.fso_vessel == vessel_name,
                                FSOOperation.date >= start_date,
                                FSOOperation.date <= end_date,
                            )
                            .all()
                        )
                    for r in rows:
                        op_text = str(getattr(r, "operation", "") or "").strip().lower()
                        if op_text.startswith("receipt"):
                            total += float(getattr(r, "net_receipt_dispatch", 0.0) or 0.0)
                except Exception:
                    pass
                return total
    
            # ---------- Resolve locations & compute DAILY PSC volumes ----------
            with get_session() as s:
                locations = s.query(Location).order_by(Location.name).all()
    
                def _canon_token(v):
                    return str(v or "").upper().replace(" ", "").replace("-", "")
    
                def _resolve(token_set):
                    targets = {_canon_token(t) for t in token_set}
                    for L in locations:
                        if {_canon_token(L.code), _canon_token(L.name)} & targets:
                            return {"id": L.id, "code": L.code or "", "name": L.name or ""}
                    return None
    
                loc_aggu = _resolve({"AGGU"})
                loc_asemoku = _resolve({"JETTY", "ASEMOKU", "ASEMOKUJETTY"})
                loc_bfs = _resolve({"BFS", "BENEKU"})
                loc_oguali = _resolve({"OGUALI", "OML157", "OGUALIOML157"})
                loc_utapate = _resolve({"UTAPATE", "OML13", "OML-13"})
                loc_ogini = _resolve({"OGINI", "UMUOGINI", "OML26", "OML-26"})
                loc_ndoni = _resolve({"NDONI"})
    
                # Aggu (receipt) via MB
                aggu_val = 0.0
                try:
                    if loc_aggu:
                        rows = MBC.calculate_material_balance(
                            None,
                            (loc_aggu["code"] or "").upper(),
                            dp_date,
                            dp_date,
                            location_id=loc_aggu["id"],
                            debug=False,
                        ) or []
                        df = pd.DataFrame(rows)
                        col = "Receipt" if "Receipt" in df.columns else ("Receipts" if "Receipts" in df.columns else None)
                        if col and not df.empty:
                            aggu_val = float(pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum())
                except Exception:
                    pass
    
                # ANZ via MB
                anz_val = 0.0
                try:
                    if loc_asemoku:
                        rows = MBC.calculate_material_balance(
                            None,
                            (loc_asemoku["code"] or "").upper(),
                            dp_date,
                            dp_date,
                            location_id=loc_asemoku["id"],
                            debug=False,
                        ) or []
                        df = pd.DataFrame(rows)
                        col = "ANZ Receipt" if "ANZ Receipt" in df.columns else None
                        if col and not df.empty:
                            anz_val = float(pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum())
                except Exception:
                    pass
    
                # GPP & OKW via GPPProductionRecord loader
                gpp_val = 0.0
                okw_val = 0.0
                try:
                    if loc_bfs:
                        recs = load_gpp_production_records(loc_bfs["id"], limit=2000)
                        df = pd.DataFrame(recs)
                        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
                        df = df[df["Date"] == dp_date]
                        if not df.empty:
                            gpp_val = float(pd.to_numeric(df["Total GPP Production"], errors="coerce").fillna(0.0).sum())
                            okw_val = float(pd.to_numeric(df["OKW Production"], errors="coerce").fillna(0.0).sum())
                except Exception:
                    pass
    
                # Oguali & Ukpichi via OFSProductionEvacuationRecord
                oguali_val = 0.0
                ukpichi_val = 0.0
                try:
                    if loc_oguali:
                        rows = (
                            s.query(OFSProductionEvacuationRecord)
                            .filter(
                                OFSProductionEvacuationRecord.location_id == loc_oguali["id"],
                                OFSProductionEvacuationRecord.date == dp_date,
                            )
                            .all()
                        )
                        oguali_val = float(
                            sum([float(getattr(r, "oguali_production", 0.0) or 0.0) for r in rows])
                        )
                        ukpichi_val = float(
                            sum(
                                [
                                    float(getattr(r, "ukpichi_production", 0.0) or 0.0)
                                    + float(getattr(r, "other_locations", 0.0) or 0.0)
                                    for r in rows
                                ]
                            )
                        )
                except Exception:
                    pass
    
            # ---------- Helper for Stock Positions tab ----------
            def _mb_value_for_column(loc_entry: dict | None, the_date, candidates: list[str]) -> float:
                if not loc_entry:
                    return 0.0
                try:
                    rows = MBC.calculate_material_balance(
                        None,
                        (loc_entry["code"] or "").upper(),
                        the_date,
                        the_date,
                        location_id=loc_entry["id"],
                        debug=False,
                    ) or []
                    df = pd.DataFrame(rows)
                    if df.empty:
                        return 0.0
                    lower_map = {str(col).strip().lower(): col for col in df.columns}
                    target_col = None
                    for cand in candidates:
                        if cand in df.columns:
                            target_col = cand
                            break
                        cand_lower = str(cand).strip().lower()
                        if cand_lower in lower_map:
                            target_col = lower_map[cand_lower]
                            break
                    if not target_col:
                        return 0.0
                    return float(pd.to_numeric(df[target_col], errors="coerce").fillna(0.0).sum())
                except Exception:
                    return 0.0
    
            # =====================================================================
            # TAB 0: PRODUCTION & EVACUATION SUMMARY (ALL CARDS STAY HERE)
            # =====================================================================
            with tabs[0]:
                st.subheader("Summary Statistics")
    
                labels = [
                    ("Aggu", aggu_val),
                    ("Anieze & Enyie", anz_val),
                    ("GPP", gpp_val),
                    ("Oguali", oguali_val),
                    ("Okwuibome", okw_val),
                    ("Ukpichi", ukpichi_val),
                ]
    
                total_psc = float(
                    (aggu_val or 0.0)
                    + (anz_val or 0.0)
                    + (gpp_val or 0.0)
                    + (oguali_val or 0.0)
                    + (okw_val or 0.0)
                    + (ukpichi_val or 0.0)
                )
    
                st.markdown("### DAILY PRODUCTION & EVACUATION")
                st.markdown("#### PSC Block")
    
                grid_left, total_right = st.columns([0.8, 0.2])
                dp_items = [
                    ("Anieze & Enyie", anz_val),
                    ("Okwuibome", okw_val),
                    ("GPP", gpp_val),
                    ("Aggu", aggu_val),
                    ("Oguali", oguali_val),
                    ("Ukpichi", ukpichi_val),
                ]
                dp_cols_all = grid_left.columns(6)
                for i, (lbl, val) in enumerate(dp_items):
                    try:
                        vtxt = f"{float(val or 0.0):,.0f} bbls"
                    except Exception:
                        vtxt = "-"
                    label_html = _psc_label_html(lbl)
                    extra_cls = _psc_card_class(lbl)
                    html_card = f"""
                    <div class="stat-card stat-card--compact psc-card {extra_cls}">
                        {label_html}
                        <div class="stat-value">{html.escape(vtxt)}</div>
                        <div class="mb-note">{pd.to_datetime(dp_date).strftime('%d-%b-%Y')}</div>
                    </div>
                    """
                    dp_cols_all[i].markdown(html_card, unsafe_allow_html=True)
    
                # ---------- Pre-compute Ndoni / Jetty / Tanvi for DAILY cards ----------
                with get_session() as s2:
                    locations = s2.query(Location).order_by(Location.name).all()
    
                    def _canon_token(v):
                        return str(v or "").upper().replace(" ", "").replace("-", "")
    
                    def _resolve(token_set):
                        targets = {_canon_token(t) for t in token_set}
                        for L in locations:
                            if {_canon_token(L.code), _canon_token(L.name)} & targets:
                                return {"id": L.id, "code": L.code or "", "name": L.name or ""}
                        return None
    
                    loc_ndoni2 = _resolve({"NDONI"})
                    loc_jetty2 = _resolve({"JETTY", "ASEMOKU", "ASEMOKUJETTY"})
                    loc_agge = _resolve({"AGGE"})
    
                    ndoni_receipt = 0.0
                    try:
                        if loc_ndoni2:
                            rows = MBC.calculate_material_balance(
                                None,
                                (loc_ndoni2["code"] or "").upper(),
                                dp_date,
                                dp_date,
                                location_id=loc_ndoni2["id"],
                                debug=False,
                            ) or []
                            df = pd.DataFrame(rows)
                            cands = ["Receipt from Agu", "Receipt from OFS", "Other Receipts"]
                            for c in cands:
                                if c in df.columns:
                                    ndoni_receipt += float(
                                        pd.to_numeric(df[c], errors="coerce").fillna(0.0).sum()
                                    )
                                else:
                                    lower_map = {str(col).strip().lower(): col for col in df.columns}
                                    key = c.strip().lower()
                                    if key in lower_map:
                                        col = lower_map[key]
                                        ndoni_receipt += float(
                                            pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum()
                                        )
                    except Exception:
                        pass
    
                    ndoni_evac = 0.0
                    try:
                        if loc_ndoni2:
                            rows = MBC.calculate_material_balance(
                                None,
                                (loc_ndoni2["code"] or "").upper(),
                                dp_date,
                                dp_date,
                                location_id=loc_ndoni2["id"],
                                debug=False,
                            ) or []
                            df = pd.DataFrame(rows)
                            cand = "Dispatch to barge"
                            col = cand if cand in df.columns else None
                            if not col:
                                lower_map = {str(col2).strip().lower(): col2 for col2 in df.columns}
                                if cand.lower() in lower_map:
                                    col = lower_map[cand.lower()]
                            if col and not df.empty:
                                ndoni_evac = float(
                                    pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum()
                                )
                    except Exception:
                        pass
    
                    jetty_evac = 0.0
                    try:
                        if loc_jetty2:
                            rows = MBC.calculate_material_balance(
                                None,
                                (loc_jetty2["code"] or "").upper(),
                                dp_date,
                                dp_date,
                                location_id=loc_jetty2["id"],
                                debug=False,
                            ) or []
                            df = pd.DataFrame(rows)
                            cand = "Dispatch to barge"
                            col = cand if cand in df.columns else None
                            if not col:
                                lower_map = {str(col2).strip().lower(): col2 for col2 in df.columns}
                                if cand.lower() in lower_map:
                                    col = lower_map[cand.lower()]
                            if col and not df.empty:
                                jetty_evac = float(
                                    pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum()
                                )
                    except Exception:
                        pass
    
                    tanvi_receipt = 0.0
                    try:
                        from models import FSOOperation
    
                        if loc_agge:
                            rows = (
                                s2.query(FSOOperation)
                                .filter(
                                    FSOOperation.location_id == loc_agge["id"],
                                    FSOOperation.fso_vessel == "MT TULJA TANVI",
                                    FSOOperation.date == dp_date,
                                )
                                .all()
                            )
                            for r in rows:
                                op_text = str(getattr(r, "operation", "") or "").strip().lower()
                                if op_text.startswith("receipt"):
                                    try:
                                        tanvi_receipt += float(
                                            getattr(r, "net_receipt_dispatch", 0.0) or 0.0
                                        )
                                    except Exception:
                                        pass
                    except Exception:
                        pass
    
                # ---------- DAILY Ndoni / Jetty / Tanvi cards + TOTAL ----------
                rec_cols = grid_left.columns(4)
                rec_items = [
                    ("NDONI RECEIPT", ndoni_receipt),
                    ("NDONI EVACUATION", ndoni_evac),
                    ("JETTY EVACUATION", jetty_evac),
                    ("FSO TANVI RECEIPT", tanvi_receipt),
                ]
                for i, (lbl, val) in enumerate(rec_items):
                    try:
                        vtxt = f"{float(val or 0.0):,.0f} bbls"
                    except Exception:
                        vtxt = "-"
                    label_html = _psc_label_html(lbl)
                    extra_cls = _psc_card_class(lbl)
                    html_card = f"""
                    <div class="stat-card stat-card--compact psc-card {extra_cls}">
                        {label_html}
                        <div class="stat-value">{html.escape(vtxt)}</div>
                        <div class="mb-note">{pd.to_datetime(dp_date).strftime('%d-%b-%Y')}</div>
                    </div>
                    """
                    rec_cols[i].markdown(html_card, unsafe_allow_html=True)
    
                # TOTAL card adjacent, spanning two rows: PSC Production + PSC Evacuation
                psc_prod = total_psc
                psc_evac = float((ndoni_evac or 0.0) + (jetty_evac or 0.0))
                total_html = f"""
                <div class="stat-card stat-card--compact total-card">
                    <div class="total-title">TOTAL</div>
                    <div>
                        {_psc_label_html("PSC Production")}
                        <div class="stat-value">{psc_prod:,.0f} bbls</div>
                    </div>
                    <div style="margin-top:0.75rem">
                        {_psc_label_html("PSC Evacuation")}
                        <div class="stat-value">{psc_evac:,.0f} bbls</div>
                    </div>
                </div>
                """
                total_right.markdown(total_html, unsafe_allow_html=True)
    
                # ---------- DAILY JV Block ----------
                oml13_prod = _mb_sum_range(loc_utapate, dp_date, dp_date, ["Receipt", "Receipts"])
                oml13_evac = _mb_sum_range(
                    loc_utapate,
                    dp_date,
                    dp_date,
                    ["Dispatch", "Dispatch to barge", "Dispatch to Barge"],
                )
                oml26_prod = _mb_sum_range(loc_ogini, dp_date, dp_date, ["Receipt", "Receipts"])
                oml26_evac = _mb_sum_range(
                    loc_ogini,
                    dp_date,
                    dp_date,
                    ["Dispatch", "Dispatch to barge", "Dispatch to Barge"],
                )
                kalyani_receipt = _fso_receipt_sum(
                    loc_utapate, dp_date, dp_date, "MT TULJA KALYANI"
                )
                total_jv_volume = float(
                    (oml13_prod or 0.0) + (oml26_prod or 0.0) + (kalyani_receipt or 0.0)
                )
    
                st.markdown("#### JV Block")
                jv_cols = st.columns(6)
                jv_items = [
                    ("OML-13 PRODUCTION", oml13_prod),
                    ("OML-13 EVACUATION", oml13_evac),
                    ("OML-26 PRODUCTION", oml26_prod),
                    ("OML-26 EVACUATION", oml26_evac),
                    ("FSO KALYANI RECEIPT", kalyani_receipt),
                    ("TOTAL JV THROUGHPUT", total_jv_volume),
                ]
                jv_note = pd.to_datetime(dp_date).strftime("%d-%b-%Y")
                for idx, (lbl, val) in enumerate(jv_items):
                    try:
                        vtxt = f"{float(val or 0.0):,.0f} bbls"
                    except Exception:
                        vtxt = "-"
                    label_html = _psc_label_html(lbl)
                    card_html = f"""
                    <div class="stat-card stat-card--compact psc-card stat-card--jv">
                        {label_html}
                        <div class="stat-value">{html.escape(vtxt)}</div>
                        <div class="mb-note">{html.escape(jv_note)}</div>
                    </div>
                    """
                    jv_cols[idx].markdown(card_html, unsafe_allow_html=True)
    
                # ---------- MONTHLY DATA ----------
                st.markdown("##### MONTHLY DATA")
                my_cols = st.columns(2)
                my_from = my_cols[0].date_input(
                    "From",
                    value=date.today().replace(day=1),
                    key=f"lagos_my_from_{active_location_id}",
                )
                my_to = my_cols[1].date_input(
                    "To",
                    value=date.today(),
                    key=f"lagos_my_to_{active_location_id}",
                )
    
                if my_from > my_to:
                    st.error("From date cannot be after To date.")
                else:
                    st.markdown("#### PSC Block")
                    my_grid_left, my_total_right = st.columns([0.8, 0.2])
    
                    with get_session() as s3:
                        locations = s3.query(Location).order_by(Location.name).all()
    
                        def _canon_token(v):
                            return str(v or "").upper().replace(" ", "").replace("-", "")
    
                        def _resolve(token_set):
                            targets = {_canon_token(t) for t in token_set}
                            for L in locations:
                                if {_canon_token(L.code), _canon_token(L.name)} & targets:
                                    return {
                                        "id": L.id,
                                        "code": L.code or "",
                                        "name": L.name or "",
                                    }
                            return None
    
                        loc_aggu_m = _resolve({"AGGU"})
                        loc_asemoku_m = _resolve({"JETTY", "ASEMOKU", "ASEMOKUJETTY"})
                        loc_bfs_m = _resolve({"BFS", "BENEKU"})
                        loc_oguali_m = _resolve({"OGUALI", "OML157", "OGUALIOML157"})
                        loc_ndoni_m = _resolve({"NDONI"})
                        loc_utapate_m = _resolve({"UTAPATE", "OML13", "OML-13"})
                        loc_ogini_m = _resolve({"OGINI", "UMUOGINI", "OML26", "OML-26"})
    
                        n_days = (my_to - my_from).days + 1
    
                        aggu_total = 0.0
                        try:
                            if loc_aggu_m:
                                rows = MBC.calculate_material_balance(
                                    None,
                                    (loc_aggu_m["code"] or "").upper(),
                                    my_from,
                                    my_to,
                                    location_id=loc_aggu_m["id"],
                                    debug=False,
                                ) or []
                                df = pd.DataFrame(rows)
                                col = (
                                    "Receipt"
                                    if "Receipt" in df.columns
                                    else ("Receipts" if "Receipts" in df.columns else None)
                                )
                                if col and not df.empty:
                                    aggu_total = float(
                                        pd.to_numeric(df[col], errors="coerce")
                                        .fillna(0.0)
                                        .sum()
                                    )
                        except Exception:
                            pass
    
                        anz_total = 0.0
                        try:
                            if loc_asemoku_m:
                                rows = MBC.calculate_material_balance(
                                    None,
                                    (loc_asemoku_m["code"] or "").upper(),
                                    my_from,
                                    my_to,
                                    location_id=loc_asemoku_m["id"],
                                    debug=False,
                                ) or []
                                df = pd.DataFrame(rows)
                                col = "ANZ Receipt" if "ANZ Receipt" in df.columns else None
                                if col and not df.empty:
                                    anz_total = float(
                                        pd.to_numeric(df[col], errors="coerce")
                                        .fillna(0.0)
                                        .sum()
                                    )
                        except Exception:
                            pass
    
                        gpp_total = 0.0
                        okw_total = 0.0
                        try:
                            if loc_bfs_m:
                                recs = load_gpp_production_records(loc_bfs_m["id"], limit=5000)
                                df = pd.DataFrame(recs)
                                df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
                                df = df[(df["Date"] >= my_from) & (df["Date"] <= my_to)]
                                if not df.empty:
                                    gpp_total = float(
                                        pd.to_numeric(
                                            df["Total GPP Production"], errors="coerce"
                                        )
                                        .fillna(0.0)
                                        .sum()
                                    )
                                    okw_total = float(
                                        pd.to_numeric(df["OKW Production"], errors="coerce")
                                        .fillna(0.0)
                                        .sum()
                                    )
                        except Exception:
                            pass
    
                        oguali_total = 0.0
                        ukpichi_total = 0.0
                        jetty_evac_total = 0.0
                        ndoni_evac_total = 0.0
                        tanvi_total = 0.0
                        try:
                            if loc_oguali_m:
                                rows = (
                                    s3.query(OFSProductionEvacuationRecord)
                                    .filter(
                                        OFSProductionEvacuationRecord.location_id
                                        == loc_oguali_m["id"],
                                        OFSProductionEvacuationRecord.date >= my_from,
                                        OFSProductionEvacuationRecord.date <= my_to,
                                    )
                                    .all()
                                )
                                oguali_total = float(
                                    sum(
                                        [
                                            float(getattr(r, "oguali_production", 0.0) or 0.0)
                                            for r in rows
                                        ]
                                    )
                                )
                                ukpichi_total = float(
                                    sum(
                                        [
                                            float(getattr(r, "ukpichi_production", 0.0) or 0.0)
                                            + float(getattr(r, "other_locations", 0.0) or 0.0)
                                            for r in rows
                                        ]
                                    )
                                )
                            if loc_asemoku_m:
                                rows = MBC.calculate_material_balance(
                                    None,
                                    (loc_asemoku_m["code"] or "").upper(),
                                    my_from,
                                    my_to,
                                    location_id=loc_asemoku_m["id"],
                                    debug=False,
                                ) or []
                                df = pd.DataFrame(rows)
                                cand = "Dispatch to barge"
                                col = cand if cand in df.columns else None
                                if not col:
                                    lower_map = {
                                        str(col2).strip().lower(): col2 for col2 in df.columns
                                    }
                                    if cand.lower() in lower_map:
                                        col = lower_map[cand.lower()]
                                if col and not df.empty:
                                    jetty_evac_total = float(
                                        pd.to_numeric(df[col], errors="coerce")
                                        .fillna(0.0)
                                        .sum()
                                    )
                            if loc_ndoni_m:
                                rows = MBC.calculate_material_balance(
                                    None,
                                    (loc_ndoni_m["code"] or "").upper(),
                                    my_from,
                                    my_to,
                                    location_id=loc_ndoni_m["id"],
                                    debug=False,
                                ) or []
                                df = pd.DataFrame(rows)
                                cand = "Dispatch to barge"
                                col = cand if cand in df.columns else None
                                if not col:
                                    lower_map = {
                                        str(col2).strip().lower(): col2 for col2 in df.columns
                                    }
                                    if cand.lower() in lower_map:
                                        col = lower_map[cand.lower()]
                                if col and not df.empty:
                                    ndoni_evac_total = float(
                                        pd.to_numeric(df[col], errors="coerce")
                                        .fillna(0.0)
                                        .sum()
                                    )
                            from models import FSOOperation
    
                            loc_agge_m = _resolve({"AGGE"})
                            if loc_agge_m:
                                rows = (
                                    s3.query(FSOOperation)
                                    .filter(
                                        FSOOperation.location_id == loc_agge_m["id"],
                                        FSOOperation.fso_vessel == "MT TULJA TANVI",
                                        FSOOperation.date >= my_from,
                                        FSOOperation.date <= my_to,
                                    )
                                    .all()
                                )
                                for r in rows:
                                    op_text = str(getattr(r, "operation", "") or "").strip().lower()
                                    if op_text.startswith("receipt"):
                                        try:
                                            tanvi_total += float(
                                                getattr(r, "net_receipt_dispatch", 0.0) or 0.0
                                            )
                                        except Exception:
                                            pass
                        except Exception:
                            pass
    
                    jv_oml13_prod = _mb_sum_range(
                        loc_utapate_m, my_from, my_to, ["Receipt", "Receipts"]
                    )
                    jv_oml13_evac = _mb_sum_range(
                        loc_utapate_m,
                        my_from,
                        my_to,
                        ["Dispatch", "Dispatch to barge", "Dispatch to Barge"],
                    )
                    jv_oml26_prod = _mb_sum_range(
                        loc_ogini_m, my_from, my_to, ["Receipt", "Receipts"]
                    )
                    jv_oml26_evac = _mb_sum_range(
                        loc_ogini_m,
                        my_from,
                        my_to,
                        ["Dispatch", "Dispatch to barge", "Dispatch to Barge"],
                    )
                    jv_kalyani_receipt = _fso_receipt_sum(
                        loc_utapate_m, my_from, my_to, "MT TULJA KALYANI"
                    )
                    jv_total_volume = float(
                        (jv_oml13_prod or 0.0)
                        + (jv_oml26_prod or 0.0)
                        + (jv_kalyani_receipt or 0.0)
                    )
                    psc_total = float(
                        sum(
                            [
                                aggu_total,
                                anz_total,
                                gpp_total,
                                oguali_total,
                                okw_total,
                                ukpichi_total,
                            ]
                        )
                    )
    
                    row2 = [
                        (
                            "JETTY EVACUATION",
                            jetty_evac_total,
                            jetty_evac_total / max(n_days, 1),
                        ),
                        (
                            "NDONI EVACUATION",
                            ndoni_evac_total,
                            ndoni_evac_total / max(n_days, 1),
                        ),
                        (
                            "FSO TANVI RECEIPT",
                            tanvi_total,
                            tanvi_total / max(n_days, 1),
                        ),
                    ]
    
                    # Arrange monthly 6 cards + adjacent TOTAL
                    row1_labels = [
                        ("Anieze & Enyie", anz_total, anz_total / max(n_days, 1)),
                        ("Okwuibome", okw_total, okw_total / max(n_days, 1)),
                        ("GPP", gpp_total, gpp_total / max(n_days, 1)),
                        ("Aggu", aggu_total, aggu_total / max(n_days, 1)),
                        ("Oguali", oguali_total, oguali_total / max(n_days, 1)),
                        ("Ukpichi", ukpichi_total, ukpichi_total / max(n_days, 1)),
                    ]
                    my_row1_cols = my_grid_left.columns(6)
                    for i, (lbl, total_v, avg_v) in enumerate(row1_labels):
                        try:
                            vtxt = f"{float(total_v or 0.0):,.0f} bbls"
                            atxt = f"Avg: {float(avg_v or 0.0):,.0f} bbls/day"
                        except Exception:
                            vtxt = "-"
                            atxt = ""
                        label_html = _psc_label_html(lbl)
                        extra_cls = _psc_card_class(lbl)
                        html_card = f"""
                        <div class="stat-card stat-card--mini {extra_cls}">
                            {label_html}
                            <div class="stat-value">{html.escape(vtxt)}</div>
                            <div class="mb-note mb-note--avg">{html.escape(atxt)}</div>
                        </div>
                        """
                        my_row1_cols[i].markdown(html_card, unsafe_allow_html=True)
    
                    my_row2_cols = my_grid_left.columns(3)
                    for i, (lbl, total_v, avg_v) in enumerate(row2):
                        try:
                            vtxt = f"{float(total_v or 0.0):,.0f} bbls"
                            atxt = f"Avg: {float(avg_v or 0.0):,.0f} bbls/day"
                        except Exception:
                            vtxt = "-"
                            atxt = ""
                        label_html = _psc_label_html(lbl)
                        extra_cls = _psc_card_class(lbl)
                        html_card = f"""
                        <div class="stat-card stat-card--mini {extra_cls}">
                            {label_html}
                            <div class="stat-value">{html.escape(vtxt)}</div>
                            <div class="mb-note mb-note--avg">{html.escape(atxt)}</div>
                        </div>
                        """
                        my_row2_cols[i].markdown(html_card, unsafe_allow_html=True)
    
                    # Monthly TOTAL card
                    psc_total = float(
                        sum([aggu_total, anz_total, gpp_total, oguali_total, okw_total, ukpichi_total])
                    )
                    psc_evac_total = float(
                        (jetty_evac_total or 0.0) + (ndoni_evac_total or 0.0)
                    )
                    total_month_html = f"""
                    <div class="stat-card stat-card--mini" style="height: 100%;">
                        {_psc_label_html("TOTAL")}
                        <div>
                            {_psc_label_html("PSC Production")}
                            <div class="stat-value">{psc_total:,.0f} bbls</div>
                            <div class="mb-note mb-note--avg">Avg: {(psc_total/max(n_days,1)):,.0f} bbls/day</div>
                        </div>
                        <div style="margin-top:0.75rem">
                            {_psc_label_html("PSC Evacuation")}
                            <div class="stat-value">{psc_evac_total:,.0f} bbls</div>
                            <div class="mb-note mb-note--avg">Avg: {(psc_evac_total/max(n_days,1)):,.0f} bbls/day</div>
                        </div>
                    </div>
                    """
                    my_total_right.markdown(total_month_html, unsafe_allow_html=True)
    
                    st.markdown("#### JV Block")
                    jv_month_cols = st.columns(6)
                    jv_month_items = [
                        ("OML-13 PRODUCTION", jv_oml13_prod),
                        ("OML-13 EVACUATION", jv_oml13_evac),
                        ("OML-26 PRODUCTION", jv_oml26_prod),
                        ("OML-26 EVACUATION", jv_oml26_evac),
                        ("FSO KALYANI RECEIPT", jv_kalyani_receipt),
                        ("TOTAL JV THROUGHPUT", jv_total_volume),
                    ]
                    for i, (lbl, total_v) in enumerate(jv_month_items):
                        try:
                            vtxt = f"{float(total_v or 0.0):,.0f} bbls"
                            atxt = f"Avg: {float((total_v or 0.0) / max(n_days, 1)):,.0f} bbls/day"
                        except Exception:
                            vtxt = "-"
                            atxt = ""
                        label_html = _psc_label_html(lbl)
                        html_card = f"""
                        <div class="stat-card stat-card--mini stat-card--jv">
                            {label_html}
                            <div class="stat-value">{html.escape(vtxt)}</div>
                            <div class="mb-note mb-note--avg">{html.escape(atxt)}</div>
                        </div>
                        """
                        jv_month_cols[i].markdown(html_card, unsafe_allow_html=True)
    
            # =====================================================================
            # TAB 1: STOCK POSITIONS (DONUT + SIDE DETAILS + FSO CYLINDERS)
            # =====================================================================
            with tabs[1]:
                st.markdown("### Stock Positions")
    
                stock_date = st.date_input(
                    "Select date",
                    value=st.session_state.get("lagos_stock_date", dp_date),
                    key="lagos_stock_date",
                )
    
                aggu_cs = (
                    _mb_value_for_column(
                        loc_aggu, stock_date, ["Closing Stock", "Book Closing Stock"]
                    )
                    if loc_aggu
                    else 0.0
                )
                jetty_cs = (
                    _mb_value_for_column(
                        loc_asemoku, stock_date, ["Closing Stock", "Book Closing Stock"]
                    )
                    if loc_asemoku
                    else 0.0
                )
                beneku_cs = (
                    _mb_value_for_column(
                        loc_bfs, stock_date, ["Closing Stock", "Book Closing Stock"]
                    )
                    if loc_bfs
                    else 0.0
                )
                ndoni_cs = (
                    _mb_value_for_column(
                        loc_ndoni, stock_date, ["Closing Stock", "Book Closing Stock"]
                    )
                    if loc_ndoni
                    else 0.0
                )
                ogini_cs = (
                    _mb_value_for_column(
                        loc_ogini, stock_date, ["Closing Stock", "Book Closing Stock"]
                    )
                    if loc_ogini
                    else 0.0
                )
                utapate_cs = (
                    _mb_value_for_column(
                        loc_utapate, stock_date, ["Closing Stock", "Book Closing Stock"]
                    )
                    if loc_utapate
                    else 0.0
                )
    
                # GPP closing stock from GPP records
                gpp_cs = 0.0
                try:
                    if loc_bfs:
                        recs = load_gpp_production_records(loc_bfs["id"], limit=2000)
                        df_gpp = pd.DataFrame(recs)
                        df_gpp["Date"] = pd.to_datetime(
                            df_gpp["Date"], errors="coerce"
                        ).dt.date
                        df_gpp = df_gpp[df_gpp["Date"] == stock_date]
                        if not df_gpp.empty:
                            gpp_cs = float(
                                pd.to_numeric(
                                    df_gpp["GPP Closing Stock"], errors="coerce"
                                )
                                .fillna(0.0)
                                .sum()
                            )
                except Exception:
                    gpp_cs = 0.0
    
                labels = [
                    "Aggu",
                    "Asemoku Jetty",
                    "Beneku",
                    "GPP",
                    "Ndoni",
                    "Ogini",
                    "Utapate",
                ]
                values = [
                    aggu_cs,
                    jetty_cs,
                    beneku_cs,
                    gpp_cs,
                    ndoni_cs,
                    ogini_cs,
                    utapate_cs,
                ]
                colors = [
                    "#1f77b4",
                    "#ff7f0e",
                    "#2ca02c",
                    "#d62728",
                    "#9467bd",
                    "#8c564b",
                    "#e377c2",
                ]
    
                total_val = float(sum(values)) or 0.0
    
                # ---------- Helper: get FSO closing stock for a vessel on a date ----------
                from datetime import datetime as _dt, time as _t, timedelta as _td
                from db import get_session
                from models import FSOOperation
    
                def _get_fso_closing_stock_for_date(
                    vessel_name: str, the_date
                ) -> float | None:
                    try:
                        with get_session() as s_fso:
                            ext_from = the_date - _td(days=1)
                            ext_to = the_date + _td(days=1)
    
                            entries = (
                                s_fso.query(FSOOperation)
                                .filter(
                                    FSOOperation.fso_vessel == vessel_name,
                                    FSOOperation.date >= ext_from,
                                    FSOOperation.date <= ext_to,
                                )
                                .order_by(FSOOperation.date, FSOOperation.time)
                                .all()
                            )
                        if not entries:
                            return None
    
                        win_start = _dt.combine(the_date, _t(6, 1))
                        win_end = _dt.combine(the_date + _td(days=1), _t(6, 0))
    
                        def _to_time(t):
                            if isinstance(t, _t):
                                return t
                            for fmt in ("%H:%M", "%H:%M:%S"):
                                try:
                                    return _dt.strptime(str(t), fmt).time()
                                except Exception:
                                    continue
                            return _t(0, 0)
    
                        period = []
                        for e in entries:
                            try:
                                edt = _dt.combine(e.date, _to_time(e.time))
                                if win_start <= edt <= win_end:
                                    period.append(e)
                            except Exception:
                                continue
    
                        if not period:
                            return None
    
                        period.sort(key=lambda e: _dt.combine(e.date, _to_time(e.time)))
    
                        def _num(val):
                            try:
                                return float(val or 0.0)
                            except Exception:
                                return 0.0
    
                        last_entry = period[-1]
                        closing_stock = _num(getattr(last_entry, "closing_stock", None))
    
                        # Stock opening adjustment (same logic pattern as MB)
                        stock_opening_entries = [
                            e
                            for e in period
                            if (getattr(e, "operation", "") or "")
                            .strip()
                            .lower()
                            == "stock opening"
                        ]
                        if stock_opening_entries:
                            stock_opening_entry = stock_opening_entries[0]
                            stock_opening_closing = _num(
                                getattr(stock_opening_entry, "closing_stock", None)
                            )
                            try:
                                stock_opening_dt = _dt.combine(
                                    stock_opening_entry.date,
                                    _to_time(stock_opening_entry.time),
                                )
                                entries_before_opening = [
                                    e
                                    for e in period
                                    if _dt.combine(e.date, _to_time(e.time))
                                    < stock_opening_dt
                                ]
                                if entries_before_opening:
                                    closing_stock = _num(
                                        getattr(
                                            entries_before_opening[-1],
                                            "closing_stock",
                                            None,
                                        )
                                    )
                                else:
                                    closing_stock = (
                                        stock_opening_closing
                                        if stock_opening_closing
                                        else closing_stock
                                    )
                            except Exception:
                                pass
    
                        return closing_stock
                    except Exception:
                        return None
    
                # ---------- Helper: FSO tank card (same cylindrical style as tank visuals) ----------
                import html as _html_mod
    
                def _fso_tank_card_html(
                    title: str,
                    stock_value: float | None,
                    element_id: str,
                    capacity: float = 1_880_000.0,
                ) -> tuple[str, int]:
                    if stock_value is None:
                        return (
                            f"""
    <div style="border:1px solid #dee2e6;border-radius:10px;padding:0.8rem;background:#fff;
                box-shadow:0 3px 8px rgba(0,0,0,0.06);margin-top:0.4rem;">
      <div style="text-align:center;font-weight:600;font-size:1rem;margin-bottom:0.3rem;">
        {_html_mod.escape(title)}
      </div>
      <div style="font-size:0.85rem;color:#666;text-align:center;padding:0.4rem 0;">
        No data for selected date
      </div>
    </div>
    """,
                            150,
                        )
    
                    try:
                        current_stock = float(stock_value or 0.0)
                    except Exception:
                        current_stock = 0.0
    
                    cap = capacity if capacity > 0 else 1.0
                    fill_percentage = max(0.0, min(current_stock / cap * 100.0, 100.0))
    
                    # Same colour logic as tank visual
                    if fill_percentage >= 80:
                        liquid_color = "#28a745"
                        liquid_dark = "#1e7e34"
                        status_emoji = "🟢"
                    elif fill_percentage >= 50:
                        liquid_color = "#ffc107"
                        liquid_dark = "#d39e00"
                        status_emoji = "🟡"
                    elif fill_percentage >= 20:
                        liquid_color = "#fd7e14"
                        liquid_dark = "#dc3545"
                        status_emoji = "🟠"
                    else:
                        liquid_color = "#dc3545"
                        liquid_dark = "#bd2130"
                        status_emoji = "🔴"
    
                    tank_height = 175.0
                    liquid_height = (fill_percentage / 100.0) * tank_height
                    liquid_y = tank_height - liquid_height
    
                    svg_code = f"""
    <svg width="100%" height="230" viewBox="0 0 150 220" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <linearGradient id="tankGrad_{element_id}" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" style="stop-color:#c0c0c0;stop-opacity:1" />
          <stop offset="50%" style="stop-color:#e8e8e8;stop-opacity:1" />
          <stop offset="100%" style="stop-color:#c0c0c0;stop-opacity:1" />
        </linearGradient>
        <linearGradient id="liquidGrad_{element_id}" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" style="stop-color:{liquid_dark};stop-opacity:0.9" />
          <stop offset="50%" style="stop-color:{liquid_color};stop-opacity:1" />
          <stop offset="100%" style="stop-color:{liquid_dark};stop-opacity:0.9" />
        </linearGradient>
        <radialGradient id="topGrad_{element_id}" cx="50%" cy="50%" r="50%">
          <stop offset="0%" style="stop-color:#ffffff;stop-opacity:1" />
          <stop offset="100%" style="stop-color:#d0d0d0;stop-opacity:1" />
        </radialGradient>
        <radialGradient id="bottomGrad_{element_id}" cx="50%" cy="50%" r="50%">
          <stop offset="0%" style="stop-color:#a0a0a0;stop-opacity:1" />
          <stop offset="100%" style="stop-color:#707070;stop-opacity:1" />
        </radialGradient>
      </defs>
    
      <!-- Stock badge (smaller) -->
      <rect x="92" y="8" width="52" height="20" rx="10" fill="{liquid_color}" opacity="0.95"/>
      <text x="118" y="22" text-anchor="middle" fill="white" font-size="11" font-weight="bold">
        {current_stock/1000.0:.1f}K
      </text>
    
      <!-- Tank top ellipse -->
      <ellipse cx="75" cy="35" rx="42" ry="13"
               fill="url(#topGrad_{element_id})" stroke="#999" stroke-width="1.5"/>
    
      <!-- Tank body -->
      <rect x="33" y="35" width="84" height="{tank_height}"
            fill="url(#tankGrad_{element_id})" stroke="#999" stroke-width="1.5"/>
    
      <!-- Liquid fill -->
      <rect x="33" y="{35 + liquid_y}" width="84" height="{liquid_height}"
            fill="url(#liquidGrad_{element_id})"/>
    
      <!-- Liquid top surface -->
      <ellipse cx="75" cy="{35 + liquid_y}" rx="42" ry="13"
               fill="{liquid_color}" opacity="0.8"/>
    
      <!-- Percentage text -->
      <text x="75" y="{35 + tank_height/2:.1f}" text-anchor="middle"
            fill="white" font-size="26" font-weight="bold"
            style="text-shadow:2px 2px 4px rgba(0,0,0,0.7);">
        {fill_percentage:.0f}%
      </text>
    
      <!-- Tank bottom ellipse -->
      <ellipse cx="75" cy="{35 + tank_height}" rx="42" ry="13"
               fill="url(#bottomGrad_{element_id})" stroke="#666" stroke-width="1.5"/>
    
      <!-- Shadow -->
      <ellipse cx="75" cy="{38 + tank_height}" rx="44" ry="6"
               fill="black" opacity="0.2"/>
    </svg>
    """
    
                    available = max(cap - current_stock, 0.0)
    
                    return (
                        f"""
    <div style="border:1px solid #dee2e6;border-radius:12px;padding:1rem;background:#fff;
                box-shadow:0 4px 10px rgba(0,0,0,0.08);margin-top:0.5rem;">
      <div style="text-align:center;font-weight:600;font-size:1rem;margin-bottom:0.4rem;">
        {_html_mod.escape(title)}
      </div>
      {svg_code}
      <div style="margin-top:0.6rem;font-size:0.82rem;color:#333;">
        <div style="display:flex;justify-content:space-between;margin-bottom:0.35rem;border-bottom:1px solid #f1f1f1;padding-bottom:0.2rem;">
          <span style="color:#666;">Stock (bbls)</span>
          <strong style="color:{liquid_color};font-size:0.9rem;">{current_stock:,.0f}</strong>
        </div>
        <div style="display:flex;justify-content:space-between;margin-bottom:0.35rem;border-bottom:1px solid #f1f1f1;padding-bottom:0.2rem;">
          <span style="color:#666;">Capacity (bbls)</span>
          <strong style="font-size:0.9rem;">{cap:,.0f}</strong>
        </div>
        <div style="display:flex;justify-content:space-between;margin-bottom:0.35rem;border-bottom:1px solid #f1f1f1;padding-bottom:0.2rem;">
          <span style="color:#666;">Available (bbls)</span>
          <strong style="font-size:0.9rem;">{available:,.0f}</strong>
        </div>
        <div style="display:flex;justify-content:space-between;margin-bottom:0.1rem;">
          <span style="color:#666;">Level</span>
          <strong style="color:{liquid_color};font-size:0.9rem;">{status_emoji} {fill_percentage:.1f}%</strong>
        </div>
      </div>
    </div>
    """,
                        420,
                    )
    
                # Layout: donut + FSO cylinders on the left, custom details list on the right
                col_chart, col_legend = st.columns([2, 1])
    
                # ------------------ LEFT: DONUT + FSO CYLINDERS ------------------
                with col_chart:
                    # Donut chart with values inside (bold)
                    fig = go.Figure(
                        data=[
                            go.Pie(
                                labels=labels,
                                values=values,
                                hole=0.5,
                                marker=dict(colors=colors),
                                textinfo="value",          # only values
                                textposition="inside",     # inside the donut
                                texttemplate="<b>%{value:,.0f}</b>",
                                hoverinfo="label+value",   # no %
                                showlegend=False,
                            )
                        ]
                    )
                    fig.update_traces(textfont=dict(size=12, color="#000000"))
                    fig.update_layout(
                        title_text=f"Stock Positions as of {stock_date.strftime('%d-%b-%Y')}",
                        margin=dict(t=40, b=20, l=0, r=0),
                    )
                    st.plotly_chart(fig, use_container_width=True)
    
                    # FSO cylindrical visuals � same look & feel as tank cards
                    st.markdown("#### FSO Stock Position")
    
                    fso_cols = st.columns(2)
    
                    tanvi_stock = _get_fso_closing_stock_for_date(
                        "MT TULJA TANVI", stock_date
                    )
                    kalyani_stock = _get_fso_closing_stock_for_date(
                        "MT TULJA KALYANI", stock_date
                    )
    
                    with fso_cols[0]:
                        tanvi_html, tanvi_height = _fso_tank_card_html(
                            "MT TULJA TANVI", tanvi_stock, "tanvi"
                        )
                        components.html(tanvi_html, height=tanvi_height)
    
                    with fso_cols[1]:
                        kalyani_html, kalyani_height = _fso_tank_card_html(
                            "MT TULJA KALYANI", kalyani_stock, "kalyani"
                        )
                        components.html(kalyani_html, height=kalyani_height)
    
                # ------------------ RIGHT: DETAILS LIST ------------------
                with col_legend:
                    st.markdown("#### Details")
                    if total_val <= 0:
                        st.markdown(
                            "<p>No stock data available for the selected date.</p>",
                            unsafe_allow_html=True,
                        )
                    else:
                        legend_lines = []
                        for lbl, val, col in zip(labels, values, colors):
                            try:
                                vtxt = f"{float(val or 0.0):,.0f} bbls"
                            except Exception:
                                vtxt = "-"
    
                            legend_lines.append(
                                f"""
                                <div style="display:flex;align-items:center;margin-bottom:4px;">
                                    <span style="
                                        display:inline-block;
                                        width:10px;
                                        height:10px;
                                        border-radius:50%;
                                        background:{col};
                                        margin-right:8px;
                                    "></span>
                                    <span>
                                        <strong>{html.escape(lbl)}</strong><br/>
                                        <strong>{html.escape(vtxt)}</strong>
                                    </span>
                                </div>
                                """
                            )
    
                        st.markdown("".join(legend_lines), unsafe_allow_html=True)
    
                # =====================================================================
                # CONVOY & VESSEL STATUS SECTION
                # =====================================================================
                st.markdown("---")
                st.markdown("### 🚦 Convoy & Vessel Status")
                st.caption(f"Saved entries from Convoy Status page for {stock_date.strftime('%d-%b-%Y')}")
                
                convoy_col, vessel_col = st.columns(2)
                yade_entries_all: list[dict[str, str]] = []
                vessel_entries_all: list[dict[str, str]] = []
                fetch_error = None
                
                try:
                    from models import ConvoyStatusYade, ConvoyStatusVessel
                    
                    # Get location IDs for Agge and Utapate
                    with get_session() as s_loc:
                        locations = s_loc.query(Location).all()
                        
                        def _canon_token(v):
                            return str(v or "").upper().replace(" ", "").replace("-", "")
                        
                        agge_loc = None
                        utapate_loc = None
                        
                        for L in locations:
                            loc_tokens = {_canon_token(L.code), _canon_token(L.name)}
                            if loc_tokens & {"AGGE"}:
                                agge_loc = L
                            if loc_tokens & {"UTAPATE", "OML13", "OML-13"}:
                                utapate_loc = L
                    
                    # Fetch convoy status entries for both locations
                    with get_session() as s_convoy:
                        # Query YADE entries from both Agge and Utapate
                        for loc in [agge_loc, utapate_loc]:
                            if not loc:
                                continue
                            
                            yade_rows = (
                                s_convoy.query(ConvoyStatusYade, YadeBarge.name)
                                .join(YadeBarge, ConvoyStatusYade.yade_barge_id == YadeBarge.id)
                                .filter(
                                    ConvoyStatusYade.location_id == loc.id,
                                    ConvoyStatusYade.date == stock_date,
                                )
                                .order_by(ConvoyStatusYade.status.asc(), YadeBarge.name.asc())
                                .all()
                            )
                            
                            for rec, yade_name in yade_rows:
                                yade_entries_all.append(
                                    {
                                        "Location": loc.name or "N/A",
                                        "Status": (rec.status or "N/A").strip(),
                                        "YADE": yade_name or "N/A",
                                        "Convoy": rec.convoy_no or "N/A",
                                        "Stock": rec.stock_display or "N/A",
                                        "Notes": rec.notes or "",
                                    }
                                )
                        
                        # Query Vessel entries from both Agge and Utapate
                        for loc in [agge_loc, utapate_loc]:
                            if not loc:
                                continue
                            
                            vessel_rows = (
                                s_convoy.query(ConvoyStatusVessel)
                                .filter(
                                    ConvoyStatusVessel.location_id == loc.id,
                                    ConvoyStatusVessel.date == stock_date,
                                )
                                .order_by(ConvoyStatusVessel.vessel_name.asc())
                                .all()
                            )
                            
                            for rec in vessel_rows:
                                vessel_entries_all.append(
                                    {
                                        "Location": loc.name or "N/A",
                                        "Vessel": (rec.vessel_name or "N/A").strip(),
                                        "Status": (rec.status or "N/A").strip(),
                                        "Shuttle": rec.shuttle_no or "N/A",
                                        "Stock": rec.stock_display or "N/A",
                                        "Notes": rec.notes or "",
                                    }
                                )
                                
                except Exception as exc:
                    fetch_error = str(exc)
                    log_error("Convoy/vessel snapshot load failed in Lagos Dashboard", exc_info=True)
                
                if fetch_error:
                    st.error(f"Unable to load convoy status snapshots: {fetch_error}")
                
                # Sort entries
                yade_entries_all.sort(key=lambda item: (item["Location"], item["Status"], item["YADE"]))
                vessel_entries_all.sort(key=lambda item: (item["Location"], item["Vessel"]))
                
                # Display YADE convoy status
                with convoy_col:
                    st.markdown("#### Convoy Status (YADE)")
                    if not yade_entries_all:
                        st.info("No YADE convoy statuses saved for this date.")
                    else:
                        # Group by location
                        location_groups = defaultdict(list)
                        for entry in yade_entries_all:
                            location_groups[entry["Location"]].append(entry)
                        
                        for location in sorted(location_groups):
                            st.markdown(f"**{location}**")
                            entries = location_groups[location]
                            
                            # Group by status within location
                            status_groups = defaultdict(list)
                            for entry in entries:
                                status_groups[entry["Status"]].append(entry)
                            
                            for status in sorted(status_groups):
                                st.markdown(f"*{status}*")
                                for entry in status_groups[status]:
                                    notes_text = f" � {entry['Notes']}" if entry['Notes'] else ""
                                    st.markdown(
                                        f"- {entry['YADE']} | Convoy: {entry['Convoy']} | Stock: {entry['Stock']}{notes_text}"
                                    )
                            st.markdown("")  # Add spacing between locations
                
                # Display Vessel status
                with vessel_col:
                    st.markdown("#### Vessel Status")
                    if not vessel_entries_all:
                        st.info("No vessel statuses saved for this date.")
                    else:
                        # Group by location
                        location_groups = defaultdict(list)
                        for entry in vessel_entries_all:
                            location_groups[entry["Location"]].append(entry)
                        
                        for location in sorted(location_groups):
                            st.markdown(f"**{location}**")
                            for entry in location_groups[location]:
                                notes_text = f" � {entry['Notes']}" if entry['Notes'] else ""
                                st.markdown(
                                    f"- **{entry['Vessel']}** � {entry['Status']} (Shuttle: {entry['Shuttle']}, Stock: {entry['Stock']}){notes_text}"
                                )
                            st.markdown("")  # Add spacing between locations
    
            st.stop()
    
        # ======================= VIEW PDF (visual � exact dashboard look) =======================
        # Put this near the top of the dashboard page (above Summary Statistics).
        import json
        import math
        import pandas as pd
        import streamlit as st
        import streamlit.components.v1 as components
        from datetime import date as _date, datetime, timedelta, time as dt_time
        from db import get_session
    
        # Fallbacks if your helpers aren't defined above in your file (keep or remove if you already have them).
        try:
            _build_tank_svg_card_html
        except NameError:
            def _build_tank_svg_card_html(
                *,
                tank: str,
                stock_bbl: float,
                capacity_bbl: float,
                product_name: str,
                status_text: str,
                tank_code: str,
            ) -> str:
                cap = max(float(capacity_bbl or 0.0), 0.0)
                stock = max(float(stock_bbl or 0.0), 0.0)
                cap_safe = cap if cap > 0 else 1.0
                fill_pct = max(0.0, min((stock / cap_safe) * 100.0, 100.0))
                available = max(cap - stock, 0.0)
                status = (status_text or "UNKNOWN").strip()
                status_class = "ok"
                if status.upper() in {"CRITICAL", "FAULT", "DOWN"}:
                    status_class = "bad"
                elif status.upper() in {"ALERT", "MAINTENANCE"}:
                    status_class = "warn"
    
                return f"""
                <div class="tank-card">
                    <div class="tank-card__header">
                        <span class="tank-card__name">{html.escape(tank or tank_code)}</span>
                        <span class="tank-card__code">{html.escape(tank_code)}</span>
                    </div>
                    <div class="tank-card__product">{html.escape(product_name or "N/A")}</div>
                    <div class="tank-gauge">
                        <div class="tank-gauge__fill" style="height:{fill_pct:.1f}%"></div>
                        <div class="tank-gauge__overlay">
                            <span>{fill_pct:.0f}%</span>
                        </div>
                    </div>
                    <div class="tank-card__stats">
                        <div><label>Stock</label><strong>{stock:,.0f}</strong></div>
                        <div><label>Capacity</label><strong>{cap:,.0f}</strong></div>
                        <div><label>Available</label><strong>{available:,.0f}</strong></div>
                    </div>
                    <div class="tank-card__status tank-card__status--{status_class}">
                        {html.escape(status or "UNKNOWN")}
                    </div>
                </div>
                """
        def _norm_status(val):
            """Return UPPERCASED string status safely from str/Enum/None."""
            try:
                if val is None:
                    return ""
                if isinstance(val, str):
                    return val.strip().upper()
                # Enum-like object support
                if hasattr(val, "value") and val.value is not None:
                    return str(val.value).strip().upper()
                if hasattr(val, "name") and val.name is not None:
                    return str(val.name).strip().upper()
                return str(val).strip().upper()
            except Exception:
                try:
                    return str(val).strip().upper()
                except Exception:
                    return ""
    
        def _canon(txt: str) -> str:
            return str(txt or "").upper().replace(" ", "").replace("-", "")
    
        def _fmt0(v):
            try:
                if v is None: return "-"
                if isinstance(v, (int, float)): return f"{v:,.0f}"
                return str(v)
            except Exception:
                return "-"
    
        def _find_col(df: pd.DataFrame, cands):
            if df is None or df.empty: return None
            for c in cands:
                if c in df.columns: return c
            lower = {str(c).strip().lower(): c for c in df.columns}
            for c in cands:
                lc = str(c).strip().lower()
                if lc in lower: return lower[lc]
            return None
    
        def _canonical_fso_code(code: str | None) -> str:
            if not code: return ""
            s2 = str(code).strip().upper()
            s_norm = s2.replace(" ", "").replace("-", "")
            aliases = {"UTAPATE":"OML-13","OML13":"OML-13","OML-13":"OML-13","OML 13":"OML-13","AGGE":"AGGE"}
            return aliases.get(s2, aliases.get(s_norm, s2))
    
        # ---------- Pull FSO �Exports (bbls)� from session caches over a range (used in Monthly Data) ----------
        def _sum_exports_from_session(md_from, md_to, canon_loc: str, vessel_name="MT TULJA KALYANI"):
            keys = [
                "fso_mb_df","fso_mb_table","fso_material_balance_df",
                "fso_mb_daily","fso_mb_cache","fso_mb_summary_df",
                "fso_mb_pivot","fso_mb_records"
            ]
            def _pick(df: pd.DataFrame):
                if df is None or df.empty: return None
                dcol = _find_col(df, ["Date","MB Date","As Of","Asof"])
                if not dcol: return None
                dfx = df.copy()
                dfx[dcol] = pd.to_datetime(dfx[dcol], errors="coerce").dt.date
                dfx = dfx[(dfx[dcol] >= md_from) & (dfx[dcol] <= md_to)]
                if dfx.empty: return None
                lcol = _find_col(dfx, ["Location","Loc","Site","Code"])
                if lcol is not None and canon_loc:
                    norm = dfx[lcol].astype(str).str.upper().str.replace(" ","",regex=False).str.replace("-","",regex=False)
                    dfx = dfx[norm == canon_loc.replace("-","").replace(" ","")]
                    if dfx.empty: return None
                vcol = _find_col(dfx, ["Vessel","FSO Vessel","Vessel Name","fso_vessel"])
                if vcol is not None and vessel_name:
                    dfx = dfx[dfx[vcol].astype(str).str.upper().str.contains(vessel_name.upper())]
                    if dfx.empty: return None
                ecol = _find_col(dfx, ["Exports (bbls)","Export (bbls)","exports_bbls","export_bbls","Exports","Export"])
                if ecol is None: return None
                return float(pd.to_numeric(dfx[ecol], errors="coerce").fillna(0).sum())
            # Try known keys
            for k in keys:
                if k in st.session_state:
                    try:
                        obj = st.session_state[k]
                        df = obj.copy() if isinstance(obj, pd.DataFrame) else pd.DataFrame(obj)
                        val = _pick(df)
                        if val is not None:
                            return val
                    except Exception:
                        pass
            # Try all DataFrame-like objects in session as a last resort
            for k, obj in list(st.session_state.items()):
                try:
                    if isinstance(obj, pd.DataFrame):
                        df = obj
                    elif isinstance(obj, (list, tuple)) and obj and isinstance(obj[0], dict):
                        df = pd.DataFrame(obj)
                    else:
                        continue
                    if df is None or df.empty: 
                        continue
                    val = _pick(df.copy())
                    if val is not None:
                        return val
                except Exception:
                    continue
            return None
    
        # ---------- Build an SVG trend (so PDF matches the dashboard visually) ----------
        def _build_trend_svg_for_pdf(s, loc_code: str, tr_from: _date, tr_to: _date, width=900, height=360):
            from material_balance_calculator import MaterialBalanceCalculator as MBCalc
    
            # Prepare date index
            dates = pd.date_range(tr_from, tr_to, freq="D").date.tolist()
            if not dates:
                return '<svg width="100%" height="1"></svg>'
    
            # Pull material balance rows for the range
            try:
                rows = MBCalc.calculate_material_balance(
                    entries=None,
                    location_code=(loc_code or "").upper(),
                    date_from=tr_from,
                    date_to=tr_to,
                    location_id=active_location_id,
                    debug=False
                )
                df = pd.DataFrame(rows) if rows else pd.DataFrame({"Date": dates})
            except Exception:
                df = pd.DataFrame({"Date": dates})
    
            # Find date column
            dcol = _find_col(df, ["Date","MB Date","As Of","Asof"]) or "Date"
            if dcol not in df.columns:
                df[dcol] = dates
            df[dcol] = pd.to_datetime(df[dcol], errors="coerce").dt.date
    
            # Determine location profile (Utapate vs Asemoku)
            is_asemoku = _canon(loc_code) in {"JETTY","ASEMOKU","ASEMOKUJETTY"}
            series_defs = []
            if is_asemoku:
                # 3 lines: ANZ Production, BFS Receipt, Evacuation
                series_defs = [
                    ("ANZ Production", ["ANZ Receipt"], "#8B4513"),  # brown
                    ("BFS Receipt",    ["OKW Receipt"], "#006400"),  # dark green
                    ("Evacuation",     ["Dispatch to barge","Dispatch to Barge","Dispatch"], "#1f77b4"),  # blue
                ]
            else:
                # Utapate (and others like OML-13): 2 lines: Production, Evacuation
                series_defs = [
                    ("Production", ["Receipt"], "#8B4513"),  # brown
                    ("Evacuation", ["Dispatch"], "#006400"), # dark green
                ]
    
            # Build a merged daily frame
            base = pd.DataFrame({ "date": dates })
            for label, cand_cols, _color in series_defs:
                col = None
                for c in cand_cols:
                    if c in df.columns: 
                        col = c; break
                if not col:
                    # try case-insensitive
                    lower_map = {str(c).lower(): c for c in df.columns}
                    for c in cand_cols:
                        if c.lower() in lower_map:
                            col = lower_map[c.lower()]
                            break
                if col:
                    sub = df[[dcol, col]].copy()
                    sub.columns = ["date", label]
                    base = base.merge(sub, on="date", how="left")
                else:
                    base[label] = 0.0
            # Fill
            for label, _, _ in series_defs:
                base[label] = pd.to_numeric(base[label], errors="coerce").fillna(0.0)
    
            # Compute y scale
            y_max = max([base[l].max() for l,_,_ in series_defs] + [0.0])
            if y_max <= 0: y_max = 1.0
            y_max = float(y_max * 1.10)  # headroom
    
            # SVG layout
            W, H = width, height
            left, right, top, bottom = 60, 20, 20, 60
            plot_w, plot_h = W - left - right, H - top - bottom
    
            def x_pos(d):
                return left + (plot_w * (dates.index(d) / max(len(dates)-1, 1)))
            def y_pos(v):
                return top + plot_h - (plot_h * (float(v) / y_max))
    
            # Build paths & markers
            series_paths = []
            labels = []
            # For global highest/lowest highlight (single label per your requirement)
            all_points = []
            for label, _cands, color in series_defs:
                pts = [(x_pos(d), y_pos(base.loc[base['date']==d, label].values[0])) for d in dates]
                # path
                if pts:
                    d_attr = "M " + " L ".join([f"{x:.1f},{y:.1f}" for x,y in pts])
                else:
                    d_attr = ""
                series_paths.append(f'<path d="{d_attr}" fill="none" stroke="{color}" stroke-width="2"/>')
                # triangle markers
                tris = []
                for (x,y), d in zip(pts, dates):
                    tris.append(f'<path d="M {x:.1f},{y-5:.1f} L {x-5:.1f},{y+5:.1f} L {x+5:.1f},{y+5:.1f} Z" fill="{color}" />')
                    all_points.append((label, d, base.loc[base["date"]==d, label].values[0], x, y, color))
                series_paths.append("".join(tris))
    
            # Determine single highest & single lowest across all series
            if all_points:
                max_pt = max(all_points, key=lambda r: float(r[2] or 0.0))
                min_pt = min(all_points, key=lambda r: float(r[2] or 0.0))
                for (tag, pt, anchor) in [("MAX", max_pt, "start"), ("MIN", min_pt, "end")]:
                    _lbl, _date, _val, _x, _y, _col = pt
                    _txt = f"{_lbl}: {_fmt0(_val)}"
                    _xo = 8 if tag=="MAX" else -8
                    labels.append(
                        f'<text x="{_x+_xo:.1f}" y="{_y-8:.1f}" font-size="12" font-weight="bold" fill="#000" text-anchor="{anchor}">{_txt}</text>'
                    )
    
            # Grid & axes
            # Y ticks (5)
            y_ticks = []
            for i in range(0,6):
                v = (y_max/5)*i
                y = y_pos(v)
                y_ticks.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" stroke="#eaeaea"/>')
                y_ticks.append(f'<text x="{left-8}" y="{y+4:.1f}" font-size="11" fill="#555" text-anchor="end">{_fmt0(v)}</text>')
    
            # X ticks: one per day
            x_ticks = []
            for d in dates:
                x = x_pos(d)
                if len(dates) <= 31 or dates.index(d) % max(1, math.ceil(len(dates)/31)) == 0:
                    x_ticks.append(f'<text x="{x:.1f}" y="{top+plot_h+18:.1f}" font-size="11" fill="#555" text-anchor="middle">{pd.to_datetime(d).strftime("%d-%b")}</text>')
    
            # Legend
            legend_items = []
            lx = left; ly = top - 5
            for label, _c, color in series_defs:
                legend_items.append(f'<rect x="{lx}" y="{ly}" width="10" height="10" fill="{color}" />')
                legend_items.append(f'<text x="{lx+14}" y="{ly+10}" font-size="12" fill="#222">{label}</text>')
                lx += 130
    
            # Axis titles
            axis_titles = [
                f'<text x="{left + plot_w/2:.1f}" y="{top+plot_h+40:.1f}" font-size="12" fill="#222" text-anchor="middle">Date</text>',
                f'<text transform="translate({left-45:.1f},{top+plot_h/2:.1f}) rotate(-90)" font-size="12" fill="#222" text-anchor="middle">Quantity in bbls</text>'
            ]
    
            svg = f'''
            <svg width="{W}" height="{H}" viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg">
            <rect x="0" y="0" width="{W}" height="{H}" fill="#fff"/>
            <!-- Legend -->
            {''.join(legend_items)}
            <!-- Plot area -->
            <rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="#fff" stroke="#ddd"/>
            <!-- Grid & Axes -->
            {''.join(y_ticks)}
            {''.join(x_ticks)}
            <!-- Series paths -->
            {''.join(series_paths)}
            <!-- Highlight labels -->
            {''.join(labels)}
            <!-- Axis titles -->
            {''.join(axis_titles)}
            <!-- In-plot totals card (top-right) -->
            <rect x="{left+plot_w-200}" y="{top+10}" width="190" height="52" rx="8" fill="rgba(255,255,255,0.95)" stroke="#ddd"/>
            <text x="{left+plot_w-105}" y="{top+30}" text-anchor="middle" font-size="12" fill="#222">
                Totals: {' / '.join([f'{name} '+_fmt0(base[name].sum()) for name,_,_ in series_defs])}
            </text>
            </svg>
            '''
            return svg
    
        # ---------- Build HTML snapshot for the 4 sections ----------
        def _build_dashboard_html_for_pdf():
            with get_session() as s:
                # Location & dates
                try:
                    from location_manager import LocationManager
                    loc_obj = LocationManager.get_location_by_id(s, active_location_id)
                    loc_code = (getattr(loc_obj, "code", "") or "")
                    loc_name = (getattr(loc_obj, "name", "") or "")
                except Exception:
                    loc_code, loc_name = "", ""
                canon_loc = _canonical_fso_code(loc_code)
                is_asemoku = _canon(loc_code) in {"JETTY","ASEMOKU","ASEMOKUJETTY"}
                is_utapate = _canon(loc_code) in {"UTAPATE","OML13","OML-13"}
    
                sel_date = st.session_state.get("dash_date_all_sites", _date.today())
                md_from  = st.session_state.get("monthly_from", sel_date.replace(day=1))
                md_to    = st.session_state.get("monthly_to", sel_date)
                tr_from  = st.session_state.get("trend_from", sel_date.replace(day=1))
                tr_to    = st.session_state.get("trend_to", sel_date)
    
                # ===== Summary (same mapping you already use) =====
                prod_bbl = 0.0; evac_bbl = 0.0; fso_receipt_bbl = None; fso_stock_bbl = None
                try:
                    from material_balance_calculator import MaterialBalanceCalculator as MBCalc
                    rows = MBCalc.calculate_material_balance(
                        entries=None, location_code=(loc_code or "").upper(),
                        date_from=sel_date, date_to=sel_date,
                        location_id=active_location_id, debug=False
                    )
                    if rows:
                        r = pd.DataFrame(rows).iloc[0].to_dict()
                        prod_bbl = float(r.get("Receipt", 0) or 0)
                        evac_bbl = float(r.get("Dispatch", 0) or 0)
                except Exception:
                    pass
    
                # FSO receipt & stock (same day)
                def _fso_from_cache(date_, code_):
                    keys = ["fso_mb_df","fso_mb_table","fso_material_balance_df","fso_mb_daily","fso_mb_cache","fso_mb_summary_df","fso_mb_pivot","fso_mb_records"]
                    canon = _canonical_fso_code(code_)
                    for k in keys:
                        if k not in st.session_state: continue
                        obj = st.session_state[k]
                        try:
                            df = obj.copy() if isinstance(obj, pd.DataFrame) else pd.DataFrame(obj)
                        except Exception:
                            continue
                        if df is None or df.empty: continue
                        dcol = _find_col(df, ["Date","MB Date","As Of","Asof"])
                        if not dcol: continue
                        dfx = df.copy()
                        dfx[dcol] = pd.to_datetime(dfx[dcol], errors="coerce").dt.date
                        dfx = dfx[dfx[dcol] == date_]
                        if dfx.empty: continue
                        lcol = _find_col(dfx, ["Location","Loc","Site","Code"])
                        if lcol is not None and canon:
                            norm = dfx[lcol].astype(str).str.upper().str.replace(" ","",regex=False).str.replace("-","",regex=False)
                            dfx = dfx[norm == canon.replace("-","").replace(" ","")]
                            if dfx.empty: continue
                        c_rec = _find_col(dfx, ["Receipt (bbls)","Receipts (bbls)","receipt_bbls","receipts","receipt"])
                        c_cls = _find_col(dfx, ["Closing Stock (bbls)","Closing Stock","closing_stock","closing_stock_bbl"])
                        rec = float(pd.to_numeric(dfx[c_rec], errors="coerce").fillna(0).sum()) if c_rec else None
                        cls = None
                        if c_cls is not None:
                            srs = pd.to_numeric(dfx[c_cls], errors="coerce").dropna()
                            if not srs.empty: cls = float(srs.iloc[-1])
                        if rec is not None or cls is not None:
                            return rec, cls
                    return None, None
    
                fso_receipt_bbl, fso_stock_bbl = _fso_from_cache(sel_date, loc_code)
    
                # ===== Ullage & Pumpable (status aware) =====
                try:
                    from models import Tank, TankTransaction, OTRRecord
                    tanks = s.query(Tank).filter(Tank.location_id == active_location_id).order_by(Tank.name).all()
                except Exception:
                    tanks = []
    
                ullage_available_bbl = 0.0
                pumpable_stock_bbl   = 0.0
                for t in tanks:
                    latest = (
                        s.query(TankTransaction.ticket_id, TankTransaction.date, TankTransaction.time)
                        .filter(TankTransaction.tank_id == t.id, TankTransaction.date <= sel_date)
                        .order_by(TankTransaction.date.desc(), TankTransaction.time.desc())
                        .first()
                    )
                    stock = 0.0
                    if latest and latest.ticket_id:
                        otr = s.query(OTRRecord).filter(OTRRecord.ticket_id == latest.ticket_id).first()
                        stock = float(getattr(otr, "nsv_bbl", 0.0) or 0.0)
                    cap = float(getattr(t, "capacity_bbl", 0.0) or 0.0)
                    ullage_available_bbl += 0.90 * max(cap - stock, 0.0)
    
                    # ---- FIXED: normalize Enum/string status safely ----
                    status_norm = _norm_status(getattr(t, "status", None))
                    if status_norm in {"IDLE","READY","DISPATCHING"}:
                        pumpable_stock_bbl += 0.85 * stock
    
                # ===== Tank visuals HTML (same card design you use) =====
                tank_html = ""
                if tanks:
                    tank_html += '<div class="tanks-grid">'
                    for t in tanks:
                        latest = (
                            s.query(TankTransaction.ticket_id, TankTransaction.date, TankTransaction.time)
                            .filter(TankTransaction.tank_id == t.id, TankTransaction.date <= sel_date)
                            .order_by(TankTransaction.date.desc(), TankTransaction.time.desc())
                            .first()
                        )
                        stock = 0.0
                        if latest and latest.ticket_id:
                            otr = s.query(OTRRecord).filter(OTRRecord.ticket_id == latest.ticket_id).first()
                            stock = float(getattr(otr, "nsv_bbl", 0.0) or 0.0)
                        cap  = float(getattr(t, "capacity_bbl", 0.0) or 0.0)
                        prod = getattr(t, "product_type", getattr(t, "product", "N/A"))
                        code = getattr(t, "code", f"T-{t.id}")
    
                        # ---- FIXED: friendly display for Enum/string status ----
                        status_norm = _norm_status(getattr(t, "status", None))
                        status_disp = status_norm.title() if status_norm else "-"
    
                        tank_html += _build_tank_svg_card_html(
                            tank=getattr(t, "name", code),
                            stock_bbl=stock, capacity_bbl=cap,
                            product_name=prod, status_text=status_disp, tank_code=code
                        )
                    tank_html += "</div>"
                else:
                    tank_html = '<div style="color:#6c757d;">No tanks configured.</div>'
    
                # ===== Monthly Data (reuse cached HTML if present) =====
                monthly_html = st.session_state.get('__monthly_html_for_pdf', '')
                if not monthly_html:
                    # Calculate as in dashboard (Utapate & Asemoku mappings)
                    from material_balance_calculator import MaterialBalanceCalculator as MBCalc
                    mb_rows = MBCalc.calculate_material_balance(
                        entries=None,
                        location_code=(loc_code or "").upper(),
                        date_from=md_from,
                        date_to=md_to,
                        location_id=active_location_id,
                        debug=False
                    )
                    df_mb = pd.DataFrame(mb_rows) if mb_rows else pd.DataFrame({"Date":[]})
                    def pick(df, cands):
                        if df.empty: return None
                        for c in cands:
                            if c in df.columns: return c
                        low = {str(c).lower(): c for c in df.columns}
                        for c in cands:
                            if c.lower() in low: return low[c.lower()]
                        return None
    
                    if is_utapate:
                        c_rec = pick(df_mb, ["Receipt"])
                        c_dis = pick(df_mb, ["Dispatch"])
                        prod_total = float(pd.to_numeric(df_mb[c_rec], errors="coerce").fillna(0).sum()) if c_rec else 0.0
                        evac_total = float(pd.to_numeric(df_mb[c_dis], errors="coerce").fillna(0).sum()) if c_dis else 0.0
                        days_count = max(len(pd.date_range(md_from, md_to)), 1)
                        avg_prod = prod_total / days_count
                        avg_evac = evac_total / days_count
                        export_total = _sum_exports_from_session(md_from, md_to, canon_loc, "MT TULJA KALYANI")
    
                        monthly_html = f"""
                        <div class="section">
                        <h2>📊 Monthly Data</h2>
                        <div class="cards four">
                            <div class="stat-card">
                            <div class="stat-label">Production</div>
                            <div class="stat-value">{_fmt0(prod_total)}</div>
                            <div style="color:#6c757d;font-size:0.8rem;">Avg Production: {_fmt0(avg_prod)}</div>
                            </div>
                            <div class="stat-card">
                            <div class="stat-label">Evacuation</div>
                            <div class="stat-value">{_fmt0(evac_total)}</div>
                            <div style="color:#6c757d;font-size:0.8rem;">Avg Evacuation: {_fmt0(avg_evac)}</div>
                            </div>
                            <div class="stat-card">
                            <div class="stat-label">Export (MT TULJA KALYANI)</div>
                            <div class="stat-value">{_fmt0(export_total)}</div>
                            <div style="color:#6c757d;font-size:0.8rem;">{_fmt0(days_count)} days</div>
                            </div>
                            <div class="stat-card">
                            <div class="stat-label">Vessel Status &amp; Stock</div>
                            <div class="stat-value">-</div>
                            <div style="color:#6c757d;font-size:0.8rem;">(to be wired)</div>
                            </div>
                        </div>
                        </div>
                        """
                    else:
                        c_anz = pick(df_mb, ["ANZ Receipt"])
                        c_okw = pick(df_mb, ["OKW Receipt"])
                        c_dis = pick(df_mb, ["Dispatch to barge","Dispatch"])
                        anz_total  = float(pd.to_numeric(df_mb[c_anz], errors="coerce").fillna(0).sum()) if c_anz else 0.0
                        bfs_total  = float(pd.to_numeric(df_mb[c_okw or c_dis], errors="coerce").fillna(0).sum()) if (c_okw or c_dis) else 0.0
                        evac_total = float(pd.to_numeric(df_mb[c_dis], errors="coerce").fillna(0).sum()) if c_dis else 0.0
                        days_count = max(len(pd.date_range(md_from, md_to)), 1)
                        avg_anz  = anz_total / days_count
                        avg_bfs  = bfs_total / days_count
                        avg_evac = evac_total / days_count
    
                        monthly_html = f"""
                        <div class="section">
                        <h2>📊 Monthly Data</h2>
                        <div class="cards four">
                            <div class="stat-card">
                            <div class="stat-label">ANZ Production</div>
                            <div class="stat-value">{_fmt0(anz_total)}</div>
                            <div style="color:#6c757d;font-size:0.8rem;">Avg: {_fmt0(avg_anz)}</div>
                            </div>
                            <div class="stat-card">
                            <div class="stat-label">BFS Receipt</div>
                            <div class="stat-value">{_fmt0(bfs_total)}</div>
                            <div style="color:#6c757d;font-size:0.8rem;">Avg: {_fmt0(avg_bfs)}</div>
                            </div>
                            <div class="stat-card">
                            <div class="stat-label">Evacuation</div>
                            <div class="stat-value">{_fmt0(evac_total)}</div>
                            <div style="color:#6c757d;font-size:0.8rem;">Avg: {_fmt0(avg_evac)}</div>
                            </div>
                            <div class="stat-card">
                            <div class="stat-label">BCCR</div>
                            <div class="stat-value">-</div>
                            <div style="color:#6c757d;font-size:0.8rem;">(to be wired)</div>
                            </div>
                        </div>
                        </div>
                        """
    
                # ===== Trend SVG (exactly for current location) =====
                trend_svg = _build_trend_svg_for_pdf(s, loc_code, tr_from, tr_to, width=980, height=380)
    
                # ===== Cards CSS & Final HTML =====
                css = """
                <style>
                body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; color:#222; }
                .wrap { padding: 18px 22px; }
                h2 { margin: 6px 0 12px; }
                .cards { display: grid; grid-gap: 12px; }
                .cards.six  { grid-template-columns: repeat(6, 1fr); }
                .cards.four { grid-template-columns: repeat(4, 1fr); }
                .section { margin-top: 18px; }
                .stat-card {
                    background: white; padding: 1.0rem; border-radius: 10px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.1); border-left: 4px solid #667eea;
                    transition: transform 0.2s;
                }
                .stat-value { font-size: 1.4rem; font-weight: bold; color: #667eea; margin: 0.25rem 0; }
                .stat-label { color: #666; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 1px; }
                .tanks-grid { display: grid; grid-template-columns: repeat(5, 1fr); grid-gap: 12px; }
                .tank-card { border: 1px solid #e5e7eb; border-radius: 10px; padding: 8px 10px; box-shadow: 0 2px 6px rgba(0,0,0,.06); }
                .tank-card__header { display:flex; justify-content:space-between; font-weight:600; color:#1f4788; font-size:0.9rem; }
                .tank-card__code { font-size:0.75rem; color:#6c757d; }
                .tank-card__product { font-size:0.78rem; color:#555; margin:4px 0 6px; }
                .tank-gauge { position:relative; height:120px; border:1px solid #d7dce3; border-radius:8px; overflow:hidden; background:linear-gradient(180deg,#eef3fb 0%,#fff 100%); }
                .tank-gauge__fill { position:absolute; bottom:0; left:0; right:0; background:linear-gradient(180deg,#00b4d8 0%,#0077b6 100%); transition:height 0.4s ease; }
                .tank-gauge__overlay { position:absolute; inset:0; display:flex; align-items:center; justify-content:center; font-size:1.15rem; font-weight:bold; color:#fff; text-shadow:0 1px 3px rgba(0,0,0,0.4); }
                .tank-card__stats { display:flex; justify-content:space-between; margin-top:8px; font-size:0.72rem; color:#6c757d; }
                .tank-card__stats div { text-align:center; flex:1; }
                .tank-card__stats strong { display:block; color:#212529; font-size:0.85rem; margin-top:2px; }
                .tank-card__status { margin-top:6px; text-align:center; font-size:0.78rem; font-weight:600; padding:4px 6px; border-radius:6px; }
                .tank-card__status--ok { color:#0f5132; background:#d1e7dd; }
                .tank-card__status--warn { color:#664d03; background:#fff3cd; }
                .tank-card__status--bad { color:#842029; background:#f8d7da; }
                .subtle { color:#6c757d; font-size: 0.78rem; }
                .mb-note { color:#6c757d; font-size:0.75rem; margin-top:2px; }
                </style>
                """
    
                sum_cards_html = f"""
                <div class="section">
                <h2>📊 Summary Statistics &nbsp; <span class="subtle">{sel_date.strftime('%d-%b-%Y')}</span></h2>
                <div class="cards six">
                    <div class="stat-card"><div class="stat-label">Production</div><div class="stat-value">{_fmt0(prod_bbl)}</div><div class="mb-note">{sel_date.strftime('%d-%b-%Y')}</div></div>
                    <div class="stat-card"><div class="stat-label">Evacuation</div><div class="stat-value">{_fmt0(evac_bbl)}</div><div class="mb-note">{sel_date.strftime('%d-%b-%Y')}</div></div>
                    <div class="stat-card"><div class="stat-label">FSO receipt</div><div class="stat-value">{_fmt0(fso_receipt_bbl)}</div><div class="mb-note">{sel_date.strftime('%d-%b-%Y')}</div></div>
                    <div class="stat-card"><div class="stat-label">FSO Stock</div><div class="stat-value">{_fmt0(fso_stock_bbl)}</div><div class="mb-note">{sel_date.strftime('%d-%b-%Y')}</div></div>
                    <div class="stat-card"><div class="stat-label">Ullage available</div><div class="stat-value">{_fmt0(ullage_available_bbl)}</div><div class="mb-note">{sel_date.strftime('%d-%b-%Y')}</div></div>
                    <div class="stat-card"><div class="stat-label">Pumpable Stock</div><div class="stat-value">{_fmt0(pumpable_stock_bbl)}</div><div class="mb-note">{sel_date.strftime('%d-%b-%Y')}</div></div>
                </div>
                </div>
                """
    
                _canon_name = str(loc_name or "").upper().replace(" ", "").replace("-", "")
                _canon_code = str(loc_code or "").upper().replace(" ", "").replace("-", "")
                _is_lagos_pdf = ("LAGOS" in _canon_name) or ("LAGOS" in _canon_code)
                _display_title = "CRUDE OPERATIONS DASHBOARD" if _is_lagos_pdf else f"{loc_name or loc_code or 'Location'} Dashboard"
                html = f"""
                <!doctype html>
                <html><head><meta charset="utf-8"/>{css}</head>
                <body>
                <div class="wrap">
                    <h2>{_display_title}</h2>
                    <div class="subtle">Generated: {datetime.now().strftime('%d-%b-%Y %H:%M')}</div>
    
                    {sum_cards_html}
    
                    <div class="section">
                    <h2>🛢️ Tank Stock Levels</h2>
                    {tank_html}
                    </div>
    
                    {monthly_html}
    
                    <div class="section">
                    <h2>📈 Production &amp; Evacuation Trend</h2>
                    {trend_svg}
                    </div>
                </div>
                </body></html>
                """
            return html
    
        def _find_local_chromium() -> str | None:
            """Attempt to locate a locally installed Chromium/Chrome/Edge executable."""
            env_paths = [
                os.environ.get("CHROME_PATH"),
                os.environ.get("GOOGLE_CHROME_PATH"),
                os.environ.get("EDGE_PATH"),
            ]
            known_paths = [
                r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
                r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
                r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
                r"C:\Program Files\Chromium\Application\chrome.exe",
            ]
            for candidate in env_paths + known_paths:
                if candidate and Path(candidate).exists():
                    return candidate
            exe = shutil.which("chrome") or shutil.which("msedge") or shutil.which("chromium")
            return exe
    
        def _render_dashboard_pdf_bytes(html_payload: str, timeout: int = 30) -> bytes:
            """Render dashboard HTML to PDF using headless Chromium via pyppeteer."""
            import asyncio
            try:
                from pyppeteer import launch
            except ImportError as exc:
                raise RuntimeError(
                    "pyppeteer is required to generate dashboard PDFs. "
                    "Please install it with 'pip install pyppeteer'."
                ) from exc
    
            executable = _find_local_chromium()
            launch_kwargs = dict(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
                handleSIGINT=False,
                handleSIGTERM=False,
                handleSIGHUP=False,
            )
            if executable:
                launch_kwargs["executablePath"] = executable
    
            async def _convert() -> bytes:
                browser = await launch(**launch_kwargs)
                try:
                    page = await browser.newPage()
                    await page.setContent(html_payload)
                    await asyncio.sleep(0.5)
                    pdf_bytes = await page.pdf(
                        format="A4",
                        printBackground=True,
                        margin={"top": "10mm", "right": "10mm", "bottom": "10mm", "left": "10mm"},
                        landscape=True,
                    )
                    await page.close()
                    return pdf_bytes
                finally:
                    await browser.close()
    
            try:
                return asyncio.run(asyncio.wait_for(_convert(), timeout=timeout))
            except RuntimeError:
                loop = asyncio.new_event_loop()
                try:
                    return loop.run_until_complete(asyncio.wait_for(_convert(), timeout=timeout))
                finally:
                    loop.close()
    
        def _open_pdf_blob(pdf_bytes: bytes, filename: str = "OTMS_Dashboard.pdf"):
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
    
    
    
        # ===== Header & Button =====
        with get_session() as s:
            try:
                from location_manager import LocationManager
                _loc = LocationManager.get_location_by_id(s, st.session_state.get("active_location_id"))
                _loc_code = (getattr(_loc, "code", "") or "")
                _loc_name = (getattr(_loc, "name", "") or "")
            except Exception:
                _loc_code, _loc_name = "", ""
        def _canon(txt: str) -> str:
            return str(txt or "").upper().replace(" ", "").replace("-", "")
        _keys = {_canon(_loc_code), _canon(_loc_name)}
        _is_lagos = any("LAGOS" in k for k in _keys)
        _dash_title = "CRUDE OPERATIONS DASHBOARD" if _is_lagos else f"{_loc_name or _loc_code or 'Location'} Dashboard"
        h1, h2 = st.columns([3,1])
        with h1:
            st.markdown(f"## {_dash_title}")
        with h2:
            view_pdf_clicked = st.button("👁️ View PDF", use_container_width=True, key="btn_view_pdf_visual_exact")
    
        if view_pdf_clicked:
            with st.spinner("Building dashboard PDF..."):
                try:
                    html_str = _build_dashboard_html_for_pdf()
                except Exception as exc:
                    log_error("Dashboard PDF generation failed", exc_info=True)
                    st.error(f"? Unable to build dashboard PDF: {exc}")
                else:
                    try:
                        pdf_bytes = _render_dashboard_pdf_bytes(html_str)
                    except RuntimeError as exc:
                        st.error(str(exc))
                    except Exception as exc:
                        log_error("Dashboard PDF rendering failed", exc_info=True)
                        st.error(f"? Unable to render dashboard PDF: {exc}")
                    else:
                        _open_pdf_blob(pdf_bytes)
                        st.success("? Dashboard PDF opened in a new tab!")
        # =======================================================================
        # ======= SUMMARY STATISTICS (location-aware: Asemoku, Aggu, Beneku) ====
        from datetime import date as _date, datetime, time as dt_time, timedelta
        import pandas as pd
    
        st.markdown("### 📊 Summary Statistics")
    
        # --- Date selector (shared across dashboard) ---
        d1, d2 = st.columns([1, 3])
        with d1:
            _selected_date = st.date_input(
                "Date",
                value=st.session_state.get("dash_date_all_sites", _date.today()),
                key="dash_date_all_sites"
            )
        with d2:
            st.write(f"Showing **{_selected_date.strftime('%d-%b-%Y')}**")
    
        prev_summary_date = _selected_date - timedelta(days=1)
    
        # Defaults
        prod_bbl = 0.0
        evac_bbl = 0.0
        prev_prod_bbl = None
        prev_evac_bbl = None
        fso_receipt_bbl = None  # renders as "-" if None
        fso_stock_bbl = None    # renders as "-" if None
    
        # OFS (Oguali) specific placeholders (wired to OFS Production & Evacuation data)
        ofs_oguali_prod_bbl = 0.0
        ofs_ukpichi_prod_bbl = 0.0
        ofs_other_prod_bbl = 0.0
        prev_ofs_oguali_prod_bbl = None
        prev_ofs_ukpichi_prod_bbl = None
        prev_ofs_other_prod_bbl = None
        ofs_evacuation_bbl = 0.0
        ofs_tankers_oguali = 0.0
        ofs_tankers_ukpichi = 0.0
        ofs_tankers_other = 0.0
    
        # Asemoku-specific values
        anz_prod_bbl = None
        okw_receipt_bbl = None
        dispatch_barge_bbl = None
        produced_water_bbl = None
        prev_anz_prod_bbl = None
        prev_okw_receipt_bbl = None
        prev_produced_water_bbl = None
        river_draft_value = None
        rainfall_value = None
        prev_river_draft_value = None
        prev_rainfall_value = None
        river_display_date = None
    
        # Beneku (BFS)-specific values
        okw_prod_bbl = None
        gpp_prod_bbl = None
        prev_okw_prod_bbl = None
        prev_gpp_prod_bbl = None
        bfs_evac_bbl = None
    
        # Agge-specific dashboard cards
        agge_receipt_from_yade_bbl = None
        agge_evacuation_bbl = None
        agge_fso_receipt_bbl = None
        agge_fso_stock_bbl = None
    
        # Aggu dashboard metrics
        aggu_tankers_dispatched = None
    
        # Ndoni tanker detail metrics
        nd_tankers_from_aggu = None
        nd_tankers_from_ofs = None
        nd_other_tankers = None
        prev_nd_receipt_agu_bbl = None
        prev_nd_receipt_ofs_bbl = None
        prev_nd_other_rcpt_bbl = None
    
        # Location flags
        is_asemoku = False
        is_aggu = False
        is_agge = False
        is_bfs = False
        is_ndoni = False
    
        with get_session() as s:
            # -------- Resolve location code/name --------
            try:
                from location_manager import LocationManager
                loc_obj = LocationManager.get_location_by_id(s, active_location_id)
                loc_code = (getattr(loc_obj, "code", "") or "")
                loc_name = (getattr(loc_obj, "name", "") or "")
            except Exception:
                loc_code, loc_name = "", ""
    
            def _canon(txt: str) -> str:
                return str(txt or "").upper().replace(" ", "").replace("-", "")
    
            _loc_keys = {_canon(loc_code), _canon(loc_name)}
            is_asemoku = bool(_loc_keys & {"JETTY", "ASEMOKU", "ASEMOKUJETTY"})
            is_aggu    = bool(_loc_keys & {"AGGU"})
            is_agge    = bool(_loc_keys & {"AGGE"})
            is_ndoni   = bool(_loc_keys & {"NDONI"})
            # Beneku (BFS) - handle code or name
            is_bfs     = bool(_loc_keys & {"BFS", "BENEKU", "BENEKU(BFS)"})
            is_utapate = bool(_loc_keys & {"UTAPATE", "OML13", "OML-13"})
            # Oguali (OML-157) � OFS Production & Evacuation summary dashboard
            is_ofs_oguali = bool(
                _loc_keys
                & {
                    "OML157",
                    "OGUALI",
                    "OGUALI(OML157)",
                    "OGUALIOML157",
                }
            )
    
            if is_asemoku or is_ndoni:
                river_target_date = _selected_date + timedelta(days=1)
                river_display_date = river_target_date
                rd_row = (
                    s.query(RiverDraftRecord)
                    .filter(
                        RiverDraftRecord.location_id == active_location_id,
                        RiverDraftRecord.date == river_target_date,
                    )
                    .order_by(RiverDraftRecord.id.desc())
                    .first()
                )
                if rd_row:
                    try:
                        river_draft_value = float(getattr(rd_row, "river_draft_m", 0.0) or 0.0)
                    except Exception:
                        river_draft_value = 0.0
                    try:
                        rainfall_value = float(getattr(rd_row, "rainfall_cm", 0.0) or 0.0)
                    except Exception:
                        rainfall_value = 0.0
    
                prev_date = river_target_date - timedelta(days=1)
                prev_row = (
                    s.query(RiverDraftRecord)
                    .filter(
                        RiverDraftRecord.location_id == active_location_id,
                        RiverDraftRecord.date == prev_date,
                    )
                    .order_by(RiverDraftRecord.id.desc())
                    .first()
                )
                if prev_row:
                    try:
                        prev_river_draft_value = float(getattr(prev_row, "river_draft_m", 0.0) or 0.0)
                    except Exception:
                        prev_river_draft_value = 0.0
                    try:
                        prev_rainfall_value = float(getattr(prev_row, "rainfall_cm", 0.0) or 0.0)
                    except Exception:
                        prev_rainfall_value = 0.0
    
            if is_asemoku:
                pw_rows = (
                    s.query(ProducedWaterRecord)
                    .filter(
                        ProducedWaterRecord.location_id == active_location_id,
                        ProducedWaterRecord.date == _selected_date,
                    )
                    .all()
                )
                if pw_rows:
                    produced_water_bbl = sum(float(getattr(row, "produced_water_bbl", 0.0) or 0.0) for row in pw_rows)
                pw_prev_rows = (
                    s.query(ProducedWaterRecord)
                    .filter(
                        ProducedWaterRecord.location_id == active_location_id,
                        ProducedWaterRecord.date == prev_summary_date,
                    )
                    .all()
                )
                if pw_prev_rows:
                    prev_produced_water_bbl = sum(
                        float(getattr(row, "produced_water_bbl", 0.0) or 0.0) for row in pw_prev_rows
                    )
    
            # -------- OFS (Oguali) values from production & evacuation records --------
            if is_ofs_oguali:
                ofs_rows = (
                    s.query(OFSProductionEvacuationRecord)
                    .filter(
                        OFSProductionEvacuationRecord.location_id == active_location_id,
                        OFSProductionEvacuationRecord.date == _selected_date,
                    )
                    .all()
                )
                ofs_prev_rows = (
                    s.query(OFSProductionEvacuationRecord)
                    .filter(
                        OFSProductionEvacuationRecord.location_id == active_location_id,
                        OFSProductionEvacuationRecord.date == prev_summary_date,
                    )
                    .all()
                )
    
                def _ofs_sum(rows, attr: str) -> float:
                    return sum(float(getattr(row, attr) or 0.0) for row in rows or [])
    
                if ofs_rows:
                    ofs_oguali_prod_bbl = _ofs_sum(ofs_rows, "oguali_production")
                    ofs_ukpichi_prod_bbl = _ofs_sum(ofs_rows, "ukpichi_production")
                    ofs_other_prod_bbl = _ofs_sum(ofs_rows, "other_locations")
                    ofs_evacuation_bbl = _ofs_sum(ofs_rows, "evacuation")
                    ofs_tankers_oguali = _ofs_sum(ofs_rows, "tankers_oguali")
                    ofs_tankers_ukpichi = _ofs_sum(ofs_rows, "tankers_ukpichi")
                    ofs_tankers_other = _ofs_sum(ofs_rows, "other_tankers")
                if ofs_prev_rows:
                    prev_ofs_oguali_prod_bbl = _ofs_sum(ofs_prev_rows, "oguali_production")
                    prev_ofs_ukpichi_prod_bbl = _ofs_sum(ofs_prev_rows, "ukpichi_production")
                    prev_ofs_other_prod_bbl = _ofs_sum(ofs_prev_rows, "other_locations")
    
            # -------- Material Balance for selected date (generic) --------
            df_mb = None
            df_mb_prev = None
            try:
                from material_balance_calculator import MaterialBalanceCalculator as MBCalc
                mb_rows = MBCalc.calculate_material_balance(
                    entries=None,
                    location_code=(loc_code or "").upper(),
                    date_from=_selected_date,
                    date_to=_selected_date,
                    location_id=active_location_id,
                    debug=False
                )
                if mb_rows:
                    df_mb = pd.DataFrame(mb_rows)
                prev_rows = MBCalc.calculate_material_balance(
                    entries=None,
                    location_code=(loc_code or "").upper(),
                    date_from=prev_summary_date,
                    date_to=prev_summary_date,
                    location_id=active_location_id,
                    debug=False,
                )
                if prev_rows:
                    df_mb_prev = pd.DataFrame(prev_rows)
            except Exception:
                pass
    
            # Generic (for locations that use "Receipt"/"Dispatch" columns)
            if df_mb is not None:
                if "Receipt" in df_mb.columns:
                    prod_bbl = float(pd.to_numeric(df_mb["Receipt"], errors="coerce").fillna(0).sum())
                if "Dispatch" in df_mb.columns:
                    evac_bbl = float(pd.to_numeric(df_mb["Dispatch"], errors="coerce").fillna(0).sum())
            if df_mb_prev is not None:
                if "Receipt" in df_mb_prev.columns:
                    prev_prod_bbl = float(pd.to_numeric(df_mb_prev["Receipt"], errors="coerce").fillna(0).sum())
                if "Dispatch" in df_mb_prev.columns:
                    prev_evac_bbl = float(pd.to_numeric(df_mb_prev["Dispatch"], errors="coerce").fillna(0).sum())
    
            # Helper to find columns case/space insensitive
            def _find_col(df: pd.DataFrame, candidates):
                if df is None or df.empty:
                    return None
                # exact first
                for c in candidates:
                    if c in df.columns:
                        return c
                lower_map = {str(c).strip().lower(): c for c in df.columns}
                for c in candidates:
                    lc = str(c).strip().lower()
                    if lc in lower_map:
                        return lower_map[lc]
                return None
    
            def _sum_bbl(df: pd.DataFrame, col_name: str | None) -> float | None:
                if not col_name or col_name not in df.columns:
                    return None
                ser = df[col_name]
                try:
                    ser = ser.astype(str).str.replace(",", "", regex=False)
                except Exception:
                    pass
                ser = pd.to_numeric(ser, errors="coerce")
                return float(ser.fillna(0.0).sum())
    
            # -------- Asemoku Jetty specific mapping --------
            # ANZ Production  -> "ANZ Receipt"
            # BFS Receipt     -> "OKW Receipt"
            # Evacuation      -> "Dispatch to barge"
            if is_asemoku and df_mb is not None:
                c_anz = _find_col(df_mb, ["ANZ Receipt"])
                c_okw = _find_col(df_mb, ["OKW Receipt"])
                c_disp_barge = _find_col(df_mb, ["Dispatch to barge"])
    
                if c_anz:
                    val = _sum_bbl(df_mb, c_anz)
                    if val is not None:
                        anz_prod_bbl = val
                if c_okw:
                    val = _sum_bbl(df_mb, c_okw)
                    if val is not None:
                        okw_receipt_bbl = val
                if c_disp_barge:
                    val = _sum_bbl(df_mb, c_disp_barge)
                    if val is not None:
                        dispatch_barge_bbl = val
            if is_asemoku and df_mb_prev is not None:
                c_prev_anz = _find_col(df_mb_prev, ["ANZ Receipt"])
                c_prev_okw = _find_col(df_mb_prev, ["OKW Receipt"])
                if c_prev_anz:
                    val = _sum_bbl(df_mb_prev, c_prev_anz)
                    if val is not None:
                        prev_anz_prod_bbl = val
                if c_prev_okw:
                    val = _sum_bbl(df_mb_prev, c_prev_okw)
                    if val is not None:
                        prev_okw_receipt_bbl = val
    
            # -------- Beneku (BFS) specific mapping (Production tab + Evacuation from MB) --------
            if is_bfs:
                try:
                    from models import GPPProductionRecord
                    def _calc_bfs_totals(rows):
                        if not rows:
                            return None, None
                        okw_total = sum(float(r.okw_production or 0.0) for r in rows)
                        gpp_total = sum(
                            float(
                                r.total_production
                                if r.total_production is not None
                                else (r.gpp1_production or 0.0) + (r.gpp2_production or 0.0)
                            )
                            for r in rows
                        )
                        return okw_total, gpp_total
    
                    prod_rows = (
                        s.query(GPPProductionRecord)
                        .filter(
                            GPPProductionRecord.location_id == active_location_id,
                            GPPProductionRecord.date == _selected_date,
                        )
                        .all()
                    )
                    prev_prod_rows = (
                        s.query(GPPProductionRecord)
                        .filter(
                            GPPProductionRecord.location_id == active_location_id,
                            GPPProductionRecord.date == prev_summary_date,
                        )
                        .all()
                    )
                    if prod_rows:
                        okw_prod_bbl, gpp_prod_bbl = _calc_bfs_totals(prod_rows)
                    if prev_prod_rows:
                        prev_okw_prod_bbl, prev_gpp_prod_bbl = _calc_bfs_totals(prev_prod_rows)
                except Exception:
                    pass
    
            if is_bfs and df_mb is not None:
                c_disp_bfs = _find_col(df_mb, ["Dispatch to Jetty"])
                if c_disp_bfs:
                    val = _sum_bbl(df_mb, c_disp_bfs)
                    if val is not None:
                        bfs_evac_bbl = val
    
            # -------- Aggu tanker dispatched (from tanker entries) --------
            if is_aggu:
                try:
                    agg_value = (
                        s.query(func.sum(LocationTankerEntry.tankers_dispatched))
                        .filter(
                            LocationTankerEntry.location_id == active_location_id,
                            LocationTankerEntry.date == _selected_date,
                        )
                        .scalar()
                    )
                    if agg_value is not None:
                        aggu_tankers_dispatched = float(agg_value or 0.0)
                except Exception:
                    pass
    
            # -------- Agge specific wiring (YADE, OTR-Vessel, FSO MB) --------
            if is_agge:
                try:
                    from models import TOAYadeSummary, YadeVoyage, OTRVessel, FSOOperation
                    from fso_config import FSOConfig
                except Exception:
                    pass
                else:
                    yade_vals = (
                        s.query(TOAYadeSummary.gsv_loaded_bbl)
                        .join(YadeVoyage, TOAYadeSummary.voyage_id == YadeVoyage.id)
                        .filter(
                            YadeVoyage.location_id == active_location_id,
                            TOAYadeSummary.date == _selected_date,
                        )
                        .all()
                    )
                    if yade_vals:
                        agge_receipt_from_yade_bbl = sum(
                            abs(float(val or 0.0)) for (val,) in yade_vals
                        )
    
                    otr_vals = (
                        s.query(OTRVessel.net_receipt_dispatch)
                        .filter(
                            OTRVessel.location_id == active_location_id,
                            OTRVessel.date == _selected_date,
                        )
                        .all()
                    )
                    if otr_vals:
                        agge_evacuation_bbl = sum(
                            abs(float(val or 0.0)) for (val,) in otr_vals
                        )
    
                    fso_vessel = None
                    loc_candidates = []
                    if loc_code:
                        loc_candidates.append(loc_code.upper())
                    if loc_name:
                        loc_candidates.append(loc_name.upper())
                    canon_loc_code = _canon(loc_code)
                    canon_loc_name = _canon(loc_name)
                    if canon_loc_code == "AGGE" or canon_loc_name == "AGGE":
                        loc_candidates.append("AGGE")
    
                    for candidate in loc_candidates:
                        if not candidate:
                            continue
                        vessels = FSOConfig.get_fso_for_location(candidate)
                        if vessels:
                            fso_vessel = vessels[0]
                            break
    
                    if not fso_vessel:
                        fso_vessel = FSOConfig.get_default_fso("AGGE")
    
                    if fso_vessel:
                        period_start = datetime.combine(_selected_date, dt_time(6, 1))
                        period_end = datetime.combine(_selected_date + timedelta(days=1), dt_time(6, 0))
    
                        fso_rows = (
                            s.query(FSOOperation)
                            .filter(
                                FSOOperation.location_id == active_location_id,
                                FSOOperation.fso_vessel == fso_vessel,
                                FSOOperation.date >= (_selected_date - timedelta(days=1)),
                                FSOOperation.date <= (_selected_date + timedelta(days=1)),
                            )
                            .order_by(FSOOperation.date, FSOOperation.time)
                            .all()
                        )
    
                        window_entries = []
                        for entry in fso_rows or []:
                            entry_time = _coerce_time(entry.time)
                            if not entry_time:
                                continue
                            entry_dt = datetime.combine(entry.date, entry_time)
                            if period_start <= entry_dt <= period_end:
                                window_entries.append((entry, entry_dt))
    
                        if window_entries:
                            window_entries.sort(key=lambda pair: pair[1])
                            agge_fso_receipt_bbl = sum(
                                abs(float(pair[0].net_receipt_dispatch or 0.0))
                                for pair in window_entries
                                if pair[0].operation == "Receipt" and pair[0].net_receipt_dispatch is not None
                            )
                            last_entry = window_entries[-1][0]
                            try:
                                agge_fso_stock_bbl = float(last_entry.closing_stock or 0.0)
                            except Exception:
                                agge_fso_stock_bbl = None
    
            # -------- Ndoni specific mapping --------
            def _ndoni_values_from_df(df: pd.DataFrame):
                if df is None or df.empty:
                    return None, None, None, None
    
                cols_raw = list(df.columns)
    
                def _col_by_idx(i):
                    try:
                        return cols_raw[i]
                    except Exception:
                        return None
    
                agu_candidates = [
                    "Receipt from Agu",
                    "Receipt from agu",
                    "Receipt from AGU",
                    "Receipt Agu",
                    "Receipt AGU",
                    "Receipt From Agu",
                    "Receipt from Agu (bbls)",
                    "Receipt from AGU (bbls)",
                    "Receipt Agu (bbls)",
                    "Receipt AGU (bbls)",
                ]
                c_rcpt_agu = _find_col(df, agu_candidates) or _col_by_idx(2)
                c_rcpt_ofs = _find_col(df, ["Receipt from OFS", "Receipt from ofs"]) or _col_by_idx(3)
                c_rcpt_other = _find_col(df, ["Other Receipts", "Other receipts"]) or _col_by_idx(4)
                c_disp_barge_nd = _find_col(df, ["Dispatch to barge", "Dispatch to Barge"]) or _col_by_idx(5)
    
                first_row = df.iloc[0]
    
                def _value_from_row(col_name):
                    if not col_name:
                        return None
                    if col_name not in first_row.index:
                        return None
                    raw = first_row[col_name]
                    if raw is None:
                        return None
                    try:
                        return float(str(raw).replace(",", ""))
                    except Exception:
                        try:
                            return float(raw)
                        except Exception:
                            return None
    
                return (
                    _value_from_row(c_rcpt_agu),
                    _value_from_row(c_rcpt_ofs),
                    _value_from_row(c_rcpt_other),
                    _value_from_row(c_disp_barge_nd),
                )
    
            nd_receipt_agu_bbl = nd_receipt_ofs_bbl = nd_other_rcpt_bbl = nd_dispatch_barge_bbl = None
            if is_ndoni:
                (
                    nd_receipt_agu_bbl,
                    nd_receipt_ofs_bbl,
                    nd_other_rcpt_bbl,
                    nd_dispatch_barge_bbl,
                ) = _ndoni_values_from_df(df_mb)
                (
                    prev_nd_receipt_agu_bbl,
                    prev_nd_receipt_ofs_bbl,
                    prev_nd_other_rcpt_bbl,
                    _,
                ) = _ndoni_values_from_df(df_mb_prev)
    
            if is_ndoni:
                try:
                    tanker_sums = (
                        s.query(
                            func.sum(LocationTankerEntry.tankers_from_aggu),
                            func.sum(LocationTankerEntry.tankers_from_ofs),
                            func.sum(LocationTankerEntry.other_tankers),
                        )
                        .filter(
                            LocationTankerEntry.location_id == active_location_id,
                            LocationTankerEntry.date == _selected_date,
                        )
                        .one_or_none()
                    )
                    if tanker_sums:
                        nd_tankers_from_aggu = float(tanker_sums[0] or 0.0)
                        nd_tankers_from_ofs = float(tanker_sums[1] or 0.0)
                        nd_other_tankers = float(tanker_sums[2] or 0.0)
                except Exception:
                    pass
    
            # -------- FSO Material Balance (kept as before; not used for BFS/Aggu cards) --------
            try:
                from fso_config import FSOConfig
                from models import FSOOperation
    
                def _canonical_fso_code(code: str | None) -> str:
                    if not code:
                        return ""
                    s = str(code).strip().upper()
                    s_norm = s.replace(" ", "").replace("-", "")
                    aliases = {
                        "UTAPATE": "OML-13",
                        "OML13": "OML-13",
                        "OML-13": "OML-13",
                        "OML 13": "OML-13",
                        "AGGE": "AGGE",
                    }
                    return aliases.get(s, aliases.get(s_norm, s))
    
                loc_code_up = (loc_code or "").upper()
    
                def _fso_values_from_mb_page(selected_date, loc_code_inner):
                    possible_keys = [
                        "fso_mb_df", "fso_mb_table", "fso_material_balance_df",
                        "fso_mb_daily", "fso_mb_cache", "fso_mb_summary_df",
                        "fso_mb_pivot", "fso_mb_records"
                    ]
                    canon_loc = _canonical_fso_code(loc_code_inner)
                    for key in possible_keys:
                        if key not in st.session_state:
                            continue
                        obj = st.session_state[key]
                        try:
                            df = obj.copy() if isinstance(obj, pd.DataFrame) else pd.DataFrame(obj)
                        except Exception:
                            continue
                        if df is None or df.empty:
                            continue
    
                        # date column
                        date_col = None
                        for c in df.columns:
                            if str(c).strip().lower() in {"date", "mb date", "as of", "asof"}:
                                date_col = c
                                break
                        if date_col is None:
                            continue
    
                        dfx = df.copy()
                        dfx[date_col] = pd.to_datetime(dfx[date_col], errors="coerce").dt.date
                        dfx = dfx[dfx[date_col] == selected_date]
                        if dfx.empty:
                            continue
    
                        # optional loc filter
                        loc_col = None
                        for c in dfx.columns:
                            if str(c).strip().lower() in {"location", "loc", "site", "code"}:
                                loc_col = c
                                break
                        if loc_col is not None and canon_loc:
                            dfx["_fso_loc_norm"] = dfx[loc_col].astype(str).str.upper().str.replace(" ", "", regex=False).str.replace("-", "", regex=False)
                            dfx = dfx[dfx["_fso_loc_norm"] == canon_loc.replace("-", "").replace(" ", "")]
                            if dfx.empty:
                                continue
    
                        # receipt / closing stock columns
                        rec_cols = [
                            "Receipt (bbls)", "Receipts (bbls)", "receipt (bbls)", "receipts (bbls)",
                            "receipt_bbls", "receipts_bbls", "receipt", "receipts"
                        ]
                        close_cols = [
                            "Closing Stock (bbls)", "closing stock (bbls)", "Closing Stock",
                            "closing stock", "closing_stock", "closing_stock_bbl"
                        ]
                        rec_col = next((c for c in dfx.columns if str(c).strip() in rec_cols), None) \
                                or next((c for c in dfx.columns if str(c).strip().lower() in [x.lower() for x in rec_cols]), None)
                        close_col = next((c for c in dfx.columns if str(c).strip() in close_cols), None) \
                                or next((c for c in dfx.columns if str(c).strip().lower() in [x.lower() for x in close_cols]), None)
    
                        rec_val = float(pd.to_numeric(dfx[rec_col], errors="coerce").fillna(0).sum()) if rec_col else None
                        close_val = None
                        if close_col:
                            cs = pd.to_numeric(dfx[close_col], errors="coerce").dropna()
                            if not cs.empty:
                                close_val = float(cs.iloc[-1])
                        if rec_val is not None or close_val is not None:
                            return rec_val, close_val
    
                    return None, None
                fso_receipt_bbl, fso_stock_bbl = _fso_values_from_mb_page(_selected_date, loc_code_up)
    
                if fso_receipt_bbl is None or fso_stock_bbl is None:
                    # fallback compute
                    fso_map = {k.upper(): v for k, v in FSOConfig.get_fso_locations().items()}
                    fso_vessel = fso_map.get(_canonical_fso_code(loc_code_up))
                    if fso_vessel:
                        ext_from = _selected_date - timedelta(days=1)
                        ext_to   = _selected_date + timedelta(days=1)
                        entries = (s.query(FSOOperation)
                                    .filter(
                                        FSOOperation.location_id == active_location_id,
                                        FSOOperation.fso_vessel == fso_vessel,
                                        FSOOperation.date >= ext_from,
                                        FSOOperation.date <= ext_to,
                                    )
                                    .order_by(FSOOperation.date, FSOOperation.time)
                                    .all())
                        if entries:
                            win_start = datetime.combine(_selected_date, dt_time(6, 1))
                            win_end   = datetime.combine(_selected_date + timedelta(days=1), dt_time(6, 0))
    
                            def _to_time(t):
                                if isinstance(t, dt_time):
                                    return t
                                try:
                                    return datetime.strptime(str(t), "%H:%M").time()
                                except Exception:
                                    return dt_time(0, 0)
    
                            period = []
                            for e in entries:
                                try:
                                    edt = datetime.combine(e.date, _to_time(e.time))
                                    if win_start <= edt <= win_end:
                                        period.append(e)
                                except Exception:
                                    continue
    
                            if period:
                                period.sort(key=lambda e: datetime.combine(e.date, _to_time(e.time)))
                                # receipts
                                rec_total = 0.0
                                for e in period:
                                    op = (getattr(e, "operation", "") or "").strip().lower()
                                    if op in {"receipt", "receipts"}:
                                        try:
                                            v = float(getattr(e, "net_receipt_dispatch", 0.0) or 0.0)
                                        except Exception:
                                            v = 0.0
                                        if v > 0:
                                            rec_total += v
                                if fso_receipt_bbl is None:
                                    fso_receipt_bbl = rec_total
    
                                # closing stock
                                last_e = period[-1]
                                try:
                                    if fso_stock_bbl is None:
                                        fso_stock_bbl = float(getattr(last_e, "closing_stock", 0.0) or 0.0)
                                except Exception:
                                    pass
            except Exception:
                pass
    
            # === Ullage available & Pumpable Stock (status-dependent; unchanged logic) ===
            from sqlalchemy import and_, or_
            from models import OTRRecord, TankTransaction, Tank
            try:
                from models import TankDailyStatus, TankOpStatus
            except Exception:
                TankDailyStatus = None
                TankOpStatus = None
    
            ullage_available_bbl = 0.0
            pumpable_stock_bbl   = 0.0
    
            _mb_close_date = _selected_date + timedelta(days=1)
            _mb_close_time = dt_time(6, 0)
            _allowed_status = {"IDLE", "READY", "DISPATCHING"}
    
            tanks_ul = s.query(Tank).filter(Tank.location_id == active_location_id).all()
    
            for tnk in tanks_ul:
                # capacity
                try:
                    cap = float(getattr(tnk, "capacity_bbl", 0.0) or 0.0)
                except Exception:
                    cap = 0.0
    
                # stock @ MB close
                last_txn = (
                    s.query(TankTransaction.ticket_id, TankTransaction.date, TankTransaction.time)
                    .filter(TankTransaction.tank_id == tnk.id)
                    .filter(
                        or_(
                            TankTransaction.date < _mb_close_date,
                            and_(TankTransaction.date == _mb_close_date, TankTransaction.time <= _mb_close_time)
                        )
                    )
                    .order_by(TankTransaction.date.desc(), TankTransaction.time.desc())
                    .first()
                )
    
                stock = 0.0
                if last_txn and last_txn.ticket_id:
                    _otr = s.query(OTRRecord).filter(OTRRecord.ticket_id == last_txn.ticket_id).first()
                    try:
                        stock = float(getattr(_otr, "nsv_bbl", 0.0) or 0.0)
                    except Exception:
                        stock = 0.0
    
                # ullage always counts
                ullage_available_bbl += max(cap - stock, 0.0)
    
                # status-gated pumpable
                status_name = "READY"
                try:
                    if TankDailyStatus is not None:
                        _row = (s.query(TankDailyStatus)
                                .filter(TankDailyStatus.tank_id == tnk.id,
                                        TankDailyStatus.date == _selected_date)
                                .first())
                        if _row is not None:
                            status_name = getattr(_row.op_status, "name", str(_row.op_status) or "READY")
                    elif hasattr(tnk, "status") and getattr(tnk, "status") is not None:
                        status_name = str(getattr(tnk, "status") or "READY")
                except Exception:
                    pass
    
                if str(status_name).strip().upper() in _allowed_status:
                    pumpable_stock_bbl += stock
    
            # percentages
            ullage_available_bbl = 0.90 * ullage_available_bbl
            pumpable_stock_bbl   = 0.85 * pumpable_stock_bbl
    
        # --------- Helper for formatting ---------
        def _fmt(v):
            return f"{v:,.0f}" if isinstance(v, (int, float)) else "-"
    
        def _value_with_delta(
            curr: Optional[float],
            prev: Optional[float],
            unit: str = "",
            fmt: str = "{:,.0f}",
        ) -> str:
            unit_suffix = f" {unit}" if unit else ""
    
            def _fmt_value(val: float | None) -> str:
                if val is None:
                    return "-"
                try:
                    return fmt.format(val)
                except Exception:
                    return f"{val}"
    
            if curr is None:
                base = "-"
            else:
                base = f"{_fmt_value(curr)}{(' ' + unit) if unit else ''}"
            if curr is None or prev is None:
                return base
            diff = curr - prev
            if abs(diff) < 1e-6:
                return (
                    f"{base} "
                    f"<span style='color:#6c757d;margin-left:6px;font-size:0.95rem;'>&harr; 0"
                    f"{unit_suffix}</span>"
                )
            arrow = "&uarr;" if diff > 0 else "&darr;"
            color = "#198754" if diff > 0 else "#dc3545"
            return (
                f"{base} "
                f"<span style='color:{color};margin-left:6px;font-size:0.95rem;'>"
                f"{arrow} {_fmt_value(abs(diff))}{unit_suffix}</span>"
            )
    
        # --------- Render cards (location-aware layout) ---------
        if is_asemoku:
            # 6 cards: Anz Prod, BFS Receipt, Evacuation, Produced Water, Ullage, Pumpable
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            anz_html = _value_with_delta(anz_prod_bbl, prev_anz_prod_bbl, "", "{:,.0f}")
            bfs_html = _value_with_delta(okw_receipt_bbl, prev_okw_receipt_bbl, "", "{:,.0f}")
            produced_water_html = _value_with_delta(produced_water_bbl, prev_produced_water_bbl, "", "{:,.0f}")
    
            with c1:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Anz Production</div>
                        <div class="stat-value">{anz_html}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c2:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">BFS Receipt</div>
                        <div class="stat-value">{bfs_html}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c3:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Evacuation</div>
                        <div class="stat-value">{_fmt(dispatch_barge_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c4:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Produced Water</div>
                        <div class="stat-value">{produced_water_html}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c5:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Ullage available</div>
                        <div class="stat-value">{_fmt(ullage_available_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c6:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Pumpable Stock</div>
                        <div class="stat-value">{_fmt(pumpable_stock_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            rd_html = _value_with_delta(river_draft_value, prev_river_draft_value, "m", "{:.2f}")
            rain_html = _value_with_delta(rainfall_value, prev_rainfall_value, "cm", "{:.2f}")
            label = river_display_date.strftime("%d-%b-%Y @ 06:00 hrs") if river_display_date else _selected_date.strftime("%d-%b-%Y")
            st.markdown(
                f"<div style='margin-top:0.75rem;font-size:1.05rem;font-weight:600;'>"
                f"River Draft: {rd_html}<br/>Rainfall: {rain_html}<br/>"
                f"<span style='color:#6c757d;font-size:0.9rem;'>Data for {label}</span></div>",
                unsafe_allow_html=True,
            )
    
        elif is_agge:
            # AGGE: 4 cards - Receipt from Yade, Evacuation (OTR), FSO Receipt, FSO Stock
            c1, c2, c3, c4 = st.columns(4)
    
            with c1:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Receipt from Yade</div>
                        <div class="stat-value">{_fmt(agge_receipt_from_yade_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c2:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Evacuation</div>
                        <div class="stat-value">{_fmt(agge_evacuation_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c3:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">FSO Receipt</div>
                        <div class="stat-value">{_fmt(agge_fso_receipt_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c4:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">FSO Stock</div>
                        <div class="stat-value">{_fmt(agge_fso_stock_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
        elif is_ofs_oguali:
            # Oguali/OML-157 summary cards sourced from OFS Production & Evacuation tab
            c1, c2, c3, c4, c5 = st.columns(5)
            ofs_oguali_html = _value_with_delta(ofs_oguali_prod_bbl, prev_ofs_oguali_prod_bbl, "", "{:,.0f}")
            ofs_ukpichi_html = _value_with_delta(ofs_ukpichi_prod_bbl, prev_ofs_ukpichi_prod_bbl, "", "{:,.0f}")
            ofs_other_html = _value_with_delta(ofs_other_prod_bbl, prev_ofs_other_prod_bbl, "", "{:,.0f}")
    
            with c1:
                st.markdown(
                    f"""
                    <div class="stat-card">
                        <div class="stat-label">Oguali Production</div>
                        <div class="stat-value">{ofs_oguali_html}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    
            with c2:
                st.markdown(
                    f"""
                    <div class="stat-card">
                        <div class="stat-label">Ukpichi Production</div>
                        <div class="stat-value">{ofs_ukpichi_html}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    
            with c3:
                st.markdown(
                    f"""
                    <div class="stat-card">
                        <div class="stat-label">Other Locations Production</div>
                        <div class="stat-value">{ofs_other_html}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    
            with c4:
                st.markdown(
                    f"""
                    <div class="stat-card">
                        <div class="stat-label">Evacuation</div>
                        <div class="stat-value">{_fmt(ofs_evacuation_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    
            total_tankers = ofs_tankers_oguali + ofs_tankers_ukpichi + ofs_tankers_other
            with c5:
                st.markdown(
                    f"""
                    <div class="stat-card">
                        <div class="stat-label">Number of Tankers</div>
                        <div class="stat-value">{_fmt(total_tankers)}</div>
                        <div style="color:#6c757d;font-size:0.78rem; margin-top:0.5rem;">
                            <div style="display:flex; justify-content:space-between;">
                                <span>From Oguali</span><strong>{_fmt(ofs_tankers_oguali)}</strong>
                            </div>
                            <div style="display:flex; justify-content:space-between;">
                                <span>From Ukpichi</span><strong>{_fmt(ofs_tankers_ukpichi)}</strong>
                            </div>
                            <div style="display:flex; justify-content:space-between;">
                                <span>From Other Locations</span><strong>{_fmt(ofs_tankers_other)}</strong>
                            </div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    
            # Oguali dashboard stops after summary cards (no monthly/trend sections required)
            st.stop()
    
        elif is_aggu:
            # AGGU: 5 cards - Production, Evacuation, Tankers Dispatched, Ullage, Pumpable
            c1, c2, c3, c4, c5 = st.columns(5)
            aggu_prod_html = _value_with_delta(prod_bbl, prev_prod_bbl, "", "{:,.0f}")
    
            with c1:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Production</div>
                        <div class="stat-value">{aggu_prod_html}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c2:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Evacuation</div>
                        <div class="stat-value">{_fmt(evac_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c3:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Tankers Dispatched</div>
                        <div class="stat-value">{_fmt(aggu_tankers_dispatched)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c4:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Ullage available</div>
                        <div class="stat-value">{_fmt(ullage_available_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c5:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Pumpable Stock</div>
                        <div class="stat-value">{_fmt(pumpable_stock_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
        elif is_bfs:
            # BENEKU (BFS): 5 cards - OKW Production, GPP Production, Evacuation, Ullage, Pumpable
            c1, c2, c3, c4, c5 = st.columns(5)
            okw_prod_html = _value_with_delta(okw_prod_bbl, prev_okw_prod_bbl, "", "{:,.0f}")
            gpp_prod_html = _value_with_delta(gpp_prod_bbl, prev_gpp_prod_bbl, "", "{:,.0f}")
    
            with c1:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">OKW Production</div>
                        <div class="stat-value">{okw_prod_html}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c2:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">GPP Production</div>
                        <div class="stat-value">{gpp_prod_html}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c3:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Evacuation</div>
                        <div class="stat-value">{_fmt(bfs_evac_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c4:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Ullage available</div>
                        <div class="stat-value">{_fmt(ullage_available_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c5:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Pumpable Stock</div>
                        <div class="stat-value">{_fmt(pumpable_stock_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
        elif is_ndoni:
            # Ndoni: 6 cards - Receipt Agu, Receipt OFS, Other Receipts, Evacuation, Ullage, Pumpable
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            nd_receipt_agu_html = _value_with_delta(nd_receipt_agu_bbl, prev_nd_receipt_agu_bbl, "", "{:,.0f}")
            nd_receipt_ofs_html = _value_with_delta(nd_receipt_ofs_bbl, prev_nd_receipt_ofs_bbl, "", "{:,.0f}")
            nd_other_receipt_html = _value_with_delta(nd_other_rcpt_bbl, prev_nd_other_rcpt_bbl, "", "{:,.0f}")
    
            with c1:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Receipt from Agu</div>
                        <div class="stat-value">{nd_receipt_agu_html}</div>
                        <div style="color:#6c757d;font-size:0.78rem;">Tankers: {_fmt(nd_tankers_from_aggu)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c2:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Receipt from OFS</div>
                        <div class="stat-value">{nd_receipt_ofs_html}</div>
                        <div style="color:#6c757d;font-size:0.78rem;">Tankers: {_fmt(nd_tankers_from_ofs)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c3:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Other Receipts</div>
                        <div class="stat-value">{nd_other_receipt_html}</div>
                        <div style="color:#6c757d;font-size:0.78rem;">Tankers: {_fmt(nd_other_tankers)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c4:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Evacuation</div>
                        <div class="stat-value">{_fmt(nd_dispatch_barge_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c5:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Ullage available</div>
                        <div class="stat-value">{_fmt(ullage_available_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c6:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Pumpable Stock</div>
                        <div class="stat-value">{_fmt(pumpable_stock_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            rd_html = _value_with_delta(river_draft_value, prev_river_draft_value, "m", "{:.2f}")
            rain_html = _value_with_delta(rainfall_value, prev_rainfall_value, "cm", "{:.2f}")
            label = river_display_date.strftime("%d-%b-%Y @ 06:00 hrs") if river_display_date else _selected_date.strftime("%d-%b-%Y")
            st.markdown(
                f"<div style='margin-top:0.75rem;font-size:1.05rem;font-weight:600;'>"
                f"River Draft: {rd_html}<br/>Rainfall: {rain_html}<br/>"
                f"<span style='color:#6c757d;font-size:0.9rem;'>Data for {label}</span></div>",
                unsafe_allow_html=True,
            )
    
        else:
            # Other locations: keep original 6 cards (Production, Evac, FSO, Ullage, Pumpable)
            c1, c2, c3, c4, c5, c6 = st.columns(6)
            default_prod_html = (
                _value_with_delta(prod_bbl, prev_prod_bbl, "", "{:,.0f}")
                if is_utapate
                else _fmt(prod_bbl)
            )
    
            with c1:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Production</div>
                        <div class="stat-value">{default_prod_html}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c2:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Evacuation</div>
                        <div class="stat-value">{_fmt(evac_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c3:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">FSO receipt</div>
                        <div class="stat-value">{_fmt(fso_receipt_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c4:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">FSO Stock</div>
                        <div class="stat-value">{_fmt(fso_stock_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c5:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Ullage available</div>
                        <div class="stat-value">{_fmt(ullage_available_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
            with c6:
                st.markdown(f"""
                    <div class="stat-card">
                        <div class="stat-label">Pumpable Stock</div>
                        <div class="stat-value">{_fmt(pumpable_stock_bbl)}</div>
                        <div style="color:#6c757d;font-size:0.8rem;">{_selected_date.strftime('%d-%b-%Y')}</div>
                    </div>
                """, unsafe_allow_html=True)
    
        # ============ TANK VISUALIZATIONS (3D CYLINDRICAL - COMPACT) ============
        if can_view_tanks and not is_agge_location:
            st.markdown("### 🛢️ Tank Stock Levels")
            
            # Use the same dashboard date if available; otherwise default to today
            from datetime import date as _date, timedelta
            selected_date = st.session_state.get("dash_date_all_sites", _date.today())
    
            # Models used in this section
            from sqlalchemy import func as sa_func
            from models import OTRRecord
            try:
                from models import TankDailyStatus, TankOpStatus
            except Exception:
                TankDailyStatus = None
                TankOpStatus = None
    
            # Auto-save callback for per-tank daily status (selector at bottom of each card)
            def _save_tank_status_cb(_key_name, _tank_id, _the_date):
                if TankDailyStatus is None or TankOpStatus is None:
                    return
                new_val = st.session_state.get(_key_name)
                if not new_val:
                    return
                with get_session() as _s:
                    row = (_s.query(TankDailyStatus)
                        .filter(TankDailyStatus.tank_id == _tank_id,
                                TankDailyStatus.date == _the_date)
                        .first())
                    if row is None:
                        row = TankDailyStatus(tank_id=_tank_id, date=_the_date, op_status=TankOpStatus[new_val])
                        _s.add(row)
                    else:
                        row.op_status = TankOpStatus[new_val]
                    # Commit the status change
                    _s.commit()
                    # ----------------------- Audit log for tank daily status save -----------------------
                    try:
                        from security import SecurityManager  # type: ignore
                        # Fetch user context from session state
                        user_ctx = st.session_state.get("auth_user") or {}
                        username = user_ctx.get("username", "unknown")
                        user_id = user_ctx.get("id")
                        # Determine action type based on whether the row existed
                        action_type = "CREATE" if row is None else "UPDATE"
                        # Create a composite ID using tank and date for the record
                        rec_id = f"{_tank_id}-{_the_date.isoformat()}"
                        SecurityManager.log_audit(
                            None,
                            username,
                            action_type,
                            resource_type="TankDailyStatus",
                            resource_id=rec_id,
                            details=f"{action_type.title()} tank daily status to {new_val}",
                            user_id=user_id,
                            location_id=active_location_id,
                        )
                    except Exception:
                        # Do not interrupt the user flow if audit logging fails
                        pass
    
            # ----------------------- TANK VISUALS (date-aware) -----------------------
            with get_session() as s:
                tanks = s.query(Tank).filter(
                    Tank.location_id == active_location_id
                ).order_by(Tank.name).all()
                
                if not tanks:
                    st.info("ℹ️ No tanks configured yet. Go to **Add Asset ? Add Tank** to create tanks.")
                else:
                    # Get stock as-of the selected date for each tank (latest txn on or before selected_date)
                    tank_stocks = {}
                    for tank in tanks:
                        latest_txn = (
                            s.query(
                                TankTransaction.ticket_id,
                                TankTransaction.date,
                                TankTransaction.time,
                            )
                            .filter(
                                TankTransaction.tank_id == tank.id,
                                TankTransaction.date <= selected_date  # <-- date-aware
                            )
                            .order_by(TankTransaction.date.desc(), TankTransaction.time.desc())
                            .first()
                        )
    
                        if latest_txn and latest_txn.ticket_id:
                            otr = s.query(OTRRecord).filter(
                                OTRRecord.ticket_id == latest_txn.ticket_id
                            ).first()
                            current_stock = float(otr.nsv_bbl if otr else 0.0)
                        else:
                            current_stock = 0.0
                        
                        tank_stocks[tank.id] = current_stock
                    
                    # Display tanks in grid (5 columns)
                    num_cols = 5
                    tank_rows = [tanks[i:i+num_cols] for i in range(0, len(tanks), num_cols)]
                    
                    for tank_row in tank_rows:
                        cols = st.columns(num_cols)
                        
                        for idx, tank in enumerate(tank_row):
                            with cols[idx]:
                                current_stock = tank_stocks.get(tank.id, 0.0)
                                
                                # Get capacity safely
                                try:
                                    capacity = float(tank.capacity_bbl or 100000)
                                except:
                                    capacity = 100000.0
                                
                                fill_percentage = min((current_stock / capacity * 100), 100) if capacity > 0 else 0
                                
                                # Color based on fill level
                                if fill_percentage >= 80:
                                    liquid_color = "#28a745"
                                    liquid_dark = "#1e7e34"
                                    status_emoji = "🟢"
                                elif fill_percentage >= 50:
                                    liquid_color = "#ffc107"
                                    liquid_dark = "#d39e00"
                                    status_emoji = "🟡"
                                elif fill_percentage >= 20:
                                    liquid_color = "#fd7e14"
                                    liquid_dark = "#dc3545"
                                    status_emoji = "🟠"
                                else:
                                    liquid_color = "#dc3545"
                                    liquid_dark = "#bd2130"
                                    status_emoji = "🔴"
                                
                                status_icon = "❌" if getattr(tank, 'status', 'INACTIVE') == "INACTIVE" else "✓"
                                
                                # Get product name safely
                                product_name = (
                                    getattr(tank, 'product_type', None) or 
                                    getattr(tank, 'product', None) or 
                                    'N/A'
                                )
                                
                                # Get tank code safely
                                tank_code = getattr(tank, 'code', f'T-{tank.id}')
                                
                                # Calculate SVG parameters (smaller dimensions)
                                tank_height = 140
                                liquid_height = (fill_percentage / 100) * tank_height
                                liquid_y = tank_height - liquid_height
                                
                                # Read current daily status for the selected date (fallback READY if not available)
                                status_text = "READY"
                                if TankDailyStatus is not None and TankOpStatus is not None:
                                    row = (s.query(TankDailyStatus)
                                        .filter(TankDailyStatus.tank_id == tank.id,
                                                TankDailyStatus.date == selected_date)
                                        .first())
                                    if row is not None:
                                        status_text = getattr(row.op_status, "name", "READY")
    
                                # Create compact 3D cylindrical tank using SVG
                                with st.container(border=True):
                                    # Tank name (compact)
                                    st.markdown(
                                        f"<div style='text-align: center; font-weight: bold; font-size: 0.9rem; margin-bottom: 0.2rem;'>{tank.name} {status_icon}</div>",
                                        unsafe_allow_html=True
                                    )
    
                                    # Status line immediately below the tank name (display only)
                                    st.markdown(
                                        f"<div style='text-align: center; font-size: 0.75rem; color:#666; margin-bottom: 0.2rem;'>Status: <b>{status_text.title()}</b></div>",
                                        unsafe_allow_html=True
                                    )
    
                                    # 3D Tank SVG (compact) � unchanged design
                                    svg_code = f'''
                                    <svg width="100%" height="200" viewBox="0 0 140 200" xmlns="http://www.w3.org/2000/svg">
                                        <defs>
                                            <linearGradient id="tankGrad{tank.id}" x1="0%" y1="0%" x2="100%" y2="0%">
                                                <stop offset="0%" style="stop-color:#c0c0c0;stop-opacity:1" />
                                                <stop offset="50%" style="stop-color:#e8e8e8;stop-opacity:1" />
                                                <stop offset="100%" style="stop-color:#c0c0c0;stop-opacity:1" />
                                            </linearGradient>
                                            
                                            <linearGradient id="liquidGrad{tank.id}" x1="0%" y1="0%" x2="100%" y2="0%">
                                                <stop offset="0%" style="stop-color:{liquid_dark};stop-opacity:0.9" />
                                                <stop offset="50%" style="stop-color:{liquid_color};stop-opacity:1" />
                                                <stop offset="100%" style="stop-color:{liquid_dark};stop-opacity:0.9" />
                                            </linearGradient>
                                            
                                            <radialGradient id="topGrad{tank.id}" cx="50%" cy="50%" r="50%">
                                                <stop offset="0%" style="stop-color:#ffffff;stop-opacity:1" />
                                                <stop offset="100%" style="stop-color:#d0d0d0;stop-opacity:1" />
                                            </radialGradient>
                                            
                                            <radialGradient id="bottomGrad{tank.id}" cx="50%" cy="50%" r="50%">
                                                <stop offset="0%" style="stop-color:#a0a0a0;stop-opacity:1" />
                                                <stop offset="100%" style="stop-color:#707070;stop-opacity:1" />
                                            </radialGradient>
                                        </defs>
                                        
                                        <!-- Stock badge (smaller) -->
                                        <rect x="85" y="5" width="50" height="18" rx="9" fill="{liquid_color}" opacity="0.9"/>
                                        <text x="110" y="17" text-anchor="middle" fill="white" font-size="10" font-weight="bold">
                                            {current_stock/1000:.1f}K
                                        </text>
                                        
                                        <!-- Tank top ellipse -->
                                        <ellipse cx="70" cy="30" rx="40" ry="12" fill="url(#topGrad{tank.id})" stroke="#999" stroke-width="1.5"/>
                                        
                                        <!-- Tank body (cylinder) -->
                                        <rect x="30" y="30" width="80" height="{tank_height}" fill="url(#tankGrad{tank.id})" stroke="#999" stroke-width="1.5"/>
                                        
                                        <!-- Liquid fill -->
                                        <rect x="30" y="{30 + liquid_y}" width="80" height="{liquid_height}" fill="url(#liquidGrad{tank.id})"/>
                                        
                                        <!-- Liquid top surface (ellipse) -->
                                        <ellipse cx="70" cy="{30 + liquid_y}" rx="40" ry="12" fill="{liquid_color}" opacity="0.8"/>
                                        
                                        <!-- Percentage text (smaller) -->
                                        <text x="70" y="{30 + tank_height/2 + 6}" text-anchor="middle" fill="white" font-size="24" font-weight="bold" 
                                            style="text-shadow: 2px 2px 4px rgba(0,0,0,0.7);">
                                            {fill_percentage:.0f}%
                                        </text>
                                        
                                        <!-- Tank bottom ellipse -->
                                        <ellipse cx="70" cy="{30 + tank_height}" rx="40" ry="12" fill="url(#bottomGrad{tank.id})" stroke="#666" stroke-width="1.5"/>
                                        
                                        <!-- Bottom shadow -->
                                        <ellipse cx="70" cy="{33 + tank_height}" rx="42" ry="6" fill="black" opacity="0.2"/>
                                    </svg>
                                    '''
                                    
                                    # Use components.html to render SVG
                                    import streamlit.components.v1 as components
                                    components.html(svg_code, height=200)
                                    
                                    # Compact info display
                                    st.markdown(f"""
                                        <div style='font-size: 0.75rem; line-height: 1.3; color: #666;'>
                                            <div style='display: flex; justify-content: space-between; margin-bottom: 0.2rem;'>
                                                <span>Stock:</span>
                                                <strong style='color: {liquid_color};'>{current_stock:,.0f}</strong>
                                            </div>
                                            <div style='display: flex; justify-content: space-between; margin-bottom: 0.2rem;'>
                                                <span>Capacity:</span>
                                                <strong>{capacity:,.0f}</strong>
                                            </div>
                                            <div style='display: flex; justify-content: space-between; margin-bottom: 0.2rem;'>
                                                <span>Available:</span>
                                                <strong>{(capacity - current_stock):,.0f}</strong>
                                            </div>
                                            <div style='display: flex; justify-content: space-between; margin-bottom: 0.2rem;'>
                                                <span>Level:</span>
                                                <strong style='color: {liquid_color};'>{status_emoji} {fill_percentage:.1f}%</strong>
                                            </div>
                                            <div style='text-align: center; padding-top: 0.3rem; border-top: 1px solid #dee2e6; margin-top: 0.3rem;'>
                                                <div style='font-size: 0.7rem;'>{product_name}</div>
                                                <div style='font-size: 0.65rem; color: #999;'>{tank_code}</div>
                                            </div>
                                        </div>
                                    """, unsafe_allow_html=True)
    
                                    # --- Selector at the bottom of the card (auto-saves on change) ---
                                    if TankDailyStatus is not None and TankOpStatus is not None:
                                        status_options = [e.name for e in TankOpStatus]
                                        _key = f"tank_status_{tank.id}_{selected_date.isoformat()}"
                                        st.selectbox(
                                            "Status",
                                            status_options,
                                            index=status_options.index(status_text) if status_text in status_options else 0,
                                            key=_key,
                                            label_visibility="collapsed",
                                            on_change=_save_tank_status_cb,
                                            args=(_key, tank.id, selected_date),
                                        )
        # ===================== MONTHLY DATA (place between Tank Stock Levels and Trend) =====================
        st.markdown("### 📊 Monthly Data")
    
        from datetime import date as _date, datetime, timedelta, time as dt_time
        import pandas as pd
    
        # --- Date range selector (default: current month to dashboard date) ---
        _md_default_to = st.session_state.get("dash_date_all_sites", _date.today())
        _md_default_from = _md_default_to.replace(day=1)
    
        mcol1, mcol2 = st.columns(2)
        with mcol1:
            md_from = st.date_input("From (Monthly Data)", value=_md_default_from, key="monthly_from")
        with mcol2:
            md_to = st.date_input("To (Monthly Data)", value=_md_default_to, key="monthly_to")
    
        # Guard
        if md_from > md_to:
            st.warning("⚠️ 'From' date is after 'To' date. Please adjust the range.")
        else:
            # ---------- Helpers ----------
            def _fmt(v):
                return f"{v:,.0f}" if isinstance(v, (int, float)) else "-"
    
            days_range_count = max(1, (md_to - md_from).days + 1)
    
            agge_monthly_receipt_from_yade = None
            agge_monthly_evacuation_bbl = None
            agge_monthly_fso_receipt_bbl = None
            agge_avg_receipt_from_yade = None
            agge_avg_evacuation_bbl = None
            agge_avg_fso_receipt_bbl = None
    
            # Resolve location code (e.g., OML-13 for Utapate) and detect site
            with get_session() as s:
                try:
                    from location_manager import LocationManager
                    loc_obj = LocationManager.get_location_by_id(s, active_location_id)
                    loc_code = (getattr(loc_obj, "code", "") or "").upper()
                    loc_name = (getattr(loc_obj, "name", "") or "")
                except Exception:
                    loc_code, loc_name = "", ""
    
                def _canon(txt: str) -> str:
                    return str(txt or "").upper().replace(" ", "").replace("-", "")
    
                loc_fingerprint = {_canon(loc_code), _canon(loc_name)}
                is_asemoku = bool(loc_fingerprint & {"JETTY", "ASEMOKU", "ASEMOKUJETTY"})
                is_utapate = bool(loc_fingerprint & {"UTAPATE", "OML13"})
                is_aggu    = bool(loc_fingerprint & {"AGGU"})
                is_agge    = bool(loc_fingerprint & {"AGGE"})
                is_bfs     = bool(loc_fingerprint & {"BFS", "BENEKU", "BENEKU(BFS)"})
                # ℹ️ NEW: Ndoni flag
                is_ndoni  = bool(loc_fingerprint & {"NDONI"})
    
                if is_agge:
                    try:
                        from models import TOAYadeSummary, YadeVoyage, OTRVessel, FSOOperation
                        from fso_config import FSOConfig
                    except Exception:
                        pass
                    else:
                        yade_rows = (
                            s.query(TOAYadeSummary.gsv_loaded_bbl)
                            .join(YadeVoyage, TOAYadeSummary.voyage_id == YadeVoyage.id)
                            .filter(
                                YadeVoyage.location_id == active_location_id,
                                TOAYadeSummary.date >= md_from,
                                TOAYadeSummary.date <= md_to,
                            )
                            .all()
                        )
                        if yade_rows:
                            agge_monthly_receipt_from_yade = sum(
                                abs(float(val or 0.0)) for (val,) in yade_rows
                            )
    
                        otr_rows = (
                            s.query(OTRVessel.net_receipt_dispatch)
                            .filter(
                                OTRVessel.location_id == active_location_id,
                                OTRVessel.date >= md_from,
                                OTRVessel.date <= md_to,
                            )
                            .all()
                        )
                        if otr_rows:
                            agge_monthly_evacuation_bbl = sum(
                                abs(float(val or 0.0)) for (val,) in otr_rows
                            )
    
                        fso_vessel = None
                        loc_candidates = []
                        if loc_code:
                            loc_candidates.append(loc_code.upper())
                        if loc_name:
                            loc_candidates.append(str(loc_name).upper())
                        if _canon(loc_code) == "AGGE" or _canon(loc_name) == "AGGE":
                            loc_candidates.append("AGGE")
    
                        for cand in loc_candidates:
                            vessels = FSOConfig.get_fso_for_location(cand)
                            if vessels:
                                fso_vessel = vessels[0]
                                break
    
                        if not fso_vessel:
                            fso_vessel = FSOConfig.get_default_fso("AGGE")
    
                        if fso_vessel:
                            extended_from = md_from - timedelta(days=1)
                            extended_to = md_to + timedelta(days=1)
                            fso_entries = (
                                s.query(FSOOperation)
                                .filter(
                                    FSOOperation.location_id == active_location_id,
                                    FSOOperation.fso_vessel == fso_vessel,
                                    FSOOperation.date >= extended_from,
                                    FSOOperation.date <= extended_to,
                                )
                                .order_by(FSOOperation.date, FSOOperation.time)
                                .all()
                            )
    
                            total_receipts = 0.0
                            for entry in fso_entries:
                                entry_time = _coerce_time(entry.time)
                                if not entry_time:
                                    continue
                                entry_dt = datetime.combine(entry.date, entry_time)
                                day_key = entry_dt.date()
                                if entry_time < dt_time(6, 1):
                                    day_key = day_key - timedelta(days=1)
                                if day_key < md_from or day_key > md_to:
                                    continue
                                op_label = (entry.operation or "").strip().lower()
                                if op_label == "receipt" and entry.net_receipt_dispatch is not None:
                                    try:
                                        total_receipts += abs(float(entry.net_receipt_dispatch or 0.0))
                                    except Exception:
                                        continue
    
                            if total_receipts:
                                agge_monthly_fso_receipt_bbl = total_receipts
    
                        if agge_monthly_receipt_from_yade is not None:
                            agge_avg_receipt_from_yade = agge_monthly_receipt_from_yade / days_range_count
                        if agge_monthly_evacuation_bbl is not None:
                            agge_avg_evacuation_bbl = agge_monthly_evacuation_bbl / days_range_count
                        if agge_monthly_fso_receipt_bbl is not None:
                            agge_avg_fso_receipt_bbl = agge_monthly_fso_receipt_bbl / days_range_count
    
                # ===== 1) Production & Evacuation totals (sum over range) from Material Balance =====
                prod_total = 0.0
                evac_total = 0.0
                avg_prod = 0.0
                avg_evac = 0.0
                df_mb = None
                try:
                    from material_balance_calculator import MaterialBalanceCalculator as MBCalc
                    mb_rows = MBCalc.calculate_material_balance(
                        entries=None,
                        location_code=loc_code,
                        date_from=md_from,
                        date_to=md_to,
                        location_id=active_location_id,
                        debug=False
                    )
                    if mb_rows:
                        df_mb = pd.DataFrame(mb_rows)
                        # Be tolerant to column casing/spacing
                        def _col(df, candidates):
                            for c in candidates:
                                if c in df.columns:
                                    return c
                            lower_map = {str(c).lower(): c for c in df.columns}
                            for c in candidates:
                                if c.lower() in lower_map:
                                    return lower_map[c.lower()]
                            return None
    
                        c_receipt  = _col(df_mb, ["Receipt"])
                        c_dispatch = _col(df_mb, ["Dispatch"])
    
                        if c_receipt is not None:
                            prod_total = float(pd.to_numeric(df_mb[c_receipt], errors="coerce").fillna(0).sum())
                        if c_dispatch is not None:
                            evac_total = float(pd.to_numeric(df_mb[c_dispatch], errors="coerce").fillna(0).sum())
    
                        # Average per day within the selected range (uniform basis)
                        avg_prod = prod_total / days_range_count
                        avg_evac = evac_total / days_range_count
                except Exception:
                    pass  # leave zeros
    
                # ===== Common case-insensitive column finder =====
                def _find_col(df: pd.DataFrame, candidates):
                    if df is None or df.empty:
                        return None
                    for c in candidates:
                        if c in df.columns:
                            return c
                    lower_map = {str(c).strip().lower(): c for c in df.columns}
                    for c in candidates:
                        lc = str(c).strip().lower()
                        if lc in lower_map:
                            return lower_map[lc]
                    return None
    
                # ===== Asemoku Jetty specific monthly mapping (cards: ANZ Production, BFS Receipt, Evacuation, BCCR) =====
                # Anz Production -> "ANZ Receipt" (sum over range)
                # BFS Receipt    -> "OKW Receipt" (sum over range)
                # Evacuation     -> "Dispatch to barge" (sum over range)
                # BCCR           -> (leave empty for now)
                anz_total = None
                bfs_receipt_total = None
                evac_total_asemoku = None
                avg_anz = None
                avg_bfs_receipt = None
                avg_evac_asemoku = None
                bccr_total = None
                avg_bccr = None
    
                if is_asemoku and df_mb is not None:
                    c_anz = _find_col(df_mb, ["ANZ Receipt"])
                    c_okw = _find_col(df_mb, ["OKW Receipt"])
                    c_disp_barge = _find_col(df_mb, ["Dispatch to barge"])
    
                    if c_anz:
                        anz_total = float(pd.to_numeric(df_mb[c_anz], errors="coerce").fillna(0).sum())
                        avg_anz = anz_total / days_range_count
                    if c_okw:
                        bfs_receipt_total = float(pd.to_numeric(df_mb[c_okw], errors="coerce").fillna(0).sum())
                        avg_bfs_receipt = bfs_receipt_total / days_range_count
                    if c_disp_barge:
                        evac_total_asemoku = float(pd.to_numeric(df_mb[c_disp_barge], errors="coerce").fillna(0).sum())
                        avg_evac_asemoku = evac_total_asemoku / days_range_count
    
                # ===== Beneku (BFS) specific monthly mapping =====
                # OKW Production        -> GPPProductionRecord.okw_production (sum range)
                # GPP Production        -> GPPProductionRecord.total_production (sum range)
                # Evacuation            -> MB "Dispatch to Jetty"
                # BFS Condensate Receipt-> MB "Receipt-Condensate"/"Receipt - Condensate"
                okw_total = 0.0
                gpp_total = 0.0
                bfs_evac_total = 0.0
                bfs_cond_total = 0.0
                avg_okw = avg_gpp = avg_bfs_evac = avg_bfs_cond = 0.0
    
                if is_bfs:
                    # From reporting page - GPPProductionRecord
                    try:
                        from models import GPPProductionRecord
                    except Exception:
                        GPPProductionRecord = None
    
                    if GPPProductionRecord is not None:
                        try:
                            recs = (
                                s.query(GPPProductionRecord)
                                .filter(
                                    GPPProductionRecord.location_id == active_location_id,
                                    GPPProductionRecord.date >= md_from,
                                    GPPProductionRecord.date <= md_to,
                                )
                                .all()
                            )
                            if recs:
                                okw_total = sum(float(getattr(r, "okw_production", 0.0) or 0.0) for r in recs)
                                gpp_total = sum(float(getattr(r, "total_production", 0.0) or 0.0) for r in recs)
                                avg_okw = okw_total / days_range_count
                                avg_gpp = gpp_total / days_range_count
                        except Exception:
                            pass
    
                    # From Material Balance
                    if df_mb is not None:
                        c_bfs_evac = _find_col(df_mb, ["Dispatch to Jetty"])
                        c_bfs_cond = _find_col(df_mb, ["Receipt-Condensate", "Receipt - Condensate"])
    
                        if c_bfs_evac:
                            bfs_evac_total = float(pd.to_numeric(df_mb[c_bfs_evac], errors="coerce").fillna(0).sum())
                            avg_bfs_evac = bfs_evac_total / days_range_count
                        if c_bfs_cond:
                            bfs_cond_total = float(pd.to_numeric(df_mb[c_bfs_cond], errors="coerce").fillna(0).sum())
                            avg_bfs_cond = bfs_cond_total / days_range_count
    
                # ===== Ndoni specific monthly mapping =====
                # Receipt from Agu   -> MB "Receipt from Agu"
                # Receipt from OFS   -> MB "Receipt from OFS"
                # Other Receipts     -> MB "Other Receipts"
                # Evacuation         -> MB "Dispatch to barge"
                agu_total = ofs_total = other_total = ndoni_evac_total = 0.0
                avg_agu = avg_ofs = avg_other = avg_ndoni_evac = 0.0
    
                if is_ndoni and df_mb is not None:
                    c_agu = _find_col(df_mb, ["Receipt from Agu"])
                    c_ofs = _find_col(df_mb, ["Receipt from OFS"])
                    c_oth = _find_col(df_mb, ["Other Receipts"])
                    c_disp_barge_nd = _find_col(df_mb, ["Dispatch to barge"])
    
                    if c_agu:
                        agu_total = float(pd.to_numeric(df_mb[c_agu], errors="coerce").fillna(0).sum())
                        avg_agu = agu_total / days_range_count
                    if c_ofs:
                        ofs_total = float(pd.to_numeric(df_mb[c_ofs], errors="coerce").fillna(0).sum())
                        avg_ofs = ofs_total / days_range_count
                    if c_oth:
                        other_total = float(pd.to_numeric(df_mb[c_oth], errors="coerce").fillna(0).sum())
                        avg_other = other_total / days_range_count
                    if c_disp_barge_nd:
                        ndoni_evac_total = float(pd.to_numeric(df_mb[c_disp_barge_nd], errors="coerce").fillna(0).sum())
                        avg_ndoni_evac = ndoni_evac_total / days_range_count
    
                # ===== 2) Export total (sum over range) from FSO Material Balance (MT TULJA KALYANI) =====
                export_total = None  # "-" if not available
                avg_export = None
    
                # Canonicalize FSO location code
                def _canonical_fso_code(code: str | None) -> str:
                    if not code:
                        return ""
                    s2 = str(code).strip().upper()
                    # normalize spaces/hyphens so OML-13 / OML 13 / OML13 all match
                    s_norm = s2.replace(" ", "").replace("-", "")
                    aliases = {
                        "UTAPATE": "OML-13",
                        "OML13": "OML-13",
                        "OML-13": "OML-13",
                        "OML 13": "OML-13",
                        "AGGE": "AGGE",
                    }
                    # try direct first, then normalized
                    return aliases.get(s2, aliases.get(s_norm, s2))
    
                canon_loc = _canonical_fso_code(loc_code)
    
                # Helpers to find columns & sum exports from a df like the FSO MB page emits
                def _find_col_generic(df: pd.DataFrame, candidates):
                    # exact then case-insensitive
                    for c in candidates:
                        if c in df.columns:
                            return c
                    lower_map = {str(c).strip().lower(): c for c in df.columns}
                    for c in candidates:
                        lc = str(c).strip().lower()
                        if lc in lower_map:
                            return lower_map[lc]
                    return None
    
                def _sum_exports_from_df(df: pd.DataFrame, date_from, date_to, vessel_name="MT TULJA KALYANI"):
                    if df is None or df.empty:
                        return None
    
                    # Identify date column
                    date_col = _find_col_generic(df, ["Date", "MB Date", "As Of", "Asof"])
                    if date_col is None:
                        return None
    
                    dfx = df.copy()
                    dfx[date_col] = pd.to_datetime(dfx[date_col], errors="coerce").dt.date
                    dfx = dfx[(dfx[date_col] >= date_from) & (dfx[date_col] <= date_to)]
                    if dfx.empty:
                        return None
    
                    # Optional location filter (when a location column exists)
                    loc_col = _find_col_generic(dfx, ["Location", "Loc", "Site", "Code"])
                    if loc_col is not None and canon_loc:
                        loc_norm = dfx[loc_col].astype(str).str.upper()
                        loc_norm = loc_norm.str.replace(" ", "", regex=False).str.replace("-", "", regex=False)
                        dfx = dfx[loc_norm == canon_loc.replace("-", "").replace(" ", "")]
                        if dfx.empty:
                            return None
    
                    # Optional vessel filter (when a vessel column exists)
                    vessel_col = _find_col_generic(dfx, ["Vessel", "FSO Vessel", "FSO", "Vessel Name", "fso_vessel"])
                    if vessel_col is not None and vessel_name:
                        dfx = dfx[dfx[vessel_col].astype(str).str.upper().str.contains(vessel_name.upper())]
                        if dfx.empty:
                            return None
    
                    # Exports column (must exist)
                    exp_col = _find_col_generic(
                        dfx,
                        ["Exports (bbls)", "Export (bbls)", "exports (bbls)", "export (bbls)",
                        "exports_bbls", "export_bbls", "Exports", "Export"]
                    )
                    if exp_col is None:
                        return None
    
                    return float(pd.to_numeric(dfx[exp_col], errors="coerce").fillna(0).sum())
    
                # 1) Try common FSO MB page cache keys first
                _possible_keys = [
                    "fso_mb_df", "fso_mb_table", "fso_material_balance_df",
                    "fso_mb_daily", "fso_mb_cache", "fso_mb_summary_df",
                    "fso_mb_pivot", "fso_mb_records"
                ]
                for _k in _possible_keys:
                    if _k in st.session_state and export_total is None:
                        try:
                            _obj = st.session_state[_k]
                            _df = _obj.copy() if isinstance(_obj, pd.DataFrame) else pd.DataFrame(_obj)
                            val = _sum_exports_from_df(_df, md_from, md_to, "MT TULJA KALYANI")
                            if val is not None:
                                export_total = val
                        except Exception:
                            pass
    
                # 2) If still not found, scan *every* DataFrame-like object in session_state
                if export_total is None:
                    for _k, _obj in list(st.session_state.items()):
                        try:
                            if isinstance(_obj, pd.DataFrame):
                                _df = _obj
                            elif isinstance(_obj, (list, tuple)) and _obj and isinstance(_obj[0], dict):
                                _df = pd.DataFrame(_obj)
                            else:
                                continue
                            if _df is None or _df.empty:
                                continue
                            val = _sum_exports_from_df(_df.copy(), md_from, md_to, "MT TULJA KALYANI")
                            if val is not None:
                                export_total = val
                                break
                        except Exception:
                            continue
    
                # 3) Fallback: compute from raw FSOOperation by day windows (06:01?06:00) and summing "Export" ops
                if export_total is None:
                    try:
                        from fso_config import FSOConfig
                        from models import FSOOperation
    
                        # Map location ? vessel
                        try:
                            fso_map = {k.upper(): v for k, v in FSOConfig.get_fso_locations().items()}
                            vessel_name = fso_map.get(canon_loc, "MT TULJA KALYANI")
                        except Exception:
                            vessel_name = "MT TULJA KALYANI"
    
                        # Pull slightly wider range so we can slice per-day windows
                        ext_from = md_from - timedelta(days=1)
                        ext_to   = md_to   + timedelta(days=1)
    
                        entries = (s.query(FSOOperation)
                                    .filter(
                                        FSOOperation.location_id == active_location_id,
                                        FSOOperation.fso_vessel == vessel_name,
                                        FSOOperation.date >= ext_from,
                                        FSOOperation.date <= ext_to,
                                    )
                                    .order_by(FSOOperation.date, FSOOperation.time)
                                    .all())
    
                        if entries:
                            def _to_time(t):
                                if isinstance(t, dt_time):
                                    return t
                                try:
                                    return datetime.strptime(str(t), "%H:%M").time()
                                except Exception:
                                    return dt_time(0, 0)
    
                            total = 0.0
                            day_list = pd.date_range(md_from, md_to, freq="D").date
                            for D in day_list:
                                win_start = datetime.combine(D, dt_time(6, 1))
                                win_end   = datetime.combine(D + timedelta(days=1), dt_time(6, 0))
                                # Filter window
                                per = []
                                for e in entries:
                                    try:
                                        edt = datetime.combine(e.date, _to_time(e.time))
                                        if win_start <= edt <= win_end:
                                            per.append(e)
                                    except Exception:
                                        continue
                                if not per:
                                    continue
                                # Sum exports: operations like "Export"/"Exports"/"Shipment"/"Ship Out"
                                day_exp = 0.0
                                for e in per:
                                    op = (getattr(e, "operation", "") or "").strip().lower()
                                    if any(tok in op for tok in ["export", "shipment", "ship out"]):
                                        try:
                                            v = float(getattr(e, "net_receipt_dispatch", 0.0) or 0.0)
                                        except Exception:
                                            v = 0.0
                                        if v != 0:
                                            day_exp += abs(v)
                                total += day_exp
                            export_total = total
                    except Exception:
                        export_total = None
    
                # Average export (only used for non-Utapate cards)
                if export_total is not None:
                    avg_export = export_total / days_range_count
    
                # ===== Render cards =====
                if is_asemoku:
                    # Asemoku Jetty cards with averages (4 cards)
                    c1, c2, c3, c4 = st.columns(4)
    
                    with c1:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Anz Production</div>
                                <div class="stat-value">{_fmt(anz_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg Anz Production: {_fmt(avg_anz)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c2:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">BFS Receipt</div>
                                <div class="stat-value">{_fmt(bfs_receipt_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg BFS Receipt: {_fmt(avg_bfs_receipt)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c3:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Evacuation</div>
                                <div class="stat-value">{_fmt(evac_total_asemoku)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg Evacuation: {_fmt(avg_evac_asemoku)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c4:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">BCCR</div>
                                <div class="stat-value">{_fmt(bccr_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg BCCR: {_fmt(avg_bccr)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    (to be wired)
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                elif is_bfs:
                    # BENEKU (BFS): 4 cards � OKW Prod, GPP Prod, Evacuation, BFS Condensate Receipt (all with averages)
                    c1, c2, c3, c4 = st.columns(4)
    
                    with c1:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">OKW Production</div>
                                <div class="stat-value">{_fmt(okw_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg OKW Production: {_fmt(avg_okw)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c2:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">GPP Production</div>
                                <div class="stat-value">{_fmt(gpp_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg GPP Production: {_fmt(avg_gpp)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c3:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Evacuation</div>
                                <div class="stat-value">{_fmt(bfs_evac_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg Evacuation: {_fmt(avg_bfs_evac)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c4:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">BFS Condensate Receipt</div>
                                <div class="stat-value">{_fmt(bfs_cond_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg Condensate Receipt: {_fmt(avg_bfs_cond)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                elif is_ndoni:
                    # NDONI: 4 cards � Receipt from Agu, Receipt from OFS, Other Receipts, Evacuation (all with averages)
                    c1, c2, c3, c4 = st.columns(4)
    
                    with c1:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Receipt from Agu</div>
                                <div class="stat-value">{_fmt(agu_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg Receipt from Agu: {_fmt(avg_agu)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c2:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Receipt from OFS</div>
                                <div class="stat-value">{_fmt(ofs_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg Receipt from OFS: {_fmt(avg_ofs)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c3:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Other Receipts</div>
                                <div class="stat-value">{_fmt(other_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg Other Receipts: {_fmt(avg_other)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c4:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Evacuation</div>
                                <div class="stat-value">{_fmt(ndoni_evac_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg Evacuation: {_fmt(avg_ndoni_evac)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                elif is_agge:
                    # AGGE: 3 cards � Receipt from Yade, Evacuation (OTR), FSO Receipt
                    c1, c2, c3 = st.columns(3)
                    range_note = f"{md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}"
    
                    with c1:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Receipt from Yade</div>
                                <div class="stat-value">{_fmt(agge_monthly_receipt_from_yade)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg: {_fmt(agge_avg_receipt_from_yade)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {range_note}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c2:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Evacuation</div>
                                <div class="stat-value">{_fmt(agge_monthly_evacuation_bbl)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg: {_fmt(agge_avg_evacuation_bbl)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {range_note}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c3:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">FSO Receipt</div>
                                <div class="stat-value">{_fmt(agge_monthly_fso_receipt_bbl)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg: {_fmt(agge_avg_fso_receipt_bbl)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {range_note}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                elif is_aggu:
                    # AGGU: 3 cards only (Production, Evacuation, Tankers Dispatched)
                    c1, c2, c3 = st.columns(3)
    
                    with c1:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Production</div>
                                <div class="stat-value">{_fmt(prod_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg Production: {_fmt(avg_prod)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c2:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Evacuation</div>
                                <div class="stat-value">{_fmt(evac_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg Evacuation: {_fmt(avg_evac)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c3:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Tankers Dispatched</div>
                                <div class="stat-value">-</div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                else:
                    # Non-Asemoku, non-AGGU, non-BFS, non-NDONI locations (incl. Utapate). Keep 4 cards with averages.
                    c1, c2, c3, c4 = st.columns(4)
    
                    with c1:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Production</div>
                                <div class="stat-value">{_fmt(prod_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg Production: {_fmt(avg_prod)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c2:
                        st.markdown(f"""
                            <div class="stat-card">
                                <div class="stat-label">Evacuation</div>
                                <div class="stat-value">{_fmt(evac_total)}</div>
                                <div style="color:#6c757d;font-size:0.8rem;">
                                    Avg Evacuation: {_fmt(avg_evac)}
                                </div>
                                <div style="color:#6c757d;font-size:0.75rem;">
                                    {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
    
                    with c3:
                        # For Utapate ONLY, hide the average line for Export
                        if is_utapate:
                            st.markdown(f"""
                                <div class="stat-card">
                                    <div class="stat-label">Export (MT TULJA KALYANI)</div>
                                    <div class="stat-value">{_fmt(export_total)}</div>
                                    <div style="color:#6c757d;font-size:0.75rem;">
                                        {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                    </div>
                                </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown(f"""
                                <div class="stat-card">
                                    <div class="stat-label">Export (MT TULJA KALYANI)</div>
                                    <div class="stat-value">{_fmt(export_total)}</div>
                                    <div style="color:#6c757d;font-size:0.8rem;">
                                        Avg Export: {_fmt(avg_export)}
                                    </div>
                                    <div style="color:#6c757d;font-size:0.75rem;">
                                        {md_from.strftime('%d-%b-%Y')} ? {md_to.strftime('%d-%b-%Y')}
                                    </div>
                                </div>
                            """, unsafe_allow_html=True)
    
                    with c4:
                        # For Utapate ONLY, hide the average line for Vessel Status & Stock
                        if is_utapate:
                            st.markdown(f"""
                                <div class="stat-card">
                                    <div class="stat-label">Vessel Status & Stock</div>
                                    <div class="stat-value">-</div>
                                    <div style="color:#6c757d;font-size:0.75rem;">
                                        (to be wired)
                                    </div>
                                </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown(f"""
                                <div class="stat-card">
                                    <div class="stat-label">Vessel Status & Stock</div>
                                    <div class="stat-value">-</div>
                                    <div style="color:#6c757d;font-size:0.8rem;">
                                        Avg Vessel Status & Stock: -
                                    </div>
                                    <div style="color:#6c757d;font-size:0.75rem;">
                                        (to be wired)
                                    </div>
                                </div>
                            """, unsafe_allow_html=True)
    
    
        if is_agge:
            st.markdown("### 🚦 Convoy & Vessel Status")
            st.caption(f"Snapshots for {_selected_date.strftime('%d-%b-%Y')} from Convoy Status page.")
            convoy_col, vessel_col = st.columns(2)
            yade_entries: list[dict[str, str]] = []
            vessel_entries: list[dict[str, str]] = []
            fetch_error = None
            try:
                from models import ConvoyStatusYade, ConvoyStatusVessel
                with get_session() as s_convoy:
                    yade_rows = (
                        s_convoy.query(ConvoyStatusYade, YadeBarge.name)
                        .join(YadeBarge, ConvoyStatusYade.yade_barge_id == YadeBarge.id)
                        .filter(
                            ConvoyStatusYade.location_id == active_location_id,
                            ConvoyStatusYade.date == _selected_date,
                        )
                        .order_by(ConvoyStatusYade.status.asc(), YadeBarge.name.asc())
                        .all()
                    )
                    for rec, yade_name in yade_rows:
                        yade_entries.append(
                            {
                                "Status": (rec.status or "N/A").strip(),
                                "YADE": yade_name or "N/A",
                                "Convoy": rec.convoy_no or "N/A",
                                "Stock": rec.stock_display or "N/A",
                            }
                        )
                    vessel_rows = (
                        s_convoy.query(ConvoyStatusVessel)
                        .filter(
                            ConvoyStatusVessel.location_id == active_location_id,
                            ConvoyStatusVessel.date == _selected_date,
                        )
                        .all()
                    )
                    for rec in vessel_rows:
                        vessel_entries.append(
                            {
                                "Vessel": (rec.vessel_name or "N/A").strip(),
                                "Status": (rec.status or "N/A").strip(),
                                "Shuttle": rec.shuttle_no or "N/A",
                                "Stock": rec.stock_display or "N/A",
                            }
                        )
            except Exception as exc:
                fetch_error = str(exc)
                log_error("Convoy/vessel snapshot load failed", exc_info=True)
    
            if fetch_error:
                st.error(f"Unable to load convoy status snapshots: {fetch_error}")
    
            yade_entries.sort(key=lambda item: (item["Status"], item["YADE"]))
            vessel_entries.sort(key=lambda item: item["Vessel"])
    
            with convoy_col:
                st.markdown("#### Convoy Status (YADE)")
                if not yade_entries:
                    st.info("No YADE convoy statuses saved for this date.")
                else:
                    status_groups = defaultdict(list)
                    for entry in yade_entries:
                        status_groups[entry["Status"]].append(entry)
                    for status in sorted(status_groups):
                        st.markdown(f"**{status}**")
                        for entry in status_groups[status]:
                            st.markdown(
                                f"- {entry['YADE']} | Convoy: {entry['Convoy']} | Stock: {entry['Stock']}"
                            )
    
            with vessel_col:
                st.markdown("#### Vessel Status")
                if not vessel_entries:
                    st.info("No vessel statuses saved for this date.")
                else:
                    for entry in vessel_entries:
                        st.markdown(
                            f"- **{entry['Vessel']}** � {entry['Status']} (Shuttle: {entry['Shuttle']}, Stock: {entry['Stock']})"
                        )
    
        else:
            # ====================================================================================================
           # ===================== PRODUCTION & EVACUATION TREND =====================
            st.markdown("### 📈 Production & Evacuation Trend")
        
            import pandas as pd
            import altair as alt
            from datetime import date as _date, datetime, timedelta
        
            # --- Separate date-range selector for Trend ---
            tcol1, tcol2 = st.columns(2)
            with tcol1:
                trend_from = st.date_input(
                    "From (Trend)",
                    value=st.session_state.get("dash_date_all_sites", _date.today()).replace(day=1),
                    key="trend_from",
                )
            with tcol2:
                trend_to = st.date_input(
                    "To (Trend)",
                    value=st.session_state.get("dash_date_all_sites", _date.today()),
                    key="trend_to",
                )
        
            if trend_from > trend_to:
                st.warning("⚠️ 'From (Trend)' is after 'To (Trend)'. Please adjust the range.")
            else:
                # Helper
                def _find_col(df: pd.DataFrame, candidates):
                    if df is None or df.empty:
                        return None
                    for c in candidates:
                        if c in df.columns:
                            return c
                    lower_map = {str(c).strip().lower(): c for c in df.columns}
                    for c in candidates:
                        lc = str(c).strip().lower()
                        if lc in lower_map:
                            return lower_map[lc]
                    return None
        
                # Resolve location (to branch for Asemoku Jetty vs BFS vs others)
                with get_session() as s:
                    try:
                        from location_manager import LocationManager
                        loc_obj = LocationManager.get_location_by_id(s, active_location_id)
                        loc_code = (getattr(loc_obj, "code", "") or "")
                        loc_name = (getattr(loc_obj, "name", "") or "")
                    except Exception:
                        loc_code, loc_name = "", ""
        
                def _canon(txt: str) -> str:
                    return str(txt or "").upper().replace(" ", "").replace("-", "")
        
                fp = {_canon(loc_code), _canon(loc_name)}
                is_asemoku = bool(fp & {"JETTY", "ASEMOKU", "ASEMOKUJETTY"})
                is_bfs     = bool(fp & {"BFS", "BENEKU", "BENEKU(BFS)"})
                is_ndoni   = bool(fp & {"NDONI"})  # ℹ️ Ndoni detection
        
                # Pull Material Balance rows for the selected range
                df_mb = None
                try:
                    from material_balance_calculator import MaterialBalanceCalculator as MBCalc
                    df_mb = pd.DataFrame(
                        MBCalc.calculate_material_balance(
                            entries=None,
                            location_code=(loc_code or "").upper(),
                            date_from=trend_from,
                            date_to=trend_to,
                            location_id=active_location_id,
                            debug=False,
                        )
                    )
                except Exception:
                    df_mb = None
        
                if df_mb is None or df_mb.empty:
                    st.info("No material balance data available for the selected range.")
                else:
                    # Ensure date column exists and is normalized
                    dcol = _find_col(df_mb, ["Date"])
                    if dcol is None:
                        st.info("Material balance result does not include a Date column.")
                    else:
                        df_mb = df_mb.copy()
                        df_mb[dcol] = pd.to_datetime(df_mb[dcol], errors="coerce").dt.date
                        full_days = pd.DataFrame({"Date": pd.date_range(trend_from, trend_to, freq="D").date})
        
                        if is_asemoku:
                            # --- ASEMOKU JETTY: 3 lines (ANZ Production, BFS Receipt, Evacuation) ---
                            c_anz  = _find_col(df_mb, ["ANZ Receipt"])
                            c_okw  = _find_col(df_mb, ["OKW Receipt"])
                            c_disp = _find_col(df_mb, ["Dispatch to barge"])
        
                            agg = df_mb.groupby(dcol).agg({
                                (c_anz if c_anz else dcol): "sum",
                                (c_okw if c_okw else dcol): "sum",
                                (c_disp if c_disp else dcol): "sum",
                            }).reset_index()
        
                            # Rename to friendly labels
                            rename_map = {}
                            if c_anz:  rename_map[c_anz]  = "ANZ Production"
                            if c_okw:  rename_map[c_okw]  = "BFS Receipt"
                            if c_disp: rename_map[c_disp] = "Evacuation"
                            agg = agg.rename(columns=rename_map)
        
                            # Merge with full date skeleton
                            df_day = full_days.merge(agg.rename(columns={dcol: "Date"}), on="Date", how="left")
                            for col in ["ANZ Production", "BFS Receipt", "Evacuation"]:
                                if col not in df_day.columns:
                                    df_day[col] = 0.0
                            df_day[["ANZ Production", "BFS Receipt", "Evacuation"]] = (
                                df_day[["ANZ Production", "BFS Receipt", "Evacuation"]]
                                .apply(pd.to_numeric, errors="coerce")
                                .fillna(0.0)
                            )
                            df_day["DateTS"] = pd.to_datetime(df_day["Date"])
        
                            # Totals for the small card
                            tot_anz  = float(df_day["ANZ Production"].sum())
                            tot_bfs  = float(df_day["BFS Receipt"].sum())
                            tot_evac = float(df_day["Evacuation"].sum())
        
                            # Extreme flags per series
                            def _extreme_flags(series):
                                s = series.fillna(0.0)
                                return (s == s.max()).astype(int), (s == s.min()).astype(int)
        
                            df_day["is_max_anz"],  df_day["is_min_anz"]  = _extreme_flags(df_day["ANZ Production"])
                            df_day["is_max_bfs"],  df_day["is_min_bfs"]  = _extreme_flags(df_day["BFS Receipt"])
                            df_day["is_max_evac"], df_day["is_min_evac"] = _extreme_flags(df_day["Evacuation"])
        
                            # Long-form for legend-friendly plotting (lines + points + labels)
                            df_anz = df_day[["Date", "DateTS", "ANZ Production", "is_max_anz", "is_min_anz"]].rename(
                                columns={"ANZ Production": "Value", "is_max_anz": "is_max", "is_min_anz": "is_min"}
                            )
                            df_anz["Series"] = "ANZ Production"
        
                            df_bfs = df_day[["Date", "DateTS", "BFS Receipt", "is_max_bfs", "is_min_bfs"]].rename(
                                columns={"BFS Receipt": "Value", "is_max_bfs": "is_max", "is_min_bfs": "is_min"}
                            )
                            df_bfs["Series"] = "BFS Receipt"
        
                            df_evac = df_day[["Date", "DateTS", "Evacuation", "is_max_evac", "is_min_evac"]].rename(
                                columns={"Evacuation": "Value", "is_max_evac": "is_max", "is_min_evac": "is_min"}
                            )
                            df_evac["Series"] = "Evacuation"
        
                            plot_df = pd.concat([df_anz, df_bfs, df_evac], ignore_index=True)
        
                            # Axis & scales
                            x_axis = alt.Axis(title="Date", format="%d-%b", labelAngle=0,
                                            tickCount={"interval": "day", "step": 1})
                            y_axis = alt.Axis(title="Quantity in bbls")
        
                            # Colors (Brown, Green, Blue for Asemoku)
                            domain = ["ANZ Production", "BFS Receipt", "Evacuation"]
                            range_  = ["#8B4513", "#006400", "#1E90FF"]
        
                            # Y domain padding for better fitting (room for labels + totals card)
                            y_domain_max = max(float(plot_df["Value"].max() or 1.0), 1.0) * 1.25
        
                            base = alt.Chart(plot_df).properties(height=360)
        
                            # Lines with legend
                            lines = base.mark_line(strokeWidth=2).encode(
                                x=alt.X("DateTS:T", axis=x_axis),
                                y=alt.Y("Value:Q", axis=y_axis, scale=alt.Scale(domain=[0, y_domain_max])),
                                color=alt.Color(
                                    "Series:N",
                                    scale=alt.Scale(domain=domain, range=range_),
                                    legend=alt.Legend(title=None, orient="top", symbolStrokeWidth=6),
                                ),
                            )
        
                            # Triangular points
                            pts = base.mark_point(shape="triangle-up", filled=False, size=60).encode(
                                x="DateTS:T",
                                y="Value:Q",
                                color=alt.Color("Series:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
                            )
        
                            # One set of value labels (black)
                            labels = base.mark_text(dy=-12, color="black", fontSize=11).encode(
                                x="DateTS:T",
                                y="Value:Q",
                                text=alt.Text("Value:Q", format=",.0f"),
                                color=alt.Color("Series:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
                            )
        
                            # Bold labels for extremes (max & min)
                            max_labels = base.transform_filter(alt.datum.is_max == 1).mark_text(
                                dy=-12, color="black", fontWeight="bold", fontSize=12
                            ).encode(
                                x="DateTS:T",
                                y="Value:Q",
                                text=alt.Text("Value:Q", format=",.0f"),
                                color=alt.Color("Series:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
                            )
        
                            min_labels = base.transform_filter(alt.datum.is_min == 1).mark_text(
                                dy=-12, color="black", fontWeight="bold", fontSize=12
                            ).encode(
                                x="DateTS:T",
                                y="Value:Q",
                                text=alt.Text("Value:Q", format=",.0f"),
                                color=alt.Color("Series:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
                            )
        
                            # --- Totals card inside the chart ---
                            y_max_all = float(max(df_day[["ANZ Production", "BFS Receipt", "Evacuation"]].max()))
                            y_min_all = float(min(df_day[["ANZ Production", "BFS Receipt", "Evacuation"]].min()))
                            card_top = max(y_domain_max * 0.98, y_max_all)  # stay within domain
                            card_bottom = max(card_top * 0.70, y_min_all)
        
                            card_df = pd.DataFrame({
                                "x0": [pd.to_datetime(df_day["Date"].min())],
                                "x1": [pd.to_datetime(df_day["Date"].min()) +
                                    pd.Timedelta(days=max(1, (trend_to - trend_from).days // 4))],
                                "y0": [card_bottom],
                                "y1": [card_top],
                                "t1": [f"ANZ: {tot_anz:,.0f} bbls"],
                                "t2": [f"BFS: {tot_bfs:,.0f} bbls"],
                                "t3": [f"Evac: {tot_evac:,.0f} bbls"],
                            })
        
                            card_rect = alt.Chart(card_df).mark_rect(
                                fill="#f8f9fa", stroke="#ccd1d6", opacity=0.9
                            ).encode(
                                x="x0:T", x2="x1:T", y="y0:Q", y2="y1:Q"
                            )
                            card_text1 = alt.Chart(card_df).mark_text(
                                align="left", dx=8, dy=8, fontSize=12
                            ).encode(
                                x="x0:T", y="y1:Q", text="t1"
                            )
                            card_text2 = alt.Chart(card_df).mark_text(
                                align="left", dx=8, dy=26, fontSize=12
                            ).encode(
                                x="x0:T", y="y1:Q", text="t2"
                            )
                            card_text3 = alt.Chart(card_df).mark_text(
                                align="left", dx=8, dy=44, fontSize=12
                            ).encode(
                                x="x0:T", y="y1:Q", text="t3"
                            )
        
                            chart_asemoku = (
                                card_rect + card_text1 + card_text2 + card_text3 +
                                lines + pts + labels + max_labels + min_labels
                            ).properties(
                                height=360,
                                padding={"left": 5, "right": 5, "top": 5, "bottom": 40}
                            ).configure_axis(
                                labelColor="#000",
                                titleColor="#000",
                                labelFontSize=11,
                                titleFontSize=12,
                            ).configure_legend(
                                labelFontSize=11,
                                titleFontSize=12,
                                orient="top",
                            ).configure_view(strokeOpacity=0)
        
                            st.altair_chart(chart_asemoku, use_container_width=True)
        
                        elif is_bfs:
                            # --- BENEKU (BFS): 3 lines (OKW Production, GPP Production, Evacuation) ---
                            # OKW & GPP from GPPProductionRecord (reporting page), Evacuation from MB ("Dispatch to Jetty")
                            try:
                                from models import GPPProductionRecord
                            except Exception:
                                GPPProductionRecord = None
        
                            gpp_agg = None
                            if GPPProductionRecord is not None:
                                try:
                                    with get_session() as s2:
                                        recs = (
                                            s2.query(GPPProductionRecord)
                                            .filter(
                                                GPPProductionRecord.location_id == active_location_id,
                                                GPPProductionRecord.date >= trend_from,
                                                GPPProductionRecord.date <= trend_to,
                                            )
                                            .all()
                                        )
                                    if recs:
                                        gpp_df = pd.DataFrame(
                                            [
                                                {
                                                    "Date": r.date,
                                                    "OKW Production": float(getattr(r, "okw_production", 0.0) or 0.0),
                                                    "GPP Production": float(getattr(r, "total_production", 0.0) or 0.0),
                                                }
                                                for r in recs
                                            ]
                                        )
                                        gpp_agg = (
                                            gpp_df.groupby("Date", as_index=False)[
                                                ["OKW Production", "GPP Production"]
                                            ].sum()
                                        )
                                except Exception:
                                    gpp_agg = None
        
                            # Evacuation from MB
                            evac_agg = None
                            c_evac = _find_col(df_mb, ["Dispatch to Jetty"])
                            if c_evac:
                                evac_agg = (
                                    df_mb.groupby(dcol, as_index=False)[c_evac]
                                    .sum()
                                    .rename(columns={dcol: "Date", c_evac: "Evacuation"})
                                )
        
                            # Merge into daily skeleton
                            df_day = full_days.copy()  # has "Date"
        
                            if gpp_agg is not None:
                                df_day = df_day.merge(gpp_agg, on="Date", how="left")
                            else:
                                df_day["OKW Production"] = 0.0
                                df_day["GPP Production"] = 0.0
        
                            if evac_agg is not None:
                                df_day = df_day.merge(evac_agg, on="Date", how="left")
                            if "Evacuation" not in df_day.columns:
                                df_day["Evacuation"] = 0.0
        
                            for col in ["OKW Production", "GPP Production", "Evacuation"]:
                                if col not in df_day.columns:
                                    df_day[col] = 0.0
        
                            df_day[["OKW Production", "GPP Production", "Evacuation"]] = (
                                df_day[["OKW Production", "GPP Production", "Evacuation"]]
                                .apply(pd.to_numeric, errors="coerce")
                                .fillna(0.0)
                            )
                            df_day["DateTS"] = pd.to_datetime(df_day["Date"])
        
                            # Totals for small card
                            tot_okw = float(df_day["OKW Production"].sum())
                            tot_gpp = float(df_day["GPP Production"].sum())
                            tot_evac = float(df_day["Evacuation"].sum())
        
                            # extremes
                            def _ext(series):
                                s = series.fillna(0.0)
                                return (s == s.max()).astype(int), (s == s.min()).astype(int)
        
                            df_day["is_max_okw"], df_day["is_min_okw"] = _ext(df_day["OKW Production"])
                            df_day["is_max_gpp"], df_day["is_min_gpp"] = _ext(df_day["GPP Production"])
                            df_day["is_max_evac"], df_day["is_min_evac"] = _ext(df_day["Evacuation"])
        
                            # Long-form for plotting
                            frames = []
                            for col, s_name, imax, imin in [
                                ("OKW Production", "OKW Production", "is_max_okw", "is_min_okw"),
                                ("GPP Production", "GPP Production", "is_max_gpp", "is_min_gpp"),
                                ("Evacuation", "Evacuation", "is_max_evac", "is_min_evac"),
                            ]:
                                sub = df_day[["Date", "DateTS", col, imax, imin]].rename(
                                    columns={col: "Value", imax: "is_max", imin: "is_min"}
                                )
                                sub["Series"] = s_name
                                frames.append(sub)
                            plot_df = pd.concat(frames, ignore_index=True)
        
                            # Axis & scales
                            x_axis = alt.Axis(
                                title="Date", format="%d-%b", labelAngle=0,
                                tickCount={"interval": "day", "step": 1}
                            )
                            y_axis = alt.Axis(title="Quantity in bbls")
        
                            # Colors (Brown, Green, Blue) for BFS
                            domain = ["OKW Production", "GPP Production", "Evacuation"]
                            range_  = ["#8B4513", "#006400", "#1E90FF"]
        
                            y_domain_max = max(float(plot_df["Value"].max() or 1.0), 1.0) * 1.25
        
                            base = alt.Chart(plot_df).properties(height=360)
        
                            # Lines with legend
                            lines = base.mark_line(strokeWidth=2).encode(
                                x=alt.X("DateTS:T", axis=x_axis),
                                y=alt.Y("Value:Q", axis=y_axis, scale=alt.Scale(domain=[0, y_domain_max])),
                                color=alt.Color(
                                    "Series:N",
                                    scale=alt.Scale(domain=domain, range=range_),
                                    legend=alt.Legend(title=None, orient="top", symbolStrokeWidth=6),
                                ),
                            )
        
                            # Triangular points
                            pts = base.mark_point(shape="triangle-up", filled=False, size=60).encode(
                                x="DateTS:T",
                                y="Value:Q",
                                color=alt.Color("Series:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
                            )
        
                            # Regular labels
                            labels = base.mark_text(dy=-12, color="black", fontSize=11).encode(
                                x="DateTS:T",
                                y="Value:Q",
                                text=alt.Text("Value:Q", format=",.0f"),
                                color=alt.Color("Series:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
                            )
        
                            # Bold labels for extremes
                            max_labels = base.transform_filter(alt.datum.is_max == 1).mark_text(
                                dy=-12, color="black", fontWeight="bold", fontSize=12
                            ).encode(
                                x="DateTS:T",
                                y="Value:Q",
                                text=alt.Text("Value:Q", format=",.0f"),
                                color=alt.Color("Series:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
                            )
        
                            min_labels = base.transform_filter(alt.datum.is_min == 1).mark_text(
                                dy=-12, color="black", fontWeight="bold", fontSize=12
                            ).encode(
                                x="DateTS:T",
                                y="Value:Q",
                                text=alt.Text("Value:Q", format=",.0f"),
                                color=alt.Color("Series:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
                            )
        
                            # Totals card
                            y_max_all = float(df_day[["OKW Production", "GPP Production", "Evacuation"]].max().max())
                            y_min_all = float(df_day[["OKW Production", "GPP Production", "Evacuation"]].min().min())
                            card_top = max(y_domain_max * 0.98, y_max_all)
                            card_bottom = max(card_top * 0.70, y_min_all)
        
                            card_df = pd.DataFrame({
                                "x0": [pd.to_datetime(df_day["Date"].min())],
                                "x1": [pd.to_datetime(df_day["Date"].min()) +
                                    pd.Timedelta(days=max(1, (trend_to - trend_from).days // 4))],
                                "y0": [card_bottom],
                                "y1": [card_top],
                                "t1": [f"OKW: {tot_okw:,.0f} bbls"],
                                "t2": [f"GPP: {tot_gpp:,.0f} bbls"],
                                "t3": [f"Evac: {tot_evac:,.0f} bbls"],
                            })
        
                            card_rect = alt.Chart(card_df).mark_rect(
                                fill="#f8f9fa", stroke="#ccd1d6", opacity=0.9
                            ).encode(
                                x="x0:T", x2="x1:T", y="y0:Q", y2="y1:Q"
                            )
                            card_text1 = alt.Chart(card_df).mark_text(
                                align="left", dx=8, dy=8, fontSize=12
                            ).encode(
                                x="x0:T", y="y1:Q", text="t1"
                            )
                            card_text2 = alt.Chart(card_df).mark_text(
                                align="left", dx=8, dy=26, fontSize=12
                            ).encode(
                                x="x0:T", y="y1:Q", text="t2"
                            )
                            card_text3 = alt.Chart(card_df).mark_text(
                                align="left", dx=8, dy=44, fontSize=12
                            ).encode(
                                x="x0:T", y="y1:Q", text="t3"
                            )
        
                            chart_bfs = (
                                card_rect + card_text1 + card_text2 + card_text3 +
                                lines + pts + labels + max_labels + min_labels
                            ).properties(
                                height=360,
                                padding={"left": 5, "right": 5, "top": 5, "bottom": 40}
                            ).configure_axis(
                                labelColor="#000",
                                titleColor="#000",
                                labelFontSize=11,
                                titleFontSize=12,
                            ).configure_legend(
                                labelFontSize=11,
                                titleFontSize=12,
                                orient="top",
                            ).configure_view(strokeOpacity=0)
        
                            st.altair_chart(chart_bfs, use_container_width=True)
        
                        elif is_ndoni:
                            # --- NDONI: 2-line trend (Receipt, Evacuation) ---
                            # Receipt = Receipt from Agu + Receipt from OFS + Other Receipts
                            # Evacuation = Dispatch to barge
        
                            c_agu   = _find_col(df_mb, ["Receipt from Agu"])
                            c_ofs   = _find_col(df_mb, ["Receipt from OFS"])
                            c_other = _find_col(df_mb, ["Other Receipts"])
                            c_disp  = _find_col(df_mb, ["Dispatch to barge"])
        
                            df_nd = df_mb.copy()
        
                            def _num(col_name):
                                if not col_name:
                                    return 0.0
                                return pd.to_numeric(df_nd[col_name], errors="coerce").fillna(0.0)
        
                            df_nd["rcpt_agu"]   = _num(c_agu)
                            df_nd["rcpt_ofs"]   = _num(c_ofs)
                            df_nd["rcpt_other"] = _num(c_other)
                            df_nd["evac"]       = _num(c_disp)
        
                            agg = (
                                df_nd.groupby(dcol, as_index=False)[["rcpt_agu", "rcpt_ofs", "rcpt_other", "evac"]]
                                .sum()
                            )
                            agg = agg.rename(columns={dcol: "Date"})
                            agg["Receipt"]    = agg["rcpt_agu"] + agg["rcpt_ofs"] + agg["rcpt_other"]
                            agg["Evacuation"] = agg["evac"]
        
                            df_day = full_days.merge(agg[["Date", "Receipt", "Evacuation"]], on="Date", how="left")
        
                            for col in ["Receipt", "Evacuation"]:
                                if col not in df_day.columns:
                                    df_day[col] = 0.0
        
                            df_day[["Receipt", "Evacuation"]] = (
                                df_day[["Receipt", "Evacuation"]].apply(pd.to_numeric, errors="coerce").fillna(0.0)
                            )
                            df_day["DateTS"] = pd.to_datetime(df_day["Date"])
        
                            # Totals for card
                            tot_receipt = float(df_day["Receipt"].sum())
                            tot_evac    = float(df_day["Evacuation"].sum())
        
                            # extremes per series
                            def _ext(series):
                                s = series.fillna(0.0)
                                return (s == s.max()).astype(int), (s == s.min()).astype(int)
        
                            df_day["is_max_rcpt"], df_day["is_min_rcpt"] = _ext(df_day["Receipt"])
                            df_day["is_max_evac"], df_day["is_min_evac"] = _ext(df_day["Evacuation"])
        
                            # Long-form for plotting (2 series)
                            frames = []
                            for col, s_name, imax, imin in [
                                ("Receipt", "Receipt", "is_max_rcpt", "is_min_rcpt"),
                                ("Evacuation", "Evacuation", "is_max_evac", "is_min_evac"),
                            ]:
                                sub = df_day[["Date", "DateTS", col, imax, imin]].rename(
                                    columns={col: "Value", imax: "is_max", imin: "is_min"}
                                )
                                sub["Series"] = s_name
                                frames.append(sub)
        
                            plot_df = pd.concat(frames, ignore_index=True)
        
                            x_axis = alt.Axis(
                                title="Date", format="%d-%b", labelAngle=0,
                                tickCount={"interval": "day", "step": 1}
                            )
                            y_axis = alt.Axis(title="Quantity in bbls")
        
                            # Colors: Brown (Receipt) & Green (Evacuation)
                            domain = ["Receipt", "Evacuation"]
                            range_ = ["#8B4513", "#006400"]
        
                            y_domain_max = max(float(plot_df["Value"].max() or 1.0), 1.0) * 1.25
                            base = alt.Chart(plot_df).properties(height=360)
        
                            # Lines
                            lines = base.mark_line(strokeWidth=2).encode(
                                x=alt.X("DateTS:T", axis=x_axis),
                                y=alt.Y("Value:Q", axis=y_axis, scale=alt.Scale(domain=[0, y_domain_max])),
                                color=alt.Color(
                                    "Series:N",
                                    scale=alt.Scale(domain=domain, range=range_),
                                    legend=alt.Legend(title=None, orient="top", symbolStrokeWidth=6),
                                ),
                            )
        
                            # Points
                            pts = base.mark_point(shape="triangle-up", filled=False, size=60).encode(
                                x="DateTS:T",
                                y="Value:Q",
                                color=alt.Color("Series:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
                            )
        
                            # Normal labels
                            labels = base.mark_text(dy=-12, color="black", fontSize=11).encode(
                                x="DateTS:T",
                                y="Value:Q",
                                text=alt.Text("Value:Q", format=",.0f"),
                                color=alt.Color("Series:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
                            )
        
                            # Bold labels for highest/lowest
                            max_labels = base.transform_filter(alt.datum.is_max == 1).mark_text(
                                dy=-12, color="black", fontWeight="bold", fontSize=12
                            ).encode(
                                x="DateTS:T",
                                y="Value:Q",
                                text=alt.Text("Value:Q", format=",.0f"),
                                color=alt.Color("Series:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
                            )
        
                            min_labels = base.transform_filter(alt.datum.is_min == 1).mark_text(
                                dy=-12, color="black", fontWeight="bold", fontSize=12
                            ).encode(
                                x="DateTS:T",
                                y="Value:Q",
                                text=alt.Text("Value:Q", format=",.0f"),
                                color=alt.Color("Series:N", scale=alt.Scale(domain=domain, range=range_), legend=None),
                            )
        
                            # Card inside graph (totals for range)
                            y_max_all = float(df_day[["Receipt", "Evacuation"]].max().max())
                            y_min_all = float(df_day[["Receipt", "Evacuation"]].min().min())
                            card_top = max(y_domain_max * 0.98, y_max_all)
                            card_bottom = max(card_top * 0.70, y_min_all)
        
                            card_df = pd.DataFrame({
                                "x0": [pd.to_datetime(df_day["Date"].min())],
                                "x1": [pd.to_datetime(df_day["Date"].min()) +
                                    pd.Timedelta(days=max(1, (trend_to - trend_from).days // 4))],
                                "y0": [card_bottom],
                                "y1": [card_top],
                                "t1": [f"Receipt: {tot_receipt:,.0f} bbls"],
                                "t2": [f"Evac: {tot_evac:,.0f} bbls"],
                            })
        
                            card_rect = alt.Chart(card_df).mark_rect(
                                fill="#f8f9fa", stroke="#ccd1d6", opacity=0.9
                            ).encode(
                                x="x0:T", x2="x1:T", y="y0:Q", y2="y1:Q"
                            )
                            card_text1 = alt.Chart(card_df).mark_text(
                                align="left", dx=8, dy=8, fontSize=12
                            ).encode(
                                x="x0:T", y="y1:Q", text="t1"
                            )
                            card_text2 = alt.Chart(card_df).mark_text(
                                align="left", dx=8, dy=26, fontSize=12
                            ).encode(
                                x="x0:T", y="y1:Q", text="t2"
                            )
        
                            chart_ndoni = (
                                card_rect + card_text1 + card_text2 +
                                lines + pts + labels + max_labels + min_labels
                            ).properties(
                                height=360,
                                padding={"left": 5, "right": 5, "top": 5, "bottom": 40}
                            ).configure_axis(
                                labelColor="#000",
                                titleColor="#000",
                                labelFontSize=11,
                                titleFontSize=12,
                            ).configure_legend(
                                labelFontSize=11,
                                titleFontSize=12,
                                orient="top",
                            ).configure_view(strokeOpacity=0)
        
                            st.altair_chart(chart_ndoni, use_container_width=True)
        
                        else:
                            # --- OTHER LOCATIONS (incl. AGGU): 2-line trend (Production=Receipt, Evacuation=Dispatch) ---
                            c_receipt  = _find_col(df_mb, ["Receipt"])
                            c_dispatch = _find_col(df_mb, ["Dispatch"])
        
                            agg = df_mb.groupby(dcol).agg({
                                c_receipt if c_receipt else dcol: "sum",
                                c_dispatch if c_dispatch else dcol: "sum",
                            }).reset_index()
        
                            rename_map = {}
                            if c_receipt:  rename_map[c_receipt]  = "Production"
                            if c_dispatch: rename_map[c_dispatch] = "Evacuation"
                            agg = agg.rename(columns=rename_map)
        
                            df_day = full_days.merge(agg.rename(columns={dcol: "Date"}), on="Date", how="left")
                            for col in ["Production", "Evacuation"]:
                                if col not in df_day.columns:
                                    df_day[col] = 0.0
                            df_day[["Production", "Evacuation"]] = df_day[["Production", "Evacuation"]].apply(
                                pd.to_numeric, errors="coerce"
                            ).fillna(0.0)
                            df_day["DateTS"] = pd.to_datetime(df_day["Date"])
        
                            # extremes
                            def _ext(series):
                                s = series.fillna(0.0)
                                return (s == s.max()).astype(int), (s == s.min()).astype(int)
        
                            df_day["is_max_prod"], df_day["is_min_prod"] = _ext(df_day["Production"])
                            df_day["is_max_evac"], df_day["is_min_evac"] = _ext(df_day["Evacuation"])
        
                            x_axis = alt.Axis(
                                title="Date", format="%d-%b", labelAngle=0,
                                tickCount={"interval": "day", "step": 1}
                            )
                            y_axis = alt.Axis(title="Quantity in bbls")
        
                            # Colors for other locations: Brown (Production) and Green (Evacuation)
                            color_prod = "#8B4513"  # brown
                            color_evac = "#006400"  # dark green
        
                            y_domain_max = max(float(df_day[["Production", "Evacuation"]].max().max() or 1.0), 1.0) * 1.25
                            base = alt.Chart(df_day).properties(height=320)
        
                            # Production
                            p_line = base.mark_line().encode(
                                x=alt.X("DateTS:T", axis=x_axis),
                                y=alt.Y("Production:Q", axis=y_axis, scale=alt.Scale(domain=[0, y_domain_max])),
                                color=alt.value(color_prod),
                            )
                            p_pts  = base.mark_point(shape="triangle-up", color=color_prod).encode(
                                x="DateTS:T", y="Production:Q"
                            )
                            p_txt  = base.mark_text(dy=-12, color="black").encode(
                                x="DateTS:T",
                                y="Production:Q",
                                text=alt.Text("Production:Q", format=",.0f"),
                            )
                            e_line = base.mark_line(strokeDash=[4, 4]).encode(
                                x=alt.X("DateTS:T", axis=x_axis),
                                y=alt.Y("Evacuation:Q", axis=y_axis, scale=alt.Scale(domain=[0, y_domain_max])),
                                color=alt.value(color_evac),
                            )
                            e_pts = base.mark_point(shape="triangle-down", color=color_evac).encode(
                                x="DateTS:T", y="Evacuation:Q"
                            )
                            e_txt = base.mark_text(dy=-12, color="black").encode(
                                x="DateTS:T",
                                y="Evacuation:Q",
                                text=alt.Text("Evacuation:Q", format=",.0f"),
                            )
                            chart_other = (p_line + p_pts + p_txt + e_line + e_pts + e_txt).properties(width="container")
                            st.altair_chart(chart_other, use_container_width=True)
        
        
        
        # ============ QUICK ACTIONS ============
        st.markdown("### ? Quick Actions")
        
        action_cols = st.columns(4)
        
        with action_cols[0]:
            if can_view_tanks:
                if st.button("➕ New Tank Transaction", use_container_width=True, type="primary"):
                    st.session_state["page"] = "Tank Transactions"
                    _st_safe_rerun()
        
        with action_cols[1]:
            if can_view_yade:
                if st.button("➕ New YADE Voyage", use_container_width=True, type="primary"):
                    st.session_state["page"] = "Yade Transactions"
                    _st_safe_rerun()
        
        with action_cols[2]:
            if can_view_tanker:
                if st.button("➕ New Tanker Dispatch", use_container_width=True, type="primary"):
                    st.session_state["page"] = "Tanker Transactions"
                    _st_safe_rerun()
        
        with action_cols[3]:
            if st.button("👁️ View Transactions", use_container_width=True, type="primary"):
                st.session_state["page"] = "View Transactions"
                _st_safe_rerun()
        
        st.markdown("---")
        
        # ============ RECENT ACTIVITY ============
        st.markdown("### 📋 Recent Activity")
        
        with get_session() as s:
            recent_tank_txns = s.query(TankTransaction).filter(
                TankTransaction.location_id == active_location_id
            ).order_by(
                TankTransaction.date.desc(),
                TankTransaction.time.desc()
            ).limit(5).all()
            
            recent_yade_txns = s.query(YadeVoyage).filter(
                YadeVoyage.location_id == active_location_id
            ).order_by(
                YadeVoyage.date.desc(),
                YadeVoyage.time.desc()
            ).limit(5).all()
            
            try:
                recent_tanker_txns = s.query(TankerTransaction).filter(
                    TankerTransaction.location_id == active_location_id
                ).order_by(
                    TankerTransaction.transaction_date.desc(),
                    TankerTransaction.transaction_time.desc()
                ).limit(5).all()
            except:
                recent_tanker_txns = []
        
        # Combine activities
        all_activities = []
        
        for txn in recent_tank_txns:
            all_activities.append({
                "icon": "🛢️",
                "datetime": datetime.combine(txn.date, txn.time),
                "description": f"Tank Transaction: {txn.tank_name} - {txn.operation.value if txn.operation else 'N/A'}",
                "user": txn.created_by or "Unknown",
                "details": f"Ticket: {txn.ticket_id}"
            })
        
        for txn in recent_yade_txns:
            # Get destination safely
            dest = txn.destination
            if hasattr(dest, 'value'):
                dest_str = dest.value
            elif dest:
                dest_str = str(dest)
            else:
                dest_str = 'N/A'
            
            all_activities.append({
                "icon": "⛴️",
                "datetime": datetime.combine(txn.date, txn.time),
                "description": f"YADE Voyage: {txn.yade_name} - Voyage {txn.voyage_no}",
                "user": txn.created_by or "Unknown",
                "details": f"Destination: {dest_str}"
            })
        
        for txn in recent_tanker_txns:
            all_activities.append({
                "icon": "⛴️",
                "datetime": datetime.combine(txn.transaction_date, txn.transaction_time),
                "description": f"Tanker Dispatch: {txn.tanker_name}",
                "user": "System",
                "details": f"Convoy: {txn.convoy_no}"
            })
        
        all_activities.sort(key=lambda x: x["datetime"], reverse=True)
        
        if not all_activities:
            st.info("ℹ️ No recent activity")
        else:
            for activity in all_activities[:10]:
                time_ago = datetime.now() - activity["datetime"]
                
                if time_ago.days > 0:
                    time_str = f"{time_ago.days} day{'s' if time_ago.days > 1 else ''} ago"
                elif time_ago.seconds >= 3600:
                    hours = time_ago.seconds // 3600
                    time_str = f"{hours} hour{'s' if hours > 1 else ''} ago"
                elif time_ago.seconds >= 60:
                    minutes = time_ago.seconds // 60
                    time_str = f"{minutes} minute{'s' if minutes > 1 else ''} ago"
                else:
                    time_str = "Just now"
                
                st.markdown(f"""
                    <div class="activity-item">
                        <div style="display: flex; justify-content: space-between; align-items: start;">
                            <div style="flex: 1;">
                                <div style="font-weight: bold; color: #2c3e50; margin-bottom: 0.3rem;">
                                    {activity['icon']} {activity['description']}
                                </div>
                                <div style="font-size: 0.9rem; color: #6c757d; margin-bottom: 0.3rem;">
                                    {activity['details']}
                                </div>
                                <div style="font-size: 0.85rem; color: #adb5bd;">
                                    By: {activity['user']}
                                </div>
                            </div>
                            <div style="white-space: nowrap; margin-left: 1rem; color: #6c757d; font-size: 0.85rem;">
                                {time_str}
                            </div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # ============ SYSTEM INFO ============
        st.markdown("### ℹ️ System Information")
        
        info_cols = st.columns(3)
        
        with info_cols[0]:
            st.info(f"**Location:** {loc.name} ({loc.code})")
        
        with info_cols[1]:
            st.info(f"**Role:** {user['role'].title()}")
        
        with info_cols[2]:
            st.info(f"**Time:** {datetime.now().strftime('%I:%M %p')}")
    
    # ========================= 2FA VERIFICATION PAGE =========================