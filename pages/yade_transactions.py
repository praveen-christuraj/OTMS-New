"""
Auto-generated module for the 'Yade Transactions' page.
"""
from __future__ import annotations
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
from db import get_session
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
from pages.helpers import st_safe_rerun, archive_payload_for_delete

def render() -> None:
        header("Yade Transactions")
        
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
                if _cfg.get("page_access", {}).get("Yade Transactions") is False:
                    st.error("⚠️ YADE Transactions page is disabled for this location.")
                    st.stop()
        except Exception:
            pass
        st.markdown("#### Record YADE Barge Loadings")
        
        # ============ LOCATION ACCESS CHECK ============
        active_location_id = st.session_state.get("active_location_id")
        if not active_location_id:
            st.error("⚠️ No active location selected. Please select a location from the Home page.")
            st.stop()
        
        # Verify user has access to this location
        user = st.session_state.get("auth_user")
        if user:
            from auth import AuthManager
            if not AuthManager.can_access_location(user, active_location_id):
                st.error("🚫 You do not have access to this location.")
                st.stop()
        
        # ========== CHECK PERMISSIONS ==========
        from permission_manager import PermissionManager
        
        with get_session() as s:
            from location_manager import LocationManager
            
            # Get location info
            loc = LocationManager.get_location_by_id(s, active_location_id)
            if not loc:
                st.error("? Location not found.")
                st.stop()
            
            st.info(f"📍 **Active Location:** {loc.name} ({loc.code})")
    
            # Apply location-based page visibility: hide page if disabled in config (non-admin)
            try:
                with get_session() as _s_cfg:
                    from location_config import LocationConfig
                    _cfg = LocationConfig.get_config(_s_cfg, active_location_id)
                if not _cfg.get("page_visibility", {}).get("show_yade_transactions", False) and (user.get("role", "").lower() not in ["admin-operations", "manager"]):
                    st.error("⚠️ YADE Transactions page is disabled for this location.")
                    st.stop()
            except Exception:
                pass
            
            # Check if feature is allowed at this location (Admin can access everywhere)
            if not PermissionManager.can_access_feature(s, active_location_id, "yade_transactions", user["role"]):
                st.error("🚫 **Access Denied**")
                st.warning(f"**YADE Transactions** are not available at **{loc.name}**")
                
                # Show where it's available
                allowed_locs = PermissionManager.get_allowed_locations_for_feature(s, "yade_transactions")
                if allowed_locs:
                    st.info(f"? This feature is available at: **{', '.join(allowed_locs)}**")
                
                st.markdown("---")
                st.caption(f"Current Location: **{loc.name} ({loc.code})**")
                st.caption("YADE Transactions Access: **? Denied**")
                st.stop()
            
            # Check if user can make entries
            can_make_entries = PermissionManager.can_make_entries(s, user["role"], active_location_id)
        
        # ============ YADE ENABLED - SHOW SUCCESS MESSAGE ============
        st.success(f"? YADE Transactions enabled at {loc.name}")
        
        # ------------ helpers ------------
        def hhmm_ok(s: str) -> bool:
            if not s or len(s) not in (4, 5): return False
            s = s.strip()
            if ":" not in s: return False
            h, m = s.split(":", 1)
            if not (h.isdigit() and m.isdigit()): return False
            h, m = int(h), int(m)
            return 0 <= h <= 23 and 0 <= m <= 59
    
        def only_digits_hyphen(s: str) -> bool:
            return bool(re.fullmatch(r"[0-9-]+", s)) and any(ch.isdigit() for ch in s)
    
        # YADE barges from DB - Global (not location-filtered)
        with get_session() as s:
            _barges = s.query(YadeBarge).order_by(YadeBarge.name).all()
    
        yade_names = [b.name for b in _barges] if _barges else ["(No YADE barges � add in Add Asset)"]
        design_map = {b.name: str(b.design) for b in _barges}
    
        # ============ YADE HELPER FUNCTIONS ============
    
        def _persist_toa_from_current_inputs(
            session,
            voyage_obj,
            yade_name: str,
            tank_ids: list,
            num_samples: int
        ):
            """
            Persist TOA (Transfer of Account) data from current YADE voyage inputs.
            Creates TOA summary and stage records with ACTUAL calculations.
            """
            from models import TOAYadeSummary, TOAYadeStage, YadeDip, YadeSampleParam
            from datetime import datetime
            
            try:
                # Import the calculator
                try:
                    from yade_toa_calculator import calculate_yade_toa
                except ImportError:
                    st.warning("⚠️ yade_toa_calculator not found. Using placeholder TOA values.")
                    # Create placeholder TOA
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
                        gsv_loaded_bbl=0.0
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
                            fw_bbl=0.0
                        )
                        session.add(stage)
                    return
                
                # Remove existing summary/stage data for this voyage before recalculating
                session.query(TOAYadeStage).filter(TOAYadeStage.voyage_id == voyage_obj.id).delete(synchronize_session=False)
                session.query(TOAYadeSummary).filter(TOAYadeSummary.voyage_id == voyage_obj.id).delete(synchronize_session=False)
                session.flush()
    
                # Get dips from database (already saved)
                before_dips_db = session.query(YadeDip).filter(
                    YadeDip.voyage_id == voyage_obj.id,
                    YadeDip.stage == "before"
                ).all()
                
                after_dips_db = session.query(YadeDip).filter(
                    YadeDip.voyage_id == voyage_obj.id,
                    YadeDip.stage == "after"
                ).all()
                
                # Get sample params from database
                before_params_db = session.query(YadeSampleParam).filter(
                    YadeSampleParam.voyage_id == voyage_obj.id,
                    YadeSampleParam.stage == "before"
                ).first()
                
                after_params_db = session.query(YadeSampleParam).filter(
                    YadeSampleParam.voyage_id == voyage_obj.id,
                    YadeSampleParam.stage == "after"
                ).first()
                
                # Prepare dip data
                dip_data = {
                    "before": {},
                    "after": {}
                }
                
                for dip in before_dips_db:
                    dip_data["before"][dip.tank_id] = {
                        "total_cm": float(dip.total_cm or 0.0),
                        "water_cm": float(dip.water_cm or 0.0)
                    }
                
                for dip in after_dips_db:
                    dip_data["after"][dip.tank_id] = {
                        "total_cm": float(dip.total_cm or 0.0),
                        "water_cm": float(dip.water_cm or 0.0)
                    }
                
                # Prepare sample data
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
                    }
                }
                
                # Calculate TOA
                toa_result = calculate_yade_toa(
                    yade_name=yade_name,
                    dip_data=dip_data,
                    sample_data=sample_data,
                    session=session
                )
                
                if not toa_result:
                    print("⚠️  TOA calculation returned None, using placeholders")
                    toa_result = {
                        "before": {"gov_bbl": 0, "gsv_bbl": 0, "bsw_pct": 0, "bsw_bbl": 0, "nsv_bbl": 0, "lt": 0, "mt": 0, "fw_bbl": 0},
                        "after": {"gov_bbl": 0, "gsv_bbl": 0, "bsw_pct": 0, "bsw_bbl": 0, "nsv_bbl": 0, "lt": 0, "mt": 0, "fw_bbl": 0},
                        "loaded": {"gsv_bbl": 0}
                    }
                
                # Create TOA Summary
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
                    gsv_loaded_bbl=toa_result.get("loaded", {}).get("gsv_bbl", 0.0)
                )
                
                session.add(summary)
                
                # Create TOA Stage records
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
                        fw_bbl=stage_data.get("fw_bbl", 0.0)
                    )
                    
                    session.add(stage)
                
                print(f"? TOA data calculated and saved for voyage {voyage_obj.id}")
            
            except Exception as ex:
                print(f"⚠️  Failed to persist TOA: {ex}")
                import traceback
                traceback.print_exc()
    
    
        def _save_yade_dips(session, voyage_id: int, tank_ids: list, stage: str):
            """Save YADE dip readings for a specific stage"""
            from models import YadeDip
            
            dips_key = f"yade_{stage}_dips"
            dips = st.session_state.get(dips_key, {})
            
            for tank_id in tank_ids:
                total_cm = dips.get(f"{tank_id}_total", 0.0)
                water_cm = dips.get(f"{tank_id}_water", 0.0)
                
                dip_entry = YadeDip(
                    voyage_id=voyage_id,
                    tank_id=tank_id,
                    stage=stage,
                    total_cm=total_cm,
                    water_cm=water_cm
                )
                
                session.add(dip_entry)
    
    
        def _save_yade_sample_params(session, voyage_id: int, stage: str):
            """Save YADE sample parameters for a specific stage"""
            from models import YadeSampleParam
            
            params_key = f"yade_{stage}_params"
            params = st.session_state.get(params_key, {})
            
            obs_mode = params.get("obs_mode", "API")
            obs_val = params.get("obs_val", 0.0)
            sample_unit = params.get("sample_unit", "C")
            sample_temp = params.get("sample_temp", 0.0)
            tank_temp = params.get("tank_temp", 0.0)
            ccf = params.get("ccf", 1.0)
            bsw_pct = params.get("bsw_pct", 0.0)
            
            sample_param = YadeSampleParam(
                voyage_id=voyage_id,
                stage=stage,
                obs_mode=obs_mode,
                obs_val=obs_val,
                sample_unit=sample_unit,
                sample_temp=sample_temp,
                tank_temp=tank_temp,
                ccf=ccf,
                bsw_pct=bsw_pct
            )
            
            session.add(sample_param)
    
    
        def _save_yade_seal_details(session, voyage_id: int):
            """Save YADE seal details"""
            from models import YadeSealDetail
            
            seals = st.session_state.get("yade_seals", {})
            
            seal_detail = YadeSealDetail(
                voyage_id=voyage_id,
                c1_mh1=seals.get("c1_mh1"),
                c1_mh2=seals.get("c1_mh2"),
                c1_lock=seals.get("c1_lock"),
                c1_diphatch=seals.get("c1_diphatch"),
                c2_mh1=seals.get("c2_mh1"),
                c2_mh2=seals.get("c2_mh2"),
                c2_lock=seals.get("c2_lock"),
                c2_diphatch=seals.get("c2_diphatch"),
                p1_mh1=seals.get("p1_mh1"),
                p1_mh2=seals.get("p1_mh2"),
                p1_lock=seals.get("p1_lock"),
                p1_diphatch=seals.get("p1_diphatch"),
                p2_mh1=seals.get("p2_mh1"),
                p2_mh2=seals.get("p2_mh2"),
                p2_lock=seals.get("p2_lock"),
                p2_diphatch=seals.get("p2_diphatch"),
                s1_mh1=seals.get("s1_mh1"),
                s1_mh2=seals.get("s1_mh2"),
                s1_lock=seals.get("s1_lock"),
                s1_diphatch=seals.get("s1_diphatch"),
                s2_mh1=seals.get("s2_mh1"),
                s2_mh2=seals.get("s2_mh2"),
                s2_lock=seals.get("s2_lock"),
                s2_diphatch=seals.get("s2_diphatch")
            )
            
            session.add(seal_detail)
    
        # ------------ TOP METADATA ------------
        st.markdown("#### Top Metadata")
    
        # [REST OF YOUR YADE FORM CODE - KEEP EVERYTHING AS IS]
        # ... (all your existing form fields, dip entries, sample parameters, seals, etc.)
        
        # Provide a helpful hint with light‑bulb icon and arrow for navigation
        st.info("💡 **View saved YADE voyages in:** View Transactions → YADE Voyages")
    
        # ------------ TOP METADATA ------------
        st.markdown("#### New YADE Voyage Entry")
    
        m1, m2, m3 = st.columns(3)
        with m1:
            yade_no = st.selectbox("1) YADE No *", yade_names, index=0, key="yt_yade_no")
            selected_design = design_map.get(yade_no)
            design_choice = selected_design or st.selectbox("Tank Design *", ["6", "4"], index=0, key="yt_design_fallback")
        with m2:
            voyage_no = st.text_input("2) Voyage number * (digits & '-' only)", placeholder="e.g., 12-3", key="yt_voyage_no")
            convoy_no = st.text_input("3) Convoy number * (digits & '-' only)", placeholder="e.g., 5-1", key="yt_convoy_no")
        with m3:
            tx_date = st.date_input("4) Date * (DD/MM/YYYY)", value=date.today(), format="DD/MM/YYYY", key="yt_date")
            tx_time = st.text_input("5) Time * (HH:MM)", value="08:00", key="yt_time")
    
        m4, m5, m6 = st.columns(3)
        with m4:
            cargo = st.selectbox("6) Cargo *",
                                 [x.value for x in CargoKind],
                                 key="yt_cargo")
        with m5:
            destination = st.selectbox("7) Destination *", [x.value for x in DestinationKind], index=0, key="yt_destination")
        with m6:
            loading_berth = st.selectbox("8) Loading Berth *", [x.value for x in LoadingBerthKind], key="yt_berth")
    
        st.markdown("---")
    
        # ------------ TANK SET BY DESIGN ------------
        tank_ids_6 = ["C1", "C2", "P1", "P2", "S1", "S2"]
        tank_ids_4 = ["P1", "P2", "S1", "S2"]
        tank_ids = tank_ids_6 if str(design_choice) == "6" else tank_ids_4
    
        # ------------ Dip Entry Tables (Before / After) ------------
        st.markdown("#### Dip Entry Tables")
        left, right = st.columns(2)
    
        with left:
            st.markdown("##### Before Loading/Unloading")
            b_dcol, b_tcol = st.columns(2)
            with b_dcol:
                before_date = st.date_input("Gauging Date (Before)", value=tx_date, format="DD/MM/YYYY", key="before_date")
            with b_tcol:
                before_time = st.text_input("Gauging Time (Before) (HH:MM)", value="07:30", key="before_time")
    
            st.caption("Enter **Total Dip** and **Water Dip** (cm) for each tank")
            for tid in tank_ids:
                r1, r2, r3 = st.columns([0.25, 0.375, 0.375])
                with r1:
                    st.text_input("Tank", value=tid, disabled=True, key=f"before_tank_{tid}")
                with r2:
                    st.number_input("Total Dip (cm)", min_value=0.0, step=0.1, key=f"before_total_{tid}")
                with r3:
                    st.number_input("Water Dip (cm)", min_value=0.0, step=0.1, key=f"before_water_{tid}")
    
        with right:
            st.markdown("##### After Loading/Unloading")
            a_dcol, a_tcol = st.columns(2)
            with a_dcol:
                after_date = st.date_input("Gauging Date (After)", value=tx_date, format="DD/MM/YYYY", key="after_date")
            with a_tcol:
                after_time = st.text_input("Gauging Time (After) (HH:MM)", value="17:30", key="after_time")
    
            st.caption("Enter **Total Dip** and **Water Dip** (cm) for each tank")
            for tid in tank_ids:
                r1, r2, r3 = st.columns([0.25, 0.375, 0.375])
                with r1:
                    st.text_input("Tank ", value=tid, disabled=True, key=f"after_tank_{tid}")
                with r2:
                    st.number_input("Total Dip (cm) ", min_value=0.0, step=0.1, key=f"after_total_{tid}")
                with r3:
                    st.number_input("Water Dip (cm) ", min_value=0.0, step=0.1, key=f"after_water_{tid}")
    
        st.markdown("---")
        
        # ====================== SAMPLE PARAMETERS (Before / After) ======================
        st.markdown("### Sample Parameters")
    
        def _to_f(c_or_f: float, unit: str) -> float:
            if unit.upper().startswith("C"):
                return round((float(c_or_f) * 1.8) + 32.0, 1)
            return float(c_or_f or 0.0)
    
        def _to_c(c_or_f: float, unit: str) -> float:
            if unit.upper().startswith("F"):
                return round((float(c_or_f) - 32.0) / 1.8, 1)
            return float(c_or_f or 0.0)
    
        WAT60_LOCAL = 999.012
        
        def api_from_density(density_kgm3: float) -> float:
            if not density_kgm3 or density_kgm3 <= 0: return 0.0
            sg = float(density_kgm3) / WAT60_LOCAL
            if sg <= 0: return 0.0
            return round(141.5 / sg - 131.5, 2)
    
        def density_from_api(api: float) -> float:
            if not api or api <= 0: return 0.0
            sg = 141.5 / (float(api) + 131.5)
            return round(sg * WAT60_LOCAL, 1)
    
        safe_yade = re.sub(r'[^A-Za-z0-9]', '_', str(yade_no))
        safe_voy  = re.sub(r'[^A-Za-z0-9]', '_', str(voyage_no))
        ns_sp     = f"ysp_{safe_yade}_{safe_voy}"
    
        if "yade_sample_params" not in st.session_state:
            st.session_state["yade_sample_params"] = {}
        if ns_sp not in st.session_state["yade_sample_params"]:
            st.session_state["yade_sample_params"][ns_sp] = {}
    
        def _sample_param_row(stage_key: str):
            st.markdown(f"#### {stage_key.title()}")
    
            c1, c2, c3, c4 = st.columns([0.28, 0.24, 0.24, 0.24])
            with c1:
                obs_mode = st.selectbox(
                    "Observed Input",
                    ["Observed API", "Observed Density (kg/m3)"],
                    index=0,
                    key=f"{ns_sp}_{stage_key}_obs_mode"
                )
            with c2:
                sample_unit = st.selectbox(
                    "Sample Temperature Unit",
                    ["°F", "°C"],
                    index=0,
                    key=f"{ns_sp}_{stage_key}_sample_unit"
                )
            with c3:
                sample_temp = _temperature_input(
                    "Sample Temperature",
                    sample_unit,
                    key=f"{ns_sp}_{stage_key}_sample_temp",
                )
            with c4:
                tank_temp = _temperature_input(
                    "Tank Temperature",
                    sample_unit,
                    key=f"{ns_sp}_{stage_key}_tank_temp",
                )
    
            d1, d2, d3 = st.columns([0.34, 0.33, 0.33])
            with d1:
                obs_min, obs_max = _observed_value_bounds(obs_mode)
                obs_val = _bounded_number_input(
                    "Observed Value",
                    key=f"{ns_sp}_{stage_key}_obs_val",
                    min_value=obs_min,
                    max_value=obs_max,
                    step=0.1,
                )
            with d2:
                ccf = st.number_input(
                    "Calibration Correction Factor",
                    min_value=0.000001,
                    value=1.0,
                    step=0.0001,
                    key=f"{ns_sp}_{stage_key}_ccf",
                    help="Default 1.0000. Cannot be 0."
                )
            with d3:
                bsw_pct = st.number_input(
                    "BS&W %",
                    min_value=0.0, max_value=100.0, step=0.01,
                    key=f"{ns_sp}_{stage_key}_bsw_pct",
                    help="Basic Sediment & Water percentage (e.g., 0.25)."
                )
    
            st.session_state["yade_sample_params"][ns_sp][stage_key] = {
                "obs_mode": obs_mode,
                "obs_val": float(obs_val or 0.0),
                "sample_unit": sample_unit,
                "sample_temp": float(sample_temp or 0.0),
                "tank_temp": float(tank_temp or 0.0),
                "ccf": float(ccf or 1.0),
                "bsw_pct": float(bsw_pct or 0.0),
            }
    
        col_before, col_after = st.columns(2)
        with col_before:
            _sample_param_row("before")
        with col_after:
            _sample_param_row("after")
    
        # ===== SEAL DETAILS =====
        st.markdown("---")
        st.markdown("### Seal Details")
    
        tanks_seal = ["C1","C2","P1","P2","S1","S2"] if str(design_choice) == "6" else ["P1","P2","S1","S2"]
    
        _safe_yade = re.sub(r"[^A-Za-z0-9]", "_", str(yade_no))
        _safe_voy  = re.sub(r"[^A-Za-z0-9]", "_", str(voyage_no))
        ns_seal    = f"yseal_{_safe_yade}_{_safe_voy}"
    
        if "yade_seals" not in st.session_state:
            st.session_state["yade_seals"] = {}
    
        if ns_seal not in st.session_state["yade_seals"]:
            st.session_state["yade_seals"][ns_seal] = {t: {"mh1":"", "mh2":"", "lock":"", "diph":""} for t in tanks_seal}
    
        hdr = st.columns([0.10, 0.225, 0.225, 0.225, 0.225])
        hdr[0].markdown("**Tank**")
        hdr[1].markdown("**Manhole-1 Seal No**")
        hdr[2].markdown("**Manhole-2 Seal No**")
        hdr[3].markdown("**Lock No**")
        hdr[4].markdown("**Dip Hatch Seal No**")
    
        for t in tanks_seal:
            row = st.columns([0.10, 0.225, 0.225, 0.225, 0.225])
            row[0].write(t)
            kbase = f"{ns_seal}_{t}"
    
            mh1 = row[1].text_input("Manhole-1 Seal No", key=f"{kbase}_mh1",
                                    value=st.session_state["yade_seals"][ns_seal][t]["mh1"], label_visibility="collapsed")
            mh2 = row[2].text_input("Manhole-2 Seal No", key=f"{kbase}_mh2",
                                    value=st.session_state["yade_seals"][ns_seal][t]["mh2"], label_visibility="collapsed")
            lk  = row[3].text_input("Lock No",            key=f"{kbase}_lock",
                                    value=st.session_state["yade_seals"][ns_seal][t]["lock"], label_visibility="collapsed")
            dh  = row[4].text_input("Dip Hatch Seal No",  key=f"{kbase}_diph",
                                    value=st.session_state["yade_seals"][ns_seal][t]["diph"], label_visibility="collapsed")
    
            st.session_state["yade_seals"][ns_seal][t]["mh1"]  = mh1.strip()
            st.session_state["yade_seals"][ns_seal][t]["mh2"]  = mh2.strip()
            st.session_state["yade_seals"][ns_seal][t]["lock"] = lk.strip()
            st.session_state["yade_seals"][ns_seal][t]["diph"] = dh.strip()
    
        # ---- FINAL Save button at very bottom ----
        st.markdown("---")
        save_key = f"yade_save_btn_{safe_yade}_{safe_voy}"
        # Use floppy‑disk icon for the save action
        save_clicked = st.button("💾 Save YADE Voyage", type="primary", key=save_key, use_container_width=True)
    
        # ------------ SAVE (bottom) ------------
        if save_clicked:
            errs = []
            if "(No YADE barges" in yade_no:
                errs.append("Please add YADE barges under **Add Asset** and select a valid YADE No.")
            if not only_digits_hyphen(voyage_no):
                errs.append("Voyage number: only digits and '-' allowed.")
            if not only_digits_hyphen(convoy_no):
                errs.append("Convoy number: only digits and '-' allowed.")
            if not hhmm_ok(tx_time) or not hhmm_ok(before_time) or not hhmm_ok(after_time):
                errs.append("All times must be HH:MM (24-hour).")
    
            if errs:
                for e in errs:
                    st.error(e)
            else:
                try:
                    tx_time_obj = datetime.strptime(tx_time, "%H:%M").time()
                    btime_obj   = datetime.strptime(before_time, "%H:%M").time()
                    atime_obj   = datetime.strptime(after_time, "%H:%M").time()
                except Exception:
                    st.error("Time parsing failed. Use HH:MM (24-hour).")
                    tx_time_obj = btime_obj = atime_obj = None
    
                if all([tx_time_obj, btime_obj, atime_obj]):
                    try:
                        from models import YadeVoyage, YadeDip, YadeSampleParam
    
                        current_user = (st.session_state.get("auth_user") or {}).get("username", "unknown")
    
                        with get_session() as s:
                            voy = YadeVoyage(
                                location_id=active_location_id,
                                yade_name=yade_no,
                                design=str(design_choice),
                                voyage_no=voyage_no.strip(),
                                convoy_no=convoy_no.strip(),
                                date=tx_date,
                                time=tx_time_obj,
                                cargo=cargo,
                                destination=destination,
                                loading_berth=loading_berth,
                                before_gauge_date=before_date,
                                before_gauge_time=btime_obj,
                                after_gauge_date=after_date,
                                after_gauge_time=atime_obj,
                                created_by=current_user
                            )
                            s.add(voy)
                            s.flush()
    
                            # Sample Parameters
                            sp_store = st.session_state.get("yade_sample_params", {})
                            blk = sp_store.get(ns_sp, sp_store)
    
                            def _upsert_stage_params(stage_key: str):
                                data = blk.get(stage_key, {}) or {
                                    "obs_mode": "Observed API",
                                    "obs_val": 0.0,
                                    "sample_unit": "°F",
                                    "sample_temp": 0.0,
                                    "tank_temp": 0.0,
                                    "ccf": 1.0,
                                    "bsw_pct": 0.0,
                                }
                                ex = (
                                    s.query(YadeSampleParam)
                                    .filter(YadeSampleParam.voyage_id == voy.id,
                                            YadeSampleParam.stage == stage_key)
                                    .one_or_none()
                                )
                                if ex is None:
                                    ex = YadeSampleParam(voyage_id=voy.id, stage=stage_key)
                                    s.add(ex)
                                ex.obs_mode    = str(data.get("obs_mode") or "Observed API")
                                ex.obs_val     = float(data.get("obs_val") or 0.0)
                                ex.sample_unit = str(data.get("sample_unit") or "°F")
                                ex.sample_temp = float(data.get("sample_temp") or 0.0)
                                ex.tank_temp   = float(data.get("tank_temp") or 0.0)
                                ex.ccf         = max(float(data.get("ccf") or 1.0), 0.000001)
                                ex.bsw_pct     = float(data.get("bsw_pct") or 0.0)
    
                            _upsert_stage_params("before")
                            _upsert_stage_params("after")
    
                            # Dips
                            for tid in tank_ids:
                                s.add_all([
                                    YadeDip(
                                        voyage_id=voy.id,
                                        tank_id=tid,
                                        stage="before",
                                        total_cm=float(st.session_state.get(f"before_total_{tid}", 0.0) or 0.0),
                                        water_cm=float(st.session_state.get(f"before_water_{tid}", 0.0) or 0.0),
                                    ),
                                    YadeDip(
                                        voyage_id=voy.id,
                                        tank_id=tid,
                                        stage="after",
                                        total_cm=float(st.session_state.get(f"after_total_{tid}", 0.0) or 0.0),
                                        water_cm=float(st.session_state.get(f"after_water_{tid}", 0.0) or 0.0),
                                    ),
                                ])
    
                            # Seal Details
                            if YadeSealDetail is not None:
                                seals_pack = st.session_state.get("yade_seals", {}).get(ns_seal, {}) or {}
                                for_save_tanks = ["C1","C2","P1","P2","S1","S2"] if str(design_choice) == "6" else ["P1","P2","S1","S2"]
    
                                row = (
                                    s.query(YadeSealDetail)
                                    .filter(YadeSealDetail.voyage_id == voy.id)
                                    .one_or_none()
                                )
                                if row is None:
                                    row = YadeSealDetail(voyage_id=voy.id)
                                    s.add(row)
    
                                for t in for_save_tanks:
                                    data = seals_pack.get(t, {})
                                    k = t.lower()
                                    setattr(row, f"{k}_mh1",      (data.get("mh1","") or "").strip())
                                    setattr(row, f"{k}_mh2",      (data.get("mh2","") or "").strip())
                                    setattr(row, f"{k}_lock",     (data.get("lock","") or "").strip())
                                    setattr(row, f"{k}_diphatch", (data.get("diph","") or "").strip())
    
                            # Compute TOA
                            _persist_toa_from_current_inputs(s, voy, yade_no, tank_ids, ns_sp)
    
                            s.commit()
                            
                            # Log audit
                            from security import SecurityManager
                            SecurityManager.log_audit(
                                s, current_user, "CREATE",
                                resource_type="YadeVoyage",
                                resource_id=str(voy.id),
                                details=f"Created YADE voyage: {yade_no} - Voyage {voyage_no}",
                                user_id=user.get("id"),
                                location_id=active_location_id
                            )
    
                        st.success(f"? YADE Voyage saved for {yade_no} � Voyage {voyage_no}")
                        import time
                        time.sleep(1)
                        _st_safe_rerun()
    
                    except Exception as ex:
                        log_error(f"Failed to save YADE voyage: {ex}", exc_info=True)
                        st.error(f"Failed to save YADE Voyage: {ex}")
                        import traceback
                        st.code(traceback.format_exc())
    
    # ========================= YADE TRACKING PAGE =========================
