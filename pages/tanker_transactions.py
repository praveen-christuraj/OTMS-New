"""
Auto-generated module for the 'Tanker Transactions' page.
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
        global st
        header("Tanker Transactions")
        
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
                if _cfg.get("page_access", {}).get("Tanker Transactions") is False:
                    st.error("🚫 Tanker Transactions page is disabled for this location.")
                    st.stop()
        except Exception:
            pass
        st.markdown("#### Record Tanker Dispatches")
        
        # ============ LOCATION ACCESS CHECK ============
        active_location_id = st.session_state.get("active_location_id")
        if not active_location_id:
            st.error("⚠️ No active location selected.")
            st.stop()
        
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
                if not _cfg.get("page_visibility", {}).get("show_tanker_transactions", False) and (user.get("role", "").lower() not in ["admin-operations", "manager"]):
                    st.error("🚫 Tanker Transactions page is disabled for this location.")
                    st.stop()
            except Exception:
                pass
            
            # Check if feature is allowed at this location (Admin can access everywhere)
            if not PermissionManager.can_access_feature(s, active_location_id, "tanker_transactions", user["role"]):
                st.error("🚫 **Access Denied**")
                st.warning(f"**Tanker Transactions** are not available at **{loc.name}**")
                
                # Show where it's available
                allowed_locs = PermissionManager.get_allowed_locations_for_feature(s, "tanker_transactions")
                if allowed_locs:
                    st.info(f"? This feature is available at: **{', '.join(allowed_locs)}**")
                
                st.markdown("---")
                st.caption(f"Current Location: **{loc.name} ({loc.code})**")
                st.caption("Tanker Transactions Access: **? Denied**")
                st.stop()
            
            # Check if user can make entries
            can_make_entries = PermissionManager.can_make_entries(s, user["role"], active_location_id)
        
        # ============ TANKER ENABLED - SHOW SUCCESS ============
        st.success(f"? Tanker Transactions enabled at {loc.name}")
        
        # ============ HELPER FUNCTIONS ============
        
        def interpolate_tanker_volume(session, tanker_name: str, compartment: str, dip_mm: float) -> float:
            """Linear interpolation for tanker volume from calibration data"""
            from models import TankerCalibration
            
            cal_data = session.query(TankerCalibration).filter(
                TankerCalibration.tanker_name == tanker_name,
                TankerCalibration.compartment == compartment
            ).order_by(TankerCalibration.dip_mm.asc()).all()
            
            if not cal_data:
                return 0.0
            
            xs = [float(c.dip_mm) for c in cal_data]
            ys = [float(c.volume_litres) for c in cal_data]
            
            if dip_mm <= xs[0]:
                return ys[0]
            if dip_mm >= xs[-1]:
                return ys[-1]
            
            import bisect
            i = bisect.bisect_left(xs, dip_mm)
            x1, y1 = xs[i-1], ys[i-1]
            x2, y2 = xs[i], ys[i]
            
            if x2 == x1:
                return y1
            
            t = (dip_mm - x1) / (x2 - x1)
            return y1 + t * (y2 - y1)
        
        # Temperature conversion helpers
        def c_to_f(c: float) -> float:
            if c is None: return 0.0
            return round((float(c) * 1.8) + 32.0, 1)
    
        def f_to_c(f: float) -> float:
            if f is None: return 0.0
            return round((float(f) - 32.0) / 1.8, 1)
        
        def _to_f(val: float, unit: str) -> float:
            """Return °F from val given unit ('°F' or '°C')."""
            return c_to_f(val) if unit.upper().startswith("C") else float(val or 0.0)
    
        def _to_c(val: float, unit: str) -> float:
            """Return °C from val given unit ('°F' or '°C')."""
            return f_to_c(val) if unit.upper().startswith("F") else float(val or 0.0)
        
        # API/Density conversion
        WAT60 = 999.012
        
        def api_from_density(density: float) -> float:
            if not density or density <= 0: return 0.0
            sg = float(density) / WAT60
            if sg <= 0: return 0.0
            return round(141.5 / sg - 131.5, 2)
        
        def density_from_api(api: float) -> float:
            if not api or api <= 0: return 0.0
            sg = 141.5 / (float(api) + 131.5)
            return round(sg * WAT60, 1)
        
        def convert_api_to_60_from_api(api_obs: float, sample_temp_val: float, temp_unit: str) -> float:
            """Your VBA (10 trials). Temp is ALWAYS °F internally."""
            if api_obs is None or api_obs <= 0:
                return 0.0
            tf = _to_f(sample_temp_val or 0.0, temp_unit)
            temp_diff = tf - 60.0
            rho_obs = (141.5 * WAT60 / (131.5 + float(api_obs))) * (
                (1.0 - 0.00001278 * temp_diff) - (0.0000000062 * temp_diff * temp_diff)
            )
            rho = rho_obs
            for _ in range(10):
                alfa = 341.0957 / (rho * rho)
                vcf  = math.exp(-alfa * temp_diff - 0.8 * alfa * alfa * temp_diff * temp_diff)
                rho  = rho_obs / vcf
            api60 = 141.5 * WAT60 / rho - 131.5
            return round(api60, 2)
    
        def convert_api_to_60_from_density(dens_obs_kgm3: float, sample_temp_val: float, temp_unit: str) -> float:
            """Density path (17 trials). Temp is ALWAYS °C internally."""
            if dens_obs_kgm3 is None or dens_obs_kgm3 <= 0:
                return 0.0
            tc = _to_c(sample_temp_val or 0.0, temp_unit)
            temp_diff = tc - 15.0
            
            # Hydrometer correction
            hyc = 1.0 - 0.000023 * temp_diff - 0.00000002 * temp_diff * temp_diff
            rho_obs_corrected = float(dens_obs_kgm3) * hyc
            
            # Initial density at 15°C
            rho15 = rho_obs_corrected
            
            # Iterative VCF calculation (17 iterations)
            for _ in range(17):
                K = 613.9723 / (rho15 * rho15)
                vcf = math.exp(-K * temp_diff * (1.0 + 0.8 * K * temp_diff))
                rho15 = rho_obs_corrected / vcf
            
            sg60 = rho15 / WAT60
            if sg60 <= 0:
                return 0.0
            
            api60 = 141.5 / sg60 - 131.5
            return round(api60, 2)
    
        def vcf_from_api60_and_temp(api60: float, tank_temp: float, tank_temp_unit: str, input_mode: str = "api") -> float:
            """Calculate VCF using ASTM D1250 Table 6A method"""
            if api60 is None or api60 <= 0:
                return 1.00000
            
            if tank_temp_unit == "°C":
                tank_temp_f = (tank_temp * 1.8) + 32.0
            else:
                tank_temp_f = tank_temp
            
            delta_t = tank_temp_f - 60.0
            
            if abs(delta_t) < 0.01:
                return 1.00000
            
            sg60 = 141.5 / (api60 + 131.5)
            rho60 = sg60 * 999.012
            
            K0 = 341.0957
            alpha = K0 / (rho60 * rho60)
            
            vcf = math.exp(-alpha * delta_t * (1.0 + 0.8 * alpha * delta_t))
            
            return round(float(vcf), 5)
        
        # LT lookup from ASTM Table 11
        def lookup_lt(session, api60: float) -> float:
            from models import Table11
            
            rows = session.query(Table11).order_by(Table11.api60).all()
            if not rows:
                return 0.0
            
            xs = [float(r.api60) for r in rows]
            ys = [float(r.lt_factor) for r in rows]
            
            if api60 <= xs[0]:
                return ys[0]
            if api60 >= xs[-1]:
                return ys[-1]
            
            import bisect
            i = bisect.bisect_left(xs, api60)
            x1, y1 = xs[i-1], ys[i-1]
            x2, y2 = xs[i], ys[i]
            
            if x2 == x1:
                return y1
            
            t = (api60 - x1) / (x2 - x1)
            return y1 + t * (y2 - y1)
        
        # ============ GET TANKERS ============
        from models import Tanker
        with get_session() as s:
            tankers = s.query(Tanker).filter(
                Tanker.status == "ACTIVE"
            ).order_by(Tanker.name).all()
        
        if not tankers:
            # Show warning with appropriate icon and arrow
            st.warning("⚠️ No tankers available. Please add tankers in Add Asset → Tanker Master.")
            st.stop()
        
        tanker_names = [t.name for t in tankers]
        
        # ==================== VIEW SAVED TANKER TRANSACTIONS ====================
        st.markdown("---")
        # Section header with clipboard icon
        st.markdown("### 📋 Saved Tanker Transactions")
    
        try:
            with get_session() as s:
                # Get recent tanker transactions for this location
                transactions = s.query(TankerTransaction).filter(
                    TankerTransaction.location_id == active_location_id
                ).order_by(
                    TankerTransaction.transaction_date.desc(),
                    TankerTransaction.transaction_time.desc()
                ).limit(50).all()
    
                if not transactions:
                    # Mailbox icon when there are no records
                    st.info("📭 No tanker transactions found. Create your first transaction below!")
                else:
                    # Create dropdown options
                    tx_options = {}
                    for tx in transactions:
                        label = f"{tx.tanker_name} | Convoy: {tx.convoy_no} | {tx.transaction_date.strftime('%Y-%m-%d')} | {tx.destination}"
                        tx_options[label] = tx.id
                    
                    # Dropdown selector
                    col1, col2 = st.columns([0.7, 0.3])
                    
                    with col1:
                        selected_tx_label = st.selectbox(
                            "🔍 Select Tanker Transaction to View",
                            options=list(tx_options.keys()),
                            key="tanker_tx_selector"
                        )
                    
                    with col2:
                        view_tx_details = st.button("👁️ View Details", use_container_width=True, key="view_tanker_details_btn")
                    
                    selected_tx_id = tx_options[selected_tx_label]
                    
                    # VIEW DETAILS
                    if view_tx_details or st.session_state.get("show_tanker_details", False):
                        st.session_state["show_tanker_details"] = True
                        
                        tx = s.query(TankerTransaction).filter(TankerTransaction.id == selected_tx_id).first()
    
                        if tx:
                            st.markdown("---")
                            # Heading with tanker icon
                            st.markdown(f"#### 🚚 Tanker Transaction - {tx.tanker_name}")
    
                            ns = "tanker" + hashlib.md5(str(tx.id).encode("utf-8")).hexdigest()[:8]
                            is_editing = (
                                st.session_state.get("tanker_edit_mode", False)
                                and st.session_state.get("tanker_edit_id") == tx.id
                            )
    
                            action_cols = st.columns([0.7, 0.3])
                            with action_cols[0]:
                                if not is_editing:
                                    if st.button("✏️ Edit", key=f"{ns}_edit_btn", help="Edit this tanker transaction"):
                                        if not _deny_edit_for_lock(tx, "TankerTransaction", f"{tx.ticket_id or tx.id}"):
                                            st.session_state["tanker_edit_mode"] = True
                                            st.session_state["tanker_edit_id"] = tx.id
                                            st_safe_rerun()
                            with action_cols[1]:
                                if st.button("↩️ Close Viewer", key=f"{ns}_close_btn"):
                                    st.session_state.pop("show_tanker_details", None)
                                    st.session_state.pop("tanker_edit_mode", None)
                                    st.session_state.pop("tanker_edit_id", None)
                                    st_safe_rerun()
    
                            # Basic Info
                            info_col1, info_col2, info_col3 = st.columns(3)
                            
                            with info_col1:
                                st.metric("Tanker Name", tx.tanker_name)
                                st.metric("Chassis No", tx.chassis_no or "N/A")
                                st.metric("Convoy No", tx.convoy_no)
                            
                            with info_col2:
                                st.metric("Date", tx.transaction_date.strftime("%Y-%m-%d"))
                                st.metric("Time", str(tx.transaction_time))
                                st.metric("Cargo", tx.cargo)
                            
                            with info_col3:
                                st.metric("Destination", tx.destination)
                                st.metric("Loading Bay", tx.loading_bay or "N/A")
                                st.metric("Compartment", f"{tx.compartment} (via {tx.manhole})")
                            
                            # Dip Readings
                            st.markdown("##### 📏 Dip Readings")
                            dip_col1, dip_col2 = st.columns(2)
                            
                            with dip_col1:
                                st.metric("Total Dip (cm)", f"{tx.total_dip_cm:.2f}")
                                st.metric("Total Dip (mm)", f"{tx.total_dip_mm:.2f}")
                            
                            with dip_col2:
                                st.metric("Water Dip (cm)", f"{tx.water_dip_cm:.2f}")
                                st.metric("Water Dip (mm)", f"{tx.water_dip_mm:.2f}")
                            
                            # Temperatures
                            st.markdown("##### 🌡️ Temperatures")
                            temp_col1, temp_col2, temp_col3, temp_col4 = st.columns(4)
                            
                            with temp_col1:
                                st.metric("Tank Temp (°C)", f"{tx.tank_temp_c:.2f}" if tx.tank_temp_c else "N/A")
                            
                            with temp_col2:
                                st.metric("Tank Temp (°F)", f"{tx.tank_temp_f:.2f}" if tx.tank_temp_f else "N/A")
                            
                            with temp_col3:
                                st.metric("Sample Temp (°C)", f"{tx.sample_temp_c:.2f}" if tx.sample_temp_c else "N/A")
                            
                            with temp_col4:
                                st.metric("Sample Temp (°F)", f"{tx.sample_temp_f:.2f}" if tx.sample_temp_f else "N/A")
                            
                            # Volume Calculations
                            st.markdown("##### 📊 Volume Calculations")
                            vol_col1, vol_col2, vol_col3 = st.columns(3)
                            
                            with vol_col1:
                                st.metric("Total Volume", f"{tx.total_volume_bbl:,.2f} bbls")
                                st.metric("GOV", f"{tx.gov_bbl:,.2f} bbls")
                                st.metric("GSV", f"{tx.gsv_bbl:,.2f} bbls")
                            
                            with vol_col2:
                                st.metric("Water Volume", f"{tx.water_volume_bbl:,.2f} bbls")
                                st.metric("BS&W %", f"{tx.bsw_pct:.2f}%")
                                st.metric("BS&W Volume", f"{tx.bsw_vol_bbl:,.2f} bbls")
                            
                            with vol_col3:
                                st.metric("NSV", f"{tx.nsv_bbl:,.2f} bbls")
                                st.metric("API@60", f"{tx.api60:.2f}" if tx.api60 else "N/A")
                                st.metric("VCF", f"{tx.vcf:.4f}" if tx.vcf else "N/A")
                            
                            # Additional Metrics
                            st.markdown("##### 📐 Additional Calculations")
                            add_col1, add_col2, add_col3 = st.columns(3)
                            
                            with add_col1:
                                st.metric("LT Factor", f"{tx.lt:.4f}" if tx.lt else "N/A")
                            
                            with add_col2:
                                lt_val = tx.nsv_bbl * tx.lt if tx.lt else 0
                                st.metric("LT (Long Tons)", f"{lt_val:,.2f}")
                            
                            with add_col3:
                                st.metric("MT (Metric Tons)", f"{tx.mt:,.2f}" if tx.mt else "N/A")
                            
                            # Seal Numbers
                            st.markdown("##### 🔐 Seal Numbers")
                            seal_col1, seal_col2, seal_col3, seal_col4 = st.columns(4)
                            
                            with seal_col1:
                                st.caption(f"**C1:** {tx.seal_c1 or 'N/A'}")
                            
                            with seal_col2:
                                st.caption(f"**C2:** {tx.seal_c2 or 'N/A'}")
                            
                            with seal_col3:
                                st.caption(f"**M1:** {tx.seal_m1 or 'N/A'}")
                            
                            with seal_col4:
                                st.caption(f"**M2:** {tx.seal_m2 or 'N/A'}")
                            
                            # Remarks
                            if tx.remarks:
                                st.markdown("##### 📝 Remarks")
                                st.info(tx.remarks)
                            
                            # Audit Info
                            st.markdown("##### 📋 Audit Trail")
                            audit_col1, audit_col2 = st.columns(2)
                            
                            with audit_col1:
                                st.caption(f"**Created By:** {tx.created_by}")
                                st.caption(f"**Created At:** {tx.created_at.strftime('%Y-%m-%d %H:%M:%S')}" if tx.created_at else "N/A")
                            
                            with audit_col2:
                                if tx.updated_by:
                                    st.caption(f"**Updated By:** {tx.updated_by}")
                                    st.caption(f"**Updated At:** {tx.updated_at.strftime('%Y-%m-%d %H:%M:%S')}" if tx.updated_at else "N/A")
    
                            if is_editing:
                                st.markdown("#### ✏️ Update Measurements")
                                with st.form(f"{ns}_edit_form"):
                                    dip_col_edit1, dip_col_edit2 = st.columns(2)
                                    with dip_col_edit1:
                                        edit_total_dip_cm = st.number_input(
                                            "Total Dip (cm)",
                                            min_value=0.0,
                                            value=float(tx.total_dip_cm or 0.0),
                                            step=0.1,
                                            key=f"{ns}_edit_total_dip_cm",
                                        )
                                        edit_water_dip_cm = st.number_input(
                                            "Water Dip (cm)",
                                            min_value=0.0,
                                            value=float(tx.water_dip_cm or 0.0),
                                            step=0.1,
                                            key=f"{ns}_edit_water_dip_cm",
                                        )
                                        edit_bsw_pct = st.number_input(
                                            "BS&W %",
                                            min_value=0.0,
                                            value=float(tx.bsw_pct or 0.0),
                                            step=0.01,
                                            key=f"{ns}_edit_bsw_pct",
                                        )
                                    with dip_col_edit2:
                                        edit_api_obs = _bounded_number_input(
                                            "Observed API",
                                            key=f"{ns}_edit_api_obs",
                                            min_value=API_MIN,
                                            max_value=API_MAX,
                                            value=float(tx.api_observed or 0.0),
                                            step=0.1,
                                        )
                                        edit_density_obs = _bounded_number_input(
                                            "Observed Density (kg/m3)",
                                            key=f"{ns}_edit_density_obs",
                                            min_value=DENSITY_MIN,
                                            max_value=DENSITY_MAX,
                                            value=float(tx.density_observed or 0.0),
                                            step=0.1,
                                        )
    
                                    temp_cols_edit = st.columns(4)
                                    with temp_cols_edit[0]:
                                        edit_tank_temp_c = _temperature_input(
                                            "Tank Temp (°C)",
                                            "°C",
                                            key=f"{ns}_edit_tank_temp_c",
                                            value=float(tx.tank_temp_c or 0.0),
                                        )
                                    with temp_cols_edit[1]:
                                        edit_tank_temp_f = _temperature_input(
                                            "Tank Temp (°F)",
                                            "°F",
                                            key=f"{ns}_edit_tank_temp_f",
                                            value=float(tx.tank_temp_f or 0.0),
                                        )
                                    with temp_cols_edit[2]:
                                        edit_sample_temp_c = _temperature_input(
                                            "Sample Temp (°C)",
                                            "°C",
                                            key=f"{ns}_edit_sample_temp_c",
                                            value=float(tx.sample_temp_c or 0.0),
                                        )
                                    with temp_cols_edit[3]:
                                        edit_sample_temp_f = _temperature_input(
                                            "Sample Temp (°F)",
                                            "°F",
                                            key=f"{ns}_edit_sample_temp_f",
                                            value=float(tx.sample_temp_f or 0.0),
                                        )
    
                                    edit_remarks = st.text_area(
                                        "Remarks",
                                        value=tx.remarks or "",
                                        key=f"{ns}_edit_remarks",
                                    )
    
                                    # Use floppy disk icon for saving changes
                                    save_tanker_edit = st.form_submit_button("💾 Save Changes", type="primary")
    
                                if st.button("✖️ Cancel edit", key=f"{ns}_cancel_edit"):
                                    st.session_state.pop("tanker_edit_mode", None)
                                    st.session_state.pop("tanker_edit_id", None)
                                    st_safe_rerun()
    
                                if save_tanker_edit:
                                    if edit_total_dip_cm <= 0:
                                        st.error("Total Dip must be greater than zero.")
                                    elif edit_api_obs <= 0 and edit_density_obs <= 0:
                                        st.error("Please provide either Observed API or Density.")
                                    else:
                                        try:
                                            from datetime import datetime as dt_now
    
                                            total_dip_mm_val = edit_total_dip_cm * 10.0
                                            water_dip_mm_val = edit_water_dip_cm * 10.0
    
                                            with get_session() as s_update:
                                                db_tx = s_update.query(TankerTransaction).filter(TankerTransaction.id == tx.id).one_or_none()
                                                if not db_tx:
                                                    raise ValueError("Transaction not found.")
    
                                                total_vol_litres = interpolate_tanker_volume(s_update, db_tx.tanker_name, db_tx.compartment, total_dip_mm_val)
                                                water_vol_litres = interpolate_tanker_volume(s_update, db_tx.tanker_name, db_tx.compartment, water_dip_mm_val)
    
                                                total_vol_bbl = total_vol_litres / 158.987
                                                water_vol_bbl = water_vol_litres / 158.987
                                                gov_bbl = max(total_vol_bbl - water_vol_bbl, 0.0)
    
                                                sample_temp_c_calc = edit_sample_temp_c or ((edit_sample_temp_f - 32.0) / 1.8 if edit_sample_temp_f else 0.0)
                                                sample_temp_f_calc = edit_sample_temp_f or ((edit_sample_temp_c * 1.8) + 32.0 if edit_sample_temp_c else 0.0)
                                                tank_temp_c_calc = edit_tank_temp_c or ((edit_tank_temp_f - 32.0) / 1.8 if edit_tank_temp_f else 0.0)
    
                                                if edit_api_obs > 0:
                                                    api60 = convert_api_to_60_from_api(edit_api_obs, sample_temp_f_calc or 60.0, "°F")
                                                    input_mode = "api"
                                                else:
                                                    api60 = convert_api_to_60_from_density(edit_density_obs, sample_temp_c_calc or 15.0, "°C")
                                                    input_mode = "density"
    
                                                vcf_val = vcf_from_api60_and_temp(api60, tank_temp_c_calc or 60.0, "°C", input_mode)
                                                gsv_bbl = round(gov_bbl * vcf_val, 2)
                                                bsw_vol_bbl = round(gsv_bbl * (edit_bsw_pct / 100.0), 2)
                                                nsv_bbl = round(gsv_bbl - bsw_vol_bbl, 2)
                                                lt_factor = lookup_lt(s_update, api60) if api60 > 0 else 0.0
                                                lt_val = round(nsv_bbl * lt_factor, 2)
                                                mt_val = round(lt_val * 1.01605, 2)
    
                                                editor_name = (st.session_state.get("auth_user") or {}).get("username", "unknown")
    
                                                db_tx.total_dip_cm = edit_total_dip_cm
                                                db_tx.total_dip_mm = total_dip_mm_val
                                                db_tx.water_dip_cm = edit_water_dip_cm
                                                db_tx.water_dip_mm = water_dip_mm_val
                                                db_tx.tank_temp_c = edit_tank_temp_c
                                                db_tx.tank_temp_f = edit_tank_temp_f
                                                db_tx.sample_temp_c = edit_sample_temp_c
                                                db_tx.sample_temp_f = edit_sample_temp_f
                                                db_tx.api_observed = edit_api_obs
                                                db_tx.density_observed = edit_density_obs
                                                db_tx.bsw_pct = edit_bsw_pct
                                                db_tx.total_volume_bbl = float(round(total_vol_bbl, 3))
                                                db_tx.water_volume_bbl = float(round(water_vol_bbl, 3))
                                                db_tx.gov_bbl = float(round(gov_bbl, 3))
                                                db_tx.api60 = float(api60)
                                                db_tx.vcf = float(vcf_val)
                                                db_tx.gsv_bbl = float(gsv_bbl)
                                                db_tx.bsw_vol_bbl = float(bsw_vol_bbl)
                                                db_tx.nsv_bbl = float(nsv_bbl)
                                                db_tx.lt = float(lt_factor)
                                                db_tx.mt = float(mt_val)
                                                db_tx.remarks = edit_remarks.strip() if edit_remarks else None
                                                db_tx.updated_by = editor_name
                                                db_tx.updated_at = dt_now.now()
    
                                                s_update.commit()
                                                # ----------------------- Audit log for tanker transaction update -----------------------
                                                try:
                                                    from security import SecurityManager  # type: ignore
                                                    user_ctx = st.session_state.get("auth_user") or {}
                                                    username = user_ctx.get("username", "unknown")
                                                    user_id = user_ctx.get("id")
                                                    location_id = active_location_id
                                                    # Determine resource ID using DB primary key if available; fallback to tanker name & convoy number
                                                    res_id = str(getattr(db_tx, "id", "")) or f"{getattr(db_tx, 'tanker_name', '')}-{getattr(db_tx, 'convoy_no', '')}"
                                                    SecurityManager.log_audit(
                                                        None,
                                                        username,
                                                        "UPDATE",
                                                        resource_type="TankerTransaction",
                                                        resource_id=res_id,
                                                        details=f"Updated tanker transaction {res_id}",
                                                        user_id=user_id,
                                                        location_id=location_id,
                                                    )
                                                except Exception:
                                                    pass
    
                                            st.success("? Tanker transaction updated.")
                                            st.session_state.pop("tanker_edit_mode", None)
                                            st.session_state.pop("tanker_edit_id", None)
                                            st_safe_rerun()
                                        except Exception as ex:
                                            log_error(f"Tanker edit failed: {ex}", exc_info=True)
                                            st.error(f"Failed to update tanker transaction: {ex}")
    
                            # Close button
                            if st.button("? Close Details", key="close_tanker_details"):
                                st.session_state.pop("show_tanker_details", None)
                                st.session_state.pop("tanker_edit_mode", None)
                                st.session_state.pop("tanker_edit_id", None)
                                st_safe_rerun()
    
        except Exception as ex:
            st.error(f"? Failed to load transactions: {ex}")
            import traceback
            st.code(traceback.format_exc())
    
        st.markdown("---")
        
        # ============ TANKER TRANSACTION FORM ============
        st.markdown("### Add New Tanker Transaction")
        
        with st.form("tanker_transaction_form"):
            # -------- TOP METADATA --------
            st.markdown("#### Metadata")
            
            meta_col1, meta_col2, meta_col3 = st.columns(3)
            
            with meta_col1:
                tanker_name = st.selectbox("Tanker Name/ID *", tanker_names, key="tanker_name")
                chassis_no = st.text_input("Chassis Number", placeholder="e.g., CH-12345", key="chassis_no")
            
            with meta_col2:
                convoy_no = st.text_input("Convoy Number *", placeholder="e.g., CVY-001", key="convoy_no")
                from datetime import date
                tx_date = st.date_input("Date *", value=date.today(), key="tanker_date")
            
            with meta_col3:
                cargo = st.selectbox(
                    "Cargo *",
                    options=["OKW", "ANZ", "CONDENSATE", "CRUDE"],
                    key="tanker_cargo"
                )
                tx_time_str = st.text_input("Time * (HH:MM)", value="08:00", key="tanker_time")
            
            meta_col4, meta_col5 = st.columns(2)
            
            with meta_col4:
                destination = st.selectbox(
                    "Destination *",
                    options=["Aggu", "OFS", "Ogini", "GPP", "Ndoni", "Other"],
                    key="tanker_destination"
                )
            
            with meta_col5:
                loading_bay = st.selectbox(
                    "Loading Bay",
                    options=["Aggu", "Ogini", "OFS", "N/A"],
                    key="tanker_loading_bay"
                )
            
            st.markdown("---")
            
            # -------- MANHOLE SELECTOR --------
            st.markdown("#### Compartment")
            
            manhole = st.selectbox(
                "Manhole (Dip Reading Point) *",
                options=["C1", "C2"],
                key="manhole_selector",
                help="Select which manhole was used to take the dip reading"
            )
            
            compartment = manhole
            
            st.caption(f"📍 Selected Manhole: {manhole}")
            
            st.markdown("---")
            
            # -------- DIP READINGS --------
            st.markdown("#### Dip Readings")
            
            dip_col1, dip_col2 = st.columns(2)
            
            with dip_col1:
                total_dip_cm = st.number_input(
                    "Total Dip (cm) *",
                    min_value=0.0,
                    step=0.1,
                    key="total_dip_cm"
                )
            
            with dip_col2:
                water_dip_cm = st.number_input(
                    "Water Dip (cm) *",
                    min_value=0.0,
                    step=0.1,
                    key="water_dip_cm"
                )
            
            # Show quick volume preview
            total_dip_mm = total_dip_cm * 10
            water_dip_mm = water_dip_cm * 10
            
            with get_session() as s:
                total_vol_litres = interpolate_tanker_volume(s, tanker_name, compartment, total_dip_mm)
                water_vol_litres = interpolate_tanker_volume(s, tanker_name, compartment, water_dip_mm)
            
            total_vol_bbl = total_vol_litres / 158.987
            water_vol_bbl = water_vol_litres / 158.987
            gov_bbl = max(total_vol_bbl - water_vol_bbl, 0.0)
            
            st.caption(f"💧 Quick Check: GOV ≈ {gov_bbl:,.2f} bbls")
            
            # -------- BS&W % --------
            bsw_pct = st.number_input(
                "BS&W % *",
                min_value=0.0,
                max_value=100.0,
                step=0.01,
                value=0.0,
                key="tanker_bsw_pct"
            )
            
            st.markdown("---")
            
            # -------- TEMPERATURE READINGS --------
            st.markdown("#### Temperature Readings")
            
            temp_col1, temp_col2 = st.columns(2)
            
            with temp_col1:
                st.markdown("##### Tank Temperature")
                tank_temp_unit = st.selectbox("Unit", ["°C", "°F"], index=0, key="tank_temp_unit")
                tank_temp_val = _temperature_input(
                    f"Temperature ({tank_temp_unit})",
                    tank_temp_unit,
                    "tank_temp_val",
                )
                
                if tank_temp_unit == "°C":
                    tank_temp_c = tank_temp_val
                    tank_temp_f = c_to_f(tank_temp_val)
                else:
                    tank_temp_f = tank_temp_val
                    tank_temp_c = f_to_c(tank_temp_val)
            
            with temp_col2:
                st.markdown("##### Sample Temperature")
                sample_temp_unit = st.selectbox("Unit", ["°F", "°C"], index=0, key="sample_temp_unit")
                sample_temp_val = _temperature_input(
                    f"Temperature ({sample_temp_unit})",
                    sample_temp_unit,
                    "sample_temp_val",
                )
                
                if sample_temp_unit == "°C":
                    sample_temp_c = sample_temp_val
                    sample_temp_f = c_to_f(sample_temp_val)
                else:
                    sample_temp_f = sample_temp_val
                    sample_temp_c = f_to_c(sample_temp_val)
            
            st.markdown("---")
            
            # -------- OBSERVED PROPERTY --------
            st.markdown("#### Quality Parameters")
            
            obs_col1, obs_col2 = st.columns([0.3, 0.7])
            
            with obs_col1:
                obs_mode = st.selectbox(
                    "Input Type",
                    options=["Observed API", "Observed Density"],
                    index=0,
                    key="obs_mode"
                )
            
            with obs_col2:
                if obs_mode == "Observed API":
                    api_observed = _bounded_number_input(
                        "Observed API *",
                        "api_obs",
                        API_MIN,
                        API_MAX,
                    )
                    density_observed = density_from_api(api_observed) if api_observed > 0 else 0.0
                    st.caption(f"? Density: {density_observed:.2f} kg/m3")
                else:
                    density_observed = _bounded_number_input(
                        "Observed Density (kg/m3) *",
                        "density_obs",
                        DENSITY_MIN,
                        DENSITY_MAX,
                    )
                    api_observed = api_from_density(density_observed) if density_observed > 0 else 0.0
                    st.caption(f"? API: {api_observed:.2f}")
            
            st.markdown("---")
            
            # -------- SEAL NUMBERS --------
            st.markdown("#### Seal Numbers")
            
            seal_col1, seal_col2, seal_col3, seal_col4 = st.columns(4)
            
            with seal_col1:
                seal_c1 = st.text_input("C1", placeholder="SC1-12345", key="seal_c1")
            
            with seal_col2:
                seal_c2 = st.text_input("C2", placeholder="SC2-12345", key="seal_c2")
            
            with seal_col3:
                seal_m1 = st.text_input("M1", placeholder="SM1-12345", key="seal_m1")
            
            with seal_col4:
                seal_m2 = st.text_input("M2", placeholder="SM2-12345", key="seal_m2")
            
            st.markdown("---")
            
            # -------- REMARKS --------
            remarks = st.text_area("Remarks (Optional)", key="tanker_remarks")
            
            # -------- SUBMIT BUTTON --------
            submit = st.form_submit_button("💾 Save Tanker Transaction", type="primary", use_container_width=True)
            
            if submit:
                # Validate time
                try:
                    from datetime import datetime
                    import math
                    tx_time_obj = datetime.strptime(tx_time_str, "%H:%M").time()
                except:
                    st.error("Invalid time format. Use HH:MM (24-hour)")
                    tx_time_obj = None
                
                # Validate required fields
                errors = []
                if not convoy_no.strip():
                    errors.append("Convoy Number is required")
                if total_dip_cm <= 0:
                    errors.append("Total dip must be greater than 0")
                if api_observed <= 0 and density_observed <= 0:
                    errors.append("Either API or Density must be provided")
                
                if errors:
                    for err in errors:
                        st.error(f"? {err}")
                elif tx_time_obj:
                    try:
                        # Calculate API@60
                        if obs_mode == "Observed API":
                            api60 = convert_api_to_60_from_api(
                                api_observed, 
                                sample_temp_val, 
                                sample_temp_unit
                            )
                        else:
                            api60 = convert_api_to_60_from_density(
                                density_observed, 
                                sample_temp_val, 
                                sample_temp_unit
                            )
                        
                        # Calculate VCF
                        input_mode = "density" if obs_mode == "Observed Density" else "api"
                        vcf_val = vcf_from_api60_and_temp(api60, tank_temp_val, tank_temp_unit, input_mode)
                        
                        # Calculate volumes
                        gsv_bbl = gov_bbl * vcf_val
                        bsw_vol_bbl = gsv_bbl * (bsw_pct / 100.0)
                        nsv_bbl = gsv_bbl - bsw_vol_bbl
                        
                        # Get LT Factor
                        with get_session() as s:
                            lt = lookup_lt(s, api60)
                        
                        # Calculate LT and MT
                        lt_val = nsv_bbl * lt
                        mt_val = lt_val * 1.01605
                        
                        # Save transaction
                        from models import TankerTransaction
                        
                        with get_session() as s:
                            new_tx = TankerTransaction(
                                location_id=active_location_id,
                                tanker_name=tanker_name,
                                chassis_no=chassis_no.strip() if chassis_no else None,
                                convoy_no=convoy_no.strip(),
                                transaction_date=tx_date,
                                transaction_time=tx_time_obj,
                                cargo=cargo,
                                destination=destination,
                                loading_bay=loading_bay if loading_bay != "N/A" else None,
                                compartment=compartment,
                                manhole=manhole,
                                total_dip_cm=float(total_dip_cm),
                                total_dip_mm=float(total_dip_mm),
                                water_dip_cm=float(water_dip_cm),
                                water_dip_mm=float(water_dip_mm),
                                tank_temp_c=float(tank_temp_c),
                                tank_temp_f=float(tank_temp_f),
                                sample_temp_c=float(sample_temp_c),
                                sample_temp_f=float(sample_temp_f),
                                api_observed=float(api_observed),
                                density_observed=float(density_observed),
                                bsw_pct=float(bsw_pct),
                                total_volume_bbl=float(total_vol_bbl),
                                water_volume_bbl=float(water_vol_bbl),
                                gov_bbl=float(gov_bbl),
                                api60=float(api60),
                                vcf=float(vcf_val),
                                gsv_bbl=float(gsv_bbl),
                                bsw_vol_bbl=float(bsw_vol_bbl),
                                nsv_bbl=float(nsv_bbl),
                                lt=float(lt),
                                mt=float(mt_val),
                                seal_c1=seal_c1.strip() if seal_c1 else None,
                                seal_c2=seal_c2.strip() if seal_c2 else None,
                                seal_m1=seal_m1.strip() if seal_m1 else None,
                                seal_m2=seal_m2.strip() if seal_m2 else None,
                                remarks=remarks.strip() if remarks else None,
                                created_by=user["username"]
                            )
                            
                            s.add(new_tx)
                            s.commit()
                            
                            st.success(f"? Saved! NSV: {nsv_bbl:,.2f} bbls | MT: {mt_val:,.2f}")
                            
                            # Log audit
                            from security import SecurityManager
                            SecurityManager.log_audit(
                                s, user["username"], "CREATE",
                                resource_type="TankerTransaction",
                                resource_id=f"{tanker_name}-{convoy_no}",
                                details=f"Tanker dispatch: {nsv_bbl:.2f} bbls",
                                user_id=user["id"],
                                location_id=active_location_id
                            )
                            
                            import time
                            time.sleep(1)
                            st_safe_rerun()
                            
                    except Exception as ex:
                        log_error(f"Failed to save transaction: {ex}", exc_info=True)
                        st.error(f"Failed to save: {ex}")
                        import traceback
                        st.code(traceback.format_exc())
        
        # ============ END OF FORM ============
    
    # ========================= TOA-YADE =========================
