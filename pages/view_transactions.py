"""
Auto-generated module for the 'View Transactions' page.
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
        header("View Transactions")
        
        # Check Admin-IT access restriction
        if st.session_state.get("auth_user", {}).get("role") == "admin-it":
            st.error("🚫 Access Denied: Admin-IT users do not have access to operational pages.")
            st.stop()
        st.markdown("### Transaction History")
        
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
        
        # Display current location
        is_bfs_location = False
        active_location_code = ""
        with get_session() as s:
            from location_manager import LocationManager
            from permission_manager import PermissionManager
            
            loc = LocationManager.get_location_by_id(s, active_location_id)
            if loc:
                active_location_code = (loc.code or "").upper()
                loc_name_upper = (loc.name or "").upper()
                is_bfs_location = (
                    "BFS" in active_location_code
                    or "BFS" in loc_name_upper
                    or "BENEKU" in loc_name_upper
                )
                st.info(f"📍 **Viewing Transactions for:** {loc.name} ({loc.code})")
            
            # Check which transaction types are enabled for this location
            show_yade = PermissionManager.can_access_feature(s, active_location_id, "yade_transactions", user["role"])
            show_tanker = PermissionManager.can_access_feature(s, active_location_id, "tanker_transactions", user["role"])
        
        # ============ BUILD SCOPE OPTIONS BASED ON LOCATION ============
        scope_options = ["Tank Transactions"]
        if show_yade:
            scope_options.append("YADE Voyages")
        if show_tanker:
            scope_options.append("Tanker Transactions")
        if is_bfs_location:
            scope_options.append("Condensate Receipts")
        
        # ---------------- Selector ----------------
        scope = st.selectbox(
            "Select data to view",
            scope_options,
            index=0,
            key="vt_scope"
        )
        st.markdown("---")
    
        def _date_series(s):
            """Return a Series[datetime.date] regardless of input dtype."""
            return pd.to_datetime(s, errors="coerce").dt.date
    
        def _fmt_date(x):
            try:
                return pd.to_datetime(x).strftime("%d/%m/%Y")
            except Exception:
                return ""
    
        def _fmt_time(x):
            try:
                return pd.to_datetime(str(x)).strftime("%H:%M")
            except Exception:
                return ""
    
        from models import CalibrationTank
    
        def _calc_tov_from_calibration(session, tank_name: str, dip_cm_val: float, location_id: int) -> float | None:
            """Return interpolated volume (bbl) for a tank dip."""
            if not tank_name or dip_cm_val is None:
                return None
            rows = (
                session.query(CalibrationTank)
                .filter(
                    CalibrationTank.tank_name == tank_name,
                    CalibrationTank.location_id == location_id,
                )
                .order_by(CalibrationTank.dip_cm.asc())
                .all()
            )
            if not rows:
                return None
            xs = [float(r.dip_cm or 0.0) for r in rows]
            ys = [float(r.volume_bbl or 0.0) for r in rows]
            if dip_cm_val <= xs[0]:
                return ys[0]
            if dip_cm_val >= xs[-1]:
                return ys[-1]
            import bisect
    
            idx = bisect.bisect_left(xs, dip_cm_val)
            x1, y1 = xs[idx - 1], ys[idx - 1]
            x2, y2 = xs[idx], ys[idx]
            if x2 == x1:
                return y1
            frac = (dip_cm_val - x1) / (x2 - x1)
            return y1 + frac * (y2 - y1)
    
        import math
        WAT60_CONST = 999.012
    
        def convert_api_to_60_from_api(api_obs: float, sample_temp_val: float, temp_unit: str) -> float:
            if not api_obs or api_obs <= 0:
                return 0.0
            tf = sample_temp_val if temp_unit == "°F" else (sample_temp_val * 1.8) + 32.0
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
    
        def convert_api_to_60_from_density(dens_obs_kgm3: float, sample_temp_val: float, temp_unit: str) -> float:
            if not dens_obs_kgm3 or dens_obs_kgm3 <= 0:
                return 0.0
            tc = sample_temp_val if temp_unit == "°C" else (sample_temp_val - 32.0) / 1.8
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
    
        def vcf_from_api60_and_temp(api60: float, tank_temp: float, tank_temp_unit: str, input_mode: str = "api") -> float:
            if not api60 or api60 <= 0:
                return 1.00000
            tank_temp_f = tank_temp if tank_temp_unit == "°F" else (tank_temp * 1.8) + 32.0
            delta_t = tank_temp_f - 60.0
            if abs(delta_t) < 0.01:
                return 1.00000
            sg60 = 141.5 / (api60 + 131.5)
            rho60 = sg60 * WAT60_CONST
            K0 = 341.0957
            alpha = K0 / (rho60 * rho60)
            vcf = math.exp(-alpha * delta_t * (1.0 + 0.8 * alpha * delta_t))
            return round(float(vcf), 5)
    
        def lookup_lt_factor(session, api60: float) -> float:
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
    
            idx = bisect.bisect_left(xs, api60)
            x1, y1 = xs[idx - 1], ys[idx - 1]
            x2, y2 = xs[idx], ys[idx]
            if x2 == x1:
                return y1
            frac = (api60 - x1) / (x2 - x1)
            return y1 + frac * (y2 - y1)
    
        from uuid import uuid4
        import hashlib
    
        def _uniq_key(*parts) -> str:
            raw = "||".join(str(p) for p in parts)
            return hashlib.md5(raw.encode("utf-8")).hexdigest()
    
        # ===========================================================
        # =============== TANK TRANSACTIONS VIEWER ==================
        # ===========================================================
        if scope == "Tank Transactions":
            st.markdown("### Tank Transactions")
    
            # -------- load rows - FILTERED BY LOCATION --------
            try:
                with get_session() as s:
                    rows = (
                        s.query(TankTransaction)
                        .filter(TankTransaction.location_id == active_location_id)
                        .order_by(TankTransaction.date.desc(), TankTransaction.time.desc())
                        .limit(1000)
                        .all()
                    )
            except Exception as ex:
                st.error(f"Failed to load transactions: {ex}")
                rows = []
    
            if not rows:
                st.info("No tank transactions saved yet for this location.")
                st.stop()
    
            rows = [r for r in rows if not _is_condensate_tx(r)]
    
            tx_dates = [r.date for r in rows if isinstance(r.date, date)]
            tx_min_date, tx_max_date = _derive_filter_bounds(tx_dates)
            tx_from_default = _ensure_date_key_in_bounds(
                "vt_tk_date_from", tx_min_date, tx_max_date, tx_min_date
            )
            tx_to_default = _ensure_date_key_in_bounds(
                "vt_tk_date_to", tx_min_date, tx_max_date, tx_max_date
            )
    
            # -------- live filters (tank) --------
            with st.container(border=True):
                st.caption("Live filters")
                c1, c2, c3, c4, c5, c6 = st.columns([0.22, 0.18, 0.18, 0.16, 0.16, 0.10])
                with c1:
                    f_ticket = st.text_input("Ticket ID", key="vt_tk_ticket")
                with c2:
                    f_tank = st.text_input("Tank", key="vt_tk_tank")
                with c3:
                    op_options = [
                        "(All)",
                        "Opening Stock", "Receipt", "Receipt from Agu", "Receipt from OFS",
                        "OKW Receipt", "ANZ Receipt", "Other Receipts","Dispatch",
                        "Dispatch to barge", "Other Dispatch",
                        "ITT - Receipt", "ITT - Dispatch", "Settling", "Draining"
                    ]
                    f_op = st.selectbox("Operation", op_options, index=0, key="vt_tk_f_op")
                with c4:
                    f_date_from = st.date_input(
                        "From date",
                        value=tx_from_default,
                        min_value=tx_min_date,
                        max_value=tx_max_date,
                        key="vt_tk_date_from",
                    )
                with c5:
                    f_date_to = st.date_input(
                        "To date",
                        value=tx_to_default,
                        min_value=tx_min_date,
                        max_value=tx_max_date,
                        key="vt_tk_date_to",
                    )
                with c6:
                    f_api_min = st.number_input("Obs API =", value=0.0, step=0.1, key="vt_tk_api_min")
            
            with get_session() as s2:
                otr_map = {
                    o.ticket_id: o.nsv_bbl
                    for o in s2.query(OTRRecord).filter(
                        OTRRecord.location_id == active_location_id,
                        OTRRecord.ticket_id.in_([r.ticket_id for r in rows])
                    ).all()
                }
                meta_map = { 
                    r.ticket_id: (r.created_by, r.updated_by, r.updated_at) 
                    for r in rows 
                }
            # to DataFrame for easy filtering & display (preserve columns even when no non-condensate rows)
            df_columns = [
                "Ticket ID", "Operation", "Tank", "Date", "Time",
                "Dip (cm)", "Water (cm)", "Tank Temp A?C", "Tank Temp A?F",
                "Obs API", "Obs Density", "Sample Temp A?C", "Sample Temp A?F",
                "BS&W %", "Qty (bbls)", "NSV (bbl)", "Remarks",
                "Created By", "Updated By", "Updated At",
            ]
            df_rows = [
                {
                    "Ticket ID": r.ticket_id,
                    "Operation": (r.operation.value if hasattr(r.operation, "value") else (str(r.operation) if r.operation else None)),
                    "Tank": r.tank_name or (r.tank.name if getattr(r, "tank", None) else None),
                    "Date": r.date,
                    "Time": r.time,
                    "Dip (cm)": r.dip_cm,
                    "Water (cm)": r.water_cm,
                    "Tank Temp A?C": r.tank_temp_c,
                    "Tank Temp A?F": r.tank_temp_f,
                    "Obs API": r.api_observed,
                    "Obs Density": r.density_observed,
                    "Sample Temp A?C": r.sample_temp_c,
                    "Sample Temp A?F": r.sample_temp_f,
                    "BS&W %": getattr(r, "bsw_pct", None),
                    "Qty (bbls)": r.qty_bbls,
                    "NSV (bbl)": round(otr_map.get(r.ticket_id, 0.0) or 0.0, 2),
                    "Remarks": r.remarks,
                    # include audit information for caution display
                    "Created By": getattr(r, "created_by", None),
                    "Updated By": getattr(r, "updated_by", None),
                    "Updated At": getattr(r, "updated_at", None),
                }
                for r in rows
            ]
            df = pd.DataFrame(df_rows, columns=df_columns)
            # apply filters
            fdf = df.copy()
            if f_ticket:
                fdf = fdf[fdf["Ticket ID"].astype(str).str.contains(f_ticket.strip(), case=False, na=False)]
            if f_tank:
                fdf = fdf[fdf["Tank"].astype(str).str.contains(f_tank.strip(), case=False, na=False)]
            if f_op != "(All)":
                fdf = fdf[fdf["Operation"] == f_op]
    
            dser = _date_series(fdf["Date"])
            if f_date_from:
                fdf = fdf[dser >= f_date_from]
            if f_date_to:
                fdf = fdf[dser <= f_date_to]
    
            if f_api_min and f_api_min > 0:
                fdf = fdf[(fdf["Obs API"].fillna(0) >= f_api_min)]
    
            st.caption(f"{len(fdf)} / {len(df)} rows")
    
            fdf = fdf.reset_index(drop=True)
    
            # Display with borders and buttons
            with st.container(border=True):
                # Header now includes User column for audit/caution display
                hdr = [
                    "Ticket ID", "Tank", "Date", "Time", "Operation", "Stock (bbls)", "User", "Actions"
                ]
                # Adjust column widths to accommodate the new "User" column. The sum should be ~1.0.
                widths = [0.14, 0.14, 0.12, 0.12, 0.12, 0.12, 0.14, 0.10]
                hcols = st.columns(widths)
                for c, h in zip(hcols, hdr):
                    c.markdown(f"**{h}**")
    
                for i, row in fdf.iterrows():
                    cols = st.columns(widths)
                    cols[0].write(str(row.get("Ticket ID", "")))
                    cols[1].write(str(row.get("Tank", "")))
                    cols[2].write(_fmt_date(row.get("Date", "")))
                    cols[3].write(_fmt_time(row.get("Time", "")))
                    tid = str(row["Ticket ID"])
                    created_by, updated_by, updated_at = meta_map.get(tid, (None, None, None))
                    badge = ""
                    if updated_by:
                        ts = updated_at.strftime("%d-%m-%Y %H:%M") if updated_at else "-"
                        badge = f' <span title="Edited by {updated_by} on {ts}"⚠️</span>'
                    cols[4].markdown(f'{row["Operation"]}{badge}', unsafe_allow_html=True)
    
                    cols[5].write(str(row.get("NSV (bbl)","")))
    
                    # Construct User cell with caution icon if record has been edited
                    created_by = row.get("Created By") or "-"
                    updated_by = row.get("Updated By")
                    updated_at = row.get("Updated At")
                    if updated_by:
                        # format updated_at if available
                        try:
                            # convert to string like YYYY-MM-DD HH:MM
                            ts_str = ""
                            if updated_at:
                                # some DBs store datetimes as strings or datetime objects
                                try:
                                    ts_obj = pd.to_datetime(updated_at)
                                    ts_str = ts_obj.strftime("%Y-%m-%d %H:%M")
                                except Exception:
                                    ts_str = str(updated_at)
                            tooltip = f"Edited by {updated_by}, {ts_str}" if ts_str else f"Edited by {updated_by}"
                        except Exception:
                            tooltip = f"Edited by {updated_by}"
                        user_html = f"{created_by} <span style=\"color: #f59e0b;\" title=\"{tooltip}\">⚠️</span>"
                    else:
                        user_html = created_by
                    cols[6].markdown(user_html, unsafe_allow_html=True)
    
                    # Actions column
                    act_c1, act_c2 = cols[7].columns([0.5, 0.5])
                    tid_str = str(row.get("Ticket ID", f"row{i}"))
                    k_view = f"tt_view_{_uniq_key('tt', tid_str, i)}"
                    k_del  = f"tt_del_{_uniq_key('tt', tid_str, i)}"
    
                    if act_c1.button("🔍", key=k_view, help="View / Edit this transaction"):
                        st.session_state["tt_view_tid"] = tid_str
                        st.session_state["tt_edit_mode"] = False
                        st.session_state.pop("tt_delete_tid_pending", None)
                        _st_safe_rerun()
                    if act_c2.button("🗑️", key=k_del, help="Delete this transaction"):
                        st.session_state["tt_delete_tid_pending"] = tid_str
                        _st_safe_rerun()
    
        elif scope == "Condensate Receipts":
            st.markdown("### Condensate Receipts")
    
            if not is_bfs_location:
                st.info("Condensate receipts are only configured for BFS/Beneku.")
                st.stop()
    
            cond_df_records, meta_map = load_condensate_transactions(active_location_id)
    
            if not cond_df_records:
                st.info("No condensate receipts captured yet.")
            else:
                cond_df = pd.DataFrame(cond_df_records)
                if not cond_df.empty:
                    cond_df["Date"] = pd.to_datetime(cond_df["Date"]).dt.date
                cond_dates = cond_df["Date"].tolist() if not cond_df.empty else []
                cond_min_date, cond_max_date = _derive_filter_bounds(cond_dates)
                cond_from_default = _ensure_date_key_in_bounds(
                    "vt_cond_from", cond_min_date, cond_max_date, cond_min_date
                )
                cond_to_default = _ensure_date_key_in_bounds(
                    "vt_cond_to", cond_min_date, cond_max_date, cond_max_date
                )
    
                with st.container(border=True):
                    st.caption("Live filters")
                    c1, c2, c3 = st.columns([0.3, 0.35, 0.35])
                    with c1:
                        f_ticket = st.text_input("Ticket ID", key="vt_cond_ticket")
                    with c2:
                        f_date_from = st.date_input(
                            "From date",
                            value=cond_from_default,
                            min_value=cond_min_date,
                            max_value=cond_max_date,
                            key="vt_cond_from",
                        )
                    with c3:
                        f_date_to = st.date_input(
                            "To date",
                            value=cond_to_default,
                            min_value=cond_min_date,
                            max_value=cond_max_date,
                            key="vt_cond_to",
                        )
    
                fdf = cond_df.copy()
                if f_ticket:
                    fdf = fdf[fdf["Ticket ID"].astype(str).str.contains(f_ticket.strip(), case=False, na=False)]
                dser = _date_series(fdf["Date"])
                if f_date_from:
                    fdf = fdf[dser >= f_date_from]
                if f_date_to:
                    fdf = fdf[dser <= f_date_to]
    
                st.caption(f"{len(fdf)} / {len(cond_df)} records")
                fdf = fdf.reset_index(drop=True)
    
                hdr = [
                    "Ticket ID", "Date", "Opening (m3)", "Closing (m3)",
                    "Net Receipt (m3)", "GOV (bbls)", "API @ 60",
                    "VCF", "GSV (bbls)", "LT", "MT", "User", "Actions"
                ]
                widths = [0.12, 0.09, 0.11, 0.11, 0.11, 0.09, 0.08, 0.08, 0.09, 0.06, 0.06, 0.07, 0.09]
                hcols = st.columns(widths)
                for c, h in zip(hcols, hdr):
                    c.markdown(f"**{h}**")
    
                for i, row in fdf.iterrows():
                    cols = st.columns(widths)
                    cols[0].write(str(row.get("Ticket ID", "")))
                    cols[1].write(_fmt_date(row.get("Date", "")))
                    cols[2].write(f"{row.get('Opening (m3)', 0.0):,.3f}")
                    cols[3].write(f"{row.get('Closing (m3)', 0.0):,.3f}")
                    cols[4].write(f"{row.get('Net Receipt (m3)', 0.0):,.3f}")
                    cols[5].write(f"{row.get('GOV (bbls)', 0.0):,.2f}")
                    cols[6].write(f"{row.get('API @ 60', 0.0):,.2f}")
                    cols[7].write(f"{row.get('VCF', 0.0):,.5f}")
                    cols[8].write(f"{row.get('GSV (bbls)', 0.0):,.2f}")
                    cols[9].write(f"{row.get('LT', 0.0):,.2f}")
                    cols[10].write(f"{row.get('MT', 0.0):,.2f}")
    
                    tid = str(row.get("Ticket ID", ""))
                    created_by, updated_by, updated_at = meta_map.get(tid, (None, None, None))
                    badge = ""
                    if updated_by:
                        ts = updated_at.strftime("%d-%m-%Y %H:%M") if updated_at else "-"
                        badge = f' <span title="Edited by {updated_by} on {ts}">?⚠️,?</span>'
                    cols[11].markdown(f"{created_by or '-'}{badge}", unsafe_allow_html=True)
    
                    act_c1, act_c2 = cols[12].columns([0.5, 0.5])
                    k_view = f"cond_view_{_uniq_key('cond', tid, i)}"
                    k_del = f"cond_del_{_uniq_key('cond', tid, i)}"
                    if act_c1.button("🔍", key=k_view, help="View / Edit this receipt"):
                        st.session_state["tt_view_tid"] = tid
                        st.session_state["tt_edit_mode"] = False
                        st.session_state.pop("tt_delete_tid_pending", None)
                        _st_safe_rerun()
                    if act_c2.button("🗑️", key=k_del, help="Delete this receipt"):
                        st.session_state["tt_delete_tid_pending"] = tid
                        _st_safe_rerun()
    
        # ===========================================================
        # ================== YADE VOYAGES VIEWER ====================
        # ===========================================================
        elif scope == "YADE Voyages":
            st.markdown("### YADE Voyages")
    
            # -------- load rows + summaries - FILTERED BY LOCATION --------
            try:
                from models import YadeVoyage, YadeDip, YadeSampleParam, YadeSealDetail, TOAYadeSummary
    
                def _enum_text(x):
                    if hasattr(x, "value"):
                        return x.value
                    return str(x)
    
                vdf_rows = []
                with get_session() as s:
                    vrows = (
                        s.query(YadeVoyage)
                        .filter(YadeVoyage.location_id == active_location_id)
                        .order_by(YadeVoyage.date.desc(), YadeVoyage.time.desc())
                        .limit(200)
                        .all()
                    )
    
                    if not vrows:
                        summaries = {}
                    else:
                        v_ids = [v.id for v in vrows]
                        summaries = {
                            r.voyage_id: r
                            for r in s.query(TOAYadeSummary).filter(TOAYadeSummary.voyage_id.in_(v_ids)).all()
                        }
    
                    for v in vrows:
                        summ = summaries.get(v.id)
                        vdf_rows.append({
                            "YADE No": v.yade_name,
                            "Voyage No": v.voyage_no,
                            "Convoy No": v.convoy_no,
                            "Date": v.date,
                            "Time": v.time,
                            "Destination": _enum_text(v.destination),
                            "Loading Berth": _enum_text(v.loading_berth),
                            "Before Qty (bbl)": float(getattr(summ, "gsv_before_bbl", 0.0) or 0.0),
                            "After Qty (bbl)":  float(getattr(summ, "gsv_after_bbl",  0.0) or 0.0),
                            "Loaded Qty (bbl)": float(getattr(summ, "gsv_loaded_bbl", 0.0) or 0.0),
                            "ID": v.id,
                            # audit fields for caution display
                            "Created By": getattr(v, "created_by", None),
                            "Updated By": getattr(v, "updated_by", None),
                            "Updated At": getattr(v, "updated_at", None),
                        })
                        yade_meta = { v.id: (v.created_by, v.updated_by, v.updated_at) for v in vrows }
            except Exception as ex:
                st.error(f"Failed to load YADE Voyages: {ex}")
                vdf_rows = []
    
            if not vdf_rows:
                st.info("No YADE Voyages yet for this location.")
                st.stop()
    
            vdf = pd.DataFrame(vdf_rows)
            if not vdf.empty:
                vdf["Date"] = pd.to_datetime(vdf["Date"]).dt.date
            yade_dates = vdf["Date"].tolist() if not vdf.empty else []
            yade_min_date, yade_max_date = _derive_filter_bounds(yade_dates)
            yade_from_default = _ensure_date_key_in_bounds(
                "vt_yd_from", yade_min_date, yade_max_date, yade_min_date
            )
            yade_to_default = _ensure_date_key_in_bounds(
                "vt_yd_to", yade_min_date, yade_max_date, yade_max_date
            )
    
            # -------- live filters --------
            with st.container(border=True):
                st.caption("Live filters")
                y1, y2, y3, y4, y5, y6 = st.columns([0.15, 0.12, 0.14, 0.14, 0.20, 0.25])
                with y1:
                    f_yade = st.text_input("YADE No", key="vt_yd_name")
                with y2:
                    f_voy = st.text_input("Voyage No", key="vt_yd_voy")
                with y3:
                    f_date_from = st.date_input(
                        "From date",
                        value=yade_from_default,
                        min_value=yade_min_date,
                        max_value=yade_max_date,
                        key="vt_yd_from",
                    )
                with y4:
                    f_date_to = st.date_input(
                        "To date",
                        value=yade_to_default,
                        min_value=yade_min_date,
                        max_value=yade_max_date,
                        key="vt_yd_to",
                    )
                
                dest_opts = ["(All)"] + sorted([x for x in vdf["Destination"].dropna().unique().tolist()])
                berth_opts = ["(All)"] + sorted([x for x in vdf["Loading Berth"].dropna().unique().tolist()])
    
                with y5:
                    f_dest = st.selectbox("Destination", dest_opts, index=0, key="vt_yd_dest")
                with y6:
                    f_berth = st.selectbox("Loading Berth", berth_opts, index=0, key="vt_yd_berth")
    
            # apply filters
            f_vdf = vdf.copy()
            if f_yade:
                f_vdf = f_vdf[f_vdf["YADE No"].astype(str).str.contains(f_yade.strip(), case=False, na=False)]
            if f_voy:
                f_vdf = f_vdf[f_vdf["Voyage No"].astype(str).str.contains(f_voy.strip(), case=False, na=False)]
    
            vser = _date_series(f_vdf["Date"])
            if f_date_from:
                f_vdf = f_vdf[vser >= f_date_from]
            if f_date_to:
                f_vdf = f_vdf[vser <= f_date_to]
            if f_dest != "(All)":
                f_vdf = f_vdf[f_vdf["Destination"] == f_dest]
            if f_berth != "(All)":
                f_vdf = f_vdf[f_vdf["Loading Berth"] == f_berth]
    
            st.caption(f"{len(f_vdf)} / {len(vdf)} rows")
    
            f_vdf = f_vdf.reset_index(drop=True)
    
            # Display with borders and buttons
            with st.container(border=True):
                # Header now includes User column for audit/caution display
                hdr = [
                    "YADE No", "Voyage No", "Convoy No", "Date", "Time", "Destination", "Loading Berth",
                    "Before Qty (bbl)", "After Qty (bbl)", "Loaded Qty (bbl)", "User", "Actions"
                ]
                # Define column widths; sum should be ~1.0
                widths = [0.09, 0.08, 0.08, 0.07, 0.06, 0.12, 0.12, 0.08, 0.08, 0.10, 0.10, 0.10]
                hcols = st.columns(widths)
                for c, h in zip(hcols, hdr):
                    c.markdown(f"**{h}**")
    
                for i, row in f_vdf.iterrows():
                    cols = st.columns(widths)
                    cols[0].write(row["YADE No"])
                    cb, ub, ut = yade_meta.get(int(row["ID"]), (None, None, None))
                    badge = ""
                    if ub:
                        ts = ut.strftime("%d-%m-%Y %H:%M") if ut else "-"
                        badge = f' <span title="Edited by {ub} on {ts}"⚠️</span>'
                    cols[1].markdown(f'{row["Voyage No"]}{badge}', unsafe_allow_html=True)
                    cols[2].write(row["Convoy No"])
                    cols[3].write(_fmt_date(row["Date"]))
                    cols[4].write(_fmt_time(row["Time"]))
                    cols[5].write(row["Destination"])
                    cols[6].write(row["Loading Berth"])
                    cols[7].write(f'{row["Before Qty (bbl)"]:.2f}')
                    cols[8].write(f'{row["After Qty (bbl)"]:.2f}')
                    cols[9].write(f'{row["Loaded Qty (bbl)"]:.2f}')
    
                    # User cell: show created_by and caution if updated
                    created_by = row.get("Created By") or "-"
                    updated_by = row.get("Updated By")
                    updated_at = row.get("Updated At")
                    if updated_by:
                        # Build tooltip
                        try:
                            ts_str = ""
                            if updated_at:
                                try:
                                    ts_obj = pd.to_datetime(updated_at)
                                    ts_str = ts_obj.strftime("%Y-%m-%d %H:%M")
                                except Exception:
                                    ts_str = str(updated_at)
                            tooltip = f"Edited by {updated_by}, {ts_str}" if ts_str else f"Edited by {updated_by}"
                        except Exception:
                            tooltip = f"Edited by {updated_by}"
                        user_html = f"{created_by} <span style=\"color: #f59e0b;\" title=\"{tooltip}\">⚠️</span>"
                    else:
                        user_html = created_by
                    cols[10].markdown(user_html, unsafe_allow_html=True)
    
                    # Actions
                    act_c1, act_c2 = cols[11].columns([0.5, 0.5])
                    vid = int(row["ID"])
                    k_view = f"yv_view_{_uniq_key('yv', vid, i)}"
                    k_del  = f"yv_del_{_uniq_key('yv', vid, i)}"
    
                    if act_c1.button("🔍", key=k_view, help="View / Edit this voyage"):
                        st.session_state["yade_view_id"] = vid
                        st.session_state["yade_edit_mode"] = False
                        st.session_state.pop("yade_delete_id_pending", None)
                        _st_safe_rerun()
                    if act_c2.button("🗑️", key=k_del, help="Delete this voyage"):
                        st.session_state["yade_delete_id_pending"] = vid
                        _st_safe_rerun()
    
            # -------- delete flow (yade) --------
            if st.session_state.get("yade_delete_id_pending"):
                vid = st.session_state["yade_delete_id_pending"]
                st.markdown("---")
                with st.container(border=True):
                    st.markdown(f"### Delete YADE Voyage � ID {vid}")
    
                    user_info = st.session_state.get("auth_user") or {"username": "unknown", "role": "operator"}
                    role = user_info.get("role", "operator")
                    username = user_info.get("username", "unknown")
    
                    def _execute_yade_delete(approver_label: str):
                        try:
                            with get_session() as s:
                                rec = s.query(YadeVoyage).filter(YadeVoyage.id == vid).one_or_none()
                                extra_payload: Dict[str, Any] = {}
                                if rec:
                                    dips_snapshot = [
                                        RecycleBinManager.snapshot_record(d)
                                        for d in s.query(YadeDip).filter(YadeDip.voyage_id == vid).all()
                                    ]
                                    samples_snapshot = [
                                        RecycleBinManager.snapshot_record(d)
                                        for d in s.query(YadeSampleParam).filter(YadeSampleParam.voyage_id == vid).all()
                                    ]
                                    seals_snapshot = [
                                        RecycleBinManager.snapshot_record(d)
                                        for d in s.query(YadeSealDetail).filter(YadeSealDetail.voyage_id == vid).all()
                                    ]
                                    summary_snapshot = [
                                        RecycleBinManager.snapshot_record(d)
                                        for d in s.query(TOAYadeSummary).filter(TOAYadeSummary.voyage_id == vid).all()
                                    ]
                                    stages_snapshot = [
                                        RecycleBinManager.snapshot_record(d)
                                        for d in s.query(TOAYadeStage).filter(TOAYadeStage.voyage_id == vid).all()
                                    ]
                                    extra_payload = {
                                        "dips": dips_snapshot,
                                        "samples": samples_snapshot,
                                        "seals": seals_snapshot,
                                        "summaries": summary_snapshot,
                                        "stages": stages_snapshot,
                                    }
                                    _archive_record_for_delete(
                                        s,
                                        rec,
                                        "YadeVoyage",
                                        reason=f"Marked YADE voyage {vid} for deletion. Approved by {approver_label}.",
                                        label=f"Voyage {vid}",
                                        extra_payload=extra_payload,
                                    )
    
                                dips_deleted = s.query(YadeDip).filter(
                                    YadeDip.voyage_id == vid
                                ).delete(synchronize_session=False)
                                samples_deleted = s.query(YadeSampleParam).filter(
                                    YadeSampleParam.voyage_id == vid
                                ).delete(synchronize_session=False)
                                seals_deleted = s.query(YadeSealDetail).filter(
                                    YadeSealDetail.voyage_id == vid
                                ).delete(synchronize_session=False)
                                summary_deleted = s.query(TOAYadeSummary).filter(
                                    TOAYadeSummary.voyage_id == vid
                                ).delete(synchronize_session=False)
                                stages_deleted = s.query(TOAYadeStage).filter(
                                    TOAYadeStage.voyage_id == vid
                                ).delete(synchronize_session=False)
    
                                if not rec:
                                    username_a, user_id, location_id = _current_user_audit_context()
                                    SecurityManager.log_audit(
                                        s,
                                        username_a,
                                        "DELETE",
                                        resource_type="YadeVoyage",
                                        resource_id=str(vid),
                                        details=(
                                            f"Cleaned orphan YADE voyage {vid} "
                                            f"(dips={dips_deleted}, samples={samples_deleted}, "
                                            f"seals={seals_deleted}, TOA summary={summary_deleted}, "
                                            f"stages={stages_deleted}). "
                                            f"Approved by {approver_label}."
                                        ),
                                        user_id=user_id,
                                        location_id=location_id,
                                    )
    
                            TaskManager.complete_tasks_for_resource(
                                "YadeVoyage",
                                vid,
                                username,
                                notes=f"Approved by {approver_label}",
                            )
                            st.success(f"Deleted voyage {vid}. Approved by {approver_label}.")
                        except Exception as ex:
                            st.error(f"Failed to delete: {ex}")
                        finally:
                            st.session_state.pop("yade_delete_id_pending", None)
                            _st_safe_rerun()
    
                    if role in ("admin-operations", "supervisor"):
                        approver = f"{username} ({role})"
                        st.info(f"Approval: {approver}")
                        # Show confirm and cancel buttons with appropriate icons
                        do_del = st.button("✅ Confirm Delete", key=f"yv_del_confirm_{vid}", type="primary")
                        cancel = st.button("✖️ Cancel", key=f"yv_del_cancel_{vid}")
    
                        if do_del:
                            _execute_yade_delete(approver)
    
                        if cancel:
                            st.session_state.pop("yade_delete_id_pending", None)
                            st.success("Deletion cancelled.")
                            _st_safe_rerun()
                    else:
                        remote_task = _render_remote_delete_request_ui(
                            "YadeVoyage",
                            vid,
                            f"YADE voyage {vid}",
                            "Yade Transactions",
                        )
                        if remote_task and remote_task.get("status") == TaskStatus.APPROVED.value:
                            remote_approver = remote_task.get("approved_by") or "Supervisor"
                            if st.button(
                                "Delete with approved request",
                                key=f"yv_remote_delete_{vid}",
                                type="primary",
                            ):
                                _execute_yade_delete(f"{remote_approver} (remote)")
    
                        with st.form(f"yv_delete_approval_{vid}"):
                            st.warning("Supervisor approval required.")
                            sup_username, sup_label = _supervisor_dropdown(
                                "Supervisor",
                                f"yv_delete_sup_{vid}",
                                active_location_id,
                            )
                            code = st.text_input(
                                "Supervisor Code",
                                type="password",
                                key=f"yv_delete_code_{vid}",
                            )
                            ok = st.form_submit_button("✅ Approve & Delete", type="primary")
    
                        cancel = st.button("✖️ Cancel", key=f"yv_del_cancel_{vid}")
    
                        if ok:
                            if not sup_username:
                                st.error("No supervisor available for approval.")
                            elif SecurityManager.verify_supervisor_code(code, sup_username):
                                display_name = sup_label or sup_username
                                _execute_yade_delete(f"{display_name} (supervisor)")
                            else:
                                st.error("Invalid supervisor code.")
    
                        if cancel:
                            st.session_state.pop("yade_delete_id_pending", None)
                            st.success("Deletion cancelled.")
                            _st_safe_rerun()
    
            # -------- view / edit prompt (yade) --------
            if st.session_state.get("yade_view_id"):
                vid = st.session_state["yade_view_id"]
    
                with get_session() as s:
                    voyage = s.query(YadeVoyage).filter(YadeVoyage.id == vid).one_or_none()
                    
                    # Get dips
                    before_dips = {d.tank_id: d for d in s.query(YadeDip).filter(
                        YadeDip.voyage_id == vid, YadeDip.stage == "before"
                    ).all()}
                    
                    after_dips = {d.tank_id: d for d in s.query(YadeDip).filter(
                        YadeDip.voyage_id == vid, YadeDip.stage == "after"
                    ).all()}
                    
                    # Get sample params
                    before_params = s.query(YadeSampleParam).filter(
                        YadeSampleParam.voyage_id == vid, YadeSampleParam.stage == "before"
                    ).first()
                    
                    after_params = s.query(YadeSampleParam).filter(
                        YadeSampleParam.voyage_id == vid, YadeSampleParam.stage == "after"
                    ).first()
                    
                    # Get seals
                    seals = s.query(YadeSealDetail).filter(YadeSealDetail.voyage_id == vid).first()
                    
                    # Get TOA
                    toa = s.query(TOAYadeSummary).filter(TOAYadeSummary.voyage_id == vid).first()
    
                st.markdown("---")
                with st.container(border=True):
                # Use an em dash instead of a replacement character in the heading
                    st.markdown(f"### YADE Voyage — {voyage.yade_name if voyage else 'N/A'}")
    
                    if not voyage:
                        st.info("Voyage not found.")
                    else:
                        ns = "yv" + hashlib.md5(str(vid).encode("utf-8")).hexdigest()[:8]
                        editing = st.session_state.get("yade_edit_mode", False)
    
                        # Action buttons
                        left_actions, right_actions = st.columns([0.7, 0.3])
                        with left_actions:
                            if not editing:
                                if st.button("✏️ Edit", key=f"{ns}_edit_open", help="Edit this voyage"):
                                    if not _deny_edit_for_lock(voyage, "YadeVoyage", f"{voyage.yade_name}-{voyage.voyage_no}"):
                                        st.session_state["yade_edit_mode"] = True
                                        _st_safe_rerun()
                            else:
                                save_clicked = st.button("💾 Save", key=f"{ns}_save", help="Save changes")
                                cancel_clicked = st.button("✖️ Cancel edit", key=f"{ns}_cancel", help="Cancel editing")
    
                        with right_actions:
                            if st.button("🗑️ Delete", key=f"{ns}_del", help="Delete this voyage"):
                                st.session_state["yade_delete_id_pending"] = vid
                                _st_safe_rerun()
    
                        # Get tank IDs based on design
                        tank_ids = ["C1", "C2", "P1", "P2", "S1", "S2"] if str(voyage.design) == "6" else ["P1", "P2", "S1", "S2"]
    
                        # ============ BASIC INFO ============
                        st.markdown("#### Basic Information")
                        basic_col1, basic_col2, basic_col3, basic_col4 = st.columns(4)
                        
                        with basic_col1:
                            st.text_input("YADE Name", value=voyage.yade_name, disabled=True, key=f"{ns}_yade_name")
                            voyage_no_val = st.text_input("Voyage No", value=voyage.voyage_no, disabled=not editing, key=f"{ns}_voyage_no")
                        
                        with basic_col2:
                            convoy_no_val = st.text_input("Convoy No", value=voyage.convoy_no, disabled=not editing, key=f"{ns}_convoy_no")
                            st.text_input("Design", value=voyage.design, disabled=True, key=f"{ns}_design")
                        
                        with basic_col3:
                            date_val = st.date_input("Date", value=voyage.date, disabled=not editing, key=f"{ns}_date")
                            time_val = st.time_input("Time", value=voyage.time, disabled=not editing, key=f"{ns}_time")
                        
                        with basic_col4:
                            from models import CargoKind, DestinationKind, LoadingBerthKind
                            cargo_opts = [c.value for c in CargoKind]
                            cargo_val = st.selectbox("Cargo", cargo_opts, 
                                                    index=cargo_opts.index(_enum_text(voyage.cargo)) if _enum_text(voyage.cargo) in cargo_opts else 0,
                                                    disabled=not editing, key=f"{ns}_cargo")
                            
                            dest_opts = [d.value for d in DestinationKind]
                            dest_val = st.selectbox("Destination", dest_opts,
                                                index=dest_opts.index(_enum_text(voyage.destination)) if _enum_text(voyage.destination) in dest_opts else 0,
                                                disabled=not editing, key=f"{ns}_dest")
    
                        berth_col1, berth_col2 = st.columns(2)
                        with berth_col1:
                            berth_opts = [b.value for b in LoadingBerthKind]
                            berth_val = st.selectbox("Loading Berth", berth_opts,
                                                    index=berth_opts.index(_enum_text(voyage.loading_berth)) if _enum_text(voyage.loading_berth) in berth_opts else 0,
                                                    disabled=not editing, key=f"{ns}_berth")
    
                        # ============ GAUGE TIMES ============
                        st.markdown("#### Gauge Times")
                        gauge_col1, gauge_col2 = st.columns(2)
                        
                        with gauge_col1:
                            st.markdown("**Before Gauge**")
                            before_gauge_date_val = st.date_input("Date", value=voyage.before_gauge_date, disabled=not editing, key=f"{ns}_before_gauge_date")
                            before_gauge_time_val = st.time_input("Time", value=voyage.before_gauge_time, disabled=not editing, key=f"{ns}_before_gauge_time")
                        
                        with gauge_col2:
                            st.markdown("**After Gauge**")
                            after_gauge_date_val = st.date_input("Date", value=voyage.after_gauge_date, disabled=not editing, key=f"{ns}_after_gauge_date")
                            after_gauge_time_val = st.time_input("Time", value=voyage.after_gauge_time, disabled=not editing, key=f"{ns}_after_gauge_time")
    
                        # ============ DIP READINGS ============
                        st.markdown("#### Dip Readings")
                        dip_col1, dip_col2 = st.columns(2)
                        
                        before_dip_vals = {}
                        after_dip_vals = {}
                        
                        with dip_col1:
                            st.markdown("**Before Loading/Unloading**")
                            for tank_id in tank_ids:
                                dip_obj = before_dips.get(tank_id)
                                st.markdown(f"**Tank {tank_id}**")
                                d1, d2 = st.columns(2)
                                with d1:
                                    before_dip_vals[f"{tank_id}_total"] = st.number_input(
                                        f"Total (cm)", 
                                        value=float(dip_obj.total_cm if dip_obj else 0.0),
                                        step=0.1, disabled=not editing, 
                                        key=f"{ns}_before_{tank_id}_total"
                                    )
                                with d2:
                                    before_dip_vals[f"{tank_id}_water"] = st.number_input(
                                        f"Water (cm)", 
                                        value=float(dip_obj.water_cm if dip_obj else 0.0),
                                        step=0.1, disabled=not editing, 
                                        key=f"{ns}_before_{tank_id}_water"
                                    )
                        
                        with dip_col2:
                            st.markdown("**After Loading/Unloading**")
                            for tank_id in tank_ids:
                                dip_obj = after_dips.get(tank_id)
                                st.markdown(f"**Tank {tank_id}**")
                                d1, d2 = st.columns(2)
                                with d1:
                                    after_dip_vals[f"{tank_id}_total"] = st.number_input(
                                        f"Total (cm)", 
                                        value=float(dip_obj.total_cm if dip_obj else 0.0),
                                        step=0.1, disabled=not editing, 
                                        key=f"{ns}_after_{tank_id}_total"
                                    )
                                with d2:
                                    after_dip_vals[f"{tank_id}_water"] = st.number_input(
                                        f"Water (cm)", 
                                        value=float(dip_obj.water_cm if dip_obj else 0.0),
                                        step=0.1, disabled=not editing, 
                                        key=f"{ns}_after_{tank_id}_water"
                                    )
    
                        # ============ SAMPLE PARAMETERS ============
                        st.markdown("#### Sample Parameters")
                        param_col1, param_col2 = st.columns(2)
                        
                        before_param_vals = {}
                        after_param_vals = {}
                        
                        with param_col1:
                            st.markdown("**Before**")
                            before_param_vals["obs_mode"] = st.selectbox(
                                "Observation Mode",
                                ["Observed API", "Observed Density"],
                                index=0 if not before_params or "API" in before_params.obs_mode else 1,
                                disabled=not editing,
                                key=f"{ns}_before_obs_mode"
                            )
                            before_obs_min, before_obs_max = _observed_value_bounds(before_param_vals["obs_mode"])
                            before_param_vals["obs_val"] = _bounded_number_input(
                                "Observed Value",
                                key=f"{ns}_before_obs_val",
                                min_value=before_obs_min,
                                max_value=before_obs_max,
                                value=float(before_params.obs_val if before_params else 0.0),
                                step=0.1,
                                disabled=not editing,
                            )
                            before_param_vals["sample_unit"] = st.selectbox(
                                "Temperature Unit",
                                ["°C", "°F"],
                                index=0 if not before_params or before_params.sample_unit == "°C" else 1,
                                disabled=not editing,
                                key=f"{ns}_before_sample_unit"
                            )
                            before_param_vals["sample_temp"] = _temperature_input(
                                "Sample Temperature",
                                before_param_vals["sample_unit"],
                                key=f"{ns}_before_sample_temp",
                                value=float(before_params.sample_temp if before_params else 0.0),
                                disabled=not editing,
                            )
                            before_param_vals["tank_temp"] = _temperature_input(
                                "Tank Temperature",
                                before_param_vals["sample_unit"],
                                key=f"{ns}_before_tank_temp",
                                value=float(before_params.tank_temp if before_params else 0.0),
                                disabled=not editing,
                            )
                            before_param_vals["ccf"] = st.number_input(
                                "CCF",
                                value=float(before_params.ccf if before_params else 1.0),
                                step=0.0001, disabled=not editing,
                                key=f"{ns}_before_ccf"
                            )
                            before_param_vals["bsw_pct"] = st.number_input(
                                "BS&W %",
                                value=float(before_params.bsw_pct if before_params else 0.0),
                                step=0.01, disabled=not editing,
                                key=f"{ns}_before_bsw"
                            )
                        
                        with param_col2:
                            st.markdown("**After**")
                            after_param_vals["obs_mode"] = st.selectbox(
                                "Observation Mode",
                                ["Observed API", "Observed Density"],
                                index=0 if not after_params or "API" in after_params.obs_mode else 1,
                                disabled=not editing,
                                key=f"{ns}_after_obs_mode"
                            )
                            after_obs_min, after_obs_max = _observed_value_bounds(after_param_vals["obs_mode"])
                            after_param_vals["obs_val"] = _bounded_number_input(
                                "Observed Value",
                                key=f"{ns}_after_obs_val",
                                min_value=after_obs_min,
                                max_value=after_obs_max,
                                value=float(after_params.obs_val if after_params else 0.0),
                                step=0.1,
                                disabled=not editing,
                            )
                            after_param_vals["sample_unit"] = st.selectbox(
                                "Temperature Unit",
                                ["°C", "°F"],
                                index=0 if not after_params or after_params.sample_unit == "°C" else 1,
                                disabled=not editing,
                                key=f"{ns}_after_sample_unit"
                            )
                            after_param_vals["sample_temp"] = _temperature_input(
                                "Sample Temperature",
                                after_param_vals["sample_unit"],
                                key=f"{ns}_after_sample_temp",
                                value=float(after_params.sample_temp if after_params else 0.0),
                                disabled=not editing,
                            )
                            after_param_vals["tank_temp"] = _temperature_input(
                                "Tank Temperature",
                                after_param_vals["sample_unit"],
                                key=f"{ns}_after_tank_temp",
                                value=float(after_params.tank_temp if after_params else 0.0),
                                disabled=not editing,
                            )
                            after_param_vals["ccf"] = st.number_input(
                                "CCF",
                                value=float(after_params.ccf if after_params else 1.0),
                                step=0.0001, disabled=not editing,
                                key=f"{ns}_after_ccf"
                            )
                            after_param_vals["bsw_pct"] = st.number_input(
                                "BS&W %",
                                value=float(after_params.bsw_pct if after_params else 0.0),
                                step=0.01, disabled=not editing,
                                key=f"{ns}_after_bsw"
                            )
    
                        # ============ SEAL DETAILS ============
                        st.markdown("#### Seal Details")
                        
                        seal_vals = {}
                        seal_header = st.columns([0.10, 0.225, 0.225, 0.225, 0.225])
                        seal_header[0].markdown("**Tank**")
                        seal_header[1].markdown("**Manhole-1**")
                        seal_header[2].markdown("**Manhole-2**")
                        seal_header[3].markdown("**Lock No**")
                        seal_header[4].markdown("**Dip Hatch**")
                        
                        for tank_id in tank_ids:
                            seal_row = st.columns([0.10, 0.225, 0.225, 0.225, 0.225])
                            seal_row[0].write(tank_id)
                            
                            tank_lower = tank_id.lower()
                            seal_vals[f"{tank_id}_mh1"] = seal_row[1].text_input(
                                "MH1", value=getattr(seals, f"{tank_lower}_mh1", "") if seals else "",
                                disabled=not editing, key=f"{ns}_seal_{tank_id}_mh1",
                                label_visibility="collapsed"
                            )
                            seal_vals[f"{tank_id}_mh2"] = seal_row[2].text_input(
                                "MH2", value=getattr(seals, f"{tank_lower}_mh2", "") if seals else "",
                                disabled=not editing, key=f"{ns}_seal_{tank_id}_mh2",
                                label_visibility="collapsed"
                            )
                            seal_vals[f"{tank_id}_lock"] = seal_row[3].text_input(
                                "Lock", value=getattr(seals, f"{tank_lower}_lock", "") if seals else "",
                                disabled=not editing, key=f"{ns}_seal_{tank_id}_lock",
                                label_visibility="collapsed"
                            )
                            seal_vals[f"{tank_id}_diphatch"] = seal_row[4].text_input(
                                "Diphatch", value=getattr(seals, f"{tank_lower}_diphatch", "") if seals else "",
                                disabled=not editing, key=f"{ns}_seal_{tank_id}_diphatch",
                                label_visibility="collapsed"
                            )
    
                        # ============ TOA SUMMARY ============
                        if toa:
                            st.markdown("#### TOA Summary")
                            toa_col1, toa_col2, toa_col3 = st.columns(3)
                            
                            with toa_col1:
                                st.metric("GSV Before", f"{toa.gsv_before_bbl:,.2f} bbls")
                            
                            with toa_col2:
                                st.metric("GSV After", f"{toa.gsv_after_bbl:,.2f} bbls")
                            
                            with toa_col3:
                                st.metric("GSV Loaded", f"{toa.gsv_loaded_bbl:,.2f} bbls", delta=f"{toa.gsv_loaded_bbl:,.2f}")
    
                        # ============ SAVE LOGIC ============
                        if editing and save_clicked:
                            try:
                                with get_session() as s:
                                    # Update voyage
                                    db_voyage = s.query(YadeVoyage).filter(YadeVoyage.id == vid).one_or_none()
                                    if db_voyage:
                                        db_voyage.voyage_no = voyage_no_val
                                        db_voyage.convoy_no = convoy_no_val
                                        db_voyage.date = date_val
                                        db_voyage.time = time_val
                                        db_voyage.cargo = cargo_val
                                        db_voyage.destination = dest_val
                                        db_voyage.loading_berth = berth_val
                                        db_voyage.before_gauge_date = before_gauge_date_val
                                        db_voyage.before_gauge_time = before_gauge_time_val
                                        db_voyage.after_gauge_date = after_gauge_date_val
                                        db_voyage.after_gauge_time = after_gauge_time_val
                                    
                                    # Update dips
                                    for tank_id in tank_ids:
                                        # Before dips
                                        before_dip = s.query(YadeDip).filter(
                                            YadeDip.voyage_id == vid,
                                            YadeDip.tank_id == tank_id,
                                            YadeDip.stage == "before"
                                        ).first()
                                        if before_dip:
                                            before_dip.total_cm = before_dip_vals[f"{tank_id}_total"]
                                            before_dip.water_cm = before_dip_vals[f"{tank_id}_water"]
                                        
                                        # After dips
                                        after_dip = s.query(YadeDip).filter(
                                            YadeDip.voyage_id == vid,
                                            YadeDip.tank_id == tank_id,
                                            YadeDip.stage == "after"
                                        ).first()
                                        if after_dip:
                                            after_dip.total_cm = after_dip_vals[f"{tank_id}_total"]
                                            after_dip.water_cm = after_dip_vals[f"{tank_id}_water"]
                                    
                                    # Update sample params
                                    db_before_params = s.query(YadeSampleParam).filter(
                                        YadeSampleParam.voyage_id == vid,
                                        YadeSampleParam.stage == "before"
                                    ).first()
                                    if db_before_params:
                                        db_before_params.obs_mode = before_param_vals["obs_mode"]
                                        db_before_params.obs_val = before_param_vals["obs_val"]
                                        db_before_params.sample_unit = before_param_vals["sample_unit"]
                                        db_before_params.sample_temp = before_param_vals["sample_temp"]
                                        db_before_params.tank_temp = before_param_vals["tank_temp"]
                                        db_before_params.ccf = before_param_vals["ccf"]
                                        db_before_params.bsw_pct = before_param_vals["bsw_pct"]
                                    
                                    db_after_params = s.query(YadeSampleParam).filter(
                                        YadeSampleParam.voyage_id == vid,
                                        YadeSampleParam.stage == "after"
                                    ).first()
                                    if db_after_params:
                                        db_after_params.obs_mode = after_param_vals["obs_mode"]
                                        db_after_params.obs_val = after_param_vals["obs_val"]
                                        db_after_params.sample_unit = after_param_vals["sample_unit"]
                                        db_after_params.sample_temp = after_param_vals["sample_temp"]
                                        db_after_params.tank_temp = after_param_vals["tank_temp"]
                                        db_after_params.ccf = after_param_vals["ccf"]
                                        db_after_params.bsw_pct = after_param_vals["bsw_pct"]
                                    
                                    # Update seals
                                    db_seals = s.query(YadeSealDetail).filter(YadeSealDetail.voyage_id == vid).first()
                                    if db_seals:
                                        for tank_id in tank_ids:
                                            tank_lower = tank_id.lower()
                                            setattr(db_seals, f"{tank_lower}_mh1", seal_vals[f"{tank_id}_mh1"])
                                            setattr(db_seals, f"{tank_lower}_mh2", seal_vals[f"{tank_id}_mh2"])
                                            setattr(db_seals, f"{tank_lower}_lock", seal_vals[f"{tank_id}_lock"])
                                            setattr(db_seals, f"{tank_lower}_diphatch", seal_vals[f"{tank_id}_diphatch"])
    
                                    if db_voyage:
                                        _persist_toa_from_current_inputs(
                                            s,
                                            db_voyage,
                                            db_voyage.yade_name,
                                            tank_ids,
                                            len(tank_ids),
                                        )
    
                                    from datetime import datetime, timezone
                                    editor = (st.session_state.get("auth_user") or {}).get("username", "unknown")
                                    if db_voyage is not None:
                                        if hasattr(db_voyage, "updated_by"):
                                            db_voyage.updated_by = editor
                                        if hasattr(db_voyage, "updated_at"):
                                            db_voyage.updated_at = datetime.now(timezone.utc)
                                    s.commit()
                                    # ----------------------- Audit log for YADE voyage update -----------------------
                                    try:
                                        from security import SecurityManager  # type: ignore
                                        user_ctx = st.session_state.get("auth_user") or {}
                                        username = user_ctx.get("username", "unknown")
                                        user_id = user_ctx.get("id")
                                        resource_id = str(getattr(db_voyage, "id", "")) or f"{db_voyage.yade_name}-{getattr(db_voyage, 'voyage_no', '')}"
                                        SecurityManager.log_audit(
                                            None,
                                            username,
                                            "UPDATE",
                                            resource_type="YadeVoyage",
                                            resource_id=resource_id,
                                            details=f"Updated YADE voyage {db_voyage.yade_name} - {getattr(db_voyage, 'voyage_no', '')}",
                                            user_id=user_id,
                                            location_id=active_location_id,
                                        )
                                    except Exception:
                                        pass
                                st.success("? Changes saved successfully!")
                                st.session_state["yade_edit_mode"] = False
                                _st_safe_rerun()
                            
                            except Exception as ex:
                                st.error(f"? Failed to save: {ex}")
                                import traceback
                                st.code(traceback.format_exc())
    
                        if editing and cancel_clicked:
                            st.session_state["yade_edit_mode"] = False
                            _st_safe_rerun()
    
                    st.divider()
                    if st.button("⬅️ Close", key=f"{ns}_close", help="Close viewer"):
                        st.session_state.pop("yade_view_id", None)
                        st.session_state.pop("yade_edit_mode", None)
                        _st_safe_rerun()
    
        # ===========================================================
        # ============== TANKER TRANSACTIONS VIEWER =================
        # ===========================================================
        elif scope == "Tanker Transactions":
            st.markdown("### Tanker Transactions")
            
            # [Keep your existing tanker code - it already has the table structure]
            # Just add the view/delete prompt windows similar to YADE above
            
            st.info("Tanker viewer - Add view/delete prompts if needed")
            
        if scope in ("Tank Transactions", "Condensate Receipts"):
            if st.session_state.get("tt_delete_tid_pending"):
                tid = st.session_state["tt_delete_tid_pending"]
                st.markdown("---")
                with st.container(border=True):
                    st.markdown(f"### Delete Transaction � {tid}")
    
                    user_info = st.session_state.get("auth_user") or {"username": "unknown", "role": "operator"}
                    role = user_info.get("role", "operator")
                    username = user_info.get("username", "unknown")
                    user_id = user_info.get("id")
                    loc_id = active_location_id
    
                    def _execute_tank_delete(approver_label: str):
                        try:
                            with get_session() as s:
                                rec = (
                                    s.query(TankTransaction)
                                    .filter(
                                        TankTransaction.ticket_id == tid,
                                        TankTransaction.location_id == loc_id,
                                    )
                                    .first()
                                )
    
                                rec_id = rec.id if rec else None
                                rec_created_by = getattr(rec, "created_by", None) if rec else None
                                otr_rows = (
                                    s.query(OTRRecord)
                                    .filter(
                                        OTRRecord.ticket_id == tid,
                                        OTRRecord.location_id == loc_id,
                                    )
                                    .all()
                                )
                                deleted_otr = len(otr_rows)
                                otr_payload = {"otr_rows": [RecycleBinManager.snapshot_record(o) for o in otr_rows]}
    
                                if rec:
                                    _archive_record_for_delete(
                                        s,
                                        rec,
                                        "TankTransaction",
                                        reason=(
                                            f"Marked tank transaction {tid} for deletion "
                                            f"(created_by={rec_created_by}, scope={scope}, OTR={deleted_otr}). "
                                            f"Approved by {approver_label}."
                                        ),
                                        label=tid,
                                        extra_payload=otr_payload,
                                    )
                                if deleted_otr:
                                    for otr in otr_rows:
                                        _archive_payload_for_delete(
                                            s,
                                            "OTRRecord",
                                            str(otr.id),
                                            payload=RecycleBinManager.snapshot_record(otr),
                                            reason=f"Mirrored OTR record for tank ticket {tid}.",
                                            label=tid,
                                        )
                                        s.delete(otr)
    
                                s.commit()
    
                            TaskManager.complete_tasks_for_resource(
                                "TankTransaction",
                                tid,
                                username,
                                notes=f"Approved by {approver_label}",
                            )
                            st.success(f"Deleted {tid}. Approved by {approver_label}.")
                        except Exception as ex:
                            st.error(f"Failed to delete: {ex}")
                        finally:
                            st.session_state.pop("tt_delete_tid_pending", None)
                            _st_safe_rerun()
    
                    if role in ("admin", "supervisor"):
                        approver = f"{username} ({role})"
                        st.info(f"Approval: {approver}")
                        do_del = st.button("Confirm Delete", key=f"tt_del_confirm_{tid}", type="primary")
                        cancel = st.button("Cancel", key=f"tt_del_cancel_{tid}")
    
                        if do_del:
                            _execute_tank_delete(approver)
    
                        if cancel:
                            st.session_state.pop("tt_delete_tid_pending", None)
                            st.success("Deletion cancelled.")
                            _st_safe_rerun()
                    else:
                        remote_task = _render_remote_delete_request_ui(
                            "TankTransaction",
                            tid,
                            f"Tank ticket {tid}",
                            "Tank Transactions",
                            metadata={"scope": scope},
                        )
                        if remote_task and remote_task.get("status") == TaskStatus.APPROVED.value:
                            remote_approver = remote_task.get("approved_by") or "Supervisor"
                            if st.button(
                                "Delete with approved request",
                                key=f"tt_remote_delete_{tid}",
                                type="primary",
                            ):
                                _execute_tank_delete(f"{remote_approver} (remote)")
    
                        with st.form(f"tt_delete_approval_{tid}"):
                            st.warning("Supervisor approval required.")
                            sup_username, sup_label = _supervisor_dropdown(
                                "Supervisor",
                                f"tt_delete_sup_{tid}",
                                active_location_id,
                            )
                            code = st.text_input(
                                "Supervisor Code",
                                type="password",
                                key=f"tt_sup_code_{tid}",
                            )
                            ok = st.form_submit_button("Approve & Delete", type="primary")
    
                        cancel = st.button("Cancel", key=f"tt_del_cancel_{tid}")
    
                        if ok:
                            if not sup_username:
                                st.error("No supervisor available for approval.")
                            elif SecurityManager.verify_supervisor_code(code, sup_username):
                                _execute_tank_delete(f"{sup_label or sup_username} (supervisor)")
                            else:
                                st.error("Invalid supervisor code.")
    
                        if cancel:
                            st.session_state.pop("tt_delete_tid_pending", None)
                            st.success("Deletion cancelled.")
                            _st_safe_rerun()
    
    
            # -------- shared view / edit prompt --------
            if st.session_state.get("tt_view_tid"):
                tid = st.session_state["tt_view_tid"]
    
                with get_session() as s:
                    rec = s.query(TankTransaction).filter(TankTransaction.ticket_id == tid).first()
    
                st.markdown("---")
                with st.container(border=True):
                    st.markdown(f"### Tank Transaction 📝\" {tid}")
                    ns = "tt" + hashlib.md5(tid.encode("utf-8")).hexdigest()[:8]
                    editing = st.session_state.get("tt_edit_mode", False)
    
                    left_actions, right_actions = st.columns([0.7, 0.3])
                    with left_actions:
                        if not editing:
                            if st.button("Edit", key=f"{ns}_edit_open", help="Edit this transaction"):
                                if not _deny_edit_for_lock(rec, "TankTransaction", tid):
                                    st.session_state["tt_edit_mode"] = True
                                    _st_safe_rerun()
                        else:
                            save_clicked   = st.button("Save", key=f"{ns}_save", help="Save edited")
                            cancel_clicked = st.button("Cancel edit", key=f"{ns}_cancel", help="Cancel edit")
    
                    with right_actions:
                        if st.button("Delete", key=f"{ns}_del", help="Delete this transaction"):
                            st.session_state["tt_delete_tid_pending"] = tid
                            _st_safe_rerun()
    
                    if not rec:
                        st.info("Record not found.")
                    else:
                        op_label = (
                            rec.operation.value
                            if hasattr(rec.operation, "value")
                            else str(rec.operation or "")
                        )
                        is_condensate = _is_condensate_tx(rec)
    
                        dip_val = float(rec.dip_cm or 0.0)
                        water_val = float(rec.water_cm or 0.0)
                        tank_temp_c_val = float(rec.tank_temp_c or 0.0)
                        tank_temp_f_val = float(rec.tank_temp_f or 0.0)
                        api_val = float(rec.api_observed or 0.0)
                        dens_val = float(rec.density_observed or 0.0)
                        bsw_val = float(getattr(rec, "bsw_pct", 0.0) or 0.0)
                        sample_temp_c_val = float(rec.sample_temp_c or 0.0)
                        sample_temp_f_val = float(rec.sample_temp_f or 0.0)
                        qty_val = float(rec.qty_bbls or 0.0)
                        opening_meter_val = float(rec.opening_meter_reading or 0.0)
                        closing_meter_val = float(rec.closing_meter_reading or 0.0)
    
                        if is_condensate:
                            meter_cols = st.columns(2)
                            with meter_cols[0]:
                                opening_meter_val = st.number_input(
                                    "Opening Meter (m�)",
                                    value=opening_meter_val,
                                    step=0.001,
                                    disabled=not editing,
                                    key=f"{ns}_cond_open"
                                )
                            with meter_cols[1]:
                                closing_meter_val = st.number_input(
                                    "Closing Meter (m�)",
                                    value=closing_meter_val,
                                    step=0.001,
                                    disabled=not editing,
                                    key=f"{ns}_cond_close"
                                )
    
                            temp_cols = st.columns(2)
                            with temp_cols[0]:
                                tank_temp_c_val = _temperature_input(
                                    "Tank Temp (°C)",
                                    "°C",
                                    key=f"{ns}_cond_ttc",
                                    value=tank_temp_c_val,
                                    disabled=not editing,
                                )
                                sample_temp_c_val = _temperature_input(
                                    "Sample Temp (°C)",
                                    "°C",
                                    key=f"{ns}_cond_stc",
                                    value=sample_temp_c_val,
                                    disabled=not editing,
                                )
                            with temp_cols[1]:
                                tank_temp_f_val = _temperature_input(
                                    "Tank Temp (°F)",
                                    "°F",
                                    key=f"{ns}_cond_ttf",
                                    value=tank_temp_f_val,
                                    disabled=not editing,
                                )
                                sample_temp_f_val = _temperature_input(
                                    "Sample Temp (°F)",
                                    "°F",
                                    key=f"{ns}_cond_stf",
                                    value=sample_temp_f_val,
                                    disabled=not editing,
                                )
    
                            prop_cols = st.columns(2)
                            with prop_cols[0]:
                                api_val = _bounded_number_input(
                                    "Observed API",
                                    key=f"{ns}_cond_api",
                                    min_value=API_MIN,
                                    max_value=API_MAX,
                                    value=api_val,
                                    step=0.1,
                                    disabled=not editing,
                                )
                            with prop_cols[1]:
                                dens_val = _bounded_number_input(
                                    "Observed Density (kg/m3)",
                                    key=f"{ns}_cond_dens",
                                    min_value=DENSITY_MIN,
                                    max_value=DENSITY_MAX,
                                    value=dens_val,
                                    step=0.1,
                                    disabled=not editing,
                                )
                            bsw_val = 0.0
                            dip_val = 0.0
                            water_val = 0.0
                        else:
                            c1, c2, c3, c4 = st.columns(4)
                            with c1:
                                dip_val = st.number_input(
                                    "Dip (cm)",
                                    value=dip_val,
                                    step=0.1,
                                    disabled=not editing,
                                    key=f"{ns}_dip"
                                )
                                water_val = st.number_input(
                                    "Water (cm)",
                                    value=water_val,
                                    step=0.1,
                                    disabled=not editing,
                                    key=f"{ns}_water"
                                )
                            with c2:
                                tank_temp_c_val = _temperature_input(
                                    "Tank Temp (degC)",
                                    "°C",
                                    key=f"{ns}_ttc",
                                    value=tank_temp_c_val,
                                    disabled=not editing,
                                )
                                tank_temp_f_val = _temperature_input(
                                    "Tank Temp (degF)",
                                    "°F",
                                    key=f"{ns}_ttf",
                                    value=tank_temp_f_val,
                                    disabled=not editing,
                                )
                            with c3:
                                api_val = _bounded_number_input(
                                    "Observed API",
                                    key=f"{ns}_api",
                                    min_value=API_MIN,
                                    max_value=API_MAX,
                                    value=api_val,
                                    step=0.1,
                                    disabled=not editing,
                                )
                                dens_val = _bounded_number_input(
                                    "Observed Density (kg/m3)",
                                    key=f"{ns}_den",
                                    min_value=DENSITY_MIN,
                                    max_value=DENSITY_MAX,
                                    value=dens_val,
                                    step=0.1,
                                    disabled=not editing,
                                )
                                bsw_val = st.number_input(
                                    "BS&W %",
                                    value=bsw_val,
                                    step=0.01,
                                    disabled=not editing,
                                    key=f"{ns}_bsw"
                                )
                            with c4:
                                sample_temp_c_val = _temperature_input(
                                    "Sample Temp (degC)",
                                    "°C",
                                    key=f"{ns}_stc",
                                    value=sample_temp_c_val,
                                    disabled=not editing,
                                )
                                sample_temp_f_val = _temperature_input(
                                    "Sample Temp (degF)",
                                    "°F",
                                    key=f"{ns}_stf",
                                    value=sample_temp_f_val,
                                    disabled=not editing,
                                )
    
                        remarks_col, extra_col = st.columns([0.6, 0.4])
                        with remarks_col:
                            remarks = st.text_area(
                                "Remarks",
                                value=rec.remarks or "",
                                disabled=not editing,
                                key=f"{ns}_remarks"
                            )
                        with extra_col:
                            extra = st.text_area(
                                "Additional Notes",
                                value="",
                                disabled=not editing,
                                key=f"{ns}_extra"
                            )
    
                        tank_name_for_calc = rec.tank_name
                        if not tank_name_for_calc and rec.tank_id:
                            with get_session() as s_lookup:
                                tank_obj = s_lookup.query(Tank).filter(Tank.id == rec.tank_id).one_or_none()
                                if tank_obj:
                                    tank_name_for_calc = tank_obj.name
    
                        if is_condensate:
                            opening_meter = float(opening_meter_val or 0.0)
                            closing_meter = float(closing_meter_val or opening_meter)
                            condensate_qty_m3 = max(closing_meter - opening_meter, 0.0)
                            tov_bbl = round(condensate_qty_m3 * CONDENSATE_M3_TO_BBL, 2)
                            fw_bbl = 0.0
                            gov_bbl = tov_bbl
    
                            sample_temp_c_used = sample_temp_c_val or (
                                (sample_temp_f_val - 32.0) / 1.8 if sample_temp_f_val else 0.0
                            )
                            sample_temp_f_used = sample_temp_f_val or (
                                (sample_temp_c_val * 1.8) + 32.0 if sample_temp_c_val else 0.0
                            )
                            tank_temp_c_used = tank_temp_c_val or (
                                (tank_temp_f_val - 32.0) / 1.8 if tank_temp_f_val else 0.0
                            )
    
                            if api_val > 0:
                                sample_unit = "°F" if sample_temp_f_used else "°C"
                                sample_temp_for_api = sample_temp_f_used if sample_unit == "°F" else sample_temp_c_used
                                api60_val = convert_api_to_60_from_api(api_val, sample_temp_for_api or 0.0, sample_unit)
                            elif dens_val > 0:
                                sample_temp_for_density = sample_temp_c_used or 15.0
                                api60_val = convert_api_to_60_from_density(dens_val, sample_temp_for_density or 0.0, "°C")
                            else:
                                api60_val = 0.0
    
                            input_mode = "api" if api_val > 0 else ("density" if dens_val > 0 else "api")
                            vcf_val = vcf_from_api60_and_temp(api60_val, tank_temp_c_used, "°C", input_mode)
                            gsv_bbl = round(gov_bbl * vcf_val, 2)
                            bsw_vol = 0.0
                            nsv_bbl = gsv_bbl
                            try:
                                with get_session() as s_lt:
                                    lt_factor = lookup_lt_factor(s_lt, api60_val) if api60_val > 0 else 0.0
                            except Exception:
                                lt_factor = 0.0
                            lt_val = round(nsv_bbl * lt_factor, 2)
                            mt_val = round(lt_val * 1.01605, 2)
                        else:
                            with get_session() as s_cal:
                                tov_bbl = _calc_tov_from_calibration(s_cal, tank_name_for_calc, dip_val, rec.location_id) if tank_name_for_calc else None
                                fw_bbl = (
                                    _calc_tov_from_calibration(s_cal, tank_name_for_calc, water_val, rec.location_id)
                                    if tank_name_for_calc and water_val > 0 else 0.0
                                )
                            if tov_bbl is None:
                                tov_bbl = float(qty_val or 0.0)
                            else:
                                tov_bbl = float(tov_bbl)
                            fw_bbl = float(fw_bbl or 0.0)
                            gov_bbl = max(tov_bbl - fw_bbl, 0.0)
    
                            if not sample_temp_f_val and sample_temp_c_val:
                                sample_temp_f_val = (sample_temp_c_val * 1.8) + 32.0
                            if not sample_temp_c_val and sample_temp_f_val:
                                sample_temp_c_val = (sample_temp_f_val - 32.0) / 1.8
    
                            sample_temp_unit = "°F"
                            sample_temp_for_api = sample_temp_f_val or 60.0
                            if api_val > 0:
                                api60_val = convert_api_to_60_from_api(api_val, sample_temp_for_api, sample_temp_unit)
                            elif dens_val > 0:
                                sample_temp_for_density = sample_temp_c_val or 15.0
                                api60_val = convert_api_to_60_from_density(dens_val, sample_temp_for_density, "°C")
                            else:
                                api60_val = 0.0
    
                            tank_temp_for_vcf = tank_temp_c_val or ((tank_temp_f_val - 32.0) / 1.8 if tank_temp_f_val else 0.0)
                            tank_temp_unit = "°C"
                            input_mode = "density" if (api_val <= 0 and dens_val > 0) else "api"
                            vcf_val = vcf_from_api60_and_temp(api60_val, tank_temp_for_vcf, tank_temp_unit, input_mode)
    
                            gsv_bbl = round(gov_bbl * vcf_val, 2)
                            bsw_vol = round(gsv_bbl * (bsw_val / 100.0), 2)
                            nsv_bbl = round(gsv_bbl - bsw_vol, 2)
                            if api60_val and api60_val > 0:
                                with get_session() as s_lt:
                                    lt_factor = lookup_lt_factor(s_lt, api60_val)
                            else:
                                lt_factor = 0.0
                            lt_val = round(nsv_bbl * lt_factor, 2)
                            mt_val = round(lt_val * 1.01605, 2)
    
                        st.caption(f"TOV: {tov_bbl:.2f} bbl | GOV: {gov_bbl:.2f} bbl | GSV: {gsv_bbl:.2f} bbl | NSV: {nsv_bbl:.2f} bbl")
    
                        if editing and save_clicked:
                            try:
                                with get_session() as s:
                                    dbrec = s.query(TankTransaction).filter(TankTransaction.ticket_id == tid).first()
                                    if not dbrec:
                                        st.error("Record not found in database.")
                                    else:
                                        if is_condensate:
                                            dbrec.dip_cm = None
                                            dbrec.water_cm = None
                                            dbrec.opening_meter_reading = float(opening_meter)
                                            dbrec.closing_meter_reading = float(closing_meter)
                                            dbrec.condensate_qty_m3 = float(condensate_qty_m3)
                                        else:
                                            dbrec.dip_cm = dip_val
                                            dbrec.water_cm = water_val
                                            dbrec.opening_meter_reading = None
                                            dbrec.closing_meter_reading = None
                                            dbrec.condensate_qty_m3 = None
    
                                        dbrec.tank_temp_c = tank_temp_c_val
                                        dbrec.tank_temp_f = tank_temp_f_val
                                        dbrec.api_observed = api_val
                                        dbrec.density_observed = dens_val
                                        dbrec.sample_temp_c = sample_temp_c_val
                                        dbrec.sample_temp_f = sample_temp_f_val
                                        dbrec.bsw_pct = bsw_val
                                        dbrec.qty_bbls = float(round(tov_bbl, 2))
                                        editor_name = (st.session_state.get("auth_user") or {}).get("username", "unknown")
                                        edit_time = datetime.now()
                                        audit = f"edited by {editor_name}, {edit_time.strftime('%Y-%m-%d %H:%M')}"
                                        if extra:
                                            audit = f"{audit} | {extra}"
                                        dbrec.remarks = ((dbrec.remarks or "") + " | " + audit).strip(" |")
                                        dbrec.updated_by = editor_name
                                        dbrec.updated_at = edit_time
    
                                        if not is_condensate:
                                            otr = s.query(OTRRecord).filter(OTRRecord.ticket_id == tid).one_or_none()
                                            if not otr:
                                                otr = OTRRecord(
                                                    location_id=dbrec.location_id,
                                                    ticket_id=tid,
                                                    tank_id=dbrec.tank_name or (str(dbrec.tank_id) if dbrec.tank_id else None),
                                                    date=dbrec.date,
                                                    time=dbrec.time,
                                                    operation=op_label,
                                                )
                                                s.add(otr)
    
                                            otr.dip_cm = dip_val
                                            otr.total_volume_bbl = float(round(tov_bbl, 2))
                                            otr.water_cm = water_val
                                            otr.free_water_bbl = float(round(fw_bbl, 2))
                                            otr.gov_bbl = float(round(gov_bbl, 2))
                                            otr.api60 = float(api60_val)
                                            otr.vcf = float(vcf_val)
                                            otr.gsv_bbl = float(gsv_bbl)
                                            otr.bsw_vol_bbl = float(bsw_vol)
                                            otr.nsv_bbl = float(nsv_bbl)
                                            otr.lt = float(lt_val)
                                            otr.mt = float(mt_val)
    
                                        s.commit()
                                        # ----------------------- Audit log for tank transaction edit -----------------------
                                        try:
                                            from security import SecurityManager  # type: ignore
                                            user_ctx = st.session_state.get("auth_user") or {}
                                            username = user_ctx.get("username", "unknown")
                                            user_id = user_ctx.get("id")
                                            location_id = st.session_state.get("active_location_id") or user_ctx.get("location_id")
                                            # Determine record ID: use dbrec.id if available, otherwise fallback to tid
                                            rec_id = str(dbrec.id) if 'dbrec' in locals() and getattr(dbrec, 'id', None) else str(tid)
                                            SecurityManager.log_audit(
                                                None,
                                                username,
                                                "UPDATE",
                                                resource_type="TankTransaction",
                                                resource_id=rec_id,
                                                details=f"Updated tank transaction (ticket {tid})",
                                                user_id=user_id,
                                                location_id=location_id,
                                            )
                                        except Exception:
                                            # Do not interrupt the user flow if audit logging fails
                                            pass
    
                                st.success("Changes saved.")
                                st.session_state["tt_edit_mode"] = False
                                _st_safe_rerun()
                            except Exception as ex:
                                st.error(f"Failed to save: {ex}")
    
                        if editing and cancel_clicked:
                            st.session_state["tt_edit_mode"] = False
                            _st_safe_rerun()
    
                    st.divider()
                    if st.button("Close", key=f"{ns}_close", help="Close viewer"):
                        st.session_state.pop("tt_view_tid", None)
                        st.session_state.pop("tt_edit_mode", None)
                        _st_safe_rerun()
    # OTR � placeholder