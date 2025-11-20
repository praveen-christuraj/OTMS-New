"""
Auto-generated module for the 'OTR' page.
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
        header("Out-Turn Report")
        
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
                if _cfg.get("page_access", {}).get("OTR") is False:
                    st.error("🚫 OTR page is disabled for this location.")
                    st.stop()
        except Exception:
            pass
    
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
    
        # Display current location and load config
        with get_session() as s:
            from location_manager import LocationManager
            from location_config import LocationConfig
            
            loc = LocationManager.get_location_by_id(s, active_location_id)
            if loc:
                st.info(f"📍 **Active Location:** {loc.name} ({loc.code})")
            
            # Load location-specific configuration
            config = LocationConfig.get_config(s, active_location_id)
            tank_config = config.get("tank_transactions", {})
    
        # Models (safe imports)
        try:
            from models import Tank, OTRRecord
        except Exception:
            pass
    
        # Get configured operations for this location
        location_operations = tank_config.get("enabled_operations", [
            "Opening Stock","Receipt - Commingled","Receipt - Condensate", "Receipt", "Receipt from Agu", "Receipt from OFS",
            "OKW Receipt", "ANZ Receipt", "Other Receipts","Dispatch",
            "Dispatch to barge", "Other Dispatch",
            "ITT - Receipt", "ITT - Dispatch", "Settling", "Draining"
        ])
    
        # Determine earliest/latest OTR entry dates for default filter bounds
        otr_first_date = None
        otr_last_date = None
        with get_session() as s_first:
            first_entry = (
                s_first.query(OTRRecord.date)
                .filter(OTRRecord.location_id == active_location_id)
                .order_by(OTRRecord.date.asc())
                .first()
            )
            if first_entry and first_entry[0]:
                otr_first_date = first_entry[0]
            last_entry = (
                s_first.query(OTRRecord.date)
                .filter(OTRRecord.location_id == active_location_id)
                .order_by(OTRRecord.date.desc())
                .first()
            )
            if last_entry and last_entry[0]:
                otr_last_date = last_entry[0]
        if otr_first_date is None:
            otr_first_date = date.today()
        if otr_last_date is None:
            otr_last_date = date.today()
        otr_last_date = min(otr_last_date, date.today())
        if otr_first_date > otr_last_date:
            otr_first_date = otr_last_date
    
        otr_from_default = _ensure_date_key_in_bounds(
            "otr_f_from", otr_first_date, otr_last_date, otr_first_date
        )
        otr_to_default = _ensure_date_key_in_bounds(
            "otr_f_to", otr_first_date, otr_last_date, otr_last_date
        )
    
        # --- Filters (with Tank dropdown as requested) ---
        with st.container(border=True):
            st.caption("Live filters")
            c1, c2, c3, c4, c5 = st.columns([0.20, 0.20, 0.18, 0.21, 0.21])
    
            # Tank list - FILTERED BY LOCATION (names)
            with get_session() as s:
                tanks_all = s.query(Tank).filter(
                    Tank.location_id == active_location_id
                ).order_by(Tank.name).all()
            tank_name_list = [t.name for t in tanks_all]
            tank_opts = ["(All Tanks)"] + tank_name_list
    
            with c1:
                f_tank = st.selectbox("Tank", tank_opts, index=0, key="otr_f_tank")
            with c2:
                f_ticket = st.text_input("Ticket ID", key="otr_f_ticket")
            with c3:
                # Operation filter - ONLY SHOW LOCATION-CONFIGURED OPERATIONS
                operation_opts = ["(All)"] + location_operations
                f_op = st.selectbox("Operation", operation_opts, index=0, key="otr_f_op")
            with c4:
                f_from = st.date_input(
                    "From date",
                    value=otr_from_default,
                    min_value=otr_first_date,
                    max_value=otr_last_date,
                    key="otr_f_from",
                )
            with c5:
                f_to = st.date_input(
                    "To date",
                    value=otr_to_default,
                    min_value=otr_first_date,
                    max_value=otr_last_date,
                    key="otr_f_to",
                )
    
        # --- Load OTR from DB - FILTERED BY LOCATION ---
        with get_session() as s:
            rows = s.query(OTRRecord).filter(
                OTRRecord.location_id == active_location_id  # LOCATION FILTER
            ).order_by(OTRRecord.date.asc(), OTRRecord.time.asc()).all()
    
        if not rows:
            st.info(f"No OTR records yet for {loc.name}.")
            st.stop()
    
        # --- All OTR records as dataframe (unfiltered) ---
        # Use tank_name for "Tank" (so Tank filter works), and stringify Operation safely
        def _safe_str(x):
            try:
                return str(x) if x is not None else ""
            except Exception:
                return ""
    
        df = pd.DataFrame([{
            "Ticket ID": r.ticket_id,
            "Tank": getattr(r, "tank_name", None) or getattr(r, "tank_id", None),  # prefer name
            "Date": r.date,
            "Time": r.time,
            "Operation": _safe_str(getattr(r, "operation", None)),
            "Dip (cm)": r.dip_cm,
            "Total Volume (bbl)": r.total_volume_bbl,
            "Water (cm)": r.water_cm,
            "Free Water (bbl)": r.free_water_bbl,
            "GOV (bbl)": r.gov_bbl,
            "API @ 60°F": r.api60,
            "VCF": r.vcf,
            "GSV (bbl)": r.gsv_bbl,
            "BS&W Vol (bbl)": r.bsw_vol_bbl,
            "NSV (bbl)": r.nsv_bbl,
            "LT": r.lt,
            "MT": r.mt,
            "Operation Enum": getattr(r, "operation", None),
        } for r in rows])
    
        # Build a proper timestamp column for correct per-tank ordering
        # Date is date, Time may be string/time; coerce both then combine
        _date = pd.to_datetime(df["Date"], errors="coerce")
        _time = pd.to_datetime(df["Time"].astype(str), errors="coerce").dt.time
        df["DT"] = [
            (datetime.combine(d.date(), t) if (pd.notna(d) and t is not pd.NaT and t is not None) else pd.NaT)
            for d, t in zip(_date, _time)
        ]
    
        # --- Apply filters ---
        fdf = df.copy()
        if f_tank and f_tank != "(All Tanks)":
            fdf = fdf[fdf["Tank"] == f_tank]
        if f_ticket:
            fdf = fdf[fdf["Ticket ID"].astype(str).str.contains(f_ticket.strip(), case=False, na=False)]
        if f_op and f_op != "(All)":
            fdf = fdf[fdf["Operation"].astype(str) == str(f_op)]
        if f_from:
            fdf = fdf[pd.to_datetime(fdf["Date"], errors="coerce").dt.date >= f_from]
        if f_to:
            fdf = fdf[pd.to_datetime(fdf["Date"], errors="coerce").dt.date <= f_to]
    
        st.caption(f"📊 Showing {len(fdf)} / {len(df)} records for **{loc.name}**")
        st.markdown("### Out-Turn Report (OTR)")
        
        columns_2dec = [
            "Dip (cm)", "Total Volume (bbl)", "Water (cm)", "Free Water (bbl)",
            "GOV (bbl)", "API @ 60°F", "GSV (bbl)", "BS&W Vol (bbl)",
            "NSV (bbl)", "LT", "MT", "Net Rece/Disp (bbls)", "Net Water Rece/Disp (bbls)"
        ]
        column_5dec = "VCF"
    
        # Cast/round numeric columns for display
        for col in columns_2dec:
            if col in fdf.columns:
                fdf[col] = pd.to_numeric(fdf[col], errors="coerce").round(2)
        if column_5dec in fdf.columns:
            fdf[column_5dec] = pd.to_numeric(fdf[column_5dec], errors="coerce").round(5)
        
        # --- Chronological sort (global for display) ---
        # Keep your current UI sort; net calcs will use per-tank DT order below.
        fdf = fdf.sort_values(["Date", "Time"], ascending=[True, True]).reset_index(drop=True)
    
        # ---------------- Tank-aware net calculations for OTR ----------------
        import numpy as np
    
        # Ensure source numeric columns exist
        if "NSV (bbl)" not in fdf.columns:
            fdf["NSV (bbl)"] = 0.0
        if "Free Water (bbl)" not in fdf.columns:
            fdf["Free Water (bbl)"] = 0.0
    
        # Cast numerics safely
        fdf["NSV (bbl)"] = pd.to_numeric(fdf["NSV (bbl)"], errors="coerce").fillna(0.0)
        fdf["Free Water (bbl)"] = pd.to_numeric(fdf["Free Water (bbl)"], errors="coerce").fillna(0.0)
    
        # Sort by Tank + DT so "previous" truly means previous entry of the same tank
        # If DT is NaT, use index order to keep stability
        sort_key = pd.Series(pd.to_datetime(fdf["DT"], errors="coerce"))
        orig_index = fdf.index
        fdf_sorted = fdf.assign(__sort_time=sort_key).sort_values(
            by=["Tank", "__sort_time", orig_index.name or "__sort_time"],
            kind="mergesort"
        )
    
        # Per-tank previous values using groupby+shift
        prev_nsv = fdf_sorted.groupby("Tank")["NSV (bbl)"].shift(1)
        prev_fw  = fdf_sorted.groupby("Tank")["Free Water (bbl)"].shift(1)
    
        # Deltas = current - previous
        net_nsv  = fdf_sorted["NSV (bbl)"] - prev_nsv
        net_fw   = fdf_sorted["Free Water (bbl)"] - prev_fw
    
        # First entries per tank ? blank (keep your �Opening/blank� behavior)
        first_of_tank_mask = prev_nsv.isna()
        net_nsv  = net_nsv.mask(first_of_tank_mask, np.nan)
        net_fw   = net_fw.mask(first_of_tank_mask, np.nan)
    
        # Align back to original order
        net_nsv_aligned = pd.Series(net_nsv.values, index=fdf_sorted.index).reindex(orig_index)
        net_fw_aligned  = pd.Series(net_fw.values,  index=fdf_sorted.index).reindex(orig_index)
    
        # Write columns; blank string for first-of-tank rows, else float (2dp formatting already set above)
        def _fmt_net(s):
            return s.where(~s.isna(), "")
    
        fdf["Net Rece/Disp (bbls)"]       = _fmt_net(pd.to_numeric(net_nsv_aligned, errors="coerce").round(2))
        fdf["Net Water Rece/Disp (bbls)"] = _fmt_net(pd.to_numeric(net_fw_aligned,  errors="coerce").round(2))
    
        # Cleanup helper column
        if "__sort_time" in fdf.columns:
            fdf.drop(columns="__sort_time", inplace=True, errors="ignore")
    
        # --- Display ---
        st.dataframe(fdf, use_container_width=True, hide_index=True)
        
        # --- PDF Generation Function ---
        def generate_otr_pdf(dataframe, selected_tank, filter_text, location_name, location_code):
            from reportlab.lib.pagesizes import A4, landscape
            from reportlab.pdfgen import canvas
            from reportlab.lib.units import cm, mm
            from reportlab.lib import colors
            from reportlab.lib.utils import ImageReader
            from io import BytesIO
            from pathlib import Path
    
            buf = BytesIO()
            c = canvas.Canvas(buf, pagesize=landscape(A4))
            width, height = landscape(A4)
    
            # Margins
            margin = 0.5 * cm
            usable_width = width - 2 * margin
            usable_height = height - 2 * margin
    
            # -- Company Logo (top left) --
            logo_path = Path("assets/logo.png")
            logo_x = margin
            logo_y = height - margin - 14*mm
            logo_w = 28 * mm
            logo_h = 18 * mm
            if logo_path.exists():
                img = ImageReader(str(logo_path))
                c.drawImage(img, logo_x, logo_y, width=logo_w, height=logo_h, preserveAspectRatio=True, mask='auto')
    
            # -- Title (centered) --
            title_y = height - margin - 3*mm
            c.setFont("Helvetica-Bold", 22)
            c.drawCentredString(width/2, title_y, "Out-Turn Report")
    
            # -- Location subtitle --
            subtitle_y = title_y - 7*mm
            c.setFont("Helvetica-Bold", 12)
            c.drawCentredString(width/2, subtitle_y, f"{location_name} ({location_code})")
            
            # -- Filter subtitle, just below location --
            filter_y = subtitle_y - 6*mm
            c.setFont("Helvetica", 10)
            c.drawCentredString(width/2, filter_y, f"Filter: {filter_text}")
            
            # Clean dataframe for PDF (drop helper/enum/ids)
            df_pdf = dataframe.copy()
            for drop_col in ["Operation Enum", "Ticket ID", "DT"]:
                if drop_col in df_pdf.columns:
                    df_pdf = df_pdf.drop(columns=[drop_col])
            
            # -- Table layout --
            cols = list(df_pdf.columns)
            n_cols = len(cols)
            base_col_width = (usable_width) / (n_cols + 1.0)
            col_widths = []
            for idx in range(n_cols):
                if idx == 0 or idx == n_cols - 1:
                    col_widths.append(base_col_width * 1.5)
                else:
                    col_widths.append(base_col_width)
            total_width = sum(col_widths)
            table_left = margin
            table_top = filter_y - 8*mm  # Table after logo/title/location/filter
            row_height = 9*mm
            max_rows = min(len(df_pdf), 22)
    
            # -- Table headers (bold, wrap if needed) --
            c.setFont("Helvetica-Bold", 6.5)
            for i, col in enumerate(cols):
                header_text = str(col)
                x_center = table_left + sum(col_widths[:i]) + col_widths[i]/2
                y_header = table_top
                if len(header_text) > 10 and " " in header_text:
                    parts = header_text.split(" ")
                    first_line = " ".join(parts[:len(parts)//2])
                    second_line = " ".join(parts[len(parts)//2:])
                    c.drawCentredString(x_center, y_header-12, first_line)
                    c.drawCentredString(x_center, y_header-19, second_line)
                else:
                    c.drawCentredString(x_center, y_header-12, header_text)
    
            # --- Table Borders: horizontal line below header ---
            c.setLineWidth(1)
            header_line_y = table_top - row_height
            c.line(table_left, header_line_y, table_left + total_width, header_line_y)
    
            # --- Table Rows/Content ---
            c.setFont("Helvetica", 7)
            for r in range(max_rows):
                y = header_line_y - (r+1)*row_height
                for i, col in enumerate(cols):
                    txt = str(df_pdf.iloc[r][col])
                    x_center = table_left + sum(col_widths[:i]) + col_widths[i]/2
    
                    if col == "Operation":
                        # Wrap text to fit cell width
                        wrapped_lines = []
                        max_chars_per_line = int(col_widths[i] // 5)
                        while txt:
                            if len(txt) <= max_chars_per_line:
                                wrapped_lines.append(txt)
                                break
                            else:
                                split_idx = txt.rfind(" ", 0, max_chars_per_line)
                                if split_idx == -1:
                                    split_idx = max_chars_per_line
                                wrapped_lines.append(txt[:split_idx])
                                txt = txt[split_idx:].lstrip()
                        for j, line in enumerate(wrapped_lines):
                            c.drawCentredString(x_center, y + row_height/2 - 2 - (j*8), line)
                    else:
                        max_chars = int(col_widths[i] // 3.2)
                        if len(txt) > max_chars:
                            txt = txt[:max_chars-3] + "..."
                        c.drawCentredString(x_center, y + row_height/2 - 2, txt)
    
            # --- Draw Table Cell Borders ---
            from reportlab.lib import colors as _colors
            c.setStrokeColor(_colors.black)
            x = table_left
            for w in col_widths:
                c.line(x, table_top, x, (header_line_y - (max_rows+1)*row_height)+25)
                x += w
            c.line(x, table_top, x, (header_line_y - (max_rows+1)*row_height)+25)  # last border
            for r in range(max_rows+2):
                y = table_top - r*row_height
                c.line(table_left, y, table_left + total_width, y)
    
            c.save()
            return buf.getvalue()
    
        # --- Export controls (CSV / XLSX / PDF) ---
        st.markdown("---")
        st.markdown("#### Export Options")
        
        ec1, ec2, ec3 = st.columns([0.25, 0.25, 0.5])
    
        # CSV / XLSX using a single button with option
        with ec1:
            fmt = st.selectbox("Download as", ["CSV", "XLSX"], index=0, key="otr_dl_fmt")
            if fmt == "CSV":
                data_bytes = fdf.to_csv(index=False).encode("utf-8")
                filename = f"OTR_{loc.code}_{f_from}_{f_to}.csv"
                st.download_button("Download", data=data_bytes, file_name=filename, mime="text/csv", key="otr_dl_csv")
            else:
                from io import BytesIO
                bio = BytesIO()
                with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
                    fdf.to_excel(writer, sheet_name="OTR", index=False)
                filename = f"OTR_{loc.code}_{f_from}_{f_to}.xlsx"
                st.download_button("Download", data=bio.getvalue(), file_name=filename,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="otr_dl_xlsx")
    
        # PDF export respects filters
        from io import BytesIO
        import base64
        import streamlit.components.v1 as components
    
        with ec2:
            selected_tank = f_tank if f_tank and f_tank != "(All Tanks)" else "All Tanks"
            filter_text_parts = []
            filter_text_parts.append(f"Tank: {selected_tank}")
            if f_op and f_op != "(All)": 
                filter_text_parts.append(f"Operation: {f_op}")
            if f_from: 
                filter_text_parts.append(f"From: {f_from}")
            if f_to: 
                filter_text_parts.append(f"To: {f_to}")
            if f_ticket: 
                filter_text_parts.append(f"Ticket: {f_ticket}")
            filter_text = ", ".join(filter_text_parts) if filter_text_parts else "No filters applied"
    
            if st.button("Download PDF", key="otr_pdf_dl"):
                pdf_bytes = generate_otr_pdf(fdf, selected_tank, filter_text, loc.name, loc.code)
                filename = f"OTR_{loc.code}_{f_from}_{f_to}.pdf"
                st.download_button("Download PDF", data=pdf_bytes, file_name=filename,
                                mime="application/pdf", key="otr_pdf_dl_real")
            
            if st.button("View PDF", key="otr_pdf_view"):
                pdf_bytes = generate_otr_pdf(fdf, selected_tank, filter_text, loc.name, loc.code)
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
        
        # --- Summary Statistics ---
        st.markdown("---")
        st.markdown("#### Summary Statistics")
        
        sum_col1, sum_col2, sum_col3, sum_col4, sum_col5 = st.columns(5)
        
        sum_col1.metric("Total Records", len(fdf))
        sum_col2.metric("Total GOV (bbl)", f"{pd.to_numeric(fdf['GOV (bbl)'], errors='coerce').sum():,.2f}")
        sum_col3.metric("Total GSV (bbl)", f"{pd.to_numeric(fdf['GSV (bbl)'], errors='coerce').sum():,.2f}")
        sum_col4.metric("Total NSV (bbl)", f"{pd.to_numeric(fdf['NSV (bbl)'], errors='coerce').sum():,.2f}")
        sum_col5.metric("Avg API @ 60°F", f"{pd.to_numeric(fdf['API @ 60°F'], errors='coerce').mean():.2f}")
    
    # BCCR page