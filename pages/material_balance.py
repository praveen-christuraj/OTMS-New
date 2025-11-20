"""
Auto-generated module for the 'Material Balance' page.
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
        header("Material Balance")
        
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
                if _cfg.get("page_access", {}).get("Material Balance") is False:
                    st.error("⚠️ Material Balance page is disabled for this location.")
                    st.stop()
        except Exception:
            pass
        
        from datetime import datetime, date, time, timedelta
        from io import BytesIO
        import pandas as pd
        import base64
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors
        from reportlab.lib.units import cm, mm
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_CENTER
        from reportlab.lib.utils import ImageReader
        from pathlib import Path
        import streamlit.components.v1 as components
        
        # Import material balance modules
        from material_balance_config import MaterialBalanceConfig
        from material_balance_calculator import MaterialBalanceCalculator
        
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
    
        # Get location details
        with get_session() as s:
            from location_manager import LocationManager
            from models import Location
            
            loc = LocationManager.get_location_by_id(s, active_location_id)
            if not loc:
                st.error("? Location not found.")
                st.stop()
            
            location_code = loc.code
            location_name = loc.name
            
            st.info(f"📍 **Active Location:** {location_name} ({location_code})")
        
        st.markdown("### 📊 Material Balance Report")
        st.caption("Auto-calculated from OTR records (06:01 - 06:00)")
        
        # Check if material balance is configured for this location
        config = MaterialBalanceConfig.get_config(location_code)
        
        if not config:
            st.warning(f"⚠️ Material Balance is not configured for {location_name}")
            st.info("Please contact administrator to configure material balance for this location.")
            
            with st.expander("🐞 Debug Info"):
                st.write(f"Location Code: '{location_code}'")
                st.write(f"Available Configs: {list(MaterialBalanceConfig.LOCATION_COLUMNS.keys())}")
            
            st.stop()
        
        # Display location-specific configuration
        st.success(f"Material Balance configured for {config['name']}")
        
        # ============ FILTERS ============
        st.markdown("#### Filters")
        
        filter_col1, filter_col2, filter_col3 = st.columns([1, 1, 1])
        
        otr_first_date = None
        with get_session() as s_otr_min:
            try:
                otr_first_date = (
                    s_otr_min.query(func.min(OTRRecord.date))
                    .filter(OTRRecord.location_id == active_location_id)
                    .scalar()
                )
            except Exception:
                otr_first_date = None
        default_mb_from = otr_first_date or (date.today() - timedelta(days=7))
    
        with filter_col1:
            mb_date_from = st.date_input(
                "From Date",
                value=st.session_state.get("mb_from", default_mb_from),
                min_value=otr_first_date or default_mb_from,
                key="mb_from"
            )
    
        with filter_col2:
            mb_date_to = st.date_input(
                "To Date",
                value=date.today(),
                key="mb_to"
            )
        
        with filter_col3:
            # Tank filter (optional)
            with get_session() as s:
                from models import Tank
                tanks_all = s.query(Tank).filter(
                    Tank.location_id == active_location_id
                ).order_by(Tank.name).all()
            
            tank_opts = ["All Tanks"] + [t.name for t in tanks_all]
            f_tank = st.selectbox("Tank", tank_opts, index=0, key="mb_tank")
        
        st.markdown("---")
        # Determine earliest OTR date for this location (for proper opening stock continuity)
        earliest_otr_date = None
        with get_session() as s:
            try:
                earliest_otr_date = s.query(func.min(OTRRecord.date)).filter(OTRRecord.location_id == active_location_id).scalar()
            except Exception:
                earliest_otr_date = None
    
        # ============ CALCULATE MATERIAL BALANCE ============
        try:
            # ? Calculate material balance using OTR data (location_id based)
            calc_from = mb_date_from
            if earliest_otr_date:
                if earliest_otr_date < calc_from:
                    calc_from = earliest_otr_date
            mb_data = MaterialBalanceCalculator.calculate_material_balance(
                entries=None,                         # let the calculator fetch from DB
                location_code=location_code,
                date_from=calc_from,
                date_to=mb_date_to,
                location_id=active_location_id        # <-- important
            )
    
            
            if not mb_data:
                st.info("ℹ️ No data available for material balance calculation.")
                st.info("ℹ️ Add transactions in Tank Transactions to see material balance here.")
            else:
    
                # Create DataFrame
    
                mb_df = pd.DataFrame(mb_data)
    
    
    
                if mb_df.empty:
    
                    st.info("dY\"- No data available for material balance calculation.")
    
                    st.info("dY'? Add transactions in Tank Transactions to see material balance here.")
    
                    st.stop()
    
    
    
                mb_df["Date"] = pd.to_datetime(mb_df["Date"], errors="coerce")
    
                mb_df = mb_df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    
    
    
                if mb_df.empty:
    
                    st.info("dY\"- No data available for material balance calculation.")
    
                    st.stop()
    
    
    
                mb_df_full = mb_df.copy()
                prev_rows = mb_df_full[mb_df_full["Date"].dt.date < mb_date_from]
    
                def _anchor_value(series, default=0.0):
                    try:
                        raw = series
                        if isinstance(raw, str):
                            raw = raw.replace(",", "")
                        return float(raw if raw not in (None, "") else default)
                    except Exception:
                        try:
                            return float(raw)
                        except Exception:
                            return float(default)
    
                if not prev_rows.empty and "Closing Stock" in prev_rows.columns:
                    opening_anchor = _anchor_value(prev_rows["Closing Stock"].iloc[-1])
                elif "Opening Stock" in mb_df_full.columns:
                    opening_anchor = _anchor_value(mb_df_full["Opening Stock"].iloc[0])
                else:
                    opening_anchor = 0.0
    
                if "Closing Stock" in mb_df_full.columns:
                    closing_anchor = _anchor_value(mb_df_full["Closing Stock"].iloc[-1])
                else:
                    closing_anchor = 0.0
    
                view_mask = (
                    (mb_df_full["Date"].dt.date >= mb_date_from) &
                    (mb_df_full["Date"].dt.date <= mb_date_to)
                )
    
                mb_df = mb_df_full.loc[view_mask].copy()
    
    
    
                if mb_df.empty:
    
                    st.info("dY\"- No material balance rows within the selected filter range.")
    
                    st.stop()
    
    
    
                mb_df["Date"] = mb_df["Date"].dt.strftime("%Y-%m-%d")
    
                total_days = len(mb_df)
    
                st.markdown(f"#### 📊 Material Balance - {config['name']} ({total_days} days)")
                st.caption("? Auto-calculated from OTR records")
                
                # ============ SUMMARY STATISTICS ============
                st.markdown("---")
                st.markdown("#### 📊 Summary")
                
                # Calculate totals based on location-specific columns
                total_opening = opening_anchor
                total_closing = closing_anchor
                
                # Calculate total receipts (sum of all receipt columns)
                receipt_columns = [col for col in mb_df.columns if 'Receipt' in col and col not in ['Book Closing Stock']]
                total_receipts = sum(mb_df[col].sum() for col in receipt_columns)
                
                # Calculate total dispatches (sum of all dispatch columns)
                dispatch_columns = [col for col in mb_df.columns if 'Dispatch' in col or 'dispatch' in col]
                total_dispatches = sum(mb_df[col].sum() for col in dispatch_columns)
                
                # Total loss/gain
                total_loss_gain = mb_df["Loss/Gain"].sum() if "Loss/Gain" in mb_df.columns else 0.0
                
                # Calculate loss/gain percentage
                loss_gain_pct = (total_loss_gain / total_receipts * 100.0) if total_receipts > 0 else 0.0
                
                sum_col1, sum_col2, sum_col3, sum_col4, sum_col5 = st.columns(5)
                
                with sum_col1:
                    st.metric("Opening Stock", f"{total_opening:,.0f} bbls")
                
                with sum_col2:
                    st.metric("Total Receipts", f"{total_receipts:,.0f} bbls")
                
                with sum_col3:
                    st.metric("Total Dispatches", f"{total_dispatches:,.0f} bbls")
                
                with sum_col4:
                    st.metric("Closing Stock", f"{total_closing:,.0f} bbls")
                
                with sum_col5:
                    st.metric(
                        "Total Loss/Gain",
                        f"{total_loss_gain:,.2f} bbls",
                        delta=f"{loss_gain_pct:.2f}%",
                        delta_color="normal" if total_loss_gain >= 0 else "inverse"
                    )
    
                st.caption("Opening/Closing stocks always reflect the full data range; other metrics follow the active filters.")
                
                st.markdown("---")
                
                # ============ DATA TABLE WITH TOTALS ============
                # Calculate totals row
                totals_row = {"Date": "TOTAL"}
                
                for col in mb_df.columns:
                    if col == "Date":
                        continue
                    elif col in ["Opening Stock", "Closing Stock", "Book Closing Stock"]:
                        totals_row[col] = ""  # Don't sum these
                    elif col == "Loss/Gain":
                        totals_row[col] = round(mb_df[col].sum(), 2)
                    else:
                        totals_row[col] = round(mb_df[col].sum(), 2)
                
                # Add totals row to dataframe
                mb_df_with_totals = pd.concat([mb_df, pd.DataFrame([totals_row])], ignore_index=True)
                
                # Display table
                st.dataframe(
                    mb_df_with_totals,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        col: st.column_config.NumberColumn(
                            col,
                            format="%.2f" if col != "Date" else None
                        ) for col in mb_df_with_totals.columns
                    }
                )
                
                # ============ EXPORT OPTIONS ============
                st.markdown("---")
                st.markdown("#### 📤 Export Options")
                
                ec1, ec2, ec3 = st.columns(3)
                
                # CSV/Excel Download
                with ec1:
                    fmt = st.selectbox("Download as", ["CSV", "XLSX"], index=0, key="mb_dl_fmt")
                    
                    if fmt == "CSV":
                        data_bytes = mb_df.to_csv(index=False).encode("utf-8")
                        st.download_button(
                            "⬇️ Download",
                            data=data_bytes,
                            file_name=f"MaterialBalance_{location_code}_{mb_date_from}_{mb_date_to}.csv",
                            mime="text/csv",
                            key="mb_dl_csv",
                            use_container_width=True
                        )
                    else:
                        bio = BytesIO()
                        with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
                            mb_df.to_excel(writer, sheet_name="MaterialBalance", index=False)
                        st.download_button(
                            "⬇️ Download",
                            data=bio.getvalue(),
                            file_name=f"MaterialBalance_{location_code}_{mb_date_from}_{mb_date_to}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="mb_dl_xlsx",
                            use_container_width=True
                        )
                
                # PDF Generation Function
                def generate_mb_pdf(dataframe, df_with_totals, filter_info, location_name, username):
                    """Generate professional Material Balance PDF report"""
                    buffer = BytesIO()
                    
                    # Create document with 0.5cm margins
                    doc = SimpleDocTemplate(
                        buffer, 
                        pagesize=landscape(A4),
                        leftMargin=0.5*cm,
                        rightMargin=0.5*cm,
                        topMargin=0.5*cm,
                        bottomMargin=0.5*cm
                    )
                    
                    elements = []
                    styles = getSampleStyleSheet()
                    
                    # Custom styles
                    title_style = ParagraphStyle(
                        'CustomTitle',
                        parent=styles['Heading1'],
                        fontSize=16,
                        textColor=colors.HexColor('#1f4788'),
                        spaceAfter=8,
                        alignment=TA_CENTER,
                        fontName='Helvetica-Bold'
                    )
                    
                    subtitle_style = ParagraphStyle(
                        'CustomSubtitle',
                        parent=styles['Normal'],
                        fontSize=10,
                        textColor=colors.HexColor('#666666'),
                        spaceAfter=6,
                        alignment=TA_CENTER
                    )
                    
                    # Title
                    title = Paragraph(f"<b>MATERIAL BALANCE REPORT</b><br/><font size=14>{location_name}</font>", title_style)
                    elements.append(title)
                    elements.append(Spacer(1, 0.3*cm))
                    
                    # Subtitle
                    subtitle = Paragraph(f"{filter_info}<br/>Generated: {datetime.now().strftime('%d-%b-%Y %H:%M')}", subtitle_style)
                    elements.append(subtitle)
                    elements.append(Spacer(1, 0.4*cm))
                    
                    # Calculate available width
                    page_width = landscape(A4)[0] - (1.0*cm)
                    table_width = page_width
                    
                    # Dynamic column widths based on number of columns
                    num_cols = len(df_with_totals.columns)
                    col_widths = [table_width / num_cols for _ in range(num_cols)]
                    
                    # Custom paragraph style for centered headers
                    header_style = ParagraphStyle(
                        'HeaderStyle',
                        parent=styles['Normal'],
                        fontSize=7,
                        leading=9,
                        alignment=TA_CENTER,
                        fontName='Helvetica-Bold'
                    )
                    
                    # Table headers
                    table_data = [[
                        Paragraph(f"<b><font color='white'>{col}</font></b>", header_style)
                        for col in df_with_totals.columns
                    ]]
                    
                    # Custom cell style
                    cell_style = ParagraphStyle(
                        'CellStyle',
                        parent=styles['Normal'],
                        fontSize=6,
                        leading=8,
                        alignment=TA_CENTER
                    )
                    
                    # Add data rows
                    for idx, row in df_with_totals.iterrows():
                        row_cells = []
                        for col in df_with_totals.columns:
                            val = row[col]
                            
                            # Format value
                            if col == "Date":
                                cell_text = str(val)
                            elif col == "Loss/Gain" and val != "" and val != "TOTAL":
                                # Color code loss/gain
                                try:
                                    numeric_val = float(val)
                                    color = '#28a745' if numeric_val >= 0 else '#dc3545'
                                    cell_text = f"<font color='{color}'><b>{numeric_val:,.2f}</b></font>"
                                except:
                                    cell_text = str(val)
                            elif val == "" or val == "TOTAL":
                                cell_text = str(val)
                            else:
                                try:
                                    cell_text = f"{float(val):,.2f}"
                                except:
                                    cell_text = str(val)
                            
                            # Make totals row bold
                            if idx == len(df_with_totals) - 1 and val != "":
                                cell_text = f"<b>{cell_text}</b>"
                            
                            row_cells.append(Paragraph(cell_text, cell_style))
                        
                        table_data.append(row_cells)
                    
                    # Create table
                    table = Table(table_data, colWidths=col_widths, repeatRows=1)
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f4788')),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 7),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 5),
                        ('TOPPADDING', (0, 0), (-1, 0), 5),
                        ('BACKGROUND', (0, 1), (-1, -2), colors.HexColor('#f8f9fa')),
                        ('ROWBACKGROUNDS', (0, 1), (-1, -2), [colors.white, colors.HexColor('#f8f9fa')]),
                        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#e9ecef')),  # Totals row
                        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                        ('FONTSIZE', (0, 1), (-1, -1), 6),
                        ('TEXTCOLOR', (0, 1), (-1, -1), colors.HexColor('#333333')),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                        ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#1f4788')),
                        ('LEFTPADDING', (0, 0), (-1, -1), 2),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 2),
                        ('TOPPADDING', (0, 1), (-1, -1), 3),
                        ('BOTTOMPADDING', (0, 1), (-1, -1), 3),
                    ]))
                    
                    elements.append(table)
                    
                    # Footer
                    elements.append(Spacer(1, 0.3*cm))
                    footer_text = f"<font size=7 color='#666666'>Generated by: {username} | OTMS - Oil Terminal Management System | {datetime.now().strftime('%d-%b-%Y %H:%M:%S')}</font>"
                    elements.append(Paragraph(footer_text, subtitle_style))
                    
                    doc.build(elements)
                    pdf_data = buffer.getvalue()
                    buffer.close()
                    
                    return pdf_data
                
                # Filter info for PDF
                filter_info = f"Tank: {f_tank} | Period: {mb_date_from} to {mb_date_to}"
                
                # PDF Download
                with ec2:
                    if st.button("📥 Download PDF", key="mb_pdf_dl", use_container_width=True):
                        pdf_bytes = generate_mb_pdf(
                            mb_df, 
                            mb_df_with_totals, 
                            filter_info, 
                            location_name,
                            user['username']
                        )
                        st.download_button(
                            "💾 Save PDF",
                            data=pdf_bytes,
                            file_name=f"MaterialBalance_{location_code}_{mb_date_from}_{mb_date_to}.pdf",
                            mime="application/pdf",
                            key="mb_pdf_dl_real",
                            use_container_width=True
                        )
    
                # PDF View
                with ec3:
                    if st.button("👁️ View PDF", key="mb_pdf_view_mb", use_container_width=True):
                        pdf_bytes = generate_mb_pdf(
                            mb_df, 
                            mb_df_with_totals, 
                            filter_info, 
                            location_name,
                            user['username']
                        )
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
                        st.success("? PDF opened in new tab!")
        
        except Exception as ex:
            st.error(f"? Failed to calculate material balance: {ex}")
            import traceback
            with st.expander("⚠️ Error Details"):
                st.code(traceback.format_exc())
                
    # =================================== ADD ASSET - admin only =============================