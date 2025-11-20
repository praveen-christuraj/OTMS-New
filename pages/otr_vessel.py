"""
Auto-generated module for the 'OTR-Vessel' page.
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
        header("OTR-Vessel Operations")
        
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
                if _cfg.get("page_access", {}).get("OTR-Vessel") is False:
                    st.error("⚠️ OTR-Vessel page is disabled for this location.")
                    st.stop()
        except Exception:
            pass
        
        from datetime import datetime, date, timedelta
        import pandas as pd
        import re
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors
        from reportlab.lib.units import cm
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_CENTER
        from io import BytesIO
        import base64
        
        # ============ PDF GENERATION FUNCTION ============
        def generate_otr_vessel_pdf(df, date_from, date_to, total_receipts, total_dispatches, total_water_in, total_water_out, username, location_name):
            """Generate OTR-Vessel PDF report"""
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
            title = Paragraph(f"<b>OTR-VESSEL REPORT</b><br/><font size=14>{location_name}</font>", title_style)
            elements.append(title)
            elements.append(Spacer(1, 0.3*cm))
            
            # Subtitle
            subtitle = Paragraph(f"Period: <b>{date_from}</b> to <b>{date_to}</b><br/>Generated: {datetime.now().strftime('%d-%b-%Y %H:%M')}", subtitle_style)
            elements.append(subtitle)
            elements.append(Spacer(1, 0.4*cm))
            
            # Calculate available width
            page_width = landscape(A4)[0] - (1.0*cm)
            table_width = page_width
            
            # Column widths (12 columns)
            col_widths = [
                table_width * 0.08,   # Date
                table_width * 0.05,   # Time
                table_width * 0.08,   # Shuttle
                table_width * 0.12,   # Vessel
                table_width * 0.10,   # Operation
                table_width * 0.08,   # Opening Stock
                table_width * 0.07,   # Opening Water
                table_width * 0.08,   # Closing Stock
                table_width * 0.07,   # Closing Water
                table_width * 0.08,   # Net R/D
                table_width * 0.07,   # Net Water
                table_width * 0.12    # Remarks
            ]
            
            # Custom paragraph style for centered headers
            header_style = ParagraphStyle(
                'HeaderStyle',
                parent=styles['Normal'],
                fontSize=8,
                leading=9,
                alignment=TA_CENTER,
                fontName='Helvetica-Bold'
            )
            
            # Table headers
            table_data = [[
                Paragraph("<b><font color='white'>Date</font></b>", header_style),
                Paragraph("<b><font color='white'>Time</font></b>", header_style),
                Paragraph("<b><font color='white'>Shuttle<br/>No</font></b>", header_style),
                Paragraph("<b><font color='white'>Vessel<br/>Name</font></b>", header_style),
                Paragraph("<b><font color='white'>Operation</font></b>", header_style),
                Paragraph("<b><font color='white'>Opening<br/>Stock</font></b>", header_style),
                Paragraph("<b><font color='white'>Opening<br/>Water</font></b>", header_style),
                Paragraph("<b><font color='white'>Closing<br/>Stock</font></b>", header_style),
                Paragraph("<b><font color='white'>Closing<br/>Water</font></b>", header_style),
                Paragraph("<b><font color='white'>Net<br/>R/D</font></b>", header_style),
                Paragraph("<b><font color='white'>Net<br/>Water</font></b>", header_style),
                Paragraph("<b><font color='white'>Remarks</font></b>", header_style)
            ]]
            
            # Custom cell style
            cell_style = ParagraphStyle(
                'CellStyle',
                parent=styles['Normal'],
                fontSize=7,
                leading=8,
                alignment=TA_CENTER
            )
            
            # Add data rows
            for _, row in df.iterrows():
                table_data.append([
                    Paragraph(row['Date'], cell_style),
                    Paragraph(row['Time'], cell_style),
                    Paragraph(str(row['Shuttle No']), cell_style),
                    Paragraph(str(row['Vessel Name'])[:30], cell_style),
                    Paragraph(str(row['Operation'])[:25], cell_style),
                    Paragraph(f"{row['Opening Stock']:,.0f}", cell_style),
                    Paragraph(f"{row['Opening Water']:,.0f}", cell_style),
                    Paragraph(f"{row['Closing Stock']:,.0f}", cell_style),
                    Paragraph(f"{row['Closing Water']:,.0f}", cell_style),
                    Paragraph(f"<font color='{'#28a745' if row['Net R/D'] >= 0 else '#dc3545'}'>{row['Net R/D']:,.0f}</font>", header_style),
                    Paragraph(f"{row['Net Water']:,.0f}", cell_style),
                    Paragraph(str(row['Remarks'])[:60] if row['Remarks'] else "-", cell_style)
                ])
            
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
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
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
            
            # Summary section
            elements.append(Spacer(1, 0.4*cm))
            
            summary_style = ParagraphStyle(
                'Summary',
                parent=styles['Normal'],
                fontSize=8,
                textColor=colors.HexColor('#333333'),
                spaceAfter=3
            )
            
            summary_header_style = ParagraphStyle(
                'SummaryHeader',
                parent=styles['Normal'],
                fontSize=9,
                textColor=colors.white,
                fontName='Helvetica-Bold',
                alignment=TA_CENTER
            )
            
            summary_data = [
                [
                    Paragraph("<b>SUMMARY STATISTICS</b>", summary_header_style),
                    "",
                    "",
                    ""
                ],
                [
                    Paragraph(f"<b>Total Receipts:</b> {total_receipts:,.2f} bbls", summary_style),
                    Paragraph(f"<b>Total Dispatches:</b> {total_dispatches:,.2f} bbls", summary_style),
                    Paragraph(f"<b>Water In:</b> {total_water_in:,.2f} bbls", summary_style),
                    Paragraph(f"<b>Water Out:</b> {total_water_out:,.2f} bbls", summary_style)
                ],
                [
                    Paragraph(f"<b>Net Movement:</b> {(total_receipts - total_dispatches):,.2f} bbls", summary_style),
                    Paragraph(f"<b>Net Water:</b> {(total_water_in - total_water_out):,.2f} bbls", summary_style),
                    "",
                    Paragraph(f"<b>Total Entries:</b> {len(df)}", summary_style)
                ]
            ]
            
            summary_col_width = table_width / 4
            summary_table = Table(summary_data, colWidths=[summary_col_width] * 4)
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f4788')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('SPAN', (0, 0), (-1, 0)),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('BOX', (0, 0), (-1, -1), 1, colors.HexColor('#1f4788')),
                ('LEFTPADDING', (0, 0), (-1, -1), 5),
                ('RIGHTPADDING', (0, 0), (-1, -1), 5),
                ('TOPPADDING', (0, 0), (-1, -1), 4),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ]))
            
            elements.append(summary_table)
            
            # Footer
            elements.append(Spacer(1, 0.3*cm))
            footer_text = f"<font size=7 color='#666666'>Generated by: {username} | OTMS - Oil Terminal Management System | {datetime.now().strftime('%d-%b-%Y %H:%M:%S')}</font>"
            elements.append(Paragraph(footer_text, subtitle_style))
            
            doc.build(elements)
            pdf_data = buffer.getvalue()
            buffer.close()
            
            return pdf_data
        
        # ============ MAIN CODE ============
        
        user = st.session_state.get("auth_user")
        
        if not user:
            st.error("Please login to access this page")
            st.stop()
        
        # ========== GET LOCATION ID ==========
        active_location_id = st.session_state.get("active_location_id")
        if not active_location_id:
            st.error("No active location selected")
            st.stop()
        
        # ========== CHECK PERMISSIONS ==========
        from permission_manager import PermissionManager
        
        preferred_vessel_ids: List[int] = []
    
        with get_session() as s:
            from location_manager import LocationManager
            from models import Location
            from location_config import LocationConfig
            
            # Get location info
            location = s.query(Location).filter(Location.id == active_location_id).first()
            if not location:
                st.error("? Location not found.")
                st.stop()
            
            location_name = location.name
            st.info(f"📍 **Active Location:** {location.name} ({location.code})")
            
            # Check if feature is allowed at this location
            if not PermissionManager.can_access_feature(s, active_location_id, "otr_vessel", user["role"]):
                st.error("🚫 **Access Denied**")
                st.warning(f"**OTR-Vessel** is not available at **{location.name}**")
                
                allowed_locs = PermissionManager.get_allowed_locations_for_feature(s, "otr_vessel")
                if allowed_locs:
                    st.info(f"? This feature is available at: **{', '.join(allowed_locs)}**")
                
                st.markdown("---")
                st.caption(f"Current Location: **{location.name} ({location.code})**")
                st.caption("OTR-Vessel Access: **? Denied**")
                st.stop()
            
            # Check permissions
            can_make_entries = PermissionManager.can_make_entries(s, user["role"], active_location_id)
            can_delete_direct = user["role"].lower() in ["admin-operations", "supervisor"]
            can_delete_with_approval = user["role"].lower() == "operator"
            preferred_vessel_ids = (
                LocationConfig.get_config(s, active_location_id)
                .get("otr_vessel", {})
                .get("preferred_vessel_ids", [])
                or []
            )
        
        # ============ VESSEL ENABLED ============
        st.success(f"? OTR-Vessel enabled at {location_name}")
    
        from models import OTRVessel, Vessel, VesselOperation, LocationVessel
    
        st.markdown("### ⛴️ Vessel Operations Tracker")
        st.caption("Direct table entry for vessel operations with live filtering and export")
    
        # ========== LOAD VESSELS & OPERATIONS ==========
        try:
            with get_session() as s:
                # Get vessels assigned to this location
                assigned_vessels_query = s.query(Vessel).join(
                    LocationVessel,
                    LocationVessel.vessel_id == Vessel.id
                ).filter(
                    LocationVessel.location_id == active_location_id,
                    LocationVessel.is_active == True,
                    Vessel.status == "ACTIVE"
                ).all()
    
                if not assigned_vessels_query:
                    st.warning(" No vessels assigned to this location. Use Location Settings > Vessel Assignments.")
                    st.stop()
    
                custom_ids = {int(v_id) for v_id in preferred_vessel_ids if isinstance(v_id, int)}
                if custom_ids:
                    assigned_vessels_query = [v for v in assigned_vessels_query if v.id in custom_ids]
                    if not assigned_vessels_query:
                        st.warning(" No vessels match the location's preferred list. Update Location Settings to include assigned vessels.")
                        st.stop()
                vessel_options = [(v.id, v.name) for v in assigned_vessels_query]
                vessel_dict = {v.id: v.name for v in assigned_vessels_query}
                
                # Get all active operations
                operations_query = s.query(VesselOperation).filter(
                    VesselOperation.is_active == True
                ).order_by(VesselOperation.operation_name).all()
                
                operation_options = [(op.id, op.operation_name) for op in operations_query]
                operation_dict = {op.id: op.operation_name for op in operations_query}
    
        except Exception as ex:
            st.error(f"? Failed to load vessels/operations: {ex}")
            st.stop()
    
        if not vessel_options:
            st.warning("⚠️ No vessels available. Please add vessels in Asset Management first.")
            st.stop()
        
        if not operation_options:
            st.warning("⚠️ No operations available. Please contact administrator.")
            st.stop()
        
        vessel_id_choices = [v[0] for v in vessel_options]
        operation_id_choices = [op[0] for op in operation_options]
    
        # ========== LIVE FILTERS ==========
        st.markdown("#### 🔎 Filters")
    
        filter_col1, filter_col2, filter_col3, filter_col4, filter_col5 = st.columns(5)
    
        with filter_col1:
            filter_date_from = st.date_input(
                "From Date",
                value=date.today() - timedelta(days=30),
                key="otr_vessel_date_from"
            )
    
        with filter_col2:
            filter_date_to = st.date_input(
                "To Date",
                value=date.today(),
                key="otr_vessel_date_to"
            )
    
        with filter_col3:
            filter_shuttle = st.text_input(
                "Shuttle No",
                placeholder="Filter...",
                key="otr_vessel_filter_shuttle"
            )
    
        with filter_col4:
                filter_vessel_names = [v[1] for v in vessel_options]
                filter_vessel = st.selectbox(
                    "Vessel Name",
                    ["All"] + filter_vessel_names,
                    key="otr_vessel_filter_vessel"
                )
    
        with filter_col5:
            filter_operation_names = [op[1] for op in operation_options]
            filter_operation = st.selectbox(
                "Operation",
                ["All"] + filter_operation_names,
                key="otr_vessel_filter_operation"
            )
    
        st.markdown("---")
    
        # ========== ADD NEW ENTRY FORM WITH WATER COLUMNS ==========
        if can_make_entries:
            with st.expander("? Add New Entry", expanded=False):
                with st.form("add_otr_vessel_form"):
                    st.markdown("##### New Vessel Operation Entry")
                    
                    form_col1, form_col2, form_col3, form_col4 = st.columns(4)
                    
                    with form_col1:
                        entry_date = st.date_input("Date *", value=date.today(), key="otr_vessel_new_date")
                        entry_time = st.text_input(
                            "Time (HH:MM) *",
                            value=datetime.now().strftime("%H:%M"),
                            placeholder="14:30",
                            key="otr_vessel_new_time",
                            max_chars=5
                        )
                        entry_shuttle = st.text_input(
                            "Shuttle No *",
                            placeholder="SH-001",
                            key="otr_vessel_new_shuttle"
                        )
                    
                    with form_col2:
                        entry_vessel_id = st.selectbox(
                            "Vessel Name *",
                            options=vessel_id_choices,
                            format_func=lambda x: vessel_dict.get(x, "Unknown"),
                            key="otr_vessel_new_vessel"
                        )
                        
                        entry_operation_id = st.selectbox(
                            "Operation *",
                            options=operation_id_choices,
                            format_func=lambda x: operation_dict.get(x, "Unknown"),
                            key="otr_vessel_new_operation"
                        )
                    
                    with form_col3:
                        entry_opening = st.number_input(
                            "Opening Stock (bbls) *",
                            min_value=0.0,
                            value=0.0,
                            step=0.01,
                            format="%.2f",
                            key="otr_vessel_new_opening"
                        )
                        entry_opening_water = st.number_input(
                            "Opening Water (bbls)",
                            min_value=0.0,
                            value=0.0,
                            step=0.01,
                            format="%.2f",
                            key="otr_vessel_new_opening_water"
                        )
                        entry_closing = st.number_input(
                            "Closing Stock (bbls) *",
                            min_value=0.0,
                            value=0.0,
                            step=0.01,
                            format="%.2f",
                            key="otr_vessel_new_closing"
                        )
                    
                    with form_col4:
                        entry_closing_water = st.number_input(
                            "Closing Water (bbls)",
                            min_value=0.0,
                            value=0.0,
                            step=0.01,
                            format="%.2f",
                            key="otr_vessel_new_closing_water"
                        )
                    
                    entry_remarks = st.text_area(
                        "Remarks",
                        placeholder="Optional remarks...",
                        key="otr_vessel_new_remarks",
                        max_chars=500
                    )
                    
                    # Calculate
                    net_stock = entry_closing - entry_opening
                    net_water = entry_closing_water - entry_opening_water
                    
                    calc_col1, calc_col2 = st.columns(2)
                    
                    with calc_col1:
                        if net_stock >= 0:
                            st.success(f"**Net Stock:** +{net_stock:,.2f} bbls")
                        else:
                            st.info(f"**Net Stock:** {net_stock:,.2f} bbls")
                    
                    with calc_col2:
                        if net_water >= 0:
                            st.success(f"**Net Water:** +{net_water:,.2f} bbls")
                        else:
                            st.info(f"**Net Water:** {net_water:,.2f} bbls")
                    
                    submit_btn = st.form_submit_button("💾 Save Entry", type="primary", use_container_width=True)
                    
                    if submit_btn:
                        if not entry_shuttle.strip():
                            st.error("? Shuttle No is required")
                        elif not entry_time.strip() or not re.match(r'^\d{2}:\d{2}$', entry_time):
                            st.error("? Invalid time format. Use HH:MM")
                        else:
                            try:
                                with get_session() as s:
                                    new_entry = OTRVessel(
                                        location_id=active_location_id,
                                        date=entry_date,
                                        time=entry_time,
                                        shuttle_no=entry_shuttle.strip(),
                                        vessel_id=entry_vessel_id,
                                        operation_id=entry_operation_id,
                                        opening_stock=entry_opening,
                                        opening_water=entry_opening_water,
                                        closing_stock=entry_closing,
                                        closing_water=entry_closing_water,
                                        net_receipt_dispatch=net_stock,
                                        net_water=net_water,
                                        remarks=entry_remarks.strip() if entry_remarks else None,
                                        created_by=user["username"],
                                    )
    
                                    s.add(new_entry)
                                    # Flush so new_entry.id is available before audit log
                                    s.flush()
                                    new_entry_id = new_entry.id
                                    s.commit()
    
                                # Log audit outside the transaction so we don't reuse the write session
                                from security import SecurityManager
                                vessel_name = vessel_dict.get(entry_vessel_id, "Unknown")
                                operation_name = operation_dict.get(entry_operation_id, "Unknown")
    
                                SecurityManager.log_audit(
                                    None,
                                    user["username"],
                                    "CREATE",
                                    resource_type="OTRVessel",
                                    resource_id=str(new_entry_id),
                                    location_id=active_location_id,
                                    details=f"Added: {vessel_name} - {entry_shuttle} - {operation_name}",
                                    user_id=user["id"],
                                )
    
                                st.success(f"? Entry saved! ID: {new_entry_id}")
                                st.balloons()
                                import time
                                time.sleep(1)
                                _st_safe_rerun()
    
                            except Exception as ex:
                                st.error(f"? Failed to save: {ex}")
                                import traceback
                                with st.expander("⚠️ Error Details"):
                                    st.code(traceback.format_exc())
        else:
            st.info("ℹ️ You don't have permission to add entries")
    
        st.markdown("---")
    
        
        # ========== FETCH AND DISPLAY DATA ==========
        try:
            with get_session() as s:
                # Build query with filters
                query = s.query(OTRVessel).filter(
                    OTRVessel.location_id == active_location_id,
                    OTRVessel.date >= filter_date_from,
                    OTRVessel.date <= filter_date_to
                )
                
                if filter_shuttle:
                    query = query.filter(OTRVessel.shuttle_no.contains(filter_shuttle))
                
                if filter_vessel != "All":
                    vessel_id_for_filter = next((v[0] for v in vessel_options if v[1] == filter_vessel), None)
                    if vessel_id_for_filter:
                        query = query.filter(OTRVessel.vessel_id == vessel_id_for_filter)
                
                if filter_operation != "All":
                    operation_id_for_filter = next((op[0] for op in operation_options if op[1] == filter_operation), None)
                    if operation_id_for_filter:
                        query = query.filter(OTRVessel.operation_id == operation_id_for_filter)
                
                entries = query.order_by(OTRVessel.date.desc(), OTRVessel.time.desc()).all()
                
                if not entries:
                    st.info("ℹ️ No vessel entries found. Add your first entry above!")
                else:
                    st.markdown(f"#### ⛴️ Vessel Operations ({len(entries)} entries)")
                    
                    # Create DataFrame for PDF export
                    export_data = []
                    for entry in entries:
                        vessel_name = vessel_dict.get(entry.vessel_id, "Unknown")
                        operation_name = operation_dict.get(entry.operation_id, "Unknown")
                        
                        export_data.append({
                            "Date": entry.date.strftime("%Y-%m-%d"),
                            "Time": entry.time,
                            "Shuttle No": entry.shuttle_no,
                            "Vessel Name": vessel_name,
                            "Operation": operation_name,
                            "Opening Stock": getattr(entry, 'opening_stock', 0.0),
                            "Opening Water": getattr(entry, 'opening_water', 0.0),
                            "Closing Stock": getattr(entry, 'closing_stock', 0.0),
                            "Closing Water": getattr(entry, 'closing_water', 0.0),
                            "Net R/D": getattr(entry, 'net_receipt_dispatch', 0.0),
                            "Net Water": getattr(entry, 'net_water', 0.0),
                            "Remarks": entry.remarks or "",
                            "Created By": entry.created_by or ""
                        })
                    
                    df_export = pd.DataFrame(export_data)
                    
                    # Display compact table (KEEPING YOUR EXISTING TABLE DISPLAY)
                    for idx, entry in enumerate(entries):
                        vessel_name = vessel_dict.get(entry.vessel_id, "Unknown")
                        operation_name = operation_dict.get(entry.operation_id, "Unknown")
                        
                        with st.container(border=True):
                            cols = st.columns([0.05, 0.10, 0.07, 0.10, 0.14, 0.13, 0.09, 0.09, 0.09, 0.15, 0.11, 0.12])
                            
                            with cols[0]:
                                st.markdown(f"**{entry.id}**")
                            
                            with cols[1]:
                                st.markdown(f"{entry.date.strftime('%Y-%m-%d')}")
                            
                            with cols[2]:
                                st.markdown(f"{entry.time}")
                            
                            with cols[3]:
                                st.markdown(f"{entry.shuttle_no}")
                            
                            with cols[4]:
                                st.markdown(f"{vessel_name}")
                            
                            with cols[5]:
                                st.markdown(f"{operation_name}")
                            
                            with cols[6]:
                                st.markdown(f"{entry.opening_stock:,.0f}")
                            
                            with cols[7]:
                                st.markdown(f"{entry.closing_stock:,.0f}")
                            
                            with cols[8]:
                                if entry.net_receipt_dispatch >= 0:
                                    st.markdown(f"<span style='color: green; font-weight: bold;'>+{entry.net_receipt_dispatch:,.0f}</span>", unsafe_allow_html=True)
                                else:
                                    st.markdown(f"<span style='color: red; font-weight: bold;'>{entry.net_receipt_dispatch:,.0f}</span>", unsafe_allow_html=True)
                            
                            with cols[9]:
                                remarks_text = entry.remarks[:35] + "..." if entry.remarks and len(entry.remarks) > 35 else (entry.remarks or "-")
                                st.markdown(f'<small>{remarks_text}</small>', unsafe_allow_html=True)
                            
                            with cols[10]:
                                if entry.updated_by:
                                    tip = (
                                        f"Edited by {entry.updated_by} on {entry.updated_at:%d-%m-%Y %H:%M}"
                                        if entry.updated_by else "Not edited"
                                    )
                                    st.markdown(
                                        f'{entry.created_by} <span style="color:#f59e0b;" title="{tip}">⏱️</span>',
                                        unsafe_allow_html=True
                                    )
    
                                else:
                                    st.markdown(entry.created_by or "-")
                            
                            with cols[11]:
                                action_btn_col1, action_btn_col2 = st.columns(2)
                                
                                with action_btn_col1:
                                    if st.button("✏️", key=f"otrv_edit_btn_{entry.id}", help="Edit", use_container_width=True):
                                        if not _deny_edit_for_lock(entry, "OTRVessel", f"{entry.id}"):
                                            st.session_state[f"otrv_editing_{entry.id}"] = True
                                            _st_safe_rerun()
                                
                                with action_btn_col2:
                                    if can_delete_direct or can_delete_with_approval:
                                        if st.button("🗑️", key=f"otrv_del_btn_{entry.id}", help="Delete", use_container_width=True):
                                            st.session_state[f"otrv_deleting_{entry.id}"] = True
                                            _st_safe_rerun()
                            
                            # Edit form (if editing)
                            if st.session_state.get(f"otrv_editing_{entry.id}", False):
                                st.markdown("---")
                                with st.form(f"otrv_edit_form_{entry.id}"):
                                    st.markdown("#### ✏️ Edit Vessel Entry")
                                    
                                    edit_col1, edit_col2, edit_col3, edit_col4 = st.columns(4)
                                    
                                    with edit_col1:
                                        edit_date = st.date_input(
                                            "Date",
                                            value=entry.date,
                                            key=f"otrv_edit_date_{entry.id}"
                                        )
                                        edit_time = st.text_input(
                                            "Time (HH:MM)",
                                            value=str(entry.time),
                                            key=f"otrv_edit_time_{entry.id}",
                                            max_chars=5
                                        )
                                        edit_shuttle = st.text_input(
                                            "Shuttle No",
                                            value=entry.shuttle_no,
                                            key=f"otrv_edit_shuttle_{entry.id}"
                                        )
                                    
                                    with edit_col2:
                                        try:
                                            vessel_index = vessel_id_choices.index(entry.vessel_id)
                                        except ValueError:
                                            vessel_index = 0
                                        edit_vessel_id = st.selectbox(
                                            "Vessel Name",
                                            options=vessel_id_choices,
                                            index=vessel_index,
                                            format_func=lambda x: vessel_dict.get(x, "Unknown"),
                                            key=f"otrv_edit_vessel_{entry.id}"
                                        )
                                        
                                        try:
                                            op_index = operation_id_choices.index(entry.operation_id)
                                        except ValueError:
                                            op_index = 0
                                        edit_operation_id = st.selectbox(
                                            "Operation",
                                            options=operation_id_choices,
                                            index=op_index,
                                            format_func=lambda x: operation_dict.get(x, "Unknown"),
                                            key=f"otrv_edit_operation_{entry.id}"
                                        )
                                    
                                    with edit_col3:
                                        edit_opening = st.number_input(
                                            "Opening Stock (bbls)",
                                            value=float(getattr(entry, "opening_stock", 0.0)),
                                            key=f"otrv_edit_opening_{entry.id}"
                                        )
                                        edit_opening_water = st.number_input(
                                            "Opening Water (bbls)",
                                            value=float(getattr(entry, "opening_water", 0.0)),
                                            key=f"otrv_edit_opening_water_{entry.id}"
                                        )
                                        edit_closing = st.number_input(
                                            "Closing Stock (bbls)",
                                            value=float(getattr(entry, "closing_stock", 0.0)),
                                            key=f"otrv_edit_closing_{entry.id}"
                                        )
                                    
                                    with edit_col4:
                                        edit_closing_water = st.number_input(
                                            "Closing Water (bbls)",
                                            value=float(getattr(entry, "closing_water", 0.0)),
                                            key=f"otrv_edit_closing_water_{entry.id}"
                                        )
                                        edit_remarks = st.text_area(
                                            "Remarks",
                                            value=entry.remarks or "",
                                            key=f"otrv_edit_remarks_{entry.id}",
                                            max_chars=500
                                        )
                                    
                                    edit_net_stock = edit_closing - edit_opening
                                    edit_net_water = edit_closing_water - edit_opening_water
                                    
                                    st.info(
                                        f"Net Stock: {edit_net_stock:,.2f} bbls | Net Water: {edit_net_water:,.2f} bbls"
                                    )
                                    
                                    save_col, cancel_col = st.columns(2)
                                    with save_col:
                                        save_edit = st.form_submit_button(
                                            "💾 Save Changes",
                                            type="primary",
                                            use_container_width=True
                                        )
                                    with cancel_col:
                                        cancel_edit = st.form_submit_button(
                                            "? Cancel",
                                            use_container_width=True
                                        )
                                    
                                    if save_edit:
                                        time_text = (edit_time or "").strip()
                                        if not edit_shuttle.strip():
                                            st.error("? Shuttle No is required")
                                        elif not time_text or not re.match(r"^\\d{2}:\\d{2}$", time_text):
                                            st.error("? Enter time in HH:MM format")
                                        else:
                                            try:
                                                with get_session() as s:
                                                    entry_to_update = (
                                                        s.query(OTRVessel)
                                                         .filter(OTRVessel.id == entry.id)
                                                         .first()
                                                    )
                                                    if not entry_to_update:
                                                        st.error("? Entry no longer exists.")
                                                    else:
                                                        entry_to_update.date = edit_date
                                                        entry_to_update.time = time_text
                                                        entry_to_update.shuttle_no = edit_shuttle.strip()
                                                        entry_to_update.vessel_id = edit_vessel_id
                                                        entry_to_update.operation_id = edit_operation_id
                                                        entry_to_update.opening_stock = float(edit_opening)
                                                        entry_to_update.opening_water = float(edit_opening_water)
                                                        entry_to_update.closing_stock = float(edit_closing)
                                                        entry_to_update.closing_water = float(edit_closing_water)
                                                        entry_to_update.net_receipt_dispatch = float(edit_net_stock)
                                                        entry_to_update.net_water = float(edit_net_water)
                                                        entry_to_update.remarks = (
                                                            edit_remarks.strip() if edit_remarks else None
                                                        )
                                                        entry_to_update.updated_by = user["username"]
                                                        entry_to_update.updated_at = datetime.now()
                                                        
                                                        from security import SecurityManager
                                                        
                                                        SecurityManager.log_audit(
                                                            s,
                                                            user["username"],
                                                            "UPDATE",
                                                            resource_type="OTRVessel",
                                                            resource_id=str(entry.id),
                                                            location_id=active_location_id,
                                                            details=f"Updated: {vessel_dict.get(edit_vessel_id, 'Unknown')} - {edit_shuttle.strip()}",
                                                            user_id=user.get("id"),
                                                        )
                                                        
                                                        s.commit()
                                                    
                                                st.success("? Entry updated!")
                                                del st.session_state[f"otrv_editing_{entry.id}"]
                                                import time
                                                time.sleep(1)
                                                _st_safe_rerun()
                                            except Exception as ex:
                                                st.error(f"? Update failed: {ex}")
                                    elif cancel_edit:
                                        del st.session_state[f"otrv_editing_{entry.id}"]
                                        _st_safe_rerun()
                            
                            # Delete confirmation (if deleting)
                            if st.session_state.get(f"otrv_deleting_{entry.id}", False):
                                st.markdown("---")
                                st.warning("⚠️ **Confirm Delete**")
    
                                def _execute_otrv_delete(approver_label: str):
                                    try:
                                        with get_session() as s:
                                            target = (
                                                s.query(OTRVessel)
                                                 .filter(OTRVessel.id == entry.id)
                                                 .first()
                                            )
                                            if not target:
                                                st.warning("Entry already removed.")
                                                return
    
                                            _archive_record_for_delete(
                                                s,
                                                target,
                                                "OTRVessel",
                                                reason=(
                                                    f"Marked OTR vessel entry #{entry.id} "
                                                    f"({vessel_dict.get(target.vessel_id, 'Unknown')} - "
                                                    f"{target.shuttle_no}) for deletion. Approved by {approver_label}."
                                                ),
                                                label=f"{entry.id}",
                                            )
                                            s.commit()
    
                                        TaskManager.complete_tasks_for_resource(
                                            "OTRVessel",
                                            entry.id,
                                            user.get("username", "unknown"),
                                            notes=f"Approved by {approver_label}",
                                        )
                                        st.success("? Entry deleted!")
                                        del st.session_state[f"otrv_deleting_{entry.id}"]
                                        import time
    
                                        time.sleep(1)
                                        _st_safe_rerun()
                                    except Exception as ex:
                                        st.error(f"? Delete failed: {ex}")
    
                                if can_delete_direct:
                                    st.write("Are you sure you want to delete this entry?")
                                    del_col1, del_col2 = st.columns(2)
    
                                    with del_col1:
                                        if st.button(
                                            "🗑️ Yes, Delete",
                                            key=f"otrv_delete_confirm_{entry.id}",
                                            type="primary",
                                            use_container_width=True
                                        ):
                                            _execute_otrv_delete(f"{user.get('username', 'admin')} ({user.get('role')})")
    
                                    with del_col2:
                                        if st.button(
                                            "? Cancel",
                                            key=f"otrv_delete_cancel_{entry.id}",
                                            use_container_width=True
                                        ):
                                            del st.session_state[f"otrv_deleting_{entry.id}"]
                                            _st_safe_rerun()
    
                                elif can_delete_with_approval:
                                    remote_task = _render_remote_delete_request_ui(
                                        "OTRVessel",
                                        entry.id,
                                        f"OTR vessel entry #{entry.id}",
                                        "OTR Vessel",
                                        metadata={
                                            "ticket": row.get("Ticket ID"),
                                            "vessel": row["Vessel Name"],
                                            "shuttle": row["Shuttle No"],
                                        },
                                    )
                                    if remote_task and remote_task.get("status") == TaskStatus.APPROVED.value:
                                        remote_label = remote_task.get("approved_by") or "Supervisor"
                                        if st.button(
                                            "Delete with approved request",
                                            key=f"otrv_remote_delete_{entry.id}",
                                            type="primary",
                                            use_container_width=True
                                        ):
                                            _execute_otrv_delete(f"{remote_label} (remote)")
    
                                    st.write("Operator access detected. Supervisor code required.")
                                    with st.form(f"otrv_delete_form_{entry.id}"):
                                        sup_username, sup_label = _supervisor_dropdown(
                                            "Supervisor",
                                            f"otrv_delete_sup_{entry.id}",
                                            active_location_id,
                                        )
                                        supervisor_code = st.text_input(
                                            "Supervisor Code",
                                            type="password",
                                            key=f"otrv_delete_code_{entry.id}"
                                        )
                                        
                                        approver_col1, approver_col2 = st.columns(2)
                                        with approver_col1:
                                            approve_delete = st.form_submit_button(
                                                "🗑️ Confirm Delete",
                                                type="primary",
                                                use_container_width=True
                                            )
                                        with approver_col2:
                                            cancel_delete = st.form_submit_button(
                                                "? Cancel",
                                                use_container_width=True
                                            )
                                        
                                        if approve_delete:
                                            if not supervisor_code.strip():
                                                st.error("? Supervisor code is required")
                                            elif not sup_username:
                                                st.error("? No supervisor available for approval")
                                            elif not SecurityManager.verify_supervisor_code(supervisor_code, sup_username):
                                                st.error("? Invalid supervisor code")
                                            else:
                                                approver = sup_label or sup_username or "Supervisor"
                                                _execute_otrv_delete(f"{approver}")
                                        elif cancel_delete:
                                            del st.session_state[f"otrv_deleting_{entry.id}"]
                                            _st_safe_rerun()
    
                    
                    st.markdown("---")
                    
                    # ========== EXPORT OPTIONS WITH PDF ==========
                    st.markdown("#### 📤 Export Data")
                    
                    export_col1, export_col2, export_col3, export_col4, export_col5 = st.columns(5)
                    
                    # CSV Export
                    with export_col1:
                        csv = df_export.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 CSV",
                            data=csv,
                            file_name=f"otr_vessel_{location_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                    
                    # Excel Export
                    with export_col2:
                        excel_buffer = BytesIO()
                        try:
                            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                                df_export.to_excel(writer, index=False, sheet_name='OTR Vessel')
                            
                            st.download_button(
                                label="📊 Excel",
                                data=excel_buffer.getvalue(),
                                file_name=f"otr_vessel_{location_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True
                            )
                        except ImportError:
                            st.button("📊 Excel", disabled=True, help="Install openpyxl", use_container_width=True)
                    
                    # PDF Download
                    with export_col3:
                        if st.button("📥 Download PDF", use_container_width=True):
                            try:
                                # Calculate summary
                                total_receipts = sum(e.net_receipt_dispatch for e in entries if e.net_receipt_dispatch > 0)
                                total_dispatches = abs(sum(e.net_receipt_dispatch for e in entries if e.net_receipt_dispatch < 0))
                                total_water_in = sum(getattr(e, 'net_water', 0.0) for e in entries if getattr(e, 'net_water', 0.0) > 0)
                                total_water_out = abs(sum(getattr(e, 'net_water', 0.0) for e in entries if getattr(e, 'net_water', 0.0) < 0))
                                
                                pdf_data = generate_otr_vessel_pdf(
                                    df_export,
                                    filter_date_from.strftime("%d-%b-%Y"),
                                    filter_date_to.strftime("%d-%b-%Y"),
                                    total_receipts,
                                    total_dispatches,
                                    total_water_in,
                                    total_water_out,
                                    user["username"],
                                    location_name
                                )
                                
                                st.download_button(
                                    label="💾 Save PDF",
                                    data=pdf_data,
                                    file_name=f"otr_vessel_{location_name.replace(' ', '_')}_{date.today()}.pdf",
                                    mime="application/pdf",
                                    key="otrv_save_pdf"
                                )
                            except Exception as ex:
                                st.error(f"? PDF generation failed: {ex}")
                    
                    # PDF View
                    with export_col4:
                        if st.button("👁️ View PDF", key="otr_vessel_pdf_view", use_container_width=True):
                            try:
                                # Calculate summary
                                total_receipts = sum(e.net_receipt_dispatch for e in entries if e.net_receipt_dispatch > 0)
                                total_dispatches = abs(sum(e.net_receipt_dispatch for e in entries if e.net_receipt_dispatch < 0))
                                total_water_in = sum(getattr(e, 'net_water', 0.0) for e in entries if getattr(e, 'net_water', 0.0) > 0)
                                total_water_out = abs(sum(getattr(e, 'net_water', 0.0) for e in entries if getattr(e, 'net_water', 0.0) < 0))
                                
                                pdf_data = generate_otr_vessel_pdf(
                                    df_export,
                                    filter_date_from.strftime("%d-%b-%Y"),
                                    filter_date_to.strftime("%d-%b-%Y"),
                                    total_receipts,
                                    total_dispatches,
                                    total_water_in,
                                    total_water_out,
                                    user["username"],
                                    location_name
                                )
                                
                                base64_pdf = base64.b64encode(pdf_data).decode('utf-8')
                                
                                pdf_html = f"""
                                <script>
                                    var pdfWindow = window.open("");
                                    pdfWindow.document.write(
                                        '<html><head><title>OTR-Vessel Report - {location_name}</title></head>' +
                                        '<body style="margin:0"><iframe width="100%" height="100%" src="data:application/pdf;base64,{base64_pdf}"></iframe></body></html>'
                                    );
                                </script>
                                """
                                
                                import streamlit.components.v1 as components
                                components.html(pdf_html, height=0)
                                st.success("? PDF opened in new tab!")
                                
                            except Exception as ex:
                                st.error(f"? PDF view failed: {ex}")
                    
                    # Summary Stats
                    with export_col5:
                        with st.popover("📊 Summary"):
                            total_opening = sum(e.opening_stock for e in entries)
                            total_closing = sum(e.closing_stock for e in entries)
                            total_net = sum(e.net_receipt_dispatch for e in entries)
                            total_water = sum(getattr(e, 'net_water', 0.0) for e in entries)
                            
                            st.metric("Entries", len(entries))
                            st.metric("Total Opening", f"{total_opening:,.0f} bbls")
                            st.metric("Total Closing", f"{total_closing:,.0f} bbls")
                            st.metric("Total Net Stock", f"{total_net:,.0f} bbls")
                            st.metric("Total Net Water", f"{total_water:,.0f} bbls")
        
        except Exception as ex:
            st.error(f"? Failed to load vessel data: {ex}")
            import traceback
            with st.expander("⚠️ Error Details"):
                st.code(traceback.format_exc())
    
    # ========================= CONVOY STATUS PAGE =========================