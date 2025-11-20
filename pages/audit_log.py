"""
Auto-generated module for the 'Audit Log' page.
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
        if st.session_state.get("auth_user", {}).get("role") not in ["admin-operations", "admin-it"]:
            header("Audit Log")
            st.error("You do not have permission to access this page. Admin-Operations or Admin-IT only.")
            st.stop()
    
        header("Audit Log")
        st.markdown("### 📜 System Audit Trail")
        
        from datetime import date, timedelta, datetime
        from security import SecurityManager
        import pandas as pd
        from models import AuditLog
        
        # Import timezone utilities
        try:
            from timezone_utils import utc_to_local, format_local_datetime, get_local_time
            TIMEZONE_AVAILABLE = True
        except ImportError:
            TIMEZONE_AVAILABLE = False
            st.warning("⚠️ Timezone utilities not found. Install pytz: `pip install pytz`")
        
        # Show current time for reference
        if TIMEZONE_AVAILABLE:
            current_time = get_local_time()
            st.caption(f"? Current Time: **{current_time.strftime('%Y-%m-%d %H:%M:%S')}** (Nigeria Time - WAT, UTC+1)")
        else:
            st.caption(f"? Current Time: **{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}** (UTC)")
        
        # Filters
        with st.container(border=True):
            st.markdown("#### 🔎 Filters")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                f_username = st.text_input("Username", placeholder="Search username...", key="audit_username")
            
            with col2:
                f_action = st.selectbox(
                    "Action", 
                    ["(All)", "LOGIN", "LOGOUT", "CREATE", "UPDATE", "DELETE", "EXPORT", "VIEW"], 
                    key="audit_action"
                )
            
            with col3:
                f_from = st.date_input(
                    "From Date", 
                    value=date.today() - timedelta(days=7), 
                    key="audit_from"
                )
            
            with col4:
                f_to = st.date_input(
                    "To Date", 
                    value=date.today(), 
                    key="audit_to"
                )
        
        # Additional filters
        with st.container(border=True):
            st.markdown("#### ⚙️ Advanced Filters")
            col5, col6, col7 = st.columns(3)
            
            with col5:
                f_resource_type = st.selectbox(
                    "Resource Type",
                    ["(All)", "TankTransaction", "YadeVoyage", "TankerTransaction", "OTRVessel", "User", "Location"],
                    key="audit_resource_type"
                )
            
            with col6:
                f_success = st.selectbox(
                    "Status",
                    ["(All)", "Success", "Failed"],
                    key="audit_success"
                )
            
            with col7:
                # Get locations for filter (admin only)
                with get_session() as s:
                    from models import Location
                    locations = s.query(Location).filter(Location.is_active == True).all()
                    location_names = ["(All)"] + [loc.name for loc in locations]
                
                f_location = st.selectbox(
                    "Location",
                    location_names,
                    key="audit_location"
                )
        
        # Fetch audit logs
        try:
            with get_session() as s:
                query = s.query(AuditLog).order_by(AuditLog.timestamp.desc())
                
                # Apply filters
                if f_username:
                    query = query.filter(AuditLog.username.like(f"%{f_username}%"))
                
                if f_action != "(All)":
                    query = query.filter(AuditLog.action == f_action)
                
                if f_resource_type != "(All)":
                    query = query.filter(AuditLog.resource_type == f_resource_type)
                
                if f_success == "Success":
                    query = query.filter(AuditLog.success == True)
                elif f_success == "Failed":
                    query = query.filter(AuditLog.success == False)
                
                if f_location != "(All)":
                    # Find location ID
                    location_obj = next((loc for loc in locations if loc.name == f_location), None)
                    if location_obj:
                        query = query.filter(AuditLog.location_id == location_obj.id)
                
                # Date range filter
                query = query.filter(AuditLog.timestamp >= datetime.combine(f_from, datetime.min.time()))
                query = query.filter(AuditLog.timestamp <= datetime.combine(f_to, datetime.max.time()))
                
                logs = query.limit(500).all()
            
            if logs:
                # ========== STATISTICS ==========
                st.markdown("---")
                
                total_logs = len(logs)
                success_logs = sum(1 for log in logs if getattr(log, 'success', True))
                failed_logs = total_logs - success_logs
                unique_users = len(set(log.username for log in logs if log.username))
                
                stat_col1, stat_col2, stat_col3, stat_col4 = st.columns(4)
                
                with stat_col1:
                    st.metric("Total Actions", total_logs)
                
                with stat_col2:
                    st.metric("Successful", success_logs)
                
                with stat_col3:
                    st.metric("Failed", failed_logs, delta_color="inverse")
                
                with stat_col4:
                    st.metric("Unique Users", unique_users)
                
                st.markdown("---")
                
                # Show timezone info
                if TIMEZONE_AVAILABLE:
                    st.caption("? Showing times in **Nigeria Time (WAT - UTC+1)**")
                else:
                    st.caption("? Showing times in **UTC** (install pytz for local time)")
                
                # Prepare display data
                display_data = []
                
                for log in logs:
                    # Format timestamp in local time
                    if log.timestamp:
                        if TIMEZONE_AVAILABLE:
                            timestamp_str = format_local_datetime(
                                log.timestamp,
                                "%Y-%m-%d %H:%M:%S",
                                naive_is_local=True
                            )
                        else:
                            timestamp_str = log.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
                    else:
                        timestamp_str = "N/A"
                    
                    # Get location name
                    location_name = "�"
                    if log.location_id:
                        location_obj = next((loc for loc in locations if loc.id == log.location_id), None)
                        if location_obj:
                            location_name = location_obj.code
                    
                    # Format resource
                    resource_str = ""
                    if log.resource_type:
                        resource_str = log.resource_type
                        if log.resource_id:
                            resource_str += f" #{log.resource_id}"
                    else:
                        resource_str = "�"
                    
                    display_data.append({
                        "Timestamp": timestamp_str,
                        "Username": log.username or "�",
                        "Action": log.action or "�",
                        "Resource": resource_str,
                        "Location": location_name,
                        "Details": log.details or "�",
                        "IP Address": log.ip_address or "�",
                        "Status": "? Success" if getattr(log, 'success', True) else "? Failed"
                    })
                
                df = pd.DataFrame(display_data)
                
                # Display table
                st.dataframe(
                    df, 
                    use_container_width=True, 
                    hide_index=True,
                    height=600,
                    column_config={
                        "Timestamp": st.column_config.TextColumn("Timestamp", width="medium"),
                        "Username": st.column_config.TextColumn("Username", width="small"),
                        "Action": st.column_config.TextColumn("Action", width="small"),
                        "Resource": st.column_config.TextColumn("Resource", width="medium"),
                        "Location": st.column_config.TextColumn("Location", width="small"),
                        "Details": st.column_config.TextColumn("Details", width="large"),
                        "IP Address": st.column_config.TextColumn("IP Address", width="medium"),
                        "Status": st.column_config.TextColumn("Status", width="small"),
                    }
                )
                
                st.caption(f"🔢 Showing **{len(logs)}** records (max 500)")
                
                # ========== ACTIVITY BREAKDOWN ==========
                st.markdown("---")
                st.markdown("#### 📊 Activity Breakdown")
                
                breakdown_col1, breakdown_col2 = st.columns(2)
                
                with breakdown_col1:
                    st.markdown("##### 🛠️ Actions")
                    action_counts = {}
                    for log in logs:
                        action = log.action or "Unknown"
                        action_counts[action] = action_counts.get(action, 0) + 1
                    
                    sorted_actions = sorted(action_counts.items(), key=lambda x: x[1], reverse=True)
                    for action, count in sorted_actions[:10]:
                        st.caption(f"� {action}: {count}")
                
                with breakdown_col2:
                    st.markdown("##### 👥 Top Users")
                    user_counts = {}
                    for log in logs:
                        username = log.username or "Unknown"
                        user_counts[username] = user_counts.get(username, 0) + 1
                    
                    sorted_users = sorted(user_counts.items(), key=lambda x: x[1], reverse=True)
                    for username, count in sorted_users[:10]:
                        st.caption(f"� {username}: {count} actions")
                
                st.markdown("---")
                
                # ========== EXPORT OPTIONS ==========
                export_col1, export_col2, export_col3 = st.columns([0.4, 0.4, 0.2])
                
                with export_col1:
                    # CSV Export
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "📥 Download CSV",
                        data=csv,
                        file_name=f"audit_log_{f_from.strftime('%Y%m%d')}_{f_to.strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                
                with export_col2:
                    # Excel Export
                    try:
                        from io import BytesIO
                        excel_buffer = BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                            df.to_excel(writer, index=False, sheet_name='Audit Log')
                        
                        st.download_button(
                            "⬇️ Download Excel",
                            data=excel_buffer.getvalue(),
                            file_name=f"audit_log_{f_from.strftime('%Y%m%d')}_{f_to.strftime('%Y%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )
                    except ImportError:
                        st.button("⬇️ Download Excel", disabled=True, help="Install openpyxl", use_container_width=True)
                
                with export_col3:
                    if st.button("🔄 Refresh", use_container_width=True):
                        _st_safe_rerun()
            
            else:
                st.info("ℹ️ No audit records found for the selected filters.")
                st.caption("Try adjusting your filters or expanding the date range.")
        
        except Exception as ex:
            st.error(f"? Failed to load audit log: {ex}")
            import traceback
            with st.expander("⚠️ Error Details"):
                st.code(traceback.format_exc())
    
    # =============================== Backup & Recovery (admin only) =============================