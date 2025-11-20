"""
Auto-generated module for the 'Login History' page.
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
        header("Login History & Security Monitoring")
        
        user = st.session_state.get("auth_user")
        
        if not user:
            st.error("Please login to access this page")
            st.stop()
        
        from ip_service import IPService
        from datetime import datetime, timedelta
        import pandas as pd
        from models import User, LoginAttempt
        
        # Import timezone utilities
        try:
            from timezone_utils import utc_to_local, format_local_datetime
            TIMEZONE_AVAILABLE = True
        except ImportError:
            TIMEZONE_AVAILABLE = False
            st.warning("⚠️ Timezone utilities not found. Install pytz: `pip install pytz`")
        
        st.markdown("### 🔐 Login Activity Monitor")
        
        # ========== FILTERS ==========
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            # Get list of users for filter
            with get_session() as s:
                if user["role"] in ["admin-operations", "manager"]:
                    all_users = s.query(User).all()
                    user_options = ["All"] + [u.username for u in all_users]
                else:
                    user_options = [user["username"]]
            
            filter_user = st.selectbox(
                "User",
                user_options,
                key="login_history_user_filter"
            )
        
        with col2:
            filter_success = st.selectbox(
                "Status",
                ["All", "Success", "Failed"],
                key="login_history_status_filter"
            )
        
        with col3:
            filter_days = st.selectbox(
                "Time Period",
                ["Last 7 days", "Last 30 days", "Last 90 days", "All time"],
                key="login_history_days_filter"
            )
        
        with col4:
            filter_2fa = st.selectbox(
                "2FA Used",
                ["All", "Yes", "No"],
                key="login_history_2fa_filter"
            )
        
        # ========== QUERY DATA ==========
        try:
            with get_session() as s:
                query = s.query(LoginAttempt).order_by(LoginAttempt.timestamp.desc())
                
                # Apply filters
                if filter_user != "All":
                    query = query.filter(LoginAttempt.username == filter_user)
                
                if filter_success == "Success":
                    query = query.filter(LoginAttempt.success == True)
                elif filter_success == "Failed":
                    query = query.filter(LoginAttempt.success == False)
                
                if filter_2fa == "Yes":
                    query = query.filter(LoginAttempt.two_factor_used == True)
                elif filter_2fa == "No":
                    query = query.filter(LoginAttempt.two_factor_used == False)
                
                # Time filter
                if filter_days != "All time":
                    days_map = {
                        "Last 7 days": 7,
                        "Last 30 days": 30,
                        "Last 90 days": 90
                    }
                    days = days_map[filter_days]
                    cutoff_date = datetime.utcnow() - timedelta(days=days)
                    query = query.filter(LoginAttempt.timestamp >= cutoff_date)
                
                attempts = query.limit(500).all()
        
        except Exception as ex:
            st.error(f"Failed to load login history: {ex}")
            import traceback
            st.code(traceback.format_exc())
            st.stop()
        
        # ========== STATISTICS ==========
        if attempts:
            total_attempts = len(attempts)
            successful_logins = sum(1 for a in attempts if a.success)
            failed_logins = total_attempts - successful_logins
            unique_ips = len(set(a.ip_address for a in attempts if a.ip_address))
            
            # Safely count 2FA logins (handle missing attribute)
            two_fa_logins = sum(1 for a in attempts if getattr(a, 'two_factor_used', False))
            
            st.markdown("---")
            
            metric_col1, metric_col2, metric_col3, metric_col4, metric_col5 = st.columns(5)
            
            with metric_col1:
                st.metric("Total Attempts", total_attempts)
            
            with metric_col2:
                success_pct = (successful_logins/total_attempts*100) if total_attempts > 0 else 0
                st.metric("Successful", successful_logins, delta=f"{success_pct:.1f}%")
            
            with metric_col3:
                fail_pct = (failed_logins/total_attempts*100) if total_attempts > 0 else 0
                st.metric("Failed", failed_logins, delta=f"{fail_pct:.1f}%", delta_color="inverse")
            
            with metric_col4:
                st.metric("Unique IPs", unique_ips)
            
            with metric_col5:
                twofa_pct = (two_fa_logins/successful_logins*100) if successful_logins > 0 else 0
                st.metric("2FA Used", two_fa_logins, delta=f"{twofa_pct:.1f}%")
            
            st.markdown("---")
            
            # ========== LOGIN ATTEMPTS TABLE ==========
            st.markdown("#### 🔐 Recent Login Attempts")
            
            # Show timezone info
            if TIMEZONE_AVAILABLE:
                st.caption("? Showing times in **Nigeria Time (WAT - UTC+1)**")
            else:
                st.caption("? Showing times in **UTC** (install pytz for local time)")
            
            # Prepare data for display
            display_data = []
            
            for attempt in attempts[:100]:  # Show last 100
                # Safely get attributes (handle missing columns)
                ip_country = getattr(attempt, 'ip_country', None) or "Unknown"
                ip_city = getattr(attempt, 'ip_city', None) or "Unknown"
                device_type = getattr(attempt, 'device_type', None) or "Unknown"
                browser = getattr(attempt, 'browser', None) or "Unknown"
                os_name = getattr(attempt, 'os', None) or "Unknown"
                two_factor_used = getattr(attempt, 'two_factor_used', False)
                
                flag = IPService.get_flag_emoji(ip_country)
                
                # Format timestamp in local time
                if attempt.timestamp:
                    if TIMEZONE_AVAILABLE:
                        # Convert UTC to local time
                        timestamp_str = format_local_datetime(attempt.timestamp, "%Y-%m-%d %H:%M:%S")
                    else:
                        # Show UTC time
                        timestamp_str = attempt.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
                else:
                    timestamp_str = "N/A"
                
                display_data.append({
                    "Timestamp": timestamp_str,
                    "User": attempt.username,
                    "Status": "? Success" if attempt.success else "? Failed",
                    "IP Address": attempt.ip_address or "N/A",
                    "Location": f"{flag} {ip_city}, {ip_country}",
                    "Device": device_type,
                    "Browser": browser,
                    "OS": os_name,
                    "2FA": "?" if two_factor_used else "?",
                    "Reason": attempt.failure_reason or "N/A"
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
                    "User": st.column_config.TextColumn("User", width="small"),
                    "Status": st.column_config.TextColumn("Status", width="small"),
                    "IP Address": st.column_config.TextColumn("IP Address", width="medium"),
                    "Location": st.column_config.TextColumn("Location", width="medium"),
                    "Device": st.column_config.TextColumn("Device", width="small"),
                    "Browser": st.column_config.TextColumn("Browser", width="small"),
                    "OS": st.column_config.TextColumn("OS", width="small"),
                    "2FA": st.column_config.TextColumn("2FA", width="small"),
                    "Reason": st.column_config.TextColumn("Reason", width="medium"),
                }
            )
            
            # ========== SECURITY ALERTS ==========
            # Check for suspicious activity
            recent_failures = [a for a in attempts[:50] if not a.success]
            if len(recent_failures) >= 5:
                st.warning(f"⚠️ **Security Alert:** {len(recent_failures)} failed login attempts in recent history")
            
            # Check for multiple IPs for same user
            if filter_user != "All":
                user_ips = set(a.ip_address for a in attempts[:50] if a.username == filter_user and a.ip_address)
                if len(user_ips) > 3:
                    st.warning(f"⚠️ **Multiple IPs Detected:** User '{filter_user}' logged in from {len(user_ips)} different IP addresses")
            
            st.markdown("---")
            
            # ========== DOWNLOAD REPORT ==========
            col_download, col_refresh = st.columns([0.8, 0.2])
            
            with col_download:
                csv_data = df.to_csv(index=False)
                st.download_button(
                    "⬇️ Download Login History (CSV)",
                    data=csv_data,
                    file_name=f"login_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with col_refresh:
                if st.button("🔄 Refresh", use_container_width=True):
                    _st_safe_rerun()
            
            # ========== ADDITIONAL INSIGHTS ==========
            st.markdown("---")
            st.markdown("#### 🔐 Login Insights")
            
            insight_col1, insight_col2 = st.columns(2)
            
            with insight_col1:
                st.markdown("##### 📍 Top Login Locations")
                location_counts = {}
                for a in attempts[:100]:
                    country = getattr(a, 'ip_country', None) or "Unknown"
                    flag = IPService.get_flag_emoji(country)
                    location_key = f"{flag} {country}"
                    location_counts[location_key] = location_counts.get(location_key, 0) + 1
                
                sorted_locations = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                for loc, count in sorted_locations:
                    st.caption(f"{loc}: {count} attempts")
            
            with insight_col2:
                st.markdown("##### 💻 Top Devices")
                device_counts = {}
                for a in attempts[:100]:
                    device = getattr(a, 'device_type', None) or "Unknown"
                    device_counts[device] = device_counts.get(device, 0) + 1
                
                sorted_devices = sorted(device_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                for device, count in sorted_devices:
                    st.caption(f"{device}: {count} attempts")
        
        else:
            st.info("ℹ️ No login attempts found matching the filters.")
            
            # Show current time for reference
            if TIMEZONE_AVAILABLE:
                from timezone_utils import get_local_time
                current_time = get_local_time()
                st.caption(f"? Current Time: {current_time.strftime('%Y-%m-%d %H:%M:%S')} (Nigeria Time - WAT)")
            else:
                st.caption(f"? Current Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} (UTC)")
                
    # ========================= OTR-VESSEL PAGE =========================