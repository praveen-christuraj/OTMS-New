"""
Auto-generated module for the '2FA Verify' page.
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
        header("Two-Factor Authentication")
        
        # Check if user is pending 2FA
        pending_user = st.session_state.get("pending_2fa_user")
        
        if not pending_user:
            st.error("No pending login. Please login first.")
            st.session_state["page"] = "Home"
            st_safe_rerun()
            st.stop()
        
        st.markdown("### 🔐 Enter Verification Code")
        
        col1, col2, col3 = st.columns([0.3, 0.4, 0.3])
        
        with col2:
            st.info(f"**User:** {pending_user.get('username')}")
            st.caption("Enter the 6-digit code from your authenticator app")
            
            with st.form("2fa_verify_form"):
                token = st.text_input(
                    "Verification Code",
                    max_chars=10,
                    placeholder="000000 or XXXX-XXXX",
                    key="2fa_token_input"
                )
                
                col_verify, col_cancel = st.columns(2)
                
                with col_verify:
                    verify_btn = st.form_submit_button("? Verify", type="primary", use_container_width=True)
                
                with col_cancel:
                    cancel_btn = st.form_submit_button("❌ Cancel", use_container_width=True)
                
                if verify_btn:
                    if not token.strip():
                        st.error("Please enter a verification code")
                    else:
                        try:
                            from twofa import TwoFactorAuth
                            
                            with get_session() as s:
                                # Verify token
                                is_valid = TwoFactorAuth.verify_token(
                                    s, 
                                    pending_user["id"], 
                                    token.strip().replace("-", "")
                                )
                                
                                if is_valid:
                                    # 2FA successful - complete login
                                    st.session_state.auth_user = pending_user
                                    st.session_state.pop("pending_2fa_user", None)
                                    
                                    # Set default active location
                                    if pending_user["role"] in ["admin-operations", "admin-it", "manager"]:
                                        from location_manager import LocationManager
                                        locations = LocationManager.get_all_locations(s, active_only=True)
                                        if locations:
                                            st.session_state.active_location_id = locations[0].id
                                    else:
                                        st.session_state.active_location_id = pending_user["location_id"]
                                    
                                    # Log successful 2FA
                                    from security import SecurityManager
                                    SecurityManager.log_audit(
                                        s, pending_user["username"], "2FA_SUCCESS",
                                        user_id=pending_user["id"],
                                        details="2FA verification successful"
                                    )
                                    
                                    st.success("? Verification successful!")
                                    st.session_state["page"] = "Home"
                                    _st_safe_rerun()
                                else:
                                    st.error("? Invalid verification code. Please try again.")
                                    
                                    # Log failed 2FA
                                    from security import SecurityManager
                                    with get_session() as s:
                                        SecurityManager.log_audit(
                                            s, pending_user["username"], "2FA_FAILED",
                                            user_id=pending_user["id"],
                                            details="Invalid 2FA token"
                                        )
                        
                        except Exception as ex:
                            st.error(f"Verification failed: {ex}")
                
                if cancel_btn:
                    st.session_state.pop("pending_2fa_user", None)
                    st.session_state["page"] = "Home"
                    st.info("Login cancelled")
                    _st_safe_rerun()
            
            st.markdown("---")
            st.caption("ℹ️ Lost your device? Use one of your backup codes")
            st.caption("ℹ️ Need help? Contact your system administrator")
    
    # ========================= 2FA SETTINGS PAGE =========================
    # ========================= 2FA SETTINGS PAGE =========================
