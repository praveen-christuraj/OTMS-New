"""
Auto-generated module for the "2FA Settings" page.
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
import streamlit as _stmod
from models import FSOOperation  # local import to avoid circular deps
import pandas as pd  # local import to avoid polluting top-level namespace
from permission_manager import PermissionManager
from security import SecurityManager
import base64
from io import BytesIO
from datetime import date, timedelta
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from db import get_session
from models import OFSProductionEvacuationRecord
import time
from models import Operation  # this Enum is defined in models.py
from models import TankTransaction, Table11
import bisect
import re
from sqlalchemy import text
from models import Operation
import json
from datetime import date, timedelta
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
import pandas as pd  # local import to avoid polluting global namespace
from sqlalchemy import or_
from models import Location, LocationTankerEntry
import streamlit.components.v1 as components
import json, base64
from datetime import date, timedelta, datetime, time
from pathlib import Path
from models import Location
from material_balance_calculator import MaterialBalanceCalculator as MBC
from reportlab.lib.pagesizes import A4  # portrait
from sqlalchemy import or_
from models import ReportDefinition
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from models import FSOOperation
from datetime import time as dt_time
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from models import FSOOperation
from sqlalchemy import and_, or_
from models import OTR  # your preferred model name
from models import OTRTransaction as OTRModel
import streamlit as _stmod
from models import MeterTransaction  # added Meter-2 fields earlier
from reportlab.lib.pagesizes import A4, landscape
from datetime import date, timedelta
import pandas as pd
import io, base64
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from typing import Dict, Tuple, Optional
from models import Table11
from models import YadeCalibration
from models import YadeSampleParam
from typing import Dict, Tuple, List, Optional
import bisect
import bisect
import math
from models import (YadeDip, YadeSampleParam, TOAYadeStage, TOAYadeSummary, YadeVoyage)
import streamlit as st  # ensure st exists in this scope
from location_manager import LocationManager
from location_config import LocationConfig
from location_manager import LocationManager
from permission_manager import PermissionManager
from security import SecurityManager
from models import YadeVoyage, TOAYadeSummary, TOAYadeStage
from datetime import datetime, timedelta
from ui import header
from db import get_session
from pages.helpers import st_safe_rerun, archive_payload_for_delete

def render():
        header("Two-Factor Authentication Settings")
        
        user = st.session_state.get("auth_user")
        
        if not user:
            st.error("Please login to access this page")
            st.stop()
        
        st.markdown("### 🔐 Two-Factor Authentication (2FA)")
        st.caption("Add an extra layer of security to your account")
        
        from twofa import TwoFactorAuth
        
        with get_session() as s:
            is_2fa_enabled = TwoFactorAuth.is_enabled(s, user["id"])
        
        # ========== 2FA Status ==========
        if is_2fa_enabled:
            st.success("? **2FA is ENABLED** for your account")
        else:
            st.warning("⚠️ **2FA is DISABLED** - Your account is less secure")
        
        st.markdown("---")
        
        # ========== Enable 2FA ==========
        if not is_2fa_enabled:
            st.markdown("#### Enable 2FA")
            
            st.info("""
            **How it works:**
            1. **Recommended:** Download **Microsoft Authenticator** app
               - iOS: App Store ? Search "Microsoft Authenticator"
               - Android: Play Store ? Search "Microsoft Authenticator"
            2. Open app ? Tap "+" ? Select "Other account" ? Scan QR code
            3. Enter the 6-digit code to verify
            4. **Save your backup codes** in a safe place!
            
            *Also works with: Google Authenticator, Authy, 1Password, etc.*
            """)
            
            if st.button("🔐 Enable 2FA", key="enable_2fa_btn", type="primary"):
                try:
                    with get_session() as s:
                        secret, backup_codes, provisioning_uri = TwoFactorAuth.enable_2fa(s, user["id"])
                    
                    # Store in session for verification
                    st.session_state["2fa_setup"] = {
                        "secret": secret,
                        "backup_codes": backup_codes,
                        "provisioning_uri": provisioning_uri
                    }
                    
                    _st_safe_rerun()
                
                except Exception as ex:
                    st.error(f"Failed to enable 2FA: {ex}")
            
            # ========== 2FA Setup Flow ==========
            if st.session_state.get("2fa_setup"):
                setup = st.session_state["2fa_setup"]
                
                st.markdown("---")
                st.markdown("#### Step 1: Scan QR Code")
                
                # Generate and display QR code
                qr_image = TwoFactorAuth.generate_qr_code(setup["provisioning_uri"])
                
                col1, col2, col3 = st.columns([0.2, 0.6, 0.2])
                with col2:
                    st.image(qr_image, caption="Scan this with Microsoft Authenticator", width=300)
                
                with st.expander("🔢 Can't scan? Enter manually"):
                    st.code(setup["secret"], language=None)
                    st.caption(f"Account: {user['username']}")
                    st.caption(f"Issuer: {TwoFactorAuth.ISSUER_NAME}")
                
                st.markdown("#### Step 2: Verify Code")
                
                # Form for verification (button only)
                with st.form("verify_2fa_setup"):
                    verification_code = st.text_input(
                        "Enter the 6-digit code from your app",
                        max_chars=6,
                        placeholder="000000",
                        key="2fa_verify_code"
                    )
                    
                    verify_btn = st.form_submit_button("? Verify & Enable", type="primary")
                
                # Handle verification OUTSIDE form
                if verify_btn:
                    if not verification_code or len(verification_code) != 6:
                        st.error("Please enter a 6-digit code")
                    else:
                        try:
                            with get_session() as s:
                                success = TwoFactorAuth.verify_and_enable(s, user["id"], verification_code)
                            
                            if success:
                                st.success("? 2FA enabled successfully!")
                                
                                # Store backup codes in session
                                st.session_state["2fa_backup_codes_ready"] = setup["backup_codes"]
                                st.session_state.pop("2fa_setup", None)
                                
                                # Log audit
                                from security import SecurityManager
                                with get_session() as s:
                                    SecurityManager.log_audit(
                                        s, user["username"], "2FA_ENABLED",
                                        user_id=user["id"],
                                        details="User enabled 2FA"
                                    )
                                
                                _st_safe_rerun()
                            else:
                                st.error("? Invalid code. Please try again.")
                        
                        except Exception as ex:
                            st.error(f"Verification failed: {ex}")
                
                # Show backup codes after successful setup
                if st.session_state.get("2fa_backup_codes_ready"):
                    backup_codes = st.session_state["2fa_backup_codes_ready"]
                    
                    st.markdown("---")
                    st.markdown("#### 🔐 IMPORTANT: Save Your Backup Codes")
                    st.warning("Store these codes in a safe place. You'll need them if you lose your device.")
                    
                    backup_codes_text = "\n".join(backup_codes)
                    st.code(backup_codes_text, language=None)
                    
                    # Download button OUTSIDE form
                    st.download_button(
                        "⬇️ Download Backup Codes",
                        data=backup_codes_text,
                        file_name=f"otms_backup_codes_{user['username']}.txt",
                        mime="text/plain",
                        key="download_backup_codes_initial"
                    )
                    
                    if st.button("? I've Saved My Backup Codes - Continue", key="finish_2fa_setup"):
                        st.session_state.pop("2fa_backup_codes_ready", None)
                        st.success("Setup complete! You can now use 2FA to login.")
                        import time
                        time.sleep(2)
                        _st_safe_rerun()
        
        # ========== Manage 2FA (if enabled) ==========
        else:
            st.markdown("#### Manage 2FA")
            
            # ========== TABS FOR MANAGING 2FA ==========
            tab1, tab2, tab3 = st.tabs(["Backup Codes", "Regenerate Codes", "Disable 2FA"])
            
            # ==================== TAB 1: VIEW BACKUP CODES ====================
            with tab1:
                st.markdown("##### Your Backup Codes")
                st.caption("Use these codes if you lose access to your authenticator app")
                
                try:
                    with get_session() as s:
                        backup_codes = TwoFactorAuth.get_backup_codes(s, user["id"])
                    
                    if backup_codes:
                        st.info(f"You have **{len(backup_codes)}** unused backup codes")
                        
                        # Initialize visibility state
                        if "backup_codes_visible" not in st.session_state:
                            st.session_state.backup_codes_visible = False
                        
                        # Toggle visibility
                        if not st.session_state.backup_codes_visible:
                            if st.button("👁️ Show Backup Codes", key="btn_show_backup_codes", type="primary"):
                                st.session_state.backup_codes_visible = True
                                _st_safe_rerun()
                        else:
                            # Display codes
                            st.code("\n".join(backup_codes), language=None)
                            
                            # Download button
                            backup_codes_text = "\n".join(backup_codes)
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.download_button(
                                    "⬇️ Download Codes",
                                    data=backup_codes_text,
                                    file_name=f"otms_backup_codes_{user['username']}.txt",
                                    mime="text/plain",
                                    key="download_existing_backup_codes",
                                    use_container_width=True
                                )
                            
                            with col2:
                                if st.button("🙈 Hide Codes", key="btn_hide_backup_codes", use_container_width=True):
                                    st.session_state.backup_codes_visible = False
                                    _st_safe_rerun()
                    else:
                        st.warning("⚠️ No backup codes remaining. Generate new ones in the next tab.")
                
                except Exception as ex:
                    st.error(f"Failed to load backup codes: {ex}")
            
            # ==================== TAB 2: REGENERATE BACKUP CODES ====================
            with tab2:
                st.markdown("##### Regenerate Backup Codes")
                st.warning("⚠️ This will **invalidate** all your old backup codes")
                
                st.markdown("""
                **When should you regenerate backup codes?**
                - You've used most of your backup codes
                - You suspect your codes may have been compromised
                - You want to refresh your codes for security
                """)
                
                # Initialize state
                if "new_backup_codes" not in st.session_state:
                    st.session_state.new_backup_codes = None
                
                # Generate button
                if st.session_state.new_backup_codes is None:
                    if st.button("🔐 Generate New Backup Codes", key="btn_regen_backup_codes", type="primary"):
                        try:
                            with get_session() as s:
                                new_codes = TwoFactorAuth.regenerate_backup_codes(s, user["id"])
                            
                            st.session_state.new_backup_codes = new_codes
                            
                            # Log audit
                            from security import SecurityManager
                            with get_session() as s:
                                SecurityManager.log_audit(
                                    s, user["username"], "2FA_BACKUP_CODES_REGENERATED",
                                    user_id=user["id"]
                                )
                            
                            _st_safe_rerun()
                        
                        except Exception as ex:
                            st.error(f"Failed to regenerate codes: {ex}")
                
                # Show newly generated codes
                if st.session_state.new_backup_codes is not None:
                    new_codes = st.session_state.new_backup_codes
                    
                    st.success("? New backup codes generated!")
                    st.warning("⚠️ **IMPORTANT:** Save these codes now. Old codes are no longer valid.")
                    
                    st.code("\n".join(new_codes), language=None)
                    
                    # Download and clear buttons
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        new_codes_text = "\n".join(new_codes)
                        st.download_button(
                            "⬇️ Download New Codes",
                            data=new_codes_text,
                            file_name=f"otms_backup_codes_{user['username']}_new.txt",
                            mime="text/plain",
                            key="download_new_backup_codes",
                            use_container_width=True
                        )
                    
                    with col2:
                        if st.button("? Done - Clear", key="btn_clear_new_backup_codes", use_container_width=True):
                            st.session_state.new_backup_codes = None
                            _st_safe_rerun()
            
            # ==================== TAB 3: DISABLE 2FA ====================
            with tab3:
                st.markdown("##### Disable 2FA")
                st.error("⚠️ **Warning:** Disabling 2FA makes your account less secure")
                
                st.markdown("""
                **Why you might disable 2FA:**
                - Switching to a new phone
                - Lost access to authenticator app
                - Technical issues
                
                **Important:** You can re-enable 2FA anytime after disabling.
                """)
                
                st.markdown("---")
                st.markdown("**To disable 2FA, please:**")
                st.markdown("1. Enter your current password")
                st.markdown("2. Confirm by typing your username")
                
                with st.form("disable_2fa_form"):
                    current_pwd = st.text_input(
                        "Current Password", 
                        type="password", 
                        key="disable_2fa_pwd",
                        placeholder="Enter your password"
                    )
                    
                    confirm_username = st.text_input(
                        f"Type your username ({user['username']}) to confirm", 
                        key="disable_2fa_username",
                        placeholder="Type username here"
                    )
                    
                    disable_btn = st.form_submit_button("🚫 Disable 2FA", type="primary")
                
                # Handle disable OUTSIDE form
                if disable_btn:
                    if not current_pwd:
                        st.error("? Please enter your password")
                    elif confirm_username.strip() != user["username"]:
                        st.error(f"? Username confirmation does not match. Expected: {user['username']}")
                    else:
                        try:
                            from auth import AuthManager
                            with get_session() as s:
                                u = s.query(User).filter(User.id == user["id"]).one_or_none()
                                
                                if u and AuthManager.verify_password(current_pwd, u.password_hash):
                                    # Disable 2FA
                                    TwoFactorAuth.disable_2fa(s, user["id"])
                                    
                                    st.success("? 2FA disabled successfully")
                                    
                                    # Log audit
                                    from security import SecurityManager
                                    SecurityManager.log_audit(
                                        s, user["username"], "2FA_DISABLED",
                                        user_id=user["id"],
                                        details="User disabled 2FA"
                                    )
                                    
                                    # Clear any 2FA-related session states
                                    st.session_state.pop("backup_codes_visible", None)
                                    st.session_state.pop("new_backup_codes", None)
                                    
                                    import time
                                    time.sleep(2)
                                    _st_safe_rerun()
                                else:
                                    st.error("? Invalid password")
                        
                        except Exception as ex:
                            st.error(f"Failed to disable 2FA: {ex}")
    
    # ========================= LOGIN HISTORY PAGE =========================