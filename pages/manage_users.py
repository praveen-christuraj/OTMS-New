"""
Auto-generated module for the 'Manage Users' page.
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
            header("Manage Users")
            st.error("You do not have permission to access this page. Admin-Operations or Admin-IT only.")
            st.stop()
    
        header("Manage Users")
        
        tab1, tab2 = st.tabs(["View Users", "Add User"])
        
        # -------- View Users --------
        with tab1:
            st.markdown("### All Users")
            
            try:
                with get_session() as s:
                    from models import User, Location
                    users = s.query(User).order_by(User.username).all()
                    
                    if users:
                        data = []
                        for u in users:
                            loc_name = "-"
                            if u.location_id:
                                loc = s.query(Location).filter(Location.id == u.location_id).one_or_none()
                                if loc:
                                    loc_name = f"{loc.name} ({loc.code})"
                            
                            data.append({
                                "ID": u.id,
                                "Username": u.username,
                                "Full Name": u.full_name or "�",
                                "Role": u.role.title(),
                                "Location": loc_name if u.role not in ["admin-operations", "admin-it", "manager"] else ("All Locations" if u.role in ["admin-operations", "manager"] else "System Admin"),
                                "Status": "? Active" if u.is_active else "? Inactive",
                                "Supervisor Code": ("Set" if u.supervisor_code_hash else "Not set") if u.role == "supervisor" else "-",
                                "Last Login": u.last_login.strftime("%Y-%m-%d %H:%M") if u.last_login else "Never"
                            })
                        
                        df = pd.DataFrame(data)
                        st.dataframe(df, use_container_width=True, hide_index=True)
                        
                        # Create user_options ONCE here
                        user_options = {u.username: u.id for u in users}
                        
                        # -------- User Management Actions --------
                        st.markdown("---")
                        st.markdown("#### User Management")
                        
                        col1, col2, col3, col4 = st.columns([0.25, 0.25, 0.25, 0.25])
                        
                        # ========== Column 1: Reset Password ==========
                        with col1:
                            st.markdown("##### Reset Password")
                            
                            selected_user_pwd = st.selectbox(
                                "Select User",
                                options=list(user_options.keys()),
                                key="mgmt_reset_pwd_user"  # UNIQUE KEY
                            )
                            
                            new_password = st.text_input(
                                "New Password",
                                type="password",
                                key="mgmt_reset_pwd_new"  # UNIQUE KEY
                            )
                            
                            confirm_password = st.text_input(
                                "Confirm Password",
                                type="password",
                                key="mgmt_reset_pwd_confirm"  # UNIQUE KEY
                            )
                            
                            if st.button("🔑 Reset Password", key="mgmt_reset_pwd_btn"):  # UNIQUE KEY
                                if not new_password:
                                    st.error("Password cannot be empty.")
                                elif new_password != confirm_password:
                                    st.error("Passwords do not match.")
                                elif len(new_password) < 4:
                                    st.error("Password must be at least 4 characters.")
                                else:
                                    try:
                                        from auth import AuthManager
                                        with get_session() as s:
                                            user_id = user_options[selected_user_pwd]
                                            AuthManager.update_password(s, user_id, new_password)
                                        st.success(f"Password reset for user '{selected_user_pwd}'.")
                                    except Exception as ex:
                                        st.error(f"Failed to reset password: {ex}")
    
                            supervisor_usernames = [u.username for u in users if u.role == "supervisor"]
                            st.markdown("##### Reset Supervisor Code")
                            if not supervisor_usernames:
                                st.info("No supervisors available.")
                            else:
                                selected_supervisor = st.selectbox(
                                    "Supervisor",
                                    options=supervisor_usernames,
                                    key="mgmt_reset_sup_user",
                                )
                                sup_new_code = st.text_input(
                                    "New Supervisor Code",
                                    type="password",
                                    key="mgmt_reset_sup_code",
                                )
                                sup_confirm_code = st.text_input(
                                    "Confirm Supervisor Code",
                                    type="password",
                                    key="mgmt_reset_sup_code_confirm",
                                )
                                if st.button("🔑 Reset Supervisor Code", key="mgmt_reset_sup_btn"):
                                    if not sup_new_code:
                                        st.error("Supervisor code cannot be empty.")
                                    elif sup_new_code != sup_confirm_code:
                                        st.error("Supervisor codes do not match.")
                                    else:
                                        try:
                                            from auth import AuthManager
                                            with get_session() as s:
                                                user_id = user_options[selected_supervisor]
                                                AuthManager.set_supervisor_code(s, user_id, sup_new_code)
                                            SecurityManager.log_audit(
                                                None,
                                                (st.session_state.get("auth_user") or {}).get("username", "admin"),
                                                "SUPERVISOR_CODE_RESET",
                                                resource_type="User",
                                                resource_id=str(user_id),
                                                details=f"Supervisor code reset for {selected_supervisor}",
                                            )
                                            st.success(f"Supervisor code reset for '{selected_supervisor}'.")
                                            _st_safe_rerun()
                                        except Exception as ex:
                                            st.error(f"Failed to reset supervisor code: {ex}")
                        
                        # ========== Column 2: Transfer User Location ==========
                        with col2:
                            st.markdown("##### Transfer User")
                            st.caption("Change user's assigned location")
                            
                            transfer_user = st.selectbox(
                                "Select User to Transfer",
                                options=list(user_options.keys()),
                                key="mgmt_transfer_user_select"  # UNIQUE KEY
                            )
                            
                            # Show current location
                            if transfer_user:
                                user_id = user_options[transfer_user]
                                try:
                                    with get_session() as s:
                                        from models import User, Location
                                        u = s.query(User).filter(User.id == user_id).one_or_none()
                                        if u:
                                            if u.role in ["admin-operations", "admin-it", "manager"]:
                                                st.info("Admin and manager users have access to all locations (no transfer needed).")
                                            else:
                                                current_loc = "Not assigned"
                                                if u.location_id:
                                                    loc = s.query(Location).filter(Location.id == u.location_id).one_or_none()
                                                    if loc:
                                                        current_loc = f"{loc.name} ({loc.code})"
                                                
                                                st.caption(f"Current: **{current_loc}**")
                                                
                                                # Get available locations
                                                from location_manager import LocationManager
                                                locs = LocationManager.get_all_locations(s, active_only=True)
                                                
                                                if locs:
                                                    loc_options = {f"{loc.name} ({loc.code})": loc.id for loc in locs}
                                                    new_location = st.selectbox(
                                                        "New Location",
                                                        options=list(loc_options.keys()),
                                                        key="mgmt_transfer_new_location"  # UNIQUE KEY
                                                    )
                                                    
                                                    if st.button("🔄 Transfer User", key="mgmt_transfer_btn"):  # UNIQUE KEY
                                                        try:
                                                            from auth import AuthManager
                                                            new_loc_id = loc_options[new_location]
                                                            
                                                            with get_session() as s:
                                                                result = AuthManager.transfer_user_to_location(s, user_id, new_loc_id)
                                                            
                                                            st.success(f"? User '{transfer_user}' transferred!")
                                                            st.info(f"From: {result['old_location']}\nTo: {result['new_location']}")
                                                            
                                                            # If transferred user is currently logged in, update their session
                                                            current_user = st.session_state.get("auth_user")
                                                            if current_user and current_user.get("id") == user_id:
                                                                st.warning("ℹ️ You transferred yourself! Please re-login to see the new location.")
                                                                st.session_state.active_location_id = new_loc_id
                                                            
                                                            import time
                                                            time.sleep(2)
                                                            _st_safe_rerun()
                                                            
                                                        except Exception as ex:
                                                            st.error(f"Failed to transfer user: {ex}")
                                except Exception as ex:
                                    st.error(f"Error loading user details: {ex}")
                        
                        # ========== Column 3: Toggle Active Status ==========
                        with col3:
                            st.markdown("##### Toggle Status")
                            st.caption("ℹ️ Deactivated users cannot login.")
                            
                            deact_user = st.selectbox(
                                "Select User",
                                options=list(user_options.keys()),
                                key="mgmt_deact_user_select"  # UNIQUE KEY
                            )
                            
                            if st.button("🔄 Toggle Active Status", key="mgmt_deact_user_btn"):  # UNIQUE KEY
                                try:
                                    from auth import AuthManager
                                    with get_session() as s:
                                        user_id = user_options[deact_user]
                                        result = AuthManager.toggle_user_status(s, user_id)
                                        status = "activated" if result["is_active"] else "deactivated"
                                        st.success(f"User '{deact_user}' {status}.")
                                        _st_safe_rerun()
                                except Exception as ex:
                                    st.error(f"Failed to update user: {ex}")
    
                        # ========== Column 4: Reset 2FA ==========
                        with col4:
                            st.markdown("##### Reset 2FA")
                            st.caption("Clear authenticator keys so the user can re-enroll.")
    
                            reset_2fa_user = st.selectbox(
                                "Select User",
                                options=list(user_options.keys()),
                                key="mgmt_reset_2fa_user",
                            )
    
                            if st.button("🔐 Reset 2FA", key="mgmt_reset_2fa_btn"):
                                if not reset_2fa_user:
                                    st.error("Please select a user.")
                                else:
                                    try:
                                        with get_session() as s:
                                            from models import User
    
                                            user_id = user_options[reset_2fa_user]
                                            target = (
                                                s.query(User)
                                                .filter(User.id == user_id)
                                                .one_or_none()
                                            )
                                            if not target:
                                                st.error("User not found.")
                                            else:
                                                target.totp_secret = None
                                                target.totp_enabled = False
                                                target.backup_codes = None
                                                s.commit()
                                                SecurityManager.log_audit(
                                                    s,
                                                    (st.session_state.get("auth_user") or {}).get(
                                                        "username", "admin"
                                                    ),
                                                    "2FA_RESET",
                                                    resource_type="User",
                                                    resource_id=str(user_id),
                                                    details=f"Admin reset 2FA for {reset_2fa_user}",
                                                )
                                                st.success(
                                                    f"2FA has been reset for '{reset_2fa_user}'. "
                                                    "They will be prompted to set up 2FA again on next login."
                                                )
                                    except Exception as ex:
                                        st.error(f"Failed to reset 2FA: {ex}")
                        
                        # -------- CHANGE USER ROLE --------
                        st.markdown("---")
                        st.markdown("#### 🔄 Change User Role")
                        st.caption("Update user roles (e.g., promote operator to supervisor)")
                        
                        role_col1, role_col2 = st.columns([0.5, 0.5])
                        
                        # Initialize variables
                        current_user_for_role = None
                        new_role = None
                        role_change_user_id = None
                        
                        with role_col1:
                            change_role_user = st.selectbox(
                                "Select User",
                                options=list(user_options.keys()),
                                key="mgmt_change_role_user_select"
                            )
                            
                            # Show current role
                            if change_role_user:
                                role_change_user_id = user_options[change_role_user]
                                try:
                                    with get_session() as s:
                                        from models import User
                                        current_user_for_role = s.query(User).filter(User.id == role_change_user_id).one_or_none()
                                        if current_user_for_role:
                                            st.caption(f"Current Role: **{current_user_for_role.role.title()}**")
                                            
                                            # Role selection - don't allow changing to same role
                                            available_roles = ["admin-operations", "admin-it", "manager", "supervisor", "operator"]
                                            role_display = {
                                                "admin-operations": "🔧 Admin-Operations - Full system & operational access",
                                                "admin-it": "💻 Admin-IT - System admin (no operations)",
                                                "manager": "👔 Manager - All locations (read-only)",
                                                "supervisor": "👷 Supervisor - Can approve actions",
                                                "operator": "👤 Operator - Standard access"
                                            }
                                            
                                            role_options = [role for role in available_roles if role != current_user_for_role.role]
                                            role_display_options = [role_display[role] for role in role_options]
                                            
                                            if role_display_options:
                                                new_role_display = st.selectbox(
                                                    "New Role",
                                                    options=role_display_options,
                                                    key="mgmt_change_role_new_role"
                                                )
                                                
                                                # Extract actual role from display
                                                for role, display in role_display.items():
                                                    if display == new_role_display:
                                                        new_role = role
                                                        break
                                except Exception as ex:
                                    st.error(f"Error loading user details: {ex}")
                        
                        with role_col2:
                            st.markdown("##### Role Change Rules:")
                            st.info("""
                            **Admin-Operations Role:**
                            - Full system & operational access
                            - Not tied to any location
                            - Can manage users and make entries
                            
                            **Admin-IT Role:**
                            - System administration only
                            - Cannot access operations/reports
                            - Can manage users and system settings
                            
                            **Manager Role:**
                            - View all locations (read-only)
                            - Cannot make entries or approve tasks
                            - Not assigned to specific location
                            
                            **Supervisor Role:**
                            - Can approve operator actions
                            - Assigned to specific location
                            - Has supervisor code
                            
                            **Operator Role:**
                            - Standard data entry access
                            - Assigned to specific location
                            - Requires supervisor approval for some actions
                            """)
                            
                            if change_role_user and new_role and current_user_for_role:
                                st.warning(f"⚠️ You are about to change **{change_role_user}**'s role to **{new_role.title()}**")
                                
                                if new_role in ["admin-operations", "admin-it", "manager"]:
                                    st.warning("ℹ️ **Note:** User will lose location assignment and get broader access.")
                                elif current_user_for_role.role in ["admin-operations", "admin-it", "manager"] and new_role in ["supervisor", "operator"]:
                                    st.warning("ℹ️ **Note:** User will need to be assigned to a specific location after role change.")
                                
                                if st.button("🔄 Change Role", key="mgmt_change_role_btn", type="primary"):
                                    try:
                                        from auth import AuthManager
                                        
                                        with get_session() as s:
                                            # Get user's current info
                                            current_user_obj = s.query(User).filter(User.id == role_change_user_id).one_or_none()
                                            old_role = current_user_obj.role if current_user_obj else "unknown"
                                            
                                            # Update role
                                            result = AuthManager.update_user_details(s, role_change_user_id, role=new_role)
                                            
                                            # Log the role change
                                            SecurityManager.log_audit(
                                                s,
                                                (st.session_state.get("auth_user") or {}).get("username", "admin"),
                                                "ROLE_CHANGE",
                                                resource_type="User",
                                                resource_id=str(role_change_user_id),
                                                details=f"Changed role from {old_role} to {new_role} for user {change_role_user}",
                                            )
                                        
                                        st.success(f"? Role changed successfully for '{change_role_user}'!")
                                        st.info(f"Old Role: {old_role.title()} ? New Role: {new_role.title()}")
                                        
                                        # Special messages based on role change
                                        if new_role in ["admin-operations", "admin-it", "manager"]:
                                            st.success("✅ User now has broader system access.")
                                        elif old_role in ["admin-operations", "admin-it", "manager"]:
                                            st.warning("ℹ️ Please assign this user to a location using the 'Transfer User' option above.")
                                        elif new_role == "supervisor" and not current_user_obj.supervisor_code_hash:
                                            st.info("ℹ️ Don't forget to set a supervisor code for this user using 'Reset Supervisor Code' above.")
                                        
                                        # If the logged-in user changed their own role, warn them
                                        current_logged_user = st.session_state.get("auth_user")
                                        if current_logged_user and current_logged_user.get("id") == role_change_user_id:
                                            st.warning("⚠️ You changed your own role! Please log out and log back in for changes to take full effect.")
                                        
                                        import time
                                        time.sleep(2)
                                        _st_safe_rerun()
                                        
                                    except Exception as ex:
                                        st.error(f"Failed to change role: {ex}")
                                        log_error(f"Role change failed for user {change_role_user}: {ex}", exc_info=True)
                        
                        # -------- PERMANENT DELETE --------
                        st.markdown("---")
                        st.markdown("#### ⚠️ DANGER ZONE: Permanent Delete User")
                        
                        with st.expander("🗑️ Permanently Delete User (Irreversible)", expanded=False):
                            st.error("""
                            **⚠️ WARNING: This action is IRREVERSIBLE!**
                            
                            This will **permanently delete** the user account.
                            
                            **Notes:**
                            - Cannot delete the last active admin user
                            - User data in transaction logs will remain (created_by fields)
                            - This action cannot be undone!
                            """)
                            
                            perm_del_user = st.selectbox(
                                "Select User to DELETE PERMANENTLY",
                                options=list(user_options.keys()),
                                key="mgmt_perm_del_user_select"  # UNIQUE KEY
                            )
                            
                            st.markdown("##### Triple Confirmation Required")
                            
                            perm_confirm_1 = st.text_input(
                                "1️⃣ Type the username exactly",
                                key="mgmt_perm_user_confirm_1"  # UNIQUE KEY
                            )
                            
                            perm_confirm_2 = st.text_input(
                                '2️⃣ Type "DELETE USER" to confirm',
                                key="mgmt_perm_user_confirm_2"  # UNIQUE KEY
                            )
                            
                            perm_confirm_3 = st.checkbox(
                                "3️⃣ I understand this action is irreversible",
                                key="mgmt_perm_user_confirm_3"  # UNIQUE KEY
                            )
                            
                            # Show user details
                            if perm_del_user:
                                user_id = user_options[perm_del_user]
                                try:
                                    with get_session() as s:
                                        from models import User, Location
                                        u = s.query(User).filter(User.id == user_id).one_or_none()
                                        
                                        if u:
                                            st.markdown("##### 👤 User Details:")
                                            user_details = {
                                                "Username": u.username,
                                                "Full Name": u.full_name or "-",
                                                "Role": u.role.title(),
                                                "Status": "? Active" if u.is_active else "? Inactive",
                                            }
                                            
                                            if u.location_id:
                                                loc = s.query(Location).filter(Location.id == u.location_id).one_or_none()
                                                if loc:
                                                    user_details["Location"] = f"{loc.name} ({loc.code})"
                                            else:
                                                user_details["Location"] = "All Locations (Admin)"
                                            
                                            st.json(user_details)
                                except Exception:
                                    pass
                            
                            if st.button("🗑️ PERMANENTLY DELETE USER", key="mgmt_perm_del_user_btn", type="primary"):  # UNIQUE KEY
                                if not perm_del_user:
                                    st.error("Please select a user.")
                                else:
                                    selected_username = perm_del_user
                                    
                                    # Validate all three confirmations
                                    if perm_confirm_1.strip() != selected_username:
                                        st.error("? Username does not match.")
                                    elif perm_confirm_2.strip() != "DELETE USER":
                                        st.error('? You must type "DELETE USER" exactly.')
                                    elif not perm_confirm_3:
                                        st.error("? You must check the confirmation checkbox.")
                                    else:
                                        # All confirmations passed - proceed with deletion
                                        try:
                                            from auth import AuthManager
                                            user_id = user_options[selected_username]
                                            
                                            with get_session() as s:
                                                user_obj = s.query(User).filter(User.id == user_id).one_or_none()
                                                if user_obj:
                                                    payload = RecycleBinManager.snapshot_record(user_obj)
                                                    _archive_payload_for_delete(
                                                        s,
                                                        resource_type="User",
                                                        resource_id=str(user_obj.id),
                                                        payload=payload,
                                                        reason=f"Permanent delete triggered by {(st.session_state.get('auth_user') or {}).get('username', 'admin')}",
                                                        label=user_obj.username,
                                                    )
                                                deletion_info = AuthManager.permanently_delete_user(s, user_id)
                                            
                                            st.success(f"? User '{deletion_info['username']}' permanently deleted.")
                                            st.json(deletion_info)
                                            
                                            # If user deleted themselves, logout
                                            current_user = st.session_state.get("auth_user")
                                            if current_user and current_user.get("username") == deletion_info["username"]:
                                                st.warning("You deleted your own account. Logging out...")
                                                st.session_state.auth_user = None
                                                st.session_state.active_location_id = None
                                            
                                            st.info("Reloading page in 3 seconds...")
                                            import time
                                            time.sleep(3)
                                            _st_safe_rerun()
                                            
                                        except Exception as ex:
                                            st.error(f"Failed to permanently delete user: {ex}")
                                            import traceback
                                            st.code(traceback.format_exc())
                    
                    else:
                        st.info("No users found.")
            
            except Exception as ex:
                st.error(f"Error loading users: {ex}")
                import traceback
                st.code(traceback.format_exc())
        
        # -------- Add User --------
        with tab2:
            st.markdown("### Add New User")
            
            with st.expander("ℹ️ Role Descriptions", expanded=False):
                st.markdown("""
                **Operator:**
                - Standard data entry access
                - Assigned to specific location
                - Cannot delete or approve requests
                
                **Supervisor:**
                - Can approve operator actions and deletion requests
                - Assigned to specific location
                - Requires supervisor code
                
                **Manager:**
                - Read-only access to all locations
                - Can view reports but cannot make entries
                - Cannot approve tasks (read-only role)
                
                **Admin-IT:**
                - System administration only
                - Can manage users, audit logs, backups, 2FA
                - Cannot access operational pages or reports
                - Can approve password reset requests
                
                **Admin-Operations:**
                - Full system and operational access
                - Can manage users and make entries everywhere
                - Can approve all types of requests
                - Access to all locations
                """)
            
            with st.form("add_user_form"):
                c1, c2 = st.columns(2)
                new_supervisor_code = ""
                confirm_supervisor_code = ""
                with c1:
                    new_username = st.text_input("Username *", placeholder="e.g., john.doe")
                    new_fullname = st.text_input("Full Name *", placeholder="e.g., John Doe")
                    new_role = st.selectbox("Role *", [
                        "operator", 
                        "supervisor", 
                        "manager", 
                        "admin-it", 
                        "admin-operations"
                    ])
                
                with c2:
                    new_password = st.text_input("Password *", type="password")
                    confirm_new_password = st.text_input("Confirm Password *", type="password")
                    
                    # Location selection (only for non-admin and non-manager)
                    if new_role not in ["admin-operations", "admin-it", "manager"]:
                        try:
                            from location_manager import LocationManager
                            with get_session() as s:
                                locs = LocationManager.get_all_locations(s, active_only=True)
                            
                            if locs:
                                loc_options = {f"{loc.name} ({loc.code})": loc.id for loc in locs}
                                selected_location = st.selectbox(
                                    "Assigned Location *",
                                    options=list(loc_options.keys()),
                                    key="new_user_location"
                                )
                                new_location_id = loc_options[selected_location]
                            else:
                                st.warning("No active locations found. Create a location first.")
                                new_location_id = None
                        except Exception:
                            new_location_id = None
                    else:
                        if new_role == "admin-operations":
                            st.info("Admin-Operations users have full access to all locations.")
                        elif new_role == "admin-it":
                            st.info("Admin-IT users manage system settings only (no location access).")
                        elif new_role == "manager":
                            st.info("Managers have read-only access to all locations.")
                        new_location_id = None
                
                if new_role == "supervisor":
                    code_col1, code_col2 = st.columns(2)
                    with code_col1:
                        new_supervisor_code = st.text_input(
                            "Supervisor Code *",
                            type="password",
                            key="new_user_supervisor_code",
                        )
                    with code_col2:
                        confirm_supervisor_code = st.text_input(
                            "Confirm Supervisor Code *",
                            type="password",
                            key="new_user_supervisor_code_confirm",
                        )
    
                submitted = st.form_submit_button("? Create User", type="primary")
                
                if submitted:
                    errors = []
                    if not new_username.strip():
                        errors.append("Username is required.")
                    if not new_fullname.strip():
                        errors.append("Full name is required.")
                    if not new_password:
                        errors.append("Password is required.")
                    elif len(new_password) < 4:
                        errors.append("Password must be at least 4 characters.")
                    elif new_password != confirm_new_password:
                        errors.append("Passwords do not match.")
                    if new_role not in ["admin-operations", "admin-it", "manager"] and not new_location_id:
                        errors.append("Location is required for non-admin users.")
                    supervisor_code_value: Optional[str] = None
                    if new_role == "supervisor":
                        if not new_supervisor_code:
                            errors.append("Supervisor code is required for supervisors.")
                        elif new_supervisor_code != confirm_supervisor_code:
                            errors.append("Supervisor codes do not match.")
                        else:
                            supervisor_code_value = new_supervisor_code
                    
                    if errors:
                        for e in errors:
                            st.error(e)
                    else:
                        try:
                            from auth import AuthManager
                            with get_session() as s:
                                new_user = AuthManager.create_user(
                                    s,
                                    username=new_username.strip(),
                                    password=new_password,
                                    full_name=new_fullname.strip(),
                                    role=new_role,
                                    location_id=new_location_id,
                                    supervisor_code=supervisor_code_value,
                                )
                            st.success(f"? User '{new_user['username']}' created successfully!")
                            _st_safe_rerun()
                        except Exception as ex:
                            st.error(f"Failed to create user: {ex}")
    
    # ================= TANK TRANSACTIONS =================