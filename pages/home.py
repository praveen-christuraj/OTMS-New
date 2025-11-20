"""
Auto-generated module for the 'Home' page.
"""
from __future__ import annotations
import streamlit as st
from ui import header
from db import get_session
from pages.helpers import st_safe_rerun, archive_payload_for_delete

def render() -> None:
        header("Home")
    
        if st.session_state.auth_user is None:
            # ---- Login card ----
            c1, c2, c3 = st.columns([0.2, 0.6, 0.2])
            with c2:
                st.markdown("### 🛢️ Oil Terminal Management System")
                st.caption("Multi-Location Operations Management")
    
                with st.container(border=True):
                    st.markdown("#### Login")
                    username = st.text_input("Username", key="home_username")
                    password = st.text_input("Password", type="password", key="home_password")
    
                    login_btn = st.button("🔐 Login", key="home_login_btn", type="primary")
    
                    st.markdown(
                        "<div style='margin-top:6px'><a href='#' onclick='return false;'>Forgot password?</a> "
                        "<span style='opacity:0.7'>(Contact your administrator)</span></div>",
                        unsafe_allow_html=True,
                    )
    
                    if login_btn:
                        if not username.strip():
                            st.error("Please enter a username.")
                        elif not password.strip():
                            st.error("Please enter a password.")
                        else:
                            try:
                                from auth import AuthManager
                                from twofa import TwoFactorAuth
                                from ip_service import IPService
                                import platform
                                
                                # ========== GET CLIENT INFO ==========
                                client_ip = IPService.get_client_ip()
                                
                                # Build detailed user agent with system info
                                system_info = platform.system()  # Windows, Linux, Darwin (Mac)
                                python_version = platform.python_version()
                                
                                # Try to get Streamlit version
                                try:
                                    import streamlit as st_module
                                    st_version = st_module.__version__
                                except:
                                    st_version = "unknown"
                                
                                # More detailed user agent
                                user_agent = f"Streamlit/{st_version} ({system_info}; Python {python_version})"
                                
                                with get_session() as s:
                                    # ========== AUTHENTICATE WITH IP AND USER AGENT ==========
                                    authenticated_user = AuthManager.authenticate(
                                        s, 
                                        username.strip(), 
                                        password,
                                        ip_address=client_ip,
                                        user_agent=user_agent  # ? PASSING USER AGENT!
                                    )
                                    
                                    if authenticated_user:
                                        # ========== AUTHENTICATION SUCCESSFUL ==========
                                        
                                        # Check if 2FA is enabled for this user
                                        is_2fa_enabled = TwoFactorAuth.is_enabled(s, authenticated_user["id"])
                                        
                                        if is_2fa_enabled:
                                            # ========== 2FA ENABLED - REQUIRE VERIFICATION ==========
                                            st.info("🔐 Two-Factor Authentication required")
                                            
                                            # Store user in session temporarily (pending 2FA)
                                            st.session_state["pending_2fa_user"] = authenticated_user
                                            
                                            # Also store IP and user_agent for 2FA logging
                                            st.session_state["pending_2fa_ip"] = client_ip
                                            st.session_state["pending_2fa_useragent"] = user_agent
                                            
                                            # Redirect to 2FA verification page
                                            st.session_state["page"] = "2FA Verify"
                                            st_safe_rerun()
                                        else:
                                            # ========== NO 2FA - COMPLETE LOGIN ==========
                                            st.session_state.auth_user = authenticated_user
                                            
                                            # Set default active location
                                            if authenticated_user["role"] in ["admin-operations", "admin-it", "manager"]:
                                                from location_manager import LocationManager
                                                locations = LocationManager.get_all_locations(s, active_only=True)
                                                if locations:
                                                    st.session_state.active_location_id = locations[0].id
                                            else:
                                                st.session_state.active_location_id = authenticated_user.get("location_id")
                                            
                                            # Check if password change required
                                            if authenticated_user.get("must_change_password"):
                                                st.warning("🔑 You must change your password before continuing.")
                                                st.session_state["force_password_change"] = True
                                            else:
                                                st.success(f"? Welcome, {authenticated_user.get('full_name') or authenticated_user['username']}!")
                                            
                                            st_safe_rerun()
                                    else:
                                        # ========== AUTHENTICATION FAILED ==========
                                        st.error("? Invalid username or password.")
                            
                            except ValueError as ve:
                                # Handle specific errors (account locked, etc.)
                                st.error(f"? {str(ve)}")
                            
                            except Exception as ex:
                                # Handle unexpected errors
                                from logger import log_error
                                log_error(f"Login failed: {ex}", exc_info=True)
                                st.error(f"? Login failed: {ex}")
                                
                                # Show detailed error for debugging (remove in production)
                                import traceback
                                with st.expander("⚠️ Error Details (Debug Info)"):
                                    st.code(traceback.format_exc())
    
        else:
            # ---- User is logged in - show enhanced dashboard ----
            user = st.session_state.get("auth_user")
            
            if user is None:
                st.error("Session error. Please login again.")
                st.session_state.auth_user = None
                _st_safe_rerun()
                st.stop()
            
            # Check for forced password change
            if st.session_state.get("force_password_change") or user.get("must_change_password"):
                st.markdown("### 🔑 Password Change Required")
                st.warning("For security reasons, you must change your password before continuing.")
                
                with st.form("force_password_change_form"):
                    current_pwd = st.text_input("Current Password", type="password", key="force_current_pwd")
                    new_pwd = st.text_input("New Password", type="password", key="force_new_pwd")
                    confirm_pwd = st.text_input("Confirm New Password", type="password", key="force_confirm_pwd")
                    
                    st.caption("Password must contain:")
                    st.caption("• At least 8 characters")
                    st.caption("• Uppercase and lowercase letters")
                    st.caption("• At least one number")
                    st.caption("• At least one special character (!@#$%^&*...)")
                    
                    submit = st.form_submit_button("Change Password", type="primary")
                    
                    if submit:
                        if not current_pwd or not new_pwd or not confirm_pwd:
                            st.error("All fields are required.")
                        elif new_pwd != confirm_pwd:
                            st.error("New passwords do not match.")
                        else:
                            try:
                                from auth import AuthManager
                                with get_session() as s:
                                    result = AuthManager.change_password(
                                        s, user["id"], current_pwd, new_pwd
                                    )
                                
                                st.success("? Password changed successfully! You can now use the system.")
                                st.session_state["force_password_change"] = False
                                st.session_state.auth_user["must_change_password"] = False
                                _st_safe_rerun()
                                
                            except Exception as ex:
                                log_error(f"Failed to change password: {ex}", exc_info=True)
                                st.error(f"Failed to change password: {ex}")
                
                st.stop()
            
            # Location selector - for Admin AND Lagos (HO) users
            from permission_manager import PermissionManager
    
            is_lagos_ho = PermissionManager.is_lagos_ho_user(user)
            global_view = user.get("role") in ["admin-operations", "manager"]
    
            if global_view or is_lagos_ho:
                from location_manager import LocationManager
                with get_session() as s:
                    accessible_locs = PermissionManager.get_accessible_locations_for_user(s, user)
                    
                if accessible_locs:
                    loc_options = {f"{loc['name']} ({loc['code']})": loc['id'] for loc in accessible_locs}
                    
                    current_loc_id = st.session_state.get("active_location_id")
                    current_idx = 0
                    if current_loc_id:
                        loc_ids = list(loc_options.values())
                        if current_loc_id in loc_ids:
                            current_idx = loc_ids.index(current_loc_id)
                    
                    if global_view:
                        label = "📍 Active Location (Admin/Manager View)"
                    else:
                        label = f"📍 Active Location ({user.get('full_name')} - Lagos HO)"
                    
                    selected = st.selectbox(
                        label,
                        options=list(loc_options.keys()),
                        index=current_idx,
                        key="location_selector"
                    )
                    
                    st.session_state.active_location_id = loc_options[selected]
                    
                    # Show info about access level
                    if is_lagos_ho:
                        if user.get("role") == "supervisor":
                            st.info("📍 **Lagos (HO) Supervisor:** Full access to all locations (create, view, delete)")
                        else:
                            st.info("📍 **Lagos (HO) Operator:** Can create and view entries at all locations (cannot delete)")
                else:
                    st.warning("No active locations found. Please add a location in Manage Locations.")
            else:
                # Non-admin, non-HO users: auto-set to their assigned location
                if user.get("location_id"):
                    st.session_state.active_location_id = user["location_id"]
                    
                    with get_session() as s:
                        from location_manager import LocationManager
                        loc = LocationManager.get_location_by_id(s, user["location_id"])
                        if loc:
                            st.info(f"📍 **Your Location:** {loc.name} ({loc.code})")
                else:
                    st.error("⚠️ Your account is not assigned to any location. Please contact your administrator.")
                    st.stop()
            
    # ========================= HOME DASHBOARD =========================
