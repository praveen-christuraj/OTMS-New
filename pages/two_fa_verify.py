"""
Streamlit page for handling two‑factor authentication (2FA) verification.

This module encapsulates all of the UI and logic required to verify a pending
2FA login attempt. It is extracted from the monolithic ``oil_app_ui.py`` file
to make the codebase easier to navigate and maintain. The function
``render()`` should be invoked by the main application when the current
page is set to ``"2FA Verify"``. It assumes that Streamlit's
``st.session_state`` contains a ``pending_2fa_user`` dictionary with the
details of the user awaiting verification.

Unlike the original inline implementation, this module includes its own
implementations of ``clear_2fa_session_states`` and ``_st_safe_rerun`` to
avoid circular imports. These helper functions perform the same duties as
their counterparts in the main app: cleaning up 2FA‑related keys in
``st.session_state`` and rerunning the app safely without raising
``DeltaGenerator.rerun`` exceptions.
"""

from __future__ import annotations

import time
import traceback
import streamlit as st

# Import the header helper from the shared ``ui`` package.  This provides
# consistent page headings throughout the application.
from ui import header

# Database session helper
from db import get_session

def clear_2fa_session_states() -> None:
    """Clear all 2FA‑related session state keys.

    When a user completes or cancels the 2FA process the application should
    discard any temporary state values related to the 2FA workflow. This
    implementation mirrors the one found in the original ``oil_app_ui.py``.
    """
    keys_to_clear = [
        "pending_2fa_user",
        "backup_codes_visible",
        "new_backup_codes",
        "2fa_setup",
        "2fa_backup_codes_ready",
        "show_backup_codes",
    ]
    for key in keys_to_clear:
        st.session_state.pop(key, None)


def _st_safe_rerun() -> None:
    """Trigger a safe rerun of the Streamlit app.

    Streamlit's ``st.rerun`` sometimes raises ``DeltaGenerator.rerun`` when
    invoked from within certain contexts. This helper attempts ``st.rerun``
    first and falls back to ``st.experimental_rerun`` if necessary. The
    implementation matches the helper used in the original application.
    """
    try:
        import streamlit as _stmod  # noqa: F401
        _stmod.rerun()
    except Exception:
        import streamlit as _stmod  # noqa: F401
        _stmod.experimental_rerun()


def render() -> None:
    """Render the Two‑Factor Authentication verification page.

    This function encapsulates the UI and business logic for verifying a
    pending 2FA token. It reads the ``pending_2fa_user`` from
    ``st.session_state``, prompts the user to enter a 6‑digit code or backup
    code, validates the code via the ``TwoFactorAuth`` service and logs
    audit events via the ``SecurityManager``. Upon success or failure it
    updates the session state appropriately and reruns the app.
    """
    header("Two-Factor Authentication")

    # Safely obtain the user awaiting verification. If there is no such user
    # we redirect back to the home page and clear any stale 2FA data.
    pending_user = st.session_state.get("pending_2fa_user")
    if not pending_user:
        st.error("❓ No pending login found. Please login first.")
        st.info("↩️ Redirecting to Home page...")
        clear_2fa_session_states()
        st.session_state["page"] = "Home"
        time.sleep(2)
        _st_safe_rerun()
        st.stop()

    # Prompt for verification code.
    st.markdown("### 🔐 Enter Verification Code")

    col1, col2, col3 = st.columns([0.3, 0.4, 0.3])
    with col2:
        st.info(f"**User:** {pending_user.get('username', 'Unknown')}")
        st.caption("Enter the 6-digit code from your authenticator app")

        with st.form("2fa_verify_form"):
            token = st.text_input(
                "Verification Code",
                max_chars=10,
                placeholder="000000 or XXXX-XXXX",
                key="2fa_token_input",
                help="Enter code from Microsoft Authenticator or your backup codes",
            )

            col_verify, col_cancel = st.columns(2)
            with col_verify:
                verify_btn = st.form_submit_button("✅ Verify", type="primary", use_container_width=True)
            with col_cancel:
                cancel_btn = st.form_submit_button("❌ Cancel", use_container_width=True)

            # Handle verification attempt
            if verify_btn:
                if not token.strip():
                    st.error("❓ Please enter a verification code")
                else:
                    try:
                        from twofa import TwoFactorAuth
                        from ip_service import IPService
                        from security import SecurityManager

                        # Obtain client IP address for logging
                        client_ip = IPService.get_client_ip()
                        with get_session() as s:
                            # Normalise the token (remove spaces and dashes)
                            clean_token = token.strip().replace("-", "").replace(" ", "")
                            is_valid = TwoFactorAuth.verify_token(
                                s,
                                pending_user["id"],
                                clean_token,
                            )
                            if is_valid:
                                # Set authenticated user and remove pending state
                                st.session_state.auth_user = pending_user
                                st.session_state.pop("pending_2fa_user", None)

                                # Assign default location depending on role
                                if pending_user.get("role") in ["admin-operations", "admin-it", "manager"]:
                                    from location_manager import LocationManager
                                    locations = LocationManager.get_all_locations(s, active_only=True)
                                    if locations:
                                        st.session_state.active_location_id = locations[0].id
                                else:
                                    st.session_state.active_location_id = pending_user.get("location_id")

                                # Log successful 2FA verification
                                SecurityManager.log_audit(
                                    s,
                                    pending_user["username"],
                                    "2FA_SUCCESS",
                                    user_id=pending_user["id"],
                                    details="2FA verification successful",
                                    ip_address=client_ip,
                                )
                                SecurityManager.log_login_attempt(
                                    s,
                                    pending_user["username"],
                                    success=True,
                                    ip_address=client_ip,
                                    user_agent="Streamlit App",
                                    two_factor_used=True,
                                )
                                st.success("✅ Verification successful!")
                                st.info("↩️ Redirecting to Home...")
                                st.session_state["page"] = "Home"
                                time.sleep(1)
                                _st_safe_rerun()
                            else:
                                # Invalid token: log attempt and inform user
                                st.error("❓ Invalid verification code. Please try again.")
                                SecurityManager.log_audit(
                                    s,
                                    pending_user["username"],
                                    "2FA_FAILED",
                                    user_id=pending_user["id"],
                                    details="Invalid 2FA token",
                                    success=False,
                                    ip_address=client_ip,
                                )
                                st.warning("⚠️ Make sure you're entering the current code from your app")
                    except Exception as ex:
                        # Catch and display unexpected errors
                        st.error(f"❓ Verification failed: {ex}")
                        st.code(traceback.format_exc())

            # Handle cancellation: clear state and return to Home
            if cancel_btn:
                st.session_state.pop("pending_2fa_user", None)
                st.session_state["page"] = "Home"
                st.info("❌ Login cancelled")
                time.sleep(1)
                _st_safe_rerun()

        # Separator line
        st.markdown("---")

        # Help & troubleshooting section
        with st.expander("❓ Help & Troubleshooting", expanded=False):
            st.markdown(
                """
                **Using Authenticator App:**
                - Open Microsoft Authenticator (or your 2FA app)
                - Find "OTMS" account
                - Enter the current 6-digit code
                - Code refreshes every 30 seconds

                **Using Backup Code:**
                - Enter one of your saved backup codes
                - Format: XXXX-XXXX (8 characters)
                - Each code can only be used once

                **Lost Access?**
                - Contact your system administrator
                - They can disable 2FA for your account
                - You'll need to set up 2FA again

                **Time Sync Issues?**
                - Make sure your device time is correct
                - Authenticator apps need accurate time to work
                """
            )
        st.caption("🔐 Your account is protected with Two-Factor Authentication")