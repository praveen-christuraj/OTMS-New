"""
Helper functions shared across Streamlit page modules.

The helper functions defined here are extracted from the original monolithic
application.  They provide utilities for safely rerunning a Streamlit
application and archiving payloads to the recycle bin while logging an
audit entry.  Importing these helpers from a standalone module avoids
circular dependencies when refactored pages import them.
"""

from __future__ import annotations
import streamlit as st
from recycle_bin import RecycleBinManager
from security import SecurityManager


def st_safe_rerun() -> None:
    """Trigger a safe rerun of the Streamlit app.

    Attempts ``st.rerun`` and falls back to ``st.experimental_rerun`` if
    necessary. This helper mirrors the behaviour of the helper defined in
    the original monolithic application but is broken out here to avoid
    circular imports.
    """
    try:
        import streamlit as _stmod
        _stmod.rerun()
    except Exception:
        import streamlit as _stmod
        _stmod.experimental_rerun()


def archive_payload_for_delete(
    session,
    resource_type: str,
    resource_id: str,
    payload: dict,
    reason: str | None = None,
    label: str | None = None,
) -> any:
    """Archive a payload to the recycle bin and log the deletion.

    This function wraps ``RecycleBinManager.archive_payload`` and
    ``SecurityManager.log_audit``.  It captures the current user's context
    from Streamlit's session state and records a deletion audit entry.
    Returning the archive entry matches the behaviour of the original helper.
    """
    auth_user = st.session_state.get("auth_user") or {}
    username = auth_user.get("username", "unknown")
    user_id = auth_user.get("id")
    location_id = st.session_state.get("active_location_id")
    entry = RecycleBinManager.archive_payload(
        session,
        resource_type=resource_type,
        resource_id=resource_id,
        payload=payload,
        username=username,
        user_id=user_id,
        location_id=location_id,
        reason=reason,
        label=label,
    )
    try:
        SecurityManager.log_audit(
            session,
            username,
            "DELETE",
            resource_type=resource_type,
            resource_id=resource_id,
            details=reason
            or f"Archived {resource_type} {resource_id} payload to recycle bin.",
            user_id=user_id,
            location_id=location_id,
        )
    except Exception:
        pass
    return entry