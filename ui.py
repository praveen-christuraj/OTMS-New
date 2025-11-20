# ui.py
import streamlit as st
from pathlib import Path

def header(title: str):
    """Simple top bar with title and user pill (no logo)."""
    # layout columns: title | right user pill
    mid, right = st.columns([0.76, 0.24])

    # Left/Middle: Title + subtitle
    with mid:
        st.markdown(f"<h2 style='margin:0'>{title}</h2>", unsafe_allow_html=True)
        st.caption("Oil Terminal Management System")

    # Right: User pill
    with right:
        user = st.session_state.get("auth_user")
        pill = f"{user['username']} · {user['role']}" if user else "Guest"
        st.markdown(
            f"<div style='text-align:right;border:1px solid #334155;"
            f"padding:6px 10px;border-radius:999px;display:inline-block'>{pill}</div>",
            unsafe_allow_html=True,
        )

    st.divider()
