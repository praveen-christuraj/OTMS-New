"""
Auto-generated module for the 'Manage Locations' page.
"""
from __future__ import annotations
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
from db import get_session
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
        if st.session_state.get("auth_user", {}).get("role") != "admin-operations":
            header("Manage Locations")
            st.error("You do not have permission to access this page. Admin-Operations only.")
            st.stop()
    
        header("Manage Locations")
        current_user = st.session_state.get("auth_user") or {}
        
        tab1, tab2 = st.tabs(["View Locations", "Add Location"])
        
        # -------- View Locations --------
        with tab1:
            st.markdown("### All Locations")
            
            try:
                from location_manager import LocationManager
                with get_session() as s:
                    locations = LocationManager.get_all_locations(s, active_only=False)
                
                if locations:
                    # Display as table
                    data = []
                    for loc in locations:
                        stats = LocationManager.get_location_stats(s, loc.id)
                        data.append({
                            "ID": loc.id,
                            "Code": loc.code,
                            "Name": loc.name,
                            "Address": loc.address or "-",
                            "Status": "? Active" if loc.is_active else "? Inactive",
                            "Tanks": stats.get("tanks", 0),
                            # REMOVED: "YADE Barges": stats.get("yade_barges", 0),
                            "Transactions": stats.get("tank_transactions", 0),
                            "Voyages": stats.get("yade_voyages", 0),  # YADE voyages, not barges
                        })
                    
                    df = pd.DataFrame(data)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    
                    # -------- Edit/Delete Location --------
                    st.markdown("---")
                    st.markdown("#### Edit Location")
                    
                    col1, col2 = st.columns([0.6, 0.4])
                    
                    with col1:
                        loc_options = {f"{loc.name} ({loc.code})": loc.id for loc in locations}
                        selected_loc = st.selectbox(
                            "Select Location to Edit",
                            options=list(loc_options.keys()),
                            key="edit_loc_select"
                        )
                        
                        if selected_loc:
                            loc_id = loc_options[selected_loc]
                            with get_session() as s:
                                loc = LocationManager.get_location_by_id(s, loc_id)
                            
                            if loc:
                                e1, e2 = st.columns(2)
                                with e1:
                                    new_name = st.text_input("Name", value=loc.name, key="edit_loc_name")
                                    new_code = st.text_input("Code", value=loc.code, key="edit_loc_code")
                                with e2:
                                    new_address = st.text_area("Address", value=loc.address or "", key="edit_loc_address")
                                    new_status = st.selectbox(
                                        "Status",
                                        ["Active", "Inactive"],
                                        index=0 if loc.is_active else 1,
                                        key="edit_loc_status"
                                    )
                                
                                if st.button("💾 Save Changes", key="edit_loc_save"):
                                    try:
                                        with get_session() as s:
                                            updated_loc = LocationManager.update_location(
                                                s,
                                                loc_id,
                                                name=new_name,
                                                code=new_code,
                                                address=new_address if new_address else None,
                                                is_active=(new_status == "Active")
                                            )
                                        st.success(f"Location '{updated_loc['name']}' updated successfully!")
                                        _st_safe_rerun()
                                    except Exception as ex:
                                        st.error(f"Failed to update location: {ex}")
                    
                    with col2:
                        st.markdown("#### Deactivate Location")
                        st.caption("ℹ️ This will make the location inaccessible but preserve all data.")
                        
                        deact_loc = st.selectbox(
                            "Select Location",
                            options=list(loc_options.keys()),
                            key="deact_loc_select"
                        )
                        
                        confirm_text = st.text_input(
                            "Type location code to confirm",
                            key="deact_confirm"
                        )
                        
                        if st.button("⚠️ Deactivate Location", key="deact_btn"):
                            if deact_loc:
                                loc_id = loc_options[deact_loc]
                                with get_session() as s:
                                    loc = LocationManager.get_location_by_id(s, loc_id)
                                
                                if loc and confirm_text.strip().upper() == loc.code:
                                    try:
                                        with get_session() as s:
                                            if loc:
                                                _archive_payload_for_delete(
                                                    s,
                                                    "LocationDeactivate",
                                                    str(loc.id),
                                                    payload=RecycleBinManager.snapshot_record(loc),
                                                    reason=f"Location {loc.code} deactivated by {current_user.get('username', 'unknown')}.",
                                                    label=f"{loc.name} ({loc.code})",
                                                )
                                            LocationManager.delete_location(s, loc_id)
                                        st.success(f"Location '{loc.name}' deactivated.")
                                        _st_safe_rerun()
                                    except Exception as ex:
                                        st.error(f"Failed to deactivate: {ex}")
                                else:
                                    st.error("Confirmation code does not match.")
    
                    # Add this NEW section below the deactivate section
                    st.markdown("---")
                    st.markdown("#### ⚠️ DANGER ZONE: Permanent Delete")
    
                    with st.expander("🗑️ Permanently Delete Location (Irreversible)", expanded=False):
                        st.error("""
                        **⚠️ WARNING: This action is IRREVERSIBLE!**
                        
                        This will **permanently delete**:
                        - The location itself
                        - All tanks and tank calibration data
                        - All YADE barges and YADE calibration data
                        - All tank transactions
                        - All YADE voyages and related data
                        - All OTR records
                        
                        **This cannot be undone!**
                        """)
                        
                        perm_del_loc = st.selectbox(
                            "Select Location to DELETE PERMANENTLY",
                            options=list(loc_options.keys()),
                            key="perm_del_loc_select"
                        )
                        
                        st.markdown("##### Triple Confirmation Required")
                        
                        perm_confirm_1 = st.text_input(
                            "1️⃣ Type the location CODE exactly",
                            key="perm_confirm_1"
                        )
                        
                        perm_confirm_2 = st.text_input(
                            '2️⃣ Type "DELETE PERMANENTLY" to confirm',
                            key="perm_confirm_2"
                        )
                        
                        perm_confirm_3 = st.checkbox(
                            "3️⃣ I understand this action is irreversible and will delete ALL data",
                            key="perm_confirm_3"
                        )
                        
                        # Show what will be deleted
                        if perm_del_loc:
                            loc_id = loc_options[perm_del_loc]
                            try:
                                with get_session() as s:
                                    loc = LocationManager.get_location_by_id(s, loc_id)
                                    stats = LocationManager.get_location_stats(s, loc_id)
                                
                                if loc:
                                    st.markdown("##### ⚠️ Data that will be DELETED:")
                                    st.write({
                                        "Location": f"{loc.name} ({loc.code})",
                                        "Tanks": stats.get("tanks", 0),
                                        "YADE Barges": stats.get("yade_barges", 0),
                                        "Tank Transactions": stats.get("tank_transactions", 0),
                                        "YADE Voyages": stats.get("yade_voyages", 0),
                                    })
                            except Exception:
                                pass
                        
                        if st.button("🗑️ PERMANENTLY DELETE LOCATION", key="perm_del_btn", type="primary"):
                            if not perm_del_loc:
                                st.error("Please select a location.")
                            else:
                                loc_id = loc_options[perm_del_loc]
                                with get_session() as s:
                                    loc = LocationManager.get_location_by_id(s, loc_id)
                                
                                # Validate all three confirmations
                                if not loc:
                                    st.error("Location not found.")
                                elif perm_confirm_1.strip().upper() != loc.code:
                                    st.error("? Location code does not match.")
                                elif perm_confirm_2.strip() != "DELETE PERMANENTLY":
                                    st.error('? You must type "DELETE PERMANENTLY" exactly.')
                                elif not perm_confirm_3:
                                    st.error("? You must check the confirmation checkbox.")
                                else:
                                    # All confirmations passed - proceed with deletion
                                    try:
                                        with get_session() as s:
                                            loc_snapshot = LocationManager.get_location_by_id(s, loc_id)
                                            stats_snapshot = LocationManager.get_location_stats(s, loc_id)
                                            if loc_snapshot:
                                                payload = RecycleBinManager.snapshot_record(loc_snapshot)
                                                payload["stats"] = stats_snapshot
                                                _archive_payload_for_delete(
                                                    s,
                                                    "Location",
                                                    str(loc_snapshot.id),
                                                    payload=payload,
                                                    reason=f"Permanent delete requested by {current_user.get('username', 'unknown')}.",
                                                    label=f"{loc_snapshot.name} ({loc_snapshot.code})",
                                                )
                                            deletion_stats = LocationManager.permanently_delete_location(s, loc_id)
                                        
                                        st.success(f"? Location '{deletion_stats['location_name']}' permanently deleted.")
                                        st.json(deletion_stats)
                                        
                                        # Clear active location if it was the deleted one
                                        if st.session_state.get("active_location_id") == loc_id:
                                            st.session_state.pop("active_location_id", None)
                                        
                                        st.info("Reloading page in 3 seconds...")
                                        import time
                                        time.sleep(3)
                                        _st_safe_rerun()
                                        
                                    except Exception as ex:
                                        st.error(f"Failed to permanently delete location: {ex}")
                                        import traceback
                                        st.code(traceback.format_exc())
                else:
                    st.info("No locations found. Add one in the 'Add Location' tab.")
            
            except Exception as ex:
                st.error(f"Error loading locations: {ex}")
        
        # -------- Add Location --------
        with tab2:
            st.markdown("### Add New Location")
            
            with st.form("add_location_form"):
                c1, c2 = st.columns(2)
                with c1:
                    loc_name = st.text_input("Location Name *", placeholder="e.g., Port Harcourt Terminal")
                    loc_code = st.text_input("Location Code *", placeholder="e.g., PHT").upper()
                with c2:
                    loc_address = st.text_area("Address", placeholder="Optional")
                
                submitted = st.form_submit_button("? Add Location", type="primary")
    
                if submitted:
                    if not loc_name.strip():
                        st.error("Location name is required.")
                    elif not loc_code.strip():
                        st.error("Location code is required.")
                    else:
                        try:
                            from location_manager import LocationManager
                            with get_session() as s:
                                new_loc = LocationManager.create_location(
                                    s,
                                    name=loc_name.strip(),
                                    code=loc_code.strip(),
                                    address=loc_address.strip() if loc_address else None
                                )
                            # new_loc is now a dict, not an ORM object
                            st.success(f"? Location '{new_loc['name']}' ({new_loc['code']}) created successfully!")
                            _st_safe_rerun()
                        except Exception as ex:
                            st.error(f"Failed to create location: {ex}")
    
    # ========================= MANAGE USERS (Admin Ops/IT) =========================
