"""
Auto-generated module for the 'Location Settings' page.
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
        if st.session_state.get("auth_user", {}).get("role") != "admin-operations":
            header("Location Settings")
            st.error("You do not have permission to access this page. Admin-Operations only.")
            st.stop()
    
        header("Location Settings")
        st.markdown("### Configure Location-Specific Settings")
        
        from location_config import LocationConfig
        from location_manager import LocationManager
        import json
        
        # Select location
        with get_session() as s:
            locations = LocationManager.get_all_locations(s, active_only=True)
        
        if not locations:
            st.warning("No locations found. Add a location first.")
            st.stop()
        
        loc_options = {f"{loc.name} ({loc.code})": loc.id for loc in locations}
        
        selected_loc = st.selectbox(
            "Select Location to Configure",
            options=list(loc_options.keys()),
            key="config_location_select"
        )
        
        if selected_loc:
            location_id = loc_options[selected_loc]
            
            # Get current config
            with get_session() as s:
                current_config = LocationConfig.get_config(s, location_id)
            
            st.markdown("---")
            
            tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
                "🧭 Page Access",
                "🛢️ Tank Transactions",
                "⛴️ YADE Transactions",
                "⚙️ OTR Settings",
                "🎨 UI Customization",
                "⛴️ Vessel Assignments",
                "📑 Report Builder"
            ])
            
            with tab2:
                st.markdown("#### Tank Transaction Configuration")
                
                with st.form("tank_tx_config_form"):
                    st.markdown("##### Enabled Operations")
                    
                    all_operations = [
                        "Opening Stock",
                        "Receipt",  # NEW
                        "Receipt from Agu",  # NEW
                        "Receipt from OFS",  # NEW
                        "OKW Receipt",
                        "ANZ Receipt",
                        "Other Receipts",
                        "ITT - Receipt",
                        "Dispatch to barge",
                        "Other Dispatch",
                        "ITT - Dispatch",
                        "Settling",
                        "Draining"
                    ]
                    
                    enabled_ops = current_config["tank_transactions"]["enabled_operations"]
                    
                    selected_ops = st.multiselect(
                        "Select Enabled Operations",
                        options=all_operations,
                        default=enabled_ops,
                        key="config_tank_ops"
                    )
                    
                    st.markdown("##### Product Types")
                    
                    all_products = ["CRUDE", "CONDENSATE", "DPK", "AGO", "PMS", "LPFO", "HPFO"]
                    enabled_products = current_config["tank_transactions"]["product_types"]
                    
                    selected_products = st.multiselect(
                        "Select Product Types",
                        options=all_products,
                        default=enabled_products,
                        key="config_products"
                    )
                    
                    st.markdown("##### Validation Rules")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        max_days = st.number_input(
                            "Max Days Backward Entry",
                            min_value=1,
                            max_value=365,
                            value=current_config["tank_transactions"]["max_days_backward"],
                            key="config_max_days"
                        )
                        
                        allow_future = st.checkbox(
                            "Allow Future Dates",
                            value=current_config["tank_transactions"]["allow_future_dates"],
                            key="config_allow_future"
                        )
                    
                    with col2:
                        auto_ticket = st.checkbox(
                            "Auto-Generate Ticket ID",
                            value=current_config["tank_transactions"]["auto_generate_ticket_id"],
                            key="config_auto_ticket"
                        )
                        
                        ticket_prefix = st.text_input(
                            "Ticket ID Prefix (if auto-generate)",
                            value=current_config["tank_transactions"]["ticket_id_prefix"],
                            placeholder="e.g., PHT-",
                            key="config_ticket_prefix"
                        )
                    
                    save_tank_config = st.form_submit_button("💾 Save Tank Transaction Settings", type="primary")
                    
                    if save_tank_config:
                        # Update config
                        new_config = current_config.copy()
                        new_config["tank_transactions"]["enabled_operations"] = selected_ops
                        new_config["tank_transactions"]["product_types"] = selected_products
                        new_config["tank_transactions"]["max_days_backward"] = max_days
                        new_config["tank_transactions"]["allow_future_dates"] = allow_future
                        new_config["tank_transactions"]["auto_generate_ticket_id"] = auto_ticket
                        new_config["tank_transactions"]["ticket_id_prefix"] = ticket_prefix
                        
                        try:
                            with get_session() as s:
                                LocationConfig.save_config(s, location_id, new_config)
                            
                            st.success("? Tank Transaction settings saved!")
                            
                            # Log audit
                            user = st.session_state.get("auth_user")
                            if user:
                                from security import SecurityManager
                                with get_session() as s:
                                    SecurityManager.log_audit(
                                        s, user["username"], "UPDATE_LOCATION_CONFIG",
                                        resource_type="Location",
                                        resource_id=str(location_id),
                                        details="Updated tank transaction config",
                                        user_id=user["id"]
                                    )
                            
                            import time
                            time.sleep(1)
                            _st_safe_rerun()
                        except Exception as ex:
                            st.error(f"Failed to save: {ex}")
            
            with tab3:
                st.markdown("#### YADE Transaction Configuration")
                
                with st.form("yade_config_form"):
                    st.markdown("##### Enabled Cargo Types")
                    
                    all_cargo = ["OKW", "ANZ", "CONDENSATE", "CRUDE", "OTHER"]
                    enabled_cargo = current_config["yade_transactions"]["enabled_cargo_types"]
                    
                    selected_cargo = st.multiselect(
                        "Select Cargo Types",
                        options=all_cargo,
                        default=enabled_cargo,
                        key="config_cargo"
                    )
                    
                    st.markdown("##### Enabled Destinations")
                    
                    all_dest = [
                        "NEMBE CK", "BONNY", "BRASS", "FORCADOS",
                        "ESCRAVOS", "WARRI", "PORT HARCOURT", "OTHER"
                    ]
                    enabled_dest = current_config["yade_transactions"]["enabled_destinations"]
                    
                    selected_dest = st.multiselect(
                        "Select Destinations",
                        options=all_dest,
                        default=enabled_dest,
                        key="config_dest"
                    )
                    
                    st.markdown("##### Loading Berths")
                    
                    all_berths = ["BERTH 1", "BERTH 2", "BERTH 3", "BERTH 4", "BERTH 5"]
                    enabled_berths = current_config["yade_transactions"]["enabled_loading_berths"]
                    
                    selected_berths = st.multiselect(
                        "Select Loading Berths",
                        options=all_berths,
                        default=enabled_berths,
                        key="config_berths"
                    )
                    
                    st.markdown("##### Options")
                    
                    enable_seals = st.checkbox(
                        "Enable Seal Tracking",
                        value=current_config["yade_transactions"]["enable_seal_tracking"],
                        key="config_seals"
                    )
                    
                    auto_voyage = st.checkbox(
                        "Auto-Generate Voyage Numbers",
                        value=current_config["yade_transactions"]["auto_generate_voyage_no"],
                        key="config_auto_voyage"
                    )
                    
                    save_yade_config = st.form_submit_button("💾 Save YADE Settings", type="primary")
                    
                    if save_yade_config:
                        new_config = current_config.copy()
                        new_config["yade_transactions"]["enabled_cargo_types"] = selected_cargo
                        new_config["yade_transactions"]["enabled_destinations"] = selected_dest
                        new_config["yade_transactions"]["enabled_loading_berths"] = selected_berths
                        new_config["yade_transactions"]["enable_seal_tracking"] = enable_seals
                        new_config["yade_transactions"]["auto_generate_voyage_no"] = auto_voyage
                        
                        try:
                            with get_session() as s:
                                LocationConfig.save_config(s, location_id, new_config)
                            
                            st.success("? YADE settings saved!")
                            
                            # Log audit
                            user = st.session_state.get("auth_user")
                            if user:
                                from security import SecurityManager
                                with get_session() as s:
                                    SecurityManager.log_audit(
                                        s, user["username"], "UPDATE_LOCATION_CONFIG",
                                        resource_type="Location",
                                        resource_id=str(location_id),
                                        details="Updated YADE transaction config",
                                        user_id=user["id"]
                                    )
                            
                            import time
                            time.sleep(1)
                            _st_safe_rerun()
                        except Exception as ex:
                            st.error(f"Failed to save: {ex}")
            
            with tab4:
                st.markdown("#### OTR Calculation Settings")
                
                with st.form("otr_config_form"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        auto_calc = st.checkbox(
                            "Auto-Calculate Volumes",
                            value=current_config["otr"]["auto_calculate_volumes"],
                            key="config_auto_calc"
                        )
                        
                        require_cal = st.checkbox(
                            "Require Calibration Data",
                            value=current_config["otr"]["require_calibration_data"],
                            key="config_req_cal"
                        )
                        
                        temp_corr = st.checkbox(
                            "Enable Temperature Correction",
                            value=current_config["otr"]["enable_temperature_correction"],
                            key="config_temp_corr"
                        )
                    
                    with col2:
                        decimal_precision = st.number_input(
                            "Decimal Precision",
                            min_value=0,
                            max_value=4,
                            value=current_config["otr"]["decimal_precision"],
                            key="config_decimal"
                        )
                        
                        volume_unit = st.selectbox(
                            "Volume Unit",
                            options=["BBL", "M3"],
                            index=0 if current_config["otr"]["volume_unit"] == "BBL" else 1,
                            key="config_vol_unit"
                        )
                        
                        temp_unit = st.selectbox(
                            "Temperature Unit",
                            options=["C", "F"],
                            index=0 if current_config["otr"]["temperature_unit"] == "C" else 1,
                            key="config_temp_unit"
                        )
                    
                    save_otr_config = st.form_submit_button("💾 Save OTR Settings", type="primary")
                    
                    if save_otr_config:
                        new_config = current_config.copy()
                        new_config["otr"]["auto_calculate_volumes"] = auto_calc
                        new_config["otr"]["require_calibration_data"] = require_cal
                        new_config["otr"]["enable_temperature_correction"] = temp_corr
                        new_config["otr"]["decimal_precision"] = decimal_precision
                        new_config["otr"]["volume_unit"] = volume_unit
                        new_config["otr"]["temperature_unit"] = temp_unit
                        
                        try:
                            with get_session() as s:
                                LocationConfig.save_config(s, location_id, new_config)
                            
                            st.success("? OTR settings saved!")
                            
                            import time
                            time.sleep(1)
                            _st_safe_rerun()
                        except Exception as ex:
                            st.error(f"Failed to save: {ex}")
    
                st.markdown("#### OTR-Vessel Dropdown Control")
                try:
                    from models import Vessel
                    with get_session() as s:
                        vessel_rows = (
                            s.query(Vessel)
                            .filter(Vessel.status == "ACTIVE")
                            .order_by(Vessel.name)
                            .all()
                        )
                except Exception as ex:
                    vessel_rows = []
                    st.error(f"Failed to load vessel list: {ex}")
    
                vessel_lookup = {v.id: v.name for v in vessel_rows}
                existing_ids = current_config.get("otr_vessel", {}).get("preferred_vessel_ids", [])
    
                if not vessel_rows:
                    st.info("No active vessels found. Add vessels in Asset Management first.")
                else:
                    with st.form("otr_vessel_list_form"):
                        selected_vessels = st.multiselect(
                            "Vessels available in OTR-Vessel dropdown",
                            options=list(vessel_lookup.keys()),
                            default=[vid for vid in existing_ids if vid in vessel_lookup],
                            format_func=lambda vid: vessel_lookup.get(vid, f"Vessel #{vid}"),
                            help="Admins can add or remove vessels that appear in the OTR entry form.",
                        )
                        save_custom_vessels = st.form_submit_button("💾 Save Vessel List", type="secondary")
                        if save_custom_vessels:
                            new_config = current_config.copy()
                            otr_vessel_conf = new_config.get("otr_vessel", {}).copy()
                            otr_vessel_conf["preferred_vessel_ids"] = selected_vessels
                            new_config["otr_vessel"] = otr_vessel_conf
                            try:
                                with get_session() as s:
                                    LocationConfig.save_config(s, location_id, new_config)
                                st.success("? Vessel dropdown updated.")
                                user = st.session_state.get("auth_user")
                                import time
                                if user:
                                    with get_session() as s_audit:
                                        SecurityManager.log_audit(
                                            s_audit,
                                            user["username"],
                                            "UPDATE_LOCATION_CONFIG",
                                            resource_type="Location",
                                            resource_id=str(location_id),
                                            details="Updated OTR vessel dropdown list",
                                            user_id=user["id"],
                                        )
                                time.sleep(1)
                                _st_safe_rerun()
                            except Exception as ex:
                                st.error(f"Failed to update vessel list: {ex}")
            
            with tab5:
                st.markdown("#### UI Customization")
                
                with st.form("ui_config_form"):
                    quick_entry = st.checkbox(
                        "Show Quick Entry Mode",
                        value=current_config["ui_customization"]["show_quick_entry_mode"],
                        key="config_quick_entry"
                    )
                    
                    bulk_upload = st.checkbox(
                        "Enable Bulk Upload",
                        value=current_config["ui_customization"]["enable_bulk_upload"],
                        key="config_bulk_upload"
                    )
                    
                    default_date = st.selectbox(
                        "Default Date Selection",
                        options=["today", "manual"],
                        index=0 if current_config["ui_customization"]["default_date"] == "today" else 1,
                        key="config_default_date"
                    )
                    
                    save_ui_config = st.form_submit_button("💾 Save UI Settings", type="primary")
                    
                    if save_ui_config:
                        new_config = current_config.copy()
                        new_config["ui_customization"]["show_quick_entry_mode"] = quick_entry
                        new_config["ui_customization"]["enable_bulk_upload"] = bulk_upload
                        new_config["ui_customization"]["default_date"] = default_date
                        
                        try:
                            with get_session() as s:
                                LocationConfig.save_config(s, location_id, new_config)
                            
                            st.success("? UI settings saved!")
                            
                            import time
                            time.sleep(1)
                            _st_safe_rerun()
                        except Exception as ex:
                            st.error(f"Failed to save: {ex}")
    
            with tab1:
                st.markdown("#### Page Access Permissions")
                with st.form("page_access_form"):
                    page_vis = current_config.get("page_visibility", {})
                    existing_access = current_config.get("page_access", {})
                    management_pages = {
                        "Home",
                        "Add Asset",
                        "Manage Locations",
                        "Manage Users",
                        "Audit Log",
                        "Recycle Bin",
                        "Backup & Recovery",
                        "Location Settings",
                        "My Tasks",
                        "2FA Settings",
                        "Login History",
                        "View Transactions",
                    }
                    all_pages = [p for p in ICONS.keys() if p not in management_pages]
                    cols = st.columns(3)
                    toggled = {}
                    for idx, p in enumerate(all_pages):
                        with cols[idx % 3]:
                            toggled[p] = st.checkbox(
                                f"Allow {p}",
                                value=existing_access.get(p, True),
                                key=f"page_access_{p}"
                            )
                    st.markdown("##### Tab Access Controls")
                    tabs_access = current_config.get("tabs_access", {})
                    with st.expander("Tank Transactions", expanded=False):
                        tt_tabs = [
                            "Tank Transactions",
                            "Meter Transactions",
                            "River Draft",
                            "Produced Water",
                            "Condensate Records",
                            "Production",
                        ]
                        tt_cols = st.columns(3)
                        tt_toggled = {}
                        for i, t in enumerate(tt_tabs):
                            with tt_cols[i % 3]:
                                tt_toggled[t] = st.checkbox(
                                    t,
                                    value=tabs_access.get("Tank Transactions", {}).get(t, True),
                                    key=f"tabs_tt_{t}"
                                )
                    with st.expander("FSO-Operations", expanded=False):
                        fso_tabs = ["🧾 OTR", "📊 Material Balance"]
                        fso_cols = st.columns(2)
                        fso_toggled = {}
                        for i, t in enumerate(fso_tabs):
                            with fso_cols[i % 2]:
                                fso_toggled[t] = st.checkbox(
                                    t,
                                    value=tabs_access.get("FSO-Operations", {}).get(t, True),
                                    key=f"tabs_fso_{t}"
                                )
                    with st.expander("BCCR", expanded=False):
                        bccr_tabs = ["Mapping", "BCCR Report"]
                        bccr_cols = st.columns(2)
                        bccr_toggled = {}
                        for i, t in enumerate(bccr_tabs):
                            with bccr_cols[i % 2]:
                                bccr_toggled[t] = st.checkbox(
                                    t,
                                    value=tabs_access.get("BCCR", {}).get(t, True),
                                    key=f"tabs_bccr_{t}"
                                )
                    with st.expander("Yade-Vessel Mapping", expanded=False):
                        yvm_tabs = ["Mapping", "Comparison"]
                        yvm_cols = st.columns(2)
                        yvm_toggled = {}
                        for i, t in enumerate(yvm_tabs):
                            with yvm_cols[i % 2]:
                                yvm_toggled[t] = st.checkbox(
                                    t,
                                    value=tabs_access.get("Yade-Vessel Mapping", {}).get(t, True),
                                    key=f"tabs_yvm_{t}"
                                )
                    save_page_access = st.form_submit_button("💾 Save Access Settings", type="primary")
                    if save_page_access:
                        new_config = current_config.copy()
                        new_config["page_access"] = toggled
                        new_page_vis = page_vis.copy()
                        new_page_vis["show_tank_transactions"] = toggled.get("Tank Transactions", True)
                        new_page_vis["show_tanker_transactions"] = toggled.get("Tanker Transactions", True)
                        new_page_vis["show_yade_transactions"] = toggled.get("Yade Transactions", True)
                        new_page_vis["show_toa_yade"] = toggled.get("TOA-Yade", True)
                        new_config["page_visibility"] = new_page_vis
                        new_tabs_access = tabs_access.copy()
                        new_tabs_access["Tank Transactions"] = tt_toggled
                        new_tabs_access["FSO-Operations"] = fso_toggled
                        new_tabs_access["BCCR"] = bccr_toggled
                        new_tabs_access["Yade-Vessel Mapping"] = yvm_toggled
                        new_config["tabs_access"] = new_tabs_access
                        try:
                            with get_session() as s:
                                LocationConfig.save_config(s, location_id, new_config)
                            st.success("? Access settings saved!")
                            user = st.session_state.get("auth_user")
                            if user:
                                from security import SecurityManager
                                with get_session() as s_a:
                                    SecurityManager.log_audit(
                                        s_a,
                                        user["username"],
                                        "UPDATE_LOCATION_CONFIG",
                                        resource_type="Location",
                                        resource_id=str(location_id),
                                        details="Updated page/tab access config",
                                        user_id=user.get("id"),
                                    )
                            import time
                            time.sleep(1)
                            _st_safe_rerun()
                        except Exception as ex:
                            st.error(f"Failed to save: {ex}")
    
            # ========== TAB 6: Vessel Assignments ==========
            with tab6:
                st.markdown("#### Vessel Assignments for this Location")
                try:
                    from models import Vessel, LocationVessel
                    with get_session() as s:
                        vessels = s.query(Vessel).order_by(Vessel.name).all()
                        existing_links = (
                            s.query(LocationVessel)
                            .filter(LocationVessel.location_id == location_id)
                            .all()
                        )
                except Exception as ex:
                    vessels = []
                    existing_links = []
                    st.error(f"Failed to load vessels: {ex}")
    
                if not vessels:
                    st.info("No vessels available. Add vessels from the Asset page first.")
                else:
                    vessel_options = {v.id: v.name for v in vessels}
                    active_ids = [
                        link.vessel_id for link in existing_links if link.is_active
                    ]
                    with st.form("location_vessel_assign_form"):
                        selected_vessels = st.multiselect(
                            "Select vessels to make available at this location",
                            options=list(vessel_options.keys()),
                            default=active_ids,
                            format_func=lambda vid: vessel_options.get(vid, f"Vessel #{vid}"),
                            help="Selected vessels appear in the OTR-Vessel dropdown for this location.",
                        )
                        save_assignments = st.form_submit_button(" Save Vessel Assignments", type="primary")
                        if save_assignments:
                            try:
                                with get_session() as s:
                                    rows = (
                                        s.query(LocationVessel)
                                        .filter(LocationVessel.location_id == location_id)
                                        .all()
                                    )
                                    row_map = {row.vessel_id: row for row in rows}
                                    selected_set = set(selected_vessels)
    
                                    for vessel_id in selected_set:
                                        if vessel_id in row_map:
                                            row_map[vessel_id].is_active = True
                                        else:
                                            s.add(
                                                LocationVessel(
                                                    location_id=location_id,
                                                    vessel_id=vessel_id,
                                                    is_active=True,
                                                )
                                            )
    
                                    for vessel_id, row in row_map.items():
                                        if vessel_id not in selected_set and row.is_active:
                                            row.is_active = False
    
                                    s.commit()
                                st.success("Vessel assignments updated.")
                                import time as _t
                                _t.sleep(1)
                                _st_safe_rerun()
                            except Exception as ex:
                                st.error(f"Failed to update assignments: {ex}")
    
            # ========== TAB 7: Report Builder ==========
            with tab7:
                import json
                import pandas as pd
                from datetime import date, timedelta, datetime
                from sqlalchemy import or_
                from models import ReportDefinition
                from material_balance_calculator import MaterialBalanceCalculator as MBC
                st.markdown("#### Report Tabs")
                try:
                    with get_session() as s:
                        report_rows = (
                            s.query(ReportDefinition)
                            .filter(
                                or_(ReportDefinition.location_id == location_id, ReportDefinition.location_id.is_(None)),
                                ReportDefinition.is_active == True,
                            )
                            .order_by(ReportDefinition.name)
                            .all()
                        )
                    st.dataframe(
                        pd.DataFrame(
                            [
                                {
                                    "Name": r.name,
                                    "Slug": r.slug,
                                    "Scope": "Location" if r.location_id else "Global",
                                }
                                for r in report_rows
                            ]
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )
                except Exception as ex:
                    st.info("No report tabs found.")
    
                st.markdown("#### Create Report Tab")
                # Build location selectors (target + source)
                try:
                    from models import Location
                    with get_session() as s_loc:
                        _locs = s_loc.query(Location).order_by(Location.name).all()
                    location_label_to_id = {f"{loc.name} ({loc.code})": loc.id for loc in _locs}
                except Exception:
                    location_label_to_id = {f"(Current) {location_id}": location_id}
                if not location_label_to_id:
                    location_label_to_id = {f"(Current) {location_id}": location_id}
                location_id_to_label = {vid: lbl for lbl, vid in location_label_to_id.items()}
                location_labels = list(location_label_to_id.keys())
    
                def _loc_index_for_id(loc_id: Optional[int]) -> int:
                    lbl = location_id_to_label.get(loc_id)
                    if lbl and lbl in location_labels:
                        return location_labels.index(lbl)
                    return 0
    
                report_loc_key = st.selectbox(
                    "Report Location",
                    location_labels,
                    index=_loc_index_for_id(location_id),
                    key="rb_report_loc",
                )
                report_location_id = location_label_to_id.get(report_loc_key, location_id)
                source_loc_key = st.selectbox(
                    "Source Location",
                    location_labels,
                    index=_loc_index_for_id(report_location_id),
                    key="rb_source_loc",
                )
                src_location_id = location_label_to_id.get(source_loc_key, report_location_id)
    
                with st.form("report_builder_create"):
                    r_name = st.text_input("Name", key="rb_name")
                    r_slug = st.text_input("Slug", key="rb_slug")
                    r_scope = st.selectbox("Scope", ["Location", "Global"], index=0, key="rb_scope")
                    source_options = _available_primary_source_keys()
                    mode_opt = st.radio("Mode", ["Single Source", "Date-Merge"], index=0, key="rb_mode")
                    primary_source = None
                    source_fields: list[str] = []
                    if mode_opt == "Single Source":
                        primary_source = st.selectbox(
                            "Primary Source",
                            source_options,
                            index=0,
                            key="rb_source",
                            format_func=_format_source_option,
                        )
                        source_fields = _discover_source_fields(primary_source, src_location_id)
    
                    if mode_opt == "Single Source":
                        editor_df = pd.DataFrame([
                            {"Column Label": "Date", "Source Field": (source_fields[0] if source_fields else "")},
                        ])
                        edited = st.data_editor(
                            editor_df,
                            num_rows="dynamic",
                            use_container_width=True,
                            key=f"rb_columns_{primary_source}_{src_location_id}",
                            column_config={
                                "Source Field": st.column_config.SelectboxColumn(options=source_fields),
                            },
                        )
                    else:
                        location_options = location_labels or [""]
                        location_name_to_id = location_label_to_id
                        map_editor = st.data_editor(
                            pd.DataFrame([
                                {
                                    "Column Label": "Agge Receipt",
                                    "Source": (source_options[0] if source_options else ""),
                                    "Source Location": location_options[0] if location_options else "",
                                    "Source Field": "qty_bbls",
                                    "Date Field": "date",
                                    "Aggregate": "sum",
                                    "Operation Filter": "Receipt",
                                },
                                {
                                    "Column Label": "Asemoku Dispatch",
                                    "Source": (source_options[0] if source_options else ""),
                                    "Source Location": location_options[0] if location_options else "",
                                    "Source Field": "qty_bbls",
                                    "Date Field": "date",
                                    "Aggregate": "sum",
                                    "Operation Filter": "Dispatch",
                                },
                            ]),
                            num_rows="dynamic",
                            use_container_width=True,
                            key="rb_mappings_editor",
                            column_config={
                                "Source": st.column_config.SelectboxColumn(options=source_options, format_func=_format_source_option),
                                "Source Location": st.column_config.SelectboxColumn(options=location_options),
                                "Aggregate": st.column_config.SelectboxColumn(options=["sum", "last", "first"]),
                            },
                        )
                        calc_editor = st.data_editor(
                            pd.DataFrame([
                                {"Result Label": "", "Expression": ""},
                            ]),
                            num_rows="dynamic",
                            use_container_width=True,
                            key="rb_calc_editor",
                        )
                        st.caption("Use expressions like Column 3 - Column 2 or wrap names in [brackets] to create calculated columns.")
                    save_btn = st.form_submit_button("💾 Save Report Tab", type="primary")
                    if save_btn:
                        if mode_opt == "Single Source":
                            cols = []
                            for _, row in edited.iterrows():
                                label = str(row.get("Column Label") or "").strip()
                                field = str(row.get("Source Field") or "").strip()
                                if label and field:
                                    cols.append({"label": label, "field": field})
                            resolved_primary = _resolve_source_key(primary_source) or primary_source
                            cfg = {"mode": "single_source", "primary_source": resolved_primary, "columns": cols}
                        else:
                            mappings = []
                            for _, row in map_editor.iterrows():
                                label = str(row.get("Column Label") or "").strip()
                                source_raw = str(row.get("Source") or "").strip()
                                source = _resolve_source_key(source_raw) or source_raw
                                loc_name = str(row.get("Source Location") or "").strip()
                                field = str(row.get("Source Field") or "").strip()
                                date_field = str(row.get("Date Field") or "date").strip()
                                agg = str(row.get("Aggregate") or "sum").strip()
                                op_filter = str(row.get("Operation Filter") or "").strip()
                                if label and source and field and date_field:
                                    mappings.append({
                                        "label": label,
                                        "source": source,
                                        "location_id": location_name_to_id.get(loc_name),
                                        "field": field,
                                        "date_field": date_field,
                                        "aggregate": agg,
                                        "operation_filter": op_filter,
                                    })
                            calcs = []
                            for _, row in calc_editor.iterrows():
                                lbl = str(row.get("Result Label") or "").strip()
                                expr = str(row.get("Expression") or "").strip()
                                if lbl and expr:
                                    calcs.append({"label": lbl, "expression": expr})
                            cfg = {"mode": "date_merge", "mappings": mappings, "calculations": calcs}
                        try:
                            with get_session() as s:
                                scope_loc_id = report_location_id if r_scope == "Location" else None
                                rd = ReportDefinition(
                                    location_id=scope_loc_id,
                                    name=r_name.strip(),
                                    slug=r_slug.strip(),
                                    config_json=json.dumps(cfg | ({"source_location_id": src_location_id} if (mode_opt == "Single Source") else {})),
                                    is_active=True,
                                    created_by=(st.session_state.get("auth_user") or {}).get("username"),
                                )
                                s.add(rd)
                                s.commit()
                            st.success("? Report tab saved")
                            import time as _t
                            _t.sleep(1)
                            _st_safe_rerun()
                        except Exception as _ex:
                            st.error(f"Failed to save: {_ex}")
                        try:
                            with get_session() as s:
                                scope_loc_id = report_location_id if r_scope == "Location" else None
                                rd = ReportDefinition(
                                    location_id=scope_loc_id,
                                    name=r_name.strip(),
                                    slug=r_slug.strip(),
                                    config_json=json.dumps(cfg),
                                    is_active=True,
                                    created_by=(st.session_state.get("auth_user") or {}).get("username"),
                                )
                                s.add(rd)
                                s.commit()
                            st.success("? Report tab saved")
                            import time as _t
                            _t.sleep(1)
                            _st_safe_rerun()
                        except Exception as _ex:
                            st.error(f"Failed to save: {_ex}")
    
                st.markdown("#### Manage Existing Tabs")
                try:
                    with get_session() as s:
                        m_rows = (
                            s.query(ReportDefinition)
                            .filter(or_(ReportDefinition.location_id == location_id, ReportDefinition.location_id.is_(None)))
                            .order_by(ReportDefinition.name)
                            .all()
                        )
                    if m_rows:
                        m_labels = [f"{r.name} ({'Location' if r.location_id else 'Global'})" for r in m_rows]
                        m_sel = st.selectbox("Select", options=m_labels, index=0, key="rb_manage_select")
                        m_idx = m_labels.index(m_sel)
                        rd = m_rows[m_idx]
                        try:
                            m_cfg = json.loads(rd.config_json or "{}")
                        except Exception:
                            m_cfg = {}
                        m_name = st.text_input("Name", value=rd.name or "", key=f"rb_m_name_{rd.id}")
                        m_slug = st.text_input("Slug", value=rd.slug or "", key=f"rb_m_slug_{rd.id}")
                        m_scope = st.selectbox("Scope", ["Location", "Global"], index=(0 if rd.location_id else 1), key=f"rb_m_scope_{rd.id}")
                        m_active = st.checkbox("Active", value=bool(rd.is_active), key=f"rb_m_active_{rd.id}")
                        m_mode = st.radio("Mode", ["Single Source", "Date-Merge"], index=(0 if (m_cfg.get("mode") == "single_source" or not m_cfg.get("mode")) else 1), key=f"rb_m_mode_{rd.id}")
                        if m_scope == "Location":
                            default_report_label = location_id_to_label.get(rd.location_id) or location_labels[0]
                            m_report_loc_key = st.selectbox(
                                "Report Location",
                                location_labels,
                                index=(location_labels.index(default_report_label) if default_report_label in location_labels else 0),
                                key=f"rb_m_report_loc_{rd.id}",
                            )
                            m_report_location_id = location_label_to_id.get(m_report_loc_key)
                        else:
                            m_report_location_id = None
                        base_source_options = _available_primary_source_keys()
                        selected_source_loc_id = m_cfg.get("source_location_id") or src_location_id
                        if m_mode == "Single Source":
                            current_source_raw = m_cfg.get("primary_source")
                            if not current_source_raw:
                                current_source_raw = base_source_options[0] if base_source_options else ""
                            select_options = list(base_source_options)
                            if current_source_raw and current_source_raw not in select_options:
                                select_options.append(current_source_raw)
                            stored_source_loc_id = m_cfg.get("source_location_id") or src_location_id
                            default_source_label = location_id_to_label.get(stored_source_loc_id) or location_labels[0]
                            m_source_loc_key = st.selectbox(
                                "Source Location",
                                location_labels,
                                index=(location_labels.index(default_source_label) if default_source_label in location_labels else 0),
                                key=f"rb_m_source_loc_{rd.id}",
                            )
                            m_source_location_id = location_label_to_id.get(m_source_loc_key, stored_source_loc_id)
                            selected_source_loc_id = m_source_location_id
                            m_src = st.selectbox(
                                "Primary Source",
                                select_options,
                                index=(select_options.index(current_source_raw) if current_source_raw in select_options else 0),
                                key=f"rb_m_src_{rd.id}",
                                format_func=_format_source_option,
                            )
                            m_fields = _discover_source_fields(m_src, m_source_location_id)
                            preset_cols = m_cfg.get("columns") or []
                            m_df = pd.DataFrame([{ "Column Label": c.get("label"), "Source Field": c.get("field") } for c in preset_cols] or [{"Column Label": "Date", "Source Field": (m_fields[0] if m_fields else "")}])
                            m_edited = st.data_editor(
                                m_df,
                                num_rows="dynamic",
                                use_container_width=True,
                                key=f"rb_m_cols_{rd.id}_{_resolve_source_key(m_src) or m_src}_{m_source_location_id}",
                                column_config={
                                    "Source Field": st.column_config.SelectboxColumn(options=m_fields),
                                },
                            )
                        else:
                            e_loc_name_to_id = location_label_to_id
                            preset_maps = m_cfg.get("mappings") or []
                            map_source_options = list(base_source_options)
                            for m in preset_maps:
                                src_val = str(m.get("source") or "").strip()
                                if src_val and src_val not in map_source_options:
                                    map_source_options.append(src_val)
                            m_map_df = pd.DataFrame([
                                {
                                    "Column Label": m.get("label"),
                                    "Source": m.get("source"),
                                    "Source Location": [k for k,v in e_loc_name_to_id.items() if v == m.get("location_id")][0] if m.get("location_id") in e_loc_name_to_id.values() else "",
                                    "Source Field": m.get("field"),
                                    "Date Field": m.get("date_field"),
                                    "Aggregate": m.get("aggregate"),
                                    "Operation Filter": m.get("operation_filter"),
                                }
                                for m in preset_maps
                            ] or [
                                {
                                    "Column Label": "Column",
                                    "Source": (base_source_options[0] if base_source_options else ""),
                                    "Source Location": [k for k,v in e_loc_name_to_id.items() if v == src_location_id][0] if src_location_id in e_loc_name_to_id.values() else "",
                                    "Source Field": "qty_bbls",
                                    "Date Field": "date",
                                    "Aggregate": "sum",
                                    "Operation Filter": "",
                                }
                            ])
                            m_map_editor = st.data_editor(
                                m_map_df,
                                num_rows="dynamic",
                                use_container_width=True,
                                key=f"rb_m_maps_{rd.id}",
                                column_config={
                                    "Source": st.column_config.SelectboxColumn(options=map_source_options, format_func=_format_source_option),
                                    "Aggregate": st.column_config.SelectboxColumn(options=["sum", "last", "first"]),
                                },
                            )
                            m_calc_df = pd.DataFrame([
                                {"Result Label": c.get("label"), "Expression": c.get("expression")}
                                for c in (m_cfg.get("calculations") or [])
                            ])
                            if m_calc_df.empty:
                                m_calc_df = pd.DataFrame([{"Result Label": "", "Expression": ""}])
                            m_calc_editor = st.data_editor(
                                m_calc_df,
                                num_rows="dynamic",
                                use_container_width=True,
                                key=f"rb_m_calc_{rd.id}",
                            )
                            st.caption("For calculations, you can reference columns by label in [brackets] or by saying Column 3 - Column 2.")
                        b_save, b_delete = st.columns(2)
                        with b_save:
                            if st.button("Save Changes", key=f"rb_m_save_{rd.id}"):
                                try:
                                    with get_session() as s:
                                        rd.name = m_name.strip()
                                        rd.slug = m_slug.strip()
                                        rd.is_active = bool(m_active)
                                        rd.location_id = (m_report_location_id if m_scope == "Location" else None)
                                        if m_mode == "Single Source":
                                            new_cols = []
                                            for _, row in m_edited.iterrows():
                                                label = str(row.get("Column Label") or "").strip()
                                                field = str(row.get("Source Field") or "").strip()
                                                if label and field:
                                                    new_cols.append({"label": label, "field": field})
                                            resolved_m_src = _resolve_source_key(m_src) or m_src
                                            rd.config_json = json.dumps({"mode": "single_source", "primary_source": resolved_m_src, "columns": new_cols, "source_location_id": selected_source_loc_id})
                                        else:
                                            new_maps = []
                                            for _, row in m_map_editor.iterrows():
                                                label = str(row.get("Column Label") or "").strip()
                                                source_raw = str(row.get("Source") or "").strip()
                                                source = _resolve_source_key(source_raw) or source_raw
                                                loc_name = str(row.get("Source Location") or "").strip()
                                                field = str(row.get("Source Field") or "").strip()
                                                date_field = str(row.get("Date Field") or "date").strip()
                                                agg = str(row.get("Aggregate") or "sum").strip()
                                                op_filter = str(row.get("Operation Filter") or "").strip()
                                                if label and source and field and date_field:
                                                    new_maps.append({
                                                        "label": label,
                                                        "source": source,
                                                        "location_id": e_loc_name_to_id.get(loc_name),
                                                        "field": field,
                                                        "date_field": date_field,
                                                        "aggregate": agg,
                                                        "operation_filter": op_filter,
                                                    })
                                            new_calcs = []
                                            for _, row in m_calc_editor.iterrows():
                                                lbl = str(row.get("Result Label") or "").strip()
                                                expr = str(row.get("Expression") or "").strip()
                                                if lbl and expr:
                                                    new_calcs.append({"label": lbl, "expression": expr})
                                            rd.config_json = json.dumps({"mode": "date_merge", "mappings": new_maps, "calculations": new_calcs})
                                        s.add(rd)
                                        s.commit()
                                    st.success("Saved")
                                    import time as _t
                                    _t.sleep(1)
                                    _st_safe_rerun()
                                except Exception as ex:
                                    st.error(f"Failed: {ex}")
                        with b_delete:
                            if st.button("Delete", key=f"rb_m_del_{rd.id}"):
                                try:
                                    with get_session() as s:
                                        obj = s.query(ReportDefinition).filter(ReportDefinition.id == rd.id).one_or_none()
                                        if obj:
                                            s.delete(obj)
                                            s.commit()
                                    st.success("Deleted")
                                    import time as _t
                                    _t.sleep(1)
                                    _st_safe_rerun()
                                except Exception as ex:
                                    st.error(f"Failed: {ex}")
                    else:
                        st.info("No report tabs found.")
                except Exception:
                    pass
    
                st.markdown("#### Source Tables Preview")
                prev_cols = st.columns(2)
                prev_from = prev_cols[0].date_input("From date", value=date.today() - timedelta(days=30), key="rb_prev_from")
                prev_to = prev_cols[1].date_input("To date", value=date.today(), key="rb_prev_to")
                preview_keys = _available_primary_source_keys()
                if not preview_keys:
                    st.info('No primary sources configured.')
                else:
                    preview_labels = [_format_source_label(k) for k in preview_keys]
                    src_tabs = st.tabs(preview_labels)
                    try:
                        with get_session() as s:
                            for idx, src_key in enumerate(preview_keys):
                                with src_tabs[idx]:
                                    meta = _get_source_meta(src_key)
                                    if not meta:
                                        st.info('No metadata available for this source.')
                                        continue
                                    if meta.get("type") == "fso_material_balance":
                                        st.info("Preview not available for FSO Material Balance. Select an FSO vessel in the single-source builder to see this data.")
                                        continue
                                    rows = _fetch_source_rows(src_key, s, src_location_id, prev_from, prev_to)
                                    if not rows:
                                        st.info('No records within the selected range.')
                                        continue
                                    limit = 500
                                    sample_rows = rows[:limit]
                                    if sample_rows and isinstance(sample_rows[0], dict):
                                        df = pd.DataFrame(sample_rows)
                                    else:
                                        col_names = _get_model_columns(src_key)
                                        if not col_names:
                                            st.info('No column metadata available for this source.')
                                            continue
                                        df = pd.DataFrame([{col: getattr(r, col, None) for col in col_names} for r in sample_rows])
                                    if df.empty:
                                        st.info('No records within the selected range.')
                                    else:
                                        st.caption(f'Showing up to {min(limit, len(rows))} rows for this source.')
                                        st.dataframe(df, use_container_width=True, hide_index=True)
                    except Exception as ex:
                        st.error(f'Failed to load previews: {ex}')
    
    
            st.markdown("---")
    
            # ========== RESET TO DEFAULT ==========
            st.markdown("### 🔄 Reset Configuration")
            
            with st.expander("Reset to Default Settings", expanded=False):
                st.warning("This will reset all configuration to system defaults.")
                
                if st.button("🔄 Reset to Default", key="reset_config_btn"):
                    try:
                        with get_session() as s:
                            LocationConfig.reset_to_default(s, location_id)
                        
                        st.success("? Configuration reset to default!")
                        
                        # Log audit
                        user = st.session_state.get("auth_user")
                        if user:
                            from security import SecurityManager
                            with get_session() as s:
                                SecurityManager.log_audit(
                                    s, user["username"], "RESET_LOCATION_CONFIG",
                                    resource_type="Location",
                                    resource_id=str(location_id),
                                    details="Reset to default configuration",
                                    user_id=user["id"]
                                )
                        
                        import time
                        time.sleep(1)
                        _st_safe_rerun()
                    except Exception as ex:
                        st.error(f"Failed to reset: {ex}")
            
            # ========== VIEW RAW JSON ==========
            st.markdown("---")
            st.markdown("### 🛠️ Advanced: View Raw Configuration")
            
            with st.expander("View JSON Configuration", expanded=False):
                st.json(current_config)
    
    # ============================= Recycle Bin (admin only) ==========================