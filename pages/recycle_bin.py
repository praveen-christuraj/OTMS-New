"""
Auto-generated module for the 'Recycle Bin' page.
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
            header("Recycle Bin")
            st.error("You do not have permission to access this page. Admin-Operations only.")
            st.stop()
    
        header("Recycle Bin")
        st.caption("View soft-deleted records stored in the recycle bin.")
    
        with get_session() as s:
            type_rows = s.query(RecycleBinEntry.resource_type).distinct().all()
            resource_types = ["(All)"] + sorted(
                {row[0] for row in type_rows if row and row[0]}
            )
            from location_manager import LocationManager
    
            loc_objs = LocationManager.get_all_locations(s, active_only=False)
            location_options = {"(All locations)": None}
            for loc in loc_objs:
                key = f"{loc.name} ({loc.code})"
                location_options[key] = loc.id
    
        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            selected_type = st.selectbox("Resource Type", resource_types, index=0)
        with filter_col2:
            selected_location = st.selectbox("Location", list(location_options.keys()), index=0)
    
        filter_col3, filter_col4 = st.columns(2)
        with filter_col3:
            from_date = st.date_input(
                "From Date",
                value=date.today() - timedelta(days=7),
                key="recycle_from_date",
            )
        with filter_col4:
            to_date = st.date_input(
                "To Date",
                value=date.today(),
                key="recycle_to_date",
            )
    
        search_text = st.text_input(
            "Search (resource id, label or reason)", key="recycle_search"
        ).strip()
        max_records = st.slider("Max records", min_value=50, max_value=500, value=200, step=25)
    
        with get_session() as s:
            query = s.query(RecycleBinEntry).order_by(RecycleBinEntry.deleted_at.desc())
            if selected_type != "(All)":
                query = query.filter(RecycleBinEntry.resource_type == selected_type)
            loc_filter_id = location_options.get(selected_location)
            if loc_filter_id:
                query = query.filter(RecycleBinEntry.location_id == loc_filter_id)
            if from_date:
                from_dt = datetime.combine(from_date, datetime.min.time())
                query = query.filter(RecycleBinEntry.deleted_at >= from_dt)
            if to_date:
                to_dt = datetime.combine(to_date, datetime.max.time())
                query = query.filter(RecycleBinEntry.deleted_at <= to_dt)
            if search_text:
                pattern = f"%{search_text}%"
                query = query.filter(
                    or_(
                        RecycleBinEntry.resource_label.ilike(pattern),
                        RecycleBinEntry.resource_id.ilike(pattern),
                        RecycleBinEntry.reason.ilike(pattern),
                    )
                )
            entries = query.limit(int(max_records)).all()
    
        if not entries:
            st.info("No archived records match your criteria.")
        else:
            import pandas as pd
    
            location_lookup = {loc.id: f"{loc.name} ({loc.code})" for loc in loc_objs}
            table_rows = []
            for entry in entries:
                table_rows.append(
                    {
                        "Deleted At": format_local_datetime(entry.deleted_at),
                        "Type": entry.resource_type,
                        "Resource": entry.resource_label or entry.resource_id,
                        "Location": location_lookup.get(entry.location_id, "-"),
                        "Deleted By": entry.deleted_by,
                        "Reason": entry.reason or "",
                    }
                )
            st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)
    
            for entry in entries:
                header_label = f"{entry.resource_type} � {entry.resource_label or entry.resource_id} ({format_local_datetime(entry.deleted_at)})"
                with st.expander(header_label, expanded=False):
                    st.write(
                        {
                            "Deleted By": entry.deleted_by,
                            "User ID": entry.deleted_by_id,
                            "Location": location_lookup.get(entry.location_id, "-"),
                            "Reason": entry.reason or "-",
                            "Record ID": entry.resource_id,
                        }
                    )
                    try:
                        payload = json.loads(entry.payload_json)
                        st.json(payload)
                    except Exception:
                        st.code(entry.payload_json)
    
    # ============================= Audit Log (admin only) ==========================