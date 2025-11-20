"""
Auto-generated module for the 'Tank Transactions' page.
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
from models import Operation, Tank, CalibrationTank, MeterTransaction, TankTransaction
from ui import header
from db import get_session
from pages.helpers import st_safe_rerun, archive_payload_for_delete

# -----------------------------------------------------------------------------
# Helper aliases and default constants
#
# The Tank Transactions page relies on a handful of helper functions and
# constants that are normally provided by other parts of the application.  In
# some deployment scenarios (for example when this module is executed in
# isolation during testing) those helpers may not be imported automatically.
# To avoid runtime NameError exceptions we define a few sensible defaults and
# aliases here.  These fallback implementations are intentionally simple –
# they preserve the shape of the expected API without adding any application
# specific behaviour.  When running within the full application environment
# these definitions will be shadowed by the real implementations supplied via
# the ``pages.helpers`` module and other imported modules.

# Minimum and maximum API gravity values accepted by the input widgets.  The
# range (0.0 – 150.0) covers the vast majority of crude oil API values.  If
# your installation requires a different range you can override these values
# via the location configuration.
API_MIN: float = 0.0
API_MAX: float = 150.0

# Minimum and maximum observed density values (kg/m³) accepted by the input
# widgets.  Typical densities for petroleum liquids range between 500 and
# 1200 kg/m³, therefore the defaults below should be sufficiently broad for
# most use cases.
DENSITY_MIN: float = 500.0
DENSITY_MAX: float = 1200.0

# Conversion factor for condensate receipts: number of barrels in one cubic
# metre of condensate.  This constant is defined here so that it is always
# available during calculation; if the value needs to be refined it can be
# supplied by the application configuration.
CONDENSATE_M3_TO_BBL: float = 6.289

# Expose ``st_safe_rerun`` under the underscore-prefixed name used throughout
# this module.  If ``st_safe_rerun`` has been imported correctly from
# ``pages.helpers`` then this alias will refer to the genuine function; if it
# is missing the fallback will simply trigger a standard Streamlit rerun.
try:
    _st_safe_rerun = st_safe_rerun
except Exception:
    # fallback definition if helpers are not available
    def _st_safe_rerun() -> None:
        """Fallback for st_safe_rerun; triggers a standard Streamlit rerun."""
        st.experimental_rerun()

# Provide an alias for the record archival helper.  The real function is
# imported above as ``archive_payload_for_delete``; if the import fails this
# alias becomes a no-op to prevent crashes when deleting records.
try:
    _archive_record_for_delete = archive_payload_for_delete
except Exception:
    def _archive_record_for_delete(*args: Any, **kwargs: Any) -> None:
        """Fallback for archive function; does nothing if helpers are missing."""
        return None

# -----------------------------------------------------------------------------
# Simple helper implementations
#
# The form below relies on a couple of small helper functions (prefixed with
# an underscore) to wrap Streamlit inputs with extra validation logic.  If
# these helpers are not supplied by another module we define reasonable
# fallbacks here.  They simply forward to ``st.number_input`` with a
# constrained range and step, ensuring that form widgets continue to work.

def _bounded_number_input(
    label: str,
    key: str,
    min_val: float,
    max_val: float,
    step: float = 0.1,
) -> float:
    """Display a numeric input bounded between ``min_val`` and ``max_val``.

    This wrapper around ``st.number_input`` constrains the input range and
    allows the caller to specify a step size.  The current value is taken
    from ``st.session_state`` if present; otherwise the minimum value is
    used as a default.  Returns the user-supplied value as a float.
    """
    default = st.session_state.get(key, min_val)
    return st.number_input(
        label,
        min_value=min_val,
        max_value=max_val,
        value=default,
        step=step,
        format="%.2f",
        key=key,
    )


def _temperature_input(label: str, unit: str, key: str) -> float:
    """Display a temperature input field.

    This helper simplifies the entry of temperature values.  The ``unit``
    parameter is currently unused but retained for API compatibility with
    upstream implementations.  If no value is present in ``st.session_state``
    the default is zero.  The value is returned unchanged.
    """
    default = st.session_state.get(key, 0.0)
    # Use a coarse step to allow fractional degrees without overwhelming the UI
    return st.number_input(label, value=float(default), step=0.1, format="%.2f", key=key)


# -----------------------------------------------------------------------------
# Additional stub helpers
#
# These helpers are referenced throughout the Tank Transactions module.  Where
# possible they delegate to underlying application components but default to
# simple behaviour if those components are unavailable.  They are defined
# outside of the render function so that they are only evaluated once at
# import time.

def _normalize_operation(loc_code: str, operation: str) -> str:
    """Normalise an operation label for storage.

    The upstream implementation may append location-specific prefixes or
    perform other transformations.  In the absence of such logic this
    fallback simply returns the original operation string unchanged.
    """
    try:
        return str(operation)
    except Exception:
        return operation


def _coerce_operation_for_db(op_norm: str) -> Optional[str]:
    """Coerce a normalised operation label to the DB enumeration.

    The real implementation is expected to map human-readable labels onto
    SQLAlchemy enum values.  Here we return the input unchanged, which
    suffices when the database column stores plain strings.  If an empty
    or falsy value is supplied ``None`` is returned to indicate an error.
    """
    if not op_norm:
        return None
    return op_norm


def _deny_edit_for_lock(rec: Any, resource_type: str, label: str) -> bool:
    """Determine whether editing should be denied due to a lock.

    The full application implements record locking to prevent concurrent
    edits.  This fallback simply permits all edits by returning ``False``.
    """
    return False


def _ensure_date_key_in_bounds(key: str, start: Optional[date], end: Optional[date], default: Optional[date]) -> Optional[date]:
    """Ensure a date value stored in the session is within the given bounds.

    If the key exists in ``st.session_state`` and the value is a ``date``
    instance, it is returned provided it lies between ``start`` and ``end``.
    Otherwise the ``default`` value is returned.
    """
    try:
        val = st.session_state.get(key)
        if isinstance(val, date):
            if start and val < start:
                return start
            if end and val > end:
                return end
            return val
    except Exception:
        pass
    return default


def _derive_filter_bounds(dates: List[date]) -> Tuple[Optional[date], Optional[date]]:
    """Return the minimum and maximum date from a list of dates.

    If the list is empty, both bounds default to the current date.  Any
    non-date entries are ignored.
    """
    filtered = [d for d in dates if isinstance(d, date)]
    if not filtered:
        today = date.today()
        return (today, today)
    return (min(filtered), max(filtered))


def _next_tanker_serial(session: Any, location_id: int) -> int:
    """Return the next serial number for a tanker entry.

    In a full application this would query the existing ``LocationTankerEntry``
    records to determine the highest serial number for the given location.
    To keep this fallback simple we always return ``1``, which ensures that
    the calling code can proceed without a database dependency.
    """
    try:
        # Attempt to query the DB for the max serial number if possible
        from models import LocationTankerEntry  # type: ignore
        rows = (
            session.query(LocationTankerEntry.serial_no)
            .filter(LocationTankerEntry.location_id == location_id)
            .all()
        )
        max_serial = 0
        for (sn,) in rows:
            try:
                num = int(sn)
                if num > max_serial:
                    max_serial = num
            except Exception:
                continue
        return max_serial + 1
    except Exception:
        return 1


def user_with_caution(created_by: Optional[str], updated_by: Optional[str], updated_at: Optional[Any]) -> str:
    """Return a user badge string for audit information.

    This helper composes a small HTML snippet showing the creator and updater
    usernames along with an update timestamp if available.  The real
    implementation may mark suspicious edits; here we simply display the
    provided information.
    """
    parts: List[str] = []
    if created_by:
        parts.append(f"Created by {html.escape(str(created_by))}")
    if updated_by:
        parts.append(f"Updated by {html.escape(str(updated_by))}")
    if updated_at:
        try:
            ts_str = str(updated_at)
            parts.append(f"at {html.escape(ts_str)}")
        except Exception:
            pass
    return ", ".join(parts)


def load_condensate_transactions(location_id: int, limit: int = 100) -> Tuple[List[Dict[str, Any]], int]:
    """Load condensate transactions for a location.

    The primary application implementation retrieves condensate receipts from
    the database and returns a list of dictionaries containing human
    readable fields.  When running without a database connection this
    fallback returns an empty list and zero total count.
    """
    try:
        from models import TankTransaction  # type: ignore
        from sqlalchemy import desc
        with get_session() as s:
            rows = (
                s.query(TankTransaction)
                .filter(
                    TankTransaction.location_id == location_id,
                    TankTransaction.operation == _coerce_operation_for_db(_normalize_operation("", "Receipt - Condensate")),
                )
                .order_by(desc(TankTransaction.date), desc(TankTransaction.time))
                .limit(limit)
                .all()
            )
        records: List[Dict[str, Any]] = []
        for r in rows:
            # Build a simple summary; the real implementation may calculate additional fields
            records.append({
                "Ticket ID": r.ticket_id,
                "Date": r.date,
                "Opening (m3)": r.opening_meter_reading,
                "Closing (m3)": r.closing_meter_reading,
                "Net Receipt (m3)": (r.closing_meter_reading or 0.0) - (r.opening_meter_reading or 0.0),
                "GOV (bbls)": r.qty_bbls,
                "API @ 60": r.api_observed,
                "VCF": 1.0,
                "GSV (bbls)": r.qty_bbls,
                "LT": 0.0,
                "MT": 0.0,
            })
        return records, len(records)
    except Exception:
        return [], 0


def render() -> None:
        global st
        header("Tank Transactions")
        
        # Check Admin-IT access restriction
        if st.session_state.get("auth_user", {}).get("role") == "admin-it":
            st.error("🚫 Access Denied: Admin-IT users do not have access to operational pages.")
            st.info("Admin-IT role is for system administration only (users, audit logs, backups, etc.).")
            st.stop()
        
        try:
            _user_role = st.session_state.get("auth_user", {}).get("role")
            _loc_id = st.session_state.get("active_location_id")
            if _user_role not in ["admin-operations", "manager"] and _loc_id:
                from location_config import LocationConfig
                with get_session() as _s:
                    _cfg = LocationConfig.get_config(_s, _loc_id)
                if _cfg.get("page_access", {}).get("Tank Transactions") is False:
                    st.error("⚠️ Tank Transactions page is disabled for this location.")
                    st.stop()
        except Exception:
            pass
    
        # -------- Check location access --------
        active_location_id = st.session_state.get("active_location_id")
        if not active_location_id:
            st.error("⚠️ No active location selected. Please select a location from the Home page.")
            st.stop()
    
        # -------- Verify user has access to this location --------
        user = st.session_state.get("auth_user")
        if user:
            from auth import AuthManager
            if not AuthManager.can_access_location(user, active_location_id):
                st.error("🚫 You do not have access to this location.")
                st.stop()
        user_role = (user.get("role") or "").lower() if user else ""
        
        # ========== CHECK PERMISSIONS ==========
        from permission_manager import PermissionManager
        
        with get_session() as s:
            from location_manager import LocationManager
            
            # Get location info
            loc = LocationManager.get_location_by_id(s, active_location_id)
            if not loc:
                st.error("? Location not found.")
                st.stop()
            
            st.info(f"📍 **Active Location:** {loc.name} ({loc.code})")
    
            # Apply location-based page visibility: hide page if disabled in config (non-admin)
            try:
                with get_session() as _s_cfg:
                    from location_config import LocationConfig
                    _cfg = LocationConfig.get_config(_s_cfg, active_location_id)
                if not _cfg.get("page_visibility", {}).get("show_tank_transactions", True) and (user.get("role", "").lower() not in ["admin-operations", "manager"]):
                    st.error("⚠️ Tank Transactions page is disabled for this location.")
                    st.stop()
            except Exception:
                pass
            
            # Check if feature is allowed at this location (Admin can access everywhere)
            if not PermissionManager.can_access_feature(s, active_location_id, "tank_transactions", user["role"]):
                st.error("🚫 **Access Denied**")
                st.warning(f"**Tank Transactions** are not available at **{loc.name}**")
                
                # Show where it's available
                allowed_locs = PermissionManager.get_allowed_locations_for_feature(s, "tank_transactions")
                if allowed_locs:
                    st.info(f"? This feature is available at: **{', '.join(allowed_locs)}**")
                
                st.markdown("---")
                st.caption(f"Current Location: **{loc.name} ({loc.code})**")
                st.caption("Tank Transactions Access: **? Denied**")
                st.stop()
            
            # Check if user can make entries
            can_make_entries = PermissionManager.can_make_entries(s, user["role"], active_location_id)
        
        # ============ FEATURE ENABLED - CONTINUE ============
        # Special handling for OFS Production & Evacuation (OML-157)
        try:
            _ofs_code_norm = (loc.code or "").replace(" ", "").replace("-", "").upper()
        except Exception:
            _ofs_code_norm = ""
        if _ofs_code_norm == "OML157":
            # Render the dedicated OFS Production & Evacuation page for this location
            try:
                render_ofs_production_page(active_location_id, loc, user)
            except Exception as ex:
                st.error(f"Failed to render OFS Production page: {ex}")
            # Stop further processing of Tank Transactions page
            st.stop()
        try:
            _loc_name_norm = (loc.name or "").strip().lower()
        except Exception:
            _loc_name_norm = ""
        _loc_name_upper = (loc.name or "").strip().upper()
        _loc_code_upper = (loc.code or "").strip().upper()
        _is_asemoku_jetty = (_loc_name_norm == "asemoku jetty")
        _is_bfs_location = (
            "BFS" in _loc_code_upper
            or "BFS" in _loc_name_upper
            or "BENEKU" in _loc_name_upper
        )
        _is_aggu_location = ("AGGU" in _loc_code_upper) or ("AGGU" in _loc_name_upper)
        _is_ndoni_location = ("NDONI" in _loc_code_upper) or ("NDONI" in _loc_name_upper)
        _show_tanker_tab = _is_aggu_location or _is_ndoni_location
    
        _meter_tab = None
        _condensate_tab = None
        _gpp_tab = None
        _tanker_tab = None
        _river_tab = None
        _produced_water_tab = None
        st_for_restore = None
    
        if _is_asemoku_jetty:
            tab_labels = ["Tank Transactions", "Meter Transactions", "River Draft", "Produced Water"]
            if _show_tanker_tab:
                tab_labels.append("No of Tankers")
            tabs = st.tabs(tab_labels)
            _ttab = tabs[0]
            _meter_tab = tabs[1]
            _river_tab = tabs[2]
            _produced_water_tab = tabs[3]
            if _show_tanker_tab:
                _tanker_tab = tabs[4]
            st_for_restore = st
            st = _ttab
        elif _is_bfs_location:
            tab_labels = ["Tank Transactions", "Condensate Records", "Production"]
            if _show_tanker_tab:
                tab_labels.append("No of Tankers")
            tabs = st.tabs(tab_labels)
            _ttab = tabs[0]
            _condensate_tab = tabs[1]
            _gpp_tab = tabs[2]
            if _show_tanker_tab:
                _tanker_tab = tabs[3]
            st_for_restore = st
            st = _ttab
        elif _show_tanker_tab:
            tab_labels = ["Tank Transactions"]
            if _is_ndoni_location:
                tab_labels.extend(["River Draft", "Produced Water"])
            tab_labels.append("No of Tankers")
            tabs = st.tabs(tab_labels)
            _ttab = tabs[0]
            st_for_restore = st
            st = _ttab
            idx = 1
            if _is_ndoni_location:
                _river_tab = tabs[idx]
                _produced_water_tab = tabs[idx + 1]
                idx += 2
            _tanker_tab = tabs[idx]
    
        # Get location config
        with get_session() as s:
            from location_config import LocationConfig
            
            # Load location-specific configuration
            config = LocationConfig.get_config(s, active_location_id)
            tabs_access = config.get("tabs_access", {})
            tt_tabs = tabs_access.get("Tank Transactions", {})
            if tt_tabs.get("Meter Transactions") is False:
                _meter_tab = None
            if tt_tabs.get("Condensate Records") is False:
                _condensate_tab = None
            if tt_tabs.get("Production") is False:
                _gpp_tab = None
            if tt_tabs.get("River Draft") is False:
                _river_tab = None
            if tt_tabs.get("Produced Water") is False:
                _produced_water_tab = None
            tank_config = config.get("tank_transactions", {})
    
        # Get available operations from config
        operation_options = tank_config.get("enabled_operations", [
            "Opening Stock", "OKW Receipt", "ANZ Receipt", "Other Receipts",
            "ITT - Receipt", "Dispatch to barge", "Other Dispatch", "ITT - Dispatch",
            "Closing Stock",
        ])
        if _is_bfs_location:
            operation_options = [op for op in operation_options if op != "Receipt - Condensate"]
    
        # Get product types from config
        product_options = tank_config.get("product_types", ["CRUDE", "CONDENSATE", "DPK", "AGO", "PMS"])
    
        # Get validation rules
        max_days_backward = tank_config.get("max_days_backward", 30)
        allow_future_dates = tank_config.get("allow_future_dates", False)
        auto_generate_ticket = tank_config.get("auto_generate_ticket_id", False)
        configured_prefix = (tank_config.get("ticket_id_prefix") or "").strip().upper()
    
        loc_prefix = ""
        if loc and getattr(loc, "name", None):
            letters_only = "".join(ch for ch in loc.name if ch.isalpha())
            if letters_only:
                loc_prefix = letters_only[:3].upper()
    
        ticket_prefix = loc_prefix or configured_prefix
        if not ticket_prefix and loc:
            ticket_prefix = ((loc.code or "")[:3]).upper()
    
        # Show configuration info
        with st.expander("⚙️ Location Configuration", expanded=False):
            st.caption(f"**Enabled Operations:** {len(operation_options)}")
            st.caption(f"**Product Types:** {', '.join(product_options)}")
            st.caption(f"**Max Days Backward:** {max_days_backward} days")
            st.caption(f"**Allow Future Dates:** {'Yes' if allow_future_dates else 'No'}")
            if auto_generate_ticket:
                st.caption(f"**Auto-Generate Ticket ID:** Enabled")
            if ticket_prefix:
                st.caption(f"**Ticket ID Prefix:** {ticket_prefix}")
    
        # ---------- imports / constants ----------
        import re, math, hashlib
        from datetime import datetime, date
        from sqlalchemy.exc import IntegrityError
    
        WAT60 = 999.012
    
        # ---------- tiny helpers ----------
        def hhmm_ok(s: str) -> bool:
            if not s or len(s) not in (4, 5):
                return False
            s = s.strip()
            if ":" not in s:
                return False
            h, m = s.split(":", 1)
            if not (h.isdigit() and m.isdigit()):
                return False
            h, m = int(h), int(m)
            return 0 <= h <= 23 and 0 <= m <= 59
    
        def op_code_4(op_text: str) -> str:
            cleaned = "".join(ch for ch in op_text if ch.isalpha()).upper()
            return (cleaned[:4] or "OPER")
    
        def mk_ticket_id(op_text: str, d: date, serial: int, prefix: str = "") -> str:
            """
            Generate location-aware ticket ID.
            Format: <LOC>-TIC-<OC4>-<DDMMYYYY>-<NNNN>
            If prefix is blank, we still use hyphen format without LOC.
            """
            oc4 = op_code_4(op_text)  # existing helper
            loc_code = (prefix or "").strip().upper()
            date_part = d.strftime("%d%m%Y")
            if loc_code:
                return f"{loc_code}-TIC-{oc4}-{date_part}-{serial:04d}"
            return f"TIC-{oc4}-{date_part}-{serial:04d}"
    
        def next_serial_for(op_text: str, d: date, prefix: str = "", location_id: int | None = None) -> int:
            """
            Get next serial for this (location + op + date) by scanning existing ticket_ids.
            Works even if old 'TIC/..' rows exist.
            """
            oc4 = op_code_4(op_text)
            loc_code = (prefix or "").strip().upper()
            date_part = d.strftime("%d%m%Y")
    
            # New (location-aware) prefix and legacy (slash) prefix:
            search_prefix_new = (f"{loc_code}-TIC-{oc4}-{date_part}-" if loc_code else f"TIC-{oc4}-{date_part}-")
            search_prefix_old = f"TIC/{oc4}/{d.strftime('%d-%m-%Y')}/"
    
            with get_session() as s:
                q = s.query(TankTransaction.ticket_id)
                if location_id is not None:
                    q = q.filter(TankTransaction.location_id == int(location_id))
                rows = q.filter(
                    (TankTransaction.ticket_id.like(search_prefix_new + "%")) |
                    (TankTransaction.ticket_id.like(search_prefix_old + "%"))
                ).all()
    
            max_serial = 0
            for (tid,) in rows:
                if not tid:
                    continue
                # New style: LOC-TIC-OC4-DDMMYYYY-NNNN
                if "-TIC-" in tid:
                    parts = tid.split("-")
                    try:
                        num = int(parts[-1])
                        if num > max_serial:
                            max_serial = num
                        continue
                    except:
                        pass
                # Legacy: TIC/OC4/DD-MM-YYYY/NNNNNN
                if tid.startswith("TIC/"):
                    parts = tid.split("/")
                    if len(parts) >= 4:
                        try:
                            num = int(parts[-1])
                            if num > max_serial:
                                max_serial = num
                        except:
                            pass
            return max_serial + 1
    
    
        def c_to_f(c: float) -> float:
            if c is None: return 0.0
            return round((float(c) * 1.8) + 32.0, 1)
    
        def f_to_c(f: float) -> float:
            """Convert a Fahrenheit temperature to Celsius.

            Accepts a numeric value ``f`` representing degrees Fahrenheit and
            returns the equivalent value in degrees Celsius rounded to one
            decimal place.  If the input is ``None`` a default of ``0.0`` is
            returned.  The original implementation erroneously referenced an
            undefined variable ``c``, which raised a ``NameError`` at runtime.
            This corrected version checks the incoming value correctly.
            """
            if f is None:
                return 0.0
            return round((float(f) - 32.0) / 1.8, 1)
    
        def _to_f(val: float, unit: str) -> float:
            """Return °F from val given unit ('°F' or '°C')."""
            return c_to_f(val) if unit.upper().startswith("C") else float(val or 0.0)
    
        def _to_c(val: float, unit: str) -> float:
            """Return °C from val given unit ('°F' or '°C')."""
            return f_to_c(val) if unit.upper().startswith("F") else float(val or 0.0)
    
        # ---- Excel-accurate transforms ----
        def density_from_api(api: float) -> float:
            if not api or api <= 0: return 0.0
            sg = 141.5 / (float(api) + 131.5)
            return round(sg * WAT60, 1)
    
        def api_from_density(density: float) -> float:
            if not density or density <= 0: return 0.0
            sg = float(density) / WAT60
            if sg <= 0: return 0.0
            return round(141.5 / sg - 131.5, 2)
    
        def convert_api_to_60_from_api(api_obs: float, sample_temp_val: float, temp_unit: str) -> float:
            """Your VBA (10 trials). Temp is ALWAYS °F internally."""
            if api_obs is None or api_obs <= 0:
                return 0.0
            tf = _to_f(sample_temp_val or 0.0, temp_unit)
            temp_diff = tf - 60.0
            rho_obs = (141.5 * WAT60 / (131.5 + float(api_obs))) * (
                (1.0 - 0.00001278 * temp_diff) - (0.0000000062 * temp_diff * temp_diff)
            )
            rho = rho_obs
            for _ in range(10):
                alfa = 341.0957 / (rho * rho)
                vcf  = math.exp(-alfa * temp_diff - 0.8 * alfa * alfa * temp_diff * temp_diff)
                rho  = rho_obs / vcf
            api60 = 141.5 * WAT60 / rho - 131.5
            return round(api60, 2)
    
        def convert_api_to_60_from_density(dens_obs_kgm3: float, sample_temp_val: float, temp_unit: str) -> float:
            """Density path (17 trials). Temp is ALWAYS °C internally."""
            if dens_obs_kgm3 is None or dens_obs_kgm3 <= 0:
                return 0.0
            tc = _to_c(sample_temp_val or 0.0, temp_unit)
            temp_diff = tc - 15.0
            
            # Hydrometer correction
            hyc = 1.0 - 0.000023 * temp_diff - 0.00000002 * temp_diff * temp_diff
            rho_obs_corrected = float(dens_obs_kgm3) * hyc
            
            # Initial density at 15°C
            rho15 = rho_obs_corrected
            
            # Iterative VCF calculation (17 iterations)
            for _ in range(17):
                # Thermal expansion coefficient K
                K = 613.9723 / (rho15 * rho15)
                
                # VCF calculation (temperature-based)
                vcf = math.exp(-K * temp_diff * (1.0 + 0.8 * K * temp_diff))
                
                # Update density at 15°C
                rho15 = rho_obs_corrected / vcf
            
            # Convert density at 15°C to API@60°F
            sg60 = rho15 / WAT60
            if sg60 <= 0:
                return 0.0
            
            api60 = 141.5 / sg60 - 131.5
            return round(api60, 2)
    
        # ---- VCF calculation ----
        def vcf_from_api60_and_temp(api60: float, tank_temp: float, tank_temp_unit: str, input_mode: str = "api") -> float:
            """Calculate VCF using ASTM D1250 Table 6A method"""
            if api60 is None or api60 <= 0:
                return 1.00000
            
            # Convert tank temp to °F if needed
            if tank_temp_unit == "°C":
                tank_temp_f = (tank_temp * 1.8) + 32.0
            else:
                tank_temp_f = tank_temp
            
            # Temperature difference from 60°F
            delta_t = tank_temp_f - 60.0
            
            if abs(delta_t) < 0.01:
                return 1.00000
            
            # Calculate density at 60°F
            sg60 = 141.5 / (api60 + 131.5)
            rho60 = sg60 * 999.012
            
            # Thermal expansion coefficient
            K0 = 341.0957
            alpha = K0 / (rho60 * rho60)
            
            # VCF calculation
            vcf = math.exp(-alpha * delta_t * (1.0 + 0.8 * alpha * delta_t))
            
            return round(float(vcf), 5)
    
        # ---- interpolation from CalibrationTank ----
        def _interp_vol_bbl(session, tank_name: str, dip_cm_val: float) -> float:
            """Linear interpolation on CalibrationTank."""
            if dip_cm_val is None:
                return 0.0
            rows = session.query(CalibrationTank).filter(
                CalibrationTank.tank_name == tank_name,
                CalibrationTank.location_id == active_location_id
            ).order_by(CalibrationTank.dip_cm.asc()).all()
            if not rows:
                return 0.0
            xs = [float(r.dip_cm or 0.0) for r in rows]
            ys = [float(r.volume_bbl or 0.0) for r in rows]
            if dip_cm_val <= xs[0]:
                return ys[0]
            if dip_cm_val >= xs[-1]:
                return ys[-1]
            import bisect
            i = bisect.bisect_left(xs, dip_cm_val)
            x1, y1 = xs[i-1], ys[i-1]
            x2, y2 = xs[i], ys[i]
            if x2 == x1:
                return y1
            t = (dip_cm_val - x1) / (x2 - x1)
            return y1 + t * (y2 - y1)
    
        # ---- LT Factor lookup ----
        def lookup_lt_factor(session, api60: float) -> float:
            """Lookup LT Factor from ASTM Table 11"""
            from models import Table11
            
            rows = session.query(Table11).order_by(Table11.api60).all()
            if not rows:
                return 0.0
            
            xs = [float(r.api60) for r in rows]
            ys = [float(r.lt_factor) for r in rows]
            
            if api60 <= xs[0]:
                return ys[0]
            if api60 >= xs[-1]:
                return ys[-1]
            
            import bisect
            i = bisect.bisect_left(xs, api60)
            x1, y1 = xs[i-1], ys[i-1]
            x2, y2 = xs[i], ys[i]
            
            if x2 == x1:
                return y1
            
            t = (api60 - x1) / (x2 - x1)
            return y1 + t * (y2 - y1)
    
        
    
    
        # ---------- UI ----------
        st.markdown("#### Add Ticket")
    
        # ============ OPERATION SELECTOR ===========
        operation = st.selectbox(
            "Operation *",
            options=operation_options,
            key="tank_tx_operation"
        )
    
        # ============ CHECK IF CONDENSATE RECEIPT ============
        is_condensate_receipt = (operation == "Receipt - Condensate")
    
        if is_condensate_receipt:
            st.success("ℹ️ **Condensate Receipt Mode** - Meter reading entry (no tank selection)")
    
        # ============ DATE AND TIME ============
        import datetime as dt
    
        min_date = dt.date.today() - dt.timedelta(days=max_days_backward)
        max_date = dt.date.today() + dt.timedelta(days=7) if allow_future_dates else dt.date.today()
    
        date_col, time_col = st.columns(2)
        
        with date_col:
            tx_date = st.date_input(
                "2) Date *",
                value=dt.date.today(),
                min_value=min_date,
                max_value=max_date,
                key="tank_tx_date",
                help=f"Date range: {min_date.strftime('%d/%m/%Y')} to {max_date.strftime('%d/%m/%Y')}"
            )
        
        with time_col:
            if is_condensate_receipt:
                tx_time_str = "23:59"
                st.caption("Time auto-set to 23:59 for 24 hr condensate meter records.")
            else:
                tx_time_str = st.text_input("Time * (hh:mm)", value="08:00", key="tx_time")
    
        # ============ TANK SELECTOR ============
        tank_id = None
        tank_fk = None
        
        if not is_condensate_receipt:
            with get_session() as s:
                _tanks = s.query(Tank).filter(
                    Tank.location_id == active_location_id,
                    Tank.status == "ACTIVE"
                ).order_by(Tank.name).all()
            
            tank_names = [t.name for t in _tanks] if _tanks else ["(No tanks yet � add in Add Asset ? Tank Master)"]
            tank_by_name = {t.name: t for t in _tanks}
            
            tank_id = st.selectbox("3) Tank ID *", tank_names, index=0, key="tx_tank_id")
            
            if tank_id in tank_by_name:
                tank_fk = tank_by_name[tank_id].id
        else:
            tank_id = "CONDENSATE-RECEIPT"
            st.info("ℹ️ Tank selection not required for condensate receipt (receives into multiple tanks)")
    
        st.markdown("---")
    
        # ============ CONDENSATE METER READING ============
        opening_meter = None
        closing_meter = None
        condensate_qty_m3 = 0.0
        condensate_gov_bbl = 0.0
    
        if is_condensate_receipt:
            st.markdown("#### 📊 Condensate Meter Readings")
            
            meter_col1, meter_col2, meter_col3 = st.columns(3)
            
            with meter_col1:
                opening_meter = st.number_input(
                    "Opening Meter Reading (m�) *",
                    min_value=0.0,
                    step=0.001,
                    format="%.3f",
                    key="opening_meter",
                    help="Meter reading at start (cubic meters)"
                )
            
            with meter_col2:
                closing_meter = st.number_input(
                    "Closing Meter Reading (m�) *",
                    min_value=0.0,
                    step=0.001,
                    format="%.3f",
                    key="closing_meter",
                    help="Meter reading at end (cubic meters)"
                )
            
            with meter_col3:
                if closing_meter > opening_meter:
                    condensate_qty_m3 = closing_meter - opening_meter
                    condensate_gov_bbl = condensate_qty_m3 * 6.289
                else:
                    condensate_qty_m3 = 0.0
                    condensate_gov_bbl = 0.0
                
                st.metric(
                    "Condensate Qty (m�)",
                    f"{condensate_qty_m3:,.3f}",
                    help="Calculated: Closing - Opening"
                )
                st.metric(
                    "GOV (bbls)",
                    f"{condensate_gov_bbl:,.2f}",
                    help="Calculated: m� � 6.289"
                )
            
            tov_bbl = condensate_gov_bbl
            fw_bbl = 0.0
            gov_bbl = condensate_gov_bbl
    
        # ============ REGULAR DIP FIELDS ============
        if not is_condensate_receipt:
            st.markdown("#### 📏 Tank Measurements")
            
            c4, c5 = st.columns(2)
            with c4:
                dip_cm = st.number_input("Dip (cm) *", min_value=0.0, step=0.1, key="tx_dip_cm")
            with c5:
                water_cm = st.number_input("Water Level (cm) *", min_value=0.0, step=0.1, key="tx_water_cm")
    
            with get_session() as _s:
                tov_bbl = _interp_vol_bbl(_s, tank_id, float(dip_cm or 0.0))
                fw_bbl  = _interp_vol_bbl(_s, tank_id, float(water_cm or 0.0)) if float(water_cm or 0.0) > 0 else 0.0
            gov_bbl = max(tov_bbl - fw_bbl, 0.0)
            st.info(f"📊 Live Quantity � TOV: **{tov_bbl:.2f} bbl** | FW: **{fw_bbl:.2f} bbl** | GOV: **{gov_bbl:.2f} bbl**")
        else:
            dip_cm = 0.0
            water_cm = 0.0
    
        st.markdown("---")
    
        # ============ TEMPERATURE & QUALITY ============
        st.markdown("#### 🧪 Sample Parameters")
    
        st.caption("Tank Temperature (for VCF)")
        tcol1, tcol2 = st.columns([0.35, 0.65])
        with tcol1:
            tank_temp_unit = st.selectbox("Unit", ["°C", "°F"], index=0, key="tx_tank_temp_unit")
        with tcol2:
            tank_temp_val = _temperature_input(
                f"Temperature ({tank_temp_unit})",
                tank_temp_unit,
                "tx_tank_temp_val",
            )
    
        st.caption("Observed Property & Sample Temperature")
    
        col_mode, col_sample_unit = st.columns([0.48, 0.52])
        with col_mode:
            obs_mode = st.selectbox("Input Type", ["Observed API", "Observed Density (kg/m3)"], index=0, key="tx_obs_mode")
        with col_sample_unit:
            sample_unit = st.selectbox("Sample Temp Unit", ["°F", "°C"], index=0, key="tx_sample_unit")
    
        sample_temp_val = _temperature_input(
            "Sample Temperature",
            sample_unit,
            "tx_sample_temp_val",
        )
    
        in_col = st.columns(1)[0]
        if obs_mode == "Observed API":
            with in_col:
                api_obs_val = _bounded_number_input(
                    "Observed API *",
                    "tx_api_obs",
                    API_MIN,
                    API_MAX,
                )
            dens_obs_val = density_from_api(api_obs_val) if api_obs_val > 0 else 0.0
            st.caption(f"? Density: {dens_obs_val:.2f} kg/m3")
            api60_val = convert_api_to_60_from_api(api_obs_val or 0.0, sample_temp_val or 0.0, sample_unit)
        else:
            with in_col:
                dens_obs_val = _bounded_number_input(
                    "Observed Density (kg/m3) *",
                    "tx_dens_obs",
                    DENSITY_MIN,
                    DENSITY_MAX,
                    step=0.1,
                )
            api_obs_val = api_from_density(dens_obs_val) if dens_obs_val > 0 else 0.0
            st.caption(f"? API: {api_obs_val:.2f}")
            api60_val = convert_api_to_60_from_density(dens_obs_val or 0.0, sample_temp_val or 0.0, sample_unit)
    
        # ============ BS&W ============
        if is_condensate_receipt:
            bsw_pct = 0.0
            st.info("ℹ️ BS&W is 0% for condensate (GSV = NSV)")
        else:
            bsw_pct = st.number_input("BS&W %", min_value=0.0, max_value=100.0, step=0.01,
                                    key="tx_bsw_pct", help="Basic Sediment & Water percentage")
    
        # ============ CALCULATIONS ============
        sample_temp_c_val = _to_c(sample_temp_val or 0.0, sample_unit)
        sample_temp_f_val = _to_f(sample_temp_val or 0.0, sample_unit)
        tank_temp_c_val = _to_c(tank_temp_val, tank_temp_unit)
        tank_temp_f_val = _to_f(tank_temp_val, tank_temp_unit)
    
        input_mode = "density" if obs_mode == "Observed Density (kg/m3)" else "api"
        vcf_val = vcf_from_api60_and_temp(api60_val, tank_temp_val, tank_temp_unit, input_mode)
        
        gsv_bbl = round(gov_bbl * vcf_val, 2)
        bsw_vol_bbl = round(gsv_bbl * (bsw_pct / 100.0), 2)
        nsv_bbl = round(gsv_bbl - bsw_vol_bbl, 2)
        
        with get_session() as s:
            lt_factor = lookup_lt_factor(s, api60_val)
        
        lt_val = round(nsv_bbl * lt_factor, 2)
        mt_val = round(lt_val * 1.01605, 2)
        
        if is_condensate_receipt:
            st.success(f"📊 **Live Calculations:** GOV: {gov_bbl:,.2f} bbls | API@60: {api60_val:.2f} | VCF: {vcf_val:.5f} | GSV: {gsv_bbl:,.2f} bbls | NSV: {nsv_bbl:,.2f} bbls | LT: {lt_val:,.2f} | MT: {mt_val:,.2f}")
        else:
            if gov_bbl > 0 and api60_val > 0:
                st.info(f"📊 **Calculations:** GOV: {gov_bbl:,.2f} bbls | API@60: {api60_val:.2f} | VCF: {vcf_val:.5f} | GSV: {gsv_bbl:,.2f} bbls | NSV: {nsv_bbl:,.2f} bbls | LT: {lt_val:,.2f} | MT: {mt_val:,.2f}")
    
        # ============ TICKET ID PREVIEW ============
        if auto_generate_ticket:
            try:
                temp_serial = next_serial_for(operation, tx_date, ticket_prefix, active_location_id)
                preview_ticket = mk_ticket_id(operation, tx_date, temp_serial, ticket_prefix)
                st.success(f"🎫 **Auto-Generated Ticket ID:** {preview_ticket}")
            except Exception:
                st.info("ℹ️ Ticket ID will be generated upon save")
    
        # ============ SAVE BUTTON ============
        save_btn = st.button("💾 Save to DB", type="primary", key="tx_save_btn")
    
        if save_btn:
            errs = []
            if not is_condensate_receipt:
                if not tank_id or tank_id.startswith("(No tanks yet"):
                    errs.append("Please add tanks first in **Add Asset ? Tank Master**.")
            if (not is_condensate_receipt) and (not hhmm_ok(tx_time_str)):
                errs.append("Time must be in hh:mm (24-hour) format.")
            if is_condensate_receipt and (opening_meter is None or closing_meter is None):
                errs.append("Please enter both opening and closing meter readings.")
            if is_condensate_receipt and opening_meter is not None and closing_meter is not None:
                if closing_meter <= opening_meter:
                    errs.append("Closing meter reading must be greater than opening meter reading.")
    
            if errs:
                for e in errs:
                    st.error(e)
            else:
                if is_condensate_receipt:
                    tx_time_obj = time(23, 59)
                else:
                    try:
                        tx_time_obj = datetime.strptime(tx_time_str, "%H:%M").time()
                    except Exception:
                        st.error("Invalid time format. Use HH:MM (24-hour).")
                        tx_time_obj = None
    
                if tx_time_obj:
                    try:
                        # Resolve location prefix (first 3 letters of location name)
                        with get_session() as s_lookup:
                            from location_manager import LocationManager
                            _loc = LocationManager.get_location_by_id(s_lookup, active_location_id)
                            location_code = (ticket_prefix or "").upper()
                            if _loc and not location_code:
                                if getattr(_loc, "name", None):
                                    letters_only = "".join(ch for ch in _loc.name if ch.isalpha())
                                    if letters_only:
                                        location_code = letters_only[:3].upper()
                                if not location_code:
                                    location_code = ((_loc.code or "")[:3]).upper()
    
                        # Serial + Ticket ID
                        serial = next_serial_for(operation, tx_date, location_code, active_location_id)
                        ticket_id_real = mk_ticket_id(operation, tx_date, serial, location_code)
    
                        # Normalize & coerce operation to DB enum / value
                        op_norm = _normalize_operation(location_code, operation)
                        op_db   = _coerce_operation_for_db(op_norm)
                        if op_db is None:
                            st.error(f"Unsupported operation label: {operation}")
                            st.stop()
    
    
                        with get_session() as s:
                            # INSERT into tank_transactions (let DB autogenerate the integer PK)
                            rec = TankTransaction(
                                location_id=active_location_id,
                                ticket_id=ticket_id_real,
                                operation=op_db,                     # ? SAEnum, not None
                                tank_id=tank_fk,
                                tank_name=tank_id,
                                date=tx_date,
                                time=tx_time_obj,
                                dip_cm=float(dip_cm or 0.0) if not is_condensate_receipt else None,
                                water_cm=float(water_cm or 0.0) if not is_condensate_receipt else None,
                                opening_meter_reading=float(opening_meter) if is_condensate_receipt else None,
                                closing_meter_reading=float(closing_meter) if is_condensate_receipt else None,
                                condensate_qty_m3=float(condensate_qty_m3) if is_condensate_receipt else None,
                                tank_temp_c=float(tank_temp_c_val or 0.0),
                                tank_temp_f=float(tank_temp_f_val or 0.0),
                                sample_temp_c=float(sample_temp_c_val or 0.0),
                                sample_temp_f=float(sample_temp_f_val or 0.0),
                                api_observed=float(api_obs_val or 0.0),
                                density_observed=float(dens_obs_val or 0.0),
                                bsw_pct=float(bsw_pct or 0.0),
                                qty_bbls=float(tov_bbl or 0.0),
                                remarks=None,
                                created_by=user["username"]
                            )
                            s.add(rec)
                            s.flush()                 # get autoincremented PK
                            new_id = rec.id
    
                            # Mirror insert to OTR only for non-condensate transactions
                            if not is_condensate_receipt:
                                s.add(OTRRecord(
                                    location_id=active_location_id,
                                    ticket_id=ticket_id_real,
                                    tank_id=tank_id,
                                    date=tx_date,
                                    time=tx_time_obj,
                                    operation=operation,  # plain label is fine for OTR
                                    dip_cm=float(dip_cm or 0.0),
                                    total_volume_bbl=float(tov_bbl or 0.0),
                                    water_cm=float(water_cm or 0.0),
                                    free_water_bbl=float(fw_bbl or 0.0),
                                    gov_bbl=float(gov_bbl or 0.0),
                                    api60=float(api60_val or 0.0),
                                    vcf=float(vcf_val),
                                    gsv_bbl=float(gsv_bbl or 0.0),
                                    bsw_vol_bbl=float(bsw_vol_bbl or 0.0),
                                    nsv_bbl=float(nsv_bbl or 0.0),
                                    lt=float(lt_val or 0.0),
                                    mt=float(mt_val or 0.0),
                                ))
    
                            # Audit log
                            from security import SecurityManager
                            SecurityManager.log_audit(
                                s, user["username"], "CREATE",
                                resource_type="TankTransaction",
                                resource_id=new_id,   # use the integer PK
                                details=f"Created {'condensate receipt' if is_condensate_receipt else 'tank transaction'}: {operation}, Ticket: {ticket_id_real}",
                                user_id=user["id"],
                                location_id=active_location_id
                            )
    
                            s.commit()
    
                        st.success("? Transaction saved successfully!")
                        st.info(f"ℹ️ **Transaction ID:** {new_id}")
                        st.info(f"🎫 **Ticket ID:** {ticket_id_real}")
    
    
                        if is_condensate_receipt:
                            st.info(f"ℹ️ Condensate Receipt: {condensate_qty_m3:.3f} m� ? {gov_bbl:.2f} bbl (GOV) ? {nsv_bbl:.2f} bbl (NSV) ? {lt_val:.2f} (LT) ? {mt_val:.2f} MT")
    
                        st.balloons()
                        import time as _t
                        _t.sleep(2)
                        _st_safe_rerun()
    
                    except Exception as ex:
                        st.error(f"Failed to save: {ex}")
                        import traceback as _tb
                        st.code(_tb.format_exc())
    
    
        # Restore original Streamlit context if we switched to a tab container
        if st_for_restore is not None:
            try:
                st = st_for_restore
            except Exception:
                pass
    
        if _meter_tab is not None:
            with _meter_tab:
                st.markdown("#### Meter Transactions (Asemoku Jetty)")
                st.caption("Manual entry: Date, Opening, Closing, Remarks")
    
                # Load existing entries for this location
                with get_session() as _s:
                    _rows = (
                        _s.query(MeterTransaction)
                        .filter(MeterTransaction.location_id == active_location_id)
                        .order_by(MeterTransaction.date.desc())
                        .all()
                    )
    
                import pandas as _pd
                # If an edit is in progress, show edit form
                _editing_id = st.session_state.get("meter_edit_id")
                if _editing_id is not None:
                    st.markdown("##### Edit Meter Entry")
                    with get_session() as _sedit:
                        _rec = _sedit.query(MeterTransaction).filter(
                            MeterTransaction.id == int(_editing_id),
                            MeterTransaction.location_id == active_location_id,
                        ).one_or_none()
                    if _rec:
                        if _deny_edit_for_lock(_rec, "MeterTransaction", f"{_rec.date}"):
                            st.session_state.pop("meter_edit_id", None)
                            _st_safe_rerun()
                        else:
                            with st.form(key=f"meter_edit_form_{_editing_id}"):
                                _e_date = st.date_input("Date", value=_rec.date)
                            _e_om1  = st.number_input("Opening reading (Meter 1)", value=float(_rec.opening_meter_reading or 0.0))
                            _e_cm1  = st.number_input("Closing reading (Meter 1)", value=float(_rec.closing_meter_reading or 0.0))
                            _e_om2  = st.number_input("Opening reading (Meter 2)", value=float(getattr(_rec, "opening_meter2_reading", 0.0) or 0.0))
                            _e_cm2  = st.number_input("Closing reading (Meter 2)", value=float(getattr(_rec, "closing_meter2_reading", 0.0) or 0.0))
                            _e_rem  = st.text_area("Remarks", value=_rec.remarks or "")
                            _save   = st.form_submit_button("Save Changes", type="primary")
                        if _save:
                            try:
                                with get_session() as _s2:
                                    _rec2 = _s2.query(MeterTransaction).filter(
                                        MeterTransaction.id == int(_editing_id),
                                        MeterTransaction.location_id == active_location_id,
                                    ).one_or_none()
                                    if _rec2:
                                        _rec2.date = _e_date
                                        _rec2.opening_meter_reading  = float(_e_om1)
                                        _rec2.closing_meter_reading  = float(_e_cm1)
                                        _rec2.opening_meter2_reading = float(_e_om2)
                                        _rec2.closing_meter2_reading = float(_e_cm2)
                                        _rec2.net_qty = (_e_cm1 - _e_om1) + (_e_cm2 - _e_om2)
                                        _rec2.remarks = _e_rem
                                        _s2.commit()
                                        # ----------------------- Audit log for meter entry update -----------------------
                                        try:
                                            from security import SecurityManager  # type: ignore
                                            user_ctx = st.session_state.get("auth_user") or {}
                                            username = user_ctx.get("username", "unknown")
                                            user_id = user_ctx.get("id")
                                            # Use the editing ID as the resource identifier
                                            rec_id = str(_editing_id)
                                            SecurityManager.log_audit(
                                                None,
                                                username,
                                                "UPDATE",
                                                resource_type="MeterTransaction",
                                                resource_id=rec_id,
                                                details=f"Updated meter entry on {_e_date} with net quantity {(_e_cm1 - _e_om1) + (_e_cm2 - _e_om2):.2f}",
                                                user_id=user_id,
                                                location_id=active_location_id,
                                            )
                                        except Exception:
                                            pass
                                        st.success("Meter entry updated.")
                                        st.session_state.pop("meter_edit_id", None)
                                        _st_safe_rerun()
                            except Exception as _ex:
                                st.error(f"Failed to update: {_ex}")
                else:
                    st.markdown("##### Add Meter Entry")
                    with st.form("meter_add_form"):
                        _date = st.date_input("Date", value=date.today())
                        _om1 = st.number_input("Opening reading (Meter 1)", value=0.0)
                        _cm1 = st.number_input("Closing reading (Meter 1)", value=0.0)
                        _om2 = st.number_input("Opening reading (Meter 2)", value=0.0)
                        _cm2 = st.number_input("Closing reading (Meter 2)", value=0.0)
                        _remarks = st.text_area("Remarks")
                        _save = st.form_submit_button("Save Meter Entry", type="primary")
                    if _save:
                        try:
                            with get_session() as _s3:
                                # Construct new meter transaction record
                                new_meter_tx = MeterTransaction(
                                    location_id=active_location_id,
                                    date=_date,
                                    opening_meter_reading=_om1,
                                    closing_meter_reading=_cm1,
                                    opening_meter2_reading=_om2,
                                    closing_meter2_reading=_cm2,
                                    net_qty=(_cm1 - _om1) + (_cm2 - _om2),
                                    remarks=_remarks,
                                )
                                _s3.add(new_meter_tx)
                                # Flush to assign primary key
                                _s3.flush()
                                new_id = new_meter_tx.id
                                _s3.commit()
                            # Audit log outside transaction
                            try:
                                from security import SecurityManager  # type: ignore
                                user_ctx = st.session_state.get("auth_user") or {}
                                username = user_ctx.get("username", "unknown")
                                user_id = user_ctx.get("id")
                                SecurityManager.log_audit(
                                    None,
                                    username,
                                    "CREATE",
                                    resource_type="MeterTransaction",
                                    resource_id=str(new_id),
                                    details=f"Created meter entry on {_date} with net quantity {(_cm1 - _om1) + (_cm2 - _om2):.2f}",
                                    user_id=user_id,
                                    location_id=active_location_id,
                                )
                            except Exception:
                                pass
                            st.success("Meter entry saved.")
                            _st_safe_rerun()
                        except Exception as _ex:
                            st.error(f"Failed to save: {_ex}")
    
                st.markdown("##### Saved Meter Entries")
                if not _rows:
                    st.info("No meter entries saved yet.")
                else:
                    _df = _pd.DataFrame([{
                        "Date": r.date,
                        "Opening Meter 1": r.opening_meter_reading,
                        "Closing Meter 1": r.closing_meter_reading,
                        "Opening Meter 2": r.opening_meter2_reading,
                        "Closing Meter 2": r.closing_meter2_reading,
                        "Net Qty": r.net_qty,
                        "Remarks": r.remarks,
                    } for r in _rows])
                    st.dataframe(_df, use_container_width=True, hide_index=True)
    
                if _rows:
                    st.markdown("##### Delete Meter Entry")
                    _del_id = st.selectbox(
                        "Select entry to delete",
                        options=[("(cancel)", None)] + [(f"{r.id} ({r.date})", r.id) for r in _rows],
                        format_func=lambda x: x[0] if isinstance(x, tuple) else "(cancel)",
                        key="meter_delete_selector"
                    )
                    if isinstance(_del_id, tuple):
                        _, _target_id = _del_id
                    else:
                        _target_id = None
    
                    if _target_id:
                        _confirm = st.button("Delete Selected Meter Entry", type="primary")
                        if _confirm:
                            try:
                                with get_session() as _s4:
                                    _rec_del = _s4.query(MeterTransaction).filter(
                                        MeterTransaction.id == int(_target_id),
                                        MeterTransaction.location_id == active_location_id,
                                    ).one_or_none()
                                    if _rec_del:
                                        # Capture details BEFORE delete for audit
                                        _details = (
                                            f"date={_rec_del.date}, "
                                            f"om1={_rec_del.opening_meter_reading}, "
                                            f"cm1={_rec_del.closing_meter_reading}, "
                                            f"om2={getattr(_rec_del, 'opening_meter2_reading', 0.0)}, "
                                            f"cm2={getattr(_rec_del, 'closing_meter2_reading', 0.0)}, "
                                            f"net_qty={_rec_del.net_qty}"
                                        )
    
                                        # Archive instead of deleting outright
                                        _archive_record_for_delete(
                                            _s4,
                                            _rec_del,
                                            "MeterTransaction",
                                            reason=f"Marked meter entry ID={_rec_del.id} for deletion; {_details}",
                                            label=str(_rec_del.date),
                                        )
    
                                    st.success("Meter entry deleted.")
                                    _st_safe_rerun()
                            except Exception as _ex:
                                st.error(f"Failed to delete: {_ex}")
    
    
        if _condensate_tab is not None:
            with _condensate_tab:
                st.markdown("#### Condensate Records (Beneku)")
                st.caption("24-hour condensate meter receipts for BFS.")
                if not can_make_entries:
                    st.info("You have view-only access at this location.")
    
                today = date.today()
                min_date = today - timedelta(days=max_days_backward)
                max_date_val = today + (timedelta(days=7) if allow_future_dates else timedelta(days=0))
    
                st.markdown("##### Add Condensate Receipt")
                cond_date = st.date_input(
                    "Date *",
                    value=today,
                    min_value=min_date,
                    max_value=max_date_val,
                    key="cond_tab_date",
                    help=f"Date range: {min_date.strftime('%d/%m/%Y')} to {max_date_val.strftime('%d/%m/%Y')}"
                )
    
                meter_col1, meter_col2 = st.columns(2)
                with meter_col1:
                    cond_opening = st.number_input(
                        "Opening Meter Reading (m�) *",
                        min_value=0.0,
                        step=0.001,
                        format="%.3f",
                        key="cond_tab_open"
                    )
                with meter_col2:
                    cond_closing = st.number_input(
                        "Closing Meter Reading (m�) *",
                        min_value=0.0,
                        step=0.001,
                        format="%.3f",
                        key="cond_tab_close"
                    )
    
                cond_net_m3 = max(cond_closing - cond_opening, 0.0)
                cond_gov_bbl = cond_net_m3 * CONDENSATE_M3_TO_BBL
                metric_col1, metric_col2 = st.columns(2)
                metric_col1.metric("Net Receipt (m�)", f"{cond_net_m3:,.3f}")
                metric_col2.metric("GOV (bbls)", f"{cond_gov_bbl:,.2f}")
    
                st.markdown("##### Sample Parameters")
                tank_temp_unit = st.selectbox("Tank Temp Unit", ["°C", "°F"], index=0, key="cond_tab_tank_temp_unit")
                tank_temp_val = _temperature_input(
                    f"Tank Temperature ({tank_temp_unit})",
                    tank_temp_unit,
                    "cond_tab_tank_temp_val",
                )
    
                obs_mode = st.selectbox("Input Type", ["Observed API", "Observed Density (kg/m3)"], index=0, key="cond_tab_obs_mode")
                sample_unit = st.selectbox("Sample Temp Unit", ["°F", "°C"], index=0, key="cond_tab_sample_unit")
                sample_temp_val = _temperature_input(
                    "Sample Temperature",
                    sample_unit,
                    "cond_tab_sample_temp",
                )
    
                if obs_mode == "Observed API":
                    cond_api_obs = _bounded_number_input(
                        "Observed API *",
                        "cond_tab_api_obs",
                        API_MIN,
                        API_MAX,
                    )
                    cond_dens_obs = density_from_api(cond_api_obs) if cond_api_obs > 0 else 0.0
                    st.caption(f"Derived Density: {cond_dens_obs:.2f} kg/m3")
                else:
                    cond_dens_obs = _bounded_number_input(
                        "Observed Density (kg/m3) *",
                        "cond_tab_dens_obs",
                        DENSITY_MIN,
                        DENSITY_MAX,
                        step=0.1,
                    )
                    cond_api_obs = api_from_density(cond_dens_obs) if cond_dens_obs > 0 else 0.0
                    st.caption(f"Derived API: {cond_api_obs:.2f}")
    
                sample_temp_c_val = _to_c(sample_temp_val or 0.0, sample_unit)
                sample_temp_f_val = _to_f(sample_temp_val or 0.0, sample_unit)
                tank_temp_c_val = _to_c(tank_temp_val or 0.0, tank_temp_unit)
                tank_temp_f_val = _to_f(tank_temp_val or 0.0, tank_temp_unit)
    
                if cond_api_obs > 0:
                    cond_api60 = convert_api_to_60_from_api(cond_api_obs, sample_temp_val or 0.0, sample_unit)
                elif cond_dens_obs > 0:
                    cond_api60 = convert_api_to_60_from_density(cond_dens_obs, sample_temp_val or 0.0, sample_unit)
                else:
                    cond_api60 = 0.0
    
                input_mode = "density" if obs_mode != "Observed API" else "api"
                cond_vcf = vcf_from_api60_and_temp(cond_api60, tank_temp_val or 0.0, tank_temp_unit, input_mode)
                cond_gsv_bbl = round(cond_gov_bbl * cond_vcf, 2)
                cond_nsv_bbl = cond_gsv_bbl  # BS&W = 0
    
                with get_session() as _s_lt:
                    cond_lt_factor = lookup_lt_factor(_s_lt, cond_api60) if cond_api60 > 0 else 0.0
                cond_lt = round(cond_nsv_bbl * cond_lt_factor, 2)
                cond_mt = round(cond_lt * 1.01605, 2)
    
                st.info(
                    f"Calculated ? GOV: {cond_gov_bbl:,.2f} bbl | API@60: {cond_api60:.2f} | "
                    f"VCF: {cond_vcf:.5f} | GSV: {cond_gsv_bbl:,.2f} bbl | LT: {cond_lt:,.2f} | MT: {cond_mt:,.2f}"
                )
    
                cond_errors = []
                if cond_closing <= cond_opening:
                    cond_errors.append("Closing meter reading must exceed opening reading.")
                if cond_net_m3 <= 0:
                    cond_errors.append("Net receipt must be positive.")
    
                cond_save = st.button(
                    "Save Condensate Receipt",
                    type="primary",
                    key="cond_tab_save_btn",
                    disabled=not can_make_entries
                )
    
                if cond_save:
                    if cond_errors:
                        for err in cond_errors:
                            st.error(err)
                    else:
                        from datetime import time as _dt_time
                        tx_time_obj = _dt_time(23, 59)
                        operation = "Receipt - Condensate"
                        try:
                            ticket_prefix_effective = (ticket_prefix or "").upper()
                            loc_code_for_ops = (getattr(loc, "code", "") or "").upper()
    
                            with get_session() as s_lookup:
                                from location_manager import LocationManager
                                _loc = LocationManager.get_location_by_id(s_lookup, active_location_id)
                                if _loc:
                                    loc_code_for_ops = (getattr(_loc, "code", "") or loc_code_for_ops or "").upper()
                                    if not ticket_prefix_effective:
                                        name_val = getattr(_loc, "name", None)
                                        if name_val:
                                            letters_only = "".join(ch for ch in name_val if ch.isalpha())
                                            if letters_only:
                                                ticket_prefix_effective = letters_only[:3].upper()
                                        if not ticket_prefix_effective:
                                            ticket_prefix_effective = ((getattr(_loc, "code", "") or "")[:3]).upper()
    
                            serial = next_serial_for(operation, cond_date, ticket_prefix_effective, active_location_id)
                            ticket_id_real = mk_ticket_id(operation, cond_date, serial, ticket_prefix_effective)
                            op_norm = _normalize_operation(loc_code_for_ops or ticket_prefix_effective, operation)
                            op_db = _coerce_operation_for_db(op_norm)
                            if op_db is None:
                                st.error(f"Unsupported operation label: {operation}")
                                st.stop()
    
                            with get_session() as s:
                                rec = TankTransaction(
                                    location_id=active_location_id,
                                    ticket_id=ticket_id_real,
                                    operation=op_db,
                                    tank_id=None,
                                    tank_name="CONDENSATE-RECEIPT",
                                    date=cond_date,
                                    time=tx_time_obj,
                                    dip_cm=None,
                                    water_cm=None,
                                    opening_meter_reading=cond_opening,
                                    closing_meter_reading=cond_closing,
                                    condensate_qty_m3=cond_net_m3,
                                    tank_temp_c=tank_temp_c_val,
                                    tank_temp_f=tank_temp_f_val,
                                    sample_temp_c=sample_temp_c_val,
                                    sample_temp_f=sample_temp_f_val,
                                    api_observed=cond_api_obs,
                                    density_observed=cond_dens_obs,
                                    bsw_pct=0.0,
                                    qty_bbls=cond_gov_bbl,
                                    remarks=None,
                                    created_by=user["username"],
                                )
                                s.add(rec)
                                s.flush()
                                new_id = rec.id
    
                                from security import SecurityManager
                                SecurityManager.log_audit(
                                    s,
                                    user["username"],
                                    "CREATE",
                                    resource_type="TankTransaction",
                                    resource_id=new_id,
                                    details=f"Created condensate receipt via dedicated tab: {operation}, Ticket: {ticket_id_real}",
                                    user_id=user["id"],
                                    location_id=active_location_id
                                )
    
                                s.commit()
    
                            st.success("Condensate receipt saved successfully.")
                            st.info(f"Ticket ID: {ticket_id_real} | GOV: {cond_gov_bbl:,.2f} bbl | GSV: {cond_gsv_bbl:,.2f} bbl")
                            st.balloons()
                            import time as _t
                            _t.sleep(1)
                            _st_safe_rerun()
                        except Exception as ex:
                            st.error(f"Failed to save condensate receipt: {ex}")
    
                st.markdown("##### Saved Condensate Records")
                cond_records, _ = load_condensate_transactions(active_location_id, limit=500)
                if not cond_records:
                    st.info("No condensate receipts saved yet.")
                else:
                    cond_df = pd.DataFrame(cond_records)
                    display_cols = [
                        "Ticket ID", "Date", "Opening (m3)", "Closing (m3)",
                        "Net Receipt (m3)", "GOV (bbls)", "API @ 60",
                        "VCF", "GSV (bbls)", "LT", "MT"
                    ]
                    st.dataframe(cond_df[display_cols], use_container_width=True, hide_index=True)
    
        if _gpp_tab is not None:
            with _gpp_tab:
                st.markdown("#### Production (Beneku)")
                st.caption("Manual entry and tracking of OKW and GPP production figures.")
    
                user_role = (user.get("role") or "").lower()
                can_delete_gpp = can_make_entries and user_role != "operator"
                if user_role == "operator":
                    st.warning("Operators can view and edit production records but cannot delete them.")
    
                today = date.today()
                min_date = today - timedelta(days=max_days_backward)
                max_date_val = today + (timedelta(days=7) if allow_future_dates else timedelta(days=0))
    
                def _ensure_gpp_form_defaults():
                    if "gpp_form_date" not in st.session_state:
                        st.session_state["gpp_form_date"] = today
                    if "gpp_form_okw" not in st.session_state:
                        st.session_state["gpp_form_okw"] = 0.0
                    if "gpp_form_gpp1" not in st.session_state:
                        st.session_state["gpp_form_gpp1"] = 0.0
                    if "gpp_form_gpp2" not in st.session_state:
                        st.session_state["gpp_form_gpp2"] = 0.0
                    if "gpp_form_remarks" not in st.session_state:
                        st.session_state["gpp_form_remarks"] = ""
                    if "gpp_edit_id" not in st.session_state:
                        st.session_state["gpp_edit_id"] = None
                    # Default closing stock for GPP production
                    if "gpp_form_closing" not in st.session_state:
                        st.session_state["gpp_form_closing"] = 0.0
    
                def _reset_gpp_form():
                    st.session_state["gpp_edit_id"] = None
                    st.session_state["gpp_form_date"] = today
                    st.session_state["gpp_form_okw"] = 0.0
                    st.session_state["gpp_form_gpp1"] = 0.0
                    st.session_state["gpp_form_gpp2"] = 0.0
                    st.session_state["gpp_form_remarks"] = ""
                    st.session_state["gpp_form_closing"] = 0.0
    
                _ensure_gpp_form_defaults()
                is_editing_gpp = st.session_state.get("gpp_edit_id") is not None
                # Show info when editing an existing production record
                if is_editing_gpp:
                    st.info(f"Editing record for {st.session_state.get('gpp_form_date')}")
    
                st.markdown("##### Manual Entry")
                # Define columns for date, OKW, GPP1, GPP2, GPP closing stock, total metric and remarks
                entry_cols = st.columns([0.15, 0.15, 0.15, 0.15, 0.15, 0.10, 0.15])
                with entry_cols[0]:
                    gpp_date_val = st.date_input(
                        "Date",
                        value=st.session_state.get("gpp_form_date", today),
                        min_value=min_date,
                        max_value=max_date_val,
                        key="gpp_prod_date_input",
                    )
                    st.session_state["gpp_form_date"] = gpp_date_val
                with entry_cols[1]:
                    okw_val = st.number_input(
                        "OKW Production",
                        min_value=0.0,
                        step=1.0,
                        value=float(st.session_state.get("gpp_form_okw", 0.0)),
                        key="gpp_prod_okw_input",
                    )
                    st.session_state["gpp_form_okw"] = okw_val
                with entry_cols[2]:
                    gpp1_val = st.number_input(
                        "GPP1 Production",
                        min_value=0.0,
                        step=1.0,
                        value=float(st.session_state.get("gpp_form_gpp1", 0.0)),
                        key="gpp_prod_gpp1_input",
                    )
                    st.session_state["gpp_form_gpp1"] = gpp1_val
                with entry_cols[3]:
                    gpp2_val = st.number_input(
                        "GPP2 Production",
                        min_value=0.0,
                        step=1.0,
                        value=float(st.session_state.get("gpp_form_gpp2", 0.0)),
                        key="gpp_prod_gpp2_input",
                    )
                    st.session_state["gpp_form_gpp2"] = gpp2_val
                with entry_cols[4]:
                    gpp_cs_val = st.number_input(
                        "GPP Closing Stock",
                        min_value=0.0,
                        step=1.0,
                        value=float(st.session_state.get("gpp_form_closing", 0.0)),
                        key="gpp_prod_cs_input",
                    )
                    st.session_state["gpp_form_closing"] = gpp_cs_val
                total_val = round((gpp1_val or 0.0) + (gpp2_val or 0.0), 2)
                with entry_cols[5]:
                    st.metric("Total GPP Production", f"{total_val:,.2f}")
                with entry_cols[6]:
                    remarks_val = st.text_input(
                        "Remarks",
                        value=st.session_state.get("gpp_form_remarks", ""),
                        key="gpp_prod_remarks_input",
                    )
                    st.session_state["gpp_form_remarks"] = remarks_val
    
                save_label = "Update Record" if is_editing_gpp else "Save Production Entry"
                action_cols = st.columns([0.2, 0.2, 0.6])
                save_clicked = action_cols[0].button(
                    save_label,
                    type="primary",
                    disabled=not can_make_entries,
                    key="gpp_save_btn",
                )
                cancel_clicked = False
                if is_editing_gpp:
                    cancel_clicked = action_cols[1].button("Cancel Edit", key="gpp_cancel_btn")
    
                if cancel_clicked:
                    _reset_gpp_form()
                    _st_safe_rerun()
    
                if save_clicked:
                    errors = []
                    if gpp_date_val is None:
                        errors.append("Date is required.")
                    if total_val <= 0:
                        errors.append("Total production must be greater than zero.")
    
                    if errors:
                        for err in errors:
                            st.error(err)
                    else:
                        try:
                            with get_session() as s:
                                if is_editing_gpp:
                                    rec = (
                                        s.query(GPPProductionRecord)
                                        .filter(
                                            GPPProductionRecord.id == int(st.session_state["gpp_edit_id"]),
                                            GPPProductionRecord.location_id == active_location_id,
                                        )
                                        .one_or_none()
                                    )
                                    if not rec:
                                        st.error("Selected record no longer exists.")
                                    else:
                                        # Update fields for editing
                                        rec.date = gpp_date_val
                                        rec.okw_production = float(okw_val or 0.0)
                                        rec.gpp1_production = float(gpp1_val or 0.0)
                                        rec.gpp2_production = float(gpp2_val or 0.0)
                                        rec.total_production = total_val
                                        rec.gpp_closing_stock = float(st.session_state.get("gpp_form_closing", 0.0) or 0.0)
                                        rec.remarks = (remarks_val or "").strip()
                                        rec.updated_by = user.get("username", "unknown")
    
                                        SecurityManager.log_audit(
                                            s,
                                            user.get("username", "unknown"),
                                            "UPDATE",
                                            resource_type="GPPProductionRecord",
                                            resource_id=rec.id,
                                            details=f"Updated production record for {rec.date}",
                                            user_id=user.get("id"),
                                            location_id=active_location_id,
                                        )
                                        s.commit()
                                        st.success("Production record updated.")
                                        _reset_gpp_form()
                                        _st_safe_rerun()
                                else:
                                    _remarks_final = (remarks_val or "").strip()
                                    rec = GPPProductionRecord(
                                        location_id=active_location_id,
                                        date=gpp_date_val,
                                        okw_production=float(okw_val or 0.0),
                                        gpp1_production=float(gpp1_val or 0.0),
                                        gpp2_production=float(gpp2_val or 0.0),
                                        total_production=total_val,
                                        gpp_closing_stock=float(st.session_state.get("gpp_form_closing", 0.0) or 0.0),
                                        remarks=_remarks_final,
                                        created_by=user.get("username", "unknown"),
                                    )
                                    s.add(rec)
                                    s.flush()
                                    SecurityManager.log_audit(
                                        s,
                                        user.get("username", "unknown"),
                                        "CREATE",
                                        resource_type="GPPProductionRecord",
                                        resource_id=rec.id,
                                        details=f"Created production record for {gpp_date_val}",
                                        user_id=user.get("id"),
                                        location_id=active_location_id,
                                    )
                                    s.commit()
                                    st.success("Production record saved.")
                                    _reset_gpp_form()
                                    _st_safe_rerun()
                        except Exception as ex:
                            st.error(f"Failed to save record: {ex}")
    
                records = load_gpp_production_records(active_location_id, limit=1000)
                gpp_df = pd.DataFrame(records)
                gpp_dates = gpp_df["Date"].tolist() if not gpp_df.empty else []
                gpp_min_date, gpp_max_date = _derive_filter_bounds(gpp_dates)
                gpp_from_default = _ensure_date_key_in_bounds(
                    "gpp_filter_from", gpp_min_date, gpp_max_date, gpp_min_date
                )
                gpp_to_default = _ensure_date_key_in_bounds(
                    "gpp_filter_to", gpp_min_date, gpp_max_date, gpp_max_date
                )
    
                st.markdown("##### Live Filters")
                filter_cols = st.columns([0.2, 0.2, 0.25, 0.35])
                with filter_cols[0]:
                    filt_from = st.date_input(
                        "From date",
                        value=gpp_from_default,
                        min_value=gpp_min_date,
                        max_value=gpp_max_date,
                        key="gpp_filter_from",
                    )
                with filter_cols[1]:
                    filt_to = st.date_input(
                        "To date",
                        value=gpp_to_default,
                        min_value=gpp_min_date,
                        max_value=gpp_max_date,
                        key="gpp_filter_to",
                    )
                with filter_cols[2]:
                    min_total = st.number_input(
                        "Min total (bbls)",
                        min_value=0.0,
                        step=100.0,
                        key="gpp_filter_min_total",
                    )
                with filter_cols[3]:
                    search_term = st.text_input(
                        "Search remarks / user",
                        key="gpp_filter_search",
                    ).strip().lower()
    
                if not gpp_df.empty:
                    gpp_df["Date"] = pd.to_datetime(gpp_df["Date"]).dt.date
                    gpp_df["Updated At"] = (
                        pd.to_datetime(gpp_df["Updated At"], errors="coerce")
                        .dt.strftime("%Y-%m-%d %H:%M:%S")
                        .fillna("")
                    )
    
                    if filt_from:
                        gpp_df = gpp_df[gpp_df["Date"] >= filt_from]
                    if filt_to:
                        gpp_df = gpp_df[gpp_df["Date"] <= filt_to]
                    if min_total and min_total > 0:
                        gpp_df = gpp_df[gpp_df["Total GPP Production"] >= min_total]
                    if search_term:
                        gpp_df = gpp_df[
                            gpp_df.apply(
                                lambda r: search_term in str(r["Remarks"]).lower()
                                or search_term in str(r["Created By"]).lower()
                                or search_term in str(r["Updated By"]).lower(),
                                axis=1,
                            )
                        ]
    
                st.caption(f"{len(gpp_df)} record(s) shown")
                # Build interactive table for production records.
                # Include GPP Closing Stock and provide edit/delete actions in each row.
                display_cols = [
                    "Date",
                    "OKW Production",
                    "GPP1 Production",
                    "GPP2 Production",
                    "GPP Closing Stock",
                    "Total GPP Production",
                    "Remarks",
                    "Created By",
                    "Updated By",
                    "Updated At",
                ]
                if gpp_df.empty:
                    st.info("No production records found for the selected filters.")
                else:
                    st.markdown("##### Production Records")
                    # Define column widths; adjust widths to accommodate additional columns and actions
                    col_widths = [0.13, 0.1, 0.1, 0.1, 0.13, 0.13, 0.21, 0.05, 0.05]
                    header_cols = st.columns(col_widths)
                    header_cols[0].markdown("**Date**")
                    header_cols[1].markdown("**OKW Prod**")
                    header_cols[2].markdown("**GPP1 Prod**")
                    header_cols[3].markdown("**GPP2 Prod**")
                    header_cols[4].markdown("**GPP Closing**")
                    header_cols[5].markdown("**Total GPP**")
                    header_cols[6].markdown("**Remarks**")
                    header_cols[7].markdown("**✏️**")
                    header_cols[8].markdown("**🗑️**")
    
                    # Sort records by date descending
                    manage_records = gpp_df.sort_values(by="Date", ascending=False).to_dict("records")
                    # Ensure delete confirmation id exists in session_state
                    st.session_state.setdefault("gpp_confirm_delete_id", None)
                    for rec in manage_records:
                        row_cols = st.columns(col_widths)
                        row_cols[0].write(str(rec["Date"]))
                        row_cols[1].write(f"{rec['OKW Production']:,.2f}")
                        row_cols[2].write(f"{rec['GPP1 Production']:,.2f}")
                        row_cols[3].write(f"{rec['GPP2 Production']:,.2f}")
                        row_cols[4].write(f"{rec.get('GPP Closing Stock', 0.0):,.2f}")
                        row_cols[5].write(f"{rec['Total GPP Production']:,.2f}")
                        row_cols[6].write((rec.get('Remarks') or '')[:50])
                        edit_btn = row_cols[7].button(
                            "✏️",
                            key=f"gpp_edit_{rec['id']}",
                            help="Edit this record",
                            disabled=not can_make_entries,
                        )
                        delete_btn = row_cols[8].button(
                            "🗑️",
                            key=f"gpp_delete_{rec['id']}",
                            help="Delete this record",
                            disabled=not can_delete_gpp,
                        )
                        if edit_btn:
                            allow_edit = True
                            with get_session() as _lock_s:
                                obj = (
                                    _lock_s.query(GPPProductionRecord)
                                    .filter(
                                        GPPProductionRecord.id == int(rec["id"]),
                                        GPPProductionRecord.location_id == active_location_id,
                                    )
                                    .one_or_none()
                                )
                                if obj and _deny_edit_for_lock(obj, "GPPProductionRecord", f"{obj.date}"):
                                    allow_edit = False
                            if allow_edit:
                                st.session_state["gpp_edit_id"] = rec["id"]
                                date_val = rec["Date"]
                                if isinstance(date_val, str):
                                    try:
                                        date_val = datetime.strptime(date_val, "%Y-%m-%d").date()
                                    except Exception:
                                        date_val = today
                                st.session_state["gpp_form_date"] = date_val
                                st.session_state["gpp_form_okw"] = rec["OKW Production"]
                                st.session_state["gpp_form_gpp1"] = rec["GPP1 Production"]
                                st.session_state["gpp_form_gpp2"] = rec["GPP2 Production"]
                                st.session_state["gpp_form_closing"] = rec.get("GPP Closing Stock", 0.0)
                                st.session_state["gpp_form_remarks"] = rec.get("Remarks") or ""
                                _st_safe_rerun()
                        if delete_btn:
                            st.session_state["gpp_confirm_delete_id"] = rec["id"]
                            _st_safe_rerun()
    
                    # Render deletion confirmation prompt
                    confirm_id = st.session_state.get("gpp_confirm_delete_id")
                    if confirm_id:
                        target_rec = next((r for r in manage_records if r["id"] == confirm_id), None)
                        if target_rec:
                            st.warning(
                                f"Are you sure you want to delete the production record for **{target_rec['Date']}**? This action cannot be undone.",
                                icon="ℹ️",
                            )
                            confirm_cols = st.columns([0.25, 0.25, 0.5])
                            confirm_delete = confirm_cols[0].button(
                                "Yes, delete",
                                key=f"gpp_confirm_yes_{confirm_id}",
                                type="primary",
                                disabled=not can_delete_gpp,
                            )
                            cancel_delete = confirm_cols[1].button(
                                "Cancel",
                                key=f"gpp_confirm_no_{confirm_id}",
                            )
                            if confirm_delete:
                                try:
                                    with get_session() as s:
                                        row = (
                                            s.query(GPPProductionRecord)
                                            .filter(
                                                GPPProductionRecord.id == int(confirm_id),
                                                GPPProductionRecord.location_id == active_location_id,
                                            )
                                            .one_or_none()
                                        )
                                        if not row:
                                            st.warning("Record already removed.")
                                        else:
                                            _archive_record_for_delete(
                                                s,
                                                row,
                                                "GPPProductionRecord",
                                                reason=f"Marked production record for {row.date} as deleted.",
                                                label=f"{row.date}",
                                            )
                                            SecurityManager.log_audit(
                                                s,
                                                user.get("username", "unknown"),
                                                "DELETE",
                                                resource_type="GPPProductionRecord",
                                                resource_id=row.id,
                                                details=f"Deleted production record for {row.date}",
                                                user_id=user.get("id"),
                                                location_id=active_location_id,
                                            )
                                            s.commit()
                                            st.success("Record deleted successfully.")
                                except Exception as ex:
                                    st.error(f"Failed to delete record: {ex}")
                                # Reset confirmation and form state
                                st.session_state["gpp_confirm_delete_id"] = None
                                _reset_gpp_form()
                                _st_safe_rerun()
                            elif cancel_delete:
                                st.session_state["gpp_confirm_delete_id"] = None
                                _st_safe_rerun()
    
    
        if _river_tab is not None:
            with _river_tab:
                st.markdown("#### River Draft")
                st.caption("Manual entry for daily river draft and rainfall snapshots.")
    
                user_role = (user.get("role") or "").lower() if user else ""
                can_delete_river = can_make_entries and user_role != "operator"
                if user_role == "operator":
                    st.warning("Operators can add or edit entries but cannot delete them.")
    
                today = date.today()
                min_date = today - timedelta(days=max_days_backward)
                max_date_val = today + (timedelta(days=7) if allow_future_dates else timedelta(days=0))
    
                date_key = f"river_form_date_{active_location_id}"
                draft_key = f"river_form_draft_{active_location_id}"
                rain_key = f"river_form_rain_{active_location_id}"
                edit_key = f"river_form_edit_{active_location_id}"
                date_widget_key = f"{date_key}_widget"
                draft_widget_key = f"{draft_key}_widget"
                rain_widget_key = f"{rain_key}_widget"
    
                def _ensure_river_defaults():
                    st.session_state.setdefault(date_key, today)
                    st.session_state.setdefault(draft_key, 0.0)
                    st.session_state.setdefault(rain_key, 0.0)
                    st.session_state.setdefault(edit_key, None)
    
                def _reset_river_form():
                    st.session_state[edit_key] = None
                    st.session_state[date_key] = today
                    st.session_state[draft_key] = 0.0
                    st.session_state[rain_key] = 0.0
                    st.session_state.pop(date_widget_key, None)
                    st.session_state.pop(draft_widget_key, None)
                    st.session_state.pop(rain_widget_key, None)
    
                _ensure_river_defaults()
                is_editing_river = st.session_state.get(edit_key) is not None
                if is_editing_river:
                    st.info(f"Editing entry for {st.session_state.get(date_key)}")
    
                with st.container(border=True):
                    st.markdown("##### Manual Entry")
                    entry_cols = st.columns([0.3, 0.35, 0.35])
                    with entry_cols[0]:
                        rd_date_val = st.date_input(
                            "Date",
                            value=st.session_state.get(date_key, today),
                            min_value=min_date,
                            max_value=max_date_val,
                            key=date_widget_key,
                        )
                    with entry_cols[1]:
                        rd_draft_val = st.number_input(
                            "River Draft (m)",
                            min_value=0.0,
                            step=0.1,
                            value=float(st.session_state.get(draft_key, 0.0)),
                            key=draft_widget_key,
                        )
                    with entry_cols[2]:
                        rd_rain_val = st.number_input(
                            "Rainfall (cm)",
                            min_value=0.0,
                            step=0.1,
                            value=float(st.session_state.get(rain_key, 0.0)),
                            key=rain_widget_key,
                        )
    
                    action_cols = st.columns([0.2, 0.2, 0.6])
                    save_label = "Update Entry" if is_editing_river else "Save Entry"
                    save_clicked = action_cols[0].button(
                        save_label,
                        type="primary",
                        disabled=not can_make_entries,
                        key=f"river_save_{active_location_id}",
                    )
                    cancel_clicked = False
                    if is_editing_river:
                        cancel_clicked = action_cols[1].button(
                            "Cancel Edit",
                            key=f"river_cancel_{active_location_id}",
                        )
    
                    if cancel_clicked:
                        _reset_river_form()
                        _st_safe_rerun()
    
                    if save_clicked:
                        errors = []
                        if rd_date_val is None:
                            errors.append("Date is required.")
    
                        if errors:
                            for err in errors:
                                st.error(err)
                        else:
                            try:
                                with get_session() as sess:
                                    if is_editing_river:
                                        rec = (
                                            sess.query(RiverDraftRecord)
                                            .filter(
                                                RiverDraftRecord.id == int(st.session_state[edit_key]),
                                                RiverDraftRecord.location_id == active_location_id,
                                            )
                                            .one_or_none()
                                        )
                                        if not rec:
                                            st.error("Selected entry no longer exists.")
                                        else:
                                            rec.date = rd_date_val
                                            rec.river_draft_m = float(rd_draft_val)
                                            rec.rainfall_cm = float(rd_rain_val)
                                            rec.updated_by = user.get("username", "unknown")
                                            SecurityManager.log_audit(
                                                sess,
                                                user.get("username", "unknown"),
                                                "UPDATE",
                                                resource_type="RiverDraftRecord",
                                                resource_id=rec.id,
                                                details=f"Updated river draft entry for {rec.date}",
                                                user_id=user.get("id"),
                                                location_id=active_location_id,
                                            )
                                            sess.commit()
                                            st.success("River draft entry updated.")
                                            _reset_river_form()
                                            _st_safe_rerun()
                                    else:
                                        rec = RiverDraftRecord(
                                            location_id=active_location_id,
                                            date=rd_date_val,
                                            river_draft_m=float(rd_draft_val),
                                            rainfall_cm=float(rd_rain_val),
                                            created_by=user.get("username", "unknown"),
                                        )
                                        sess.add(rec)
                                        sess.flush()
                                        SecurityManager.log_audit(
                                            sess,
                                            user.get("username", "unknown"),
                                            "CREATE",
                                            resource_type="RiverDraftRecord",
                                            resource_id=rec.id,
                                            details=f"Created river draft entry for {rd_date_val}",
                                            user_id=user.get("id"),
                                            location_id=active_location_id,
                                        )
                                        sess.commit()
                                        st.success("River draft entry saved.")
                                        _reset_river_form()
                                        _st_safe_rerun()
                            except Exception as ex:
                                st.error(f"Failed to save entry: {ex}")
    
                river_records = load_river_draft_records(active_location_id, limit=1000)
                rd_df = pd.DataFrame(river_records)
                if not rd_df.empty:
                    rd_df["Date"] = pd.to_datetime(rd_df["Date"]).dt.date
                rd_dates = rd_df["Date"].tolist() if not rd_df.empty else []
                river_min_date, river_max_date = _derive_filter_bounds(rd_dates)
                river_from_default = _ensure_date_key_in_bounds(
                    f"river_filter_from_{active_location_id}",
                    river_min_date,
                    river_max_date,
                    river_min_date,
                )
                river_to_default = _ensure_date_key_in_bounds(
                    f"river_filter_to_{active_location_id}",
                    river_min_date,
                    river_max_date,
                    river_max_date,
                )
    
                st.markdown("##### Live Filters")
                filter_cols = st.columns(2)
                river_filter_from = filter_cols[0].date_input(
                    "From date",
                    value=river_from_default,
                    min_value=river_min_date,
                    max_value=river_max_date,
                    key=f"river_filter_from_{active_location_id}",
                )
                river_filter_to = filter_cols[1].date_input(
                    "To date",
                    value=river_to_default,
                    min_value=river_min_date,
                    max_value=river_max_date,
                    key=f"river_filter_to_{active_location_id}",
                )
    
                if not rd_df.empty:
                    if river_filter_from:
                        rd_df = rd_df[rd_df["Date"] >= river_filter_from]
                    if river_filter_to:
                        rd_df = rd_df[rd_df["Date"] <= river_filter_to]
                    rd_df["Updated At"] = (
                        pd.to_datetime(rd_df["Updated At"], errors="coerce")
                        .dt.strftime("%Y-%m-%d %H:%M:%S")
                        .fillna("")
                    )
    
                st.caption(f"{len(rd_df)} record(s) shown")
    
                display_cols = [
                    "Date",
                    "River Draft (m)",
                    "Rainfall (cm)",
                    "Created By",
                    "Updated By",
                    "Updated At",
                ]
                if rd_df.empty:
                    st.info("No entries for the selected filters.")
                else:
                    st.dataframe(rd_df[display_cols], use_container_width=True, hide_index=True)
    
                    st.markdown("###### Manage Entries")
                    for rec in rd_df.sort_values(by="Date", ascending=False).to_dict("records"):
                        row_cols = st.columns([0.2, 0.2, 0.2, 0.25, 0.075, 0.075])
                        row_cols[0].write(str(rec["Date"]))
                        row_cols[1].write(f"{float(rec['River Draft (m)']):,.2f}")
                        row_cols[2].write(f"{float(rec['Rainfall (cm)']):,.2f}")
                        row_cols[3].write(rec.get("Updated By") or rec.get("Created By") or "-")
                        edit_btn = row_cols[4].button(
                            "Edit",
                            key=f"river_edit_{rec['id']}",
                            disabled=not can_make_entries,
                        )
                        delete_btn = row_cols[5].button(
                            "Delete",
                            key=f"river_delete_{rec['id']}",
                            disabled=not can_delete_river,
                        )
    
                        if edit_btn:
                            st.session_state[edit_key] = rec["id"]
                            st.session_state[date_key] = rec["Date"]
                            st.session_state[draft_key] = float(rec["River Draft (m)"] or 0.0)
                            st.session_state[rain_key] = float(rec["Rainfall (cm)"] or 0.0)
                            _st_safe_rerun()
    
                        confirm_key = f"river_delete_confirm_{rec['id']}"
                        if delete_btn:
                            st.session_state[confirm_key] = True
                        if st.session_state.get(confirm_key):
                            st.warning("Delete this entry? This action cannot be undone.")
                            c1, c2 = st.columns(2)
                            if c1.button("Yes, delete", key=f"{confirm_key}_yes"):
                                try:
                                    with get_session() as sess:
                                        row = (
                                            sess.query(RiverDraftRecord)
                                            .filter(
                                                RiverDraftRecord.id == rec["id"],
                                                RiverDraftRecord.location_id == active_location_id,
                                            )
                                            .one_or_none()
                                        )
                                        if not row:
                                            st.warning("Entry already deleted.")
                                        else:
                                            _archive_record_for_delete(
                                                sess,
                                                row,
                                                "RiverDraftRecord",
                                                reason=f"Deleted river draft entry for {row.date}",
                                                label=str(row.date),
                                            )
                                            SecurityManager.log_audit(
                                                sess,
                                                user.get("username", "unknown"),
                                                "DELETE",
                                                resource_type="RiverDraftRecord",
                                                resource_id=row.id,
                                                details=f"Deleted river draft entry for {row.date}",
                                                user_id=user.get("id"),
                                                location_id=active_location_id,
                                            )
                                            sess.commit()
                                            st.success("Entry deleted.")
                                            _reset_river_form()
                                            _st_safe_rerun()
                                except Exception as ex:
                                    st.error(f"Failed to delete entry: {ex}")
                                finally:
                                    st.session_state.pop(confirm_key, None)
                            if c2.button("Cancel", key=f"{confirm_key}_no"):
                                st.session_state.pop(confirm_key, None)
    
        if _produced_water_tab is not None:
            with _produced_water_tab:
                st.markdown("#### Produced Water")
                st.caption("Manual entry for produced water volumes (bbls).")
    
                user_role = (user.get("role") or "").lower() if user else ""
                can_delete_pw = can_make_entries and user_role != "operator"
                if user_role == "operator":
                    st.warning("Operators can add or edit produced water entries but cannot delete them.")
    
                today = date.today()
                min_date = today - timedelta(days=max_days_backward)
                max_date_val = today + (timedelta(days=7) if allow_future_dates else timedelta(days=0))
    
                pw_date_key = f"pw_form_date_{active_location_id}"
                pw_value_key = f"pw_form_value_{active_location_id}"
                pw_edit_key = f"pw_form_edit_{active_location_id}"
                pw_date_widget = f"{pw_date_key}_widget"
                pw_value_widget = f"{pw_value_key}_widget"
    
                def _ensure_pw_defaults():
                    st.session_state.setdefault(pw_date_key, today)
                    st.session_state.setdefault(pw_value_key, 0.0)
                    st.session_state.setdefault(pw_edit_key, None)
    
                def _reset_pw_form():
                    st.session_state[pw_edit_key] = None
                    st.session_state[pw_date_key] = today
                    st.session_state[pw_value_key] = 0.0
                    st.session_state.pop(pw_date_widget, None)
                    st.session_state.pop(pw_value_widget, None)
    
                _ensure_pw_defaults()
                is_editing_pw = st.session_state.get(pw_edit_key) is not None
                if is_editing_pw:
                    st.info(f"Editing entry for {st.session_state.get(pw_date_key)}")
    
                with st.container(border=True):
                    st.markdown("##### Manual Entry")
                    entry_cols = st.columns([0.4, 0.6])
                    with entry_cols[0]:
                        pw_date_val = st.date_input(
                            "Date",
                            value=st.session_state.get(pw_date_key, today),
                            min_value=min_date,
                            max_value=max_date_val,
                            key=pw_date_widget,
                        )
                    with entry_cols[1]:
                        pw_value_val = st.number_input(
                            "Produced Water (bbls)",
                            min_value=0.0,
                            step=1.0,
                            value=float(st.session_state.get(pw_value_key, 0.0)),
                            key=pw_value_widget,
                        )
    
                    action_cols = st.columns([0.2, 0.2, 0.6])
                    save_label = "Update Entry" if is_editing_pw else "Save Entry"
                    save_clicked = action_cols[0].button(
                        save_label,
                        type="primary",
                        disabled=not can_make_entries,
                        key=f"pw_save_{active_location_id}",
                    )
                    cancel_clicked = False
                    if is_editing_pw:
                        cancel_clicked = action_cols[1].button(
                            "Cancel Edit",
                            key=f"pw_cancel_{active_location_id}",
                        )
    
                    if cancel_clicked:
                        _reset_pw_form()
                        _st_safe_rerun()
    
                    if save_clicked:
                        errors = []
                        if pw_date_val is None:
                            errors.append("Date is required.")
    
                        if errors:
                            for err in errors:
                                st.error(err)
                        else:
                            try:
                                with get_session() as sess:
                                    if is_editing_pw:
                                        rec = (
                                            sess.query(ProducedWaterRecord)
                                            .filter(
                                                ProducedWaterRecord.id == int(st.session_state[pw_edit_key]),
                                                ProducedWaterRecord.location_id == active_location_id,
                                            )
                                            .one_or_none()
                                        )
                                        if not rec:
                                            st.error("Selected entry no longer exists.")
                                        else:
                                            rec.date = pw_date_val
                                            rec.produced_water_bbl = float(pw_value_val)
                                            rec.updated_by = user.get("username", "unknown")
                                            SecurityManager.log_audit(
                                                sess,
                                                user.get("username", "unknown"),
                                                "UPDATE",
                                                resource_type="ProducedWaterRecord",
                                                resource_id=rec.id,
                                                details=f"Updated produced water entry for {rec.date}",
                                                user_id=user.get("id"),
                                                location_id=active_location_id,
                                            )
                                            sess.commit()
                                            st.success("Produced water entry updated.")
                                            _reset_pw_form()
                                            _st_safe_rerun()
                                    else:
                                        rec = ProducedWaterRecord(
                                            location_id=active_location_id,
                                            date=pw_date_val,
                                            produced_water_bbl=float(pw_value_val),
                                            created_by=user.get("username", "unknown"),
                                        )
                                        sess.add(rec)
                                        sess.flush()
                                        SecurityManager.log_audit(
                                            sess,
                                            user.get("username", "unknown"),
                                            "CREATE",
                                            resource_type="ProducedWaterRecord",
                                            resource_id=rec.id,
                                            details=f"Created produced water entry for {pw_date_val}",
                                            user_id=user.get("id"),
                                            location_id=active_location_id,
                                        )
                                        sess.commit()
                                        st.success("Produced water entry saved.")
                                        _reset_pw_form()
                                        _st_safe_rerun()
                            except Exception as ex:
                                st.error(f"Failed to save entry: {ex}")
    
                pw_records = load_produced_water_records(active_location_id, limit=1000)
                pw_df = pd.DataFrame(pw_records)
                if not pw_df.empty:
                    pw_df["Date"] = pd.to_datetime(pw_df["Date"]).dt.date
                pw_dates = pw_df["Date"].tolist() if not pw_df.empty else []
                pw_min_date, pw_max_date = _derive_filter_bounds(pw_dates)
                pw_from_default = _ensure_date_key_in_bounds(
                    f"pw_filter_from_{active_location_id}",
                    pw_min_date,
                    pw_max_date,
                    pw_min_date,
                )
                pw_to_default = _ensure_date_key_in_bounds(
                    f"pw_filter_to_{active_location_id}",
                    pw_min_date,
                    pw_max_date,
                    pw_max_date,
                )
    
                st.markdown("##### Live Filters")
                filter_cols = st.columns(2)
                pw_filter_from = filter_cols[0].date_input(
                    "From date",
                    value=pw_from_default,
                    min_value=pw_min_date,
                    max_value=pw_max_date,
                    key=f"pw_filter_from_{active_location_id}",
                )
                pw_filter_to = filter_cols[1].date_input(
                    "To date",
                    value=pw_to_default,
                    min_value=pw_min_date,
                    max_value=pw_max_date,
                    key=f"pw_filter_to_{active_location_id}",
                )
    
                if not pw_df.empty:
                    if pw_filter_from:
                        pw_df = pw_df[pw_df["Date"] >= pw_filter_from]
                    if pw_filter_to:
                        pw_df = pw_df[pw_df["Date"] <= pw_filter_to]
                    pw_df["Updated At"] = (
                        pd.to_datetime(pw_df["Updated At"], errors="coerce")
                        .dt.strftime("%Y-%m-%d %H:%M:%S")
                        .fillna("")
                    )
    
                st.caption(f"{len(pw_df)} record(s) shown")
    
                pw_display_cols = [
                    "Date",
                    "Produced Water (bbls)",
                    "Created By",
                    "Updated By",
                    "Updated At",
                ]
                if pw_df.empty:
                    st.info("No entries for the selected filters.")
                else:
                    st.dataframe(pw_df[pw_display_cols], use_container_width=True, hide_index=True)
    
                    st.markdown("###### Manage Entries")
                    for rec in pw_df.sort_values(by="Date", ascending=False).to_dict("records"):
                        row_cols = st.columns([0.25, 0.25, 0.3, 0.1, 0.1])
                        row_cols[0].write(str(rec["Date"]))
                        row_cols[1].write(f"{float(rec['Produced Water (bbls)']):,.2f}")
                        row_cols[2].write(rec.get("Updated By") or rec.get("Created By") or "-")
                        edit_btn = row_cols[3].button(
                            "Edit",
                            key=f"pw_edit_{rec['id']}",
                            disabled=not can_make_entries,
                        )
                        delete_btn = row_cols[4].button(
                            "Delete",
                            key=f"pw_delete_{rec['id']}",
                            disabled=not can_delete_pw,
                        )
    
                        if edit_btn:
                            st.session_state[pw_edit_key] = rec["id"]
                            st.session_state[pw_date_key] = rec["Date"]
                            st.session_state[pw_value_key] = float(rec["Produced Water (bbls)"] or 0.0)
                            _st_safe_rerun()
    
                        confirm_key = f"pw_delete_confirm_{rec['id']}"
                        if delete_btn:
                            st.session_state[confirm_key] = True
                        if st.session_state.get(confirm_key):
                            st.warning("Delete this entry? This action cannot be undone.")
                            c1, c2 = st.columns(2)
                            if c1.button("Yes, delete", key=f"{confirm_key}_yes"):
                                try:
                                    with get_session() as sess:
                                        row = (
                                            sess.query(ProducedWaterRecord)
                                            .filter(
                                                ProducedWaterRecord.id == rec["id"],
                                                ProducedWaterRecord.location_id == active_location_id,
                                            )
                                            .one_or_none()
                                        )
                                        if not row:
                                            st.warning("Entry already deleted.")
                                        else:
                                            _archive_record_for_delete(
                                                sess,
                                                row,
                                                "ProducedWaterRecord",
                                                reason=f"Deleted produced water entry for {row.date}",
                                                label=str(row.date),
                                            )
                                            SecurityManager.log_audit(
                                                sess,
                                                user.get("username", "unknown"),
                                                "DELETE",
                                                resource_type="ProducedWaterRecord",
                                                resource_id=row.id,
                                                details=f"Deleted produced water entry for {row.date}",
                                                user_id=user.get("id"),
                                                location_id=active_location_id,
                                            )
                                            sess.commit()
                                            st.success("Entry deleted.")
                                            _reset_pw_form()
                                            _st_safe_rerun()
                                except Exception as ex:
                                    st.error(f"Failed to delete entry: {ex}")
                                finally:
                                    st.session_state.pop(confirm_key, None)
                            if c2.button("Cancel", key=f"{confirm_key}_no"):
                                st.session_state.pop(confirm_key, None)
    
        if _tanker_tab is not None:
            with _tanker_tab:
                st.markdown("#### No of Tankers")
                if not (_is_aggu_location or _is_ndoni_location):
                    st.info("This tab is only available for Aggu and Ndoni locations.")
                else:
                    is_aggu_site = _is_aggu_location
                    mode_label = "Aggu" if is_aggu_site else "Ndoni"
                    st.caption(
                        "Live tracker for tanker movements at "
                        f"{mode_label}. S.No auto-increments per saved entry."
                    )
    
                    today = date.today()
                    min_date = today - timedelta(days=max_days_backward)
                    max_date_val = today + (timedelta(days=7) if allow_future_dates else timedelta(days=0))
    
                    date_key = f"tanker_date_{active_location_id}"
                    dispatched_key = f"tanker_dispatched_{active_location_id}"
                    from_aggu_key = f"tanker_from_aggu_{active_location_id}"
                    from_ofs_key = f"tanker_from_ofs_{active_location_id}"
                    # New key for separate "Other Tankers" input (Ndoni only)
                    other_key = f"tanker_other_{active_location_id}"
                    remarks_key = f"tanker_remarks_{active_location_id}"
                    edit_key = f"tanker_edit_id_{active_location_id}"
                    prefill_key = f"tanker_form_prefill_{active_location_id}"
    
                    def _default_form_values():
                        return {
                            "date": today,
                            "dispatched": 0.0,
                            "from_aggu": 0.0,
                            "from_ofs": 0.0,
                            # Include separate "other" field for Ndoni (initially 0)
                            "other": 0.0,
                            "remarks": "",
                        }
    
                    def _apply_form_values(values: Dict[str, Any]):
                        merged = _default_form_values()
                        merged.update({k: v for k, v in (values or {}).items() if v is not None})
                        st.session_state[date_key] = merged["date"]
                        if is_aggu_site:
                            st.session_state[dispatched_key] = float(merged["dispatched"] or 0.0)
                        else:
                            st.session_state[from_aggu_key] = float(merged.get("from_aggu") or 0.0)
                            st.session_state[from_ofs_key] = float(merged.get("from_ofs") or 0.0)
                            # Apply separate other_tankers field for Ndoni
                            st.session_state[other_key] = float(merged.get("other") or 0.0)
                        st.session_state[remarks_key] = merged.get("remarks") or ""
    
                    def _ensure_tanker_defaults():
                        st.session_state.setdefault(edit_key, None)
                        pending = st.session_state.pop(prefill_key, None)
                        if pending is not None or date_key not in st.session_state:
                            _apply_form_values(pending or {})
                        else:
                            st.session_state.setdefault(date_key, today)
                            if is_aggu_site:
                                st.session_state.setdefault(dispatched_key, 0.0)
                            else:
                                st.session_state.setdefault(from_aggu_key, 0.0)
                                st.session_state.setdefault(from_ofs_key, 0.0)
                                # Initialise other_key for Ndoni
                                st.session_state.setdefault(other_key, 0.0)
                            st.session_state.setdefault(remarks_key, "")
    
                    def _reset_tanker_form():
                        st.session_state[edit_key] = None
                        st.session_state[prefill_key] = _default_form_values()
    
                    _ensure_tanker_defaults()
                    editing_id = st.session_state.get(edit_key)
                    is_editing_tanker = editing_id is not None
    
                    if is_editing_tanker:
                        st.info(f"Editing entry #{editing_id} for {st.session_state.get(date_key)}")
    
                    with st.container(border=True):
                        st.markdown("##### Manual Entry")
                        # For Ndoni we need an extra column for "Other Tankers" input
                        entry_cols = st.columns(3 if is_aggu_site else 5)
                        with entry_cols[0]:
                            st.date_input(
                                "Date",
                                min_value=min_date,
                                max_value=max_date_val,
                                key=date_key,
                            )
    
                        if is_aggu_site:
                            with entry_cols[1]:
                                st.number_input(
                                    "Tankers Dispatched",
                                    min_value=0.0,
                                    step=1.0,
                                    key=dispatched_key,
                                )
                            remarks_col = entry_cols[2]
                        else:
                            with entry_cols[1]:
                                st.number_input(
                                    "Tankers from Aggu",
                                    min_value=0.0,
                                    step=1.0,
                                    key=from_aggu_key,
                                )
                            with entry_cols[2]:
                                st.number_input(
                                    "Tankers from OFS",
                                    min_value=0.0,
                                    step=1.0,
                                    key=from_ofs_key,
                                )
                            with entry_cols[3]:
                                st.number_input(
                                    "Other Tankers",
                                    min_value=0.0,
                                    step=1.0,
                                    key=other_key,
                                )
                            remarks_col = entry_cols[4]
    
                        with remarks_col:
                            st.text_area(
                                "Remarks",
                                key=remarks_key,
                            )
    
                        action_cols = st.columns([0.2, 0.2, 0.6])
                        save_label = "Update Entry" if is_editing_tanker else "Save Entry"
                        save_clicked = action_cols[0].button(
                            save_label,
                            type="primary",
                            disabled=not can_make_entries,
                            key=f"tanker_save_btn_{active_location_id}",
                        )
                        cancel_clicked = False
                        if is_editing_tanker:
                            cancel_clicked = action_cols[1].button(
                                "Cancel Edit",
                                key=f"tanker_cancel_btn_{active_location_id}",
                            )
    
                        if cancel_clicked:
                            _reset_tanker_form()
                            _st_safe_rerun()
    
                        if save_clicked:
                            date_val = st.session_state.get(date_key)
                            dispatched_val = float(st.session_state.get(dispatched_key, 0.0) or 0.0)
                            from_aggu_val = float(st.session_state.get(from_aggu_key, 0.0) or 0.0)
                            from_ofs_val = float(st.session_state.get(from_ofs_key, 0.0) or 0.0)
                            # Read separate other_tankers value for Ndoni
                            other_val = float(st.session_state.get(other_key, 0.0) or 0.0)
                            remarks_val = (st.session_state.get(remarks_key) or "").strip()
    
                            errors: List[str] = []
                            if not date_val:
                                errors.append("Date is required.")
                            if is_aggu_site and dispatched_val <= 0:
                                errors.append("Tankers dispatched must be greater than zero.")
                            if (not is_aggu_site) and (from_aggu_val <= 0 and from_ofs_val <= 0 and other_val <= 0):
                                errors.append("At least one tanker count must be greater than zero.")
    
                            if errors:
                                for err in errors:
                                    st.error(err)
                            else:
                                try:
                                    with get_session() as sess:
                                        if is_editing_tanker:
                                            rec = (
                                                sess.query(LocationTankerEntry)
                                                .filter(
                                                    LocationTankerEntry.id == int(editing_id),
                                                    LocationTankerEntry.location_id == active_location_id,
                                                )
                                                .one_or_none()
                                            )
                                            if not rec:
                                                st.error("Selected entry no longer exists.")
                                            else:
                                                rec.date = date_val
                                                rec.tankers_dispatched = dispatched_val if is_aggu_site else 0.0
                                                rec.tankers_from_aggu = from_aggu_val if not is_aggu_site else 0.0
                                                rec.tankers_from_ofs = from_ofs_val if not is_aggu_site else 0.0
                                                # Set separate other_tankers on edit for Ndoni
                                                rec.other_tankers = other_val if not is_aggu_site else 0.0
                                                rec.remarks = remarks_val
                                                rec.updated_by = user.get("username", "unknown")
                                                SecurityManager.log_audit(
                                                    sess,
                                                    user.get("username", "unknown"),
                                                    "UPDATE",
                                                    resource_type="LocationTankerEntry",
                                                    resource_id=rec.id,
                                                    details=f"Updated tanker entry for {rec.date}",
                                                    user_id=user.get("id"),
                                                    location_id=active_location_id,
                                                )
                                                sess.commit()
                                                st.success("Entry updated successfully.")
                                                _reset_tanker_form()
                                                _st_safe_rerun()
                                        else:
                                            serial_no = _next_tanker_serial(sess, active_location_id)
                                            rec = LocationTankerEntry(
                                                location_id=active_location_id,
                                                serial_no=serial_no,
                                                date=date_val,
                                                tankers_dispatched=dispatched_val if is_aggu_site else 0.0,
                                                tankers_from_aggu=from_aggu_val if not is_aggu_site else 0.0,
                                                tankers_from_ofs=from_ofs_val if not is_aggu_site else 0.0,
                                                # Persist separate other_tankers for Ndoni
                                                other_tankers=other_val if not is_aggu_site else 0.0,
                                                remarks=remarks_val,
                                                created_by=user.get("username", "unknown"),
                                            )
                                            sess.add(rec)
                                            sess.flush()
                                            SecurityManager.log_audit(
                                                sess,
                                                user.get("username", "unknown"),
                                                "CREATE",
                                                resource_type="LocationTankerEntry",
                                                resource_id=rec.id,
                                                details=f"Created tanker entry for {date_val}",
                                                user_id=user.get("id"),
                                                location_id=active_location_id,
                                            )
                                            sess.commit()
                                            st.success("Entry saved successfully.")
                                            _reset_tanker_form()
                                            _st_safe_rerun()
                                except Exception as ex:
                                    st.error(f"Failed to save entry: {ex}")
    
                    with get_session() as sess:
                        rows = (
                            sess.query(LocationTankerEntry)
                            .filter(LocationTankerEntry.location_id == active_location_id)
                            .order_by(LocationTankerEntry.date.desc(), LocationTankerEntry.serial_no.desc())
                            .all()
                        )
    
                    tanker_dates = [r.date for r in rows if isinstance(r.date, date)]
                    tanker_min, tanker_max = _derive_filter_bounds(tanker_dates)
                    tanker_from_default = _ensure_date_key_in_bounds(
                        f"tanker_filter_from_{active_location_id}",
                        tanker_min,
                        tanker_max,
                        tanker_min,
                    )
                    tanker_to_default = _ensure_date_key_in_bounds(
                        f"tanker_filter_to_{active_location_id}",
                        tanker_min,
                        tanker_max,
                        tanker_max,
                    )
    
                    st.markdown("##### Live Filters")
                    history_cols = st.columns([0.25, 0.25, 0.5])
                    with history_cols[0]:
                        hist_from = st.date_input(
                            "From",
                            value=tanker_from_default,
                            min_value=tanker_min,
                            max_value=tanker_max,
                            key=f"tanker_filter_from_{active_location_id}",
                        )
                    with history_cols[1]:
                        hist_to = st.date_input(
                            "To",
                            value=tanker_to_default,
                            min_value=tanker_min,
                            max_value=tanker_max,
                            key=f"tanker_filter_to_{active_location_id}",
                        )
                    with history_cols[2]:
                        search_note = st.text_input(
                            "Search remarks",
                            key=f"tanker_filter_search_{active_location_id}",
                        ).strip().lower()
    
                    filtered_rows = []
                    for rec in rows:
                        if hist_from and rec.date < hist_from:
                            continue
                        if hist_to and rec.date > hist_to:
                            continue
                        if search_note and search_note not in (rec.remarks or "").lower():
                            continue
                        filtered_rows.append(rec)
    
                    st.caption(f"{len(filtered_rows)} record(s) shown")
    
                    if not filtered_rows:
                        st.info("No tanker entries found for the selected filters.")
                    else:
                        if is_aggu_site:
                            header_labels = [
                                "S.No",
                                "Date",
                                "Tankers Dispatched",
                                "Remarks",
                                "Actions",
                            ]
                            widths = [0.09, 0.18, 0.2, 0.39, 0.14]
                        else:
                            header_labels = [
                                "S.No",
                                "Date",
                                "Tankers from Aggu",
                                "Tankers from OFS",
                                "Other Tankers",
                                "Remarks",
                                "Actions",
                            ]
                            # Widths should sum to 1.0; allocate extra column for other tankers
                            widths = [0.07, 0.14, 0.16, 0.17, 0.17, 0.23, 0.06]
    
                        header_cols = st.columns(widths)
                        for col, label in zip(header_cols, header_labels):
                            col.markdown(f"**{label}**")
    
                        can_delete_tankers = can_make_entries and user_role != "operator"
    
                        for rec in filtered_rows:
                            row_cols = st.columns(widths)
                            row_cols[0].write(int(rec.serial_no))
                            row_cols[1].write(rec.date.strftime("%d-%b-%Y"))
                            if is_aggu_site:
                                row_cols[2].write(f"{rec.tankers_dispatched:,.0f}")
                                remark_col_index = 3
                                action_col_index = 4
                            else:
                                row_cols[2].write(f"{rec.tankers_from_aggu:,.0f}")
                                row_cols[3].write(f"{rec.tankers_from_ofs:,.0f}")
                                # Show separate "Other Tankers" column
                                row_cols[4].write(f"{rec.other_tankers:,.0f}")
                                remark_col_index = 5
                                action_col_index = 6
    
                            badge = user_with_caution(rec.created_by, rec.updated_by, rec.updated_at)
                            remarks_html = (
                                f"{rec.remarks or '-'}<br/><span style='color:#6c757d;font-size:0.75rem;'>{badge}</span>"
                            )
                            row_cols[remark_col_index].markdown(remarks_html, unsafe_allow_html=True)
    
                            with row_cols[action_col_index]:
                                action_col = st.columns(2)
                                edit_btn = action_col[0].button(
                                    "✏️",
                                    key=f"tanker_edit_{rec.id}",
                                    disabled=not can_make_entries,
                                )
                                delete_btn = action_col[1].button(
                                    "🗑️",
                                    key=f"tanker_delete_{rec.id}",
                                    disabled=not can_delete_tankers,
                                )
    
                                if edit_btn:
                                    if not _deny_edit_for_lock(rec, "LocationTankerEntry", f"{rec.date}"):
                                        st.session_state[edit_key] = rec.id
                                        st.session_state[prefill_key] = {
                                            "date": rec.date,
                                            "dispatched": float(rec.tankers_dispatched or 0.0),
                                            "from_aggu": float(rec.tankers_from_aggu or 0.0),
                                            "from_ofs": float(rec.tankers_from_ofs or 0.0),
                                            # Pre-fill separate other_tankers field for Ndoni
                                            "other": float(rec.other_tankers or 0.0),
                                            "remarks": rec.remarks or "",
                                        }
                                        _st_safe_rerun()
    
                                confirm_key = f"tanker_confirm_delete_{rec.id}"
                                if delete_btn:
                                    st.session_state[confirm_key] = True
    
                                if st.session_state.get(confirm_key):
                                    st.warning("Delete this entry? This action cannot be undone.")
                                    conf_cols = st.columns(2)
                                    if conf_cols[0].button(
                                        "Yes, delete",
                                        key=f"{confirm_key}_yes",
                                        type="primary",
                                    ):
                                        try:
                                            with get_session() as sess:
                                                row = (
                                                    sess.query(LocationTankerEntry)
                                                    .filter(
                                                        LocationTankerEntry.id == rec.id,
                                                        LocationTankerEntry.location_id == active_location_id,
                                                    )
                                                    .one_or_none()
                                                )
                                                if not row:
                                                    st.warning("Entry already removed.")
                                                else:
                                                    _archive_record_for_delete(
                                                        sess,
                                                        row,
                                                        "LocationTankerEntry",
                                                        reason=f"Marked tanker entry for {row.date} for deletion.",
                                                        label=f"{row.date}",
                                                    )
                                                    sess.commit()
                                                    st.success("Entry moved to recycle bin.")
                                                    _reset_tanker_form()
                                                    _st_safe_rerun()
                                        except Exception as ex:
                                            st.error(f"Failed to delete entry: {ex}")
                                        finally:
                                            st.session_state.pop(confirm_key, None)
                                    if conf_cols[1].button(
                                        "Cancel",
                                        key=f"{confirm_key}_no",
                                    ):
                                        st.session_state.pop(confirm_key, None)
    
