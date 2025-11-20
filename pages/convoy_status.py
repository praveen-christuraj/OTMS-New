"""
Auto-generated module for the 'Convoy Status' page.
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
        header("Convoy Status")
        
        # Check Admin-IT access restriction
        if st.session_state.get("auth_user", {}).get("role") == "admin-it":
            st.error("🚫 Access Denied: Admin-IT users do not have access to operational pages.")
            st.stop()
    
        user = st.session_state.get("auth_user")
        if not user:
            st.error("Please login to access this page.")
            st.stop()
    
        user_role = (user.get("role") or "").lower()
        active_location_id = st.session_state.get("active_location_id")
        can_delete_snapshots = user_role not in {"operator"}
        if not active_location_id and user_role != "admin-operations":
            st.error("No active location selected.")
            st.stop()
    
        from location_manager import LocationManager
        from models import (
            Location,
            YadeBarge,
            YadeVoyage,
            TOAYadeSummary,
            TOAYadeStage,
            OTRVessel,
            LocationVessel,
            Vessel,
            FSOOperation,
            ConvoyStatusYade,
            ConvoyStatusVessel,
        )
    
        def _norm_txt(value: Optional[str]) -> str:
            return (value or "").strip()
    
        def _norm_loc_name(value: Optional[str]) -> str:
            return _norm_txt(value).lower()
    
        # Load location context and allowed options
        with get_session() as s:
            all_locations = s.query(Location).order_by(Location.name).all()
            allowed_location_objs = [
                loc for loc in all_locations if _norm_loc_name(loc.name) in CONVOY_STATUS_ALLOWED_LOCATIONS
            ]
            active_location = (
                LocationManager.get_location_by_id(s, active_location_id) if active_location_id else None
            )
        if user_role != "admin-operations":
            if not active_location:
                st.error("Active location not found.")
                st.stop()
            if _norm_loc_name(active_location.name) not in CONVOY_STATUS_ALLOWED_LOCATIONS:
                st.error("Convoy Status is restricted to Agge, Utapate, Lagos (HO), or administrators.")
                st.stop()
    
        # Determine which location we are viewing
        target_location_id = active_location.id if active_location else None
        if user_role in ["admin-operations", "manager"]:
            loc_options = {loc.id: f"{loc.name} ({loc.code})" for loc in allowed_location_objs}
            if not loc_options:
                st.error("No eligible locations configured for Convoy Status.")
                st.stop()
            default_loc_id = st.session_state.get("convoy_status_admin_loc") or target_location_id
            if default_loc_id not in loc_options:
                default_loc_id = next(iter(loc_options.keys()))
            option_items = sorted(loc_options.items(), key=lambda item: item[1])
            target_location_id = st.selectbox(
                "Select Location",
                option_items,
                format_func=lambda item: item[1],
                index=[idx for idx, (loc_id, _) in enumerate(option_items) if loc_id == default_loc_id][0],
                key="convoy_status_admin_loc_selector",
            )[0]
            st.session_state["convoy_status_admin_loc"] = target_location_id
            with get_session() as s:
                target_location = LocationManager.get_location_by_id(s, target_location_id)
        else:
            target_location = active_location
    
        if not target_location:
            st.error("Unable to determine selected location.")
            st.stop()
    
        target_location_name = target_location.name or "Unknown"
        target_location_code = target_location.code or ""
        target_location_norm = _norm_loc_name(target_location_name)
        st.success(f"Viewing Convoy Status for **{target_location_name} ({target_location_code})**")
    
        is_agge = target_location_norm == "agge"
        is_utapate = target_location_norm == "utapate"
        show_yade_tab = not is_utapate
    
        with get_session() as s:
            assigned_rows = (
                s.query(LocationVessel)
                .filter(LocationVessel.location_id == target_location_id, LocationVessel.is_active == True)
                .all()
            )
            assigned_vessel_ids = [row.vessel_id for row in assigned_rows]
            if assigned_vessel_ids:
                _assigned_vessels = (
                    s.query(Vessel)
                    .filter(Vessel.id.in_(assigned_vessel_ids))
                    .order_by(Vessel.name)
                    .all()
                )
                active_vessel_names = [v.name for v in _assigned_vessels]
            else:
                active_vessel_names = []
    
        # Determine if Asemoku Jetty data should be shown
        asemoku_location_id = next(
            (loc.id for loc in all_locations if "asemoku" in _norm_loc_name(loc.name)), None
        )
        include_asemoku = (
            asemoku_location_id is not None and target_location_norm == "agge"
        )
        if include_asemoku:
            st.caption("YADE data from Asemoku Jetty is also available for the dropdown selections.")
    
        source_location_ids = [target_location_id]
        if include_asemoku:
            source_location_ids.append(asemoku_location_id)
    
        location_by_norm = {_norm_loc_name(loc.name): loc for loc in all_locations}
        location_by_code_norm = {
            _norm_loc_name(loc.code): loc for loc in all_locations if getattr(loc, "code", None)
        }
    
        def _extend_loc_mapping(store: Dict[str, Location]) -> None:
            additions = {}
            for key, loc in list(store.items()):
                sanitized = key.replace(" ", "").replace("-", "")
                if sanitized and sanitized not in store:
                    additions[sanitized] = loc
            store.update(additions)
    
        _extend_loc_mapping(location_by_norm)
        _extend_loc_mapping(location_by_code_norm)
        special_vessel_locations: Dict[str, Dict[str, Any]] = {}
        for vessel_name, loc_norms in CONVOY_STATUS_SPECIAL_VESSEL_LOCATIONS.items():
            loc_obj = None
            for candidate in loc_norms:
                candidate_norm = candidate.strip().lower()
                alt_norm = candidate_norm.replace(" ", "").replace("-", "")
                loc_obj = (
                    location_by_norm.get(candidate_norm)
                    or location_by_norm.get(alt_norm)
                    or location_by_code_norm.get(candidate_norm)
                    or location_by_code_norm.get(alt_norm)
                )
                if loc_obj:
                    break
            if loc_obj:
                special_vessel_locations[vessel_name.upper()] = {
                    "location_id": loc_obj.id,
                    "location_code": loc_obj.code or "",
                    "vessel_name": vessel_name,
                }
    
        with get_session() as s:
            yade_barges = s.query(YadeBarge).order_by(YadeBarge.name).all()
            if active_vessel_names:
                vessel_rows = s.query(Vessel).filter(Vessel.name.in_(active_vessel_names)).all()
            else:
                vessel_rows = []
        vessel_id_map = {(_norm_txt(v.name)).upper(): v.id for v in vessel_rows}
    
        def _load_yade_dropdown_data(target_date: date, loc_ids: List[int]):
            convoy_map: Dict[str, List[str]] = defaultdict(list)
            stock_map: Dict[tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
            if not loc_ids:
                return convoy_map, stock_map
            with get_session() as s:
                location_lookup = {
                    loc.id: loc.name for loc in s.query(Location).filter(Location.id.in_(loc_ids)).all()
                }
                query = (
                    s.query(
                        YadeVoyage.yade_name,
                        YadeVoyage.convoy_no,
                        YadeVoyage.voyage_no,
                        YadeVoyage.location_id,
                        YadeVoyage.date.label("voyage_date"),
                        TOAYadeSummary.date,
                        TOAYadeSummary.time,
                        TOAYadeStage.nsv_bbl,
                    )
                    .outerjoin(TOAYadeSummary, TOAYadeSummary.voyage_id == YadeVoyage.id)
                    .outerjoin(
                        TOAYadeStage,
                        and_(TOAYadeStage.voyage_id == YadeVoyage.id, TOAYadeStage.stage == "after"),
                    )
                    .filter(YadeVoyage.location_id.in_(loc_ids))
                    .order_by(
                        TOAYadeSummary.date.desc().nullslast(),
                        TOAYadeSummary.time.desc().nullslast(),
                        YadeVoyage.date.desc(),
                    )
                    .limit(1000)
                )
                rows = query.all()
    
            for (
                yade_name,
                convoy_no,
                voyage_no,
                loc_id,
                voyage_date,
                sum_date,
                sum_time,
                nsv_bbl,
            ) in rows:
                yade_name = _norm_txt(yade_name)
                convoy_no = _norm_txt(convoy_no)
                if yade_name and convoy_no:
                    if convoy_no not in convoy_map[yade_name]:
                        convoy_map[yade_name].append(convoy_no)
                if (
                    yade_name
                    and convoy_no
                    and nsv_bbl is not None
                    and isinstance(nsv_bbl, (int, float))
                ):
                    time_label = sum_time.strftime("%H:%M") if sum_time else ""
                    date_label = None
                    if sum_date:
                        date_label = sum_date.strftime("%d-%b-%Y")
                    elif voyage_date:
                        date_label = voyage_date.strftime("%d-%b-%Y")
                    location_label = location_lookup.get(loc_id)
                    parts = [f"{float(nsv_bbl):,.2f} bbls"]
                    if voyage_no:
                        parts.append(f"Voy {voyage_no}")
                    if time_label:
                        parts.append(f"@ {time_label}")
                    if date_label:
                        parts.append(f"on {date_label}")
                    if include_asemoku and loc_id != target_location_id and location_label:
                        parts.append(f"� {location_label}")
                    label = " ".join(parts)
                    stock_map[(yade_name, convoy_no)].append(
                        {"label": label, "value": float(nsv_bbl)}
                    )
    
            for k in convoy_map:
                convoy_map[k] = sorted(convoy_map[k])
            return convoy_map, stock_map
    
        def _load_vessel_dropdown_data(
            target_date: date, loc_id: int, special_location_map: Dict[str, Dict[str, Any]], allowed_vessel_ids: List[int]
        ):
            shuttle_map: Dict[str, List[str]] = defaultdict(list)
            stock_map: Dict[tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
            special_stock_map: Dict[str, Dict[str, Any]] = {}
            seen_shuttles: Dict[str, set] = defaultdict(set)
    
            def _fmt_time(val) -> str:
                if not val:
                    return ''
                try:
                    return val.strftime('%H:%M')
                except AttributeError:
                    return str(val)
    
            with get_session() as s:
                if not allowed_vessel_ids:
                    otr_rows = []
                else:
                    query = (
                        s.query(
                            Vessel.name,
                            OTRVessel.shuttle_no,
                            OTRVessel.closing_stock,
                            OTRVessel.time,
                            OTRVessel.date,
                        )
                        .join(Vessel, Vessel.id == OTRVessel.vessel_id)
                        .filter(
                            OTRVessel.location_id == loc_id,
                            OTRVessel.vessel_id.in_(allowed_vessel_ids),
                        )
                        .order_by(OTRVessel.date.desc(), OTRVessel.time.desc().nullslast())
                        .limit(1000)
                    )
                    otr_rows = query.all()
    
            for vessel_name, shuttle_no, closing_stock, tx_time, tx_date in otr_rows:
                vessel_name = _norm_txt(vessel_name)
                shuttle_no = _norm_txt(shuttle_no)
                if vessel_name and shuttle_no:
                    seen = seen_shuttles[vessel_name]
                    if shuttle_no not in seen:
                        seen.add(shuttle_no)
                        shuttle_map[vessel_name].append(shuttle_no)
                if vessel_name and closing_stock is not None:
                    label_parts = [f'{float(closing_stock):,.2f} bbls']
                    time_label = _fmt_time(tx_time)
                    if time_label:
                        label_parts.append(f'@ {time_label}')
                    if tx_date:
                        try:
                            label_parts.append(f"on {tx_date.strftime('%d-%b-%Y')}")
                        except AttributeError:
                            label_parts.append(f'on {tx_date}')
                    if shuttle_no:
                        label_parts.append(f' {shuttle_no}')
                    label = ' '.join(label_parts)
                    stock_map[(vessel_name, shuttle_no or '')].append(
                        {'label': label, 'value': float(closing_stock)}
                    )
    
            for vessel_name in shuttle_map:
                shuttle_map[vessel_name] = sorted(shuttle_map[vessel_name])
    
            for vessel_key, loc_info in special_location_map.items():
                display_value, numeric_value = _convoy_fetch_mb_closing_value(
                    target_date, loc_info.get("location_code")
                )
                if numeric_value is None:
                    numeric_value = _convoy_fallback_closing_from_ops(
                        target_date, loc_info.get("location_id"), loc_info.get("vessel_name")
                    )
                    if numeric_value is not None and not display_value:
                        display_value = f"{numeric_value:,.2f}"
                special_stock_map[vessel_key] = {
                    "label": display_value,
                    "value": numeric_value,
                }
    
            return shuttle_map, stock_map, special_stock_map
    
    
    
        def _load_saved_yade_records(loc_id: int, target_date: date):
            with get_session() as s:
                rows = (
                    s.query(ConvoyStatusYade)
                    .filter(
                        ConvoyStatusYade.location_id == loc_id,
                        ConvoyStatusYade.date == target_date,
                    )
                    .all()
                )
            return {row.yade_barge_id: row for row in rows}
    
        def _load_saved_vessel_records(loc_id: int, target_date: date):
            with get_session() as s:
                rows = (
                    s.query(ConvoyStatusVessel)
                    .filter(
                        ConvoyStatusVessel.location_id == loc_id,
                        ConvoyStatusVessel.date == target_date,
                    )
                    .all()
                )
            return {(row.vessel_name or "").upper(): row for row in rows}
    
        def _load_saved_dates(model_cls):
            with get_session() as s:
                results = (
                    s.query(model_cls.date)
                    .filter(model_cls.location_id == target_location_id)
                    .distinct()
                    .order_by(model_cls.date.desc())
                    .all()
                )
            return [row[0] for row in results]
    
        def _open_pdf(pdf_bytes: bytes, title: str):
            base64_pdf = base64.b64encode(pdf_bytes).decode("utf-8")
            pdf_html = f"""
            <script>
                var pdfWindow = window.open("");
                pdfWindow.document.write(
                    '<html><head><title>{title}</title></head>' +
                    '<body style="margin:0"><iframe width="100%" height="100%" src="data:application/pdf;base64,{base64_pdf}"></iframe></body></html>'
                );
            </script>
            """
            components.html(pdf_html, height=0)
    
        def _generate_pdf(title: str, subtitle: str, headers: List[str], rows: List[List[str]]):
            ok, err, _ = ensure_reportlab()
            if not ok:
                raise RuntimeError(err or "reportlab unavailable")
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
            from reportlab.lib.styles import getSampleStyleSheet
    
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4)
            styles = getSampleStyleSheet()
            elems = [
                Paragraph(f"<b>{title}</b>", styles["Title"]),
                Paragraph(subtitle, styles["Normal"]),
                Spacer(1, 12),
            ]
            table = Table([headers] + rows, repeatRows=1)
            table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                        ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ]
                )
            )
            elems.append(table)
            doc.build(elems)
            pdf_data = buffer.getvalue()
            buffer.close()
            return pdf_data
    
        def _delete_convoy_snapshot(kind: str, entry_date: date):
            model = ConvoyStatusYade if kind == "yade" else ConvoyStatusVessel
            resource_type = "ConvoyStatusYade" if kind == "yade" else "ConvoyStatusVessel"
            with get_session() as s:
                s.query(model).filter(
                    model.location_id == target_location_id,
                    model.date == entry_date,
                ).delete(synchronize_session=False)
                s.commit()
            SecurityManager.log_audit(
                None,
                user["username"],
                "DELETE",
                resource_type=resource_type,
                resource_id=f"{target_location_id}:{entry_date.isoformat()}",
                location_id=target_location_id,
                details=f"Deleted {kind.upper()} convoy snapshot for {entry_date.isoformat()}",
                user_id=user.get("id"),
            )
    
        def _request_convoy_delete(kind: str, entry_date: date):
            resource_type = "ConvoyStatusYade" if kind == "yade" else "ConvoyStatusVessel"
            resource_id = f"{target_location_id}:{entry_date.isoformat()}"
            label = f"{kind.upper()} snapshot {entry_date.strftime('%d-%b-%Y')}"
            TaskManager.create_delete_request(
                resource_type=resource_type,
                resource_id=resource_id,
                resource_label=label,
                raised_by=user.get("username", "operator"),
                raised_by_role=user_role,
                location_id=target_location_id,
                metadata={"date": entry_date.isoformat(), "kind": kind},
            )
    
        tab_specs: List[Tuple[str, str]] = []
        if show_yade_tab:
            tab_specs.append(("Yade", "yade"))
        tab_specs.append(("Vessel", "vessel"))
        tab_specs.append(("Saved Entries", "saved"))
        tabs = st.tabs([label for label, _ in tab_specs])
        tab_indices = {kind: idx for idx, (_, kind) in enumerate(tab_specs)}
    
        if show_yade_tab:
            # ---------------------- YADE TAB ----------------------
            with tabs[tab_indices['yade']]:
                if 'convoy_status_yade_date' not in st.session_state:
                    st.session_state['convoy_status_yade_date'] = date.today()
                yade_date = st.date_input('Select Date', key='convoy_status_yade_date')
                convoy_map, yade_stock_map = _load_yade_dropdown_data(yade_date, source_location_ids)
                saved_yade_records = _load_saved_yade_records(target_location_id, yade_date)
    
                st.markdown('#### Yade Convoy Tracker')
                header_cols = st.columns([2.2, 2, 2, 2])
                header_cols[0].markdown('**YADE No**')
                header_cols[1].markdown('**Convoy**')
                header_cols[2].markdown('**Stock (After Loading NSV)**')
                header_cols[3].markdown('**Status**')
    
                for yade in yade_barges:
                    row_cols = st.columns([2.2, 2, 2, 2])
                    row_cols[0].markdown(f"**{yade.name}**")
                    yade_name_norm = _norm_txt(yade.name)
                    saved_record = saved_yade_records.get(yade.id)
    
                    convoy_options = ['N/A'] + convoy_map.get(yade_name_norm, [])
                    if saved_record and saved_record.convoy_no and saved_record.convoy_no not in convoy_options:
                        convoy_options.append(saved_record.convoy_no)
                    convoy_key = f"convoy_status_yade_{yade_date.isoformat()}_{yade.id}_convoy"
                    if (
                        convoy_key not in st.session_state
                        or st.session_state[convoy_key] not in convoy_options
                    ):
                        st.session_state[convoy_key] = (
                            saved_record.convoy_no if saved_record and saved_record.convoy_no else 'N/A'
                        )
                    selected_convoy = row_cols[1].selectbox(
                        'Convoy',
                        options=convoy_options,
                        key=convoy_key,
                        label_visibility='collapsed',
                    )
    
                    stock_candidates = yade_stock_map.get((yade_name_norm, selected_convoy), [])
                    stock_labels = ['N/A'] + [opt['label'] for opt in stock_candidates]
                    if saved_record and saved_record.stock_display and saved_record.stock_display not in stock_labels:
                        stock_labels.append(saved_record.stock_display)
                    stock_key = f"convoy_status_yade_{yade_date.isoformat()}_{yade.id}_stock"
                    current_stock = st.session_state.get(stock_key)
                    if current_stock not in stock_labels:
                        st.session_state[stock_key] = (
                            saved_record.stock_display if saved_record and saved_record.stock_display else 'N/A'
                        )
                    selected_stock = row_cols[2].selectbox(
                        'Stock',
                        options=stock_labels,
                        key=stock_key,
                        label_visibility='collapsed',
                    )
    
                    status_options = ['Select status'] + CONVOY_STATUS_YADE_STATUS_OPTIONS
                    if saved_record and saved_record.status and saved_record.status not in status_options:
                        status_options.append(saved_record.status)
                    status_key = f"convoy_status_yade_{yade_date.isoformat()}_{yade.id}_status"
                    current_status = st.session_state.get(status_key)
                    if current_status not in status_options:
                        st.session_state[status_key] = saved_record.status if saved_record else 'Select status'
                    row_cols[3].selectbox(
                        'Status',
                        options=status_options,
                        key=status_key,
                        label_visibility='collapsed',
                    )
    
                save_key = f"convoy_status_yade_save_{target_location_id}"
                if st.button(' Save YADE Status', key=save_key, use_container_width=True):
                    try:
                        with get_session() as s:
                            existing_rows = (
                                s.query(ConvoyStatusYade)
                                .filter(
                                    ConvoyStatusYade.location_id == target_location_id,
                                    ConvoyStatusYade.date == yade_date,
                                )
                                .all()
                            )
                            existing = {row.yade_barge_id: row for row in existing_rows}
                            changes = 0
                            for yade in yade_barges:
                                yade_name_norm = _norm_txt(yade.name)
                                convoy_key = f"convoy_status_yade_{yade_date.isoformat()}_{yade.id}_convoy"
                                stock_key = f"convoy_status_yade_{yade_date.isoformat()}_{yade.id}_stock"
                                status_key = f"convoy_status_yade_{yade_date.isoformat()}_{yade.id}_status"
                                selected_convoy = st.session_state.get(convoy_key, 'N/A')
                                selected_stock = st.session_state.get(stock_key, 'N/A')
                                selected_status = st.session_state.get(status_key, 'Select status')
                                if selected_status == 'Select status':
                                    continue
                                selected_stock_value = None
                                for opt in yade_stock_map.get((yade_name_norm, selected_convoy), []):
                                    if opt['label'] == selected_stock:
                                        selected_stock_value = opt['value']
                                        break
                                record = existing.get(yade.id)
                                if not record:
                                    record = ConvoyStatusYade(
                                        location_id=target_location_id,
                                        date=yade_date,
                                        yade_barge_id=yade.id,
                                        created_by=user.get('username', 'unknown'),
                                    )
                                    s.add(record)
                                record.convoy_no = None if selected_convoy == 'N/A' else selected_convoy
                                record.stock_display = None if selected_stock == 'N/A' else selected_stock
                                record.stock_value_bbl = selected_stock_value
                                record.status = selected_status
                                record.updated_by = user.get('username')
                                changes += 1
                            if changes:
                                s.commit()
                            else:
                                s.rollback()
                        if changes:
                            SecurityManager.log_audit(
                                None,
                                user['username'],
                                'UPDATE',
                                resource_type='ConvoyStatusYade',
                                resource_id=f"{target_location_id}:{yade_date.isoformat()}",
                                location_id=target_location_id,
                                details=f"Saved {changes} YADE convoy rows for {yade_date}",
                                user_id=user.get('id'),
                            )
                        st.success('YADE convoy status saved.')
                        import time as _t
                        _t.sleep(1)
                        _st_safe_rerun()
                    except Exception as ex:
                        st.error(f'Failed to save YADE status: {ex}')
    
        # ---------------------- VESSEL TAB ----------------------
        with tabs[tab_indices['vessel']]:
            if 'convoy_status_vessel_date' not in st.session_state:
                st.session_state['convoy_status_vessel_date'] = date.today()
            vessel_date = st.date_input('Select Date', key='convoy_status_vessel_date')
            shuttle_map, vessel_stock_map, special_stock_map = _load_vessel_dropdown_data(
                vessel_date, target_location_id, special_vessel_locations, assigned_vessel_ids
            )
            saved_vessel_records = _load_saved_vessel_records(target_location_id, vessel_date)
    
            st.markdown('#### Vessel Convoy Tracker')
            header_cols = st.columns([2.2, 2, 2, 2])
            header_cols[0].markdown('**Vessel Name**')
            header_cols[1].markdown('**Shuttle No**')
            header_cols[2].markdown('**Stock (Closing)**')
            header_cols[3].markdown('**Status**')
    
            special_display_snapshot: Dict[str, Dict[str, Any]] = {}
    
            for vessel_name in active_vessel_names:
                row_cols = st.columns([2.2, 2, 2, 2])
                row_cols[0].markdown(f"**{vessel_name}**")
                vessel_key = vessel_name.upper()
                saved_record = saved_vessel_records.get(vessel_key)
                is_special = vessel_name in CONVOY_STATUS_SPECIAL_VESSELS
    
                if is_special:
                    row_cols[1].markdown('_N/A_')
                    stock_info = special_stock_map.get(vessel_key, {})
                    display_label = stock_info.get('label')
                    numeric_val = stock_info.get('value')
                    if display_label:
                        row_cols[2].markdown(f"**{display_label}**")
                    elif numeric_val is not None:
                        display_label = f"{numeric_val:,.2f}"
                        row_cols[2].markdown(f"**{display_label}**")
                    else:
                        display_label = None
                        row_cols[2].markdown('_No closing stock for selected date_')
                    special_display_snapshot[vessel_key] = {
                        'label': display_label,
                        'value': numeric_val,
                    }
                else:
                    shuttle_options = ['N/A'] + shuttle_map.get(vessel_name, [])
                    if (
                        saved_record
                        and saved_record.shuttle_no
                        and saved_record.shuttle_no not in shuttle_options
                    ):
                        shuttle_options.append(saved_record.shuttle_no)
                    shuttle_key = f"convoy_status_vessel_{vessel_date.isoformat()}_{vessel_key}_shuttle"
                    if (
                        shuttle_key not in st.session_state
                        or st.session_state[shuttle_key] not in shuttle_options
                    ):
                        st.session_state[shuttle_key] = (
                            saved_record.shuttle_no if saved_record and saved_record.shuttle_no else 'N/A'
                        )
                    selected_shuttle = row_cols[1].selectbox(
                        'Shuttle',
                        options=shuttle_options,
                        key=shuttle_key,
                        label_visibility='collapsed',
                    )
    
                    stock_candidates = vessel_stock_map.get((vessel_name, selected_shuttle or ''), [])
                    stock_labels = ['N/A'] + [opt['label'] for opt in stock_candidates]
                    if (
                        saved_record
                        and saved_record.stock_display
                        and saved_record.stock_display not in stock_labels
                    ):
                        stock_labels.append(saved_record.stock_display)
                    stock_key = f"convoy_status_vessel_{vessel_date.isoformat()}_{vessel_key}_stock"
                    if stock_key not in st.session_state or st.session_state[stock_key] not in stock_labels:
                        st.session_state[stock_key] = (
                            saved_record.stock_display if saved_record and saved_record.stock_display else 'N/A'
                        )
                    row_cols[2].selectbox(
                        'Stock',
                        options=stock_labels,
                        key=stock_key,
                        label_visibility='collapsed',
                    )
    
                status_options = ['Select status'] + CONVOY_STATUS_VESSEL_STATUS_OPTIONS
                if saved_record and saved_record.status and saved_record.status not in status_options:
                    status_options.append(saved_record.status)
                status_key = f"convoy_status_vessel_{vessel_date.isoformat()}_{vessel_key}_status"
                if status_key not in st.session_state or st.session_state[status_key] not in status_options:
                    st.session_state[status_key] = saved_record.status if saved_record else 'Select status'
                row_cols[3].selectbox(
                    'Status',
                    options=status_options,
                    key=status_key,
                    label_visibility='collapsed',
                )
    
            vessel_save_key = f"convoy_status_vessel_save_{target_location_id}"
            if st.button(' Save Vessel Status', key=vessel_save_key, use_container_width=True):
                try:
                    with get_session() as s:
                        existing_rows = (
                            s.query(ConvoyStatusVessel)
                            .filter(
                                ConvoyStatusVessel.location_id == target_location_id,
                                ConvoyStatusVessel.date == vessel_date,
                            )
                            .all()
                        )
                        existing = {(row.vessel_name or '').upper(): row for row in existing_rows}
                        changes = 0
                        for vessel_name in active_vessel_names:
                            vessel_key = vessel_name.upper()
                            status_key = f"convoy_status_vessel_{vessel_date.isoformat()}_{vessel_key}_status"
                            selected_status = st.session_state.get(status_key, 'Select status')
                            if selected_status == 'Select status':
                                continue
                            if vessel_name in CONVOY_STATUS_SPECIAL_VESSELS:
                                stock_data = special_display_snapshot.get(vessel_key, {})
                                selected_shuttle = 'N/A'
                                stock_value = stock_data.get('value') if stock_data else None
                                display_label = stock_data.get('label') if stock_data else None
                                if not display_label and stock_value is not None:
                                    display_label = f"{stock_value:,.2f}"
                                selected_stock = display_label or 'N/A'
                            else:
                                shuttle_key = f"convoy_status_vessel_{vessel_date.isoformat()}_{vessel_key}_shuttle"
                                stock_key = f"convoy_status_vessel_{vessel_date.isoformat()}_{vessel_key}_stock"
                                selected_shuttle = st.session_state.get(shuttle_key, 'N/A')
                                selected_stock = st.session_state.get(stock_key, 'N/A')
                                stock_value = None
                                record = existing.get(vessel_key)
                                stock_candidates = vessel_stock_map.get((vessel_name, selected_shuttle or ''), [])
                                for opt in stock_candidates:
                                    if opt['label'] == selected_stock:
                                        stock_value = opt['value']
                                        break
                                if stock_value is None and record and record.stock_display == selected_stock:
                                    stock_value = record.stock_value_bbl
                            record = existing.get(vessel_key)
                            if not record:
                                record = ConvoyStatusVessel(
                                    location_id=target_location_id,
                                    date=vessel_date,
                                    vessel_name=vessel_name,
                                    vessel_id=vessel_id_map.get(vessel_key),
                                    created_by=user.get('username', 'unknown'),
                                )
                                s.add(record)
                            record.shuttle_no = None if selected_shuttle == 'N/A' else selected_shuttle
                            record.stock_display = None if selected_stock == 'N/A' else selected_stock
                            record.stock_value_bbl = stock_value
                            record.status = selected_status
                            record.updated_by = user.get('username')
                            changes += 1
                        if changes:
                            s.commit()
                        else:
                            s.rollback()
                    if changes:
                        SecurityManager.log_audit(
                            None,
                            user['username'],
                            'UPDATE',
                            resource_type='ConvoyStatusVessel',
                            resource_id=f"{target_location_id}:{vessel_date.isoformat()}",
                            location_id=target_location_id,
                            details=f"Saved {changes} vessel convoy rows for {vessel_date}",
                            user_id=user.get('id'),
                        )
                    st.success('Vessel convoy status saved.')
                    import time as _t
                    _t.sleep(1)
                    _st_safe_rerun()
                except Exception as ex:
                    st.error(f'Failed to save vessel status: {ex}')
    
        # ---------------------- SAVED ENTRIES TAB ----------------------
        with tabs[tab_indices["saved"]]:
            st.markdown("#### Saved Entries")
            yade_saved_dates = _load_saved_dates(ConvoyStatusYade)
            vessel_saved_dates = _load_saved_dates(ConvoyStatusVessel)
    
            yade_col, vessel_col = st.columns(2)
    
            with yade_col:
                st.markdown("**YADE Entries**")
                if not yade_saved_dates:
                    st.info("No YADE entries saved yet.")
                else:
                    for idx, entry_date in enumerate(yade_saved_dates, start=1):
                        row_cols = st.columns([0.3, 1, 0.35, 0.35])
                        row_cols[0].markdown(f"**{idx}**")
                        row_cols[1].markdown(entry_date.strftime("%d-%b-%Y"))
                        view_key = f"convoy_status_view_yade_{entry_date}"
                        delete_key = f"convoy_status_delete_yade_{entry_date}"
                        confirm_key = f"convoy_status_confirm_delete_yade_{entry_date}"
    
                        if row_cols[2].button(
                            "👁️",
                            key=view_key,
                            use_container_width=True,
                            help="View PDF",
                        ):
                            with get_session() as s:
                                rows = (
                                    s.query(ConvoyStatusYade, YadeBarge.name)
                                    .join(YadeBarge, ConvoyStatusYade.yade_barge_id == YadeBarge.id)
                                    .filter(
                                        ConvoyStatusYade.location_id == target_location_id,
                                        ConvoyStatusYade.date == entry_date,
                                    )
                                    .order_by(YadeBarge.name)
                                    .all()
                                )
                            pdf_rows = [
                                [
                                    name,
                                    rec.convoy_no or "N/A",
                                    rec.stock_display or "-",
                                    rec.status,
                                ]
                                for rec, name in rows
                            ]
                            if not pdf_rows:
                                st.warning("No rows saved for that date.")
                            else:
                                pdf_bytes = _generate_pdf(
                                    "YADE Convoy Status",
                                    f"{target_location_name} � {entry_date.strftime('%d-%b-%Y')}",
                                    ["YADE", "Convoy", "Stock", "Status"],
                                    pdf_rows,
                                )
                                _open_pdf(pdf_bytes, f"YADE Status - {target_location_name}")
                                st.success("YADE PDF opened in a new tab.")
    
                        if row_cols[3].button(
                            "🗑️",
                            key=delete_key,
                            use_container_width=True,
                            help="Delete entry",
                        ):
                            st.session_state[confirm_key] = True
    
                        if st.session_state.get(confirm_key):
                            prompt = (
                                f"Confirm deletion of YADE snapshot for {entry_date.strftime('%d-%b-%Y')}?"
                                if can_delete_snapshots
                                else f"Request supervisor approval to delete YADE snapshot for {entry_date.strftime('%d-%b-%Y')}?"
                            )
                            st.warning(prompt)
                            confirm_cols = st.columns(2)
                            if confirm_cols[0].button(
                                "? Confirm",
                                key=f"{confirm_key}_yes",
                                use_container_width=True,
                            ):
                                try:
                                    if can_delete_snapshots:
                                        _delete_convoy_snapshot("yade", entry_date)
                                        st.success("YADE snapshot deleted.")
                                    else:
                                        _request_convoy_delete("yade", entry_date)
                                        st.success("Delete request sent to supervisor.")
                                    st.session_state.pop(confirm_key, None)
                                    import time as _t
    
                                    _t.sleep(1)
                                    _st_safe_rerun()
                                except Exception as _ex:
                                    st.error(f"Delete failed: {_ex}")
                                    st.session_state.pop(confirm_key, None)
                            if confirm_cols[1].button(
                                "Cancel",
                                key=f"{confirm_key}_no",
                                use_container_width=True,
                            ):
                                st.session_state.pop(confirm_key, None)
    
            with vessel_col:
                st.markdown("**Vessel Entries**")
                if not vessel_saved_dates:
                    st.info("No Vessel entries saved yet.")
                else:
                    for idx, entry_date in enumerate(vessel_saved_dates, start=1):
                        row_cols = st.columns([0.3, 1, 0.35, 0.35])
                        row_cols[0].markdown(f"**{idx}**")
                        row_cols[1].markdown(entry_date.strftime("%d-%b-%Y"))
                        view_key = f"convoy_status_view_vessel_{entry_date}"
                        delete_key = f"convoy_status_delete_vessel_{entry_date}"
                        confirm_key = f"convoy_status_confirm_delete_vessel_{entry_date}"
    
                        if row_cols[2].button(
                            "👁️",
                            key=view_key,
                            use_container_width=True,
                            help="View PDF",
                        ):
                            with get_session() as s:
                                rows = (
                                    s.query(ConvoyStatusVessel)
                                    .filter(
                                        ConvoyStatusVessel.location_id == target_location_id,
                                        ConvoyStatusVessel.date == entry_date,
                                    )
                                    .order_by(ConvoyStatusVessel.vessel_name)
                                    .all()
                                )
                            pdf_rows = [
                                [
                                    rec.vessel_name,
                                    rec.shuttle_no or "N/A",
                                    rec.stock_display or "-",
                                    rec.status,
                                ]
                                for rec in rows
                            ]
                            if not pdf_rows:
                                st.warning("No rows saved for that date.")
                            else:
                                pdf_bytes = _generate_pdf(
                                    "Vessel Convoy Status",
                                    f"{target_location_name} � {entry_date.strftime('%d-%b-%Y')}",
                                    ["Vessel", "Shuttle", "Stock", "Status"],
                                    pdf_rows,
                                )
                                _open_pdf(pdf_bytes, f"Vessel Status - {target_location_name}")
                                st.success("Vessel PDF opened in a new tab.")
    
                        if row_cols[3].button(
                            "🗑️",
                            key=delete_key,
                            use_container_width=True,
                            help="Delete entry",
                        ):
                            st.session_state[confirm_key] = True
    
                        if st.session_state.get(confirm_key):
                            prompt = (
                                f"Confirm deletion of Vessel snapshot for {entry_date.strftime('%d-%b-%Y')}?"
                                if can_delete_snapshots
                                else f"Request supervisor approval to delete Vessel snapshot for {entry_date.strftime('%d-%b-%Y')}?"
                            )
                            st.warning(prompt)
                            confirm_cols = st.columns(2)
                            if confirm_cols[0].button(
                                "? Confirm",
                                key=f"{confirm_key}_yes",
                                use_container_width=True,
                            ):
                                try:
                                    if can_delete_snapshots:
                                        _delete_convoy_snapshot("vessel", entry_date)
                                        st.success("Vessel snapshot deleted.")
                                    else:
                                        _request_convoy_delete("vessel", entry_date)
                                        st.success("Delete request sent to supervisor.")
                                    st.session_state.pop(confirm_key, None)
                                    import time as _t
    
                                    _t.sleep(1)
                                    _st_safe_rerun()
                                except Exception as _ex:
                                    st.error(f"Delete failed: {_ex}")
                                    st.session_state.pop(confirm_key, None)
                            if confirm_cols[1].button(
                                "Cancel",
                                key=f"{confirm_key}_no",
                                use_container_width=True,
                            ):
                                st.session_state.pop(confirm_key, None)
    
    # ========================= REPORTING PAGE =========================