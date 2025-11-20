"""
Auto-generated module for the 'Yade-Vessel Mapping' page.
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
        header("Yade-Vessel Mapping")
        
        # Check Admin-IT access restriction
        if st.session_state.get("auth_user", {}).get("role") == "admin-it":
            st.error("🚫 Access Denied: Admin-IT users do not have access to operational pages.")
            st.stop()
    
        active_location_id = st.session_state.get("active_location_id")
        if not active_location_id:
            st.error("⚠️ No active location selected. Please pick Agge or Lagos (HO) to continue.")
            st.stop()
    
        user = st.session_state.get("auth_user") or {}
        if user:
            from auth import AuthManager
    
            if not AuthManager.can_access_location(user, active_location_id):
                st.error("🚫 You do not have access to this location.")
                st.stop()
        else:
            st.error("User session expired. Please sign in again.")
            st.stop()
    
        from location_manager import LocationManager
        from models import OTRVessel, FSOOperation
    
        def _combine_datetime(date_val, time_val):
            """Return a datetime that can be used for sorting."""
            if not isinstance(date_val, date):
                return datetime.min
            if isinstance(time_val, datetime):
                return time_val
            if isinstance(time_val, time):
                return datetime.combine(date_val, time_val)
            if isinstance(time_val, str):
                try:
                    parts = [int(p) for p in time_val.split(":")]
                except Exception:
                    parts = []
                hour = parts[0] if len(parts) > 0 else 0
                minute = parts[1] if len(parts) > 1 else 0
                second = parts[2] if len(parts) > 2 else 0
                try:
                    return datetime.combine(date_val, time(hour, minute, second))
                except Exception:
                    return datetime.combine(date_val, time.min)
            return datetime.combine(date_val, time.min)
    
        def _fetch_yade_rows(sess, location_id, limit=500):
            voyages = (
                sess.query(YadeVoyage)
                .filter(YadeVoyage.location_id == location_id)
                .order_by(YadeVoyage.date.desc(), YadeVoyage.time.desc())
                .limit(limit)
                .all()
            )
            if not voyages:
                return []
            voyage_ids = [v.id for v in voyages]
            stage_rows = sess.query(TOAYadeStage).filter(TOAYadeStage.voyage_id.in_(voyage_ids)).all()
            stage_map: Dict[int, Dict[str, TOAYadeStage]] = {}
            for stage in stage_rows:
                key = (stage.stage or "").strip().lower()
                stage_map.setdefault(stage.voyage_id, {})[key] = stage
    
            rows = []
            for voyage in voyages:
                per_stage = stage_map.get(voyage.id, {})
                rob = float(getattr(per_stage.get("before"), "nsv_bbl", 0.0) or 0.0)
                tob = float(getattr(per_stage.get("after"), "nsv_bbl", 0.0) or 0.0)
                rows.append(
                    {
                        "id": voyage.id,
                        "Date": voyage.date,
                        "Convoy No": voyage.convoy_no or "",
                        "Yade No": voyage.yade_name or "",
                        "TOB Qty": round(tob, 2),
                        "ROB Qty": round(rob, 2),
                        "Net Offloaded": round(tob - rob, 2),
                        "SortKey": _combine_datetime(voyage.date, voyage.time),
                    }
                )
            return rows
    
        def _fetch_vessel_rows(sess, location_id, limit=500):
            entries = (
                sess.query(OTRVessel)
                .options(joinedload(OTRVessel.vessel))
                .filter(OTRVessel.location_id == location_id)
                .order_by(OTRVessel.date.desc(), OTRVessel.id.desc())
                .limit(limit)
                .all()
            )
            rows = []
            for entry in entries:
                vessel_name = ""
                try:
                    vessel_name = (entry.vessel.name if entry.vessel else "") or ""
                except Exception:
                    vessel_name = ""
                rows.append(
                    {
                        "id": entry.id,
                        "Date": entry.date,
                        "Shuttle No": entry.shuttle_no or "",
                        "Vessel Name": vessel_name,
                        "Net Receipt/Dispatch": round(float(entry.net_receipt_dispatch or 0.0), 2),
                        "SortKey": _combine_datetime(entry.date, entry.time or "00:00"),
                    }
                )
            return rows
    
        def _fetch_fso_rows(sess, location_id, limit=500):
            entries = (
                sess.query(FSOOperation)
                .filter(FSOOperation.location_id == location_id)
                .order_by(FSOOperation.date.desc(), FSOOperation.time.desc())
                .limit(limit)
                .all()
            )
            rows = []
            for entry in entries:
                rows.append(
                    {
                        "id": entry.id,
                        "Date": entry.date,
                        "Shuttle No": entry.shuttle_no or "",
                        "Vessel Name": entry.vessel_name or "",
                        "Qty Received": round(float(entry.net_receipt_dispatch or 0.0), 2),
                        "SortKey": _combine_datetime(entry.date, entry.time),
                    }
                )
            return rows
    
        with get_session() as sess:
            location = LocationManager.get_location_by_id(sess, active_location_id)
            if not location:
                st.error("Location not found.")
                st.stop()
    
            location_name = location.name or "Unknown"
            st.info(f"📍 **Active Location:** {location_name} ({location.code})")
    
            allowed_locations = {"agge", "lagos (ho)"}
            user_role = (user.get("role") or "").lower()
            if user_role != "admin-operations" and (location_name or "").strip().lower() not in allowed_locations:
                st.error("⚠️ Yade-Vessel Mapping is only available for Agge, Lagos (HO), or administrators.")
                st.stop()
    
            yade_rows = _fetch_yade_rows(sess, active_location_id)
            vessel_rows = _fetch_vessel_rows(sess, active_location_id)
            fso_rows = _fetch_fso_rows(sess, active_location_id)
        can_delete_mapping_rows = user_role in {"admin", "supervisor"}
    
        yade_lookup = {row["id"]: row for row in yade_rows}
        vessel_lookup = {row["id"]: row for row in vessel_rows}
        fso_lookup = {row["id"]: row for row in fso_rows}
    
        yade_df = pd.DataFrame(yade_rows)
        vessel_df = pd.DataFrame(vessel_rows)
        fso_df = pd.DataFrame(fso_rows)
    
        for df in (yade_df, vessel_df, fso_df):
            if not df.empty and "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
    
        if "yvm_records" not in st.session_state:
            st.session_state["yvm_records"] = []
        if "yvm_pending_payload" not in st.session_state:
            st.session_state["yvm_pending_payload"] = None
        if "yvm_selected_yade" not in st.session_state:
            st.session_state["yvm_selected_yade"] = set()
        if "yvm_selected_vessel" not in st.session_state:
            st.session_state["yvm_selected_vessel"] = set()
        if "yvm_selected_fso" not in st.session_state:
            st.session_state["yvm_selected_fso"] = set()
        if "yvm_revision" not in st.session_state:
            st.session_state["yvm_revision"] = 0
        if "yvm_delete_target" not in st.session_state:
            st.session_state["yvm_delete_target"] = None
    
        def _filter_table(df: pd.DataFrame, start_value, end_value, text_value: str, text_columns: list[str]):
            if df.empty:
                return df
            filtered = df.copy()
            if start_value:
                filtered = filtered[filtered["Date"] >= pd.to_datetime(start_value)]
            if end_value:
                filtered = filtered[filtered["Date"] <= pd.to_datetime(end_value)]
            if text_value:
                needle = text_value.strip().lower()
                if needle:
                    mask = pd.Series(False, index=filtered.index)
                    for col in text_columns:
                        if col in filtered.columns:
                            mask = mask | filtered[col].astype(str).str.lower().str.contains(needle, na=False)
                    filtered = filtered[mask]
            return filtered
    
        def _render_selection_table(
            df: pd.DataFrame,
            label: str,
            session_key: str,
            table_key: str,
            column_config: dict,
            *,
            height: int = 320,
        ):
            st.markdown(f"##### {label}")
            if df.empty:
                st.info(f"No {label.lower()} available for this location.")
                st.session_state[session_key] = set()
                return set()
            display_df = df.sort_values("SortKey", ascending=False).drop(columns=["SortKey"], errors="ignore").copy()
            selected_ids = st.session_state.get(session_key, set())
            display_df.insert(0, "Select", display_df["id"].apply(lambda rid: rid in selected_ids))
            display_df = display_df.set_index("id")
            widget_key = f"{table_key}_{st.session_state.get('yvm_revision', 0)}"
            edited = st.data_editor(
                display_df,
                hide_index=True,
                use_container_width=True,
                key=widget_key,
                column_config=column_config,
                disabled=[col for col in display_df.columns if col != "Select"],
                height=height,
            )
            chosen = set(int(idx) for idx in edited.index[edited["Select"]])
            st.session_state[session_key] = chosen
            return chosen
    
        def _comparison_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
            columns = [
                "S.No",
                "Date",
                "Yade Dispatch (bbls)",
                "Vessel Receipt (bbls)",
                "Difference Y vs V (bbls)",
                "FSO Receipt (bbls)",
                "Difference V vs TT (bbls)",
                "Remarks",
            ]
            if not records:
                return pd.DataFrame(columns=columns)
            sorted_rows = sorted(records, key=lambda row: (row["s_no"], row["date"]))
            payload = []
            for row in sorted_rows:
                date_value = row.get("date")
                if hasattr(date_value, "strftime"):
                    date_value = date_value.strftime("%Y-%m-%d")
                payload.append(
                    {
                        "S.No": row.get("s_no"),
                        "Date": date_value,
                        "Yade Dispatch (bbls)": row.get("yade_dispatch", 0.0),
                        "Vessel Receipt (bbls)": row.get("vessel_receipt", 0.0),
                        "Difference Y vs V (bbls)": row.get("diff_y_vs_v", 0.0),
                        "FSO Receipt (bbls)": row.get("fso_receipt", 0.0),
                        "Difference V vs TT (bbls)": row.get("diff_v_vs_tt", 0.0),
                        "Remarks": row.get("remarks", ""),
                    }
                )
            return pd.DataFrame(payload, columns=columns)
    
        def _generate_mapping_pdf(df: pd.DataFrame, location_name: str, username: str) -> bytes:
            if df.empty:
                return b""
            buffer = BytesIO()
            doc = SimpleDocTemplate(
                buffer,
                pagesize=landscape(A4),
                leftMargin=0.6 * cm,
                rightMargin=0.6 * cm,
                topMargin=0.7 * cm,
                bottomMargin=0.7 * cm,
            )
            styles = getSampleStyleSheet()
            title_style = ParagraphStyle(
                "yvmTitle",
                parent=styles["Heading1"],
                alignment=TA_CENTER,
                fontSize=16,
                textColor=colors.HexColor("#1f4788"),
                spaceAfter=6,
            )
            subtitle_style = ParagraphStyle(
                "yvmSubtitle",
                parent=styles["Normal"],
                alignment=TA_CENTER,
                fontSize=10,
                textColor=colors.HexColor("#5c5f66"),
                spaceAfter=8,
            )
            elements = [
                Paragraph("<b>Yade � Vessel Mapping Comparison</b>", title_style),
                Paragraph(
                    f"{location_name} � Generated by {username or 'user'} on {datetime.now().strftime('%d-%b-%Y %H:%M')}",
                    subtitle_style,
                ),
                Spacer(1, 6),
            ]
            header = list(df.columns)
            table_data = [header] + df.round(2).astype(str).values.tolist()
            table = Table(table_data, repeatRows=1)
            table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4788")),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#dfe3eb")),
                        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.HexColor("#f7f9fc")]),
                        ("FONTSIZE", (0, 0), (-1, -1), 9),
                    ]
                )
            )
            elements.append(table)
            doc.build(elements)
            buffer.seek(0)
            return buffer.read()
    
        yade_column_config = {
            "Select": st.column_config.CheckboxColumn("Select", help="Tick to include transaction in mapping."),
            "Date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
            "Convoy No": st.column_config.TextColumn("Convoy No"),
            "Yade No": st.column_config.TextColumn("Yade No"),
            "TOB Qty": st.column_config.NumberColumn("TOB Qty (bbls)", format="%.2f"),
            "ROB Qty": st.column_config.NumberColumn("ROB Qty (bbls)", format="%.2f"),
            "Net Offloaded": st.column_config.NumberColumn("Net Offloaded (bbls)", format="%.2f"),
        }
        vessel_column_config = {
            "Select": st.column_config.CheckboxColumn("Select", help="Include shuttle entry in mapping."),
            "Date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
            "Shuttle No": st.column_config.TextColumn("Shuttle No"),
            "Vessel Name": st.column_config.TextColumn("Vessel Name"),
            "Net Receipt/Dispatch": st.column_config.NumberColumn("Net R/D (bbls)", format="%.2f"),
        }
        fso_column_config = {
            "Select": st.column_config.CheckboxColumn("Select", help="Include FSO entry in mapping."),
            "Date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
            "Shuttle No": st.column_config.TextColumn("Shuttle No"),
            "Vessel Name": st.column_config.TextColumn("Vessel Name"),
            "Qty Received": st.column_config.NumberColumn("Qty Received (bbls)", format="%.2f"),
        }
    
        mapping_tab, comparison_tab = st.tabs(["Mapping", "Comparison"])
    
        with mapping_tab:
            map_button_area = st.container()
            st.markdown("#### Mapping")
            st.caption("Select YADE, Vessel, and FSO transactions with the live filters below, then map them into a comparison row.")
    
            if st.session_state.get("yvm_pending_payload"):
                st.success("Selection already staged. Complete it under the Comparison tab or clear it below.")
    
            default_from = date.today() - timedelta(days=30)
            default_to = date.today()
    
            st.markdown(
                """
                <style>
                .yvm-mapping [data-testid="stDataFrame"] table,
                .yvm-mapping [data-testid="stDataEditor"] table {
                    font-size:0.82rem;
                }
                .yvm-mapping .stDataFrame,
                .yvm-mapping .stDataEditor {
                    border: 1px solid #dfe3e8;
                    border-radius: 6px;
                    margin-top: 0.1rem;
                }
                .yvm-mapping .filter-box {
                    background: #f9fafc;
                    padding: 0.25rem 0.45rem;
                    border-radius: 6px;
                    border: 1px solid #edf0f5;
                    margin-bottom: 0.15rem;
                }
                .yvm-mapping .filter-box > div {
                    margin-bottom: 0.12rem;
                }
                .yvm-subheader {
                    font-size: 0.92rem;
                    font-weight: 600;
                    margin: 0 0 0.15rem 0;
                }
                </style>
                <div class="yvm-mapping">
                """,
                unsafe_allow_html=True,
            )
    
            yade_col, vessel_col, fso_col = st.columns(3)
    
            with yade_col:
                st.markdown('<p class="yvm-subheader">Yade transactions</p>', unsafe_allow_html=True)
                with st.container():
                    st.markdown('<div class="filter-box">', unsafe_allow_html=True)
                    yade_dates = st.columns(2)
                    with yade_dates[0]:
                        st.caption("From")
                        yade_from = st.date_input(
                            "From",
                            value=default_from,
                            key="yvm_yade_from",
                            label_visibility="collapsed",
                        )
                    with yade_dates[1]:
                        st.caption("To")
                        yade_to = st.date_input(
                            "To",
                            value=default_to,
                            key="yvm_yade_to",
                            label_visibility="collapsed",
                        )
                    st.caption("Search")
                    yade_search = st.text_input(
                        "Convoy / Yade No",
                        key="yvm_yade_search",
                        label_visibility="collapsed",
                        placeholder="Convoy / YADE No",
                    )
                    st.markdown("</div>", unsafe_allow_html=True)
                filtered_yade_df = _filter_table(yade_df, yade_from, yade_to, yade_search, ["Convoy No", "Yade No"])
                yade_selection = _render_selection_table(
                    filtered_yade_df,
                    "Yade transactions",
                    "yvm_selected_yade",
                    "yvm_yade",
                    yade_column_config,
                    height=280,
                )
    
            with vessel_col:
                st.markdown('<p class="yvm-subheader">Vessel transactions</p>', unsafe_allow_html=True)
                with st.container():
                    st.markdown('<div class="filter-box">', unsafe_allow_html=True)
                    vessel_dates = st.columns(2)
                    with vessel_dates[0]:
                        st.caption("From")
                        vessel_from = st.date_input(
                            "From ",
                            value=default_from,
                            key="yvm_vessel_from",
                            label_visibility="collapsed",
                        )
                    with vessel_dates[1]:
                        st.caption("To")
                        vessel_to = st.date_input(
                            "To ",
                            value=default_to,
                            key="yvm_vessel_to",
                            label_visibility="collapsed",
                        )
                    st.caption("Search")
                    vessel_search = st.text_input(
                        "Shuttle / Vessel",
                        key="yvm_vessel_search",
                        label_visibility="collapsed",
                        placeholder="Shuttle / Vessel",
                    )
                    st.markdown("</div>", unsafe_allow_html=True)
                filtered_vessel_df = _filter_table(
                    vessel_df,
                    vessel_from,
                    vessel_to,
                    vessel_search,
                    ["Shuttle No", "Vessel Name"],
                )
                vessel_selection = _render_selection_table(
                    filtered_vessel_df,
                    "Vessel transactions",
                    "yvm_selected_vessel",
                    "yvm_vessel",
                    vessel_column_config,
                    height=280,
            )
    
            with fso_col:
                st.markdown('<p class="yvm-subheader">FSO transactions</p>', unsafe_allow_html=True)
                with st.container():
                    st.markdown('<div class="filter-box">', unsafe_allow_html=True)
                    fso_dates = st.columns(2)
                    with fso_dates[0]:
                        st.caption("From")
                        fso_from = st.date_input(
                            "From  ",
                            value=default_from,
                            key="yvm_fso_from",
                            label_visibility="collapsed",
                        )
                    with fso_dates[1]:
                        st.caption("To")
                        fso_to = st.date_input(
                            "To  ",
                            value=default_to,
                            key="yvm_fso_to",
                            label_visibility="collapsed",
                        )
                    st.caption("Search")
                    fso_search = st.text_input(
                        "Shuttle / Vessel ",
                        key="yvm_fso_search",
                        label_visibility="collapsed",
                        placeholder="Shuttle / Vessel",
                    )
                    st.markdown("</div>", unsafe_allow_html=True)
                filtered_fso_df = _filter_table(fso_df, fso_from, fso_to, fso_search, ["Shuttle No", "Vessel Name"])
                fso_selection = _render_selection_table(
                    filtered_fso_df,
                    "FSO transactions",
                    "yvm_selected_fso",
                    "yvm_fso",
                    fso_column_config,
                    height=280,
                )
    
            st.markdown("</div>", unsafe_allow_html=True)
    
            total_selected = len(yade_selection) + len(vessel_selection) + len(fso_selection)
            selection_cols = st.columns(3)
            selection_cols[0].metric("Yade selected", len(yade_selection))
            selection_cols[1].metric("Vessel selected", len(vessel_selection))
            selection_cols[2].metric("FSO selected", len(fso_selection))
    
            with map_button_area:
                map_clicked = st.button(
                    "MAP selected transactions",
                    type="primary",
                    use_container_width=True,
                    disabled=total_selected == 0,
                )
            if map_clicked:
                def _sum_selection(ids: set[int], lookup: Dict[int, Dict[str, Any]], field: str) -> float:
                    total = 0.0
                    for rec_id in ids:
                        rec = lookup.get(rec_id)
                        if not rec:
                            continue
                        try:
                            total += abs(float(rec.get(field) or 0.0))
                        except Exception:
                            continue
                    return round(total, 2)
    
                payload = {
                    "yade_ids": sorted(yade_selection),
                    "vessel_ids": sorted(vessel_selection),
                    "fso_ids": sorted(fso_selection),
                    "yade_dispatch": _sum_selection(yade_selection, yade_lookup, "Net Offloaded"),
                    "vessel_receipt": _sum_selection(vessel_selection, vessel_lookup, "Net Receipt/Dispatch"),
                    "fso_receipt": _sum_selection(fso_selection, fso_lookup, "Qty Received"),
                }
                st.session_state["yvm_pending_payload"] = payload
                st.session_state["yvm_selected_yade"] = set()
                st.session_state["yvm_selected_vessel"] = set()
                st.session_state["yvm_selected_fso"] = set()
                st.session_state["yvm_revision"] = st.session_state.get("yvm_revision", 0) + 1
                st.success("Selection moved to Comparison tab. Provide the S.No, date, and remarks to complete the mapping.")
    
        with comparison_tab:
            st.markdown("#### Comparison")
            pending_payload = st.session_state.get("yvm_pending_payload")
            stored_rows: List[Dict[str, Any]] = st.session_state.get("yvm_records", [])
            comparison_df = _comparison_dataframe(stored_rows)
            next_s_no = (max((row["s_no"] for row in stored_rows), default=0) + 1) if stored_rows else 1
            delete_target = st.session_state.get("yvm_delete_target")
    
            def _remove_mapping_record(target_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
                """Remove a stored mapping row and return it if found."""
                if not target_row:
                    return None
                records = st.session_state.get("yvm_records", [])
                record_id = target_row.get("record_id")
                s_no = target_row.get("s_no")
                date_val = target_row.get("date")
    
                removed: Optional[Dict[str, Any]] = None
                remaining: List[Dict[str, Any]] = []
    
                for row in records:
                    match = False
                    if record_id and row.get("record_id") == record_id:
                        match = True
                    elif not record_id and row.get("s_no") == s_no and row.get("date") == date_val:
                        match = True
    
                    if match and removed is None:
                        removed = row
                        continue
                    remaining.append(row)
    
                st.session_state["yvm_records"] = remaining
                if removed:
                    try:
                        with get_session() as log_session:
                            SecurityManager.log_audit(
                                log_session,
                                user.get("username", "unknown"),
                                "DELETE",
                                resource_type="YadeVesselMapping",
                                resource_id=str(removed.get("record_id") or removed.get("s_no")),
                                details=f"Deleted Yade-Vessel mapping row S.No {removed.get('s_no')} dated {removed.get('date')}",
                                user_id=user.get("id"),
                                location_id=active_location_id,
                            )
                    except Exception:
                        log_warning("Failed to log Yade-Vessel mapping deletion", exc_info=True)
                return removed
    
            if delete_target:
                target_date = delete_target.get("date")
                if hasattr(target_date, "strftime"):
                    date_text = target_date.strftime("%Y-%m-%d")
                else:
                    date_text = str(target_date or "")
                st.warning(
                    f"Confirm deletion of mapping row **S.No {delete_target.get('s_no')}** dated "
                    f"{date_text}?"
                )
                confirm_cols = st.columns(2)
                if confirm_cols[0].button("Yes, delete mapping", key="yvm_confirm_delete"):
                    removed_row = _remove_mapping_record(delete_target)
                    st.session_state["yvm_delete_target"] = None
                    if removed_row:
                        st.success("Mapping row deleted.")
                    else:
                        st.info("Row already deleted.")
                    _st_safe_rerun()
                if confirm_cols[1].button("Cancel", key="yvm_cancel_delete"):
                    st.session_state["yvm_delete_target"] = None
                    _st_safe_rerun()
    
            download_cols = st.columns(4)
            csv_data = comparison_df.to_csv(index=False).encode("utf-8") if not comparison_df.empty else "".encode()
            xlsx_bytes = BytesIO()
            if not comparison_df.empty:
                with pd.ExcelWriter(xlsx_bytes, engine="xlsxwriter") as writer:
                    comparison_df.to_excel(writer, index=False, sheet_name="Mapping")
                xlsx_bytes.seek(0)
            pdf_bytes = _generate_mapping_pdf(comparison_df, location_name, user.get("username", "")) if not comparison_df.empty else b""
    
            download_cols[0].download_button(
                "📥 CSV",
                data=csv_data if csv_data else "No data",
                file_name="yade_vessel_mapping.csv",
                mime="text/csv",
                use_container_width=True,
                disabled=comparison_df.empty,
            )
            download_cols[1].download_button(
                "📥 XLSX",
                data=xlsx_bytes.getvalue() if not comparison_df.empty else b"",
                file_name="yade_vessel_mapping.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                disabled=comparison_df.empty,
            )
            download_cols[2].download_button(
                "📥 PDF",
                data=pdf_bytes or b"",
                file_name="yade_vessel_mapping.pdf",
                mime="application/pdf",
                use_container_width=True,
                disabled=comparison_df.empty,
            )
            if download_cols[3].button("👁️ View PDF", use_container_width=True, disabled=comparison_df.empty):
                if pdf_bytes:
                    encoded_pdf = base64.b64encode(pdf_bytes).decode("utf-8")
                    components.html(
                        f"""
                        <script>
                            const pdfWindow = window.open("");
                            pdfWindow.document.write('<iframe width="100%" height="100%" src="data:application/pdf;base64,{encoded_pdf}"></iframe>');
                        </script>
                        """,
                        height=0,
                    )
    
            if pending_payload:
                st.info("Mapping selection received. Provide the next available S.No and date, then save to lock it in.")
                metric_cols = st.columns(3)
                metric_cols[0].metric("Yade Dispatch (bbls)", f"{pending_payload['yade_dispatch']:,.2f}")
                delta_v = pending_payload["vessel_receipt"] - pending_payload["yade_dispatch"]
                metric_cols[1].metric(
                    "Vessel Receipt (bbls)",
                    f"{pending_payload['vessel_receipt']:,.2f}",
                    delta=f"{delta_v:,.2f} vs YADE",
                )
                delta_f = pending_payload["fso_receipt"] - pending_payload["vessel_receipt"]
                metric_cols[2].metric(
                    "FSO Receipt (bbls)",
                    f"{pending_payload['fso_receipt']:,.2f}",
                    delta=f"{delta_f:,.2f} vs Vessel",
                )
    
                with st.form("yvm_finalize_mapping"):
                    form_cols = st.columns([0.4, 0.5, 1.6])
                    with form_cols[0]:
                        st.caption("Next S.No")
                        s_no_value = st.number_input(
                            "Next available S.No",
                            min_value=1,
                            value=next_s_no,
                            step=1,
                            label_visibility="collapsed",
                        )
                    with form_cols[1]:
                        st.caption("Date")
                        mapping_date = st.date_input(
                            "Date",
                            value=date.today(),
                            label_visibility="collapsed",
                        )
                    with form_cols[2]:
                        st.caption("Remarks")
                        remarks_value = st.text_input(
                            "Remarks",
                            placeholder="Add remarks (optional)",
                            label_visibility="collapsed",
                        )
                    submitted = st.form_submit_button("Save comparison row", use_container_width=True)
    
                if st.button("Discard pending selection", type="secondary", key="yvm_discard_pending"):
                    st.session_state["yvm_pending_payload"] = None
                    _st_safe_rerun()
    
                if submitted:
                    record = {
                        "record_id": str(uuid4()),
                        "s_no": int(s_no_value),
                        "date": mapping_date,
                        "yade_dispatch": pending_payload["yade_dispatch"],
                        "vessel_receipt": pending_payload["vessel_receipt"],
                        "diff_y_vs_v": round(pending_payload["vessel_receipt"] - pending_payload["yade_dispatch"], 2),
                        "fso_receipt": pending_payload["fso_receipt"],
                        "diff_v_vs_tt": round(pending_payload["fso_receipt"] - pending_payload["vessel_receipt"], 2),
                        "remarks": (remarks_value or "").strip(),
                        "source_ids": {
                            "yade_ids": pending_payload["yade_ids"],
                            "vessel_ids": pending_payload["vessel_ids"],
                            "fso_ids": pending_payload["fso_ids"],
                        },
                    }
                    st.session_state["yvm_records"].append(record)
                    st.session_state["yvm_pending_payload"] = None
                    st.success("Comparison row saved.")
                    _st_safe_rerun()
            else:
                st.caption("No pending selection from the Mapping tab.")
    
            st.markdown("##### Saved mappings")
            if not stored_rows:
                st.info("No comparison rows captured yet.")
            else:
                st.markdown(
                    """
                    <style>
                    .yvm-table-wrapper {
                        border: 1px solid #dfe3e8;
                        border-radius: 8px;
                        margin-top: 0.2rem;
                        background: #fff;
                    }
                    .yvm-table-header {
                        background: #f5f7fb;
                        border-bottom: 1px solid #dfe3e8;
                        padding: 0.15rem 0.15rem;
                        font-weight: 600;
                        font-size: 0.82rem;
                    }
                    .yvm-table-body {
                        max-height: 340px;
                        overflow-y: auto;
                    }
                    .yvm-table-row {
                        border-bottom: 1px solid #eef1f5;
                        padding: 0.05rem 0.15rem;
                    }
                    .yvm-table-row:last-child {
                        border-bottom: none;
                    }
                    .yvm-table-row:nth-child(even) {
                        background: #fbfcff;
                    }
                    .yvm-compare .yvm-table-row div[data-testid="column"] {
                        padding: 0 0.2rem !important;
                    }
                    .yvm-compare .yvm-table-row .markdown-text-container {
                        margin: -0.05rem 0 !important;
                        line-height: 1.15;
                    }
                    .yvm-compare .yvm-table-body button {
                        padding: 0.15rem 0.35rem;
                    }
                    </style>
                    <div class="yvm-table-wrapper yvm-mapping yvm-compare">
                    """,
                    unsafe_allow_html=True,
                )
                column_widths = [0.7, 1.0, 1.2, 1.2, 1.1, 1.1, 1.1, 1.3, 0.5]
                headers = [
                    "S.No",
                    "Date",
                    "Yade Dispatch (bbls)",
                    "Vessel Receipt (bbls)",
                    "Difference Y vs V",
                    "FSO Receipt (bbls)",
                    "Difference V vs TT",
                    "Remarks",
                    "Actions",
                ]
    
                st.markdown('<div class="yvm-table-header">', unsafe_allow_html=True)
                header_cols = st.columns(column_widths)
                for col, label in zip(header_cols, headers):
                    col.markdown(f"**{label}**")
                st.markdown("</div>", unsafe_allow_html=True)
    
                st.markdown('<div class="yvm-table-body">', unsafe_allow_html=True)
                sorted_rows = sorted(stored_rows, key=lambda row: (row["s_no"], row["date"]))
                for idx, row_data in enumerate(sorted_rows):
                    st.markdown('<div class="yvm-table-row">', unsafe_allow_html=True)
                    row_cols = st.columns(column_widths)
                    row_cols[0].markdown(str(row_data["s_no"]))
                    row_cols[1].markdown(row_data["date"].strftime("%Y-%m-%d"))
                    row_cols[2].markdown(f"{row_data['yade_dispatch']:,.2f}")
                    row_cols[3].markdown(f"{row_data['vessel_receipt']:,.2f}")
                    row_cols[4].markdown(f"{row_data['diff_y_vs_v']:,.2f}")
                    row_cols[5].markdown(f"{row_data['fso_receipt']:,.2f}")
                    row_cols[6].markdown(f"{row_data['diff_v_vs_tt']:,.2f}")
                    row_cols[7].markdown(row_data.get("remarks") or "�")
                    row_id = row_data.get("record_id") or f"{row_data['s_no']}_{row_data['date']}_{idx}"
                    if can_delete_mapping_rows:
                        if row_cols[8].button("🗑️", key=f"yvm_remove_{row_id}", help="Request deletion"):
                            st.session_state["yvm_delete_target"] = row_data.copy()
                            _st_safe_rerun()
                    else:
                        row_cols[8].markdown("�")
                    st.markdown("</div>", unsafe_allow_html=True)
                st.markdown("</div></div>", unsafe_allow_html=True)
    
    # ========================= FSO-OPERATIONS PAGE =========================