"""
Auto-generated module for the 'BCCR' page.
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
        import re
        header("BCCR")
        
        # Check Admin-IT access restriction
        if st.session_state.get("auth_user", {}).get("role") == "admin-it":
            st.error("🚫 Access Denied: Admin-IT users do not have access to operational pages.")
            st.stop()
        
        try:
            _user_role = st.session_state.get("auth_user", {}).get("role")
            _loc_id = st.session_state.get("active_location_id")
            if _user_role not in ["admin-operations", "manager"] and _loc_id:
                from location_config import LocationConfig
                with get_session() as _s:
                    _cfg = LocationConfig.get_config(_s, _loc_id)
                if _cfg.get("page_access", {}).get("BCCR") is False:
                    st.error("🚫 BCCR page is disabled for this location.")
                    st.stop()
        except Exception:
            pass
    
        active_location_id = st.session_state.get("active_location_id")
        user = st.session_state.get("auth_user")
        if not active_location_id:
            st.error("⚠️ No active location selected. Please choose a location from the Home page.")
            st.stop()
        if not user:
            st.error("⚠️ User session expired. Please log in again.")
            st.stop()
    
        user_role = (user.get("role") or "").lower()
    
        def _canon(txt: str | None) -> str:
            return re.sub(r"[^A-Z0-9]", "", str(txt or "").upper())
    
        allowed_loc_tokens = {"JETTY", "ASEMOKU", "ASEMOKUJETTY", "NDONI"}
        lagos_tokens = {"LAGOSHO"}
    
        with get_session() as s:
            from location_manager import LocationManager
            from models import Location
    
            current_loc = LocationManager.get_location_by_id(s, active_location_id)
            if not current_loc:
                st.error("Location not found.")
                st.stop()
    
            current_tokens = {_canon(current_loc.code), _canon(current_loc.name)}
            is_allowed_location = bool(current_tokens & allowed_loc_tokens)
            is_lagos_viewer = bool(current_tokens & lagos_tokens)
            is_admin = user_role in ["admin-operations", "manager"]
    
            if not (is_allowed_location or is_admin or is_lagos_viewer):
                st.error("⚠️ BCCR is only available for Asemoku Jetty or Ndoni.")
                st.stop()
    
            def _eligible_location(loc_obj) -> bool:
                tokens = {_canon(loc_obj.code), _canon(loc_obj.name)}
                return bool(tokens & allowed_loc_tokens)
    
            eligible_locations = [
                loc for loc in s.query(Location).order_by(Location.name).all()
                if _eligible_location(loc)
            ]
    
        can_select_location = is_admin or is_lagos_viewer
        target_location_id = active_location_id
    
        if "bccr_target_location_id" not in st.session_state:
            st.session_state["bccr_target_location_id"] = active_location_id
    
        if can_select_location and eligible_locations:
            options = {loc.id: f"{loc.name} ({loc.code})" for loc in eligible_locations}
            default_id = st.session_state.get("bccr_target_location_id", active_location_id)
            if default_id not in options:
                default_id = eligible_locations[0].id
            target_location_id = st.selectbox(
                "📍 Select BCCR Location",
                options=sorted(options.items(), key=lambda item: item[1]),
                format_func=lambda opt: opt[1],
                index=[idx for idx, (loc_id, _) in enumerate(sorted(options.items(), key=lambda item: item[1]))
                       if loc_id == default_id][0],
                key="bccr_location_selector"
            )[0]
            st.session_state["bccr_target_location_id"] = target_location_id
        else:
            st.session_state["bccr_target_location_id"] = active_location_id
    
        if not can_select_location and not is_allowed_location:
            st.error("⚠️ BCCR is only available for Asemoku Jetty or Ndoni.")
            st.stop()
    
        with get_session() as s_target:
            from location_manager import LocationManager
            target_location = LocationManager.get_location_by_id(s_target, target_location_id)
            if not target_location:
                st.error("Target location not found.")
                st.stop()
            target_location_name = target_location.name or "Unknown"
            target_location_code = target_location.code or ""
    
        st.info(f"📍 **Viewing Location:** {target_location_name} ({target_location_code})")
    
        if "bccr_selected_yade" not in st.session_state:
            st.session_state["bccr_selected_yade"] = {}
        if "bccr_selected_otr" not in st.session_state:
            st.session_state["bccr_selected_otr"] = {}
        if "bccr_pending" not in st.session_state:
            st.session_state["bccr_pending"] = {}
        if "bccr_records" not in st.session_state:
            st.session_state["bccr_records"] = {}
    
        def _get_selection(store_key: str) -> set[int]:
            store = st.session_state.setdefault(store_key, {})
            if target_location_id not in store:
                store[target_location_id] = set()
            return store[target_location_id]
    
        def _set_selection(store_key: str, values: set[int]) -> None:
            st.session_state.setdefault(store_key, {})[target_location_id] = values
    
        def _get_pending():
            return st.session_state["bccr_pending"].get(target_location_id)
    
        def _set_pending(data):
            st.session_state["bccr_pending"][target_location_id] = data
    
        def _get_records() -> list[dict]:
            store = st.session_state["bccr_records"].setdefault(target_location_id, [])
            return store
    
        def _set_records(records: list[dict]):
            st.session_state["bccr_records"][target_location_id] = records
    
        def _generate_bccr_pdf(df: pd.DataFrame, location_name: str, location_code: str, filters_text: str) -> bytes:
            """Create a compact PDF table for the BCCR export."""
            from io import BytesIO
            from reportlab.lib import colors
            from reportlab.lib.enums import TA_CENTER, TA_LEFT
            from reportlab.lib.pagesizes import landscape, A4
            from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
            from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
            import numbers
    
            buffer = BytesIO()
            doc = SimpleDocTemplate(
                buffer,
                pagesize=landscape(A4),
                leftMargin=25,
                rightMargin=25,
                topMargin=35,
                bottomMargin=25,
            )
    
            styles = getSampleStyleSheet()
            title_style = ParagraphStyle(
                "BCCRTitle",
                parent=styles["Title"],
                alignment=TA_CENTER,
                fontSize=18,
            )
            subtitle_style = ParagraphStyle(
                "BCCRSubtitle",
                parent=styles["Heading3"],
                alignment=TA_CENTER,
                fontSize=12,
            )
            filter_style = ParagraphStyle(
                "BCCRFilters",
                parent=styles["BodyText"],
                alignment=TA_CENTER,
                fontSize=10,
            )
            table_text_style = ParagraphStyle(
                "BCCRTable",
                parent=styles["BodyText"],
                alignment=TA_LEFT,
                fontSize=9,
            )
    
            elements = [
                Paragraph("BCCR Report", title_style),
                Paragraph(f"{location_name} ({location_code})", subtitle_style),
                Paragraph(filters_text, filter_style),
                Spacer(1, 12),
            ]
    
            if df.empty:
                elements.append(Paragraph("No records available for the selected filters.", table_text_style))
            else:
                table_data = [list(df.columns)]
                for _, row in df.iterrows():
                    row_values = []
                    for col in df.columns:
                        value = row[col]
                        if value is None:
                            row_values.append("")
                        elif isinstance(value, numbers.Integral):
                            row_values.append(str(int(value)))
                        elif isinstance(value, numbers.Number):
                            row_values.append(f"{float(value):,.2f}")
                        else:
                            row_values.append(str(value))
                    table_data.append(row_values)
    
                table = Table(table_data, repeatRows=1)
                table.setStyle(
                    TableStyle(
                        [
                            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#003366")),
                            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                            ("FONTSIZE", (0, 0), (-1, 0), 10),
                            ("FONTSIZE", (0, 1), (-1, -1), 9),
                            ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
                            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.lightgrey]),
                            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ]
                    )
                )
                elements.append(table)
    
            doc.build(elements)
            return buffer.getvalue()
    
        def _load_yade_transactions(location_id: int) -> list[dict]:
            with get_session() as s_load:
                from models import TOAYadeSummary, TOAYadeStage, YadeVoyage
    
                summaries = (
                    s_load.query(TOAYadeSummary, YadeVoyage)
                    .join(YadeVoyage, TOAYadeSummary.voyage_id == YadeVoyage.id)
                    .filter(YadeVoyage.location_id == location_id)
                    .order_by(TOAYadeSummary.date.desc(), TOAYadeSummary.time.desc())
                    .limit(200)
                    .all()
                )
                if not summaries:
                    return []
    
                voyage_ids = [summary.voyage_id for summary, _ in summaries]
                stages = (
                    s_load.query(TOAYadeStage)
                    .filter(TOAYadeStage.voyage_id.in_(voyage_ids))
                    .all()
                )
    
                stage_map: dict[int, dict[str, TOAYadeStage]] = {}
                for stage in stages:
                    stage_map.setdefault(stage.voyage_id, {})[(stage.stage or "").strip().lower()] = stage
    
                rows = []
                for summary, voyage in summaries:
                    per_stage = stage_map.get(summary.voyage_id, {})
                    before = per_stage.get("before")
                    after = per_stage.get("after")
                    rob_qty = float(getattr(before, "nsv_bbl", 0.0) or 0.0)
                    rob_water = float(getattr(before, "fw_bbl", 0.0) or 0.0)
                    tob_qty = float(getattr(after, "nsv_bbl", 0.0) or 0.0)
                    tob_water = float(getattr(after, "fw_bbl", 0.0) or 0.0)
                    net_loaded = float(summary.gsv_loaded_bbl or (tob_qty - rob_qty))
                    net_water = float(tob_water - rob_water)
                    rows.append(
                        {
                            "id": summary.id,
                            "Date": summary.date,
                            "Convoy No": summary.convoy_no or "",
                            "Yade No": summary.yade_name or "",
                            "ROB Qty": round(rob_qty, 2),
                            "ROB Water": round(rob_water, 2),
                            "TOB Qty": round(tob_qty, 2),
                            "TOB Water": round(tob_water, 2),
                            "Net Loaded": round(net_loaded, 2),
                            "Net Water": round(net_water, 2),
                        }
                    )
                return rows
    
        def _load_dispatch_rows(location_id: int) -> list[dict]:
            with get_session() as s_load:
                records = (
                    s_load.query(OTRRecord)
                    .filter(OTRRecord.location_id == location_id)
                    .order_by(OTRRecord.date.asc(), OTRRecord.time.asc())
                    .all()
                )
                if not records:
                    return []
    
                data = []
                for rec in records:
                    tank_label = getattr(rec, "tank_name", None) or (rec.tank_id or "")
                    dt_val = rec.date
                    tm_val = rec.time
                    data.append(
                        {
                            "id": rec.id,
                            "Ticket ID": rec.ticket_id,
                            "Tank": tank_label,
                            "Date": rec.date,
                            "Time": tm_val,
                            "Operation": rec.operation or "",
                            "NSV (bbl)": float(rec.nsv_bbl or 0.0),
                            "Free Water (bbl)": float(rec.free_water_bbl or 0.0),
                        }
                    )
    
                if not data:
                    return []
    
                df = pd.DataFrame(data)
                df["Tank"] = df["Tank"].fillna("")
                df["DT"] = pd.to_datetime(df["Date"]) + pd.to_timedelta(
                    df["Time"].astype(str).replace("None", "00:00:00")
                )
                df.sort_values(["Tank", "DT"], inplace=True)
    
                df["Prev NSV"] = df.groupby("Tank")["NSV (bbl)"].shift(1)
                df["Prev FW"] = df.groupby("Tank")["Free Water (bbl)"].shift(1)
                df["Net Rece/Disp (bbls)"] = df["NSV (bbl)"] - df["Prev NSV"]
                df["Net Water Rece/Disp (bbls)"] = df["Free Water (bbl)"] - df["Prev FW"]
    
                df = df[df["Operation"].str.strip().str.lower() == "dispatch to barge"]
                df = df.sort_values("Date", ascending=False).head(200)
    
                df["Net Rece/Disp (bbls)"] = df["Net Rece/Disp (bbls)"].round(2)
                df["Net Water Rece/Disp (bbls)"] = df["Net Water Rece/Disp (bbls)"].round(2)
    
                rows = df[
                    [
                        "id",
                        "Date",
                        "Ticket ID",
                        "Tank",
                        "Operation",
                        "Net Rece/Disp (bbls)",
                        "Net Water Rece/Disp (bbls)",
                    ]
                ].to_dict(orient="records")
                return rows
    
        yade_rows = _load_yade_transactions(target_location_id)
        otr_rows = _load_dispatch_rows(target_location_id)
        yade_lookup = {row["id"]: row for row in yade_rows}
        otr_lookup = {row["id"]: row for row in otr_rows}
    
        tab_map, tab_report = st.tabs(["Mapping", "BCCR Report"])
    
        def _render_selectable_table(rows: list[dict], key_prefix: str, column_order: list[str], column_labels: dict[str, str]):
            if not rows:
                st.info("No records available.")
                return set()
    
            df = pd.DataFrame(rows).reset_index(drop=True)
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%d-%b-%Y")
            df = df[column_order].copy()
            selection_key = f"{key_prefix}_selected"
            selected_ids = set(_get_selection(f"bccr_{selection_key}"))
            df.insert(0, "Select", df[column_order[0]].index.map(lambda idx: rows[idx]["id"] in selected_ids))
            df.insert(1, "Item ID", [rows[idx]["id"] for idx in range(len(rows))])
    
            editor_key = f"{key_prefix}_editor_{target_location_id}"
            edited = st.data_editor(
                df,
                hide_index=True,
                use_container_width=True,
                key=editor_key,
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select"),
                    "Item ID": st.column_config.Column("ID", help="Internal reference", disabled=True),
                    **{field: st.column_config.Column(column_labels.get(field, field)) for field in column_order}
                },
                disabled=[col for col in df.columns if col not in {"Select"}],
            )
    
            if isinstance(edited, pd.DataFrame):
                new_selected = set(
                    edited.loc[edited["Select"], "Item ID"].astype(int).tolist()
                )
            else:
                edited_df = pd.DataFrame(edited)
                new_selected = set(
                    edited_df.loc[edited_df["Select"], "Item ID"].astype(int).tolist()
                )
    
            _set_selection(f"bccr_{selection_key}", new_selected)
            return new_selected
    
        with tab_map:
            st.markdown("### Mapping")
            st.caption("Select YADE and Dispatch entries to compare and map them into BCCR records.")
    
            map_col1, map_col2 = st.columns(2)
    
            with map_col1:
                st.subheader("YADE Transactions (TOA)")
                yc1, yc2 = st.columns(2)
                with yc1:
                    yade_convoy_filter = st.text_input(
                        "Convoy No",
                        key=f"bccr_yade_convoy_{target_location_id}"
                    )
                    yade_no_filter = st.text_input(
                        "Yade No",
                        key=f"bccr_yade_yade_{target_location_id}"
                    )
                with yc2:
                    yade_date_filter = st.date_input(
                        "Date",
                        value=None,
                        key=f"bccr_yade_date_{target_location_id}"
                    )
    
                def _matches_yade(row):
                    if yade_convoy_filter and yade_convoy_filter.strip():
                        if yade_convoy_filter.strip().lower() not in (row.get("Convoy No") or "").lower():
                            return False
                    if yade_no_filter and yade_no_filter.strip():
                        if yade_no_filter.strip().lower() not in (row.get("Yade No") or "").lower():
                            return False
                    if yade_date_filter and row.get("Date"):
                        if row["Date"] != yade_date_filter:
                            return False
                    return True
    
                filtered_yade_rows = [r for r in yade_rows if _matches_yade(r)]
    
                yade_selected = _render_selectable_table(
                    filtered_yade_rows,
                    "yade",
                    ["Date", "Convoy No", "Yade No", "ROB Qty", "ROB Water", "TOB Qty", "TOB Water", "Net Loaded", "Net Water"],
                    {
                        "Date": "Date",
                        "Convoy No": "Convoy No",
                        "Yade No": "Yade No",
                        "ROB Qty": "ROB Qty (bbls)",
                        "ROB Water": "ROB Water (bbls)",
                        "TOB Qty": "TOB Qty (bbls)",
                        "TOB Water": "TOB Water (bbls)",
                        "Net Loaded": "Net Loaded (bbls)",
                        "Net Water": "Net Water (bbls)",
                    }
                )
    
            with map_col2:
                st.subheader("Dispatch to Barge (OTR)")
                oc1, oc2 = st.columns(2)
                with oc1:
                    otr_date_filter = st.date_input(
                        "Date",
                        value=None,
                        key=f"bccr_otr_date_{target_location_id}"
                    )
                with oc2:
                    otr_tank_filter = st.text_input(
                        "Tank",
                        key=f"bccr_otr_tank_{target_location_id}"
                    )
    
                def _matches_otr(row):
                    if otr_date_filter and row.get("Date"):
                        if row["Date"] != otr_date_filter:
                            return False
                    if otr_tank_filter and otr_tank_filter.strip():
                        if otr_tank_filter.strip().lower() not in (row.get("Tank") or "").lower():
                            return False
                    return True
    
                filtered_otr_rows = [r for r in otr_rows if _matches_otr(r)]
    
                otr_selected = _render_selectable_table(
                    filtered_otr_rows,
                    "otr",
                    ["Date", "Ticket ID", "Tank", "Operation", "Net Rece/Disp (bbls)", "Net Water Rece/Disp (bbls)"],
                    {
                        "Date": "Date",
                        "Ticket ID": "Ticket",
                        "Tank": "Tank",
                        "Operation": "Operation",
                        "Net Rece/Disp (bbls)": "Net Rece/Disp (bbls)",
                        "Net Water Rece/Disp (bbls)": "Net Water Rece/Disp (bbls)",
                    }
                )
    
            can_map = bool(yade_selected and otr_selected)
            if st.button("MAP Selected Rows", disabled=not can_map, type="primary"):
                if not can_map:
                    st.warning("Please select at least one YADE record and one Dispatch record.")
                else:
                    selected_yade_rows = [yade_lookup[row_id] for row_id in yade_selected if row_id in yade_lookup]
                    selected_otr_rows = [otr_lookup[row_id] for row_id in otr_selected if row_id in otr_lookup]
    
                    if not selected_yade_rows or not selected_otr_rows:
                        st.warning("Unable to locate selected rows. Please try again.")
                    else:
                        pending_payload = {
                            "yade_ids": list(yade_selected),
                            "otr_ids": list(otr_selected),
                            "rob_qty": round(sum(r["ROB Qty"] for r in selected_yade_rows), 2),
                            "rob_water": round(sum(r["ROB Water"] for r in selected_yade_rows), 2),
                            "tob_qty": round(sum(r["TOB Qty"] for r in selected_yade_rows), 2),
                            "tob_water": round(sum(r["TOB Water"] for r in selected_yade_rows), 2),
                            "net_yade": round(sum(r["Net Loaded"] for r in selected_yade_rows), 2),
                            "net_water": round(sum(r["Net Water"] for r in selected_yade_rows), 2),
                            "bccr_qty": round(sum(r["Net Rece/Disp (bbls)"] for r in selected_otr_rows if isinstance(r["Net Rece/Disp (bbls)"], (int, float))), 2),
                        }
                        _set_pending(pending_payload)
                        _set_selection("bccr_yade_selected", set())
                        _set_selection("bccr_otr_selected", set())
                        st.session_state.pop(f"yade_editor_{target_location_id}", None)
                        st.session_state.pop(f"otr_editor_{target_location_id}", None)
                        st.success("Selections mapped. Review and finalize in the BCCR Report tab.")
    
        with tab_report:
            st.markdown("### BCCR Report")
            pending_data = _get_pending()
            records = _get_records()
    
            rep_col1, rep_col2 = st.columns(2)
            with rep_col1:
                report_date_filter = st.date_input(
                    "Filter by Date",
                    value=None,
                    key=f"bccr_report_date_{target_location_id}"
                )
            with rep_col2:
                report_convoy_filter = st.text_input(
                    "Filter by Convoy No",
                    key=f"bccr_report_convoy_{target_location_id}"
                )
    
            export_container = st.container()
    
            if pending_data:
                st.info("Pending mapping detected. Review the values below and save to BCCR report.")
                default_sno = max([rec["sno"] for rec in records], default=0) + 1
                with st.form(f"bccr_add_form_{target_location_id}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        new_sno = st.number_input("S.No", min_value=1, value=default_sno, step=1)
                        new_date = st.date_input("Date", value=date.today())
                        new_convoy = st.text_input("Convoy No", value="")
                        new_rob_qty = st.number_input("ROB Qty (bbls)", value=float(pending_data["rob_qty"]), format="%.2f")
                        new_rob_water = st.number_input("ROB Water (bbls)", value=float(pending_data["rob_water"]), format="%.2f")
                        new_tob_qty = st.number_input("TOB Qty (bbls)", value=float(pending_data["tob_qty"]), format="%.2f")
                    with col2:
                        new_tob_water = st.number_input("TOB Water (bbls)", value=float(pending_data["tob_water"]), format="%.2f")
                        new_net_yade = st.number_input("Net YADE Receipt (bbls)", value=float(pending_data["net_yade"]), format="%.2f")
                        new_net_water = st.number_input("Net Water (bbls)", value=float(pending_data["net_water"]), format="%.2f")
                        new_bccr_qty = st.number_input("BCCR Quantity (bbls)", value=float(pending_data["bccr_qty"]), format="%.2f")
                        difference_value = new_bccr_qty - new_net_yade
                        st.metric("Difference (BCCR - YADE)", f"{difference_value:,.2f} bbls")
                    new_remarks = st.text_area("Remarks", value="")
    
                    if st.form_submit_button("Save BCCR Mapping", type="primary"):
                        record = {
                            "id": str(uuid4()),
                            "sno": int(new_sno),
                            "date": new_date.strftime("%Y-%m-%d"),
                            "convoy": new_convoy,
                            "rob_qty": round(new_rob_qty, 2),
                            "rob_water": round(new_rob_water, 2),
                            "tob_qty": round(new_tob_qty, 2),
                            "tob_water": round(new_tob_water, 2),
                            "net_yade": round(new_net_yade, 2),
                            "net_water": round(new_net_water, 2),
                            "bccr_qty": round(new_bccr_qty, 2),
                            "difference": round(difference_value, 2),
                            "remarks": new_remarks.strip(),
                        }
                        records.append(record)
                        _set_records(records)
                        _set_pending(None)
                        st.success("Mapping saved.")
            else:
                st.caption("Select records in the Mapping tab and click MAP to create a pending entry.")
    
            def _record_matches(rec: dict) -> bool:
                if report_date_filter:
                    try:
                        rec_date = datetime.strptime(rec["date"], "%Y-%m-%d").date()
                        if rec_date != report_date_filter:
                            return False
                    except Exception:
                        return False
                if report_convoy_filter and report_convoy_filter.strip():
                    if report_convoy_filter.strip().lower() not in (rec.get("convoy") or "").lower():
                        return False
                return True
    
            filtered_records = [rec for rec in records if _record_matches(rec)]
            column_map = {
                "sno": "S.No",
                "date": "Date",
                "convoy": "Convoy No",
                "rob_qty": "ROB Qty",
                "rob_water": "ROB Water",
                "tob_qty": "TOB Qty",
                "tob_water": "TOB Water",
                "net_yade": "Net YADE Receipt",
                "net_water": "Net Water",
                "bccr_qty": "BCCR Quantity",
                "difference": "Difference Yade vs BCCR",
                "remarks": "Remarks",
            }
            download_df = pd.DataFrame()
            if filtered_records:
                records_df_sorted = pd.DataFrame(filtered_records).sort_values("sno")
                renamed_df = records_df_sorted.rename(columns=column_map)
                download_df = renamed_df.drop(columns=["id"], errors="ignore")
    
            export_disabled = download_df.empty
            filter_parts = []
            if report_date_filter:
                filter_parts.append(f"Date: {report_date_filter.strftime('%d %b %Y')}")
            if report_convoy_filter and report_convoy_filter.strip():
                filter_parts.append(f"Convoy: {report_convoy_filter.strip()}")
            filter_description = " | ".join(filter_parts) if filter_parts else "No filters applied"
    
            date_token = report_date_filter.strftime("%Y%m%d") if report_date_filter else "ALL"
            convoy_raw = (report_convoy_filter or "").strip()
            convoy_token = re.sub(r"[^A-Za-z0-9]+", "_", convoy_raw).strip("_") or "ALL"
            location_token = _canon(target_location_code) or _canon(target_location_name) or "LOCATION"
            base_filename = f"BCCR_{location_token}_{date_token}_{convoy_token}"
    
            from io import BytesIO
    
            csv_bytes = download_df.to_csv(index=False).encode("utf-8") if not export_disabled else b""
            xlsx_bytes = b""
            if not export_disabled:
                xlsx_buffer = BytesIO()
                with pd.ExcelWriter(xlsx_buffer, engine="xlsxwriter") as writer:
                    download_df.to_excel(writer, sheet_name="BCCR", index=False)
                xlsx_bytes = xlsx_buffer.getvalue()
    
            pdf_bytes = b""
            pdf_error_message = None
            if not export_disabled:
                try:
                    pdf_bytes = _generate_bccr_pdf(download_df, target_location_name, target_location_code, filter_description)
                except Exception as exc:
                    pdf_error_message = str(exc)
                    pdf_bytes = b""
    
            with export_container:
                st.markdown("#### Export & Downloads")
                col_csv, col_xlsx, col_pdf, col_view = st.columns(4)
                with col_csv:
                    st.download_button(
                        "📥 CSV",
                        data=csv_bytes,
                        file_name=f"{base_filename}.csv",
                        mime="text/csv",
                        disabled=export_disabled,
                        key=f"bccr_csv_{target_location_id}",
                    )
                with col_xlsx:
                    st.download_button(
                        "📥 XLSX",
                        data=xlsx_bytes,
                        file_name=f"{base_filename}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        disabled=export_disabled,
                        key=f"bccr_xlsx_{target_location_id}",
                    )
                with col_pdf:
                    st.download_button(
                        "📥 PDF",
                        data=pdf_bytes,
                        file_name=f"{base_filename}.pdf",
                        mime="application/pdf",
                        disabled=export_disabled or pdf_error_message is not None,
                        key=f"bccr_pdf_{target_location_id}",
                    )
                with col_view:
                    if st.button(
                        "👁️ View PDF",
                        key=f"bccr_pdf_view_{target_location_id}",
                        disabled=export_disabled or pdf_error_message is not None,
                    ):
                        import base64
                        import streamlit.components.v1 as components
    
                        b64 = base64.b64encode(pdf_bytes).decode("utf-8")
                        components.html(
                            f"""
                            <script>
                                const pdfData = "{b64}";
                                const byteCharacters = atob(pdfData);
                                const byteNumbers = new Array(byteCharacters.length);
                                for (let i = 0; i < byteCharacters.length; i++) {{
                                    byteNumbers[i] = byteCharacters.charCodeAt(i);
                                }}
                                const byteArray = new Uint8Array(byteNumbers);
                                const file = new Blob([byteArray], {{ type: "application/pdf" }});
                                const fileURL = URL.createObjectURL(file);
                                window.open(fileURL, "_blank");
                            </script>
                            """,
                            height=0,
                        )
                        st.success("BCCR PDF opened in a new tab.")
    
                if pdf_error_message:
                    st.warning(f"PDF export unavailable: {pdf_error_message}")
    
            if filtered_records:
                st.subheader("Mapped Records")
                st.dataframe(download_df, use_container_width=True, hide_index=True)
    
                for rec in filtered_records:
                    if st.button(f"Delete S.No {rec['sno']}", key=f"bccr_delete_{rec['id']}"):
                        records = [r for r in records if r["id"] != rec["id"]]
                        _set_records(records)
                        st.success(f"Deleted mapping S.No {rec['sno']}")
                        break
    
            else:
                st.info("No BCCR mappings found for the current filters.")
    
    # MATERIAL BALANCE � placeholder reports