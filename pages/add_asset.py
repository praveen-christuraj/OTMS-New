"""
Auto-generated module for the 'Add Asset' page.
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
        from sqlalchemy.exc import IntegrityError
    
        if st.session_state.get("auth_user", {}).get("role") != "admin-operations":
            header("Add Asset")
            st.subheader("Assets")
            st.error("You do not have permission to access this page. Admin-Operations only.")
            st.stop()
    
        header("Add Asset")
        st.subheader("Assets")
    
        # ============ LOCATION CONTEXT ============
        active_location_id = st.session_state.get("active_location_id")
        if not active_location_id:
            st.warning("⚠️ Please select a location from the Home page first.")
            st.stop()
    
        # Display current location
        with get_session() as s:
            from location_manager import LocationManager
            loc = LocationManager.get_location_by_id(s, active_location_id)
            if loc:
                st.info(f"📍 **Adding assets to Location:** {loc.name} ({loc.code})")
    
        # ------------------------ Shared helpers (once) ------------------------
        def _read_table(file) -> pd.DataFrame:
            """Read CSV/XLSX to DataFrame and lowercase headers."""
            name = (getattr(file, "name", "") or "").lower()
            try:
                if name.endswith(".csv"):
                    df = pd.read_csv(file)
                else:
                    try:
                        import openpyxl
                    except Exception:
                        st.error("XLSX support needs 'openpyxl'. Install: pip install openpyxl")
                        raise
                    df = pd.read_excel(file)
            except Exception as e:
                st.error(f"Failed to read file: {e}")
                raise
            df.columns = [str(c).strip().lower() for c in df.columns]
            return df
    
        def _require_cols(df: pd.DataFrame, required: set):
            miss = list(required - set(df.columns))
            if miss:
                raise ValueError(f"Missing required columns: {', '.join(miss)}")
    
        def _numeric(df: pd.DataFrame, cols: list) -> pd.DataFrame:
            """Coerce numeric columns; drop rows with invalid numbers (warn)."""
            for c in cols:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            need = [c for c in cols if c in df.columns]
            if need:
                bad = df[df[need].isna().any(axis=1)]
                if not bad.empty:
                    st.warning(f"{len(bad)} row(s) have invalid numbers and will be skipped.")
                    df = df.dropna(subset=need)
            return df
    
        # UPDATE TABS TO INCLUDE TANKER MASTER AND TANKER CALIBRATION
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "Tank Master",
            "ASTM Table 11",
            "YADE Barges",
            "Tanker Master & Calibration",  # NEW - Combined
            "Vessels",
        ])
    
        # ============================== TAB 1: Tank Master ==============================
        with tab1:
            st.markdown("#### Add / Edit Tank")
            
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                tank_name = st.text_input("Tank Name *", placeholder="e.g., OST-A", key="asset_tank_name")
            with c2:
                capacity_bbl = st.number_input("Capacity (bbl)", min_value=0.0, step=1.0, key="asset_capacity")
            with c3:
                product = st.text_input("Product", value="CRUDE", key="asset_product")
            with c4:
                status = st.selectbox("Status", ["ACTIVE", "INACTIVE"], key="asset_status")
    
            bL, bR = st.columns([0.50, 0.50])
    
            # --------- Left: Upload Tank Calibration with overwrite protection ---------
            with bL:
                with st.expander("📤 Upload Calibration Chart (CSV/XLSX)", expanded=False):
                    st.caption("Required columns in file: **dip_cm, volume_bbl**. "
                               "The **tank_name** is taken from the input box above.")
                    up_tank_cal = st.file_uploader("Select file", type=["csv", "xlsx"], key="tank_cal_upl")
    
                    df_preview = None
                    if up_tank_cal is not None:
                        try:
                            df = _read_table(up_tank_cal)
                            _require_cols(df, {"dip_cm", "volume_bbl"})
                            df = _numeric(df, ["dip_cm", "volume_bbl"]).sort_values("dip_cm").reset_index(drop=True)
    
                            st.markdown("**Preview (first 50 rows)**")
                            st.dataframe(df.head(50), use_container_width=True, hide_index=True)
                            df_preview = df
                        except Exception as ex:
                            st.error(f"Upload error: {ex}")
    
                    if df_preview is not None:
                        if not tank_name.strip():
                            st.info("Enter **Tank Name** above to continue.")
                        else:
                            # Show overwrite status against DB for this tank + location
                            try:
                                from models import CalibrationTank
                                with get_session() as s:
                                    exist_cnt = s.query(CalibrationTank).filter(
                                        CalibrationTank.location_id == active_location_id,
                                        CalibrationTank.tank_name == tank_name.strip()
                                    ).count()
                                if exist_cnt > 0:
                                    st.warning(
                                        f"Existing calibration found for **{tank_name.strip()}** at this location "
                                        f"({exist_cnt} row(s)). Importing will overwrite."
                                    )
                                    overwrite_ok = st.checkbox(
                                        f"I confirm to overwrite calibration for {tank_name.strip()}",
                                        key=f"tankcal_overwrite_{tank_name.strip()}"
                                    )
                                else:
                                    st.success("No existing calibration found. A fresh set will be saved.")
                                    overwrite_ok = True
                            except Exception as ex:
                                overwrite_ok = False
                                st.error(f"Can't check existing calibration: {ex}")
    
                            if st.button("Import Calibration to DB", key="tank_cal_import_btn"):
                                if not overwrite_ok:
                                    st.error("Please tick the overwrite confirmation to proceed.")
                                else:
                                    try:
                                        from models import CalibrationTank
                                        with get_session() as s:
                                            # delete previous rows for this tank + location
                                            s.query(CalibrationTank).filter(
                                                CalibrationTank.location_id == active_location_id,
                                                CalibrationTank.tank_name == tank_name.strip()
                                            ).delete()
                                            # bulk insert new
                                            s.bulk_save_objects([
                                                CalibrationTank(
                                                    location_id=active_location_id,
                                                    tank_name=tank_name.strip(),
                                                    dip_cm=float(r["dip_cm"]),
                                                    volume_bbl=float(r["volume_bbl"])
                                                ) for _, r in df_preview.iterrows()
                                            ])
                                            s.commit()
                                            # ----------------------- Audit log for calibration import -----------------------
                                            try:
                                                from security import SecurityManager  # type: ignore
                                                user_ctx = st.session_state.get("auth_user") or {}
                                                username = user_ctx.get("username", "unknown")
                                                user_id = user_ctx.get("id")
                                                location_id = st.session_state.get("active_location_id") or user_ctx.get("location_id")
                                                SecurityManager.log_audit(
                                                    None,
                                                    username,
                                                    "IMPORT",
                                                    resource_type="CalibrationTank",
                                                    resource_id=tank_name.strip(),
                                                    details=f"Imported {len(df_preview)} calibration rows for tank {tank_name.strip()}",
                                                    user_id=user_id,
                                                    location_id=location_id,
                                                )
                                            except Exception:
                                                # Do not interrupt user flow if logging fails
                                                pass
                                        st.success(
                                            f"Calibration imported for **{tank_name.strip()}** "
                                            f"({len(df_preview)} rows)."
                                        )
                                    except Exception as ex:
                                        st.error(f"Failed to import calibration: {ex}")
    
            # --------- Right: Save Tank master record ---------
            with bR:
                if st.button("Save Tank", key="asset_save_tank"):
                    errs = []
                    if not tank_name.strip():
                        errs.append("Tank Name is required.")
                    if errs:
                        for e in errs:
                            st.error(e)
                    else:
                        try:
                            from models import Tank, TankStatus
                            with get_session() as s:
                                existing = s.query(Tank).filter(
                                    Tank.location_id == active_location_id,
                                    Tank.name == tank_name.strip()
                                ).one_or_none()
                                
                                if existing:
                                    existing.capacity_bbl = capacity_bbl
                                    existing.product = product.strip()
                                    existing.status = TankStatus[status]
                                else:
                                    s.add(Tank(
                                        location_id=active_location_id,
                                        name=tank_name.strip(),
                                        capacity_bbl=capacity_bbl,
                                        product=product.strip(),
                                        status=TankStatus[status]
                                    ))
                                s.commit()
                                # ----------------------- Audit log for tank save -----------------------
                                try:
                                    from security import SecurityManager  # type: ignore
                                    user_ctx = st.session_state.get("auth_user") or {}
                                    username = user_ctx.get("username", "unknown")
                                    user_id = user_ctx.get("id")
                                    # Determine action based on existence of tank
                                    action_type = "UPDATE" if existing else "CREATE"
                                    SecurityManager.log_audit(
                                        None,
                                        username,
                                        action_type,
                                        resource_type="Tank",
                                        resource_id=tank_name.strip(),
                                        details=f"{action_type.title()} tank {tank_name.strip()}",
                                        user_id=user_id,
                                        location_id=active_location_id,
                                    )
                                except Exception:
                                    pass
                            st.success("Tank saved.")
                        except Exception as ex:
                            st.error(f"Failed to save tank: {ex}")
    
            st.markdown("##### Tanks (from DB)")
            try:
                from models import Tank
                with get_session() as s:
                    rows = s.query(Tank).filter(
                        Tank.location_id == active_location_id
                    ).order_by(Tank.name).all()
                if rows:
                    st.dataframe(
                        pd.DataFrame([{
                            "Name": r.name,
                            "Capacity (bbl)": r.capacity_bbl,
                            "Product": r.product,
                            "Status": r.status.value if r.status else None
                        } for r in rows]),
                        use_container_width=True, hide_index=True
                    )
                else:
                    st.info("No tanks yet for this location. Add one above.")
            except Exception as ex:
                st.error(f"Failed to load tanks: {ex}")
    
            st.markdown("##### Delete Tank (Admin)")
            try:
                from models import Tank
                with get_session() as s:
                    tank_list = s.query(Tank).filter(
                        Tank.location_id == active_location_id
                    ).order_by(Tank.name).all()
                
                if tank_list:
                    del_col1, del_col2, del_col3 = st.columns([0.4, 0.4, 0.2])
                    with del_col1:
                        del_tank_name = st.selectbox(
                            "Select Tank to delete",
                            [t.name for t in tank_list],
                            key="asset_del_tank_select"
                        )
                    with del_col2:
                        confirm_text = st.text_input(
                            "Type the tank name to confirm",
                            key="asset_del_tank_confirm"
                        )
                    with del_col3:
                        if st.button("Delete Tank", key="asset_del_tank_btn"):
                            if not del_tank_name:
                                st.error("Please select a tank.")
                            elif confirm_text.strip() != del_tank_name:
                                st.error("Confirmation text does not match the selected tank name.")
                            else:
                                try:
                                    with get_session() as s:
                                        obj = s.query(Tank).filter(
                                            Tank.location_id == active_location_id,
                                            Tank.name == del_tank_name
                                        ).one_or_none()
                                        if not obj:
                                            st.warning("Tank not found (maybe already deleted).")
                                        else:
                                            _archive_record_for_delete(
                                                s,
                                                obj,
                                                "Tank",
                                                reason=f"Marked tank {del_tank_name} for deletion.",
                                                label=del_tank_name,
                                            )
                                            s.commit()
                                    st.success(f"Deleted tank: {del_tank_name}")
                                    _st_safe_rerun()
                                except IntegrityError:
                                    st.error("Cannot delete: this tank is referenced by existing transactions.")
                                except Exception as ex:
                                    st.error(f"Failed to delete tank: {ex}")
                else:
                    st.info("No tanks to delete for this location.")
            except Exception as ex:
                st.error(f"Delete UI error (tanks): {ex}")
    
        # ============================== TAB 2: ASTM Table 11 ==============================
        with tab2:
            st.markdown("#### ASTM Table 11 � Import (.xlsx)")
            st.info("ℹ️ **Note:** ASTM Table 11 is shared across ALL locations (not location-specific).")
            
            st.caption("""
            Upload an Excel file (.xlsx) with exactly **two columns** (case-insensitive):
            1) **API @ 60°F** (variants like `API_60`, `api 60f`, `api @60 f` handled)
            2) **LT factor** (variants like `lt`, `lt_factor`)
            We'll normalize headers automatically and store to the **table11** dataset.
            """)
    
            def _read_xlsx(file) -> pd.DataFrame:
                try:
                    import openpyxl
                except Exception:
                    st.error("XLSX support needs 'openpyxl'. Install: pip install openpyxl")
                    raise
                df = pd.read_excel(file)
                df.columns = [str(c).strip().lower() for c in df.columns]
                return df
    
            def _normalize_table11(df: pd.DataFrame) -> pd.DataFrame:
                colmap = {}
                for c in df.columns:
                    cl = c.lower().strip()
                    cl = cl.replace("�", "�").replace("deg", "�").replace("degrees", "�")
                    cl = cl.replace("@60f", "@ 60°F").replace("@ 60 f", "@ 60°F").replace("@60 °F", "@ 60°F")
                    cl = " ".join(cl.split())
                    if cl in {"api @ 60°F","api@60°F","api 60°F","api @60°F","api @ 60°F","api_60","api60","api 60f","api @60 f"}:
                        colmap[c] = "api60"
                    elif cl in {"lt factor","lt","ltf","lt_factor"}:
                        colmap[c] = "lt_factor"
    
                if "api60" not in colmap.values() or "lt_factor" not in colmap.values():
                    raise ValueError("Could not detect both required columns: API @ 60°F and LT factor.")
    
                df = df.rename(columns=colmap)[["api60", "lt_factor"]]
                df["api60"] = pd.to_numeric(df["api60"], errors="coerce")
                df["lt_factor"] = pd.to_numeric(df["lt_factor"], errors="coerce")
                bad = df[df[["api60","lt_factor"]].isna().any(axis=1)]
                if not bad.empty:
                    st.warning(f"{len(bad)} row(s) have invalid numbers and will be skipped.")
                    df = df.dropna(subset=["api60","lt_factor"])
                return df.sort_values("api60").reset_index(drop=True)
    
            up_tbl11 = st.file_uploader("Select ASTM Table 11 (.xlsx only)", type=["xlsx"], key="astm11_upl")
    
            df11_preview = None
            if up_tbl11 is not None:
                try:
                    df11 = _read_xlsx(up_tbl11)
                    df11 = _normalize_table11(df11)
    
                    st.markdown("**Preview (first 100 rows)**")
                    st.dataframe(df11.head(100), use_container_width=True, hide_index=True)
                    df11_preview = df11
                except Exception as ex:
                    st.error(f"Upload error: {ex}")
    
            if df11_preview is not None:
                try:
                    from models import Table11
                    with get_session() as s:
                        existing = s.query(Table11).count()
                    if existing > 0:
                        st.warning(f"Existing ASTM Table 11 has **{existing}** row(s). Importing will overwrite.")
                        astm_overwrite_ok = st.checkbox("I confirm to overwrite ASTM Table 11", key="astm11_overwrite_ck")
                    else:
                        st.success("No existing ASTM Table 11 found. A fresh dataset will be saved.")
                        astm_overwrite_ok = True
                except Exception as ex:
                    st.error(f"Can't check existing ASTM Table 11: {ex}")
                    astm_overwrite_ok = False
    
                if st.button("Import ASTM Table 11 to DB", key="astm11_import_btn"):
                    if not astm_overwrite_ok:
                        st.error("Please tick the overwrite confirmation to proceed.")
                    else:
                        try:
                            from models import Table11
                            with get_session() as s:
                                s.query(Table11).delete()
                                s.bulk_save_objects([
                                    Table11(api60=float(r["api60"]), lt_factor=float(r["lt_factor"]))
                                    for _, r in df11_preview.iterrows()
                                ])
                                s.commit()
                                # ----------------------- Audit log for ASTM Table 11 import -----------------------
                                try:
                                    from security import SecurityManager  # type: ignore
                                    user_ctx = st.session_state.get("auth_user") or {}
                                    username = user_ctx.get("username", "unknown")
                                    user_id = user_ctx.get("id")
                                    SecurityManager.log_audit(
                                        None,
                                        username,
                                        "IMPORT",
                                        resource_type="ASTMTable11",
                                        resource_id="Table11",
                                        details=f"Imported ASTM Table 11 with {len(df11_preview)} rows",
                                        user_id=user_id,
                                        location_id=None,
                                    )
                                except Exception:
                                    pass
                            st.success(f"ASTM Table 11 imported: {len(df11_preview)} rows.")
                        except Exception as ex:
                            st.error(f"Failed to import ASTM Table 11: {ex}")
            else:
                st.info("Choose an .xlsx file to preview and import.")
    
        # ============================== TAB 3: YADE Barges ==============================
        with tab3:
            st.markdown("#### Add / Edit YADE Barge")
            st.info("ℹ️ **Note:** YADE barges are shared across ALL locations (same barges travel between terminals).")
            
            y1, y2 = st.columns(2)
            with y1:
                yade_name = st.text_input("YADE No *", placeholder="e.g., YADE-001", key="yade_name")
            with y2:
                design = st.selectbox("Tank Design *", ["6", "4"], index=0, key="yade_design")
    
            ybL, ybR = st.columns([0.50, 0.50])
    
            # --------- Left: Upload YADE Calibration ---------
            with ybL:
                with st.expander("📤 Upload YADE Calibration (CSV/XLSX)", expanded=False):
                    st.caption("""Required columns:
        **yade_name, tank_id, dip_mm, vol_bbl** and (optionally) **mm1..mm9**.
        - `yade_name` must match the YADE No field (or will be overwritten by it if filled above).
        - `tank_id` must be C1,C2,P1,P2,S1,S2 (6-tank) or P1,P2,S1,S2 (4-tank).
        - **Calibration is shared globally** - same for all locations.""")
    
                    up_yade_cal = st.file_uploader("Select file", type=["csv", "xlsx"], key="yade_cal_upl")
    
                    dfy_preview = None
                    if up_yade_cal is not None:
                        try:
                            dfy = _read_table(up_yade_cal)
                            _require_cols(dfy, {"yade_name", "tank_id", "dip_mm", "vol_bbl"})
    
                            dfy["tank_id"] = dfy["tank_id"].astype(str).str.upper().str.strip()
                            dfy["yade_name"] = dfy["yade_name"].astype(str).str.strip()
    
                            if yade_name.strip():
                                dfy["yade_name"] = yade_name.strip()
    
                            num_cols = ["dip_mm", "vol_bbl", "mm1","mm2","mm3","mm4","mm5","mm6","mm7","mm8","mm9"]
                            dfy = _numeric(dfy, num_cols)
                            dfy = dfy.sort_values(["yade_name", "tank_id", "dip_mm"]).reset_index(drop=True)
    
                            st.markdown("**Preview (first 60 rows)**")
                            st.dataframe(dfy.head(60), use_container_width=True, hide_index=True)
                            dfy_preview = dfy
                        except Exception as ex:
                            st.error(f"Upload error: {ex}")
    
                    if dfy_preview is not None:
                        affected_yades = sorted(dfy_preview["yade_name"].unique().tolist())
                        affected_pairs = sorted(dfy_preview[["yade_name","tank_id"]].drop_duplicates().itertuples(index=False, name=None))
    
                        st.info("**Will import calibration for:**")
                        st.write(", ".join([f"{yn}" for yn in affected_yades]))
                        st.caption("Pairs (YADE, Tank): " + ", ".join([f"({yn},{tk})" for yn, tk in affected_pairs]))
    
                        try:
                            from models import YadeCalibration
                            with get_session() as s:
                                existing_rows = (
                                    s.query(YadeCalibration.yade_name, YadeCalibration.tank_id)
                                    .filter(YadeCalibration.yade_name.in_(affected_yades))
                                    .all()
                                )
                            if existing_rows:
                                existing_set = sorted(set(existing_rows))
                                st.warning(
                                    "Existing calibration found for the following pairs (will be overwritten):\n" +
                                    ", ".join([f"({yn},{tk})" for yn, tk in existing_set])
                                )
                                yade_overwrite_ok = st.checkbox(
                                    "I confirm to overwrite the existing YADE calibration for the pairs listed above",
                                    key="yade_overwrite_ck"
                                )
                            else:
                                st.success("No existing calibration found for these YADE(s). A fresh set will be saved.")
                                yade_overwrite_ok = True
                        except Exception as ex:
                            yade_overwrite_ok = False
                            st.error(f"Can't check existing YADE calibration: {ex}")
    
                        if st.button("Import YADE Calibration to DB", key="yade_cal_import_btn"):
                            if not yade_overwrite_ok:
                                st.error("Please tick the overwrite confirmation to proceed.")
                            else:
                                try:
                                    from models import YadeCalibration
                                    with get_session() as s:
                                        s.query(YadeCalibration).filter(
                                            YadeCalibration.yade_name.in_(affected_yades)
                                        ).delete(synchronize_session=False)
    
                                        objs = []
                                        has_mm = [c for c in ["mm1","mm2","mm3","mm4","mm5","mm6","mm7","mm8","mm9"] if c in dfy_preview.columns]
                                        for _, r in dfy_preview.iterrows():
                                            kwargs = dict(
                                                yade_name=r["yade_name"],
                                                tank_id=r["tank_id"],
                                                dip_mm=float(r["dip_mm"]),
                                                vol_bbl=float(r["vol_bbl"]),
                                            )
                                            for mm in has_mm:
                                                val = r.get(mm, None)
                                                kwargs[mm] = float(val) if pd.notna(val) else None
                                            objs.append(YadeCalibration(**kwargs))
    
                                        s.bulk_save_objects(objs)
                                        s.commit()
                                        # ----------------------- Audit log for YADE calibration import -----------------------
                                        try:
                                            from security import SecurityManager  # type: ignore
                                            user_ctx = st.session_state.get("auth_user") or {}
                                            username = user_ctx.get("username", "unknown")
                                            user_id = user_ctx.get("id")
                                            SecurityManager.log_audit(
                                                None,
                                                username,
                                                "IMPORT",
                                                resource_type="YadeCalibration",
                                                resource_id="*",
                                                details=f"Imported {len(dfy_preview)} YADE calibration rows for {len(affected_yades)} YADE(s)",
                                                user_id=user_id,
                                                location_id=None,
                                            )
                                        except Exception:
                                            pass
    
                                    st.success(
                                        f"YADE calibration imported for {len(affected_yades)} YADE(s), "
                                        f"{len(dfy_preview)} row(s) total (shared globally)."
                                    )
                                except Exception as ex:
                                    st.error(f"Failed to import YADE calibration: {ex}")
    
            # --------- Right: Save YADE Barge master ---------
            with ybR:
                if st.button("Save YADE Barge", key="yade_save_btn"):
                    if not yade_name.strip():
                        st.error("YADE No is required.")
                    else:
                        try:
                            from models import YadeBarge
                            with get_session() as s:
                                existing = s.query(YadeBarge).filter(
                                    YadeBarge.name == yade_name.strip()
                                ).one_or_none()
                                
                                if existing:
                                    existing.design = design
                                    st.info(f"Updated existing YADE barge '{yade_name.strip()}' (shared globally).")
                                else:
                                    s.add(YadeBarge(
                                        name=yade_name.strip(),
                                        design=design
                                    ))
                                    st.success(f"Created new YADE barge '{yade_name.strip()}' (available to all locations).")
                                s.commit()
                                # ----------------------- Audit log for YADE barge save -----------------------
                                try:
                                    from security import SecurityManager  # type: ignore
                                    user_ctx = st.session_state.get("auth_user") or {}
                                    username = user_ctx.get("username", "unknown")
                                    user_id = user_ctx.get("id")
                                    action_type = "UPDATE" if existing else "CREATE"
                                    SecurityManager.log_audit(
                                        None,
                                        username,
                                        action_type,
                                        resource_type="YadeBarge",
                                        resource_id=yade_name.strip(),
                                        details=f"{action_type.title()} YADE barge {yade_name.strip()}",
                                        user_id=user_id,
                                        location_id=None,
                                    )
                                except Exception:
                                    pass
                        except Exception as ex:
                            st.error(f"Failed to save YADE barge: {ex}")
    
            st.markdown("##### YADE Barges (All - Shared Globally)")
            try:
                from models import YadeBarge
                with get_session() as s:
                    rows = s.query(YadeBarge).order_by(YadeBarge.name).all()
                if rows:
                    st.dataframe(
                        pd.DataFrame([{"YADE No": r.name, "Design": r.design} for r in rows]),
                        use_container_width=True, hide_index=True
                    )
                    st.caption(f"🔢 Total: {len(rows)} YADE barge(s) (shared across all locations)")
                else:
                    st.info("No YADE barges yet. Add one above.")
            except Exception as ex:
                st.error(f"Failed to load YADE barges: {ex}")
    
            st.markdown("##### Delete YADE Barge (Admin)")
            st.warning("⚠️ Deleting a YADE barge affects ALL locations!")
                    
            try:
                from models import YadeBarge
                with get_session() as s:
                    barge_list = s.query(YadeBarge).order_by(YadeBarge.name).all()
                
                if barge_list:
                    yd1, yd2, yd3 = st.columns([0.4, 0.4, 0.2])
                    with yd1:
                        del_yade_name = st.selectbox(
                            "Select YADE to delete",
                            [b.name for b in barge_list],
                            key="asset_del_yade_select"
                        )
                    with yd2:
                        yade_confirm = st.text_input(
                            "Type the YADE No to confirm",
                            key="asset_del_yade_confirm"
                        )
                    with yd3:
                        if st.button("Delete YADE Barge", key="asset_del_yade_btn"):
                            if not del_yade_name:
                                st.error("Please select a YADE barge.")
                            elif yade_confirm.strip() != del_yade_name:
                                st.error("Confirmation text does not match the selected YADE No.")
                            else:
                                try:
                                    with get_session() as s:
                                        obj = s.query(YadeBarge).filter(
                                            YadeBarge.name == del_yade_name
                                        ).one_or_none()
                                        if not obj:
                                            st.warning("YADE barge not found (maybe already deleted).")
                                        else:
                                            _archive_record_for_delete(
                                                s,
                                                obj,
                                                "YadeBarge",
                                                reason=f"Marked YADE barge '{del_yade_name}' for deletion (affects all locations).",
                                                label=del_yade_name,
                                            )
    
                                    st.success(f"Deleted YADE barge: {del_yade_name} (from all locations)")
                                    _st_safe_rerun()
                                except IntegrityError:
                                    st.error("Cannot delete: this YADE is referenced by existing YADE records.")
                                except Exception as ex:
                                    st.error(f"Failed to delete YADE barge: {ex}")
                else:
                    st.info("No YADE barges to delete.")
            except Exception as ex:
                st.error(f"Delete UI error (YADE): {ex}")
    
        
        # ============================== TAB 4: Tanker Master & Calibration (COMBINED) ==============================
        with tab4:
            st.markdown("#### Tanker Master & Calibration")
            st.info("ℹ️ **Note:** Tankers and their calibration are shared across ALL locations (same tankers travel between terminals).")
            
            # ========== SECTION 1: Add New Tanker ==========
            with st.expander("? Add New Tanker", expanded=False):
                with st.form("add_tanker_form"):
                    st.markdown("##### Tanker Details")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        tanker_name = st.text_input(
                            "Tanker Name/ID *",
                            placeholder="e.g., TKR-001",
                            key="tanker_name",
                            help="Unique identifier for the tanker"
                        )
                        
                        chassis_no = st.text_input(
                            "Chassis Number",
                            placeholder="e.g., ABC-1234-XYZ",
                            key="tanker_chassis",
                            help="Vehicle chassis number"
                        )
                    
                    with col2:
                        capacity = st.number_input(
                            "Capacity (Litres)",
                            min_value=0.0,
                            step=100.0,
                            key="tanker_capacity",
                            help="Total tanker capacity in litres"
                        )
                        
                        tanker_status = st.selectbox(
                            "Status",
                            options=["ACTIVE", "INACTIVE"],
                            key="tanker_status"
                        )
                    
                    submit_tanker = st.form_submit_button("➕ Add Tanker", type="primary")
                    
                    if submit_tanker:
                        if not tanker_name.strip():
                            st.error("? Tanker name is required")
                        else:
                            try:
                                from models import Tanker, TankStatus
                                with get_session() as s:
                                    # Check if tanker already exists
                                    existing = s.query(Tanker).filter(
                                        Tanker.name == tanker_name.strip()
                                    ).one_or_none()
                                    
                                    if existing:
                                        st.error(f"? Tanker '{tanker_name}' already exists")
                                    else:
                                        new_tanker = Tanker(
                                            name=tanker_name.strip(),
                                            registration_no=chassis_no.strip() if chassis_no else None,  # Stored as registration_no in DB
                                            capacity_litres=float(capacity) if capacity else None,
                                            status=TankStatus.ACTIVE if tanker_status == "ACTIVE" else TankStatus.INACTIVE
                                        )
                                        s.add(new_tanker)
                                        s.commit()
                                        
                                        st.success(f"? Tanker '{tanker_name}' added successfully!")
                                        
                                        # Log audit
                                        user = st.session_state.get("auth_user")
                                        if user:
                                            from security import SecurityManager
                                            SecurityManager.log_audit(
                                                s, user["username"], "CREATE",
                                                resource_type="Tanker",
                                                resource_id=tanker_name,
                                                details=f"Added tanker: {tanker_name} (Chassis: {chassis_no})",
                                                user_id=user["id"]
                                            )
                                        
                                        import time
                                        time.sleep(1)
                                        _st_safe_rerun()
                            except Exception as ex:
                                st.error(f"? Failed to add tanker: {ex}")
            
            st.markdown("---")
            
            # ========== SECTION 2: List Existing Tankers ==========
            st.markdown("##### Existing Tankers (All - Shared Globally)")
            
            try:
                from models import Tanker
                with get_session() as s:
                    tankers = s.query(Tanker).order_by(Tanker.name).all()
                
                if tankers:
                    import pandas as pd
                    df = pd.DataFrame([{
                        "Name": t.name,
                        "Chassis No": t.registration_no or "�",  # Display as "Chassis No"
                        "Capacity (L)": f"{t.capacity_litres:,.0f}" if t.capacity_litres else "�",
                        "Status": t.status.value if hasattr(t.status, 'value') else t.status,
                        "Created": t.created_at.strftime("%d/%m/%Y") if t.created_at else "�"
                    } for t in tankers])
                    
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    st.caption(f"🔢 Total tankers: {len(tankers)} (shared across all locations)")
                else:
                    st.info("No tankers added yet. Add your first tanker above.")
            except Exception as ex:
                st.error(f"? Failed to load tankers: {ex}")
            
            st.markdown("---")
            
            # ========== SECTION 3: Upload Tanker Calibration ==========
            st.markdown("##### Upload Tanker Calibration")
            
            try:
                from models import Tanker, TankStatus
                with get_session() as s:
                    tankers = s.query(Tanker).filter(Tanker.status == TankStatus.ACTIVE).order_by(Tanker.name).all()
                
                if not tankers:
                    st.warning("⚠️ No active tankers available. Please add tankers first.")
                else:
                    tanker_names = [t.name for t in tankers]
                    
                    cal_col1, cal_col2 = st.columns([0.4, 0.6])
                    
                    with cal_col1:
                        selected_tanker = st.selectbox(
                            "Select Tanker",
                            options=tanker_names,
                            key="tanker_cal_select"
                        )
                        
                        compartment = st.selectbox(
                            "Compartment",
                            options=["C1", "C2"],
                            key="tanker_compartment",
                            help="Which compartment to calibrate"
                        )
                    
                    with cal_col2:
                        st.caption("**Required columns:** dip_mm, volume_litres")
                        uploaded_file = st.file_uploader(
                            "Upload Calibration CSV",
                            type=["csv"],
                            key="tanker_cal_upload",
                            help="CSV should have columns: dip_mm, volume_litres"
                        )
                    
                    if uploaded_file:
                        try:
                            import pandas as pd
                            df = pd.read_csv(uploaded_file)
                            
                            # Normalize column names
                            df.columns = [c.strip().lower() for c in df.columns]
                            
                            # Validate columns
                            required_cols = ["dip_mm", "volume_litres"]
                            if not all(col in df.columns for col in required_cols):
                                st.error(f"? CSV must have columns: {', '.join(required_cols)}")
                            else:
                                # Convert to numeric
                                df["dip_mm"] = pd.to_numeric(df["dip_mm"], errors="coerce")
                                df["volume_litres"] = pd.to_numeric(df["volume_litres"], errors="coerce")
                                
                                # Drop invalid rows
                                df = df.dropna(subset=required_cols)
                                
                                # Sort by dip
                                df = df.sort_values("dip_mm").reset_index(drop=True)
                                
                                st.success(f"? Loaded {len(df)} calibration points")
                                
                                # Preview
                                with st.expander("🔎 Preview Calibration Data", expanded=True):
                                    st.dataframe(df.head(20), use_container_width=True, hide_index=True)
                                
                                if st.button("💾 Save Calibration Data", key="save_tanker_cal", type="primary"):
                                    try:
                                        from models import TankerCalibration
                                        with get_session() as s:
                                            # Delete existing calibration for this tanker-compartment
                                            deleted = s.query(TankerCalibration).filter(
                                                TankerCalibration.tanker_name == selected_tanker,
                                                TankerCalibration.compartment == compartment
                                            ).delete()
                                            
                                            # Add new calibration points
                                            for _, row in df.iterrows():
                                                cal = TankerCalibration(
                                                    tanker_name=selected_tanker,
                                                    compartment=compartment,
                                                    dip_mm=float(row['dip_mm']),
                                                    volume_litres=float(row['volume_litres'])
                                                )
                                                s.add(cal)
                                            
                                            s.commit()
                                            
                                            st.success(f"? Saved {len(df)} calibration points for {selected_tanker} - {compartment}")
                                            if deleted > 0:
                                                st.info(f"🔄 Replaced {deleted} existing calibration points")
                                            
                                            # Log audit
                                            user = st.session_state.get("auth_user")
                                            if user:
                                                from security import SecurityManager
                                                with get_session() as s2:
                                                    SecurityManager.log_audit(
                                                        s2, user["username"], "CREATE",
                                                        resource_type="TankerCalibration",
                                                        resource_id=f"{selected_tanker}-{compartment}",
                                                        details=f"Uploaded {len(df)} calibration points",
                                                        user_id=user["id"]
                                                    )
                                            
                                            import time
                                            time.sleep(1)
                                            _st_safe_rerun()
                                    except Exception as ex:
                                        st.error(f"? Failed to save calibration: {ex}")
                                        import traceback
                                        st.code(traceback.format_exc())
                        
                        except Exception as ex:
                            st.error(f"? Failed to read CSV: {ex}")
                    
                    # ========== SECTION 4: View Existing Calibration ==========
                    st.markdown("---")
                    st.markdown(f"##### Current Calibration: {selected_tanker} - {compartment}")
                    
                    try:
                        from models import TankerCalibration
                        with get_session() as s:
                            cal_data = s.query(TankerCalibration).filter(
                                TankerCalibration.tanker_name == selected_tanker,
                                TankerCalibration.compartment == compartment
                            ).order_by(TankerCalibration.dip_mm).all()
                        
                        if cal_data:
                            import pandas as pd
                            df_cal = pd.DataFrame([{
                                "Dip (mm)": c.dip_mm,
                                "Volume (Litres)": f"{c.volume_litres:,.2f}"
                            } for c in cal_data])
                            
                            st.dataframe(df_cal, use_container_width=True, hide_index=True)
                            st.caption(f"🔢 Total calibration points: {len(cal_data)}")
                            
                            # Export and Delete buttons
                            exp_col, del_col = st.columns([0.5, 0.5])
                            
                            with exp_col:
                                # Export existing calibration
                                csv_data = df_cal.to_csv(index=False).encode('utf-8')
                                st.download_button(
                                    "⬇️ Download Current Calibration",
                                    data=csv_data,
                                    file_name=f"{selected_tanker}_{compartment}_calibration.csv",
                                    mime="text/csv",
                                    key="download_tanker_cal"
                                )
                            
                            with del_col:
                                # Delete calibration
                                if st.button(f"🗑️ Delete Calibration", key="delete_tanker_cal"):
                                    try:
                                        from models import TankerCalibration
                                        with get_session() as s:
                                            deleted = s.query(TankerCalibration).filter(
                                                TankerCalibration.tanker_name == selected_tanker,
                                                TankerCalibration.compartment == compartment
                                            ).delete()
                                            s.commit()
                                        
                                        st.success(f"? Deleted {deleted} calibration points")
                                        
                                        # ----------------------- Audit log for tanker calibration deletion -----------------------
                                        try:
                                            from security import SecurityManager  # type: ignore
                                            user_ctx = st.session_state.get("auth_user") or {}
                                            username = user_ctx.get("username", "unknown")
                                            user_id = user_ctx.get("id")
                                            SecurityManager.log_audit(
                                                None,
                                                username,
                                                "DELETE",
                                                resource_type="TankerCalibration",
                                                resource_id=f"{selected_tanker}-{compartment}",
                                                details=f"Deleted {deleted} calibration points",
                                                user_id=user_id,
                                                location_id=active_location_id,
                                            )
                                        except Exception:
                                            pass
                                        
                                        import time
                                        time.sleep(1)
                                        _st_safe_rerun()
                                    except Exception as ex:
                                        st.error(f"? Failed to delete calibration: {ex}")
                        else:
                            st.info(f"ℹ️ No calibration data for {selected_tanker} - {compartment}. Upload CSV above.")
                    except Exception as ex:
                        st.error(f"? Failed to load calibration: {ex}")
            
            except Exception as ex:
                st.error(f"? Failed to load tankers: {ex}")
            
            st.markdown("---")
            
            # ========== SECTION 5: Delete Tanker ==========
            st.markdown("##### Delete Tanker (Admin)")
            st.warning("⚠️ Deleting a tanker affects ALL locations and removes all its calibration data!")
            
            try:
                from models import Tanker
                with get_session() as s:
                    tanker_list = s.query(Tanker).order_by(Tanker.name).all()
                
                if tanker_list:
                    td1, td2, td3 = st.columns([0.4, 0.4, 0.2])
                    with td1:
                        del_tanker_name = st.selectbox(
                            "Select Tanker to delete",
                            [t.name for t in tanker_list],
                            key="asset_del_tanker_select"
                        )
                    with td2:
                        tanker_confirm = st.text_input(
                            "Type the tanker name to confirm",
                            key="asset_del_tanker_confirm"
                        )
                    with td3:
                        if st.button("🗑️ Delete", key="asset_del_tanker_btn"):
                            if not del_tanker_name:
                                st.error("? Please select a tanker.")
                            elif tanker_confirm.strip() != del_tanker_name:
                                st.error("? Confirmation text does not match the selected tanker name.")
                            else:
                                try:
                                    with get_session() as s:
                                        obj = s.query(Tanker).filter(
                                            Tanker.name == del_tanker_name
                                        ).one_or_none()
                                        if not obj:
                                            st.warning("⚠️ Tanker not found (maybe already deleted).")
                                        else:
                                            _archive_record_for_delete(
                                                s,
                                                obj,
                                                "Tanker",
                                                reason=f"Marked tanker {del_tanker_name} for deletion.",
                                                label=del_tanker_name,
                                            )
                                            s.commit()
                                    st.success(f"? Deleted tanker: {del_tanker_name}")
                                    _st_safe_rerun()
                                except IntegrityError:
                                    st.error("? Cannot delete: this tanker is referenced by existing transactions.")
                                except Exception as ex:
                                    st.error(f"? Failed to delete tanker: {ex}")
                else:
                    st.info("ℹ️ No tankers to delete.")
            except Exception as ex:
                st.error(f"? Delete UI error (tankers): {ex}")
    
    # ============================== TAB 5: Vessel Assets ==============================
        with tab5:
            st.markdown("#### Vessel Assets")
            st.caption("Add or edit vessel master records and assign them to locations.")
    
            try:
                from models import Vessel, Location, LocationVessel
                with get_session() as s:
                    vessel_rows = s.query(Vessel).order_by(Vessel.name).all()
                    location_rows = s.query(Location).order_by(Location.name).all()
                    if vessel_rows:
                        assignment_rows = (
                            s.query(LocationVessel)
                            .filter(LocationVessel.is_active == True)
                            .filter(LocationVessel.vessel_id.in_([v.id for v in vessel_rows]))
                            .all()
                        )
                    else:
                        assignment_rows = []
            except Exception as ex:
                vessel_rows = []
                location_rows = []
                assignment_rows = []
                st.error(f"Failed to load vessel data: {ex}")
    
            vessel_lookup = {v.id: v for v in vessel_rows}
            location_labels = {
                loc.id: f"{loc.name} ({loc.code})" if loc.code else loc.name for loc in location_rows
            }
    
            vessel_select_items = ["? Add New Vessel"] + [f"{v.name} (#{v.id})" for v in vessel_rows]
            selected_label = st.selectbox(
                "Select Vessel to Edit",
                vessel_select_items,
                key="asset_vessel_selector",
            )
            selected_vessel_id = 0
            if selected_label != "? Add New Vessel":
                for vessel in vessel_rows:
                    if f"{vessel.name} (#{vessel.id})" == selected_label:
                        selected_vessel_id = vessel.id
                        break
    
            vessel_state_key = "asset_vessel_selected_id"
            if vessel_state_key not in st.session_state:
                st.session_state[vessel_state_key] = 0
            if st.session_state[vessel_state_key] != selected_vessel_id:
                st.session_state[vessel_state_key] = selected_vessel_id
                if selected_vessel_id and selected_vessel_id in vessel_lookup:
                    vessel_obj = vessel_lookup[selected_vessel_id]
                    st.session_state["asset_vessel_name"] = vessel_obj.name
                    st.session_state["asset_vessel_code"] = vessel_obj.registration_no or ""
                    st.session_state["asset_vessel_capacity"] = vessel_obj.capacity_bbl or 0.0
                    default_loc = None
                    for link in assignment_rows:
                        if link.vessel_id == selected_vessel_id and link.is_active:
                            default_loc = link.location_id
                            break
                    st.session_state["asset_vessel_location"] = default_loc
                else:
                    st.session_state["asset_vessel_name"] = ""
                    st.session_state["asset_vessel_code"] = ""
                    st.session_state["asset_vessel_capacity"] = 0.0
                    st.session_state["asset_vessel_location"] = active_location_id
    
            if st.session_state.pop("asset_vessel_reset_flag", False):
                st.session_state[vessel_state_key] = 0
                st.session_state["asset_vessel_name"] = ""
                st.session_state["asset_vessel_code"] = ""
                st.session_state["asset_vessel_capacity"] = 0.0
                st.session_state["asset_vessel_location"] = None
    
            with st.form("asset_vessel_form"):
                col_a, col_b = st.columns(2)
                with col_a:
                    vessel_name = st.text_input(
                        "Vessel Name *",
                        value=st.session_state.get("asset_vessel_name", ""),
                        key="asset_vessel_name",
                    )
                with col_b:
                    vessel_code = st.text_input(
                        "Vessel ID / Registration *",
                        value=st.session_state.get("asset_vessel_code", ""),
                        key="asset_vessel_code",
                    )
    
                col_c, col_d = st.columns(2)
                with col_c:
                    vessel_capacity = st.number_input(
                        "Capacity (bbl)",
                        min_value=0.0,
                        step=1.0,
                        value=float(st.session_state.get("asset_vessel_capacity", 0.0) or 0.0),
                        key="asset_vessel_capacity",
                    )
                location_options = [None] + [loc.id for loc in location_rows]
                loc_labels = {None: "-- Select Location --"}
                loc_labels.update(location_labels)
                with col_d:
                    assigned_location = st.selectbox(
                        "Assign to Location (optional)",
                        options=location_options,
                        index=(
                            location_options.index(st.session_state.get("asset_vessel_location"))
                            if st.session_state.get("asset_vessel_location") in location_options
                            else 0
                        ),
                        format_func=lambda opt: loc_labels.get(opt, "-- Select Location --"),
                        key="asset_vessel_location",
                    )
    
                action_cols = st.columns([0.3, 0.3, 0.4])
                save_vessel = action_cols[0].form_submit_button("Save Vessel", type="primary")
                reset_form = action_cols[1].form_submit_button("Reset", type="secondary")
    
                if reset_form:
                    st.session_state["asset_vessel_reset_flag"] = True
                    _st_safe_rerun()
    
                if save_vessel:
                    errors = []
                    clean_name = (vessel_name or "").strip()
                    clean_code = (vessel_code or "").strip()
                    if not clean_name:
                        errors.append("Vessel name is required.")
                    if not clean_code:
                        errors.append("Vessel ID / Registration is required.")
                    if errors:
                        for err in errors:
                            st.error(err)
                    else:
                        try:
                            with get_session() as s:
                                if selected_vessel_id:
                                    vessel_obj = (
                                        s.query(Vessel)
                                        .filter(Vessel.id == selected_vessel_id)
                                        .one_or_none()
                                    )
                                    if not vessel_obj:
                                        st.error("Selected vessel no longer exists.")
                                        st.stop()
                                    duplicate = (
                                        s.query(Vessel)
                                        .filter(Vessel.name == clean_name, Vessel.id != selected_vessel_id)
                                        .first()
                                    )
                                    if duplicate:
                                        st.error("Another vessel already uses that name.")
                                        st.stop()
                                else:
                                    duplicate = s.query(Vessel).filter(Vessel.name == clean_name).first()
                                    if duplicate:
                                        st.error("A vessel with that name already exists. Select it above to edit.")
                                        st.stop()
                                    vessel_obj = Vessel(name=clean_name)
                                    s.add(vessel_obj)
                                    s.flush()
                                    selected_vessel_id = vessel_obj.id
    
                                vessel_obj.name = clean_name
                                vessel_obj.registration_no = clean_code
                                vessel_obj.capacity_bbl = float(vessel_capacity or 0.0)
                                vessel_obj.status = "ACTIVE"
                                s.flush()
    
                                if assigned_location:
                                    existing_link = (
                                        s.query(LocationVessel)
                                        .filter(
                                            LocationVessel.location_id == assigned_location,
                                            LocationVessel.vessel_id == vessel_obj.id,
                                        )
                                        .one_or_none()
                                    )
                                    if existing_link:
                                        existing_link.is_active = True
                                    else:
                                        s.add(
                                            LocationVessel(
                                                location_id=assigned_location,
                                                vessel_id=vessel_obj.id,
                                                is_active=True,
                                            )
                                        )
    
                                s.commit()
                                st.success("Vessel saved successfully.")
                                _st_safe_rerun()
                        except IntegrityError as ex:
                            st.error(f"Database error: {ex}")
                        except Exception as ex:
                            st.error(f"Failed to save vessel: {ex}")
    
            if vessel_rows:
                st.markdown("##### Existing Vessels")
                assignment_map: Dict[int, List[str]] = {}
                for link in assignment_rows:
                    assignment_map.setdefault(link.vessel_id, []).append(
                        location_labels.get(link.location_id, f"Location #{link.location_id}")
                    )
                vessel_data = []
                for vessel in vessel_rows:
                    vessel_data.append(
                        {
                            "Vessel": vessel.name,
                            "Vessel ID": vessel.registration_no or "-",
                            "Capacity (bbl)": vessel.capacity_bbl or "-",
                            "Assigned Locations": ", ".join(assignment_map.get(vessel.id, [])) or "-",
                        }
                    )
                st.dataframe(
                    pd.DataFrame(vessel_data),
                    use_container_width=True,
                    hide_index=True,
                )
                vessel_display_map = {v.id: f"{v.name} (#{v.id})" for v in vessel_rows}
                location_option_ids = [None] + [loc.id for loc in location_rows]
                formatted_locations = {None: "-- No Location --"}
                formatted_locations.update(location_labels)
    
                st.markdown("##### Transfer Existing Vessel")
                with st.form("asset_vessel_transfer_form"):
                    transfer_vessel_id = st.selectbox(
                        "Select vessel",
                        options=list(vessel_display_map.keys()),
                        format_func=lambda vid: vessel_display_map.get(vid, f"Vessel #{vid}"),
                    )
                    transfer_location_id = st.selectbox(
                        "Assign to Location",
                        options=location_option_ids,
                        format_func=lambda opt: formatted_locations.get(opt, "-- No Location --"),
                    )
                    transfer_submit = st.form_submit_button("Update Assignment", type="primary")
                    if transfer_submit:
                        try:
                            with get_session() as s:
                                vessel_obj = (
                                    s.query(Vessel)
                                    .filter(Vessel.id == transfer_vessel_id)
                                    .one_or_none()
                                )
                                if not vessel_obj:
                                    st.error("Selected vessel could not be found.")
                                else:
                                    links = (
                                        s.query(LocationVessel)
                                        .filter(LocationVessel.vessel_id == vessel_obj.id)
                                        .all()
                                    )
                                    row_map = {row.location_id: row for row in links}
                                    # deactivate all existing links
                                    for row in links:
                                        row.is_active = False
                                    # activate or create selected assignment
                                    if transfer_location_id:
                                        existing = row_map.get(transfer_location_id)
                                        if existing:
                                            existing.is_active = True
                                        else:
                                            s.add(
                                                LocationVessel(
                                                    location_id=transfer_location_id,
                                                    vessel_id=vessel_obj.id,
                                                    is_active=True,
                                                )
                                            )
                                    s.commit()
                                    st.success("Vessel assignment updated.")
                                    import time as _t
    
                                    _t.sleep(1)
                                    _st_safe_rerun()
                        except Exception as ex:
                            st.error(f"Failed to update assignment: {ex}")
    
                st.markdown("##### Delete Vessel")
                with st.form("asset_vessel_delete_form"):
                    delete_vessel_id = st.selectbox(
                        "Select vessel to delete",
                        options=list(vessel_display_map.keys()),
                        format_func=lambda vid: vessel_display_map.get(vid, f"Vessel #{vid}"),
                    )
                    confirm_text = st.text_input(
                        "Type DELETE to confirm removal",
                        value="",
                    )
                    delete_submit = st.form_submit_button("Delete Vessel", type="secondary")
                    if delete_submit:
                        if confirm_text.strip().upper() != "DELETE":
                            st.error("Please type DELETE to confirm.")
                        else:
                            try:
                                with get_session() as s:
                                    vessel_obj = (
                                        s.query(Vessel)
                                        .filter(Vessel.id == delete_vessel_id)
                                        .one_or_none()
                                    )
                                    if not vessel_obj:
                                        st.error("Selected vessel does not exist.")
                                    else:
                                        s.query(LocationVessel).filter(
                                            LocationVessel.vessel_id == vessel_obj.id
                                        ).delete(synchronize_session=False)
                                        s.delete(vessel_obj)
                                        s.commit()
                                        st.success("Vessel deleted. Please re-add with updated details if needed.")
                                        import time as _t
    
                                        _t.sleep(1)
                                        _st_safe_rerun()
                            except IntegrityError as ex:
                                st.error(
                                    "Unable to delete vessel because it is referenced by existing records. "
                                    "Please remove related transactions first."
                                )
                            except Exception as ex:
                                st.error(f"Failed to delete vessel: {ex}")
            else:
                st.info("No vessels available yet. Add a vessel using the form above.")
    
    # ================== NEW PAGE: Location Settings (admin only) ==================