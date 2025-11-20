"""
Auto-generated module for the 'TOA-Yade' page.
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
        import os, base64
        from io import BytesIO
        from pathlib import Path
        from datetime import datetime as _dt
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.lib import colors
        from reportlab.pdfgen import canvas
        from reportlab.lib.utils import ImageReader
    
        header("TOA � YADE")
        try:
            _user_role = st.session_state.get("auth_user", {}).get("role")
            _loc_id = st.session_state.get("active_location_id")
            if _user_role not in ["admin-operations", "manager"] and _loc_id:
                from location_config import LocationConfig
                with get_session() as _s:
                    _cfg = LocationConfig.get_config(_s, _loc_id)
                if _cfg.get("page_access", {}).get("TOA-Yade") is False:
                    st.error("🚫 TOA-Yade page is disabled for this location.")
                    st.stop()
        except Exception:
            pass
        
        # ============ LOCATION ACCESS CHECK ============
        active_location_id = st.session_state.get("active_location_id")
        if not active_location_id:
            st.error("⚠️ No active location selected.")
            st.stop()
        
        user = st.session_state.get("auth_user")
        if user:
            from auth import AuthManager
            if not AuthManager.can_access_location(user, active_location_id):
                st.error("🚫 You do not have access to this location.")
                st.stop()
        
        # Apply location-based page visibility: hide page if disabled in config (non-admin)
        try:
            if user:
                with get_session() as _s_cfg:
                    from location_config import LocationConfig
                    _cfg = LocationConfig.get_config(_s_cfg, active_location_id)
                # Use show_toa_yade flag to determine visibility
                if not _cfg.get("page_visibility", {}).get("show_toa_yade", False) and (user.get("role", "").lower() not in ["admin-operations", "manager"]):
                    st.error("🚫 TOA-Yade page is disabled for this location.")
                    st.stop()
        except Exception:
            pass
    
        # ========== CHECK PERMISSIONS (FOLLOWS YADE PERMISSIONS) ==========
        from permission_manager import PermissionManager
        
        with get_session() as s:
            from location_manager import LocationManager
            
            # Get location info
            loc = LocationManager.get_location_by_id(s, active_location_id)
            if not loc:
                st.error("? Location not found.")
                st.stop()
            
            st.info(f"📍 **Active Location:** {loc.name} ({loc.code})")
            
            # TOA-Yade uses YADE permissions (Admin can access everywhere)
            if not PermissionManager.can_access_feature(s, active_location_id, "yade_transactions", user["role"]):
                st.error("🚫 **Access Denied**")
                st.warning(f"**TOA-Yade** is not available at **{loc.name}**")
                st.info("🚫 TOA-Yade reports require YADE Transactions permission")
                
                # Show where it's available
                allowed_locs = PermissionManager.get_allowed_locations_for_feature(s, "yade_transactions")
                if allowed_locs:
                    st.info(f"? This feature is available at: **{', '.join(allowed_locs)}**")
                
                st.markdown("---")
                st.caption(f"Current Location: **{loc.name} ({loc.code})**")
                st.caption("TOA-Yade Access: **? Denied** (YADE permission required)")
                st.stop()
        
        # ============ TOA-YADE ENABLED ============
        st.success(f"? TOA-Yade enabled at {loc.name}")
    
        # ---------- small helpers ----------
        def _kind_text(x):
            try:
                return x.value if hasattr(x, "value") else (str(x) if x is not None else "")
            except Exception:
                return str(x or "")
    
        def _fmt_date(d):
            try:    return d.strftime("%d/%m/%Y")
            except: return str(d or "")
    
        def _fmt_time(t):
            try:    return t.strftime("%H:%M")
            except: return str(t or "")
    
        # ---------- PDF builder ----------
        def _pdf_support_ok():
            try:
                import reportlab
                return True
            except ImportError:
                return False
    
        def _mk_pdf_bytes(sess, voyage_id: int) -> bytes | None:
            """Builds the TOA PDF"""
            if not _pdf_support_ok():
                return None
    
            try:
                from reportlab.lib.pagesizes import A4
                from reportlab.lib.units import mm
                from reportlab.lib import colors
                from reportlab.pdfgen import canvas
                from reportlab.lib.utils import ImageReader
                from io import BytesIO
                from pathlib import Path
    
                v   = sess.query(YadeVoyage).filter(YadeVoyage.id == voyage_id).one()
                stB = sess.query(TOAYadeStage).filter(TOAYadeStage.voyage_id==voyage_id, TOAYadeStage.stage=="before").one_or_none()
                stA = sess.query(TOAYadeStage).filter(TOAYadeStage.voyage_id==voyage_id, TOAYadeStage.stage=="after").one_or_none()
                sm  = sess.query(TOAYadeSummary).filter(TOAYadeSummary.voyage_id==voyage_id).one_or_none()
    
                def g(o, k): return float(getattr(o, k) or 0.0) if o else 0.0
    
                buf = BytesIO()
                c   = canvas.Canvas(buf, pagesize=A4)
                W, H = A4
                LM, RM, TM, BM = 15*mm, 15*mm, 15*mm, 15*mm
                x = LM
                y = H - TM
    
                # ---- Section 1: Header bar + logos + title ----
                bar_h = 22*mm
                c.setFillColorRGB(0.98, 0.98, 0.98)
                c.setStrokeColorRGB(0.85, 0.85, 0.85)
                c.roundRect(x, y - bar_h, W - LM - RM, bar_h, 4, fill=1, stroke=1)
                c.setFillColor(colors.black)
                c.setStrokeColor(colors.black)
    
                Lx, Ly, Lw, Lh = x + 6*mm, y - bar_h + 3*mm, 26*mm, bar_h - 6*mm
                Rx, Ry, Rw, Rh = x + (W - LM - RM) - 26*mm - 6*mm, Ly, 26*mm, Lh
    
                def _draw_img_or_box(path: Path | None, x0, y0, w0, h0, label):
                    try:
                        if path and path.exists():
                            img = ImageReader(str(path))
                            iw, ih = img.getSize()
                            if iw > 0 and ih > 0:
                                scale = min(w0/iw, h0/ih)
                                dw, dh = iw*scale, ih*scale
                                ox = x0 + (w0 - dw)/2.0
                                oy = y0 + (h0 - dh)/2.0
                                c.drawImage(img, ox, oy, dw, dh, preserveAspectRatio=True, mask='auto')
                                return
                    except Exception:
                        pass
                    c.setStrokeColor(colors.black)
                    c.rect(x0, y0, w0, h0, stroke=1, fill=0)
                    c.setFont("Helvetica", 7)
                    c.drawCentredString(x0 + w0/2, y0 - 10, label)
    
                def _first_existing(paths):
                    for p in paths:
                        pth = Path(p)
                        if pth.exists():
                            return pth
                    return None
    
                COMPANY_LOGO = _first_existing([
                    "assets/logos/company_logo.png",
                    "assets/icons/company_logo.png",
                    "assets/company_logo.png",
                ])
                YADE_LOGO = _first_existing([
                    "assets/logos/yade_logo.png",
                    "assets/icons/yade_logo.png",
                    "assets/yade_logo.png",
                ])
    
                _draw_img_or_box(COMPANY_LOGO, Lx, Ly, Lw, Lh, "Company Logo")
                _draw_img_or_box(YADE_LOGO,    Rx, Ry, Rw, Rh, "YADE Logo")
    
                c.setFont("Helvetica-Bold", 13)
                c.drawCentredString(LM + (W - LM - RM)/2, y - 7*mm, "TRANSHIPMENT ORDER & ADVICE")
                c.setFont("Helvetica", 9)
                c.drawCentredString(LM + (W - LM - RM)/2, y - 13*mm, f"Report for {v.yade_name} � Voyage {v.voyage_no}")
                y -= (bar_h + 6*mm)
    
                # ---- Section 2: metadata box ----
                meta_h = 34*mm
                c.setStrokeColor(colors.black)
                c.rect(LM, y - meta_h, W - LM - RM, meta_h, stroke=1, fill=0)
                c.setFont("Helvetica-Bold", 10)
                c.setFillColor(colors.black)
                c.drawString(LM + 4*mm, y - 7*mm, "Voyage Details")
                c.setFont("Helvetica", 9)
                mrow = y - 14*mm
                meta_items = [
                    ("Date",          _fmt_date(v.date)),
                    ("Time",          _fmt_time(v.time)),
                    ("YADE No",       v.yade_name),
                    ("Voyage No",     v.voyage_no),
                    ("Convoy No",     v.convoy_no),
                    ("Cargo",         _kind_text(v.cargo)),
                    ("Destination",   _kind_text(v.destination)),
                    ("Loading Berth", _kind_text(v.loading_berth)),
                ]
                left = meta_items[:4]
                right = meta_items[4:]
                xL = LM + 6*mm
                xR = LM + (W - LM - RM)/2 + 6*mm
                for (k,vv) in left:  c.drawString(xL, mrow, f"{k}:  {vv}"); mrow -= 6*mm
                mrow = y - 14*mm
                for (k,vv) in right: c.drawString(xR, mrow, f"{k}:  {vv}"); mrow -= 6*mm
                y -= (meta_h + 10*mm)
    
                # ---- Section 3: quantities table ----
                B_GOV = g(stB, "gov_bbl"); B_GSV = g(stB, "gsv_bbl"); B_FW = g(stB, "fw_bbl")
                A_GOV = g(stA, "gov_bbl"); A_GSV = g(stA, "gsv_bbl"); A_FW = g(stA, "fw_bbl")
                B_NSV = g(stB, "nsv_bbl"); A_NSV = g(stA, "nsv_bbl")
                B_LTF = g(stB, "lt");      A_LTF = g(stA, "lt")
    
                L_GOV = (A_GOV or 0.0) - (B_GOV or 0.0)
                L_GSV = (A_GSV or 0.0) - (B_GSV or 0.0)
                L_FW  = (A_FW  or 0.0) - (B_FW  or 0.0)
                L_NSV = (A_NSV or 0.0) - (B_NSV or 0.0)
    
                B_LT_TONS = (B_NSV or 0.0) * (B_LTF or 0.0)
                A_LT_TONS = (A_NSV or 0.0) * (A_LTF or 0.0)
                L_LT_TONS = A_LT_TONS - B_LT_TONS
    
                B_MT = B_LT_TONS * 1.01605
                A_MT = A_LT_TONS * 1.01605
                L_MT = A_MT - B_MT
    
                rows = [
                    ("Total Volume (bbl)", (B_GOV or 0.0) + (B_FW or 0.0), (A_GOV or 0.0) + (A_FW or 0.0), (L_GOV or 0.0) + (L_FW or 0.0)),
                    ("Free Water (bbl)",   B_FW,  A_FW,  L_FW),
                    ("GOV (bbl)",          B_GOV, A_GOV, L_GOV),
                    ("GSV (bbl)",          B_GSV, A_GSV, L_GSV),
                    ("NSV (bbl)",          B_NSV, A_NSV, L_NSV),
                    ("Long Tons (LT)",     B_LT_TONS, A_LT_TONS, L_LT_TONS),
                    ("MT",                 B_MT,  A_MT,  L_MT),
                ]
    
                table_h = 7 * 18 + 28
                col_w   = (W - LM - RM) / 4.0
                x_before = LM + 1.2*col_w - 6
                x_after  = LM + 2.3*col_w - 6
                x_loaded = LM + 3.5*col_w - 6
    
                c.setFont("Helvetica-Bold", 10)
                c.drawString(LM + 4*mm, y - 7*mm, "Certified Quantity loaded in the Barge")
                y -= 10*mm
    
                c.setFont("Helvetica-Bold", 9)
                c.drawString(LM + 6, y - 12, "Quantity")
                c.drawRightString(x_before, y - 12, "Before")
                c.drawRightString(x_after,  y - 12, "After")
                c.drawRightString(x_loaded, y - 12, "Loaded")
    
                c.setStrokeColor(colors.black)
                c.rect(LM, y - (table_h + 18), (W - LM - RM), (table_h + 18), stroke=1, fill=0)
    
                c.setFont("Helvetica", 9)
                ry = y - 28
                def _fmt(v): 
                    try: return f"{(v or 0):,.2f}"
                    except: return "0.00"
                for name, vb, va, vl in rows:
                    c.line(LM, ry + 6, W - RM, ry + 6)
                    c.drawString(LM + 6, ry - 6, str(name))
                    c.drawRightString(x_before, ry - 6, "" if vb is None else _fmt(vb))
                    c.drawRightString(x_after,  ry - 6, "" if va is None else _fmt(va))
                    c.drawRightString(x_loaded, ry - 6, "" if vl is None else _fmt(vl))
                    ry -= 18
                y -= (table_h + 30)
    
                # ---- Section 4: Seal details and dips ----
                seal_h = 32*mm
                c.setFont("Helvetica-Bold", 10)
                c.setFillColor(colors.black)
                c.drawString(LM + 4*mm, y - 5*mm, "Seal & Dip Details")
    
                seals = sess.query(YadeSealDetail).filter(YadeSealDetail.voyage_id == voyage_id).one_or_none()
                dips_after = sess.query(YadeDip).filter(YadeDip.voyage_id == voyage_id, YadeDip.stage == "after").all()
                dips_map = {d.tank_id: d for d in dips_after}
    
                tanks = ["C1","C2","P1","P2","S1","S2"] if str(v.design) == "6" else ["P1","P2","S1","S2"]
    
                row_height = 8*mm
                col_widths = [20*mm, 24*mm, 24*mm, 28*mm, 28*mm, 28*mm, 28*mm]
                table_left_x = LM 
                table_top_y = y - 7*mm
                table_width = sum(col_widths)
                table_height = (len(tanks) + 1) * row_height
    
                c.setLineWidth(1)
                c.rect(table_left_x, table_top_y - table_height, table_width, table_height, stroke=1, fill=0)
    
                # Vertical lines
                current_x = table_left_x
                for w in col_widths:
                    c.line(current_x, table_top_y, current_x, table_top_y - table_height)
                    current_x += w
                c.line(table_left_x + table_width, table_top_y, table_left_x + table_width, table_top_y - table_height)
    
                # Horizontal lines
                for i in range(len(tanks) + 2):
                    row_y = table_top_y - i * row_height
                    c.line(table_left_x, row_y, table_left_x + table_width, row_y)
    
                # Header
                hdrs = ["Tank", "Total Dip (cm)", "Water Dip (cm)", "Manhole-1", "Manhole-2", "Lock No", "Dip Hatch"]
                c.setFont("Helvetica-Bold", 9)
                header_y = table_top_y - row_height/2 + 3
                current_x = table_left_x
                for i, h in enumerate(hdrs):
                    c.drawCentredString(current_x + col_widths[i]/2, header_y, h)
                    current_x += col_widths[i]
    
                # Data rows
                c.setFont("Helvetica", 8)
                for r, t in enumerate(tanks):
                    k = t.upper()
                    dip_row = dips_map.get(k)
                    total_dip = f"{dip_row.total_cm:.2f}" if dip_row else ""
                    water_dip = f"{dip_row.water_cm:.2f}" if dip_row else ""
    
                    seals_k = t.lower()
                    mh1 = getattr(seals, f"{seals_k}_mh1", "") if seals else ""
                    mh2 = getattr(seals, f"{seals_k}_mh2", "") if seals else ""
                    lk  = getattr(seals, f"{seals_k}_lock", "") if seals else ""
                    dh  = getattr(seals, f"{seals_k}_diphatch", "") if seals else ""
    
                    vals = [t, total_dip, water_dip, mh1, mh2, lk, dh]
                    row_y = table_top_y - (r + 1) * row_height - row_height/2 + 3
                    current_x = table_left_x
                    for i, vv in enumerate(vals):
                        c.drawCentredString(current_x + col_widths[i]/2, row_y, str(vv or ""))
                        current_x += col_widths[i]
    
                y -= (table_height + 12*mm)
    
                # ---- Section 5: Authorized Signatory ----
                sig_h = 40*mm
                sig_y = BM + 5*mm
                c.rect(LM, sig_y, W - LM - RM, sig_h, stroke=1, fill=0)
                c.setFont("Helvetica-Bold", 10)
                c.drawString(LM + 4*mm, sig_y + sig_h - 6*mm, "Authorized Signatory")
                c.setFont("Helvetica", 9)
                c.drawString(LM + 4*mm, sig_y + 6*mm,  "For SEEPCO                                                         For Yade Barge Operators Ltd")
                c.drawRightString(W - RM - 4*mm, sig_y + 6*mm, f"For Barge Master of {v.yade_name}")
    
                c.showPage()
                c.save()
                out = buf.getvalue()
                buf.close()
                return out
    
            except Exception as e:
                import traceback
                st.error(f"PDF generation error: {e}")
                st.write("Traceback:", traceback.format_exc())
                return None
    
        # ---------- Listing UI ----------
        try:
            from models import TOAYadeSummary
            import pandas as pd
    
            with get_session() as s:
                rows = (
                    s.query(TOAYadeSummary)
                     .join(YadeVoyage, TOAYadeSummary.voyage_id == YadeVoyage.id)
                     .filter(YadeVoyage.location_id == active_location_id)
                     .order_by(TOAYadeSummary.date.desc(), TOAYadeSummary.time.desc())
                     .limit(500)
                     .all()
                )
    
            rows = [r for r in rows if (r and r.yade_name and r.date and r.time)]
    
            if not rows:
                st.info("🚫 No TOA/YADE summaries yet. Save a YADE voyage first.")
            else:
                df = pd.DataFrame([{
                    "Voyage ID":          r.voyage_id,
                    "YADE No":            r.yade_name or "",
                    "Convoy No":          r.convoy_no or "",
                    "Date":               r.date,
                    "Time":               r.time,
                    "Destination":        _kind_text(r.destination),
                    "Loading Berth":      _kind_text(r.loading_berth),
                    "Before GSV (bbl)":   float(r.gsv_before_bbl or 0.0),
                    "After GSV (bbl)":    float(r.gsv_after_bbl  or 0.0),
                    "Loaded GSV (bbl)":   float(r.gsv_loaded_bbl or 0.0),
                } for r in rows])
    
                st.caption(f"📊 {len(df)} TOA/YADE reports available")
    
                headers = [
                    "YADE No","Convoy No","Date","Time",
                    "Destination","Loading Berth",
                    "Before GSV (bbl)","After GSV (bbl)","Loaded GSV (bbl)","Create TOA"
                ]
                widths  = [0.10, 0.10, 0.08, 0.07, 0.14, 0.14, 0.10, 0.10, 0.10, 0.07]
                hcols = st.columns(widths)
                for c,h in zip(hcols, headers):
                    c.markdown(f"**{h}**")
    
                import streamlit.components.v1 as components
                for _, r in df.iterrows():
                    cols = st.columns(widths)
                    cols[0].write(r["YADE No"])
                    cols[1].write(r["Convoy No"])
                    cols[2].write(_fmt_date(r["Date"]))
                    cols[3].write(_fmt_time(r["Time"]))
                    cols[4].write(r["Destination"])
                    cols[5].write(r["Loading Berth"])
                    cols[6].write(f'{r["Before GSV (bbl)"]:.2f}')
                    cols[7].write(f'{r["After GSV (bbl)"]:.2f}')
                    cols[8].write(f'{r["Loaded GSV (bbl)"]:.2f}')
    
                    vid = int(r["Voyage ID"])
                    if cols[9].button("Create TOA", key=f"toa_btn_{vid}"):
                        with get_session() as s:
                            pdf_bytes = _mk_pdf_bytes(s, vid)
    
                        if not pdf_bytes:
                            st.error("? PDF export failed. Please ensure reportlab is installed.")
                        else:
                            b64 = base64.b64encode(pdf_bytes).decode("utf-8")
                            components.html(
                                f"""
                                <script>
                                (function(){{
                                  const b64="{b64}";
                                  const byteChars=atob(b64);
                                  const byteNums=new Array(byteChars.length);
                                  for (let i=0;i<byteChars.length;i++) byteNums[i]=byteChars.charCodeAt(i);
                                  const blob=new Blob([new Uint8Array(byteNums)],{{type:'application/pdf'}});
                                  const url=URL.createObjectURL(blob);
                                  window.open(url,'_blank');
                                  setTimeout(()=>URL.revokeObjectURL(url),60000);
                                }})();
                                </script>
                                """,
                                height=0
                            )
                            st.success("? TOA PDF opened in new tab!")
            
            # Show ReportLab status
            try:
                import reportlab
                st.caption("? PDF export available (ReportLab installed)")
            except Exception:
                st.warning("⚠️ To export PDFs, install: `pip install reportlab`")
    
        except Exception as ex:
            st.error(f"❌ Failed to load TOA-Yade: {ex}")
            log_error(f"Failed to load TOA-Yade: {ex}", exc_info=True)
            import traceback
            with st.expander("⚠️ Error Details"):
                st.code(traceback.format_exc())
    
    # ========================= VIEW TRANSACTIONS =========================