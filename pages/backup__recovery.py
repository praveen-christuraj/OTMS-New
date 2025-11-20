"""
Auto-generated module for the 'Backup & Recovery' page.
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
        if st.session_state.get("auth_user", {}).get("role") not in ["admin-operations", "admin-it"]:
            header("Backup & Recovery")
            st.error("You do not have permission to access this page. Admin-Operations or Admin-IT only.")
            st.stop()
    
        header("Backup & Recovery")
        st.markdown("### Database Backup & Recovery Management")
        
        from backup_manager import BackupManager
        from datetime import datetime
        import pandas as pd
        
        tab1, tab2, tab3 = st.tabs(["🗄️ Backups", "🗄️ Create Backup", "📤 Export Location"])
        
        # ========== TAB 1: Manage Backups ==========
        with tab1:
            st.markdown("#### Available Backups")
            
            try:
                backups = BackupManager.list_backups()
                
                if backups:
                    # Display backups table
                    df = pd.DataFrame([{
                        "Timestamp": datetime.fromisoformat(b["datetime"]).strftime("%Y-%m-%d %H:%M:%S"),
                        "Type": b["type"].title(),
                        "Description": b["description"],
                        "Size (MB)": b["size_mb"],
                        "File": b["filename"]
                    } for b in backups])
                    
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    st.caption(f"Total backups: {len(backups)}")
                    
                    # -------- Restore Section --------
                    st.markdown("---")
                    st.markdown("#### Restore from Backup")
                    
                    col1, col2 = st.columns([0.6, 0.4])
                    
                    with col1:
                        backup_options = {
                            f"{datetime.fromisoformat(b['datetime']).strftime('%Y-%m-%d %H:%M:%S')} - {b['description']} ({b['size_mb']} MB)": b["timestamp"]
                            for b in backups
                        }
                        
                        selected_backup = st.selectbox(
                            "Select backup to restore",
                            options=list(backup_options.keys()),
                            key="restore_backup_select"
                        )
                        
                        if selected_backup:
                            backup_ts = backup_options[selected_backup]
                            
                            # Show backup details
                            backup_info = BackupManager.get_backup_info(backup_ts)
                            if backup_info and backup_info.get("tables"):
                                st.markdown("##### Backup Contents:")
                                tables_data = []
                                for table, count in backup_info["tables"].items():
                                    tables_data.append({"Table": table.title(), "Records": count})
                                st.table(pd.DataFrame(tables_data))
                            
                            st.warning("⚠️ **WARNING:** Restoring will replace your current database!")
                            st.caption("A backup of the current database will be created before restoration.")
                            
                            confirm_restore = st.text_input(
                                'Type "RESTORE" to confirm',
                                key="restore_confirm"
                            )
                            
                            if st.button("♻️ Restore Database", key="restore_btn", type="primary"):
                                if confirm_restore == "RESTORE":
                                    try:
                                        result = BackupManager.restore_backup(backup_ts, create_backup_before=True)
                                        
                                        st.success(f"? Database restored successfully from {backup_ts}")
                                        if result.get("pre_restore_backup"):
                                            st.info(f"Pre-restore backup created: {result['pre_restore_backup']['filename']}")
                                        
                                        st.warning("⚠️ Please restart the application for changes to take effect.")
                                        
                                        # Log audit
                                        user = st.session_state.get("auth_user")
                                        if user:
                                            from security import SecurityManager
                                            with get_session() as s:
                                                SecurityManager.log_audit(
                                                    s, user["username"], "RESTORE_BACKUP",
                                                    details=f"Restored from {backup_ts}",
                                                    user_id=user["id"]
                                                )
                                        
                                    except Exception as ex:
                                        st.error(f"Restore failed: {ex}")
                                else:
                                    st.error('Please type "RESTORE" to confirm.')
                    
                    with col2:
                        st.markdown("##### Delete Backup")
        
                        delete_backup = st.selectbox(
                            "Select backup to delete",
                            options=list(backup_options.keys()),
                            key="delete_backup_select"
                        )
                        
                        if delete_backup:
                            backup_ts = backup_options[delete_backup]
                            
                            # Show backup details for confirmation
                            st.caption(f"Selected: {delete_backup}")
                            
                            # Two-step confirmation
                            if "delete_backup_step" not in st.session_state:
                                st.session_state.delete_backup_step = 0
                            if "delete_backup_pending" not in st.session_state:
                                st.session_state.delete_backup_pending = None
                            
                            # Step 1: Initial delete button
                            if st.session_state.delete_backup_step == 0:
                                if st.button("🗑️ Delete Backup", key="delete_backup_btn_step1"):
                                    st.session_state.delete_backup_step = 1
                                    st.session_state.delete_backup_pending = backup_ts
                                    _st_safe_rerun()
                            
                            # Step 2: Confirmation with typed verification
                            elif st.session_state.delete_backup_step == 1:
                                st.warning("⚠️ **Are you sure you want to delete this backup?**")
                                st.caption("This action cannot be undone!")
                                
                                # Extract just the timestamp for confirmation
                                confirm_text = backup_ts  # e.g., "20250106_143022"
                                
                                st.info(f"Type **{confirm_text}** to confirm deletion")
                                
                                user_input = st.text_input(
                                    "Confirmation",
                                    key="delete_backup_confirm_input",
                                    placeholder=confirm_text
                                )
                                
                                col_confirm, col_cancel = st.columns(2)
                                
                                with col_confirm:
                                    if st.button("? Confirm Delete", key="delete_backup_btn_step2", type="primary"):
                                        if user_input.strip() == confirm_text:
                                            try:
                                                BackupManager.delete_backup(st.session_state.delete_backup_pending)
                                                
                                                # Log audit
                                                user = st.session_state.get("auth_user")
                                                if user:
                                                    from security import SecurityManager
                                                    with get_session() as s:
                                                        SecurityManager.log_audit(
                                                            s, user["username"], "DELETE_BACKUP",
                                                            details=f"Deleted backup {st.session_state.delete_backup_pending}",
                                                            user_id=user["id"]
                                                        )
                                                
                                                st.success(f"? Backup deleted: {st.session_state.delete_backup_pending}")
                                                
                                                # Reset state
                                                st.session_state.delete_backup_step = 0
                                                st.session_state.delete_backup_pending = None
                                                
                                                import time
                                                time.sleep(1)
                                                _st_safe_rerun()
                                                
                                            except Exception as ex:
                                                st.error(f"Failed to delete: {ex}")
                                        else:
                                            st.error("? Confirmation text does not match. Please try again.")
                                
                                with col_cancel:
                                    if st.button("❌ Cancel", key="delete_backup_cancel"):
                                        st.session_state.delete_backup_step = 0
                                        st.session_state.delete_backup_pending = None
                                        st.info("Deletion cancelled.")
                                        _st_safe_rerun()
                    
                    # -------- Cleanup Old Backups --------
                    st.markdown("---")
                    st.markdown("#### Cleanup Old Backups")
                    
                    cleanup_col1, cleanup_col2 = st.columns(2)
                    
                    with cleanup_col1:
                        days = st.number_input("Delete backups older than (days)", min_value=1, value=30, step=1, key="cleanup_days")
                        keep_min = st.number_input("Always keep minimum", min_value=1, value=5, step=1, key="cleanup_keep_min")
                    
                    with cleanup_col2:
                        if st.button("🧹 Run Cleanup", key="cleanup_btn"):
                            try:
                                result = BackupManager.cleanup_old_backups(days=days, keep_minimum=keep_min)
                                st.success(f"? Cleanup complete! Deleted: {result['deleted']}, Kept: {result['kept']}")
                                if result['deleted'] > 0:
                                    _st_safe_rerun()
                            except Exception as ex:
                                st.error(f"Cleanup failed: {ex}")
                else:
                    st.info("No backups found. Create your first backup in the 'Create Backup' tab.")
            
            except Exception as ex:
                st.error(f"Failed to load backups: {ex}")
        
        # ========== TAB 2: Create Backup ==========
        with tab2:
            st.markdown("#### Create Manual Backup")
            
            with st.form("create_backup_form"):
                description = st.text_input(
                    "Backup Description",
                    placeholder="e.g., Before data migration, End of month backup, etc.",
                    key="backup_description"
                )
                
                submitted = st.form_submit_button("🗄️ Create Backup Now", type="primary")
                
                if submitted:
                    try:
                        backup_info = BackupManager.create_backup(
                            description=description or "Manual backup",
                            backup_type="manual"
                        )
                        
                        st.success(f"? Backup created successfully!")
                        
                        col1, col2 = st.columns(2)
                        col1.metric("Filename", backup_info['filename'])
                        col2.metric("Size", f"{backup_info['size_mb']} MB")
                        
                        st.info(f"📁 Location: backups/{backup_info['filename']}")
                        
                        # Log audit
                        user = st.session_state.get("auth_user")
                        if user:
                            from security import SecurityManager
                            with get_session() as s:
                                SecurityManager.log_audit(
                                    s, user["username"], "CREATE_BACKUP",
                                    details=description or "Manual backup",
                                    user_id=user["id"]
                                )
                        
                        import time
                        time.sleep(2)
                        _st_safe_rerun()
                        
                    except Exception as ex:
                        st.error(f"Backup creation failed: {ex}")
            
            st.markdown("---")
            st.markdown("#### Current Database Statistics")
            
            try:
                from models import (
                    Location, User, Tank, YadeBarge, 
                    TankTransaction, YadeVoyage, OTRRecord
                )
                
                with get_session() as s:
                    stats = {
                        "Locations": s.query(Location).count(),
                        "Users": s.query(User).count(),
                        "Tanks": s.query(Tank).count(),
                        "YADE Barges": s.query(YadeBarge).count(),
                        "Tank Transactions": s.query(TankTransaction).count(),
                        "YADE Voyages": s.query(YadeVoyage).count(),
                        "OTR Records": s.query(OTRRecord).count(),
                    }
                
                col1, col2, col3, col4 = st.columns(4)
                cols = [col1, col2, col3, col4]
                
                for i, (key, value) in enumerate(stats.items()):
                    cols[i % 4].metric(key, f"{value:,}")
            
            except Exception as ex:
                st.error(f"Failed to load stats: {ex}")
        
        # ========== TAB 3: Export Location ==========
        with tab3:
            st.markdown("#### Export Location Data")
            st.caption("Export all data for a specific location to a ZIP file")
            
            # Get locations
            try:
                from location_manager import LocationManager
                with get_session() as s:
                    locations = LocationManager.get_all_locations(s, active_only=False)
                
                if locations:
                    loc_options = {f"{loc.name} ({loc.code})": loc.id for loc in locations}
                    
                    selected_loc = st.selectbox(
                        "Select Location to Export",
                        options=list(loc_options.keys()),
                        key="export_location_select"
                    )
                    
                    if st.button("📤 Export Location Data", key="export_location_btn", type="primary"):
                        if selected_loc:
                            loc_id = loc_options[selected_loc]
                            
                            with st.spinner("Exporting location data..."):
                                try:
                                    export_path = BackupManager.export_location_data(loc_id)
                                    
                                    st.success(f"? Location data exported successfully!")
                                    st.info(f"📄 File: {export_path.name}")
                                    
                                    # Provide download button
                                    with open(export_path, 'rb') as f:
                                        st.download_button(
                                            "⬇️ Download Export",
                                            data=f.read(),
                                            file_name=export_path.name,
                                            mime="application/zip"
                                        )
                                    
                                    # Log audit
                                    user = st.session_state.get("auth_user")
                                    if user:
                                        from security import SecurityManager
                                        with get_session() as s:
                                            SecurityManager.log_audit(
                                                s, user["username"], "EXPORT_LOCATION",
                                                resource_type="Location",
                                                resource_id=str(loc_id),
                                                details=f"Exported {selected_loc}",
                                                user_id=user["id"]
                                            )
                                    
                                except Exception as ex:
                                    st.error(f"Export failed: {ex}")
                else:
                    st.warning("No locations found.")
            
            except Exception as ex:
                st.error(f"Failed to load locations: {ex}")
    
    # ========================= 2FA VERIFICATION PAGE =========================