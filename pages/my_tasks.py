"""
Auto-generated module for the 'My Tasks' page.
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
        header("My Tasks")
        current_user = st.session_state.get("auth_user")
        if not current_user:
            st.info("Please login to access this page.")
            st.stop()
    
        st.caption("Track pending deletion approvals and error alerts assigned to you.")
    
        if current_user.get("role") in ("operator", "supervisor"):
            with st.expander("Request Password Reset", expanded=False):
                st.info("Submit a reset request that will be routed to the admin team.")
                reset_reason = st.text_area(
                    "Reason (optional)",
                    placeholder="Explain why you need a reset�",
                    key="pwd_reset_reason",
                )
                if st.button("📧 Send Reset Request", key="pwd_reset_request_btn"):
                    try:
                        TaskManager.create_password_reset_request(
                            current_user, reset_reason.strip() or None
                        )
                        st.success("Password reset request sent to admins.")
                    except Exception as ex:
                        st.error(f"Failed to submit request: {ex}")
    
        status_options = {
            "Pending": [TaskStatus.PENDING.value],
            "Approved": [TaskStatus.APPROVED.value],
            "Rejected": [TaskStatus.REJECTED.value],
            "Completed": [TaskStatus.COMPLETED.value],
            "All": None,
        }
        filter_col1, filter_col2 = st.columns([0.5, 0.5])
        selected_status = filter_col1.selectbox("Status", list(status_options.keys()), index=0)
        include_history = filter_col2.checkbox("Include resolved tasks", value=False)
        statuses = status_options[selected_status]
    
        tasks = TaskManager.fetch_tasks_for_user(
            current_user,
            statuses=statuses,
            include_history=include_history,
        )
    
        if not tasks:
            st.info("No tasks found for the selected filters.")
        else:
            for task in tasks:
                metadata = task.get("metadata") or {}
                resource_label = metadata.get("resource_label")
                if not resource_label and task.get("resource_type"):
                    resource_label = f"{task['resource_type']} #{task.get('resource_id')}" if task.get("resource_id") else None
    
                with st.container(border=True):
                    status_badge = task["status"].title()
                    st.markdown(
                        f"**{task['title']}**  \n"
                        f"Status: `{status_badge}` � Type: `{task['task_type']}`"
                    )
                    st.caption(
                        f"Raised by {task.get('raised_by', 'unknown')} on "
                        f"{_format_task_timestamp(task.get('raised_at'))}"
                    )
                    if resource_label:
                        st.caption(f"Resource: {resource_label}")
                    if task.get("description"):
                        st.write(task["description"])
                    context_text = metadata.get("context")
                    if context_text:
                        st.code(context_text, language="text")
                    if task.get("resolution_notes") and task["status"] in (
                        TaskStatus.REJECTED.value,
                        TaskStatus.COMPLETED.value,
                    ):
                        st.caption(f"Notes: {task['resolution_notes']}")
    
                    if task["task_type"] == TaskType.PASSWORD_RESET.value:
                        if (
                            current_user.get("role") in ["admin-operations", "admin-it"]
                            and task["status"] == TaskStatus.PENDING.value
                        ):
                            with st.form(f"pwd_reset_action_{task['id']}"):
                                new_pwd = st.text_input(
                                    "New Password",
                                    type="password",
                                    key=f"pwd_reset_new_{task['id']}",
                                )
                                confirm_pwd = st.text_input(
                                    "Confirm Password",
                                    type="password",
                                    key=f"pwd_reset_confirm_{task['id']}",
                                )
                                submitted = st.form_submit_button(
                                    "Reset Password & Complete", type="primary"
                                )
                                if submitted:
                                    if not new_pwd:
                                        st.error("Password cannot be empty.")
                                    elif new_pwd != confirm_pwd:
                                        st.error("Passwords do not match.")
                                    else:
                                        try:
                                            TaskManager.resolve_password_reset(
                                                task["id"],
                                                current_user.get("username", "admin"),
                                                new_pwd,
                                            )
                                            st.success("Password reset and task completed.")
                                            _st_safe_rerun()
                                        except Exception as ex:
                                            st.error(f"Failed to reset password: {ex}")
                        elif (
                            task.get("raised_by") == current_user.get("username")
                            and task["status"] == TaskStatus.PENDING.value
                        ):
                            st.info("? Waiting for an admin to complete this reset request.")
    
                    if (
                        task["task_type"] == TaskType.DELETE_REQUEST.value
                        and task["status"] == TaskStatus.APPROVED.value
                        and current_user.get("role") == "operator"
                    ):
                        st.info(
                            "? Remote approval granted. Return to the original page to complete the deletion."
                        )
    
                    acted = False
                    if TaskManager.user_can_act_on_task(task, current_user):
                        with st.form(f"task_action_{task['id']}"):
                            notes = st.text_area(
                                "Supervisor notes",
                                key=f"task_notes_{task['id']}",
                                placeholder="Optional notes for audit trail",
                                height=60,
                            )
                            act_col1, act_col2 = st.columns(2)
                            approve = act_col1.form_submit_button("? Approve")
                            reject = act_col2.form_submit_button("? Reject")
                            if approve:
                                TaskManager.update_status(
                                    task["id"],
                                    TaskStatus.APPROVED.value,
                                    current_user.get("username", "unknown"),
                                    notes or None,
                                )
                                st.success("Task approved.")
                                acted = True
                            elif reject:
                                if not notes:
                                    st.error("Please provide a reason before rejecting.")
                                else:
                                    TaskManager.update_status(
                                        task["id"],
                                        TaskStatus.REJECTED.value,
                                        current_user.get("username", "unknown"),
                                        notes,
                                    )
                                    st.warning("Task rejected.")
                                    acted = True
                        if acted:
                            import time
    
                            time.sleep(1)
                            _st_safe_rerun()
    
                    elif (
                        task["task_type"] == TaskType.ERROR_ALERT.value
                        and task["status"] == TaskStatus.PENDING.value
                        and current_user.get("role") in ["admin-operations", "admin-it"]
                    ):
                        with st.form(f"task_error_resolve_{task['id']}"):
                            notes = st.text_area(
                                "Resolution notes",
                                key=f"task_error_notes_{task['id']}",
                                height=60,
                            )
                            resolved = st.form_submit_button("? Mark as resolved")
                            if resolved:
                                TaskManager.update_status(
                                    task["id"],
                                    TaskStatus.COMPLETED.value,
                                    current_user.get("username", "unknown"),
                                    notes or "Resolved from My Tasks",
                                )
                                st.success("Task marked as resolved.")
                                import time
    
                                time.sleep(1)
                                _st_safe_rerun()
    
                    with st.expander("Activity log", expanded=False):
                        activities = task.get("activities") or []
                        if not activities:
                            st.caption("No activity recorded yet.")
                        else:
                            for activity in activities:
                                st.caption(
                                    f"{_format_task_timestamp(activity.get('timestamp'))} � "
                                    f"{activity.get('username') or 'system'} ? "
                                    f"{activity.get('action')}  "
                                    f"{activity.get('notes') or ''}"
                                )