# task_manager.py
"""
Centralized helper for OTMS "My Tasks" workflow.

Handles creation of approval requests, error alerts, and task lifecycle updates.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import or_, func
from sqlalchemy.orm import Session

from db import get_session
from models import Task, TaskActivity, TaskStatus, TaskType
from security import SecurityManager
from logger import log_error


class TaskManager:
    """Utility layer for creating and managing workflow tasks."""

    ERROR_KEYWORDS = ("failed", "error", "unable", "exception", "denied")

    # --------------------------------------------------------------------- #
    # Internal helpers
    # --------------------------------------------------------------------- #
    @staticmethod
    def _ensure_session(session: Optional[Session]) -> tuple[Session, bool]:
        if session is not None:
            return session, False
        new_session = get_session()
        return new_session, True

    @staticmethod
    def _close_session(session: Session, owns_session: bool):
        if owns_session and session is not None:
            session.close()

    @staticmethod
    def _serialize_metadata(metadata: Optional[Dict[str, Any]]) -> Optional[str]:
        if not metadata:
            return None
        try:
            return json.dumps(metadata, default=str)
        except Exception:
            safe_meta = {k: str(v) for k, v in metadata.items()}
            return json.dumps(safe_meta)

    @staticmethod
    def _deserialize_metadata(payload: Optional[str]) -> Dict[str, Any]:
        if not payload:
            return {}
        try:
            return json.loads(payload)
        except Exception:
            return {}

    @staticmethod
    def _add_activity(
        session: Session,
        task: Task,
        username: Optional[str],
        action: str,
        notes: Optional[str] = None,
    ) -> None:
        activity = TaskActivity(
            task_id=task.id,
            username=username,
            action=action,
            notes=notes,
        )
        session.add(activity)

    @staticmethod
    def _task_query_for_resource(
        session: Session,
        resource_type: str,
        resource_id: Any,
    ):
        return (
            session.query(Task)
            .filter(
                Task.resource_type == resource_type,
                Task.resource_id == str(resource_id),
            )
            .order_by(Task.raised_at.desc())
        )

    # --------------------------------------------------------------------- #
    # Generic helpers
    # --------------------------------------------------------------------- #
    @staticmethod
    def serialize_task(task: Task, include_activities: bool = True) -> Dict[str, Any]:
        return {
            "id": task.id,
            "title": task.title,
            "description": task.description,
            "task_type": task.task_type,
            "status": task.status,
            "priority": task.priority,
            "resource_type": task.resource_type,
            "resource_id": task.resource_id,
            "location_id": task.location_id,
            "target_role": task.target_role,
            "raised_by": task.raised_by,
            "raised_by_role": task.raised_by_role,
            "raised_at": task.raised_at,
            "approved_by": task.approved_by,
            "approved_at": task.approved_at,
            "resolved_by": task.resolved_by,
            "resolved_at": task.resolved_at,
            "resolution_notes": task.resolution_notes,
            "metadata": TaskManager._deserialize_metadata(task.metadata_json),
            "activities": [
                {
                    "id": act.id,
                    "timestamp": act.timestamp,
                    "username": act.username,
                    "action": act.action,
                    "notes": act.notes,
                }
                for act in sorted(
                    task.activities, key=lambda a: a.timestamp or datetime.utcnow()
                )
            ]
            if include_activities
            else [],
        }

    @staticmethod
    def should_capture_error(message: Optional[str]) -> bool:
        if not message:
            return False
        lower_text = message.lower()
        return any(keyword in lower_text for keyword in TaskManager.ERROR_KEYWORDS)

    # --------------------------------------------------------------------- #
    # Task creation helpers
    # --------------------------------------------------------------------- #
    @staticmethod
    def create_delete_request(
        resource_type: str,
        resource_id: Any,
        resource_label: str,
        raised_by: str,
        raised_by_role: str,
        location_id: Optional[int],
        metadata: Optional[Dict[str, Any]] = None,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:
        """Create (or return existing) delete approval request."""
        session, owns_session = TaskManager._ensure_session(session)
        try:
            existing = (
                TaskManager._task_query_for_resource(session, resource_type, resource_id)
                .filter(
                    Task.task_type == TaskType.DELETE_REQUEST.value,
                    Task.status.in_(
                        [TaskStatus.PENDING.value, TaskStatus.APPROVED.value]
                    ),
                )
                .first()
            )
            if existing:
                return TaskManager.serialize_task(existing)

            merged_meta = {"resource_label": resource_label}
            if metadata:
                merged_meta.update(metadata)

            task = Task(
                title=f"Delete request • {resource_label}",
                description=(
                    f"{raised_by} requested permission to delete {resource_label}."
                ),
                task_type=TaskType.DELETE_REQUEST.value,
                status=TaskStatus.PENDING.value,
                resource_type=resource_type,
                resource_id=str(resource_id),
                location_id=location_id,
                target_role="supervisor",
                raised_by=raised_by,
                raised_by_role=raised_by_role,
                metadata_json=TaskManager._serialize_metadata(merged_meta),
            )
            session.add(task)
            session.flush()
            TaskManager._add_activity(
                session,
                task,
                raised_by,
                "CREATED",
                f"Delete approval requested for {resource_label}",
            )
            SecurityManager.log_audit(
                session,
                raised_by,
                "TASK_CREATE",
                resource_type="Task",
                resource_id=str(task.id),
                details=f"Delete approval requested for {resource_type}#{resource_id}",
                location_id=location_id,
            )
            session.commit()
            return TaskManager.serialize_task(task)
        finally:
            TaskManager._close_session(session, owns_session)

    @staticmethod
    def log_ui_error_task(
        message: str,
        user: Optional[Dict[str, Any]],
        location_id: Optional[int],
        context: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> None:
        """Create a high-priority task for admin when UI operations fail."""
        normalized = (message or "").strip()
        if not normalized:
            return

        session, owns_session = TaskManager._ensure_session(session)
        try:
            title = normalized[:180]
            recent = (
                session.query(Task)
                .filter(
                    Task.task_type == TaskType.ERROR_ALERT.value,
                    Task.status == TaskStatus.PENDING.value,
                    Task.title == title,
                )
                .order_by(Task.raised_at.desc())
                .first()
            )
            if recent and recent.raised_at:
                if datetime.utcnow() - recent.raised_at < timedelta(minutes=20):
                    return  # Avoid spamming admins with duplicate errors

            raised_by = (user or {}).get("username", "system")
            raised_role = (user or {}).get("role")
            meta = {"context": context} if context else {}

            task = Task(
                title=title,
                description=normalized,
                task_type=TaskType.ERROR_ALERT.value,
                status=TaskStatus.PENDING.value,
                priority="HIGH",
                target_role="admin-operations",
                resource_type="UI",
                resource_id=None,
                location_id=location_id,
                raised_by=raised_by,
                raised_by_role=raised_role,
                metadata_json=TaskManager._serialize_metadata(meta),
            )
            session.add(task)
            session.flush()
            TaskManager._add_activity(
                session,
                task,
                raised_by,
                "CREATED",
                context or "UI error reported by user",
            )
            SecurityManager.log_audit(
                session,
                raised_by,
                "TASK_CREATE",
                resource_type="Task",
                resource_id=str(task.id),
                details=f"UI error reported: {title}",
                location_id=location_id,
            )
            session.commit()
        except Exception:
            session.rollback()
            log_error("Failed to log UI error task", exc_info=True)
        finally:
            TaskManager._close_session(session, owns_session)

    # --------------------------------------------------------------------- #
    # Status helpers
    # --------------------------------------------------------------------- #
    @staticmethod
    def get_task_for_resource(
        resource_type: str,
        resource_id: Any,
        statuses: Optional[Sequence[str]] = None,
        session: Optional[Session] = None,
    ) -> Optional[Dict[str, Any]]:
        session, owns_session = TaskManager._ensure_session(session)
        try:
            query = TaskManager._task_query_for_resource(session, resource_type, resource_id)
            if statuses:
                query = query.filter(Task.status.in_(list(statuses)))
            task = query.first()
            return TaskManager.serialize_task(task) if task else None
        finally:
            TaskManager._close_session(session, owns_session)

    @staticmethod
    def update_status(
        task_id: int,
        new_status: str,
        actor: str,
        notes: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> Optional[Dict[str, Any]]:
        session, owns_session = TaskManager._ensure_session(session)
        try:
            task = session.query(Task).filter(Task.id == task_id).one_or_none()
            if not task:
                return None

            task.status = new_status
            now = datetime.utcnow()
            if new_status == TaskStatus.APPROVED.value:
                task.approved_by = actor
                task.approved_at = now
            elif new_status in (
                TaskStatus.REJECTED.value,
                TaskStatus.COMPLETED.value,
                TaskStatus.CANCELLED.value,
            ):
                task.resolved_by = actor
                task.resolved_at = now
                if notes:
                    task.resolution_notes = notes

            TaskManager._add_activity(session, task, actor, new_status, notes)
            SecurityManager.log_audit(
                session,
                actor,
                "TASK_STATUS",
                resource_type="Task",
                resource_id=str(task.id),
                details=f"Task set to {new_status}. {notes or ''}".strip(),
                location_id=task.location_id,
            )
            session.commit()
            return TaskManager.serialize_task(task)
        finally:
            TaskManager._close_session(session, owns_session)

    @staticmethod
    def complete_tasks_for_resource(
        resource_type: str,
        resource_id: Any,
        actor: str,
        notes: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> None:
        session, owns_session = TaskManager._ensure_session(session)
        try:
            tasks = (
                TaskManager._task_query_for_resource(session, resource_type, resource_id)
                .filter(
                    Task.task_type == TaskType.DELETE_REQUEST.value,
                    Task.status.in_(
                        [TaskStatus.PENDING.value, TaskStatus.APPROVED.value]
                    ),
                )
                .all()
            )
            if not tasks:
                return

            now = datetime.utcnow()
            for task in tasks:
                task.status = TaskStatus.COMPLETED.value
                task.resolved_by = actor
                task.resolved_at = now
                if notes:
                    task.resolution_notes = notes
                TaskManager._add_activity(
                    session, task, actor, TaskStatus.COMPLETED.value, notes
                )
                SecurityManager.log_audit(
                    session,
                    actor,
                    "TASK_STATUS",
                    resource_type="Task",
                    resource_id=str(task.id),
                    details=f"Task auto-completed after deleting {resource_type}#{resource_id}",
                    location_id=task.location_id,
                )
            session.commit()
        finally:
            TaskManager._close_session(session, owns_session)

    # --------------------------------------------------------------------- #
    # Queries for UI
    # --------------------------------------------------------------------- #
    @staticmethod
    def fetch_tasks_for_user(
        user: Optional[Dict[str, Any]],
        statuses: Optional[Sequence[str]] = None,
        include_history: bool = False,
        session: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        session, owns_session = TaskManager._ensure_session(session)
        try:
            query = session.query(Task)
            if statuses:
                query = query.filter(Task.status.in_(list(statuses)))
            elif not include_history:
                query = query.filter(Task.status == TaskStatus.PENDING.value)

            role = (user or {}).get("role")
            username = (user or {}).get("username")
            location_id = (user or {}).get("location_id")

            if role in ["admin-operations", "admin-it"]:
                # Admins see all tasks
                pass
            elif role == "supervisor":
                query = query.filter(
                    or_(Task.location_id == location_id, Task.location_id.is_(None))
                ).filter(Task.target_role.in_(["supervisor", "admin-it", "all"]))
            elif role == "manager":
                # Managers cannot see any tasks (not assignable to tasks)
                return []
            else:
                query = query.filter(
                    or_(Task.raised_by == username, Task.target_role == "operator")
                )

            tasks = (
                query.order_by(Task.raised_at.desc()).limit(500).all()
            )
            return [TaskManager.serialize_task(task) for task in tasks]
        finally:
            TaskManager._close_session(session, owns_session)

    @staticmethod
    def count_pending_tasks_for_user(
        user: Optional[Dict[str, Any]], session: Optional[Session] = None
    ) -> int:
        """Lightweight counter for pending tasks assigned to the given user/role."""
        if not user:
            return 0
        session, owns_session = TaskManager._ensure_session(session)
        try:
            query = session.query(func.count(Task.id)).filter(
                Task.status == TaskStatus.PENDING.value
            )
            role = user.get("role")
            username = user.get("username")
            location_id = user.get("location_id")

            if role in ["admin-operations", "admin-it"]:
                # Admins see all pending tasks
                pass
            elif role == "supervisor":
                query = query.filter(
                    or_(Task.location_id == location_id, Task.location_id.is_(None))
                ).filter(Task.target_role.in_(["supervisor", "admin-it", "all"]))
            elif role == "manager":
                # Managers cannot see any tasks
                return 0
            else:
                query = query.filter(
                    or_(Task.raised_by == username, Task.target_role == "operator")
                )
            return int(query.scalar() or 0)
        finally:
            TaskManager._close_session(session, owns_session)

    @staticmethod
    def create_password_reset_request(
        user: Dict[str, Any],
        reason: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> Dict[str, Any]:
        """Raise a password reset request task for admins."""
        session, owns_session = TaskManager._ensure_session(session)
        try:
            existing = (
                session.query(Task)
                .filter(
                    Task.task_type == TaskType.PASSWORD_RESET.value,
                    Task.resource_type == "User",
                    Task.resource_id == str(user.get("id")),
                    Task.status == TaskStatus.PENDING.value,
                )
                .one_or_none()
            )
            if existing:
                return TaskManager.serialize_task(existing)

            metadata = {
                "username": user.get("username"),
                "role": user.get("role"),
                "reason": reason,
            }
            task = Task(
                title=f"Password reset • {user.get('username')}",
                description=reason or "Password reset requested",
                task_type=TaskType.PASSWORD_RESET.value,
                status=TaskStatus.PENDING.value,
                target_role="admin-it",
                resource_type="User",
                resource_id=str(user.get("id")),
                location_id=None,
                raised_by=user.get("username"),
                raised_by_role=user.get("role"),
                metadata_json=TaskManager._serialize_metadata(metadata),
            )
            session.add(task)
            session.flush()
            TaskManager._add_activity(
                session,
                task,
                user.get("username"),
                "CREATED",
                reason or "Password reset requested",
            )
            SecurityManager.log_audit(
                session,
                user.get("username", "unknown"),
                "TASK_CREATE",
                resource_type="Task",
                resource_id=str(task.id),
                details=f"Password reset requested for {user.get('username')}",
            )
            session.commit()
            return TaskManager.serialize_task(task)
        finally:
            TaskManager._close_session(session, owns_session)

    @staticmethod
    def resolve_password_reset(
        task_id: int, actor: str, new_password: str, session: Optional[Session] = None
    ) -> Dict[str, Any]:
        """Complete a password reset request by setting the new password."""
        session, owns_session = TaskManager._ensure_session(session)
        try:
            task = (
                session.query(Task)
                .filter(Task.id == task_id)
                .one_or_none()
            )
            if not task or task.task_type != TaskType.PASSWORD_RESET.value:
                raise ValueError("Password reset task not found")
            if task.status != TaskStatus.PENDING.value:
                raise ValueError("Task already resolved")
            user_id = task.resource_id
            if not user_id:
                raise ValueError("Invalid task payload")
            from auth import AuthManager

            AuthManager.update_password(session, int(user_id), new_password)
            task.status = TaskStatus.COMPLETED.value
            task.resolved_by = actor
            task.resolved_at = datetime.utcnow()
            task.resolution_notes = "Password reset completed"
            TaskManager._add_activity(
                session, task, actor, TaskStatus.COMPLETED.value, "Password reset issued"
            )
            SecurityManager.log_audit(
                session,
                actor,
                "TASK_STATUS",
                resource_type="Task",
                resource_id=str(task.id),
                details=f"Password reset completed for user #{user_id}",
            )
            session.commit()
            return TaskManager.serialize_task(task)
        except Exception:
            session.rollback()
            raise
        finally:
            TaskManager._close_session(session, owns_session)

    @staticmethod
    def user_can_act_on_task(task: Dict[str, Any], user: Optional[Dict[str, Any]]) -> bool:
        """Determine whether the given user can approve / reject the task."""
        if not task or not user:
            return False
        role = user.get("role")
        if not role:
            return False
        status = task.get("status")
        
        # Admin-operations and Admin-IT can act on pending tasks
        if role in ["admin-operations", "admin-it"]:
            return status == TaskStatus.PENDING.value
        
        # Managers cannot act on any tasks
        if role == "manager":
            return False
        
        # Supervisors can act on tasks assigned to them
        if role == "supervisor":
            if status != TaskStatus.PENDING.value:
                return False
            loc_id = user.get("location_id")
            task_loc = task.get("location_id")
            return task.get("target_role") in ("supervisor", "admin-it", "all") and (
                task_loc is None or task_loc == loc_id
            )
        return False

    @staticmethod
    def operator_has_approved_task(
        resource_type: str,
        resource_id: Any,
        session: Optional[Session] = None,
    ) -> Optional[Dict[str, Any]]:
        """Check if a delete task for this resource has already been approved."""
        return TaskManager.get_task_for_resource(
            resource_type,
            resource_id,
            statuses=[TaskStatus.APPROVED.value],
            session=session,
        )
