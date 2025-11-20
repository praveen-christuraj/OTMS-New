"""
Recycle bin helper utilities.

Provides a centralized way to archive a snapshot of records before they are removed
from their primary tables. The archived payload can be inspected (or restored manually)
by administrators from the Recycle Bin page.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional

from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.orm import Session

from models import RecycleBinEntry


def _json_default(value: Any) -> Any:
    """Best-effort serializer for dataclasses, datetimes, decimals, etc."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


class RecycleBinManager:
    """Utility helpers for archiving deleted data."""

    @staticmethod
    def snapshot_record(record: Any) -> Dict[str, Any]:
        mapper = sa_inspect(record.__class__)
        data: Dict[str, Any] = {}
        for column in mapper.columns:
            data[column.key] = getattr(record, column.key)
        return data

    @staticmethod
    def _resolve_identifier(record: Any) -> str:
        for attr in ("id", "ticket_id", "serial_no"):
            if hasattr(record, attr):
                value = getattr(record, attr)
                if value is not None:
                    return str(value)
        return record.__class__.__name__

    @staticmethod
    def archive_record(
        session: Session,
        record: Any,
        resource_type: str,
        username: str,
        user_id: Optional[int] = None,
        location_id: Optional[int] = None,
        reason: Optional[str] = None,
        label: Optional[str] = None,
    ) -> RecycleBinEntry:
        """
        Persist a snapshot of the record in the recycle bin table and delete
        the original instance from the session.
        """
        payload = RecycleBinManager.snapshot_record(record)
        entry = RecycleBinEntry(
            resource_type=resource_type,
            resource_id=RecycleBinManager._resolve_identifier(record),
            resource_label=label or payload.get("ticket_id") or payload.get("id"),
            payload_json=json.dumps(payload, default=_json_default),
            deleted_by=username or "unknown",
            deleted_by_id=user_id,
            location_id=location_id,
            reason=reason,
        )
        session.add(entry)
        session.flush()
        session.delete(record)
        return entry

    @staticmethod
    def archive_payload(
        session: Session,
        resource_type: str,
        resource_id: str,
        payload: Dict[str, Any],
        username: str,
        user_id: Optional[int] = None,
        location_id: Optional[int] = None,
        reason: Optional[str] = None,
        label: Optional[str] = None,
    ) -> RecycleBinEntry:
        """Archive arbitrary payload data (for bulk deletes)."""
        entry = RecycleBinEntry(
            resource_type=resource_type,
            resource_id=str(resource_id),
            resource_label=label or resource_id,
            payload_json=json.dumps(payload, default=_json_default),
            deleted_by=username or "unknown",
            deleted_by_id=user_id,
            location_id=location_id,
            reason=reason,
        )
        session.add(entry)
        session.flush()
        return entry
