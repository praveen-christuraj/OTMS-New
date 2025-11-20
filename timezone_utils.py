# timezone_utils.py
"""
Timezone utilities for OTMS
Handles conversion between UTC and local time
"""

from datetime import datetime, timezone
import pytz

# Define your local timezone (Nigeria uses WAT - West Africa Time, UTC+1)
LOCAL_TIMEZONE = pytz.timezone('Africa/Lagos')  # UTC+1

def get_local_time() -> datetime:
    """Get current time in local timezone"""
    utc_now = datetime.now(timezone.utc)
    return utc_now.astimezone(LOCAL_TIMEZONE)

def utc_to_local(utc_dt: datetime) -> datetime:
    """Convert UTC datetime to local timezone"""
    if utc_dt is None:
        return None
    
    # If datetime is naive (no timezone), assume it's UTC
    if utc_dt.tzinfo is None:
        utc_dt = pytz.utc.localize(utc_dt)
    
    return utc_dt.astimezone(LOCAL_TIMEZONE)

def local_to_utc(local_dt: datetime) -> datetime:
    """Convert local datetime to UTC"""
    if local_dt is None:
        return None
    
    # If datetime is naive, assume it's in local timezone
    if local_dt.tzinfo is None:
        local_dt = LOCAL_TIMEZONE.localize(local_dt)
    
    return local_dt.astimezone(timezone.utc)

def format_local_datetime(
    dt: datetime,
    format_str: str = "%Y-%m-%d %H:%M:%S",
    naive_is_local: bool = False,
) -> str:
    """
    Format datetime in local timezone.
    
    Args:
        dt: datetime value from the database.
        format_str: strftime-compatible format string.
        naive_is_local: if True and dt is naive (no tzinfo), treat it as already
            being in the local timezone instead of assuming UTC.
    """
    if dt is None:
        return ""
    
    if dt.tzinfo is None and naive_is_local:
        local_dt = LOCAL_TIMEZONE.localize(dt)
    else:
        local_dt = utc_to_local(dt)
    return local_dt.strftime(format_str)
