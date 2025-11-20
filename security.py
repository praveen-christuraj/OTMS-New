# security.py
"""
Security and audit utilities for OTMS.
Handles audit logging, password validation, session management.
"""

import os
import re
from datetime import datetime, timedelta
from typing import Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy import func
from models import AuditLog, LoginAttempt, User
import json
import bcrypt

from db import get_session

SUPERVISOR_CODE = os.getenv("SUPERVISOR_CODE", "1234")

class SecurityManager:
    """Centralized security and audit management"""
    
    # Password policy constants
    MIN_PASSWORD_LENGTH = 8
    MAX_FAILED_ATTEMPTS = 5
    ACCOUNT_LOCKOUT_MINUTES = 30
    SESSION_TIMEOUT_MINUTES = 30
    PASSWORD_EXPIRY_DAYS = 90
    
    @staticmethod
    def validate_password_strength(password: str) -> tuple[bool, str]:
        """
        Validate password meets security requirements.
        Returns (is_valid, error_message)
        """
        if len(password) < SecurityManager.MIN_PASSWORD_LENGTH:
            return False, f"Password must be at least {SecurityManager.MIN_PASSWORD_LENGTH} characters"
        
        if not re.search(r'[A-Z]', password):
            return False, "Password must contain at least one uppercase letter"
        
        if not re.search(r'[a-z]', password):
            return False, "Password must contain at least one lowercase letter"
        
        if not re.search(r'[0-9]', password):
            return False, "Password must contain at least one number"
        
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            return False, "Password must contain at least one special character (!@#$%^&*...)"
        
        return True, ""
    
    @staticmethod
    def log_audit(session: Optional[Session], username: str, action: str,
                resource_type: str = None, resource_id: str = None,
                details: str = None, user_id: int = None, 
                location_id: int = None, ip_address: str = None, 
                success: bool = True):
        """
        Log audit trail entry with LOCAL TIME
        
        Args:
            session: Database session
            username: Username performing the action
            action: Action type (LOGIN, LOGOUT, CREATE, UPDATE, DELETE, etc.)
            resource_type: Type of resource (TankTransaction, User, etc.)
            resource_id: ID of the resource
            details: Additional details about the action
            user_id: ID of the user performing the action
            location_id: Location where action was performed
            ip_address: IP address of the user
            success: Whether the action was successful (default: True)
        """
        
        try:
            from timezone_utils import get_local_time
            TIMEZONE_AVAILABLE = True
        except ImportError:
            TIMEZONE_AVAILABLE = False
        
        # Get current time in local timezone (not UTC)
        if TIMEZONE_AVAILABLE:
            local_time = get_local_time()
            timestamp = local_time.replace(tzinfo=None)  # Store as naive datetime in local time
        else:
            # Fallback to UTC if timezone utils not available
            from datetime import datetime
            timestamp = datetime.utcnow()
        
        owns_session = False
        if session is None:
            # Lazily import to avoid circulars
            from db import SessionLocal
            session = SessionLocal()
            owns_session = True

        log = AuditLog(
            username=username,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details,
            user_id=user_id,
            location_id=location_id,
            ip_address=ip_address,  # ← NOW ACCEPTS IP ADDRESS
            success=success,        # ← NOW ACCEPTS SUCCESS FLAG
            timestamp=timestamp
        )
        
        session.add(log)

        try:
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Failed to log audit: {e}")
            # Don't raise - audit logging failure shouldn't break the app
        finally:
            if owns_session:
                session.close()
    
    @staticmethod
    def log_login_attempt(
        session: Session,
        username: str,
        success: bool,
        ip_address: Optional[str] = None,
        failure_reason: Optional[str] = None,
        user_agent: Optional[str] = None,
        two_factor_used: bool = False
    ):
        """Log a login attempt with enhanced tracking"""
        try:
            from ip_service import IPService
            from datetime import datetime
            
            # Get location from IP
            location = IPService.get_location_from_ip(ip_address) if ip_address else {}
            
            # Parse user agent
            device_info = IPService.parse_user_agent(user_agent) if user_agent else {}
            
            # Generate session ID
            session_id = IPService.generate_session_id(
                username, 
                ip_address or "unknown", 
                datetime.utcnow()
            )
            
            attempt = LoginAttempt(
                username=username,
                success=success,
                ip_address=ip_address,
                ip_country=location.get("country"),
                ip_city=location.get("city"),
                ip_region=location.get("region"),
                user_agent=user_agent,
                device_type=device_info.get("device_type"),
                browser=device_info.get("browser"),
                os=device_info.get("os"),
                failure_reason=failure_reason,
                two_factor_used=two_factor_used,
                session_id=session_id
            )
            session.add(attempt)
            session.commit()
        
        except Exception as e:
            # Fallback to basic logging if enhanced tracking fails
            print(f"Enhanced login tracking failed: {e}")
            attempt = LoginAttempt(
                username=username,
                success=success,
                ip_address=ip_address,
                failure_reason=failure_reason
            )
            session.add(attempt)
            session.commit()
    
    @staticmethod
    def check_account_locked(session: Session, username: str) -> tuple[bool, Optional[datetime]]:
        """
        Check if account is locked due to failed login attempts.
        Returns (is_locked, locked_until)
        """
        user = session.query(User).filter(User.username == username).one_or_none()
        if not user:
            return False, None
        
        if user.account_locked_until:
            if datetime.utcnow() < user.account_locked_until:
                return True, user.account_locked_until
            else:
                # Lock expired, reset
                user.account_locked_until = None
                user.failed_login_attempts = 0
                session.commit()
        
        return False, None
    
    @staticmethod
    def record_failed_login(session: Session, username: str):
        """Record a failed login and lock account if threshold exceeded"""
        user = session.query(User).filter(User.username == username).one_or_none()
        if not user:
            return
        
        user.failed_login_attempts = (user.failed_login_attempts or 0) + 1
        
        if user.failed_login_attempts >= SecurityManager.MAX_FAILED_ATTEMPTS:
            user.account_locked_until = datetime.utcnow() + timedelta(
                minutes=SecurityManager.ACCOUNT_LOCKOUT_MINUTES
            )
        
        session.commit()
    
    @staticmethod
    def reset_failed_login_attempts(session: Session, username: str):
        """Reset failed login counter on successful login"""
        user = session.query(User).filter(User.username == username).one_or_none()
        if user:
            user.failed_login_attempts = 0
            user.account_locked_until = None
            session.commit()
    
    @staticmethod
    def is_session_expired(user_dict: Dict) -> bool:
        """Check if user session has expired due to inactivity"""
        last_activity = user_dict.get("last_activity")
        if not last_activity:
            return False
        
        if isinstance(last_activity, str):
            last_activity = datetime.fromisoformat(last_activity)
        
        timeout = timedelta(minutes=SecurityManager.SESSION_TIMEOUT_MINUTES)
        return datetime.utcnow() - last_activity > timeout
    
    @staticmethod
    def update_last_activity(session: Session, user_id: int):
        """Update user's last activity timestamp"""
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if user:
            user.last_activity = datetime.utcnow()
            session.commit()

    @staticmethod
    def verify_supervisor_code(code: Optional[str], supervisor_username: Optional[str] = None) -> bool:
        """Validate supervisor override code (per supervisor when provided)."""
        if not code:
            return False
        code = code.strip()
        if supervisor_username:
            try:
                with get_session() as session:
                    supervisor = (
                        session.query(User)
                        .filter(
                            func.lower(User.username) == supervisor_username.lower(),
                            User.role == "supervisor",
                            User.is_active == True,  # noqa: E712
                        )
                        .one_or_none()
                    )
                    if supervisor and supervisor.supervisor_code_hash:
                        try:
                            return bcrypt.checkpw(
                                code.encode("utf-8"),
                                supervisor.supervisor_code_hash.encode("utf-8"),
                            )
                        except Exception:
                            return False
            except Exception:
                return False
        return code == SUPERVISOR_CODE
    
    @staticmethod
    def password_expired(user: User) -> bool:
        """Check if password has expired"""
        if not user.password_changed_at:
            return False  # Never changed, don't enforce expiry
        
        expiry = timedelta(days=SecurityManager.PASSWORD_EXPIRY_DAYS)
        return datetime.utcnow() - user.password_changed_at > expiry
    
    @staticmethod
    def get_audit_trail(
        session: Session,
        user_id: Optional[int] = None,
        location_id: Optional[int] = None,
        action: Optional[str] = None,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        limit: int = 100
    ) -> list:
        """Retrieve audit trail with filters"""
        query = session.query(AuditLog).order_by(AuditLog.timestamp.desc())
        
        if user_id:
            query = query.filter(AuditLog.user_id == user_id)
        if location_id:
            query = query.filter(AuditLog.location_id == location_id)
        if action:
            query = query.filter(AuditLog.action == action)
        if date_from:
            query = query.filter(AuditLog.timestamp >= date_from)
        if date_to:
            query = query.filter(AuditLog.timestamp <= date_to)
        
        return query.limit(limit).all()
