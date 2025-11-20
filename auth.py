# auth.py
"""
Authentication and user management for OTMS multi-location support.
Provides login, session management, and access control.
"""

import hashlib
import os
from typing import Optional, Dict, List
from datetime import datetime
from sqlalchemy.orm import Session
from models import User, Location
from security import SecurityManager
import bcrypt

class AuthManager:
    """Handles user authentication and session management"""
    
    @staticmethod
    def hash_password(password: str) -> str:
        """Hash a password using bcrypt"""
        return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    @staticmethod
    def verify_password(password: str, password_hash: str) -> bool:
        """Verify a password against its bcrypt hash"""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
        except Exception as e:
            print(f"Password verification error: {e}")
            return False
    
    @staticmethod
    def authenticate(
        session: Session, 
        username: str, 
        password: str, 
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Authenticate user credentials with security checks.
        Returns user dict if successful, None otherwise.
        """
        # Check if account is locked
        is_locked, locked_until = SecurityManager.check_account_locked(session, username)
        if is_locked:
            minutes_left = int((locked_until - datetime.utcnow()).total_seconds() / 60)
            SecurityManager.log_login_attempt(
                session, username, False, ip_address,
                failure_reason=f"Account locked until {locked_until.strftime('%H:%M')} ({minutes_left} min)",
                user_agent=user_agent
            )
            raise ValueError(f"Account locked due to multiple failed attempts. Try again in {minutes_left} minutes.")
        
        user = session.query(User).filter(
            User.username == username,
            User.is_active == True
        ).one_or_none()
        
        if not user:
            SecurityManager.log_login_attempt(
                session, username, False, ip_address,
                failure_reason="User not found or inactive",
                user_agent=user_agent
            )
            return None
        
        # Verify password using bcrypt
        if not AuthManager.verify_password(password, user.password_hash):
            SecurityManager.record_failed_login(session, username)
            SecurityManager.log_login_attempt(
                session, username, False, ip_address,
                failure_reason="Invalid password",
                user_agent=user_agent
            )
            return None
        
        # Success - reset failed attempts
        SecurityManager.reset_failed_login_attempts(session, username)
        
        # Update last login and activity
        user.last_login = datetime.utcnow()
        user.last_activity = datetime.utcnow()
        session.commit()
        
        # Log successful login
        SecurityManager.log_login_attempt(
            session, username, True, ip_address,
            user_agent=user_agent,
            two_factor_used=False  # Will be updated if 2FA is used
        )
        
        SecurityManager.log_audit(
            session, username, "LOGIN", user_id=user.id,
            location_id=user.location_id, ip_address=ip_address
        )
        
        # Load location info if user is location-bound
        location_info = None
        if user.location_id:
            loc = session.query(Location).filter(Location.id == user.location_id).one_or_none()
            if loc:
                location_info = {
                    "id": loc.id,
                    "name": loc.name,
                    "code": loc.code
                }
        
        return {
            "id": user.id,
            "username": user.username,
            "full_name": user.full_name,
            "role": user.role,
            "location_id": user.location_id,
            "location": location_info,
            "must_change_password": user.must_change_password,
            "last_activity": user.last_activity.isoformat() if user.last_activity else None
        }
    
    @staticmethod
    def create_user(
        session: Session,
        username: str,
        password: str,
        full_name: str,
        role: str,
        location_id: Optional[int] = None,
        supervisor_code: Optional[str] = None,
    ) -> Dict:
        """
        Create a new user account.
        Returns a dictionary (not the ORM object) to avoid session detachment issues.
        """
        
        # Validate role
        if role not in ["admin-operations", "admin-it", "manager", "supervisor", "operator"]:
            raise ValueError("Invalid role. Must be admin-operations, admin-it, manager, supervisor, or operator")
        
        # Admin users and managers should not be tied to a location
        if role in ["admin-operations", "admin-it", "manager"]:
            location_id = None
        elif location_id is None:
            raise ValueError("Non-admin users must be assigned to a location")
        
        # Check if username already exists
        existing = session.query(User).filter(User.username == username).one_or_none()
        if existing:
            raise ValueError(f"Username '{username}' already exists")
        
        if role == "supervisor" and not supervisor_code:
            raise ValueError("Supervisor code is required for supervisor accounts")

        # Create user with bcrypt-hashed password
        user = User(
            username=username,
            password_hash=AuthManager.hash_password(password),
            full_name=full_name,
            role=role,
            location_id=location_id,
            is_active=True
        )
        if role == "supervisor" and supervisor_code:
            user.supervisor_code_hash = AuthManager.hash_password(supervisor_code)
            user.supervisor_code_set_at = datetime.utcnow()
        
        session.add(user)
        session.commit()
        
        # Get location info if applicable
        location_info = None
        if location_id:
            loc = session.query(Location).filter(Location.id == location_id).one_or_none()
            if loc:
                location_info = {
                    "id": loc.id,
                    "name": loc.name,
                    "code": loc.code
                }
        
        # Return a dict instead of the ORM object
        return {
            "id": user.id,
            "username": user.username,
            "full_name": user.full_name,
            "role": user.role,
            "location_id": user.location_id,
            "location": location_info,
            "is_active": user.is_active
        }

    @staticmethod
    def set_supervisor_code(session: Session, user_id: int, new_code: str) -> Dict:
        """Assign or reset a supervisor's override code."""
        if not new_code:
            raise ValueError("Supervisor code cannot be empty")
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            raise ValueError("User not found")
        if user.role != "supervisor":
            raise ValueError("Supervisor code can only be set for supervisors")

        user.supervisor_code_hash = AuthManager.hash_password(new_code)
        user.supervisor_code_set_at = datetime.utcnow()
        session.commit()
        return {"username": user.username}

    @staticmethod
    def get_active_supervisors(
        session: Session, location_id: Optional[int] = None
    ) -> List[User]:
        """Return active supervisor ORM objects, optionally filtered by location."""
        query = session.query(User).filter(
            User.role == "supervisor",
            User.is_active == True,  # noqa: E712
        )
        if location_id:
            query = query.filter(User.location_id == location_id)
        return query.order_by(User.full_name, User.username).all()
    
    @staticmethod
    def update_password(session: Session, user_id: int, new_password: str):
        """Update user password"""
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            raise ValueError("User not found")
        
        user.password_hash = AuthManager.hash_password(new_password)
        session.commit()
    
    @staticmethod
    def get_user_locations(session: Session, user_dict: Dict) -> list:
        """
        Get list of locations accessible to this user.
        - Admin-operations and managers can access all active locations
        - Supervisors and operators can ONLY access their assigned location
        """
        if user_dict["role"] in ["admin-operations", "manager"]:
            # Admin-operations and managers see all active locations
            locations = session.query(Location).filter(Location.is_active == True).order_by(Location.name).all()
            return [{
                "id": loc.id,
                "name": loc.name,
                "code": loc.code
            } for loc in locations]
        else:
            # Supervisors and operators only see their assigned location
            if user_dict["location_id"]:
                loc = session.query(Location).filter(Location.id == user_dict["location_id"]).one_or_none()
                if loc and loc.is_active:
                    return [{
                        "id": loc.id,
                        "name": loc.name,
                        "code": loc.code
                    }]
            return []
    
    @staticmethod
    def can_access_location(user_dict: Dict, location_id: int) -> bool:
        """
        Check if user has access to a specific location.
        - Admin-operations and managers can access ALL locations
        - Supervisors and operators can ONLY access their assigned location
        """
        if user_dict["role"] in ["admin-operations", "manager"]:
            return True  # Admin-operations and managers can access all locations
        
        # Supervisors and operators are restricted to their assigned location
        return user_dict.get("location_id") == location_id
    
    @staticmethod
    def get_user_by_id(session: Session, user_id: int) -> Optional[Dict]:
        """
        Get user by ID.
        Returns a dictionary (not the ORM object).
        """
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            return None
        
        location_info = None
        if user.location_id:
            loc = session.query(Location).filter(Location.id == user.location_id).one_or_none()
            if loc:
                location_info = {
                    "id": loc.id,
                    "name": loc.name,
                    "code": loc.code
                }
        
        return {
            "id": user.id,
            "username": user.username,
            "full_name": user.full_name,
            "role": user.role,
            "location_id": user.location_id,
            "location": location_info,
            "is_active": user.is_active,
            "last_login": user.last_login
        }
    
    @staticmethod
    def toggle_user_status(session: Session, user_id: int) -> Dict:
        """
        Toggle user active/inactive status.
        Returns updated user dict.
        """
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            raise ValueError("User not found")
        
        user.is_active = not user.is_active
        session.commit()
        
        return {
            "id": user.id,
            "username": user.username,
            "is_active": user.is_active
        }
    
    @staticmethod
    def transfer_user_to_location(session: Session, user_id: int, new_location_id: Optional[int]) -> Dict:
        """
        Transfer a user to a different location.
        Only for non-admin users. Admins don't have location assignments.
        
        Returns updated user dict with transfer details.
        """
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            raise ValueError("User not found")
        
        if user.role in ["admin-operations", "admin-it", "manager"]:
            raise ValueError("Admin and manager users cannot be assigned to specific locations")
        
        # Validate new location exists and is active
        if new_location_id is not None:
            loc = session.query(Location).filter(
                Location.id == new_location_id,
                Location.is_active == True
            ).one_or_none()
            if not loc:
                raise ValueError("Invalid or inactive location")
        else:
            raise ValueError("Non-admin users must be assigned to a location")
        
        # Get old location info before updating
        old_location_id = user.location_id
        old_loc = None
        if old_location_id:
            old_loc = session.query(Location).filter(Location.id == old_location_id).one_or_none()
        
        # Update user's location
        user.location_id = new_location_id
        session.commit()
        
        # Get new location info
        new_loc = session.query(Location).filter(Location.id == new_location_id).one_or_none()
        
        return {
            "id": user.id,
            "username": user.username,
            "full_name": user.full_name,
            "role": user.role,
            "old_location": f"{old_loc.name} ({old_loc.code})" if old_loc else "None",
            "new_location": f"{new_loc.name} ({new_loc.code})" if new_loc else "None",
            "location_id": new_location_id
        }
    
    @staticmethod
    def update_user_details(
        session: Session,
        user_id: int,
        full_name: Optional[str] = None,
        role: Optional[str] = None,
        location_id: Optional[int] = None
    ) -> Dict:
        """
        Update user details (full name, role, location).
        Returns updated user dict.
        """
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            raise ValueError("User not found")
        
        # Update full name
        if full_name is not None:
            user.full_name = full_name
        
        # Update role
        if role is not None:
            if role not in ["admin-operations", "admin-it", "manager", "supervisor", "operator"]:
                raise ValueError("Invalid role. Must be admin-operations, admin-it, manager, supervisor, or operator")
            user.role = role
            
            # If changing to admin or manager, remove location assignment
            if role in ["admin-operations", "admin-it", "manager"]:
                user.location_id = None
        
        # Update location (only for non-admin roles that are location-bound)
        if location_id is not None and user.role not in ["admin-operations", "admin-it", "manager"]:
            loc = session.query(Location).filter(
                Location.id == location_id,
                Location.is_active == True
            ).one_or_none()
            if not loc:
                raise ValueError("Invalid or inactive location")
            user.location_id = location_id
        
        session.commit()
        
        # Get location info
        location_info = None
        if user.location_id:
            loc = session.query(Location).filter(Location.id == user.location_id).one_or_none()
            if loc:
                location_info = {
                    "id": loc.id,
                    "name": loc.name,
                    "code": loc.code
                }
        
        return {
            "id": user.id,
            "username": user.username,
            "full_name": user.full_name,
            "role": user.role,
            "location_id": user.location_id,
            "location": location_info,
            "is_active": user.is_active
        }
    
    @staticmethod
    def permanently_delete_user(session: Session, user_id: int) -> Dict:
        """
        PERMANENTLY delete a user account.
        ⚠️ WARNING: This is irreversible!
        
        Returns a dict with deletion details.
        """
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            raise ValueError("User not found")
        
        # Don't allow deleting the last admin
        if user.role == "admin-operations":
            admin_count = session.query(User).filter(
                User.role == "admin-operations",
                User.is_active == True
            ).count()
            if admin_count <= 1:
                raise ValueError("Cannot delete the last active admin user")
        
        # Gather info before deletion
        deletion_info = {
            "username": user.username,
            "full_name": user.full_name,
            "role": user.role,
            "location_id": user.location_id,
        }
        
        # Get location name if applicable
        if user.location_id:
            loc = session.query(Location).filter(Location.id == user.location_id).one_or_none()
            if loc:
                deletion_info["location_name"] = loc.name
        
        # Delete the user
        session.delete(user)
        session.commit()
        
        return deletion_info

    @staticmethod
    def change_password(
        session: Session,
        user_id: int,
        old_password: str,
        new_password: str
    ) -> Dict:
        """
        Change user password with validation.
        Returns updated user dict.
        """
        user = session.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            raise ValueError("User not found")
        
        # Verify old password
        if not AuthManager.verify_password(old_password, user.password_hash):
            raise ValueError("Current password is incorrect")
        
        # Validate new password strength
        is_valid, error_msg = SecurityManager.validate_password_strength(new_password)
        if not is_valid:
            raise ValueError(error_msg)
        
        # Check if new password is same as old
        if AuthManager.verify_password(new_password, user.password_hash):
            raise ValueError("New password cannot be the same as current password")
        
        # Update password
        user.password_hash = AuthManager.hash_password(new_password)
        user.password_changed_at = datetime.utcnow()
        user.must_change_password = False
        session.commit()
        
        # Log audit
        SecurityManager.log_audit(
            session, user.username, "PASSWORD_CHANGE",
            user_id=user.id, location_id=user.location_id,
            details="User changed their password"
        )
        
        return {
            "id": user.id,
            "username": user.username,
            "must_change_password": False
        }
