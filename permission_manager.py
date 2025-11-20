# permission_manager.py
"""
Permission Manager for OTMS
Controls location-based and role-based access to features
"""

from typing import Dict, Optional
from sqlalchemy.orm import Session
from models import Location, LocationConfiguration, User
import json

def _normalize_role(role: Optional[str]) -> str:
    """Map legacy/admin aliases to current role keys."""
    if not role:
        return "operator"
    role = role.lower()
    if role == "admin":
        return "admin-operations"
    return role

class PermissionManager:
    """Manage location-based and role-based permissions"""
    
    @staticmethod
    def get_location_permissions(session: Session, location_id: int) -> Dict:
        """Get permissions for a specific location"""
        
        # Get location configuration
        config = session.query(LocationConfiguration).filter(
            LocationConfiguration.location_id == location_id
        ).first()
        
        if config and config.config_json:
            try:
                full_config = json.loads(config.config_json)
                return full_config.get("permissions", {})
            except:
                pass
        
        # Default: no permissions
        return {
            "tank_transactions": False,
            "yade_transactions": False,
            "tanker_transactions": False,
            "otr_vessel": False,
            "fso_operations": False,
            "is_head_office": False
        }
    
    @staticmethod
    def can_access_feature(session: Session, location_id: int, feature: str, user_role: str = None) -> bool:
        """
        Check if a feature is allowed at a location
        
        ADMIN-OPERATIONS OVERRIDE: Admin-operations can access ALL features at ALL locations
        MANAGER & ADMIN-IT: Cannot access operational features (read-only)
        """
        user_role = _normalize_role(user_role)
        # ADMIN-OPERATIONS OVERRIDE - Can access everything everywhere
        if user_role == "admin-operations":
            return True
        
        # Manager and Admin-IT cannot access features (read-only roles)
        if user_role in ["manager", "admin-it"]:
            return False
        
        # For non-admins, check location permissions
        permissions = PermissionManager.get_location_permissions(session, location_id)
        return permissions.get(feature, False)
    
    @staticmethod
    def is_head_office(session: Session, location_id: int) -> bool:
        """Check if location is Head Office"""
        permissions = PermissionManager.get_location_permissions(session, location_id)
        return permissions.get("is_head_office", False)
    
    @staticmethod
    def is_lagos_ho_location(session: Session, location_id: int) -> bool:
        """Check if a location is Lagos (HO)"""
        location = session.query(Location).filter(Location.id == location_id).first()
        if not location:
            return False
        
        # Check if location code is LAGOS or HO or name contains "Head Office"
        code = location.code.upper()
        name = location.name.upper()
        
        return code in ["LAGOS", "HO", "LAGOS (HO)"] or "HEAD OFFICE" in name or "LAGOS" in name
    
    @staticmethod
    def is_lagos_ho_user(user: Dict) -> bool:
        """
        Check if user is from Lagos (HO) location.
        Lagos (HO) users can access ALL locations based on their role.
        """
        user_role = _normalize_role(user.get("role"))
        # Admin and manager roles are NOT considered HO user (separate categories)
        if user_role in ["admin-operations", "admin-it", "manager"]:
            return False
        
        # Check if user's home location is Lagos (HO)
        user_location_id = user.get("location_id")
        if not user_location_id:
            return False
        
        from db import get_session
        with get_session() as s:
            return PermissionManager.is_lagos_ho_location(s, user_location_id)
    
    @staticmethod
    def can_make_entries(session: Session, user_role: str, location_id: int) -> bool:
        """
        Check if user can make entries at this location
        
        ADMIN-OPERATIONS: Can make entries everywhere
        ADMIN-IT: Cannot make entries (system admin only)
        MANAGER: Cannot make entries (read-only)
        LAGOS (HO) USERS: Can make entries everywhere (operator & supervisor)
        SUPERVISOR/OPERATOR: Can make entries at their assigned location only
        """
        user_role = _normalize_role(user_role)
        # ADMIN-OPERATIONS OVERRIDE - Can make entries everywhere
        if user_role == "admin-operations":
            return True
        
        # Admin-IT and Manager cannot make entries
        if user_role in ["admin-it", "manager"]:
            return False
        
        # All other roles can make entries
        if user_role in ["supervisor", "operator"]:
            return True
        
        return False
    
    @staticmethod
    def can_delete_entries(user: Dict) -> bool:
        """
        Check if user can delete entries.
        
        Rules:
        - Admin-operations: Can delete everywhere
        - Admin-IT: Cannot delete (system admin only)
        - Manager: Cannot delete (read-only)
        - Supervisor (anywhere including Lagos HO): Can delete
        - Operator (anywhere including Lagos HO): Cannot delete
        """
        role = _normalize_role(user.get("role"))
        
        if role == "admin-operations":
            return True
        
        if role in ["admin-it", "manager"]:
            return False
        
        if role == "supervisor":
            return True
        
        return False  # Operators cannot delete
    
    @staticmethod
    def can_access_management_pages(user: Dict) -> bool:
        """
        Check if user can access management pages (Users, Locations, Assets, etc.).
        
        ONLY ADMIN-OPERATIONS and ADMIN-IT can access these pages.
        Admin-IT has limited access (no location/operations pages)
        Lagos (HO) users with supervisor/operator role CANNOT.
        """
        return _normalize_role(user.get("role")) in ["admin-operations", "admin-it"]
    
    @staticmethod
    def can_manage_system(session: Session, user_role: str, location_id: Optional[int]) -> bool:
        """
        Check if user can manage system (users, locations, assets, etc.)
        
        ONLY ADMIN-OPERATIONS can manage system fully
        ADMIN-IT can manage users and system settings but not operations
        Lagos (HO) users CANNOT manage system
        """
        role = _normalize_role(user_role)
        return role in ["admin-operations", "admin-it"]
    
    @staticmethod
    def can_access_operational_pages(user: Dict) -> bool:
        """
        Check if user can access operational/transaction pages and reports.
        
        ADMIN-OPERATIONS: Full access to all operations
        ADMIN-IT: NO access (system admin only)
        MANAGER: Read-only access to reports (no entry pages)
        SUPERVISOR/OPERATOR: Full access to their locations
        """
        role = _normalize_role(user.get("role"))
        return role in ["admin-operations", "manager", "supervisor", "operator"]
    
    @staticmethod
    def can_view_all_locations(user: Dict) -> bool:
        """
        Check if user can view data from all locations.
        
        ADMIN-OPERATIONS: Yes
        ADMIN-IT: No (no operational access)
        MANAGER: Yes (read-only)
        SUPERVISOR/OPERATOR: Only their assigned location
        """
        role = _normalize_role(user.get("role"))
        return role in ["admin-operations", "manager"]
    
    @staticmethod
    def can_manage_users(user: Dict) -> bool:
        """
        Check if user can manage other users (create, edit, delete).
        
        ADMIN-OPERATIONS: Full user management
        ADMIN-IT: Full user management
        MANAGER: Cannot manage users
        SUPERVISOR/OPERATOR: Cannot manage users
        """
        role = _normalize_role(user.get("role"))
        return role in ["admin-operations", "admin-it"]
    
    @staticmethod
    def can_approve_tasks(user: Dict) -> bool:
        """
        Check if user can be assigned tasks for approval (deletion requests, etc.).
        
        ADMIN-OPERATIONS: Yes
        ADMIN-IT: Yes (for password resets)
        MANAGER: No (read-only role)
        SUPERVISOR: Yes
        OPERATOR: No
        """
        role = _normalize_role(user.get("role"))
        return role in ["admin-operations", "admin-it", "supervisor"]
    
    @staticmethod
    def can_access_system_admin_pages(user: Dict) -> bool:
        """
        Check if user can access system admin pages (users, audit log, backups, 2FA reset).
        
        ADMIN-OPERATIONS: Full access
        ADMIN-IT: Full access
        MANAGER: No access
        SUPERVISOR/OPERATOR: No access
        """
        role = _normalize_role(user.get("role"))
        return role in ["admin-operations", "admin-it"]
    
    @staticmethod
    def get_accessible_locations(session: Session, user: Dict) -> list:
        """Get list of locations user can access"""
        
        role = _normalize_role(user.get("role"))
        user_location_id = user.get("location_id")
        
        # Admin-operations and managers can access all locations
        if role in ["admin-operations", "manager"]:
            locations = session.query(Location).filter(
                Location.is_active == True
            ).order_by(Location.name).all()
            
            return [{
                "id": loc.id,
                "name": loc.name,
                "code": loc.code,
                "is_ho": PermissionManager.is_head_office(session, loc.id)
            } for loc in locations]
        
        # Check if Lagos (HO) user
        if PermissionManager.is_lagos_ho_user(user):
            # Lagos (HO) users can access ALL locations
            locations = session.query(Location).filter(
                Location.is_active == True
            ).order_by(Location.name).all()
            
            return [{
                "id": loc.id,
                "name": loc.name,
                "code": loc.code,
                "is_ho": PermissionManager.is_head_office(session, loc.id)
            } for loc in locations]
        
        # Supervisor and Operator: only their assigned location
        if role in ["supervisor", "operator"] and user_location_id:
            location = session.query(Location).filter(
                Location.id == user_location_id,
                Location.is_active == True
            ).first()
            
            if location:
                return [{
                    "id": location.id,
                    "name": location.name,
                    "code": location.code,
                    "is_ho": PermissionManager.is_head_office(session, location.id)
                }]
        
        return []
    
    @staticmethod
    def get_accessible_locations_for_user(session: Session, user: Dict) -> list:
        """
        Get list of locations a user can access based on their role and home location.
        
        Rules:
        - Admin-Operations/Manager: ALL locations
        - Lagos (HO) Operator/Supervisor: ALL locations (for making entries)
        - Other Operator/Supervisor: ONLY their assigned location
        """
        role = _normalize_role(user.get("role"))
        user_location_id = user.get("location_id")
        
        # Admin-operations and managers can access all locations
        if role in ["admin-operations", "manager"]:
            locations = session.query(Location).filter(
                Location.is_active == True
            ).order_by(Location.name).all()
            return [{"id": loc.id, "name": loc.name, "code": loc.code} for loc in locations]
        
        # Check if Lagos (HO) user
        if PermissionManager.is_lagos_ho_user(user):
            # Lagos (HO) users can access ALL locations
            locations = session.query(Location).filter(
                Location.is_active == True
            ).order_by(Location.name).all()
            return [{"id": loc.id, "name": loc.name, "code": loc.code} for loc in locations]
        
        # Regular users: only their assigned location
        if user_location_id:
            location = session.query(Location).filter(
                Location.id == user_location_id,
                Location.is_active == True
            ).first()
            
            if location:
                return [{"id": location.id, "name": location.name, "code": location.code}]
        
        return []
    
    @staticmethod
    def get_available_pages(session: Session, user_role: str, location_id: Optional[int]) -> list:
        """
        Get list of pages user can access based on role
        
        IMPORTANT: ALWAYS show ALL transaction pages in sidebar
        Permission checking happens when user clicks the page
        """
        user_role = _normalize_role(user_role)
        
        base_pages = []
        
        # Always available
        base_pages.extend([
            "🏠 Home",
            "📊 Dashboard",
        ])
        
        # ========== ALWAYS SHOW ALL TRANSACTION PAGES ==========
        # Permission check happens on page access, not in sidebar
        base_pages.extend([
            "🛢️ Tank Transactions",
            "🚢 YADE Transactions",
            "🚚 Tanker Transactions",
            "🚢 OTR-Vessel",
        ])
        
        # Reports (available to all)
        base_pages.extend([
            "📈 Reports",
            "📋 Out-Turn Report (OTR)",
            "📊 TOA-Yade",
            "📊 Material Balance",
        ])
        
        # MANAGEMENT PAGES - ONLY ADMIN
        if user_role == "admin-operations":
            base_pages.extend([
                "➕ Add Asset",
                "📍 Manage Locations",
                "👥 Manage Users",
                "⚙️ Location Settings",
                "💾 Backup & Recovery",
                "📜 Audit Log",
            ])
        
        # User settings (available to all)
        base_pages.extend([
            "🔐 2FA Settings",
            "🔍 Login History",
        ])
        
        return base_pages
    
    @staticmethod
    def get_permission_summary(session: Session, location_id: int) -> str:
        """Get human-readable permission summary for a location"""
        
        permissions = PermissionManager.get_location_permissions(session, location_id)
        
        if permissions.get("is_head_office"):
            return "🏢 Head Office (Full Access to All Features)"
        
        allowed = []
        if permissions.get("tank_transactions"):
            allowed.append("Tank Transactions")
        if permissions.get("yade_transactions"):
            allowed.append("YADE Transactions")
        if permissions.get("tanker_transactions"):
            allowed.append("Tanker Transactions")
        if permissions.get("otr_vessel"):
            allowed.append("OTR-Vessel")
        
        if allowed:
            return "✅ " + ", ".join(allowed)
        else:
            return "❌ No transaction permissions"
    
    @staticmethod
    def get_allowed_locations_for_feature(session: Session, feature: str) -> list:
        """Get list of location names where a feature is allowed"""
        
        locations = session.query(Location).filter(Location.is_active == True).all()
        allowed = []
        
        for loc in locations:
            permissions = PermissionManager.get_location_permissions(session, loc.id)
            if permissions.get(feature, False) or permissions.get("is_head_office", False):
                allowed.append(loc.name)
        
        return allowed
