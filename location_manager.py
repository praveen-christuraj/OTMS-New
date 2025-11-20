# location_manager.py
"""
Location management utilities for OTMS.
"""

from typing import Optional, List, Dict
from sqlalchemy.orm import Session
from models import Location

class LocationManager:
    """Handles location CRUD operations"""
    
    @staticmethod
    def create_location(
        session: Session,
        name: str,
        code: str,
        address: Optional[str] = None
    ) -> Dict:
        """
        Create a new location.
        Returns a dictionary (not the ORM object) to avoid session detachment issues.
        """
        
        # Validate uniqueness
        existing = session.query(Location).filter(
            (Location.name == name) | (Location.code == code)
        ).first()
        
        if existing:
            if existing.name == name:
                raise ValueError(f"Location name '{name}' already exists")
            if existing.code == code:
                raise ValueError(f"Location code '{code}' already exists")
        
        location = Location(
            name=name,
            code=code.upper(),
            address=address,
            is_active=True
        )
        
        session.add(location)
        session.commit()
        
        # Return a dict instead of the ORM object
        return {
            "id": location.id,
            "name": location.name,
            "code": location.code,
            "address": location.address,
            "is_active": location.is_active
        }
    
    @staticmethod
    def get_all_locations(session: Session, active_only: bool = True) -> List[Location]:
        """Get all locations"""
        query = session.query(Location)
        if active_only:
            query = query.filter(Location.is_active == True)
        return query.order_by(Location.name).all()
    
    @staticmethod
    def get_location_by_id(session: Session, location_id: int) -> Optional[Location]:
        """Get location by ID"""
        return session.query(Location).filter(Location.id == location_id).one_or_none()
    
    @staticmethod
    def update_location(
        session: Session,
        location_id: int,
        name: Optional[str] = None,
        code: Optional[str] = None,
        address: Optional[str] = None,
        is_active: Optional[bool] = None
    ) -> Dict:
        """
        Update location details.
        Returns a dictionary to avoid session detachment issues.
        """
        
        location = session.query(Location).filter(Location.id == location_id).one_or_none()
        if not location:
            raise ValueError(f"Location ID {location_id} not found")
        
        if name:
            location.name = name
        if code:
            location.code = code.upper()
        if address is not None:
            location.address = address
        if is_active is not None:
            location.is_active = is_active
        
        session.commit()
        
        # Return a dict instead of the ORM object
        return {
            "id": location.id,
            "name": location.name,
            "code": location.code,
            "address": location.address,
            "is_active": location.is_active
        }
    
    @staticmethod
    def delete_location(session: Session, location_id: int):
        """
        Soft delete a location (mark as inactive).
        Does not actually delete to preserve data integrity.
        """
        location = session.query(Location).filter(Location.id == location_id).one_or_none()
        if not location:
            raise ValueError(f"Location ID {location_id} not found")
        
        location.is_active = False
        session.commit()
    
    @staticmethod
    def get_location_stats(session: Session, location_id: int) -> Dict:
        """Get statistics for a location"""
        from models import Tank, TankTransaction, YadeVoyage
        # NOTE: YadeBarge is NOT counted per location anymore (it's shared)
        
        stats = {
            "tanks": session.query(Tank).filter(Tank.location_id == location_id).count(),
            # REMOVED: yade_barges count (no longer location-specific)
            "tank_transactions": session.query(TankTransaction).filter(TankTransaction.location_id == location_id).count(),
            "yade_voyages": session.query(YadeVoyage).filter(YadeVoyage.location_id == location_id).count(),
        }
        
        return stats

    @staticmethod
    def permanently_delete_location(session: Session, location_id: int) -> Dict:
        """
        PERMANENTLY delete a location and ALL associated data.
        NOTE: YADE barges and calibration are NOT deleted (they're shared).
        """
        from models import (
            Tank, TankTransaction, YadeVoyage, YadeDip,
            CalibrationTank, OTRRecord,
            TOAYadeSummary, TOAYadeStage, YadeSampleParam, YadeSealDetail
        )
        # NOTE: YadeBarge and YadeCalibration are NOT included (shared across locations)
        
        location = session.query(Location).filter(Location.id == location_id).one_or_none()
        if not location:
            raise ValueError(f"Location ID {location_id} not found")
        
        # Gather statistics before deletion
        stats = {
            "location_name": location.name,
            "location_code": location.code,
            "tanks_deleted": 0,
            "tank_transactions_deleted": 0,
            # REMOVED: yade_barges_deleted (not deleted, shared)
            "yade_voyages_deleted": 0,
            "calibration_records_deleted": 0,
            "otr_records_deleted": 0,
        }
        
        # Count before deletion
        stats["tanks_deleted"] = session.query(Tank).filter(Tank.location_id == location_id).count()
        stats["tank_transactions_deleted"] = session.query(TankTransaction).filter(TankTransaction.location_id == location_id).count()
        stats["yade_voyages_deleted"] = session.query(YadeVoyage).filter(YadeVoyage.location_id == location_id).count()
        stats["calibration_records_deleted"] = session.query(CalibrationTank).filter(CalibrationTank.location_id == location_id).count()
        stats["otr_records_deleted"] = session.query(OTRRecord).filter(OTRRecord.location_id == location_id).count()
        
        # Delete all associated data (in correct order to avoid FK constraint violations)
        
        # 1. Delete TOA/YADE related data (via voyage cascade)
        voyage_ids = [v.id for v in session.query(YadeVoyage).filter(YadeVoyage.location_id == location_id).all()]
        if voyage_ids:
            session.query(TOAYadeSummary).filter(TOAYadeSummary.voyage_id.in_(voyage_ids)).delete(synchronize_session=False)
            session.query(TOAYadeStage).filter(TOAYadeStage.voyage_id.in_(voyage_ids)).delete(synchronize_session=False)
            session.query(YadeSampleParam).filter(YadeSampleParam.voyage_id.in_(voyage_ids)).delete(synchronize_session=False)
            session.query(YadeSealDetail).filter(YadeSealDetail.voyage_id.in_(voyage_ids)).delete(synchronize_session=False)
            session.query(YadeDip).filter(YadeDip.voyage_id.in_(voyage_ids)).delete(synchronize_session=False)
        
        # 2. Delete YADE voyages
        session.query(YadeVoyage).filter(YadeVoyage.location_id == location_id).delete(synchronize_session=False)
        
        # NOTE: NOT deleting YadeBarge or YadeCalibration (shared)
        
        # 3. Delete tank transactions
        session.query(TankTransaction).filter(TankTransaction.location_id == location_id).delete(synchronize_session=False)
        
        # 4. Delete OTR records
        session.query(OTRRecord).filter(OTRRecord.location_id == location_id).delete(synchronize_session=False)
        
        # 5. Delete tank calibration
        session.query(CalibrationTank).filter(CalibrationTank.location_id == location_id).delete(synchronize_session=False)
        
        # 6. Delete tanks
        session.query(Tank).filter(Tank.location_id == location_id).delete(synchronize_session=False)
        
        # 7. Finally, delete the location itself
        session.delete(location)
        
        # Commit all deletions
        session.commit()
        
        return stats

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
    
    # Don't allow deleting the last admin-operations
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