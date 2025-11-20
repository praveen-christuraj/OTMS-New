# location_config.py
"""
Location-specific configuration management.
Allows each location to have customized settings for operations, validations, etc.
"""

from typing import Dict, Any
import json
from sqlalchemy.orm import Session


# ==================== DEFAULT CONFIGURATION ====================
DEFAULT_CONFIG = {
    "page_visibility": {
        "show_tank_transactions": True,
        "show_tanker_transactions": False,  # Enabled only for specific locations
        "show_yade_transactions": False,    # Enabled only for specific locations
        "show_toa_yade": False,             # Enabled only for specific locations
    },
    "page_access": {
        "Tank Transactions": True,
        "Yade Transactions": True,
        "Tanker Transactions": True,
        "Yade Tracking": True,
        "Yade-Vessel Mapping": True,
        "Convoy Status": True,
        "OTR-Vessel": True,
        "FSO-Operations": True,
        "TOA-Yade": True,
        "OTR": True,
        "BCCR": True,
        "Material Balance": True,
        "Reporting": True,
    },
    "tabs_access": {
        "Tank Transactions": {
            "Tank Transactions": True,
            "Meter Transactions": True,
            "River Draft": True,
            "Produced Water": True,
            "Condensate Records": True,
            "Production": True
        },
        "FSO-Operations": {
            "📊 OTR": True,
            "📈 Material Balance": True
        },
        "BCCR": {
            "Mapping": True,
            "BCCR Report": True
        },
        "Yade-Vessel Mapping": {
            "Mapping": True,
            "Comparison": True
        }
    },
    "tank_transactions": {
        "enabled_operations": [
            "Opening Stock",
            "Receipt",
            "Receipt from Agu",
            "Receipt from OFS",
            "OKW Receipt",
            "ANZ Receipt",
            "Other Receipts",
            "ITT - Receipt",
            "Dispatch to barge",
            "Other Dispatch",
            "ITT - Dispatch",
            "Settling",
            "Draining"
        ],
        "product_types": [
            "CRUDE",
            "CONDENSATE",
            "DPK",
            "AGO",
            "PMS"
        ],
        "max_days_backward": 30,
        "allow_future_dates": False,
        "auto_generate_ticket_id": True,
        "ticket_id_prefix": ""
    },
    "yade_transactions": {
        "enabled_cargo_types": ["OKW", "ANZ", "CONDENSATE", "CRUDE"],
        "enabled_destinations": [
            "NEMBE CK", "BONNY", "BRASS", "FORCADOS",
            "ESCRAVOS", "WARRI", "PORT HARCOURT"
        ],
        "enabled_loading_berths": ["BERTH 1", "BERTH 2", "BERTH 3"],
        "enable_seal_tracking": True,
        "auto_generate_voyage_no": False
    },
    "otr": {
        "auto_calculate_volumes": True,
        "require_calibration_data": True,
        "enable_temperature_correction": True,
        "decimal_precision": 2,
        "volume_unit": "BBL",
        "temperature_unit": "C"
    },
    "otr_vessel": {
        "preferred_vessel_ids": []
    },
    "ui_customization": {
        "show_quick_entry_mode": True,
        "enable_bulk_upload": False,
        "default_date": "today"
    }
}


# ==================== LocationConfig CLASS ====================
class LocationConfig:
    """Manage location-specific configurations"""
    
    @staticmethod
    def get_config(session: Session, location_id: int) -> Dict[str, Any]:
        """
        Get configuration for a specific location.
        Applies location-specific overrides based on location code.
        """
        from models import Location, LocationConfiguration
        
        # Start with default config
        config = DEFAULT_CONFIG.copy()
        
        # Deep copy nested dicts to avoid mutation
        config["page_visibility"] = DEFAULT_CONFIG["page_visibility"].copy()
        config["tank_transactions"] = DEFAULT_CONFIG["tank_transactions"].copy()
        config["yade_transactions"] = DEFAULT_CONFIG["yade_transactions"].copy()
        config["otr"] = DEFAULT_CONFIG["otr"].copy()
        config["otr_vessel"] = DEFAULT_CONFIG["otr_vessel"].copy()
        config["ui_customization"] = DEFAULT_CONFIG["ui_customization"].copy()
        
        # Load from database if exists
        db_config = session.query(LocationConfiguration).filter(
            LocationConfiguration.location_id == location_id
        ).one_or_none()
        
        if db_config and db_config.config_json:
            try:
                stored_config = json.loads(db_config.config_json)
                # Deep merge stored config into default
                for key, value in stored_config.items():
                    if isinstance(value, dict) and key in config:
                        config[key].update(value)
                    else:
                        config[key] = value
            except Exception:
                pass  # Use default if parsing fails
        
        # Location-specific overrides (by code)
        loc = session.query(Location).filter(Location.id == location_id).one_or_none()
        if loc:
            code = loc.code.upper()
            
            # TANKER LOCATIONS (Ndoni, Aggu, Oguali, Ogini)
            if code in ["NDONI", "AGGU", "OGUALI", "OGINI"]:
                config["page_visibility"]["show_tanker_transactions"] = True
            
            # YADE LOCATIONS (Ndoni only for now)
            if code == "NDONI":
                config["page_visibility"]["show_yade_transactions"] = True
                config["page_visibility"]["show_toa_yade"] = True
            else:
                config["page_visibility"]["show_yade_transactions"] = False
                config["page_visibility"]["show_toa_yade"] = False
            
            # Tank transactions (enabled for all locations by default)
            config["page_visibility"]["show_tank_transactions"] = True
        
        return config
    
    @staticmethod
    def save_config(session: Session, location_id: int, config: Dict[str, Any]) -> bool:
        """Save configuration for a location"""
        from models import LocationConfiguration
        
        try:
            db_config = session.query(LocationConfiguration).filter(
                LocationConfiguration.location_id == location_id
            ).one_or_none()
            
            config_json = json.dumps(config)
            
            if db_config:
                db_config.config_json = config_json
            else:
                db_config = LocationConfiguration(
                    location_id=location_id,
                    config_json=config_json
                )
                session.add(db_config)
            
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            raise e
    
    @staticmethod
    def reset_to_default(session: Session, location_id: int) -> bool:
        """Reset location configuration to default"""
        from models import LocationConfiguration
        
        try:
            db_config = session.query(LocationConfiguration).filter(
                LocationConfiguration.location_id == location_id
            ).one_or_none()
            
            if db_config:
                session.delete(db_config)
                session.commit()
            
            return True
        except Exception as e:
            session.rollback()
            raise e
    
    @staticmethod
    def get_enabled_operations(session: Session, location_id: int) -> list:
        """Get list of enabled operations for a location"""
        config = LocationConfig.get_config(session, location_id)
        return config.get("tank_transactions", {}).get("enabled_operations", [])
    
    @staticmethod
    def is_operation_enabled(session: Session, location_id: int, operation: str) -> bool:
        """Check if a specific operation is enabled for a location"""
        enabled_ops = LocationConfig.get_enabled_operations(session, location_id)
        return operation in enabled_ops
    
    @staticmethod
    def enable_tanker_transactions_for_location(session: Session, location_code: str) -> bool:
        """
        Enable tanker transactions for a specific location.
        Helper method for one-time setup.
        """
        from models import Location
        
        loc = session.query(Location).filter(Location.code == location_code).one_or_none()
        if not loc:
            return False
        
        config = LocationConfig.get_config(session, loc.id)
        
        # Enable tanker transactions
        config["page_visibility"]["show_tanker_transactions"] = True
        
        LocationConfig.save_config(session, loc.id, config)
        return True


# ==================== UTILITY FUNCTIONS ====================
def setup_tanker_locations():
    """
    One-time setup to enable tanker transactions for the 4 locations.
    Run this once after adding locations (or it will auto-enable via get_config).
    """
    from db import get_session
    
    tanker_locations = ["NDONI", "AGGU", "OGUALI", "OGINI"]
    
    with get_session() as s:
        for code in tanker_locations:
            result = LocationConfig.enable_tanker_transactions_for_location(s, code)
            if result:
                print(f"✅ Enabled tanker transactions for {code}")
            else:
                print(f"⚠️ Location {code} not found")
        s.commit()


def get_location_page_visibility(session: Session, location_id: int) -> Dict[str, bool]:
    """
    Quick helper to get page visibility settings for a location.
    Returns: {"show_tank_transactions": bool, "show_tanker_transactions": bool, ...}
    """
    config = LocationConfig.get_config(session, location_id)
    return config.get("page_visibility", {})
