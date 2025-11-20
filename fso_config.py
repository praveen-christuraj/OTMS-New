# fso_config.py
"""
FSO Vessel Configuration
Maps locations to their FSO vessels

IMPORTANT:
- Only 2 locations have FSO operations: AGGE and OML-13 (Utapate)
- AGGE → MT TULJA TANVI
- OML-13 (Utapate) → MT TULJA KALYANI
- Admin-Operations can access both FSO vessels
- All other locations: NO FSO access
"""

from typing import Dict, List

class FSOConfig:
    """FSO Vessel configuration manager"""
    
    # ✅ ONLY 2 FSO LOCATIONS
    FSO_VESSELS = {
        "AGGE": ["MT TULJA TANVI"],        # Agge has MT TULJA TANVI
        "OML-13": ["MT TULJA KALYANI"],    # Utapate has MT TULJA KALYANI
    }
    
    # All FSO vessels in the system
    ALL_FSO_VESSELS = [
        "MT TULJA TANVI",
        "MT TULJA KALYANI"
    ]
    
    @staticmethod
    def get_fso_for_location(location_code: str) -> List[str]:
        """
        Get FSO vessel(s) for a location
        
        Args:
            location_code: Location code (e.g., 'AGGE', 'OML-13')
            
        Returns:
            List[str]: List of FSO vessel names for this location (empty if no FSO)
        """
        if not location_code:
            return []
        
        code = location_code.upper()
        
        # Only AGGE and OML-13 have FSO vessels
        return FSOConfig.FSO_VESSELS.get(code, [])
    
    @staticmethod
    def get_all_fso_vessels() -> List[str]:
        """Get all available FSO vessels (for admin selection)"""
        return FSOConfig.ALL_FSO_VESSELS
    
    @staticmethod
    def can_select_fso(user: Dict, location_code: str) -> bool:
        """
        Check if user can select FSO vessel
        
        Rules:
        - Admin: Can select FSO at AGGE and OML-13 (sees both FSO vessels)
        - Regular users at AGGE: Auto-assigned MT TULJA TANVI (no selection)
        - Regular users at OML-13: Auto-assigned MT TULJA KALYANI (no selection)
        - All other locations: No FSO access at all
        
        Args:
            user: User dictionary with role and permissions
            location_code: Current location code
            
        Returns:
            bool: True if user can select FSO, False if auto-assigned or no access
        """
        if not user:
            return False
        
        role = user.get("role", "").lower()
        
        # Only admin-operations can select FSO vessels (managers are read-only)
        if role == "admin-operations":
            return True
        
        # Regular users get auto-assigned (no selection)
        return False
    
    @staticmethod
    def get_default_fso(location_code: str) -> str:
        """
        Get default FSO for a location
        
        Args:
            location_code: Location code
            
        Returns:
            str: Default FSO vessel name (empty string if no FSO at this location)
        """
        vessels = FSOConfig.get_fso_for_location(location_code)
        return vessels[0] if vessels else ""
    
    @staticmethod
    def has_fso_operations(location_code: str) -> bool:
        """
        Check if location has FSO operations enabled
        
        Only AGGE and OML-13 have FSO operations
        
        Args:
            location_code: Location code
            
        Returns:
            bool: True if location has FSO operations, False otherwise
        """
        if not location_code:
            return False
        
        code = location_code.upper()
        return code in FSOConfig.FSO_VESSELS
    
    @staticmethod
    def get_fso_locations() -> Dict[str, str]:
        """
        Get all locations with FSO operations and their vessels
        
        Returns:
            Dict[str, str]: Location code → FSO vessel name
        """
        return {
            "AGGE": "MT TULJA TANVI",
            "OML-13": "MT TULJA KALYANI"
        }
    
    @staticmethod
    def get_location_name_for_fso(location_code: str) -> str:
        """Get friendly location name for FSO location"""
        names = {
            "AGGE": "Agge",
            "OML-13": "Utapate"
        }
        return names.get(location_code.upper(), location_code)
