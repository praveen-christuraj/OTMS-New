# setup_fso_permissions.py
"""
Configure FSO-Operations permissions for Agge, Utapate, and Lagos (HO)
Run this ONCE
"""

from db import get_session
from location_manager import LocationManager
from location_config import LocationConfig

def setup_fso_permissions():
    """Enable FSO-Operations for specific locations"""
    
    # Updated to include all variations of location codes
    FSO_LOCATIONS = {
        "AGGE": True,
        "OML-13": True,      # Utapate
        "UTAPATE": True,     # Utapate alternative
        "LAGOS": True,
        "HO": True,
        "LAGOS (HO)": True
    }
    
    with get_session() as s:
        locations = LocationManager.get_all_locations(s, active_only=True)
        
        for loc in locations:
            code = loc.code.upper()
            name = loc.name.upper()
            
            # Check if location should have FSO access
            should_enable = False
            
            # Check code matches
            if code in FSO_LOCATIONS:
                should_enable = True
            
            # Check if name contains key terms
            if "UTAPATE" in name or "OML-13" in code or "OML-13" in name:
                should_enable = True
            
            if "AGGE" in name or "AGGE" in code:
                should_enable = True
            
            if "LAGOS" in name or "LAGOS" in code:
                should_enable = True
            
            if should_enable:
                # Get current config
                config = LocationConfig.get_config(s, loc.id)
                
                # Add FSO permission
                if "permissions" not in config:
                    config["permissions"] = {}
                
                config["permissions"]["fso_operations"] = True
                
                # Save
                LocationConfig.save_config(s, loc.id, config)
                
                print(f"✅ Enabled FSO-Operations for {loc.name} ({code})")
            else:
                print(f"⏭️  Skipped {loc.name} ({code})")
        
        s.commit()
        print("\n✅ FSO-Operations permissions configured!")
        print("\n📋 Summary:")
        print("   - Agge: ✅ Enabled")
        print("   - Utapate (OML-13): ✅ Enabled")
        print("   - Lagos (HO): ✅ Enabled")

if __name__ == "__main__":
    print("🔧 Setting up FSO-Operations permissions...")
    print("=" * 60)
    setup_fso_permissions()
    print("=" * 60)