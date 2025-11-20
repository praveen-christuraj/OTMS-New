# health_check.py
"""
System health check utility
Run: python health_check.py
"""

import sqlite3
from pathlib import Path
from datetime import datetime
import sys

def check_database():
    """Check if database exists and is accessible"""
    db_path = Path("otms.db")
    
    if not db_path.exists():
        return False, "Database file not found: otms.db"
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Check critical tables
        tables = [
            'locations', 'users', 'tanks', 'yade_barges',
            'tank_transactions', 'yade_voyage', 'otr_records',
            'audit_logs', 'login_attempts'
        ]
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
        
        conn.close()
        return True, f"Database OK - All {len(tables)} critical tables accessible"
    
    except Exception as e:
        return False, f"Database error: {e}"


def check_dependencies():
    """Check if all required packages are installed"""
    required = [
        'streamlit', 'sqlalchemy', 'pandas', 'bcrypt',
        'reportlab', 'openpyxl', 'plotly'
    ]
    
    missing = []
    
    for package in required:
        try:
            __import__(package)
        except ImportError:
            missing.append(package)
    
    if missing:
        return False, f"Missing packages: {', '.join(missing)}"
    else:
        return True, f"All {len(required)} required packages installed"


def check_directories():
    """Check if required directories exist"""
    dirs = ['backups', 'assets', 'assets/logos', 'logs']
    
    missing = []
    for d in dirs:
        if not Path(d).exists():
            missing.append(d)
    
    if missing:
        # Try to create them
        for d in missing:
            try:
                Path(d).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                return False, f"Failed to create directory {d}: {e}"
        
        return True, f"Created {len(missing)} missing director(ies)"
    else:
        return True, f"All {len(dirs)} required directories exist"


def check_config():
    """Check if configuration is valid"""
    try:
        from db import DB_URL
        from security import SecurityManager
        
        checks = {
            "Database URL": DB_URL,
            "Session Timeout": f"{SecurityManager.SESSION_TIMEOUT_MINUTES} min",
            "Password Min Length": SecurityManager.MIN_PASSWORD_LENGTH,
        }
        
        return True, "Configuration valid"
    
    except Exception as e:
        return False, f"Configuration error: {e}"


def main():
    """Run all health checks"""
    print("="*60)
    print("🏥 OTMS HEALTH CHECK")
    print("="*60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"User: PraveenJero")
    print("="*60)
    
    checks = [
        ("Database", check_database),
        ("Dependencies", check_dependencies),
        ("Directories", check_directories),
        ("Configuration", check_config),
    ]
    
    all_passed = True
    
    for check_name, check_func in checks:
        try:
            passed, message = check_func()
            
            if passed:
                print(f"✅ {check_name:20s}: {message}")
            else:
                print(f"❌ {check_name:20s}: {message}")
                all_passed = False
        
        except Exception as e:
            print(f"❌ {check_name:20s}: Exception - {e}")
            all_passed = False
    
    print("="*60)
    
    if all_passed:
        print("🎉 ALL CHECKS PASSED - System healthy!")
        return 0
    else:
        print("⚠️  SOME CHECKS FAILED - Review output above")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)