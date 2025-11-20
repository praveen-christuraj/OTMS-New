# backup_manager.py
"""
Automated backup and recovery system for OTMS.
Handles database backups, restoration, and data export/import.
"""

import shutil
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import json
import zipfile
from io import BytesIO

class BackupManager:
    """Manages database backups and recovery"""
    
    BACKUP_DIR = Path("backups")
    DB_PATH = Path("otms.db")
    
    def __init__(self):
        self.BACKUP_DIR.mkdir(exist_ok=True)
    
    @staticmethod
    def create_backup(description: Optional[str] = None, backup_type: str = "manual") -> Dict:
        """
        Create a new database backup.
        Returns backup info dict.
        """
        BackupManager.BACKUP_DIR.mkdir(exist_ok=True)
        
        if not BackupManager.DB_PATH.exists():
            raise FileNotFoundError("Database file not found")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"otms_backup_{timestamp}.db"
        backup_path = BackupManager.BACKUP_DIR / backup_name
        
        # Copy database file
        shutil.copy2(BackupManager.DB_PATH, backup_path)
        
        # Get file size
        size_bytes = backup_path.stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        
        # Create metadata file
        metadata = {
            "timestamp": timestamp,
            "datetime": datetime.now().isoformat(),
            "type": backup_type,  # "manual", "auto", "before_migration"
            "description": description or f"{backup_type.title()} backup",
            "filename": backup_name,
            "size_bytes": size_bytes,
            "size_mb": round(size_mb, 2),
            "created_by": "system"
        }
        
        metadata_path = BackupManager.BACKUP_DIR / f"otms_backup_{timestamp}.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        return metadata
    
    @staticmethod
    def list_backups(limit: Optional[int] = None) -> List[Dict]:
        """List all available backups with metadata"""
        backups = []
        
        if not BackupManager.BACKUP_DIR.exists():
            return backups
        
        # Find all .db backup files
        for backup_file in sorted(BackupManager.BACKUP_DIR.glob("otms_backup_*.db"), reverse=True):
            timestamp_str = backup_file.stem.replace("otms_backup_", "")
            metadata_file = BackupManager.BACKUP_DIR / f"otms_backup_{timestamp_str}.json"
            
            if metadata_file.exists():
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
            else:
                # Create basic metadata if json doesn't exist
                stat = backup_file.stat()
                metadata = {
                    "timestamp": timestamp_str,
                    "datetime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "type": "unknown",
                    "description": "Legacy backup (no metadata)",
                    "filename": backup_file.name,
                    "size_bytes": stat.st_size,
                    "size_mb": round(stat.st_size / (1024 * 1024), 2),
                    "created_by": "unknown"
                }
            
            metadata["path"] = str(backup_file)
            backups.append(metadata)
        
        if limit:
            return backups[:limit]
        return backups
    
    @staticmethod
    def restore_backup(backup_timestamp: str, create_backup_before: bool = True) -> Dict:
        """
        Restore database from a backup.
        Returns restoration info dict.
        """
        backup_file = BackupManager.BACKUP_DIR / f"otms_backup_{backup_timestamp}.db"
        
        if not backup_file.exists():
            raise FileNotFoundError(f"Backup file not found: {backup_file}")
        
        # Create a backup before restoration
        if create_backup_before:
            pre_restore_backup = BackupManager.create_backup(
                description=f"Before restoration of {backup_timestamp}",
                backup_type="before_restore"
            )
        
        # Close any open connections (important!)
        # This is handled by SQLAlchemy session management in Streamlit
        
        # Restore the backup
        shutil.copy2(backup_file, BackupManager.DB_PATH)
        
        return {
            "restored_from": backup_timestamp,
            "restored_at": datetime.now().isoformat(),
            "pre_restore_backup": pre_restore_backup if create_backup_before else None
        }
    
    @staticmethod
    def delete_backup(backup_timestamp: str):
        """Delete a backup and its metadata"""
        backup_file = BackupManager.BACKUP_DIR / f"otms_backup_{backup_timestamp}.db"
        metadata_file = BackupManager.BACKUP_DIR / f"otms_backup_{backup_timestamp}.json"
        
        if backup_file.exists():
            backup_file.unlink()
        if metadata_file.exists():
            metadata_file.unlink()
    
    @staticmethod
    def cleanup_old_backups(days: int = 30, keep_minimum: int = 5):
        """
        Delete backups older than specified days.
        Always keeps at least keep_minimum backups.
        """
        backups = BackupManager.list_backups()
        
        if len(backups) <= keep_minimum:
            return {"deleted": 0, "kept": len(backups)}
        
        cutoff = datetime.now() - timedelta(days=days)
        deleted = 0
        
        # Sort by date, keep newest
        for backup in backups[keep_minimum:]:
            backup_date = datetime.fromisoformat(backup["datetime"])
            if backup_date < cutoff:
                BackupManager.delete_backup(backup["timestamp"])
                deleted += 1
        
        return {"deleted": deleted, "kept": len(backups) - deleted}
    
    @staticmethod
    def get_backup_info(backup_timestamp: str) -> Optional[Dict]:
        """Get detailed info about a specific backup"""
        backup_file = BackupManager.BACKUP_DIR / f"otms_backup_{backup_timestamp}.db"
        
        if not backup_file.exists():
            return None
        
        # Get basic file info
        stat = backup_file.stat()
        
        # Try to get metadata
        metadata_file = BackupManager.BACKUP_DIR / f"otms_backup_{backup_timestamp}.json"
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                info = json.load(f)
        else:
            info = {
                "timestamp": backup_timestamp,
                "datetime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "filename": backup_file.name,
                "size_bytes": stat.st_size,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
            }
        
        # Get table counts from backup
        try:
            conn = sqlite3.connect(backup_file)
            cursor = conn.cursor()
            
            # Count records in key tables
            tables_info = {}
            for table in ["locations", "users", "tanks", "yade_barges", "tank_transaction", "yade_voyage", "otr"]:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    tables_info[table] = count
                except Exception:
                    tables_info[table] = "N/A"
            
            conn.close()
            info["tables"] = tables_info
        except Exception:
            info["tables"] = {}
        
        info["path"] = str(backup_file)
        return info
    
    @staticmethod
    def export_location_data(location_id: int, output_path: Optional[Path] = None) -> Path:
        """
        Export all data for a specific location to a ZIP file.
        Includes tanks, transactions, calibrations, etc.
        """
        from db import get_session
        from models import (
            Location, Tank, TankTransaction, YadeVoyage, 
            CalibrationTank, OTRRecord
        )
        import pandas as pd
        
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = BackupManager.BACKUP_DIR / f"location_export_{location_id}_{timestamp}.zip"
        
        with get_session() as s:
            # Get location info
            loc = s.query(Location).filter(Location.id == location_id).one_or_none()
            if not loc:
                raise ValueError(f"Location {location_id} not found")
            
            # Create temporary directory for exports
            temp_dir = BackupManager.BACKUP_DIR / "temp_export"
            temp_dir.mkdir(exist_ok=True)
            
            try:
                # Export location info
                loc_data = {
                    "id": loc.id,
                    "name": loc.name,
                    "code": loc.code,
                    "address": loc.address,
                    "is_active": loc.is_active
                }
                with open(temp_dir / "location.json", 'w') as f:
                    json.dump(loc_data, f, indent=2)
                
                # Export tanks
                tanks = s.query(Tank).filter(Tank.location_id == location_id).all()
                if tanks:
                    df = pd.DataFrame([{
                        "name": t.name,
                        "capacity_bbl": t.capacity_bbl,
                        "product": t.product,
                        "status": t.status.value if t.status else None
                    } for t in tanks])
                    df.to_csv(temp_dir / "tanks.csv", index=False)
                
                # Export tank transactions
                txs = s.query(TankTransaction).filter(TankTransaction.location_id == location_id).all()
                if txs:
                    df = pd.DataFrame([{
                        "ticket_id": tx.ticket_id,
                        "tank_name": tx.tank_name,
                        "operation": tx.operation.value if tx.operation else None,
                        "date": tx.date,
                        "time": tx.time,
                        "dip_cm": tx.dip_cm,
                        "water_cm": tx.water_cm,
                        "tank_temp_c": tx.tank_temp_c,
                        "tank_temp_f": tx.tank_temp_f,
                        "api_observed": tx.api_observed,
                        "density_observed": tx.density_observed,
                        "sample_temp_c": tx.sample_temp_c,
                        "sample_temp_f": tx.sample_temp_f,
                        "bsw_pct": tx.bsw_pct,
                        "qty_bbls": tx.qty_bbls,
                        "remarks": tx.remarks
                    } for tx in txs])
                    df.to_csv(temp_dir / "tank_transactions.csv", index=False)
                
                # Export OTR records
                otrs = s.query(OTRRecord).filter(OTRRecord.location_id == location_id).all()
                if otrs:
                    df = pd.DataFrame([{
                        "ticket_id": o.ticket_id,
                        "tank_id": o.tank_id,
                        "date": o.date,
                        "time": o.time,
                        "operation": o.operation,
                        "dip_cm": o.dip_cm,
                        "total_volume_bbl": o.total_volume_bbl,
                        "water_cm": o.water_cm,
                        "free_water_bbl": o.free_water_bbl,
                        "gov_bbl": o.gov_bbl,
                        "api60": o.api60,
                        "vcf": o.vcf,
                        "gsv_bbl": o.gsv_bbl,
                        "bsw_vol_bbl": o.bsw_vol_bbl,
                        "nsv_bbl": o.nsv_bbl,
                        "lt": o.lt,
                        "mt": o.mt
                    } for o in otrs])
                    df.to_csv(temp_dir / "otr_records.csv", index=False)
                
                # Export YADE voyages
                voyages = s.query(YadeVoyage).filter(YadeVoyage.location_id == location_id).all()
                if voyages:
                    df = pd.DataFrame([{
                        "yade_name": v.yade_name,
                        "voyage_no": v.voyage_no,
                        "convoy_no": v.convoy_no,
                        "date": v.date,
                        "time": v.time,
                        "cargo": v.cargo.value if hasattr(v.cargo, 'value') else v.cargo,
                        "destination": v.destination.value if hasattr(v.destination, 'value') else v.destination,
                        "loading_berth": v.loading_berth.value if hasattr(v.loading_berth, 'value') else v.loading_berth
                    } for v in voyages])
                    df.to_csv(temp_dir / "yade_voyages.csv", index=False)
                
                # Export tank calibration
                cals = s.query(CalibrationTank).filter(CalibrationTank.location_id == location_id).all()
                if cals:
                    df = pd.DataFrame([{
                        "tank_name": c.tank_name,
                        "dip_cm": c.dip_cm,
                        "volume_bbl": c.volume_bbl
                    } for c in cals])
                    df.to_csv(temp_dir / "tank_calibration.csv", index=False)
                
                # Create ZIP file
                with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for file in temp_dir.glob("*"):
                        zipf.write(file, file.name)
                
            finally:
                # Cleanup temp directory
                shutil.rmtree(temp_dir, ignore_errors=True)
        
        return output_path