# backup_scheduler.py
"""
Automated backup scheduler - runs daily backups.
Run this as a separate process or scheduled task.
"""

import schedule
import time
from datetime import datetime
from backup_manager import BackupManager

def run_daily_backup():
    """Perform daily automated backup"""
    try:
        print(f"\n{'='*60}")
        print(f"🔄 Starting automated backup: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
        backup_info = BackupManager.create_backup(
            description="Automated daily backup",
            backup_type="auto"
        )
        
        print(f"✅ Backup created successfully!")
        print(f"   Filename: {backup_info['filename']}")
        print(f"   Size: {backup_info['size_mb']} MB")
        print(f"   Location: backups/{backup_info['filename']}")
        
        # Log audit entry for this automated backup
        # We attempt to record the backup operation in the audit log using the
        # SecurityManager. If the audit logging infrastructure is not
        # available (e.g. outside of the full application context), we catch
        # and report the exception so the backup can still complete.
        try:
            # Import session management and security logging only when needed
            from db import get_session  # type: ignore
            from security import SecurityManager  # type: ignore

            with get_session() as _session:
                # Log the backup with a system user. The action uses the same
                # code as manual backups (CREATE_BACKUP) so that all backups
                # appear consistently in the audit log. Details identify this
                # as an automated hourly backup.
                SecurityManager.log_audit(
                    _session,
                    "system",  # username; could be replaced with a service account
                    "CREATE_BACKUP",
                    details="Automated hourly backup",
                    user_id=None,
                    location_id=None,
                    ip_address=None,
                )
        except Exception as log_ex:
            # If logging fails, print an informative message and continue
            print(f"⚠️  Failed to write audit log: {log_ex}")

        # Cleanup old backups (keep last 30 days, minimum 5 backups)
        cleanup_result = BackupManager.cleanup_old_backups(days=30, keep_minimum=5)
        print(f"   Cleanup: Deleted {cleanup_result['deleted']} old backup(s)")
        
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"❌ Backup failed: {e}")

def main():
    """Main scheduler loop"""
    print("=" * 60)
    print("📅 OTMS Backup Scheduler Started")
    print("=" * 60)
    print(f"Schedule: Daily at 02:00 AM")
    print(f"Retention: 30 days (minimum 5 backups)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Schedule daily backup at 2 AM
    schedule.every().day.at("02:00").do(run_daily_backup)
    
    # For testing: uncomment to run every 5 minutes
    # schedule.every(5).minutes.do(run_daily_backup)
    
    print("\n⏳ Waiting for scheduled time...")
    print("   (Press Ctrl+C to stop)\n")
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Backup scheduler stopped by user.")