# dashboard_utils.py
"""
Dashboard utilities for OTMS analytics and metrics.
Provides data aggregation and analysis functions.
"""

from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from models import (
    Location, Tank, TankTransaction, YadeVoyage, YadeBarge,
    TankerTransaction, OTRRecord, User, Operation
)

class DashboardMetrics:
    """Calculate dashboard metrics and analytics"""
    
    @staticmethod
    def get_location_summary(session: Session, location_id: int) -> Dict:
        """Get comprehensive summary for a location"""
        
        # Basic counts - USE STRING, NOT ENUM
        total_tanks = session.query(Tank).filter(Tank.location_id == location_id).count()
        active_tanks = session.query(Tank).filter(
            Tank.location_id == location_id,
            Tank.status == "ACTIVE"  # ← FIXED: Use string instead of TankStatus.ACTIVE
        ).count()
        
        # Recent activity (last 7 days)
        week_ago = date.today() - timedelta(days=7)
        recent_transactions = session.query(TankTransaction).filter(
            TankTransaction.location_id == location_id,
            TankTransaction.date >= week_ago
        ).count()
        
        recent_voyages = session.query(YadeVoyage).filter(
            YadeVoyage.location_id == location_id,
            YadeVoyage.date >= week_ago
        ).count()
        
        # Recent tanker transactions
        recent_tanker_tx = session.query(TankerTransaction).filter(
            TankerTransaction.location_id == location_id,
            TankerTransaction.transaction_date >= week_ago
        ).count()
        
        # Latest OTR records for current stock
        latest_otrs = session.query(OTRRecord).filter(
            OTRRecord.location_id == location_id
        ).order_by(OTRRecord.date.desc(), OTRRecord.time.desc()).limit(10).all()
        
        current_stock = {}
        for otr in latest_otrs:
            if otr.tank_id not in current_stock:
                current_stock[otr.tank_id] = {
                    "tank": otr.tank_id,
                    "volume": otr.nsv_bbl or 0,
                    "date": otr.date,
                    "time": otr.time
                }
        
        total_stock = sum(s["volume"] for s in current_stock.values())
        
        return {
            "total_tanks": total_tanks,
            "active_tanks": active_tanks,
            "inactive_tanks": total_tanks - active_tanks,
            "recent_transactions": recent_transactions,
            "recent_voyages": recent_voyages,
            "recent_tanker_dispatches": recent_tanker_tx,
            "current_stock_bbl": round(total_stock, 2),
            "tanks_with_stock": len(current_stock)
        }
    
    @staticmethod
    def get_fleet_summary(session: Session) -> Dict:
        """Get summary across all locations (admin view)"""
        
        total_locations = session.query(Location).filter(Location.is_active == True).count()
        total_users = session.query(User).filter(User.is_active == True).count()
        total_tanks = session.query(Tank).count()
        total_yade_barges = session.query(YadeBarge).count()
        
        # Today's activity
        today = date.today()
        today_transactions = session.query(TankTransaction).filter(
            TankTransaction.date == today
        ).count()
        
        today_voyages = session.query(YadeVoyage).filter(
            YadeVoyage.date == today
        ).count()
        
        today_tanker_tx = session.query(TankerTransaction).filter(
            TankerTransaction.transaction_date == today
        ).count()
        
        return {
            "total_locations": total_locations,
            "total_users": total_users,
            "total_tanks": total_tanks,
            "total_yade_barges": total_yade_barges,
            "today_transactions": today_transactions,
            "today_voyages": today_voyages,
            "today_tanker_dispatches": today_tanker_tx
        }
    
    @staticmethod
    def get_stock_levels(session: Session, location_id: int) -> List[Dict]:
        """Get current stock levels for all tanks at a location"""
        
        # USE STRING, NOT ENUM
        tanks = session.query(Tank).filter(
            Tank.location_id == location_id,
            Tank.status == "ACTIVE"  # ← FIXED: Use string
        ).order_by(Tank.name).all()
        
        stock_levels = []
        
        for tank in tanks:
            # Get latest OTR for this tank
            latest_otr = session.query(OTRRecord).filter(
                OTRRecord.location_id == location_id,
                OTRRecord.tank_id == tank.name
            ).order_by(OTRRecord.date.desc(), OTRRecord.time.desc()).first()
            
            current_volume = latest_otr.nsv_bbl if latest_otr else 0
            capacity = tank.capacity_bbl or 1  # Avoid division by zero
            fill_pct = (current_volume / capacity * 100) if capacity > 0 else 0
            
            # Determine status
            if fill_pct >= 90:
                status = "🔴 Critical High"
                status_class = "critical"
            elif fill_pct >= 75:
                status = "🟡 High"
                status_class = "warning"
            elif fill_pct <= 10:
                status = "🔵 Low"
                status_class = "info"
            elif fill_pct <= 25:
                status = "🟢 Medium Low"
                status_class = "success"
            else:
                status = "✅ Normal"
                status_class = "normal"
            
            stock_levels.append({
                "tank_name": tank.name,
                "product": tank.product,
                "capacity_bbl": round(capacity, 2),
                "current_volume_bbl": round(current_volume, 2),
                "available_space_bbl": round(capacity - current_volume, 2),
                "fill_percentage": round(fill_pct, 1),
                "status": status,
                "status_class": status_class,
                "last_updated": latest_otr.date if latest_otr else None
            })
        
        return stock_levels
    
    @staticmethod
    def get_recent_activity(session: Session, location_id: int, days: int = 7, limit: int = 20) -> List[Dict]:
        """Get recent activity at a location"""
        
        cutoff_date = date.today() - timedelta(days=days)
        
        activity = []
        
        # Get recent tank transactions
        transactions = session.query(TankTransaction).filter(
            TankTransaction.location_id == location_id,
            TankTransaction.date >= cutoff_date
        ).order_by(TankTransaction.date.desc(), TankTransaction.time.desc()).limit(limit).all()
        
        for tx in transactions:
            # Safely get operation name
            operation_name = tx.operation.value if tx.operation else "N/A"
            
            activity.append({
                "type": "Tank Transaction",
                "icon": "🛢️",
                "description": f"{operation_name} - {tx.tank_name or 'Unknown Tank'}",
                "details": f"{tx.qty_bbls or 0:.0f} bbls",
                "date": tx.date,
                "time": tx.time,
                "ticket_id": tx.ticket_id,
                "timestamp": datetime.combine(tx.date, tx.time)
            })
        
        # Get recent YADE voyages
        voyages = session.query(YadeVoyage).filter(
            YadeVoyage.location_id == location_id,
            YadeVoyage.date >= cutoff_date
        ).order_by(YadeVoyage.date.desc(), YadeVoyage.time.desc()).limit(limit).all()
        
        for voyage in voyages:
            activity.append({
                "type": "YADE Voyage",
                "icon": "🚢",
                "description": f"{voyage.yade_name} - Voyage #{voyage.voyage_no}",
                "details": f"To: {voyage.destination}",
                "date": voyage.date,
                "time": voyage.time,
                "ticket_id": voyage.voyage_no,
                "timestamp": datetime.combine(voyage.date, voyage.time)
            })
        
        # Get recent tanker dispatches
        tanker_txs = session.query(TankerTransaction).filter(
            TankerTransaction.location_id == location_id,
            TankerTransaction.transaction_date >= cutoff_date
        ).order_by(TankerTransaction.transaction_date.desc(), TankerTransaction.transaction_time.desc()).limit(limit).all()
        
        for tx in tanker_txs:
            activity.append({
                "type": "Tanker Dispatch",
                "icon": "🚚",
                "description": f"{tx.tanker_name} - Convoy #{tx.convoy_no}",
                "details": f"{tx.nsv_bbl or 0:.0f} bbls to {tx.destination}",
                "date": tx.transaction_date,
                "time": tx.transaction_time,
                "ticket_id": tx.convoy_no,
                "timestamp": datetime.combine(tx.transaction_date, tx.transaction_time)
            })
        
        # Sort by timestamp (most recent first)
        activity.sort(key=lambda x: x["timestamp"], reverse=True)
        
        # Remove timestamp field (used only for sorting)
        for item in activity:
            del item["timestamp"]
        
        return activity[:limit]
    
    @staticmethod
    def get_operations_breakdown(session: Session, location_id: int, days: int = 30) -> Dict:
        """Get breakdown of operations by type"""
        
        cutoff_date = date.today() - timedelta(days=days)
        
        # Query transactions grouped by operation
        operations = session.query(
            TankTransaction.operation,
            func.count(TankTransaction.id).label('count'),
            func.sum(TankTransaction.qty_bbls).label('total_volume')
        ).filter(
            TankTransaction.location_id == location_id,
            TankTransaction.date >= cutoff_date
        ).group_by(TankTransaction.operation).all()
        
        breakdown = {}
        for op, count, volume in operations:
            op_name = op.value if op else "Unknown"
            breakdown[op_name] = {
                "count": count,
                "total_volume": round(volume or 0, 2)
            }
        
        # Sort by count (most frequent first)
        breakdown = dict(sorted(breakdown.items(), key=lambda x: x[1]["count"], reverse=True))
        
        return breakdown
    
    @staticmethod
    def get_yade_utilization(session: Session, location_id: int, days: int = 30) -> List[Dict]:
        """Get YADE barge utilization stats"""
        
        cutoff_date = date.today() - timedelta(days=days)
        
        # Query voyages grouped by YADE
        yade_stats = session.query(
            YadeVoyage.yade_name,
            func.count(YadeVoyage.id).label('voyage_count')
        ).filter(
            YadeVoyage.location_id == location_id,
            YadeVoyage.date >= cutoff_date
        ).group_by(YadeVoyage.yade_name).all()
        
        utilization = []
        for yade_name, voyage_count in yade_stats:
            utilization.append({
                "yade_name": yade_name,
                "voyages": voyage_count,
                "avg_per_week": round(voyage_count / (days / 7), 1) if days >= 7 else voyage_count
            })
        
        # Sort by voyage count
        utilization.sort(key=lambda x: x["voyages"], reverse=True)
        
        return utilization
    
    @staticmethod
    def get_alerts(session: Session, location_id: int) -> List[Dict]:
        """Generate alerts for tanks and operations"""
        
        alerts = []
        
        # Check tank levels - USE STRING, NOT ENUM
        tanks = session.query(Tank).filter(
            Tank.location_id == location_id,
            Tank.status == "ACTIVE"  # ← FIXED: Use string
        ).all()
        
        for tank in tanks:
            # Get latest OTR
            latest_otr = session.query(OTRRecord).filter(
                OTRRecord.location_id == location_id,
                OTRRecord.tank_id == tank.name
            ).order_by(OTRRecord.date.desc(), OTRRecord.time.desc()).first()
            
            if latest_otr:
                current_volume = latest_otr.nsv_bbl or 0
                capacity = tank.capacity_bbl or 1
                fill_pct = (current_volume / capacity * 100) if capacity > 0 else 0
                
                # High level alert
                if fill_pct >= 90:
                    alerts.append({
                        "severity": "critical",
                        "icon": "🔴",
                        "type": "High Tank Level",
                        "message": f"Tank {tank.name} is {fill_pct:.1f}% full ({current_volume:,.0f} bbls)",
                        "action": "Consider dispatching to avoid overflow",
                        "tank": tank.name,
                        "priority": 1
                    })
                
                # Low level alert
                elif fill_pct <= 10:
                    alerts.append({
                        "severity": "warning",
                        "icon": "🔵",
                        "type": "Low Tank Level",
                        "message": f"Tank {tank.name} is only {fill_pct:.1f}% full ({current_volume:,.0f} bbls)",
                        "action": "Schedule receipt if needed",
                        "tank": tank.name,
                        "priority": 2
                    })
                
                # Stale data alert (no update in 7 days)
                days_since_update = (date.today() - latest_otr.date).days
                if days_since_update > 7:
                    alerts.append({
                        "severity": "info",
                        "icon": "ℹ️",
                        "type": "Stale Data",
                        "message": f"Tank {tank.name} has no updates for {days_since_update} days",
                        "action": "Verify tank status and record current reading",
                        "tank": tank.name,
                        "priority": 3
                    })
            else:
                # No data alert
                alerts.append({
                    "severity": "warning",
                    "icon": "⚠️",
                    "type": "No Data",
                    "message": f"Tank {tank.name} has no recorded data",
                    "action": "Record initial tank reading",
                    "tank": tank.name,
                    "priority": 2
                })
        
        # Sort by priority (1 = highest)
        alerts.sort(key=lambda x: x["priority"])
        
        return alerts
    
    @staticmethod
    def get_monthly_summary(session: Session, location_id: int, month: Optional[date] = None) -> Dict:
        """Get monthly summary of operations"""
        
        if month is None:
            month = date.today()
        
        # Get first and last day of month
        first_day = month.replace(day=1)
        if month.month == 12:
            last_day = date(month.year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = date(month.year, month.month + 1, 1) - timedelta(days=1)
        
        # Count transactions
        tank_tx_count = session.query(TankTransaction).filter(
            TankTransaction.location_id == location_id,
            TankTransaction.date >= first_day,
            TankTransaction.date <= last_day
        ).count()
        
        yade_tx_count = session.query(YadeVoyage).filter(
            YadeVoyage.location_id == location_id,
            YadeVoyage.date >= first_day,
            YadeVoyage.date <= last_day
        ).count()
        
        tanker_tx_count = session.query(TankerTransaction).filter(
            TankerTransaction.location_id == location_id,
            TankerTransaction.transaction_date >= first_day,
            TankerTransaction.transaction_date <= last_day
        ).count()
        
        # Sum volumes
        total_receipts = session.query(func.sum(TankTransaction.qty_bbls)).filter(
            TankTransaction.location_id == location_id,
            TankTransaction.date >= first_day,
            TankTransaction.date <= last_day,
            TankTransaction.operation.in_([
                Operation.RECEIPT,
                Operation.RECEIPT_CRUDE,
                Operation.RECEIPT_CONDENSATE,
                Operation.RECEIPT_FROM_AGU,
                Operation.RECEIPT_FROM_OFS,
                Operation.OKW_RECEIPT,
                Operation.ANZ_RECEIPT,
                Operation.OTHER_RECEIPTS
            ])
        ).scalar() or 0
        
        total_dispatches = session.query(func.sum(TankTransaction.qty_bbls)).filter(
            TankTransaction.location_id == location_id,
            TankTransaction.date >= first_day,
            TankTransaction.date <= last_day,
            TankTransaction.operation.in_([
                Operation.DISPATCH_TO_BARGE,
                Operation.DISPATCH_TO_JETTY,
                Operation.OTHER_DISPATCH
            ])
        ).scalar() or 0
        
        return {
            "month": month.strftime("%B %Y"),
            "tank_transactions": tank_tx_count,
            "yade_voyages": yade_tx_count,
            "tanker_dispatches": tanker_tx_count,
            "total_transactions": tank_tx_count + yade_tx_count + tanker_tx_count,
            "total_receipts_bbl": round(total_receipts, 2),
            "total_dispatches_bbl": round(total_dispatches, 2),
            "net_movement_bbl": round(total_receipts - total_dispatches, 2)
        }