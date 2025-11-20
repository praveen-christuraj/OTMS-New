# models.py
"""
Database models for OTMS (Oil Terminal Management System)
Multi-location support with comprehensive security and audit trails
"""

from datetime import datetime
import enum

from sqlalchemy import (
    Column, Integer, Float, String, Date, Time, DateTime, Boolean, Text,
    ForeignKey, Enum as SAEnum, UniqueConstraint, Index
)
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.sql import func

try:
    from db import engine
except Exception:
    engine = None

Base = declarative_base()

# ============================================================================
# ENUMS
# ============================================================================

class TankStatus(enum.Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"

class TankOpStatus(enum.Enum):
    RECEIVING = "RECEIVING"       # not pumpable
    DISPATCHING = "DISPATCHING"   # pumpable
    IDLE = "IDLE"                 # pumpable
    READY = "READY"               # pumpable
    SETTLING = "SETTLING"         # not pumpable
    MAINTENANCE = "MAINTENANCE"   # not pumpable
    DRAINING = "DRAINING"         # not pumpable
    ISOLATED = "ISOLATED"         # not pumpable

class Operation(enum.Enum):
    """Operation types for tank transactions"""
    # Opening / Closing
    OPENING_STOCK = "Opening Stock"
    CLOSING_STOCK = "Closing Stock"
    
    # Receipts
    RECEIPT = "Receipt"
    RECEIPT_CRUDE = "Receipt - Commingled"
    RECEIPT_CONDENSATE = "Receipt - Condensate"
    RECEIPT_FROM_AGU = "Receipt from Agu"
    RECEIPT_FROM_OFS = "Receipt from OFS"
    OKW_RECEIPT = "OKW Receipt"
    ANZ_RECEIPT = "ANZ Receipt"
    OTHER_RECEIPTS = "Other Receipts"
    
    # Dispatches
    DISPATCH = "Dispatch"
    DISPATCH_TO_BARGE = "Dispatch to barge"
    DISPATCH_TO_JETTY = "Dispatch to Jetty"
    OTHER_DISPATCH = "Other Dispatch"
    
    # Inter-Tank Transfers
    ITT_RECEIPT = "ITT - Receipt"
    ITT_DISPATCH = "ITT - Dispatch"
    
    # Maintenance
    SETTLING = "Settling"
    DRAINING = "Draining"


class TaskType(enum.Enum):
    DELETE_REQUEST = "DELETE_REQUEST"
    ERROR_ALERT = "ERROR_ALERT"
    PASSWORD_RESET = "PASSWORD_RESET"
    INFO = "INFO"


class TaskStatus(enum.Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"

class TankDailyStatus(Base):
    __tablename__ = "tank_daily_statuses"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"), nullable=False)
    date = Column(Date, nullable=False)
    op_status = Column(SAEnum(TankOpStatus), nullable=False, default=TankOpStatus.READY)
    note = Column(Text)
    created_by = Column(String(50))
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("tank_id", "date", name="uq_tank_date_status"),
        Index("idx_tank_date", "tank_id", "date"),
    )

    tank = relationship("Tank")

    def __repr__(self):
        return f"<TankDailyStatus tank={self.tank_id} date={self.date} status={self.op_status}>"


class CargoKind(enum.Enum):
    OKWUIBOME_CRUDE = "Okwuibome Blend Crude"
    OKWUIBOME_CONDENSATE = "Okwuibome Condensate"
    AGO = "AGO"
    PMS = "PMS"

class DestinationKind(enum.Enum):
    AGGE = "Agge"
    NDONI = "Ndoni"
    ASEMOKU = "Asemoku"

class LoadingBerthKind(enum.Enum):
    NDONI_JETTY = "Ndoni Jetty"
    ASEMOKU_JETTY = "Asemoku Jetty"
    STS = "STS"

# ============================================================================
# LOCATION SUPPORT
# ============================================================================

class Location(Base):
    """Location/Terminal master - represents different oil terminals"""
    __tablename__ = "locations"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    code = Column(String(20), unique=True, nullable=False)
    address = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, onupdate=datetime.utcnow)
    
    # Relationships - Location has many entities
    tanks = relationship("Tank", back_populates="location", lazy="dynamic", cascade="all, delete-orphan")
    users = relationship("User", back_populates="location", lazy="dynamic")
    tank_transactions = relationship("TankTransaction", back_populates="location", lazy="dynamic")
    gpp_productions = relationship("GPPProductionRecord", back_populates="location", lazy="dynamic", cascade="all, delete-orphan")
    river_draft_entries = relationship(
        "RiverDraftRecord",
        back_populates="location",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )
    produced_water_entries = relationship(
        "ProducedWaterRecord",
        back_populates="location",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )
    yade_voyages = relationship("YadeVoyage", back_populates="location", lazy="dynamic")
    tanker_transactions = relationship("TankerTransaction", back_populates="location", lazy="dynamic")
    otr_records = relationship("OTRRecord", back_populates="location", lazy="dynamic")
    calibrations = relationship("CalibrationTank", back_populates="location", lazy="dynamic")
    fso_operations = relationship("FSOOperation", back_populates="location", lazy="dynamic", cascade="all, delete-orphan")  # ✅ ADDED
    tanker_counts = relationship(
        "LocationTankerEntry",
        back_populates="location",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )

    # Relationship for OFS production & evacuation records
    ofs_records = relationship(
        "OFSProductionEvacuationRecord",
        back_populates="location",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return f"<Location(id={self.id}, name='{self.name}', code='{self.code}')>"


class LocationConfiguration(Base):
    """Store location-specific configuration as JSON"""
    __tablename__ = "location_configurations"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False, unique=True)
    config_json = Column(Text, nullable=True)  # JSON configuration
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())
    
    # Relationship
    location = relationship("Location")


# ============================================================================
# OFS PRODUCTION & EVACUATION
# ============================================================================
class OFSProductionEvacuationRecord(Base):
    """
    Capture daily production and evacuation figures for OFS locations (e.g. OML‑157).

    Each record stores volumes for Oguali and Ukpichi production, production from
    other locations, total evacuation, and tanker counts per location. A serial
    number per location ensures a stable ascending index for reporting.
    """

    __tablename__ = "ofs_production_evacuation_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    serial_no = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    oguali_production = Column(Float, default=0.0)
    ukpichi_production = Column(Float, default=0.0)
    other_locations = Column(Float, default=0.0)
    evacuation = Column(Float, default=0.0)
    tankers_oguali = Column(Float, default=0.0)
    tankers_ukpichi = Column(Float, default=0.0)
    other_tankers = Column(Float, default=0.0)
    created_by = Column(String(50))
    updated_by = Column(String(50))
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    location = relationship("Location", back_populates="ofs_records")

    __table_args__ = (
        UniqueConstraint("location_id", "serial_no", name="uq_ofs_serial_per_location"),
        Index("idx_ofs_location_date", "location_id", "date"),
    )

    def __repr__(self) -> str:
        return (
            f"<OFSProductionEvacuationRecord id={self.id} loc={self.location_id} "
            f"date={self.date} serial={self.serial_no}>"
        )


class LocationTankerEntry(Base):
    """Manual tanker dispatch/receipt logs for Aggu and Ndoni dashboards."""

    __tablename__ = "location_tanker_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    serial_no = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)

    # Metrics (unused columns remain zero for specific locations)
    tankers_dispatched = Column(Float, default=0.0)
    tankers_from_aggu = Column(Float, default=0.0)
    tankers_from_ofs = Column(Float, default=0.0)
    other_tankers = Column(Float, default=0.0)

    remarks = Column(Text)
    created_by = Column(String(50))
    updated_by = Column(String(50))
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    location = relationship("Location", back_populates="tanker_counts")

    __table_args__ = (
        UniqueConstraint("location_id", "serial_no", name="uq_tanker_entry_serial"),
        Index("idx_tanker_entry_loc_date", "location_id", "date"),
    )

    def __repr__(self) -> str:
        return f"<LocationTankerEntry id={self.id} loc={self.location_id} serial={self.serial_no} date={self.date}>"


# ============================================================================
# USER & AUTHENTICATION
# ============================================================================

class User(Base):
    """User accounts with location assignment and 2FA support"""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100), unique=True, nullable=False)
    password_hash = Column(String(250), nullable=False)
    full_name = Column(String(150), nullable=True)
    role = Column(String(30), nullable=False)  # admin-operations, admin-it, manager, supervisor, operator
    
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)
    
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    last_login = Column(DateTime, nullable=True)
    
    # Security fields
    must_change_password = Column(Boolean, default=True, nullable=False)
    password_changed_at = Column(DateTime, nullable=True)
    failed_login_attempts = Column(Integer, default=0)
    account_locked_until = Column(DateTime, nullable=True)
    last_activity = Column(DateTime, nullable=True)
    
    # 2FA fields
    totp_secret = Column(String(32), nullable=True)
    totp_enabled = Column(Boolean, default=False, nullable=False)
    backup_codes = Column(String(500), nullable=True)
    supervisor_code_hash = Column(String(255), nullable=True)
    supervisor_code_set_at = Column(DateTime, nullable=True)
    
    # Relationship
    location = relationship("Location", back_populates="users")

    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}', role='{self.role}')>"


# ============================================================================
# TANK MASTER & CALIBRATION
# ============================================================================

class Tank(Base):
    __tablename__ = "tanks"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    name = Column(String(100), nullable=False)
    capacity_bbl = Column(Float, nullable=False)
    product = Column(String(50), nullable=False)
    status = Column(SAEnum(TankStatus), default=TankStatus.ACTIVE, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, onupdate=datetime.utcnow)
    
    # Composite unique constraint: Tank name must be unique PER LOCATION
    __table_args__ = (
        UniqueConstraint('location_id', 'name', name='uq_tank_location_name'),
    )
    
    # Relationships
    location = relationship("Location", back_populates="tanks")
    calibration = relationship("CalibrationTank", back_populates="tank", lazy="dynamic", cascade="all, delete-orphan")
    transactions = relationship("TankTransaction", back_populates="tank", lazy="dynamic")

    def __repr__(self):
        return f"<Tank(id={self.id}, name='{self.name}', location_id={self.location_id})>"


class CalibrationTank(Base):
    """Tank calibration - location-specific"""
    __tablename__ = "calibration_tank"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    tank_id = Column(Integer, ForeignKey("tanks.id"), nullable=False)
    tank_name = Column(String(50), index=True, nullable=False)
    dip_cm = Column(Float, nullable=False)
    volume_bbl = Column(Float, nullable=False)
    
    # Relationships
    location = relationship("Location", back_populates="calibrations")
    tank = relationship("Tank", back_populates="calibration")

    def __repr__(self):
        return f"<CalibrationTank(tank='{self.tank_name}', dip={self.dip_cm}cm, vol={self.volume_bbl}bbl)>"


# ============================================================================
# YADE BARGE MASTER & CALIBRATION (SHARED GLOBALLY)
# ============================================================================

class YadeBarge(Base):
    """YADE barge master - shared globally across all locations"""
    __tablename__ = "yade_barges"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    design = Column(String(2), nullable=False)  # "6" or "4"
    created_at = Column(DateTime, server_default=func.now())

    def __repr__(self):
        return f"<YadeBarge(name='{self.name}', design='{self.design}')>"


class YadeCalibration(Base):
    """YADE calibration - shared globally"""
    __tablename__ = "yade_calibration"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    yade_name = Column(String(100), index=True, nullable=False)
    tank_id = Column(String(10), nullable=False)
    dip_mm = Column(Float, nullable=False)
    vol_bbl = Column(Float, nullable=False)
    mm1 = Column(Float, nullable=True)
    mm2 = Column(Float, nullable=True)
    mm3 = Column(Float, nullable=True)
    mm4 = Column(Float, nullable=True)
    mm5 = Column(Float, nullable=True)
    mm6 = Column(Float, nullable=True)
    mm7 = Column(Float, nullable=True)
    mm8 = Column(Float, nullable=True)
    mm9 = Column(Float, nullable=True)


# ============================================================================
# TANKER MASTER & CALIBRATION (SHARED GLOBALLY)
# ============================================================================

class Tanker(Base):
    """Tanker master - shared globally across all locations"""
    __tablename__ = "tankers"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    registration_no = Column(String(50), nullable=True)
    capacity_litres = Column(Float, nullable=True)
    status = Column(SAEnum(TankStatus), default=TankStatus.ACTIVE, nullable=False)
    created_at = Column(DateTime, server_default=func.now())

    def __repr__(self):
        return f"<Tanker(name='{self.name}')>"


class TankerCalibration(Base):
    """Tanker calibration - shared globally"""
    __tablename__ = "tanker_calibration"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    tanker_name = Column(String(100), index=True, nullable=False)
    compartment = Column(String(10), nullable=False)  # "C1" or "C2"
    dip_mm = Column(Float, nullable=False)
    volume_litres = Column(Float, nullable=False)

# ============================================================================
# VESSEL MASTER (SHARED GLOBALLY LIKE YADE/TANKER)
# ============================================================================

class Vessel(Base):
    """Vessel master - shared globally across all locations"""
    __tablename__ = "vessels"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)
    vessel_type = Column(String(50), nullable=True)  # MT (Motor Tanker), Barge, etc.
    capacity_bbl = Column(Float, nullable=True)
    registration_no = Column(String(50), nullable=True)
    status = Column(String(20), default="ACTIVE")
    created_at = Column(DateTime, server_default=func.now())
    
    def __repr__(self):
        return f"<Vessel(name='{self.name}', type='{self.vessel_type}')>"


class VesselOperation(Base):
    """Vessel operation types - shared globally"""
    __tablename__ = "vessel_operations"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    operation_name = Column(String(50), nullable=False, unique=True)
    category = Column(String(50), nullable=True)  # LOADING, OFFLOADING, TRANSIT, STANDBY
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    
    def __repr__(self):
        return f"<VesselOperation(name='{self.operation_name}')>"


class LocationVessel(Base):
    """Location-Vessel assignment - which vessels are available at which location"""
    __tablename__ = "location_vessels"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    vessel_id = Column(Integer, ForeignKey("vessels.id"), nullable=False)
    is_active = Column(Boolean, default=True)
    assigned_at = Column(DateTime, server_default=func.now())
    
    # Composite unique: Each vessel can only be assigned once per location
    __table_args__ = (
        UniqueConstraint('location_id', 'vessel_id', name='uq_location_vessel'),
    )
    
    # Relationships
    location = relationship("Location")
    vessel = relationship("Vessel")
    
    def __repr__(self):
        return f"<LocationVessel(location_id={self.location_id}, vessel_id={self.vessel_id})>"

# ============================================================================
# ASTM TABLE 11 (SHARED GLOBALLY)
# ============================================================================

class Table11(Base):
    """ASTM Table 11 - LT factors (shared globally)"""
    __tablename__ = "table11"
    
    id = Column(Integer, primary_key=True)
    api60 = Column(Float, nullable=False)
    lt_factor = Column(Float, nullable=False)


# ============================================================================
# TANK TRANSACTIONS (LOCATION-SPECIFIC)
# ============================================================================

class TankTransaction(Base):
    """Tank transactions - location-specific"""
    __tablename__ = "tank_transactions"
    __table_args__ = {'extend_existing': True}
    
    id = Column(Integer, primary_key=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    ticket_id = Column(String(100), index=True, nullable=False)
    operation = Column(SAEnum(Operation), nullable=False)
    tank_id = Column(Integer, ForeignKey("tanks.id"), nullable=True)
    tank_name = Column(String(50), nullable=True)
    date = Column(Date, nullable=False)
    time = Column(Time, nullable=False)
    
    dip_cm = Column(Float, default=0)
    water_cm = Column(Float, default=0)
    
    tank_temp_c = Column(Float, nullable=True)
    tank_temp_f = Column(Float, nullable=True)
    
    api_observed = Column(Float, nullable=True)
    density_observed = Column(Float, nullable=True)
    bsw_pct = Column(Float, nullable=True)
    sample_temp_c = Column(Float, nullable=True)
    sample_temp_f = Column(Float, nullable=True)
    
    qty_bbls = Column(Float, nullable=True)
    remarks = Column(String(250), nullable=True)

    # Condensate receipt fields (for BFS meter readings)
    opening_meter_reading = Column(Float, nullable=True)
    closing_meter_reading = Column(Float, nullable=True)
    condensate_qty_m3 = Column(Float, nullable=True)
    
    # Audit fields
    created_by = Column(String(50), nullable=False, default='system')
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_by = Column(String(50), nullable=True)
    updated_at = Column(DateTime, nullable=True)
    
    # Relationships
    location = relationship("Location", back_populates="tank_transactions")
    tank = relationship("Tank", back_populates="transactions")


# ============================================================================
# METER TRANSACTIONS (LOCATION-SPECIFIC, E.g., Asemoku Jetty)
# ============================================================================
 
class MeterTransaction(Base):
    """Manual meter transactions for a location (e.g., Asemoku Jetty)."""
    __tablename__ = "meter_transactions"
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False, index=True)

    date = Column(Date, nullable=False)
    opening_meter_reading = Column(Float, nullable=False)
    closing_meter_reading = Column(Float, nullable=False)
    opening_meter2_reading = Column(Float, nullable=True)
    closing_meter2_reading = Column(Float, nullable=True)
    net_qty = Column(Float, nullable=False)  # (M1 close - M1 open) + (M2 close - M2 open)
    remarks = Column(String(250), nullable=True)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

    # Relationship
    location = relationship("Location")


Index('idx_meter_tx_location_date', MeterTransaction.location_id, MeterTransaction.date)


# ============================================================================
# GPP PRODUCTION (BENEKU)
# ============================================================================

class GPPProductionRecord(Base):
    """Daily GPP production summary per location."""
    __tablename__ = "gpp_production_records"
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False, index=True)
    date = Column(Date, nullable=False)
    okw_production = Column(Float, nullable=False, default=0.0)
    gpp1_production = Column(Float, nullable=False, default=0.0)
    gpp2_production = Column(Float, nullable=False, default=0.0)
    total_production = Column(Float, nullable=False, default=0.0)
    gpp_closing_stock = Column(Float, nullable=False, default=0.0)
    remarks = Column(Text, nullable=True)

    created_by = Column(String(50), nullable=False, default="system")
    created_at = Column(DateTime, server_default=func.now())
    updated_by = Column(String(50), nullable=True)
    updated_at = Column(DateTime, onupdate=func.now())

    location = relationship("Location", back_populates="gpp_productions")

    def __repr__(self):
        return (
            f"<GPPProductionRecord(id={self.id}, date={self.date}, "
            f"gpp1={self.gpp1_production}, gpp2={self.gpp2_production})>"
        )


Index('idx_gpp_prod_location_date', GPPProductionRecord.location_id, GPPProductionRecord.date)


class RiverDraftRecord(Base):
    """Manual capture of river draft and rainfall per location/date."""
    __tablename__ = "river_draft_records"
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False, index=True)
    date = Column(Date, nullable=False)
    river_draft_m = Column(Float, nullable=False, default=0.0)
    rainfall_cm = Column(Float, nullable=False, default=0.0)

    created_by = Column(String(50), nullable=False, default="system")
    created_at = Column(DateTime, server_default=func.now())
    updated_by = Column(String(50), nullable=True)
    updated_at = Column(DateTime, onupdate=func.now())

    location = relationship("Location", back_populates="river_draft_entries")

    def __repr__(self):
        return f"<RiverDraftRecord(id={self.id}, date={self.date}, river_draft_m={self.river_draft_m}, rainfall_cm={self.rainfall_cm})>"


Index('idx_river_draft_location_date', RiverDraftRecord.location_id, RiverDraftRecord.date)


class ProducedWaterRecord(Base):
    """Manual capture of produced water per location/date."""
    __tablename__ = "produced_water_records"
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False, index=True)
    date = Column(Date, nullable=False)
    produced_water_bbl = Column(Float, nullable=False, default=0.0)

    created_by = Column(String(50), nullable=False, default="system")
    created_at = Column(DateTime, server_default=func.now())
    updated_by = Column(String(50), nullable=True)
    updated_at = Column(DateTime, onupdate=func.now())

    location = relationship("Location", back_populates="produced_water_entries")

    def __repr__(self):
        return f"<ProducedWaterRecord(id={self.id}, date={self.date}, bbl={self.produced_water_bbl})>"


Index('idx_produced_water_location_date', ProducedWaterRecord.location_id, ProducedWaterRecord.date)


# ============================================================================
# YADE VOYAGE TRANSACTIONS (LOCATION-SPECIFIC)
# ============================================================================

class YadeVoyage(Base):
    """YADE voyage - location-specific"""
    __tablename__ = "yade_voyage"
    __table_args__ = {'extend_existing': True}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    
    yade_name = Column(String(64), nullable=False)
    design = Column(String(2), nullable=False)
    voyage_no = Column(String(32), nullable=False)
    convoy_no = Column(String(32), nullable=False)
    
    date = Column(Date, nullable=False)
    time = Column(Time, nullable=False)
    
    cargo = Column(String(64), nullable=False)
    destination = Column(String(64), nullable=False)
    loading_berth = Column(String(64), nullable=False)
    
    before_gauge_date = Column(Date, nullable=False)
    before_gauge_time = Column(Time, nullable=False)
    after_gauge_date = Column(Date, nullable=False)
    after_gauge_time = Column(Time, nullable=False)
    
    # Audit fields
    created_by = Column(String(64), default='system')
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_by = Column(String(64), nullable=True)
    updated_at = Column(DateTime, nullable=True)
    
    # Relationship
    location = relationship("Location", back_populates="yade_voyages")


class YadeDip(Base):
    """YADE dip readings"""
    __tablename__ = "yade_dips"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    voyage_id = Column(Integer, ForeignKey("yade_voyage.id", ondelete="CASCADE"), nullable=False)
    
    tank_id = Column(String(8), nullable=False)
    stage = Column(String(8), nullable=False)
    
    total_cm = Column(Float, nullable=False, default=0.0)
    water_cm = Column(Float, nullable=False, default=0.0)


class YadeSampleParam(Base):
    """YADE sample parameters"""
    __tablename__ = "yade_sample_param"
    
    id = Column(Integer, primary_key=True)
    voyage_id = Column(Integer, ForeignKey("yade_voyage.id", ondelete="CASCADE"), index=True, nullable=False)
    stage = Column(String(10), nullable=False)
    obs_mode = Column(String(32), nullable=False)
    obs_val = Column(Float, nullable=False, default=0.0)
    sample_unit = Column(String(4), nullable=False)
    sample_temp = Column(Float, nullable=False, default=0.0)
    tank_temp = Column(Float, nullable=False, default=0.0)
    ccf = Column(Float, nullable=False, default=1.0)
    bsw_pct = Column(Float, nullable=False, default=0.0)
    
    __table_args__ = (UniqueConstraint("voyage_id", "stage", name="uq_ysp_voyage_stage"),)


class YadeSealDetail(Base):
    """YADE seal details"""
    __tablename__ = "yade_seal_detail"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    voyage_id = Column(Integer, ForeignKey("yade_voyage.id", ondelete="CASCADE"), nullable=False)
    
    c1_mh1 = Column(String(32), nullable=True)
    c1_mh2 = Column(String(32), nullable=True)
    c1_lock = Column(String(32), nullable=True)
    c1_diphatch = Column(String(32), nullable=True)
    
    c2_mh1 = Column(String(32), nullable=True)
    c2_mh2 = Column(String(32), nullable=True)
    c2_lock = Column(String(32), nullable=True)
    c2_diphatch = Column(String(32), nullable=True)
    
    p1_mh1 = Column(String(32), nullable=True)
    p1_mh2 = Column(String(32), nullable=True)
    p1_lock = Column(String(32), nullable=True)
    p1_diphatch = Column(String(32), nullable=True)
    
    p2_mh1 = Column(String(32), nullable=True)
    p2_mh2 = Column(String(32), nullable=True)
    p2_lock = Column(String(32), nullable=True)
    p2_diphatch = Column(String(32), nullable=True)
    
    s1_mh1 = Column(String(32), nullable=True)
    s1_mh2 = Column(String(32), nullable=True)
    s1_lock = Column(String(32), nullable=True)
    s1_diphatch = Column(String(32), nullable=True)
    
    s2_mh1 = Column(String(32), nullable=True)
    s2_mh2 = Column(String(32), nullable=True)
    s2_lock = Column(String(32), nullable=True)
    s2_diphatch = Column(String(32), nullable=True)


class TOAYadeSummary(Base):
    """TOA YADE summary"""
    __tablename__ = "toa_yade_summary"
    
    id = Column(Integer, primary_key=True)
    voyage_id = Column(Integer, ForeignKey("yade_voyage.id", ondelete="CASCADE"), unique=True, index=True)
    ticket_id = Column(String(64), nullable=True)
    date = Column(Date, nullable=False)
    time = Column(Time, nullable=False)
    yade_name = Column(String(64), nullable=False)
    convoy_no = Column(String(64), nullable=True)
    destination = Column(String(64), nullable=True)
    loading_berth = Column(String(64), nullable=True)
    
    gsv_before_bbl = Column(Float, default=0.0)
    gsv_after_bbl = Column(Float, default=0.0)
    gsv_loaded_bbl = Column(Float, default=0.0)


class TOAYadeStage(Base):
    """TOA YADE stage details"""
    __tablename__ = "toa_yade_stage"
    
    id = Column(Integer, primary_key=True)
    voyage_id = Column(Integer, ForeignKey("yade_voyage.id", ondelete="CASCADE"), index=True, nullable=False)
    stage = Column(String(10), nullable=False)
    
    gov_bbl = Column(Float, default=0.0)
    gsv_bbl = Column(Float, default=0.0)
    bsw_pct = Column(Float, default=0.0)
    bsw_bbl = Column(Float, default=0.0)
    nsv_bbl = Column(Float, default=0.0)
    lt = Column(Float, default=0.0)
    mt = Column(Float, default=0.0)
    fw_bbl = Column(Float, default=0.0)
    
    __table_args__ = (UniqueConstraint("voyage_id", "stage", name="uq_toa_stage_voyage_stage"),)


# ============================================================================
# TANKER TRANSACTIONS (LOCATION-SPECIFIC)
# ============================================================================

class TankerTransaction(Base):
    """Tanker dispatch transactions - location-specific"""
    __tablename__ = "tanker_transactions"
    __table_args__ = {'extend_existing': True}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    
    # Top metadata
    tanker_name = Column(String(100), nullable=False)
    chassis_no = Column(String(100), nullable=True)
    convoy_no = Column(String(100), nullable=False)
    transaction_date = Column(Date, nullable=False)
    transaction_time = Column(Time, nullable=False)
    cargo = Column(String(50), nullable=False)
    destination = Column(String(100), nullable=False)
    loading_bay = Column(String(100), nullable=True)
    
    # Compartment (single tank) and Manhole (C1 or C2)
    compartment = Column(String(10), nullable=False)
    manhole = Column(String(10), nullable=False)
    
    # Dips
    total_dip_cm = Column(Float, nullable=False)
    total_dip_mm = Column(Float, nullable=False)
    water_dip_cm = Column(Float, nullable=False)
    water_dip_mm = Column(Float, nullable=False)
    
    # Temperatures
    tank_temp_c = Column(Float, nullable=True)
    tank_temp_f = Column(Float, nullable=True)
    sample_temp_c = Column(Float, nullable=True)
    sample_temp_f = Column(Float, nullable=True)
    
    # Observed properties
    api_observed = Column(Float, nullable=True)
    density_observed = Column(Float, nullable=True)
    
    # BS&W
    bsw_pct = Column(Float, nullable=False, default=0.0)
    
    # Volumes (all in bbls, converted from litres using 158.987)
    total_volume_bbl = Column(Float, nullable=False)
    water_volume_bbl = Column(Float, nullable=False)
    gov_bbl = Column(Float, nullable=False)
    api60 = Column(Float, nullable=True)
    vcf = Column(Float, nullable=True)
    gsv_bbl = Column(Float, nullable=False)
    bsw_vol_bbl = Column(Float, nullable=False)
    nsv_bbl = Column(Float, nullable=False)
    lt = Column(Float, nullable=True)
    mt = Column(Float, nullable=True)
    
    # Seal numbers (4 seals: C1, C2, M1, M2)
    seal_c1 = Column(String(100), nullable=True)
    seal_c2 = Column(String(100), nullable=True)
    seal_m1 = Column(String(100), nullable=True)
    seal_m2 = Column(String(100), nullable=True)
    
    # Remarks
    remarks = Column(String(500), nullable=True)
    
    # Audit fields
    created_by = Column(String(50), nullable=False, default='system')
    created_at = Column(DateTime, server_default=func.now())
    updated_by = Column(String(50), nullable=True)
    updated_at = Column(DateTime, nullable=True)
    
    # Relationship
    location = relationship("Location", back_populates="tanker_transactions")


class TOATanker(Base):
    """TOA (Transfer of Account) for Tanker"""
    __tablename__ = "toa_tanker"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    
    tanker_name = Column(String(100), nullable=False)
    transaction_date = Column(Date, nullable=False)
    waybill_no = Column(String(100), nullable=True)
    destination = Column(String(100), nullable=True)
    
    # Compartment readings
    compartment = Column(String(10), nullable=False)
    dip_mm = Column(Float, nullable=False)
    volume_litres = Column(Float, nullable=False)
    volume_bbl = Column(Float, nullable=False)
    
    # Temperature and density
    temperature_c = Column(Float, nullable=True)
    api_observed = Column(Float, nullable=True)
    api60 = Column(Float, nullable=True)
    
    # Volume calculations
    gov_bbl = Column(Float, nullable=True)
    gsv_bbl = Column(Float, nullable=True)
    nsv_bbl = Column(Float, nullable=True)
    
    created_at = Column(DateTime, server_default=func.now())


# ============================================================================
# OUT-TURN REPORT (LOCATION-SPECIFIC)
# ============================================================================

class OTRRecord(Base):
    """Out-Turn Report records - location-specific"""
    __tablename__ = "otr_records"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    
    ticket_id = Column(String(100), index=True, nullable=False)
    tank_id = Column(String(50), nullable=True)
    date = Column(Date, nullable=False)
    time = Column(Time, nullable=False)
    operation = Column(String(20), nullable=False)
    
    dip_cm = Column(Float, nullable=True)
    total_volume_bbl = Column(Float, nullable=True)
    water_cm = Column(Float, nullable=True)
    free_water_bbl = Column(Float, nullable=True)
    gov_bbl = Column(Float, nullable=True)
    api60 = Column(Float, nullable=True)
    vcf = Column(Float, nullable=True)
    gsv_bbl = Column(Float, nullable=True)
    bsw_vol_bbl = Column(Float, nullable=True)
    nsv_bbl = Column(Float, nullable=True)
    lt = Column(Float, nullable=True)
    mt = Column(Float, nullable=True)
    
    created_at = Column(DateTime, server_default=func.now())
    
    # Relationship
    location = relationship("Location", back_populates="otr_records")

# ============================================================================
# OTR VESSEL (NEW)
# ============================================================================

# In models.py - Update OTRVessel class

class OTRVessel(Base):
    """OTR Vessel - Direct table entry for vessel operations"""
    __tablename__ = "otr_vessel"
    __table_args__ = {'extend_existing': True}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    
    # Entry fields
    date = Column(Date, nullable=False, index=True)
    time = Column(String(5), nullable=False)  # HH:MM format
    shuttle_no = Column(String(50), nullable=False, index=True)
    vessel_id = Column(Integer, ForeignKey("vessels.id"), nullable=False)
    operation_id = Column(Integer, ForeignKey("vessel_operations.id"), nullable=False)
    
    # Stock values
    opening_stock = Column(Float, nullable=False, default=0.0)
    opening_water = Column(Float, nullable=False, default=0.0)  # ✅ NEW
    closing_stock = Column(Float, nullable=False, default=0.0)
    closing_water = Column(Float, nullable=False, default=0.0)  # ✅ NEW
    net_receipt_dispatch = Column(Float, nullable=False, default=0.0)
    net_water = Column(Float, nullable=False, default=0.0)  # ✅ NEW
    
    # Additional info
    remarks = Column(String(500), nullable=True)
    
    # Audit fields
    created_by = Column(String(100), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_by = Column(String(100), nullable=True)
    updated_at = Column(DateTime, onupdate=func.now())
    
    # Relationships
    location = relationship("Location")
    vessel = relationship("Vessel")
    operation = relationship("VesselOperation")

    def __repr__(self):
        return f"<OTRVessel(date='{self.date}', vessel_id={self.vessel_id}, shuttle='{self.shuttle_no}')>"

# ============================================================================
# FSO OPERATIONS
# ============================================================================

# In models.py - Update FSOOperation class

class FSOOperation(Base):
    """FSO-Operations table for Agge, Utapate, and Lagos (HO)"""
    __tablename__ = 'fso_operations'
    __table_args__ = {'extend_existing': True}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey('locations.id'), nullable=False)
    fso_vessel = Column(String(50), nullable=False)
    
    # Entry fields
    date = Column(Date, nullable=False)
    time = Column(Time, nullable=False)
    shuttle_no = Column(String(50), nullable=False)
    vessel_name = Column(String(100), nullable=False)
    operation = Column(String(50), nullable=False)
    
    # Stock values (in bbls)
    opening_stock = Column(Float, nullable=False)
    opening_water = Column(Float, nullable=False, default=0.0)  # ✅ NEW
    closing_stock = Column(Float, nullable=False)
    closing_water = Column(Float, nullable=False, default=0.0)  # ✅ NEW
    net_receipt_dispatch = Column(Float, nullable=False)
    net_water = Column(Float, nullable=False, default=0.0)  # ✅ NEW
    vessel_quantity = Column(Float, nullable=True)
    variance = Column(Float, nullable=True)  # ✅ NEW
    
    remarks = Column(Text, nullable=True)
    
    # Audit fields
    created_by = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_by = Column(String(50), nullable=True)
    updated_at = Column(DateTime, nullable=True)
    
    # Relationship
    location = relationship("Location", back_populates="fso_operations")

    def __repr__(self):
        return f"<FSOOperation(id={self.id}, fso='{self.fso_vessel}', date='{self.date}')>"


# ============================================================================
# CONVOY STATUS SNAPSHOTS (YADE / VESSEL)
# ============================================================================

class ConvoyStatusYade(Base):
    """Daily YADE convoy status snapshot for dashboards."""
    __tablename__ = "convoy_status_yade"
    __table_args__ = (
        UniqueConstraint("location_id", "date", "yade_barge_id", name="uq_convoy_yade_loc_date"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    yade_barge_id = Column(Integer, ForeignKey("yade_barges.id"), nullable=False, index=True)
    convoy_no = Column(String(64), nullable=True)
    stock_display = Column(String(200), nullable=True)
    stock_value_bbl = Column(Float, nullable=True)
    status = Column(String(64), nullable=False)
    notes = Column(String(255), nullable=True)
    created_by = Column(String(64), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_by = Column(String(64), nullable=True)
    updated_at = Column(DateTime, onupdate=func.now())

    location = relationship("Location")
    yade = relationship("YadeBarge")

    def __repr__(self):
        return (
            f"<ConvoyStatusYade(date={self.date}, yade='{self.yade_barge_id}', "
            f"status='{self.status}')>"
        )


class ConvoyStatusVessel(Base):
    """Daily vessel convoy status snapshot for dashboards."""
    __tablename__ = "convoy_status_vessel"
    __table_args__ = (
        UniqueConstraint("location_id", "date", "vessel_name", name="uq_convoy_vessel_loc_date"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    vessel_id = Column(Integer, ForeignKey("vessels.id"), nullable=True, index=True)
    vessel_name = Column(String(100), nullable=False)
    shuttle_no = Column(String(64), nullable=True)
    stock_display = Column(String(200), nullable=True)
    stock_value_bbl = Column(Float, nullable=True)
    status = Column(String(64), nullable=False)
    notes = Column(String(255), nullable=True)
    created_by = Column(String(64), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_by = Column(String(64), nullable=True)
    updated_at = Column(DateTime, onupdate=func.now())

    location = relationship("Location")
    vessel = relationship("Vessel")

    def __repr__(self):
        return (
            f"<ConvoyStatusVessel(date={self.date}, vessel='{self.vessel_name}', "
            f"status='{self.status}')>"
        )

# ============================================================================
# SECURITY & AUDIT
# ============================================================================

class Task(Base):
    """Workflow tasks for approvals and alerts"""
    __tablename__ = "tasks"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    task_type = Column(String(50), nullable=False, default=TaskType.INFO.value)
    status = Column(String(20), nullable=False, default=TaskStatus.PENDING.value)
    priority = Column(String(20), nullable=False, default="NORMAL")
    resource_type = Column(String(100), nullable=True)
    resource_id = Column(String(100), nullable=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)
    target_role = Column(String(30), nullable=False, default="supervisor")
    raised_by = Column(String(100), nullable=False)
    raised_by_role = Column(String(30), nullable=True)
    raised_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    approved_by = Column(String(100), nullable=True)
    approved_at = Column(DateTime, nullable=True)
    resolved_by = Column(String(100), nullable=True)
    resolved_at = Column(DateTime, nullable=True)
    resolution_notes = Column(Text, nullable=True)
    metadata_json = Column(Text, nullable=True)
    last_updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    location = relationship("Location")
    activities = relationship("TaskActivity", back_populates="task", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Task(id={self.id}, type='{self.task_type}', status='{self.status}')>"


class TaskActivity(Base):
    """Timeline entries for each task"""
    __tablename__ = "task_activities"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(Integer, ForeignKey("tasks.id"), nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)
    username = Column(String(100), nullable=True)
    action = Column(String(50), nullable=False)
    notes = Column(Text, nullable=True)
    
    task = relationship("Task", back_populates="activities")


class RecycleBinEntry(Base):
    """Archived snapshot of deleted records (soft delete bin)."""
    __tablename__ = "recycle_bin_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    resource_type = Column(String(100), nullable=False, index=True)
    resource_id = Column(String(100), nullable=False, index=True)
    resource_label = Column(String(255), nullable=True)
    payload_json = Column(Text, nullable=False)
    reason = Column(String(255), nullable=True)
    location_id = Column(Integer, nullable=True, index=True)
    deleted_by = Column(String(100), nullable=False)
    deleted_by_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    deleted_at = Column(DateTime, server_default=func.now(), nullable=False)

    deleted_by_user = relationship("User", foreign_keys=[deleted_by_id])

    __table_args__ = (
        Index(
            "idx_recycle_resource_lookup",
            "resource_type",
            "resource_id",
        ),
    )

    def __repr__(self):
        return f"<RecycleBinEntry(resource_type='{self.resource_type}', resource_id='{self.resource_id}')>"


class AuditLog(Base):
    """Audit log for tracking system actions"""
    __tablename__ = "audit_log"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, server_default=func.now())
    username = Column(String(100), nullable=False)
    action = Column(String(50), nullable=False)  # LOGIN, LOGOUT, CREATE, UPDATE, DELETE, etc.
    resource_type = Column(String(100), nullable=True)  # TankTransaction, User, etc.
    resource_id = Column(String(100), nullable=True)
    details = Column(String(500), nullable=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)
    ip_address = Column(String(50), nullable=True)
    success = Column(Boolean, default=True)
    
    # Relationships
    user = relationship("User", foreign_keys=[user_id])
    location = relationship("Location", foreign_keys=[location_id])
    
    def __repr__(self):
        return f"<AuditLog(user='{self.username}', action='{self.action}', time='{self.timestamp}')>"


class LoginAttempt(Base):
    """Track login attempts for security monitoring"""
    __tablename__ = "login_attempts"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, server_default=func.now(), nullable=False, index=True)
    username = Column(String(100), nullable=False, index=True)
    ip_address = Column(String(50), nullable=True)
    success = Column(Boolean, nullable=False)
    failure_reason = Column(String(200), nullable=True)
    
    # IP tracking fields
    ip_country = Column(String(100), nullable=True)
    ip_city = Column(String(100), nullable=True)
    ip_region = Column(String(100), nullable=True)
    
    # Device tracking fields
    device_type = Column(String(50), nullable=True)  # Desktop, Mobile, Tablet
    browser = Column(String(100), nullable=True)
    os = Column(String(100), nullable=True)
    user_agent = Column(String(500), nullable=True)
    
    # 2FA tracking
    two_factor_used = Column(Boolean, default=False, nullable=False)
    
    # Session tracking
    session_id = Column(String(64), nullable=True)


# ============================================================================
# DATABASE INDEXES FOR PERFORMANCE
# ============================================================================

# Tank transactions
Index('idx_tank_tx_location_date', TankTransaction.location_id, TankTransaction.date)
Index('idx_tank_tx_ticket', TankTransaction.ticket_id)

# OTR records
Index('idx_otr_location_date', OTRRecord.location_id, OTRRecord.date)
Index('idx_otr_ticket', OTRRecord.ticket_id)

# YADE voyages
Index('idx_yade_voyage_location', YadeVoyage.location_id, YadeVoyage.date)

# Tanker transactions
Index('idx_tanker_tx_location', TankerTransaction.location_id, TankerTransaction.transaction_date)

# Calibrations
Index('idx_tank_calibration', CalibrationTank.location_id, CalibrationTank.tank_name)
Index('idx_yade_calibration', YadeCalibration.yade_name, YadeCalibration.tank_id)

# Security & Audit
Index('idx_audit_timestamp', AuditLog.timestamp)
Index('idx_audit_user', AuditLog.user_id, AuditLog.timestamp)
Index('idx_login_attempts', LoginAttempt.username, LoginAttempt.timestamp)

# Vessel indexes
Index('idx_vessel_name', Vessel.name)
Index('idx_vessel_operation_name', VesselOperation.operation_name)
Index('idx_location_vessel', LocationVessel.location_id, LocationVessel.vessel_id)
Index('idx_otr_vessel_vessel_id', OTRVessel.vessel_id)
Index('idx_otr_vessel_operation_id', OTRVessel.operation_id)

# FSO indexes ✅ ADDED
Index('idx_fso_location_date', FSOOperation.location_id, FSOOperation.date)
Index('idx_fso_vessel', FSOOperation.fso_vessel)
Index('idx_fso_shuttle', FSOOperation.shuttle_no)

# ============================================================================
# CREATE TABLES
# ============================================================================

if engine is not None:
    Base.metadata.create_all(bind=engine)
    # Avoid non-ASCII to prevent Windows console encoding issues
    print("All database tables created successfully!")
class ReportDefinition(Base):
    __tablename__ = "report_definitions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=True)
    name = Column(String(100), nullable=False)
    slug = Column(String(100), unique=True, nullable=False)
    config_json = Column(Text, nullable=False)
    is_active = Column(Boolean, default=True)
    created_by = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, onupdate=datetime.utcnow)
