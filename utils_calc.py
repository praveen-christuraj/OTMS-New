# utils_calc.py
import math
from typing import Optional, Tuple
import pandas as pd
from sqlalchemy.orm import Session

from models import CalibrationTank, Table11

# ---------- Linear interpolation helpers ----------
def _interp(x1, y1, x2, y2, x):
    if x1 == x2:
        return float(y1)
    return float(y1) + (float(x) - float(x1)) * (float(y2) - float(y1)) / (float(x2) - float(x1))

def _two_point(df: pd.DataFrame, xcol: str, ycol: str, x: float) -> float:
    lower = df[df[xcol] <= x].tail(1)
    upper = df[df[xcol] >= x].head(1)
    if lower.empty and upper.empty:
        raise ValueError("No data points for interpolation")
    if lower.empty:
        return float(upper[ycol].values[0])
    if upper.empty:
        return float(lower[ycol].values[0])
    x1, y1 = float(lower[xcol].values[0]), float(lower[ycol].values[0])
    x2, y2 = float(upper[xcol].values[0]), float(upper[ycol].values[0])
    return _interp(x1, y1, x2, y2, x)

# ---------- Tank Calibration lookups ----------
def tank_volume_from_dip_cm(sess: Session, tank_name: str, dip_cm: float) -> float:
    rows = (sess.query(CalibrationTank)
                 .filter(CalibrationTank.tank_name == tank_name)
                 .all())
    if not rows:
        raise ValueError(f"No tank calibration for {tank_name}")
    df = pd.DataFrame([{"dip_cm": r.dip_cm, "volume_bbl": r.volume_bbl} for r in rows]).sort_values("dip_cm")
    return _two_point(df, "dip_cm", "volume_bbl", dip_cm)

# ---------- Free Water by water dip ----------
def free_water_from_water_cm(sess: Session, tank_name: str, water_cm: float) -> float:
    if water_cm is None or water_cm <= 0:
        return 0.0
    # reuse same calibration curve (dip->vol) for water height
    return tank_volume_from_dip_cm(sess, tank_name, water_cm)

# ---------- VCF (from doc) ----------
def calculate_vcf(api60: float, temp_f: float) -> float:
    if api60 <= 0:
        raise ValueError("API gravity must be positive")
    temp_diff = float(temp_f) - 60.0
    rho = (141.5 * 999.012) / (float(api60) + 131.5)
    alpha = 341.0957 / (rho * rho)
    vcf = math.exp(-alpha * temp_diff - 0.8 * alpha * alpha * temp_diff * temp_diff)
    return round(vcf, 5)

# ---------- LT factor from Table11 with interpolation ----------
def get_lt_factor(sess: Session, api60: float) -> float:
    rows = sess.query(Table11).order_by(Table11.api60).all()
    if not rows:
        raise ValueError("Table11 is empty. Please import data.")
    df = pd.DataFrame([{"api60": r.api60, "lt_factor": r.lt_factor} for r in rows])
    return _two_point(df, "api60", "lt_factor", api60)

# ---------- Convert API(Observed @ sample F) -> API @60°F (VBA-ported) ----------
def api_observed_to_api60(api_obs: float, temp_obs_f: float) -> float:
    temp_diff = temp_obs_f - 60.0
    rho_obs = (141.5 * 999.012 / (131.5 + api_obs)) * ((1 - 0.00001278 * temp_diff) - (0.0000000062 * temp_diff * temp_diff))
    RH = rho_obs
    for _ in range(10):
        alfa = 341.0957 / (RH * RH)
        vcf = math.exp(-alfa * temp_diff - 0.8 * alfa * alfa * temp_diff * temp_diff)
        RH = rho_obs / vcf
    api60 = 141.5 * 999.012 / RH - 131.5
    return float(api60)

# ---------- Convert Density(observed @ °C) -> API @60°F (doc algorithm) ----------
def density_obs_to_api60(density_obs: float, sample_temp_c: float) -> Tuple[float, float]:
    # returns (api60, density15)
    temp_diff = sample_temp_c - 15.0
    HYC = 1 - 0.000023 * temp_diff - 0.00000002 * temp_diff * temp_diff
    RHO1 = density_obs * HYC

    RH = RHO1
    for _ in range(12):
        alfa = 613.9723 * RH * RH
        vcf = math.exp(-alfa * RH - 0.8 * alfa * alfa * RH * RH)
        RH = RHO1 * vcf

    density15 = RHO1 * vcf
    sg60 = density15 / 999.012
    api60 = (141.5 / sg60) - 131.5
    return float(api60), float(density15)

# ---------- Driver: compute all ----------
def compute_all_for_tank_tx(
    sess: Session,
    *,
    tank_name: str,
    dip_cm: float,
    water_cm: float,
    tank_temp_c: Optional[float],
    tank_temp_f: Optional[float],
    api_observed: Optional[float],
    density_observed: Optional[float],
    sample_temp_c: Optional[float],
    sample_temp_f: Optional[float],
    bsw_pct: Optional[float],
) -> dict:
    # TOV/FW/GOV
    TOV = tank_volume_from_dip_cm(sess, tank_name, float(dip_cm or 0.0))
    FW = free_water_from_water_cm(sess, tank_name, float(water_cm or 0.0))
    GOV = max(TOV - FW, 0.0)

    # API @60 from whichever observed is provided
    api60 = None
    density15 = None
    if (api_observed or 0) > 0 and (sample_temp_f or 0) > 0:
        api60 = api_observed_to_api60(float(api_observed), float(sample_temp_f))
    elif (density_observed or 0) > 0 and (sample_temp_c or 0) > 0:
        api60, density15 = density_obs_to_api60(float(density_observed), float(sample_temp_c))
    else:
        api60 = 0.0

    # tank temp °F
    if tank_temp_f is None and tank_temp_c is not None:
        tank_temp_f = (float(tank_temp_c) * 1.8) + 32.0
    tank_temp_f = float(tank_temp_f or 60.0)

    # VCF
    vcf = calculate_vcf(float(api60), float(tank_temp_f)) if api60 and api60 > 0 else 1.0

    # GSV (round 0dp)
    GSV = int(round(GOV * vcf, 0))

    # BS&W volume (assume percent → fraction)
    bsw_fraction = float(bsw_pct or 0.0) / 100.0
    bsw_vol = int(round(GSV * bsw_fraction, 0))

    # NSV = GSV - BS&W (see note above)
    NSV = int(round(GSV - bsw_vol, 0))

    # LT factor
    lt_factor = get_lt_factor(sess, float(api60)) if api60 and api60 > 0 else 0.0

    # LT / MT (round 0dp)
    LT = int(round(NSV * lt_factor, 0))
    MT = int(round(LT * 1.01605, 0))

    return {
        "TOV": float(TOV),
        "FW": float(FW),
        "GOV": float(GOV),
        "api60": float(api60 or 0.0),
        "density15": float(density15 or 0.0),
        "vcf": float(vcf),
        "GSV": int(GSV),
        "bsw_vol": int(bsw_vol),
        "NSV": int(NSV),
        "lt_factor": float(lt_factor or 0.0),
        "LT": int(LT),
        "MT": int(MT),
    }
