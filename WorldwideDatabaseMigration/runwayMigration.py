#!/usr/bin/env python3

import os
import re
import pandas as pd
import sqlite3
import logging
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Paths to your files (update if needed)
XLSX_PATH   = os.path.expanduser('~/Downloads/navdb/Runways.xlsx')
DB_PATH     = os.path.expanduser('~/Dev/MCDUDatabaseWorldwide.db')
TABLE_NAME  = 'primary_P_G_base_Airport - Runways'

# Regex to parse DMS strings (e.g. N 61° 09' 15.85")
DMS_RE = re.compile(
    r"""^\s*([NSEW])\s*        # Hemisphere
        (\d+)[°\s]+           # Degrees
        (\d+)[\'\s]+          # Minutes
        ([0-9.]+)"?           # Seconds
        \s*$""",
    re.VERBOSE
)

def dms_to_decimal(s: str) -> Optional[float]:
    if not isinstance(s, str):
        return None
    m = DMS_RE.match(s.strip())
    if not m:
        logger.warning(f"Cannot parse DMS coordinate: '{s}'")
        return None
    hemi, deg, mins, secs = m.groups()
    dd = float(deg) + float(mins) / 60 + float(secs) / 3600
    return -dd if hemi in ('S', 'W') else dd

def parse_bearing(raw: str) -> str:
    """
    Strip any trailing letters (e.g. " M"), parse as float,
    multiply by 10, and zero-pad to 4 digits.
    """
    if not isinstance(raw, str):
        raw = ''
    # remove any non-numeric/trailing characters
    cleaned = re.sub(r'[^0-9.]+$', '', raw.strip())
    try:
        val = float(cleaned)
        bearing_val = int(round(val * 10))
        return str(bearing_val).zfill(4)
    except Exception:
        logger.warning(f"Invalid Bearing '{raw}', defaulting to 0000")
        return '0000'

def main():
    df = pd.read_excel(XLSX_PATH, dtype=str)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        ident  = row.get('Airport', '').strip()
        rwy_id = row.get('Id',      '').strip()
        if not ident or not rwy_id:
            logger.warning("Skipping row with missing Airport or Id")
            skipped += 1
            continue

        cursor.execute(
            f"SELECT 1 FROM \"{TABLE_NAME}\" WHERE LandingFacilityIcaoIdentifier=? AND RunwayIdentifier=?",
            (ident, rwy_id)
        )
        if cursor.fetchone():
            logger.info(f"Skipping duplicate: {ident} / {rwy_id}")
            skipped += 1
            continue

        # Parse latitude/longitude DMS → decimal
        raw_lat = row.get('Latitude', '')
        raw_lon = row.get('Longitude','')
        dec_lat = dms_to_decimal(raw_lat)
        dec_lon = dms_to_decimal(raw_lon)
        if dec_lat is None or dec_lon is None:
            logger.error(f"Invalid coords for {ident}/{rwy_id}: '{raw_lat}', '{raw_lon}'")
            skipped += 1
            continue

        # Elevation → 5-digit zero-padded
        try:
            elev = str(int(float(row.get('Elevation', 0)))).zfill(5)
        except:
            elev = '00000'
            logger.warning(f"Invalid Elevation for {ident}/{rwy_id}")

        # Bearing → cleaned, *10, zero-pad to 4
        bearing = parse_bearing(row.get('Bearing', ''))

        # Other numeric fields
        length = float(row.get('Length', 0) or 0)
        width  = float(row.get('Width', 0)  or 0)
        tch    = float(row.get('Threshold Crossing Height', 0) or 0)

        try:
            dtd = str(int(float(row.get('Threshold Displacement Distance', 0)))).zfill(4)
        except:
            dtd = '0000'
            logger.warning(f"Invalid DisplacedThresholdDistance for {ident}/{rwy_id}")

        grad = float(row.get('Gradient', 0) or 0)

        # Insert into the database
        cursor.execute(
            f"""
            INSERT INTO "{TABLE_NAME}" (
                LandingFacilityIcaoIdentifier,
                RunwayIdentifier,
                RunwayLatitude,
                RunwayLongitude,
                LandingThresholdElevation,
                RunwayMagneticBearing,
                RunwayLength,
                RunwayWidth,
                ThresholdCrossingHeight,
                DisplacedThresholdDistance,
                RunwayGradient,
                RunwayLongitude_WGS84,
                RunwayLatitude_WGS84
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ident,
                rwy_id,
                dec_lat,
                dec_lon,
                elev,
                bearing,
                length,
                width,
                tch,
                dtd,
                grad,
                dec_lon,
                dec_lat
            )
        )
        logger.info(f"Inserted: {ident} / {rwy_id}")
        inserted += 1

    conn.commit()
    conn.close()
    logger.info(f"Runway data import complete: {inserted} inserted, {skipped} skipped.")

if __name__ == '__main__':
    main()
