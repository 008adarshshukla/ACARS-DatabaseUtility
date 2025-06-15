import pandas as pd
import sqlite3
import os
import logging
import re
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Map exactly to your normalized Excel headers
KEY_COLS = {
    'id':        'id',
    'latitude':  'latitude',
    'longitude': 'longitude',
    'frequency': 'frequency',
    'name':      'name',
}

# Regex to parse DMS: allow N, S, E, or W, e.g. “N 52° 04' 28.00"
DMS_RE = re.compile(
    r"""^\s*([NSEW])\s*    # Hemisphere
        (\d+)[°\s]+       # Degrees
        (\d+)[\'\s]+      # Minutes
        ([0-9.]+)"?       # Seconds
        \s*$""",
    re.VERBOSE
)

def normalize_dms_string(dms_str: str) -> Optional[str]:
    """Strip everything except the hemisphere letter and digits."""
    if not dms_str:
        return None
    norm = re.sub(r'[^A-Za-z0-9]', '', dms_str)
    return norm or None

def dms_to_decimal(dms_str: str) -> Optional[float]:
    """Convert a DMS string to decimal degrees."""
    if not dms_str:
        return None
    m = DMS_RE.match(dms_str)
    if not m:
        return None
    hemi, deg, mins, secs = m.groups()
    dd = float(deg) + float(mins) / 60 + float(secs) / 3600
    return -dd if hemi in ('S', 'W') else dd

def deg_min(value: float) -> Tuple[int, int]:
    """Return (degrees, minutes) from a decimal-degree value."""
    a = abs(value)
    d = int(a)
    m = int((a - d) * 60)
    return d, m

def load_excel(excel_path: str) -> pd.DataFrame:
    """Read and normalize the Excel data."""
    raw = pd.read_excel(excel_path, header=None, dtype=str)
    header = raw.iloc[0, 0]
    cols = [c.strip().strip('"').strip("'") for c in header.split(',')]
    records = []
    for i in range(1, len(raw)):
        line = raw.iloc[i, 0]
        if pd.isna(line):
            continue
        vals = [v.strip().strip('"').strip("'") for v in str(line).split(',')]
        if len(vals) != len(cols):
            logging.warning(f"Skipping malformed row {i+1}: expected {len(cols)} values but got {len(vals)}")
            continue
        records.append(vals)
    df = pd.DataFrame(records, columns=cols)
    df.columns = [c.lower().strip() for c in df.columns]
    return df

def merge_into_sqlite(df: pd.DataFrame, db_path: str):
    table = 'primary_D_B_base_Navaid - NDB Navaid'
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute('PRAGMA busy_timeout = 30000;')
    cursor = conn.cursor()

    # Ensure required columns exist
    for key, col in KEY_COLS.items():
        if col not in df.columns:
            raise KeyError(f"Missing column '{col}' (available: {df.columns.tolist()})")

    total = skipped_empty = skipped_exact = skipped_degmin = inserted = 0

    for _, row in df.iterrows():
        total += 1
        ndb_id = row[KEY_COLS['id']] or ''
        if not ndb_id:
            skipped_empty += 1
            logging.warning("Skipping row with empty ID")
            continue

        raw_lat = row[KEY_COLS['latitude']] or ''
        raw_lon = row[KEY_COLS['longitude']] or ''
        freq_str = row[KEY_COLS['frequency']] or ''
        name     = row[KEY_COLS['name']] or None

        lat_norm = normalize_dms_string(raw_lat)
        lon_norm = normalize_dms_string(raw_lon)
        lat_dec  = dms_to_decimal(raw_lat)
        lon_dec  = dms_to_decimal(raw_lon)

        # 1) Exact normalized DMS-string duplicate
        cursor.execute(f"""
            SELECT 1 FROM "{table}"
             WHERE NDBIdentifier = ?
               AND NDBLatitude   = ?
               AND NDBLongitude  = ?
             LIMIT 1
        """, (ndb_id, lat_norm, lon_norm))
        if cursor.fetchone():
            skipped_exact += 1
            continue

        # 2) Deg/min duplicate for same NDBIdentifier
        if lat_dec is not None and lon_dec is not None:
            cursor.execute(f"""
                SELECT NDBLatitude_WGS84, NDBLongitude_WGS84
                  FROM "{table}"
                 WHERE NDBIdentifier = ?
            """, (ndb_id,))
            for ext_lat_raw, ext_lon_raw in cursor.fetchall():
                try:
                    ext_lat = float(ext_lat_raw)
                    ext_lon = float(ext_lon_raw)
                except (TypeError, ValueError):
                    continue
                nd, nm = deg_min(lat_dec)
                od, om = deg_min(ext_lat)
                if nd == od and nm == om:
                    ld, lm = deg_min(lon_dec)
                    od2, om2 = deg_min(ext_lon)
                    if ld == od2 and lm == om2:
                        skipped_degmin += 1
                        break
            else:
                pass

            if skipped_exact + skipped_degmin + skipped_empty + inserted == total:
                continue

        # Frequency ×10
        try:
            freq = int(float(freq_str) * 10) if freq_str else None
        except ValueError:
            logging.warning(f"Invalid frequency '{freq_str}' for '{ndb_id}' → NULL")
            freq = None

        # Insert row
        cursor.execute(f"""
            INSERT INTO "{table}"
              (NDBIdentifier,
               NDBLatitude,
               NDBLongitude,
               NDBFrequency,
               NDBName,
               NDBLatitude_WGS84,
               NDBLongitude_WGS84)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            ndb_id,
            lat_norm,
            lon_norm,
            freq,
            name,
            lat_dec,
            lon_dec,
        ))
        inserted += 1

    conn.commit()
    conn.close()

    # Summary
    logging.info(f"Total rows read:          {total}")
    logging.info(f"Skipped (empty ID):       {skipped_empty}")
    logging.info(f"Skipped (exact dup):      {skipped_exact}")
    logging.info(f"Skipped (deg/min dup):    {skipped_degmin}")
    logging.info(f"Inserted new rows:        {inserted}")

if __name__ == '__main__':
    EXCEL = os.path.expanduser('~/Downloads/navdb/ndb.xlsx')
    DB    = os.path.expanduser('~/Dev/MCDUDatabaseWorldwide.db')

    logging.info("Loading Excel data…")
    df = load_excel(EXCEL)
    logging.info(f"Loaded {len(df)} rows; columns: {df.columns.tolist()}")

    logging.info("Merging into SQLite database…")
    merge_into_sqlite(df, DB)
    logging.info("Done.")
