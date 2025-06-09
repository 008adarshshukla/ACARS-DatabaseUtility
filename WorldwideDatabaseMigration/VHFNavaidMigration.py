import pandas as pd
import sqlite3
import os
import logging
import re
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Map normalized Excel headers to DataFrame keys
KEY_COLS = {
    'id':          'id',
    'vor_freq':    'frequency',
    'vor_lat':     'vor latitude',
    'vor_lon':     'vor longitude',
    'dme_lat':     'dme latitude',
    'dme_lon':     'dme longitude',
    'declination': 'station declination',
    'elevation':   'elevation',
    'name':        'name',
}

# Regex to parse DMS: allow N, S, E, or W
DMS_RE = re.compile(
    r"""^\s*([NSEW])\s*    # Hemisphere
        (\d+)[°\s]+       # Degrees
        (\d+)[\'\s]+      # Minutes
        ([0-9.]+)"?       # Seconds
        \s*$""",
    re.VERBOSE
)

def normalize_dms_string(dms_str: str) -> Optional[str]:
    if not dms_str:
        return None
    norm = re.sub(r'[^A-Za-z0-9]', '', dms_str)
    return norm or None

def dms_to_decimal(dms_str: str) -> Optional[float]:
    if not dms_str:
        return None
    m = DMS_RE.match(dms_str)
    if not m:
        return None
    hemi, deg, mins, secs = m.groups()
    dd = float(deg) + float(mins) / 60 + float(secs) / 3600
    return -dd if hemi in ('S', 'W') else dd

def deg_min(value: float) -> Tuple[int, int]:
    a = abs(value)
    d = int(a)
    m = int((a - d) * 60)
    return d, m

def format_declination(val_str: str) -> Optional[str]:
    if not val_str:
        return None
    try:
        v = float(val_str)
    except ValueError:
        logging.warning(f"Invalid declination '{val_str}'; using NULL")
        return None
    hemi = 'E' if v >= 0 else 'W'
    mag = int(abs(v) * 10)
    return f"{hemi}{mag:04d}"

def load_excel(excel_path: str) -> pd.DataFrame:
    raw = pd.read_excel(excel_path, header=None, dtype=str)
    header = raw.iloc[0, 0]
    cols = [c.strip().strip('"').strip("'").lower() for c in header.split(',')]
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
    return df

def merge_vhf(df: pd.DataFrame, db_path: str):
    table = 'primary_D__base_Navaid - VHF Navaid'
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute('PRAGMA busy_timeout = 30000;')
    cursor = conn.cursor()

    # Verify required columns
    for key, col in KEY_COLS.items():
        if col not in df.columns:
            raise KeyError(f"Missing column '{col}' (found: {df.columns.tolist()})")

    total_rows = skipped_empty = skipped_exact = skipped_degmin = inserted = 0

    for _, row in df.iterrows():
        total_rows += 1
        vid = row[KEY_COLS['id']] or ''
        if not vid:
            skipped_empty += 1
            logging.warning("Skipping row with empty VORIdentifier")
            continue

        raw_lat = row[KEY_COLS['vor_lat']] or ''
        raw_lon = row[KEY_COLS['vor_lon']] or ''
        norm_lat = normalize_dms_string(raw_lat)
        norm_lon = normalize_dms_string(raw_lon)
        dec_lat = dms_to_decimal(raw_lat)
        dec_lon = dms_to_decimal(raw_lon)

        # 1) Exact DMS-string duplicate
        cursor.execute(f"""
            SELECT 1 FROM "{table}"
             WHERE VORIdentifier = ?
               AND VORLatitude   = ?
               AND VORLongitude  = ?
             LIMIT 1
        """, (vid, norm_lat, norm_lon))
        if cursor.fetchone():
            skipped_exact += 1
            continue

        # 2) Deg/min duplicate for same Identifier
        if dec_lat is not None and dec_lon is not None:
            cursor.execute(f"""
                SELECT VORLatitude_WGS84, VORLongitude_WGS84
                  FROM "{table}"
                 WHERE VORIdentifier = ?
            """, (vid,))
            for ext_lat_raw, ext_lon_raw in cursor.fetchall():
                # Coerce to float, skip non-numeric
                try:
                    ext_lat = float(ext_lat_raw)
                    ext_lon = float(ext_lon_raw)
                except (TypeError, ValueError):
                    continue

                nd, nm = deg_min(dec_lat)
                od, om = deg_min(ext_lat)
                if nd == od and nm == om:
                    ld, lm = deg_min(dec_lon)
                    od2, om2 = deg_min(ext_lon)
                    if ld == od2 and lm == om2:
                        skipped_degmin += 1
                        break
            else:
                # no break → not duplicate by deg/min
                pass

            # if we incremented skipped_degmin just now, skip insert
            if skipped_degmin + skipped_exact + skipped_empty + inserted == total_rows:
                continue

        # Convert other fields
        try:
            vor_freq = int(float(row[KEY_COLS['vor_freq']]) * 100)
        except:
            logging.warning(f"Invalid frequency '{row[KEY_COLS['vor_freq']]}' for '{vid}'; using NULL")
            vor_freq = None

        decl = format_declination(row[KEY_COLS['declination']])
        try:
            elev = float(row[KEY_COLS['elevation']])
        except:
            logging.warning(f"Invalid elevation '{row[KEY_COLS['elevation']]}' for '{vid}'; using NULL")
            elev = None

        raw_dme_lat = row[KEY_COLS['dme_lat']] or ''
        raw_dme_lon = row[KEY_COLS['dme_lon']] or ''
        norm_dme_lat = normalize_dms_string(raw_dme_lat)
        norm_dme_lon = normalize_dms_string(raw_dme_lon)
        dec_dme_lat = dms_to_decimal(raw_dme_lat)
        dec_dme_lon = dms_to_decimal(raw_dme_lon)

        name = row[KEY_COLS['name']] or None

        cursor.execute(f"""
            INSERT INTO "{table}"
              (VORIdentifier,
               VORFrequency,
               VORLatitude,
               VORLongitude,
               DMELatitude,
               DMELongitude,
               StationDeclination,
               DMEElevation,
               VORName,
               DMELongitude_WGS84,
               DMELatitude_WGS84,
               VORLongitude_WGS84,
               VORLatitude_WGS84)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            vid,
            vor_freq,
            norm_lat,
            norm_lon,
            norm_dme_lat,
            norm_dme_lon,
            decl,
            elev,
            name,
            dec_dme_lon,
            dec_dme_lat,
            dec_lon,
            dec_lat,
        ))
        inserted += 1

    conn.commit()
    conn.close()

    # Summary logging
    logging.info(f"Total rows read:           {total_rows}")
    logging.info(f"Skipped (empty ID):        {skipped_empty}")
    logging.info(f"Skipped (exact DMS dup):   {skipped_exact}")
    logging.info(f"Skipped (deg/min dup):     {skipped_degmin}")
    logging.info(f"Inserted new rows:         {inserted}")

if __name__ == '__main__':
    EXCEL_FILE = os.path.expanduser('~/Downloads/navdb/Navaids.xlsx')
    DB_FILE    = os.path.expanduser('~/Dev/MCDUDatabaseWorldwide.db')

    logging.info("Loading Excel data…")
    df = load_excel(EXCEL_FILE)
    logging.info(f"Loaded {len(df)} valid rows; columns: {df.columns.tolist()}")

    logging.info("Merging VHF Navaids…")
    merge_vhf(df, DB_FILE)
    logging.info("Merge complete.")
