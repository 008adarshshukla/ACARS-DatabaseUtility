import pandas as pd
import sqlite3
import os
import logging
import re
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')

# Mapping of Excel header substrings to our keys
EXCEL_COLS = {
    'id':         'id',
    'latitude':   'latitude',
    'longitude':  'longitude',
    'elevation':  'elevation',
    'magvar':     'magnetic variation',
    'speed_alt':  'speed limit altitude',
    'trans_alt':  'transition altitude',
    'trans_lvl':  'transition level',
    'name':       'name',
}

# Regex to parse DMS strings
DMS_RE = re.compile(
    r"""^\s*([NSEW])\s*      # Hemisphere
        (\d+)[°\s]+         # Degrees
        (\d+)[\'\s]+        # Minutes
        ([0-9.]+)"?         # Seconds
        \s*$""", 
    re.VERBOSE
)

def normalize_dms_string(s: str) -> Optional[str]:
    if not s:
        return None
    return re.sub(r'[^A-Za-z0-9]', '', s) or None

def dms_to_decimal(s: str) -> Optional[float]:
    if not s:
        return None
    m = DMS_RE.match(s)
    if not m:
        return None
    hemi, deg, mins, secs = m.groups()
    dd = float(deg) + float(mins)/60 + float(secs)/3600
    return -dd if hemi in ('S','W') else dd

def format_magvar(s: str) -> Optional[str]:
    if not s:
        return None
    try:
        v = float(s)
    except ValueError:
        logging.warning(f"Invalid magnetic variation '{s}'; NULL")
        return None
    hemi = 'E' if v >= 0 else 'W'
    mag = int(abs(v)*10)
    return f"{hemi}{mag:04d}"

def load_excel(path: str) -> pd.DataFrame:
    """
    Load Airports.xlsx, using row 2 (index 1) as header,
    data from row 3 onward. Skip malformed rows.
    """
    raw = pd.read_excel(path, header=None, dtype=str)
    # row index 1 -> second row in file
    header_row = raw.iloc[1, 0]
    cols = [c.strip().strip('"').strip("'").lower() for c in header_row.split(',')]
    records = []
    for i in range(2, len(raw)):  # start at index 2 (third row)
        line = raw.iloc[i, 0]
        if pd.isna(line):
            continue
        parts = [p.strip().strip('"').strip("'") for p in str(line).split(',')]
        if len(parts) != len(cols):
            logging.warning(f"Skipping malformed row {i+1}: expected {len(cols)} values but got {len(parts)}")
            continue
        records.append(parts)
    return pd.DataFrame(records, columns=cols)

def find_col(df: pd.DataFrame, substr: str) -> str:
    for c in df.columns:
        if substr in c:
            return c
    raise KeyError(f"Missing column containing '{substr}' (got {df.columns.tolist()})")

def merge_airports(df: pd.DataFrame, db_path: str):
    table = "primary_P_A_base_Airport - Reference Points"
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA busy_timeout = 30000;")
    cur = conn.cursor()

    # Map DataFrame columns
    col = {
        'id':         find_col(df, EXCEL_COLS['id']),
        'lat':        find_col(df, EXCEL_COLS['latitude']),
        'lon':        find_col(df, EXCEL_COLS['longitude']),
        'elev':       find_col(df, EXCEL_COLS['elevation']),
        'magvar':     find_col(df, EXCEL_COLS['magvar']),
        'speed_alt':  find_col(df, EXCEL_COLS['speed_alt']),
        'trans_alt':  find_col(df, EXCEL_COLS['trans_alt']),
        'trans_lvl':  find_col(df, EXCEL_COLS['trans_lvl']),
        'name':       find_col(df, EXCEL_COLS['name']),
    }

    total = skipped_empty = skipped_dup = inserted = 0

    for _, row in df.iterrows():
        total += 1
        ident = row[col['id']] or ''
        if not ident:
            skipped_empty += 1
            logging.warning("Skipping row with empty ICAO identifier")
            continue

        # Duplicate check: skip if identifier already exists
        cur.execute(f"""
            SELECT 1 FROM "{table}"
             WHERE LandingFacilityIcaoIdentifier = ?
             LIMIT 1
        """, (ident,))
        if cur.fetchone():
            skipped_dup += 1
            continue

        raw_lat = row[col['lat']] or ''
        raw_lon = row[col['lon']] or ''
        norm_lat = normalize_dms_string(raw_lat)
        norm_lon = normalize_dms_string(raw_lon)
        dec_lat = dms_to_decimal(raw_lat)
        dec_lon = dms_to_decimal(raw_lon)

        elev = None
        try:
            elev = float(row[col['elev']])
        except:
            logging.warning(f"Invalid elevation '{row[col['elev']]}' for '{ident}'")

        magvar = format_magvar(row[col['magvar']])
        speed_alt = None
        try:
            speed_alt = int(row[col['speed_alt']])
        except:
            logging.warning(f"Invalid speed limit altitude '{row[col['speed_alt']]}' for '{ident}'")

        trans_alt = None
        try:
            trans_alt = int(row[col['trans_alt']])
        except:
            logging.warning(f"Invalid transition altitude '{row[col['trans_alt']]}' for '{ident}'")

        trans_lvl = None
        try:
            trans_lvl = int(row[col['trans_lvl']])
        except:
            logging.warning(f"Invalid transition level '{row[col['trans_lvl']]}' for '{ident}'")

        name = row[col['name']] or None

        cur.execute(f"""
            INSERT INTO "{table}"
              (LandingFacilityIcaoIdentifier,
               AirportReferencePtLatitude,
               AirportReferencePtLongitude,
               AirportElevation,
               MagneticVariation,
               SpeedLimitAltitude,
               TransitionsAltitude,
               TransitionLevel,
               AirportName,
               AirportReferencePtLongitude_WGS84,
               AirportReferencePtLatitude_WGS84)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ident,
            norm_lat,
            norm_lon,
            elev,
            magvar,
            speed_alt,
            trans_alt,
            trans_lvl,
            name,
            dec_lon,
            dec_lat
        ))
        inserted += 1

    conn.commit()
    conn.close()

    logging.info(f"Total rows read:       {total}")
    logging.info(f"Skipped (empty ID):    {skipped_empty}")
    logging.info(f"Skipped (duplicates):  {skipped_dup}")
    logging.info(f"Inserted new rows:     {inserted}")

if __name__ == "__main__":
    EXCEL = os.path.expanduser("~/Downloads/navdb/Airports.xlsx")
    DB    = os.path.expanduser("~/Dev/MCDUDatabaseWorldwide.db")

    logging.info("Loading Excel data…")
    df = load_excel(EXCEL)
    logging.info(f"Loaded {len(df)} valid rows; columns: {df.columns.tolist()}")

    logging.info("Merging Airport reference points…")
    merge_airports(df, DB)
    logging.info("Done.")
