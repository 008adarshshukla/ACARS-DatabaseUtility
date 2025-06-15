import sqlite3
import logging
import os

# ——— CONFIGURATION ———
DB_PATH = os.path.expanduser('~/Dev/MCDUDatabase.db')

# The two tables you want to update:
TARGET_TABLES = [
    "primary_P_D_base_Airport - SIDs",
    "primary_P_E_base_Airport - STARs",
]

# In order, the source tables and their matching columns:
SOURCES = [
    ("primary_P_C_base_Airport - Terminal Waypoints",
     "WaypointIdentifier", "WaypointLatitude_WGS84", "WaypointLongitude_WGS84"),
    ("primary_E_A_base_Enroute - Grid Waypoints",
     "WaypointIdentifier", "WaypointLatitude_WGS84", "WaypointLongitude_WGS84"),
    ("primary_D_B_base_Navaid_Enroute - NDB Navaid",
     "NDBIdentifier",      "NDBLatitude_WGS84",      "NDBLongitude_WGS84"),
    ("primary_D_B_base_Navaid_Terminal - NDB Navaid",
     "NDBIdentifier",      "NDBLatitude_WGS84",      "NDBLongitude_WGS84"),
    ("primary_D__base_Navaid - VHF Navaid",
     "VORIdentifier",      "VORLatitude_WGS84",      "VORLongitude_WGS84")
]

# ——— LOGGING SETUP ———
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

def ensure_declination_column(cur, table_name):
    """Add Declination column if it doesn't exist."""
    cols = [r["name"].lower() for r in cur.execute(f"PRAGMA table_info([{table_name}])")]
    if "declination" not in cols:
        logger.info(f"Adding Declination column to [{table_name}]")
        cur.execute(f"ALTER TABLE [{table_name}] ADD COLUMN Declination REAL;")
    else:
        logger.info(f"Declination column already exists in [{table_name}]")

def find_declination(cur, ident, lat, lon):
    """Search each source table in order; return first Declination found or None."""
    for tbl, idcol, latcol, loncol in SOURCES:
        row = cur.execute(
            f"""SELECT Declination
                  FROM [{tbl}]
                 WHERE {idcol} = ?
                   AND {latcol} = ?
                   AND {loncol} = ?
                 LIMIT 1""",
            (ident, lat, lon)
        ).fetchone()
        if row:
            logger.debug(f"Matched in [{tbl}] → Declination={row[0]}")
            return row[0]
    return None

def update_table(db_path, table_name):
    logger.info(f"--- Starting update for [{table_name}] ---")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    ensure_declination_column(cur, table_name)
    conn.commit()

    # fetch all relevant rows
    cur.execute(f"""
        SELECT rowid, FixIdentifier, FixIdentifierLatitude_WGS84 AS lat, FixIdentifierLongitude_WGS84 AS lon
          FROM [{table_name}]
    """)
    rows = cur.fetchall()
    total = len(rows)
    logger.info(f"Found {total} rows to process in [{table_name}]")

    # counters
    updated = 0
    skipped = 0
    no_match = 0

    for idx, row in enumerate(rows, 1):
        ident, lat, lon = row["FixIdentifier"], row["lat"], row["lon"]
        if not (ident and lat is not None and lon is not None):
            skipped += 1
            logger.warning(f"Row {idx}/{total} (rowid={row['rowid']}) missing key data; skipping")
            continue

        decl = find_declination(cur, ident, lat, lon)
        if decl is not None:
            cur.execute(
                f"UPDATE [{table_name}] SET Declination = ? WHERE rowid = ?",
                (decl, row["rowid"])
            )
            updated += 1
            logger.info(f"Row {idx}/{total} (rowid={row['rowid']}): set Declination={decl}")
        else:
            no_match += 1
            logger.info(f"Row {idx}/{total} (rowid={row['rowid']}): no matching declination found")

    conn.commit()
    conn.close()
    logger.info(
        f"--- Finished [{table_name}]: {updated} updated, "
        f"{no_match} no-match, {skipped} skipped out of {total} rows ---"
    )

if __name__ == "__main__":
    for tbl in TARGET_TABLES:
        update_table(DB_PATH, tbl)
