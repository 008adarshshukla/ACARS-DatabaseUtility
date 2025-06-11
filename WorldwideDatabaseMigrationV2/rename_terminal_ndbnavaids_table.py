#!/usr/bin/env python3
import sqlite3
import os
import sys

# Path to the SQLite database file
db_path = os.path.expanduser('~/Dev/MCDUWorldwideDatabase.db')

# Old and new table names
old_table_name = "tbl_terminal_ndbnavaids"
new_table_name = "primary_D_B_base_Navaid_Terminal - NDB Navaid"

# List of (old_column, new_column) tuples
column_renames = [
    ("area_code", "CustomerAreaCode"),
    ("airport_identifier", "LandingFacilityIcaoIdentifier"),
    ("icao_code", "NdbIcaoRegionCode"),
    ("ndb_identifier", "NDBIdentifier"),
    ("ndb_name", "NDBName"),
    ("ndb_frequency", "NDBFrequency"),
    ("navaid_class", "NDBClass"),
    ("ndb_latitude", "NDBLatitude_WGS84"),
    ("ndb_longitude", "NDBLongitude_WGS84"),
    ("range", "Range")
]

def rename_terminal_ndb_table_and_columns(db_path):
    if not os.path.isfile(db_path):
        print(f"Error: database file not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    try:
        # Rename the table
        cur.execute(
            f'ALTER TABLE "{old_table_name}" '
            f'RENAME TO "{new_table_name}";'
        )

        # Rename each column one by one
        for old_col, new_col in column_renames:
            cur.execute(
                f'ALTER TABLE "{new_table_name}" '
                f'RENAME COLUMN "{old_col}" TO "{new_col}";'
            )

        conn.commit()
        print("Terminal NDB Navaids table and columns renamed successfully.")

    except sqlite3.OperationalError as e:
        print("SQLite error during renaming:", e, file=sys.stderr)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    rename_terminal_ndb_table_and_columns(db_path)
