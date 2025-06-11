#!/usr/bin/env python3
import sqlite3
import os
import sys

# Path to the SQLite database file
db_path = os.path.expanduser('~/Dev/MCDUWorldwideDatabase.db')

# Old and new table names
old_table_name = "tbl_airports"
new_table_name = "primary_P_A_base_Airport - Reference Points"

# List of (old_column, new_column) tuples
column_renames = [
    ("area_code", "CustomerAreaCode"),
    ("icao_code", "LandingFacilityIcaoRegionCode"),
    ("airport_identifier", "LandingFacilityIcaoIdentifier"),
    ("airport_identifier_3letter", "LandingFacilityIcaoIdentifier_3Letter"),
    ("airport_name", "AirportName"),
    ("airport_ref_latitude", "AirportReferencePtLatitude_WGS84"),
    ("airport_ref_longitude", "AirportReferencePtLongitude_WGS84"),
    ("ifr_capability", "IFRCapability"),
    ("longest_runway_surface_code", "LongestRunwaySurfaceCode"),
    ("elevation", "AirportElevation"),
    ("transition_altitude", "TransitionsAltitude"),
    ("transition_level", "TransitionLevel"),
    ("speed_limit", "SpeedLimit"),
    ("speed_limit_altitude", "SpeedLimitAltitude"),
    ("iata_ata_designator", "ATAIATADesignator")
]

def rename_table_and_columns(db_path):
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
        print("Table and columns renamed successfully.")

    except sqlite3.OperationalError as e:
        print("SQLite error during renaming:", e, file=sys.stderr)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    rename_table_and_columns(db_path)

