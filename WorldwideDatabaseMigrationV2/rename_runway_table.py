#!/usr/bin/env python3
import sqlite3
import os
import sys

# Path to the SQLite database file
db_path = os.path.expanduser('~/Dev/MCDUWorldwideDatabase.db')

# Old and new table names
old_table_name = "tbl_runways"
new_table_name = "primary_P_G_base_Airport - Runways"

# List of (old_column, new_column) tuples
column_renames = [
    ("area_code", "CustomerAreaCode"),
    ("icao_code", "LandingFacilityIcaoRegionCode"),
    ("airport_identifier", "LandingFacilityIcaoIdentifier"),
    ("runway_identifier", "RunwayIdentifier"),
    ("runway_latitude", "RunwayLatitude_WGS84"),
    ("runway_longitude", "RunwayLongitude_WGS84"),
    ("runway_gradient", "RunwayGradient"),
    ("runway_magnetic_bearing", "RunwayMagneticBearing"),
    ("runway_true_bearing", "RunwayTrueBearing"),
    ("landing_threshold_elevation", "LandingThresholdElevation"),
    ("displaced_threshold_distance", "DisplacedThresholdDistance"),
    ("threshold_crossing_height", "ThresholdCrossingHeight"),
    ("runway_length", "RunwayLength"),
    ("runway_width", "RunwayWidth"),
    ("llz_identifier", "LocalizerMLSGLSRefPathIdentifier"),
    ("llz_mls_gls_category", "LocalizerMLSGLSCategoryClass"),
    ("surface_code", "SurfaceCode")
]

def rename_runways_table_and_columns(db_path):
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
        print("Runways table and columns renamed successfully.")

    except sqlite3.OperationalError as e:
        print("SQLite error during renaming:", e, file=sys.stderr)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    rename_runways_table_and_columns(db_path)
