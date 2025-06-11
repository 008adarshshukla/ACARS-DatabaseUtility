#!/usr/bin/env python3
import sqlite3
import os
import sys

# Path to the SQLite database file
db_path = os.path.expanduser('~/Dev/MCDUWorldwideDatabase.db')

# Old and new table names
old_table_name = "tbl_localizers_glideslopes"
new_table_name = "primary_P_I_base_Airport - Localizer/Glide Slope"

# List of (old_column, new_column) tuples
column_renames = [
    ("area_code", "CustomerAreaCode"),
    ("icao_code", "LandingFacilityIcaoRegionCode"),
    ("airport_identifier", "LandingFacilityIcaoIdentifier"),
    ("runway_identifier", "RunwayIdentifier"),
    ("llz_identifier", "LocalizerIdentifier"),
    ("llz_latitude", "LocalizerLatitude_WGS84"),
    ("llz_longitude", "LocalizerLongitude_WGS84"),
    ("llz_frequency", "LocalizerFrequency"),
    ("llz_bearing", "LocalizerBearing"),
    ("llz_width", "LocalizerWidth"),
    ("ils_mls_gls_category", "ILSCategory"),
    ("gs_latitude", "GlideSlopeLatitude_WGS84"),
    ("gs_longitude", "GlideSlopeLongitude_WGS84"),
    ("gs_angle", "GlideSlopeAngle"),
    ("gs_elevation", "GlideSlopeElevation"),
    ("station_declination", "StationDeclination")
]

def rename_localizer_glideslope_table_and_columns(db_path):
    # Verify the database file exists
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

        # Rename each column sequentially
        for old_col, new_col in column_renames:
            cur.execute(
                f'ALTER TABLE "{new_table_name}" '
                f'RENAME COLUMN "{old_col}" TO "{new_col}";'
            )

        conn.commit()
        print("Localizer/Glide Slope table and columns renamed successfully.")

    except sqlite3.OperationalError as e:
        print("SQLite error during renaming:", e, file=sys.stderr)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    rename_localizer_glideslope_table_and_columns(db_path)

