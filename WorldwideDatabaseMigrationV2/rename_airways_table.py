#!/usr/bin/env python3
import sqlite3
import os
import sys

# Path to the SQLite database file
db_path = os.path.expanduser('~/Dev/MCDUWorldwideDatabase.db')

# Old and new table names
old_table_name = "tbl_enroute_airways"
new_table_name = "primary_E_R_base_Enroute - Airways and Routes"

# List of (old_column, new_column) tuples
column_renames = [
    ("area_code", "CustomerAreaCode"),
    ("route_identifier", "RouteIdentifier"),
    ("seqno", "SequenceNumber"),
    ("waypoint_identifier", "FixIdentifier"),
    ("icao_code", "FixIcaoRegionCode"),
    ("waypoint_latitude", "WaypointLatitude_WGS84"),
    ("waypoint_longitude", "WaypointLongitude_WGS84"),
    ("waypoint_description_code", "WaypointDescriptionCodes"),
    ("route_type", "RouteType"),
    ("flightlevel", "Level"),
    ("direction_restriction", "DirectionRestriction"),
    ("crusing_table_identifier", "CruiseTableIndicator"),
    ("minimum_altitude1", "MinimumAltitude_1"),
    ("minimum_altitude2", "MinimumAltitude_2"),
    ("maximum_altitude", "MaximumAltitude"),
    ("outbound_course", "OutboundMagneticCourse"),
    ("inbound_course", "InboundMagneticCourse"),
    ("inbound_distance", "RouteDistanceFrom")
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
