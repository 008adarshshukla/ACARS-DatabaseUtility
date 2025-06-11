#!/usr/bin/env python3
import sqlite3
import os
import sys

# Path to the SQLite database file
db_path = os.path.expanduser('~/Dev/MCDUWorldwideDatabase.db')

# Old and new table names
old_table_name = "tbl_enroute_waypoints"
new_table_name = "primary_E_A_base_Enroute - Grid Waypoints"

# List of (old_column, new_column) tuples
column_renames = [
    ("area_code", "CustomerAreaCode"),
    ("icao_code", "WaypointIcaoRegionCode"),
    ("waypoint_identifier", "WaypointIdentifier"),
    ("waypoint_name", "WaypointNameDescription"),
    ("waypoint_type", "WaypointType"),
    ("waypoint_usage", "WaypointUsage"),
    ("waypoint_latitude", "WaypointLatitude_WGS84"),
    ("waypoint_longitude", "WaypointLongitude_WGS84")
]

def rename_waypoints_table_and_columns(db_path):
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
        print("Enroute waypoints table and columns renamed successfully.")

    except sqlite3.OperationalError as e:
        print("SQLite error during renaming:", e, file=sys.stderr)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    rename_waypoints_table_and_columns(db_path)
