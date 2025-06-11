#!/usr/bin/env python3
import sqlite3
import os
import sys

# Path to the SQLite database file
db_path = os.path.expanduser('~/Dev/MCDUWorldwideDatabase.db')

# Old and new table names
old_table_name = "tbl_stars"
new_table_name = "primary_P_E_base_Airport - STARs"

# List of (old_column, new_column) tuples
column_renames = [
    ("area_code", "CustomerAreaCode"),
    ("airport_identifier", "LandingFacilityIcaoIdentifier"),
    ("procedure_identifier", "SIDSTARApproachIdentifier"),
    ("route_type", "RouteType"),
    ("transition_identifier", "TransitionIdentifier"),
    ("seqno", "SequenceNumber"),
    ("waypoint_icao_code", "FixIcaoRegionCode"),
    ("waypoint_identifier", "FixIdentifier"),
    ("waypoint_latitude", "FixIdentifierLatitude_WGS84"),
    ("waypoint_longitude", "FixIdentifierLongitude_WGS84"),
    ("waypoint_description_code", "WaypointDescriptionCodes"),
    ("turn_direction", "TurnDirection"),
    ("rnp", "RNP"),
    ("path_termination", "PathAndTermination"),
    ("recommanded_navaid", "RecommendedNavaid"),
    ("recommanded_navaid_latitude", "RecommendedNavaidLatitude_WGS84"),
    ("recommanded_navaid_longitude", "RecommendedNavaidLongitude_WGS84"),
    ("arc_radius", "ARCRadius"),
    ("theta", "Theta"),
    ("rho", "Rho"),
    ("magnetic_course", "MagneticCourse"),
    ("route_distance_holding_distance_time", "RouteDistanceHoldingDistanceOrTime"),
    ("distance_time", "DistanceOrTime"),
    ("altitude_description", "AltitudeDescription"),
    ("altitude1", "Altitude_1"),
    ("altitude2", "Altitude_2"),
    ("transition_altitude", "TransitionAltitude"),
    ("speed_limit_description", "SpeedLimitDescription"),
    ("speed_limit", "SpeedLimit"),
    ("vertical_angle", "VerticalAngle"),
    ("center_waypoint", "CenterWaypoint"),
    ("center_waypoint_latitude", "CenterWaypointLatitude_WGS84"),
    ("center_waypoint_longitude", "CenterWaypointLongitude_WGS84"),
    ("aircraft_category", "AircraftCategory")
]

def rename_stars_table_and_columns(db_path):
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
        print("STARs table and columns renamed successfully.")

    except sqlite3.OperationalError as e:
        print("SQLite error during renaming:", e, file=sys.stderr)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    rename_stars_table_and_columns(db_path)
