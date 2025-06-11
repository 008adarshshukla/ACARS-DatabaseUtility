#!/usr/bin/env python3
import sqlite3
import os
import sys

# Path to the SQLite database file
db_path = os.path.expanduser('~/Dev/MCDUWorldwideDatabase.db')

# Table names
source_table = "primary_P_D_base_Airport - SIDs"
target_table = "primary_P_D_base_Airport - SIDs"
temp_table = "temp_SIDs_new"

# Desired columns in the new schema
target_cols = [
    "CustomerAreaCode",
    "LandingFacilityIcaoIdentifier",
    "SIDSTARApproachIdentifier",
    "RouteType",
    "TransitionIdentifier",
    "SequenceNumber",
    "FixIcaoRegionCode",
    "FixIdentifier",
    "FixIdentifierLatitude_WGS84",
    "FixIdentifierLongitude_WGS84",  # override with dept
    "WaypointDescriptionCodes",
    "TurnDirection",
    "RNP",
    "PathAndTermination",
    "RecommendedNavaid",
    "RecommendedNavaidLatitude_WGS84",
    "RecommendedNavaidLongitude_WGS84",
    "ARCRadius",
    "Theta",
    "Rho",
    "MagneticCourse",
    "RouteDistanceHoldingDistanceOrTime",
    "DistanceOrTime",
    "AltitudeDescription",
    "Altitude_1",
    "Altitude_2",
    "TransitionAltitude",
    "SpeedLimitDescription",
    "SpeedLimit",
    "VerticalAngle",
    "center_waypoint",
    "CenterWaypointLatitude_WGS84",
    "CenterWaypointLongitude_WGS84",
    "AircraftCategory"
]

def main():
    if not os.path.isfile(db_path):
        print(f"Error: database file not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Read source schema info
    cur.execute(f'PRAGMA table_info("{source_table}")')
    info = cur.fetchall()  # (cid, name, type, notnull, dflt, pk)
    name_to_idx = {row[1]: row[0] for row in info}

    # Drop temp if exists
    cur.execute(f'DROP TABLE IF EXISTS "{temp_table}";')

    # Create temp table with target schema and types
    cols_defs = []
    for col in target_cols:
        col_type = info[name_to_idx[col]][2]
        cols_defs.append(f'"{col}" {col_type}')
    cur.execute(f'CREATE TABLE "{temp_table}" ({", ".join(cols_defs)});')

    # Prepare insert statement
    cols_list = ", ".join(f'"{c}"' for c in target_cols)
    placeholders = ", ".join("?" for _ in target_cols)
    insert_sql = f'INSERT INTO "{temp_table}" ({cols_list}) VALUES ({placeholders});'

    # Fetch all rows
    cur.execute(f'SELECT * FROM "{source_table}";')
    rows = cur.fetchall()

    # Process rows
    for row in rows:
        orig = row[name_to_idx["FixIdentifierLongitude_WGS84"]]
        dept_str = orig if isinstance(orig, str) else ""

        def make_vals(dept_override):
            return [
                dept_override if c == "FixIdentifierLongitude_WGS84" else row[name_to_idx[c]]
                for c in target_cols
            ]

        if dept_str.startswith("RW") and dept_str.endswith("B"):
            num = dept_str[2:-1]
            for side in ("L", "R"):
                cur.execute(insert_sql, make_vals(f"RW{num}{side}"))
        elif dept_str == "ALL":
            lid = row[name_to_idx["LandingFacilityIcaoIdentifier"]]
            cur.execute(
                'SELECT RunwayIdentifier FROM "primary_P_G_base_Airport - Runways" '
                'WHERE LandingFacilityIcaoIdentifier = ?',
                (lid,)
            )
            for (rw,) in cur.fetchall():
                cur.execute(insert_sql, make_vals(rw))
        else:
            cur.execute(insert_sql, make_vals(orig))

    # Replace source table
    # cur.execute(f'DROP TABLE "{source_table}";')
    cur.execute(f'DROP TABLE "{source_table}";')
    cur.execute(f'ALTER TABLE "{temp_table}" RENAME TO "{target_table}";')

    conn.commit()
    conn.close()
    print("SIDs migration completed successfully.")

if __name__ == "__main__":
    main()
