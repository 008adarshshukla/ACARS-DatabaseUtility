import json
import sqlite3
import os

# File paths
downloads_path = os.path.expanduser("~/Downloads")
json_path = os.path.join(downloads_path, "wind_temp_height_profiles.json")
db_path = os.path.join(downloads_path, "wind_profiles.db")

# Read JSON file
with open(json_path, "r") as f:
    data = json.load(f)

# Connect to SQLite DB
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# Create table
cur.execute("""
    CREATE TABLE IF NOT EXISTS wind_profiles (
        latitude REAL,
        longitude REAL,
        height_m REAL,
        pressure_hPa REAL,
        u_wind REAL,
        v_wind REAL,
        temperature_K REAL
    )
""")

# Flatten and insert data
insert_count = 0
for entry in data:
    lat = entry["latitude"]
    lon = entry["longitude"]
    for level in entry["profile"]:
        cur.execute("""
            INSERT INTO wind_profiles (latitude, longitude, height_m, pressure_hPa, u_wind, v_wind, temperature_K)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            lat,
            lon,
            level["height_m"],
            level.get("pressure_hPa"),
            level["u_wind"],
            level["v_wind"],
            level["temperature_K"]
        ))
        insert_count += 1

# Commit and close
conn.commit()
conn.close()

print(f"✅ Converted JSON to SQLite DB: {db_path}")
print(f"📦 Inserted {insert_count} rows into 'wind_profiles' table.")
