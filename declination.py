import sqlite3
import os
import asyncio
import aiohttp
import json 

# Database path (update with the correct path to your desktop if needed)
db_path = os.path.expanduser("~/Desktop/RUNWY_DATA.db")

# API URL and parameters
api_url = "https://www.ngdc.noaa.gov/geomag-web/calculators/calculateDeclination"
params_template = {
    "browserRequest": "true",
    "magneticComponent": "d",
    "key": "zNEw7",
    "model": "WMM",
    "startYear": 2024,
    "startMonth": 11,
    "startDay": 2,
    "resultFormat": "json"
}

async def fetch_declination(session, waypoint_id, lat, lon):
    lat1 = abs(lat)
    lat1Hemisphere = "N" if lat >= 0 else "S"
    lon1 = abs(lon)
    lon1Hemisphere = "E" if lon >= 0 else "W"
    params = {**params_template, "lat1": lat1, "lat1Hemisphere": lat1Hemisphere, "lon1": lon1, "lon1Hemisphere": lon1Hemisphere}

    async with session.get(api_url, params=params) as response:
        if response.status == 200:
            try:
                # Read the response text and parse it as JSON
                text = await response.text()
                data = json.loads(text)  # Parse the JSON response
                
                # Extract the declination value
                declination = data["result"][0]["declination"]
                return waypoint_id, declination
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                print(f"Error parsing JSON response for Waypoint {waypoint_id}: {e}")
                return waypoint_id, None
        else:
            print(f"API request failed for Waypoint {waypoint_id}: {response.status}")
            return waypoint_id, None

async def main():
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create a new table to store the WaypointIdentifier and declination values
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS GridWaypointDeclinations (
            WaypointIdentifier TEXT PRIMARY KEY,
            Declination REAL
        )
    ''')

    # Query the necessary data from the "primary_P_C_base_Airport - Terminal Waypoints" table
    cursor.execute('''
        SELECT WaypointIdentifier, WaypointLatitude_WGS84, WaypointLongitude_WGS84 
        FROM "primary_E_A_base_Enroute - Grid Waypoints"
    ''')

    all_rows = cursor.fetchall()
    tasks = []
    async with aiohttp.ClientSession() as session:
        for row in all_rows:
            waypoint_id, lat_str, lon_str = row
            if lat_str is None or lon_str is None:
                print(f"Skipping Waypoint {waypoint_id} due to missing latitude or longitude.")
                continue

            try:
                lat = float(lat_str)
                lon = float(lon_str)
                tasks.append(fetch_declination(session, waypoint_id, lat, lon))
            except ValueError:
                print(f"Invalid latitude or longitude for Waypoint {waypoint_id}. Skipping.")
                continue

            # Process the tasks in batches of 50
            if len(tasks) == 50:
                results = await asyncio.gather(*tasks)

                # Insert results into the database
                insert_count = 0
                for waypoint_id, declination in results:
                    if declination is not None:
                        cursor.execute('''
                            INSERT OR REPLACE INTO WaypointDeclinations (WaypointIdentifier, Declination)
                            VALUES (?, ?)
                        ''', (waypoint_id, declination))
                        insert_count += 1
                
                # Log the number of records inserted
                print(f"Inserted {insert_count} records into the database for the current batch.")

                # Clear the tasks list for the next batch
                tasks.clear()

        # Process any remaining tasks
        if tasks:
            results = await asyncio.gather(*tasks)
            insert_count = 0
            for waypoint_id, declination in results:
                if declination is not None:
                    cursor.execute('''
                        INSERT OR REPLACE INTO WaypointDeclinations (WaypointIdentifier, Declination)
                        VALUES (?, ?)
                    ''', (waypoint_id, declination))
                    insert_count += 1

            # Log the number of records inserted for the final batch
            print(f"Inserted {insert_count} records into the database for the final batch.")

    # Commit changes and close the connection
    conn.commit()
    conn.close()
    print("Process completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
