import sqlite3
import os
import asyncio
import aiohttp
import json 

# Database path (update with the correct path to your desktop if needed)
db_path = os.path.expanduser('~/Dev/MCDUWorldwideDatabase.db')

# API URL and parameters
api_url = "https://www.ngdc.noaa.gov/geomag-web/calculators/calculateDeclination"
params_template = {
    "browserRequest": "true",
    "magneticComponent": "d",
    "key": "zNEw7",
    "model": "WMM",
    "startYear": 2025,
    "startMonth": 1,
    "startDay": 29,
    "resultFormat": "json"
}

async def fetch_declination(session, landing_facility, lat, lon):
    lat1 = abs(lat)
    lat1Hemisphere = "N" if lat >= 0 else "S"
    lon1 = abs(lon)
    lon1Hemisphere = "E" if lon >= 0 else "W"
    params = {**params_template, "lat1": lat1, "lat1Hemisphere": lat1Hemisphere, "lon1": lon1, "lon1Hemisphere": lon1Hemisphere}

    async with session.get(api_url, params=params) as response:
        if response.status == 200:
            try:
                text = await response.text()
                data = json.loads(text)
                declination = data["result"][0]["declination"]
                return landing_facility, declination
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                print(f"Error parsing JSON response for Facility {landing_facility}: {e}")
                return landing_facility, None
        else:
            print(f"API request failed for Facility {landing_facility}: {response.status}")
            return landing_facility, None

async def main():
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Add the Declination column if it doesn't exist
    cursor.execute('''
        ALTER TABLE "primary_P_A_base_Airport - Reference Points"
        ADD COLUMN Declination REAL
    ''')
    conn.commit()

    # Query the necessary data from the table
    cursor.execute('''
        SELECT LandingFacilityIcaoIdentifier, AirportReferencePtLatitude_WGS84, AirportReferencePtLongitude_WGS84 
        FROM "primary_P_A_base_Airport - Reference Points"
    ''')

    all_rows = cursor.fetchall()
    tasks = []
    async with aiohttp.ClientSession() as session:
        for row in all_rows:
            landing_facility, lat_str, lon_str = row
            if lat_str is None or lon_str is None:
                print(f"Skipping Facility {landing_facility} due to missing latitude or longitude.")
                continue

            try:
                lat = float(lat_str)
                lon = float(lon_str)
                tasks.append(fetch_declination(session, landing_facility, lat, lon))
            except ValueError:
                print(f"Invalid latitude or longitude for Facility {landing_facility}. Skipping.")
                continue

            # Process the tasks in batches of 50
            if len(tasks) == 50:
                results = await asyncio.gather(*tasks)
                update_count = 0
                for landing_facility, declination in results:
                    if declination is not None:
                        cursor.execute('''
                            UPDATE "primary_P_A_base_Airport - Reference Points"
                            SET Declination = ?
                            WHERE LandingFacilityIcaoIdentifier = ?
                        ''', (declination, landing_facility))
                        update_count += 1

                print(f"Updated {update_count} records in the database for the current batch.")
                conn.commit()
                tasks.clear()

        # Process any remaining tasks
        if tasks:
            results = await asyncio.gather(*tasks)
            update_count = 0
            for landing_facility, declination in results:
                if declination is not None:
                    cursor.execute('''
                        UPDATE "primary_P_A_base_Airport - Reference Points"
                        SET Declination = ?
                        WHERE LandingFacilityIcaoIdentifier = ?
                    ''', (declination, landing_facility))
                    update_count += 1

            print(f"Updated {update_count} records in the database for the final batch.")
            conn.commit()

    conn.close()
    print("Process completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
