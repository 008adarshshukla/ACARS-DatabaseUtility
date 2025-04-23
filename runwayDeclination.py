import sqlite3
import os
import asyncio
import aiohttp
import json

# Database path
db_path = os.path.expanduser("~/Dev/MCDUDatabase.db")

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

async def fetch_declination(session, landing_facility, runway_id, lat, lon):
    lat1 = abs(lat)
    lat1Hemisphere = "N" if lat >= 0 else "S"
    lon1 = abs(lon)
    lon1Hemisphere = "E" if lon >= 0 else "W"
    params = {
        **params_template,
        "lat1": lat1,
        "lat1Hemisphere": lat1Hemisphere,
        "lon1": lon1,
        "lon1Hemisphere": lon1Hemisphere
    }

    async with session.get(api_url, params=params) as response:
        if response.status == 200:
            try:
                text = await response.text()
                data = json.loads(text)
                declination = data["result"][0]["declination"]
                return landing_facility, runway_id, declination
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                print(f"Error parsing JSON response for Facility {landing_facility}, Runway {runway_id}: {e}")
                return landing_facility, runway_id, None
        else:
            print(f"API request failed for Facility {landing_facility}, Runway {runway_id}: {response.status}")
            return landing_facility, runway_id, None

async def main():
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Add the Declination column if it doesn't exist
    try:
        cursor.execute('''
            ALTER TABLE "primary_P_G_base_Airport - Runways"
            ADD COLUMN Declination REAL
        ''')
        conn.commit()
        print("Declination column added.")
    except sqlite3.OperationalError:
        # Likely already exists
        print("Declination column already exists.")

    # Query the necessary data
    cursor.execute('''
        SELECT LandingFacilityIcaoIdentifier, RunwayIdentifier, RunwayLatitude_WGS84, RunwayLongitude_WGS84
        FROM "primary_P_G_base_Airport - Runways"
    ''')

    all_rows = cursor.fetchall()
    tasks = []

    async with aiohttp.ClientSession() as session:
        for row in all_rows:
            landing_facility, runway_id, lat_str, lon_str = row
            if lat_str is None or lon_str is None:
                print(f"Skipping runway {runway_id} for Airport {landing_facility} due to missing coordinates.")
                continue

            try:
                lat = float(lat_str)
                lon = float(lon_str)
                tasks.append(fetch_declination(session, landing_facility, runway_id, lat, lon))
            except ValueError:
                print(f"Invalid latitude or longitude for Facility {landing_facility}. Skipping.")
                continue

            # Process in batches of 50
            if len(tasks) == 50:
                results = await asyncio.gather(*tasks)
                update_count = 0
                for landing_facility, runway_id, declination in results:
                    if declination is not None:
                        cursor.execute('''
                            UPDATE "primary_P_G_base_Airport - Runways"
                            SET Declination = ?
                            WHERE LandingFacilityIcaoIdentifier = ?
                            AND RunwayIdentifier = ?
                        ''', (declination, landing_facility, runway_id))
                        update_count += 1

                print(f"Updated {update_count} records in the database for the current batch.")
                conn.commit()
                tasks.clear()

        # Final batch
        if tasks:
            results = await asyncio.gather(*tasks)
            update_count = 0
            for landing_facility, runway_id, declination in results:
                if declination is not None:
                    cursor.execute('''
                        UPDATE "primary_P_G_base_Airport - Runways"
                        SET Declination = ?
                        WHERE LandingFacilityIcaoIdentifier = ?
                        AND RunwayIdentifier = ?
                    ''', (declination, landing_facility, runway_id))
                    update_count += 1

            print(f"Updated {update_count} records in the database for the final batch.")
            conn.commit()

    conn.close()
    print("Process completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
