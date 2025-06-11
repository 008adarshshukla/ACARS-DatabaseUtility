import sqlite3
import os
import asyncio
import aiohttp
import json

# Database path
db_path = os.path.expanduser('~/Dev/MCDUWorldwideDatabase.db')

# Change this to switch tables:
# tableName = "primary_D_B_base_Navaid_Enroute - NDB Navaid"
tableName = "primary_D_B_base_Navaid_Terminal - NDB Navaid"

# We'll use a quoted version everywhere:
quoted_table = f'"{tableName}"'

# API URL and base parameters
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

async def fetch_declination(session, ndb_id, lat, lon):
    lat1 = abs(lat)
    latHem = "N" if lat >= 0 else "S"
    lon1 = abs(lon)
    lonHem = "E" if lon >= 0 else "W"
    params = {
        **params_template,
        "lat1": lat1,
        "lat1Hemisphere": latHem,
        "lon1": lon1,
        "lon1Hemisphere": lonHem
    }

    async with session.get(api_url, params=params) as response:
        text = await response.text()
        if response.status == 200:
            try:
                data = json.loads(text)
                decl = data["result"][0]["declination"]
                return ndb_id, decl
            except (KeyError, IndexError, json.JSONDecodeError) as e:
                print(f"[JSON error] {ndb_id}: {e}")
        else:
            print(f"[HTTP {response.status}] {ndb_id}")
    return ndb_id, None

async def main():
    # 1) Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 2) Add the Declination column if it doesn't exist
    try:
        cursor.execute(f'''
            ALTER TABLE {quoted_table}
            ADD COLUMN Declination REAL
        ''')
        conn.commit()
        print("Declination column added.")
    except sqlite3.OperationalError:
        print("Declination column already exists.")

    # 3) Query data
    cursor.execute(f'''
        SELECT NDBIdentifier, NDBLatitude_WGS84, NDBLongitude_WGS84
          FROM {quoted_table}
    ''')
    rows = cursor.fetchall()
    total = len(rows)
    print(f"Total NDBs: {total}")

    tasks = []

    async with aiohttp.ClientSession() as session:
        for ndb_id, lat_s, lon_s in rows:
            if not lat_s or not lon_s:
                print(f"Skipping {ndb_id}: missing coords")
                continue

            try:
                lat = float(lat_s)
                lon = float(lon_s)
            except ValueError:
                print(f"Skipping {ndb_id}: invalid coords")
                continue

            tasks.append(fetch_declination(session, ndb_id, lat, lon))

            # batch of 70
            if len(tasks) == 70:
                results = await asyncio.gather(*tasks)
                updated = 0
                for nid, decl in results:
                    if decl is not None:
                        cursor.execute(f'''
                            UPDATE {quoted_table}
                               SET Declination = ?
                             WHERE NDBIdentifier = ?
                        ''', (decl, nid))
                        updated += 1
                conn.commit()
                print(f"Updated {updated} records in batch of 70.")
                tasks.clear()

        # final batch
        if tasks:
            results = await asyncio.gather(*tasks)
            updated = 0
            for nid, decl in results:
                if decl is not None:
                    cursor.execute(f'''
                        UPDATE {quoted_table}
                           SET Declination = ?
                         WHERE NDBIdentifier = ?
                    ''', (decl, nid))
                    updated += 1
            conn.commit()
            print(f"Updated {updated} records in final batch.")

    conn.close()
    print("NDB declination update completed.")

if __name__ == "__main__":
    asyncio.run(main())
