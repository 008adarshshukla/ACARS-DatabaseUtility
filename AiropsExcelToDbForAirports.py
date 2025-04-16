import sqlite3
import pandas as pd
import os

def create_database_with_schema(db_path: str, table_name: str):
    # Delete the file if it already exists
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    create_table_sql = f"""
    CREATE TABLE "{table_name}" (
        _id INTEGER PRIMARY KEY,
        RecordType TEXT,
        CustomerAreaCode TEXT,
        SectionCode TEXT,
        LandingFacilityIcaoIdentifier TEXT,
        LandingFacilityIcaoRegionCode TEXT,
        SubSectionCode TEXT,
        ATAIATADesignator TEXT,
        ReservedExpansion_1 TEXT,
        ContinuationRecordNumber TEXT,
        SpeedLimitAltitude TEXT,
        LongestRunway TEXT,
        IFRCapability TEXT,
        LongestRunwaySurfaceCode TEXT,
        AirportReferencePtLatitude TEXT,
        AirportReferencePtLongitude TEXT,
        MagneticVariation TEXT,
        AirportElevation TEXT,
        SpeedLimit TEXT,
        RecommendedNavaid TEXT,
        RecommendedNavaidIcaoRegionCode TEXT,
        TransitionsAltitude TEXT,
        TransitionLevel TEXT,
        PublicMilitaryIndicator TEXT,
        TimeZone TEXT,
        DaylightIndicator TEXT,
        MagneticTrueIndicator TEXT,
        DatumCode TEXT,
        ReservedExpansion_2 TEXT,
        AirportName TEXT,
        FileRecordNumber TEXT,
        CycleDate TEXT,
        AirportReferencePtLongitude_WGS84 TEXT,
        AirportReferencePtLatitude_WGS84 TEXT,
        Declination REAL
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()
    conn.close()
    print(f"📦 Created new database and table at: {db_path}")


def insert_excel_to_sqlite(
    excel_path: str,
    db_path: str,
    table_name: str,
    column_mapping: dict
):
    df = pd.read_excel(excel_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all columns from the table
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    table_info = cursor.fetchall()
    db_columns = [col[1] for col in table_info]

    # Prepare the DataFrame for insert
    db_df = pd.DataFrame(columns=db_columns)

    # Fill mapped columns
    for excel_col, db_col in column_mapping.items():
        if excel_col in df.columns and db_col in db_columns:
            db_df[db_col] = df[excel_col]

    # Assign default None to remaining columns (already defaulted)
    db_df = db_df.fillna(value=pd.NA)

    # Auto-generate _id as row index + 1
    db_df["_id"] = range(1, len(db_df) + 1)

    # Insert into the database
    db_df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

    print(f"✅ Inserted {len(db_df)} rows into '{table_name}' successfully.")


# === USAGE ===
if __name__ == "__main__":
    # Customize these paths
    excel_file_path = os.path.expanduser("~/Desktop/Airports.xlsx")
    sqlite_db_path = os.path.expanduser("~/Desktop/output_new.db")
    table_name = "primary_P_A_base_Airport - Reference Point"

    # Mapping from Excel -> DB
    column_mapping = {
        "Id": "LandingFacilityIcaoIdentifier",
        "Latitude": "AirportReferencePtLatitude",
        "Longitude": "AirportReferencePtLongitude",
        "Elevation": "AirportElevation",
        "Magnetic Variation": "MagneticVariation",
        "Speed Limit Altitude": "SpeedLimit",
        "Transition Altitude": "TransitionsAltitude",
        "Transition Level": "TransitionLevel"
    }

    # Create DB and insert data
    create_database_with_schema(sqlite_db_path, table_name)
    insert_excel_to_sqlite(excel_file_path, sqlite_db_path, table_name, column_mapping)
