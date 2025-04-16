import os
import sqlite3
import pandas as pd


def create_runway_table_schema(db_path: str, table_name: str):
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # SQL schema for the Runways table (you can update this if the schema changes)
    create_sql = f"""
    CREATE TABLE "{table_name}" (
        _id INTEGER PRIMARY KEY,
        RecordType TEXT,
        CustomerAreaCode TEXT,
        SectionCode TEXT,
        SubsectionCode TEXT,
        LandingFacilityIcaoIdentifier TEXT,
        RunwayIdentifier TEXT,
        RunwayLatitude TEXT,
        RunwayLongitude TEXT,
        RunwayMagneticBearing TEXT,
        LandingThresholdElevation TEXT,
        RunwayLength TEXT,
        RunwayWidth TEXT,
        RunwaySurfaceCode TEXT,
        RunwayPavementClassification TEXT,
        RunwayPavementType TEXT,
        RunwayLightingCode TEXT,
        SecondaryRunwayIdentifier TEXT,
        SecondaryRunwayLatitude TEXT,
        SecondaryRunwayLongitude TEXT,
        SecondaryRunwayMagneticBearing TEXT,
        SecondaryLandingThresholdElevation TEXT,
        SecondaryRunwayLength TEXT,
        SecondaryRunwayWidth TEXT,
        SecondaryRunwaySurfaceCode TEXT,
        SecondaryRunwayPavementClassification TEXT,
        SecondaryRunwayPavementType TEXT,
        SecondaryRunwayLightingCode TEXT,
        MagneticVariation TEXT,
        SpeedLimit TEXT,
        TransitionsAltitude TEXT,
        TransitionLevel TEXT,
        ThresholdCrossingHeight TEXT,
        DisplacedThresholdDistance TEXT,
        RunwayGradient TEXT,
        FileRecordNumber TEXT,
        CycleDate TEXT
    );
    """
    cursor.execute(create_sql)
    conn.commit()
    conn.close()
    print(f"✅ Created new DB and table: {db_path}")


def insert_excel_to_runway_table(
    excel_path: str,
    db_path: str,
    table_name: str,
    column_mapping: dict
):
    df = pd.read_excel(excel_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get table column names
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    table_columns = [col[1] for col in cursor.fetchall()]

    # Create an empty DataFrame with all columns
    db_df = pd.DataFrame(columns=table_columns)

    # Fill mapped columns
    for excel_col, db_col in column_mapping.items():
        if excel_col in df.columns and db_col in table_columns:
            db_df[db_col] = df[excel_col]

    # Fill missing columns with NULL (pd.NA)
    db_df = db_df.fillna(value=pd.NA)

    # Add _id column (primary key)
    db_df["_id"] = range(1, len(db_df) + 1)

    # Insert into SQLite
    db_df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

    print(f"✅ Inserted {len(db_df)} rows into '{table_name}'")


# ============================
# 🔧 CONFIGURATION SECTION
# ============================

if __name__ == "__main__":
    # Input Excel file path
    excel_path = os.path.expanduser("~/Desktop/Runways.xlsx")

    # Output SQLite DB path
    db_path = os.path.expanduser("~/Desktop/Runway_Output.db")

    # Table name
    table_name = "primary_P_G_base_Airport - Runways"

    # Mapping: Excel column name → DB column name
    column_mapping = {
        "Airport": "LandingFacilityIcaoIdentifier",
        "Id": "RunwayIdentifier",
        "Latitude": "RunwayLatitude",
        "Longitude": "RunwayLongitude",
        "Elevation": "LandingThresholdElevation",
        "Bearing": "RunwayMagneticBearing",
        "Length": "RunwayLength",
        "Width": "RunwayWidth",
        "Threshold Crossing Height": "ThresholdCrossingHeight",
        "Threshold Displacement Distance": "DisplacedThresholdDistance",
        "Gradient": "RunwayGradient"
    }

    # Run process
    create_runway_table_schema(db_path, table_name)
    insert_excel_to_runway_table(excel_path, db_path, table_name, column_mapping)
