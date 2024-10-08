# This script is used to modify the SID table in the database. 
# It is used to add the runway information to the SID table.

# Note: Before running this script make sure to create a new table "your_table_name" in the database with same columns as "primary_P_D_base_Airport - SIDs"

"""
1. pragma table_info("primary_P_D_base_Airport - SIDs"); - to get the column names
2. Copy the columns to generate the create table query using GPT
3. Run the query to create the new table
4. Run the script to insert the data into the new table
5. Delete the existing table
6. Rename the new table to the original table name
"""

"""
CREATE TABLE your_table_name (
    _id INTEGER PRIMARY KEY AUTOINCREMENT,
    RecordType TEXT,
    CustomerAreaCode TEXT,
    SectionCode TEXT,
    LandingFacilityIcaoIdentifier TEXT,
    LandingFacilityIcaoRegionCode TEXT,
    SubSectionCode TEXT,
    SIDSTARApproachIdentifier TEXT,
    RouteType TEXT,
    TransitionIdentifier TEXT,
    SequenceNumber TEXT,
    FixIdentifier TEXT,
    FixIcaoRegionCode TEXT,
    FixSectionCode TEXT,
    FixSubSectionCode TEXT,
    ContinuationRecordNumber TEXT,
    WaypointDescriptionCode1 TEXT,
    WaypointDescriptionCode2 TEXT,
    WaypointDescriptionCode3 TEXT,
    WaypointDescriptionCode4 TEXT,
    TurnDirection TEXT,
    RNP TEXT,
    PathAndTermination TEXT,
    TurnDirectionValid TEXT,
    RecommendedNavaid TEXT,
    RecommendedNavaidIcaoRegionCode TEXT,
    ARCRadius TEXT,
    Theta TEXT,
    Rho TEXT,
    MagneticCourse TEXT,
    RouteDistanceHoldingDistanceOrTime TEXT,
    RecommendedNAVAIDSection TEXT,
    RecommendedNAVAIDSubSection TEXT,
    Reservedexpansion TEXT,
    AltitudeDescription TEXT,
    ATCIndicator TEXT,
    Altitude_1 TEXT,
    Altitude_2 TEXT,
    TransitionAltitude TEXT,
    SpeedLimit TEXT,
    VerticalAngle TEXT,
    CenterFixOrTAAProcedureTurnIndicator TEXT,
    MultipleCodeOrTAASectorIdentifier TEXT,
    CenterFixOrTAAProcedureTurnIndicatorIcaoRegionCode TEXT,
    CenterFixOrTAAProcedureTurnIndicatorSectionCode TEXT,
    CenterFixOrTAAProcedureTurnIndicatorSubSectionCode TEXT,
    GNSSFMSIndication TEXT,
    SpeedLimitDescription TEXT,
    ApchRouteQualifier1 TEXT,
    ApchRouteQualifier2 TEXT,
    FileRecordNumber TEXT,
    CycleDate TEXT
);
"""

import sqlite3

conn = sqlite3.connect('/Users/adarshshukla/Desktop/RUNWY_DATA.db')
cursor = conn.cursor()

cursor.execute('SELECT * FROM "primary_P_D_base_Airport - SIDs";')
rows = cursor.fetchall()

insert_query = '''
    INSERT INTO your_table_name (
        RecordType,
        CustomerAreaCode,
        SectionCode,
        LandingFacilityIcaoIdentifier,
        LandingFacilityIcaoRegionCode,
        SubSectionCode,
        SIDSTARApproachIdentifier,
        RouteType,
        TransitionIdentifier,
        SequenceNumber,
        FixIdentifier,
        FixIcaoRegionCode,
        FixSectionCode,
        FixSubSectionCode,
        ContinuationRecordNumber,
        WaypointDescriptionCode1,
        WaypointDescriptionCode2,
        WaypointDescriptionCode3,
        WaypointDescriptionCode4,
        TurnDirection,
        RNP,
        PathAndTermination,
        TurnDirectionValid,
        RecommendedNavaid,
        RecommendedNavaidIcaoRegionCode,
        ARCRadius,
        Theta,
        Rho,
        MagneticCourse,
        RouteDistanceHoldingDistanceOrTime,
        RecommendedNAVAIDSection,
        RecommendedNAVAIDSubSection,
        Reservedexpansion,
        AltitudeDescription,
        ATCIndicator,
        Altitude_1,
        Altitude_2,
        TransitionAltitude,
        SpeedLimit,
        VerticalAngle,
        CenterFixOrTAAProcedureTurnIndicator,
        MultipleCodeOrTAASectorIdentifier,
        CenterFixOrTAAProcedureTurnIndicatorIcaoRegionCode,
        CenterFixOrTAAProcedureTurnIndicatorSectionCode,
        CenterFixOrTAAProcedureTurnIndicatorSubSectionCode,
        GNSSFMSIndication,
        SpeedLimitDescription,
        ApchRouteQualifier1,
        ApchRouteQualifier2,
        FileRecordNumber,
        CycleDate
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    '''

for row in rows:
    department = row[9] 
    
    if department.startswith('RW') and department.endswith('B'):
        number = department[2:-1]

        department_L = f'RW{number}L'
        department_R = f'RW{number}R'

        cursor.execute(insert_query, (
            row[1],  
            row[2],  
            row[3],  
            row[4],  
            row[5],  
            row[6],  
            row[7],
            row[8],  
            department_L,  
            row[10], 
            row[11], 
            row[12], 
            row[13], 
            row[14], 
            row[15], 
            row[16], 
            row[17], 
            row[18], 
            row[19], 
            row[20], 
            row[21], 
            row[22], 
            row[23], 
            row[24], 
            row[25], 
            row[26], 
            row[27], 
            row[28], 
            row[29], 
            row[30], 
            row[31], 
            row[32], 
            row[33], 
            row[34], 
            row[35], 
            row[36], 
            row[37], 
            row[38], 
            row[39], 
            row[40], 
            row[41], 
            row[42], 
            row[43], 
            row[44], 
            row[45], 
            row[46], 
            row[47], 
            row[48], 
            row[49], 
            row[50],
            row[51]
        ))

        cursor.execute(insert_query, (
            row[1],  
            row[2],  
            row[3],  
            row[4],  
            row[5],  
            row[6],  
            row[7],
            row[8],  
            department_R, 
            row[10], 
            row[11], 
            row[12], 
            row[13], 
            row[14], 
            row[15],
            row[16],
            row[17],
            row[18],
            row[19],
            row[20],
            row[21],
            row[22], 
            row[23], 
            row[24], 
            row[25], 
            row[26], 
            row[27], 
            row[28], 
            row[29], 
            row[30], 
            row[31], 
            row[32], 
            row[33], 
            row[34], 
            row[35], 
            row[36], 
            row[37], 
            row[38], 
            row[39], 
            row[40], 
            row[41], 
            row[42], 
            row[43], 
            row[44], 
            row[45], 
            row[46], 
            row[47], 
            row[48], 
            row[49], 
            row[50],
            row[51]
        ))
    elif department == 'ALL':
        landingFacilityIcaoIdentifier = row[4]
        cursor.execute('''
            select RunwayIdentifier from "primary_P_G_base_Airport - Runways" where LandingFacilityIcaoIdentifier = ?
                       ''', (landingFacilityIcaoIdentifier,))
        runway_rows = cursor.fetchall()
        for runway_row in runway_rows:
            cursor.execute(insert_query, (
            row[1],  
            row[2],  
            row[3],  
            row[4],  
            row[5],  
            row[6],  
            row[7], 
            row[8], 
            runway_row[0],  
            row[10], 
            row[11], 
            row[12], 
            row[13], 
            row[14], 
            row[15], 
            row[16], 
            row[17], 
            row[18], 
            row[19], 
            row[20], 
            row[21], 
            row[22], 
            row[23], 
            row[24], 
            row[25], 
            row[26], 
            row[27], 
            row[28], 
            row[29], 
            row[30], 
            row[31], 
            row[32], 
            row[33], 
            row[34], 
            row[35], 
            row[36], 
            row[37], 
            row[38], 
            row[39], 
            row[40], 
            row[41], 
            row[42], 
            row[43], 
            row[44], 
            row[45], 
            row[46], 
            row[47], 
            row[48], 
            row[49], 
            row[50],
            row[51]
        ))

    else:
        cursor.execute(insert_query, (
            row[1],  
            row[2],  
            row[3],  
            row[4],  
            row[5],  
            row[6],  
            row[7],  
            row[8],  
            row[9],  
            row[10], 
            row[11], 
            row[12], 
            row[13], 
            row[14], 
            row[15], 
            row[16], 
            row[17], 
            row[18], 
            row[19], 
            row[20], 
            row[21], 
            row[22], 
            row[23], 
            row[24], 
            row[25], 
            row[26], 
            row[27], 
            row[28], 
            row[29], 
            row[30], 
            row[31], 
            row[32], 
            row[33], 
            row[34], 
            row[35], 
            row[36], 
            row[37], 
            row[38], 
            row[39], 
            row[40], 
            row[41], 
            row[42], 
            row[43], 
            row[44], 
            row[45], 
            row[46], 
            row[47], 
            row[48], 
            row[49], 
            row[50],
            row[51]
        ))

# Delete the existing table
cursor.execute('DROP TABLE "primary_P_D_base_Airport - SIDs";')

# Rename the new table to the original table name
cursor.execute('ALTER TABLE your_table_name RENAME TO "primary_P_D_base_Airport - SIDs";')

conn.commit()
conn.close()
