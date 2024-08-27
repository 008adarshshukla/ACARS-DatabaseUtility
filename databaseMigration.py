import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('/Users/adarshshukla/Desktop/RUNWY_DATA.db')

# Create a cursor object
cursor = conn.cursor()

# Fetch all rows from the original employee table
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
            
            row[1],  # CustomerAreaCode
            row[2],  # SectionCode
            row[3],  # LandingFacilityIcaoIdentifier
            row[4],  # LandingFacilityIcaoRegionCode
            row[5],  # SubSectionCode
            row[6],  # SIDSTARApproachIdentifier
            row[7], 
            row[8], # RouteType
            runway_row[0],  # TransitionIdentifier  # SequenceNumber
            row[10], # FixIdentifier
            row[11], # FixIcaoRegionCode
            row[12], # FixSectionCode
            row[13], # FixSubSectionCode
            row[14], # ContinuationRecordNumber
            row[15], # WaypointDescriptionCode1
            row[16], # WaypointDescriptionCode2
            row[17], # WaypointDescriptionCode3
            row[18], # WaypointDescriptionCode4
            row[19], # TurnDirection
            row[20], # RNP
            row[21], # PathAndTermination
            row[22], # TurnDirectionValid
            row[23], # RecommendedNavaid
            row[24], # RecommendedNavaidIcaoRegionCode
            row[25], # ARCRadius
            row[26], # Theta
            row[27], # Rho
            row[28], # MagneticCourse
            row[29], # RouteDistanceHoldingDistanceOrTime
            row[30], # RecommendedNAVAIDSection
            row[31], # RecommendedNAVAIDSubSection
            row[32], # Reservedexpansion
            row[33], # AltitudeDescription
            row[34], # ATCIndicator
            row[35], # Altitude_1
            row[36], # Altitude_2
            row[37], # TransitionAltitude
            row[38], # SpeedLimit
            row[39], # VerticalAngle
            row[40], # CenterFixOrTAAProcedureTurnIndicator
            row[41], # MultipleCodeOrTAASectorIdentifier
            row[42], # CenterFixOrTAAProcedureTurnIndicatorIcaoRegionCode
            row[43], # CenterFixOrTAAProcedureTurnIndicatorSectionCode
            row[44], # CenterFixOrTAAProcedureTurnIndicatorSubSectionCode
            row[45], # GNSSFMSIndication
            row[46], # SpeedLimitDescription
            row[47], # ApchRouteQualifier1
            row[48], # ApchRouteQualifier2
            row[49], # FileRecordNumber
            row[50],
            row[51]
             
                # CycleDate
        ))

    else:
        # Insert the row as-is into the new employee table if the department doesn't match the RW<number>B pattern
        cursor.execute(insert_query, (
            row[1],  # CustomerAreaCode
            row[2],  # SectionCode
            row[3],  # LandingFacilityIcaoIdentifier
            row[4],  # LandingFacilityIcaoRegionCode
            row[5],  # SubSectionCode
            row[6],  # SIDSTARApproachIdentifier
            row[7],  # RouteType
            row[8],  # TransitionIdentifier
            row[9],  # SequenceNumber
            row[10], # FixIdentifier
            row[11], # FixIcaoRegionCode
            row[12], # FixSectionCode
            row[13], # FixSubSectionCode
            row[14], # ContinuationRecordNumber
            row[15], # WaypointDescriptionCode1
            row[16], # WaypointDescriptionCode2
            row[17], # WaypointDescriptionCode3
            row[18], # WaypointDescriptionCode4
            row[19], # TurnDirection
            row[20], # RNP
            row[21], # PathAndTermination
            row[22], # TurnDirectionValid
            row[23], # RecommendedNavaid
            row[24], # RecommendedNavaidIcaoRegionCode
            row[25], # ARCRadius
            row[26], # Theta
            row[27], # Rho
            row[28], # MagneticCourse
            row[29], # RouteDistanceHoldingDistanceOrTime
            row[30], # RecommendedNAVAIDSection
            row[31], # RecommendedNAVAIDSubSection
            row[32], # Reservedexpansion
            row[33], # AltitudeDescription
            row[34], # ATCIndicator
            row[35], # Altitude_1
            row[36], # Altitude_2
            row[37], # TransitionAltitude
            row[38], # SpeedLimit
            row[39], # VerticalAngle
            row[40], # CenterFixOrTAAProcedureTurnIndicator
            row[41], # MultipleCodeOrTAASectorIdentifier
            row[42], # CenterFixOrTAAProcedureTurnIndicatorIcaoRegionCode
            row[43], # CenterFixOrTAAProcedureTurnIndicatorSectionCode
            row[44], # CenterFixOrTAAProcedureTurnIndicatorSubSectionCode
            row[45], # GNSSFMSIndication
            row[46], # SpeedLimitDescription
            row[47], # ApchRouteQualifier1
            row[48], # ApchRouteQualifier2
            row[49], # FileRecordNumber
            row[50],
            row[51]
              # CycleDate
        ))

# Commit the changes to the database
conn.commit()

# Close the connection
conn.close()