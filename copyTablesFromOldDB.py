import sqlite3

def copy_database(source_db_path, destination_db_path):
    # Connect to the source database
    src_conn = sqlite3.connect(source_db_path)
    src_cursor = src_conn.cursor()
    
    # Connect to the destination database
    dest_conn = sqlite3.connect(destination_db_path)
    dest_cursor = dest_conn.cursor()
    
    # Get the list of tables in the source database
    src_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = src_cursor.fetchall()

    # Copy each table from the source to the destination
    for table in tables:
        table_name = table[0]
        
        # Get the CREATE TABLE statement
        src_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        create_table_sql = src_cursor.fetchone()[0]
        
        # Create the table in the destination database
        dest_cursor.execute(create_table_sql)
        
        # Copy the data from the source table to the destination table
        src_cursor.execute(f"SELECT * FROM {table_name}")
        rows = src_cursor.fetchall()
        
        # Get column names
        columns = [description[0] for description in src_cursor.description]
        column_names = ', '.join(columns)
        placeholders = ', '.join(['?' for _ in columns])
        
        # Insert data into the destination table
        dest_cursor.executemany(f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})", rows)
    
    # Commit and close the destination connection
    dest_conn.commit()
    dest_conn.close()
    
    # Close the source connection
    src_conn.close()

# Example usage
source_db = '/Users/adarshshukla/Documents/ACARS-DatabaseUtility/OLD_DATA.db'
destination_db = '/Users/adarshshukla/Desktop/RUNWY_DATA.db'
copy_database(source_db, destination_db)
