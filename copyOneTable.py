import sqlite3
import os

# Define paths to the databases
source_db = os.path.expanduser("~/Desktop/RUNWY_DATA.db")
target_db = os.path.expanduser("~/Desktop/RUNWY_DATA 2.db")
table_name = "WaypointDeclinations"

# Function to copy a table from one database to another
def copy_table(source_db, target_db, table_name):
    # Connect to the source database
    source_conn = sqlite3.connect(source_db)
    source_cursor = source_conn.cursor()
    
    # Connect to the target database
    target_conn = sqlite3.connect(target_db)
    target_cursor = target_conn.cursor()

    # Fetch column information and build CREATE TABLE statement
    source_cursor.execute(f"PRAGMA table_info({table_name})")
    columns = source_cursor.fetchall()
    if not columns:
        raise ValueError(f"Table '{table_name}' does not exist or has no columns in '{source_db}'.")

    column_definitions = ", ".join([f"{col[1]} {col[2]}" for col in columns])
    print(f"CREATE TABLE statement for {table_name}: CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions})")

    # Create the table in the target database
    target_cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions})")
    
    # Copy data from source to target
    source_cursor.execute(f"SELECT * FROM {table_name}")
    rows = source_cursor.fetchall()
    target_cursor.executemany(f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in columns])})", rows)
    
    # Commit and close connections
    target_conn.commit()
    source_conn.close()
    target_conn.close()
    print(f"Table '{table_name}' copied from {source_db} to {target_db}.")

# Copy the table
copy_table(source_db, target_db, table_name)
