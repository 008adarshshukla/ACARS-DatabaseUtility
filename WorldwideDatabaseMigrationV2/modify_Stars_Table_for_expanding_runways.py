#!/usr/bin/env python3
import sqlite3
import os
import sys

# Path to the SQLite database file
db_path = os.path.expanduser('~/Dev/MCDUDatabase.db')

# Source and intermediate table names
source_table = "primary_P_E_base_Airport - STARs"
new_table    = "primary_P_E_base_Airport - STARs-new"

def main():
    # 1) Verify database file exists
    if not os.path.isfile(db_path):
        print(f"Error: database file not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 2) Read source schema
    cur.execute(f'PRAGMA table_info("{source_table}")')
    cols_info = cur.fetchall()  # (cid, name, type, notnull, dflt_value, pk)
    col_names = [row[1] for row in cols_info]
    col_types = {row[1]: row[2] for row in cols_info}
    name_to_idx = {row[1]: row[0] for row in cols_info}

    # 3) Drop intermediate table if it exists
    cur.execute(f'DROP TABLE IF EXISTS "{new_table}";')

    # 4) Create new table with identical schema
    cols_defs = ", ".join(f'"{name}" {col_types[name]}' for name in col_names)
    cur.execute(f'CREATE TABLE "{new_table}" ({cols_defs});')

    # 5) Prepare insert SQL
    placeholders = ", ".join("?" for _ in col_names)
    cols_list    = ", ".join(f'"{c}"' for c in col_names)
    insert_sql   = f'INSERT INTO "{new_table}" ({cols_list}) VALUES ({placeholders});'

    # 6) Fetch all rows from source
    cur.execute(f'SELECT * FROM "{source_table}";')
    rows = cur.fetchall()

    # 7) Transform and insert rows
    for row in rows:
        trans_id = row[name_to_idx["TransitionIdentifier"]]
        lid      = row[name_to_idx["LandingFacilityIcaoIdentifier"]]

        def make_row(new_trans):
            new_row = list(row)
            new_row[name_to_idx["TransitionIdentifier"]] = new_trans
            return tuple(new_row)

        # Expand ALL
        if trans_id == "ALL":
            cur.execute(
                'SELECT RunwayIdentifier FROM "primary_P_G_base_Airport - Runways" '
                'WHERE LandingFacilityIcaoIdentifier = ?', (lid,)
            )
            for (rw,) in cur.fetchall():
                cur.execute(insert_sql, make_row(rw))

        # Split RW..B into L and R
        elif isinstance(trans_id, str) and trans_id.startswith("RW") and trans_id.endswith("B"):
            base = trans_id[:-1]
            cur.execute(insert_sql, make_row(base + "L"))
            cur.execute(insert_sql, make_row(base + "R"))

        # Copy unchanged
        else:
            cur.execute(insert_sql, row)

    # 8) Swap tables: drop old, rename new to original name
    cur.execute(f'DROP TABLE "{source_table}";')
    cur.execute(f'ALTER TABLE "{new_table}" RENAME TO "{source_table}";')

    conn.commit()
    conn.close()
    print(f"Migration complete: '{source_table}' updated with expanded rows.")

if __name__ == "__main__":
    main()

