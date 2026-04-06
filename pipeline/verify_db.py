import argparse
import os
import sqlite3
import sys
import pandas as pd
pd.set_option('display.max_columns', None)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from config import get_county_config


def verify_database(db_name):
    print(f"Connecting to {db_name}...\n")
    
    try:
        conn = sqlite3.connect(db_name)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # 1. Get a list of all tables in the database
    query_tables = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = pd.read_sql_query(query_tables, conn)['name'].tolist()
    
    if not tables:
        print("The database exists, but no tables were found!")
        return
        
    print(f"Found {len(tables)} tables: {', '.join(tables)}\n")
    print("="*60)

    # 2. Iterate through each table, get row counts, and show a preview
    for table in tables:
        # Get row count
        count_query = f"SELECT COUNT(*) as count FROM {table}"
        row_count = pd.read_sql_query(count_query, conn).iloc[0]['count']
        
        print(f"\nTABLE: \033[1m{table}\033[0m")
        print(f"Total Rows: {row_count}")
        
        # Get a preview of the first 3 rows
        if row_count > 0:
            preview_query = f"SELECT * FROM {table} LIMIT 3"
            df_preview = pd.read_sql_query(preview_query, conn)
            
            # Print the dataframe nicely, truncating long columns so it fits on screen
            # pd.set_option('display.max_columns', 10)
            # pd.set_option('display.width', 1000)
            print("Preview (First 3 rows):")
            print(df_preview.head())
        else:
            print("Table is empty.")
            
        print("-" * 60)

    # 3. Test a relational JOIN
    print("\n\033[1m--- TESTING RELATIONAL JOIN ---\033[0m")
    print("Testing if we can join the 'general' table with 'taxable_summary' using pAccountID...")
    
    try:
        join_query = """
            SELECT 
                g.pAccountID, 
                g.streetAddress, 
                g.ownerPct,
                t.estimatedTaxes,
                t.totalTaxRate
            FROM general g
            JOIN taxable_summary t ON g.pAccountID = t.pAccountID
            LIMIT 5;
        """
        df_join = pd.read_sql_query(join_query, conn)
        print("Success! Here is a sample of joined data:")
        print(df_join)
    except Exception as e:
        print(f"Join query failed (you might not have both tables yet). Error: {e}")

    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify SQLite database contents")
    parser.add_argument("--county", default="Travis", help="County name (e.g. Travis, Williamson)")
    args = parser.parse_args()

    config = get_county_config(args.county)
    db_path = os.path.join(PROJECT_ROOT, config["db_file"])
    verify_database(db_path)