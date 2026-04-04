import os
import glob
import json
import sqlite3
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_DIRECTORY = os.path.join(PROJECT_ROOT, "scraped_data")
DB_NAME = os.path.join(PROJECT_ROOT, "tcad_data.db")

def append_to_sql(df, table_name, conn):
    """Helper function to append a dataframe to SQLite safely."""
    if df.empty:
        return
    try:
        df.to_sql(table_name, conn, if_exists='append', index=False)
    except Exception as e:
        print(f"  [!] SQL error inserting {table_name} data: {e}")

def build_database():
    conn = sqlite3.connect(DB_NAME)
    
    print(f"Searching for JSON files in '{JSON_DIRECTORY}'...")
    json_files = glob.glob(os.path.join(JSON_DIRECTORY, '**', '*.json'), recursive=True)
    
    if not json_files:
        print("No JSON files found. Exiting.")
        return

    print(f"Found {len(json_files)} JSON files. Starting data ingestion...\n")

    for index, file_path in enumerate(json_files, 1):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            continue
            
        p_account_id = data.get("pAccountID")
        print(f"[{index}/{len(json_files)}] Processing pAccountID: {p_account_id}")
        
        # 1. Standard List Tables (general, land, value_history)
        for table in ["general", "land", "value_history"]:
            if table in data and isinstance(data[table], dict) and "results" in data[table]:
                results = data[table]["results"]
                if isinstance(results, list) and results:
                    df = pd.DataFrame(results)
                    if "pAccountID" not in df.columns:
                        df["pAccountID"] = p_account_id
                    append_to_sql(df, table, conn)

        # 2. Complex Structure: Taxable
        if "taxable" in data and isinstance(data["taxable"], dict) and "results" in data["taxable"]:
            tax_results = data["taxable"]["results"]
            
            if isinstance(tax_results, dict):
                # A. Taxable Summary (Top level dict properties)
                summary_data = {k: v for k, v in tax_results.items() if not isinstance(v, (list, dict))}
                if summary_data:
                    summary_data["pAccountID"] = p_account_id
                    df_summary = pd.DataFrame([summary_data])
                    append_to_sql(df_summary, "taxable_summary", conn)
                
                # B. Taxing Units (Nested list inside the dict)
                if "taxingUnits" in tax_results and isinstance(tax_results["taxingUnits"], list):
                    units = tax_results["taxingUnits"]
                    if units:
                        df_units = pd.DataFrame(units)
                        df_units["pAccountID"] = p_account_id
                        append_to_sql(df_units, "taxable_units", conn)

        # 3. Complex Structure: Improvements
        if "improvement" in data and isinstance(data["improvement"], dict) and "results" in data["improvement"]:
            imprv_results = data["improvement"]["results"]
            
            if isinstance(imprv_results, list) and imprv_results:
                imprv_master_list = []
                imprv_details_list = []
                
                for imprv in imprv_results:
                    # Isolate the top level improvement data
                    imprv_master = {k: v for k, v in imprv.items() if not isinstance(v, (list, dict))}
                    imprv_master["pAccountID"] = p_account_id
                    imprv_master_list.append(imprv_master)
                    
                    # Extract the nested "details" list
                    if "details" in imprv and isinstance(imprv["details"], list):
                        for detail in imprv["details"]:
                            # Link detail row back to its parent property
                            detail["pAccountID"] = p_account_id
                            # It inherently has 'pImprovementID' to link to the master improvement row
                            imprv_details_list.append(detail)
                
                # Write Master Improvements
                if imprv_master_list:
                    df_imprv = pd.DataFrame(imprv_master_list)
                    append_to_sql(df_imprv, "improvement", conn)
                    
                # Write Improvement Details
                if imprv_details_list:
                    df_details = pd.DataFrame(imprv_details_list)
                    append_to_sql(df_details, "improvement_details", conn)
                    
        # 4. Complex Structure: Parcel (GeoJSON)
        if "parcel" in data and isinstance(data["parcel"], dict) and "results" in data["parcel"]:
            parcel_results = data["parcel"]["results"]
            
            if isinstance(parcel_results, list) and parcel_results:
                # Dig into the deeply nested GeoJSON structure
                row_to_json = parcel_results[0].get("row_to_json", {})
                features = row_to_json.get("features", [])
                
                parcel_list = []
                for feature in features:
                    properties = feature.get("properties", {})
                    geometry = feature.get("geometry", {})
                    
                    # Flatten it out: extract the properties, and stringify the polygon geometry
                    parcel_row = properties.copy()
                    parcel_row["geometry"] = json.dumps(geometry) # Store the polygon as a JSON string
                    parcel_row["pAccountID"] = p_account_id
                    
                    parcel_list.append(parcel_row)
                    
                if parcel_list:
                    df_parcel = pd.DataFrame(parcel_list)
                    append_to_sql(df_parcel, "parcel", conn)

    print(f"\nDone! Database successfully saved to: {DB_NAME}")
    conn.close()

if __name__ == "__main__":
    build_database()