import argparse
import os
import glob
import json
import sqlite3
import sys
import pandas as pd
from tqdm import tqdm

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from config import get_county_config


def build_database(json_directory, db_name, batch_size=1000):
    print(f"Searching for JSON files in '{json_directory}'...")
    json_files = glob.glob(os.path.join(json_directory, '**', '*.json'), recursive=True)

    if not json_files:
        print("No JSON files found. Exiting.")
        return

    print(f"Found {len(json_files)} JSON files.\n")

    # Delete existing DB to avoid duplicates on re-run
    if os.path.exists(db_name):
        os.remove(db_name)

    conn = sqlite3.connect(db_name)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")

    total_batches = (len(json_files) + batch_size - 1) // batch_size
    pbar_read = tqdm(total=len(json_files), desc="Reading JSON", unit="file", position=0, dynamic_ncols=True)
    pbar_write = tqdm(total=total_batches, desc="Writing SQL ", unit="batch", position=1, dynamic_ncols=True, colour="green")

    TABLE_NAMES = ["general", "land", "value_history", "taxable_summary",
                   "taxable_units", "improvement", "improvement_details", "parcel"]

    def empty_tables():
        return {t: [] for t in TABLE_NAMES}

    def flush_tables(tables, first_batch):
        """Write accumulated rows to SQLite. First batch uses 'replace', rest use 'append'."""
        mode = 'replace' if first_batch else 'append'
        for table_name, rows in tables.items():
            if not rows:
                continue
            df = pd.DataFrame(rows)
            df.to_sql(table_name, conn, if_exists=mode, index=False)

    tables = empty_tables()
    first_batch = True

    for i, file_path in enumerate(json_files):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            tqdm.write(f"Error reading {file_path}: {e}")
            pbar_read.update(1)
            continue

        p_account_id = data.get("pAccountID")

        # 1. Standard List Tables (general, land, value_history)
        for table in ["general", "land", "value_history"]:
            if table in data and isinstance(data[table], dict) and "results" in data[table]:
                results = data[table]["results"]
                if isinstance(results, list) and results:
                    for row in results:
                        if "pAccountID" not in row:
                            row["pAccountID"] = p_account_id
                        tables[table].append(row)

        # 2. Complex Structure: Taxable
        if "taxable" in data and isinstance(data["taxable"], dict) and "results" in data["taxable"]:
            tax_results = data["taxable"]["results"]

            if isinstance(tax_results, dict):
                summary_data = {k: v for k, v in tax_results.items() if not isinstance(v, (list, dict))}
                if summary_data:
                    summary_data["pAccountID"] = p_account_id
                    tables["taxable_summary"].append(summary_data)

                if "taxingUnits" in tax_results and isinstance(tax_results["taxingUnits"], list):
                    for unit in tax_results["taxingUnits"]:
                        unit["pAccountID"] = p_account_id
                        tables["taxable_units"].append(unit)

        # 3. Complex Structure: Improvements
        if "improvement" in data and isinstance(data["improvement"], dict) and "results" in data["improvement"]:
            imprv_results = data["improvement"]["results"]

            if isinstance(imprv_results, list) and imprv_results:
                for imprv in imprv_results:
                    imprv_master = {k: v for k, v in imprv.items() if not isinstance(v, (list, dict))}
                    imprv_master["pAccountID"] = p_account_id
                    tables["improvement"].append(imprv_master)

                    if "details" in imprv and isinstance(imprv["details"], list):
                        for detail in imprv["details"]:
                            detail["pAccountID"] = p_account_id
                            tables["improvement_details"].append(detail)

        # 4. Complex Structure: Parcel (GeoJSON)
        if "parcel" in data and isinstance(data["parcel"], dict) and "results" in data["parcel"]:
            parcel_results = data["parcel"]["results"]

            if isinstance(parcel_results, list) and parcel_results:
                row_to_json = parcel_results[0].get("row_to_json", {})
                features = row_to_json.get("features", [])

                for feature in features:
                    properties = feature.get("properties", {})
                    geometry = feature.get("geometry", {})

                    parcel_row = properties.copy()
                    parcel_row["geometry"] = json.dumps(geometry)
                    parcel_row["pAccountID"] = p_account_id
                    tables["parcel"].append(parcel_row)

        pbar_read.update(1)

        # Flush every batch_size files
        if (i + 1) % batch_size == 0:
            flush_tables(tables, first_batch)
            first_batch = False
            tables = empty_tables()
            pbar_write.update(1)

    # Flush remaining
    if any(rows for rows in tables.values()):
        flush_tables(tables, first_batch)
        pbar_write.update(1)

    pbar_read.close()
    pbar_write.close()

    conn.execute("PRAGMA synchronous=FULL")
    conn.close()
    print(f"\nDone! Database saved to: {db_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build SQLite database from scraped JSON files")
    parser.add_argument("--county", default="Travis", help="County name (e.g. Travis, Williamson)")
    args = parser.parse_args()

    config = get_county_config(args.county)
    json_dir = os.path.join(PROJECT_ROOT, config["scraped_data_dir"])
    db_path = os.path.join(PROJECT_ROOT, config["db_file"])

    build_database(json_dir, db_path)
