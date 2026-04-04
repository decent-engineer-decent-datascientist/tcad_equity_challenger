import os
import json
import glob
import shutil

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'scraped_data')

def clean_bad_data():
    if not os.path.exists(OUTPUT_DIR):
        print(f"Directory '{OUTPUT_DIR}' does not exist. Nothing to clean.")
        return

    # Find all data.json files in the subdirectories
    json_files = glob.glob(os.path.join(OUTPUT_DIR, '**', 'data.json'), recursive=True)
    print(f"Found {len(json_files)} total downloaded properties. Scanning for missing values...\n")

    deleted_count = 0
    
    # The endpoints that MUST have data for a successful scrape
    core_keys = ["general", "land", "taxable", "value_history", "improvement"]

    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError:
            # If the file is completely empty or corrupted, flag for deletion
            data = {}

        is_bad = False
        missing_key_name = ""

        # Check if the file is empty or missing the core data elements
        if not data:
            is_bad = True
            missing_key_name = "Corrupted/Empty JSON"
        else:
            for key in core_keys:
                if data.get(key) is None:
                    is_bad = True
                    missing_key_name = key
                    break
        
        # If bad, delete the parent directory (e.g., scraped_data/947344)
        if is_bad:
            dir_path = os.path.dirname(file_path)
            prop_id = os.path.basename(dir_path)
            print(f"[!] PID {prop_id} is missing '{missing_key_name}' -> Deleting folder.")
            
            try:
                shutil.rmtree(dir_path)
                deleted_count += 1
            except Exception as e:
                print(f"    Error deleting {dir_path}: {e}")

    print(f"\nCleanup complete! Deleted {deleted_count} incomplete property records.")
    print(f"Remaining clean properties: {len(json_files) - deleted_count}")

if __name__ == "__main__":
    clean_bad_data()