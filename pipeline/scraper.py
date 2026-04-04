import requests
import pandas as pd
import time
import json
import os
import random
from tqdm import tqdm

# --- CONFIGURATION ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(PROJECT_ROOT, 'export.xlsx')
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'scraped_data')
OFFICE = "Travis"
BASE_URL = "https://prod-container.trueprodigyapi.com"

# Stealth Settings
MIN_DELAY = 2.5  
MAX_DELAY = 5.0
COOLDOWN_PENALTY = 60.0  
MAX_RETRIES = 3

# Modern, common User-Agents to cycle through
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
]

# Global Session Object for TCP connection reuse
session = requests.Session()
CURRENT_TOKEN = None

def log(msg):
    """Wrapper to safely print logs above the tqdm progress bar."""
    tqdm.write(msg)

def get_random_headers(token=None):
    headers = {
        "Accept": "*/*",
        "Origin": "https://travis.prodigycad.com",
        "Referer": "https://travis.prodigycad.com/",
        "User-Agent": random.choice(USER_AGENTS)
    }
    if token:
        headers["Authorization"] = token
    else:
        headers["Content-Type"] = "application/json"
    return headers

def refresh_auth():
    global CURRENT_TOKEN, session
    log("\n  [!] Fetching fresh auth token and rotating identity...")
    
    url = f"{BASE_URL}/trueprodigy/cadpublic/auth/token"
    payload = {"office": OFFICE}
    
    session.headers.clear()
    session.headers.update(get_random_headers())
    
    response = session.post(url, json=payload, timeout=15)
    response.raise_for_status()
    
    try:
        token_data = response.json()
        token = token_data.get('user', {}).get('token')
        if not token:
            token = response.text.strip()
    except json.JSONDecodeError:
        token = response.text.strip()
        
    CURRENT_TOKEN = token.strip(' "\'\n\r')
    session.headers.update({"Authorization": CURRENT_TOKEN})

def polite_sleep():
    sleep_time = random.uniform(MIN_DELAY, MAX_DELAY)
    time.sleep(sleep_time)

def safe_request(method, url, payload=None):
    global session
    
    if CURRENT_TOKEN is None:
        refresh_auth()

    for attempt in range(MAX_RETRIES):
        try:
            if method.upper() == 'GET':
                response = session.get(url, timeout=15)
            else:
                response = session.post(url, json=payload, timeout=15)
            
            if response.status_code in [401, 403, 429]:
                log(f"  [!] Blocked by server ({response.status_code}). Taking a {COOLDOWN_PENALTY}s coffee break...")
                time.sleep(COOLDOWN_PENALTY)
                refresh_auth()  
                continue
                
            if response.status_code == 200:
                return response.json()
                
            # The 204 "No Content" fast-exit logic
            elif response.status_code == 204:
                return None 
                
            else:
                log(f"  [!] HTTP {response.status_code} on {url.split('/')[-1]} (Attempt {attempt+1}/{MAX_RETRIES})")
                polite_sleep()
                
        except requests.exceptions.RequestException as e:
            backoff = (2 ** attempt) * 2
            log(f"  [!] Network error: {e}. Backing off for {backoff}s (Attempt {attempt+1}/{MAX_RETRIES})")
            time.sleep(backoff)
            
    return None

def search_property(pid):
    url = f"{BASE_URL}/public/property/search"
    payload = {"pid": {"operator": "=", "value": str(pid)}}
    
    data = safe_request('POST', url, payload)
    if not data:
        return None
        
    results = data.get("results", [])
    if not results:
        log(f"  [!] No results found for PID {pid}")
        return None
        
    return results[-1]

def fetch_endpoint_data(endpoint_name, paccount_id):
    url = f"{BASE_URL}/public/propertyaccount/{paccount_id}/{endpoint_name}"
    return safe_request('GET', url)

def fetch_parcel_data(lon, lat):
    url = f"{BASE_URL}/gama/layeratpoint/{lon}/{lat}/parcels"
    return safe_request('GET', url)

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Could not find {INPUT_FILE}.")
        return

    print(f"Loading properties from {INPUT_FILE}...")
    df = pd.read_excel(INPUT_FILE)
    
    if 'Appraised Value' in df.columns:
        df = df[df['Appraised Value'] > 300000]
    
    if 'PropID' not in df.columns:
        print("Error: 'PropID' column not found.")
        return
        
    property_ids = df['PropID'].dropna().unique().astype(int)
    print(f"Found {len(property_ids)} unique properties to process.\n")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Initialize the Progress Bar
    pbar = tqdm(total=len(property_ids), desc="Scraping Progress", unit="prop", dynamic_ncols=True, colour='green')

    for index, pid in enumerate(property_ids, 1):
        pid_str = str(pid)
        
        prop_dir = os.path.join(OUTPUT_DIR, pid_str)
        prop_file = os.path.join(prop_dir, 'data.json')

        if os.path.exists(prop_file):
            log(f"[{index}/{len(property_ids)}] Skipping PID {pid_str} - Data already exists.")
            pbar.update(1)
            continue

        log(f"[{index}/{len(property_ids)}] Processing PID: {pid_str}")
        
        search_record = search_property(pid_str)
        if not search_record:
            polite_sleep()
            pbar.update(1)
            continue
            
        paccount_id = search_record.get("pAccountID")
        log(f"  > Found pAccountID: {paccount_id}")
        
        general_data = fetch_endpoint_data("general", paccount_id)
        land_data = fetch_endpoint_data("land", paccount_id)
        tax_data = fetch_endpoint_data("taxable", paccount_id)
        value_data = fetch_endpoint_data("valuehistory", paccount_id)
        improvement_data = fetch_endpoint_data("improvement", paccount_id)
        
        if not tax_data or not value_data:
            log(f"  [!] Critical data missing for PID {pid_str}. Skipping save.")
            polite_sleep()
            pbar.update(1)
            continue
        
        lat = None
        lon = None
        if general_data:
            lat = general_data.get('latitude') or general_data.get('lat')
            lon = general_data.get('longitude') or general_data.get('lng') or general_data.get('long')
        if not lat or not lon:
            lat = search_record.get('latitude') or search_record.get('lat')
            lon = search_record.get('longitude') or search_record.get('lng') or search_record.get('long')

        parcel_data = None
        if lat is not None and lon is not None:
            parcel_data = fetch_parcel_data(lon, lat)

        property_data = {
            "pAccountID": paccount_id,
            "general": general_data,
            "land": land_data,
            "taxable": tax_data,
            "value_history": value_data,
            "improvement": improvement_data,
            "parcel": parcel_data
        }
        
        os.makedirs(prop_dir, exist_ok=True)
        with open(prop_file, 'w') as f:
            json.dump(property_data, f, indent=4)
            
        log(f"  > Saved to {prop_file}")
        
        # Advance the progress bar by 1
        pbar.update(1)
        
        polite_sleep()

    pbar.close()
    print("\nScraping routine finished successfully!")

if __name__ == "__main__":
    main()