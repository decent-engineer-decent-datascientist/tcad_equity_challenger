"""
WCAD (Williamson County) property scraper.
Uses the search.wcad.org advanced search API to discover properties by neighborhood,
then fetches and parses the HTML property detail pages with BeautifulSoup.

Usage:
    python pipeline/wcad_scraper.py --neighborhood "CAT HOLLOW"
    python pipeline/wcad_scraper.py --neighborhood "CAT HOLLOW" --max-pages 5
"""

import argparse
import json
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add project root to path so we can import config
import sys
sys.path.insert(0, PROJECT_ROOT)
from config import get_county_config, DEFAULT_YEAR

COUNTY_CONFIG = get_county_config("Williamson")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, COUNTY_CONFIG["scraped_data_dir"])

# Stealth settings
MIN_DELAY = 0.25
MAX_DELAY = 1.0
MAX_RETRIES = 3

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]

session = requests.Session()         # used for search API (single-threaded)
_thread_local = __import__('threading').local()  # per-thread sessions for detail fetches


def _get_thread_session():
    """Return a requests.Session local to the current thread."""
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = requests.Session()
    return _thread_local.session


def log(msg):
    tqdm.write(msg)


def polite_sleep():
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))


def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://search.wcad.org/Advanced-Search",
        "Cookie": "dnn_IsMobile=False; language=en-US",
    }


def get_search_headers():
    return {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Origin": "https://search.wcad.org",
        "Referer": "https://search.wcad.org/Advanced-Search",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": random.choice(USER_AGENTS),
        "Cookie": "dnn_IsMobile=False; language=en-US",
    }


# ---------------------------------------------------------------------------
# SEARCH API: Discover all properties in a neighborhood
# ---------------------------------------------------------------------------

def search_neighborhood(neighborhood, year=DEFAULT_YEAR, page_size=20, max_pages=None, shuffle=True):
    """Paginate through the WCAD advanced search API for a given neighborhood.

    Yields (batch, total_count) tuples so callers can process properties
    as each page arrives instead of waiting for all pages to finish.
    If shuffle=True, randomizes page order after the first page.
    """

    log(f"Searching WCAD for neighborhood: {neighborhood} (year {year})...")

    def _fetch_page(page_num):
        skip = (page_num - 1) * page_size
        data = {
            "pn": page_num,
            "PropertyID": "", "CADID": "", "NameFirst": "", "NameLast": "",
            "PropertyOwnerID": "", "BusinessName": "",
            "StreetNoFrom": "", "StreetNoTo": "", "StreetName": "",
            "City": "", "ZipCode": "",
            "Neighborhood": neighborhood,
            "pStatus": "All",
            "AbstractSubdivisionCode": "", "AbstractSubdivisionName": "",
            "Block": "", "TractLot": "", "AcresFrom": "", "AcresTo": "",
            "ty": year, "pvty": year,
            "pt": "RP",  # Real Property
            "st": "9", "so": "1",
            "take": page_size, "skip": skip,
            "page": page_num, "pageSize": page_size,
        }

        for attempt in range(MAX_RETRIES):
            try:
                resp = session.post(
                    COUNTY_CONFIG["search_url"],
                    headers=get_search_headers(),
                    data=data,
                    timeout=30,
                )
                if resp.status_code == 200:
                    return resp.json()
                else:
                    log(f"  [!] Search returned HTTP {resp.status_code} (attempt {attempt+1})")
                    polite_sleep()
            except Exception as e:
                log(f"  [!] Search error: {e} (attempt {attempt+1})")
                time.sleep((2 ** attempt) * 2)

        log(f"  [!] Failed to fetch search page {page_num} after {MAX_RETRIES} attempts.")
        return None

    # Fetch page 1 to discover total
    result = _fetch_page(1)
    if not result:
        return

    batch = result.get("ResultList", [])
    total = result.get("RecordCount", 0)
    total_pages = result.get("TotalPageCount", 1)
    log(f"  Found {total} properties across {total_pages} pages.")

    yield batch, total

    if total_pages <= 1:
        return

    # Build list of remaining pages
    remaining = list(range(2, total_pages + 1))
    if max_pages:
        remaining = remaining[:max_pages - 1]  # already fetched page 1
    if shuffle:
        random.shuffle(remaining)

    for page_num in remaining:
        polite_sleep()
        result = _fetch_page(page_num)
        if not result:
            continue
        batch = result.get("ResultList", [])
        yield batch, total


# ---------------------------------------------------------------------------
# HTML PARSING: Extract structured data from the property detail page
# ---------------------------------------------------------------------------

def clean_text(element):
    """Extract and clean text from a BeautifulSoup element."""
    if element is None:
        return None
    text = element.get_text(strip=True)
    if text in ("-", "", "-\n"):
        return None
    return text


def clean_money(text):
    """Parse a money string like '$554,085' into an integer."""
    if text is None or text in ("N/A", "-", ""):
        return None
    cleaned = text.replace("$", "").replace(",", "").strip()
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def parse_property_html(html_content, property_quick_ref_id, party_quick_ref_id):
    """Parse a WCAD property detail HTML page into structured data matching the TCAD JSON schema."""
    soup = BeautifulSoup(html_content, "html.parser")
    data = {"pAccountID": property_quick_ref_id}

    # --- GENERAL INFORMATION ---
    general = {"results": [{}]}
    r = general["results"][0]

    r["pAccountID"] = property_quick_ref_id
    r["pYear"] = DEFAULT_YEAR
    r["propType"] = "R"

    # Property header values
    header_value = soup.find(id="dnn_ctr1460_View_tdPropertyValueHeader")
    address_el = soup.find(id="dnn_ctr1460_View_tdPropertyAddress")
    r["situsAddr"] = clean_text(address_el)
    r["streetAddress"] = clean_text(address_el)

    owner_el = soup.find(id="dnn_ctr1460_View_divOwnersLabel")
    r["name"] = clean_text(owner_el)

    # General info fields
    r["propertyStatus"] = clean_text(soup.find(id="dnn_ctr1460_View_tdGIPropertyStatus"))
    r["legalDescription"] = clean_text(soup.find(id="dnn_ctr1460_View_tdGILegalDescription"))
    r["marketArea"] = clean_text(soup.find(id="dnn_ctr1460_View_tdGINeighborhood"))
    r["mapID"] = clean_text(soup.find(id="dnn_ctr1460_View_tdGIMapNumber"))

    # Owner info fields
    r["ownerPct"] = clean_text(soup.find(id="dnn_ctr1460_View_tdOIPercentOwnership"))
    if r["ownerPct"]:
        r["ownerPct"] = r["ownerPct"].replace("%", "").strip()

    exemptions_el = soup.find(id="dnn_ctr1460_View_tdOIExemptions")
    r["exemptionList"] = clean_text(exemptions_el) or ""

    mailing_el = soup.find(id="dnn_ctr1460_View_tdOIMailingAddress")
    r["address"] = clean_text(mailing_el)

    agent_el = soup.find(id="dnn_ctr1460_View_tdAgent")
    r["agent"] = clean_text(agent_el)

    # Use the PropertyNumber as geoID equivalent
    account_el = soup.find(id="dnn_ctr1460_View_tdGIAccount")
    r["geoID"] = clean_text(account_el)
    r["pID"] = property_quick_ref_id

    data["general"] = general

    # --- VALUE INFORMATION (current year summary) ---
    vi = {}
    vi["ownerImprovementValue"] = clean_money(clean_text(soup.find(id="dnn_ctr1460_View_tdVITotalImprovementMV")))
    vi["ownerLandValue"] = clean_money(clean_text(soup.find(id="dnn_ctr1460_View_tdVITotalLandMV")))
    vi["ownerMarketValue"] = clean_money(clean_text(soup.find(id="dnn_ctr1460_View_tdVITotalMV")))
    vi["ownerAppraisedValue"] = clean_money(clean_text(soup.find(id="dnn_ctr1460_View_tdVITotalAppraisedValue")))
    vi["ownerNetAppraisedValue"] = clean_money(clean_text(soup.find(id="dnn_ctr1460_View_tdVITotalAssessedValueRP")))

    # --- VALUE HISTORY ---
    value_history = {"results": []}
    vh_table = soup.find(id="dnn_ctr1460_View_tblValueHistoryDataRP")
    if vh_table:
        rows = vh_table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 12:
                texts = [clean_text(c) for c in cells]
                vh_row = {
                    "pAccountID": property_quick_ref_id,
                    "pid": property_quick_ref_id,
                    "pYear": texts[0],
                    "pVersion": 0,
                    "pRollCorr": 0,
                    "ownerLandValue": clean_money(texts[2]),
                    "ownerImprovementValue": clean_money(texts[1]),
                    "ownerMarketValue": clean_money(texts[3]),
                    "ownerAppraisedValue": clean_money(texts[8]),
                    "ownerNetAppraisedValue": clean_money(texts[11]),
                    "ownerSUExclusionValue": 0,
                    "ownerTaxLimitationValue": 0,
                }
                value_history["results"].append(vh_row)

    # Add current year from VALUE INFORMATION section
    current_year_vh = {
        "pAccountID": property_quick_ref_id,
        "pid": property_quick_ref_id,
        "pYear": DEFAULT_YEAR,
        "pVersion": 0,
        "pRollCorr": 0,
        "ownerLandValue": vi.get("ownerLandValue"),
        "ownerImprovementValue": vi.get("ownerImprovementValue"),
        "ownerMarketValue": vi.get("ownerMarketValue"),
        "ownerAppraisedValue": vi.get("ownerAppraisedValue"),
        "ownerNetAppraisedValue": vi.get("ownerNetAppraisedValue"),
        "ownerSUExclusionValue": 0,
        "ownerTaxLimitationValue": 0,
    }
    value_history["results"].insert(0, current_year_vh)
    data["value_history"] = value_history

    # --- LAND SEGMENTS ---
    land = {"results": []}
    land_table = soup.find(id="dnn_ctr1460_View_tblLandSegmentsData")
    if land_table:
        rows = land_table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 7 and not row.get("class", [None]) == ["totals"]:
                if "totals" in row.get("class", []):
                    # Extract total sqft/acres from totals row
                    total_text = clean_text(cells[-1])
                    if total_text:
                        sqft_match = re.search(r"([\d,]+)\s*Sq\.\s*ft", total_text)
                        acres_match = re.search(r"([\d.]+)\s*acres", total_text)
                        land_sqft = sqft_match.group(1).replace(",", "") if sqft_match else None
                        land_acres = acres_match.group(1) if acres_match else None
                else:
                    texts = [clean_text(c) for c in cells]
                    # Parse land size from last column
                    size_text = texts[6] or ""
                    acres_match = re.search(r"([\d.]+)\s*acres", size_text, re.IGNORECASE)
                    sqft_match = re.search(r"([\d,]+)\s*[Ss]q", size_text)

                    land_row = {
                        "landType": "LAND",
                        "landDescription": texts[0],
                        "sizeAcres": acres_match.group(1) if acres_match else "0",
                        "sizeSqft": sqft_match.group(1).replace(",", "") if sqft_match else "0",
                        "costPerSqft": "0",
                        "mktValue": clean_money(texts[3]) or 0,
                        "suValue": 0,
                        "pAccountID": property_quick_ref_id,
                    }
                    land["results"].append(land_row)

    # If no individual land rows parsed, create one from the totals
    if not land["results"]:
        land_table_totals = soup.find(id="dnn_ctr1460_View_tblLandSegmentsData")
        if land_table_totals:
            totals_row = land_table_totals.find("tr", class_="totals")
            if totals_row:
                cells = totals_row.find_all("td")
                total_text = clean_text(cells[-1]) if cells else ""
                sqft_match = re.search(r"([\d,]+)\s*Sq\.\s*ft", total_text or "")
                acres_match = re.search(r"([\d.]+)\s*acres", total_text or "")
                land["results"].append({
                    "landType": "LAND",
                    "landDescription": "Land",
                    "sizeAcres": acres_match.group(1) if acres_match else "0",
                    "sizeSqft": sqft_match.group(1).replace(",", "") if sqft_match else "0",
                    "costPerSqft": "0",
                    "mktValue": vi.get("ownerLandValue") or 0,
                    "suValue": 0,
                    "pAccountID": property_quick_ref_id,
                })
    data["land"] = land

    # --- TAXABLE (summary + taxing units) ---
    taxable = {"results": {}}
    tax_results = taxable["results"]
    tax_results["estimatedTaxes"] = 0
    tax_results["totalTaxRate"] = 0
    tax_results["displayValues"] = 1

    taxing_units = []
    ee_table = soup.find(id="tblEntitiesAndExemptionsData")
    if ee_table:
        rows = ee_table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 5:
                if "totals" in row.get("class", []):
                    rate_text = clean_text(cells[-1])
                    if rate_text:
                        try:
                            tax_results["totalTaxRate"] = float(rate_text)
                        except ValueError:
                            pass
                else:
                    texts = [clean_text(c) for c in cells]
                    entity_name = texts[0] or ""
                    # Strip the icon prefix if present
                    entity_name = re.sub(r"^[A-Z0-9]+-\s*", "", entity_name).strip()

                    rate_str = texts[4] or "0"
                    try:
                        rate = float(rate_str)
                    except ValueError:
                        rate = 0

                    taxable_val = clean_money(texts[3]) or 0

                    taxing_units.append({
                        "taxingUnitCode": "",
                        "taxingUnitName": entity_name,
                        "arbStatus": "NOT CERTIFIED",
                        "totalTaxRate": rate,
                        "netAppraisedValue": taxable_val,
                        "taxableValue": taxable_val,
                        "estimatedTaxes": 0,
                        "estimatedTaxesWoutExemptions": 0,
                        "displayValues": 1,
                    })

    tax_results["taxingUnits"] = taxing_units
    data["taxable"] = taxable

    # --- IMPROVEMENTS ---
    improvement = {"results": []}
    imprv_sections = soup.find_all("table", class_=lambda c: c and "improvementTable" in c)

    for imprv_table in imprv_sections:
        # Get the improvement header row data
        header_cells = imprv_table.find_all("td", class_="improvementsFieldData")
        if not header_cells:
            continue

        imprv_name = clean_text(header_cells[0]) if len(header_cells) > 0 else None
        state_code_text = clean_text(header_cells[1]) if len(header_cells) > 1 else None
        homesite = clean_text(header_cells[2]) if len(header_cells) > 2 else None
        total_area_text = clean_text(header_cells[3]) if len(header_cells) > 3 else None

        # Parse state code (e.g. "A1 - Residential Single Family" -> "A1")
        state_cd = ""
        if state_code_text:
            parts = state_code_text.split(" - ")
            state_cd = parts[0].strip()

        # Parse total area
        living_area = "0"
        if total_area_text:
            area_match = re.search(r"([\d,]+)", total_area_text)
            if area_match:
                living_area = area_match.group(1).replace(",", "")

        imprv_id = f"{property_quick_ref_id}_imprv_{len(improvement['results'])}"

        imprv_record = {
            "pImprovementID": imprv_id,
            "imprvDescription": "1 FAM DWELLING",
            "imprvSpecificDescription": state_code_text or "1 FAM DWELLING",
            "improvementValue": vi.get("ownerImprovementValue") or 0,
            "stateCd": state_cd,
            "grossBuildingArea": "0",
            "livingArea": living_area,
            "pAccountID": property_quick_ref_id,
            "details": [],
        }

        # Parse segment rows (the detail rows under this improvement)
        parent_li = imprv_table.find_parent("li")
        if parent_li:
            segment_tables = parent_li.find_all("table", class_="fullWidthTable")
            for seg_table in segment_tables:
                seg_rows = seg_table.find_all("tr", recursive=False)
                for seg_row in seg_rows:
                    seg_cells = seg_row.find_all("td", class_="table-responsive-cell")
                    if len(seg_cells) >= 4:
                        seg_texts = [clean_text(c) for c in seg_cells]
                        record_num = seg_texts[0]
                        seg_type = seg_texts[1] or ""
                        year_built = seg_texts[2] or ""
                        sqft = seg_texts[3] or "0"

                        # Parse details from the expandable detail rows
                        detail_class = ""
                        bedrooms = "0"
                        baths = "0"
                        fireplaces = "0"

                        # Look for the segment detail table
                        detail_tables = seg_row.find_next_siblings("tr", class_="detailsRow")
                        if not detail_tables:
                            # Try the parent context
                            next_row = seg_row.find_next_sibling("tr")
                            if next_row and "detailsRow" in next_row.get("class", []):
                                detail_tables = [next_row]

                        for detail_tr in detail_tables[:1]:
                            detail_tbl = detail_tr.find("table", class_="segmentDetailsTable")
                            if detail_tbl:
                                detail_cells = detail_tbl.find_all("td")
                                for i, cell in enumerate(detail_cells):
                                    label = clean_text(cell) or ""
                                    if "Class" in label and i + 1 < len(detail_cells):
                                        detail_class = clean_text(detail_cells[i + 1]) or ""
                                    elif "Bedrooms" in label and i + 1 < len(detail_cells):
                                        bedrooms = clean_text(detail_cells[i + 1]) or "0"
                                    elif "Baths" in label and i + 1 < len(detail_cells):
                                        baths = clean_text(detail_cells[i + 1]) or "0"
                                    elif "Fireplaces" in label and i + 1 < len(detail_cells):
                                        fireplaces = clean_text(detail_cells[i + 1]) or "0"

                        # Map segment type to TCAD-compatible detailTypeDescription
                        type_map = {
                            "Main Area": "1st Floor",
                            "Second Floor": "2nd Floor",
                            "Garage": "GARAGE ATT 1ST F",
                            "Patio": "PATIO",
                            "Fireplace": "FIREPLACE",
                        }
                        detail_type = type_map.get(seg_type, seg_type.upper())

                        # Extract class code from full string like "R4 (R4 - SINGLE FAMILY RESIDENCE)"
                        class_code = ""
                        if detail_class:
                            class_match = re.match(r"^(\w+)", detail_class)
                            if class_match:
                                class_code = class_match.group(1)

                        detail_record = {
                            "pImprovementID": imprv_id,
                            "pDetailID": f"{imprv_id}_seg_{record_num}",
                            "imprvDetailType": seg_type,
                            "createDt": "",
                            "detailTypeDescription": detail_type,
                            "class": class_code,
                            "units": 0,
                            "effYearBuilt": year_built,
                            "actualYearBuilt": year_built,
                            "area": sqft.replace(",", ""),
                            "primaryFeature": None,
                            "pAccountID": property_quick_ref_id,
                        }
                        imprv_record["details"].append(detail_record)

                        # Emit synthetic detail records for bed/bath/fireplace counts
                        # so the hedonic regression SQL can find them via LIKE patterns
                        synthetic_features = []
                        if bedrooms != "0" and bedrooms:
                            synthetic_features.append(("BEDROOMS", bedrooms))
                        if baths != "0" and baths:
                            # Parse full/half baths from "X.Y" format (e.g. "3.1" = 3 full, 1 half)
                            bath_parts = baths.split(".")
                            full_baths = bath_parts[0]
                            synthetic_features.append(("BATHROOM", full_baths))
                            if len(bath_parts) > 1 and bath_parts[1] != "0":
                                synthetic_features.append(("HALF BATHROOM", bath_parts[1]))
                        if fireplaces != "0" and fireplaces:
                            synthetic_features.append(("FIREPLACE", fireplaces))

                        for feat_type, feat_area in synthetic_features:
                            synth_record = {
                                "pImprovementID": imprv_id,
                                "pDetailID": f"{imprv_id}_seg_{record_num}_{feat_type.lower().replace(' ', '_')}",
                                "imprvDetailType": feat_type,
                                "createDt": "",
                                "detailTypeDescription": feat_type,
                                "class": "",
                                "units": 0,
                                "effYearBuilt": year_built,
                                "actualYearBuilt": year_built,
                                "area": feat_area,
                                "primaryFeature": None,
                                "pAccountID": property_quick_ref_id,
                            }
                            imprv_record["details"].append(synth_record)

        improvement["results"].append(imprv_record)
    data["improvement"] = improvement

    # No parcel data available from WCAD HTML
    data["parcel"] = None

    return data


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def fetch_property_detail(property_id, party_id, year=DEFAULT_YEAR):
    """Fetch the full HTML property detail page from WCAD."""
    url = COUNTY_CONFIG["detail_url_template"].format(
        property_id=property_id,
        party_id=party_id,
        year=year,
    )

    for attempt in range(MAX_RETRIES):
        try:
            s = _get_thread_session()
            resp = s.get(url, headers=get_headers(), timeout=30)
            if resp.status_code == 200:
                return resp.text
            else:
                log(f"  [!] Detail page returned HTTP {resp.status_code} (attempt {attempt+1})")
                polite_sleep()
        except Exception as e:
            backoff = (2 ** attempt) * 2
            log(f"  [!] Network error: {e}. Backing off {backoff}s (attempt {attempt+1})")
            time.sleep(backoff)

    return None


def main():
    parser = argparse.ArgumentParser(description="Scrape WCAD property data by neighborhood")
    parser.add_argument("--neighborhood", required=True, help="Neighborhood name (e.g. 'CAT HOLLOW')")
    parser.add_argument("--year", default=DEFAULT_YEAR, help="Tax year (default: 2026)")
    parser.add_argument("--max-pages", type=int, default=None, help="Max search result pages to fetch")
    parser.add_argument("--min-value", type=float, default=0, help="Min property value filter (default: 0)")
    parser.add_argument("--workers", type=int, default=8, help="Parallel detail page fetchers (default: 8)")
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Stream search pages and scrape properties as each batch arrives
    pbar_props = None
    pbar_pages = None
    processed = 0
    found_any = False

    for batch, total in search_neighborhood(args.neighborhood, args.year, max_pages=args.max_pages):
        if not found_any and not batch:
            continue
        found_any = True

        # Initialize progress bars on first batch (now we know the total)
        if pbar_props is None:
            total_pages = (total + 19) // 20  # ceil(total / page_size)
            if args.max_pages:
                total_pages = min(total_pages, args.max_pages)
                total = min(total, args.max_pages * 20)
            pbar_pages = tqdm(total=total_pages, desc="Search pages", unit="pg", position=0, dynamic_ncols=True)
            pbar_props = tqdm(total=total, desc="Properties  ", unit="prop", position=1, dynamic_ncols=True, colour="green")

        pbar_pages.update(1)

        # Filter batch by minimum value
        filtered_batch = [r for r in batch if (r.get("PropertyValue") or 0) >= args.min_value]

        # Split into already-scraped and needs-scraping
        to_scrape = []
        for result in filtered_batch:
            prop_id = result["PropertyQuickRefID"]
            prop_file = os.path.join(OUTPUT_DIR, prop_id, "data.json")
            if os.path.exists(prop_file):
                processed += 1
                log(f"[{processed}] Skipping {prop_id} - already exists.")
                pbar_props.update(1)
            else:
                to_scrape.append(result)

        # Fetch + parse in parallel
        def _process_one(result):
            """Fetch, parse, enrich, and save one property. Returns (prop_id, ok)."""
            prop_id = result["PropertyQuickRefID"]
            party_id = result["PartyQuickRefID"]

            html = fetch_property_detail(prop_id, party_id, args.year)
            if not html:
                return prop_id, False, "fetch failed"

            try:
                property_data = parse_property_html(html, prop_id, party_id)
            except Exception as e:
                return prop_id, False, str(e)

            # Guard against empty/broken HTML — require at least an address or appraised value
            gen_results = property_data.get("general", {}).get("results", [])
            val_results = property_data.get("value_history", {}).get("results", [])
            has_address = gen_results and gen_results[0].get("streetAddress")
            has_value = val_results and any(r.get("ownerAppraisedValue") for r in val_results)
            if not has_address and not has_value:
                return prop_id, False, "empty/broken HTML (no address or value found)"

            # Enrich with search result data
            if property_data.get("general", {}).get("results"):
                gen = property_data["general"]["results"][0]
                if not gen.get("streetAddress"):
                    gen["streetAddress"] = result.get("SitusAddress")
                if not gen.get("name"):
                    gen["name"] = result.get("OwnerName")

            prop_dir = os.path.join(OUTPUT_DIR, prop_id)
            os.makedirs(prop_dir, exist_ok=True)
            with open(os.path.join(prop_dir, "data.json"), "w") as f:
                json.dump(property_data, f, indent=4)

            return prop_id, True, os.path.join(prop_dir, "data.json")

        if to_scrape:
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = {executor.submit(_process_one, r): r for r in to_scrape}
                for future in as_completed(futures):
                    processed += 1
                    result = futures[future]
                    prop_id = result["PropertyQuickRefID"]
                    addr = result.get("SitusAddress", "N/A")
                    try:
                        pid, ok, info = future.result()
                        if ok:
                            log(f"[{processed}] {pid} ({addr}) > Saved to {info}")
                        else:
                            log(f"[{processed}] {pid} ({addr}) [!] {info}. Skipping.")
                    except Exception as e:
                        log(f"[{processed}] {prop_id} ({addr}) [!] Unexpected error: {e}")
                    pbar_props.update(1)

        # Update bar for any filtered-out properties in this batch
        skipped = len(batch) - len(filtered_batch)
        if skipped > 0:
            pbar_props.update(skipped)

    if not found_any:
        print("No properties found. Check the neighborhood name.")
        return

    if pbar_pages:
        pbar_pages.close()
    if pbar_props:
        pbar_props.close()
    print("\nWCAD scraping routine finished successfully!")


if __name__ == "__main__":
    main()
