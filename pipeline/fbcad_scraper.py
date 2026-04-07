"""
FBCAD (Fort Bend County) property scraper.
Uses the esearch.fbcad.org search API to discover properties by subdivision,
then fetches and parses the HTML property detail pages with BeautifulSoup.

Usage:
    python pipeline/fbcad_scraper.py --subdivision "5741-01 - Parks Edge Sec 1"
    python pipeline/fbcad_scraper.py --subdivision "5741-01 - Parks Edge Sec 1" --max-pages 5
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

import sys
sys.path.insert(0, PROJECT_ROOT)
from config import get_county_config, DEFAULT_YEAR

COUNTY_CONFIG = get_county_config("Fort Bend")
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

session = requests.Session()
_thread_local = __import__('threading').local()


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
        "Referer": "https://esearch.fbcad.org/",
    }


def get_search_headers():
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "https://esearch.fbcad.org/Search/Result",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": random.choice(USER_AGENTS),
    }


# ---------------------------------------------------------------------------
# SEARCH API: Discover all properties in a subdivision
# ---------------------------------------------------------------------------

def search_subdivision(subdivision, year=DEFAULT_YEAR, page_size=100, max_pages=None, shuffle=True):
    """Paginate through the FBCAD search API for a given subdivision.

    Yields (batch, total_count) tuples so callers can process properties
    as each page arrives instead of waiting for all pages to finish.
    If shuffle=True, randomizes page order after the first page.
    """

    log(f"Searching FBCAD for subdivision: {subdivision} (year {year})...")

    def _fetch_page(page_num):
        params = {
            "keywords": f'Subdivision:"{subdivision}" Year:{year}',
            "page": page_num,
            "pageSize": page_size,
        }

        for attempt in range(MAX_RETRIES):
            try:
                resp = session.get(
                    COUNTY_CONFIG["search_url"],
                    headers=get_search_headers(),
                    params=params,
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

    batch = result.get("resultsList", [])
    total = result.get("totalResults", 0)
    total_pages = result.get("totalPages", 1)
    log(f"  Found {total} properties across {total_pages} pages.")

    yield batch, total

    if total_pages <= 1:
        return

    # Build list of remaining pages
    remaining = list(range(2, total_pages + 1))
    if max_pages:
        remaining = remaining[:max_pages - 1]
    if shuffle:
        random.shuffle(remaining)

    for page_num in remaining:
        polite_sleep()
        result = _fetch_page(page_num)
        if not result:
            continue
        batch = result.get("resultsList", [])
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
    """Parse a money string like '$554,085 (+)' into an integer."""
    if text is None or text in ("N/A", "-", ""):
        return None
    # Strip FBCAD suffixes like (+), (-), (=)
    cleaned = re.sub(r'\s*\([+\-=]\)\s*$', '', text)
    cleaned = cleaned.replace("$", "").replace(",", "").strip()
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def _find_panel(soup, heading_text):
    """Find a panel div by its heading text (e.g. 'Property Details')."""
    for heading in soup.find_all("div", class_="panel-heading"):
        if heading_text in heading.get_text():
            return heading.find_parent("div", class_="panel")
    return None


def _get_detail_field(table, label):
    """Get a value from a Property Details table by its <th> label text."""
    if table is None:
        return None
    for th in table.find_all("th"):
        if label in th.get_text():
            td = th.find_next_sibling("td")
            if td:
                return clean_text(td)
    # Also check <strong> tags inside <td> elements (Geographic ID, Zoning, etc.)
    for td in table.find_all("td"):
        strong = td.find("strong")
        if strong and label in strong.get_text():
            # The value is the text after the strong tag
            remaining = strong.next_sibling
            if remaining:
                val = remaining.strip() if isinstance(remaining, str) else clean_text(remaining)
                return val if val else None
    return None


def _get_value_field(table, label):
    """Get a dollar value from a Property Values table by its <th> label text."""
    if table is None:
        return None
    for th in table.find_all("th"):
        if label in th.get_text():
            td = th.find_next_sibling("td")
            if td:
                return clean_money(clean_text(td))
    return None


def parse_property_html(html_content, property_id, owner_id):
    """Parse an FBCAD property detail HTML page into structured data matching the standard JSON schema."""
    soup = BeautifulSoup(html_content, "html.parser")
    data = {"pAccountID": property_id}

    # --- PROPERTY DETAILS PANEL ---
    details_panel = _find_panel(soup, "Property Details")
    details_table = details_panel.find("table") if details_panel else None

    general = {"results": [{}]}
    r = general["results"][0]

    r["pAccountID"] = property_id
    r["pYear"] = DEFAULT_YEAR
    r["propType"] = "R"

    r["pID"] = property_id
    r["geoID"] = _get_detail_field(details_table, "Geographic ID")
    r["situsAddr"] = _get_detail_field(details_table, "Situs Address")
    r["streetAddress"] = r["situsAddr"]
    r["legalDescription"] = _get_detail_field(details_table, "Legal Description")
    r["marketArea"] = _get_detail_field(details_table, "Neighborhood")
    r["mapID"] = _get_detail_field(details_table, "Map ID")
    r["name"] = _get_detail_field(details_table, "Name:")
    r["agent"] = _get_detail_field(details_table, "Agent:")
    r["address"] = None
    mailing_th = details_table.find("th", string=lambda s: s and "Mailing Address" in s) if details_table else None
    if mailing_th:
        mailing_td = mailing_th.find_next_sibling("td")
        if mailing_td:
            r["address"] = mailing_td.get_text(separator=", ", strip=True)

    r["ownerPct"] = _get_detail_field(details_table, "% Ownership")
    if r["ownerPct"]:
        r["ownerPct"] = r["ownerPct"].replace("%", "").strip()

    r["exemptionList"] = _get_detail_field(details_table, "Exemptions") or ""
    r["propertyStatus"] = None
    r["abstractSubdivision"] = _get_detail_field(details_table, "Abstract/Subdivision")

    data["general"] = general

    # --- PROPERTY VALUES PANEL ---
    values_panel = _find_panel(soup, "Property Values")
    values_table = values_panel.find("table") if values_panel else None

    vi = {}
    vi["ownerImprovementValue"] = None
    vi["ownerLandValue"] = None

    # Sum homesite + non-homesite for improvement and land
    imp_hs = _get_value_field(values_table, "Improvement Homesite Value") or 0
    imp_nhs = _get_value_field(values_table, "Improvement Non-Homesite Value") or 0
    vi["ownerImprovementValue"] = imp_hs + imp_nhs if (imp_hs or imp_nhs) else None

    land_hs = _get_value_field(values_table, "Land Homesite Value") or 0
    land_nhs = _get_value_field(values_table, "Land Non-Homesite Value") or 0
    vi["ownerLandValue"] = land_hs + land_nhs if (land_hs or land_nhs) else None

    vi["ownerMarketValue"] = _get_value_field(values_table, "Market Value")
    vi["ownerAppraisedValue"] = _get_value_field(values_table, "Appraised Value")
    vi["ownerNetAppraisedValue"] = vi["ownerAppraisedValue"]  # FBCAD doesn't separate these

    # --- VALUE HISTORY PANEL ---
    value_history = {"results": []}
    history_panel = _find_panel(soup, "Roll Value History")
    if history_panel:
        history_table = history_panel.find("table")
        if history_table:
            for row in history_table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) >= 6:
                    texts = [clean_text(c) for c in cells]
                    # Columns: Year, Improvements, Land Market, Ag Valuation, HS Cap Loss, Appraised
                    imp_val = clean_money(texts[1])
                    land_val = clean_money(texts[2])
                    appraised = clean_money(texts[5])
                    market = None
                    if imp_val is not None and land_val is not None:
                        market = imp_val + land_val

                    vh_row = {
                        "pAccountID": property_id,
                        "pid": property_id,
                        "pYear": texts[0],
                        "pVersion": 0,
                        "pRollCorr": 0,
                        "ownerLandValue": land_val,
                        "ownerImprovementValue": imp_val,
                        "ownerMarketValue": market,
                        "ownerAppraisedValue": appraised,
                        "ownerNetAppraisedValue": appraised,
                        "ownerSUExclusionValue": 0,
                        "ownerTaxLimitationValue": 0,
                    }
                    value_history["results"].append(vh_row)

    # If current year not in history rows, add from values panel
    current_years = {r.get("pYear") for r in value_history["results"]}
    if DEFAULT_YEAR not in current_years:
        current_year_vh = {
            "pAccountID": property_id,
            "pid": property_id,
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

    # --- LAND PANEL ---
    land = {"results": []}
    land_panel = _find_panel(soup, "Property Land")
    if land_panel:
        land_table = land_panel.find("table")
        if land_table:
            for row in land_table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) >= 7:
                    land_type = clean_text(cells[0]) or "LAND"
                    description = clean_text(cells[1]) or ""

                    # Acreage is stored in data-acres attribute
                    acreage_cell = cells[2]
                    acres = acreage_cell.get("data-acres", "0") if acreage_cell else "0"

                    sqft_text = clean_text(cells[3]) or "0"
                    sqft = sqft_text.replace(",", "").replace(".00", "")

                    market_val = clean_money(clean_text(cells[6])) or 0

                    land["results"].append({
                        "landType": land_type,
                        "landDescription": description,
                        "sizeAcres": acres,
                        "sizeSqft": sqft,
                        "costPerSqft": "0",
                        "mktValue": market_val,
                        "suValue": 0,
                        "pAccountID": property_id,
                    })

    # Fallback: create a single land row from values panel
    if not land["results"] and vi.get("ownerLandValue"):
        land["results"].append({
            "landType": "LAND",
            "landDescription": "Land",
            "sizeAcres": "0",
            "sizeSqft": "0",
            "costPerSqft": "0",
            "mktValue": vi.get("ownerLandValue") or 0,
            "suValue": 0,
            "pAccountID": property_id,
        })

    data["land"] = land

    # --- TAXING JURISDICTION PANEL ---
    taxable = {"results": {}}
    tax_results = taxable["results"]
    tax_results["estimatedTaxes"] = 0
    tax_results["totalTaxRate"] = 0
    tax_results["displayValues"] = 1

    taxing_units = []
    tax_panel = _find_panel(soup, "Taxing Jurisdiction")
    if tax_panel:
        tax_table = tax_panel.find("table")
        if tax_table:
            for row in tax_table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) >= 4:
                    entity_code = clean_text(cells[0]) or ""
                    entity_name = clean_text(cells[1]) or ""
                    market_val = clean_money(clean_text(cells[2])) or 0
                    taxable_val = clean_money(clean_text(cells[3])) or 0

                    taxing_units.append({
                        "taxingUnitCode": entity_code,
                        "taxingUnitName": entity_name,
                        "arbStatus": "NOT CERTIFIED",
                        "totalTaxRate": 0,
                        "netAppraisedValue": market_val,
                        "taxableValue": taxable_val,
                        "estimatedTaxes": 0,
                        "estimatedTaxesWoutExemptions": 0,
                        "displayValues": 1,
                    })

        # Total tax rate from panel footer
        footer = tax_panel.find("div", class_="panel-footer")
        if footer:
            rate_text = footer.get_text()
            rate_match = re.search(r'Total Tax Rate:\s*([\d.]+)', rate_text)
            if rate_match:
                try:
                    tax_results["totalTaxRate"] = float(rate_match.group(1))
                except ValueError:
                    pass

    tax_results["taxingUnits"] = taxing_units
    data["taxable"] = taxable

    # --- IMPROVEMENT BUILDING PANEL ---
    improvement = {"results": []}
    imprv_panel = _find_panel(soup, "Improvement - Building")
    if imprv_panel:
        responsive_divs = imprv_panel.find_all("div", class_="table-responsive")
        for div in responsive_divs:
            info_div = div.find("div", class_="panel-table-info")
            if not info_div:
                continue

            # Parse header: Type, State Code, Living Area, Value
            imprv_type = ""
            state_cd = ""
            living_area = "0"
            imprv_value = 0

            for span in info_div.find_all("span"):
                text = span.get_text(strip=True)
                if text.startswith("Type:"):
                    imprv_type = text.replace("Type:", "").strip()
                elif text.startswith("State Code:"):
                    state_cd = text.replace("State Code:", "").strip()
                elif text.startswith("Living Area:"):
                    area_text = text.replace("Living Area:", "").strip()
                    area_match = re.search(r'([\d,]+)', area_text)
                    if area_match:
                        living_area = area_match.group(1).replace(",", "")

            # Value is in a <strong> directly in the info div (not inside a span)
            for strong in info_div.find_all("strong"):
                if "Value:" in strong.get_text():
                    val_text = strong.next_sibling
                    if val_text and isinstance(val_text, str):
                        imprv_value = clean_money(val_text.strip()) or 0

            imprv_id = f"{property_id}_imprv_{len(improvement['results'])}"

            imprv_record = {
                "pImprovementID": imprv_id,
                "imprvDescription": imprv_type or "1 FAM DWELLING",
                "imprvSpecificDescription": f"{state_cd} - {imprv_type}" if state_cd else imprv_type,
                "improvementValue": imprv_value,
                "stateCd": state_cd,
                "grossBuildingArea": "0",
                "livingArea": living_area,
                "pAccountID": property_id,
                "details": [],
            }

            # Parse detail rows from the table in this responsive div
            table = div.find("table")
            if table:
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) >= 5:
                        seg_type = clean_text(cells[0]) or ""
                        seg_desc = clean_text(cells[1]) or ""
                        class_cd = clean_text(cells[2]) or ""
                        year_built = clean_text(cells[3]) or ""
                        sqft_text = clean_text(cells[4]) or "0"

                        detail_record = {
                            "pImprovementID": imprv_id,
                            "pDetailID": f"{imprv_id}_seg_{len(imprv_record['details'])}",
                            "imprvDetailType": seg_type,
                            "createDt": "",
                            "detailTypeDescription": seg_desc,
                            "class": class_cd,
                            "units": 0,
                            "effYearBuilt": year_built,
                            "actualYearBuilt": year_built,
                            "area": sqft_text.replace(",", "").replace(".00", ""),
                            "primaryFeature": None,
                            "pAccountID": property_id,
                        }
                        imprv_record["details"].append(detail_record)

            improvement["results"].append(imprv_record)

    data["improvement"] = improvement

    # No parcel data available from FBCAD HTML
    data["parcel"] = None

    return data


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def fetch_property_detail(property_id, owner_id, year=DEFAULT_YEAR):
    """Fetch the full HTML property detail page from FBCAD."""
    url = COUNTY_CONFIG["detail_url_template"].format(
        property_id=property_id,
        owner_id=owner_id,
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
    parser = argparse.ArgumentParser(description="Scrape FBCAD property data by subdivision")
    parser.add_argument("--subdivision", required=True, help="Subdivision name (e.g. '5741-01 - Parks Edge Sec 1')")
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

    for batch, total in search_subdivision(args.subdivision, args.year, max_pages=args.max_pages):
        if not found_any and not batch:
            continue
        found_any = True

        # Initialize progress bars on first batch
        if pbar_props is None:
            total_pages = (total + 99) // 100  # ceil(total / page_size)
            if args.max_pages:
                total_pages = min(total_pages, args.max_pages)
                total = min(total, args.max_pages * 100)
            pbar_pages = tqdm(total=total_pages, desc="Search pages", unit="pg", position=0, dynamic_ncols=True)
            pbar_props = tqdm(total=total, desc="Properties  ", unit="prop", position=1, dynamic_ncols=True, colour="green")

        pbar_pages.update(1)

        # Filter batch by minimum value
        filtered_batch = [r for r in batch if (r.get("appraisedValue") or 0) >= args.min_value]

        # Split into already-scraped and needs-scraping
        to_scrape = []
        for result in filtered_batch:
            prop_id = result["propertyId"]
            prop_file = os.path.join(OUTPUT_DIR, prop_id, "data.json")
            if os.path.exists(prop_file):
                processed += 1
                log(f"[{processed}] Skipping {prop_id} - already exists.")
                pbar_props.update(1)
            else:
                to_scrape.append(result)

        # Fetch + parse in parallel
        def _process_one(result):
            """Fetch, parse, enrich, and save one property. Returns (prop_id, ok, info)."""
            prop_id = result["propertyId"]
            o_id = result.get("ownerId", "")

            html = fetch_property_detail(prop_id, o_id, args.year)
            if not html:
                return prop_id, False, "fetch failed"

            try:
                property_data = parse_property_html(html, prop_id, o_id)
            except Exception as e:
                return prop_id, False, str(e)

            # Guard against empty/broken HTML
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
                    gen["streetAddress"] = result.get("address")
                    gen["situsAddr"] = result.get("address")
                if not gen.get("name"):
                    gen["name"] = result.get("ownerName")
                if not gen.get("abstractSubdivision"):
                    gen["abstractSubdivision"] = result.get("subdivision")
                if not gen.get("geoID"):
                    gen["geoID"] = result.get("geoId")

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
                    prop_id = result["propertyId"]
                    addr = result.get("address", "N/A")
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
        print("No properties found. Check the subdivision name.")
        return

    if pbar_pages:
        pbar_pages.close()
    if pbar_props:
        pbar_props.close()
    print("\nFBCAD scraping routine finished successfully!")


if __name__ == "__main__":
    main()
