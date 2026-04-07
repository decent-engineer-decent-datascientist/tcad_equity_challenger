#!/usr/bin/env bash
# Scrape all 19 Parks Edge subdivisions from FBCAD, then load into SQLite.

set -e

for i in $(seq 1 19); do
    echo "=== Scraping Parks Edge Sec $i ==="
    python pipeline/fbcad_scraper.py \
        --subdivision "5741-$(printf '%02d' $i) - Parks Edge Sec $i" \
        --min-value 100000 \
        --workers 4
    echo ""
done

echo "=== Loading into SQLite ==="
python pipeline/json_to_sqlite.py --county "Fort Bend"

echo "Done!"
