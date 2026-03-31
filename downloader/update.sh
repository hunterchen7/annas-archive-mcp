#!/bin/bash
set -euo pipefail

# Anna's Archive Metadata Updater
# Checks for newer versions of collections already downloaded,
# downloads new ones, and re-ingests them.

ANNAS_BASE_URL="${ANNAS_BASE_URL:-annas-archive.gl}"
OUTPUT_DIR="${OUTPUT_DIR:-/data/aac}"
DATABASE_URL="${DATABASE_URL:-postgresql://annas:annas@postgres:5432/annas}"
WORKERS="${WORKERS:-8}"

echo "╔══════════════════════════════════════════════════╗"
echo "║      Anna's Archive Metadata Updater            ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "  Checking for updated metadata collections..."
echo ""

# Fetch current torrents
fetch_torrents() {
    for domain in "$ANNAS_BASE_URL" annas-archive.gl annas-archive.gd annas-archive.pk; do
        local result
        result=$(curl -sL --connect-timeout 10 "https://$domain/dyn/torrents.json" 2>/dev/null || true)
        if [ -n "$result" ] && echo "$result" | python3 -c "import json,sys; json.load(sys.stdin)" 2>/dev/null; then
            echo "$result"
            return 0
        fi
    done
    return 1
}

TORRENTS_JSON=$(fetch_torrents)
if [ -z "$TORRENTS_JSON" ]; then
    echo "ERROR: Could not fetch torrents.json"
    exit 1
fi

# Find which collections we already have by checking existing files
EXISTING=$(find "$OUTPUT_DIR" -name "*.zst" -not -name "*.aria2" -exec basename {} \; 2>/dev/null | sort)

if [ -z "$EXISTING" ]; then
    echo "  No existing collections found in $OUTPUT_DIR"
    echo "  Run the downloader first to get initial data."
    exit 0
fi

echo "  Existing collections:"
echo "$EXISTING" | while read f; do echo "    $f"; done
echo ""

# Compare with latest available — find newer versions of same collection types
UPDATES=$(echo "$TORRENTS_JSON" | python3 -c "
import json, sys, os, re

data = json.load(sys.stdin)
output_dir = '$OUTPUT_DIR'
meta = [t for t in data if t.get('is_metadata') and not t.get('obsolete')]

# Parse existing files to get collection names and their date ranges
existing = {}
for f in os.listdir(output_dir):
    if not f.endswith('.zst') or f.endswith('.aria2'):
        continue
    # Extract collection name: annas_archive_meta__aacid__COLLECTION__dates.jsonl.seekable.zst
    m = re.match(r'annas_archive_meta__aacid__(.+?)__(\d{8}T\d{6}Z)--(\d{8}T\d{6}Z)', f)
    if m:
        collection = m.group(1)
        end_date = m.group(3)
        existing[collection] = {'file': f, 'end_date': end_date}

if not existing:
    print('NO_EXISTING', file=sys.stderr)
    sys.exit(0)

# Find newer versions
updates = []
for t in meta:
    name = t.get('display_name', '')
    for collection, info in existing.items():
        if collection in name:
            m = re.match(r'annas_archive_meta__aacid__.+?__(\d{8}T\d{6}Z)--(\d{8}T\d{6}Z)', name)
            if m:
                new_end = m.group(2)
                if new_end > info['end_date']:
                    size_gb = t.get('data_size', 0) / (1024**3)
                    print(f'UPDATE\t{collection}\t{info[\"end_date\"]}\t{new_end}\t{size_gb:.1f}\t{t[\"magnet_link\"]}')
                    updates.append(collection)
            break

if not updates:
    print('NO_UPDATES', file=sys.stderr)
" 2>&1)

if echo "$UPDATES" | grep -q "NO_UPDATES"; then
    echo "  All collections are up to date!"
    exit 0
fi

if echo "$UPDATES" | grep -q "NO_EXISTING"; then
    echo "  Could not parse existing collection files."
    exit 1
fi

# Show what needs updating
echo "  Updates available:"
echo "$UPDATES" | grep "^UPDATE" | while IFS=$'\t' read _ collection old_date new_date size magnet; do
    echo "    $collection: $old_date → $new_date ($size GB)"
done
echo ""

# Download updates
echo "  Downloading updates..."
INPUT_FILE=$(mktemp)
echo "$UPDATES" | grep "^UPDATE" | while IFS=$'\t' read _ collection old_date new_date size magnet; do
    echo "$magnet"
done > "$INPUT_FILE"

aria2c \
    --dir="$OUTPUT_DIR" \
    --input-file="$INPUT_FILE" \
    --seed-time=0 \
    --max-concurrent-downloads=5 \
    --max-connection-per-server=16 \
    --split=8 \
    --bt-tracker="udp://tracker.opentrackr.org:1337/announce,udp://tracker.openbittorrent.com:6969/announce" \
    --file-allocation=falloc \
    --console-log-level=notice \
    --summary-interval=30 \
    --enable-dht=true \
    --enable-peer-exchange=true \
    --human-readable=true \
    || echo "  Some downloads may have failed"

rm -f "$INPUT_FILE"

echo ""
echo "  Downloads complete. Starting re-ingestion..."
echo ""

# Re-ingest updated collections
echo "$UPDATES" | grep "^UPDATE" | while IFS=$'\t' read _ collection old_date new_date size magnet; do
    echo "  Ingesting $collection..."
    # The ingest binary uses UPSERT so re-ingesting is safe
    annas-ingest --source "$collection" --input "$OUTPUT_DIR/*${collection}*.zst" --db "$DATABASE_URL" --workers "$WORKERS" || {
        echo "  WARNING: Ingestion of $collection failed"
    }
done

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║              Update complete                     ║"
echo "╚══════════════════════════════════════════════════╝"
