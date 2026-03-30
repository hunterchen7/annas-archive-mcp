#!/bin/bash
set -euo pipefail

# Anna's Archive Metadata Downloader
# Downloads AAC metadata collections via BitTorrent using aria2c
# All collections download in parallel.

ANNAS_BASE_URL="${ANNAS_BASE_URL:-annas-archive.gl}"
OUTPUT_DIR="${OUTPUT_DIR:-/data/aac}"
COLLECTIONS="${COLLECTIONS:-zlib3_records,upload_records,ia2_records}"
SEED_TIME="${SEED_TIME:-0}"
MAX_CONNECTIONS="${MAX_CONNECTIONS:-16}"

echo "╔══════════════════════════════════════════════════╗"
echo "║     Anna's Archive Metadata Downloader          ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "  Collections: $COLLECTIONS"
echo "  Output dir:  $OUTPUT_DIR"
echo ""

mkdir -p "$OUTPUT_DIR"

# Fetch torrents.json with fallback domains
fetch_torrents() {
    for domain in "$ANNAS_BASE_URL" annas-archive.gl annas-archive.gd annas-archive.pk; do
        echo "  Fetching torrent index from $domain..." >&2
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
    echo "ERROR: Could not fetch torrents.json from any domain."
    exit 1
fi
echo "  OK - torrent index loaded"
echo ""

# If COLLECTIONS is "list", print available and exit
if [ "$COLLECTIONS" = "list" ]; then
    echo "Available metadata collections:"
    echo "───────────────────────────────────────────────────────────────"
    echo "$TORRENTS_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
meta = [t for t in data if t.get('is_metadata') and not t.get('obsolete')]
meta.sort(key=lambda t: -t.get('data_size', 0))
print(f'  {\"Collection\":<40} {\"Size\":>8}  {\"Seeders\":>7}')
print(f'  {\"─\"*40} {\"─\"*8}  {\"─\"*7}')
for t in meta:
    name = t.get('display_name', '')
    size_gb = t.get('data_size', 0) / (1024**3)
    seeders = t.get('seeders', 0)
    parts = name.split('__')
    collection = ''
    for p in parts:
        if '_records' in p or '_files' in p:
            collection = p.split('__')[0]
            break
    if not collection:
        collection = name.split('.')[0]
    print(f'  {collection:<40} {size_gb:>7.1f}G  {seeders:>5}')
"
    echo ""
    echo "Set COLLECTIONS=name1,name2,... in .env to download specific collections."
    exit 0
fi

echo "Resolving collections..."

# Write an aria2c input file with all magnets (parallel download)
INPUT_FILE=$(mktemp)
echo "$TORRENTS_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
collections = [c.strip() for c in sys.argv[1].split(',')]
meta = [t for t in data if t.get('is_metadata') and not t.get('obsolete')]
found = {}
for t in meta:
    name = t.get('display_name', '')
    for c in collections:
        if c in name and c not in found:
            size_gb = t.get('data_size', 0) / (1024**3)
            seeders = t.get('seeders', 0)
            found[c] = t
            print(f'  {c:<30} {size_gb:>7.1f} GB  {seeders:>3} seeders', file=sys.stderr)
            break

total_gb = sum(t.get('data_size',0) / (1024**3) for t in found.values())
print(f'', file=sys.stderr)
print(f'  Total: ~{total_gb:.1f} GB across {len(found)} collection(s)', file=sys.stderr)

# aria2c input file format: URI on one line, options on next lines prefixed with space
for c, t in found.items():
    print(t['magnet_link'])
    print(f'  out={t[\"display_name\"]}')
    print()

for c in collections:
    if c not in found:
        print(f'  WARNING: \"{c}\" not found', file=sys.stderr)
" "$COLLECTIONS" > "$INPUT_FILE" 2>&1

# Print the info messages (they went to the file via stderr redirect)
grep -v '^magnet:' "$INPUT_FILE" | grep -v '^ ' | grep -v '^$' || true

# Check we have at least one magnet
if ! grep -q '^magnet:' "$INPUT_FILE"; then
    echo "ERROR: No matching collections found."
    echo "Run with COLLECTIONS=list to see available collections."
    rm -f "$INPUT_FILE"
    exit 1
fi

echo ""
echo "Starting parallel downloads with aria2c..."
echo "(all collections download simultaneously)"
echo ""

START_TIME=$(date +%s)

aria2c \
    --dir="$OUTPUT_DIR" \
    --input-file="$INPUT_FILE" \
    --seed-time="$SEED_TIME" \
    --max-concurrent-downloads=10 \
    --max-connection-per-server="$MAX_CONNECTIONS" \
    --split=8 \
    --min-split-size=10M \
    --bt-tracker="udp://tracker.opentrackr.org:1337/announce,udp://tracker.openbittorrent.com:6969/announce,udp://open.stealth.si:80/announce,udp://tracker.torrent.eu.org:451/announce,udp://explodie.org:6969/announce" \
    --file-allocation=falloc \
    --console-log-level=notice \
    --summary-interval=10 \
    --bt-enable-lpd=true \
    --enable-dht=true \
    --enable-peer-exchange=true \
    --bt-max-peers=100 \
    --human-readable=true \
    --show-console-readout=true \
    || {
        echo ""
        echo "WARNING: aria2c exited with an error. Some downloads may have failed."
        echo ""
    }

rm -f "$INPUT_FILE"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║              All downloads complete              ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Files in $OUTPUT_DIR:"
ls -lh "$OUTPUT_DIR"
echo ""
TOTAL_ELAPSED=$(( $(date +%s) - START_TIME ))
echo "Total time: $((TOTAL_ELAPSED / 3600))h $((TOTAL_ELAPSED % 3600 / 60))m"
echo ""
echo "Next step: run ingestion with"
echo "  docker compose --profile ingest run --rm ingest --source <name> --input '/data/aac/*.zst'"
