#!/bin/bash
set -euo pipefail

# Anna's Archive Monthly Update Script
# Checks for newer metadata dumps, downloads them, and re-ingests.
# Run via cron: 0 3 1 * * /path/to/update.sh >> /var/log/annas-update.log 2>&1

COMPOSE_DIR="${COMPOSE_DIR:-/var/lib/annas-archive-mcp}"
cd "$COMPOSE_DIR"

echo "$(date '+%Y-%m-%d %H:%M:%S') === Starting metadata update ==="

# Fetch latest torrent list
TORRENTS_JSON=$(curl -sL --connect-timeout 10 "https://annas-archive.gl/dyn/torrents.json" 2>/dev/null || \
                curl -sL --connect-timeout 10 "https://annas-archive.gd/dyn/torrents.json" 2>/dev/null || \
                curl -sL --connect-timeout 10 "https://annas-archive.pk/dyn/torrents.json" 2>/dev/null || true)

if [ -z "$TORRENTS_JSON" ]; then
    echo "ERROR: Could not fetch torrents.json from any domain"
    exit 1
fi

# Get the Docker volume mount path for aac-data
AAC_DIR=$(docker volume inspect annas-archive-mcp_aac-data --format '{{.Mountpoint}}')

# Find collections that have newer versions available
UPDATES=$(echo "$TORRENTS_JSON" | python3 -c "
import json, sys, os, re

data = json.load(sys.stdin)
aac_dir = '$AAC_DIR'
meta = [t for t in data if t.get('is_metadata') and not t.get('obsolete')]

existing = {}
for f in os.listdir(aac_dir):
    if not f.endswith('.zst') or f.endswith('.aria2'):
        continue
    m = re.match(r'annas_archive_meta__aacid__(.+?)__(\d{8}T\d{6}Z)--(\d{8}T\d{6}Z)', f)
    if m:
        existing[m.group(1)] = {'file': f, 'end_date': m.group(3)}

updates = []
for t in meta:
    name = t.get('display_name', '')
    for coll, info in existing.items():
        if coll in name:
            m = re.match(r'annas_archive_meta__aacid__.+?__(\d{8}T\d{6}Z)--(\d{8}T\d{6}Z)', name)
            if m and m.group(2) > info['end_date']:
                size_gb = t.get('data_size', 0) / (1024**3)
                updates.append(coll)
                print(f'{coll}')
            break

if not updates:
    sys.exit(0)
" 2>/dev/null)

if [ -z "$UPDATES" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') All collections up to date."
    exit 0
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') Updates available for: $UPDATES"

# Download updated collections using the download service
COLLECTIONS=$(echo "$UPDATES" | tr '\n' ',' | sed 's/,$//')
echo "$(date '+%Y-%m-%d %H:%M:%S') Downloading: $COLLECTIONS"

COLLECTIONS="$COLLECTIONS" docker compose --profile download run --rm download

# Re-ingest each updated collection
for coll in $UPDATES; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') Ingesting: $coll"
    docker compose --profile ingest run --rm ingest \
        --source "$coll" --input "/data/aac/*${coll}*.zst" --workers 8
done

# Vacuum analyze after bulk updates
echo "$(date '+%Y-%m-%d %H:%M:%S') Running VACUUM ANALYZE..."
docker compose exec -T postgres psql -U annas -c "VACUUM ANALYZE documents;"

echo "$(date '+%Y-%m-%d %H:%M:%S') === Update complete ==="
