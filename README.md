# Anna's Archive MCP Server

A self-hosted [MCP](https://modelcontextprotocol.io) server that indexes Anna's Archive metadata into a local PostgreSQL database. Search books, papers, and documents by title, author, DOI, or ISBN with full-text search, diacritic-insensitive matching, and MD5 deduplication. Get direct download URLs via the Anna's Archive API.

This project only indexes publicly available metadata. It does not host or distribute any copyrighted content. Downloading files **requires your own [Anna's Archive membership](https://annas-archive.gl/account) API key**.

Works with Claude Code, Claude Desktop, claude.ai, and any MCP-compatible client.

```
                          ┌──────────────────────┐
                     ┌───▶│     PostgreSQL       │
┌──────────────┐     │    │  FTS + trigram index │
│  MCP Client  │     │    └──────────────────────┘
│              │─────┤
│  Claude Code │     │    ┌──────────────────────┐
│  Claude.ai   │◀────┤    │  Anna's Archive API  │
│  Any client  │     └───▶│  fast_download.json  │
└──────────────┘          └──────────────────────┘
                MCP Server
               (TypeScript)
```

## Tools

| Tool       | Description                                                                                                                        |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `search`   | Query by title, author, DOI, ISBN, or keywords. Returns metadata + MD5 hashes. Supports language and format filters.               |
| `download` | Get a direct download URL for a document by MD5 hash. **Requires your own Anna's Archive membership API key** (provided via client headers). |
| `stats`    | Index statistics — total records and breakdown by source collection.                                                               |

## Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/hunterchen7/annas-archive-mcp
cd annas-archive-mcp
cp .env.example .env
# Edit .env — set POSTGRES_PASSWORD

# 2. Start Postgres + MCP server
docker compose up -d

# 3. Download metadata collections (~98 GB for the default set)
docker compose --profile download run --rm download

# 4. Ingest into PostgreSQL
docker compose --profile ingest run --rm ingest \
  --source zlib3 --input '/data/aac/*zlib3_records*.zst' --workers 8

# 5. Verify
curl http://localhost:3001/health
```

## Connecting to MCP Clients

### Claude Code

```bash
# Without AA download key (search only)
claude mcp add --transport http annas-archive http://localhost:3001/mcp

# With AA download key (search + download)
claude mcp add --transport http annas-archive http://localhost:3001/mcp \
  --header "X-Annas-Secret-Key: YOUR_AA_API_KEY"
```

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "annas-archive": {
      "url": "http://localhost:3001/mcp",
      "headers": {
        "X-Annas-Secret-Key": "YOUR_AA_API_KEY"
      }
    }
  }
}
```

### claude.ai (Custom Connector)

For remote access, set up a Cloudflare Tunnel:

```bash
docker compose --profile tunnel up -d
```

Then in claude.ai: Settings -> Integrations -> Add custom connector:

```
URL: https://your-tunnel-url.com/mcp?aa_key=YOUR_AA_API_KEY
```

## Collections

The downloader fetches metadata from Anna's Archive via BitTorrent. Configure which collections to download via the `COLLECTIONS` env var:

```bash
# Default: books + papers (~98 GB)
COLLECTIONS=zlib3_records,upload_records,ia2_records,nexusstc_records

# List all available collections
COLLECTIONS=list docker compose --profile download run --rm download
```

| Collection          | Description                       | Size   |
| ------------------- | --------------------------------- | ------ |
| `zlib3_records`     | Z-Library books (22M+ records)    | 21 GB  |
| `upload_records`    | User uploads incl. LibGen content | 17 GB  |
| `ia2_records`       | Internet Archive books            | 2.7 GB |
| `nexusstc_records`  | Nexus/STC academic papers         | 56 GB  |
| `duxiu_records`     | Chinese academic library          | 35 GB  |
| `gbooks_records`    | Google Books metadata             | 9.5 GB |
| `goodreads_records` | Goodreads book metadata           | 7.7 GB |
| `ebscohost_records` | EBSCOhost academic database       | 1.4 GB |

See [torrents.md](torrents.md) for the full list of 50+ collections with magnet links.

## Architecture

```
annas-archive-mcp/
├── docker-compose.yml          # Full stack: Postgres, MCP server, ingest, download, tunnel
├── server/                     # TypeScript MCP server
│   ├── src/
│   │   ├── index.ts            # Entrypoint — stdio vs HTTP transport
│   │   ├── server.ts           # MCP tool definitions (search, download, stats)
│   │   ├── db.ts               # PostgreSQL queries (FTS, trigram, DOI/ISBN lookup)
│   │   └── download.ts         # Anna's Archive API client with domain fallback
│   └── Dockerfile              # Multi-stage Node.js build
├── ingest/                     # Rust ingestion binary
│   ├── src/main.rs             # Parallel workers, temp-table COPY, MD5 dedup
│   ├── schema.sql              # PostgreSQL schema with unaccent FTS
│   └── Dockerfile              # Multi-stage Rust build
└── downloader/                 # BitTorrent downloader
    ├── download.sh             # aria2c-based parallel torrent downloads
    └── Dockerfile
```

### Key Design Decisions

- **MD5 as primary key** — one row per unique file, deduplicating across all source collections
- **Metadata completeness scoring** — when duplicate MD5s are ingested from different sources, the record with more non-null fields wins
- **Unaccent FTS** — searching "Zizek" finds "Žižek"; diacritics are stripped at both index and query time
- **AND matching** — multi-word queries require all terms to match; trigram fallback only for single-word typo correction
- **Domain fallback** — Anna's Archive domains change frequently; the server tries `gl` → `gd` → `pk` automatically
- **Client-provided API key** — the AA membership key is sent via `X-Annas-Secret-Key` header, never stored on the server

## Configuration

### Environment Variables

| Variable                  | Description                                   | Default                                                     |
| ------------------------- | --------------------------------------------- | ----------------------------------------------------------- |
| `POSTGRES_PASSWORD`       | PostgreSQL password                           | `annas`                                                     |
| `RATE_LIMIT`              | Max requests per minute per IP                | `60`                                                        |
| `TRANSPORT`               | `http` or `stdio`                             | `http`                                                      |
| `COLLECTIONS`             | Comma-separated collection names to download  | `zlib3_records,upload_records,ia2_records,nexusstc_records` |
| `CLOUDFLARE_TUNNEL_TOKEN` | Named tunnel token for permanent external URL | (none)                                                      |
| `SEED_TIME`               | Seconds to seed after download                | `0`                                                         |

### PostgreSQL Tuning

The default Postgres settings are tuned for 16 GB RAM. For larger machines, adjust in `docker-compose.yml`:

| Setting                | 16 GB  | 32 GB  | 96 GB  |
| ---------------------- | ------ | ------ | ------ |
| `shared_buffers`       | 4 GB   | 8 GB   | 24 GB  |
| `effective_cache_size` | 8 GB   | 24 GB  | 72 GB  |
| `work_mem`             | 256 MB | 256 MB | 256 MB |
| `maintenance_work_mem` | 1 GB   | 1 GB   | 2 GB   |

## Ingestion

The Rust ingestion binary streams `.jsonl.zst` files, normalizes metadata across collection formats, and bulk-inserts via PostgreSQL COPY protocol with parallel workers.

```bash
# Ingest a single collection
docker compose --profile ingest run --rm ingest \
  --source zlib3 --input '/data/aac/*zlib3_records*.zst' --workers 8

# Ingest all downloaded collections
for src in zlib3 upload ia2 nexusstc duxiu gbooks goodreads; do
  docker compose --profile ingest run -d --rm --name "ingest-$src" ingest \
    --source "$src" --input "/data/aac/*${src}*.zst" --workers 4
done
```

Features:

- **Parallel workers** (default 8) with independent DB connections
- **Temp table + INSERT ON CONFLICT** — COPY into unindexed temp table, then merge with dedup
- **Metadata merging** — duplicate MD5s keep the record with the most complete metadata
- **Skips `deleted_as_duplicate`** records flagged by Anna's Archive
- **Filename-derived titles** as fallback for collections without title metadata

## Resource Requirements

| Resource           | Books only (~30M) | Full index (~50M+) |
| ------------------ | ----------------- | ------------------ |
| Download size      | ~40 GB            | ~150 GB            |
| PostgreSQL on disk | ~20 GB            | ~80 GB             |
| RAM (recommended)  | 8 GB              | 16+ GB             |
| Ingestion time     | ~15 min           | ~1 hour            |

## Disclaimer

This project provides a search interface over publicly available metadata published by Anna's Archive. It does **not** host, distribute, or store any copyrighted content.

- **Metadata only** — the database contains bibliographic information (titles, authors, ISBNs, etc.), not the actual files.
- **Downloads** require the user to provide their own Anna's Archive membership API key. This project does not provide, share, or store API keys.
- **No affiliation** — this project is not affiliated with, endorsed by, or connected to Anna's Archive.
- **User responsibility** — users are solely responsible for how they use this tool and for complying with all applicable laws in their jurisdiction.
- **No warranty** — this software is provided as-is with no guarantees of any kind.

## License

MIT
