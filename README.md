# Anna's Archive MCP Server

A self-hosted [MCP](https://modelcontextprotocol.io) server that indexes Anna's Archive metadata into a local PostgreSQL database. Search books, papers, and documents by title, author, DOI, or ISBN with full-text search, diacritic-insensitive matching, and MD5 deduplication. Get direct download URLs via the Anna's Archive API.

This project only indexes publicly available metadata. It does not host or distribute any copyrighted content. Access to the index — **both searching and downloading** — requires your own [Anna's Archive membership](https://annas-archive.gl/account) secret key.

Works with ChatGPT, Codex, Claude Code, Claude Desktop, claude.ai, and other
MCP-compatible clients. Remote clients can use the built-in OAuth linking flow
without creating an account for this server.

```
                          ┌──────────────────────┐
                     ┌───▶│     PostgreSQL       │
                     │    │  FTS + trigram index │
┌──────────────┐     │    └──────────────────────┘
│  MCP Client  │     │
│              │─────┤    ┌──────────────────────┐
│  Claude Code │     │    │  Anna's Archive      │
│  Claude.ai   │◀────┼───▶│  POST /account/      │ ← key validation
│  Any client  │     │    │  fast_download.json  │ ← downloads
└──────────────┘     │    └──────────────────────┘
       (OAuth or legacy header)
                MCP Server
               (TypeScript)
```

## Tools

| Tool       | Description                                                                                                                                                              |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `search`   | Granular search with dedicated fields for title, author, year range, publisher, ISBN, DOI, language, and format. All combinable. **Requires a validated AA secret key.** |
| `download` | Get a fast download URL for a document by MD5 hash. **Requires your own Anna's Archive membership secret key** (provided via client headers).                            |
| `read`     | Extract and return text content from a document by MD5 hash. Supports PDF, EPUB, DJVU, MOBI, and more. Results are cached. **Requires a validated AA secret key.**       |
| `stats`    | Index statistics — total records and breakdown by source collection. (No key required.)                                                                                  |

### Search Parameters

All parameters are optional and combinable. At least one of `query`, `title`, `author`, `isbn`, or `doi` is required.

| Parameter   | Type   | Description                                                |
| ----------- | ------ | ---------------------------------------------------------- |
| `query`     | string | General full-text search across title, author, publisher   |
| `title`     | string | Search within titles only                                  |
| `author`    | string | Search within authors only                                 |
| `year_from` | number | Minimum publication year (inclusive)                       |
| `year_to`   | number | Maximum publication year (inclusive)                       |
| `publisher` | string | Search within publishers only                              |
| `isbn`      | string | Exact ISBN lookup (10 or 13 digits)                        |
| `doi`       | string | Exact DOI lookup                                           |
| `language`  | string | Filter by language (e.g. `english`, `chinese`, `french`)   |
| `format`    | string | Filter by file format (e.g. `pdf`, `epub`, `djvu`, `mobi`) |
| `limit`     | number | Max results (default 10, max 50)                           |

## Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/hunterchen7/annas-archive-mcp
cd annas-archive-mcp
cp .env.example .env
# Edit .env — POSTGRES_PASSWORD is intentionally blank and must be set

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

### Upgrading an existing installation

PostgreSQL only uses `POSTGRES_PASSWORD` when it creates a new data volume.
Before starting this version with an existing `pgdata` volume, recreate only
PostgreSQL with the new environment and then rotate the existing `annas` role:

```bash
docker compose up -d --no-deps --force-recreate --wait postgres
docker compose exec -u postgres postgres \
  psql --username annas --dbname postgres \
  --file /docker-entrypoint-initdb.d/02-sync-password.sql
docker compose exec postgres \
  psql --set ON_ERROR_STOP=1 --username annas --dbname annas \
  --file /docker-entrypoint-initdb.d/03-oauth-schema.sql
```

Older releases also wrote the optional disk cache as root. If you previously
used `CACHE_MODE=disk`, migrate that named volume once:

```bash
docker compose run --rm --no-deps --user root --cap-add CHOWN \
  --entrypoint chown mcp-server -R bun:bun /data/cache
```

Then run `docker compose up -d`. The `/health` endpoint now verifies the
database connection, so a password mismatch reports HTTP 503 instead of a
misleading healthy response.

## Connecting to MCP Clients

### ChatGPT, Codex, and claude.ai (OAuth)

For a public HTTPS deployment, set these values once on the server:

```dotenv
PUBLIC_BASE_URL=https://aa-mcp.example.com
# Generate once with: openssl rand -base64 32
OAUTH_KEY_ENCRYPTION_KEY=YOUR_32_BYTE_BASE64_MASTER_KEY
```

Apply the OAuth schema and restart:

```bash
docker compose up -d --no-deps --force-recreate --wait postgres
docker compose exec postgres psql --username annas --dbname annas \
  --set ON_ERROR_STOP=1 --file /docker-entrypoint-initdb.d/03-oauth-schema.sql
docker compose up -d --build mcp-server
```

Register this connector URL:

```text
https://aa-mcp.example.com/mcp
```

Leave OAuth Client ID and OAuth Client Secret blank. The server supports
OAuth dynamic client registration and PKCE, which current ChatGPT/Codex and
Claude custom connectors can negotiate. A browser page then asks for the
Anna's Archive membership key and displays the complete plaintext/encryption
disclosure before linking. No connector-server signup is required.

The default retention choice is **Until I disconnect**. Its encrypted key has
no automatic expiry, while one-hour access tokens refresh automatically. An
optional **One-hour session** stores the encrypted key only for that session
and issues no refresh token.

### Legacy header mode

OAuth is optional. Local clients and clients with secure custom-header support
can still send the key on every request:

```bash
claude mcp add --transport http annas-archive http://localhost:3001/mcp \
  --header "X-Annas-Secret-Key: YOUR_AA_SECRET_KEY"
```

For Claude Desktop, add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "annas-archive": {
      "url": "http://localhost:3001/mcp",
      "headers": {
        "X-Annas-Secret-Key": "YOUR_AA_SECRET_KEY"
      }
    }
  }
}
```

For remote access, expose the service only through HTTPS, for example with the
included Cloudflare Tunnel:

```bash
docker compose --profile tunnel up -d
```

Secret keys in inbound connector URL query parameters are always rejected
because those URLs are commonly retained by browsers, proxies, analytics, and
request logs. Anna's Archive's fast-download API separately requires the key
in its outbound HTTPS query string during download/read; see the disclosure
below.

### Key handling and plaintext disclosure

The linking page at `/oauth` gives users this disclosure before accepting a
key:

- **Initial link:** the key is plaintext in server process memory while the
  server sends it over HTTPS to Anna's Archive's account endpoint for
  live validation. It is encrypted only after successful validation. Until the
  client completes the OAuth code exchange, that encrypted record is
  provisional and expires with the five-minute authorization code.
- **Client identity:** dynamic client names are self-supplied and unverified.
  The portal displays the exact callback origin and requires the user to
  confirm that destination before it accepts a key.
- **Search:** OAuth metadata searches use the previously validated connection.
  They do not decrypt the stored key.
- **Download and read:** the key is decrypted in process memory for the active
  request because Anna's Archive requires the real key. Its fast-download API
  requires that key in an outbound HTTPS query parameter, where Anna's Archive
  and any egress proxy/tracer can observe it. Operators must redact or disable
  outbound query-string logging.
- **Periodic validation:** persistent connections decrypt and revalidate the
  key at most once every 24 hours during automatic token refresh.
- **At rest:** PostgreSQL stores AES-256-GCM ciphertext, a random nonce,
  authentication tag, and a keyed HMAC fingerprint. The master key is kept in
  server configuration, outside PostgreSQL.
- **Important limit:** encryption at rest does not stop the running application
  or an operator controlling both the application and its master key from
  accessing the key. JavaScript strings also cannot be reliably zeroed from
  process memory.
- **Deletion:** OAuth revocation/disconnect deletes the encrypted key.
  Persistent connections otherwise have no automatic expiry. Session
  access expires after one hour; its database row is deleted by hourly cleanup
  (which also runs at startup), normally within the following hour.
- **Deletion limits:** active database rows are deleted, but encrypted
  ciphertext can remain in PostgreSQL WAL, replicas, snapshots, and backups
  until the operator's retention windows expire. A global master key can still
  decrypt those retained copies, so operators must set and disclose backup
  retention accordingly.
- **Legacy header:** the plaintext key is request-scoped and is not written to
  PostgreSQL. Only a short-lived process-local HMAC validation verdict is
  cached.

The application does not place membership keys in inbound portal/callback URLs,
cookies, OAuth tokens, browser storage, or application logs. The outbound
fast-download query parameter above is the explicit exception. Operators must
configure TLS termination, egress tracing, error reporting, database backups,
and host access consistently with that policy. These controls are inspectable
implementation and operational guarantees, not zero-knowledge proof.

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
│   │   ├── index.ts            # Entrypoint — stdio vs HTTP transport, REST + /mcp routes
│   │   ├── server.ts           # MCP tool definitions (search, download, read, stats)
│   │   ├── db.ts               # PostgreSQL queries (FTS, trigram, DOI/ISBN lookup)
│   │   ├── download.ts         # Anna's Archive API client with domain fallback
│   │   ├── reader.ts           # Text extraction with format detection and LRU cache
│   │   ├── auth.ts             # AA key validation with a process-local HMAC verdict cache
│   │   ├── oauthProvider.ts    # OAuth DCR, PKCE, token rotation, encrypted key records
│   │   ├── oauthPortal.ts      # Key-link form and plaintext/encryption disclosure
│   │   ├── oauthCrypto.ts      # AES-256-GCM envelopes and keyed fingerprints
│   │   └── cache.ts            # LRU file cache for downloaded files and extracted text
│   └── Dockerfile              # Multi-stage Bun build with calibre, poppler, djvulibre
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
- **Granular search** — dedicated title, author, year range, publisher, ISBN, and DOI parameters with per-field GIN indexes
- **AND matching with fallbacks** — multi-word queries require all terms to match; OR fallback for multi-word, trigram for single-word typo correction
- **Domain fallback** — Anna's Archive domains change frequently; the server tries `gl` → `gd` → `pk` automatically
- **Two explicit key modes** — OAuth validates once and persists only an
  AES-256-GCM encrypted key until disconnect (or for the optional one-hour
  session). The legacy header mode validates per client request and never
  writes the key to PostgreSQL. Both modes remove credential headers from the
  generic request object and avoid intentional application logging.
- **Decrypt only when needed** — an OAuth search checks its validated
  connection without decrypting. Download/read requests and at-most-daily
  membership revalidation decrypt in request-scoped process memory.
- **No misleading cryptographic promise** — the disclosure plainly states
  that an operator controlling the application and encryption secret can
  decrypt stored keys. This design minimizes storage exposure; it is not
  zero-knowledge or remote attestation.

## Configuration

### Environment Variables

| Variable                  | Description                                                                      | Default                                                     |
| ------------------------- | -------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| `POSTGRES_PASSWORD`       | Required PostgreSQL password (use a long, random URL-safe value)                 | (none)                                                      |
| `MCP_PORT`                | Loopback-only host port for MCP                                                  | `3001`                                                      |
| `POSTGRES_PORT`           | Loopback-only host port for PostgreSQL                                           | `5432`                                                      |
| `RATE_LIMIT`              | Max requests per minute per IP                                                   | `60`                                                        |
| `TRUST_PROXY`             | Express trusted proxy ranges (default suits the included Docker tunnel)          | `loopback, linklocal, uniquelocal`                          |
| `TRANSPORT`               | `http` or `stdio`                                                                | `http`                                                      |
| `PUBLIC_BASE_URL`          | Public HTTPS origin; required with `OAUTH_KEY_ENCRYPTION_KEY` to enable OAuth    | (none)                                                      |
| `OAUTH_KEY_ENCRYPTION_KEY` | Stable base64-encoded 32-byte master key for linked AA keys                     | (none)                                                      |
| `COLLECTIONS`             | Comma-separated collection names to download                                     | `zlib3_records,upload_records,ia2_records,nexusstc_records` |
| `CLOUDFLARE_TUNNEL_TOKEN` | Named tunnel token for permanent external URL                                    | (none)                                                      |
| `CACHE_MODE`              | `memory` (nothing on disk) or `disk` (LRU file cache)                            | `memory`                                                    |
| `MCP_SHM_SIZE`            | `/dev/shm` size for the mcp-server container (memory-mode extractors write here) | `256m`                                                      |
| `MCP_MEMORY_LIMIT`        | Container memory limit for the MCP server and native parsers                    | `2g`                                                        |
| `MCP_CPU_LIMIT`           | Container CPU limit for the MCP server and native parsers                       | `2.0`                                                       |
| `MAX_DOWNLOAD_MB`         | Maximum document download size (hard-capped at 1024 MB)                          | `128`                                                       |
| `SEED_TIME`               | Seconds to seed after download                                                   | `0`                                                         |

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

## Why local index instead of scraping?

This project indexes metadata locally rather than scraping Anna's Archive at query time. A few reasons:

- **robots.txt** — Anna's Archive [disallows](https://annas-archive.gl/robots.txt) automated access to `/search`. We respect that.
- **Speed** — local PostgreSQL full-text search returns results in milliseconds, vs seconds for a network round-trip.
- **Reliability** — no dependency on Anna's Archive being up or reachable at query time. Domains change frequently.
- **Rate limiting** — scraping at scale would put unnecessary load on their servers.

Downloads use the official `fast_download.json` API, which is the sanctioned way to interact programmatically.

## Disclaimer

This project provides a search interface over publicly available metadata published by Anna's Archive. It does **not** serve a permanent public document collection.

- **Cache behavior is explicit.** The default `CACHE_MODE=memory` does not persist downloaded documents. `CACHE_MODE=disk` intentionally stores bounded document and extracted-text caches in the `cache` volume; operators must account for that content in their storage, backup, and retention policies.
- **The `download` tool does not proxy document bytes.** It returns a short-lived URL from AA's `fast_download.json` API. The `read` tool does download a document to bounded temporary storage, runs text extraction, and removes the temporary file unless disk caching was explicitly enabled.
- **Access requires an Anna's Archive membership** — users supply their own AA
  key. In OAuth mode, the validated key is encrypted at rest until disconnect
  (or the one-hour session expires); in legacy-header mode it is not persisted.
  The server necessarily handles plaintext during validation, download/read,
  and periodic revalidation, as described above.
- **No scraping** — search is performed against a local index built from publicly available metadata dumps. We do not scrape or crawl Anna's Archive, in accordance with their robots.txt.
- **No affiliation** — this project is not affiliated with, endorsed by, or connected to Anna's Archive.
- **User responsibility** — users are solely responsible for how they use this tool and for complying with all applicable laws in their jurisdiction.
- **No warranty** — this software is provided as-is with no guarantees of any kind.

## License

MIT
