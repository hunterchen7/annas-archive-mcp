# Anna's Archive MCP Server

## What this is
A self-hosted MCP server that provides search and download access to Anna's Archive metadata (books, papers, documents). The metadata index runs on a PostgreSQL database on an Olares box, exposed via Cloudflare Tunnel at `https://aa-mcp.hunterchen.ca`.

## MCP Tools Available
- **search** — query by title, author, DOI, ISBN, keywords. Returns metadata + MD5 hashes.
- **download** — takes an MD5 hash, returns a **direct download URL** (not a file). The URL is temporary.
- **stats** — shows total records and breakdown by source.

## Workflow: Finding and Downloading a Book/Paper
1. Use `search` to find what the user wants
2. Use `download` with the MD5 hash from search results to get a download URL
3. Use `curl -L -o <filename> '<url>'` via Bash to download the file to the user's local machine (e.g. ~/Downloads/)
4. Present the file path to the user

## Architecture
- **Server**: TypeScript MCP server (server/)
- **Database**: PostgreSQL with full-text search + trigram indexes
- **Ingestion**: Rust binary that streams AAC metadata .jsonl.zst files into Postgres (ingest/)
- **Downloader**: aria2c-based torrent downloader for AAC metadata collections (downloader/)
- **Tunnel**: Cloudflare named tunnel for external HTTPS access
- **Deploy target**: Olares box at `ssh olares-deploy` (restricted user, no sudo)

## Key Config
- AA API key is in MCP client headers (`X-Annas-Secret-Key`), never in server code
- Auth token for MCP endpoint is in `Authorization: Bearer` header
- Cloudflare tunnel token is in `.env` on the server
- Anna's Archive domains have built-in fallback (gl → gd → pk)

## Development
```bash
# Sync code to server
rsync -avz --exclude node_modules --exclude dist --exclude .git --exclude 'ingest/target' --exclude '.env' --exclude '.claude' . olares-deploy:/var/lib/annas-archive-mcp/

# Rebuild and restart
ssh olares-deploy "cd /var/lib/annas-archive-mcp && docker compose build mcp-server && docker compose up -d mcp-server"

# Check ingestion progress
ssh olares-deploy "docker logs <ingest-container> 2>&1 | tail -10"

# Check download progress
ssh olares-deploy "tail -20 /var/lib/annas-archive-mcp/download.log"
```
