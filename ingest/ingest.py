#!/usr/bin/env python3
"""
Ingest Anna's Archive AAC metadata JSONL.zst files into PostgreSQL.

Usage:
    python ingest.py --source zlib3 --input /data/aac/zlib3_records.jsonl.zst
    python ingest.py --source libgenrs --input /data/aac/libgenrs*.jsonl.zst
"""

import argparse
import glob
import io
import os
import sys
import time
from pathlib import Path

import orjson
import psycopg
import zstandard

DB_URL = os.environ.get("DATABASE_URL", "postgresql://annas:annas@localhost:5432/annas")
BATCH_SIZE = 10_000
COLUMNS = [
    "source", "source_id", "md5", "title", "author", "publisher",
    "language", "year", "extension", "filesize", "pages", "series",
    "edition", "doi", "isbn", "description", "aacid", "date_added",
]


def extract_metadata(record: dict, source: str) -> dict | None:
    """Normalize an AAC record into the common schema."""
    meta = record.get("metadata", {})
    if not meta:
        return None

    # Extract MD5 — field name varies by collection
    md5 = (
        meta.get("md5_reported")
        or meta.get("md5")
        or meta.get("md5_hash")
        or ""
    ).lower().strip()

    if not md5 or len(md5) != 32:
        return None

    # Extract source ID — varies by collection
    source_id = str(
        meta.get("zlibrary_id")
        or meta.get("libgen_id")
        or meta.get("id")
        or meta.get("doi")
        or ""
    )

    # Year parsing
    year_raw = meta.get("year") or meta.get("year_best")
    year = None
    if year_raw:
        try:
            y = int(str(year_raw).strip()[:4])
            if 1000 <= y <= 2100:
                year = y
        except (ValueError, TypeError):
            pass

    # Filesize
    filesize = None
    for key in ("filesize_reported", "filesize", "filesize_best"):
        if meta.get(key):
            try:
                filesize = int(meta[key])
            except (ValueError, TypeError):
                pass
            break

    return {
        "source": source,
        "source_id": source_id or None,
        "md5": md5,
        "title": (meta.get("title") or meta.get("title_best") or "")[:4000] or None,
        "author": (meta.get("author") or meta.get("author_best") or "")[:2000] or None,
        "publisher": (meta.get("publisher") or "")[:1000] or None,
        "language": (meta.get("language") or meta.get("language_best") or "")[:100] or None,
        "year": year,
        "extension": (meta.get("extension") or meta.get("extension_best") or "")[:20] or None,
        "filesize": filesize,
        "pages": str(meta.get("pages") or "")[:50] or None,
        "series": (meta.get("series") or "")[:1000] or None,
        "edition": (meta.get("edition") or "")[:500] or None,
        "doi": (meta.get("doi") or "")[:200] or None,
        "isbn": extract_isbn(meta),
        "description": (meta.get("description") or "")[:10000] or None,
        "aacid": record.get("aacid"),
        "date_added": meta.get("date_added") or None,
    }


def extract_isbn(meta: dict) -> str | None:
    """Extract ISBN from various field formats."""
    isbn = meta.get("isbn") or meta.get("identifier_isbn")
    if not isbn:
        isbns = meta.get("isbns") or meta.get("isbn_multiple")
        if isbns and isinstance(isbns, list):
            isbn = isbns[0] if isbns else None
    if isbn:
        isbn = str(isbn).replace("-", "").strip()[:13]
        return isbn if isbn else None
    return None


def copy_batch(conn: psycopg.Connection, batch: list[dict]) -> int:
    """Bulk insert a batch using COPY."""
    buf = io.BytesIO()
    for row in batch:
        values = []
        for col in COLUMNS:
            v = row.get(col)
            if v is None:
                values.append("\\N")
            else:
                values.append(str(v).replace("\\", "\\\\").replace("\t", " ").replace("\n", " ").replace("\r", ""))
        buf.write(("\t".join(values) + "\n").encode("utf-8"))

    buf.seek(0)
    with conn.cursor() as cur:
        with cur.copy(f"COPY documents ({','.join(COLUMNS)}) FROM STDIN") as copy:
            copy.write(buf.read())
    conn.commit()
    return len(batch)


def stream_jsonl_zst(filepath: str):
    """Stream lines from a .jsonl.zst file."""
    dctx = zstandard.ZstdDecompressor()
    with open(filepath, "rb") as fh:
        with dctx.stream_reader(fh) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8", errors="replace")
            for line in text_stream:
                line = line.strip()
                if line:
                    yield line


def ingest_file(conn: psycopg.Connection, filepath: str, source: str) -> tuple[int, int]:
    """Ingest a single .jsonl.zst file. Returns (ingested, skipped)."""
    batch: list[dict] = []
    total_ingested = 0
    total_skipped = 0
    start = time.time()

    print(f"  Ingesting: {filepath}")

    for line in stream_jsonl_zst(filepath):
        try:
            record = orjson.loads(line)
        except Exception:
            total_skipped += 1
            continue

        row = extract_metadata(record, source)
        if row is None:
            total_skipped += 1
            continue

        batch.append(row)

        if len(batch) >= BATCH_SIZE:
            try:
                total_ingested += copy_batch(conn, batch)
            except Exception as e:
                # On COPY failure (e.g. duplicate aacid), fall back to individual inserts
                conn.rollback()
                for r in batch:
                    try:
                        insert_single(conn, r)
                        total_ingested += 1
                    except Exception:
                        total_skipped += 1
            batch = []

            if total_ingested % 100_000 == 0:
                elapsed = time.time() - start
                rate = total_ingested / elapsed if elapsed > 0 else 0
                print(f"    {total_ingested:>12,} ingested | {total_skipped:>8,} skipped | {rate:,.0f} rec/s")

    # Final batch
    if batch:
        try:
            total_ingested += copy_batch(conn, batch)
        except Exception:
            conn.rollback()
            for r in batch:
                try:
                    insert_single(conn, r)
                    total_ingested += 1
                except Exception:
                    total_skipped += 1

    elapsed = time.time() - start
    rate = total_ingested / elapsed if elapsed > 0 else 0
    print(f"    Done: {total_ingested:,} ingested, {total_skipped:,} skipped in {elapsed:.1f}s ({rate:,.0f} rec/s)")
    return total_ingested, total_skipped


def insert_single(conn: psycopg.Connection, row: dict):
    """Insert a single row, ignoring conflicts on aacid."""
    cols = ", ".join(COLUMNS)
    placeholders = ", ".join(f"%({c})s" for c in COLUMNS)
    conn.execute(
        f"INSERT INTO documents ({cols}) VALUES ({placeholders}) ON CONFLICT (aacid) DO NOTHING",
        row,
    )
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Ingest AAC metadata into PostgreSQL")
    parser.add_argument("--source", required=True, help="Source collection name (e.g. zlib3, libgenrs, scihub)")
    parser.add_argument("--input", required=True, help="Path to .jsonl.zst file(s) — supports glob patterns")
    parser.add_argument("--db", default=DB_URL, help="PostgreSQL connection URL")
    args = parser.parse_args()

    files = sorted(glob.glob(args.input))
    if not files:
        print(f"Error: no files match '{args.input}'", file=sys.stderr)
        sys.exit(1)

    print(f"Source: {args.source}")
    print(f"Files: {len(files)}")
    print(f"Database: {args.db.split('@')[-1] if '@' in args.db else args.db}")
    print()

    conn = psycopg.connect(args.db)

    grand_total = 0
    grand_skipped = 0
    for filepath in files:
        ingested, skipped = ingest_file(conn, filepath, args.source)
        grand_total += ingested
        grand_skipped += skipped

    print(f"\nAll done: {grand_total:,} total ingested, {grand_skipped:,} total skipped across {len(files)} file(s)")
    conn.close()


if __name__ == "__main__":
    main()
