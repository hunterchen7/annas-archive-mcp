use clap::Parser;
use futures_util::SinkExt;
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};

const BATCH_SIZE: usize = 10_000;
const COLUMNS: &[&str] = &[
    "source",
    "source_id",
    "md5",
    "title",
    "author",
    "publisher",
    "language",
    "year",
    "extension",
    "filesize",
    "pages",
    "series",
    "edition",
    "doi",
    "isbn",
    "description",
    "aacid",
    "date_added",
];

#[derive(Parser)]
#[command(about = "Ingest Anna's Archive AAC metadata into PostgreSQL")]
struct Args {
    #[arg(long)]
    source: String,

    #[arg(long)]
    input: String,

    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "postgresql://annas:annas@localhost:5432/annas"
    )]
    db: String,

    #[arg(long, default_value = "8", help = "Number of parallel DB workers")]
    workers: usize,
}

struct Row {
    source: String,
    source_id: Option<String>,
    md5: String,
    title: Option<String>,
    author: Option<String>,
    publisher: Option<String>,
    language: Option<String>,
    year: Option<i16>,
    extension: Option<String>,
    filesize: Option<i64>,
    pages: Option<String>,
    series: Option<String>,
    edition: Option<String>,
    doi: Option<String>,
    isbn: Option<String>,
    description: Option<String>,
    aacid: Option<String>,
    date_added: Option<String>,
}

fn truncate(s: &str, max: usize) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.len() <= max {
        return Some(trimmed.to_string());
    }
    let end = trimmed
        .char_indices()
        .take_while(|(i, _)| *i < max)
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(max);
    Some(trimmed[..end].to_string())
}

fn get_str<'a>(meta: &'a Value, keys: &[&str]) -> Option<&'a str> {
    for key in keys {
        if let Some(Value::String(s)) = meta.get(*key) {
            if !s.is_empty() {
                return Some(s);
            }
        }
    }
    None
}

fn extract_isbn(meta: &Value) -> Option<String> {
    if let Some(isbn) = get_str(meta, &["isbn", "identifier_isbn"]) {
        let cleaned: String = isbn.replace('-', "").trim().chars().take(13).collect();
        if !cleaned.is_empty() {
            return Some(cleaned);
        }
    }
    for key in &["isbns", "isbn_multiple"] {
        if let Some(Value::Array(arr)) = meta.get(*key) {
            if let Some(Value::String(s)) = arr.first() {
                let cleaned: String = s.replace('-', "").trim().chars().take(13).collect();
                if !cleaned.is_empty() {
                    return Some(cleaned);
                }
            }
        }
    }
    None
}

/// Extract a human-readable title from a filename like
/// "vine-deloria-jr-custer-died-for-your-sins-an-indian-manifesto.pdf"
fn title_from_filename(meta: &Value) -> Option<String> {
    let filepath = get_str(meta, &["filename", "filepath"])?;
    // Get just the filename, strip path prefix and extension
    let name = filepath.rsplit('/').next().unwrap_or(filepath);
    let name = name.rsplit('.').last().unwrap_or(name);
    if name.is_empty() || name.starts_with("part_") {
        return None;
    }
    // Replace hyphens and underscores with spaces, title case
    let title: String = name
        .replace('-', " ")
        .replace('_', " ")
        .split_whitespace()
        .map(|w| {
            let mut chars = w.chars();
            match chars.next() {
                Some(c) => {
                    let upper: String = c.to_uppercase().collect();
                    upper + chars.as_str()
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    if title.len() < 3 {
        return None;
    }
    truncate(&title, 4000)
}

fn extract_metadata(record: &Value, source: &str) -> Option<Row> {
    let meta = record.get("metadata")?;

    // Skip records flagged as duplicates by Anna's Archive
    if let Some(Value::Bool(true)) = meta.get("deleted_as_duplicate") {
        return None;
    }

    let md5_raw = get_str(meta, &["md5_reported", "md5", "md5_hash"])?;
    let md5: String = md5_raw.trim().to_lowercase();
    if md5.len() != 32 || !md5.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }

    let source_id = meta
        .get("zlibrary_id")
        .or_else(|| meta.get("libgen_id"))
        .or_else(|| meta.get("id"))
        .or_else(|| meta.get("primary_id"))
        .or_else(|| meta.get("doi"))
        .and_then(|v| match v {
            Value::String(s) if !s.is_empty() => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        });

    let year = meta
        .get("year")
        .or_else(|| meta.get("year_best"))
        .and_then(|v| {
            let s = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                _ => return None,
            };
            let t = s.trim();
            if t.len() >= 4 {
                t[..4]
                    .parse::<i16>()
                    .ok()
                    .filter(|&y| (1000..=2100).contains(&y))
            } else {
                None
            }
        });

    let filesize = ["filesize_reported", "filesize", "filesize_best"]
        .iter()
        .find_map(|key| {
            meta.get(*key).and_then(|v| match v {
                Value::Number(n) => n.as_i64(),
                Value::String(s) => s.parse::<i64>().ok(),
                _ => None,
            })
        });

    let aacid = record
        .get("aacid")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Title: try metadata fields first, fall back to filename
    let title = get_str(meta, &["title", "title_best"])
        .and_then(|s| truncate(s, 4000))
        .or_else(|| title_from_filename(meta));

    // Extension: try metadata fields, fall back to file_type
    let extension = get_str(meta, &["extension", "extension_best"])
        .or_else(|| get_str(meta, &["file_type"]))
        .and_then(|s| truncate(s, 20));

    // Pages: try metadata fields, fall back to total_pages
    let pages = meta.get("pages")
        .and_then(|v| match v {
            Value::String(s) if !s.is_empty() => truncate(s, 50),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        })
        .or_else(|| meta.get("total_pages").and_then(|v| match v {
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        }));

    Some(Row {
        source: source.to_string(),
        source_id,
        md5,
        title,
        author: get_str(meta, &["author", "author_best"]).and_then(|s| truncate(s, 2000)),
        publisher: get_str(meta, &["publisher"]).and_then(|s| truncate(s, 1000)),
        language: get_str(meta, &["language", "language_best"]).and_then(|s| truncate(s, 100)),
        year,
        extension,
        filesize,
        pages,
        series: get_str(meta, &["series"]).and_then(|s| truncate(s, 1000)),
        edition: get_str(meta, &["edition"]).and_then(|s| truncate(s, 500)),
        doi: get_str(meta, &["doi"]).and_then(|s| truncate(s, 200)),
        isbn: extract_isbn(meta),
        description: get_str(meta, &["description"]).and_then(|s| truncate(s, 10000)),
        aacid,
        date_added: get_str(meta, &["date_added"]).map(|s| s.to_string()),
    })
}

fn escape_copy(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', " ")
        .replace('\n', " ")
        .replace('\r', "")
}

fn opt(val: Option<&str>) -> String {
    match val {
        Some(s) => escape_copy(s),
        None => "\\N".to_string(),
    }
}

fn row_to_copy_line(row: &Row) -> String {
    [
        escape_copy(&row.source),
        opt(row.source_id.as_deref()),
        escape_copy(&row.md5),
        opt(row.title.as_deref()),
        opt(row.author.as_deref()),
        opt(row.publisher.as_deref()),
        opt(row.language.as_deref()),
        row.year.map_or("\\N".into(), |y| y.to_string()),
        opt(row.extension.as_deref()),
        row.filesize.map_or("\\N".into(), |f| f.to_string()),
        opt(row.pages.as_deref()),
        opt(row.series.as_deref()),
        opt(row.edition.as_deref()),
        opt(row.doi.as_deref()),
        opt(row.isbn.as_deref()),
        opt(row.description.as_deref()),
        opt(row.aacid.as_deref()),
        opt(row.date_added.as_deref()),
    ]
    .join("\t")
}

/// Insert a batch using a temp table + INSERT ... ON CONFLICT to handle dupes without failing COPY
async fn insert_batch(client: &Client, batch: &[Row], worker_id: usize) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let tmp = format!("_tmp_ingest_{worker_id}");
    let cols = COLUMNS.join(",");

    // Create temp table (no indexes, no constraints — COPY always succeeds)
    client.batch_execute(&format!(
        "CREATE TEMP TABLE IF NOT EXISTS {tmp} (\
            source TEXT, source_id TEXT, md5 TEXT, title TEXT, author TEXT, \
            publisher TEXT, language TEXT, year SMALLINT, extension TEXT, \
            filesize BIGINT, pages TEXT, series TEXT, edition TEXT, doi TEXT, \
            isbn TEXT, description TEXT, aacid TEXT, date_added TEXT\
        ); TRUNCATE {tmp};"
    )).await?;

    // COPY into temp table (no unique constraints = no failures)
    let copy_stmt = format!("COPY {tmp} ({cols}) FROM STDIN WITH (FORMAT text)");
    let writer = client.copy_in(&copy_stmt).await?;
    let mut writer = std::pin::pin!(writer);

    let mut buf = Vec::with_capacity(batch.len() * 512);
    for row in batch {
        buf.extend_from_slice(row_to_copy_line(row).as_bytes());
        buf.push(b'\n');
    }
    writer.as_mut().send(bytes::Bytes::copy_from_slice(&buf)).await?;
    writer.as_mut().finish().await?;

    // Move from temp to real table
    // Deduplicate by MD5: on conflict, keep the record with more metadata (longer title)
    let select_cols = COLUMNS.iter().map(|&c| {
        if c == "date_added" {
            // Try casting to date, NULL on failure
            format!("CASE WHEN date_added ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}' THEN date_added::date ELSE NULL END")
        } else {
            c.to_string()
        }
    }).collect::<Vec<_>>().join(",");
    // Metadata completeness score: count non-null fields
    let score = |prefix: &str| format!(
        "({p}.title IS NOT NULL)::int + ({p}.author IS NOT NULL)::int + \
         ({p}.year IS NOT NULL)::int + ({p}.language IS NOT NULL)::int + \
         ({p}.isbn IS NOT NULL)::int + ({p}.doi IS NOT NULL)::int + \
         ({p}.description IS NOT NULL)::int + ({p}.publisher IS NOT NULL)::int",
        p = prefix
    );
    let new_score = score("EXCLUDED");
    let old_score = score("documents");

    let inserted = client.execute(&format!(
        "INSERT INTO documents ({cols}) SELECT {select_cols} FROM {tmp} \
         ON CONFLICT (md5) DO UPDATE SET \
           source = CASE WHEN ({new_score}) >= ({old_score}) THEN EXCLUDED.source ELSE documents.source END, \
           source_id = CASE WHEN ({new_score}) >= ({old_score}) THEN EXCLUDED.source_id ELSE documents.source_id END, \
           title = CASE WHEN ({new_score}) >= ({old_score}) THEN EXCLUDED.title ELSE documents.title END, \
           author = CASE WHEN ({new_score}) >= ({old_score}) THEN EXCLUDED.author ELSE documents.author END, \
           publisher = COALESCE(EXCLUDED.publisher, documents.publisher), \
           language = COALESCE(EXCLUDED.language, documents.language), \
           year = COALESCE(EXCLUDED.year, documents.year), \
           extension = COALESCE(EXCLUDED.extension, documents.extension), \
           filesize = COALESCE(EXCLUDED.filesize, documents.filesize), \
           pages = COALESCE(EXCLUDED.pages, documents.pages), \
           series = COALESCE(EXCLUDED.series, documents.series), \
           edition = COALESCE(EXCLUDED.edition, documents.edition), \
           doi = COALESCE(EXCLUDED.doi, documents.doi), \
           isbn = COALESCE(EXCLUDED.isbn, documents.isbn), \
           description = COALESCE(EXCLUDED.description, documents.description)"
    ), &[]).await?;

    Ok(inserted)
}

struct Stats {
    ingested: AtomicU64,
    skipped: AtomicU64,
}

/// Worker task: receives batches from channel and inserts them
async fn worker(
    worker_id: usize,
    db_url: String,
    mut rx: mpsc::Receiver<Vec<Row>>,
    stats: Arc<Stats>,
) {
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls)
        .await
        .expect("Worker failed to connect to PostgreSQL");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Worker {worker_id} connection error: {e}");
        }
    });

    while let Some(batch) = rx.recv().await {
        let batch_len = batch.len() as u64;
        match insert_batch(&client, &batch, worker_id).await {
            Ok(inserted) => {
                stats.ingested.fetch_add(inserted, Ordering::Relaxed);
                stats.skipped.fetch_add(batch_len - inserted, Ordering::Relaxed);
            }
            Err(e) => {
                eprintln!("    Worker {worker_id} batch error: {e}");
                stats.skipped.fetch_add(batch_len, Ordering::Relaxed);
            }
        }
    }
}

fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let files: Vec<String> = glob::glob(&args.input)
        .expect("Invalid glob pattern")
        .filter_map(|entry| entry.ok().map(|p| p.to_string_lossy().to_string()))
        .collect();

    if files.is_empty() {
        eprintln!("Error: no files match '{}'", args.input);
        std::process::exit(1);
    }

    let db_display = args.db.find('@').map_or(&args.db[..], |i| &args.db[i..]);

    eprintln!("Source: {}", args.source);
    eprintln!("Files: {}", files.len());
    eprintln!("Workers: {}", args.workers);
    eprintln!("Database: {db_display}");
    eprintln!();

    let stats = Arc::new(Stats {
        ingested: AtomicU64::new(0),
        skipped: AtomicU64::new(0),
    });

    // Spawn worker pool
    let mut senders = Vec::new();
    let mut handles = Vec::new();
    for i in 0..args.workers {
        let (tx, rx) = mpsc::channel::<Vec<Row>>(4);
        let db_url = args.db.clone();
        let stats = Arc::clone(&stats);
        handles.push(tokio::spawn(worker(i, db_url, rx, stats)));
        senders.push(tx);
    }

    let start = Instant::now();
    let mut batch: Vec<Row> = Vec::with_capacity(BATCH_SIZE);
    let mut worker_idx = 0usize;
    let mut lines_read: u64 = 0;

    for filepath in &files {
        eprintln!("  Ingesting: {filepath}");

        let file = std::fs::File::open(filepath).expect("Failed to open file");
        let decoder = zstd::Decoder::new(file).expect("Failed to create zstd decoder");
        let reader = BufReader::with_capacity(1024 * 1024, decoder);

        for line_result in reader.lines() {
            let line = match line_result {
                Ok(l) => l,
                Err(_) => {
                    stats.skipped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            if line.trim().is_empty() {
                continue;
            }

            let record: Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(_) => {
                    stats.skipped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            match extract_metadata(&record, &args.source) {
                Some(row) => batch.push(row),
                None => {
                    stats.skipped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }

            if batch.len() >= BATCH_SIZE {
                // Round-robin to workers
                let _ = senders[worker_idx % args.workers]
                    .send(std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE)))
                    .await;
                worker_idx += 1;
            }

            lines_read += 1;
            if lines_read % 100_000 == 0 {
                let ingested = stats.ingested.load(Ordering::Relaxed);
                let skipped = stats.skipped.load(Ordering::Relaxed);
                let elapsed = start.elapsed().as_secs_f64();
                let rate = if elapsed > 0.0 { ingested as f64 / elapsed } else { 0.0 };
                eprintln!(
                    "    {:>12} ingested | {:>8} skipped | {:.0} rec/s",
                    fmt_num(ingested),
                    fmt_num(skipped),
                    rate
                );
            }
        }
    }

    // Send final batch
    if !batch.is_empty() {
        let _ = senders[worker_idx % args.workers].send(batch).await;
    }

    // Drop senders to signal workers to finish
    drop(senders);

    // Wait for all workers
    for h in handles {
        let _ = h.await;
    }

    let ingested = stats.ingested.load(Ordering::Relaxed);
    let skipped = stats.skipped.load(Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64();
    let rate = if elapsed > 0.0 { ingested as f64 / elapsed } else { 0.0 };

    eprintln!(
        "\nAll done: {} ingested, {} skipped in {:.1}s ({:.0} rec/s)",
        fmt_num(ingested),
        fmt_num(skipped),
        elapsed,
        rate
    );
}
