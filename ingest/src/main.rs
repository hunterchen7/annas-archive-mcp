use clap::Parser;
use futures_util::SinkExt;
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::time::Instant;
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

fn extract_metadata(record: &Value, source: &str) -> Option<Row> {
    let meta = record.get("metadata")?;

    let md5_raw = get_str(meta, &["md5_reported", "md5", "md5_hash"])?;
    let md5: String = md5_raw.trim().to_lowercase();
    if md5.len() != 32 || !md5.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }

    let source_id = meta
        .get("zlibrary_id")
        .or_else(|| meta.get("libgen_id"))
        .or_else(|| meta.get("id"))
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

    Some(Row {
        source: source.to_string(),
        source_id,
        md5,
        title: get_str(meta, &["title", "title_best"]).and_then(|s| truncate(s, 4000)),
        author: get_str(meta, &["author", "author_best"]).and_then(|s| truncate(s, 2000)),
        publisher: get_str(meta, &["publisher"]).and_then(|s| truncate(s, 1000)),
        language: get_str(meta, &["language", "language_best"]).and_then(|s| truncate(s, 100)),
        year,
        extension: get_str(meta, &["extension", "extension_best"]).and_then(|s| truncate(s, 20)),
        filesize,
        pages: meta.get("pages").and_then(|v| match v {
            Value::String(s) if !s.is_empty() => truncate(s, 50),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        }),
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

async fn copy_batch(client: &Client, batch: &[Row]) -> Result<u64, Box<dyn std::error::Error>> {
    let cols = COLUMNS.join(",");
    let stmt = format!("COPY documents ({cols}) FROM STDIN WITH (FORMAT text)");

    let writer = client.copy_in(&stmt).await?;
    let mut writer = std::pin::pin!(writer);

    let mut buf = Vec::with_capacity(batch.len() * 512);
    for row in batch {
        buf.extend_from_slice(row_to_copy_line(row).as_bytes());
        buf.push(b'\n');
    }

    writer.as_mut().send(bytes::Bytes::copy_from_slice(&buf)).await?;
    let rows = writer.as_mut().finish().await?;
    Ok(rows)
}

async fn insert_single(client: &Client, row: &Row) -> Result<(), tokio_postgres::Error> {
    client
        .execute(
            "INSERT INTO documents (source,source_id,md5,title,author,publisher,\
             language,year,extension,filesize,pages,series,edition,doi,isbn,\
             description,aacid,date_added) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) \
             ON CONFLICT (aacid) DO NOTHING",
            &[
                &row.source,
                &row.source_id,
                &row.md5,
                &row.title,
                &row.author,
                &row.publisher,
                &row.language,
                &row.year,
                &row.extension,
                &row.filesize,
                &row.pages,
                &row.series,
                &row.edition,
                &row.doi,
                &row.isbn,
                &row.description,
                &row.aacid,
                &row.date_added,
            ],
        )
        .await?;
    Ok(())
}

async fn flush_batch(client: &Client, batch: &[Row]) -> (u64, u64) {
    match copy_batch(client, batch).await {
        Ok(_) => (batch.len() as u64, 0),
        Err(e) => {
            eprintln!("    COPY failed ({e}), falling back to individual inserts");
            let mut ok = 0u64;
            let mut fail = 0u64;
            for row in batch {
                match insert_single(client, row).await {
                    Ok(_) => ok += 1,
                    Err(_) => fail += 1,
                }
            }
            (ok, fail)
        }
    }
}

async fn ingest_file(client: &Client, filepath: &str, source: &str) -> (u64, u64) {
    let start = Instant::now();
    let mut total_ingested: u64 = 0;
    let mut total_skipped: u64 = 0;

    eprintln!("  Ingesting: {filepath}");

    let file = std::fs::File::open(filepath).expect("Failed to open file");
    let decoder = zstd::Decoder::new(file).expect("Failed to create zstd decoder");
    let reader = BufReader::with_capacity(1024 * 1024, decoder);

    let mut batch: Vec<Row> = Vec::with_capacity(BATCH_SIZE);

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => {
                total_skipped += 1;
                continue;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let record: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => {
                total_skipped += 1;
                continue;
            }
        };

        match extract_metadata(&record, source) {
            Some(row) => batch.push(row),
            None => {
                total_skipped += 1;
                continue;
            }
        }

        if batch.len() >= BATCH_SIZE {
            let (ok, fail) = flush_batch(client, &batch).await;
            total_ingested += ok;
            total_skipped += fail;
            batch.clear();

            if total_ingested % 100_000 == 0 && total_ingested > 0 {
                let elapsed = start.elapsed().as_secs_f64();
                let rate = total_ingested as f64 / elapsed;
                eprintln!(
                    "    {:>12} ingested | {:>8} skipped | {:.0} rec/s",
                    fmt_num(total_ingested),
                    fmt_num(total_skipped),
                    rate
                );
            }
        }
    }

    if !batch.is_empty() {
        let (ok, fail) = flush_batch(client, &batch).await;
        total_ingested += ok;
        total_skipped += fail;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let rate = if elapsed > 0.0 {
        total_ingested as f64 / elapsed
    } else {
        0.0
    };
    eprintln!(
        "    Done: {} ingested, {} skipped in {:.1}s ({:.0} rec/s)",
        fmt_num(total_ingested),
        fmt_num(total_skipped),
        elapsed,
        rate
    );

    (total_ingested, total_skipped)
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
    eprintln!("Database: {db_display}");
    eprintln!();

    let (client, connection) = tokio_postgres::connect(&args.db, NoTls)
        .await
        .expect("Failed to connect to PostgreSQL");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    let mut grand_total: u64 = 0;
    let mut grand_skipped: u64 = 0;

    for filepath in &files {
        let (ingested, skipped) = ingest_file(&client, filepath, &args.source).await;
        grand_total += ingested;
        grand_skipped += skipped;
    }

    eprintln!(
        "\nAll done: {} total ingested, {} total skipped across {} file(s)",
        fmt_num(grand_total),
        fmt_num(grand_skipped),
        files.len()
    );
}
