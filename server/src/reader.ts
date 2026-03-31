import fs from "fs";
import path from "path";
import { execSync } from "child_process";
import { getDownloadUrl } from "./download.js";
import { FileCache } from "./cache.js";
import https from "https";
import http from "http";

const CACHE_DIR = process.env.CACHE_DIR || "/data/cache";
const FILE_CACHE_MB = parseInt(process.env.FILE_CACHE_MB || "2000", 10);
const TEXT_CACHE_MB = parseInt(process.env.TEXT_CACHE_MB || "500", 10);
const MAX_OUTPUT_CHARS = parseInt(process.env.MAX_OUTPUT_CHARS || "50000", 10);

// Two separate LRU caches: files (large) and extracted text (smaller)
const fileCache = new FileCache(
  path.join(CACHE_DIR, "files"),
  FILE_CACHE_MB * 1024 * 1024
);
const textCache = new FileCache(
  path.join(CACHE_DIR, "text"),
  TEXT_CACHE_MB * 1024 * 1024
);

interface ReadResult {
  text?: string;
  pageCount?: number;
  format?: string;
  error?: string;
}

function downloadToFile(url: string, dest: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https") ? https : http;
    client.get(url, (res) => {
      if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return downloadToFile(res.headers.location, dest).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`Download failed: HTTP ${res.statusCode}`));
      }
      const stream = fs.createWriteStream(dest);
      res.pipe(stream);
      stream.on("finish", () => { stream.close(); resolve(); });
      stream.on("error", reject);
    }).on("error", reject);
  });
}

async function ensureFile(md5: string, ext: string, secretKey: string): Promise<string> {
  const key = `${md5}.${ext}`;
  const cached = fileCache.get(key);
  if (cached) return cached;

  const result = await getDownloadUrl(md5, secretKey);
  if (result.error || !result.downloadUrl) {
    throw new Error(result.error || "No download URL");
  }

  const filePath = fileCache.pathFor(md5, ext);
  await downloadToFile(result.downloadUrl, filePath);
  fileCache.put(key, filePath);
  return filePath;
}

function extractPdf(filePath: string): string {
  return execSync(`pdftotext -layout "${filePath}" -`, {
    maxBuffer: 100 * 1024 * 1024,
    encoding: "utf-8",
  });
}

function extractEpub(filePath: string): string {
  try {
    const tmpDir = `/tmp/epub_${Date.now()}`;
    execSync(`rm -rf "${tmpDir}" && mkdir -p "${tmpDir}" && unzip -o -q "${filePath}" -d "${tmpDir}" 2>/dev/null || true`);

    const htmlFiles = execSync(
      `find "${tmpDir}" -name "*.html" -o -name "*.xhtml" -o -name "*.htm" | sort`,
      { encoding: "utf-8" }
    ).trim().split("\n").filter(Boolean);

    let text = "";
    for (const htmlFile of htmlFiles) {
      const html = fs.readFileSync(htmlFile, "utf-8");
      const stripped = html
        .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, "")
        .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, "")
        .replace(/<[^>]+>/g, " ")
        .replace(/&nbsp;/g, " ")
        .replace(/&amp;/g, "&")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .replace(/&quot;/g, '"')
        .replace(/&#?\w+;/g, "")
        .replace(/\s+/g, " ")
        .trim();
      if (stripped) text += stripped + "\n\n";
    }

    execSync(`rm -rf "${tmpDir}"`);
    return text;
  } catch {
    return "[Failed to extract EPUB text]";
  }
}

function extractDjvu(filePath: string): string {
  return execSync(`djvutxt "${filePath}"`, {
    maxBuffer: 100 * 1024 * 1024,
    encoding: "utf-8",
  });
}

function extractText(filePath: string, ext: string): string {
  switch (ext) {
    case "pdf": return extractPdf(filePath);
    case "epub": return extractEpub(filePath);
    case "djvu": return extractDjvu(filePath);
    default:
      try { return extractPdf(filePath); }
      catch { return fs.readFileSync(filePath, "utf-8"); }
  }
}

function splitPages(text: string): string[] {
  // Split on form feed (PDF page break)
  const ffPages = text.split("\f").filter((p) => p.trim());
  if (ffPages.length > 1) return ffPages;

  // Fallback: split into ~3000 char chunks
  const pages: string[] = [];
  const chunkSize = 3000;
  let i = 0;
  while (i < text.length) {
    let end = Math.min(i + chunkSize, text.length);
    if (end < text.length) {
      const newline = text.lastIndexOf("\n\n", end);
      if (newline > i + chunkSize * 0.5) end = newline + 2;
    }
    const page = text.slice(i, end).trim();
    if (page) pages.push(page);
    i = end;
  }
  return pages;
}

function ensureText(md5: string, filePath: string, ext: string): string {
  const key = `${md5}.txt`;
  const cached = textCache.get(key);
  if (cached) return fs.readFileSync(cached, "utf-8");

  const text = extractText(filePath, ext);
  const textPath = textCache.pathFor(md5, "txt");
  fs.writeFileSync(textPath, text);
  textCache.put(key, textPath);
  return text;
}

export async function readDocument(
  md5: string,
  ext: string,
  secretKey: string,
  pageRange?: string
): Promise<ReadResult> {
  // Download file (cached)
  let filePath: string;
  try {
    filePath = await ensureFile(md5, ext, secretKey);
  } catch (e) {
    return { error: `Failed to download: ${e}` };
  }

  // Extract text (cached)
  let fullText: string;
  try {
    fullText = ensureText(md5, filePath, ext);
  } catch (e) {
    return { error: `Failed to extract text: ${e}` };
  }

  const pages = splitPages(fullText);
  const pageCount = pages.length;

  // No page range → return overview + first page preview
  if (!pageRange) {
    const preview = pages[0]?.slice(0, 2000) || "[Empty document]";
    return {
      text: `Document: ${pageCount} pages, format: ${ext}\n\n--- Page 1 preview ---\n${preview}\n\n[Request specific pages with the "pages" parameter, e.g. "1-10"]`,
      pageCount,
      format: ext,
    };
  }

  // Parse page range
  let startPage = 0;
  let endPage = pageCount - 1;

  if (pageRange === "all") {
    // full text, capped
  } else if (pageRange.includes("-")) {
    const [s, e] = pageRange.split("-").map((n) => parseInt(n, 10) - 1);
    startPage = Math.max(0, s);
    endPage = Math.min(pageCount - 1, e);
  } else {
    const p = parseInt(pageRange, 10) - 1;
    startPage = Math.max(0, p);
    endPage = startPage;
  }

  const selectedPages = pages.slice(startPage, endPage + 1);
  let text = selectedPages
    .map((p, i) => `--- Page ${startPage + i + 1} ---\n${p}`)
    .join("\n\n");

  if (text.length > MAX_OUTPUT_CHARS) {
    const truncatedAt = endPage + 1;
    text = text.slice(0, MAX_OUTPUT_CHARS) + `\n\n[Truncated at ${MAX_OUTPUT_CHARS} chars. Request a smaller page range.]`;
  }

  return {
    text: `Pages ${startPage + 1}-${endPage + 1} of ${pageCount}:\n\n${text}`,
    pageCount,
    format: ext,
  };
}
