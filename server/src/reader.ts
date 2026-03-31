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

// Magic bytes for format detection
function detectFormat(filePath: string): string {
  const size = fs.statSync(filePath).size;
  const readSize = Math.min(size, 128);
  const buf = Buffer.alloc(readSize);
  const fd = fs.openSync(filePath, "r");
  fs.readSync(fd, buf, 0, readSize, 0);
  fs.closeSync(fd);

  const head16 = buf.slice(0, 16).toString("ascii");

  // PDF: starts with %PDF
  if (buf[0] === 0x25 && buf[1] === 0x50 && buf[2] === 0x44 && buf[3] === 0x46) {
    return "pdf";
  }
  // ZIP-based (EPUB, DOCX, etc): starts with PK\x03\x04
  if (buf[0] === 0x50 && buf[1] === 0x4b && buf[2] === 0x03 && buf[3] === 0x04) {
    try {
      const output = execSync(`unzip -p "${filePath}" mimetype 2>/dev/null || true`, { encoding: "utf-8" });
      if (output.includes("application/epub")) return "epub";
    } catch { /* not epub */ }
    try {
      const output = execSync(`unzip -l "${filePath}" 2>/dev/null | head -20 || true`, { encoding: "utf-8" });
      if (output.includes("word/document.xml")) return "docx";
      if (output.includes("[Content_Types].xml")) return "docx";
    } catch { /* not docx */ }
    return "zip";
  }
  // DJVU: starts with AT&T
  if (buf[0] === 0x41 && buf[1] === 0x54 && buf[2] === 0x26 && buf[3] === 0x54) {
    return "djvu";
  }
  // MOBI/AZW: "BOOKMOBI" can be at offset 60 (after PDB header with title)
  // or at offset 0 in some files
  const fullStr = buf.toString("ascii");
  if (fullStr.includes("BOOKMOBI")) {
    return "mobi";
  }
  // Also check PDB header: if bytes 60-67 contain "MOBI" or "BOOK"
  if (readSize >= 68) {
    const pdbMagic = buf.slice(60, 68).toString("ascii");
    if (pdbMagic.includes("BOOK") || pdbMagic.includes("MOBI")) {
      return "mobi";
    }
  }
  // FB2 (XML-based): starts with <?xml or <FictionBook
  if (head16.startsWith("<?xml") || head16.startsWith("<Fic")) {
    // Could be FB2 or other XML — check deeper
    try {
      const sample = fs.readFileSync(filePath, { encoding: "utf-8", flag: "r" }).slice(0, 500);
      if (sample.includes("FictionBook")) return "fb2";
    } catch { /* not text */ }
    return "fb2";
  }
  // RTF: starts with {\rtf
  if (head16.startsWith("{\\rtf")) {
    return "rtf";
  }
  // Plain text fallback
  try {
    const sample = fs.readFileSync(filePath, { encoding: "utf-8", flag: "r" }).slice(0, 1000);
    if (sample.length > 0) return "txt";
  } catch { /* binary */ }

  return "unknown";
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

async function ensureFile(md5: string, secretKey: string): Promise<{ filePath: string; format: string }> {
  // Check cache for any existing file with this md5
  for (const ext of ["pdf", "epub", "djvu", "mobi", "fb2", "docx", "txt", "bin"]) {
    const cached = fileCache.get(`${md5}.${ext}`);
    if (cached) return { filePath: cached, format: ext === "bin" ? detectFormat(cached) : ext };
  }

  const result = await getDownloadUrl(md5, secretKey);
  if (result.error || !result.downloadUrl) {
    throw new Error(result.error || "No download URL");
  }

  // Download to a temp file first, detect format, then rename
  const tmpPath = fileCache.pathFor(md5, "bin");
  await downloadToFile(result.downloadUrl, tmpPath);

  const format = detectFormat(tmpPath);
  const finalPath = fileCache.pathFor(md5, format);

  if (tmpPath !== finalPath) {
    fs.renameSync(tmpPath, finalPath);
  }

  fileCache.put(`${md5}.${format}`, finalPath);
  return { filePath: finalPath, format };
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

function extractMobi(filePath: string): string {
  // Try converting mobi to epub first via calibre's ebook-convert if available
  try {
    const tmpEpub = `/tmp/mobi_${Date.now()}.epub`;
    execSync(`ebook-convert "${filePath}" "${tmpEpub}" 2>/dev/null`, { timeout: 60000 });
    const text = extractEpub(tmpEpub);
    fs.unlinkSync(tmpEpub);
    return text;
  } catch {
    // Fallback: try extracting raw text with strings
    return execSync(`strings "${filePath}" | head -10000`, {
      maxBuffer: 100 * 1024 * 1024,
      encoding: "utf-8",
    });
  }
}

function extractDocx(filePath: string): string {
  try {
    const tmpDir = `/tmp/docx_${Date.now()}`;
    execSync(`mkdir -p "${tmpDir}" && unzip -o -q "${filePath}" -d "${tmpDir}" 2>/dev/null || true`);
    const xmlPath = path.join(tmpDir, "word/document.xml");
    if (fs.existsSync(xmlPath)) {
      const xml = fs.readFileSync(xmlPath, "utf-8");
      const text = xml
        .replace(/<w:br[^>]*\/>/gi, "\n")
        .replace(/<\/w:p>/gi, "\n\n")
        .replace(/<[^>]+>/g, "")
        .replace(/\s+/g, " ")
        .trim();
      execSync(`rm -rf "${tmpDir}"`);
      return text;
    }
    execSync(`rm -rf "${tmpDir}"`);
    return "[No document.xml found in DOCX]";
  } catch {
    return "[Failed to extract DOCX text]";
  }
}

function extractFb2(filePath: string): string {
  const xml = fs.readFileSync(filePath, "utf-8");
  return xml
    .replace(/<binary[^>]*>[\s\S]*?<\/binary>/gi, "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractText(filePath: string, format: string): string {
  switch (format) {
    case "pdf": return extractPdf(filePath);
    case "epub": return extractEpub(filePath);
    case "djvu": return extractDjvu(filePath);
    case "mobi": return extractMobi(filePath);
    case "docx": return extractDocx(filePath);
    case "fb2": return extractFb2(filePath);
    case "rtf":
    case "txt": return fs.readFileSync(filePath, "utf-8");
    default:
      // Try each extractor until one works
      for (const fn of [extractPdf, extractEpub]) {
        try { const t = fn(filePath); if (t.length > 100) return t; } catch { /* next */ }
      }
      return fs.readFileSync(filePath, "utf-8");
  }
}

function splitPages(text: string): string[] {
  const ffPages = text.split("\f").filter((p) => p.trim());
  if (ffPages.length > 1) return ffPages;

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

function ensureText(md5: string, filePath: string, format: string): string {
  const key = `${md5}.txt`;
  const cached = textCache.get(key);
  if (cached) return fs.readFileSync(cached, "utf-8");

  const text = extractText(filePath, format);
  const textPath = textCache.pathFor(md5, "txt");
  fs.writeFileSync(textPath, text);
  textCache.put(key, textPath);
  return text;
}

export async function readDocument(
  md5: string,
  hintExt: string,
  secretKey: string,
  pageRange?: string
): Promise<ReadResult> {
  // Download file (cached) — format auto-detected from magic bytes
  let filePath: string;
  let format: string;
  try {
    const file = await ensureFile(md5, secretKey);
    filePath = file.filePath;
    format = file.format;
  } catch (e) {
    return { error: `Failed to download: ${e}` };
  }

  // Extract text (cached)
  let fullText: string;
  try {
    fullText = ensureText(md5, filePath, format);
  } catch (e) {
    return { error: `Failed to extract text (format: ${format}): ${e}` };
  }

  const pages = splitPages(fullText);
  const pageCount = pages.length;

  if (!pageRange) {
    const preview = pages[0]?.slice(0, 2000) || "[Empty document]";
    return {
      text: `Document: ${pageCount} pages, detected format: ${format}\n\n--- Page 1 preview ---\n${preview}\n\n[Request specific pages with the "pages" parameter, e.g. "1-10"]`,
      pageCount,
      format,
    };
  }

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
    text = text.slice(0, MAX_OUTPUT_CHARS) + `\n\n[Truncated at ${MAX_OUTPUT_CHARS} chars. Request a smaller page range.]`;
  }

  return {
    text: `Pages ${startPage + 1}-${endPage + 1} of ${pageCount} (${format}):\n\n${text}`,
    pageCount,
    format,
  };
}
