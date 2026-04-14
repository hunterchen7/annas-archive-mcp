import fs from "fs";
import path from "path";
import crypto from "crypto";
import { execSync, spawnSync } from "child_process";
import { getDownloadUrl } from "./download.js";
import { FileCache } from "./cache.js";
import { MemoryTextCache } from "./memoryCache.js";
import https from "https";
import http from "http";

// CACHE_MODE: "memory" (default) keeps nothing on disk across requests —
// downloaded files are streamed through a per-request tmp path and unlinked
// immediately after text extraction; only extracted text is retained, in a
// bounded in-memory LRU. "disk" persists both files and text to CACHE_DIR.
const CACHE_MODE = (process.env.CACHE_MODE || "memory").toLowerCase();
const CACHE_DIR = process.env.CACHE_DIR || "/data/cache";
const FILE_CACHE_MB = parseInt(process.env.FILE_CACHE_MB || "2000", 10);
const TEXT_CACHE_MB = parseInt(process.env.TEXT_CACHE_MB || "500", 10);
const MAX_OUTPUT_CHARS = parseInt(process.env.MAX_OUTPUT_CHARS || "50000", 10);

const USE_DISK = CACHE_MODE === "disk";

const fileCache = USE_DISK
  ? new FileCache(path.join(CACHE_DIR, "files"), FILE_CACHE_MB * 1024 * 1024)
  : null;
const diskTextCache = USE_DISK
  ? new FileCache(path.join(CACHE_DIR, "text"), TEXT_CACHE_MB * 1024 * 1024)
  : null;
const memTextCache = USE_DISK ? null : new MemoryTextCache(TEXT_CACHE_MB * 1024 * 1024);

interface ReadResult {
  text?: string;
  pageCount?: number;
  format?: string;
  error?: string;
}

// Magic bytes for format detection — accepts either a file path or a Buffer
function detectFormat(source: string | Buffer): string {
  let buf: Buffer;
  let filePath: string | null = null;
  let fullBuf: Buffer;
  if (typeof source === "string") {
    filePath = source;
    const size = fs.statSync(filePath).size;
    const readSize = Math.min(size, 128);
    buf = Buffer.alloc(readSize);
    const fd = fs.openSync(filePath, "r");
    fs.readSync(fd, buf, 0, readSize, 0);
    fs.closeSync(fd);
    fullBuf = buf;
  } else {
    buf = source.slice(0, 128);
    fullBuf = source;
  }

  const readSize = buf.length;
  const head16 = buf.slice(0, 16).toString("ascii");

  // PDF: starts with %PDF
  if (buf[0] === 0x25 && buf[1] === 0x50 && buf[2] === 0x44 && buf[3] === 0x46) {
    return "pdf";
  }
  // ZIP-based (EPUB, DOCX, etc): starts with PK\x03\x04
  if (buf[0] === 0x50 && buf[1] === 0x4b && buf[2] === 0x03 && buf[3] === 0x04) {
    if (filePath) {
      try {
        const output = execSync(`unzip -p "${filePath}" mimetype 2>/dev/null || true`, { encoding: "utf-8" });
        if (output.includes("application/epub")) return "epub";
      } catch { /* not epub */ }
      try {
        const output = execSync(`unzip -l "${filePath}" 2>/dev/null | head -20 || true`, { encoding: "utf-8" });
        if (output.includes("word/document.xml")) return "docx";
        if (output.includes("[Content_Types].xml")) return "docx";
      } catch { /* not docx */ }
    } else {
      // In-memory detection: search for filenames stored in the ZIP central directory.
      // EPUBs always contain META-INF/container.xml; DOCX always contains word/document.xml.
      const asStr = fullBuf.toString("latin1");
      if (asStr.includes("META-INF/container.xml") || asStr.includes("application/epub")) return "epub";
      if (asStr.includes("word/document.xml") || asStr.includes("[Content_Types].xml")) return "docx";
    }
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
    const sample = filePath
      ? (() => { try { return fs.readFileSync(filePath!, { encoding: "utf-8", flag: "r" }).slice(0, 500); } catch { return ""; } })()
      : fullBuf.slice(0, 500).toString("utf-8");
    if (sample.includes("FictionBook")) return "fb2";
    return "fb2";
  }
  // RTF: starts with {\rtf
  if (head16.startsWith("{\\rtf")) {
    return "rtf";
  }
  // Plain text fallback
  const textSample = filePath
    ? (() => { try { return fs.readFileSync(filePath!, { encoding: "utf-8", flag: "r" }).slice(0, 1000); } catch { return ""; } })()
    : fullBuf.slice(0, 1000).toString("utf-8");
  if (textSample.length > 0) return "txt";

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

function downloadToBuffer(url: string): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https") ? https : http;
    client.get(url, (res) => {
      if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return downloadToBuffer(res.headers.location).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`Download failed: HTTP ${res.statusCode}`));
      }
      const chunks: Buffer[] = [];
      res.on("data", (c: Buffer) => chunks.push(c));
      res.on("end", () => resolve(Buffer.concat(chunks)));
      res.on("error", reject);
    }).on("error", reject);
  });
}

async function ensureFile(md5: string, secretKey: string): Promise<{ filePath: string; format: string }> {
  // Check cache for any existing file with this md5
  for (const ext of ["pdf", "epub", "djvu", "mobi", "fb2", "docx", "txt", "bin"]) {
    const cached = fileCache!.get(`${md5}.${ext}`);
    if (cached) return { filePath: cached, format: ext === "bin" ? detectFormat(cached) : ext };
  }

  const result = await getDownloadUrl(md5, secretKey);
  if (result.error || !result.downloadUrl) {
    throw new Error(result.error || "No download URL");
  }

  // Download to a temp file first, detect format, then rename
  const tmpPath = fileCache!.pathFor(md5, "bin");
  await downloadToFile(result.downloadUrl, tmpPath);

  const format = detectFormat(tmpPath);
  const finalPath = fileCache!.pathFor(md5, format);

  if (tmpPath !== finalPath) {
    fs.renameSync(tmpPath, finalPath);
  }

  fileCache!.put(`${md5}.${format}`, finalPath);
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

// Universal fallback: calibre's ebook-convert handles MOBI, AZW, AZW3, FB2, LIT, PDB, CBR, CBZ, DOCX, RTF, etc.
function extractWithCalibre(filePath: string): string {
  const tmpTxt = `/tmp/calibre_${Date.now()}.txt`;
  try {
    execSync(`ebook-convert "${filePath}" "${tmpTxt}" 2>/dev/null`, {
      timeout: 120000,
      maxBuffer: 100 * 1024 * 1024,
    });
    const text = fs.readFileSync(tmpTxt, "utf-8");
    fs.unlinkSync(tmpTxt);
    return text;
  } catch {
    try { fs.unlinkSync(tmpTxt); } catch { /* ignore */ }
    throw new Error("ebook-convert failed");
  }
}

// Memory-mode extraction: for formats whose tools support stdin we pipe the
// buffer in directly; for everything else we materialize to /dev/shm (a
// RAM-backed tmpfs on Linux — never touches persistent storage) and unlink
// in finally.
function extractPdfFromBuffer(buf: Buffer): string {
  const result = spawnSync("pdftotext", ["-layout", "-", "-"], {
    input: buf,
    maxBuffer: 100 * 1024 * 1024,
    encoding: "utf-8",
  });
  if (result.status !== 0) throw new Error(`pdftotext exited ${result.status}`);
  return result.stdout || "";
}

function withShmFile<T>(buf: Buffer, ext: string, fn: (p: string) => T): T {
  const shmDir = fs.existsSync("/dev/shm") ? "/dev/shm" : "/tmp";
  const p = path.join(shmDir, `aa-${crypto.randomBytes(8).toString("hex")}.${ext}`);
  fs.writeFileSync(p, buf);
  try {
    return fn(p);
  } finally {
    try { fs.unlinkSync(p); } catch { /* already gone */ }
  }
}

function extractTextFromBuffer(buf: Buffer, format: string): string {
  if (format === "pdf") return extractPdfFromBuffer(buf);
  if (format === "txt") return buf.toString("utf-8");
  // Everything else needs a file path — materialize to tmpfs (RAM), extract, unlink.
  if (format === "epub") return withShmFile(buf, "epub", extractEpub);
  if (format === "djvu") return withShmFile(buf, "djvu", extractDjvu);
  return withShmFile(buf, format || "bin", (p) => {
    try { return extractWithCalibre(p); }
    catch {
      try { return extractPdfFromBuffer(buf); } catch { /* not pdf */ }
      return buf.toString("utf-8");
    }
  });
}

function extractText(filePath: string, format: string): string {
  // PDF: pdftotext is best
  if (format === "pdf") return extractPdf(filePath);
  // EPUB: direct HTML extraction is faster than calibre
  if (format === "epub") return extractEpub(filePath);
  // DJVU: dedicated tool
  if (format === "djvu") return extractDjvu(filePath);
  // Plain text: just read it
  if (format === "txt") return fs.readFileSync(filePath, "utf-8");
  // Everything else: calibre handles MOBI, AZW, AZW3, FB2, LIT, PDB, CBR, CBZ, DOCX, RTF, etc.
  try {
    return extractWithCalibre(filePath);
  } catch {
    // Last resort: try pdftotext, then raw read
    try { return extractPdf(filePath); } catch { /* not pdf */ }
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

function ensureTextDisk(md5: string, filePath: string, format: string): string {
  const key = `${md5}.txt`;
  const cached = diskTextCache!.get(key);
  if (cached) return fs.readFileSync(cached, "utf-8");

  const text = extractText(filePath, format);
  const textPath = diskTextCache!.pathFor(md5, "txt");
  fs.writeFileSync(textPath, text);
  diskTextCache!.put(key, textPath);
  return text;
}

async function readInMemory(md5: string, secretKey: string): Promise<{ text: string; format: string }> {
  const cachedText = memTextCache!.get(md5);
  if (cachedText) {
    // Format is not reliably known for cached text; re-derive from stored marker if needed,
    // but downstream splitPages/pagination doesn't require it. Return "txt" as a neutral label.
    return { text: cachedText, format: "txt" };
  }

  const result = await getDownloadUrl(md5, secretKey);
  if (result.error || !result.downloadUrl) {
    throw new Error(result.error || "No download URL");
  }

  const buf = await downloadToBuffer(result.downloadUrl);
  const format = detectFormat(buf);
  const text = extractTextFromBuffer(buf, format);
  memTextCache!.put(md5, text);
  return { text, format };
}

export async function readDocument(
  md5: string,
  hintExt: string,
  secretKey: string,
  pageRange?: string
): Promise<ReadResult> {
  let fullText: string;
  let format: string;

  if (USE_DISK) {
    let filePath: string;
    try {
      const file = await ensureFile(md5, secretKey);
      filePath = file.filePath;
      format = file.format;
    } catch (e) {
      return { error: `Failed to download: ${e}` };
    }
    try {
      fullText = ensureTextDisk(md5, filePath, format);
    } catch (e) {
      return { error: `Failed to extract text (format: ${format}): ${e}` };
    }
  } else {
    try {
      const r = await readInMemory(md5, secretKey);
      fullText = r.text;
      format = r.format;
    } catch (e) {
      return { error: `Failed to read: ${e}` };
    }
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
