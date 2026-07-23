import fs from "fs";
import path from "path";
import os from "os";
import { getDownloadUrl } from "./download.js";
import { FileCache } from "./cache.js";
import { MemoryTextCache } from "./memoryCache.js";
import https from "https";
import http from "http";
import { keyValidationError, validateKey } from "./auth.js";
import { runTextCommand } from "./command.js";
import {
  inspectZipArchive,
  listSafeRegularFiles,
  resolveSafeFile,
} from "./safeArchive.js";

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

interface Chapter {
  index: number;
  title: string;
  startPage: number;
  endPage: number;
}

export interface ReadOptions {
  pageRange?: string;
  chapter?: number;
  listChapters?: boolean;
}

interface ReadResult {
  text?: string;
  pageCount?: number;
  format?: string;
  chapters?: Chapter[];
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
        const output = runTextCommand("unzip", ["-p", filePath, "mimetype"], {
          timeoutMs: 10_000,
          maxBufferBytes: 1024 * 1024,
        });
        if (output.includes("application/epub")) return "epub";
      } catch { /* not epub */ }
      try {
        const output = runTextCommand("unzip", ["-Z1", filePath], {
          timeoutMs: 10_000,
          maxBufferBytes: 10 * 1024 * 1024,
        });
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
  return runTextCommand("pdftotext", ["-layout", filePath, "-"]);
}

// Invisible sentinel used to embed native EPUB chapter titles inline in the
// extracted text. Stays part of the cached string so chapter structure
// survives memory/disk cache hits without a sidecar. Detected in
// detectChapters() and stripped when rendering output.
// Uses Unicode private-use chars — effectively impossible to collide with
// natural text.
const NATIVE_CHAPTER_SENTINEL = "CH";

function stripHtmlToText(html: string): string {
  return html
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
}

interface EpubToc {
  opfDir: string;
  spine: string[]; // idrefs in reading order
  manifest: Map<string, string>; // id → href
  titleByHref: Map<string, string>; // href → chapter title from nav/NCX
}

// Parses an unzipped EPUB's metadata (container.xml → OPF → NCX/nav) to
// recover the spine + chapter titles. Returns null if anything essential
// is missing or malformed — caller falls back to blind HTML concatenation.
function parseEpubToc(tmpDir: string): EpubToc | null {
  try {
    const containerPath = resolveSafeFile(tmpDir, "META-INF/container.xml");
    if (!containerPath) return null;
    const container = fs.readFileSync(containerPath, "utf-8");
    const opfMatch = container.match(/<rootfile[^>]+full-path=["']([^"']+)["']/i);
    if (!opfMatch) return null;
    const opfPath = resolveSafeFile(tmpDir, opfMatch[1]);
    if (!opfPath) return null;
    const opfDir = path.dirname(opfPath);
    const opf = fs.readFileSync(opfPath, "utf-8");

    const manifest = new Map<string, string>();
    const allItems: { id: string; href: string; attrs: string }[] = [];
    const itemRx = /<item\s+([^>]+?)\/?>/gi;
    let m: RegExpExecArray | null;
    while ((m = itemRx.exec(opf)) !== null) {
      const attrs = m[1];
      const id = attrs.match(/\bid=["']([^"']+)["']/)?.[1];
      const href = attrs.match(/\bhref=["']([^"']+)["']/)?.[1];
      if (id && href) {
        manifest.set(id, href);
        allItems.push({ id, href, attrs });
      }
    }

    const spine: string[] = [];
    const spineRx = /<itemref\s+([^>]+?)\/?>/gi;
    while ((m = spineRx.exec(opf)) !== null) {
      const idref = m[1].match(/\bidref=["']([^"']+)["']/)?.[1];
      if (idref) spine.push(idref);
    }
    if (spine.length === 0) return null;

    const titleByHref = new Map<string, string>();

    // EPUB3 nav document: manifest item with properties="nav"
    const navItem = allItems.find((i) => /\bproperties=["'][^"']*\bnav\b[^"']*["']/i.test(i.attrs));
    if (navItem) {
      const navPath = resolveSafeFile(tmpDir, navItem.href, opfDir);
      if (navPath) {
        const nav = fs.readFileSync(navPath, "utf-8");
        const tocMatch = nav.match(/<nav\b[^>]*epub:type=["'][^"']*\btoc\b[^"']*["'][^>]*>([\s\S]*?)<\/nav>/i);
        const section = tocMatch ? tocMatch[1] : nav;
        const navDir = path.dirname(navItem.href);
        const linkRx = /<a\b[^>]+href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
        while ((m = linkRx.exec(section)) !== null) {
          const rawHref = m[1].split("#")[0];
          if (!rawHref) continue;
          // Resolve relative to nav file, then make relative to opfDir to match manifest hrefs
          const resolved = path.posix.normalize(path.posix.join(navDir, rawHref));
          const title = stripHtmlToText(m[2]);
          if (title && !titleByHref.has(resolved)) titleByHref.set(resolved, title);
          // Also register the raw href as-is, in case the TOC uses the same pathing style as the manifest
          if (title && !titleByHref.has(rawHref)) titleByHref.set(rawHref, title);
        }
      }
    }

    // EPUB2 NCX fallback
    if (titleByHref.size === 0) {
      const ncxItem = allItems.find((i) => /media-type=["']application\/x-dtbncx\+xml["']/i.test(i.attrs));
      if (ncxItem) {
        const ncxPath = resolveSafeFile(tmpDir, ncxItem.href, opfDir);
        if (ncxPath) {
          const ncx = fs.readFileSync(ncxPath, "utf-8");
          const ncxDir = path.dirname(ncxItem.href);
          const npRx = /<navPoint\b[^>]*>([\s\S]*?)<\/navPoint>/gi;
          while ((m = npRx.exec(ncx)) !== null) {
            const inner = m[1];
            const textMatch = inner.match(/<navLabel>\s*<text>([\s\S]*?)<\/text>/i);
            const srcMatch = inner.match(/<content\s+src=["']([^"']+)["']/i);
            if (textMatch && srcMatch) {
              const rawHref = srcMatch[1].split("#")[0];
              if (!rawHref) continue;
              const resolved = path.posix.normalize(path.posix.join(ncxDir, rawHref));
              const title = stripHtmlToText(textMatch[1]);
              if (title && !titleByHref.has(resolved)) titleByHref.set(resolved, title);
              if (title && !titleByHref.has(rawHref)) titleByHref.set(rawHref, title);
            }
          }
        }
      }
    }

    return { opfDir, spine, manifest, titleByHref };
  } catch {
    return null;
  }
}

function extractEpub(filePath: string): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "aa-epub-"));
  try {
    inspectZipArchive(filePath);
    runTextCommand("unzip", ["-o", "-q", filePath, "-d", tmpDir]);
    const htmlFiles = listSafeRegularFiles(
      tmpDir,
      new Set([".html", ".xhtml", ".htm"]),
    );

    const toc = parseEpubToc(tmpDir);
    if (toc) {
      let text = "";
      for (const idref of toc.spine) {
        const href = toc.manifest.get(idref);
        if (!href) continue;
        const itemPath = resolveSafeFile(tmpDir, href, toc.opfDir);
        if (!itemPath) continue;

        const title = toc.titleByHref.get(href) || toc.titleByHref.get(path.posix.normalize(href));
        if (title) {
          text += `\n\n${NATIVE_CHAPTER_SENTINEL}${title}\n\n`;
        }
        const stripped = stripHtmlToText(fs.readFileSync(itemPath, "utf-8"));
        if (stripped) text += stripped + "\n\n";
      }
      if (text.trim()) return text;
    }

    // Fallback: blind sorted concatenation (original behavior)
    let text = "";
    for (const htmlFile of htmlFiles) {
      const stripped = stripHtmlToText(fs.readFileSync(htmlFile, "utf-8"));
      if (stripped) text += stripped + "\n\n";
    }
    return text;
  } catch {
    return "[Failed to extract EPUB text]";
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

function extractDjvu(filePath: string): string {
  return runTextCommand("djvutxt", [filePath]);
}

// Universal fallback: calibre's ebook-convert handles MOBI, AZW, AZW3, FB2, LIT, PDB, CBR, CBZ, DOCX, RTF, etc.
function extractWithCalibre(filePath: string): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "aa-calibre-"));
  const tmpTxt = path.join(tmpDir, "output.txt");
  try {
    const header = Buffer.alloc(4);
    const fd = fs.openSync(filePath, "r");
    const bytesRead = fs.readSync(fd, header, 0, header.length, 0);
    fs.closeSync(fd);
    if (
      bytesRead === 4 &&
      header[0] === 0x50 &&
      header[1] === 0x4b &&
      header[2] === 0x03 &&
      header[3] === 0x04
    ) {
      inspectZipArchive(filePath);
    }
    runTextCommand("ebook-convert", [filePath, tmpTxt]);
    const text = fs.readFileSync(tmpTxt, "utf-8");
    return text;
  } catch {
    throw new Error("ebook-convert failed");
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

// Memory-mode extraction: for formats whose tools support stdin we pipe the
// buffer in directly; for everything else we materialize to /dev/shm (a
// RAM-backed tmpfs on Linux — never touches persistent storage) and unlink
// in finally.
function extractPdfFromBuffer(buf: Buffer): string {
  return runTextCommand("pdftotext", ["-layout", "-", "-"], {
    input: buf,
  });
}

function withShmFile<T>(buf: Buffer, ext: string, fn: (p: string) => T): T {
  const tempRoot = fs.existsSync("/dev/shm") ? "/dev/shm" : os.tmpdir();
  const tmpDir = fs.mkdtempSync(path.join(tempRoot, "aa-file-"));
  const safeExt = /^[a-z0-9]{1,10}$/i.test(ext) ? ext : "bin";
  const p = path.join(tmpDir, `input.${safeExt}`);
  fs.writeFileSync(p, buf, { mode: 0o600 });
  try {
    return fn(p);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
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

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// Matches a sentinel marker followed by the chapter title up to the next newline
const NATIVE_MARKER_RX = new RegExp(escapeRegex(NATIVE_CHAPTER_SENTINEL) + "([^\\n\\r]+)", "g");

// Removes embedded sentinels from text destined for the user. Done only at
// render time so that detectChapters (which operates on the same `pages`
// array) can still see them.
function stripNativeMarkers(text: string): string {
  return text.replace(NATIVE_MARKER_RX, "$1");
}

// Chapter detection. First looks for embedded native sentinels (EPUB with
// parsed spine + TOC); if none found, falls back to a heuristic regex that
// scans page tops for "Chapter N", "Part II", "Prologue", "Appendix A", etc.
function detectChapters(pages: string[]): Chapter[] {
  // Pass 1: native EPUB chapter sentinels
  const nativeMarkers: { page: number; title: string }[] = [];
  for (let p = 0; p < pages.length; p++) {
    NATIVE_MARKER_RX.lastIndex = 0;
    let m: RegExpExecArray | null;
    while ((m = NATIVE_MARKER_RX.exec(pages[p])) !== null) {
      const title = m[1].trim();
      if (!title) continue;
      if (!nativeMarkers.length || nativeMarkers[nativeMarkers.length - 1].page !== p + 1) {
        nativeMarkers.push({ page: p + 1, title });
      }
    }
  }
  if (nativeMarkers.length >= 2) {
    return nativeMarkers.map((m, i) => ({
      index: i + 1,
      title: m.title,
      startPage: m.page,
      endPage: i + 1 < nativeMarkers.length ? nativeMarkers[i + 1].page - 1 : pages.length,
    }));
  }

  // Pass 2: heuristic regex (PDF, other formats)
  const structuralRx = /^(chapter|part|book|section)\s+(\d{1,3}|[ivxlcdm]{1,6})\b.*$/i;
  const namedRx = /^(prologue|epilogue|introduction|preface|foreword|conclusion|afterword|acknowledg(?:e)?ments?|references|bibliography|notes|appendix(?:\s+[a-z0-9]{1,3})?)\b.*$/i;

  const markers: { page: number; title: string }[] = [];

  for (let p = 0; p < pages.length; p++) {
    const raw = pages[p].split("\n");
    let scanned = 0;
    for (const rawLine of raw) {
      const line = rawLine.trim();
      if (!line) continue;
      scanned++;
      if (scanned > 15) break;
      if (line.length < 3 || line.length > 120) continue;

      if (structuralRx.test(line) || namedRx.test(line)) {
        if (markers.length > 0 && markers[markers.length - 1].page === p + 1) break;
        markers.push({ page: p + 1, title: line });
        break;
      }
    }
  }

  if (markers.length < 2) return [];

  return markers.map((m, i) => ({
    index: i + 1,
    title: m.title,
    startPage: m.page,
    endPage: i + 1 < markers.length ? markers[i + 1].page - 1 : pages.length,
  }));
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
  opts: ReadOptions = {}
): Promise<ReadResult> {
  const authError = keyValidationError(await validateKey(secretKey));
  if (authError) {
    return { error: authError.message };
  }

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
  const chapters = detectChapters(pages);

  if (opts.listChapters) {
    if (chapters.length === 0) {
      return {
        text: `Document: ${pageCount} pages, format: ${format}\n\nNo chapter structure detected. Use start_page/end_page to read specific pages.`,
        pageCount,
        format,
        chapters: [],
      };
    }
    const lines = [
      `Document: ${pageCount} pages, ${chapters.length} chapters, format: ${format}`,
      "",
      "Chapters:",
    ];
    for (const c of chapters) {
      lines.push(`  ${c.index}. ${c.title} (pp. ${c.startPage}-${c.endPage})`);
    }
    return { text: lines.join("\n"), pageCount, format, chapters };
  }

  if (opts.chapter != null) {
    if (chapters.length === 0) {
      return {
        error: `No chapter structure detected in this ${format}. Use start_page/end_page instead, or list_chapters=true to confirm.`,
      };
    }
    const ch = chapters.find((c) => c.index === opts.chapter);
    if (!ch) {
      return {
        error: `Chapter ${opts.chapter} not found. Document has ${chapters.length} chapters (1-${chapters.length}). Use list_chapters=true to see them.`,
      };
    }

    const selected = pages.slice(ch.startPage - 1, ch.endPage);
    let body = selected
      .map((p, i) => `--- Page ${ch.startPage + i} ---\n${stripNativeMarkers(p)}`)
      .join("\n\n");

    let truncated = false;
    if (body.length > MAX_OUTPUT_CHARS) {
      body = body.slice(0, MAX_OUTPUT_CHARS);
      truncated = true;
    }

    const header = `Chapter ${ch.index} of ${chapters.length}: ${ch.title} (pp. ${ch.startPage}-${ch.endPage} of ${pageCount})`;
    const footer = truncated
      ? `\n\n[Truncated at ${MAX_OUTPUT_CHARS} chars. Use start_page/end_page within this chapter to continue.]`
      : "";
    return {
      text: `${header}\n\n${body}${footer}`,
      pageCount,
      format,
      chapters,
    };
  }

  const pageRange = opts.pageRange;

  if (!pageRange) {
    const preview = stripNativeMarkers(pages[0] || "").slice(0, 2000) || "[Empty document]";
    const chapterLine = chapters.length > 0
      ? `, ${chapters.length} chapters`
      : "";
    const chapterHint = chapters.length > 0
      ? ` Use list_chapters=true for the table of contents or chapter=N to read a chapter.`
      : "";
    return {
      text: `Document: ${pageCount} pages${chapterLine}, detected format: ${format}\n\n--- Page 1 preview ---\n${preview}\n\n[Request specific pages with start_page/end_page.${chapterHint}]`,
      pageCount,
      format,
      chapters,
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
    .map((p, i) => `--- Page ${startPage + i + 1} ---\n${stripNativeMarkers(p)}`)
    .join("\n\n");

  if (text.length > MAX_OUTPUT_CHARS) {
    text = text.slice(0, MAX_OUTPUT_CHARS) + `\n\n[Truncated at ${MAX_OUTPUT_CHARS} chars. Request a smaller page range.]`;
  }

  return {
    text: `Pages ${startPage + 1}-${endPage + 1} of ${pageCount} (${format}):\n\n${text}`,
    pageCount,
    format,
    chapters,
  };
}
