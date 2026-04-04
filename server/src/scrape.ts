import https from "https";

const DOMAINS = ["annas-archive.gl", "annas-archive.gd", "annas-archive.pk"];

export interface ScrapeResult {
  md5: string;
  title: string;
  author: string | null;
  publisher: string | null;
  year: number | null;
  language: string | null;
  extension: string | null;
  filesize: string | null;
  description: string | null;
  category: string | null;
  sources: string | null;
}

export interface ScrapeSearchOptions {
  query: string;
  extension?: string;
  language?: string;
  sort?: string;
}

function fetch(url: string, timeout = 15000): Promise<string> {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers: { "User-Agent": "Mozilla/5.0" } }, (res) => {
      if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetch(res.headers.location, timeout).then(resolve, reject);
      }
      if (res.statusCode && res.statusCode >= 400) {
        let body = "";
        res.on("data", (chunk) => (body += chunk));
        res.on("end", () => reject(new Error(`HTTP ${res.statusCode}: ${body.slice(0, 200)}`)));
        return;
      }
      let data = "";
      res.on("data", (chunk: string) => (data += chunk));
      res.on("end", () => resolve(data));
      res.on("error", reject);
    });
    req.on("error", reject);
    req.setTimeout(timeout, () => { req.destroy(); reject(new Error("Timeout")); });
  });
}

function decodeEntities(s: string): string {
  return s
    .replace(/&#39;/g, "'")
    .replace(/&#x27;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n)));
}

function parseResults(html: string): ScrapeResult[] {
  const results: ScrapeResult[] = [];
  const seen = new Set<string>();

  // Split by result blocks
  const blocks = html.split(/<div class="flex\s+pt-3/);

  for (const block of blocks) {
    // Extract MD5
    const md5Match = block.match(/href="\/md5\/([a-f0-9]{32})"/);
    if (!md5Match || seen.has(md5Match[1])) continue;
    const md5 = md5Match[1];
    seen.add(md5);

    // Title: main link with font-semibold text-lg
    const titleMatch = block.match(/font-semibold text-lg[^>]*>([^<]+)</);
    const title = titleMatch ? decodeEntities(titleMatch[1].trim()) : "Untitled";

    // Author: link with user-edit icon
    const authorMatch = block.match(/icon-\[mdi--user-edit\][^<]*<\/span>\s*([^<]+)/);
    const author = authorMatch ? decodeEntities(authorMatch[1].trim()) : null;

    // Publisher: link with company icon
    const pubMatch = block.match(/icon-\[mdi--company\][^<]*<\/span>\s*([^<]+)/);
    const publisher = pubMatch ? decodeEntities(pubMatch[1].trim()) : null;

    // Summary line: "English [en] · PDF · 1.4MB · 2013 · 📘 Book (non-fiction) · 🚀/lgli/lgrs"
    const summaryMatch = block.match(/class="text-gray-800[^"]*font-semibold text-sm[^"]*"[^>]*>([^]*?)<a/);
    let year: number | null = null;
    let language: string | null = null;
    let extension: string | null = null;
    let filesize: string | null = null;
    let category: string | null = null;
    let sources: string | null = null;

    if (summaryMatch) {
      const summary = decodeEntities(summaryMatch[1].replace(/<[^>]+>/g, "").trim());
      const parts = summary.split("·").map((s) => s.trim());
      for (const part of parts) {
        // Language: "English [en]"
        const langMatch = part.match(/^(\w[\w\s]*?)\s*\[(\w+)\]$/);
        if (langMatch) { language = langMatch[1].toLowerCase(); continue; }
        // Filesize: number + unit like "1.4MB"
        if (/^\d+(\.\d+)?\s*(B|KB|MB|GB|TB)$/i.test(part)) { filesize = part; continue; }
        // Year: 4-digit number
        if (/^\d{4}$/.test(part)) { year = parseInt(part); continue; }
        // Sources: 🚀/lgli/lgrs/... (rocket emoji + slashes)
        if (part.startsWith("\u{1F680}")) { sources = part; continue; }
        // Category: 📘 Book (non-fiction), 📗 Book (fiction), 📄 Paper, etc.
        if (/^[\u{1F300}-\u{1FAD6}]/u.test(part)) { category = part; continue; }
        // Extension: short alphabetic string like "PDF", "EPUB" (not a year)
        if (/^[a-zA-Z0-9]{2,5}$/.test(part)) { extension = part.toLowerCase(); continue; }
      }
    }

    // Description
    const descMatch = block.match(/text-gray-600[^"]*"[^>]*>([^]*?)<\/div>/);
    let description: string | null = null;
    if (descMatch) {
      const desc = decodeEntities(descMatch[1].replace(/<[^>]+>/g, "").trim());
      if (desc.length > 10) description = desc;
    }

    results.push({ md5, title, author, publisher, year, language, extension, filesize, description, category, sources });
  }

  return results;
}

export async function scrapeSearch(opts: ScrapeSearchOptions): Promise<{ results: ScrapeResult[]; error?: string }> {
  const params = new URLSearchParams();
  params.set("q", opts.query);
  if (opts.extension) params.set("ext", opts.extension);
  if (opts.language) params.set("lang", opts.language);
  if (opts.sort) params.set("sort", opts.sort);

  let html: string | undefined;
  let lastError = "";
  for (const domain of DOMAINS) {
    const url = `https://${domain}/search?${params.toString()}`;
    try {
      html = await fetch(url, 20000);
      break;
    } catch (e) {
      lastError = `${e}`;
    }
  }

  if (!html) {
    return { results: [], error: `All domains failed. Last error: ${lastError}` };
  }

  return { results: parseResults(html) };
}
