import fs from "fs";
import path from "path";

interface CacheEntry {
  path: string;
  size: number;
  accessedAt: number;
}

/**
 * Simple LRU file cache with a max size limit.
 * Evicts least-recently-accessed files when the cache exceeds maxBytes.
 */
export class FileCache {
  private entries = new Map<string, CacheEntry>();
  private currentSize = 0;
  private maxBytes: number;
  private baseDir: string;

  constructor(baseDir: string, maxBytes: number) {
    this.baseDir = baseDir;
    this.maxBytes = maxBytes;
    fs.mkdirSync(baseDir, { recursive: true });
    this.loadExisting();
  }

  /** Scan existing files on startup to populate the LRU */
  private loadExisting() {
    try {
      for (const subdir of fs.readdirSync(this.baseDir)) {
        const subdirPath = path.join(this.baseDir, subdir);
        if (!fs.statSync(subdirPath).isDirectory()) continue;
        for (const file of fs.readdirSync(subdirPath)) {
          const filePath = path.join(subdirPath, file);
          const stat = fs.statSync(filePath);
          if (!stat.isFile()) continue;
          this.entries.set(file, {
            path: filePath,
            size: stat.size,
            accessedAt: stat.atimeMs,
          });
          this.currentSize += stat.size;
        }
      }
      this.evictIfNeeded();
    } catch {
      // Empty cache dir, that's fine
    }
  }

  /** Get a cached file path, or null if not cached */
  get(key: string): string | null {
    const entry = this.entries.get(key);
    if (!entry || !fs.existsSync(entry.path)) {
      if (entry) {
        this.currentSize -= entry.size;
        this.entries.delete(key);
      }
      return null;
    }
    // Touch — update access time
    entry.accessedAt = Date.now();
    return entry.path;
  }

  /** Get the path where a file should be stored (doesn't create it) */
  pathFor(key: string, ext?: string): string {
    const md5 = key.replace(/\.[^.]+$/, "");
    const subdir = path.join(this.baseDir, md5.slice(0, 2));
    fs.mkdirSync(subdir, { recursive: true });
    return path.join(subdir, ext ? `${md5}.${ext}` : key);
  }

  /** Register a file that was just written to the cache */
  put(key: string, filePath: string) {
    const stat = fs.statSync(filePath);
    const existing = this.entries.get(key);
    if (existing) {
      this.currentSize -= existing.size;
    }
    this.entries.set(key, {
      path: filePath,
      size: stat.size,
      accessedAt: Date.now(),
    });
    this.currentSize += stat.size;
    this.evictIfNeeded();
  }

  /** Evict least-recently-accessed entries until under maxBytes */
  private evictIfNeeded() {
    if (this.currentSize <= this.maxBytes) return;

    // Sort by access time, oldest first
    const sorted = [...this.entries.entries()].sort(
      (a, b) => a[1].accessedAt - b[1].accessedAt
    );

    for (const [key, entry] of sorted) {
      if (this.currentSize <= this.maxBytes * 0.8) break; // Evict to 80% to avoid thrashing
      try {
        fs.unlinkSync(entry.path);
      } catch {
        // File already gone
      }
      this.currentSize -= entry.size;
      this.entries.delete(key);
    }
  }

  /** Current cache size in bytes */
  get size() {
    return this.currentSize;
  }

  /** Number of entries */
  get count() {
    return this.entries.size;
  }
}
