import fs from "fs";
import path from "path";
import { isMd5 } from "./identifiers.js";

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
    const baseStat = fs.lstatSync(baseDir);
    if (!baseStat.isDirectory() || baseStat.isSymbolicLink()) {
      throw new Error("Cache root must be a real directory");
    }
    this.baseDir = fs.realpathSync(baseDir);
    this.loadExisting();
  }

  private isInsideBase(candidate: string): boolean {
    const relative = path.relative(this.baseDir, candidate);
    return relative === "" ||
      (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative));
  }

  /** Scan existing files on startup to populate the LRU */
  private loadExisting() {
    try {
      for (const subdir of fs.readdirSync(this.baseDir)) {
        if (!/^[a-f0-9]{2}$/i.test(subdir)) continue;
        const subdirPath = path.join(this.baseDir, subdir);
        const subdirStat = fs.lstatSync(subdirPath);
        if (!subdirStat.isDirectory() || subdirStat.isSymbolicLink()) continue;
        for (const file of fs.readdirSync(subdirPath)) {
          const filePath = path.join(subdirPath, file);
          if (!/^[a-f0-9]{32}\.[a-z0-9]{1,10}$/i.test(file)) continue;
          const stat = fs.lstatSync(filePath);
          if (!stat.isFile() || stat.isSymbolicLink()) continue;
          const realPath = fs.realpathSync(filePath);
          if (!this.isInsideBase(realPath)) continue;
          this.entries.set(file, {
            path: realPath,
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
    let valid = false;
    if (entry) {
      try {
        const stat = fs.lstatSync(entry.path);
        valid = stat.isFile() &&
          !stat.isSymbolicLink() &&
          this.isInsideBase(fs.realpathSync(entry.path));
      } catch {
        valid = false;
      }
    }
    if (!entry || !valid) {
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
    if (!isMd5(md5)) {
      throw new Error("Cache key must contain a 32-character hexadecimal MD5");
    }
    const suffix = ext ?? path.extname(key).slice(1);
    if (!/^[a-z0-9]{1,10}$/i.test(suffix)) {
      throw new Error("Cache extension must contain only letters and numbers");
    }
    const subdir = path.join(this.baseDir, md5.slice(0, 2));
    fs.mkdirSync(subdir, { recursive: true });
    const subdirStat = fs.lstatSync(subdir);
    if (!subdirStat.isDirectory() || subdirStat.isSymbolicLink()) {
      throw new Error("Cache shard must be a real directory");
    }
    const realSubdir = fs.realpathSync(subdir);
    if (!this.isInsideBase(realSubdir)) {
      throw new Error("Cache shard escaped the configured root");
    }
    return path.join(realSubdir, `${md5}.${suffix}`);
  }

  /** Register a file that was just written to the cache */
  put(key: string, filePath: string) {
    const expectedPath = this.pathFor(key);
    if (path.resolve(filePath) !== expectedPath) {
      throw new Error("Cache file path does not match its key");
    }
    const stat = fs.lstatSync(filePath);
    if (!stat.isFile() || stat.isSymbolicLink()) {
      throw new Error("Cache entry must be a regular file");
    }
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
