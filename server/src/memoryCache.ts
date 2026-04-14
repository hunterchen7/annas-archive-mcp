interface Entry {
  value: string;
  size: number;
  accessedAt: number;
}

// Bounded LRU for extracted text. No disk, no files — just strings in a Map.
// Size tracked by UTF-8 byte length; evicted to 80% of cap on overflow.
export class MemoryTextCache {
  private entries = new Map<string, Entry>();
  private currentSize = 0;
  private maxBytes: number;

  constructor(maxBytes: number) {
    this.maxBytes = maxBytes;
  }

  get(key: string): string | null {
    const entry = this.entries.get(key);
    if (!entry) return null;
    entry.accessedAt = Date.now();
    return entry.value;
  }

  put(key: string, value: string): void {
    const size = Buffer.byteLength(value, "utf-8");
    const existing = this.entries.get(key);
    if (existing) this.currentSize -= existing.size;
    this.entries.set(key, { value, size, accessedAt: Date.now() });
    this.currentSize += size;
    this.evictIfNeeded();
  }

  private evictIfNeeded() {
    if (this.currentSize <= this.maxBytes) return;
    const sorted = [...this.entries.entries()].sort(
      (a, b) => a[1].accessedAt - b[1].accessedAt,
    );
    for (const [key, entry] of sorted) {
      if (this.currentSize <= this.maxBytes * 0.8) break;
      this.entries.delete(key);
      this.currentSize -= entry.size;
    }
  }

  get size(): number { return this.currentSize; }
  get count(): number { return this.entries.size; }
}
