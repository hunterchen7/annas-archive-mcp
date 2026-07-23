import fs from "node:fs";
import path from "node:path";
import { runTextCommand } from "./command.js";

const MAX_ARCHIVE_ENTRIES = 2_000;
const MAX_ARCHIVE_BYTES = 256 * 1024 * 1024;
const MAX_ARCHIVE_PATH_LENGTH = 1_024;

function isInside(root: string, candidate: string): boolean {
  const relative = path.relative(root, candidate);
  return relative === "" || (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative));
}

export function validateArchiveEntryNames(names: readonly string[]): void {
  if (names.length > MAX_ARCHIVE_ENTRIES) {
    throw new Error(`Archive has more than ${MAX_ARCHIVE_ENTRIES} entries`);
  }
  for (const rawName of names) {
    if (!rawName || rawName.length > MAX_ARCHIVE_PATH_LENGTH || /[\0\r\n]/.test(rawName)) {
      throw new Error("Archive contains an invalid entry name");
    }
    const name = rawName.replaceAll("\\", "/");
    if (name.startsWith("/") || /^[a-zA-Z]:/.test(name)) {
      throw new Error("Archive contains an absolute path");
    }
    if (name.split("/").some((part) => part === "..")) {
      throw new Error("Archive contains a parent-directory traversal");
    }
  }
}

export function sumZipListingBytes(listing: string, expectedEntries?: number): number {
  let total = 0;
  let parsedEntries = 0;
  for (const line of listing.split(/\r?\n/)) {
    const match = line.match(
      /^\s*(\d+)\s+(?:\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{4})\s+\d{2}:\d{2}\s+/,
    );
    if (!match) continue;
    parsedEntries++;
    const size = Number(match[1]);
    if (!Number.isSafeInteger(size) || size < 0 || size > MAX_ARCHIVE_BYTES) {
      throw new Error("Archive entry is too large");
    }
    total += size;
    if (total > MAX_ARCHIVE_BYTES) {
      throw new Error(`Archive expands beyond ${MAX_ARCHIVE_BYTES} bytes`);
    }
  }
  if (expectedEntries !== undefined && parsedEntries !== expectedEntries) {
    throw new Error(
      `Could not verify every archive entry size (expected ${expectedEntries}, parsed ${parsedEntries})`,
    );
  }
  return total;
}

export function inspectZipArchive(filePath: string): void {
  const names = runTextCommand("unzip", ["-Z1", filePath], {
    timeoutMs: 10_000,
    maxBufferBytes: 10 * 1024 * 1024,
  }).split(/\r?\n/).filter(Boolean);
  validateArchiveEntryNames(names);

  const longListing = runTextCommand("unzip", ["-Z", "-l", filePath], {
    timeoutMs: 10_000,
    maxBufferBytes: 10 * 1024 * 1024,
  });
  if (longListing.split(/\r?\n/).some((line) => /^l[rwx-]{9}\s/.test(line))) {
    throw new Error("Archive contains a symbolic link");
  }

  const sizeListing = runTextCommand("unzip", ["-l", filePath], {
    timeoutMs: 10_000,
    maxBufferBytes: 10 * 1024 * 1024,
  });
  sumZipListingBytes(sizeListing, names.length);
}

export function resolveSafeFile(
  rootDir: string,
  relativePath: string,
  baseDir = rootDir,
): string | null {
  if (!relativePath || /[\0\r\n]/.test(relativePath)) return null;
  const normalizedInput = relativePath.replaceAll("\\", "/");
  const root = path.resolve(rootDir);
  const candidate = path.resolve(baseDir, normalizedInput);
  if (!isInside(root, candidate)) return null;

  try {
    const stat = fs.lstatSync(candidate);
    if (!stat.isFile() || stat.isSymbolicLink()) return null;
    const realRoot = fs.realpathSync(root);
    const realCandidate = fs.realpathSync(candidate);
    return isInside(realRoot, realCandidate) ? realCandidate : null;
  } catch {
    return null;
  }
}

export function listSafeRegularFiles(
  rootDir: string,
  extensions: ReadonlySet<string>,
): string[] {
  const root = fs.realpathSync(rootDir);
  const pending = [root];
  const files: string[] = [];
  let totalBytes = 0;
  let entries = 0;

  while (pending.length > 0) {
    const dir = pending.pop()!;
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      entries++;
      if (entries > MAX_ARCHIVE_ENTRIES) {
        throw new Error(`Extracted archive has more than ${MAX_ARCHIVE_ENTRIES} entries`);
      }
      const entryPath = path.join(dir, entry.name);
      const stat = fs.lstatSync(entryPath);
      if (stat.isSymbolicLink()) throw new Error("Extracted archive contains a symbolic link");
      if (stat.isDirectory()) {
        pending.push(entryPath);
        continue;
      }
      if (!stat.isFile()) throw new Error("Extracted archive contains a special file");

      const realPath = fs.realpathSync(entryPath);
      if (!isInside(root, realPath)) throw new Error("Extracted archive escaped its temporary directory");
      totalBytes += stat.size;
      if (totalBytes > MAX_ARCHIVE_BYTES) {
        throw new Error(`Extracted archive is larger than ${MAX_ARCHIVE_BYTES} bytes`);
      }
      if (extensions.has(path.extname(entry.name).toLowerCase())) files.push(realPath);
    }
  }
  return files.sort();
}
