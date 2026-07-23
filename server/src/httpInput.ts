import type { SearchOptions } from "./db.js";
import type { ReadOptions } from "./reader.js";

export type ParseResult<T> =
  | { ok: true; value: T }
  | { ok: false; error: string };

const SEARCH_TEXT_MAX = 256;
const DOI_MAX = 256;
const ISBN_MAX = 32;
const FILTER_MAX = 64;
const MAX_PAGE = 1_000_000;
const MAX_PAGE_SPAN = 100;

function optionalString(
  query: Record<string, unknown>,
  name: string,
  maxLength: number,
): ParseResult<string | undefined> {
  const value = query[name];
  if (value === undefined) return { ok: true, value: undefined };
  if (typeof value !== "string") {
    return { ok: false, error: `${name} must be provided once as a string.` };
  }
  const trimmed = value.trim();
  if (!trimmed) return { ok: false, error: `${name} must not be empty.` };
  if (trimmed.length > maxLength) {
    return { ok: false, error: `${name} must be at most ${maxLength} characters.` };
  }
  return { ok: true, value: trimmed };
}

function optionalInteger(
  query: Record<string, unknown>,
  name: string,
  min: number,
  max: number,
): ParseResult<number | undefined> {
  const value = query[name];
  if (value === undefined) return { ok: true, value: undefined };
  if (typeof value !== "string" || !/^-?\d+$/.test(value)) {
    return { ok: false, error: `${name} must be an integer.` };
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) {
    return { ok: false, error: `${name} must be between ${min} and ${max}.` };
  }
  return { ok: true, value: parsed };
}

export function parseSearchQuery(
  query: Record<string, unknown>,
): ParseResult<SearchOptions> {
  const stringFields = {
    query: SEARCH_TEXT_MAX,
    title: SEARCH_TEXT_MAX,
    author: SEARCH_TEXT_MAX,
    publisher: SEARCH_TEXT_MAX,
    isbn: ISBN_MAX,
    doi: DOI_MAX,
    language: FILTER_MAX,
    format: FILTER_MAX,
  } as const;
  const values: Record<string, string | undefined> = {};
  for (const [name, max] of Object.entries(stringFields)) {
    const result = optionalString(query, name, max);
    if (!result.ok) return result;
    values[name] = result.value;
  }

  const yearFrom = optionalInteger(query, "year_from", 0, 3000);
  if (!yearFrom.ok) return yearFrom;
  const yearTo = optionalInteger(query, "year_to", 0, 3000);
  if (!yearTo.ok) return yearTo;
  const limit = optionalInteger(query, "limit", 1, 50);
  if (!limit.ok) return limit;
  if (yearFrom.value !== undefined && yearTo.value !== undefined && yearFrom.value > yearTo.value) {
    return { ok: false, error: "year_from must not be greater than year_to." };
  }

  return {
    ok: true,
    value: {
      query: values.query,
      title: values.title,
      author: values.author,
      publisher: values.publisher,
      isbn: values.isbn,
      doi: values.doi,
      language: values.language,
      format: values.format,
      yearFrom: yearFrom.value,
      yearTo: yearTo.value,
      limit: limit.value ?? 10,
    },
  };
}

export function parseReadQuery(
  query: Record<string, unknown>,
): ParseResult<ReadOptions> {
  const start = optionalInteger(query, "start_page", 1, MAX_PAGE);
  if (!start.ok) return start;
  const end = optionalInteger(query, "end_page", 1, MAX_PAGE);
  if (!end.ok) return end;
  const chapter = optionalInteger(query, "chapter", 1, MAX_PAGE);
  if (!chapter.ok) return chapter;

  const rawList = query.list_chapters;
  if (
    rawList !== undefined &&
    rawList !== "true" &&
    rawList !== "1" &&
    rawList !== "false" &&
    rawList !== "0"
  ) {
    return { ok: false, error: "list_chapters must be true, false, 1, or 0." };
  }
  const listChapters = rawList === "true" || rawList === "1";

  if (end.value !== undefined && start.value === undefined) {
    return { ok: false, error: "end_page requires start_page." };
  }
  if (
    chapter.value !== undefined &&
    (start.value !== undefined || end.value !== undefined || listChapters)
  ) {
    return { ok: false, error: "chapter cannot be combined with page ranges or list_chapters." };
  }
  if (listChapters && (start.value !== undefined || end.value !== undefined)) {
    return { ok: false, error: "list_chapters cannot be combined with a page range." };
  }
  const effectiveEnd = start.value === undefined
    ? undefined
    : end.value ?? Math.min(start.value + 19, MAX_PAGE);
  if (start.value !== undefined && effectiveEnd !== undefined) {
    if (effectiveEnd < start.value) {
      return { ok: false, error: "end_page must not be less than start_page." };
    }
    if (effectiveEnd - start.value + 1 > MAX_PAGE_SPAN) {
      return { ok: false, error: `A read request can include at most ${MAX_PAGE_SPAN} pages.` };
    }
  }

  if (listChapters) return { ok: true, value: { listChapters: true } };
  if (chapter.value !== undefined) {
    return { ok: true, value: { chapter: chapter.value } };
  }
  if (start.value !== undefined && effectiveEnd !== undefined) {
    return { ok: true, value: { pageRange: `${start.value}-${effectiveEnd}` } };
  }
  return { ok: true, value: {} };
}
