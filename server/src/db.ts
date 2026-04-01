import pg from "pg";

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
});

export interface Document {
  md5: string;
  title: string | null;
  author: string | null;
  publisher: string | null;
  language: string | null;
  year: number | null;
  extension: string | null;
  filesize: number | null;
  source: string;
  doi: string | null;
  isbn: string | null;
  pages: string | null;
  series: string | null;
  description: string | null;
}

export interface SearchOptions {
  query?: string;
  title?: string;
  author?: string;
  yearFrom?: number;
  yearTo?: number;
  publisher?: string;
  language?: string;
  format?: string;
  isbn?: string;
  doi?: string;
  limit?: number;
}

const COLUMNS = `md5, title, author, publisher, language, year, extension, filesize, source, doi, isbn, pages, series, description`;

export async function search(opts: SearchOptions): Promise<Document[]> {
  const limit = Math.min(opts.limit ?? 10, 50);

  // Direct lookups — isbn/doi are exact match
  if (opts.doi) {
    const result = await pool.query(
      `SELECT ${COLUMNS} FROM documents WHERE doi = $1 LIMIT $2`,
      [opts.doi, limit]
    );
    return result.rows;
  }
  if (opts.isbn) {
    const cleaned = opts.isbn.replace(/[-\s]/g, "");
    const result = await pool.query(
      `SELECT ${COLUMNS} FROM documents WHERE isbn = $1 LIMIT $2`,
      [cleaned, limit]
    );
    return result.rows;
  }

  // Auto-detect DOI/ISBN in general query
  if (opts.query && !opts.title && !opts.author) {
    if (/^10\.\d{4,9}\//.test(opts.query)) {
      const result = await pool.query(
        `SELECT ${COLUMNS} FROM documents WHERE doi = $1 LIMIT $2`,
        [opts.query, limit]
      );
      if (result.rows.length > 0) return result.rows;
    }
    const stripped = opts.query.replace(/[-\s]/g, "");
    if (/^\d{10}(\d{3})?$/.test(stripped)) {
      const result = await pool.query(
        `SELECT ${COLUMNS} FROM documents WHERE isbn = $1 LIMIT $2`,
        [stripped, limit]
      );
      if (result.rows.length > 0) return result.rows;
    }
  }

  // Build dynamic FTS query
  const params: (string | number)[] = [];
  let paramIdx = 1;
  const conditions: string[] = [];
  const rankExprs: string[] = [];

  if (opts.query) {
    conditions.push(`search_vector @@ plainto_tsquery('english_unaccent', $${paramIdx})`);
    rankExprs.push(`ts_rank(search_vector, plainto_tsquery('english_unaccent', $${paramIdx}))`);
    params.push(opts.query);
    paramIdx++;
  }

  if (opts.title) {
    conditions.push(`to_tsvector('english_unaccent', coalesce(title, '')) @@ plainto_tsquery('english_unaccent', $${paramIdx})`);
    rankExprs.push(`ts_rank(to_tsvector('english_unaccent', coalesce(title, '')), plainto_tsquery('english_unaccent', $${paramIdx}))`);
    params.push(opts.title);
    paramIdx++;
  }

  if (opts.author) {
    conditions.push(`to_tsvector('english_unaccent', coalesce(author, '')) @@ plainto_tsquery('english_unaccent', $${paramIdx})`);
    rankExprs.push(`ts_rank(to_tsvector('english_unaccent', coalesce(author, '')), plainto_tsquery('english_unaccent', $${paramIdx}))`);
    params.push(opts.author);
    paramIdx++;
  }

  if (opts.publisher) {
    conditions.push(`to_tsvector('english_unaccent', coalesce(publisher, '')) @@ plainto_tsquery('english_unaccent', $${paramIdx})`);
    params.push(opts.publisher);
    paramIdx++;
  }

  // Filters
  if (opts.yearFrom != null) {
    conditions.push(`year >= $${paramIdx++}`);
    params.push(opts.yearFrom);
  }
  if (opts.yearTo != null) {
    conditions.push(`year <= $${paramIdx++}`);
    params.push(opts.yearTo);
  }
  if (opts.language) {
    conditions.push(`language = $${paramIdx++}`);
    params.push(opts.language);
  }
  if (opts.format) {
    conditions.push(`extension = $${paramIdx++}`);
    params.push(opts.format);
  }

  if (conditions.length === 0) {
    return [];
  }

  const rankExpr = rankExprs.length > 0
    ? `, ${rankExprs.join(" + ")} AS rank`
    : "";
  const orderBy = rankExprs.length > 0
    ? "ORDER BY rank DESC"
    : "ORDER BY date_added DESC NULLS LAST";

  const sql = `SELECT ${COLUMNS}${rankExpr}
    FROM documents
    WHERE ${conditions.join(" AND ")}
    ${orderBy}
    LIMIT $${paramIdx}`;
  params.push(limit);

  let result = await pool.query(sql, params);

  // OR fallback — only for general `query` when AND returns nothing
  if (result.rows.length === 0 && opts.query && !opts.title && !opts.author) {
    const words = opts.query.trim().split(/\s+/).filter((w) => w.length > 1);
    if (words.length >= 2) {
      const orQuery = words
        .map((w) => w.replace(/[^a-zA-Z0-9\u4e00-\u9fff]/g, ""))
        .filter(Boolean)
        .join(" | ");
      if (orQuery) {
        const orParams: (string | number)[] = [orQuery];
        let orIdx = 2;
        const orConds = [`search_vector @@ to_tsquery('english_unaccent', $1)`];

        if (opts.yearFrom != null) { orConds.push(`year >= $${orIdx++}`); orParams.push(opts.yearFrom); }
        if (opts.yearTo != null) { orConds.push(`year <= $${orIdx++}`); orParams.push(opts.yearTo); }
        if (opts.language) { orConds.push(`language = $${orIdx++}`); orParams.push(opts.language); }
        if (opts.format) { orConds.push(`extension = $${orIdx++}`); orParams.push(opts.format); }

        const orSql = `SELECT ${COLUMNS},
            ts_rank(search_vector, to_tsquery('english_unaccent', $1)) AS rank
          FROM documents WHERE ${orConds.join(" AND ")}
          ORDER BY rank DESC LIMIT $${orIdx}`;
        orParams.push(limit);
        result = await pool.query(orSql, orParams);
      }
    }
  }

  // Trigram fallback — single-word general query, no specific fields
  if (result.rows.length === 0 && opts.query && !opts.title && !opts.author) {
    const words = opts.query.trim().split(/\s+/);
    if (words.length <= 1) {
      const triParams: (string | number)[] = [opts.query];
      let triIdx = 2;
      const triConds = [`(similarity(title, $1) > 0.3 OR similarity(author, $1) > 0.3)`];

      if (opts.yearFrom != null) { triConds.push(`year >= $${triIdx++}`); triParams.push(opts.yearFrom); }
      if (opts.yearTo != null) { triConds.push(`year <= $${triIdx++}`); triParams.push(opts.yearTo); }
      if (opts.language) { triConds.push(`language = $${triIdx++}`); triParams.push(opts.language); }
      if (opts.format) { triConds.push(`extension = $${triIdx++}`); triParams.push(opts.format); }

      const triSql = `SELECT ${COLUMNS}
        FROM documents WHERE ${triConds.join(" AND ")}
        ORDER BY greatest(similarity(title, $1), similarity(author, $1)) DESC LIMIT $${triIdx}`;
      triParams.push(limit);
      result = await pool.query(triSql, triParams);
    }
  }

  return result.rows;
}

export async function getByMd5(md5: string): Promise<Document | null> {
  const result = await pool.query(
    `SELECT ${COLUMNS} FROM documents WHERE md5 = $1 LIMIT 1`,
    [md5]
  );
  return result.rows[0] ?? null;
}

export async function getStats(): Promise<{ total: number; by_source: Record<string, number> }> {
  const totalResult = await pool.query("SELECT count(*)::int AS total FROM documents");
  const sourceResult = await pool.query(
    "SELECT source, count(*)::int AS count FROM documents GROUP BY source ORDER BY count DESC"
  );
  const by_source: Record<string, number> = {};
  for (const row of sourceResult.rows) {
    by_source[row.source] = row.count;
  }
  return { total: totalResult.rows[0].total, by_source };
}

export { pool };
