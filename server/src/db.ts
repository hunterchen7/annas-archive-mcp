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

interface SearchOptions {
  query: string;
  language?: string;
  format?: string;
  limit?: number;
}

const DOI_REGEX = /^10\.\d{4,9}\//;
const ISBN_REGEX = /^(?:\d{10}|\d{13})$/;

function detectQueryType(query: string): "doi" | "isbn" | "text" {
  if (DOI_REGEX.test(query)) return "doi";
  const stripped = query.replace(/[-\s]/g, "");
  if (ISBN_REGEX.test(stripped)) return "isbn";
  return "text";
}

export async function search(opts: SearchOptions): Promise<Document[]> {
  const limit = Math.min(opts.limit ?? 10, 50);
  const queryType = detectQueryType(opts.query);

  let sql: string;
  const params: (string | number)[] = [];
  let paramIdx = 1;

  if (queryType === "doi") {
    sql = `SELECT md5, title, author, publisher, language, year, extension, filesize, source, doi, isbn, pages, series, description
           FROM documents WHERE doi = $${paramIdx++}`;
    params.push(opts.query);
  } else if (queryType === "isbn") {
    const stripped = opts.query.replace(/[-\s]/g, "");
    sql = `SELECT md5, title, author, publisher, language, year, extension, filesize, source, doi, isbn, pages, series, description
           FROM documents WHERE isbn = $${paramIdx++}`;
    params.push(stripped);
  } else {
    // Full-text search with trigram fallback
    sql = `SELECT md5, title, author, publisher, language, year, extension, filesize, source, doi, isbn, pages, series, description,
             ts_rank(search_vector, plainto_tsquery('english_unaccent', $${paramIdx})) AS rank
           FROM documents
           WHERE search_vector @@ plainto_tsquery('english_unaccent', $${paramIdx++})`;
    params.push(opts.query);
  }

  if (opts.language) {
    sql += ` AND language = $${paramIdx++}`;
    params.push(opts.language);
  }
  if (opts.format) {
    sql += ` AND extension = $${paramIdx++}`;
    params.push(opts.format);
  }

  if (queryType === "text") {
    sql += ` ORDER BY rank DESC`;
  }

  sql += ` LIMIT $${paramIdx++}`;
  params.push(limit);

  let result = await pool.query(sql, params);

  // Fallback 1: OR matching when AND returns no results (for 2+ word queries)
  if (queryType === "text" && result.rows.length === 0) {
    const words = opts.query.trim().split(/\s+/).filter((w) => w.length > 1);
    if (words.length >= 2) {
      const orQuery = words.map((w) => w.replace(/[^a-zA-Z0-9\u4e00-\u9fff]/g, "")).filter(Boolean).join(" | ");
      if (orQuery) {
        const orParams: (string | number)[] = [orQuery];
        let orIdx = 2;
        let orSql = `SELECT md5, title, author, publisher, language, year, extension, filesize, source, doi, isbn, pages, series, description,
                       ts_rank(search_vector, to_tsquery('english_unaccent', $1)) AS rank
                     FROM documents
                     WHERE search_vector @@ to_tsquery('english_unaccent', $1)`;
        if (opts.language) {
          orSql += ` AND language = $${orIdx++}`;
          orParams.push(opts.language);
        }
        if (opts.format) {
          orSql += ` AND extension = $${orIdx++}`;
          orParams.push(opts.format);
        }
        orSql += ` ORDER BY rank DESC LIMIT $${orIdx++}`;
        orParams.push(limit);
        result = await pool.query(orSql, orParams);
      }
    }
  }

  // Fallback 2: Trigram similarity for single-word queries (typo correction)
  if (queryType === "text" && result.rows.length === 0) {
    const words = opts.query.trim().split(/\s+/);
    if (words.length <= 1) {
      const trigramParams: (string | number)[] = [opts.query];
      let triIdx = 2;
      let trigramSql = `SELECT md5, title, author, publisher, language, year, extension, filesize, source, doi, isbn, pages, series, description
                        FROM documents
                        WHERE similarity(title, $1) > 0.3 OR similarity(author, $1) > 0.3`;
      if (opts.language) {
        trigramSql += ` AND language = $${triIdx++}`;
        trigramParams.push(opts.language);
      }
      if (opts.format) {
        trigramSql += ` AND extension = $${triIdx++}`;
        trigramParams.push(opts.format);
      }
      trigramSql += ` ORDER BY greatest(similarity(title, $1), similarity(author, $1)) DESC LIMIT $${triIdx++}`;
      trigramParams.push(limit);
      result = await pool.query(trigramSql, trigramParams);
    }
  }

  return result.rows;
}

export async function getByMd5(md5: string): Promise<Document | null> {
  const result = await pool.query(
    `SELECT md5, title, author, publisher, language, year, extension, filesize, source, doi, isbn, pages, series, description
     FROM documents WHERE md5 = $1 LIMIT 1`,
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
