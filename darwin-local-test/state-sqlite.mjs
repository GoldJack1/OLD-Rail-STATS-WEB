/**
 * Dual-run SQLite store for Darwin unit catalog.
 * History gzip shards stay on disk — copying them here would duplicate ~100GB.
 * JSON catalog remains the rollback path.
 *
 * Catalog is one gzipped JSON blob (smaller on disk than unit-catalog.json,
 * same in-memory Map after load). Row-per-service tables were larger than JSON.
 *
 * Env:
 *   DARWIN_SQLITE_PATH     default state/darwin-state.sqlite
 * Unit catalog is SQLite-only. unit-catalog.json is not written; it may still
 * be read if sqlite load fails.
 */
import { existsSync, mkdirSync, statSync } from 'node:fs';
import { dirname } from 'node:path';
import { gzipSync, gunzipSync } from 'node:zlib';

export function parseStoreEnv() {
  return {
    store: 'sqlite',
    jsonWrite: false,
    sqliteWrite: true,
  };
}

export async function createStateSqlite(dbPath) {
  let DatabaseSync;
  try {
    ({ DatabaseSync } = await import('node:sqlite'));
  } catch (e) {
    return { ok: false, error: e.message, handle: null };
  }
  try {
    mkdirSync(dirname(dbPath), { recursive: true });
    const db = new DatabaseSync(dbPath);
    db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA busy_timeout = 8000;
      PRAGMA synchronous = NORMAL;
      CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT
      );
      CREATE TABLE IF NOT EXISTS catalog_blob (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        saved_at TEXT,
        unit_count INTEGER,
        payload BLOB
      );
    `);
    db.prepare('INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)').run('schema', '2');
    try {
      db.exec(`
        DROP TABLE IF EXISTS unit_services;
        DROP TABLE IF EXISTS unit_mileage;
        DROP TABLE IF EXISTS units;
      `);
      db.exec('VACUUM');
    } catch {}
    return { ok: true, error: null, handle: wrapDb(db, dbPath) };
  } catch (e) {
    return { ok: false, error: e.message, handle: null };
  }
}

function wrapDb(db, dbPath) {
  const upsertBlob = db.prepare(`
    INSERT INTO catalog_blob (id, saved_at, unit_count, payload)
    VALUES (1, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      saved_at = excluded.saved_at,
      unit_count = excluded.unit_count,
      payload = excluded.payload
  `);
  const selectBlob = db.prepare('SELECT saved_at, unit_count, payload FROM catalog_blob WHERE id = 1');
  const setMeta = db.prepare('INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)');
  const getMeta = db.prepare('SELECT value FROM meta WHERE key = ?');

  function saveCatalog(unitCatalogById, savedAt) {
    const payload = {
      savedAt,
      units: [...unitCatalogById.entries()],
    };
    const gz = gzipSync(Buffer.from(JSON.stringify(payload), 'utf8'), { level: 6 });
    db.exec('BEGIN IMMEDIATE');
    try {
      upsertBlob.run(savedAt, unitCatalogById.size, gz);
      setMeta.run('unit_catalog_saved_at', savedAt);
      setMeta.run('unit_count', String(unitCatalogById.size));
      db.exec('COMMIT');
    } catch (e) {
      try { db.exec('ROLLBACK'); } catch {}
      throw e;
    }
    try { db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); } catch {}
  }

  function loadCatalog() {
    const row = selectBlob.get();
    if (!row?.payload) return null;
    const json = gunzipSync(Buffer.from(row.payload)).toString('utf8');
    const parsed = JSON.parse(json);
    if (!parsed?.units?.length) return null;
    return {
      savedAt: parsed.savedAt || row.saved_at || getMeta.get('unit_catalog_saved_at')?.value || null,
      units: parsed.units,
    };
  }

  function stats() {
    let size = null;
    try { size = statSync(dbPath).size; } catch {}
    let n = 0;
    let savedAt = null;
    let blobBytes = null;
    try {
      const row = selectBlob.get();
      n = row?.unit_count || 0;
      savedAt = row?.saved_at || null;
      blobBytes = row?.payload ? Buffer.byteLength(row.payload) : null;
    } catch {}
    return {
      path: dbPath,
      bytes: size,
      blobBytes,
      units: n,
      savedAt,
      exists: existsSync(dbPath),
    };
  }

  function close() {
    try { db.close(); } catch {}
  }

  return { saveCatalog, loadCatalog, stats, close };
}
