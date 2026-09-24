/**
 * 30s history layout: one manifest per day + content-hashed gzip blobs.
 * Old daemon-cache.<stamp> names remain readable until converted.
 */
import { basename, resolve } from 'node:path';
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { gunzipSync, gzipSync } from 'node:zlib';
import { createHash } from 'node:crypto';

export const HISTORY_MANIFEST_SCHEMA = 1;
export const HISTORY_MANIFEST_FILE = 'manifest.json';

export function historyManifestPath(dayDir) {
  return resolve(dayDir, HISTORY_MANIFEST_FILE);
}

export function blobBasenames(paths) {
  return (Array.isArray(paths) ? paths : []).filter(Boolean).map((p) => basename(p));
}

export function readHistoryManifest(dayDir) {
  const p = historyManifestPath(dayDir);
  if (!existsSync(p)) return null;
  try {
    const parsed = JSON.parse(readFileSync(p, 'utf8'));
    if (!parsed || !Array.isArray(parsed.snaps)) return null;
    return parsed;
  } catch {
    return null;
  }
}

export function writeHistoryManifest(dayDir, manifest) {
  if (!existsSync(dayDir)) mkdirSync(dayDir, { recursive: true });
  const p = historyManifestPath(dayDir);
  const tmp = `${p}.tmp`;
  writeFileSync(tmp, JSON.stringify(manifest));
  renameSync(tmp, p);
}

export function appendHistoryManifestSnap(dayDir, snap) {
  const prev = readHistoryManifest(dayDir) || {
    schema: HISTORY_MANIFEST_SCHEMA,
    snaps: [],
  };
  const snaps = Array.isArray(prev.snaps) ? prev.snaps.filter((s) => s && s.ms !== snap.ms) : [];
  snaps.push(snap);
  snaps.sort((a, b) => a.ms - b.ms);
  writeHistoryManifest(dayDir, {
    schema: HISTORY_MANIFEST_SCHEMA,
    date: prev.date || snap.date || null,
    builtAt: new Date().toISOString(),
    snaps,
  });
  return snaps.length;
}

export function hashJsonToHex(json) {
  return createHash('sha256').update(json).digest('hex');
}

export function writeCoreBlob(blobDir, coreJson, gzipLevel = 6) {
  if (!existsSync(blobDir)) mkdirSync(blobDir, { recursive: true });
  const hash = hashJsonToHex(coreJson);
  const name = `core.${hash}.json.gz`;
  const dest = resolve(blobDir, name);
  if (!existsSync(dest)) {
    const tmp = `${dest}.tmp`;
    writeFileSync(tmp, gzipSync(coreJson, { level: gzipLevel }));
    renameSync(tmp, dest);
  }
  return name;
}

export function manifestSnapsToList(dayDir, blobDir, snaps) {
  return (snaps || [])
    .filter((s) => s && Number.isFinite(s.ms) && s.core)
    .map((s) => ({
      file: `manifest:${s.ms}`,
      path: resolve(blobDir, s.core),
      savedAt: s.savedAt || null,
      ms: s.ms,
      manifestSnap: s,
      blobDir,
    }))
    .sort((a, b) => a.ms - b.ms);
}

function parseGzipJsonArray(gzPath) {
  try {
    const v = JSON.parse(gunzipSync(readFileSync(gzPath)).toString('utf8'));
    return Array.isArray(v) ? v : null;
  } catch {
    return null;
  }
}

export function attachHeavyFromManifestSnap(raw, blobDir, snap, opts = {}) {
  if (!raw || !blobDir || !snap) return raw;
  const attach = (field, names, skip) => {
    if (skip) return;
    const list = Array.isArray(names) ? names : names ? [names] : [];
    if (!list.length) return;
    const merged = [];
    for (const name of list) {
      const arr = parseGzipJsonArray(resolve(blobDir, name));
      if (arr) merged.push(...arr);
    }
    const prevN = Array.isArray(raw[field]) ? raw[field].length : 0;
    if (merged.length > 0 || prevN === 0) raw[field] = merged;
  };
  attach('formations', snap.formations, opts.skipFormations);
  attach('consistByRid', snap.consist, opts.skipConsist);
  attach('unitsById', snap.units, opts.skipUnits);
  attach('liveOverlayByRid', snap.overlay, opts.skipOverlay);
  return raw;
}

export function locActualSliceFromEntry(entry) {
  if (!entry || typeof entry !== 'object') return null;
  const isActual = (kind) => typeof kind === 'string' && kind.startsWith('actual');
  const out = {};
  if (isActual(entry.bestKind) && entry.bestTime) {
    out.bestTime = entry.bestTime;
    out.bestKind = entry.bestKind;
    if (entry.liveSource) out.liveSource = entry.liveSource;
    if (entry.liveSourceInstance) out.liveSourceInstance = entry.liveSourceInstance;
  }
  if (isActual(entry.arrLiveKind) && entry.arrLiveTime) {
    out.arrLiveTime = entry.arrLiveTime;
    out.arrLiveKind = entry.arrLiveKind;
    if (entry.arrLiveSource) out.arrLiveSource = entry.arrLiveSource;
    if (entry.arrLiveSourceInstance) out.arrLiveSourceInstance = entry.arrLiveSourceInstance;
  }
  return Object.keys(out).length ? out : null;
}

export function extractActualsFromOverlayEntries(overlayEntries) {
  const rids = {};
  if (!Array.isArray(overlayEntries)) return rids;
  for (const pair of overlayEntries) {
    const rid = pair?.[0];
    const ov = pair?.[1];
    if (!rid || !ov) continue;
    const locs = Array.isArray(ov.locs) ? ov.locs : [];
    const locMap = {};
    for (const locPair of locs) {
      const tpl = locPair?.[0];
      const slice = locActualSliceFromEntry(locPair?.[1]);
      if (tpl && slice) locMap[String(tpl).toUpperCase()] = slice;
    }
    if (Object.keys(locMap).length) rids[rid] = locMap;
  }
  return rids;
}

export function mergeActualsMaps(into, from) {
  const isActual = (kind) => typeof kind === 'string' && kind.startsWith('actual');
  for (const [rid, locs] of Object.entries(from || {})) {
    if (!into[rid]) into[rid] = {};
    for (const [tpl, slice] of Object.entries(locs || {})) {
      const prev = into[rid][tpl] || {};
      const next = { ...prev };
      if (isActual(slice.bestKind) && slice.bestTime) {
        next.bestTime = slice.bestTime;
        next.bestKind = slice.bestKind;
        if (slice.liveSource) next.liveSource = slice.liveSource;
        if (slice.liveSourceInstance) next.liveSourceInstance = slice.liveSourceInstance;
      }
      if (isActual(slice.arrLiveKind) && slice.arrLiveTime) {
        next.arrLiveTime = slice.arrLiveTime;
        next.arrLiveKind = slice.arrLiveKind;
        if (slice.arrLiveSource) next.arrLiveSource = slice.arrLiveSource;
        if (slice.arrLiveSourceInstance) next.arrLiveSourceInstance = slice.arrLiveSourceInstance;
      }
      into[rid][tpl] = next;
    }
  }
}
