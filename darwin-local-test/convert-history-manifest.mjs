#!/usr/bin/env node
/**
 * Convert history/<date>/ 30s stamped names into manifest.json + blobs/.
 *
 *   node convert-history-manifest.mjs --dry-run
 *   node convert-history-manifest.mjs --apply
 *   node convert-history-manifest.mjs --apply --purge-stamped
 *   node convert-history-manifest.mjs --apply --only-date 2026-09-21
 *   node convert-history-manifest.mjs --apply --remove-date 2026-09-20
 *
 * Skips today (live persist). Does not touch unit-catalog.json.
 */
import { readdirSync, readFileSync, renameSync, rmSync, mkdirSync, existsSync, writeFileSync, unlinkSync, statSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gunzipSync, gzipSync } from 'node:zlib';
import {
  extractActualsFromOverlayEntries,
  hashJsonToHex,
  mergeActualsMaps,
  writeCoreBlob,
  writeHistoryManifest,
} from './history-manifest.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const dryRun = !args.includes('--apply');
const purgeStamped = args.includes('--purge-stamped');
const skipActuals = !args.includes('--actuals');
const skipToday = !args.includes('--include-today');
const stateDir = resolve(__dirname, argValue('--state-dir') || 'state');
const historyDir = resolve(stateDir, 'history');
const ttRoot = resolve(__dirname, 'tt');
const onlyDates = new Set(argValues('--only-date'));
const removeDates = argValues('--remove-date');

const HEAVY_RE = /^daemon-cache-heavy\.(.+)\.(formations|consist|units|overlay)(?:\.(\d+))?\.json\.gz$/;
const CORE_RE = /^daemon-cache\.(.+)\.json(?:\.gz)?$/;

function argValue(flag) {
  const i = args.indexOf(flag);
  return i >= 0 ? args[i + 1] : null;
}
function argValues(flag) {
  const out = [];
  for (let i = 0; i < args.length; i++) {
    if (args[i] === flag && args[i + 1]) out.push(args[i + 1]);
  }
  return out;
}

function railwayToday() {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date());
  const p = (t) => parts.find((x) => x.type === t)?.value;
  return `${p('year')}-${p('month')}-${p('day')}`;
}

function stampToMs(stamp) {
  const sm = /^(\d{4})-(\d{2})-(\d{2})T(\d{2})-(\d{2})-(\d{2})-(\d{3})Z$/.exec(stamp);
  if (!sm) return NaN;
  return Date.parse(`${sm[1]}-${sm[2]}-${sm[3]}T${sm[4]}:${sm[5]}:${sm[6]}.${sm[7]}Z`);
}

function blobNameByInode(blobDir) {
  const map = new Map();
  if (!existsSync(blobDir)) return map;
  let names = [];
  try { names = readdirSync(blobDir); } catch { return map; }
  for (const name of names) {
    if (!name.endsWith('.json.gz')) continue;
    try {
      const st = statSync(resolve(blobDir, name));
      map.set(`${st.dev}:${st.ino}`, name);
    } catch {}
  }
  return map;
}

function ensureArrayBlob(blobDir, label, gzPath, partIdx, inodeMap) {
  try {
    const st = statSync(gzPath);
    const hit = inodeMap?.get(`${st.dev}:${st.ino}`);
    if (hit) return hit;
  } catch {}
  const json = gunzipSync(readFileSync(gzPath)).toString('utf8');
  const hash = hashJsonToHex(json);
  const name = partIdx == null
    ? `${label}.${hash}.json.gz`
    : `${label}.${partIdx}.${hash}.json.gz`;
  const dest = resolve(blobDir, name);
  if (!existsSync(dest)) {
    mkdirSync(blobDir, { recursive: true });
    const tmp = `${dest}.tmp`;
    writeFileSync(tmp, gzipSync(json, { level: 6 }));
    renameSync(tmp, dest);
  }
  try {
    const st = statSync(existsSync(gzPath) ? gzPath : dest);
    inodeMap?.set(`${st.dev}:${st.ino}`, name);
  } catch {}
  return name;
}

function collectHeavyForStamp(dayDir, stamp) {
  const byLabel = { formations: [], consist: [], units: [], overlay: [] };
  let names = [];
  try { names = readdirSync(dayDir); } catch { return byLabel; }
  const prefix = `daemon-cache-heavy.${stamp}.`;
  const files = names.filter((n) => n.startsWith(prefix) && n.endsWith('.json.gz'));
  files.sort();
  for (const name of files) {
    const m = HEAVY_RE.exec(name);
    if (!m || m[1] !== stamp) continue;
    const label = m[2];
    const part = m[3] != null ? Number(m[3]) : null;
    if (!byLabel[label]) continue;
    byLabel[label].push({ name, part, path: resolve(dayDir, name) });
  }
  for (const label of Object.keys(byLabel)) {
    byLabel[label].sort((a, b) => (a.part ?? -1) - (b.part ?? -1));
  }
  return byLabel;
}

function convertDay(ymd, { apply, purge }) {
  const dayDir = resolve(historyDir, ymd);
  if (!existsSync(dayDir)) return { ymd, skipped: true, reason: 'no history dir' };
  const blobDir = resolve(dayDir, 'blobs');
  let files = [];
  try { files = readdirSync(dayDir); } catch { return { ymd, skipped: true, reason: 'unreadable' }; }
  const cores = files.filter((f) => CORE_RE.test(f) && f !== 'daemon-cache.latest.json');
  console.log(`[convert] ${ymd} ${cores.length} stamped cores`);
  const snaps = [];
  const actuals = {};
  const overlayBlobsSeen = new Set();
  const inodeMap = blobNameByInode(blobDir);

  let n = 0;
  for (const file of cores) {
    const stamp = CORE_RE.exec(file)[1];
    const ms = stampToMs(stamp);
    if (!Number.isFinite(ms)) continue;
    const corePath = resolve(dayDir, file);
    let coreJson;
    try {
      const buf = readFileSync(corePath);
      coreJson = file.endsWith('.gz')
        ? gunzipSync(buf).toString('utf8')
        : buf.toString('utf8');
      JSON.parse(coreJson);
    } catch {
      continue;
    }
    if (!apply) {
      snaps.push({ ms, savedAt: new Date(ms).toISOString(), core: file, file });
      continue;
    }
    const coreName = writeCoreBlob(blobDir, coreJson, 6);
    const heavy = collectHeavyForStamp(dayDir, stamp);
    const snap = {
      ms,
      savedAt: new Date(ms).toISOString(),
      core: coreName,
      formations: [],
      consist: [],
      units: [],
      overlay: [],
    };
    for (const label of ['formations', 'consist', 'units', 'overlay']) {
      const parts = heavy[label];
      if (!parts.length) continue;
      const names = [];
      for (const p of parts) {
        try {
          names.push(ensureArrayBlob(blobDir, label, p.path, parts.length > 1 ? p.part : null, inodeMap));
        } catch (e) {
          console.warn(`[convert] ${ymd} ${p.name}: ${e.message}`);
        }
      }
      snap[label] = names;
      if (label === 'overlay' && !skipActuals) {
        for (const n of names) {
          if (overlayBlobsSeen.has(n)) continue;
          overlayBlobsSeen.add(n);
          try {
            const arr = JSON.parse(gunzipSync(readFileSync(resolve(blobDir, n))).toString('utf8'));
            mergeActualsMaps(actuals, extractActualsFromOverlayEntries(arr));
          } catch {}
        }
      }
    }
    snaps.push(snap);
    n++;
    if (n === 1 || n % 100 === 0) console.log(`[convert] ${ymd} ${n}/${cores.length}`);
  }

  snaps.sort((a, b) => a.ms - b.ms);
  if (apply) {
    writeHistoryManifest(dayDir, {
      schema: 1,
      date: ymd,
      builtAt: new Date().toISOString(),
      convertedFrom: 'stamped-names',
      snaps,
    });
    if (Object.keys(actuals).length) {
      const actualsPath = resolve(dayDir, 'actuals.json.gz');
      const prev = existsSync(actualsPath)
        ? (() => {
          try {
            return JSON.parse(gunzipSync(readFileSync(actualsPath)).toString('utf8'))?.rids || {};
          } catch { return {}; }
        })()
        : {};
      mergeActualsMaps(prev, actuals);
      const tmp = `${actualsPath}.tmp`;
      writeFileSync(tmp, gzipSync(JSON.stringify({ savedAt: new Date().toISOString(), rids: prev }), { level: 6 }));
      renameSync(tmp, actualsPath);
    }
    if (purge) {
      for (const file of files) {
        if (file === 'daemon-cache.latest.json' || file === 'manifest.json' || file === 'snapshot-index.json' || file === 'actuals.json.gz') continue;
        if (file === 'blobs') continue;
        if (CORE_RE.test(file) || file.startsWith('daemon-cache-heavy.')) {
          try { unlinkSync(resolve(dayDir, file)); } catch {}
        }
      }
    }
  }
  return { ymd, snaps: snaps.length, apply, purge, actualRids: Object.keys(actuals).length };
}

function removeBrokenDate(ymd) {
  const historyPath = resolve(historyDir, ymd);
  const ttPath = resolve(ttRoot, ymd.replace(/-/g, ''));
  const summary = { ymd, history: existsSync(historyPath), tt: existsSync(ttPath) };
  if (dryRun) return { ...summary, dryRun: true };
  if (existsSync(historyPath)) rmSync(historyPath, { recursive: true, force: true });
  if (existsSync(ttPath)) rmSync(ttPath, { recursive: true, force: true });
  return { ...summary, removed: true };
}

const today = railwayToday();
console.log(`[convert] state=${stateDir} dryRun=${dryRun} purgeStamped=${purgeStamped} today=${today}`);

for (const d of removeDates) {
  console.log('[convert] remove broken date', removeBrokenDate(d));
}

let days = [];
try { days = readdirSync(historyDir).filter((d) => /^\d{4}-\d{2}-\d{2}$/.test(d)).sort(); } catch {
  console.warn('[convert] no history dir');
  process.exit(0);
}

for (const ymd of days) {
  if (onlyDates.size && !onlyDates.has(ymd)) continue;
  if (removeDates.includes(ymd)) continue;
  if (skipToday && ymd === today) {
    console.log(`[convert] skip live day ${ymd}`);
    continue;
  }
  const result = convertDay(ymd, { apply: !dryRun, purge: !dryRun && purgeStamped });
  console.log('[convert]', result);
}

if (dryRun) console.log('[convert] dry-run only; pass --apply to write manifests (add --purge-stamped after you have verified loads)');
