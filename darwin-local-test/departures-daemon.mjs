#!/usr/bin/env node
/*
 * Darwin live-departures daemon.
 *
 * Boot:
 *   1. Load today's timetable (fetch only if missing).
 *   2. Restore persisted live + formations/PTAC/messages.
 *   3. Prime last 7 days (if data exists), next 7 days of timetable, then today's boards.
 *   4. Darwin + PTAC Kafka connected.
 *   5. Then bind HTTP and accept connections.
 *
 * Endpoints:
 *   GET  /api/health
 *   GET  /api/ping          (minimal liveness; no API key; OK during warmup — for uptime monitors)
 *   GET  /api/station/:code
 *   GET  /api/departures/:code?hours=N
 *   GET  /api/messages/:crs
 *   GET  /api/service/:rid?date=YYYY-MM-DD&at=HH:MM
 *   GET  /api/unit/:resourceGroupId   (PTAC unit / day diagram)
 *   GET  /api/units/catalog?fleet=158
 *   GET  /api/history/dates
 *   POST /api/plan/bash     station-bash itinerary (alight at each CRS)
 *
 * Env vars (all optional except DARWIN_*):
 *   DAEMON_PORT            HTTP port (default 4001)
 *   DAEMON_HOST            listen address (default 0.0.0.0; use 127.0.0.1 in production)
 *   CORS_ORIGIN            Access-Control-Allow-Origin (default http://localhost:5173)
 *   DEFAULT_WINDOW_HOURS   default look-ahead when ?hours is omitted (default 3)
 *   INITIAL_REPLAY_MIN     Kafka replay on startup (default 1080 = 18h Darwin
 *                          scheduleFormations). PTAC unit/formation allocations
 *                          are often published 12–48h before that working day —
 *                          see PTAC_INITIAL_REPLAY_MIN (default 2880 = 48h).

 *   HEARTBEAT_SEC          stats log interval (default 60)
 *   DAY_ROLLOVER_CHECK_SEC  how often to compare UK railway day vs loadedDate (default 60, clamped 15–600)
 *   SCHEDULED_FETCH_TICK_SEC how often to evaluate DARWIN_AUTO_FETCH_TIME (default 30, clamped 15–120)
 *   DARWIN_AUTO_FETCH_TIME   Europe/London HH:MM to start pulling new PPTimetable files (default 04:00)
 *   DARWIN_AUTO_FETCH_GRACE_MIN  minutes after that to keep retrying until today’s v8 appears (default 35 → 04:00–04:35)

 *   DARWIN_LOOKBACK_TIMETABLE_DAYS  yesterday (default 1) timetable-only if no snapshot
 *   DARWIN_FUTURE_TIMETABLE_DAYS    cap on Darwin-file SSDs (default 30). TTIS CIF
 *                                   dates use the file's own last valid day unless this is set.
 *   TTIS_MCA_PATH          ATOC/TTIS full CIF MCA for dates beyond Darwin snapshots
 *   DARWIN_*               Kafka creds from .env
 *   KAFKA_*_TIMEOUT_MS / KAFKA_RETRY_*  optional KafkaJS tuning (Darwin + PTAC share timeouts/retry)
 *   PTAC_CONNECT_DELAY_MS      ms before PTAC connects (default 2500; staggers TLS vs Darwin)
 *   PTAC_START_AFTER_WARMUP      default true — connect PTAC after historical warmup (less Kafka churn vs disk-heavy warmup)
 *   DARWIN_SESSION_TIMEOUT_MS / DARWIN_HEARTBEAT_INTERVAL_MS / DARWIN_REBALANCE_TIMEOUT_MS — Darwin consumer group (default 90s / 10s / 90s)
 *   PTAC_SESSION_TIMEOUT_MS / PTAC_HEARTBEAT_INTERVAL_MS / PTAC_REBALANCE_TIMEOUT_MS — PTAC consumer (default 120s / 10s / 90s)
 */

import { readdirSync, readFileSync, writeFileSync, renameSync, mkdirSync, existsSync, unlinkSync, rmSync, appendFileSync, linkSync, copyFileSync, statSync, createWriteStream } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { basename, dirname, resolve } from 'node:path';
import { createHash } from 'node:crypto';
import { parseStoreEnv, createStateSqlite } from './state-sqlite.mjs';
import { createServer } from 'node:http';
import { execFile } from 'node:child_process';
import { gzipSync, gunzipSync, createGzip } from 'node:zlib';
import { pipeline } from 'node:stream/promises';
import { createRequire } from 'node:module';
import dotenv from 'dotenv';
import { Kafka, logLevel } from 'kafkajs';
import { flushJidxWriteQueue, loadAllJourneysIndexedByTiploc } from './timetable-loader.mjs';
import {
  appendHistoryManifestSnap,
  attachHeavyFromManifestSnap,
  blobBasenames,
  manifestSnapsToList,
  readHistoryManifest,
  writeCoreBlob,
} from './history-manifest.mjs';
import { buildPlannerIndex, planBashItinerary } from './bash-planner.mjs';
import { expandTtisDay, loadTtisIndex } from './cif-ttis-loader.mjs';
import { loadTodaysReasons } from './reasons-loader.mjs';
import { loadTodaysLocations, loadSupplementalNamesFromFile, makeResolvers } from './locations-loader.mjs';
import { parseConsistMessage, consistJoinKey, KNOWN_PTAC_TOC } from './consist-parser.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, '.env') });

/** Prefix every console line with Europe/London wall time (KafkaJS JSON logs already carry ISO timestamps). */
function londonLogStamp(d = new Date()) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(d);
  const p = (type) => parts.find((x) => x.type === type)?.value || '00';
  return `${p('year')}-${p('month')}-${p('day')} ${p('hour')}:${p('minute')}:${p('second')}`;
}

(function installLogTimestamps() {
  if (globalThis.__darwinLogTs) return;
  globalThis.__darwinLogTs = true;
  for (const method of ['log', 'info', 'warn', 'error']) {
    const orig = console[method].bind(console);
    console[method] = (...args) => orig(`[${londonLogStamp()}]`, ...args);
  }
})();

const cfg = {
  bootstrap:       process.env.DARWIN_BOOTSTRAP,
  username:        process.env.DARWIN_USERNAME,
  password:        process.env.DARWIN_PASSWORD,
  topic:           process.env.DARWIN_TOPIC || 'prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON',
  groupId:         process.env.DARWIN_GROUP_ID,
  port:            Number(process.env.DAEMON_PORT || 4001),
  host:            (process.env.DAEMON_HOST || '0.0.0.0').trim() || '0.0.0.0',
  // Comma-separated list. The handler echoes the matching origin (or "*" if
  // the special "*" entry is present) so multiple local dev URLs can connect.
  corsOrigins:     (process.env.CORS_ORIGIN || 'http://localhost:3000,http://localhost:3001').split(',').map((s) => s.trim()),
  windowHours:     Number(process.env.DEFAULT_WINDOW_HOURS || 3),
  initialReplay:   Number(process.env.INITIAL_REPLAY_MIN || 1080),
  heartbeat:       Number(process.env.HEARTBEAT_SEC || 60),
  departuresCacheMs: Number(process.env.DEPARTURES_CACHE_MS || 3000),
  // Historical ?date=&at= responses are expensive (gzip + full JSON). Keep
  // the assembled snapshot longer than live boards (still seconds-level for live).
  departuresHistCacheMs: Math.max(0, Number(process.env.DEPARTURES_HIST_CACHE_MS || 300_000)),
  internalApiKeys: (() => {
    const list = (process.env.INTERNAL_API_KEYS || '')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
    if (list.length > 0) return list;
    const single = (process.env.INTERNAL_API_KEY || '').trim();
    return single ? [single] : [];
  })(),
  autoFetchFiles:  !['0', 'false', 'no'].includes(String(process.env.DARWIN_AUTO_FETCH_FILES || 'true').toLowerCase()),
  autoFetchTime:   process.env.DARWIN_AUTO_FETCH_TIME || '04:00',
  rawArchiveEnabled: !['0', 'false', 'no'].includes(String(process.env.RAW_ARCHIVE_ENABLED || 'true').toLowerCase()),
  rawArchiveCompress: !['0', 'false', 'no'].includes(String(process.env.RAW_ARCHIVE_COMPRESS || 'true').toLowerCase()),
  rawArchiveDir: process.env.RAW_ARCHIVE_DIR || resolve(__dirname, 'state/raw-feed'),
  rawArchiveRetentionDays: Math.max(1, Number(process.env.RAW_ARCHIVE_RETENTION_DAYS || 30)),
  // Background day-by-day historical warmup. After the API is "live_ready",
  // the daemon walks the most recent N days of state/history/<date>/ to prime
  // historical timetable, snapshot-list and context caches without blocking
  // live request handling. Guardrails skip a date if RSS or event-loop lag
  // breach the configured ceilings.
  warmupEnabled: !['0', 'false', 'no'].includes(String(process.env.WARMUP_ENABLED || 'true').toLowerCase()),
  warmupDays: Math.max(0, Number(process.env.WARMUP_DAYS || 7)),
  warmupFutureDays: Math.max(0, Number(process.env.WARMUP_FUTURE_DAYS || 7)),
  /** Stop priming ±N day boards if RSS exceeds this (MB). 12GB box ~9000; 16GB ~11000. */
  hotHorizonMaxRssMb: Math.max(2000, Number(process.env.HOT_HORIZON_MAX_RSS_MB || 10000)),
  warmupMaxRssMb: Math.max(0, Number(process.env.WARMUP_MAX_RSS_MB || 4500)),
  warmupLagMs: Math.max(0, Number(process.env.WARMUP_LAG_MS || 200)),
  hotBoardCrs: String(process.env.HOT_BOARD_CRS || 'LDS,KGX,EUS,PAD,MAN,BHX,EDB,GLC,NCL,YRK,LIV,SHF,NOT,BRI,RDG,DEW')
    .split(',')
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean),
  hotBoardBatch: Math.max(1, Number(process.env.HOT_BOARD_BATCH || 40)),
  hotBoardTickMs: Math.max(0, Number(process.env.HOT_BOARD_TICK_MS || 10)),
  /** How often (seconds) we compare railway day vs `loadedDate` for rollover (15–600, default 60). */
  dayRolloverCheckSec: Math.min(600, Math.max(15, Number(process.env.DAY_ROLLOVER_CHECK_SEC || 60))),
  /** How often we evaluate `DARWIN_AUTO_FETCH_TIME` (15–120s, default 30) so the HH:MM slot is not skipped. */
  scheduledFetchTickSec: Math.min(120, Math.max(15, Number(process.env.SCHEDULED_FETCH_TICK_SEC || 30))),
  /** Minutes after `DARWIN_AUTO_FETCH_TIME` we keep retrying GCS until today’s file lands (default 35). */
  autoFetchGraceMin: Math.min(120, Math.max(5, Number(process.env.DARWIN_AUTO_FETCH_GRACE_MIN || 35))),
  /** How many upcoming railway days to add to the PTAC join index (unit data lands 12–48h early). */
  ptacAheadDays: Math.min(7, Math.max(1, Number(process.env.PTAC_AHEAD_DAYS || 2))),
  ttisMcaPath: (process.env.TTIS_MCA_PATH || '').trim() || resolve(__dirname, 'ttis/RJTTF963MCA.txt'),
};

// PTAC (S506) feed config — separate creds and consumer group from Darwin.
// All optional: if any are missing, the PTAC consumer is skipped silently.
const ptacCfg = {
  bootstrap:    process.env.PTAC_BOOTSTRAP || cfg.bootstrap,  // same Confluent cluster
  username:     process.env.PTAC_USERNAME,
  password:     process.env.PTAC_PASSWORD,
  topic:        process.env.PTAC_TOPIC || 'prod-1033-Passenger-Train-Allocation-and-Consist-1_0',
  groupId:      process.env.PTAC_GROUP_ID,
  // Default 48h replay: PTAC unit/formation allocations are often published
  // 12–48h before the working day. Broker retention may still clip this.
  initialReplay: Number(process.env.PTAC_INITIAL_REPLAY_MIN || 2880),
  /**
   * rolling (default): replay last PTAC_INITIAL_REPLAY_MIN minutes from now.
   * ssd_0001: replay from 00:01 Europe/London on the current timetable SSD — use once to backfill after outages.
   */
  replayAnchor: (process.env.PTAC_REPLAY_ANCHOR || 'rolling').toLowerCase().trim(),
};

/** When true (default), PTAC Kafka connects only after historical warmup — avoids overlapping TLS + replay with disk/CPU-heavy warmup (fewer Confluent handshake drops). */
const PTAC_START_AFTER_WARMUP = !['0', 'false', 'no'].includes(
  String(process.env.PTAC_START_AFTER_WARMUP ?? 'true').toLowerCase(),
);

// ---------- pick today's timetable file ------------------------------------
/** When GCS only has the previous SSD’s `YYYYMMDDHHMMSS_vN.xml.gz` names, the fetch
 *  script still places them under `tt/<todayRailwayYmd>/`; accept any v8-shaped file
 *  in that directory (newest timetable schema version wins). */
/** Prefer the newest 14-digit stamp when several v8 files share a schema version. */
function timetableStampFromName(filename) {
  const m = String(filename || '').match(/(\d{14})_v\d+\.xml\.gz$/i);
  return m ? Number(m[1]) : 0;
}

function pickLooseTimetableV8FromDir(dir) {
  let files = [];
  try { files = readdirSync(dir); } catch { return null; }
  const looseRe = /^(\d{14})_v(\d+)\.xml\.gz$/;
  const matches = [];
  for (const f of files) {
    if (f.includes('_ref_')) continue;
    const m = f.match(looseRe);
    if (!m) continue;
    matches.push({
      path: resolve(dir, f),
      ver: Number(m[2] || 0),
      stamp: Number(m[1] || 0),
    });
  }
  if (matches.length === 0) return null;
  matches.sort((a, b) => b.ver - a.ver || b.stamp - a.stamp);
  return matches[0].path;
}

function pickTodaysTimetable() {
  // Must match UK railway day (not UTC calendar date) so tt/YYYYMMDD aligns with journey SSD.
  const ymd = railwayDayYmd(new Date()).replace(/-/g, '');
  const dirs = [
    resolve(__dirname, `./tt/${ymd}`),
    resolve(__dirname, '../docs/V8s'),
    resolve(__dirname, '../docs/timetablefiles'),
  ];
  const nameRe = new RegExp(`^(?:PPTimetable_)?${ymd}\\d{6}_v(\\d+)\\.xml\\.gz$`);
  const all = [];
  for (const dir of dirs) {
    let files = []; try { files = readdirSync(dir); } catch { continue; }
    for (const f of files) {
      if (f.includes('_ref_')) continue;
      const m = f.match(nameRe);
      if (!m) continue;
      const ver = Number(m[1] || 0);
      all.push({ path: resolve(dir, f), ver, stamp: timetableStampFromName(f) });
    }
  }
  if (all.length === 0) {
    const loose = pickLooseTimetableV8FromDir(resolve(__dirname, `./tt/${ymd}`));
    if (loose) return loose;
    throw new Error(`no timetable file for today (${ymd})`);
  }
  all.sort((a, b) => b.ver - a.ver || b.stamp - a.stamp);
  return all[0].path;
}

function pickTimetableForDate(ymdDashed) {
  if (!isIsoDate(ymdDashed)) return null;
  const ymd = ymdToCompact(ymdDashed);
  const dir = resolve(__dirname, `./tt/${ymd}`);
  let files = [];
  try { files = readdirSync(dir); } catch { return null; }
  const nameRe = new RegExp(`^(?:PPTimetable_)?${ymd}\\d{6}_v(\\d+)\\.xml\\.gz$`);
  const matches = [];
  for (const f of files) {
    if (f.includes('_ref_')) continue;
    const m = f.match(nameRe);
    if (!m) continue;
    matches.push({ path: resolve(dir, f), ver: Number(m[1] || 0), stamp: timetableStampFromName(f) });
  }
  if (matches.length === 0) return pickLooseTimetableV8FromDir(dir);
  matches.sort((a, b) => b.ver - a.ver || b.stamp - a.stamp);
  return matches[0].path;
}

// ---------- helpers --------------------------------------------------------
function asArray(x) { return x == null ? [] : Array.isArray(x) ? x : [x]; }
function decodeKafkaJson(rawBuf) {
  const v = JSON.parse(rawBuf.toString('utf8'));
  return typeof v.bytes === 'string' ? JSON.parse(v.bytes) : v;
}
function unwrap(v) { return v == null ? v : typeof v === 'object' ? (v['#text'] || v._ || v['']) : v; }

/**
 * Flatten a fast-xml-parser-style mixed-content tree into plain text + a
 * naive HTML representation. NRCC `OW.Msg` arrives shaped like:
 *   { "": "leading text ", a: { href: "...", "": "link text" } }
 * or with multiple paragraphs:
 *   { p: ["para 1", "para 2"] }
 * The empty-string key holds bare text content; named keys are nested HTML
 * elements. Children may be objects, strings, or arrays. Iteration order on
 * a plain object is insertion order in V8, which matches XML reading order
 * for the cases observed in the wild.
 */
function flattenHtml(node) {
  if (node == null) return { plain: '', html: '' };
  if (typeof node === 'string' || typeof node === 'number') {
    const s = String(node);
    return { plain: s, html: s };
  }
  if (Array.isArray(node)) {
    let plain = '', html = '';
    for (const item of node) {
      const r = flattenHtml(item);
      // Separate array items with a space so adjacent paragraphs don't
      // smush into one word.
      plain += (plain && r.plain ? ' ' : '') + r.plain;
      html  += r.html;
    }
    return { plain, html };
  }
  if (typeof node === 'object') {
    let plain = '', html = '';
    for (const [key, val] of Object.entries(node)) {
      // Bare text content of this element (fast-xml-parser convention).
      if (key === '' || key === '#text' || key === '_') {
        const r = flattenHtml(val);
        plain += r.plain; html += r.html;
        continue;
      }
      // XML attributes: skip in the flattened text. The href etc. live
      // inside child elements; if we have an `a` with href it's already
      // an object and the href is processed below.
      if (typeof val !== 'object') {
        // Scalar attribute (e.g. on the parent) — ignore.
        continue;
      }
      const inner = flattenHtml(val);
      // Wrap recognised inline elements in real HTML. For `a` we also
      // pull the href out of the child object so links are clickable.
      if (key === 'a') {
        // val may be a single anchor object or an array of them.
        const anchors = Array.isArray(val) ? val : [val];
        for (const anc of anchors) {
          const href = anc && (anc.href || anc.HREF);
          const r    = flattenHtml(anc);
          if (href) html += `<a href="${escapeAttr(String(href))}" target="_blank" rel="noopener">${escapeText(r.plain)}</a>`;
          else      html += escapeText(r.plain);
          plain += r.plain;
        }
      } else if (key === 'br') {
        html  += '<br>';
        plain += '\n';
      } else if (key === 'p') {
        const r = flattenHtml(val);
        html  += `<p>${escapeText(r.plain)}</p>`;
        plain += (plain ? '\n\n' : '') + r.plain;
      } else if (key === 'b' || key === 'strong' || key === 'i' || key === 'em' || key === 'u' || key === 'span') {
        html  += `<${key}>${escapeText(inner.plain)}</${key}>`;
        plain += inner.plain;
      } else {
        // Unknown element: keep its text content but drop the wrapper.
        plain += inner.plain;
        html  += escapeText(inner.plain);
      }
    }
    return { plain, html };
  }
  return { plain: '', html: '' };
}

function escapeText(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
function escapeAttr(s) {
  return escapeText(s).replace(/"/g, '&quot;');
}

function todayYmd(d = new Date()) {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

/** London wall-clock minute-of-day when the labelled operating day advances (02:00 → previous segment ends 01:59). */
const UK_RAILWAY_DAY_START_MINUTES = 2 * 60;

function railwayDayYmd(now = new Date()) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(now);

  const pick = (type) => parts.find((p) => p.type === type)?.value || '00';
  const year = Number(pick('year'));
  const month = Number(pick('month'));
  const day = Number(pick('day'));
  const hour = Number(pick('hour'));
  const minute = Number(pick('minute'));

  const londonAsUtcMs = Date.UTC(year, month - 1, day, hour, minute);
  const railwayDayUtcMs = londonAsUtcMs - (UK_RAILWAY_DAY_START_MINUTES * 60 * 1000);
  return new Date(railwayDayUtcMs).toISOString().slice(0, 10);
}

function londonWallPartsAtUtcMs(ms) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(new Date(ms));
  const pick = (t) => Number(parts.find((p) => p.type === t)?.value || 0);
  return { y: pick('year'), mo: pick('month'), d: pick('day'), h: pick('hour'), mi: pick('minute'), s: pick('second') };
}

/** First UTC ms where Europe/London is calendar date `ssd` (YYYY-MM-DD) and local time ≥ 00:01:00. */
function ssdLondonCalendar001UtcMs(ssd) {
  const [Y, M, D] = ssd.split('-').map(Number);
  let lo = Date.UTC(Y, M - 1, D - 1, 12, 0, 0);
  let hi = Date.UTC(Y, M - 1, D + 1, 12, 0, 0);
  while (hi - lo > 1) {
    const mid = Math.floor((lo + hi) / 2);
    const p = londonWallPartsAtUtcMs(mid);
    const dayMs = Date.UTC(p.y, p.mo - 1, p.d);
    const targetMs = Date.UTC(Y, M - 1, D);
    let ok = false;
    if (dayMs > targetMs) ok = true;
    else if (dayMs < targetMs) ok = false;
    else ok = p.h > 0 || (p.h === 0 && p.mi >= 1);
    if (ok) hi = mid;
    else lo = mid;
  }
  return hi;
}

function todayYmdCompact(d = new Date()) {
  return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, '0')}${String(d.getDate()).padStart(2, '0')}`;
}
function anchorTime(hhmm, ssd) {
  if (!hhmm) return null;
  const m = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(hhmm);
  if (!m) return null;
  const [, h, mn, s] = m;
  return new Date(`${ssd}T${h.padStart(2, '0')}:${mn}:${s || '00'}+01:00`);
}

/** Minutes since midnight from a timetable HH:MM[:SS] field. */
function scheduledMinutesFromMidnight(hhmm) {
  const m = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(String(hhmm || '').trim());
  if (!m) return null;
  const hh = Number(m[1]);
  const mm = Number(m[2]);
  if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
  if (hh < 0 || hh > 23 || mm < 0 || mm > 59) return null;
  return hh * 60 + mm;
}

/** Aligns with railwayDayYmd(): times before 02:00 on the SSD calendar roll +24h for overnight display. */
const UK_RAIL_ROLLOVER_MINUTES = UK_RAILWAY_DAY_START_MINUTES;

function adjustScheduledInstantForRailwayOvernight(scheduledAt, scheduledTime) {
  const mins = scheduledMinutesFromMidnight(scheduledTime);
  if (mins != null && mins < UK_RAIL_ROLLOVER_MINUTES) {
    return new Date(scheduledAt.getTime() + 24 * 60 * 60_000);
  }
  return scheduledAt;
}
function describeLiveTime(node, actualKind, estKind) {
  if (!node || typeof node !== 'object') return null;
  const source = node.src ? String(unwrap(node.src)) : null;
  const sourceInstance = node.srcInst ? String(unwrap(node.srcInst)) : null;
  const unknownDelay = node.delayed === true || node.delayed === 'true';
  const manualUnknownDelay = node.etUnknown === true || node.etUnknown === 'true';
  if (node.at) {
    return { time: String(unwrap(node.at)), kind: actualKind, source, sourceInstance, unknownDelay, manualUnknownDelay };
  }
  if (node.et) {
    return { time: String(unwrap(node.et)), kind: estKind, source, sourceInstance, unknownDelay, manualUnknownDelay };
  }
  if (unknownDelay || manualUnknownDelay) {
    return { time: null, kind: estKind, source, sourceInstance, unknownDelay, manualUnknownDelay };
  }
  return null;
}

function bestDepartureFromLiveLoc(loc) {
  const dep = describeLiveTime(loc?.dep, 'actual', 'est');
  if (dep) return dep;
  // Passing points publish under `pass` (not `dep`/`arr`) in TS updates.
  const pass = describeLiveTime(loc?.pass, 'actual', 'est');
  if (pass) return pass;
  const arr = describeLiveTime(loc?.arr, 'actual-arr', 'est-arr');
  if (arr) return arr;
  return null;
}

function parseHmToMinutes(hhmm) {
  if (!hhmm || typeof hhmm !== 'string') return null;
  const m = /^(\d{1,2}):(\d{2})/.exec(hhmm.trim());
  if (!m) return null;
  const h = Number(m[1]);
  const mm = Number(m[2]);
  if (!Number.isFinite(h) || !Number.isFinite(mm) || h < 0 || h > 23 || mm < 0 || mm > 59) return null;
  return h * 60 + mm;
}

function computeDelayMinutes(scheduledTime, liveTime, liveKind) {
  if (!liveTime) return null;
  if (liveKind === 'scheduled' || liveKind === 'working') return 0;
  const sched = parseHmToMinutes(scheduledTime);
  const live = parseHmToMinutes(liveTime);
  if (sched == null || live == null) return null;
  // Choose the closest same-day/overnight delta in range [-12h, +12h].
  let diff = live - sched;
  if (diff > 720) diff -= 1440;
  if (diff < -720) diff += 1440;
  return diff;
}

/**
 * Normalise Darwin feed metadata into UI-facing service classes.
 * Output values are stable strings consumed by the frontend filter.
 */
function classifyServiceType({ trainCat, isPassenger, trainId, originName, destinationName }) {
  const cat = String(trainCat || '').toUpperCase();
  const headcodeClass = String(trainId || '').charAt(0);
  const replacementHints = ['rail replacement', 'replacement bus', 'bus replacement', 'bus service'];
  const endpoints = `${originName || ''} ${destinationName || ''}`.toLowerCase();
  if (replacementHints.some((hint) => endpoints.includes(hint))) return 'rail-replacement';
  if (cat === 'BR' || cat === 'BS' || cat.startsWith('B')) return 'rail-replacement';
  if (isPassenger) return 'passenger';
  if (['4', '6', '7', '8'].includes(headcodeClass)) return 'freight';
  if (cat.startsWith('E') || cat.startsWith('F') || cat.startsWith('H') || cat.startsWith('J') || cat.startsWith('M')) return 'freight';
  return 'other';
}

// ---------- reference data (reloadable on day rollover) --------------------
let byRid, byTiploc;                  // from timetable
let lateReasons, cancelReasons;       // from ref file
let locations, tocs, resolve_;        // from ref file
let supplementalStationNames;         // from optional non-Darwin station reference file
let crsToTiplocs;                     // CRS -> Array<TIPLOC>
let timetablePath, loadedDate;
// PTAC join key index built from byRid each reload.
//   "ssd|headcode|originTiploc|originHHMM" -> rid
// Plus a fallback "ssd|headcode|originTiploc" -> [rid, ...] for stop-time-only matches.
let ptacJoinByTuple, ptacJoinByOrigin;
let lastAutoFetchRunYmd = null;
let awaitingTimetableYmd = null;
let lastTimetableFetchAttemptMs = 0;
let lastLoadedTimetableStamp = 0;
const departuresCache = new Map(); // key -> { expiresAtMs, snapshot }
const serviceDetailCache = new Map(); // rid|date|at -> { expiresAtMs, detail }

function runDailyFileFetch(reason = 'scheduled') {
  return new Promise((resolvePromise) => {
    execFile(
      'node',
      [resolve(__dirname, 'fetch-daily-timetables.mjs')],
      { cwd: __dirname, env: process.env },
      (error, stdout, stderr) => {
        if (stdout && stdout.trim()) {
          console.log(`[daemon] auto-fetch (${reason}) stdout:\n${stdout.trim()}`);
        }
        if (stderr && stderr.trim()) {
          console.warn(`[daemon] auto-fetch (${reason}) stderr:\n${stderr.trim()}`);
        }
        if (error) {
          console.warn(`[daemon] auto-fetch (${reason}) failed: ${error.message}`);
          resolvePromise(false);
          return;
        }
        console.log(`[daemon] auto-fetch (${reason}) completed.`);
        resolvePromise(true);
      }
    );
  });
}

function londonHHMM(d = new Date()) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(d);
  const h = parts.find((p) => p.type === 'hour')?.value ?? '00';
  const m = parts.find((p) => p.type === 'minute')?.value ?? '00';
  return `${h}:${m}`;
}

/** Minute-of-day 0..1439 in Europe/London (same clock as `railwayDayYmd`). */
function minutesSinceMidnightLondon(d = new Date()) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(d);
  const h = Number(parts.find((p) => p.type === 'hour')?.value ?? '0');
  const m = Number(parts.find((p) => p.type === 'minute')?.value ?? '0');
  return h * 60 + m;
}

/** Parse `HH:MM` (24h) to minutes since midnight; null if invalid. */
function parseHHMMToMinutes(s) {
  const m = /^(\d{1,2}):(\d{2})$/.exec(String(s || '').trim());
  if (!m) return null;
  const hh = Number(m[1]);
  const mm = Number(m[2]);
  if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
  if (hh < 0 || hh > 23 || mm < 0 || mm > 59) return null;
  return hh * 60 + mm;
}

async function maybeRunScheduledAutoFetch(now = new Date()) {
  if (!cfg.autoFetchFiles) return;
  const today = railwayDayYmd(now).replace(/-/g, '');
  const targetMin = parseHHMMToMinutes(cfg.autoFetchTime);
  if (targetMin == null) {
    console.warn(`[daemon] invalid DARWIN_AUTO_FETCH_TIME="${cfg.autoFetchTime}" (want HH:MM); skipping scheduled fetch`);
    return;
  }
  const nowMin = minutesSinceMidnightLondon(now);
  const graceEnd = targetMin + cfg.autoFetchGraceMin;
  if (nowMin < targetMin || nowMin > graceEnd) return;
  if (Date.now() - lastTimetableFetchAttemptMs < 120_000) return;
  lastTimetableFetchAttemptMs = Date.now();

  let stampBefore = 0;
  try {
    stampBefore = timetableStampFromName(pickTodaysTimetable().split('/').pop());
  } catch { /* no file yet */ }

  console.log(
    `[daemon] timetable fetch attempt (${londonHHMM(now)} Europe/London, railway day ${today}, `
    + `window ${cfg.autoFetchTime} + ${cfg.autoFetchGraceMin}m, local stamp=${stampBefore || 'none'})`
  );
  await runDailyFileFetch('scheduled timetable fetch');

  let stampAfter = 0;
  try {
    stampAfter = timetableStampFromName(pickTodaysTimetable().split('/').pop());
  } catch { /* still missing */ }

  if (!stampAfter) {
    console.warn('[daemon] today’s PPTimetable still missing (typical until ~04:30). Retrying inside the fetch window.');
    return;
  }
  const newer = stampAfter > stampBefore;
  const firstToday = lastAutoFetchRunYmd !== today;
  if (!newer && !firstToday) {
    console.log(`[daemon] no newer timetable than ${stampAfter}; will check again until the window ends.`);
    return;
  }
  lastAutoFetchRunYmd = today;
  lastLoadedTimetableStamp = stampAfter;
  try {
    await reloadAllDataAndResetLive('post-fetch');
    console.log(`[daemon] loaded timetable stamp ${stampAfter}${newer ? ' (newer file)' : ''}`);
  } catch (e) {
    console.warn(`[daemon] post-fetch reload failed: ${e.message}`);
  }
}

async function reloadReferenceData() {
  timetablePath = pickTodaysTimetable();
  console.log(`[daemon] loading timetable ${timetablePath.split('/').pop()}`);
  const prevRids = byRid?.size || 0;
  const prevTiplocs = byTiploc?.size || 0;
  if (prevRids || prevTiplocs) {
    byRid = new Map();
    byTiploc = new Map();
    if (typeof global.gc === 'function') global.gc();
    console.log(`[daemon] dropped previous timetable (${prevRids} RIDs / ${prevTiplocs} TIPLOCs) before parse`);
  }
  ({ byRid, byTiploc } = await loadAllJourneysIndexedByTiploc(timetablePath));
  await flushJidxWriteQueue();
  ({ lateReasons, cancelReasons } = loadTodaysReasons());
  ({ locations, tocs } = loadTodaysLocations());
  const supplementalPath = process.env.DARWIN_STATIONS_REF_XML
    || resolve(__dirname, 'StationsRefData_v1.2.xml');
  supplementalStationNames = new Map();
  try {
    if (existsSync(supplementalPath)) {
      supplementalStationNames = loadSupplementalNamesFromFile(supplementalPath);
    } else {
      console.log(`[supplemental] file not found, skipping: ${supplementalPath}`);
    }
  } catch (e) {
    console.warn(`[supplemental] failed to load ${supplementalPath}: ${e.message}`);
  }
  resolve_ = makeResolvers({ locations, tocs, supplementalNames: supplementalStationNames });

  crsToTiplocs = new Map();
  for (const [tpl, info] of locations) {
    if (!info.crs) continue;
    const crs = info.crs.toUpperCase();
    let arr = crsToTiplocs.get(crs);
    if (!arr) { arr = []; crsToTiplocs.set(crs, arr); }
    arr.push(tpl);
  }

  // PTAC join index for today, plus the next working days (allocations
  // are often published 12–48h before that SSD).
  ptacJoinByTuple = new Map();
  ptacJoinByOrigin = new Map();
  addJourneysToPtacJoin(byRid);
  loadedDate = railwayDayYmd(new Date());
  await extendPtacJoinWithUpcomingDays();
}

function indexOneJourneyForPtac(rid, j) {
  const origin = j?.slots?.find((s) => s.slot === 'OR' || s.slot === 'OPOR');
  if (!origin?.tpl) return false;
  const time = (origin.ptd || origin.wtd || '').slice(0, 5);
  if (!j.trainId || !j.ssd) return false;
  const exact = `${j.ssd}|${j.trainId}|${origin.tpl}|${time}`;
  const loose = `${j.ssd}|${j.trainId}|${origin.tpl}`;
  ptacJoinByTuple.set(exact, rid);
  let arr = ptacJoinByOrigin.get(loose);
  if (!arr) { arr = []; ptacJoinByOrigin.set(loose, arr); }
  if (!arr.includes(rid)) arr.push(rid);
  return true;
}

function addJourneysToPtacJoin(journeyMap) {
  if (!journeyMap) return 0;
  let n = 0;
  for (const [rid, j] of journeyMap) {
    if (indexOneJourneyForPtac(rid, j)) n++;
  }
  return n;
}

/** PTAC often publishes tomorrow/day-after workings 12–48h early — join those RIDs now. */
async function extendPtacJoinWithUpcomingDays() {
  const origin = loadedDate || railwayDayYmd(new Date());
  let added = 0;
  for (let i = 1; i <= cfg.ptacAheadDays; i++) {
    const ymd = addDaysIsoDate(origin, i);
    try {
      const ctx = await getTimetableOnlyContext(ymd);
      added += addJourneysToPtacJoin(ctx?.byRid);
    } catch (e) {
      console.warn(`[ptac] upcoming join index ${ymd} failed: ${e.message}`);
    }
  }
  if (added > 0) {
    console.log(`[ptac] indexed ${added} extra join keys for the next ${cfg.ptacAheadDays} working day(s) (unit data lands 12–48h early)`);
  }
  return added;
}

function ssdForPtacKeep(rid, consist) {
  const fromConsist = normalizeSsdYmd(consist?.diagramDate);
  if (fromConsist) return fromConsist;
  const fromJourney = normalizeSsdYmd(byRid?.get(rid)?.ssd);
  if (fromJourney) return fromJourney;
  const m = String(rid || '').match(/^(\d{8})/);
  return m ? normalizeSsdYmd(m[1]) : '';
}

/** Keep PTAC rows for yesterday–plus-ahead workings; do not drop 12–48h-early allocations. */
function shouldKeepAheadPtac(rid, consist) {
  if (!loadedDate) return true;
  const ssd = ssdForPtacKeep(rid, consist);
  if (!ssd) return true;
  const from = addDaysIsoDate(loadedDate, -1);
  const to = addDaysIsoDate(loadedDate, cfg.ptacAheadDays);
  return compareIsoDate(ssd, from) >= 0 && compareIsoDate(ssd, to) <= 0;
}

/** Normalize timetable SSD for comparisons (`YYYY-MM-DD` vs compact `YYYYMMDD`). */
function normalizeSsdYmd(v) {
  const s = String(v ?? '').trim();
  if (/^\d{8}$/.test(s)) return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  return s.slice(0, 10);
}

/**
 * Journey exists on the **currently loaded schedule date** (`loadedDate`).
 * Matches how `/api/health` counts `unitsOnLoadedDate` / `consistsOnLoadedDate`.
 */
function ridIsOnLoadedScheduleDate(rid) {
  if (!rid || !byRid || !loadedDate) return false;
  const j = byRid.get(rid);
  if (!j) return false;
  return normalizeSsdYmd(j.ssd) === normalizeSsdYmd(loadedDate);
}

let ptacDayCountCache = { date: '', units: 0, consists: 0, at: 0 };

function refreshPtacDayCounts() {
  let units = 0;
  let consists = 0;
  if (loadedDate) {
    for (const u of unitsById.values()) {
      const svcs = u.services || [];
      let onDay = false;
      for (let i = 0; i < svcs.length; i++) {
        const j = byRid.get(svcs[i].rid);
        if (j?.ssd === loadedDate) {
          onDay = true;
          break;
        }
      }
      if (onDay) units++;
    }
    for (const rid of consistByRid.keys()) {
      if (byRid.get(rid)?.ssd === loadedDate) consists++;
    }
  }
  ptacDayCountCache = { date: loadedDate || '', units, consists, at: Date.now() };
}

/**
 * After a new timetable is loaded, drop RID-keyed state that no longer maps to
 * a journey in `byRid`. Used on **manual** reloads only.
 * Skipped for **`post-fetch`** and **`day rollover`**: PTAC/unit data typically
 * arrives on Kafka around 02:00, while the new PPTimetable files land ~04:00–04:30.
 * Those reloads only refresh timetable join indexes and retry unmatched consists.
 *
 * PTAC (`consistByRid`, `unitsById`): drop rows/services unless the RID maps to a
 * journey whose timetable **`ssd`** matches **`loadedDate`** — aligns with
 * `unitsOnLoadedDate` / `consistsOnLoadedDate`.
 *
 * **Darwin `formationsByRid`**: prune only when the RID is missing from **`byRid`**.
 * Many overnight services keep a formation on a RID that still exists in the
 * loaded file while **`j.ssd`** can differ from **`loadedDate`**; SSD-strict pruning
 * here wrongly stripped almost all coach lists.
 */
function pruneMapsToValidRids() {
  if (!byRid) return;
  for (const rid of [...liveOverlayByRid.keys()]) {
    if (!byRid.has(rid)) liveOverlayByRid.delete(rid);
  }
  for (const rid of [...cancelled.keys()]) {
    if (!byRid.has(rid)) cancelled.delete(rid);
  }
  for (const rid of [...delayReason.keys()]) {
    if (!byRid.has(rid)) delayReason.delete(rid);
  }
  for (const rid of [...reverseFormation]) {
    if (!byRid.has(rid)) reverseFormation.delete(rid);
  }
  for (const rid of [...associationsByRid.keys()]) {
    if (!byRid.has(rid)) associationsByRid.delete(rid);
  }
  for (const rid of [...alertsByRid.keys()]) {
    if (!byRid.has(rid)) alertsByRid.delete(rid);
  }
  for (const rid of [...consistByRid.keys()]) {
    if (shouldKeepAheadPtac(rid, consistByRid.get(rid))) continue;
    if (!ridIsOnLoadedScheduleDate(rid)) consistByRid.delete(rid);
  }
  const liveUids = new Set();
  for (const j of byRid.values()) {
    const u = j?.uid != null ? String(j.uid).trim().toUpperCase() : '';
    if (u) liveUids.add(u);
  }
  for (const rid of [...formationsByRid.keys()]) {
    if (liveOverlayByRid.has(rid)) continue;
    if (byRid.has(rid) || shouldKeepAheadPtac(rid, null)) continue;
    const uid = uidFromRid(rid);
    if (uid && liveUids.has(uid)) continue;
    formationsByRid.delete(rid);
    if (uid && formationsByUid.get(uid) === rid) formationsByUid.delete(uid);
  }
  for (const [unitId, entry] of [...unitsById.entries()]) {
    const svcs = (entry.services || []).filter((s) => {
      if (!s.rid) return false;
      if (shouldKeepAheadPtac(s.rid, consistByRid.get(s.rid))) return true;
      return ridIsOnLoadedScheduleDate(s.rid);
    });
    if (svcs.length === 0) unitsById.delete(unitId);
    else unitsById.set(unitId, { ...entry, services: svcs });
  }
}

async function reloadAllDataAndResetLive(reason = 'manual') {
  const ownPause = !heavyReloadBusy;
  if (ownPause) beginHeavyReload(reason);
  try {
    console.log(`[daemon] reloading timetable/reference data (${reason}) ...`);
    await reloadReferenceData();
    departuresCache.clear();
    serviceDetailCache.clear();
    refreshHotCrsList();

    const keepPtac = reason === 'post-fetch' || reason === 'day rollover';
    if (keepPtac) {
      console.log(
        `[daemon] ${reason}: keeping PTAC/consist/units/formations (often published 12–48h before the working day).`
      );
    } else {
      try {
        persistActualsArchive(loadedDate || railwayDayYmd(new Date()), serializeLiveOverlayEntries());
      } catch (e) {
        console.warn(`[daemon] actuals archive before prune failed: ${e.message}`);
      }
      stationMessages.clear();
      messagesById.clear();
      pruneMapsToValidRids();
    }

    const retried = retryUnmatchedConsists();
    if (retried > 0) {
      console.log(`[daemon] PTAC retry after timetable reload: matched ${retried} previously-unmatched consists.`);
    }
    historicalContextCache.clear();
    // Persist + full-station reprime used to run while HTTP and hot boards were
    // still live; that double-held the new timetable and OOM'd the 16 GB box.
    // Hot boards refill after pause; interval persist runs once reads resume.
    console.log(`[daemon] ${reason}: skipped immediate persist and warmAllStationsOnce (hot boards will refill)`);
  } finally {
    if (ownPause) endHeavyReload();
  }
}

// ---------- live overlay state (keyed by RID, global) ----------------------
const liveOverlayByRid = new Map();   // rid -> { locs: Map<tpl, {ptd,pta,plat,...}>, latestTs }
const cancelled   = new Map();        // rid -> { reason, source, code? }
const delayReason = new Map();        // rid -> { reason, source, code? }
const reverseFormation = new Set();   // rids known to run with reversed coach order
// uR.scheduleFormations — maps RID to its formation { fid, coaches: [{coachNumber, coachClass}] }.
// Long-lived (one per service per day); a service may also be re-formed mid-day so
// later messages overwrite earlier.
const formationsByRid = new Map();    // rid -> { fid, coaches: [{number, class}] }
/** Latest RID that has a coach list for this schedule UID (overnight date-flip pairing). */
const formationsByUid = new Map();    // uid -> rid
// uR.serviceLoading — overall load %, set on the per-TIPLOC overlay entry.
// uR.formationLoading — per-coach load values; stashed on the overlay entry too.
// Both decay naturally with the live overlay map.

// uR.OW — Operational Warning (NRCC station messages). Indexed by CRS so a
// board lookup is O(1). Each message is referenced by every CRS it lists.
const stationMessages = new Map();    // crs -> Set<id>
const messagesById    = new Map();    // id  -> { id, severity, category, htmlMessage, plainMessage, stations: [crs], suppress }

// uR.association — service joins/divides/next-portion. Keyed by main RID and
// (mirrored) by associated RID so either side can look up.
const associationsByRid = new Map();  // rid -> Array<{ category, mainRid, assocRid, tiploc, ... }>

function uidFromRid(rid, byRidMap = byRid) {
  const fromJourney = byRidMap?.get?.(rid)?.uid;
  if (fromJourney) return String(fromJourney).trim().toUpperCase();
  const m = String(rid || '').match(/^\d{8}([A-Z0-9]{5,6})/i);
  return m ? m[1].toUpperCase() : '';
}

function formationHasCoaches(f) {
  return !!(f && Array.isArray(f.coaches) && f.coaches.length > 0);
}

function rememberFormation(rid, formation) {
  if (!rid || !formation) return;
  formationsByRid.set(rid, formation);
  const uid = uidFromRid(rid);
  if (uid && formationHasCoaches(formation)) formationsByUid.set(uid, rid);
}

function indexFormationsByUid(fmap, byRidMap) {
  const idx = new Map();
  if (!fmap) return idx;
  for (const [frid, f] of fmap) {
    if (!formationHasCoaches(f)) continue;
    const uid = uidFromRid(frid, byRidMap);
    if (uid) idx.set(uid, frid);
  }
  return idx;
}

function resolveFormationForRid(rid, ctx = null) {
  const fmap = ctx?.formationsByRid || formationsByRid;
  const assocMap = ctx?.associationsByRid || associationsByRid;
  const byRidMap = ctx?.byRid || byRid;
  const uidIndex = ctx?.formationsByUid || (fmap === formationsByRid ? formationsByUid : null);
  const direct = fmap.get(rid);
  if (formationHasCoaches(direct)) return direct;
  for (const rec of assocMap.get(rid) || []) {
    for (const other of [rec.mainRid, rec.assocRid]) {
      if (!other || other === rid) continue;
      const f = fmap.get(other);
      if (formationHasCoaches(f)) return f;
    }
  }
  const uid = uidFromRid(rid, byRidMap);
  if (uid && uidIndex) {
    const altRid = uidIndex.get(uid);
    if (altRid && altRid !== rid) {
      const f = fmap.get(altRid);
      if (formationHasCoaches(f)) return f;
    }
  }
  return direct || null;
}

// uR.trainAlert — short text alerts per service.
const alertsByRid = new Map();        // rid -> Array<{ id, type, audience, text, source, locations }>

// ---------- PTAC (S506 Passenger Train Allocation and Consist) state ------
// Each PTAC message describes one train's physical formation (which units,
// vehicles, defects, etc.). Keyed by Darwin RID after the (ssd, headcode,
// originTpl, originHHMM) join.
const consistByRid = new Map();       // rid -> { allocations: [...], parsedAt, ptacCompany, sourceCore }
// Unit-tracking index: which RIDs has this physical unit worked today, in
// chronological order. Useful for the "follow this unit" page.
const unitsById    = new Map();       // unitId -> { fleetId, vehicles, lastSeenRid, services, endOfDayMileageByDate, lastEndOfDayMiles, updatedAt }
// Stash messages we couldn't immediately match — PTAC unit/formation
// allocations often arrive 12–48h before that working day’s timetable is loaded.
const unmatchedConsists = new Map();  // "ssd|headcode|tpl|hhmm" -> parsed
const PTAC_UNMATCHED_CAP = 25000;

const stats = {
  consumed: 0,
  updates:  0,
  startedAt: new Date().toISOString(),
  lastKafkaMsgAt: null,
};

// ---------- persistence ---------------------------------------------------
// Long-lived caches (formations, station messages, associations, alerts,
// reverseFormation) accumulate slowly because Darwin only broadcasts each
// piece of data once per service per day. Confluent's broker retention is
// shorter than a day, so a daemon restart wipes a lot of useful state. We
// persist these caches to disk every PERSIST_INTERVAL_SEC and on graceful
// shutdown, then reload them on startup. Formations + PTAC consists + units
// are stored as three gzip shards beside each core JSON so stringify stays
// within V8 limits. Live overlays (forecasts, actuals, platform changes) are
// high-churn and Darwin rebroadcasts them frequently.
const STATE_DIR  = resolve(__dirname, 'state');
const STATE_FILE = resolve(STATE_DIR, 'daemon-cache.json');
const STATE_HISTORY_DIR = resolve(STATE_DIR, 'history');
const UNIT_CATALOG_FILE = resolve(STATE_DIR, 'unit-catalog.json');
const SQLITE_PATH = resolve(STATE_DIR, process.env.DARWIN_SQLITE_PATH || 'darwin-state.sqlite');
const STORE_OVERRIDE_FILE = resolve(STATE_DIR, 'store-override.json');
let STORE_ENV = parseStoreEnv();
let sqliteHandle = null;
let sqliteInitError = null;
let lastCatalogLoad = { source: null, ms: null, at: null, fallback: false };
let catalogLoadBySource = { json: null, sqlite: null };

function catalogProcessMem() {
  const m = process.memoryUsage();
  return {
    heapMB: +(m.heapUsed / 1024 / 1024).toFixed(1),
    rssMB: +(m.rss / 1024 / 1024).toFixed(1),
  };
}

function recordCatalogSourceLoad(source, extra = {}) {
  catalogLoadBySource[source] = {
    source,
    at: new Date().toISOString(),
    ...catalogProcessMem(),
    ...extra,
  };
}

function applyStoreOverrideFile() {
  STORE_ENV = parseStoreEnv();
  console.log('[daemon] unit catalog: sqlite only (JSON file not written)');
}
applyStoreOverrideFile();

function persistStoreOverride() {
  try {
    if (!existsSync(STATE_DIR)) mkdirSync(STATE_DIR, { recursive: true });
    const payload = {
      store: 'sqlite',
      jsonWrite: false,
      sqliteWrite: true,
      savedAt: new Date().toISOString(),
    };
    const tmp = STORE_OVERRIDE_FILE + '.tmp';
    writeFileSync(tmp, JSON.stringify(payload, null, 2));
    renameSync(tmp, STORE_OVERRIDE_FILE);
  } catch (e) {
    console.warn(`[daemon] failed to persist store override: ${e.message}`);
  }
}

function fileSizeBytes(path) {
  try { return statSync(path).size; } catch { return null; }
}

function storeSnapshot() {
  const sqliteStats = sqliteHandle ? sqliteHandle.stats() : { error: sqliteInitError, path: SQLITE_PATH, exists: existsSync(SQLITE_PATH) };
  return {
    store: STORE_ENV.store,
    jsonWrite: STORE_ENV.jsonWrite,
    sqliteWrite: STORE_ENV.sqliteWrite,
    jsonFile: UNIT_CATALOG_FILE,
    jsonFileBytes: fileSizeBytes(UNIT_CATALOG_FILE),
    sqlite: sqliteStats,
    lastLoad: lastCatalogLoad,
    loadBySource: catalogLoadBySource,
  };
}
const PERSIST_INTERVAL_SEC = Number(process.env.PERSIST_INTERVAL_SEC || 30);
const KEEP_STATE_ACROSS_DAYS = !['0', 'false', 'no'].includes(String(process.env.KEEP_STATE_ACROSS_DAYS || 'true').toLowerCase());
const PROTECT_RICHER_STATE = !['0', 'false', 'no'].includes(String(process.env.PROTECT_RICHER_STATE || 'true').toLowerCase());
const STATE_SNAPSHOT_COUNT = Math.max(0, Number(process.env.STATE_SNAPSHOT_COUNT || 24));
const STATE_HISTORY_RETENTION_DAYS = Math.max(1, Number(process.env.STATE_HISTORY_RETENTION_DAYS || 90));
const LOOKBACK_TIMETABLE_DAYS = Math.max(0, Number(process.env.DARWIN_LOOKBACK_TIMETABLE_DAYS || 1));
const FUTURE_TIMETABLE_DAYS_ENV = process.env.DARWIN_FUTURE_TIMETABLE_DAYS;
const FUTURE_TIMETABLE_DAYS = Math.max(1, Number(FUTURE_TIMETABLE_DAYS_ENV || 30));
const STATE_HISTORY_PRUNE_ON_PERSIST = !['0', 'false', 'no'].includes(String(process.env.STATE_HISTORY_PRUNE_ON_PERSIST || 'true').toLowerCase());
const STATE_HISTORY_COMPRESS_SNAPSHOTS = !['0', 'false', 'no'].includes(String(process.env.STATE_HISTORY_COMPRESS_SNAPSHOTS || 'true').toLowerCase());
const STATE_HISTORY_GZIP_LEVEL = Math.max(1, Math.min(9, Number(process.env.STATE_HISTORY_GZIP_LEVEL || 6)));
/** Shard gzip level — separate env so operators can favour speed on huge PTAC maps without touching history snaps. */
const STATE_HEAVY_GZIP_LEVEL = Math.max(1, Math.min(9, Number(process.env.STATE_HEAVY_GZIP_LEVEL || STATE_HISTORY_GZIP_LEVEL)));
/** Content-addressed heavy shards: gzip once per unique payload, hard-link 30s snapshot names to the blob. */
const STATE_HISTORY_DEDUP_SHARDS = !['0', 'false', 'no'].includes(String(process.env.STATE_HISTORY_DEDUP_SHARDS || 'true').toLowerCase());
/** Persist 30s points into manifest.json + blobs/ (keep interval; stop extra stamped names by default). */
const STATE_HISTORY_MANIFEST = !['0', 'false', 'no'].includes(String(process.env.STATE_HISTORY_MANIFEST || 'true').toLowerCase());
/** Also write daemon-cache.<stamp> + heavy names (legacy). Off once the loader reads the manifest. */
const STATE_HISTORY_STAMPED_NAMES = ['1', 'true', 'yes'].includes(String(process.env.STATE_HISTORY_STAMPED_NAMES || 'false').toLowerCase());
const HISTORY_EXCLUDE_DATES = new Set(
  String(process.env.HISTORY_EXCLUDE_DATES || '2026-09-20')
    .split(',')
    .map((s) => s.trim())
    .filter((s) => /^\d{4}-\d{2}-\d{2}$/.test(s)),
);
/** Max uncompressed JSON chars per heavy gzip part. Must stay under V8's ~512MB string cap. */
const STATE_HEAVY_JSON_MAX_CHARS = Math.max(8 * 1024 * 1024, Number(process.env.STATE_HEAVY_JSON_MAX_CHARS || 120 * 1024 * 1024));
const HEAVY_SHARD_LABELS = ['formations', 'consist', 'units', 'overlay'];
const lastHeavySplitLog = new Map(); // label -> "parts:rows"
let lastPersistAt = null;
let persistStateBusy = false;
let lastUnitCatalogPersistAt = null;
const unitCatalogById = new Map(); // unitId -> cumulative unit record across days
const historicalContextCache = new Map(); // date -> { loadedAtMs, byRid, overlays }
const historicalContextInflight = new Map(); // cacheKey -> Promise
const historicalTimetableCache = new Map(); // date -> { loadedAtMs, byRid, byTiploc }
const ttisDayCache = new Map(); // ymd -> { loadedAtMs, byRid, byTiploc }
let ttisIndex = null;
let ttisIndexPromise = null;
const historicalStateFileCache = new Map(); // path -> { loadedAtMs, raw }
const historySnapshotListCache = new Map(); // ymd -> { loadedAtMs, list }
const snapshotIndexCache = new Map();       // ymd -> { loadedAtMs, list }
const rawArchivePrunedYmd = new Set();

// ---------- daemon lifecycle mode + warmup tracking ----------------------
// Surfaced in /api/health so callers can tell whether long-lived caches have
// finished warming. Order of transitions:
//   cold_starting -> live_ready -> warming_history -> fully_warm
// The mode never goes backwards once advanced; if the warmup fails we still
// land in fully_warm and surface the error in warmupState.errors.
let daemonMode = 'cold_starting';
/** After background applyPersistedStateRest (formations, PTAC, messages, …). Used to gate client reads when policy is `restored`. */
let liveCachesReady = false;
/** Timetable parse / day rollover / post-fetch: 503 data APIs, skip persist + hot boards. */
let heavyReloadBusy = false;
let heavyReloadReason = '';
let heavyReloadStartedAt = 0;

function beginHeavyReload(reason) {
  heavyReloadBusy = true;
  heavyReloadReason = reason || 'reload';
  heavyReloadStartedAt = Date.now();
  liveCachesReady = false;
  console.log(`[daemon] pausing client reads, persist, and hot boards for ${heavyReloadReason}`);
}

function endHeavyReload() {
  if (!heavyReloadBusy) return;
  const ms = Date.now() - heavyReloadStartedAt;
  const reason = heavyReloadReason;
  heavyReloadBusy = false;
  heavyReloadReason = '';
  heavyReloadStartedAt = 0;
  liveCachesReady = true;
  console.log(`[daemon] resumed client reads after ${reason} (${(ms / 1000).toFixed(1)}s)`);
}
const warmupState = {
  enabled: !['0', 'false', 'no'].includes(String(process.env.WARMUP_ENABLED || 'true').toLowerCase()),
  days: Math.max(0, Number(process.env.WARMUP_DAYS || 7)),
  startedAt: null,
  finishedAt: null,
  current: null,
  done: [],
  skipped: [],
  errors: [],
};
// Historical timetable XML parsing is ~20–30s per date on the VM — keep parsed
// indexes hot for a full railway day so browsing ?date=&at= stays responsive
// after idle gaps. (Still bounded by HIST_TIMETABLE_CACHE_MAX.)
const HIST_TIMETABLE_CACHE_TTL_MS = Number(process.env.HIST_TIMETABLE_CACHE_TTL_MS || 24 * 60 * 60_000);
const HIST_TIMETABLE_CACHE_MAX = Math.max(1, Number(process.env.HIST_TIMETABLE_CACHE_MAX || 21));
const HIST_CONTEXT_CACHE_TTL_MS = Number(process.env.HIST_CONTEXT_CACHE_TTL_MS || 2 * 60 * 60_000);
const HIST_CONTEXT_CACHE_MAX = Math.max(1, Number(process.env.HIST_CONTEXT_CACHE_MAX || 8));
const HIST_STATE_FILE_CACHE_TTL_MS = Number(process.env.HIST_STATE_FILE_CACHE_TTL_MS || 60 * 60_000);
const HIST_STATE_FILE_CACHE_MAX = Math.max(1, Number(process.env.HIST_STATE_FILE_CACHE_MAX || 6));
/** Snapshot list re-reads `snapshot-index.json` / directory; keep hot longer to avoid disk hits on busy days. */
const HIST_SNAPSHOT_LIST_CACHE_TTL_MS = Number(process.env.HIST_SNAPSHOT_LIST_CACHE_TTL_MS || 10 * 60_000);
/** Cap `historySnapshotListCache` entries (was incorrectly using HIST_CONTEXT_CACHE_MAX). */
const HIST_SNAPSHOT_LIST_CACHE_MAX = Math.max(4, Number(process.env.HIST_SNAPSHOT_LIST_CACHE_MAX || 48));
/** Browser may reuse identical historical ?date= URLs for this many seconds (0 = always no-store). */
const HIST_DEPARTURES_HTTP_MAX_AGE_SEC = Math.max(0, Number(process.env.HIST_DEPARTURES_HTTP_MAX_AGE_SEC || 120));

function pruneHistorySnapshotListCache() {
  if (historySnapshotListCache.size <= HIST_SNAPSHOT_LIST_CACHE_MAX) return;
  const ordered = [...historySnapshotListCache.entries()].sort((a, b) => (a[1].loadedAtMs || 0) - (b[1].loadedAtMs || 0));
  for (const [k] of ordered.slice(0, historySnapshotListCache.size - HIST_SNAPSHOT_LIST_CACHE_MAX)) historySnapshotListCache.delete(k);
}

function isIsoDate(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s || ''));
}

function ymdToCompact(ymd) {
  return String(ymd || '').replace(/-/g, '');
}

function compareIsoDate(a, b) {
  if (!isIsoDate(a) || !isIsoDate(b)) return 0;
  return a.localeCompare(b);
}

function addDaysIsoDate(ymd, days) {
  const base = new Date(`${ymd}T00:00:00Z`);
  if (!Number.isFinite(base.getTime())) return ymd;
  base.setUTCDate(base.getUTCDate() + days);
  return base.toISOString().slice(0, 10);
}

function timetableFileWindow(loadedDay) {
  return {
    min: addDaysIsoDate(loadedDay, -LOOKBACK_TIMETABLE_DAYS),
    max: addDaysIsoDate(loadedDay, FUTURE_TIMETABLE_DAYS),
  };
}

function maxSupportedTimetableDate(loadedDay) {
  const envCap = addDaysIsoDate(loadedDay, FUTURE_TIMETABLE_DAYS);
  const ttisMax = ttisIndex?.dateMaxIso;
  if (ttisMax && isIsoDate(ttisMax)) {
    if (!FUTURE_TIMETABLE_DAYS_ENV) return ttisMax;
    return compareIsoDate(ttisMax, envCap) < 0 ? ttisMax : envCap;
  }
  return envCap;
}

function isInTimetableFileWindow(ymdDashed, loadedDay) {
  if (!isIsoDate(ymdDashed) || !isIsoDate(loadedDay)) return false;
  const { min, max } = timetableFileWindow(loadedDay);
  return compareIsoDate(ymdDashed, min) >= 0 && compareIsoDate(ymdDashed, max) <= 0;
}

function filterJourneyMapsToSsd(byRidSrc, byTiplocSrc, ymdDashed) {
  const want = normalizeSsdYmd(ymdDashed);
  const nextByRid = new Map();
  for (const [rid, j] of byRidSrc || []) {
    if (normalizeSsdYmd(j?.ssd) === want) nextByRid.set(rid, j);
  }
  const nextByTiploc = new Map();
  for (const [tpl, entries] of byTiplocSrc || []) {
    const kept = (entries || []).filter((e) => nextByRid.has(e.rid));
    if (kept.length) nextByTiploc.set(tpl, kept);
  }
  return { byRid: nextByRid, byTiploc: nextByTiploc };
}

function availableHistoryDates() {
  let entries = [];
  try { entries = readdirSync(STATE_HISTORY_DIR); } catch { return []; }
  return entries.filter((d) => isIsoDate(d)).sort().reverse();
}

function availableTimetableIsoDates() {
  const ttRoot = resolve(__dirname, './tt');
  let entries = [];
  try { entries = readdirSync(ttRoot); } catch { return []; }
  const out = [];
  for (const d of entries) {
    if (!/^\d{8}$/.test(d)) continue;
    out.push(`${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`);
  }
  return out;
}

function knownBoardDates() {
  return [...new Set([...availableHistoryDates(), ...availableTimetableIsoDates()])]
    .filter((d) => !HISTORY_EXCLUDE_DATES.has(d))
    .sort()
    .reverse();
}

function pruneHistoryDirsByRetention() {
  if (!STATE_HISTORY_PRUNE_ON_PERSIST) return;
  const cutoff = Date.now() - (STATE_HISTORY_RETENTION_DAYS * 24 * 60 * 60 * 1000);
  const dates = availableHistoryDates();
  for (const d of dates) {
    const ts = Date.parse(`${d}T00:00:00Z`);
    if (!Number.isFinite(ts) || ts >= cutoff) continue;
    try {
      rmSync(resolve(STATE_HISTORY_DIR, d), { recursive: true, force: true });
    } catch {}
  }
}

function pruneRawArchiveByRetention() {
  if (!cfg.rawArchiveEnabled) return;
  let dayDirs = [];
  try { dayDirs = readdirSync(cfg.rawArchiveDir); } catch { return; }
  const cutoff = Date.now() - (cfg.rawArchiveRetentionDays * 24 * 60 * 60 * 1000);
  for (const d of dayDirs) {
    if (!/^\d{8}$/.test(d)) continue;
    const ymd = `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`;
    const ts = Date.parse(`${ymd}T00:00:00Z`);
    if (!Number.isFinite(ts) || ts >= cutoff) continue;
    try { rmSync(resolve(cfg.rawArchiveDir, d), { recursive: true, force: true }); } catch {}
  }
}

function archiveRawFeed(feed, rawBuf) {
  if (!cfg.rawArchiveEnabled || !rawBuf) return;
  try {
    const now = new Date();
    const ymd = `${now.getUTCFullYear()}${String(now.getUTCMonth() + 1).padStart(2, '0')}${String(now.getUTCDate()).padStart(2, '0')}`;
    const hh = String(now.getUTCHours()).padStart(2, '0');
    const dayDir = resolve(cfg.rawArchiveDir, ymd);
    if (!existsSync(dayDir)) mkdirSync(dayDir, { recursive: true });
    const out = resolve(dayDir, `${feed}-${hh}.ndjson${cfg.rawArchiveCompress ? '.gz' : ''}`);
    const line = JSON.stringify({
      ts: now.toISOString(),
      feed,
      payload: rawBuf.toString('utf8'),
    }) + '\n';
    if (cfg.rawArchiveCompress) appendFileSync(out, gzipSync(line));
    else appendFileSync(out, line);
    if (!rawArchivePrunedYmd.has(ymd)) {
      pruneRawArchiveByRetention();
      rawArchivePrunedYmd.add(ymd);
      if (rawArchivePrunedYmd.size > 7) {
        for (const v of [...rawArchivePrunedYmd].slice(0, rawArchivePrunedYmd.size - 7)) rawArchivePrunedYmd.delete(v);
      }
    }
  } catch (e) {
    console.warn(`[archive] failed writing ${feed} raw message: ${e.message}`);
  }
}

function stateScore(raw = {}) {
  const count = (arr) => Array.isArray(arr) ? arr.length : 0;
  return (
    count(raw.formations) * 5
    + count(raw.consistByRid) * 4
    + count(raw.unitsById) * 4
    + count(raw.associations) * 2
    + count(raw.messagesById)
    + count(raw.stationMessages)
    + count(raw.alerts)
    + count(raw.reverseFormation)
    + count(raw.unmatchedConsists)
  );
}

/** Path prefix (no suffix) for formations/consist/units shards next to a core state file. */
function heavyStemForCore(corePath) {
  const dir = dirname(corePath);
  const base = basename(corePath);
  if (base === 'daemon-cache.json') return resolve(dir, 'daemon-cache-heavy');
  if (base === 'daemon-cache.latest.json') return resolve(dir, 'daemon-cache-heavy.latest');
  let m = /^daemon-cache\.(.+)\.json$/.exec(base);
  if (m) return resolve(dir, `daemon-cache-heavy.${m[1]}`);
  m = /^daemon-cache\.(.+)\.json\.gz$/.exec(base);
  if (m) return resolve(dir, `daemon-cache-heavy.${m[1]}`);
  return null;
}

function deleteHeavyLabelFiles(stem, label) {
  try { unlinkSync(`${stem}.${label}.json.gz`); } catch {}
  for (let i = 0; i < 512; i++) {
    const p = `${stem}.${label}.${i}.json.gz`;
    if (!existsSync(p)) break;
    try { unlinkSync(p); } catch {}
  }
}

function deleteHeavyShards(stem) {
  if (!stem) return;
  for (const label of HEAVY_SHARD_LABELS) deleteHeavyLabelFiles(stem, label);
}

/** Split an array into JSON-array strings small enough to stringify/gzip. */
async function jsonStringifyArrayParts(arr, maxChars = STATE_HEAVY_JSON_MAX_CHARS) {
  const items = Array.isArray(arr) ? arr : [];
  if (items.length === 0) return ['[]'];
  const parts = [];
  let pieces = [];
  let chars = 2;
  let n = 0;
  for (const item of items) {
    const s = JSON.stringify(item);
    const piece = s === undefined ? 'null' : s;
    const extra = (pieces.length ? 1 : 0) + piece.length;
    if (pieces.length && chars + extra > maxChars) {
      parts.push(`[${pieces.join(',')}]`);
      pieces = [];
      chars = 2;
      await sleepMs(0);
    }
    pieces.push(piece);
    chars += extra;
    n++;
    if (n % 80 === 0) await sleepMs(0);
  }
  parts.push(`[${pieces.join(',')}]`);
  return parts;
}

async function writeGzipJsonFile(destPath, json) {
  const tmpPath = `${destPath}.tmp`;
  const gzip = createGzip({ level: STATE_HEAVY_GZIP_LEVEL });
  const out = createWriteStream(tmpPath);
  const piped = pipeline(gzip, out);
  const CHUNK = 64 * 1024;
  for (let i = 0; i < json.length; i += CHUNK) {
    const slice = json.slice(i, i + CHUNK);
    if (!gzip.write(slice)) await new Promise((r) => gzip.once('drain', r));
    if (i % (CHUNK * 4) === 0) await sleepMs(0);
  }
  gzip.end();
  await piped;
  renameSync(tmpPath, destPath);
}

function heavyShardBlobDir(ymdDashed) {
  return resolve(STATE_HISTORY_DIR, ymdDashed, 'blobs');
}

function hashHeavyShardJson(json) {
  return createHash('sha256').update(json).digest('hex');
}

/** Write gzip blob(s) without ever JSON.stringify-ing the whole array. */
async function ensureHeavyBlobs(blobDir, label, data) {
  const arr = Array.isArray(data) ? data : [];
  if (label === 'overlay' && arr.length === 0) return [];
  if (!existsSync(blobDir)) mkdirSync(blobDir, { recursive: true });
  const parts = await jsonStringifyArrayParts(arr);
  const splitKey = `${parts.length}:${arr.length}`;
  if (parts.length > 1 && lastHeavySplitLog.get(label) !== splitKey) {
    lastHeavySplitLog.set(label, splitKey);
    console.log(`[persist] ${label} split into ${parts.length} gzip parts (${arr.length} rows)`);
  }
  const paths = [];
  for (let i = 0; i < parts.length; i++) {
    const json = parts[i];
    const hash = hashHeavyShardJson(json);
    const name = parts.length === 1 ? `${label}.${hash}.json.gz` : `${label}.${i}.${hash}.json.gz`;
    const blobPath = resolve(blobDir, name);
    if (!existsSync(blobPath)) await writeGzipJsonFile(blobPath, json);
    paths.push(blobPath);
    await sleepMs(0);
  }
  return paths;
}

function installShardFromBlob(blobPath, destPath) {
  if (!blobPath) {
    try { unlinkSync(destPath); } catch {}
    return;
  }
  try {
    if (existsSync(destPath)) {
      try {
        const a = statSync(destPath);
        const b = statSync(blobPath);
        if (a.ino === b.ino && a.dev === b.dev) return;
      } catch {}
      unlinkSync(destPath);
    }
  } catch {}
  try {
    linkSync(blobPath, destPath);
  } catch {
    copyFileSync(blobPath, destPath);
  }
}

function installHeavyLabelFromBlobs(stem, label, blobPaths) {
  const paths = Array.isArray(blobPaths) ? blobPaths.filter(Boolean) : (blobPaths ? [blobPaths] : []);
  const single = `${stem}.${label}.json.gz`;
  if (paths.length <= 1) {
    installShardFromBlob(paths[0] || null, single);
    for (let i = 0; i < 512; i++) {
      const p = `${stem}.${label}.${i}.json.gz`;
      if (!existsSync(p)) break;
      try { unlinkSync(p); } catch {}
    }
    return;
  }
  try { unlinkSync(single); } catch {}
  paths.forEach((blobPath, i) => installShardFromBlob(blobPath, `${stem}.${label}.${i}.json.gz`));
  for (let i = paths.length; i < 512; i++) {
    const p = `${stem}.${label}.${i}.json.gz`;
    if (!existsSync(p)) break;
    try { unlinkSync(p); } catch {}
  }
}

function installHeavyShardsFromBlobs(stem, blobs) {
  if (!stem || !blobs) return;
  for (const label of HEAVY_SHARD_LABELS) {
    installHeavyLabelFromBlobs(stem, label, blobs[label] || []);
  }
}

async function buildHeavyShardBlobs(blobDir, formations, consistByRid, unitsById, liveOverlayEntries) {
  return {
    formations: await ensureHeavyBlobs(blobDir, 'formations', formations),
    consist: await ensureHeavyBlobs(blobDir, 'consist', consistByRid),
    units: await ensureHeavyBlobs(blobDir, 'units', unitsById),
    overlay: await ensureHeavyBlobs(blobDir, 'overlay', liveOverlayEntries),
  };
}

async function writeHeavyLabelParts(stem, label, arr) {
  const data = Array.isArray(arr) ? arr : [];
  if (label === 'overlay' && data.length === 0) {
    deleteHeavyLabelFiles(stem, label);
    return;
  }
  const parts = await jsonStringifyArrayParts(data);
  if (parts.length === 1) {
    await writeGzipJsonFile(`${stem}.${label}.json.gz`, parts[0]);
    for (let i = 0; i < 512; i++) {
      const p = `${stem}.${label}.${i}.json.gz`;
      if (!existsSync(p)) break;
      try { unlinkSync(p); } catch {}
    }
    return;
  }
  try { unlinkSync(`${stem}.${label}.json.gz`); } catch {}
  for (let i = 0; i < parts.length; i++) {
    await writeGzipJsonFile(`${stem}.${label}.${i}.json.gz`, parts[i]);
    await sleepMs(0);
  }
  for (let i = parts.length; i < 512; i++) {
    const p = `${stem}.${label}.${i}.json.gz`;
    if (!existsSync(p)) break;
    try { unlinkSync(p); } catch {}
  }
}

async function writeHeavyShardsAtomic(stem, formations, consistByRid, unitsById, liveOverlayEntries, blobDir = null) {
  if (!stem) return;
  const overlay = Array.isArray(liveOverlayEntries) ? liveOverlayEntries : [];
  if (STATE_HISTORY_DEDUP_SHARDS && blobDir) {
    installHeavyShardsFromBlobs(stem, await buildHeavyShardBlobs(blobDir, formations, consistByRid, unitsById, overlay));
    return;
  }
  await writeHeavyLabelParts(stem, 'formations', formations);
  await writeHeavyLabelParts(stem, 'consist', consistByRid);
  await writeHeavyLabelParts(stem, 'units', unitsById);
  await writeHeavyLabelParts(stem, 'overlay', overlay);
}

const HEAVY_SHARD_FILE_RE = /^daemon-cache-heavy\.(.+)\.(formations|consist|units|overlay)(?:\.(\d+))?\.json\.gz$/;

function collectBlobInodes(blobDir) {
  const inodes = new Set();
  let names = [];
  try { names = readdirSync(blobDir); } catch { return inodes; }
  for (const name of names) {
    if (!name.endsWith('.json.gz')) continue;
    try {
      const st = statSync(resolve(blobDir, name));
      if (st.isFile()) inodes.add(`${st.dev}:${st.ino}`);
    } catch {}
  }
  return inodes;
}

/**
 * Collapse duplicate gzip heavy shards in `dir` onto content-addressed files in `blobDir`.
 * Snapshot names stay the same; they become hard links. Skips files newer than skipRecentMs
 * so a live persist cannot race the converter.
 */
function convertDirHeavyShardsToBlobs(dir, blobDir, skipRecentMs = 0) {
  const summary = { files: 0, newBlobs: 0, linked: 0, already: 0, skipped: 0 };
  let names = [];
  try { names = readdirSync(dir); } catch { return summary; }
  if (!existsSync(blobDir)) mkdirSync(blobDir, { recursive: true });
  const blobInodes = collectBlobInodes(blobDir);
  const gzipHashToBlob = new Map();
  const now = Date.now();
  for (const name of names) {
    if (!HEAVY_SHARD_FILE_RE.test(name)) continue;
    const destPath = resolve(dir, name);
    let st;
    try { st = statSync(destPath); } catch { continue; }
    if (!st.isFile()) continue;
    summary.files++;
    if (skipRecentMs > 0 && (now - st.mtimeMs) < skipRecentMs) {
      summary.skipped++;
      continue;
    }
    const inodeKey = `${st.dev}:${st.ino}`;
    if (blobInodes.has(inodeKey)) {
      summary.already++;
      continue;
    }
    const label = name.match(HEAVY_SHARD_FILE_RE)[2];
    let gzipBuf;
    try { gzipBuf = readFileSync(destPath); } catch { summary.skipped++; continue; }
    const gzipHash = createHash('sha256').update(gzipBuf).digest('hex');
    let blobPath = gzipHashToBlob.get(gzipHash);
    if (!blobPath) {
      let json;
      try { json = gunzipSync(gzipBuf).toString('utf8'); } catch { summary.skipped++; continue; }
      const jsonHash = hashHeavyShardJson(json);
      blobPath = resolve(blobDir, `${label}.${jsonHash}.json.gz`);
      if (!existsSync(blobPath)) {
        try {
          renameSync(destPath, blobPath);
        } catch {
          const tmpPath = `${blobPath}.tmp`;
          writeFileSync(tmpPath, gzipBuf);
          renameSync(tmpPath, blobPath);
        }
        summary.newBlobs++;
        try {
          const bst = statSync(blobPath);
          blobInodes.add(`${bst.dev}:${bst.ino}`);
        } catch {}
        gzipHashToBlob.set(gzipHash, blobPath);
        try {
          linkSync(blobPath, destPath);
          summary.linked++;
        } catch {
          try { copyFileSync(blobPath, destPath); } catch {}
        }
        continue;
      }
      gzipHashToBlob.set(gzipHash, blobPath);
    }
    try {
      const a = statSync(destPath);
      const b = statSync(blobPath);
      if (a.ino === b.ino && a.dev === b.dev) {
        summary.already++;
        continue;
      }
    } catch {}
    try { unlinkSync(destPath); } catch {}
    try {
      linkSync(blobPath, destPath);
      summary.linked++;
    } catch {
      try { copyFileSync(blobPath, destPath); } catch {}
    }
  }
  return summary;
}

function convertDayHeavyShardsToBlobs(ymdDashed, skipRecentMs = 0) {
  return convertDirHeavyShardsToBlobs(
    resolve(STATE_HISTORY_DIR, ymdDashed),
    heavyShardBlobDir(ymdDashed),
    skipRecentMs,
  );
}

function convertAllHistoryHeavyShardsToBlobs(opts = {}) {
  const skipRecentMs = opts.skipRecentMs ?? 120_000;
  const dates = availableHistoryDates().slice().reverse();
  const totals = { dates: 0, files: 0, newBlobs: 0, linked: 0, already: 0, skipped: 0 };
  for (const ymd of dates) {
    const one = convertDayHeavyShardsToBlobs(ymd, skipRecentMs);
    totals.dates++;
    totals.files += one.files;
    totals.newBlobs += one.newBlobs;
    totals.linked += one.linked;
    totals.already += one.already;
    totals.skipped += one.skipped;
    console.log(`[persist] convert ${ymd}: ${one.newBlobs} blobs, ${one.linked} linked, ${one.already} already, ${one.skipped} skipped (${one.files} shard files)`);
  }
  try {
    const today = railwayDayYmd(new Date());
    const stateDir = convertDirHeavyShardsToBlobs(STATE_DIR, heavyShardBlobDir(today), skipRecentMs);
    totals.files += stateDir.files;
    totals.newBlobs += stateDir.newBlobs;
    totals.linked += stateDir.linked;
    totals.already += stateDir.already;
    totals.skipped += stateDir.skipped;
    console.log(`[persist] convert state/: ${stateDir.newBlobs} blobs, ${stateDir.linked} linked, ${stateDir.already} already, ${stateDir.skipped} skipped`);
  } catch (e) {
    console.warn(`[persist] convert state/ failed: ${e.message}`);
  }
  return totals;
}

function heavyShardArrayLen(v) {
  return Array.isArray(v) ? v.length : 0;
}

/**
 * Merge one gzip shard array into `raw[field]`.
 * If the shard parses empty but the core still has a richer inline array (legacy / crash ordering),
 * keep inline — empty shards often come from PROTECT splits or partial writes and must not wipe PTAC/units.
 */
function parseGzipJsonArrayFile(gzPath) {
  if (!existsSync(gzPath)) return null;
  let fromShard;
  try {
    fromShard = JSON.parse(gunzipSync(readFileSync(gzPath)).toString('utf8'));
  } catch (e) {
    console.warn(`[daemon] skip unreadable shard ${gzPath}: ${e.message}`);
    return null;
  }
  return fromShard;
}

function attachHeavyArrayShard(raw, field, gzPath) {
  const fromShard = parseGzipJsonArrayFile(gzPath);
  if (fromShard == null) return;
  const prev = raw[field];
  const prevN = heavyShardArrayLen(prev);
  const shN = heavyShardArrayLen(fromShard);
  if (shN > 0) raw[field] = fromShard;
  else if (prevN === 0) raw[field] = fromShard;
}

function attachHeavyField(raw, field, stem, label) {
  const part0 = `${stem}.${label}.0.json.gz`;
  if (existsSync(part0)) {
    const merged = [];
    for (let i = 0; i < 512; i++) {
      const p = `${stem}.${label}.${i}.json.gz`;
      if (!existsSync(p)) break;
      try {
        const arr = JSON.parse(gunzipSync(readFileSync(p)).toString('utf8'));
        if (Array.isArray(arr)) merged.push(...arr);
      } catch {}
    }
    const prevN = heavyShardArrayLen(raw[field]);
    if (merged.length > 0) raw[field] = merged;
    else if (prevN === 0) raw[field] = merged;
    return;
  }
  attachHeavyArrayShard(raw, field, `${stem}.${label}.json.gz`);
}

/** Merge gzip shards written beside core JSON (schema v2). Legacy cores still carry inline arrays. */
function attachHeavyShards(raw, corePath, opts = {}) {
  if (!raw || typeof raw !== 'object') return raw;
  const stem = heavyStemForCore(corePath);
  if (!stem) return raw;
  try {
    if (!opts.skipFormations) attachHeavyField(raw, 'formations', stem, 'formations');
    if (!opts.skipConsist) attachHeavyField(raw, 'consistByRid', stem, 'consist');
    if (!opts.skipUnits) attachHeavyField(raw, 'unitsById', stem, 'units');
    if (!opts.skipOverlay) attachHeavyField(raw, 'liveOverlayByRid', stem, 'overlay');
  } catch (e) {
    console.warn(`[daemon] failed to merge heavy state shards for ${corePath}: ${e.message}`);
  }
  return raw;
}

/** Historical boards need overlay + formations. Consist/units unzip to hundreds of MB and OOM the box. */
function historicalHeavyOpts(kind = 'board') {
  if (kind === 'core') {
    return { skipFormations: true, skipConsist: true, skipUnits: true, skipOverlay: true };
  }
  return { skipFormations: false, skipConsist: true, skipUnits: true, skipOverlay: false };
}

function readMergedStateFromDisk() {
  if (!existsSync(STATE_FILE)) return null;
  try {
    const raw = JSON.parse(readFileSync(STATE_FILE, 'utf8'));
    return attachHeavyShards(raw, STATE_FILE);
  } catch {
    return null;
  }
}

/** Strip heavy arrays + live overlay for core JSON; shards hold gzip JSON (even when []). */
function splitMergedForPersist(merged) {
  const formations = Array.isArray(merged.formations) ? merged.formations : [];
  const consistByRid = Array.isArray(merged.consistByRid) ? merged.consistByRid : [];
  const unitsById = Array.isArray(merged.unitsById) ? merged.unitsById : [];
  const liveOverlayByRid = Array.isArray(merged.liveOverlayByRid) ? merged.liveOverlayByRid : [];
  const {
    formations: _fa,
    consistByRid: _ca,
    unitsById: _ua,
    liveOverlayByRid: _lo,
    ...coreRest
  } = merged;
  const core = {
    ...coreRest,
    stateSchema: merged.stateSchema ?? 2,
  };
  return { core, formations, consistByRid, unitsById, liveOverlayByRid };
}

function pruneOldStateSnapshots() {
  if (STATE_SNAPSHOT_COUNT <= 0) return;
  const prefix = 'daemon-cache.';
  const suffix = '.json';
  let files = [];
  try { files = readdirSync(STATE_DIR); } catch { return; }
  const snapshots = files
    .filter((f) => f.startsWith(prefix) && f.endsWith(suffix) && f !== 'daemon-cache.json')
    .sort()
    .reverse();
  for (const stale of snapshots.slice(STATE_SNAPSHOT_COUNT)) {
    try { unlinkSync(resolve(STATE_DIR, stale)); } catch {}
    const stampMatch = /^daemon-cache\.(.+)\.json$/.exec(stale);
    if (stampMatch) {
      deleteHeavyShards(resolve(STATE_DIR, `daemon-cache-heavy.${stampMatch[1]}`));
    }
  }
}

function toMap(entries) {
  const m = new Map();
  if (!Array.isArray(entries)) return m;
  for (const [k, v] of entries) m.set(k, v);
  return m;
}

/** Overlay locs stay as arrays until a RID is actually read (service/board row). */
function lazyLiveOverlay(entries) {
  if (entries instanceof Map) return entries;
  const index = new Map();
  if (Array.isArray(entries)) {
    for (const pair of entries) {
      if (pair && pair[0] != null) index.set(pair[0], pair[1]);
    }
  }
  const hydrated = new Map();
  return {
    get(rid) {
      if (hydrated.has(rid)) return hydrated.get(rid);
      const ov = index.get(rid);
      if (!ov) return undefined;
      const locs = Array.isArray(ov.locs)
        ? new Map(ov.locs)
        : (ov.locs instanceof Map ? ov.locs : new Map());
      const next = { ...ov, locs };
      hydrated.set(rid, next);
      return next;
    },
    has(rid) {
      return hydrated.has(rid) || index.has(rid);
    },
  };
}

function serializeLiveOverlayEntries() {
  return [...liveOverlayByRid.entries()].map(([rid, ov]) => [
    rid,
    {
      ...(ov || {}),
      locs: [...((ov?.locs || new Map()).entries())],
    },
  ]);
}

function isActualLiveKind(kind) {
  return typeof kind === 'string' && kind.startsWith('actual');
}

function locActualSlice(entry) {
  if (!entry || typeof entry !== 'object') return null;
  const out = {};
  if (isActualLiveKind(entry.bestKind) && entry.bestTime) {
    out.bestTime = entry.bestTime;
    out.bestKind = entry.bestKind;
    if (entry.liveSource) out.liveSource = entry.liveSource;
    if (entry.liveSourceInstance) out.liveSourceInstance = entry.liveSourceInstance;
  }
  if (isActualLiveKind(entry.arrLiveKind) && entry.arrLiveTime) {
    out.arrLiveTime = entry.arrLiveTime;
    out.arrLiveKind = entry.arrLiveKind;
    if (entry.arrLiveSource) out.arrLiveSource = entry.arrLiveSource;
    if (entry.arrLiveSourceInstance) out.arrLiveSourceInstance = entry.arrLiveSourceInstance;
  }
  return Object.keys(out).length ? out : null;
}

function extractActualsFromOverlayEntries(overlayEntries) {
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
      const slice = locActualSlice(locPair?.[1]);
      if (tpl && slice) locMap[String(tpl).toUpperCase()] = slice;
    }
    if (Object.keys(locMap).length) rids[rid] = locMap;
  }
  return rids;
}

function mergeActualsMaps(into, from) {
  for (const [rid, locs] of Object.entries(from || {})) {
    if (!into[rid]) into[rid] = {};
    for (const [tpl, slice] of Object.entries(locs || {})) {
      const prev = into[rid][tpl] || {};
      const next = { ...prev };
      if (isActualLiveKind(slice.bestKind) && slice.bestTime) {
        next.bestTime = slice.bestTime;
        next.bestKind = slice.bestKind;
        if (slice.liveSource) next.liveSource = slice.liveSource;
        if (slice.liveSourceInstance) next.liveSourceInstance = slice.liveSourceInstance;
      }
      if (isActualLiveKind(slice.arrLiveKind) && slice.arrLiveTime) {
        next.arrLiveTime = slice.arrLiveTime;
        next.arrLiveKind = slice.arrLiveKind;
        if (slice.arrLiveSource) next.arrLiveSource = slice.arrLiveSource;
        if (slice.arrLiveSourceInstance) next.arrLiveSourceInstance = slice.arrLiveSourceInstance;
      }
      into[rid][tpl] = next;
    }
  }
}

function actualsArchivePath(ymdDashed) {
  return resolve(STATE_HISTORY_DIR, ymdDashed, 'actuals.json.gz');
}

function loadActualsArchive(ymdDashed) {
  const p = actualsArchivePath(ymdDashed);
  if (!existsSync(p)) return {};
  try {
    const parsed = JSON.parse(gunzipSync(readFileSync(p)).toString('utf8'));
    return parsed?.rids && typeof parsed.rids === 'object' ? parsed.rids : {};
  } catch {
    return {};
  }
}

function persistActualsArchive(ymdDashed, overlayEntries) {
  if (!isIsoDate(ymdDashed)) return;
  const extracted = extractActualsFromOverlayEntries(overlayEntries);
  const existingPath = actualsArchivePath(ymdDashed);
  if (!Object.keys(extracted).length && !existsSync(existingPath)) return;
  const dayDir = resolve(STATE_HISTORY_DIR, ymdDashed);
  if (!existsSync(dayDir)) mkdirSync(dayDir, { recursive: true });
  const merged = loadActualsArchive(ymdDashed);
  mergeActualsMaps(merged, extracted);
  const tmp = existingPath + '.tmp';
  writeFileSync(tmp, gzipSync(JSON.stringify({ savedAt: new Date().toISOString(), rids: merged }), { level: STATE_HISTORY_GZIP_LEVEL }));
  renameSync(tmp, existingPath);
}

function applyActualsToOverlayEntries(overlayEntries, actualsRids) {
  if (!actualsRids || !Object.keys(actualsRids).length) return overlayEntries || [];
  const byRid = new Map(Array.isArray(overlayEntries) ? overlayEntries : []);
  for (const [rid, locs] of Object.entries(actualsRids)) {
    const ov = byRid.get(rid) || { locs: [] };
    const locMap = new Map(Array.isArray(ov.locs) ? ov.locs : []);
    for (const [tpl, slice] of Object.entries(locs || {})) {
      const prev = locMap.get(tpl) || {};
      const next = { ...prev };
      if (isActualLiveKind(slice.bestKind) && !isActualLiveKind(next.bestKind)) {
        next.bestTime = slice.bestTime;
        next.bestKind = slice.bestKind;
        if (slice.liveSource) next.liveSource = slice.liveSource;
        if (slice.liveSourceInstance) next.liveSourceInstance = slice.liveSourceInstance;
      }
      if (isActualLiveKind(slice.arrLiveKind) && !isActualLiveKind(next.arrLiveKind)) {
        next.arrLiveTime = slice.arrLiveTime;
        next.arrLiveKind = slice.arrLiveKind;
        if (slice.arrLiveSource) next.arrLiveSource = slice.arrLiveSource;
        if (slice.arrLiveSourceInstance) next.arrLiveSourceInstance = slice.arrLiveSourceInstance;
      }
      locMap.set(tpl, next);
    }
    byRid.set(rid, { ...ov, locs: [...locMap.entries()] });
  }
  return [...byRid.entries()];
}

function restoreLiveOverlayEntries(entries) {
  if (!Array.isArray(entries)) return;
  for (const [rid, ov] of entries) {
    if (!rid || !ov) continue;
    const locs = new Map(Array.isArray(ov.locs) ? ov.locs : []);
    liveOverlayByRid.set(rid, { ...ov, locs });
  }
}

function overlayLocsMap(ov) {
  const locs = ov?.locs;
  if (!locs) return null;
  if (locs instanceof Map) return locs;
  if (Array.isArray(locs)) {
    const m = new Map(locs);
    ov.locs = m;
    return m;
  }
  if (typeof locs === 'object') return new Map(Object.entries(locs));
  return null;
}

function overlayLocHasLoading(entry) {
  return Boolean(entry && ((entry.coachLoading && entry.coachLoading.length > 0) || entry.loadPct != null));
}

function getOverlayLoc(ov, tpl) {
  if (!ov || !tpl) return null;
  const locs = overlayLocsMap(ov);
  if (!locs) return null;
  const key = String(tpl).toUpperCase();
  const exact = locs.get(key) || locs.get(tpl) || null;
  if (exact && overlayLocHasLoading(exact)) return exact;
  const crs = resolve_?.tiplocToCrs?.(key);
  const siblings = crs && crsToTiplocs ? crsToTiplocs.get(crs) : null;
  if (Array.isArray(siblings)) {
    let any = exact;
    for (const alt of siblings) {
      const entry = locs.get(alt) || locs.get(String(alt).toUpperCase());
      if (!entry) continue;
      if (overlayLocHasLoading(entry)) return entry;
      if (!any) any = entry;
    }
    if (any) return any;
  }
  return exact;
}

function parseAtToMinutes(at) {
  if (!at) return null;
  const m = /^(\d{1,2}):(\d{2})$/.exec(String(at).trim());
  if (!m) return null;
  const h = Number(m[1]); const mm = Number(m[2]);
  if (!Number.isFinite(h) || !Number.isFinite(mm) || h < 0 || h > 23 || mm < 0 || mm > 59) return null;
  return h * 60 + mm;
}

function londonCalendarDateIso(now = new Date()) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(now);
  const pick = (t) => parts.find((p) => p.type === t)?.value || '00';
  return `${pick('year')}-${pick('month')}-${pick('day')}`;
}

function londonWallClockHmLondon(now = new Date()) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(now);
  const hh = parts.find((p) => p.type === 'hour')?.value || '00';
  const mm = parts.find((p) => p.type === 'minute')?.value || '00';
  return { hh, mm };
}

/**
 * Wall-clock London instant for ?date=&at= — fixed +01:00 matches anchorTime().
 * When opts.anchorNoAtToLiveLondonClock is true and at is omitted, a date equal to
 * today's London calendar date uses the current London hour/minute (not noon) so
 * 00:00–01:59 stays on the correct timetable SSD vs misleading noon anchoring.
 */
function londonWallInstantFromDateAt(dateStr, atStr, opts = {}) {
  const anchorNoAtToLiveLondonClock = !!opts.anchorNoAtToLiveLondonClock;
  const nowRef = opts.nowRef instanceof Date ? opts.nowRef : new Date();
  if (!dateStr || !isIsoDate(dateStr)) return null;
  const atMins = atStr != null ? parseAtToMinutes(atStr) : null;
  if (atMins != null) {
    const hh = String(Math.floor(atMins / 60)).padStart(2, '0');
    const mm = String(atMins % 60).padStart(2, '0');
    return new Date(`${dateStr}T${hh}:${mm}:00+01:00`);
  }
  if (
    anchorNoAtToLiveLondonClock &&
    dateStr === londonCalendarDateIso(nowRef)
  ) {
    const { hh, mm } = londonWallClockHmLondon(nowRef);
    return new Date(`${dateStr}T${hh}:${mm}:00+01:00`);
  }
  return new Date(`${dateStr}T12:00:00+01:00`);
}

function normalizeCancellationInfo(info) {
  if (!info) return null;
  const reason = String(info.reason || '').trim().toLowerCase();
  // Darwin occasionally emits generic "schedule deactivated" cancellations
  // that are not useful for passenger-facing history views.
  if (reason.includes('schedule deactivated')) return null;
  return info;
}

function pruneHistoricalTimetableCache() {
  const now = Date.now();
  for (const [k, v] of historicalTimetableCache.entries()) {
    if (now - (v.loadedAtMs || 0) > HIST_TIMETABLE_CACHE_TTL_MS) historicalTimetableCache.delete(k);
  }
  if (historicalTimetableCache.size <= HIST_TIMETABLE_CACHE_MAX) return;
  const ordered = [...historicalTimetableCache.entries()].sort((a, b) => (a[1].loadedAtMs || 0) - (b[1].loadedAtMs || 0));
  for (const [k] of ordered.slice(0, historicalTimetableCache.size - HIST_TIMETABLE_CACHE_MAX)) {
    historicalTimetableCache.delete(k);
  }
}

async function getHistoricalTimetable(ymdDashed) {
  // Same-day lookups should reuse already-loaded live timetable indexes to
  // avoid expensive synchronous reparsing on request path.
  if (ymdDashed === loadedDate && byRid && byTiploc) {
    return { loadedAtMs: Date.now(), byRid, byTiploc };
  }
  const cached = historicalTimetableCache.get(ymdDashed);
  const now = Date.now();
  if (cached && now - (cached.loadedAtMs || 0) <= HIST_TIMETABLE_CACHE_TTL_MS) return cached;
  const timetable = pickTimetableForDate(ymdDashed);
  if (!timetable) return null;
  const parsed = await loadAllJourneysIndexedByTiploc(timetable);
  const entry = { loadedAtMs: now, byRid: parsed.byRid, byTiploc: parsed.byTiploc };
  historicalTimetableCache.set(ymdDashed, entry);
  pruneHistoricalTimetableCache();
  return entry;
}

// state/history/<date>/snapshot-index.json. Stores a pre-computed sorted list
// of [{ file, ms }] for every snapshot we've persisted on that day. Building
// it once turns the per-request `readdirSync` + filename regex parsing into a
// single fs.read + JSON.parse, which is materially faster on cold dates after
// 24h of accumulated snapshots (~720 files at PERSIST_INTERVAL_SEC=120s).
function snapshotIndexFile(ymdDashed) {
  return resolve(STATE_HISTORY_DIR, ymdDashed, 'snapshot-index.json');
}

function snapshotsFromReaddir(ymdDashed) {
  const dayDir = resolve(STATE_HISTORY_DIR, ymdDashed);
  let files = [];
  try { files = readdirSync(dayDir); } catch { return []; }
  const re = /^daemon-cache\.(.+)\.json(?:\.gz)?$/;
  return files
    .filter((f) => re.test(f) && f !== 'daemon-cache.latest.json')
    .map((f) => {
      const m = f.match(re);
      const stamp = m?.[1] || '';
      const sm = /^(\d{4})-(\d{2})-(\d{2})T(\d{2})-(\d{2})-(\d{2})-(\d{3})Z$/.exec(stamp);
      const iso = sm ? `${sm[1]}-${sm[2]}-${sm[3]}T${sm[4]}:${sm[5]}:${sm[6]}.${sm[7]}Z` : null;
      const t = iso ? Date.parse(iso) : NaN;
      return { file: f, path: resolve(dayDir, f), savedAt: Number.isFinite(t) ? new Date(t).toISOString() : null, ms: Number.isFinite(t) ? t : -1 };
    })
    .filter((e) => e.ms >= 0)
    .sort((a, b) => a.ms - b.ms);
}

function buildSnapshotIndexForDate(ymdDashed) {
  if (!isIsoDate(ymdDashed)) return [];
  const dayDir = resolve(STATE_HISTORY_DIR, ymdDashed);
  if (!existsSync(dayDir)) return [];
  const list = snapshotsFromReaddir(ymdDashed);
  try {
    const indexPath = snapshotIndexFile(ymdDashed);
    const tmp = indexPath + '.tmp';
    const payload = {
      builtAt: new Date().toISOString(),
      date: ymdDashed,
      list: list.map((e) => ({ file: e.file, ms: e.ms, savedAt: e.savedAt })),
    };
    writeFileSync(tmp, JSON.stringify(payload));
    renameSync(tmp, indexPath);
  } catch (e) {
    // Best-effort — failures here just mean we'll rebuild next request.
  }
  snapshotIndexCache.set(ymdDashed, { loadedAtMs: Date.now(), list });
  historySnapshotListCache.set(ymdDashed, { loadedAtMs: Date.now(), list });
  pruneHistorySnapshotListCache();
  return list;
}

function readSnapshotIndexForDate(ymdDashed) {
  const cached = snapshotIndexCache.get(ymdDashed);
  if (cached && Date.now() - (cached.loadedAtMs || 0) <= HIST_SNAPSHOT_LIST_CACHE_TTL_MS) return cached.list;
  const indexPath = snapshotIndexFile(ymdDashed);
  if (!existsSync(indexPath)) return null;
  try {
    const parsed = JSON.parse(readFileSync(indexPath, 'utf8'));
    const dayDir = resolve(STATE_HISTORY_DIR, ymdDashed);
    const list = (parsed.list || [])
      .filter((e) => e && Number.isFinite(e.ms))
      .map((e) => ({ file: e.file, ms: e.ms, savedAt: e.savedAt || null, path: resolve(dayDir, e.file) }))
      .sort((a, b) => a.ms - b.ms);
    snapshotIndexCache.set(ymdDashed, { loadedAtMs: Date.now(), list });
    return list;
  } catch {
    return null;
  }
}

// Binary search — return the latest snapshot whose ms <= cutoffMs, or null.
function findSnapshotAtOrBefore(list, cutoffMs) {
  if (!Array.isArray(list) || list.length === 0) return null;
  let lo = 0, hi = list.length - 1, best = null;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const entry = list[mid];
    if (entry.ms <= cutoffMs) { best = entry; lo = mid + 1; }
    else hi = mid - 1;
  }
  return best;
}

function listHistorySnapshotsForDate(ymdDashed) {
  if (!isIsoDate(ymdDashed)) return [];
  const now = Date.now();
  const cached = historySnapshotListCache.get(ymdDashed);
  if (cached && now - (cached.loadedAtMs || 0) <= HIST_SNAPSHOT_LIST_CACHE_TTL_MS) return cached.list;
  const dayDir = resolve(STATE_HISTORY_DIR, ymdDashed);
  const blobDir = heavyShardBlobDir(ymdDashed);
  const byMs = new Map();
  const indexed = readSnapshotIndexForDate(ymdDashed);
  const fromDisk = indexed || snapshotsFromReaddir(ymdDashed);
  for (const e of fromDisk) byMs.set(e.ms, e);
  const manifest = readHistoryManifest(dayDir);
  if (manifest?.snaps?.length) {
    for (const e of manifestSnapsToList(dayDir, blobDir, manifest.snaps)) byMs.set(e.ms, e);
  }
  const list = [...byMs.values()].sort((a, b) => a.ms - b.ms);
  historySnapshotListCache.set(ymdDashed, { loadedAtMs: now, list });
  pruneHistorySnapshotListCache();
  if (!indexed) {
    setImmediate(() => {
      try { buildSnapshotIndexForDate(ymdDashed); } catch {}
    });
  }
  return list;
}

/** Read gzip/plain JSON state and merge selected shards beside the core file. */
function readHistoricalStateFile(corePath, heavyOpts = historicalHeavyOpts('board')) {
  if (!existsSync(corePath)) return null;
  const now = Date.now();
  const cacheKey = [
    corePath,
    heavyOpts.manifestSnap?.ms || '',
    heavyOpts.skipFormations ? 'F0' : 'F1',
    heavyOpts.skipConsist ? 'C0' : 'C1',
    heavyOpts.skipUnits ? 'U0' : 'U1',
    heavyOpts.skipOverlay ? 'O0' : 'O1',
  ].join('|');
  const cached = historicalStateFileCache.get(cacheKey);
  if (cached && now - (cached.loadedAtMs || 0) <= HIST_STATE_FILE_CACHE_TTL_MS) return cached.raw;
  try {
    let raw;
    if (corePath.endsWith('.gz')) {
      raw = JSON.parse(gunzipSync(readFileSync(corePath)).toString('utf8'));
    } else {
      raw = JSON.parse(readFileSync(corePath, 'utf8'));
    }
    if (heavyOpts.manifestSnap && heavyOpts.blobDir) {
      attachHeavyFromManifestSnap(raw, heavyOpts.blobDir, heavyOpts.manifestSnap, heavyOpts);
    } else {
      attachHeavyShards(raw, corePath, heavyOpts);
    }
    historicalStateFileCache.set(cacheKey, { loadedAtMs: now, raw });
    if (historicalStateFileCache.size > HIST_STATE_FILE_CACHE_MAX) {
      const ordered = [...historicalStateFileCache.entries()].sort((a, b) => (a[1].loadedAtMs || 0) - (b[1].loadedAtMs || 0));
      for (const [k] of ordered.slice(0, historicalStateFileCache.size - HIST_STATE_FILE_CACHE_MAX)) historicalStateFileCache.delete(k);
    }
    return raw;
  } catch {
    return null;
  }
}

function buildOverlayHistorySeries(hours = 36) {
  const nowMs = Date.now();
  const safeHours = Math.max(1, Math.min(168, Number(hours) || 36));
  const cutoffMs = nowMs - (safeHours * 60 * 60 * 1000);
  const points = [];
  const dates = availableHistoryDates().filter((d) => {
    const dayMs = Date.parse(`${d}T00:00:00Z`);
    return Number.isFinite(dayMs) && dayMs >= (cutoffMs - 24 * 60 * 60 * 1000);
  });

  for (const d of dates) {
    const snaps = listHistorySnapshotsForDate(d);
    for (const s of snaps) {
      if (!s.path || !Number.isFinite(s.ms) || s.ms < cutoffMs) continue;
      const raw = readHistoricalStateFile(s.path, historicalHeavyOpts('core'));
      if (!raw) continue;
      points.push({
        savedAt: raw.savedAt || (s.savedAt || null),
        formations: Array.isArray(raw.formations) ? raw.formations.length : 0,
        consists: Array.isArray(raw.consistByRid) ? raw.consistByRid.length : 0,
        units: Array.isArray(raw.unitsById) ? raw.unitsById.length : 0,
      });
    }
  }

  points.sort((a, b) => String(a.savedAt || '').localeCompare(String(b.savedAt || '')));
  return {
    hours: safeHours,
    count: points.length,
    points,
    updatedAt: new Date().toISOString(),
  };
}

function unitDayFromValue(v) {
  const s = String(v ?? '').trim();
  if (/^\d{8}$/.test(s)) return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  const ymd = s.slice(0, 10);
  return isIsoDate(ymd) ? ymd : null;
}

function isRetainedUnitDay(ymd, loadedDay = loadedDate || railwayDayYmd()) {
  if (!isIsoDate(ymd) || ymd < '2024-01-01') return false;
  const min = addDaysIsoDate(loadedDay, -STATE_HISTORY_RETENTION_DAYS);
  const max = addDaysIsoDate(loadedDay, 2);
  return ymd >= min && ymd <= max;
}

function sanitizeUnitMileageByDate(map) {
  const out = {};
  if (!map || typeof map !== 'object') return out;
  for (const [k, v] of Object.entries(map)) {
    const day = unitDayFromValue(k);
    if (!day || !isRetainedUnitDay(day)) continue;
    out[day] = v;
  }
  return out;
}

function sanitizeUnitServices(services) {
  if (!Array.isArray(services)) return [];
  return services.filter((s) => {
    const day = unitDayFromValue(s?.start);
    return day && isRetainedUnitDay(day);
  });
}

function pruneUnitCatalogStaleDays() {
  let dropped = 0;
  for (const entry of unitCatalogById.values()) {
    const beforeMiles = Object.keys(entry.endOfDayMileageByDate || {}).length;
    const beforeSvcs = (entry.services || []).length;
    entry.endOfDayMileageByDate = sanitizeUnitMileageByDate(entry.endOfDayMileageByDate);
    entry.services = sanitizeUnitServices(entry.services);
    dropped += Math.max(0, beforeMiles - Object.keys(entry.endOfDayMileageByDate).length);
    dropped += Math.max(0, beforeSvcs - (entry.services || []).length);
  }
  if (dropped > 0) unitsCatalogCache = { at: 0, payload: null };
  return dropped;
}

function applyUnitCatalogPayload(raw, source) {
  for (const [unitId, entry] of (raw.units || [])) {
    if (!unitId || !entry) continue;
    unitCatalogById.set(unitId, entry);
  }
  const dropped = pruneUnitCatalogStaleDays();
  lastCatalogLoad.source = source;
  lastCatalogLoad.at = new Date().toISOString();
  console.log(`[daemon] restored unit catalog from ${source}: ${unitCatalogById.size} units${dropped ? `, pruned ${dropped} stale day entries` : ''}.`);
}

function loadUnitCatalogFromJson() {
  if (!existsSync(UNIT_CATALOG_FILE)) return false;
  const t0 = Date.now();
  try {
    const raw = JSON.parse(readFileSync(UNIT_CATALOG_FILE, 'utf8'));
    applyUnitCatalogPayload(raw, 'json');
    recordCatalogSourceLoad('json', {
      ms: Date.now() - t0,
      units: unitCatalogById.size,
      bytes: fileSizeBytes(UNIT_CATALOG_FILE),
    });
    return true;
  } catch (e) {
    console.warn(`[daemon] failed to load unit catalog JSON: ${e.message}`);
    return false;
  }
}

function loadUnitCatalogFromSqlite() {
  if (!sqliteHandle) return false;
  const t0 = Date.now();
  try {
    const raw = sqliteHandle.loadCatalog();
    if (!raw) return false;
    applyUnitCatalogPayload(raw, 'sqlite');
    const st = sqliteHandle.stats();
    recordCatalogSourceLoad('sqlite', {
      ms: Date.now() - t0,
      units: unitCatalogById.size,
      bytes: st.bytes,
      blobBytes: st.blobBytes,
    });
    return true;
  } catch (e) {
    console.warn(`[daemon] failed to load unit catalog sqlite: ${e.message}`);
    return false;
  }
}

function loadUnitCatalog() {
  const t0 = Date.now();
  lastCatalogLoad.fallback = false;
  let ok = loadUnitCatalogFromSqlite();
  if (!ok) {
    console.warn('[daemon] sqlite catalog empty or failed; falling back to JSON file');
    lastCatalogLoad.fallback = true;
    ok = loadUnitCatalogFromJson();
  }
  lastCatalogLoad.ms = Date.now() - t0;
  if (!ok) lastCatalogLoad.source = lastCatalogLoad.source || 'none';
}

async function ensureSqliteHandle() {
  if (sqliteHandle) return true;
  const opened = await createStateSqlite(SQLITE_PATH);
  if (opened.ok) {
    sqliteHandle = opened.handle;
    sqliteInitError = null;
    return true;
  }
  sqliteInitError = opened.error;
  return false;
}

function maybeGc() {
  try { if (typeof globalThis.gc === 'function') globalThis.gc(); } catch {}
}

function benchmarkCatalogStores() {
  const compared = { json: null, sqlite: null };
  maybeGc();
  if (existsSync(UNIT_CATALOG_FILE)) {
    const t0 = Date.now();
    try {
      const raw = JSON.parse(readFileSync(UNIT_CATALOG_FILE, 'utf8'));
      const units = Array.isArray(raw.units) ? raw.units.length : 0;
      compared.json = {
        source: 'json',
        at: new Date().toISOString(),
        ms: Date.now() - t0,
        units,
        bytes: fileSizeBytes(UNIT_CATALOG_FILE),
        ...catalogProcessMem(),
      };
      catalogLoadBySource.json = compared.json;
    } catch (e) {
      compared.json = { error: e.message };
    }
  }
  maybeGc();
  if (sqliteHandle) {
    const t0 = Date.now();
    try {
      const raw = sqliteHandle.loadCatalog();
      const units = raw?.units?.length || 0;
      const st = sqliteHandle.stats();
      compared.sqlite = {
        source: 'sqlite',
        at: new Date().toISOString(),
        ms: Date.now() - t0,
        units,
        bytes: st.bytes,
        blobBytes: st.blobBytes,
        ...catalogProcessMem(),
      };
      catalogLoadBySource.sqlite = compared.sqlite;
    } catch (e) {
      compared.sqlite = { error: e.message };
    }
  }
  return compared;
}

function mergeUnitIntoCatalog(unitEntry) {
  if (!unitEntry?.unitId) return;
  const nowIso = new Date().toISOString();
  const existing = unitCatalogById.get(unitEntry.unitId);
  const incomingMileageByDate = sanitizeUnitMileageByDate(
    unitEntry.endOfDayMileageByDate && typeof unitEntry.endOfDayMileageByDate === 'object'
      ? unitEntry.endOfDayMileageByDate
      : {}
  );
  const incomingServices = sanitizeUnitServices(unitEntry.services || []);
  if (!existing) {
    unitCatalogById.set(unitEntry.unitId, {
      unitId: unitEntry.unitId,
      fleetId: unitEntry.fleetId || null,
      vehicles: unitEntry.vehicles || [],
      services: incomingServices,
      endOfDayMileageByDate: incomingMileageByDate,
      lastEndOfDayMiles: unitEntry.lastEndOfDayMiles ?? null,
      firstSeenAt: unitEntry.updatedAt || nowIso,
      lastSeenAt: unitEntry.updatedAt || nowIso,
      updatedAt: unitEntry.updatedAt || nowIso,
    });
    return;
  }
  existing.fleetId = unitEntry.fleetId || existing.fleetId || null;
  if (Array.isArray(unitEntry.vehicles) && unitEntry.vehicles.length > 0) {
    existing.vehicles = unitEntry.vehicles;
  }
  const seen = new Set((existing.services || []).map((s) => `${s.rid}|${s.start || ''}|${s.end || ''}`));
  for (const svc of incomingServices) {
    const key = `${svc.rid}|${svc.start || ''}|${svc.end || ''}`;
    if (seen.has(key)) continue;
    existing.services.push(svc);
    seen.add(key);
  }
  existing.services.sort((a, b) => (a.start || '').localeCompare(b.start || ''));
  existing.services = sanitizeUnitServices(existing.services);
  existing.endOfDayMileageByDate = sanitizeUnitMileageByDate({
    ...((existing.endOfDayMileageByDate && typeof existing.endOfDayMileageByDate === 'object') ? existing.endOfDayMileageByDate : {}),
    ...incomingMileageByDate,
  });
  if (unitEntry.lastEndOfDayMiles != null) existing.lastEndOfDayMiles = unitEntry.lastEndOfDayMiles;
  existing.lastSeenAt = unitEntry.updatedAt || nowIso;
  existing.updatedAt = unitEntry.updatedAt || nowIso;
}

function persistUnitCatalog() {
  if (heavyReloadBusy) return;
  try {
    pruneUnitCatalogStaleDays();
    if (!existsSync(STATE_DIR)) mkdirSync(STATE_DIR, { recursive: true });
    const savedAt = new Date().toISOString();
    if (sqliteHandle) {
      try {
        sqliteHandle.saveCatalog(unitCatalogById, savedAt);
      } catch (e) {
        console.warn(`[daemon] failed to persist unit catalog sqlite: ${e.message}`);
      }
    }
    lastUnitCatalogPersistAt = savedAt;
  } catch (e) {
    console.warn(`[daemon] failed to persist unit catalog: ${e.message}`);
  }
}

function historyFileForDate(ymdDashed) {
  return resolve(STATE_HISTORY_DIR, ymdDashed, 'daemon-cache.latest.json');
}

function historyDayHasState(ymdDashed) {
  return existsSync(historyFileForDate(ymdDashed))
    || existsSync(resolve(STATE_HISTORY_DIR, ymdDashed, 'manifest.json'));
}

function loadPersistedStateForDate(ymdDashed, at = null) {
  if (!isIsoDate(ymdDashed) || HISTORY_EXCLUDE_DATES.has(ymdDashed)) return null;
  const atMin = parseAtToMinutes(at);
  if (at && atMin != null) {
    const snaps = listHistorySnapshotsForDate(ymdDashed);
    const cutoff = Date.parse(`${ymdDashed}T${String(Math.floor(atMin / 60)).padStart(2, '0')}:${String(atMin % 60).padStart(2, '0')}:59Z`);
    const pick = findSnapshotAtOrBefore(snaps, cutoff) || snaps[snaps.length - 1];
    if (pick?.path) {
      const opts = historicalHeavyOpts('board');
      if (pick.manifestSnap) {
        opts.manifestSnap = pick.manifestSnap;
        opts.blobDir = pick.blobDir || heavyShardBlobDir(ymdDashed);
      }
      const raw = readHistoricalStateFile(pick.path, opts);
      if (raw) return raw;
    }
  }
  const candidates = [
    historyFileForDate(ymdDashed),
    resolve(STATE_DIR, `daemon-cache.${ymdDashed}.json`),
    resolve(STATE_DIR, `daemon-cache.${ymdDashed}.json.gz`),
  ];
  if (ymdDashed === loadedDate) candidates.unshift(STATE_FILE);
  for (const p of candidates) {
    const raw = readHistoricalStateFile(p, historicalHeavyOpts('board'));
    if (raw) return raw;
  }
  return null;
}

async function getHistoricalContext(ymdDashed, at = null) {
  if (!isIsoDate(ymdDashed) || HISTORY_EXCLUDE_DATES.has(ymdDashed)) return null;
  const cacheKey = `${ymdDashed}|${at || ''}`;
  const cached = historicalContextCache.get(cacheKey);
  if (cached && Date.now() - cached.loadedAtMs <= HIST_CONTEXT_CACHE_TTL_MS) return cached;
  const inflight = historicalContextInflight.get(cacheKey);
  if (inflight) return inflight;
  const pending = loadHistoricalContext(ymdDashed, at, cacheKey).finally(() => {
    historicalContextInflight.delete(cacheKey);
  });
  historicalContextInflight.set(cacheKey, pending);
  return pending;
}

async function loadHistoricalContext(ymdDashed, at, cacheKey) {
  const [histTimetable, state] = await Promise.all([
    getHistoricalTimetable(ymdDashed),
    Promise.resolve().then(() => loadPersistedStateForDate(ymdDashed, at)),
  ]);
  if (!histTimetable) return null;
  const histByRid = histTimetable.byRid;
  const histByTiploc = histTimetable.byTiploc;
  if (!state) return getTimetableOnlyContext(ymdDashed, at);
  let overlayEntries = state.liveOverlayByRid || [];
  // Point-in-time (?at=) stays as the snapshot. Latest historical view fills
  // in arrived/passed/departed from the day's accumulating actuals archive.
  if (!at) {
    overlayEntries = applyActualsToOverlayEntries(overlayEntries, loadActualsArchive(ymdDashed));
  }
  console.log(
    `[hist] ${ymdDashed}${at ? ` at=${at}` : ''} overlay=${Array.isArray(overlayEntries) ? overlayEntries.length : 0} `
    + `formations=${Array.isArray(state.formations) ? state.formations.length : 0} consist=skipped units=skipped`
  );

  const ctx = {
    loadedAtMs: Date.now(),
    historicalDate: ymdDashed,
    historicalAt: at || null,
    byRid: histByRid,
    byTiploc: histByTiploc,
    liveOverlayByRid: lazyLiveOverlay(overlayEntries),
    cancelled: toMap(state.cancelled || []),
    delayReason: toMap(state.delayReason || []),
    reverseFormation: new Set(state.reverseFormation || []),
    formationsByRid: toMap(state.formations || []),
    consistByRid: toMap(state.consistByRid || []),
    associationsByRid: toMap(state.associations || []),
    alertsByRid: toMap(state.alerts || []),
    stateSavedAt: state.savedAt || null,
  };
  ctx.formationsByUid = indexFormationsByUid(ctx.formationsByRid, histByRid);
  historicalContextCache.set(cacheKey, ctx);
  if (historicalContextCache.size > HIST_CONTEXT_CACHE_MAX) {
    const ordered = [...historicalContextCache.entries()].sort((a, b) => (a[1].loadedAtMs || 0) - (b[1].loadedAtMs || 0));
    for (const [k] of ordered.slice(0, historicalContextCache.size - HIST_CONTEXT_CACHE_MAX)) historicalContextCache.delete(k);
  }
  return ctx;
}

function pruneTtisDayCache() {
  if (ttisDayCache.size <= 6) return;
  const ordered = [...ttisDayCache.entries()].sort((a, b) => (a[1].loadedAtMs || 0) - (b[1].loadedAtMs || 0));
  for (const [k] of ordered.slice(0, ttisDayCache.size - 6)) ttisDayCache.delete(k);
}

async function ensureTtisIndex() {
  if (ttisIndex) return ttisIndex;
  if (ttisIndexPromise) return ttisIndexPromise;
  const path = cfg.ttisMcaPath;
  if (!path || !existsSync(path)) {
    console.log(`[ttis] no MCA at ${path || '(unset)'} — future dates beyond Darwin snapshots will be empty`);
    ttisIndex = { schedules: [], tpls: [''] };
    return ttisIndex;
  }
  ttisIndexPromise = loadTtisIndex(path)
    .then((idx) => {
      ttisIndex = idx;
      return idx;
    })
    .catch((e) => {
      console.warn(`[ttis] load failed: ${e.message}`);
      ttisIndex = { schedules: [], tpls: [''] };
      return ttisIndex;
    });
  return ttisIndexPromise;
}

async function getTtisDay(ymdDashed) {
  if (!isIsoDate(ymdDashed)) return null;
  const cached = ttisDayCache.get(ymdDashed);
  if (cached) return cached;
  const index = await ensureTtisIndex();
  if (!index?.schedules?.length) return null;
  if (index.dateMinIso && compareIsoDate(ymdDashed, index.dateMinIso) < 0) return null;
  if (index.dateMaxIso && compareIsoDate(ymdDashed, index.dateMaxIso) > 0) return null;
  const t0 = Date.now();
  const crsToPrimaryTpl = new Map();
  for (const [crs, tpls] of crsToTiplocs || []) {
    if (tpls?.length) crsToPrimaryTpl.set(String(crs).toUpperCase(), tpls[0]);
  }
  const maps = expandTtisDay(index, ymdDashed, crsToPrimaryTpl);
  const entry = { loadedAtMs: Date.now(), byRid: maps.byRid, byTiploc: maps.byTiploc };
  ttisDayCache.set(ymdDashed, entry);
  pruneTtisDayCache();
  console.log(`[ttis] expanded ${ymdDashed}: ${maps.byRid.size} journeys, ${maps.byTiploc.size} tiplocs (${Date.now() - t0}ms)`);
  return entry;
}

/** Darwin v8 in `byRid` is ~today/+1. A handful of later SSDs must not hide CIF. */
const MIN_DARWIN_SSD_JOURNEYS_FOR_LIVE_MAP = 500;

async function getTimetableOnlyContext(ymdDashed, at = null) {
  if (!isIsoDate(ymdDashed)) return null;
  const liveDay = loadedDate || railwayDayYmd(new Date());
  const darwinLiveHorizon = addDaysIsoDate(liveDay, 1);
  let byRidSrc = null;
  let byTiplocSrc = null;
  const inDarwinLiveHorizon =
    compareIsoDate(ymdDashed, liveDay) >= 0 && compareIsoDate(ymdDashed, darwinLiveHorizon) <= 0;
  if (byRid && inDarwinLiveHorizon) {
    const fromLive = filterJourneyMapsToSsd(byRid, byTiploc, ymdDashed);
    if (fromLive.byRid.size >= MIN_DARWIN_SSD_JOURNEYS_FOR_LIVE_MAP) {
      byRidSrc = fromLive.byRid;
      byTiplocSrc = fromLive.byTiploc;
    }
  }
  if (!byRidSrc) {
    const ttis = await getTtisDay(ymdDashed);
    if (ttis?.byRid?.size) {
      byRidSrc = ttis.byRid;
      byTiplocSrc = ttis.byTiploc;
    }
  }
  if (!byRidSrc) {
    const timetable = await getHistoricalTimetable(ymdDashed);
    if (!timetable) return null;
    const fromFile = filterJourneyMapsToSsd(timetable.byRid, timetable.byTiploc, ymdDashed);
    if (!fromFile.byRid.size) return null;
    byRidSrc = fromFile.byRid;
    byTiplocSrc = fromFile.byTiploc;
  }
  let overlay = new Map();
  if (!at) {
    overlay = lazyLiveOverlay(applyActualsToOverlayEntries([], loadActualsArchive(ymdDashed)));
  }
  return {
    loadedAtMs: Date.now(),
    historicalDate: ymdDashed,
    historicalAt: at || null,
    byRid: byRidSrc,
    byTiploc: byTiplocSrc,
    liveOverlayByRid: overlay,
    cancelled: new Map(),
    delayReason: new Map(),
    reverseFormation: new Set(),
    formationsByRid: new Map(),
    consistByRid: new Map(),
    associationsByRid: new Map(),
    alertsByRid: new Map(),
    stateSavedAt: null,
  };
}

function getLiveTimedContext(ymdDashed, at = null) {
  if (!isIsoDate(ymdDashed)) return null;
  const liveDate = loadedDate || railwayDayYmd(new Date());
  if (ymdDashed !== liveDate) return null;
  return {
    loadedAtMs: Date.now(),
    historicalDate: ymdDashed,
    historicalAt: at || null,
    byRid,
    byTiploc,
    liveOverlayByRid,
    cancelled,
    delayReason,
    reverseFormation,
    formationsByRid,
    formationsByUid,
    consistByRid,
    associationsByRid,
    alertsByRid,
    stateSavedAt: null,
  };
}

// Read the persisted state file once and apply the freshness check that the
// "discard stale-day" mode wants. Returns the parsed payload or null. We split
// state restore into a fast "live subset" pass (so the API can answer real
// requests within a few seconds of restart) and a "rest" pass run in the
// background. Both passes use the payload returned here, so the file is read
// at most once per restart.
function readPersistedStateRawIfFresh() {
  if (!existsSync(STATE_FILE)) {
    console.log(`[daemon] no persisted state at ${STATE_FILE} — starting fresh.`);
    return null;
  }
  try {
    const raw = JSON.parse(readFileSync(STATE_FILE, 'utf8'));
    attachHeavyShards(raw, STATE_FILE);
    if (!KEEP_STATE_ACROSS_DAYS && raw.savedDate && raw.savedDate !== railwayDayYmd(new Date())) {
      console.log(`[daemon] persisted state is from ${raw.savedDate}, today is ${railwayDayYmd(new Date())} — discarding.`);
      return null;
    }
    return raw;
  } catch (e) {
    console.warn(`[daemon] failed to load persisted state: ${e.message}`);
    return null;
  }
}

// Restore only the small, high-value caches that affect /api/departures and
// /api/service immediately on boot — predicted/actual times, cancellations
// and delay reasons. Cheap (a few MB) and fast to apply.
function applyPersistedStateLive(raw) {
  if (!raw) return;
  if (raw.liveOverlayByRid)  restoreLiveOverlayEntries(raw.liveOverlayByRid);
  if (raw.cancelled)         for (const [k, v] of raw.cancelled)         cancelled.set(k, v);
  if (raw.delayReason)       for (const [k, v] of raw.delayReason)       delayReason.set(k, v);
}

// Restore the larger long-lived caches (formations, NRCC messages, PTAC
// consists & unit index, associations, alerts). These take longer to walk
// because the per-RID payloads are richer — we run this *after* server.listen
// returns, so the socket is already accepting requests.
function applyPersistedStateRest(raw) {
  if (!raw) return;
  if (raw.formations)        for (const [k, v] of raw.formations)        rememberFormation(k, v);
  if (raw.messagesById) {
    let dropped = 0;
    for (const [k, v] of raw.messagesById) {
      // Old cache entries may have lost link text from their plainMessage
      // (the previous flattener didn't walk into nested anchor objects).
      // Tidy up dangling ",.", ",," that came from that bug so the UI
      // doesn't render "can be found in ,." until Darwin re-broadcasts.
      if (v?.plainMessage)
        v.plainMessage = v.plainMessage.replace(/\s*[,;]+\s*\.?\s*$/, '.').replace(/\s+/g, ' ').trim();
      // Drop entries with no usable text — they were broken by the old
      // flattener and will be re-populated correctly when NRCC re-issues.
      if (!v?.plainMessage || v.plainMessage.length < 3) { dropped++; continue; }
      messagesById.set(k, v);
    }
    if (dropped) console.log(`[daemon] dropped ${dropped} empty/broken cached messages from previous flattener.`);
  }
  if (raw.stationMessages)   for (const [k, v] of raw.stationMessages)   stationMessages.set(k, new Set(v));
  if (raw.associations)      for (const [k, v] of raw.associations)      associationsByRid.set(k, v);
  if (raw.alerts)            for (const [k, v] of raw.alerts)            alertsByRid.set(k, v);
  if (raw.reverseFormation)  for (const r of raw.reverseFormation)       reverseFormation.add(r);
  if (raw.consistByRid) for (const [k, v] of raw.consistByRid) consistByRid.set(k, v);
  if (raw.unitsById)    for (const [k, v] of raw.unitsById)    unitsById.set(k, v);
  if (raw.unmatchedConsists) for (const [k, v] of raw.unmatchedConsists) unmatchedConsists.set(k, v);
  const retriedAfterRestore = retryUnmatchedConsists();
  if (retriedAfterRestore > 0) {
    console.log(`[daemon] PTAC retry after persisted restore: matched ${retriedAfterRestore} queued consists.`);
  }
  console.log(
    `[daemon] restored persisted state: ${formationsByRid.size} formations, `
    + `${consistByRid.size} consists, ${unitsById.size} units, `
    + `${messagesById.size} messages, ${associationsByRid.size} associations, `
    + `${alertsByRid.size} alerted services, ${reverseFormation.size} reverse formations.`
  );
}

function loadPersistedState() {
  const raw = readPersistedStateRawIfFresh();
  if (!raw) return;
  applyPersistedStateLive(raw);
  applyPersistedStateRest(raw);
}

// ---------- Phase 3: background day-by-day historical warmup ----------------
// After /api/health is "live_ready" we walk the most recent N days of
// state/history/<date>/ from newest to oldest. For each date we prime:
//   - historicalTimetableCache (parsed timetable index)
//   - historySnapshotListCache (per-day snapshot list)
//   - historicalContextCache (final composed context for the latest snapshot)
//   - state/history/<date>/snapshot-index.json (Phase 4 disk index)
// We yield between dates and skip ahead if RSS or event-loop lag breach the
// configured ceilings. Progress is written to state/warmup-progress.json.

function sleepMs(ms) { return new Promise((r) => setTimeout(r, ms)); }

function measureLoopLag() {
  return new Promise((resolve) => {
    const t = Date.now();
    setImmediate(() => resolve(Date.now() - t));
  });
}

async function shouldSkipWarmupForGuardrails() {
  const rssMb = process.memoryUsage().rss / 1024 / 1024;
  if (cfg.warmupMaxRssMb > 0 && rssMb >= cfg.warmupMaxRssMb) {
    return { skip: true, reason: `rss=${rssMb.toFixed(0)}MB >= cap ${cfg.warmupMaxRssMb}MB` };
  }
  const lagMs = await measureLoopLag();
  if (cfg.warmupLagMs > 0 && lagMs >= cfg.warmupLagMs) {
    return { skip: true, reason: `loop-lag=${lagMs}ms >= cap ${cfg.warmupLagMs}ms` };
  }
  return { skip: false };
}

function persistWarmupProgress() {
  try {
    if (!existsSync(STATE_DIR)) mkdirSync(STATE_DIR, { recursive: true });
    const file = resolve(STATE_DIR, 'warmup-progress.json');
    const tmp = file + '.tmp';
    writeFileSync(tmp, JSON.stringify({ ...warmupState, mode: daemonMode, persistedAt: new Date().toISOString() }));
    renameSync(tmp, file);
  } catch {}
}

async function primePastDay(ymd) {
  if (HISTORY_EXCLUDE_DATES.has(ymd)) {
    console.log(`[boot] past ${ymd} skipped (excluded)`);
    return { skipped: true };
  }
  const hasState = historyDayHasState(ymd);
  const hasTt = !!pickTimetableForDate(ymd);
  if (!hasState && !hasTt) {
    console.log(`[boot] past ${ymd} skipped (no data)`);
    return { skipped: true };
  }
  const t0 = Date.now();
  if (hasTt) await getHistoricalTimetable(ymd);
  if (hasState) {
    await getHistoricalContext(ymd);
    setImmediate(() => {
      try { buildSnapshotIndexForDate(ymd); } catch {}
    });
    console.log(`[boot] past ${ymd} primed in ${Date.now() - t0}ms`);
  } else {
    await getTimetableOnlyContext(ymd);
    console.log(`[boot] past ${ymd} timetable-only primed in ${Date.now() - t0}ms`);
  }
  return { skipped: false };
}

async function primeFutureDay(ymd) {
  const t0 = Date.now();
  const ctx = await getTimetableOnlyContext(ymd);
  const n = ctx?.byRid?.size || 0;
  console.log(`[boot] future ${ymd} primed ${n} journeys in ${Date.now() - t0}ms`);
  return n;
}

async function runHorizonWarmup() {
  const origin = loadedDate || railwayDayYmd(new Date());
  daemonMode = 'warming_history';
  warmupState.startedAt = new Date().toISOString();
  warmupState.done = [];
  warmupState.skipped = [];
  warmupState.errors = [];

  const pastDays = cfg.warmupDays;
  console.log(`[boot] priming past ${pastDays} days (skip missing)`);
  for (let i = 1; i <= pastDays; i++) {
    const ymd = addDaysIsoDate(origin, -i);
    try {
      const result = await primePastDay(ymd);
      if (result.skipped) warmupState.skipped.push({ date: ymd, reason: 'no data' });
      else warmupState.done.push({ date: ymd });
    } catch (e) {
      warmupState.errors.push({ date: ymd, error: e.message });
      console.warn(`[boot] past ${ymd} failed: ${e.message}`);
    }
  }

  console.log(`[boot] loading TTIS index for future dates`);
  await ensureTtisIndex();
  const futureDays = cfg.warmupFutureDays;
  console.log(`[boot] priming future ${futureDays} days`);
  for (let i = 1; i <= futureDays; i++) {
    const ymd = addDaysIsoDate(origin, i);
    try {
      await primeFutureDay(ymd);
      warmupState.done.push({ date: ymd });
    } catch (e) {
      warmupState.errors.push({ date: ymd, error: e.message });
      console.warn(`[boot] future ${ymd} failed: ${e.message}`);
    }
  }

  await flushJidxWriteQueue();
  warmupState.current = null;
  warmupState.finishedAt = new Date().toISOString();
  daemonMode = 'fully_warm';
  persistWarmupProgress();
  console.log(`[boot] horizon ready: done=${warmupState.done.length} skipped=${warmupState.skipped.length} errors=${warmupState.errors.length}`);
  const extraJoins = await extendPtacJoinWithUpcomingDays();
  const retried = retryUnmatchedConsists();
  if (extraJoins || retried) {
    console.log(`[ptac] after horizon: extra join keys=${extraJoins}, unmatched resolved=${retried}, still unmatched=${unmatchedConsists.size}`);
  }
}

async function runHistoricalWarmup() {
  await runHorizonWarmup();
}

async function persistState(opts = {}) {
  if (heavyReloadBusy && !opts.force) return;
  if (persistStateBusy) return;
  persistStateBusy = true;
  try {
    if (!existsSync(STATE_DIR)) mkdirSync(STATE_DIR, { recursive: true });
    const savedAt = new Date().toISOString();
    const savedDate = loadedDate || railwayDayYmd(new Date());

    let formations = [...formationsByRid.entries()];
    let consistByRidArr = [...consistByRid.entries()];
    let unitsByIdArr = [...unitsById.entries()];
    let overlayEntries = serializeLiveOverlayEntries();

    let corePayload = {
      savedAt,
      savedDate,
      cancelled: [...cancelled.entries()],
      delayReason: [...delayReason.entries()],
      messagesById: [...messagesById.entries()],
      stationMessages: [...stationMessages.entries()].map(([k, v]) => [k, [...v]]),
      associations: [...associationsByRid.entries()],
      alerts: [...alertsByRid.entries()],
      reverseFormation: [...reverseFormation],
      unmatchedConsists: [...unmatchedConsists.entries()],
      stateSchema: 2,
      ...(lastAutoFetchRunYmd ? { lastAutoFetchRunYmd } : {}),
    };

    const mergedForScore = {
      ...corePayload,
      liveOverlayByRid: overlayEntries,
      formations,
      consistByRid: consistByRidArr,
      unitsById: unitsByIdArr,
    };

    if (PROTECT_RICHER_STATE && !liveCachesReady && !lastPersistAt && existsSync(STATE_FILE)) {
      try {
        const diskMerged = readMergedStateFromDisk();
        if (diskMerged) {
          const diskScore = stateScore(diskMerged);
          const nextScore = stateScore(mergedForScore);
          if (diskScore > nextScore * 1.2) {
            console.warn(`[daemon] skip persist: on-disk cache looks richer (${diskScore} > ${nextScore}).`);
            const split = splitMergedForPersist(diskMerged);
            corePayload = split.core;
            if (!corePayload.savedAt) corePayload.savedAt = savedAt;
            if (!corePayload.savedDate) corePayload.savedDate = savedDate;
            formations = split.formations;
            consistByRidArr = split.consistByRid;
            unitsByIdArr = split.unitsById;
            overlayEntries = split.liveOverlayByRid;
          }
        }
      } catch {}
    }

    if (lastAutoFetchRunYmd) corePayload.lastAutoFetchRunYmd = lastAutoFetchRunYmd;

    try {
      persistActualsArchive(corePayload.savedDate, overlayEntries);
    } catch (e) {
      console.warn(`[daemon] actuals archive persist failed: ${e.message}`);
    }

    if (!existsSync(STATE_HISTORY_DIR)) mkdirSync(STATE_HISTORY_DIR, { recursive: true });
    const dayDir = resolve(STATE_HISTORY_DIR, corePayload.savedDate);
    if (!existsSync(dayDir)) mkdirSync(dayDir, { recursive: true });
    const blobDir = (STATE_HISTORY_DEDUP_SHARDS || STATE_HISTORY_MANIFEST)
      ? heavyShardBlobDir(corePayload.savedDate)
      : null;
    if (blobDir && !existsSync(blobDir)) mkdirSync(blobDir, { recursive: true });

    // Heavy gz shards first (formations, PTAC, units, live overlay), gzipped in
    // <=~120MB JSON parts so V8 never hits Invalid string length. Dedup: gzip
    // each unique part once under history/<date>/blobs/, then hard-link names.
    const heavyBlobs = blobDir
      ? await buildHeavyShardBlobs(blobDir, formations, consistByRidArr, unitsByIdArr, overlayEntries)
      : null;
    const writeHeavy = async (stem) => {
      if (!stem) return;
      if (heavyBlobs) installHeavyShardsFromBlobs(stem, heavyBlobs);
      else await writeHeavyShardsAtomic(stem, formations, consistByRidArr, unitsByIdArr, overlayEntries);
    };

    await writeHeavy(heavyStemForCore(STATE_FILE));

    let coreJson;
    try {
      coreJson = JSON.stringify(corePayload);
    } catch (e) {
      console.warn(`[daemon] failed to stringify core state (${e.message}); heavy shards updated, core/history snapshots skipped`);
      historicalStateFileCache.clear();
      lastPersistAt = new Date().toISOString();
      return;
    }

    const tmpMain = STATE_FILE + '.tmp';
    writeFileSync(tmpMain, coreJson);
    renameSync(tmpMain, STATE_FILE);

    const dayLatest = resolve(dayDir, 'daemon-cache.latest.json');
    const dayLatestTmp = dayLatest + '.tmp';
    writeFileSync(dayLatestTmp, coreJson);
    renameSync(dayLatestTmp, dayLatest);
    await writeHeavy(heavyStemForCore(dayLatest));

    if (STATE_HISTORY_MANIFEST && heavyBlobs) {
      try {
        const coreBlobName = writeCoreBlob(blobDir, coreJson, STATE_HISTORY_GZIP_LEVEL);
        const ms = Date.parse(corePayload.savedAt || savedAt);
        appendHistoryManifestSnap(dayDir, {
          ms: Number.isFinite(ms) ? ms : Date.now(),
          savedAt: corePayload.savedAt || savedAt,
          date: corePayload.savedDate,
          core: coreBlobName,
          formations: blobBasenames(heavyBlobs.formations),
          consist: blobBasenames(heavyBlobs.consist),
          units: blobBasenames(heavyBlobs.units),
          overlay: blobBasenames(heavyBlobs.overlay),
        });
      } catch (e) {
        console.warn(`[daemon] history manifest persist failed: ${e.message}`);
      }
    }

    if (STATE_HISTORY_STAMPED_NAMES) {
      const dayStamp = (corePayload.savedAt || savedAt).replace(/[:.]/g, '-');
      const stampedStem = resolve(dayDir, `daemon-cache-heavy.${dayStamp}`);
      if (STATE_HISTORY_COMPRESS_SNAPSHOTS) {
        const daySnap = resolve(dayDir, `daemon-cache.${dayStamp}.json.gz`);
        const daySnapTmp = daySnap + '.tmp';
        try {
          writeFileSync(daySnapTmp, gzipSync(coreJson, { level: STATE_HISTORY_GZIP_LEVEL }));
          renameSync(daySnapTmp, daySnap);
        } catch {}
      } else {
        const daySnap = resolve(dayDir, `daemon-cache.${dayStamp}.json`);
        const daySnapTmp = daySnap + '.tmp';
        try {
          writeFileSync(daySnapTmp, coreJson);
          renameSync(daySnapTmp, daySnap);
        } catch {}
      }
      try {
        await writeHeavy(stampedStem);
      } catch {}
    }

    pruneHistoryDirsByRetention();
    try { buildSnapshotIndexForDate(corePayload.savedDate); } catch {}

    if (STATE_SNAPSHOT_COUNT > 0) {
      const stamp = (corePayload.savedAt || savedAt).replace(/[:.]/g, '-');
      const snap = resolve(STATE_DIR, `daemon-cache.${stamp}.json`);
      try {
        const snapTmp = snap + '.tmp';
        writeFileSync(snapTmp, coreJson);
        renameSync(snapTmp, snap);
        await writeHeavy(heavyStemForCore(snap));
        pruneOldStateSnapshots();
      } catch {}
    }
    historicalStateFileCache.clear();
    lastPersistAt = new Date().toISOString();
  } catch (e) {
    console.warn(`[daemon] failed to persist state: ${e.message}`);
  } finally {
    persistStateBusy = false;
  }
}

function getOverlay(rid) {
  let o = liveOverlayByRid.get(rid);
  if (!o) { o = { locs: new Map() }; liveOverlayByRid.set(rid, o); }
  return o;
}

function processMessage(pport) {
  for (const env of ['uR', 'sR']) {
    const e = pport[env]; if (!e) continue;

    // --- TS (live times per location) ---
    for (const ts of asArray(e.TS)) {
      if (!ts.rid) continue;
      stats.lastKafkaMsgAt = new Date().toISOString();

      // Reverse-formation flag (e.g. when a unit runs the wrong way round; the
      // platform numbering of carriages is mirrored). Useful for the future
      // formation-aware UI: passenger asking "which end is coach 1?".
      if (ts.isReverseFormation === 'true' || ts.isReverseFormation === true) reverseFormation.add(ts.rid);
      else if (ts.isReverseFormation === 'false' || ts.isReverseFormation === false) reverseFormation.delete(ts.rid);

      if (ts.lateReason) {
        const code = String(unwrap(ts.lateReason));
        delayReason.set(ts.rid, { code, source: 'ts', reason: lateReasons.get(code) || `code ${code}` });
        stats.updates++;
      }
      if (ts.cancelReason) {
        const code = String(unwrap(ts.cancelReason));
        cancelled.set(ts.rid, { code, source: 'ts', reason: cancelReasons.get(code) || `code ${code}` });
        stats.updates++;
      }

      const ov = getOverlay(ts.rid);
      for (const loc of asArray(ts.Location)) {
        const tpl = String(loc.tpl || '').toUpperCase();
        if (!tpl) continue;
        let entry = ov.locs.get(tpl);
        if (!entry) { entry = {}; ov.locs.set(tpl, entry); }

        const dep = bestDepartureFromLiveLoc(loc);
        if (dep) {
          entry.bestTime = dep.time;
          entry.bestKind = dep.kind;
          entry.liveSource = dep.source || null;
          entry.liveSourceInstance = dep.sourceInstance || null;
          entry.unknownDelay = !!dep.unknownDelay;
          entry.manualUnknownDelay = !!dep.manualUnknownDelay;
          stats.updates++;
        }

        const arrTs = describeLiveTime(loc?.arr, 'actual-arr', 'est-arr');
        if (arrTs) {
          entry.arrLiveTime = arrTs.time;
          entry.arrLiveKind = arrTs.kind;
          entry.arrLiveSource = arrTs.source || null;
          entry.arrLiveSourceInstance = arrTs.sourceInstance || null;
          entry.arrUnknownDelay = !!arrTs.unknownDelay;
          entry.arrManualUnknownDelay = !!arrTs.manualUnknownDelay;
          stats.updates++;
        }

        // plat in JSON feed: { platsrc, conf, "": "3A" }
        if (loc.plat != null) {
          const platStr = typeof loc.plat === 'string' ? loc.plat
            : (loc.plat[''] || loc.plat['#text'] || loc.plat._);
          if (platStr && platStr !== entry.livePlat) { entry.livePlat = platStr; stats.updates++; }
          const platSrc = typeof loc.plat === 'object' ? (loc.plat.platsrc ? String(unwrap(loc.plat.platsrc)) : null) : null;
          const platConf = typeof loc.plat === 'object' ? (loc.plat.conf === true || loc.plat.conf === 'true') : false;
          const platSupp = typeof loc.plat === 'object' ? ((loc.plat.platsup === true || loc.plat.platsup === 'true') || (loc.plat.cisPlatsup === true || loc.plat.cisPlatsup === 'true')) : false;
          entry.platformSource = platSrc || null;
          entry.platformConfirmed = platConf;
          entry.platformSuppressed = platSupp;
        }
        if (loc.length != null) {
          const length = Number(unwrap(loc.length));
          entry.trainLength = Number.isFinite(length) && length > 0 ? length : null;
        }
        // PARTIAL CANCELLATION semantics: a `can="true"` or `cancelReason`
        // attribute on a single Location means *just this stop* is cancelled
        // (e.g. service runs A→B but is cancelled B→F). Do NOT promote that
        // to a whole-service cancellation; the board/detail responses derive
        // per-stop status from entry.cancelled and per-stop cancelReason.
        if (loc.can === 'true' || loc.can === true) {
          entry.cancelled = true;
          stats.updates++;
        }
        if (loc.lateReason) {
          const code = String(unwrap(loc.lateReason));
          delayReason.set(ts.rid, { code, source: 'ts-loc', reason: lateReasons.get(code) || `code ${code}` });
          stats.updates++;
        }
        if (loc.cancelReason) {
          const code = String(unwrap(loc.cancelReason));
          entry.cancelled    = true;
          entry.cancelReason = { code, reason: cancelReasons.get(code) || `code ${code}` };
          stats.updates++;
        }
      }
    }

    // --- schedule (full / partial cancellations announced up-front) ---
    for (const sc of asArray(e.schedule)) {
      if (!sc?.rid) continue;
      if (sc.cancelReason) {
        const code = String(unwrap(sc.cancelReason));
        cancelled.set(sc.rid, { code, source: 'schedule', reason: cancelReasons.get(code) || `code ${code}` });
        stats.updates++;
      }
      for (const k of ['OR','IP','PP','DT','OPOR','OPIP','OPPP','OPDT']) {
        for (const node of asArray(sc[k])) {
          // Same partial-cancellation rule as the TS branch: per-stop
          // cancelled flags don't cancel the whole service, only that stop.
          if (node?.cancelled === 'true' || node?.can === 'true') {
            const tpl = unwrap(node?.tpl);
            if (tpl) {
              const ov = getOverlay(sc.rid);
              let entry = ov.locs.get(tpl);
              if (!entry) { entry = {}; ov.locs.set(tpl, entry); }
              entry.cancelled = true;
              stats.updates++;
            }
          }
        }
      }
    }

    // --- deactivated (schedule removed, no reason) ---
    for (const d of asArray(e.deactivated)) {
      if (!d?.rid) continue;
      if (!cancelled.has(d.rid)) cancelled.set(d.rid, { source: 'deactivated', reason: 'schedule deactivated' });
      stats.updates++;
    }

    // --- serviceLoading (overall passenger load % at a stop) ----------------
    // {rid, tpl, wta/wtd/pta/ptd, loadingPercentage:"61"}
    for (const sl of asArray(e.serviceLoading)) {
      if (!sl?.rid || !sl.tpl) continue;
      const ov = getOverlay(sl.rid);
      const tpl = String(sl.tpl).toUpperCase();
      let entry = ov.locs.get(tpl);
      if (!entry) { entry = {}; ov.locs.set(tpl, entry); }
      const pct = Number(unwrap(sl.loadingPercentage));
      if (Number.isFinite(pct)) { entry.loadPct = pct; stats.updates++; }
    }

    // --- formationLoading (per-coach load values at a stop) -----------------
    // {fid, rid, tpl, ..., loading:[{coachNumber, "":"7"}, ...]}
    // Loading values are 0–100% (Darwin v15 formationLoading).
    for (const fl of asArray(e.formationLoading)) {
      if (!fl?.rid || !fl.tpl) continue;
      const ov = getOverlay(fl.rid);
      const tpl = String(fl.tpl).toUpperCase();
      let entry = ov.locs.get(tpl);
      if (!entry) { entry = {}; ov.locs.set(tpl, entry); }
      entry.fid = String(unwrap(fl.fid) || '');
      entry.coachLoading = asArray(fl.loading).map((c) => ({
        number: String(unwrap(c.coachNumber) || ''),
        value:  Number(unwrap(c['']) ?? unwrap(c['#text']) ?? unwrap(c._)),
      })).filter((c) => c.number);
      stats.updates++;
    }

    // --- scheduleFormations (coach list + class for an FID) -----------------
    // {rid, formation:{fid, coaches:{coach:[{coachNumber, coachClass}]}}}
    for (const sf of asArray(e.scheduleFormations)) {
      if (!sf?.rid) continue;
      const f = sf.formation || {};
      const fid = String(unwrap(f.fid) || '');
      const coaches = asArray(f.coaches?.coach || f.coach).map((c) => ({
        number: String(unwrap(c.coachNumber) || ''),
        class:  String(unwrap(c.coachClass)  || ''),
        // Optional Darwin attrs we pass through if present.
        toilet:        c.toilet     ? String(unwrap(c.toilet))     : null,
        catering:      c.catering   ? String(unwrap(c.catering))   : null,
      }));
      rememberFormation(sf.rid, { fid, coaches });
      stats.updates++;
    }

    // --- association (service joins / divides / next-portion) ---------------
    // {tiploc, category:"VV"|"JJ"|"NP", main:{rid, wtd/ptd...}, assoc:{rid,...}, isCancelled?, isDeleted?}
    // Categories: JJ=join, VV=divide, NP=next-portion. We index on BOTH RIDs
    // so either side of the relationship can find the other.
    for (const a of asArray(e.association)) {
      const tiploc = String(unwrap(a.tiploc) || '').toUpperCase();
      const category = String(unwrap(a.category) || '');
      const main = a.main || {};
      const assoc = a.assoc || {};
      const mainRid  = String(unwrap(main.rid)  || '');
      const assocRid = String(unwrap(assoc.rid) || '');
      if (!mainRid || !assocRid) continue;
      const isDeleted = a.isDeleted === 'true' || a.isDeleted === true;
      const isCancelled = a.isCancelled === 'true' || a.isCancelled === true;
      const record = {
        category, tiploc, mainRid, assocRid, isCancelled, isDeleted,
        mainTime:  unwrap(main.ptd)  || unwrap(main.wtd)  || unwrap(main.pta)  || unwrap(main.wta)  || null,
        assocTime: unwrap(assoc.ptd) || unwrap(assoc.wtd) || unwrap(assoc.pta) || unwrap(assoc.wta) || null,
      };
      if (isDeleted) {
        // Withdraw any existing association between this pair at this tiploc.
        for (const rid of [mainRid, assocRid]) {
          const arr = associationsByRid.get(rid);
          if (!arr) continue;
          const left = arr.filter((x) => !(x.tiploc === tiploc && x.mainRid === mainRid && x.assocRid === assocRid));
          if (left.length) associationsByRid.set(rid, left);
          else associationsByRid.delete(rid);
        }
      } else {
        for (const rid of [mainRid, assocRid]) {
          const arr = associationsByRid.get(rid) || [];
          // Replace any existing record for the same tiploc+pair.
          const filtered = arr.filter((x) => !(x.tiploc === tiploc && x.mainRid === mainRid && x.assocRid === assocRid));
          filtered.push(record);
          associationsByRid.set(rid, filtered);
        }
        const mainForm = formationsByRid.get(mainRid);
        const assocForm = formationsByRid.get(assocRid);
        if (formationHasCoaches(mainForm) && !formationHasCoaches(assocForm)) rememberFormation(assocRid, mainForm);
        if (formationHasCoaches(assocForm) && !formationHasCoaches(mainForm)) rememberFormation(mainRid, assocForm);
      }
      stats.updates++;
    }

    // --- OW (Operational Warning / NRCC station message) -------------------
    // {id, cat, sev:"0"-"3", suppress?, Station:[{crs}], Msg:"<html...>"}
    // Severity levels: 0=info, 1=minor, 2=major, 3=severe.
    // Setting Station to empty or sev to deletion-equivalent isn't standardised;
    // these messages are typically refreshed by being re-broadcast or removed
    // by NRCC choosing not to mention them again. We expire stale ones via TTL.
    for (const ow of asArray(e.OW)) {
      const id = String(unwrap(ow.id) || '');
      if (!id) continue;
      const stations = asArray(ow.Station).map((s) => String(unwrap(s.crs) || '').toUpperCase()).filter(Boolean);
      const severity = Number(unwrap(ow.sev) ?? 0);
      const category = String(unwrap(ow.cat) || '');
      const suppress = ow.suppress === 'true' || ow.suppress === true;
      // Msg comes as a tree (mixed text + nested anchor / paragraph elements
      // courtesy of the JSON serializer). Walk the tree to recover both a
      // plain-text variant and a sanitised HTML one with clickable links.
      const flat  = flattenHtml(ow.Msg);
      const plain = flat.plain.replace(/\s+/g, ' ').trim();
      const html  = flat.html.replace(/\s+/g, ' ').trim();
      // Drop any prior CRS associations (the message might cover a different list now).
      const prior = messagesById.get(id);
      if (prior) for (const c of prior.stations) stationMessages.get(c)?.delete(id);
      // An empty plain text means the flattener couldn't recover anything
      // useful from this Msg shape — usually a malformed broadcast or a
      // structure we don't know about yet. Suppress rather than show a
      // blank banner; if NRCC re-issues with proper content we'll catch it.
      if (suppress || stations.length === 0 || !plain || plain.length < 3) {
        messagesById.delete(id);
      } else {
        messagesById.set(id, { id, severity, category, htmlMessage: html, plainMessage: plain, stations, receivedAt: new Date().toISOString() });
        for (const c of stations) {
          let set = stationMessages.get(c);
          if (!set) { set = new Set(); stationMessages.set(c, set); }
          set.add(id);
        }
      }
      stats.updates++;
    }

    // --- trainAlert (per-service free-text alert) --------------------------
    // {AlertID, AlertServices:{AlertService:{RID,...}}, AlertText, AlertType, Audience, Source}
    for (const al of asArray(e.trainAlert)) {
      const id = String(unwrap(al.AlertID) || '');
      if (!id) continue;
      const text = String(unwrap(al.AlertText) || '').trim();
      const type = String(unwrap(al.AlertType) || '');
      const audience = String(unwrap(al.Audience) || '');
      const source = String(unwrap(al.Source) || '');
      const services = asArray(al.AlertServices?.AlertService || al.AlertService);
      for (const svc of services) {
        const rid = String(unwrap(svc.RID) || '');
        if (!rid) continue;
        const locations = asArray(svc.Location).map((l) => String(unwrap(l) || '').toUpperCase()).filter(Boolean);
        const arr = alertsByRid.get(rid) || [];
        // Deduplicate by alert id; replace any prior copy.
        const filtered = arr.filter((x) => x.id !== id);
        filtered.push({ id, type, audience, source, text, locations });
        alertsByRid.set(rid, filtered);
        stats.updates++;
      }
    }
  }
}

// ---------- PTAC ingestion -------------------------------------------------
const ptacStats = {
  consumed: 0,
  parsed:   0,
  matched:  0,
  unmatched: 0,
  errors:   0,
  startedAt: null,
  lastMessageAt: null,
};

/**
 * Convert a HH:MM string into minutes since midnight; returns null on bad input.
 */
function parseHHMM(s) {
  if (!s) return null;
  const m = /^(\d{1,2}):(\d{2})/.exec(s);
  if (!m) return null;
  return Number(m[1]) * 60 + Number(m[2]);
}

function mergeDefects(existing = [], incoming = []) {
  if (!Array.isArray(existing) || existing.length === 0) return Array.isArray(incoming) ? incoming : [];
  if (!Array.isArray(incoming) || incoming.length === 0) return existing;
  const out = [...existing];
  const seen = new Set(existing.map((d) => `${d?.code || ''}|${d?.description || ''}|${d?.status || ''}|${d?.location || ''}|${d?.maintenanceUid || ''}`));
  for (const d of incoming) {
    const key = `${d?.code || ''}|${d?.description || ''}|${d?.status || ''}|${d?.location || ''}|${d?.maintenanceUid || ''}`;
    if (seen.has(key)) continue;
    out.push(d);
    seen.add(key);
  }
  return out;
}

function mergeVehicle(existing, incoming) {
  if (!existing) return incoming;
  if (!incoming) return existing;
  const merged = { ...existing };
  for (const [k, v] of Object.entries(incoming)) {
    if (k === 'defects') continue;
    if (v !== null && v !== undefined && v !== '') merged[k] = v;
  }
  merged.defects = mergeDefects(existing.defects, incoming.defects);
  return merged;
}

function mergeOneResourceGroup(base, incoming) {
  const merged = { ...(base || {}) };
  for (const [k, v] of Object.entries(incoming || {})) {
    if (k === 'vehicles') continue;
    if (k === 'endOfDayMiles') {
      if (v !== null && v !== undefined && v !== '') merged[k] = v;
      continue;
    }
    if (v !== null && v !== undefined && v !== '') merged[k] = v;
  }
  const existingVehicles = Array.isArray(base?.vehicles) ? base.vehicles : [];
  const byVKey = new Map(existingVehicles.map((v, vi) => [`${v?.vehicleId || ''}|${v?.position ?? ''}|${vi}`, mergeVehicle(v, v)]));
  (incoming?.vehicles || []).forEach((nv, vi) => {
    const vKey = `${nv?.vehicleId || ''}|${nv?.position ?? ''}|${vi}`;
    byVKey.set(vKey, mergeVehicle(byVKey.get(vKey), nv));
  });
  merged.vehicles = [...byVKey.values()];
  return merged;
}

/**
 * PTAC repeats are delete-and-replace for the unit list. Keep vehicle/defect
 * fields for units that are still present; do not accumulate leftover units
 * (e.g. a planned 150 then the 158 that actually ran).
 */
function mergeResourceGroups(existing = [], incoming = []) {
  if (!Array.isArray(incoming) || incoming.length === 0) {
    return Array.isArray(existing) ? existing : [];
  }
  if (!Array.isArray(existing) || existing.length === 0) {
    return incoming.map((ng) => ({ ...(ng || {}), vehicles: [...(ng?.vehicles || [])] }));
  }
  const existingByUnit = new Map();
  for (const g of existing) {
    const id = g?.unitId != null ? String(g.unitId).trim() : '';
    if (id && !existingByUnit.has(id)) existingByUnit.set(id, g);
  }
  return incoming.map((ng) => {
    const id = ng?.unitId != null ? String(ng.unitId).trim() : '';
    const base = (id && existingByUnit.get(id)) || {};
    return mergeOneResourceGroup(base, ng);
  });
}

function allocationMergeKey(a) {
  return [
    a?.allocationOriginDateTime || a?.trainOriginDateTime || '',
    a?.allocationDestinationDateTime || a?.trainDestDateTime || '',
    a?.resourceGroupPosition ?? '',
    a?.allocationOrigin?.tiploc || a?.trainOrigin?.tiploc || '',
  ].join('|');
}

function mergeAllocations(existing = [], incoming = []) {
  if (!Array.isArray(incoming) || incoming.length === 0) {
    return Array.isArray(existing) ? existing : [];
  }
  if (!Array.isArray(existing) || existing.length === 0) return incoming;
  const prevByKey = new Map();
  for (const a of existing) prevByKey.set(allocationMergeKey(a), a);
  return incoming.map((na) => {
    const base = prevByKey.get(allocationMergeKey(na)) || {};
    const merged = { ...base };
    for (const [k, v] of Object.entries(na || {})) {
      if (k === 'resourceGroups') continue;
      if (v !== null && v !== undefined && v !== '') merged[k] = v;
    }
    merged.resourceGroups = mergeResourceGroups(base.resourceGroups || [], na.resourceGroups || []);
    return merged;
  });
}

/**
 * Map a parsed PTAC allocation message to a Darwin RID using the same join
 * rules as live ingestion (exact tuple, then loose + origin-time proximity).
 */
function resolveRidForParsedConsist(parsed) {
  const key = consistJoinKey(parsed);
  if (!key) return { rid: null, exactKey: null };
  const exact = `${key.ssd}|${key.headcode}|${key.originTpl}|${key.originHHMM}`;
  const loose = `${key.ssd}|${key.headcode}|${key.originTpl}`;

  let rid = ptacJoinByTuple?.get(exact);
  if (!rid) {
    const cands = ptacJoinByOrigin?.get(loose) || [];
    if (cands.length === 1) rid = cands[0];
    else if (cands.length > 1) {
      const target = parseHHMM(key.originHHMM);
      let best = null; let bestDelta = Infinity;
      for (const r of cands) {
        const j = byRid.get(r);
        const o = j?.slots?.find((s) => s.slot === 'OR' || s.slot === 'OPOR');
        const t = parseHHMM((o?.ptd || o?.wtd || '').slice(0, 5));
        if (target == null || t == null) continue;
        const delta = Math.abs(t - target);
        if (delta < bestDelta) { bestDelta = delta; best = r; }
      }
      if (best && bestDelta <= 5) rid = best;
    }
  }
  return { rid: rid || null, exactKey: exact };
}

/**
 * Process a single raw PTAC XML message off the Kafka topic. We:
 *   1. Parse the XML to a normalised JS object.
 *   2. Resolve to a Darwin RID via the (ssd, headcode, originTpl, originHHMM) join.
 *   3. Store the consist by RID (latest broadcast wins).
 *   4. Update the unit-tracking index.
 * Unmatched messages are stashed in case a timetable reload later resolves them.
 */
function processConsistMessage(rawXml) {
  ptacStats.consumed++;
  ptacStats.lastMessageAt = new Date().toISOString();
  let parsed;
  try { parsed = parseConsistMessage(rawXml); }
  catch (e) { ptacStats.errors++; return; }
  if (!parsed) { ptacStats.errors++; return; }
  ptacStats.parsed++;

  // Empty messages = "remove allocation"; nothing to record but we should
  // still drop any prior consist for this train if we can match it.
  if (!parsed.allocations || parsed.allocations.length === 0) return;

  const { rid, exactKey } = resolveRidForParsedConsist(parsed);
  if (!exactKey) { ptacStats.unmatched++; return; }

  if (!rid) {
    // Stash for later resolution — bounded to prevent unbounded growth.
    if (unmatchedConsists.size >= PTAC_UNMATCHED_CAP) {
      // Drop the oldest entry (Maps iterate in insertion order).
      const firstKey = unmatchedConsists.keys().next().value;
      if (firstKey) unmatchedConsists.delete(firstKey);
    }
    unmatchedConsists.set(exactKey, parsed);
    ptacStats.unmatched++;
    return;
  }

  ptacStats.matched++;
  applyConsistToRid(rid, parsed);
}

function unitIdsFromAllocations(allocs) {
  const ids = new Set();
  for (const a of allocs || []) {
    for (const rg of a.resourceGroups || []) {
      const id = rg?.unitId != null ? String(rg.unitId).trim() : '';
      if (id) ids.add(id);
    }
  }
  return ids;
}

/**
 * Store the consist against a RID and update the unit-tracking index.
 * Always overwrites — latest broadcast wins, per the spec's "delete and
 * replace" semantics for repeated messages on the same train.
 */
function applyConsistToRid(rid, parsed) {
  const nowIso = new Date().toISOString();
  const mileageDay = unitDayFromValue(parsed.allocations?.[0]?.diagramDate)
    || loadedDate
    || railwayDayYmd(new Date());
  const prev = consistByRid.get(rid);
  const prevIds = unitIdsFromAllocations(prev?.allocations);
  const mergedAllocations = mergeAllocations(prev?.allocations || [], parsed.allocations || []);
  consistByRid.set(rid, {
    parsedAt: nowIso,
    company: parsed.company || prev?.company || null,
    companyDarwin: parsed.companyDarwin || prev?.companyDarwin || null,
    core: parsed.core || prev?.core || null,
    diagramDate: parsed.allocations?.[0]?.diagramDate || prev?.diagramDate || null,
    allocations: mergedAllocations,
  });

  const nextIds = unitIdsFromAllocations(mergedAllocations);
  for (const uid of prevIds) {
    if (nextIds.has(uid)) continue;
    const entry = unitsById.get(uid);
    if (!entry) continue;
    entry.services = (entry.services || []).filter((s) => s.rid !== rid);
  }

  // Refresh the unit-tracking index. Each unit (ResourceGroupId) gets a
  // record of every service it's been seen on today.
  const seenUnits = new Set();
  for (const a of parsed.allocations) {
    for (const rg of a.resourceGroups || []) {
      if (!rg.unitId || seenUnits.has(rg.unitId)) continue;
      seenUnits.add(rg.unitId);
      let entry = unitsById.get(rg.unitId);
      if (!entry) {
        entry = {
          unitId: rg.unitId,
          fleetId: rg.fleetId,
          vehicles: rg.vehicles,
          services: [],
          endOfDayMileageByDate: {},
          lastEndOfDayMiles: null,
        };
        unitsById.set(rg.unitId, entry);
      } else {
        // Keep cached data and only patch in newly-seen fields/logs/vehicles.
        entry.fleetId = rg.fleetId || entry.fleetId;
        entry.vehicles = mergeResourceGroups(
          [{ vehicles: entry.vehicles || [] }],
          [{ vehicles: rg.vehicles || [], endOfDayMiles: rg.endOfDayMiles ?? null }]
        )[0]?.vehicles || entry.vehicles;
      }
        if (rg.endOfDayMiles != null && isRetainedUnitDay(mileageDay)) {
        if (!entry.endOfDayMileageByDate || typeof entry.endOfDayMileageByDate !== 'object') {
          entry.endOfDayMileageByDate = {};
        }
        entry.endOfDayMileageByDate[mileageDay] = rg.endOfDayMiles;
        entry.lastEndOfDayMiles = rg.endOfDayMiles;
      }
      // Replace any existing service entry for this RID.
      entry.services = entry.services.filter((s) => s.rid !== rid);
      entry.services.push({
        rid,
        start: a.allocationOriginDateTime || a.trainOriginDateTime || null,
        end:   a.allocationDestinationDateTime || a.trainDestDateTime || null,
        startTpl: a.allocationOrigin?.tiploc || a.trainOrigin?.tiploc || null,
        endTpl:   a.allocationDestination?.tiploc || a.trainDest?.tiploc || null,
        headcode: parsed.headcode,
        position: a.resourceGroupPosition,
        reversed: a.reversed,
      });
      entry.services.sort((a, b) => (a.start || '').localeCompare(b.start || ''));
      entry.lastSeenRid = rid;
      entry.updatedAt   = nowIso;
      mergeUnitIntoCatalog(entry);
    }
  }
}

/**
 * After a timetable reload, retry unmatched PTAC messages — unit/formation
 * allocations published 12–48h before the working day resolve once that SSD is indexed.
 */
function retryUnmatchedConsists() {
  if (unmatchedConsists.size === 0) return 0;
  let resolved = 0;
  for (const [k, parsed] of [...unmatchedConsists]) {
    const { rid } = resolveRidForParsedConsist(parsed);
    if (rid) {
      applyConsistToRid(rid, parsed);
      unmatchedConsists.delete(k);
      resolved++;
    }
  }
  return resolved;
}

function darwinTrainLengthForRid(rid, ctx = null) {
  const ov = (ctx?.liveOverlayByRid || liveOverlayByRid).get?.(rid);
  const locs = overlayLocsMap(ov);
  if (!locs) return null;
  let found = null;
  for (const loc of locs.values()) {
    const n = Number(loc?.trainLength);
    if (Number.isFinite(n) && n > 0) found = n;
  }
  return found;
}

function resourceGroupVehicleCount(group) {
  return Array.isArray(group?.vehicles) ? group.vehicles.length : 0;
}

/** Drop leftover units when Darwin published a shorter train than PTAC accumulated. */
function trimConsistToPublishedLength(consist, trainLength) {
  if (!consist?.allocations?.length || !(trainLength > 0)) return consist;
  let changed = false;
  const allocations = consist.allocations.map((a) => {
    const groups = a.resourceGroups || [];
    if (groups.length <= 1) return a;
    const total = groups.reduce((n, g) => n + resourceGroupVehicleCount(g), 0);
    if (total <= trainLength) return a;
    const kept = [];
    let n = 0;
    for (let i = groups.length - 1; i >= 0; i--) {
      const c = resourceGroupVehicleCount(groups[i]) || 1;
      if (kept.length && n + c > trainLength) continue;
      kept.unshift(groups[i]);
      n += c;
      if (n >= trainLength) break;
    }
    if (kept.length === groups.length) return a;
    changed = true;
    return { ...a, resourceGroups: kept };
  });
  return changed ? { ...consist, allocations } : consist;
}

/**
 * Historical boards skip unzipping consist/units shards (OOM). Live PTAC is kept
 * across the railway-day rollover, so yesterday’s RIDs are still in `consistByRid`.
 */
function consistForRid(rid, ctx = null) {
  if (!rid) return null;
  const fromCtx = ctx?.consistByRid?.get?.(rid);
  const raw = fromCtx || consistByRid.get(rid) || null;
  if (!raw) return null;
  return trimConsistToPublishedLength(raw, darwinTrainLengthForRid(rid, ctx));
}

/** PTAC unit numbers for departures-board hints (no extra /api/service round-trip). */
function ptacUnitIdsFromConsist(consist) {
  if (!consist?.allocations?.length) return null;
  const ids = [];
  const seen = new Set();
  for (const a of consist.allocations) {
    for (const rg of a.resourceGroups || []) {
      const u = rg.unitId != null && String(rg.unitId).trim();
      if (!u || seen.has(u)) continue;
      seen.add(u);
      ids.push(u);
    }
  }
  return ids.length ? ids : null;
}

// ---------- snapshot builder (per-TIPLOC, on demand) -----------------------
function buildDeparturesAndArrivalsForTiplocs(tiplocs, windowHours, ctx = null) {
  const byRidMap = ctx?.byRid || byRid;
  const byTiplocMap = ctx?.byTiploc || byTiploc;
  const liveOverlayMap = ctx?.liveOverlayByRid || liveOverlayByRid;
  const cancelledMap = ctx?.cancelled || cancelled;
  const delayMap = ctx?.delayReason || delayReason;
  const reverseSet = ctx?.reverseFormation || reverseFormation;
  const associationsMap = ctx?.associationsByRid || associationsByRid;
  const alertsMap = ctx?.alertsByRid || alertsByRid;
  const consistMap = ctx?.consistByRid || consistByRid;
  const formationsMap = ctx?.formationsByRid || formationsByRid;
  // tiplocs is an array — large interchanges share a CRS across multiple
  // TIPLOCs (e.g. STP = STPX [plat 1-4] + STPANCI [plat 5-13] + STPXBOX
  // [Thameslink low-level plat A]); a single CRS query must return all of
  // them. Single-TIPLOC queries pass a 1-element array.
  if (!Array.isArray(tiplocs) || tiplocs.length === 0) return null;

  const upper = tiplocs.map((t) => t.toUpperCase());
  const entries = [];
  let anyKnown = false;
  for (const tip of upper) {
    const list = byTiplocMap.get(tip);
    if (!list) continue;
    anyKnown = true;
    for (const e of list) entries.push({ ...e, sourceTpl: tip });
  }
  if (!anyKnown) return null;

  let now = new Date();
  if (ctx?.historicalDate) {
    if (ctx.boardWallDate && parseAtToMinutes(ctx.historicalAt || '') != null) {
      const w = londonWallInstantFromDateAt(ctx.boardWallDate, ctx.historicalAt);
      if (w) now = w;
    } else if (ctx.boardWallDate) {
      const w = londonWallInstantFromDateAt(ctx.boardWallDate, null);
      if (w) now = w;
    } else {
      const atMin = parseAtToMinutes(ctx.historicalAt || '');
      if (atMin != null) {
        const hh = String(Math.floor(atMin / 60)).padStart(2, '0');
        const mm = String(atMin % 60).padStart(2, '0');
        now = new Date(`${ctx.historicalDate}T${hh}:${mm}:00+01:00`);
      } else {
        now = new Date(`${ctx.historicalDate}T12:00:00+01:00`);
      }
    }
  }
  const ssdTarget =
    ctx?.historicalDate && ctx.historicalAt != null && ctx.boardWallDate
      ? railwayDayYmd(now)
      : (ctx?.historicalDate || railwayDayYmd(new Date()));
  const horizon = new Date(now.getTime() + windowHours * 3600_000);
  const todayRailwaySsd = railwayDayYmd(new Date());
  const isFutureTimetableCtx =
    !!ctx?.historicalDate && compareIsoDate(ctx.historicalDate, todayRailwaySsd) > 0;
  const allowSpansNextSsd =
    (!ctx || ctx.stateSavedAt == null) && !isFutureTimetableCtx;
  const ssdsNeeded = new Set([ssdTarget]);
  if (!ctx || ctx.stateSavedAt == null) {
    ssdsNeeded.add(addDaysIsoDate(ssdTarget, -1));
  }
  if (allowSpansNextSsd) {
    const horizonSsd = railwayDayYmd(horizon);
    if (horizonSsd !== ssdTarget) ssdsNeeded.add(horizonSsd);
  }

  const departures = [];
  const arrivals = [];
  const seenDepRid = new Set();
  const seenArrRid = new Set();

  for (const { rid, stopIdx, sourceTpl } of entries) {
    const j = byRidMap.get(rid);
    if (!j || !ssdsNeeded.has(j.ssd)) continue;
    const stop = j.slots[stopIdx];

    const ov = liveOverlayMap.get(rid);
    const liveLoc = getOverlayLoc(ov, sourceTpl);
    const wholeCancel = normalizeCancellationInfo(cancelledMap.get(rid) || null);
    const stopCancel = liveLoc?.cancelled
      ? (liveLoc.cancelReason
          ? { ...liveLoc.cancelReason, source: 'ts-loc', scope: 'stop' }
          : { reason: 'Cancelled at this stop', source: 'ts-loc', scope: 'stop' })
      : null;
    const cancelInfo = normalizeCancellationInfo(wholeCancel || stopCancel);
    const delayInfo = delayMap.get(rid) || null;

    const callingAfter = [];
    for (let i = stopIdx + 1; i < j.slots.length; i++) {
      const s = j.slots[i];
      if (s.slot === 'PP' || s.slot === 'OPPP') continue;
      callingAfter.push(s.tpl);
    }

    const serviceType = classifyServiceType({
      trainCat: j.trainCat,
      isPassenger: j.isPassenger,
      trainId: j.trainId,
      originName: resolve_.tiplocToName(j.origin),
      destinationName: resolve_.tiplocToName(j.destination),
    });

    const formation = resolveFormationForRid(rid, ctx);
    const baseRow = () => ({
      rid: j.rid,
      trainId: j.trainId,
      uid: j.uid,
      toc: j.toc,
      tocName: resolve_.tocToName(j.toc),
      trainCat: j.trainCat || null,
      serviceType,
      origin: j.origin,
      originName: resolve_.tiplocToName(j.origin),
      originCrs: resolve_.tiplocToCrs(j.origin),
      destination: j.destination,
      destinationName: resolve_.tiplocToName(j.destination),
      destinationCrs: resolve_.tiplocToCrs(j.destination),
      callingAfter,
      callingAfterNames: callingAfter.map(resolve_.tiplocToName),
      callingAfterCrs: callingAfter.map(resolve_.tiplocToCrs),
      isPassenger: j.isPassenger,
      cancelled: cancelInfo ? true : false,
      cancellation: cancelInfo,
      delayReason: delayInfo,
      trainLength: liveLoc?.trainLength ?? null,
      platform: stop.plat,
      livePlatform: liveLoc?.livePlat || null,
      platformSource: liveLoc?.platformSource || null,
      platformConfirmed: !!liveLoc?.platformConfirmed,
      platformSuppressed: !!liveLoc?.platformSuppressed,
      loadingPercentage: liveLoc?.loadPct ?? null,
      coachLoading: liveLoc?.coachLoading ?? null,
      reverseFormation: reverseSet.has(rid),
      hasAssociations: (associationsMap.get(rid)?.length ?? 0) > 0,
      hasAlerts: (alertsMap.get(rid)?.length ?? 0) > 0,
      hasConsist: !!consistForRid(rid, ctx),
      hasFormation: !!formation,
      formation,
      unitIds: ptacUnitIdsFromConsist(consistForRid(rid, ctx)),
      sourceTiploc: sourceTpl,
    });

    // ----- Departures (skip pure terminating stops; TF marks termination) -----
    if (!seenDepRid.has(rid)) {
      const isPassing = stop.slot === 'PP' || stop.slot === 'OPPP';
      const skipDep = stop.slot === 'DT' || stop.slot === 'OPDT' || stop.act === 'TF';
      if (!skipDep) {
        const scheduledTime = isPassing ? (stop.wtp || stop.wtd) : (stop.ptd || stop.wtd);
        if (scheduledTime && scheduledTime.includes(':')) {
          let scheduledAt = anchorTime(scheduledTime, j.ssd);
          if (scheduledAt) {
            scheduledAt = adjustScheduledInstantForRailwayOvernight(scheduledAt, scheduledTime);
            if (scheduledAt.getTime() >= now.getTime() - 5 * 60_000 && scheduledAt <= horizon) {
              seenDepRid.add(rid);
              const unknownDelay = !!liveLoc?.unknownDelay;
              const manualUnknownDelay = !!liveLoc?.manualUnknownDelay;
              const bestTime = liveLoc?.bestTime || scheduledTime;
              const bestKind = liveLoc?.bestKind || 'scheduled';
              const delayMinutes = unknownDelay ? null : computeDelayMinutes(scheduledTime, bestTime, bestKind);
              departures.push({
                ...baseRow(),
                movement: 'departure',
                isPassing,
                scheduledTime,
                scheduledAt: scheduledAt.toISOString(),
                liveTime: bestTime,
                liveKind: bestKind,
                liveSource: liveLoc?.liveSource || null,
                liveSourceInstance: liveLoc?.liveSourceInstance || null,
                unknownDelay,
                manualUnknownDelay,
                delayMinutes,
                status: cancelInfo
                  ? 'CANCELLED'
                  : unknownDelay
                    ? 'delayed'
                    : delayMinutes == null
                      ? ((bestKind === 'scheduled' || bestKind === 'working') ? 'on time' : `${bestKind} ${bestTime}`)
                      : (delayMinutes === 0 ? 'on time' : `${bestKind} ${bestTime} (${delayMinutes > 0 ? '+' : ''}${delayMinutes}m)`),
              });
            }
          }
        }
      }
    }

    // ----- Arrivals (terminators + intermediate stops with public/working arr) -----
    if (!seenArrRid.has(rid)) {
      const slot = stop.slot;
      const arrivalEligible =
        slot === 'DT' || slot === 'OPDT'
        || slot === 'IP' || slot === 'OPIP';
      if (arrivalEligible) {
        const scheduledTime = (slot === 'DT' || slot === 'OPDT')
          ? (stop.pta || stop.wta || stop.ptd || stop.wtd)
          : (stop.pta || stop.wta || stop.ptd || stop.wtd);
        if (scheduledTime && scheduledTime.includes(':')) {
          let scheduledAt = anchorTime(scheduledTime, j.ssd);
          if (scheduledAt) {
            scheduledAt = adjustScheduledInstantForRailwayOvernight(scheduledAt, scheduledTime);
            if (scheduledAt.getTime() >= now.getTime() - 5 * 60_000 && scheduledAt <= horizon) {
              seenArrRid.add(rid);
              const unknownDelay = !!(liveLoc?.arrUnknownDelay ?? liveLoc?.unknownDelay);
              const manualUnknownDelay = !!(liveLoc?.arrManualUnknownDelay ?? liveLoc?.manualUnknownDelay);
              const bestTime = (liveLoc?.arrLiveTime != null && liveLoc.arrLiveTime !== '')
                ? liveLoc.arrLiveTime
                : scheduledTime;
              const bestKind = liveLoc?.arrLiveKind || 'scheduled';
              const delayMinutes = unknownDelay ? null : computeDelayMinutes(scheduledTime, bestTime, bestKind);
              arrivals.push({
                ...baseRow(),
                movement: 'arrival',
                isPassing: false,
                scheduledTime,
                scheduledAt: scheduledAt.toISOString(),
                liveTime: bestTime,
                liveKind: bestKind,
                liveSource: liveLoc?.arrLiveSource ?? liveLoc?.liveSource ?? null,
                liveSourceInstance: liveLoc?.arrLiveSourceInstance ?? liveLoc?.liveSourceInstance ?? null,
                unknownDelay,
                manualUnknownDelay,
                delayMinutes,
                status: cancelInfo
                  ? 'CANCELLED'
                  : unknownDelay
                    ? 'delayed'
                    : delayMinutes == null
                      ? ((bestKind === 'scheduled' || bestKind === 'working') ? 'on time' : `${bestKind} ${bestTime}`)
                      : (delayMinutes === 0 ? 'on time' : `${bestKind} ${bestTime} (${delayMinutes > 0 ? '+' : ''}${delayMinutes}m)`),
              });
            }
          }
        }
      }
    }
  }

  departures.sort((a, b) => a.scheduledAt.localeCompare(b.scheduledAt));
  arrivals.sort((a, b) => a.scheduledAt.localeCompare(b.scheduledAt));
  return { departures, arrivals };
}

// All currently-known NRCC messages for a CRS, freshest first within each
// severity bucket so the UI can pick a representative one for a banner.
function listMessagesForCrs(crs) {
  const ids = stationMessages.get(String(crs).toUpperCase());
  if (!ids || ids.size === 0) return [];
  const out = [];
  for (const id of ids) {
    const m = messagesById.get(id);
    if (m) out.push(m);
  }
  out.sort((a, b) => (b.severity - a.severity) || b.receivedAt.localeCompare(a.receivedAt));
  return out;
}

function buildSnapshot(tiplocs, windowHours, primaryTiploc, ctx = null) {
  const all = Array.isArray(tiplocs) ? tiplocs : [tiplocs];
  const primary = (primaryTiploc || all[0]).toUpperCase();
  const built = buildDeparturesAndArrivalsForTiplocs(all, windowHours, ctx);
  if (built === null) return null;
  const { departures: rows, arrivals: arrRows } = built;
  const stationCrs = resolve_.tiplocToCrs(primary);
  const messages = stationCrs ? listMessagesForCrs(stationCrs) : [];
  const combined = [...rows, ...arrRows];
  return {
    tiploc: primary,
    // When the station spans several TIPLOCs, expose the full set so the
    // caller (and the website) can see what's been merged.
    tiplocs: all.length > 1 ? all : undefined,
    stationName: resolve_.tiplocToName(primary),
    stationCrs,
    updatedAt: new Date().toISOString(),
    historicalDate: ctx?.historicalDate || null,
    historicalAt: ctx?.historicalAt || null,
    historicalSavedAt: ctx?.stateSavedAt || null,
    wallClockDate: ctx?.boardWallDate || null,
    timetableFile: timetablePath.split('/').pop(),
    windowHours,
    counts: {
      departures: rows.length,
      arrivals: arrRows.length,
      cancelled: combined.filter((r) => r.cancelled).length,
      withDelay: combined.filter((r) => r.delayReason).length,
      messages: messages.length,
    },
    messages,
    kafka: {
      consumed: stats.consumed,
      updatesApplied: stats.updates,
      startedAt: stats.startedAt,
      lastMessageAt: stats.lastKafkaMsgAt,
    },
    departures: rows,
    arrivals: arrRows,
  };
}

function getCachedSnapshot(cacheKey, allowStale = true) {
  const hit = departuresCache.get(cacheKey);
  if (!hit?.snapshot) return null;
  const stale = Date.now() > hit.expiresAtMs;
  if (stale && !allowStale) return null;
  return hit.snapshot;
}

function snapshotIsStale(cacheKey) {
  const hit = departuresCache.get(cacheKey);
  if (!hit) return true;
  return Date.now() > hit.expiresAtMs;
}

function putCachedSnapshot(cacheKey, snapshot, ttlMs = null) {
  const ttl = ttlMs ?? cfg.departuresCacheMs;
  if (ttl <= 0) return;
  departuresCache.set(cacheKey, {
    expiresAtMs: Date.now() + ttl,
    snapshot,
  });
}

function serviceDetailCacheKey(rid, date = '', at = '') {
  return `${rid}|${date || ''}|${at || ''}`;
}

function getCachedServiceDetail(cacheKey, allowStale = true) {
  const hit = serviceDetailCache.get(cacheKey);
  if (!hit?.detail) return null;
  const stale = Date.now() > hit.expiresAtMs;
  if (stale && !allowStale) return null;
  return hit.detail;
}

function serviceDetailIsStale(cacheKey) {
  const hit = serviceDetailCache.get(cacheKey);
  if (!hit) return true;
  return Date.now() > hit.expiresAtMs;
}

function putCachedServiceDetail(cacheKey, detail, ttlMs = null) {
  const ttl = ttlMs ?? cfg.departuresCacheMs;
  if (ttl <= 0 || !detail) return;
  serviceDetailCache.set(cacheKey, { expiresAtMs: Date.now() + ttl, detail });
  while (serviceDetailCache.size > 25000) {
    const first = serviceDetailCache.keys().next().value;
    if (first == null) break;
    serviceDetailCache.delete(first);
  }
}

/** Today's live board/service — ignore ?date=today so it hits the hot cache (keys use empty date). */
function isLiveCacheQuery(dateParam, atParam) {
  if (atParam) return false;
  if (!dateParam) return true;
  const liveDay = loadedDate || railwayDayYmd(new Date());
  return dateParam === liveDay;
}

function collectRidsFromSnap(snap, into) {
  for (const row of snap?.departures || []) if (row?.rid) into.add(row.rid);
  for (const row of snap?.arrivals || []) if (row?.rid) into.add(row.rid);
}

function cacheLiveServiceDetails(rids, ttl, ctx = null, date = '', at = '') {
  let n = 0;
  for (const rid of rids) {
    const key = serviceDetailCacheKey(rid, date, at);
    if (serviceDetailCache.has(key)) continue;
    try {
      const detail = buildServiceDetail(rid, ctx);
      if (!detail) continue;
      putCachedServiceDetail(key, detail, ttl);
      n++;
    } catch {}
  }
  return n;
}

let historyDatesLiteCache = { at: 0, payload: null };
let unitsCatalogCache = { at: 0, payload: null };
let hotBoardTimer = null;
let hotCrsOrdered = [];
let hotBoardCursor = 0;
let hotBoardCycleStartedAt = 0;
let lastHotBoardCycleMs = 0;
let hotBoardBusy = false;

function refreshHotCrsList() {
  const all = [...(crsToTiplocs?.keys() || [])];
  const priority = cfg.hotBoardCrs.filter((c) => crsToTiplocs?.has(c));
  const seen = new Set(priority);
  hotCrsOrdered = [...priority, ...all.filter((c) => !seen.has(c))];
  hotBoardCursor = 0;
  hotBoardCycleStartedAt = Date.now();
}

function getHistoryDatesLitePayload() {
  if (historyDatesLiteCache.payload && Date.now() - historyDatesLiteCache.at < 30_000) {
    return historyDatesLiteCache.payload;
  }
  const dates = knownBoardDates().map((d) => ({
    date: d,
    hasState: historyDayHasState(d),
    hasTimetable: !!pickTimetableForDate(d),
    snapshots: [],
  }));
  const payload = {
    count: dates.length,
    retentionDays: STATE_HISTORY_RETENTION_DAYS,
    pruneOnPersist: STATE_HISTORY_PRUNE_ON_PERSIST,
    dates,
    updatedAt: new Date().toISOString(),
  };
  historyDatesLiteCache = { at: Date.now(), payload };
  return payload;
}

function getUnitsCatalogPayload(fleetFilter) {
  if (!fleetFilter && unitsCatalogCache.payload && Date.now() - unitsCatalogCache.at < 15_000) {
    return unitsCatalogCache.payload;
  }
  const units = [...unitCatalogById.values()]
    .filter((u) => !fleetFilter || String(u.fleetId || '').toUpperCase().includes(fleetFilter))
    .sort((a, b) => String(a.unitId || '').localeCompare(String(b.unitId || '')));
  const fleets = new Map();
  for (const u of units) {
    const f = (u.fleetId || 'unknown').toString();
    if (!fleets.has(f)) fleets.set(f, { fleetId: f, unitCount: 0 });
    fleets.get(f).unitCount += 1;
  }
  const payload = {
    count: units.length,
    fleetFilter: fleetFilter || null,
    fleets: [...fleets.values()].sort((a, b) => a.fleetId.localeCompare(b.fleetId)),
    units,
    updatedAt: new Date().toISOString(),
  };
  if (!fleetFilter) unitsCatalogCache = { at: Date.now(), payload };
  return payload;
}

function cacheLiveBoard(crs, hours, ttl) {
  const resolved = resolveStationCode(crs);
  if (!resolved) return null;
  const cacheKey = `${resolved.tiplocs.join(',')}|${hours}||`;
  const snap = buildSnapshot(resolved.tiplocs, hours, resolved.tiploc, null);
  if (!snap) return null;
  snap.stationName = resolved.name || snap.stationName;
  snap.stationCrs = resolved.crs || snap.stationCrs;
  snap.matchedAs = resolved.matchedAs;
  if (resolved.alternates) snap.alternates = resolved.alternates;
  putCachedSnapshot(cacheKey, snap, ttl ?? cfg.departuresCacheMs);
  return snap;
}

function cacheDatedBoard(crs, hours, ttl, ctx, date, at) {
  const resolved = resolveStationCode(crs);
  if (!resolved) return null;
  const cacheKey = `${resolved.tiplocs.join(',')}|${hours}|${date || ''}|${at || ''}`;
  const snap = buildSnapshot(resolved.tiplocs, hours, resolved.tiploc, ctx);
  if (!snap) return null;
  snap.stationName = resolved.name || snap.stationName;
  snap.stationCrs = resolved.crs || snap.stationCrs;
  snap.matchedAs = resolved.matchedAs;
  if (resolved.alternates) snap.alternates = resolved.alternates;
  putCachedSnapshot(cacheKey, snap, ttl);
  return snap;
}

async function warmHotBoardBatch() {
  if (heavyReloadBusy) return;
  if (hotBoardBusy) return;
  if (!crsToTiplocs || crsToTiplocs.size === 0) return;
  if (hotCrsOrdered.length !== crsToTiplocs.size) refreshHotCrsList();
  if (hotCrsOrdered.length === 0) return;
  hotBoardBusy = true;
  try {
    const hours = 1;
    const ttl = cfg.departuresCacheMs;
    const end = Math.min(hotCrsOrdered.length, hotBoardCursor + cfg.hotBoardBatch);
    for (; hotBoardCursor < end; hotBoardCursor++) {
      const crs = hotCrsOrdered[hotBoardCursor];
      try {
        cacheLiveBoard(crs, hours, ttl);
      } catch (e) {
        console.warn(`[daemon] hot board ${crs} failed: ${e.message}`);
      }
      await sleepMs(0);
    }
    if (hotBoardCursor >= hotCrsOrdered.length) {
      lastHotBoardCycleMs = Date.now() - hotBoardCycleStartedAt;
      hotBoardCursor = 0;
      hotBoardCycleStartedAt = Date.now();
      console.log(`[daemon] hot boards: ${hotCrsOrdered.length} CRS cycle ${(lastHotBoardCycleMs / 1000).toFixed(1)}s (${serviceDetailCache.size} service details)`);
    }
  } finally {
    hotBoardBusy = false;
  }
}

function scheduleHotBoardTick() {
  if (hotBoardTimer) clearTimeout(hotBoardTimer);
  hotBoardTimer = setTimeout(() => {
    warmHotBoardBatch()
      .catch((e) => console.warn(`[daemon] hot board refresh failed: ${e.message}`))
      .finally(() => scheduleHotBoardTick());
  }, cfg.hotBoardTickMs);
  if (typeof hotBoardTimer.unref === 'function') hotBoardTimer.unref();
}

async function warmAllStationsOnce() {
  refreshHotCrsList();
  const n = hotCrsOrdered.length;
  const t0 = Date.now();
  const hours = 1;
  const ttl = cfg.departuresCacheMs;
  const rids = new Set();
  for (let i = 0; i < n; i++) {
    try {
      const snap = cacheLiveBoard(hotCrsOrdered[i], hours, ttl);
      if (snap) collectRidsFromSnap(snap, rids);
    } catch (e) {
      console.warn(`[boot] hot board ${hotCrsOrdered[i]} failed: ${e.message}`);
    }
    if ((i + 1) % 200 === 0 || i + 1 === n) {
      console.log(`[boot] hot boards ${i + 1}/${n}`);
      await sleepMs(0);
    }
  }
  console.log(`[boot] priming ${rids.size} live service details`);
  let built = 0;
  let i = 0;
  for (const rid of rids) {
    built += cacheLiveServiceDetails([rid], ttl);
    i++;
    if (i % 200 === 0) await sleepMs(0);
  }
  lastHotBoardCycleMs = Date.now() - t0;
  hotBoardCursor = 0;
  hotBoardCycleStartedAt = Date.now();
  console.log(`[boot] hot boards ready: ${n} CRS, ${serviceDetailCache.size} service details (${built} built) in ${(lastHotBoardCycleMs / 1000).toFixed(1)}s`);
}

async function warmHorizonStationCache() {
  if (!hotCrsOrdered.length) refreshHotCrsList();
  const origin = loadedDate || railwayDayYmd(new Date());
  const hours = 1;
  const ttl = Math.max(cfg.departuresHistCacheMs, 6 * 60 * 60_000);
  const days = [];
  for (let i = cfg.warmupDays; i >= 1; i--) days.push({ ymd: addDaysIsoDate(origin, -i), kind: 'past' });
  for (let i = 1; i <= cfg.warmupFutureDays; i++) days.push({ ymd: addDaysIsoDate(origin, i), kind: 'future' });

  for (const { ymd, kind } of days) {
    const rssMb = process.memoryUsage().rss / 1024 / 1024;
    if (rssMb >= cfg.hotHorizonMaxRssMb) {
      console.warn(`[boot] horizon boards stopped at ${ymd}: RSS ${rssMb.toFixed(0)}MB >= ${cfg.hotHorizonMaxRssMb}MB`);
      break;
    }
    let ctx = kind === 'past'
      ? (await getHistoricalContext(ymd)) || (await getTimetableOnlyContext(ymd))
      : await getTimetableOnlyContext(ymd);
    if (!ctx) {
      console.log(`[boot] ${kind} ${ymd} boards skipped (no context)`);
      continue;
    }
    ctx = { ...ctx, boardWallDate: ymd };
    const rids = new Set();
    const n = hotCrsOrdered.length;
    const t0 = Date.now();
    for (let i = 0; i < n; i++) {
      try {
        const snap = cacheDatedBoard(hotCrsOrdered[i], hours, ttl, ctx, ymd, '');
        if (snap) collectRidsFromSnap(snap, rids);
      } catch (e) {
        console.warn(`[boot] ${kind} ${ymd} ${hotCrsOrdered[i]} failed: ${e.message}`);
      }
      if ((i + 1) % 200 === 0) await sleepMs(0);
    }
    let built = 0;
    let j = 0;
    for (const rid of rids) {
      built += cacheLiveServiceDetails([rid], ttl, ctx, ymd, '');
      j++;
      if (j % 200 === 0) await sleepMs(0);
    }
    const heapMb = process.memoryUsage().heapUsed / 1024 / 1024;
    console.log(
      `[boot] ${kind} ${ymd}: ${n} CRS, ${rids.size} services (${built} built) in ${((Date.now() - t0) / 1000).toFixed(1)}s heap=${heapMb.toFixed(0)}MB rss=${rssMb.toFixed(0)}MB`,
    );
  }
}

async function runLiveHotWarmup() {
  refreshPtacDayCounts();
  getHistoryDatesLitePayload();
  getUnitsCatalogPayload('');
  await warmAllStationsOnce();
  await warmHorizonStationCache();
}

// ---------- CRS / TIPLOC resolution ---------------------------------------
function resolveStationCode(rawCode) {
  if (!rawCode) return null;
  const code = rawCode.toUpperCase();
  // If it's already a valid TIPLOC, use it directly.
  if (locations.has(code)) {
    const info = locations.get(code);
    return {
      tiploc: code,                  // primary
      tiplocs: [code],               // all (1)
      crs: info.crs,
      name: info.name || code,
      matchedAs: 'tiploc',
    };
  }
  // CRS lookup — a CRS can map to multiple TIPLOCs that ALL belong to the
  // same station (e.g. STP = STPX [plat 1-4] + STPANCI [plat 5-13] +
  // STPXBOX [Thameslink low-level plat A]). We aggregate departures across
  // all of them; the "primary" is just the one with the most services for
  // labelling purposes.
  const candidates = crsToTiplocs.get(code) || [];
  if (candidates.length === 0) return null;
  // Sort by service count desc so primary is candidates[0].
  const ranked = candidates
    .map((t) => ({ t, n: (byTiploc.get(t) || []).length }))
    .sort((a, b) => b.n - a.n)
    .map((x) => x.t);
  const primary = ranked[0];
  const info = locations.get(primary);
  return {
    tiploc: primary,
    tiplocs: ranked,                  // all (>= 1) — all aggregated by /api/departures
    crs: code,
    name: info?.name || primary,
    matchedAs: 'crs',
    alternates: ranked.length > 1 ? ranked.slice(1) : undefined,
  };
}

let plannerIndexCache = { byRid: null, ssd: null, index: null };

function plannerTplToCrs(tpl) {
  const crs = locations.get(String(tpl || '').toUpperCase())?.crs;
  return crs ? String(crs).toUpperCase() : null;
}

function nameForCrs(crs) {
  const resolved = resolveStationCode(crs);
  return resolved?.name || String(crs || '').toUpperCase();
}

function byRidForPlannerSsd(byRidMap, ssd) {
  if (!ssd || !byRidMap) return byRidMap;
  const want = normalizeSsdYmd(ssd);
  const next = new Map();
  for (const [rid, j] of byRidMap) {
    if (normalizeSsdYmd(j?.ssd) === want) next.set(rid, j);
  }
  return next;
}

function getPlannerIndex(byRidMap, ssd = null) {
  if (
    plannerIndexCache.byRid === byRidMap
    && plannerIndexCache.ssd === (ssd || null)
    && plannerIndexCache.index
  ) {
    return plannerIndexCache.index;
  }
  const source = byRidForPlannerSsd(byRidMap, ssd) || byRidMap;
  const index = buildPlannerIndex(source, plannerTplToCrs);
  plannerIndexCache = { byRid: byRidMap, ssd: ssd || null, index };
  return index;
}

function readJsonBody(req, maxBytes = 200000) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let n = 0;
    req.on('data', (c) => {
      n += c.length;
      if (n > maxBytes) {
        reject(Object.assign(new Error('body too large'), { status: 413 }));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8');
      if (!raw.trim()) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(raw));
      } catch {
        reject(Object.assign(new Error('invalid json'), { status: 400 }));
      }
    });
    req.on('error', reject);
  });
}

function attachBashBoardCards(result, byRidMap, ctx, ssd) {
  if (!result?.ok || !Array.isArray(result.hops)) return result;
  const liveDay = loadedDate || railwayDayYmd(new Date());
  const isLiveDay = normalizeSsdYmd(ssd) === normalizeSsdYmd(liveDay);
  const liveOverlay = isLiveDay ? liveOverlayByRid : (ctx?.liveOverlayByRid || liveOverlayByRid);
  const cancelledMap = isLiveDay ? cancelled : (ctx?.cancelled || cancelled);
  const delayMap = isLiveDay ? delayReason : (ctx?.delayReason || delayReason);
  const reverseSet = isLiveDay ? reverseFormation : (ctx?.reverseFormation || reverseFormation);
  const formationsMap = isLiveDay ? formationsByRid : (ctx?.formationsByRid || new Map());
  const consistMap = isLiveDay ? consistByRid : (ctx?.consistByRid || new Map());
  const assocMap = isLiveDay ? associationsByRid : (ctx?.associationsByRid || associationsByRid);
  const alertsMap = isLiveDay ? alertsByRid : (ctx?.alertsByRid || alertsByRid);

  for (const hop of result.hops) {
    for (const leg of hop.legs || []) {
      if (!leg?.rid || leg.rid === '__WALK__' || leg.trainId === 'Walk') {
        leg.board = null;
        continue;
      }
      const j = byRidMap.get(leg.rid);
      if (!j) {
        leg.board = null;
        continue;
      }
      const fromTpl = leg.fromTpl || null;
      const stop = (j.slots || []).find((s) => fromTpl && s.tpl === fromTpl)
        || (j.slots || []).find((s) => plannerTplToCrs(s.tpl) === String(leg.fromCrs || '').toUpperCase());
      const ov = liveOverlay.get(leg.rid);
      const liveLoc = getOverlayLoc(ov, stop?.tpl || fromTpl);
      const legFormation = resolveFormationForRid(leg.rid, {
        formationsByRid: formationsMap,
        associationsByRid: assocMap,
        byRid: byRidMap,
        formationsByUid: isLiveDay ? formationsByUid : ctx?.formationsByUid,
      });
      const cancelInfo = normalizeCancellationInfo(cancelledMap.get(leg.rid) || null);
      const delayInfo = delayMap.get(leg.rid) || null;
      const scheduledTime = String(leg.dep || '').slice(0, 5);
      const scheduledAt = anchorTime(scheduledTime, ssd);
      const callingAfter = [];
      if (stop) {
        const idx = (j.slots || []).indexOf(stop);
        for (let i = idx + 1; i < (j.slots || []).length; i++) {
          const s = j.slots[i];
          if (s.slot === 'PP' || s.slot === 'OPPP') continue;
          callingAfter.push(s.tpl);
        }
      }
      const liveDep = liveLoc?.bestTime || scheduledTime;
      const liveKind = liveLoc?.bestKind || 'scheduled';
      const unknownDelay = !!liveLoc?.unknownDelay;
      const manualUnknownDelay = !!liveLoc?.manualUnknownDelay;
      const delayMinutes = unknownDelay ? null : computeDelayMinutes(scheduledTime, liveDep, liveKind);
      const status = cancelInfo
        ? 'CANCELLED'
        : unknownDelay || manualUnknownDelay
          ? 'delayed'
          : delayMinutes == null
            ? ((liveKind === 'scheduled' || liveKind === 'working') ? 'on time' : `${liveKind} ${liveDep}`)
            : (delayMinutes === 0 ? 'on time' : `${liveKind} ${liveDep} (${delayMinutes > 0 ? '+' : ''}${delayMinutes}m)`);
      leg.board = {
        rid: j.rid,
        trainId: j.trainId,
        uid: j.uid || '',
        movement: 'departure',
        toc: j.toc || '',
        tocName: resolve_.tocToName(j.toc),
        trainCat: j.trainCat || null,
        serviceType: classifyServiceType({
          trainCat: j.trainCat,
          isPassenger: j.isPassenger,
          trainId: j.trainId,
          originName: resolve_.tiplocToName(j.origin),
          destinationName: resolve_.tiplocToName(j.destination),
        }),
        scheduledTime,
        scheduledAt: scheduledAt ? scheduledAt.toISOString() : `${ssd}T${scheduledTime}:00`,
        liveTime: liveDep,
        liveKind,
        liveSource: liveLoc?.liveSource || null,
        liveSourceInstance: liveLoc?.liveSourceInstance || null,
        unknownDelay,
        manualUnknownDelay,
        delayMinutes,
        trainLength: liveLoc?.trainLength ?? null,
        platform: stop?.plat || leg.fromPlat || null,
        livePlatform: liveLoc?.livePlat || null,
        platformSource: liveLoc?.platformSource || null,
        platformConfirmed: !!liveLoc?.platformConfirmed,
        platformSuppressed: !!liveLoc?.platformSuppressed,
        origin: j.origin,
        originName: resolve_.tiplocToName(j.origin),
        originCrs: resolve_.tiplocToCrs(j.origin),
        destination: j.destination,
        destinationName: resolve_.tiplocToName(j.destination),
        destinationCrs: resolve_.tiplocToCrs(j.destination),
        callingAfter,
        callingAfterNames: callingAfter.map(resolve_.tiplocToName),
        callingAfterCrs: callingAfter.map(resolve_.tiplocToCrs),
        isPassenger: j.isPassenger !== false,
        cancelled: !!cancelInfo,
        cancellation: cancelInfo,
        delayReason: delayInfo,
        loadingPercentage: liveLoc?.loadPct ?? null,
        coachLoading: liveLoc?.coachLoading ?? null,
        reverseFormation: reverseSet.has(leg.rid),
        hasAssociations: (assocMap.get(leg.rid)?.length ?? 0) > 0,
        hasAlerts: (alertsMap.get(leg.rid)?.length ?? 0) > 0,
        hasConsist: !!consistForRid(leg.rid, ctx),
        hasFormation: !!legFormation,
        formation: legFormation,
        unitIds: ptacUnitIdsFromConsist(consistForRid(leg.rid, ctx)),
        sourceTiploc: stop?.tpl || fromTpl || undefined,
        isPassing: false,
        status,
      };
    }
  }
  return result;
}

// ---------- service detail (full calling pattern) --------------------------
function buildServiceDetail(rid, ctx = null) {
  const byRidMap = ctx?.byRid || byRid;
  const liveOverlay = ctx?.liveOverlayByRid || liveOverlayByRid;
  const cancelledMap = ctx?.cancelled || cancelled;
  const delayMap = ctx?.delayReason || delayReason;
  const reverseSet = ctx?.reverseFormation || reverseFormation;
  const formationsMap = ctx?.formationsByRid || formationsByRid;
  const consistMap = ctx?.consistByRid || consistByRid;
  const assocMap = ctx?.associationsByRid || associationsByRid;
  const alertsMap = ctx?.alertsByRid || alertsByRid;
  const j = byRidMap.get(rid);
  if (!j) return null;
  const ov = liveOverlay.get(rid);
  const cancelInfo = normalizeCancellationInfo(cancelledMap.get(rid) || null);
  const delayInfo  = delayMap.get(rid) || null;

  function resolveStopName(tpl, crs) {
    const direct = resolve_.tiplocToName(tpl);
    if (direct && direct.toUpperCase() !== tpl.toUpperCase()) return direct;
    if (!crs) return null;
    const candidates = crsToTiplocs.get(String(crs).toUpperCase()) || [];
    for (const candidateTpl of candidates) {
      const n = resolve_.tiplocToName(candidateTpl);
      if (n && n.toUpperCase() !== candidateTpl.toUpperCase()) return n;
    }
    return null;
  }

  const baseStops = j.slots.map((s) => {
    const live = getOverlayLoc(ov, s.tpl);
    const crs = resolve_.tiplocToCrs(s.tpl);
    return {
      tpl: s.tpl,
      name: resolveStopName(s.tpl, crs),
      crs,
      slot: s.slot,                          // OR / IP / PP / DT / OPxx
      pta:  s.pta,
      ptd:  s.ptd,
      wta:  s.wta,
      wtd:  s.wtd,
      wtp:  s.wtp,
      platform: s.plat,
      livePlatform: live?.livePlat || null,
      platformSource: live?.platformSource || null,
      platformConfirmed: !!live?.platformConfirmed,
      platformSuppressed: !!live?.platformSuppressed,
      activity: s.act,
      liveTime: live?.bestTime || null,
      liveKind: live?.bestKind || null,
      liveSource: live?.liveSource || null,
      liveSourceInstance: live?.liveSourceInstance || null,
      unknownDelay: !!live?.unknownDelay,
      manualUnknownDelay: !!live?.manualUnknownDelay,
      trainLength: live?.trainLength ?? null,
      cancelledAtStop: live?.cancelled || false,
      cancelReasonAtStop: live?.cancelReason || null,
      // Loading at this stop (if Darwin published it): overall % and/or
      // per-coach 0–100%. Null means no live loading data yet.
      loadingPercentage: live?.loadPct ?? null,
      coachLoading: live?.coachLoading ?? null,
    };
  });

  const stops = baseStops;

  // A "partial cancellation" is when the whole service isn't cancelled but
  // one or more individual stops are. The UI uses this to show a banner
  // explaining the situation alongside per-stop strikethroughs.
  const partiallyCancelled = !cancelInfo && stops.some((s) => s.cancelledAtStop);

  // Resolve associated services to human-readable summaries. We only look up
  // basic info on the *other* RID — full traversal can be done by the client
  // by following the RID into another /api/service/:rid call.
  const associations = (assocMap.get(rid) || []).map((a) => {
    const otherRid = a.mainRid === rid ? a.assocRid : a.mainRid;
    const other = byRidMap.get(otherRid);
    return {
      ...a,
      role: a.mainRid === rid ? 'main' : 'associated',
      otherRid,
      otherTrainId: other?.trainId || null,
      otherToc: other?.toc || null,
      otherOriginName:      other ? resolve_.tiplocToName(other.origin) : null,
      otherDestinationName: other ? resolve_.tiplocToName(other.destination) : null,
      tiplocName: resolve_.tiplocToName(a.tiploc),
      tiplocCrs:  resolve_.tiplocToCrs(a.tiploc),
    };
  });

  return {
    rid: j.rid,
    uid: j.uid,
    trainId: j.trainId,
    ssd: j.ssd,
    toc: j.toc,
    tocName: resolve_.tocToName(j.toc),
    trainCat: j.trainCat,
    isPassenger: j.isPassenger,
    origin: j.origin,
    originName: resolve_.tiplocToName(j.origin),
    destination: j.destination,
    destinationName: resolve_.tiplocToName(j.destination),
    cancelled: cancelInfo ? true : false,
    cancellation: cancelInfo,
    partiallyCancelled,
    delayReason: delayInfo,
    reverseFormation: reverseSet.has(rid),
    formation:    resolveFormationForRid(rid, ctx),
    // PTAC consist (physical reality view): unit numbers, vehicles, defects,
    // class identification. Null when no PTAC message has been received for
    // this RID yet (most regional services + LNER don't publish to PTAC).
    consist:      consistForRid(rid, ctx),
    associations,
    alerts:       alertsMap.get(rid) || [],
    stops,
    updatedAt: new Date().toISOString(),
  };
}

// ---------- HTTP server ----------------------------------------------------
function pickCorsOrigin(req) {
  if (cfg.corsOrigins.includes('*')) return '*';
  const origin = req.headers.origin;
  if (origin) {
    if (cfg.corsOrigins.includes(origin)) return origin;
    // Allow controlled wildcard entries like https://*.railstatistics.co.uk
    for (const allowed of cfg.corsOrigins) {
      if (!allowed.includes('*')) continue;
      const wildcard = allowed.match(/^(https?:\/\/)\*\.([^/:]+)(:\d+)?$/i);
      if (!wildcard) continue;
      const [, proto, rootHost, portPart = ''] = wildcard;
      const originMatch = origin.match(/^(https?:\/\/)([^/:]+)(:\d+)?$/i);
      if (!originMatch) continue;
      const [, originProto, originHost, originPort = ''] = originMatch;
      if (originProto.toLowerCase() !== proto.toLowerCase()) continue;
      if (originPort !== portPart) continue;
      if (originHost.toLowerCase() === rootHost.toLowerCase()) continue;
      if (originHost.toLowerCase().endsWith(`.${rootHost.toLowerCase()}`)) return origin;
    }
    // Browser-origin request, but not allow-listed.
    return null;
  }
  // Non-browser/no-origin requests (CLI/health checks).
  return cfg.corsOrigins[0] || 'http://localhost:3000';
}
/** Env: DARWIN_CLIENT_READY_AFTER=live (default) | restored | warm.
 *  HTTP does not bind until live caches are ready, so default is open. */
function clientReadsAllowed() {
  if (heavyReloadBusy) return false;
  const policy = String(process.env.DARWIN_CLIENT_READY_AFTER || 'live').trim().toLowerCase();
  if (policy === 'warm' || policy === 'fully_warm' || policy === 'full') {
    return daemonMode === 'fully_warm';
  }
  if (policy === 'restored') return liveCachesReady;
  return daemonMode === 'live_ready' || daemonMode === 'warming_history' || daemonMode === 'fully_warm';
}

/**
 * @param {import('node:http').IncomingMessage} req
 * @param {{ cacheControl?: string }} [opts] — e.g. historical departures allow short private caching to cut repeat bytes.
 */
function sendJson(res, status, body, req, opts = {}) {
  const pretty = ['1', 'true', 'yes'].includes(String(process.env.DARWIN_JSON_PRETTY || '').toLowerCase());
  const json = pretty ? JSON.stringify(body, null, 2) : JSON.stringify(body);
  const cors = pickCorsOrigin(req);
  const headers = {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': opts.cacheControl || 'no-store',
    'Vary': 'Origin',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };
  if (cors) headers['Access-Control-Allow-Origin'] = cors;
  res.writeHead(status, {
    ...headers,
  });
  res.end(json);
}

async function handleRequest(req, res) {
  if (req.method === 'OPTIONS') {
    const cors = pickCorsOrigin(req);
    const headers = {
      'Vary': 'Origin',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, X-API-Key',
    };
    if (cors) headers['Access-Control-Allow-Origin'] = cors;
    res.writeHead(204, {
      ...headers,
    });
    res.end();
    return;
  }
  const url = new URL(req.url, `http://localhost:${cfg.port}`);
  const parts = url.pathname.split('/').filter(Boolean);
  const isPlanBash = req.method === 'POST' && parts.length === 3 && parts[0] === 'api' && parts[1] === 'plan' && parts[2] === 'bash';
  const isStateStore = parts.length === 3 && parts[0] === 'api' && parts[1] === 'admin' && parts[2] === 'state-store';
  if (req.method !== 'GET' && !isPlanBash && !(isStateStore && req.method === 'POST')) {
    sendJson(res, 405, { error: 'method not allowed' }, req);
    return;
  }

  const isHealth = parts.length === 2 && parts[0] === 'api' && parts[1] === 'health';
  const isPing = parts.length === 2 && parts[0] === 'api' && parts[1] === 'ping';
  // Optional API-key auth guard. When INTERNAL_API_KEY is set, every request
  // must include matching X-API-Key. /api/ping is exempt so public uptime checks work.
  if (cfg.internalApiKeys.length > 0 && !isPing) {
    const presented = (req.headers['x-api-key'] || '').toString().trim();
    if (!cfg.internalApiKeys.includes(presented)) {
      sendJson(res, 401, { error: 'unauthorized' }, req);
      return;
    }
  }

  if (!isHealth && !isPing && !isStateStore && !clientReadsAllowed()) {
    sendJson(res, 503, {
      ok: false,
      error: heavyReloadBusy ? 'reloading' : 'starting',
      mode: daemonMode,
      liveCachesReady,
      heavyReloadBusy,
      heavyReloadReason: heavyReloadReason || undefined,
      retryAfterSec: heavyReloadBusy ? 15 : 3,
      hint: heavyReloadBusy
        ? 'Timetable reload in progress; retry shortly.'
        : 'Caches still loading after startup; retry shortly.',
    }, req);
    return;
  }

  // /api/ping — tiny JSON liveness for load balancers / GCP uptime (no API key; survives warmup gate above).
  if (isPing) {
    sendJson(res, 200, { ok: true, service: 'darwin-daemon', time: new Date().toISOString() }, req);
    return;
  }

  if (isStateStore) {
    if (req.method === 'GET') {
      sendJson(res, 200, { ok: true, ...storeSnapshot(), catalogSize: unitCatalogById.size }, req);
      return;
    }
    sendJson(res, 410, { ok: false, error: 'Catalog store is SQLite only; JSON/SQLite toggles were removed.' }, req);
    return;
  }

  // /api/health
  if (parts.length === 2 && parts[0] === 'api' && parts[1] === 'health') {
    const mem = process.memoryUsage();
    const processStartedAt = stats.startedAt;
    const uptimeMs = processStartedAt ? Math.max(0, Date.now() - Date.parse(processStartedAt)) : null;
    if (ptacDayCountCache.date !== loadedDate || Date.now() - ptacDayCountCache.at > 30_000) {
      setImmediate(refreshPtacDayCounts);
    }
    sendJson(res, 200, {
      ok: true,
      mode: daemonMode,
      liveCachesReady,
      heavyReloadBusy,
      heavyReloadReason: heavyReloadReason || undefined,
      clientReadsAllowed: clientReadsAllowed(),
      clientReadyPolicy: process.env.DARWIN_CLIENT_READY_AFTER || 'restored',
      processStartedAt,
      uptimeMs,
      uptimeSec: uptimeMs != null ? Math.round(uptimeMs / 1000) : null,
      startedAt: processStartedAt,
      tiplocsIndexed: byTiploc.size,
      journeysLoaded: byRid.size,
      timetableFile: timetablePath.split('/').pop(),
      loadedDate,
      kafka: {
        ...stats,
        sinceProcessStart: true,
        processStartedAt,
      },
      overlaySize: {
        live: liveOverlayByRid.size,
        cancelled: cancelled.size,
        delayed: delayReason.size,
        formations: formationsByRid.size,
        stationsWithMessages: stationMessages.size,
        messages: messagesById.size,
        ridsWithAssociations: associationsByRid.size,
        ridsWithAlerts: alertsByRid.size,
        reverseFormation: reverseFormation.size,
        // PTAC (S506) consist & unit caches
        consists: consistByRid.size,
        units: unitsById.size,
        unmatchedConsists: unmatchedConsists.size,
      },
      ptac: {
        enabled: !!(ptacCfg.username && ptacCfg.groupId),
        topic: ptacCfg.topic,
        /** Distinct physical units with ≥1 service on `loadedDate` (today's timetable SSD). */
        unitsOnLoadedDate: ptacDayCountCache.units,
        /** RIDs on `loadedDate` that have a PTAC consist attached. */
        consistsOnLoadedDate: ptacDayCountCache.consists,
        replayAnchor: ptacCfg.replayAnchor,
        ...ptacStats,
      },
      memoryMB: { heap: +(mem.heapUsed/1024/1024).toFixed(1), rss: +(mem.rss/1024/1024).toFixed(1) },
      persistence: {
        stateFile: STATE_FILE,
        intervalSec: PERSIST_INTERVAL_SEC,
        lastPersistAt,
        dedupShards: STATE_HISTORY_DEDUP_SHARDS,
        fileSizeBytes: fileSizeBytes(STATE_FILE),
        stateFileBytes: fileSizeBytes(STATE_FILE),
      },
      unitCatalog: {
        file: UNIT_CATALOG_FILE,
        size: unitCatalogById.size,
        lastPersistAt: lastUnitCatalogPersistAt,
        fileSizeBytes: fileSizeBytes(UNIT_CATALOG_FILE),
        fileBytes: fileSizeBytes(UNIT_CATALOG_FILE),
        ...storeSnapshot(),
      },
      history: {
        dir: STATE_HISTORY_DIR,
        dates: availableHistoryDates().slice(0, 30),
        retentionDays: STATE_HISTORY_RETENTION_DAYS,
        pruneOnPersist: STATE_HISTORY_PRUNE_ON_PERSIST,
        cacheTuning: {
          timetableTtlMs: HIST_TIMETABLE_CACHE_TTL_MS,
          timetableMax: HIST_TIMETABLE_CACHE_MAX,
          contextTtlMs: HIST_CONTEXT_CACHE_TTL_MS,
          contextMax: HIST_CONTEXT_CACHE_MAX,
          stateFileTtlMs: HIST_STATE_FILE_CACHE_TTL_MS,
          stateFileMax: HIST_STATE_FILE_CACHE_MAX,
          snapshotListTtlMs: HIST_SNAPSHOT_LIST_CACHE_TTL_MS,
          snapshotListMax: HIST_SNAPSHOT_LIST_CACHE_MAX,
          departuresHttpMaxAgeSec: HIST_DEPARTURES_HTTP_MAX_AGE_SEC,
        },
      },
      warmup: {
        enabled: cfg.warmupEnabled,
        days: cfg.warmupDays,
        startedAt: warmupState.startedAt,
        finishedAt: warmupState.finishedAt,
        current: warmupState.current,
        done: warmupState.done.length,
        skipped: warmupState.skipped.length,
        errors: warmupState.errors.length,
        guardrails: { maxRssMb: cfg.warmupMaxRssMb, lagMs: cfg.warmupLagMs },
      },
      rawArchive: {
        enabled: cfg.rawArchiveEnabled,
        dir: cfg.rawArchiveDir,
        retentionDays: cfg.rawArchiveRetentionDays,
      },
      timetableWindow: {
        lookbackDays: LOOKBACK_TIMETABLE_DAYS,
        futureDaysCap: FUTURE_TIMETABLE_DAYS,
        minDate: addDaysIsoDate(loadedDate || railwayDayYmd(new Date()), -LOOKBACK_TIMETABLE_DAYS),
        maxDate: maxSupportedTimetableDate(loadedDate || railwayDayYmd(new Date())),
        ttis: {
          loaded: !!(ttisIndex?.schedules?.length),
          dateMin: ttisIndex?.dateMinIso || null,
          dateMax: ttisIndex?.dateMaxIso || null,
        },
      },
      departuresCacheMs: {
        live: cfg.departuresCacheMs,
        historical: cfg.departuresHistCacheMs,
      },
    }, req);
    return;
  }

  // /api/station/:code
  if (parts.length === 3 && parts[0] === 'api' && parts[1] === 'station') {
    const resolved = resolveStationCode(parts[2]);
    if (!resolved) { sendJson(res, 404, { error: `unknown station code "${parts[2]}"` }, req); return; }
    sendJson(res, 200, resolved, req);
    return;
  }

  // /api/departures/:code?hours=N
  if (parts.length === 3 && parts[0] === 'api' && parts[1] === 'departures') {
    const resolved = resolveStationCode(parts[2]);
    if (!resolved) { sendJson(res, 404, { error: `unknown station code "${parts[2]}"` }, req); return; }
    const hoursParam = url.searchParams.get('hours');
    const dateParam = url.searchParams.get('date');
    const atParam = url.searchParams.get('at');
    const hours = Math.max(0.25, Math.min(24, Number(hoursParam || cfg.windowHours)));
    if (dateParam && !isIsoDate(dateParam)) {
      sendJson(res, 400, { error: 'invalid date format', hint: 'use YYYY-MM-DD' }, req); return;
    }
    if (atParam && parseAtToMinutes(atParam) == null) {
      sendJson(res, 400, { error: 'invalid at format', hint: 'use HH:MM' }, req); return;
    }
    const loadedDay = loadedDate || railwayDayYmd(new Date());
    const wallInstant = dateParam
      ? londonWallInstantFromDateAt(dateParam, atParam || null, { anchorNoAtToLiveLondonClock: true })
      : null;
    const anchorDay = wallInstant ? railwayDayYmd(wallInstant) : null;
    const dateComparison = anchorDay ? compareIsoDate(anchorDay, loadedDay) : 0;
    const isHistorical = !!dateParam && dateComparison < 0;
    const isTimedCurrentDay = !!dateParam && dateComparison === 0 && !!atParam;
    const isFutureTimetable = !!dateParam && dateComparison > 0;
    if (isFutureTimetable) {
      const maxFutureDate = maxSupportedTimetableDate(loadedDay);
      if (compareIsoDate(anchorDay || dateParam, maxFutureDate) > 0) {
        sendJson(res, 400, { error: 'future date out of range', hint: `max supported future date is ${maxFutureDate}` }, req); return;
      }
    }
    const cacheDate = isLiveCacheQuery(dateParam, atParam) ? '' : (dateParam || '');
    const cacheAt = isLiveCacheQuery(dateParam, atParam) ? '' : (atParam || '');
    const cacheKey = `${resolved.tiplocs.join(',')}|${hours}|${cacheDate}|${cacheAt}`;
    const histHttpOpts = isHistorical && HIST_DEPARTURES_HTTP_MAX_AGE_SEC > 0
      ? { cacheControl: `private, max-age=${HIST_DEPARTURES_HTTP_MAX_AGE_SEC}, stale-while-revalidate=600` }
      : {};
    const cached = getCachedSnapshot(cacheKey, true);
    if (cached) {
      sendJson(res, 200, cached, req, histHttpOpts);
      if (!dateParam && snapshotIsStale(cacheKey)) {
        setImmediate(() => {
          try {
            const fresh = buildSnapshot(resolved.tiplocs, hours, resolved.tiploc, null);
            if (!fresh) return;
            fresh.stationName = resolved.name || fresh.stationName;
            fresh.stationCrs = resolved.crs || fresh.stationCrs;
            fresh.matchedAs = resolved.matchedAs;
            if (resolved.alternates) fresh.alternates = resolved.alternates;
            putCachedSnapshot(cacheKey, fresh, cfg.departuresCacheMs);
          } catch {}
        });
      }
      return;
    }
    // Pass the FULL set of TIPLOCs that share this CRS so a station with
    // multiple platform groups (St Pancras, Edinburgh, etc.) returns all
    // its departures, not just one platform group.
    const loadDate = anchorDay || dateParam;
    let queryCtx = null;
    if (isHistorical) {
      queryCtx = await getHistoricalContext(loadDate, atParam);
      if (!queryCtx && compareIsoDate(loadDate, addDaysIsoDate(loadedDay, -LOOKBACK_TIMETABLE_DAYS)) >= 0) {
        queryCtx = await getTimetableOnlyContext(loadDate, atParam);
      }
      if (!queryCtx) {
        sendJson(res, 404, { error: `no historical data for ${loadDate}` }, req); return;
      }
    } else if (isTimedCurrentDay) {
      queryCtx = getLiveTimedContext(loadDate, atParam);
      if (!queryCtx) {
        sendJson(res, 404, { error: `no live context for ${loadDate}` }, req); return;
      }
    } else if (isFutureTimetable) {
      queryCtx = await getTimetableOnlyContext(loadDate, atParam);
      if (!queryCtx) {
        sendJson(res, 404, { error: `no timetable data for ${loadDate}` }, req); return;
      }
    }
    if (queryCtx && dateParam) queryCtx.boardWallDate = dateParam;
    const snap = buildSnapshot(resolved.tiplocs, hours, resolved.tiploc, queryCtx);
    if (!snap) { sendJson(res, 404, { error: `no services indexed for ${resolved.tiploc}` }, req); return; }
    snap.stationName = resolved.name || snap.stationName;
    snap.stationCrs  = resolved.crs  || snap.stationCrs;
    snap.matchedAs   = resolved.matchedAs;
    if (resolved.alternates) snap.alternates = resolved.alternates;
    const histTtl = isHistorical ? cfg.departuresHistCacheMs : cfg.departuresCacheMs;
    putCachedSnapshot(cacheKey, snap, histTtl);
    sendJson(res, 200, snap, req, histHttpOpts);
    return;
  }

  // /api/messages/:crs — currently-known NRCC station messages for that CRS.
  // Falls through to 200 with empty list if the CRS is unknown so the UI can
  // call this freely without 404 noise.
  if (parts.length === 3 && parts[0] === 'api' && parts[1] === 'messages') {
    const crs = parts[2].toUpperCase();
    const messages = listMessagesForCrs(crs);
    sendJson(res, 200, { crs, messages, count: messages.length, updatedAt: new Date().toISOString() }, req);
    return;
  }

  // /api/service/:rid
  if (parts.length === 3 && parts[0] === 'api' && parts[1] === 'service') {
    const dateParam = url.searchParams.get('date');
    const atParam = url.searchParams.get('at');
    const rid = parts[2];
    const liveQuery = isLiveCacheQuery(dateParam, atParam);
    const svcKey = serviceDetailCacheKey(rid, liveQuery ? '' : (dateParam || ''), liveQuery ? '' : (atParam || ''));
    const cachedDetail = getCachedServiceDetail(svcKey, true);
    if (cachedDetail) {
      sendJson(res, 200, cachedDetail, req);
      if (!dateParam && serviceDetailIsStale(svcKey)) {
        setImmediate(() => {
          try {
            const fresh = buildServiceDetail(rid);
            if (fresh) putCachedServiceDetail(svcKey, fresh, cfg.departuresCacheMs);
          } catch {}
        });
      }
      return;
    }
    if (dateParam && (dateParam !== loadedDate || !!atParam)) {
      if (!isIsoDate(dateParam)) {
        sendJson(res, 400, { error: 'invalid date format', hint: 'use YYYY-MM-DD' }, req);
        return;
      }
      if (atParam && parseAtToMinutes(atParam) == null) {
        sendJson(res, 400, { error: 'invalid at format', hint: 'use HH:MM' }, req);
        return;
      }
      const liveDay = loadedDate || railwayDayYmd(new Date());
      const wallInstant = londonWallInstantFromDateAt(dateParam, atParam || null, { anchorNoAtToLiveLondonClock: true });
      const anchorDay = wallInstant ? railwayDayYmd(wallInstant) : dateParam;
      const dayCmp = compareIsoDate(anchorDay, liveDay);
      if (dayCmp > 0 && compareIsoDate(anchorDay, maxSupportedTimetableDate(liveDay)) > 0) {
        sendJson(res, 400, {
          error: 'future date out of range',
          hint: `max supported future date is ${maxSupportedTimetableDate(liveDay)}`,
        }, req);
        return;
      }
      const isHistorical = dayCmp < 0;
      const isTimedCurrentDay = dayCmp === 0 && !!atParam;
      let histCtx = null;
      if (isHistorical) {
        histCtx = await getHistoricalContext(anchorDay, atParam);
        if (!histCtx && compareIsoDate(anchorDay, addDaysIsoDate(liveDay, -LOOKBACK_TIMETABLE_DAYS)) >= 0) {
          histCtx = await getTimetableOnlyContext(anchorDay, atParam);
        }
      } else if (isTimedCurrentDay) {
        histCtx = getLiveTimedContext(anchorDay, atParam) || await getHistoricalContext(anchorDay, atParam);
      } else {
        histCtx = await getTimetableOnlyContext(anchorDay, atParam);
      }
      if (!histCtx) {
        sendJson(res, 404, { error: `no historical data for ${dateParam}` }, req);
        return;
      }
      // Timed views for the *current* timetable day still build overlays from the
      // persisted snapshot, but Darwin formations + PTAC consists stream in live
      // and may not be present in that snapshot (persist can skip or fail when
      // the JSON payload is huge). Use in-memory caches for coach/consist data.
      const detailCtx =
        anchorDay === liveDay
          ? { ...histCtx, formationsByRid, consistByRid }
          : { ...histCtx, consistByRid };
      const hist = buildServiceDetail(parts[2], detailCtx);
      if (!hist) {
        sendJson(res, 404, { error: `rid not found for ${dateParam}: "${parts[2]}"` }, req);
        return;
      }
      const payload = { ...hist, historicalDate: dateParam, historicalSavedAt: histCtx.stateSavedAt || null, historicalAt: atParam || null };
      putCachedServiceDetail(svcKey, payload, Math.max(cfg.departuresHistCacheMs, 6 * 60 * 60_000));
      const histHttpOpts = isHistorical && HIST_DEPARTURES_HTTP_MAX_AGE_SEC > 0
        ? { cacheControl: `private, max-age=${HIST_DEPARTURES_HTTP_MAX_AGE_SEC}, stale-while-revalidate=600` }
        : {};
      sendJson(res, 200, payload, req, histHttpOpts);
      return;
    }
    const detail = buildServiceDetail(parts[2]);
    if (!detail) { sendJson(res, 404, { error: `rid not found: "${parts[2]}"` }, req); return; }
    putCachedServiceDetail(svcKey, detail);
    sendJson(res, 200, detail, req);
    return;
  }

  // /api/unit/:resourceGroupId — physical unit detail + day's diagram
  if (parts.length === 3 && parts[0] === 'api' && parts[1] === 'unit') {
    const unit = unitsById.get(parts[2]);
    if (!unit) { sendJson(res, 404, { error: `unit not seen today: "${parts[2]}"` }, req); return; }
    // Enrich each service entry with friendly origin/destination names.
    const services = (unit.services || []).map((s) => ({
      ...s,
      startName: s.startTpl ? resolve_.tiplocToName(s.startTpl) : null,
      endName:   s.endTpl   ? resolve_.tiplocToName(s.endTpl)   : null,
    }));
    sendJson(res, 200, { ...unit, services, updatedAt: new Date().toISOString() }, req);
    return;
  }

  // /api/units/catalog?fleet=158
  if (parts.length === 3 && parts[0] === 'api' && parts[1] === 'units' && parts[2] === 'catalog') {
    const fleetFilter = (url.searchParams.get('fleet') || '').trim().toUpperCase();
    sendJson(res, 200, getUnitsCatalogPayload(fleetFilter), req);
    return;
  }

  // /api/history/dates
  if (parts.length === 3 && parts[0] === 'api' && parts[1] === 'history' && parts[2] === 'dates') {
    const includeSnapshots = url.searchParams.get('snapshots') === '1';
    if (!includeSnapshots) {
      sendJson(res, 200, getHistoryDatesLitePayload(), req);
      return;
    }
    const dates = knownBoardDates().map((d) => ({
      date: d,
      hasState: historyDayHasState(d),
      hasTimetable: !!pickTimetableForDate(d),
      snapshots: listHistorySnapshotsForDate(d).map((s) => s.savedAt).filter(Boolean).slice(-24),
    }));
    sendJson(res, 200, {
      count: dates.length,
      retentionDays: STATE_HISTORY_RETENTION_DAYS,
      pruneOnPersist: STATE_HISTORY_PRUNE_ON_PERSIST,
      dates,
      updatedAt: new Date().toISOString(),
    }, req);
    return;
  }

  // /api/history/overlay-series?hours=36
  if (parts.length === 3 && parts[0] === 'api' && parts[1] === 'history' && parts[2] === 'overlay-series') {
    const hoursParam = Number(url.searchParams.get('hours') || '36');
    sendJson(res, 200, buildOverlayHistorySeries(hoursParam), req);
    return;
  }

  // POST /api/plan/bash
  if (isPlanBash) {
    let body;
    try {
      body = await readJsonBody(req);
    } catch (e) {
      sendJson(res, e.status === 413 ? 413 : 400, { error: e.message || 'invalid body' }, req);
      return;
    }
    const dateParam = body.date ? String(body.date).trim() : '';
    const atParam = body.at ? String(body.at).trim() : '';
    if (dateParam && !isIsoDate(dateParam)) {
      sendJson(res, 400, { error: 'invalid date format', hint: 'use YYYY-MM-DD' }, req);
      return;
    }
    const atMin = parseAtToMinutes(atParam);
    if (atMin == null) {
      sendJson(res, 400, { error: 'invalid at format', hint: 'use HH:MM' }, req);
      return;
    }
    const visit = Array.isArray(body.visit) ? body.visit : [];
    const loadedDay = loadedDate || railwayDayYmd(new Date());
    const wallInstant = dateParam
      ? londonWallInstantFromDateAt(dateParam, atParam, { anchorNoAtToLiveLondonClock: true })
      : null;
    const anchorDay = wallInstant ? railwayDayYmd(wallInstant) : loadedDay;
    const dateComparison = compareIsoDate(anchorDay, loadedDay);
    const isHistorical = !!dateParam && dateComparison < 0;
    const isTimedCurrentDay = !dateParam || dateComparison === 0;
    const isFutureTimetable = !!dateParam && dateComparison > 0;
    if (isFutureTimetable) {
      const maxFutureDate = maxSupportedTimetableDate(loadedDay);
      if (compareIsoDate(anchorDay, maxFutureDate) > 0) {
        sendJson(res, 400, { error: 'future date out of range', hint: `max supported future date is ${maxFutureDate}` }, req);
        return;
      }
    }
    const loadDate = anchorDay || dateParam || loadedDay;
    let queryCtx = null;
    if (isHistorical) {
      queryCtx = await getHistoricalContext(loadDate, atParam);
      if (!queryCtx && compareIsoDate(loadDate, addDaysIsoDate(loadedDay, -LOOKBACK_TIMETABLE_DAYS)) >= 0) {
        queryCtx = await getTimetableOnlyContext(loadDate, atParam);
      }
      if (!queryCtx) {
        sendJson(res, 404, { error: `no historical data for ${loadDate}` }, req);
        return;
      }
    } else if (isFutureTimetable) {
      queryCtx = await getTimetableOnlyContext(loadDate, atParam);
      if (!queryCtx) {
        sendJson(res, 404, { error: `no timetable data for ${loadDate}` }, req);
        return;
      }
    } else if (isTimedCurrentDay) {
      queryCtx = getLiveTimedContext(loadDate, atParam) || {
        byRid,
        cancelled,
      };
    }
    const byRidMap = queryCtx?.byRid || byRid;
    const cancelledMap = queryCtx?.cancelled || cancelled;
    if (!byRidMap?.size) {
      sendJson(res, 503, { error: 'timetable not loaded' }, req);
      return;
    }
    const index = getPlannerIndex(byRidMap, loadDate);
    const result = planBashItinerary({
      index,
      startCrs: body.start,
      endCrs: body.end,
      visitCrs: visit,
      atMin,
      resolveCrs: resolveStationCode,
      tplToCrs: plannerTplToCrs,
      cancelled: cancelledMap,
      nameOf: nameForCrs,
    });
    if (result.ok) attachBashBoardCards(result, byRidMap, queryCtx, loadDate);
    sendJson(res, result.ok ? 200 : 400, { ...result, date: loadDate }, req);
    return;
  }

  sendJson(res, 404, { error: 'not found', hint: 'try /api/health, /api/ping, /api/station/:code, /api/departures/:code, /api/messages/:crs, /api/service/:rid?date=YYYY-MM-DD, /api/unit/:id, /api/units/catalog, /api/history/dates, /api/history/overlay-series, POST /api/plan/bash' }, req);
}

const server = createServer((req, res) => {
  Promise.resolve(handleRequest(req, res)).catch((e) => {
    console.error('[daemon] handleRequest error:', e);
    try {
      if (!res.headersSent) sendJson(res, 500, { error: 'internal error' }, req);
      else res.destroy();
    } catch {
      /* ignore */
    }
  });
});

// ---------- Kafka loop -----------------------------------------------------
/** KafkaJS 2.2.4: empty queue still `setTimeout(..., throttledUntil - Date.now())` with throttledUntil=0 → TimeoutNegativeWarning. */
function patchKafkaJsNegativeTimeouts() {
  try {
    const require = createRequire(import.meta.url);
    const RequestQueue = require('kafkajs/src/network/requestQueue');
    const proto = RequestQueue?.prototype;
    if (!proto || typeof proto.scheduleCheckPendingRequests !== 'function') return;
    if (proto.scheduleCheckPendingRequests.__rsPatched) return;
    proto.scheduleCheckPendingRequests = function scheduleCheckPendingRequestsPatched() {
      if (this.throttleCheckTimeoutId) return;
      const remainingThrottle = this.throttledUntil - Date.now();
      if (this.pending.length === 0 && remainingThrottle <= 0) return;
      const scheduleAt = remainingThrottle > 0 ? remainingThrottle : 10;
      this.throttleCheckTimeoutId = setTimeout(() => {
        this.throttleCheckTimeoutId = null;
        this.checkPendingRequests();
      }, scheduleAt);
    };
    proto.scheduleCheckPendingRequests.__rsPatched = true;
  } catch {
    /* kafkajs layout changed — warning may still appear */
  }
}
patchKafkaJsNegativeTimeouts();

/** Shared KafkaJS tuning — Confluent often resets TLS mid-handshake under burst connects at startup; retries recover. */
const kafkaJsTimeouts = {
  connectionTimeout: Math.max(1000, Number(process.env.KAFKA_CONNECTION_TIMEOUT_MS || 30000) || 30000),
  authenticationTimeout: Math.max(1000, Number(process.env.KAFKA_AUTH_TIMEOUT_MS || 30000) || 30000),
  requestTimeout: Math.max(1000, Number(process.env.KAFKA_REQUEST_TIMEOUT_MS || 30000) || 30000),
};
const kafkaJsRetry = {
  retries: Number(process.env.KAFKA_RETRY_COUNT || 12),
  initialRetryTime: Number(process.env.KAFKA_RETRY_INITIAL_MS || 400),
  maxRetryTime: Number(process.env.KAFKA_RETRY_MAX_MS || 60000),
  multiplier: 2,
};

const kafka = new Kafka({
  clientId: 'rs-departures-daemon',
  brokers: [cfg.bootstrap], ssl: true,
  sasl: { mechanism: 'plain', username: cfg.username, password: cfg.password },
  ...kafkaJsTimeouts,
  retry: kafkaJsRetry,
  logLevel: logLevel.WARN,
});
/** Longer session tolerates main-thread stalls (timetable parse, persist stringify) before "coordinator is not aware of this member". */
const darwinSessionTimeout = Math.max(10000, Number(process.env.DARWIN_SESSION_TIMEOUT_MS || 90000));
const darwinHeartbeatRaw = Number(process.env.DARWIN_HEARTBEAT_INTERVAL_MS || 10000);
const darwinHeartbeatInterval = Math.min(
  Math.max(3000, darwinHeartbeatRaw),
  Math.floor(darwinSessionTimeout / 3) - 500,
);
const darwinRebalanceTimeout = Math.max(10000, Number(process.env.DARWIN_REBALANCE_TIMEOUT_MS || 90000));
const consumer = kafka.consumer({
  groupId: cfg.groupId,
  sessionTimeout: darwinSessionTimeout,
  heartbeatInterval: darwinHeartbeatInterval,
  rebalanceTimeout: darwinRebalanceTimeout,
});

// PTAC consumer — separate Kafka client (different SASL credentials) on the
// same Confluent cluster. Created lazily inside startPtacConsumer() so it
// is silently skipped when the PTAC creds aren't set in .env.
let ptacKafka = null;
let ptacConsumer = null;

let shuttingDown = false;
async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log('\n[daemon] shutting down ...');
  try { server.close(); } catch {}
  try { await consumer.disconnect(); } catch {}
  if (ptacConsumer) try { await ptacConsumer.disconnect(); } catch {}
  // Final flush so the latest accumulated state survives the restart.
  console.log('[daemon] persisting final state ...');
  await persistState({ force: true });
  persistUnitCatalog();
  console.log('[daemon] bye.');
  process.exit(0);
}
process.on('SIGINT',  shutdown);
process.on('SIGTERM', shutdown);

/**
 * Spin up the PTAC consumer alongside Darwin. Silent no-op if PTAC creds
 * are missing — Darwin still works fine without it. Failures here log a
 * warning but don't crash the process; the daemon should keep serving
 * Darwin data even if Network Rail's feed is unreachable.
 */
async function startPtacConsumer() {
  if (!ptacCfg.username || !ptacCfg.password || !ptacCfg.groupId) {
    console.log('[ptac] disabled: missing PTAC_USERNAME / PTAC_PASSWORD / PTAC_GROUP_ID in .env');
    return;
  }
  ptacStats.startedAt = new Date().toISOString();
  const ptacConnectDelayMs = Number(process.env.PTAC_CONNECT_DELAY_MS || 2500);
  if (ptacConnectDelayMs > 0) {
    await new Promise((r) => setTimeout(r, ptacConnectDelayMs));
  }
  ptacKafka = new Kafka({
    clientId: 'rs-departures-daemon-ptac',
    brokers:  [ptacCfg.bootstrap], ssl: true,
    sasl:     { mechanism: 'plain', username: ptacCfg.username, password: ptacCfg.password },
    ...kafkaJsTimeouts,
    retry: kafkaJsRetry,
    logLevel: logLevel.WARN,
  });
  const ptacSessionTimeout = Math.max(10000, Number(process.env.PTAC_SESSION_TIMEOUT_MS || 120000));
  const ptacHeartbeatRaw = Number(process.env.PTAC_HEARTBEAT_INTERVAL_MS || 10000);
  const ptacHeartbeatInterval = Math.min(
    Math.max(3000, ptacHeartbeatRaw),
    Math.floor(ptacSessionTimeout / 3) - 500,
  );
  const ptacRebalanceTimeout = Math.max(10000, Number(process.env.PTAC_REBALANCE_TIMEOUT_MS || 90000));
  ptacConsumer = ptacKafka.consumer({
    groupId: ptacCfg.groupId,
    sessionTimeout: ptacSessionTimeout,
    heartbeatInterval: ptacHeartbeatInterval,
    rebalanceTimeout: ptacRebalanceTimeout,
  });

  try {
    await ptacConsumer.connect();
    await ptacConsumer.subscribe({ topic: ptacCfg.topic, fromBeginning: false });
  } catch (e) {
    console.warn(`[ptac] connect failed: ${e.message} — feed disabled.`);
    ptacConsumer = null;
    return;
  }

  // Replay window — rolling minutes from now, or SSD-calendar 00:01 London for backfill (PTAC_REPLAY_ANCHOR).
  let replayOffsets = null;
  let sinceMs = null;
  let replayLabel = '';
  if (ptacCfg.replayAnchor === 'ssd_0001') {
    const ssd = railwayDayYmd(new Date());
    sinceMs = ssdLondonCalendar001UtcMs(ssd);
    replayLabel = `anchor=ssd_0001 SSD=${ssd} since=${new Date(sinceMs).toISOString()}`;
  } else if (ptacCfg.initialReplay > 0) {
    sinceMs = Date.now() - ptacCfg.initialReplay * 60_000;
    replayLabel = `anchor=rolling last ${ptacCfg.initialReplay} min since=${new Date(sinceMs).toISOString()}`;
  }
  if (sinceMs != null) {
    const admin = ptacKafka.admin();
    try {
      await admin.connect();
      replayOffsets = await admin.fetchTopicOffsetsByTimestamp(ptacCfg.topic, sinceMs);
      console.log(`[ptac] will replay (${replayLabel}) across ${replayOffsets.length} partition(s).`);
    } catch (e) { console.warn(`[ptac] offset fetch failed: ${e.message}`); }
    finally { await admin.disconnect(); }
  }

  let pendingReplaySeek = replayOffsets;
  ptacConsumer.on(ptacConsumer.events.GROUP_JOIN, () => {
    if (!pendingReplaySeek?.length) return;
    const toSeek = pendingReplaySeek;
    pendingReplaySeek = null;
    setImmediate(() => {
      for (const o of toSeek) {
        try {
          ptacConsumer.seek({ topic: ptacCfg.topic, partition: o.partition, offset: o.offset });
        } catch (e) {
          console.warn(`[ptac] seek p${o.partition} failed: ${e.message}`);
        }
      }
      console.log('[ptac] startup replay seek applied.');
    });
  });

  ptacConsumer.run({
    eachMessage: async ({ message }) => {
      archiveRawFeed('ptac', message.value);
      try { processConsistMessage(message.value); }
      catch (e) { /* don't die on one bad message */ ptacStats.errors++; }
    },
  }).catch((e) => {
    console.error('[ptac] consumer.run error:', e);
    // Don't shutdown — keep Darwin running even if PTAC dies.
    ptacConsumer = null;
  });

  console.log('[ptac] consumer running.');
}

function logBoot(step, detail = '') {
  console.log(`[boot] ${step}${detail ? ` — ${detail}` : ''}`);
}

function listenHttp() {
  return new Promise((resolveListen, reject) => {
    server.once('error', reject);
    server.listen(cfg.port, cfg.host, () => {
      server.removeListener('error', reject);
      if (daemonMode === 'cold_starting') daemonMode = 'live_ready';
      resolveListen();
    });
  });
}

async function startDarwinFeed() {
  await consumer.connect();
  await consumer.subscribe({ topic: cfg.topic, fromBeginning: false });

  let replayOffsets = null;
  if (cfg.initialReplay > 0) {
    const admin = kafka.admin();
    await admin.connect();
    try {
      const sinceMs = Date.now() - cfg.initialReplay * 60_000;
      replayOffsets = await admin.fetchTopicOffsetsByTimestamp(cfg.topic, sinceMs);
      console.log(`[darwin] will replay last ${cfg.initialReplay} min (${replayOffsets.length} partition(s)).`);
    } catch (e) {
      console.warn(`[darwin] offset fetch failed: ${e.message}`);
    } finally {
      await admin.disconnect();
    }
  }

  let pendingReplaySeek = replayOffsets;
  const joined = new Promise((resolve) => {
    const timeout = setTimeout(() => {
      console.warn('[darwin] GROUP_JOIN wait timed out — continuing boot');
      resolve();
    }, 45_000);
    consumer.on(consumer.events.GROUP_JOIN, () => {
      clearTimeout(timeout);
      if (pendingReplaySeek?.length) {
        const toSeek = pendingReplaySeek;
        pendingReplaySeek = null;
        for (const o of toSeek) {
          try { consumer.seek({ topic: cfg.topic, partition: o.partition, offset: o.offset }); }
          catch (e) { console.warn(`[darwin] seek p${o.partition} failed: ${e.message}`); }
        }
        console.log('[darwin] startup replay seek applied.');
      }
      resolve();
    });
  });

  consumer.run({
    eachMessage: async ({ message }) => {
      stats.consumed++;
      archiveRawFeed('darwin', message.value);
      let inner;
      try { inner = decodeKafkaJson(message.value); } catch { return; }
      const pport = inner.Pport || inner;
      try { processMessage(pport); } catch { /* don't die on one bad message */ }
    },
  }).catch((e) => {
    console.error('[darwin] consumer.run error:', e);
    shutdown();
  });
  await joined;
}

function startMaintenanceTimers() {
  setInterval(() => {
    const mem = process.memoryUsage();
    console.log(
      `[daemon] heartbeat: darwin consumed=${stats.consumed} updates=${stats.updates} live=${liveOverlayByRid.size} cancelled=${cancelled.size} formations=${formationsByRid.size}`
      + ` | ptac consumed=${ptacStats.consumed} matched=${ptacStats.matched} unmatched=${unmatchedConsists.size} consists=${consistByRid.size} units=${unitsById.size}`
      + ` | heap=${(mem.heapUsed/1024/1024).toFixed(0)}MB`
    );
  }, cfg.heartbeat * 1000);

  setInterval(() => {
    persistState().catch((e) => console.warn(`[daemon] persist failed: ${e.message}`));
  }, PERSIST_INTERVAL_SEC * 1000);
  setInterval(persistUnitCatalog, PERSIST_INTERVAL_SEC * 1000);

  setInterval(() => {
    const t = railwayDayYmd(new Date());
    if (t === loadedDate) {
      awaitingTimetableYmd = null;
      return;
    }
    if (pickTimetableForDate(t)) {
      console.log(`[daemon] day rollover detected (${loadedDate} → ${t}); loading timetable, keeping PTAC/units from the 02:00 Kafka window`);
      awaitingTimetableYmd = null;
      reloadAllDataAndResetLive('day rollover').catch((e) => {
        console.warn(`[daemon] day rollover timetable reload failed: ${e.message}`);
      });
      return;
    }
    if (awaitingTimetableYmd !== t) {
      awaitingTimetableYmd = t;
      console.log(
        `[daemon] railway day is now ${t} but today’s PPTimetable is not on disk yet `
        + `(files usually land ~04:00–04:30). Keeping the previous timetable. `
        + 'PTAC unit data continues on Kafka (~02:00); unmatched consists will join after the 04:00 fetch.'
      );
    }
  }, cfg.dayRolloverCheckSec * 1000);

  setInterval(() => {
    maybeRunScheduledAutoFetch().catch((e) => {
      console.warn(`[daemon] scheduled auto-fetch failed: ${e.message}`);
    });
  }, cfg.scheduledFetchTickSec * 1000);
}

async function start() {
  const bootStarted = Date.now();
  daemonMode = 'cold_starting';
  logBoot('start');

  const today = railwayDayYmd(new Date());
  if (cfg.autoFetchFiles && !pickTimetableForDate(today)) {
    logBoot('fetch', `no timetable for ${today}`);
    await runDailyFileFetch('startup');
  } else if (cfg.autoFetchFiles) {
    logBoot('fetch', 'skipped (today already on disk)');
  } else {
    logBoot('fetch', 'disabled');
  }

  logBoot('timetable');
  await reloadReferenceData();

  const opened = await createStateSqlite(SQLITE_PATH);
  if (opened.ok) {
    sqliteHandle = opened.handle;
    logBoot('sqlite', SQLITE_PATH);
  } else {
    sqliteInitError = opened.error;
    console.warn(`[daemon] sqlite unavailable (${opened.error})`);
  }
  persistStoreOverride();

  const persistedRaw = readPersistedStateRawIfFresh();
  if (persistedRaw?.lastAutoFetchRunYmd && /^\d{8}$/.test(String(persistedRaw.lastAutoFetchRunYmd))) {
    lastAutoFetchRunYmd = String(persistedRaw.lastAutoFetchRunYmd);
  }
  logBoot('restore', 'live overlay');
  applyPersistedStateLive(persistedRaw);
  logBoot('restore', 'formations / PTAC / messages');
  applyPersistedStateRest(persistedRaw);
  loadUnitCatalog();
  for (const unit of unitsById.values()) mergeUnitIntoCatalog(unit);
  setImmediate(() => {
    try { persistUnitCatalog(); } catch (e) {
      console.warn(`[daemon] initial catalog persist failed: ${e.message}`);
    }
  });

  liveCachesReady = true;
  daemonMode = 'live_ready';
  startMaintenanceTimers();
  await listenHttp();
  scheduleHotBoardTick();
  logBoot('ready', `${((Date.now() - bootStarted) / 1000).toFixed(1)}s — accepting connections on :${cfg.port} (history warmup in background)`);

  logBoot('feeds', 'Darwin + PTAC');
  try {
    await startDarwinFeed();
  } catch (e) {
    console.error('[boot] Darwin feed failed:', e.message);
  }
  try {
    await startPtacConsumer();
  } catch (e) {
    console.warn('[ptac] start failed:', e.message);
  }

  logBoot('prime', `past ${cfg.warmupDays} days + future ${cfg.warmupFutureDays} days`);
  try {
    await runHorizonWarmup();
  } catch (e) {
    console.warn(`[boot] horizon warmup failed: ${e.message}`);
    daemonMode = 'fully_warm';
  }
  logBoot('prime', 'today + ±7 day boards and service details');
  try {
    await runLiveHotWarmup();
  } catch (e) {
    console.warn(`[boot] live hot warmup failed: ${e.message}`);
  }
  if (daemonMode !== 'fully_warm') daemonMode = 'fully_warm';
  logBoot('ready', `${((Date.now() - bootStarted) / 1000).toFixed(1)}s — fully booted`);
  console.log(
    `[daemon] overnight: railway day 02:00 Europe/London. `
    + `PTAC unit/formation data often arrives 12–48h before that working day (kept in persist + unmatched queue; join index covers the next ${cfg.ptacAheadDays} days). `
    + `Timetable GCS pull ${cfg.autoFetchTime} then retry for ${cfg.autoFetchGraceMin}m (files usually ~04:00–04:30). `
    + 'Keep this process awake — macOS sleep pauses Kafka and the 04:00 fetch.'
  );
}

if (process.argv.includes('--convert-history-blobs')) {
  const skipArg = process.argv.find((a) => a.startsWith('--skip-recent-ms='));
  const skipRecentMs = skipArg ? Math.max(0, Number(skipArg.split('=')[1])) : 120_000;
  try {
    const summary = convertAllHistoryHeavyShardsToBlobs({ skipRecentMs });
    console.log(`[persist] convert-history-blobs finished: ${JSON.stringify(summary)}`);
    process.exit(0);
  } catch (e) {
    console.error('[persist] convert-history-blobs failed:', e);
    process.exit(1);
  }
} else {
  start().catch((e) => { console.error('[daemon] fatal:', e); process.exit(1); });
}
