#!/usr/bin/env node
/**
 * Attach to the running departures-daemon, pause inside persistState(),
 * and write RAM maps to state/emergency-live-* as gzip parts.
 */
import { writeFileSync, mkdirSync } from 'node:fs';

const PID = Number(process.argv[2] || process.env.DAEMON_PID);
const OUT = process.argv[3];
if (!PID || !OUT) {
  console.error('usage: node dump-live-from-inspector.mjs <pid> <out-dir>');
  process.exit(1);
}

async function fetchJson(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url} ${r.status}`);
  return r.json();
}

async function waitInspector(timeoutMs = 15000) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    for (const port of [9229, 9230, 9231, 9232]) {
      try {
        const list = await fetchJson(`http://127.0.0.1:${port}/json/list`);
        const target = (list || []).find((t) => t.webSocketDebuggerUrl)
          || null;
        if (target?.webSocketDebuggerUrl) return { port, target };
      } catch {}
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error('inspector not listening on 9229-9232');
}

function cdp(ws) {
  let id = 0;
  const pending = new Map();
  const handlers = new Map();
  ws.addEventListener('message', (ev) => {
    const msg = JSON.parse(ev.data);
    if (msg.id && pending.has(msg.id)) {
      const { resolve, reject } = pending.get(msg.id);
      pending.delete(msg.id);
      if (msg.error) reject(new Error(msg.error.message || JSON.stringify(msg.error)));
      else resolve(msg.result);
      return;
    }
    if (msg.method && handlers.has(msg.method)) {
      for (const fn of handlers.get(msg.method)) fn(msg.params);
    }
  });
  return {
    send(method, params) {
      const thisId = ++id;
      return new Promise((resolve, reject) => {
        pending.set(thisId, { resolve, reject });
        ws.send(JSON.stringify({ id: thisId, method, params }));
      });
    },
    on(method, fn) {
      if (!handlers.has(method)) handlers.set(method, []);
      handlers.get(method).push(fn);
    },
  };
}

const DUMP_EXPR = `(function () {
  const fs = process.getBuiltinModule('fs');
  const zlib = process.getBuiltinModule('zlib');
  const path = process.getBuiltinModule('path');
  const outDir = ${JSON.stringify(OUT)};
  fs.mkdirSync(outDir, { recursive: true });
  const MAX = 80 * 1024 * 1024;
  function writeParts(label, arr) {
    const items = Array.isArray(arr) ? arr : [];
    let part = 0;
    let pieces = [];
    let chars = 2;
    const flush = () => {
      const json = '[' + pieces.join(',') + ']';
      fs.writeFileSync(path.join(outDir, label + '.' + String(part).padStart(3, '0') + '.json.gz'), zlib.gzipSync(json));
      part += 1;
      pieces = [];
      chars = 2;
    };
    for (const item of items) {
      const s = JSON.stringify(item);
      const piece = s === undefined ? 'null' : s;
      const extra = (pieces.length ? 1 : 0) + piece.length;
      if (pieces.length && chars + extra > MAX) flush();
      pieces.push(piece);
      chars += extra;
    }
    flush();
    return part;
  }
  const overlay = serializeLiveOverlayEntries();
  const core = {
    savedAt: new Date().toISOString(),
    savedDate: loadedDate,
    cancelled: [...cancelled.entries()],
    delayReason: [...delayReason.entries()],
    messagesById: [...messagesById.entries()],
    stationMessages: [...stationMessages.entries()].map(([k, v]) => [k, [...v]]),
    associations: [...associationsByRid.entries()],
    alerts: [...alertsByRid.entries()],
    reverseFormation: [...reverseFormation],
    unmatchedConsists: [...unmatchedConsists.entries()],
    stateSchema: 2,
    emergencyDump: true,
  };
  fs.writeFileSync(path.join(outDir, 'daemon-cache.json'), JSON.stringify(core));
  const counts = {
    overlay: overlay.length,
    overlayParts: writeParts('overlay', overlay),
    formations: formationsByRid.size,
    formationParts: writeParts('formations', [...formationsByRid.entries()]),
    consist: consistByRid.size,
    consistParts: writeParts('consist', [...consistByRid.entries()]),
    units: unitsById.size,
    unitParts: writeParts('units', [...unitsById.entries()]),
  };
  fs.writeFileSync(path.join(outDir, 'manifest.json'), JSON.stringify(counts, null, 2));
  return counts;
})()`;

const { port, target } = await (async () => {
  try {
    return await waitInspector(800);
  } catch {
    process.kill(PID, 'SIGUSR1');
    console.log(`[dump] sent SIGUSR1 to ${PID}`);
    return await waitInspector(20000);
  }
})();

console.log(`[dump] inspector on :${port} ${target.webSocketDebuggerUrl}`);
const ws = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((resolve, reject) => {
  ws.addEventListener('open', resolve);
  ws.addEventListener('error', reject);
});
const api = cdp(ws);
const scripts = [];
api.on('Debugger.scriptParsed', (p) => scripts.push(p));
let pausedResolve;
const pausedP = new Promise((r) => { pausedResolve = r; });
api.on('Debugger.paused', (params) => pausedResolve(params));
await api.send('Debugger.enable');
await api.send('Runtime.enable');

mkdirSync(OUT, { recursive: true });
writeFileSync(`${OUT}/.inspector-started`, new Date().toISOString());

await new Promise((r) => setTimeout(r, 800));
const daemonScript = scripts.find((s) => String(s.url || '').includes('departures-daemon.mjs'));
console.log('[dump] daemon script', daemonScript?.scriptId, daemonScript?.url);

if (daemonScript?.scriptId) {
  const src = await api.send('Debugger.getScriptSource', { scriptId: daemonScript.scriptId });
  const text = src.scriptSource || '';
  const lines = text.split('\n');
  const hits = [];
  lines.forEach((line, i) => {
    if (line.includes('function persistState') || line.includes('JSON.stringify(arr)') || line.includes('failed to persist state')) {
      hits.push({ line: i + 1, text: line.trim().slice(0, 120) });
    }
  });
  console.log('[dump] source hits', hits);
  writeFileSync(`${OUT}/script-hits.json`, JSON.stringify(hits, null, 2));
  for (const h of hits) {
    const bp = await api.send('Debugger.setBreakpoint', {
      location: { scriptId: daemonScript.scriptId, lineNumber: h.line - 1, columnNumber: 0 },
    });
    console.log('[dump] bp', h.line, JSON.stringify(bp));
  }
}

console.log('[dump] pausing now');
await api.send('Debugger.pause');

const waitMs = 180000;
const pause = await Promise.race([
  pausedP,
  new Promise((_, rej) => setTimeout(() => rej(new Error('timed out waiting for pause')), waitMs)),
]);
console.log('[dump] paused reason', pause.reason);
console.log('[dump] frames', (pause.callFrames || []).slice(0, 12).map((f) => `${f.functionName}:${f.location?.lineNumber}`).join(' | '));

const frameId = pause.callFrames?.find((f) => /persist|ensureHeavy|writeHeavy|gzip/i.test(f.functionName || ''))?.callFrameId
  || pause.callFrames?.[0]?.callFrameId;
console.log('[dump] using frame', frameId);
try {
  const result = await api.send('Debugger.evaluateOnCallFrame', {
    callFrameId: frameId,
    expression: DUMP_EXPR,
    returnByValue: true,
    timeout: 180000,
  });
  console.log('[dump] result', JSON.stringify(result, null, 2).slice(0, 4000));
  writeFileSync(`${OUT}/inspector-result.json`, JSON.stringify(result, null, 2));
} finally {
  await api.send('Debugger.resume').catch(() => {});
  ws.close();
}
console.log('[dump] done');
