/**
 * ATOC/TTIS full CIF (MCA) → Darwin-shaped byRid / byTiploc for one railway day.
 * Used when Darwin PPTimetable has no journeys for that SSD.
 */

import { createReadStream, existsSync, readdirSync, realpathSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { createInterface } from 'node:readline'

const PASSENGER_STATUS = new Set(['P', '1', 'B'])

function yyMMddToCompact(s) {
  const raw = String(s || '').trim()
  if (!/^\d{6}$/.test(raw)) return 0
  const yy = Number(raw.slice(0, 2))
  const year = yy >= 70 ? 1900 + yy : 2000 + yy
  return year * 10000 + Number(raw.slice(2, 4)) * 100 + Number(raw.slice(4, 6))
}

function isoToCompact(iso) {
  return Number(String(iso || '').replace(/-/g, ''))
}

function compactToIso(n) {
  const s = String(n || '')
  if (!/^\d{8}$/.test(s)) return null
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`
}

function parsePlat(kind, line) {
  const raw = (kind === 'LI' ? line.slice(33, 36) : line.slice(19, 22)).trim()
  if (!raw || raw === '0') return null
  return raw
}

function parseAct(kind, line) {
  const raw = (
    kind === 'LI' ? line.slice(42, 54) : kind === 'LT' ? line.slice(25, 37) : line.slice(29, 41)
  )
    .replace(/\s+/g, ' ')
    .trim()
  return raw || null
}

function cifTime(raw) {
  const s = String(raw || '').trim()
  if (!s) return null
  const half = /H$/i.test(s)
  const d = s.replace(/H$/i, '')
  if (!/^\d{3,4}$/.test(d)) return null
  if (/^0+$/.test(d)) return null
  const hhmm = d.padStart(4, '0')
  const hh = Number(hhmm.slice(0, 2))
  const mm = Number(hhmm.slice(2, 4))
  if (hh > 23 || mm > 59) return null
  return half ? `${hhmm.slice(0, 2)}:${hhmm.slice(2, 4)}:30` : `${hhmm.slice(0, 2)}:${hhmm.slice(2, 4)}`
}

function pad80(line) {
  const s = line.replace(/\r$/, '')
  return s.length >= 80 ? s : s.padEnd(80, ' ')
}

function intern(map, arr, value) {
  const v = String(value || '').trim().toUpperCase()
  if (!v) return 0
  const hit = map.get(v)
  if (hit) return hit
  const id = arr.length
  arr.push(v)
  map.set(v, id)
  return id
}

function findMsnPath(mcaPath) {
  let dir = dirname(mcaPath)
  try {
    dir = dirname(realpathSync(mcaPath))
  } catch {}
  let files = []
  try {
    files = readdirSync(dir)
  } catch {
    return null
  }
  const hit = files.find((f) => /MSN\.txt$/i.test(f) || /MSN/i.test(f) && f.endsWith('.txt'))
  return hit ? resolve(dir, hit) : null
}

async function loadMsnTplToCrs(msnPath) {
  const map = new Map()
  if (!msnPath || !existsSync(msnPath)) return map
  const rl = createInterface({
    input: createReadStream(msnPath, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  })
  for await (const raw of rl) {
    if (!raw.startsWith('A')) continue
    const line = pad80(raw)
    const tpl = line.slice(36, 43).trim().toUpperCase()
    const crs = line.slice(49, 52).trim().toUpperCase()
    if (tpl && /^[A-Z]{3}$/.test(crs)) map.set(tpl, crs)
  }
  return map
}

function parseLocationLine(kind, line, tplMap, tpls) {
  const loc = intern(tplMap, tpls, line.slice(2, 10))
  if (!loc) return null
  let wta = null
  let wtd = null
  let wtp = null
  let pta = null
  let ptd = null
  let slot = 'IP'
  if (kind === 'LO') {
    wtd = cifTime(line.slice(10, 15))
    ptd = cifTime(line.slice(15, 19))
    slot = ptd ? 'OR' : 'OPOR'
  } else if (kind === 'LT') {
    wta = cifTime(line.slice(10, 15))
    pta = cifTime(line.slice(15, 19))
    slot = pta ? 'DT' : 'OPDT'
  } else {
    wta = cifTime(line.slice(10, 15))
    wtd = cifTime(line.slice(15, 20))
    wtp = cifTime(line.slice(20, 25))
    pta = cifTime(line.slice(25, 29))
    ptd = cifTime(line.slice(29, 33))
    if (pta || ptd) slot = 'IP'
    else if (wtp && !wta && !wtd) slot = 'PP'
    else slot = 'OPIP'
  }
  if (!wta && !wtd && !wtp && !pta && !ptd) return null
  return {
    loc,
    slot,
    pta,
    ptd,
    wta,
    wtd,
    wtp,
    plat: parsePlat(kind, line),
    act: parseAct(kind, line),
  }
}

/**
 * @param {string} filePath
 */
export async function loadTtisIndex(filePath) {
  if (!filePath || !existsSync(filePath)) {
    throw new Error(`TTIS MCA not found: ${filePath || '(empty path)'}`)
  }
  const t0 = Date.now()
  const tplMap = new Map()
  const tpls = ['']
  const schedules = []
  let current = null
  let parsedBs = 0
  let dateMin = 0
  let dateMax = 0

  const rl = createInterface({
    input: createReadStream(filePath, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  })

  for await (const raw of rl) {
    if (!raw) continue
    const line = pad80(raw)
    const kind = line.slice(0, 2)
    if (kind === 'BS') {
      if (current?.stops?.length >= 2) schedules.push(current)
      parsedBs += 1
      current = {
        uid: line.slice(3, 9).trim(),
        from: yyMMddToCompact(line.slice(9, 15)),
        to: yyMMddToCompact(line.slice(15, 21)),
        days: line.slice(21, 28),
        status: line[29],
        cat: line.slice(30, 32).trim(),
        trainId: line.slice(32, 36).trim(),
        toc: '',
        stp: line[79] || 'P',
        stops: [],
      }
      if (current.from) dateMin = dateMin ? Math.min(dateMin, current.from) : current.from
      if (current.to) dateMax = Math.max(dateMax, current.to)
      continue
    }
    if (!current) continue
    if (kind === 'BX') {
      const toc = line.slice(11, 13).trim().toUpperCase()
      if (/^[A-Z0-9]{2}$/.test(toc)) current.toc = toc
      continue
    }
    if (kind === 'LO' || kind === 'LI' || kind === 'LT') {
      const stop = parseLocationLine(kind, line, tplMap, tpls)
      if (stop) current.stops.push(stop)
      if (kind === 'LT') {
        if (current.stops.length >= 2) schedules.push(current)
        current = null
      }
    }
  }
  if (current?.stops?.length >= 2) schedules.push(current)

  const elapsed = Date.now() - t0
  const msnPath = findMsnPath(filePath)
  const tplToCrs = await loadMsnTplToCrs(msnPath)
  const dateMinIso = compactToIso(dateMin)
  const dateMaxIso = compactToIso(dateMax)
  console.log(
    `[ttis] ${filePath.split('/').pop()}: ${parsedBs} BS, ${schedules.length} schedules, ${tpls.length - 1} tiplocs, msn=${tplToCrs.size}, ${dateMinIso || '?'}–${dateMaxIso || '?'} (${elapsed}ms)`,
  )
  return { schedules, tpls, tplToCrs, dateMinIso, dateMaxIso }
}

function runsOnDay(sched, compactDay) {
  if (!sched.from || compactDay < sched.from || compactDay > sched.to) return false
  const days = sched.days || ''
  if (days.length < 7) return false
  const iso = `${String(compactDay).slice(0, 4)}-${String(compactDay).slice(4, 6)}-${String(compactDay).slice(6, 8)}`
  const jsDow = new Date(`${iso}T12:00:00Z`).getUTCDay()
  const cifBit = jsDow === 0 ? 6 : jsDow - 1
  return days[cifBit] === '1'
}

function chooseStp(list) {
  const hasC = list.some((s) => s.stp === 'C')
  const overlays = list.filter((s) => s.stp === 'O')
  const news = list.filter((s) => s.stp === 'N')
  const perms = list.filter((s) => s.stp === 'P')
  const chosen = []
  if (overlays.length) chosen.push(...overlays)
  else if (!hasC) chosen.push(...perms)
  chosen.push(...news)
  return chosen
}

/**
 * @param {{ schedules: object[], tpls: string[], tplToCrs?: Map<string, string> }} index
 * @param {string} ymdDashed
 * @param {Map<string, string> | null} crsToPrimaryTpl CRS → Darwin TIPLOC
 */
export function expandTtisDay(index, ymdDashed, crsToPrimaryTpl = null) {
  const compactDay = isoToCompact(ymdDashed)
  if (!compactDay) return { byRid: new Map(), byTiploc: new Map() }
  const byUid = new Map()
  for (const sched of index.schedules) {
    if (!runsOnDay(sched, compactDay)) continue
    if (!byUid.has(sched.uid)) byUid.set(sched.uid, [])
    byUid.get(sched.uid).push(sched)
  }

  const byRid = new Map()
  const byTiploc = new Map()
  const tpls = index.tpls
  const tplToCrs = index.tplToCrs || new Map()

  function resolveTpl(cifTpl) {
    const crs = tplToCrs.get(cifTpl)
    if (crs && crsToPrimaryTpl?.get(crs)) return crsToPrimaryTpl.get(crs)
    return cifTpl
  }

  for (const [uid, list] of byUid) {
    for (const sched of chooseStp(list)) {
      const passenger = PASSENGER_STATUS.has(sched.status)
      const slots = sched.stops.map((s) => ({
        tpl: resolveTpl(tpls[s.loc]),
        slot: s.slot,
        ptd: s.ptd,
        pta: s.pta,
        wtd: s.wtd,
        wta: s.wta,
        wtp: s.wtp,
        plat: s.plat || null,
        act: s.act || null,
      }))
      const originTpl = slots.find((s) => s.slot === 'OR' || s.slot === 'OPOR')?.tpl || slots[0]?.tpl || ''
      let destTpl = slots[slots.length - 1]?.tpl || ''
      for (let i = slots.length - 1; i >= 0; i--) {
        if (slots[i].slot === 'DT' || slots[i].slot === 'OPDT') {
          destTpl = slots[i].tpl
          break
        }
      }
      const rid = `CIF${uid}${ymdDashed.replace(/-/g, '')}${sched.stp}${sched.trainId || 'XXXX'}`
      byRid.set(rid, {
        rid,
        ssd: ymdDashed,
        uid,
        trainId: sched.trainId || '',
        toc: sched.toc || '',
        trainCat: sched.cat || '',
        status: sched.status || '',
        isPassenger: passenger,
        origin: originTpl,
        destination: destTpl,
        slots,
      })
      for (let i = 0; i < slots.length; i++) {
        const tp = slots[i].tpl
        if (!tp) continue
        let arr = byTiploc.get(tp)
        if (!arr) {
          arr = []
          byTiploc.set(tp, arr)
        }
        arr.push({ rid, stopIdx: i })
      }
    }
  }

  return { byRid, byTiploc }
}
