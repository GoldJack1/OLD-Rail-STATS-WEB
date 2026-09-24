/**
 * Timetable CSA + visit-all (Held–Karp) for Station Bash.
 * Forced alight at every listed CRS; planned interchange is 5 minutes.
 * Tight 0–5 min departures are returned as risky hints only.
 */

export const MAX_BASH_STATIONS = 12
export const SAFE_WAIT_MIN = 5
export const RISKY_WINDOW_MIN = 5
export const WALK_MIN = 12
export const WALK_RID = '__WALK__'

const PASSENGER_SLOTS = new Set(['OR', 'OPOR', 'IP', 'OPIP', 'DT', 'OPDT'])

/** Same-city stations that need a walk; CSA only changes at one CRS. */
const WALK_GROUPS = [
  ['MAN', 'MCO', 'MCV', 'DGT'],
  ['WGN', 'WGW'],
  ['BDI', 'BDQ'],
  ['LIV', 'LVJ', 'LVC'],
  ['BMO', 'BHM'],
]

const walkAdj = (() => {
  const map = new Map()
  for (const group of WALK_GROUPS) {
    for (const a of group) {
      for (const b of group) {
        if (a === b) continue
        if (!map.has(a)) map.set(a, [])
        map.get(a).push(b)
      }
    }
  }
  return map
})()

export function timeToMinutes(t) {
  const m = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(String(t || '').trim())
  if (!m) return null
  return Number(m[1]) * 60 + Number(m[2])
}

export function minutesToHmm(min) {
  const n = ((Math.round(min) % 1440) + 1440) % 1440
  const h = Math.floor(n / 60)
  const mm = n % 60
  return `${String(h).padStart(2, '0')}:${String(mm).padStart(2, '0')}`
}

function lowerBoundDep(connections, t) {
  let lo = 0
  let hi = connections.length
  while (lo < hi) {
    const mid = (lo + hi) >> 1
    if (connections[mid].dep < t) lo = mid + 1
    else hi = mid
  }
  return lo
}

/**
 * @param {Map<string, object>} byRid
 * @param {(tpl: string) => string | null} tplToCrs
 */
export function buildPlannerIndex(byRid, tplToCrs) {
  const passengerStopsByRid = new Map()
  const connections = []

  for (const [rid, j] of byRid) {
    if (j?.isPassenger === false) continue
    const raw = Array.isArray(j.slots) ? j.slots : []
    const stops = []
    for (const s of raw) {
      if (!PASSENGER_SLOTS.has(s.slot) || !s.tpl) continue
      const dep = timeToMinutes(s.ptd || s.wtd || s.pta || s.wta)
      const arr = timeToMinutes(s.pta || s.wta || s.ptd || s.wtd)
      if (dep == null && arr == null) continue
      stops.push({
        tpl: s.tpl,
        crs: tplToCrs(s.tpl) || null,
        dep: dep ?? arr,
        arr: arr ?? dep,
        plat: s.plat ? String(s.plat).trim() : null,
      })
    }
    if (stops.length < 2) continue

    let offset = 0
    let prevDep = stops[0].dep
    const abs = stops.map((s, i) => {
      let dep = s.dep + offset * 1440
      let arr = s.arr + offset * 1440
      if (i > 0) {
        if (dep < prevDep - 360) {
          offset += 1
          dep += 1440
          arr += 1440
        }
        // Arrive-before-depart at the same stop is dwell (often 1 min), not midnight.
      }
      prevDep = dep
      return { ...s, dep, arr }
    })

    passengerStopsByRid.set(rid, {
      trainId: j.trainId || '',
      stops: abs,
    })

    for (let i = 0; i < abs.length - 1; i++) {
      const a = abs[i]
      const b = abs[i + 1]
      const fromNode = a.crs || a.tpl
      const toNode = b.crs || b.tpl
      if (!fromNode || !toNode || fromNode === toNode) continue
      connections.push({
        from: fromNode,
        to: toNode,
        fromCrs: a.crs || null,
        toCrs: b.crs || null,
        fromTpl: a.tpl,
        toTpl: b.tpl,
        dep: a.dep,
        arr: b.arr,
        rid,
        trainId: j.trainId || '',
        fromIdx: i,
        toIdx: i + 1,
      })
    }
  }

  connections.sort((x, y) => x.dep - y.dep || x.arr - y.arr)
  return { connections, passengerStopsByRid }
}

function walkConn(from, to, arr) {
  return {
    from,
    to,
    fromCrs: from,
    toCrs: to,
    fromTpl: from,
    toTpl: to,
    dep: arr,
    arr: arr + WALK_MIN,
    rid: WALK_RID,
    trainId: 'Walk',
    fromIdx: 0,
    toIdx: 0,
  }
}

function expandWalks(tau, prev, node, arr) {
  const nbrs = walkAdj.get(node)
  if (!nbrs) return
  for (const n of nbrs) {
    const walkArr = arr + WALK_MIN
    const best = tau.get(n)
    if (best == null || walkArr < best) {
      tau.set(n, walkArr)
      prev.set(n, walkConn(node, n, arr))
    }
  }
}

function earliestAll(connections, originNodes, readyMin, cancelled, interchangeMin, forbidRid) {
  const originSet = new Set(originNodes)
  const tau = new Map()
  const prev = new Map()
  for (const node of originNodes) {
    tau.set(node, readyMin)
    expandWalks(tau, prev, node, readyMin)
  }

  const start = lowerBoundDep(connections, readyMin)
  for (let pass = 0; pass < 2; pass++) {
    for (let i = start; i < connections.length; i++) {
      const c = connections[i]
      if (cancelled?.has(c.rid) || c.rid === WALK_RID) continue
      if (forbidRid && c.rid === forbidRid) continue
      const arrFrom = tau.get(c.from) ?? (c.fromCrs ? tau.get(c.fromCrs) : null)
      if (arrFrom == null) continue
      const last = prev.get(c.from) || (c.fromCrs ? prev.get(c.fromCrs) : null)
      const sameTrain = last && last.rid === c.rid
      const afterWalk = last && last.rid === WALK_RID
      const atOrigin = originSet.has(c.from) || (c.fromCrs && originSet.has(c.fromCrs))
      const ready = sameTrain || afterWalk || (atOrigin && !last)
        ? arrFrom
        : arrFrom + interchangeMin
      if (ready > c.dep) continue
      const best = tau.get(c.to)
      if (best == null || c.arr < best) {
        tau.set(c.to, c.arr)
        prev.set(c.to, c)
        if (c.toCrs && c.toCrs !== c.to) {
          const crsBest = tau.get(c.toCrs)
          if (crsBest == null || c.arr < crsBest) {
            tau.set(c.toCrs, c.arr)
            prev.set(c.toCrs, c)
          }
        }
        expandWalks(tau, prev, c.toCrs || c.to, c.arr)
      }
    }
  }
  return { tau, prev }
}

function bestNodeArrival(tau, nodes) {
  let best = null
  let node = null
  for (const t of nodes) {
    const a = tau.get(t)
    if (a == null) continue
    if (best == null || a < best) {
      best = a
      node = t
    }
  }
  return { arr: best, node }
}

function reconstructNodePath(prev, originSet, targetNode) {
  const chain = []
  let cur = targetNode
  const guard = new Set()
  while (!originSet.has(cur)) {
    if (guard.has(cur)) return null
    guard.add(cur)
    const c = prev.get(cur)
    if (!c) return null
    chain.push(c)
    if (originSet.has(c.from) || (c.fromCrs && originSet.has(c.fromCrs))) break
    cur = originSet.has(c.fromCrs) ? c.fromCrs : c.from
  }
  chain.reverse()
  return chain
}

function mergeLegs(chain) {
  if (!chain.length) return []
  const legs = []
  let cur = {
    rid: chain[0].rid,
    trainId: chain[0].trainId,
    fromCrs: chain[0].fromCrs || chain[0].from,
    toCrs: chain[0].toCrs || chain[0].to,
    fromTpl: chain[0].fromTpl || chain[0].from,
    toTpl: chain[0].toTpl || chain[0].to,
    dep: chain[0].dep,
    arr: chain[0].arr,
    fromIdx: chain[0].fromIdx,
    toIdx: chain[0].toIdx,
  }
  for (let i = 1; i < chain.length; i++) {
    const c = chain[i]
    if (c.rid === cur.rid) {
      cur.toTpl = c.toTpl || c.to
      cur.toCrs = c.toCrs || c.to
      cur.arr = c.arr
      cur.toIdx = c.toIdx
    } else {
      legs.push(cur)
      cur = {
        rid: c.rid,
        trainId: c.trainId,
        fromCrs: c.fromCrs || c.from,
        toCrs: c.toCrs || c.to,
        fromTpl: c.fromTpl || c.from,
        toTpl: c.toTpl || c.to,
        dep: c.dep,
        arr: c.arr,
        fromIdx: c.fromIdx,
        toIdx: c.toIdx,
      }
    }
  }
  legs.push(cur)
  return legs
}

function callingCrsAfterOrigin(legs, passengerStopsByRid, originCrs) {
  const crs = []
  for (const leg of legs) {
    const rec = passengerStopsByRid.get(leg.rid)
    if (!rec) continue
    const fromI = Math.max(0, leg.fromIdx)
    const toI = Math.min(rec.stops.length - 1, leg.toIdx)
    for (let i = fromI + 1; i <= toI; i++) {
      const code = rec.stops[i].crs
      if (code && code !== originCrs) crs.push(code)
    }
  }
  return crs
}

function riskyFromArrival({
  connections,
  passengerStopsByRid,
  fromNodes,
  arrMin,
  remainingCrs,
  cancelled,
  tplToCrs,
  nameOf,
  arrivingRid,
}) {
  const remaining = new Set(remainingCrs)
  if (remaining.size === 0) return []
  const fromSet = new Set(fromNodes)
  const start = lowerBoundDep(connections, arrMin)
  const end = arrMin + RISKY_WINDOW_MIN
  const seen = new Set()
  const out = []
  for (let i = start; i < connections.length; i++) {
    const c = connections[i]
    if (c.dep >= end) break
    if (c.dep < arrMin) continue
    if (c.rid === WALK_RID) continue
    if (arrivingRid && c.rid === arrivingRid) continue
    if (cancelled?.has(c.rid)) continue
    if (!fromSet.has(c.from)) continue
    const key = `${c.rid}|${c.dep}`
    if (seen.has(key)) continue
    seen.add(key)
    const rec = passengerStopsByRid.get(c.rid)
    if (!rec) continue
    let destCrs = null
    for (let s = c.toIdx; s < rec.stops.length; s++) {
      const code = rec.stops[s].crs
      if (code && remaining.has(code)) {
        destCrs = code
        break
      }
    }
    if (!destCrs) continue
    out.push({
      rid: c.rid,
      trainId: c.trainId,
      destCrs,
      destName: nameOf(destCrs),
      dep: minutesToHmm(c.dep),
      minsAfterArrival: Math.max(0, Math.round(c.dep - arrMin)),
    })
    if (out.length >= 8) break
  }
  void tplToCrs
  return out
}

function uniqueCrs(codes) {
  const out = []
  const seen = new Set()
  for (const raw of codes) {
    const c = String(raw || '').trim().toUpperCase()
    if (!c || seen.has(c)) continue
    seen.add(c)
    out.push(c)
  }
  return out
}

/**
 * @param {object} args
 */
export function planBashItinerary({
  index,
  startCrs,
  endCrs,
  visitCrs,
  atMin,
  resolveCrs,
  tplToCrs,
  cancelled,
  nameOf,
}) {
  const start = String(startCrs || '').trim().toUpperCase()
  const end = String(endCrs || '').trim().toUpperCase()
  const visit = uniqueCrs(visitCrs)
  if (!start || !end) {
    return { ok: false, error: 'start and end are required' }
  }
  const all = uniqueCrs([start, end, ...visit])
  if (all.length > MAX_BASH_STATIONS) {
    return {
      ok: false,
      error: `too many stations (max ${MAX_BASH_STATIONS} including start and end)`,
    }
  }
  if (!visit.length) {
    return { ok: false, error: 'visit list is empty' }
  }

  const resolved = new Map()
  for (const crs of all) {
    const r = resolveCrs(crs)
    if (!r?.tiplocs?.length) {
      return { ok: false, error: `unknown station code "${crs}"` }
    }
    resolved.set(crs, r)
  }

  const n = visit.length
  const bitOf = new Map(visit.map((c, i) => [c, 1 << i]))
  const startMask = bitOf.has(start) ? bitOf.get(start) : 0
  const fullMask = (1 << n) - 1
  const endInVisit = bitOf.has(end)

  const { connections, passengerStopsByRid } = index
  const csaCache = new Map()

  function runCsa(fromCrs, readyMin, forbidRid) {
    const key = `${fromCrs}|${readyMin}|${forbidRid || ''}`
    const hit = csaCache.get(key)
    if (hit) return hit
    const resolvedFrom = resolved.get(fromCrs)
    const originNodes = [fromCrs, ...(resolvedFrom?.tiplocs || [])]
    const result = earliestAll(connections, originNodes, readyMin, cancelled, SAFE_WAIT_MIN, forbidRid)
    result.originSet = new Set(originNodes)
    csaCache.set(key, result)
    return result
  }

  function hopTo(fromCrs, readyMin, targetCrs, unvisited, depth = 0, forbidRid = null) {
    if (depth > 8) return null
    const csa = runCsa(fromCrs, readyMin, forbidRid)
    const { arr, node } = bestNodeArrival(csa.tau, [targetCrs])
    if (arr == null || !node) return null
    const chain = reconstructNodePath(csa.prev, csa.originSet, node)
    if (!chain?.length) return null
    const legs = mergeLegs(chain)
    const namedLegs = legs.map((leg) => {
      const rec = passengerStopsByRid.get(leg.rid)
      const fromStop = rec?.stops?.[leg.fromIdx]
      const toStop = rec?.stops?.[leg.toIdx]
      const fromCode = leg.fromCrs || tplToCrs(leg.fromTpl) || fromCrs
      const toCode = leg.toCrs || tplToCrs(leg.toTpl) || targetCrs
      return {
        rid: leg.rid,
        trainId: leg.trainId,
        fromCrs: fromCode,
        fromName: nameOf(fromCode),
        toCrs: toCode,
        toName: nameOf(toCode),
        fromTpl: leg.fromTpl || fromStop?.tpl || null,
        toTpl: leg.toTpl || toStop?.tpl || null,
        fromPlat: fromStop?.plat || null,
        toPlat: toStop?.plat || null,
        dep: minutesToHmm(leg.dep),
        arr: minutesToHmm(leg.arr),
      }
    })
    const arrivingRid = [...namedLegs].reverse().find((leg) => leg.rid && leg.rid !== WALK_RID)?.rid || null
    return {
      fromCrs,
      fromName: nameOf(fromCrs),
      toCrs: targetCrs,
      toName: nameOf(targetCrs),
      dep: namedLegs[0]?.dep || minutesToHmm(readyMin),
      arr: minutesToHmm(arr),
      arrMin: arr,
      arrivingRid,
      legs: namedLegs,
    }
  }

  /** @type {Map<string, { ready: number, hop: object | null, prevKey: string | null }>} */
  const dp = new Map()
  const initKey = `${startMask}|${start}`
  dp.set(initKey, { ready: atMin, hop: null, prevKey: null, arrivingRid: null })

  const keysByMask = Array.from({ length: fullMask + 1 }, () => [])
  keysByMask[startMask].push(initKey)

  for (let mask = 0; mask <= fullMask; mask++) {
    for (const key of keysByMask[mask]) {
      const st = dp.get(key)
      if (!st) continue
      const last = key.slice(key.indexOf('|') + 1)
      const remaining = []
      for (let i = 0; i < n; i++) {
        if ((mask & (1 << i)) === 0) remaining.push(visit[i])
      }
      const unvisited = new Set(remaining)

      if (remaining.length === 0) {
        if (last === end) continue
        const hop = hopTo(last, st.ready, end, unvisited, 0, st.arrivingRid)
        if (!hop) continue
        const nextKey = `${mask}|${end}`
        const nextReady = hop.arrMin
        const prev = dp.get(nextKey)
        if (!prev || nextReady < prev.ready) {
          dp.set(nextKey, { ready: nextReady, hop, prevKey: key, arrivingRid: hop.arrivingRid })
        }
        continue
      }

      const targets = remaining.filter((crs) => {
        if (endInVisit && crs === end && remaining.length > 1) return false
        return true
      })
      for (const target of targets) {
        const hop = hopTo(last, st.ready, target, unvisited, 0, st.arrivingRid)
        if (!hop) continue
        const bit = bitOf.get(hop.toCrs)
        if (bit == null) continue
        if (mask & bit) continue
        const nextMask = mask | bit
        const nextKey = `${nextMask}|${hop.toCrs}`
        const isFinish = nextMask === fullMask && hop.toCrs === end
        const nextReady = isFinish ? hop.arrMin : hop.arrMin + SAFE_WAIT_MIN
        const prev = dp.get(nextKey)
        if (!prev || nextReady < prev.ready) {
          dp.set(nextKey, { ready: nextReady, hop, prevKey: key, arrivingRid: hop.arrivingRid })
          if (nextMask !== mask) keysByMask[nextMask].push(nextKey)
        }
      }
    }
  }

  const finishKey = `${fullMask}|${end}`
  const finish = dp.get(finishKey)
  if (!finish) {
    return {
      ok: false,
      error:
        'no same-day itinerary found that alights at every listed station and finishes at the end. Try an earlier start time, or a start/end that can connect via the day’s passenger trains (changes at the same CRS are allowed).',
    }
  }

  const hops = []
  let cursor = finishKey
  while (cursor) {
    const st = dp.get(cursor)
    if (!st?.hop) break
    hops.push(st.hop)
    cursor = st.prevKey
  }
  hops.reverse()

  let prevArrMin = atMin
  let prevPlat = null
  for (let i = 0; i < hops.length; i++) {
    const hop = hops[i]
    const depMin = timeToMinutes(hop.dep)
    hop.waitMin = depMin != null ? Math.max(0, Math.round(depMin - prevArrMin)) : 0
    hop.arriveAtFrom = minutesToHmm(i === 0 ? atMin : prevArrMin)
    const boardPlat = hop.legs[0]?.fromPlat || null
    const alightPlat = hop.legs[hop.legs.length - 1]?.toPlat || null
    hop.boardPlatform = boardPlat
    hop.alightPlatform = alightPlat
    hop.prevAlightPlatform = prevPlat
    hop.platformChange = Boolean(
      prevPlat && boardPlat && String(prevPlat).trim().toUpperCase() !== String(boardPlat).trim().toUpperCase(),
    )
    prevArrMin = hop.arrMin
    prevPlat = alightPlat
  }

  const visitOrder = []
  const visited = new Set()
  if (bitOf.has(start)) {
    visited.add(start)
    visitOrder.push({
      crs: start,
      name: nameOf(start),
      arr: minutesToHmm(atMin),
      waitMin: 0,
    })
  }
  for (let i = 0; i < hops.length; i++) {
    const hop = hops[i]
    const isLast = i === hops.length - 1
    visited.add(hop.toCrs)
    visitOrder.push({
      crs: hop.toCrs,
      name: hop.toName,
      arr: hop.arr,
      waitMin: isLast ? 0 : hops[i + 1].waitMin,
    })
    hop.riskyConnections = riskyFromArrival({
      connections,
      passengerStopsByRid,
      fromNodes: [hop.toCrs],
      arrMin: hop.arrMin,
      remainingCrs: visit.filter((c) => !visited.has(c)),
      cancelled,
      tplToCrs,
      nameOf,
      arrivingRid: hop.legs[hop.legs.length - 1]?.rid,
    })
  }

  const firstArr = hops[0] ? hops[0].arrMin : atMin
  const lastArr = hops.length ? hops[hops.length - 1].arrMin : atMin

  return {
    ok: true,
    start: { crs: start, name: nameOf(start) },
    end: { crs: end, name: nameOf(end) },
    at: minutesToHmm(atMin),
    safeWaitMin: SAFE_WAIT_MIN,
    visitOrder,
    hops: hops.map((h) => ({
      fromCrs: h.fromCrs,
      fromName: h.fromName,
      toCrs: h.toCrs,
      toName: h.toName,
      dep: h.dep,
      arr: h.arr,
      waitMin: h.waitMin,
      arriveAtFrom: h.arriveAtFrom,
      boardPlatform: h.boardPlatform || null,
      alightPlatform: h.alightPlatform || null,
      prevAlightPlatform: h.prevAlightPlatform || null,
      platformChange: !!h.platformChange,
      legs: h.legs,
      riskyConnections: h.riskyConnections || [],
    })),
    totalMin: Math.max(0, Math.round(lastArr - atMin)),
    caution:
      'Trains shown as risky leave before a 5 minute interchange. Catching them depends on station layout, platforms, and delay.',
    firstArrival: minutesToHmm(firstArr),
    finishAt: minutesToHmm(lastArr),
  }
}
