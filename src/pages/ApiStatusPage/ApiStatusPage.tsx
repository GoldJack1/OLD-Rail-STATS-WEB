import React, { useEffect, useMemo, useState } from 'react'
import { fetchDarwin } from '../../utils/darwinReadyFetch'
import { PageTopHeader } from '../../components/misc'
import { BUTWideButton } from '../../components/buttons'
import './ApiStatusPage.css'

type HealthPayload = {
  ok: boolean
  mode?: string
  liveCachesReady?: boolean
  heavyReloadBusy?: boolean
  heavyReloadReason?: string
  clientReadsAllowed?: boolean
  loadedDate?: string
  startedAt?: string
  processStartedAt?: string
  uptimeSec?: number
  uptimeMs?: number
  journeysLoaded?: number
  tiplocsIndexed?: number
  timetableFile?: string
  memoryMB?: { heap?: number; rss?: number }
  kafka?: {
    consumed?: number
    updates?: number
    startedAt?: string
    lastKafkaMsgAt?: string
    sinceProcessStart?: boolean
    processStartedAt?: string
  }
  ptac?: {
    enabled?: boolean
    consumed?: number
    matched?: number
    unmatched?: number
    errors?: number
    lastMessageAt?: string
    unitsOnLoadedDate?: number
    consistsOnLoadedDate?: number
    topic?: string
  }
  overlaySize?: {
    live?: number
    cancelled?: number
    delayed?: number
    formations?: number
    consists?: number
    units?: number
    unmatchedConsists?: number
    stationsWithMessages?: number
    messages?: number
    ridsWithAssociations?: number
    ridsWithAlerts?: number
  }
  persistence?: {
    intervalSec?: number
    lastPersistAt?: string
    stateFile?: string
    fileSizeBytes?: number
    stateFileBytes?: number
    dedupShards?: boolean
  }
  unitCatalog?: {
    size?: number
    lastPersistAt?: string
    file?: string
    fileSizeBytes?: number
    fileBytes?: number
    jsonFileBytes?: number
    store?: 'json' | 'sqlite'
    sqlite?: {
      path?: string
      bytes?: number | null
      blobBytes?: number | null
      units?: number
      savedAt?: string | null
      exists?: boolean
      error?: string | null
    }
    lastLoad?: { source?: string | null; ms?: number | null; at?: string | null; fallback?: boolean }
  }
  warmup?: {
    enabled?: boolean
    days?: number
    startedAt?: string | null
    finishedAt?: string | null
    current?: string | null
    done?: number
    skipped?: number
    errors?: number
  }
  history?: { retentionDays?: number; dates?: string[] }
}

type HistoryDatesPayload = {
  count: number
  retentionDays: number
  dates: Array<{
    date: string
    hasState: boolean
    hasTimetable: boolean
    snapshots: string[]
  }>
}

function formatNum(v: unknown): string {
  return typeof v === 'number' ? v.toLocaleString('en-GB') : '-'
}

function displayDateTime(isoLike: string): string {
  const d = new Date(isoLike)
  if (Number.isNaN(d.getTime())) return isoLike
  return d.toLocaleString('en-GB', {
    day: '2-digit',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

function formatBytes(v: unknown): string {
  if (typeof v !== 'number' || !Number.isFinite(v) || v < 0) return '-'
  if (v < 1024) return `${Math.round(v)} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let size = v / 1024
  let idx = 0
  while (size >= 1024 && idx < units.length - 1) {
    size /= 1024
    idx += 1
  }
  const rounded = size >= 100 ? Math.round(size) : Number(size.toFixed(1))
  return `${rounded.toLocaleString('en-GB')} ${units[idx]}`
}

function pickNumber(...values: Array<unknown>): number | null {
  for (const v of values) {
    if (typeof v === 'number' && Number.isFinite(v) && v >= 0) return v
  }
  return null
}

function formatUptimeFromMs(v: unknown): string {
  if (typeof v !== 'number' || !Number.isFinite(v) || v < 0) return '-'
  const totalSec = Math.floor(v / 1000)
  const days = Math.floor(totalSec / 86_400)
  const hours = Math.floor((totalSec % 86_400) / 3600)
  const mins = Math.floor((totalSec % 3600) / 60)
  if (days > 0) return `${days}d ${hours}h ${mins}m`
  if (hours > 0) return `${hours}h ${mins}m`
  return `${mins}m`
}

function Card({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <article className="api-status-card">
      <h3>{title}</h3>
      <p>{children}</p>
    </article>
  )
}

const ApiStatusPage: React.FC = () => {
  const [health, setHealth] = useState<HealthPayload | null>(null)
  const [status, setStatus] = useState<'loading' | 'ok' | 'error'>('loading')
  const [error, setError] = useState<string | null>(null)
  const [lastUpdatedAt, setLastUpdatedAt] = useState<string | null>(null)
  const [available, setAvailable] = useState<HistoryDatesPayload | null>(null)

  const runFetch = async (isInitial = false) => {
    if (isInitial) setStatus('loading')
    try {
      const res = await fetch('/api/darwin/health')
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const next: HealthPayload = await res.json()
      setHealth(next)
      setLastUpdatedAt(new Date().toISOString())
      setStatus('ok')
      setError(null)
    } catch (e) {
      setStatus('error')
      const msg = (e as Error)?.message || 'Failed to load health.'
      setError(msg)
    }
  }

  const fetchAvailable = async () => {
    try {
      const res = await fetchDarwin('/api/darwin/history/dates')
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const payload: HistoryDatesPayload = await res.json()
      setAvailable(payload)
    } catch {}
  }

  useEffect(() => {
    runFetch(true)
    fetchAvailable()
    const t = window.setInterval(() => void runFetch(false), 15000)
    const a = window.setInterval(() => void fetchAvailable(), 120000)
    return () => {
      window.clearInterval(t)
      window.clearInterval(a)
    }
  }, [])

  const summary = useMemo(() => {
    const stateBytes = pickNumber(health?.persistence?.fileSizeBytes, health?.persistence?.stateFileBytes)
    const jsonBytes = pickNumber(health?.unitCatalog?.jsonFileBytes, health?.unitCatalog?.fileSizeBytes, health?.unitCatalog?.fileBytes)
    const sqliteBytes = pickNumber(health?.unitCatalog?.sqlite?.bytes, health?.unitCatalog?.sqlite?.blobBytes)
    const uptimeFromNumericMs = pickNumber(
      health?.uptimeMs,
      typeof health?.uptimeSec === 'number' ? health.uptimeSec * 1000 : null
    )
    const startedAtIso = health?.processStartedAt || health?.startedAt || health?.kafka?.startedAt
    const derivedFromStartedAtMs = startedAtIso
      ? Math.max(0, Date.now() - new Date(startedAtIso).getTime())
      : null
    const uptimeMs = uptimeFromNumericMs ?? (Number.isFinite(derivedFromStartedAtMs) ? derivedFromStartedAtMs : null)

    return {
      daemonStatus: health?.ok ? (health.heavyReloadBusy ? 'Reloading' : health.mode || 'Healthy') : 'Unavailable',
      loadedDate: health?.loadedDate || '-',
      uptime: formatUptimeFromMs(uptimeMs),
      processStartedAt: startedAtIso || '-',
      kafkaConsumed: formatNum(health?.kafka?.consumed),
      kafkaUpdates: formatNum(health?.kafka?.updates),
      journeys: formatNum(health?.journeysLoaded),
      tiplocs: formatNum(health?.tiplocsIndexed),
      timetableFile: health?.timetableFile || '-',
      live: formatNum(health?.overlaySize?.live),
      cancelled: formatNum(health?.overlaySize?.cancelled),
      formations: formatNum(health?.overlaySize?.formations),
      consists: formatNum(health?.overlaySize?.consists),
      units: formatNum(health?.overlaySize?.units),
      unitCatalog: formatNum(health?.unitCatalog?.size),
      messages: formatNum(health?.overlaySize?.messages),
      stationsWithMessages: formatNum(health?.overlaySize?.stationsWithMessages),
      associations: formatNum(health?.overlaySize?.ridsWithAssociations),
      alerts: formatNum(health?.overlaySize?.ridsWithAlerts),
      unmatchedConsists: formatNum(health?.overlaySize?.unmatchedConsists),
      heap: formatNum(health?.memoryMB?.heap),
      rss: formatNum(health?.memoryMB?.rss),
      persistEvery: `${formatNum(health?.persistence?.intervalSec)}s`,
      lastPersist: health?.persistence?.lastPersistAt || '-',
      ptac: health?.ptac?.enabled ? 'Enabled' : 'Disabled',
      ptacConsumed: formatNum(health?.ptac?.consumed),
      ptacErrors: formatNum(health?.ptac?.errors),
      ptacMatched: formatNum(health?.ptac?.matched),
      ptacUnmatched: formatNum(health?.ptac?.unmatched),
      ptacUnitsToday: formatNum(health?.ptac?.unitsOnLoadedDate),
      ptacConsistsToday: formatNum(health?.ptac?.consistsOnLoadedDate),
      kafkaLastMessageAt: health?.kafka?.lastKafkaMsgAt || '-',
      stateCacheFileName: health?.persistence?.stateFile || '-',
      unitCatalogFileName: health?.unitCatalog?.file || '-',
      stateCacheFileSize: formatBytes(stateBytes),
      unitCatalogFileSize: formatBytes(jsonBytes),
      sqliteFileSize: formatBytes(sqliteBytes),
      sqliteBlobSize: formatBytes(health?.unitCatalog?.sqlite?.blobBytes),
      lastLoadSource: health?.unitCatalog?.lastLoad?.source || '-',
      lastLoadMs: health?.unitCatalog?.lastLoad?.ms != null ? `${health.unitCatalog.lastLoad.ms} ms` : '-',
      lastLoadAt: health?.unitCatalog?.lastLoad?.at || '-',
      lastLoadFallback: health?.unitCatalog?.lastLoad?.fallback ? 'Yes' : 'No',
      liveCachesReady: health?.liveCachesReady ? 'Yes' : 'No',
      clientReads: health?.clientReadsAllowed ? 'Yes' : 'No',
      warmup: health?.warmup
        ? `${formatNum(health.warmup.done)} done / ${formatNum(health.warmup.skipped)} skipped / ${formatNum(health.warmup.errors)} errors`
        : '-',
    }
  }, [health])

  const historyTotals = useMemo(() => {
    const rows = available?.dates || []
    const snapshots = rows.reduce((total, row) => total + row.snapshots.length, 0)
    const withState = rows.filter((row) => row.hasState).length
    const withTimetable = rows.filter((row) => row.hasTimetable).length
    return { snapshots, withState, withTimetable }
  }, [available])

  return (
    <div className="api-status-shell">
      <PageTopHeader
        title="API Status"
        subtitle={status === 'ok' ? 'Live Darwin daemon health and Kafka counters' : 'Connecting to API health...'}
      />
      <div className="api-status-page">
        <section className="api-status-controls">
          <BUTWideButton width="hug" instantAction onClick={() => void runFetch(false)}>
            Refresh now
          </BUTWideButton>
          {status === 'error' && <p className="api-status-error">{error}</p>}
          {status === 'ok' && error && <p className="api-status-error">{error}</p>}
          {status === 'ok' && <p className="api-status-meta">Last update: {lastUpdatedAt ? displayDateTime(lastUpdatedAt) : '-'}</p>}
        </section>

        <section className="api-panel" aria-label="Unit catalog">
          <h2>Unit catalog</h2>
          <p className="api-panel-subtitle">
            Stored in SQLite only. The old unit-catalog.json file may still be on disk as a backup and is not updated.
          </p>
          <div className="api-status-grid">
            <Card title="Store">SQLite</Card>
            <Card title="Last load source">{summary.lastLoadSource}</Card>
            <Card title="Last load time">{summary.lastLoadMs}</Card>
            <Card title="Fell back">{summary.lastLoadFallback}</Card>
            <Card title="SQLite file">{summary.sqliteFileSize}</Card>
            <Card title="SQLite blob">{summary.sqliteBlobSize}</Card>
            <Card title="Catalog units">{summary.unitCatalog}</Card>
          </div>
        </section>

        <section className="api-status-grid" aria-label="Daemon">
          <Card title="Daemon status">{summary.daemonStatus}</Card>
          <Card title="Live caches ready">{summary.liveCachesReady}</Card>
          <Card title="Client reads">{summary.clientReads}</Card>
          <Card title="Loaded date">{summary.loadedDate}</Card>
          <Card title="Uptime">{summary.uptime}</Card>
          <Card title="Process started">{summary.processStartedAt === '-' ? '-' : displayDateTime(summary.processStartedAt)}</Card>
          <Card title="Journeys">{summary.journeys}</Card>
          <Card title="TIPLOCs">{summary.tiplocs}</Card>
          <Card title="Timetable">{summary.timetableFile}</Card>
          <Card title="Warmup">{summary.warmup}</Card>
        </section>

        <section className="api-panel" aria-label="Kafka">
          <h2>Kafka</h2>
          <p className="api-panel-subtitle">
            Consumed and updates count from this process only. They reset to zero on every Darwin restart.
            Overlay caches are restored from disk separately.
          </p>
          <div className="api-status-grid">
            <Card title="Consumed">{summary.kafkaConsumed}</Card>
            <Card title="Updates">{summary.kafkaUpdates}</Card>
            <Card title="Last message">{summary.kafkaLastMessageAt === '-' ? '-' : displayDateTime(summary.kafkaLastMessageAt)}</Card>
            <Card title="PTAC">{summary.ptac}</Card>
            <Card title="PTAC consumed">{summary.ptacConsumed}</Card>
            <Card title="PTAC matched">{summary.ptacMatched}</Card>
            <Card title="PTAC unmatched">{summary.ptacUnmatched}</Card>
            <Card title="PTAC errors">{summary.ptacErrors}</Card>
            <Card title="PTAC units today">{summary.ptacUnitsToday}</Card>
            <Card title="PTAC consists today">{summary.ptacConsistsToday}</Card>
          </div>
        </section>

        <section className="api-panel" aria-label="In-memory caches">
          <h2>In-memory caches</h2>
          <div className="api-status-grid">
            <Card title="Live overlays">{summary.live}</Card>
            <Card title="Cancelled">{summary.cancelled}</Card>
            <Card title="Formations">{summary.formations}</Card>
            <Card title="Consists">{summary.consists}</Card>
            <Card title="Units (today)">{summary.units}</Card>
            <Card title="Unmatched consists">{summary.unmatchedConsists}</Card>
            <Card title="Messages">{summary.messages}</Card>
            <Card title="Stations with messages">{summary.stationsWithMessages}</Card>
            <Card title="Associations">{summary.associations}</Card>
            <Card title="Alerts">{summary.alerts}</Card>
            <Card title="Heap MB">{summary.heap}</Card>
            <Card title="RSS MB">{summary.rss}</Card>
            <Card title="Persist">{summary.persistEvery} · {summary.lastPersist === '-' ? '-' : displayDateTime(summary.lastPersist)}</Card>
          </div>
        </section>

        <section className="api-panel" aria-label="Cache file details">
          <h2>Cache files</h2>
          <p className="api-panel-subtitle">On-disk footprint for live state and the SQLite unit catalog. unit-catalog.json is a leftover backup if present.</p>
          <div className="api-status-grid">
            <Card title="State cache filename">{summary.stateCacheFileName}</Card>
            <Card title="State cache file">{summary.stateCacheFileSize}</Card>
            <Card title="SQLite catalog">{summary.sqliteFileSize}</Card>
            <Card title="Leftover JSON backup">{summary.unitCatalogFileSize}</Card>
          </div>
        </section>

        <section className="api-panel" aria-label="Available historical data">
          <h2>Available data</h2>
          <p className="api-panel-subtitle">
            {available
              ? `${available.count} date(s) · retention ${available.retentionDays} days`
              : 'Loading available history dates...'}
          </p>
          <div className="api-status-grid">
            <Card title="Total snapshots">{formatNum(historyTotals.snapshots)}</Card>
            <Card title="Dates with state">{formatNum(historyTotals.withState)}</Card>
            <Card title="Dates with timetable">{formatNum(historyTotals.withTimetable)}</Card>
            <Card title="History dates listed">{formatNum(available?.count)}</Card>
          </div>
          <div className="api-available-list">
            {(available?.dates || []).slice(0, 10).map((d) => (
              <article key={d.date} className="api-available-item">
                <div>
                  <strong>{d.date}</strong>
                  <div className="api-available-meta">
                    <span>state: {d.hasState ? 'yes' : 'no'}</span>
                    <span>timetable: {d.hasTimetable ? 'yes' : 'no'}</span>
                    <span>snapshots: {d.snapshots.length}</span>
                  </div>
                </div>
                <span className="api-available-last">
                  {d.snapshots.length > 0
                    ? new Date(d.snapshots[d.snapshots.length - 1]).toLocaleString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' })
                    : '-'}
                </span>
              </article>
            ))}
          </div>
        </section>
      </div>
    </div>
  )
}

export default ApiStatusPage
