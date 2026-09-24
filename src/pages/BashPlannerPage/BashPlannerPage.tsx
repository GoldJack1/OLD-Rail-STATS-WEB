import React, { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { BUTWideButton } from '../../components/buttons'
import { PageTopHeader } from '../../components/misc'
import TXTINPWideButton from '../../components/textInputs/plain/TXTINPWideButton'
import { fetchDarwin } from '../../utils/darwinReadyFetch'
import './BashPlannerPage.css'

type BashLeg = {
  rid?: string
  trainId?: string
  fromCrs?: string
  toCrs?: string
  fromName?: string
  toName?: string
  dep?: string
  arr?: string
  board?: { rid?: string; trainId?: string; tocName?: string; originName?: string; destinationName?: string } | null
}

type BashHop = {
  fromCrs: string
  fromName?: string
  toCrs: string
  toName?: string
  dep: string
  arr: string
  waitMin?: number
  boardPlatform?: string | null
  alightPlatform?: string | null
  platformChange?: boolean
  legs?: BashLeg[]
}

type BashResult = {
  ok: boolean
  error?: string
  date?: string
  at?: string
  start?: { crs: string; name?: string }
  end?: { crs: string; name?: string }
  visitOrder?: Array<{ crs: string; name?: string; arr?: string; waitMin?: number }>
  hops?: BashHop[]
  totalMin?: number
  finishAt?: string
  caution?: string
}

function parseVisitList(raw: string): string[] {
  return raw
    .split(/[\s,;]+/)
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean)
}

const BashPlannerPage: React.FC = () => {
  const [start, setStart] = useState('')
  const [end, setEnd] = useState('')
  const [visit, setVisit] = useState('')
  const [at, setAt] = useState('08:00')
  const [date, setDate] = useState('')
  const [status, setStatus] = useState<'idle' | 'loading' | 'ok' | 'error'>('idle')
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<BashResult | null>(null)

  const visitList = useMemo(() => parseVisitList(visit), [visit])

  const plan = async () => {
    setError(null)
    const startCrs = start.trim().toUpperCase()
    const endCrs = end.trim().toUpperCase()
    if (!startCrs || !endCrs) {
      setError('Enter start and end CRS codes.')
      setStatus('error')
      return
    }
    if (!/^\d{1,2}:\d{2}$/.test(at.trim())) {
      setError('Start time must be HH:MM.')
      setStatus('error')
      return
    }
    setStatus('loading')
    try {
      const res = await fetchDarwin('/api/darwin/plan/bash', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          start: startCrs,
          end: endCrs,
          visit: visitList,
          at: at.trim(),
          ...(date.trim() ? { date: date.trim() } : {}),
        }),
      })
      const payload = (await res.json()) as BashResult
      if (!res.ok || !payload.ok) {
        setResult(payload)
        setError(payload.error || `HTTP ${res.status}`)
        setStatus('error')
        return
      }
      setResult(payload)
      setStatus('ok')
    } catch (e) {
      setError((e as Error)?.message || 'Plan failed.')
      setStatus('error')
    }
  }

  return (
    <div className="bash-planner-shell">
      <PageTopHeader
        title="Station bash"
        subtitle="Alight at each listed station. Interchange is planned at 5 minutes."
      />
      <div className="bash-planner-page">
        <section className="bash-planner-form" aria-label="Bash inputs">
          <label>
            Start CRS
            <TXTINPWideButton value={start} onChange={setStart} placeholder="LDS" />
          </label>
          <label>
            End CRS
            <TXTINPWideButton value={end} onChange={setEnd} placeholder="MAN" />
          </label>
          <label>
            Start time
            <TXTINPWideButton value={at} onChange={setAt} placeholder="08:00" />
          </label>
          <label>
            Date (optional)
            <TXTINPWideButton value={date} onChange={setDate} placeholder="YYYY-MM-DD" />
          </label>
          <label className="bash-planner-visit">
            Visit CRS (comma or space separated, max 12)
            <TXTINPWideButton value={visit} onChange={setVisit} placeholder="YRK SHF NGP" />
          </label>
          <BUTWideButton width="hug" instantAction disabled={status === 'loading'} onClick={() => void plan()}>
            {status === 'loading' ? 'Planning…' : 'Plan itinerary'}
          </BUTWideButton>
        </section>

        {error && <p className="bash-planner-error">{error}</p>}
        {result?.caution && status === 'ok' && <p className="bash-planner-caution">{result.caution}</p>}

        {status === 'ok' && result?.ok && (
          <section className="bash-planner-result" aria-label="Itinerary">
            <p>
              {result.start?.name || result.start?.crs} → {result.end?.name || result.end?.crs}
              {result.date ? ` · ${result.date}` : ''} · leave {result.at} · finish {result.finishAt}
              {result.totalMin != null ? ` · ${result.totalMin} min` : ''}
            </p>
            <ol className="bash-planner-hops">
              {(result.hops || []).map((hop, i) => (
                <li key={`${hop.fromCrs}-${hop.toCrs}-${i}`}>
                  <strong>
                    {hop.dep} {hop.fromName || hop.fromCrs} → {hop.arr} {hop.toName || hop.toCrs}
                  </strong>
                  {hop.waitMin ? <span> wait {hop.waitMin} min</span> : null}
                  {hop.boardPlatform ? <span> plat {hop.boardPlatform}</span> : null}
                  {hop.platformChange ? <span> platform change</span> : null}
                  <ul>
                    {(hop.legs || []).map((leg, j) => {
                      const rid = leg.board?.rid || leg.rid
                      const label = `${leg.dep || ''} ${leg.board?.trainId || leg.trainId || ''} ${leg.board?.tocName || ''} ${leg.fromName || leg.fromCrs} → ${leg.toName || leg.toCrs}`.trim()
                      return (
                        <li key={`${rid || 'walk'}-${j}`}>
                          {rid && rid !== '__WALK__' ? (
                            <Link to={`/services/${encodeURIComponent(rid)}`}>{label}</Link>
                          ) : (
                            label || 'Walk'
                          )}
                        </li>
                      )
                    })}
                  </ul>
                </li>
              ))}
            </ol>
          </section>
        )}
      </div>
    </div>
  )
}

export default BashPlannerPage
