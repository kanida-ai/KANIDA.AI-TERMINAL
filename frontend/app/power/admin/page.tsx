/**
 * /power/admin — operator-only dashboard.
 *
 * Phase 1b (2026-05-23): authentication moved to JWT cookie.
 *   - SSR layout (/power/admin/layout.tsx) verifies role=admin via the
 *     `power_jwt` HTTPOnly cookie before this page ever renders.
 *   - All backend calls below use credentials:'include' so the same cookie
 *     is automatically attached. Backend's require_admin_or_jwt reads the
 *     cookie and authorises.
 *   - The legacy localStorage-based ADMIN_SECRET flow is preserved as a
 *     fallback (`?secret=…` URL param) so ops scripts and emergency access
 *     still work, but it's no longer the default UX.
 *
 * Panels:
 *   1. Zerodha auto-auth status
 *   2. Cohort metrics (DAU, WAU, codes used vs unused, top routes)
 *   3. Invite issuance — mint N codes, optional expiry
 *   4. Invite history
 */
'use client'

import { useEffect, useState } from 'react'

// API base — same-origin in the browser (Next.js rewrites proxy /api/power/* to the backend).
// NEXT_PUBLIC_API_URL overrides on Vercel. Resolved per-call so Turbopack
// doesn't static-evaluate `window` at build time.
function apiBase(): string {
  if (process.env.NEXT_PUBLIC_API_URL) return process.env.NEXT_PUBLIC_API_URL
  if (typeof window !== 'undefined')   return ''
  return 'http://127.0.0.1:8001'
}
const STORAGE_KEY = 'kanida_admin_secret'

/**
 * Build the headers + credentials for an admin API call.
 *
 * Same-origin model: Next.js rewrites send /api/power/* to localhost:8001,
 * so the browser sees these as same-origin and fetch()'s default
 * credentials='same-origin' auto-attaches the power_jwt cookie. We do NOT
 * pass credentials:'include' — that escalates to a CORS preflight which the
 * backend isn't configured for, and the browser then surfaces it as
 * "Failed to fetch" with no useful error in the panel UI.
 *
 * Fallback: if a legacy admin secret is in localStorage, send it as
 * X-Admin-Secret too. require_admin_or_jwt on the backend accepts either.
 */
export function adminFetchInit(extra?: RequestInit): RequestInit {
  const init: RequestInit = { ...extra }
  if (typeof window !== 'undefined') {
    const legacy = localStorage.getItem(STORAGE_KEY)
    if (legacy) {
      init.headers = { ...(init.headers || {}), 'X-Admin-Secret': legacy }
    }
  }
  return init
}


export default function AdminPage() {
  return (
    <div className="space-y-6">
      <header className="flex items-baseline justify-between">
        <div>
          <h1 className="text-2xl font-bold">Power User — Admin</h1>
          <p className="text-sm text-neutral-400 mt-1">
            Invite issuance + cohort metrics. Authenticated as admin via session cookie.
          </p>
        </div>
        <LegacySecretToggle />
      </header>

      <ZerodhaAuthPanel />
      <DailyJobsPanel />
      <TierReviewPanel />
      <MetricsPanel />
      <WaitlistPanel />
      <InviteIssuancePanel />
      <InviteListPanel />
    </div>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// Daily Jobs panel (Phase 1b — surfaces existing /api/jobs/* infra +
// the roster from /api/power/admin/jobs/scheduled). No new pipeline logic
// is implemented here — this panel only DRIVES the canonical engine.
// ─────────────────────────────────────────────────────────────────────────

type PipelineStatus = {
  running:     boolean
  last_run:    string | null
  last_result: string | null
}

type ScheduledJob = {
  name:         string
  cadence:      string
  description:  string
  status:       string
  next_run?:    string | null
  last_run?:    string | null
  last_result?: string | null
  implemented:  boolean
  trigger?:     { method: string; path: string }
}

type FreshnessResp = {
  last_trading_day: string
  ohlcv:    { latest_date: Record<string, string>; nse_fresh_tickers: number; nse_stale_tickers: number; is_fresh: boolean }
  signals?: { latest_snapshot_date: string | null; signal_count: number; is_fresh: boolean }
  overall_healthy: boolean
}


function DailyJobsPanel() {
  const [jobs,      setJobs]      = useState<ScheduledJob[] | null>(null)
  const [pipeline,  setPipeline]  = useState<PipelineStatus | null>(null)
  const [freshness, setFreshness] = useState<FreshnessResp | null>(null)
  const [error,     setError]     = useState<string | null>(null)
  const [busy,      setBusy]      = useState(false)
  const [flash,     setFlash]     = useState<string | null>(null)

  const refresh = async () => {
    try {
      const [j, p, f] = await Promise.all([
        fetch(`${apiBase()}/api/power/admin/jobs/scheduled`, adminFetchInit()),
        fetch(`${apiBase()}/api/power/admin/jobs/pipeline`,  adminFetchInit()),
        fetch(`${apiBase()}/api/power/admin/jobs/status`,    adminFetchInit()),
      ])
      if (!j.ok) throw new Error(`scheduled: HTTP ${j.status}`)
      if (!p.ok) throw new Error(`pipeline: HTTP ${p.status}`)
      if (!f.ok) throw new Error(`freshness: HTTP ${f.status}`)
      const jj = await j.json()
      setJobs(jj.jobs || [])
      setPipeline(await p.json())
      setFreshness(await f.json())
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Jobs fetch failed.')
    }
  }

  useEffect(() => {
    void refresh()
    const id = setInterval(refresh, 15_000)
    return () => clearInterval(id)
  }, [])

  const onTrigger = async () => {
    setBusy(true); setFlash(null); setError(null)
    try {
      const r = await fetch(`${apiBase()}/api/power/admin/jobs/run`,
                            adminFetchInit({ method: 'POST' }))
      if (!r.ok) {
        const b = await r.json().catch(() => ({}))
        throw new Error(b.detail?.message || `HTTP ${r.status}`)
      }
      const body = await r.json()
      setFlash(body.message || 'Pipeline triggered.')
      setTimeout(refresh, 2_000)
      setTimeout(refresh, 8_000)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Pipeline trigger failed.')
    } finally {
      setBusy(false)
    }
  }

  if (error && !jobs) return <ErrorBox text={`Jobs: ${error}`} />
  if (!jobs)          return <SkeletonBox lines={4} />

  const running = pipeline?.running === true

  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded p-4 space-y-4">
      <header className="flex items-baseline justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold text-neutral-300">Daily jobs</h2>
          <p className="text-xs text-neutral-500 mt-0.5">
            Existing engine pipeline + supporting schedulers. Manual trigger drives the same code path the 16:05 IST scheduler runs.
          </p>
        </div>
        <button
          type="button" onClick={onTrigger} disabled={busy || running}
          className={['px-3 py-1.5 rounded text-sm font-semibold transition-colors', running ? 'bg-amber-400/20 text-amber-200 cursor-not-allowed' : 'bg-mint-400 text-neutral-950 hover:bg-mint-300 disabled:opacity-50'].join(' ')}
        >
          {running ? 'Pipeline running…' : busy ? 'Triggering…' : 'Run pipeline now'}
        </button>
      </header>

      {/* Pipeline status strip */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
        <KV label="Pipeline"
            value={running ? 'RUNNING' : (pipeline?.last_result ?? 'idle')}
            mono />
        <KV label="Last run"  value={fmtIst(pipeline?.last_run ?? null)} mono />
        <KV label="OHLC fresh"
            value={freshness?.ohlcv?.is_fresh ? `yes (${freshness.ohlcv.latest_date?.NSE ?? '—'})` : 'STALE'}
            mono />
        <KV label="Last trading day"
            value={freshness?.last_trading_day ?? '—'} mono />
      </div>

      {flash && (
        <p role="status" className="px-3 py-2 rounded bg-green-500/10 text-green-200 border border-green-500/40 text-xs">
          {flash}
        </p>
      )}
      {error && <ErrorBox text={error} />}

      {/* Scheduled jobs roster */}
      <div className="space-y-2">
        {jobs.map(j => (
          <div key={j.name}
               className={['bg-neutral-950/60 border rounded p-3', j.implemented ? 'border-neutral-800' : 'border-amber-400/30 bg-amber-400/[0.03]'].join(' ')}>
            <div className="flex items-baseline justify-between gap-3 flex-wrap">
              <div className="flex items-baseline gap-2 flex-wrap">
                <span className="font-semibold text-sm text-neutral-100">{j.name}</span>
                <span className="text-[11px] text-neutral-500 font-mono">{j.cadence}</span>
              </div>
              <JobStatusBadge status={j.status} implemented={j.implemented} />
            </div>
            <p className="mt-1 text-xs text-neutral-400 leading-relaxed">{j.description}</p>
            {(j.last_run || j.next_run) && (
              <div className="mt-2 text-[11px] text-neutral-500 flex gap-4 flex-wrap font-mono">
                {j.last_run && <span>last: {fmtIst(j.last_run)}</span>}
                {j.next_run && <span>next: {fmtIst(j.next_run)}</span>}
                {j.last_result && <span className="text-neutral-400">→ {j.last_result}</span>}
              </div>
            )}
          </div>
        ))}
      </div>
    </section>
  )
}

function JobStatusBadge({ status, implemented }: { status: string; implemented: boolean }) {
  const palette = !implemented
    ? 'bg-amber-400/[0.12] text-amber-200 border-amber-400/40'
    : status === 'running'        ? 'bg-amber-400/[0.12] text-amber-200 border-amber-400/40'
    : status === 'token_invalid'  ? 'bg-red-500/15 text-red-200 border-red-500/40'
    : status === 'token_valid'    ? 'bg-green-500/15 text-green-200 border-green-500/40'
    : status === 'idle' || status === 'implemented' || status === 'manual'
                                  ? 'bg-neutral-800 text-neutral-300 border-neutral-700'
                                  : 'bg-neutral-800 text-neutral-400 border-neutral-700'
  const label = !implemented ? 'NOT YET LIVE' : status.replace('_', ' ').toUpperCase()
  return (
    <span className={['inline-flex items-center px-2 py-0.5 rounded text-[10px] font-semibold uppercase tracking-wide border font-mono', palette].join(' ')}>
      {label}
    </span>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// Optional: surface a legacy ADMIN_SECRET if the admin needs to layer one
// on top (e.g. transient JWT verification issue). Hidden in a dropdown by
// default so it doesn't clutter the page.
// ─────────────────────────────────────────────────────────────────────────

function LegacySecretToggle() {
  const [open, setOpen] = useState(false)
  const [val, setVal]   = useState('')
  const [hasStored, setHasStored] = useState(false)
  useEffect(() => {
    setHasStored(!!localStorage.getItem(STORAGE_KEY))
  }, [])
  if (!open) {
    return (
      <button type="button" onClick={() => setOpen(true)}
              className="text-[11px] text-neutral-500 hover:text-neutral-300">
        {hasStored ? 'legacy admin secret set ▾' : 'set legacy admin secret ▾'}
      </button>
    )
  }
  return (
    <div className="flex items-center gap-2">
      <input type="password" value={val} onChange={e => setVal(e.target.value)}
             placeholder="X-Admin-Secret (fallback)"
             className="bg-neutral-950 border border-neutral-700 rounded px-2 py-1 text-xs font-mono w-56" />
      <button type="button"
              onClick={() => { if (val) localStorage.setItem(STORAGE_KEY, val); setHasStored(true); setOpen(false); setVal('') }}
              className="text-xs px-2 py-1 bg-mint-500 text-neutral-950 rounded">save</button>
      <button type="button"
              onClick={() => { localStorage.removeItem(STORAGE_KEY); setHasStored(false); setOpen(false) }}
              className="text-xs text-red-400 underline hover:text-red-300">clear</button>
    </div>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// Zerodha auto-auth panel — token health + manual refresh + push subscribe
// (Sprint 5c-1 — Layer 1 status + Layer 2 enrolment)
// ─────────────────────────────────────────────────────────────────────────

type AuthStatus = {
  computed_at:         string
  token_valid:         boolean
  token_user:          string | null
  token_set_by:        string | null
  token_date:          string | null
  active_until:        string | null
  last_auto_attempt:   {
    attempt_at:     string
    status:         'success' | 'failed' | 'skipped' | 'partial'
    attempt_of_day: number
    error_code:     string | null
    error_detail:   string | null
    trigger_kind:   string
    token_preview:  string | null
  } | null
  next_scheduled_at:   string | null
  today_attempts:      number
  degraded:            boolean
  fallbacks_pending:   boolean
}

type AuthLogEntry = {
  id:             number
  attempt_at:     string
  attempt_of_day: number
  trigger_kind:   string
  status:         'success' | 'failed' | 'skipped' | 'partial'
  stage:          string | null
  error_code:     string | null
  error_detail:   string | null
  token_preview:  string | null
  elapsed_ms:     number | null
}

function ZerodhaAuthPanel() {
  const [status, setStatus] = useState<AuthStatus | null>(null)
  const [log,    setLog]    = useState<AuthLogEntry[] | null>(null)
  const [error,  setError]  = useState<string | null>(null)
  const [busy,   setBusy]   = useState(false)
  const [flash,  setFlash]  = useState<string | null>(null)

  const refresh = async () => {
    try {
      const [s, l] = await Promise.all([
        fetch(`${apiBase()}/api/power/admin/auth/status`, adminFetchInit()),
        fetch(`${apiBase()}/api/power/admin/auth/log?limit=10`, adminFetchInit()),
      ])
      if (!s.ok) throw new Error(`status: HTTP ${s.status}`)
      if (!l.ok) throw new Error(`log: HTTP ${l.status}`)
      setStatus(await s.json())
      const lj = await l.json()
      setLog(lj.entries || [])
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Auth-status fetch failed.')
    }
  }

  useEffect(() => {
    void refresh()
    const id = setInterval(refresh, 15_000)      // light polling — admin widget only
    return () => clearInterval(id)
  }, [])

  const onManualRefresh = async () => {
    setBusy(true); setFlash(null)
    try {
      const r = await fetch(`${apiBase()}/api/power/admin/auth/refresh-now`,
                            adminFetchInit({ method: 'POST' }))
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      setFlash('Manual attempt queued — refresh status in ~30s')
      // Eagerly re-poll once
      setTimeout(refresh, 3_000)
      setTimeout(refresh, 12_000)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Manual refresh failed.')
    } finally {
      setBusy(false)
    }
  }

  if (error && !status) return <ErrorBox text={`Auth: ${error}`} />
  if (!status) return <SkeletonBox lines={3} />

  const last = status.last_auto_attempt
  const tokenLabel = status.token_valid ? 'VALID' : 'INVALID'
  const tokenCls   = status.token_valid
    ? 'border-green-500/40 text-green-300'
    : 'border-red-500/40 text-red-300'

  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded p-4 space-y-4">
      <header className="flex items-baseline justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold text-neutral-300">Zerodha auto-auth</h2>
          <p className="text-xs text-neutral-500 mt-0.5">
            Layer 1: Playwright bot at 6:30 / 7:30 / 8:30 / 9:00 IST · Layer 2: push fallback
          </p>
        </div>
        <span className={`inline-flex items-center text-[11px] px-2 py-0.5 rounded border font-mono ${tokenCls}`}>
          {tokenLabel}
        </span>
      </header>

      {status.degraded && (
        <div role="status" className="px-3 py-2 rounded border border-mint-500/40 bg-mint-500/10
                                       text-mint-100 text-xs">
          ⚠ <span className="font-semibold">Degraded mode</span> — token down past 09:30 IST.
          The /power/live overlay is hidden. EOD picks remain valid.
        </div>
      )}

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
        <KV label="User"          value={status.token_user ?? '—'} />
        <KV label="Set by"        value={status.token_set_by ?? '—'} mono />
        <KV label="Active until"  value={fmtIst(status.active_until)} mono />
        <KV label="Next attempt"  value={fmtIst(status.next_scheduled_at)} mono />
      </div>

      {last && (
        <div className="bg-neutral-950/60 border border-neutral-800 rounded p-3 text-xs">
          <div className="flex items-center justify-between gap-3 mb-1">
            <span className="text-neutral-500">Last attempt</span>
            <span className={`font-mono ${
              last.status === 'success' ? 'text-green-300'
              : last.status === 'skipped' ? 'text-neutral-400'
              : 'text-red-300'
            }`}>
              {last.status.toUpperCase()} · attempt #{last.attempt_of_day} · {last.trigger_kind}
            </span>
          </div>
          <div className="text-neutral-300 font-mono">{fmtIst(last.attempt_at)}</div>
          {last.error_code && (
            <div className="mt-1 text-red-200/80">
              <span className="font-mono">{last.error_code}</span>
              {last.error_detail && <span className="ml-2 text-red-200/60">— {last.error_detail}</span>}
            </div>
          )}
        </div>
      )}

      <div className="flex flex-wrap items-center gap-3">
        <button
          type="button" onClick={onManualRefresh} disabled={busy}
          className="px-3 py-1.5 bg-mint-500 text-neutral-950 rounded text-sm font-semibold
                      hover:bg-mint-400 disabled:opacity-50"
        >
          {busy ? 'Triggering…' : 'Refresh token now'}
        </button>
        <PushSubscribeButton onFlash={setFlash} onError={setError} />
        <button
          type="button" onClick={refresh}
          className="text-xs text-neutral-400 underline hover:text-neutral-200"
        >
          reload status
        </button>
      </div>

      {flash && (
        <p role="status" className="px-3 py-2 rounded bg-green-500/10 text-green-200
                                      border border-green-500/40 text-xs">
          {flash}
        </p>
      )}
      {error && <ErrorBox text={error} />}

      <details className="text-xs">
        <summary className="cursor-pointer text-neutral-500 hover:text-neutral-300">
          Recent attempts ({log?.length ?? 0})
        </summary>
        {log && log.length > 0 ? (
          <table className="w-full mt-2">
            <thead className="text-neutral-500">
              <tr>
                <th className="text-left py-1">At (IST)</th>
                <th className="text-left">#</th>
                <th className="text-left">Trigger</th>
                <th className="text-left">Status</th>
                <th className="text-left">Stage / error</th>
                <th className="text-right">ms</th>
              </tr>
            </thead>
            <tbody>
              {log.map(e => (
                <tr key={e.id} className="border-t border-neutral-800">
                  <td className="font-mono py-1 text-neutral-400">{fmtIst(e.attempt_at)}</td>
                  <td className="font-mono">{e.attempt_of_day || '—'}</td>
                  <td className="text-neutral-400">{e.trigger_kind}</td>
                  <td className={`font-mono ${
                    e.status === 'success' ? 'text-green-300'
                    : e.status === 'skipped' ? 'text-neutral-400'
                    : 'text-red-300'
                  }`}>{e.status}</td>
                  <td className="text-neutral-400 font-mono truncate max-w-[18rem]">
                    {e.error_code ? `${e.error_code}${e.stage ? ` · ${e.stage}` : ''}` : (e.stage ?? '—')}
                  </td>
                  <td className="text-right font-mono text-neutral-500">{e.elapsed_ms ?? '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p className="mt-2 text-neutral-500">No attempts logged yet.</p>
        )}
      </details>
    </section>
  )
}


function KV({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="bg-neutral-950/60 border border-neutral-800 rounded p-2">
      <div className="text-[10px] uppercase tracking-wider text-neutral-500">{label}</div>
      <div className={`text-sm ${mono ? 'font-mono' : ''} text-neutral-200 truncate`} title={value}>
        {value}
      </div>
    </div>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// Push subscription — service-worker register + VAPID subscribe + POST keys
// ─────────────────────────────────────────────────────────────────────────

function PushSubscribeButton({
  onFlash, onError,
}: { onFlash: (m: string) => void; onError: (m: string) => void }) {
  const [state, setState] = useState<'idle' | 'subscribing' | 'subscribed' | 'unsupported' | 'blocked'>('idle')

  useEffect(() => {
    // Detect support + current state
    if (typeof window === 'undefined') return
    if (!('serviceWorker' in navigator) || !('PushManager' in window)) {
      setState('unsupported'); return
    }
    if (Notification.permission === 'denied') {
      setState('blocked'); return
    }
    void (async () => {
      try {
        const reg = await navigator.serviceWorker.getRegistration('/sw-push.js')
        const sub = await reg?.pushManager.getSubscription()
        if (sub) setState('subscribed')
      } catch { /* leave idle */ }
    })()
  }, [])

  const onSubscribe = async () => {
    setState('subscribing')
    try {
      // 1. Permission
      if (Notification.permission !== 'granted') {
        const p = await Notification.requestPermission()
        if (p !== 'granted') {
          setState('blocked')
          onError('Push permission denied. Re-enable in browser site settings.')
          return
        }
      }
      // 2. Service worker register
      const reg = await navigator.serviceWorker.register('/sw-push.js', { scope: '/' })
      await navigator.serviceWorker.ready
      // 3. Fetch VAPID public key
      const r = await fetch(`${apiBase()}/api/power/admin/push/vapid-public-key`,
                            adminFetchInit())
      if (!r.ok) throw new Error(`vapid: HTTP ${r.status}`)
      const { key, configured } = await r.json()
      if (configured !== 'true' || !key) {
        throw new Error('VAPID not configured server-side. Set VAPID_PUBLIC_KEY + VAPID_PRIVATE_KEY in backend env.')
      }
      // 4. Subscribe with applicationServerKey
      const sub = await reg.pushManager.subscribe({
        userVisibleOnly:       true,
        applicationServerKey:  urlBase64ToUint8Array(key) as BufferSource,
      })
      // 5. POST endpoint + keys to backend
      const json = sub.toJSON() as { endpoint: string; keys?: { p256dh: string; auth: string } }
      if (!json.endpoint || !json.keys?.p256dh || !json.keys?.auth) {
        throw new Error('Browser returned an incomplete PushSubscription.')
      }
      const post = await fetch(`${apiBase()}/api/power/admin/push/subscribe`, adminFetchInit({
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({
          endpoint:    json.endpoint,
          p256dh:      json.keys.p256dh,
          auth:        json.keys.auth,
          user_agent:  navigator.userAgent.slice(0, 200),
        }),
      }))
      if (!post.ok) throw new Error(`subscribe: HTTP ${post.status}`)
      setState('subscribed')
      onFlash('Push notifications enabled — you\'ll get a tap-to-refresh link if auto-auth fails 4× tomorrow.')
    } catch (e) {
      setState('idle')
      onError(e instanceof Error ? e.message : 'Push subscribe failed.')
    }
  }

  if (state === 'unsupported') {
    return <span className="text-xs text-neutral-500">Push not supported in this browser.</span>
  }
  if (state === 'subscribed') {
    return (
      <span className="inline-flex items-center text-xs text-green-300 px-2 py-1 rounded border border-green-500/30 font-mono">
        ✓ Push enabled
      </span>
    )
  }
  return (
    <button
      type="button" onClick={onSubscribe} disabled={state === 'subscribing' || state === 'blocked'}
      className="px-3 py-1.5 border border-mint-500/40 text-mint-300 rounded text-sm font-semibold
                  hover:bg-mint-500/10 disabled:opacity-50"
    >
      {state === 'subscribing' ? 'Subscribing…'
        : state === 'blocked' ? 'Push blocked — re-enable in site settings'
        : 'Enable push fallback'}
    </button>
  )
}


function urlBase64ToUint8Array(base64String: string): Uint8Array {
  const padding = '='.repeat((4 - (base64String.length % 4)) % 4)
  const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/')
  const raw = atob(base64)
  const out = new Uint8Array(raw.length)
  for (let i = 0; i < raw.length; ++i) out[i] = raw.charCodeAt(i)
  return out
}


function fmtIst(iso: string | null | undefined): string {
  if (!iso) return '—'
  try {
    const d = new Date(iso)
    if (isNaN(d.getTime())) return iso
    // Render IST (UTC+5:30) with day + HH:mm
    const parts = new Intl.DateTimeFormat('en-IN', {
      timeZone: 'Asia/Kolkata', month: 'short', day: '2-digit',
      hour: '2-digit', minute: '2-digit', hour12: false,
    }).formatToParts(d)
    const get = (t: string) => parts.find(p => p.type === t)?.value ?? ''
    return `${get('day')} ${get('month')} ${get('hour')}:${get('minute')}`
  } catch {
    return iso
  }
}


// ─────────────────────────────────────────────────────────────────────────
// Metrics panel
// ─────────────────────────────────────────────────────────────────────────

type Metrics = {
  users:   { total_active: number; dau: number; wau: number }
  invites: { codes_used: number; codes_unused: number; waitlist: number }
  top_routes_7d: Array<{ route: string; n: number; avg_ms: number }>
}

function MetricsPanel() {
  const [data, setData]   = useState<Metrics | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetch(`${apiBase()}/api/power/admin/metrics`, adminFetchInit())
      .then(async r => {
        if (!r.ok) {
          const b = await r.json().catch(() => ({}))
          throw new Error(b.detail?.message || `HTTP ${r.status}`)
        }
        return r.json()
      })
      .then((m: Metrics) => setData(m))
      .catch((e: Error) => setError(e.message))
  }, [])

  if (error) return <ErrorBox text={`Metrics: ${error}`} />
  if (!data) return <SkeletonBox lines={3} />

  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
      <h2 className="text-sm font-semibold text-neutral-300 mb-3">Cohort metrics</h2>
      <div className="grid grid-cols-2 md:grid-cols-6 gap-3 text-center text-sm">
        <Cell label="Active users" value={data.users.total_active} />
        <Cell label="DAU"          value={data.users.dau} />
        <Cell label="WAU"          value={data.users.wau} />
        <Cell label="Codes used"   value={data.invites.codes_used} />
        <Cell label="Codes unused" value={data.invites.codes_unused} accent="amber" />
        <Cell label="Waitlist"     value={data.invites.waitlist} />
      </div>
      {data.top_routes_7d.length > 0 && (
        <div className="mt-4 pt-4 border-t border-neutral-800">
          <h3 className="text-xs uppercase tracking-wider text-neutral-500 mb-2">Top routes (7d)</h3>
          <table className="w-full text-xs">
            <thead className="text-neutral-500">
              <tr><th className="text-left py-1">Route</th><th className="text-right">N</th><th className="text-right">Avg ms</th></tr>
            </thead>
            <tbody>
              {data.top_routes_7d.map(r => (
                <tr key={r.route} className="border-t border-neutral-800">
                  <td className="font-mono py-1 text-neutral-300">{r.route}</td>
                  <td className="text-right font-mono">{r.n}</td>
                  <td className="text-right font-mono text-neutral-400">{r.avg_ms}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// Waitlist panel — surfaces /api/power/admin/waitlist + per-row "Issue invite"
//
// MVP: shows the list, one-click issues a code with note=email, displays
// the code inline with a copy button. We do NOT mark waitlist.invite_issued
// because the backend has no update endpoint yet — the freshly-issued code
// stays visible until reload. Future enhancement: POST /waitlist/{email}/mark.
// ─────────────────────────────────────────────────────────────────────────

type WaitlistRow = {
  email:           string
  joined_at:       string
  source:          string | null
  invite_issued:   number    // 0 or 1
}

function WaitlistPanel() {
  const [rows, setRows]   = useState<WaitlistRow[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy]   = useState<string | null>(null)            // email being issued
  const [codes, setCodes] = useState<Record<string, string>>({})     // email → freshly-issued code

  const refresh = async () => {
    try {
      const r = await fetch(`${apiBase()}/api/power/admin/waitlist`, adminFetchInit())
      if (!r.ok) {
        const b = await r.json().catch(() => ({}))
        throw new Error(b.detail?.message || `HTTP ${r.status}`)
      }
      const d = await r.json()
      setRows(d.waitlist || [])
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Waitlist fetch failed.')
    }
  }

  useEffect(() => { void refresh() }, [])

  const onIssue = async (email: string) => {
    setBusy(email); setError(null)
    try {
      const r = await fetch(`${apiBase()}/api/power/admin/invites/issue`, adminFetchInit({
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ n: 1, expires_in_days: null, note: email }),
      }))
      if (!r.ok) {
        const b = await r.json().catch(() => ({}))
        throw new Error(b.detail?.message || `HTTP ${r.status}`)
      }
      const body = await r.json()
      const code = body.codes?.[0]
      if (!code) throw new Error('Backend returned no code')
      setCodes(prev => ({ ...prev, [email]: code }))
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Issue failed.')
    } finally {
      setBusy(null)
    }
  }

  if (error && !rows) return <ErrorBox text={`Waitlist: ${error}`} />
  if (!rows)          return <SkeletonBox lines={3} />

  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
      <header className="flex items-baseline justify-between mb-3 gap-3">
        <div>
          <h2 className="text-sm font-semibold text-neutral-300">Waitlist ({rows.length})</h2>
          <p className="text-xs text-neutral-500 mt-0.5">
            People who submitted their email at /power/waitlist. Click &quot;Issue invite&quot; to mint a code with their email as the note — copy the code and send via WhatsApp/email.
          </p>
        </div>
        <button
          type="button"
          onClick={refresh}
          className="text-xs text-neutral-400 underline hover:text-neutral-200 shrink-0"
        >
          reload
        </button>
      </header>

      {error && <ErrorBox text={error} />}

      {rows.length === 0 ? (
        <p className="text-sm text-neutral-500">Nobody on the waitlist yet.</p>
      ) : (
        <table className="w-full text-xs">
          <thead className="text-neutral-500">
            <tr>
              <th className="text-left py-1">Email</th>
              <th className="text-left">Joined (IST)</th>
              <th className="text-left">Source</th>
              <th className="text-right">Invite</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(r => (
              <tr key={r.email} className="border-t border-neutral-800 align-top">
                <td className="font-mono py-1 text-neutral-100 select-all">{r.email}</td>
                <td className="text-neutral-400 font-mono">{fmtIst(r.joined_at)}</td>
                <td className="text-neutral-500">{r.source ?? '—'}</td>
                <td className="text-right">
                  {codes[r.email] ? (
                    <span className="inline-flex items-center gap-2">
                      <span className="font-mono text-mint-300 select-all">{codes[r.email]}</span>
                      <button
                        type="button"
                        onClick={() => navigator.clipboard?.writeText(codes[r.email])}
                        className="text-mint-400 underline hover:text-mint-300"
                      >
                        copy
                      </button>
                    </span>
                  ) : (
                    <button
                      type="button"
                      onClick={() => onIssue(r.email)}
                      disabled={busy === r.email}
                      className="px-2 py-1 bg-mint-500/20 text-mint-300 border border-mint-500/40 rounded hover:bg-mint-500/30 disabled:opacity-50"
                    >
                      {busy === r.email ? 'issuing…' : 'Issue invite'}
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// Invite issuance
// ─────────────────────────────────────────────────────────────────────────

function InviteIssuancePanel() {
  const [n, setN] = useState(5)
  const [days, setDays] = useState<number | ''>('')
  const [note, setNote] = useState('')
  const [busy, setBusy] = useState(false)
  const [result, setResult] = useState<{ codes: string[]; note: string | null } | null>(null)
  const [error, setError] = useState<string | null>(null)

  const onIssue = async () => {
    setBusy(true); setError(null); setResult(null)
    try {
      const r = await fetch(`${apiBase()}/api/power/admin/invites/issue`, adminFetchInit({
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({
          n,
          expires_in_days: days === '' ? null : days,
          note:            note || null,
        }),
      }))
      if (!r.ok) {
        const b = await r.json().catch(() => ({}))
        throw new Error(b.detail?.message || `HTTP ${r.status}`)
      }
      const body = await r.json()
      setResult({ codes: body.codes, note: body.note })
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Issue failed.')
    } finally {
      setBusy(false)
    }
  }

  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
      <h2 className="text-sm font-semibold text-neutral-300 mb-3">Issue invites</h2>
      <div className="grid grid-cols-1 md:grid-cols-4 gap-3 items-end">
        <Field label="Count">
          <input type="number" min={1} max={100} value={n}
                  onChange={e => setN(Math.max(1, Math.min(100, +e.target.value || 1)))}
                  className="w-full bg-neutral-950 border border-neutral-700 rounded px-2 py-1 font-mono" />
        </Field>
        <Field label="Expires in (days)">
          <input type="number" min={1} max={365} value={days}
                  onChange={e => setDays(e.target.value === '' ? '' : +e.target.value)}
                  placeholder="never"
                  className="w-full bg-neutral-950 border border-neutral-700 rounded px-2 py-1 font-mono" />
        </Field>
        <Field label="Note (optional)">
          <input type="text" value={note}
                  onChange={e => setNote(e.target.value)}
                  placeholder="Influencer cohort A"
                  className="w-full bg-neutral-950 border border-neutral-700 rounded px-2 py-1" />
        </Field>
        <button
          type="button" onClick={onIssue} disabled={busy}
          className="px-4 py-2 bg-mint-400 text-neutral-950 rounded font-semibold hover:bg-mint-300 disabled:opacity-50"
        >
          {busy ? 'Issuing…' : `Issue ${n}`}
        </button>
      </div>
      {error && <ErrorBox text={error} />}
      {result && (
        <div className="mt-4 bg-neutral-950/60 border border-green-500/30 rounded p-3 space-y-2">
          <div className="flex items-baseline justify-between">
            <p className="text-sm text-green-300">✓ {result.codes.length} codes issued{result.note && ` · "${result.note}"`}</p>
            <button
              type="button"
              onClick={() => navigator.clipboard?.writeText(result.codes.join('\n'))}
              className="text-xs text-mint-400 underline hover:text-mint-300"
            >
              copy all
            </button>
          </div>
          <ul className="space-y-1">
            {result.codes.map(c => (
              <li key={c} className="font-mono text-sm text-neutral-200 select-all">{c}</li>
            ))}
          </ul>
        </div>
      )}
    </section>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// Invite list
// ─────────────────────────────────────────────────────────────────────────

type InviteRow = {
  code: string
  issued_at: string
  expires_at: string | null
  used_by_user_id: number | null
  used_at: string | null
  note: string | null
  /** NEW (2026-05-25): when used_by_user_id is set, this reflects the
   *  power_user_users.is_active flag for that user. NULL when the code
   *  is unused (no joined user row). Drives the "deactivate / reactivate"
   *  button rendering on consumed rows. */
  used_by_is_active: number | null     // SQLite stores bool as 0/1
}

function InviteListPanel() {
  const [rows, setRows]   = useState<InviteRow[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [onlyUnused, setOnlyUnused] = useState(false)
  const [busy, setBusy]   = useState<string | null>(null)        // code being revoked

  // Codes never auto-expire by default (2026-05-25). Treat any row whose
  // expires_at is in the past as "revoked" — that's how the backend marks
  // a manually-deactivated invite.
  const isRevoked = (r: InviteRow): boolean => {
    if (!r.expires_at) return false
    const exp = new Date(r.expires_at).getTime()
    return Number.isFinite(exp) && exp <= Date.now()
  }

  const fetchRows = () => {
    fetch(`${apiBase()}/api/power/admin/invites/list?only_unused=${onlyUnused}`,
          adminFetchInit())
      .then(async r => {
        if (!r.ok) {
          const b = await r.json().catch(() => ({}))
          throw new Error(b.detail?.message || `HTTP ${r.status}`)
        }
        return r.json()
      })
      .then(d => setRows(d.codes))
      .catch((e: Error) => setError(e.message))
  }

  useEffect(fetchRows, [onlyUnused])

  const onRevoke = async (code: string) => {
    if (!confirm(`Revoke ${code}? The invitee will no longer be able to use this code.`)) return
    setBusy(code); setError(null)
    try {
      const r = await fetch(`${apiBase()}/api/power/admin/invites/${encodeURIComponent(code)}/revoke`,
        adminFetchInit({ method: 'POST' }))
      if (!r.ok) {
        const b = await r.json().catch(() => ({}))
        throw new Error(b.detail?.message || `HTTP ${r.status}`)
      }
      fetchRows()                                              // re-read so the UI flips to "revoked"
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Revoke failed.')
    } finally {
      setBusy(null)
    }
  }

  // Deactivate / reactivate the user account that consumed this code.
  // The backend blocks deactivating admin users (HTTP 400 CANNOT_TOUCH_ADMIN),
  // so we don't need to gate the button — just let the API return the message.
  const onToggleUserActive = async (userId: number, code: string, currentlyActive: boolean) => {
    const verb = currentlyActive ? 'deactivate' : 'reactivate'
    const msg  = currentlyActive
      ? `Deactivate user #${userId}? They will be blocked from logging in.`
      : `Reactivate user #${userId}? They will be able to log in again.`
    if (!confirm(msg)) return
    setBusy(code); setError(null)
    try {
      const r = await fetch(
        `${apiBase()}/api/power/admin/users/${userId}/${verb}`,
        adminFetchInit({ method: 'POST' }),
      )
      if (!r.ok) {
        const b = await r.json().catch(() => ({}))
        throw new Error(b.detail?.message || `HTTP ${r.status}`)
      }
      fetchRows()
    } catch (e) {
      setError(e instanceof Error ? e.message : `${verb} failed.`)
    } finally {
      setBusy(null)
    }
  }

  if (error && !rows) return <ErrorBox text={`List: ${error}`} />
  if (!rows)          return <SkeletonBox lines={3} />

  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
      <div className="flex items-baseline justify-between mb-3 gap-3">
        <div>
          <h2 className="text-sm font-semibold text-neutral-300">Invite history ({rows.length})</h2>
          <p className="text-xs text-neutral-500 mt-0.5">
            Codes never auto-expire. Click <span className="font-mono">revoke</span> to deactivate any unused code.
          </p>
        </div>
        <label className="text-xs text-neutral-400 flex items-center gap-1 shrink-0">
          <input type="checkbox" checked={onlyUnused}
                  onChange={e => setOnlyUnused(e.target.checked)} />
          unused only
        </label>
      </div>
      {error && <ErrorBox text={error} />}
      {rows.length === 0 ? (
        <p className="text-sm text-neutral-500">No codes yet.</p>
      ) : (
        <table className="w-full text-xs">
          <thead className="text-neutral-500">
            <tr>
              <th className="text-left py-1">Code</th>
              <th className="text-left">Issued</th>
              <th className="text-left">Note</th>
              <th className="text-left">Status</th>
              <th className="text-right">Action</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(r => {
              const used        = r.used_by_user_id != null
              const userActive  = used && r.used_by_is_active === 1
              const userOff     = used && r.used_by_is_active === 0
              const revoked     = !used && isRevoked(r)
              return (
                <tr key={r.code} className="border-t border-neutral-800">
                  <td className="font-mono py-1 text-neutral-200">{r.code}</td>
                  <td className="text-neutral-500">{r.issued_at?.slice(0, 16).replace('T', ' ')}</td>
                  <td className="text-neutral-400 truncate max-w-[12rem]">{r.note ?? '—'}</td>
                  <td className="font-mono">
                    {userActive ? <span className="text-neutral-300">user #{r.used_by_user_id}</span>
                     : userOff   ? <span className="text-red-300">user #{r.used_by_user_id} · deactivated</span>
                     : revoked   ? <span className="text-red-300">revoked</span>
                                 : <span className="text-green-300">unused</span>}
                  </td>
                  <td className="text-right">
                    {!used && !revoked && (
                      <button
                        type="button"
                        onClick={() => onRevoke(r.code)}
                        disabled={busy === r.code}
                        className="px-2 py-0.5 text-[11px] text-red-300 hover:text-red-200 hover:bg-red-500/10 border border-red-500/30 rounded disabled:opacity-50"
                      >
                        {busy === r.code ? 'revoking…' : 'revoke'}
                      </button>
                    )}
                    {userActive && (
                      <button
                        type="button"
                        onClick={() => onToggleUserActive(r.used_by_user_id!, r.code, true)}
                        disabled={busy === r.code}
                        className="px-2 py-0.5 text-[11px] text-red-300 hover:text-red-200 hover:bg-red-500/10 border border-red-500/30 rounded disabled:opacity-50"
                      >
                        {busy === r.code ? 'deactivating…' : 'deactivate'}
                      </button>
                    )}
                    {userOff && (
                      <button
                        type="button"
                        onClick={() => onToggleUserActive(r.used_by_user_id!, r.code, false)}
                        disabled={busy === r.code}
                        className="px-2 py-0.5 text-[11px] text-mint-300 hover:text-mint-200 hover:bg-mint-500/10 border border-mint-500/30 rounded disabled:opacity-50"
                      >
                        {busy === r.code ? 'reactivating…' : 'reactivate'}
                      </button>
                    )}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      )}
    </section>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// Tiny presentational helpers
// ─────────────────────────────────────────────────────────────────────────

function Cell({ label, value, accent }: { label: string; value: number; accent?: 'amber' }) {
  return (
    <div className={`bg-neutral-950/60 border rounded p-2 ${
      accent === 'amber' ? 'border-mint-500/30' : 'border-neutral-800'
    }`}>
      <div className="text-[10px] uppercase tracking-wider text-neutral-500">{label}</div>
      <div className={`text-xl font-bold font-mono ${
        accent === 'amber' ? 'text-mint-300' : 'text-neutral-100'
      }`}>
        {value}
      </div>
    </div>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="block text-xs">
      <span className="block text-neutral-500 mb-1">{label}</span>
      {children}
    </label>
  )
}

function ErrorBox({ text }: { text: string }) {
  return (
    <p role="alert" className="mt-3 px-3 py-2 rounded bg-red-500/10 text-red-200
                                  border border-red-500/40 text-xs">
      {text}
    </p>
  )
}

function SkeletonBox({ lines }: { lines: number }) {
  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded p-4 animate-pulse space-y-2">
      {Array.from({ length: lines }).map((_, i) =>
        <div key={i} className="h-4 bg-neutral-800 rounded" style={{ width: `${100 - i * 15}%` }} />
      )}
    </section>
  )
}


// ─────────────────────────────────────────────────────────────────────────
// Tier rulebook review/approve — "what the AI learned this week" (Phase 2).
// Surfaces the weekly shadow learner's champion-vs-challenger snapshot and
// lets an admin promote a challenger to active (gated server-side: N>=50).
// ─────────────────────────────────────────────────────────────────────────

type TierReviewRule = {
  rule_id: string
  tier: string
  champion: { wr: number | null; ret: number | null; n: number | null }
  challenger: { wr: number | null; ret: number | null; n: number | null; as_of: string | null } | null
  wr_delta: number | null
  conditions_changed: boolean
}
type TierReviewResp = {
  rules: TierReviewRule[]
  has_divergence: boolean
  challenger_as_of: string | null
  n_active: number
  n_challenger: number
  note?: string
}

function TierReviewPanel() {
  const [data,  setData]  = useState<TierReviewResp | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy,  setBusy]  = useState(false)
  const [flash, setFlash] = useState<string | null>(null)

  const refresh = async () => {
    try {
      const r = await fetch(`${apiBase()}/api/power/admin/tier-review`, adminFetchInit())
      if (!r.ok) throw new Error('HTTP ' + r.status)
      setData(await r.json()); setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Tier-review fetch failed.')
    }
  }
  useEffect(() => { refresh() }, [])

  const promote = async (rid: string) => {
    if (typeof window !== 'undefined' &&
        !window.confirm(`Promote "${rid}" challenger → active?\nThe live classifier picks it up within ~5 min.`)) return
    setBusy(true); setFlash(null)
    try {
      const r = await fetch(`${apiBase()}/api/power/admin/tier-promote`,
        adminFetchInit({ method: 'POST', headers: { 'Content-Type': 'application/json' },
                         body: JSON.stringify({ rule_id: rid }) }))
      const j = await r.json()
      if (!r.ok) throw new Error(j?.detail?.message || j?.message || ('HTTP ' + r.status))
      setFlash(`Promoted ${rid} (N=${j.challenger_n}, WR=${j.challenger_wr}%).`)
      await refresh()
    } catch (e) {
      setFlash(e instanceof Error ? e.message : 'Promote failed.')
    } finally { setBusy(false) }
  }

  const pct = (n: number | null | undefined) => n == null ? '—' : `${n}%`
  const num = (n: number | null | undefined) => n == null ? '—' : `${n}`

  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded-lg p-4">
      <div className="flex items-baseline justify-between mb-1">
        <h2 className="text-lg font-semibold">Tier rulebook — what the AI learned</h2>
        <button onClick={refresh} className="text-xs text-neutral-400 hover:text-neutral-200">refresh</button>
      </div>
      <p className="text-sm text-neutral-400 mb-3">
        Weekly shadow learner snapshot{data?.challenger_as_of ? ` · as of ${data.challenger_as_of}` : ''}.
        {' '}Promotion is gated (challenger N≥50) and applies to the live classifier within ~5 min.
      </p>

      {error && <div className="text-red-300 text-sm mb-2">Error: {error}</div>}
      {flash && <div className="text-mint-300 text-sm mb-2">{flash}</div>}
      {data?.note && <div className="text-yellow-300 text-sm mb-2">{data.note}</div>}

      {data && !data.has_divergence && (
        <div className="text-sm text-neutral-300 bg-neutral-800/40 border border-neutral-700 rounded p-3 mb-3">
          No divergence yet — champion and challenger match. The learner re-grades the live
          rules every Sunday; rules to approve light up here once their metrics drift or the
          learner proposes new thresholds.
        </div>
      )}

      {data && data.rules.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-neutral-500 text-left text-xs">
                <th className="py-1 pr-3">rule</th>
                <th className="pr-3">tier</th>
                <th className="pr-3">champion WR / N</th>
                <th className="pr-3">challenger WR / N</th>
                <th className="pr-3">ΔWR</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {data.rules.map(r => {
                const d = r.wr_delta
                const dcls = d == null ? 'text-neutral-500'
                          : d > 0 ? 'text-green-300' : d < 0 ? 'text-red-300' : 'text-neutral-400'
                const canPromote = !!r.challenger && (r.challenger.n ?? 0) >= 50 &&
                                   (r.conditions_changed || (d != null && Math.abs(d) >= 1))
                return (
                  <tr key={r.rule_id} className="border-t border-neutral-800">
                    <td className="py-1.5 pr-3 font-mono text-neutral-200">{r.rule_id}</td>
                    <td className="pr-3 text-neutral-300">{r.tier}</td>
                    <td className="pr-3 font-mono">{pct(r.champion.wr)} / {num(r.champion.n)}</td>
                    <td className="pr-3 font-mono">
                      {r.challenger ? `${pct(r.challenger.wr)} / ${num(r.challenger.n)}` : '—'}
                    </td>
                    <td className={`pr-3 font-mono ${dcls}`}>
                      {d == null ? '—' : (d > 0 ? '+' : '') + d}
                      {r.conditions_changed && <span className="text-yellow-300 ml-1" title="rule thresholds changed">*</span>}
                    </td>
                    <td>
                      {canPromote && (
                        <button disabled={busy} onClick={() => promote(r.rule_id)}
                          className="text-xs px-2 py-0.5 rounded border border-mint-500/40 text-mint-300 hover:bg-mint-500/10 disabled:opacity-50">
                          Approve
                        </button>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
      {!data && !error && <div className="text-sm text-neutral-500">Loading…</div>}
    </section>
  )
}
