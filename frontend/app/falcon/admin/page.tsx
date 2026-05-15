'use client'
import { useEffect, useState } from 'react'
import { FalconAPI, FalconJobRun, FalconStatus } from '../../../lib/falcon-api'
import { fetchKiteStatus, refreshKiteToken, type KiteStatus } from '@/lib/admin-api'
import { PreflightBanner } from '@/components/PreflightBanner'

const JOBS = [
  { id: 'daily_data_refresh', label: 'Daily OHLC refresh' },
  { id: 'daily_features',     label: 'Daily features' },
  { id: 'daily_signals',      label: 'Daily signals' },
  { id: 'weekly_remine',      label: 'Weekly re-mine (heavy)' },
]

const SECRET_KEY = 'kanida_admin_secret'
const KITE_API_KEY = process.env.NEXT_PUBLIC_KITE_API_KEY || ''

export default function FalconAdminPage() {
  const [status, setStatus] = useState<FalconStatus | null>(null)
  const [runs, setRuns] = useState<FalconJobRun[]>([])
  const [busy, setBusy] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)

  const refresh = () => {
    FalconAPI.adminStatus().then(setStatus).catch(() => {})
    FalconAPI.adminRuns(20).then(setRuns).catch(() => {})
  }

  useEffect(() => {
    refresh()
    const t = setInterval(refresh, 10000)
    return () => clearInterval(t)
  }, [])

  const triggerJob = async (jobId: string) => {
    setBusy(jobId); setMsg(null)
    try {
      const r = await FalconAPI.adminRerun(jobId)
      setMsg(`${r.job_name}: ${r.status}. ${r.tip}`)
      refresh()
    } catch (e) {
      setMsg(`Failed: ${e}`)
    } finally {
      setBusy(null)
    }
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Falcon — Admin</h1>

      {/* Preflight: the single structural health signal. Renders red/yellow
          when something blocks auto-trade; minimal green pill when all clear. */}
      <PreflightBanner alwaysShow />

      {/* Zerodha Auth — first, because the user needs token valid before triggering jobs */}
      <ZerodhaAuthSection />

      {status && (
        <>
          <PatternSyncBanner status={status} />
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
            <Stat label="Engine"    value={status.engine_version} accent />
            <Stat label="Patterns"  value={String(status.n_promoted_patterns)} />
            <Stat label="Signals"   value={String(status.n_signals_emitted)} />
            <Stat label="Latest"    value={status.latest_signal_date ?? '—'} />
            <Stat label="Features"  value={status.n_features_rows.toLocaleString()} />
            <Stat label="Outcomes"  value={status.n_outcomes_rows.toLocaleString()} />
            <Stat label="DB size"   value={`${status.db_size_mb.toFixed(0)} MB`} />
            <Stat label="DB path"   value={status.db_path.split('/').slice(-2).join('/')} />
          </div>
        </>
      )}

      <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
        <h2 className="text-lg font-semibold mb-3">Manual job triggers</h2>
        {msg && <div className="text-amber-400 text-sm mb-3">{msg}</div>}
        <div className="flex flex-wrap gap-2">
          {JOBS.map(j => (
            <button key={j.id} onClick={() => triggerJob(j.id)}
                    disabled={!!busy}
                    className="bg-amber-500/20 border border-amber-500/40 hover:bg-amber-500/30
                               text-amber-400 rounded px-3 py-2 text-sm disabled:opacity-50">
              {busy === j.id ? '⏳ ' : '▶ '} {j.label}
            </button>
          ))}
        </div>
      </section>

      <section className="bg-neutral-900 border border-neutral-800 rounded">
        <h2 className="text-lg font-semibold p-4 pb-2">Recent runs</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-neutral-400">
              <tr>
                <th className="text-left px-4 py-2">Job</th>
                <th className="text-left">Status</th>
                <th className="text-left">Started</th>
                <th className="text-left">Finished</th>
                <th className="text-right">Rows</th>
                <th className="text-left pl-4">Notes / Error</th>
              </tr>
            </thead>
            <tbody>
              {runs.map(r => (
                <tr key={r.id} className="border-t border-neutral-800">
                  <td className="px-4 py-2 font-medium">{r.job_name}</td>
                  <td>
                    <span className={
                      r.status === 'success' ? 'text-emerald-400'
                        : r.status === 'failed' ? 'text-red-400'
                        : 'text-amber-400'
                    }>{r.status}</span>
                  </td>
                  <td className="text-neutral-400">{r.started_at}</td>
                  <td className="text-neutral-400">{r.finished_at ?? '—'}</td>
                  <td className="text-right">{r.rows_affected ?? 0}</td>
                  <td className="pl-4 text-xs text-neutral-300">
                    {r.error ? <span className="text-red-400">{r.error}</span> : (r.notes ?? '')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}

function Stat({ label, value, accent }: { label: string; value: string; accent?: boolean }) {
  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-3">
      <div className="text-neutral-500 text-xs">{label}</div>
      <div className={`text-base font-semibold ${accent ? 'text-amber-400' : ''}`}>{value}</div>
    </div>
  )
}

// ─── Pattern sync banner (Workflow B4 visibility) ───────────────────────────
// Shows the result of the latest publish_patterns.py run. Three states:
//   green = recent success (R&D->PROD synced)
//   amber = stale (last sync >7 days OR never run)
//   red   = last run failed
// Goal: any drift between R&D and PROD is loud and operator-visible.
function PatternSyncBanner({ status }: { status: FalconStatus }) {
  const lastIso     = status.patterns_last_published        // 'YYYY-MM-DD HH:MM:SS' or null
  const lastStatus  = status.patterns_last_published_status // 'success' | 'failed' | null
  const notes       = status.patterns_last_published_notes  // free text
  const nPatterns   = status.n_promoted_patterns

  // Compute staleness (days since last successful sync)
  let daysSince: number | null = null
  if (lastIso) {
    const t = Date.parse(lastIso.replace(' ', 'T'))  // SQLite stores 'YYYY-MM-DD HH:MM:SS'
    if (!Number.isNaN(t)) daysSince = Math.floor((Date.now() - t) / 86400_000)
  }

  // State determination
  const isFailed  = lastStatus === 'failed'
  const isMissing = !lastIso || nPatterns === 0
  const isStale   = lastStatus === 'success' && daysSince !== null && daysSince > 7

  const tone =
    isFailed                   ? 'red'   :
    isMissing                  ? 'red'   :
    isStale                    ? 'amber' :
                                 'green'

  const cls = {
    green: 'bg-green-500/10 border-green-500/30 text-green-200',
    amber: 'bg-amber-500/10 border-amber-500/30 text-amber-200',
    red:   'bg-red-500/10   border-red-500/30   text-red-200',
  }[tone]

  const headline =
    isFailed  ? `✗ Pattern sync FAILED on last run` :
    isMissing ? `⚠ Patterns NOT published to PROD — run publish_patterns.py` :
    isStale   ? `⚠ Pattern sync stale — ${daysSince}d since last publish` :
                `✓ Patterns synced — ${nPatterns} promoted in PROD`

  return (
    <div className={`rounded border p-3 ${cls}`}>
      <div className="flex items-baseline justify-between gap-3 flex-wrap">
        <div className="text-sm font-semibold">{headline}</div>
        <div className="text-[11px] text-neutral-400 font-mono">
          {lastIso ? `last publish: ${lastIso}` : 'never published'}
        </div>
      </div>
      {notes && (
        <div className="mt-1 text-xs text-neutral-300/80 font-mono truncate">
          {notes}
        </div>
      )}
      {(isMissing || isFailed) && (
        <div className="mt-2 text-[11px] text-neutral-400">
          Run from project root: <code className="text-neutral-200">python scripts/publish_patterns.py</code>
        </div>
      )}
    </div>
  )
}

// ─── Zerodha Auth section ────────────────────────────────────────────────────
// Migrated from the legacy /admin page — same business logic (fetchKiteStatus,
// refreshKiteToken), restyled to match Falcon's dark theme. Handles the OAuth
// callback (?request_token=...&status=success) on either /admin (which now
// redirects here preserving query params) or directly on /falcon/admin if the
// Kite developer console's redirect URL is updated.

function ZerodhaAuthSection() {
  const [secret, setSecret]               = useState('')
  const [requestToken, setRequestToken]   = useState('')
  const [status, setStatus]               = useState<{
    status:           'ok' | 'error'
    message?:         string
    token_preview?:   string
    railway_updated?: boolean
    pipeline?:        { kicked_off: boolean; reason_skipped: string | null }
  } | null>(null)
  const [loading, setLoading]             = useState(false)
  const [tokenStatus, setTokenStatus]     = useState<KiteStatus | null>(null)
  const [autoDetected, setAutoDetected]   = useState(false)
  const [secretSavedFlash, setSecretSavedFlash] = useState(false)

  const loginUrl = KITE_API_KEY
    ? `https://kite.zerodha.com/connect/login?api_key=${KITE_API_KEY}&v=3`
    : 'https://kite.zerodha.com'

  useEffect(() => {
    // Initial token status
    fetchKiteStatus().then(setTokenStatus).catch(() => {})

    // Restore admin secret from localStorage
    const saved = localStorage.getItem(SECRET_KEY)
    if (saved) setSecret(saved)

    // Auto-detect OAuth callback (request_token=... in URL, from Kite redirect)
    const params = new URLSearchParams(window.location.search)
    const rt = params.get('request_token')
    const ok = params.get('status')
    if (rt && ok === 'success') {
      setRequestToken(rt)
      setAutoDetected(true)
      // Strip query params from URL so refresh doesn't double-process
      window.history.replaceState({}, '', '/falcon/admin')
    }
  }, [])

  // When secret + request_token both present (auto-callback path), trigger refresh
  useEffect(() => {
    if (autoDetected && requestToken && secret) {
      doRefresh(requestToken, secret)
    }
  }, [autoDetected, requestToken, secret])

  async function doRefresh(rt: string, sec: string) {
    setLoading(true)
    setStatus(null)
    try {
      const data = await refreshKiteToken(rt, sec)
      setStatus({
        status:          'ok',
        token_preview:   data.token_preview,
        railway_updated: data.railway_updated,
        pipeline:        data.pipeline,
      })
      localStorage.setItem(SECRET_KEY, sec)
      setRequestToken('')
      setAutoDetected(false)
      // Re-fetch token status
      fetchKiteStatus().then(setTokenStatus).catch(() => {})
    } catch (e: unknown) {
      setStatus({ status: 'error', message: e instanceof Error ? e.message : String(e) })
    } finally {
      setLoading(false)
    }
  }

  const saveSecret = () => {
    localStorage.setItem(SECRET_KEY, secret)
    setSecretSavedFlash(true)
    setTimeout(() => setSecretSavedFlash(false), 2000)
  }

  const tokenValid = tokenStatus?.valid

  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded p-4 space-y-4">
      <div className="flex items-baseline justify-between">
        <h2 className="text-lg font-semibold">Zerodha Auth</h2>
        <span className="text-xs text-neutral-500">Token expires daily at midnight IST · refresh before 9:14</span>
      </div>

      {/* Token status */}
      <div className={
        'rounded border p-3 flex items-center justify-between '
        + (tokenStatus == null
          ? 'bg-neutral-950 border-neutral-800'
          : tokenValid
            ? 'bg-green-500/10 border-green-500/30'
            : 'bg-red-500/10 border-red-500/30')
      }>
        <div>
          <div className="text-xs text-neutral-400 mb-1">TOKEN STATUS</div>
          <div className={
            'text-sm font-medium '
            + (tokenStatus == null ? 'text-neutral-400'
                : tokenValid ? 'text-green-300' : 'text-red-300')
          }>
            {tokenStatus == null
              ? 'Checking…'
              : tokenValid
                ? `✓ Valid — ${tokenStatus.user ?? 'authenticated'}`
                : '✗ Expired — refresh required'}
          </div>
        </div>
        <button
          onClick={() => fetchKiteStatus().then(setTokenStatus).catch(() => {})}
          className="px-3 py-1.5 text-xs text-neutral-300 hover:text-neutral-100 border border-neutral-700 rounded">
          Recheck
        </button>
      </div>

      {/* Admin secret */}
      <div className="rounded bg-neutral-950 border border-neutral-800 p-3">
        <div className="text-xs text-neutral-400 mb-2">ADMIN SECRET</div>
        <div className="flex gap-2">
          <input
            type="password"
            value={secret}
            onChange={e => setSecret(e.target.value)}
            placeholder="Enter ADMIN_SECRET value"
            className="flex-1 bg-neutral-900 border border-neutral-700 rounded px-3 py-1.5 text-sm text-neutral-100 font-mono" />
          <button
            onClick={saveSecret}
            className="px-3 py-1.5 text-sm border border-neutral-700 hover:border-neutral-500 rounded">
            {secretSavedFlash ? '✓ Saved' : 'Save'}
          </button>
        </div>
        <div className="text-[11px] text-neutral-500 mt-1.5">
          Saved locally in your browser. Required for token refresh, manual job triggers, and the V7.1 pipeline.
        </div>
      </div>

      {/* One-click Zerodha login (only when invalid) */}
      {!tokenValid && !loading && (
        <div className="rounded bg-neutral-950 border border-amber-500/30 p-3">
          <div className="text-sm text-neutral-300 mb-2">
            Click below — Zerodha redirects back here automatically with a fresh token.
          </div>
          <a
            href={loginUrl}
            className="block text-center bg-amber-500 hover:bg-amber-400 text-neutral-950 px-4 py-2 rounded font-semibold">
            Login with Zerodha →
          </a>
        </div>
      )}

      {/* Loading state during auto-exchange */}
      {loading && (
        <div className="rounded bg-neutral-950 border border-amber-500/30 p-4 text-center">
          <div className="text-sm text-amber-300 animate-pulse">⟳ Authenticating with Zerodha…</div>
          <div className="text-xs text-neutral-500 mt-1">Exchanging request_token for access_token</div>
        </div>
      )}

      {/* Success / error after exchange */}
      {status?.status === 'ok' && (
        <div className="rounded bg-green-500/10 border border-green-500/30 p-3 text-sm space-y-2">
          <div>
            <div className="text-green-300 font-semibold mb-1">✓ Authentication complete</div>
            <div className="text-green-200/80">
              Token saved to DB — all services use it automatically.
              {status.railway_updated && ' Railway env also updated.'}
            </div>
            {status.token_preview && (
              <div className="text-xs text-neutral-500 mt-2">
                Preview: <code className="text-neutral-300">{status.token_preview}</code>
              </div>
            )}
          </div>
          {/* Pipeline kick-off indicator (auto-runs on token refresh) */}
          {status.pipeline?.kicked_off && (
            <div className="rounded bg-amber-500/10 border border-amber-500/30 p-2 text-xs">
              <div className="text-amber-300 font-semibold animate-pulse">
                ⟳ Daily pipeline kicked off
              </div>
              <div className="text-amber-200/80 mt-1">
                <code>daily_data_refresh</code> → <code>daily_features</code> → <code>daily_signals</code>
                {' '}running in background (~30-60s). Watch <strong>Recent runs</strong> below — rows
                appear as each step completes.
              </div>
            </div>
          )}
          {status.pipeline && !status.pipeline.kicked_off && (
            <div className="text-[11px] text-neutral-500">
              Pipeline already up-to-date today
              {status.pipeline.reason_skipped === 'ALREADY_RUNNING' && ' (in progress)'}
              {status.pipeline.reason_skipped === 'ALREADY_COMPLETED_TODAY' && ' (today\'s daily_signals already succeeded)'}
              .
            </div>
          )}
        </div>
      )}

      {status?.status === 'error' && (
        <div className="rounded bg-red-500/10 border border-red-500/30 p-3 text-sm text-red-300">
          ✗ {status.message}
        </div>
      )}
    </section>
  )
}
