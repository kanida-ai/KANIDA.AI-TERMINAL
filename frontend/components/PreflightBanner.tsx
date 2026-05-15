'use client'

// PreflightBanner — the structural health signal for auto-trade.
//
// Polls /api/falcon/preflight every 30s. Renders:
//   RED  → blocks the page's primary actions; lists exact failing checks + fix hints
//   YELLOW → soft-warns, doesn't block
//   GREEN → tiny pill in the corner, "all systems go"
//
// Designed to be the single source of truth across /trade, /premarket, /admin.

import { useEffect, useState } from 'react'

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001'

export type PreflightCheck = {
  name:        string
  status:      'green' | 'yellow' | 'red'
  detail:      string
  remediation: string
  elapsed_ms:  number
}

export type PreflightResult = {
  ok:           boolean
  has_warnings: boolean
  target_date:  string
  ran_at:       string
  elapsed_ms:   number
  red:          string[]
  yellow:       string[]
  green:        string[]
  checks:       PreflightCheck[]
}

export function usePreflight(pollMs: number = 30000) {
  const [data,  setData]  = useState<PreflightResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy,  setBusy]  = useState(false)

  const refresh = async (force = false) => {
    setBusy(true)
    try {
      const r = await fetch(`${API}/api/falcon/preflight${force ? '?force=true' : ''}`)
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      const d = await r.json()
      setData(d as PreflightResult)
      setError(null)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  useEffect(() => {
    refresh(false)
    const id = setInterval(() => refresh(false), pollMs)
    return () => clearInterval(id)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pollMs])

  return { data, error, busy, refresh }
}


/**
 * Compact banner. Renders nothing when all-green by default (use `alwaysShow`
 * to force a green pill).
 */
export function PreflightBanner({ alwaysShow = false }: { alwaysShow?: boolean }) {
  const { data, error, busy, refresh } = usePreflight()

  if (error) {
    return (
      <div className="rounded border border-red-500/40 bg-red-500/10 p-3 text-sm text-red-200">
        Preflight unavailable: {error}
        <button onClick={() => refresh(true)} className="ml-3 underline text-red-100">retry</button>
      </div>
    )
  }
  if (!data) return null

  // GREEN — minimal pill, hidden unless alwaysShow
  if (data.ok && !data.has_warnings) {
    if (!alwaysShow) return null
    return (
      <div className="inline-flex items-center gap-2 text-xs text-green-300">
        <span className="w-2 h-2 rounded-full bg-green-400" />
        <span>preflight OK · {data.green.length} checks · last {timeAgo(data.ran_at)}</span>
        <button onClick={() => refresh(true)}
                className="text-neutral-500 hover:text-neutral-300 underline">
          re-run
        </button>
      </div>
    )
  }

  // RED — blocks actions visually
  if (!data.ok) {
    const reds = data.checks.filter(c => c.status === 'red')
    return (
      <div className="rounded border border-red-500/50 bg-red-500/10 p-4 space-y-3">
        <div className="flex items-baseline justify-between">
          <h3 className="text-sm font-bold text-red-300">
            ⛔ Auto-trade blocked — {reds.length} preflight check{reds.length !== 1 ? 's' : ''} failed
          </h3>
          <button onClick={() => refresh(true)} disabled={busy}
                  className="text-xs text-red-200 underline hover:text-red-100">
            {busy ? 'rechecking…' : 're-run preflight'}
          </button>
        </div>
        <div className="space-y-2">
          {reds.map(c => (
            <div key={c.name} className="bg-neutral-950/60 border border-red-500/30 rounded p-2 text-xs">
              <div className="font-mono text-red-200">{c.name}</div>
              <div className="text-neutral-300">{c.detail}</div>
              {c.remediation && (
                <div className="text-amber-300 mt-1">→ {c.remediation}</div>
              )}
            </div>
          ))}
        </div>
        <div className="text-xs text-neutral-500">
          The deployer and Place Now will refuse to call Kite while any RED check is active.
          Fix the items above, then click <strong>re-run preflight</strong>.
        </div>
      </div>
    )
  }

  // YELLOW
  const yellows = data.checks.filter(c => c.status === 'yellow')
  return (
    <div className="rounded border border-yellow-500/30 bg-yellow-500/5 p-3 space-y-2">
      <div className="flex items-baseline justify-between">
        <h3 className="text-sm font-semibold text-yellow-300">
          ⚠ Preflight: {yellows.length} warning{yellows.length !== 1 ? 's' : ''}
        </h3>
        <button onClick={() => refresh(true)} disabled={busy}
                className="text-xs text-yellow-200 underline hover:text-yellow-100">
          {busy ? 'rechecking…' : 're-run'}
        </button>
      </div>
      <ul className="text-xs space-y-1 text-yellow-100/90">
        {yellows.map(c => (
          <li key={c.name}>
            <span className="font-mono text-yellow-300">{c.name}</span>: {c.detail}
          </li>
        ))}
      </ul>
      <div className="text-xs text-yellow-200/70">
        Auto-trade is enabled, but verify the warnings above before a large batch.
      </div>
    </div>
  )
}


function timeAgo(iso: string): string {
  const t = new Date(iso).getTime()
  const ms = Date.now() - t
  if (ms < 60_000)     return `${Math.floor(ms / 1000)}s ago`
  if (ms < 3_600_000)  return `${Math.floor(ms / 60_000)}m ago`
  return `${Math.floor(ms / 3_600_000)}h ago`
}
