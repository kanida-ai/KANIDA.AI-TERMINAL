'use client'

/**
 * PortfolioAutoTrade — OPERATOR-ONLY console for the LIVE multi-broker Portfolio
 * AutoTrade backend (/api/autotrade/*, operator-token gated, reached via the
 * same-origin Falcon proxy through lib/autotrade-api.ts).
 *
 * HARD HONESTY (real money is involved):
 *   • The backend SHIPS DISABLED. Sessions default to PAPER mode (no real
 *     orders); the kill switch defaults OFF; and real LIVE orders ADDITIONALLY
 *     require the server env flag FALCON_AUTOTRADE_ENABLED — so even a LIVE
 *     session stays inert until that flag is on, server-side.
 *   • This UI NEVER implies trading is on. PAPER is presented as the green/safe
 *     default; LIVE is red, gated behind a typed confirm + a standing warning.
 *   • All numbers come from the backend. We render "—" / honest empty + error
 *     states; we fabricate no fills, no P&L, no positions.
 *
 * Flow: Config form → Create → Start (per-symbol placed/skipped) → live Status
 * card (gross return, kill-switch state, open positions) with poll → KILL
 * (red, typed-confirm). Plus read-only saved-configs + brokers lists.
 *
 * Reuses the cotrade-kit F2 palette + icon set so it reads as one product family
 * with Co-Trading / the existing AutoTrade console.
 */
import { useCallback, useEffect, useRef, useState } from 'react'
import {
  C, ICON, Gear, MECHANISM_CSS, fmtCapital, fmtPct, pctTone,
} from '@/components/power/shared/cotrade-kit'
import {
  AutoTradeAPI,
  type Mode, type StartWhen, type SizingMode, type OrderProduct, type KillDirection,
  type SessionConfig, type CreateResponse, type StartResponse,
  type StatusResponse, type SavedConfig, type Broker, type SessionSummary,
} from '@/lib/autotrade-api'

// ── Safe defaults — paper + kill switch OFF, per the ships-disabled contract ──
const DEFAULT_CONFIG: SessionConfig = {
  total_allocated_capital: 500_000,
  top_n_stocks: 5,
  sizing_mode: 'equal',
  max_pct_per_position: 25,
  order_product: 'CNC',
  kill_switch_enabled: false,
  kill_switch_pct: 5,
  kill_switch_direction: 'loss',
  entry_time: '09:15',
}

// A SCHEDULED session that lost its in-memory timer (backend restart) reports
// scheduler_armed === false. Everything else is "armed enough to wait".
const isScheduled = (s?: string | null) => (s ?? '').toUpperCase() === 'SCHEDULED'

// Live countdown helper — turns seconds into "2h 14m 03s" / "14m 03s" / "43s".
function fmtCountdown(totalSec: number): string {
  const s = Math.max(0, Math.floor(totalSec))
  const h = Math.floor(s / 3600)
  const m = Math.floor((s % 3600) / 60)
  const sec = s % 60
  const pad = (n: number) => String(n).padStart(2, '0')
  if (h > 0) return `${h}h ${pad(m)}m ${pad(sec)}s`
  if (m > 0) return `${m}m ${pad(sec)}s`
  return `${sec}s`
}

const TOP_N_OPTIONS = [3, 5, 7, 10]
const SIZING_OPTIONS: { id: SizingMode; label: string; hint: string }[] = [
  { id: 'equal',   label: 'Equal',   hint: 'Split capital evenly across picks' },
  { id: 'pct_cap', label: '% cap',   hint: 'Cap each position at a max % of capital' },
  { id: 'manual',  label: 'Manual',  hint: 'Per-symbol amounts (advanced)' },
]
const PRODUCT_OPTIONS: OrderProduct[] = ['CNC', 'MIS', 'MTF', 'NRML']
const KILL_DIR_OPTIONS: { id: KillDirection; label: string }[] = [
  { id: 'loss',   label: 'Loss only' },
  { id: 'profit', label: 'Profit only' },
  { id: 'both',   label: 'Both' },
]
const CAPITAL_PRESETS = [100_000, 500_000, 1_000_000, 3_000_000]

// 'list' is the HOME phase: your saved sessions (newest first). 'config' is the
// explicit New-Session form. 'created'/'running' are the live session views,
// reached either by creating one OR by RESUMING an existing one from the list.
type Phase = 'list' | 'config' | 'created' | 'running'

// ── Small shared field primitives ────────────────────────────────────────────
function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1.5">
      <label className="text-[11px] font-semibold uppercase tracking-[0.05em]" style={{ color: C.muted }}>
        {label}
      </label>
      {children}
      {hint && <span className="text-[10.5px] leading-snug" style={{ color: C.faint }}>{hint}</span>}
    </div>
  )
}

function Segmented<T extends string | number>({
  options, value, onChange,
}: {
  options: { id: T; label: string }[]; value: T; onChange: (v: T) => void
}) {
  return (
    <div className="inline-flex rounded-xl border p-0.5" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
      {options.map((o) => {
        const active = o.id === value
        return (
          <button
            key={String(o.id)}
            type="button"
            onClick={() => onChange(o.id)}
            className="px-3 py-1.5 rounded-lg text-[12px] font-medium transition-colors"
            style={{
              color: active ? '#06130c' : C.ink2,
              background: active ? C.mint : 'transparent',
            }}
          >
            {o.label}
          </button>
        )
      })}
    </div>
  )
}

const inputStyle: React.CSSProperties = {
  background: 'rgba(255,255,255,0.03)',
  border: `1px solid ${C.line2}`,
  color: C.ink,
}

export function PortfolioAutoTrade() {
  const [config, setConfig] = useState<SessionConfig>(DEFAULT_CONFIG)
  const [mode, setMode] = useState<Mode>('paper')

  const [phase, setPhase] = useState<Phase>('list')
  const [session, setSession] = useState<CreateResponse | null>(null)
  const [startResult, setStartResult] = useState<StartResponse | null>(null)
  const [status, setStatus] = useState<StatusResponse | null>(null)

  // Your Sessions list (newest first) — the HOME view. Resumes survive reload.
  const [sessions, setSessions] = useState<SessionSummary[] | null>(null)
  const [sessionsLoading, setSessionsLoading] = useState(false)
  const [sessionsErr, setSessionsErr] = useState<string | null>(null)

  // Multi-select delete (paper/test housekeeping). `selected` holds the chosen
  // session_ids; `deleting` gates the bulk action; `deleteErr` is honest.
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [deleting, setDeleting] = useState(false)
  const [deleteErr, setDeleteErr] = useState<string | null>(null)

  const [busy, setBusy] = useState<null | 'create' | 'start' | 'status' | 'kill'>(null)
  const [error, setError] = useState<string | null>(null)

  // Live-mode typed confirmation + kill typed confirmation
  const [liveConfirm, setLiveConfirm] = useState('')
  const [killArmed, setKillArmed] = useState(false)
  const [killConfirm, setKillConfirm] = useState('')

  // Poll toggle
  const [poll, setPoll] = useState(true)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Live countdown for a SCHEDULED session. Seeded from status.seconds_remaining
  // on every poll, then ticked down locally each second so the display is smooth
  // between the (slower) status polls. Re-sync on each fresh status keeps it
  // honest — the backend remains the source of truth.
  const [countdown, setCountdown] = useState<number | null>(null)
  const tickRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const set = <K extends keyof SessionConfig>(k: K, v: SessionConfig[K]) =>
    setConfig((c) => ({ ...c, [k]: v }))

  const liveReady = mode === 'paper' || liveConfirm.trim().toUpperCase() === 'LIVE'

  // ── Your Sessions (list + resume) ────────────────────────────────────────────
  const loadSessions = useCallback(async () => {
    setSessionsLoading(true); setSessionsErr(null)
    try {
      const res = await AutoTradeAPI.listSessions()
      // Newest first — sort by created_at desc when present, else keep order.
      const list = (res.sessions ?? []).slice().sort((a, b) => {
        const ta = a.created_at ? Date.parse(a.created_at) : 0
        const tb = b.created_at ? Date.parse(b.created_at) : 0
        return tb - ta
      })
      setSessions(list)
    } catch (e) {
      setSessionsErr(e instanceof Error ? e.message : 'Could not load your sessions.')
    } finally {
      setSessionsLoading(false)
    }
  }, [])

  // Fetch the list once on mount so a reload RESTORES your sessions (the
  // "session disappears" fix) instead of dumping you on a blank form.
  useEffect(() => { loadSessions() }, [loadSessions])

  // ── Multi-select delete (paper/test housekeeping) ────────────────────────────
  const toggleSelect = useCallback((id: string) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      return next
    })
  }, [])

  const allIds = (sessions ?? []).map((s) => s.session_id).filter(Boolean) as string[]
  const allSelected = allIds.length > 0 && allIds.every((id) => selected.has(id))
  const toggleSelectAll = useCallback(() => {
    setSelected((prev) => (allIds.length > 0 && allIds.every((id) => prev.has(id)))
      ? new Set()
      : new Set(allIds))
  }, [allIds])

  const onDeleteSelected = useCallback(async () => {
    const ids = Array.from(selected)
    if (!ids.length) return
    if (!window.confirm(
      `Delete ${ids.length} session${ids.length > 1 ? 's' : ''}? This removes the ` +
      `session record${ids.length > 1 ? 's' : ''} (paper/test housekeeping) and cannot be undone.`,
    )) return
    setDeleting(true); setDeleteErr(null)
    try {
      await AutoTradeAPI.deleteSessions(ids)
      setSelected(new Set())
      await loadSessions()
    } catch (e) {
      setDeleteErr(e instanceof Error ? e.message : 'Could not delete the selected sessions.')
    } finally {
      setDeleting(false)
    }
  }, [selected, loadSessions])

  // Resume an existing session: jump straight to its live view + pull status.
  // RESUME FIX: fetch status IMMEDIATELY using the row's id (not relying on the
  // poll interval, which would leave the view on a blank "No open positions"
  // for up to 12s while positions/LTP/gross are unknown). The poll effect still
  // takes over afterwards; this just makes the first paint correct at once.
  const onResume = useCallback(async (s: SessionSummary) => {
    setError(null)
    setSession({ session_id: s.session_id, status: s.status ?? 'unknown', mode: s.mode ?? 'paper' })
    setStartResult(null)
    setStatus(null)
    setLiveConfirm(''); setKillArmed(false); setKillConfirm('')
    // Seed the countdown from the list row if this is a SCHEDULED session, so
    // the waiting view is correct the instant you resume (the poll re-syncs it).
    setCountdown(isScheduled(s.status) && typeof s.seconds_remaining === 'number' ? s.seconds_remaining : null)
    setPhase('running')
    setPoll(true)
    // Immediate, non-poll-dependent status pull so positions + LTP + gross show
    // right away. Errors here are non-fatal — the poll will retry.
    setBusy('status')
    try {
      const res = await AutoTradeAPI.sessionStatus(s.session_id)
      setStatus(res)
      if (isScheduled(res.status) && typeof res.seconds_remaining === 'number') {
        setCountdown(res.seconds_remaining)
      } else if (!isScheduled(res.status)) {
        setCountdown(null)
      }
    } catch {
      /* poll will retry; keep the loading state honest */
    } finally {
      setBusy(null)
    }
  }, [])

  // ── Actions ────────────────────────────────────────────────────────────────
  const onCreate = useCallback(async () => {
    setError(null); setBusy('create')
    try {
      const res = await AutoTradeAPI.createSession(mode, config)
      setSession(res)
      setPhase('created')
      setStartResult(null)
      setStatus(null)
      // Keep the list fresh so the new session shows the moment you return.
      loadSessions()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to create session')
    } finally {
      setBusy(null)
    }
  }, [mode, config, loadSessions])

  const onStart = useCallback(async (when: StartWhen) => {
    if (!session) return
    setError(null); setBusy('start')
    try {
      const res = await AutoTradeAPI.startSession(session.session_id, when)
      setStartResult(res)
      // A scheduled start places nothing yet — seed the countdown from the
      // backend's seconds_remaining so the waiting state is immediate.
      if (isScheduled(res.status) && typeof res.seconds_remaining === 'number') {
        setCountdown(res.seconds_remaining)
      }
      setPhase('running')
      setPoll(true)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to start session')
    } finally {
      setBusy(null)
    }
  }, [session])

  const refreshStatus = useCallback(async (silent = false) => {
    if (!session) return
    if (!silent) { setError(null); setBusy('status') }
    try {
      const res = await AutoTradeAPI.sessionStatus(session.session_id)
      setStatus(res)
      // Re-sync the countdown from the backend on every poll while SCHEDULED;
      // clear it once the session has flipped to RUNNING/CLOSED.
      if (isScheduled(res.status) && typeof res.seconds_remaining === 'number') {
        setCountdown(res.seconds_remaining)
      } else if (!isScheduled(res.status)) {
        setCountdown(null)
      }
    } catch (e) {
      if (!silent) setError(e instanceof Error ? e.message : 'Failed to load status')
    } finally {
      if (!silent) setBusy(null)
    }
  }, [session])

  const onKill = useCallback(async () => {
    if (!session) return
    setError(null); setBusy('kill')
    try {
      const res = await AutoTradeAPI.killSession(session.session_id)
      setKillArmed(false); setKillConfirm('')
      await refreshStatus(true)
      if (res?.trigger_reason) setError(`Kill complete — ${res.trigger_reason}`)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to kill session')
    } finally {
      setBusy(null)
    }
  }, [session, refreshStatus])

  // ── Poll status while running ────────────────────────────────────────────────
  // While SCHEDULED, poll FASTER (6s) so the auto-flip to RUNNING (and the
  // placement that comes with it) shows promptly; otherwise 12s is plenty.
  const scheduledNow = isScheduled(status?.status)
  useEffect(() => {
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null }
    if (phase === 'running' && poll && session) {
      refreshStatus(true)
      pollRef.current = setInterval(() => refreshStatus(true), scheduledNow ? 6_000 : 12_000)
    }
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [phase, poll, session, refreshStatus, scheduledNow])

  // ── Local 1s ticker for the SCHEDULED countdown ──────────────────────────────
  // Runs only while a countdown is active; floors at 0 (the next poll then
  // confirms the flip to RUNNING). Independent of the auto-refresh toggle.
  useEffect(() => {
    if (tickRef.current) { clearInterval(tickRef.current); tickRef.current = null }
    if (phase === 'running' && scheduledNow && countdown !== null) {
      tickRef.current = setInterval(() => {
        setCountdown((c) => (c === null ? null : Math.max(0, c - 1)))
      }, 1_000)
    }
    return () => { if (tickRef.current) clearInterval(tickRef.current) }
  }, [phase, scheduledNow, countdown !== null])  // eslint-disable-line react-hooks/exhaustive-deps

  // Open the explicit New-Session form (resets the in-flight session).
  const openNewSession = () => {
    setPhase('config'); setSession(null); setStartResult(null); setStatus(null)
    setConfig(DEFAULT_CONFIG); setMode('paper'); setCountdown(null)
    setLiveConfirm(''); setKillArmed(false); setKillConfirm(''); setError(null)
  }

  // Return to the Your-Sessions list and refresh it (so a just-created/started
  // session is visible — the disappearing-session fix).
  const backToList = () => {
    setPhase('list'); setSession(null); setStartResult(null); setStatus(null)
    setCountdown(null)
    setLiveConfirm(''); setKillArmed(false); setKillConfirm(''); setError(null)
    loadSessions()
  }

  // ════════════════════════════════════════════════════════════════════════════
  return (
    <div className="flex flex-col gap-4">
      <style>{MECHANISM_CSS}</style>
      {/* Scoped live-indicator pulse (mint), namespaced to avoid token clashes. */}
      <style>{`@keyframes at-live-pulse{0%,100%{opacity:1}50%{opacity:.35}}.live-dot{animation:at-live-pulse 1.6s ease-in-out infinite}@media (prefers-reduced-motion: reduce){.live-dot{animation:none}}`}</style>

      {/* ── Ships-disabled honesty banner (always on) ────────────────────────── */}
      <div
        className="flex items-start gap-2.5 rounded-2xl border px-4 py-3"
        style={{ borderColor: 'rgba(230,180,80,0.32)', background: 'rgba(230,180,80,0.06)' }}
      >
        <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.shield(16)}</span>
        <div className="text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
          <b style={{ color: C.amber }}>Ships disabled.</b>{' '}
          Sessions default to <b>PAPER</b> mode (no real orders). The kill switch is{' '}
          <b>OFF</b> unless you enable it. Real LIVE orders ADDITIONALLY require the
          server flag <code style={{ color: C.ink }}>FALCON_AUTOTRADE_ENABLED</code> — until
          that is set on the backend, a LIVE session stays inert and places nothing.
        </div>
      </div>

      {/* ── LIST PHASE (HOME) — Your Sessions, newest first ──────────────────── */}
      {phase === 'list' && (
        <div className="flex flex-col gap-4">
          <div className="flex items-center gap-2 flex-wrap">
            <span style={{ color: C.mint }}>{ICON.bolt(16)}</span>
            <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Your sessions</span>
            <div className="ml-auto flex items-center gap-2">
              {/* Delete-selected — appears only when something is checked. These
                  are paper/test session records; deletion is record cleanup. */}
              {selected.size > 0 && (
                <button type="button" disabled={deleting} onClick={onDeleteSelected}
                  className="flex items-center gap-1.5 text-[11.5px] font-semibold px-3 py-1.5 rounded-lg transition-opacity disabled:opacity-40"
                  style={{ color: C.red, background: 'rgba(232,115,107,0.12)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }}>
                  {ICON.close(12)} {deleting ? 'Deleting…' : `Delete selected (${selected.size})`}
                </button>
              )}
              <button type="button" disabled={sessionsLoading} onClick={loadSessions}
                className="text-[11.5px] px-2.5 py-1.5 rounded-lg transition-colors disabled:opacity-40"
                style={{ color: C.muted, border: `1px solid ${C.line}` }}>
                {sessionsLoading ? 'Refreshing…' : 'Refresh'}
              </button>
              <button type="button" onClick={openNewSession}
                className="flex items-center gap-1.5 text-[12px] font-semibold px-3.5 py-1.5 rounded-lg transition-opacity"
                style={{ color: '#06130c', background: C.mint }}>
                {ICON.bolt(13)} New session
              </button>
            </div>
          </div>

          <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
            {sessionsLoading && sessions === null ? (
              <p className="text-[12px]" style={{ color: C.muted }}>Loading your sessions…</p>
            ) : sessionsErr ? (
              <div className="flex items-start gap-2 text-[12px] leading-snug" style={{ color: C.ink2 }}>
                <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
                <span>{sessionsErr} <button type="button" onClick={loadSessions} className="underline" style={{ color: C.mint }}>Retry</button></span>
              </div>
            ) : !sessions?.length ? (
              <div className="flex flex-col items-start gap-3">
                <p className="text-[12.5px]" style={{ color: C.muted }}>
                  No sessions yet. Create one to begin — it stays here after you create or
                  reload, so you can resume its live view anytime.
                </p>
                <button type="button" onClick={openNewSession}
                  className="flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold"
                  style={{ color: '#06130c', background: C.mint }}>
                  {ICON.bolt(14)} Create your first session
                </button>
              </div>
            ) : (
              <>
                {/* Honest delete error (record cleanup failed) */}
                {deleteErr && (
                  <div className="mb-3 flex items-start gap-2 rounded-lg border px-3 py-2 text-[11.5px] leading-snug"
                    style={{ borderColor: 'rgba(232,115,107,0.35)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
                    <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(13)}</span>
                    <span>{deleteErr}</span>
                    <button type="button" onClick={() => setDeleteErr(null)} className="ml-auto shrink-0" style={{ color: C.faint }}>
                      {ICON.close(12)}
                    </button>
                  </div>
                )}

                {/* Select-all + housekeeping note */}
                <div className="mb-2 flex items-center gap-2.5 px-1">
                  <label className="flex items-center gap-2 text-[11px] cursor-pointer select-none" style={{ color: C.muted }}>
                    <input type="checkbox" checked={allSelected} onChange={toggleSelectAll} />
                    Select all
                  </label>
                  <span className="text-[10.5px]" style={{ color: C.faint }}>
                    Checkboxes select paper/test session records for deletion — they don&apos;t open the session.
                  </span>
                </div>

                <ul className="flex flex-col gap-2">
                {sessions.map((s, i) => {
                  const ret = typeof s.gross_return === 'number' ? s.gross_return : null
                  const sched = isScheduled(s.status)
                  const running = (s.status ?? '').toUpperCase() === 'RUNNING'
                  const nOpen = typeof s.n_open_positions === 'number' ? s.n_open_positions : null
                  const checked = selected.has(s.session_id)
                  return (
                    <li key={s.session_id ?? i}>
                      <div
                        className="w-full flex items-center gap-3 rounded-xl border px-3.5 py-3 transition-colors"
                        style={{
                          borderColor: checked ? 'rgba(232,115,107,0.45)' : (sched ? 'rgba(63,227,164,0.32)' : C.line2),
                          background: checked ? 'rgba(232,115,107,0.05)' : 'rgba(255,255,255,0.015)',
                        }}>
                        {/* Selection checkbox — not part of the resume click target */}
                        <label className="shrink-0 flex items-center cursor-pointer" onClick={(e) => e.stopPropagation()}>
                          <input type="checkbox" checked={checked}
                            onChange={() => s.session_id && toggleSelect(s.session_id)} />
                        </label>

                        {/* Resume target — the rest of the row opens the live view */}
                        <button type="button" onClick={() => onResume(s)}
                          className="min-w-0 flex-1 flex items-center gap-3 text-left">
                          <span className="shrink-0" style={{ color: C.mint }}>{sched ? ICON.clock(15) : ICON.bolt(15)}</span>
                          <div className="min-w-0 flex-1">
                            <div className="flex items-center gap-2">
                              {sched ? <SchedPill /> : running ? <RunningPill /> : (
                                <span className="text-[12.5px] font-semibold truncate" style={{ color: C.ink }}>
                                  {s.status ?? 'session'}
                                </span>
                              )}
                              {s.mode && <ModePill mode={s.mode} />}
                            </div>
                            <div className="text-[10.5px] mt-0.5 font-mono truncate" style={{ color: C.faint }}>
                              {sched && s.fires_at
                                ? <span style={{ color: C.mint }}>fires {s.fires_at}{typeof s.seconds_remaining === 'number' ? ` · in ${fmtCountdown(s.seconds_remaining)}` : ''}</span>
                                : <>{s.session_id}{s.created_at ? ` · ${s.created_at}` : ''}</>}
                            </div>
                          </div>

                          {/* Positions — a RUNNING session holds positions; never
                              look empty. Show the count if the list gives one,
                              else an honest "open" / "—" with the live dot. */}
                          {!sched && (
                            <div className="shrink-0 text-right">
                              <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>Positions</div>
                              <div className="text-[13px] font-semibold flex items-center justify-end gap-1.5" style={{ color: nOpen ? C.ink : C.ink2 }}>
                                {running && <span className="inline-block w-1.5 h-1.5 rounded-full live-dot" style={{ background: C.mint }} />}
                                {nOpen != null ? nOpen : (running ? 'open' : '—')}
                              </div>
                            </div>
                          )}

                          <div className="shrink-0 text-right">
                            <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>{sched ? 'Status' : 'Return'}</div>
                            <div className="text-[13px] font-semibold" style={{ color: sched ? C.mint : (ret == null ? C.faint : pctTone(ret)) }}>
                              {sched ? 'Scheduled' : (ret == null ? '—' : fmtPct(ret))}
                            </div>
                          </div>
                          <div className="shrink-0 text-right hidden sm:block">
                            <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>Capital</div>
                            <div className="text-[13px] font-semibold" style={{ color: C.ink2 }}>
                              {typeof s.total_allocated_capital === 'number' ? fmtCapital(s.total_allocated_capital) : '—'}
                            </div>
                          </div>
                          <span className="shrink-0" style={{ color: C.faint }}>{ICON.chevronR(14)}</span>
                        </button>
                      </div>
                    </li>
                  )
                })}
                </ul>
              </>
            )}
          </div>
        </div>
      )}

      {/* ── CONFIG PHASE — explicit New Session form ─────────────────────────── */}
      {phase === 'config' && (
        <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
          <button type="button" onClick={backToList}
            className="mb-4 inline-flex items-center gap-1.5 text-[12px] px-3 py-1.5 rounded-lg transition-colors"
            style={{ color: C.muted, border: `1px solid ${C.line}` }}>
            ← Your sessions
          </button>
          {/* Mode selector */}
          <div className="flex flex-col sm:flex-row sm:items-center gap-3 mb-5">
            <span className="text-[11px] font-semibold uppercase tracking-[0.05em]" style={{ color: C.muted }}>Mode</span>
            <div className="flex gap-2">
              <button
                type="button"
                onClick={() => { setMode('paper'); setLiveConfirm('') }}
                className="flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors"
                style={{
                  color: mode === 'paper' ? '#06130c' : C.mint,
                  background: mode === 'paper' ? C.mint : 'rgba(63,227,164,0.10)',
                  boxShadow: mode === 'paper' ? 'none' : 'inset 0 0 0 1px rgba(63,227,164,0.4)',
                }}
              >
                {ICON.shield(14)} Paper — safe
              </button>
              <button
                type="button"
                onClick={() => setMode('live')}
                className="flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors"
                style={{
                  color: mode === 'live' ? '#1a0908' : C.red,
                  background: mode === 'live' ? C.red : 'rgba(232,115,107,0.10)',
                  boxShadow: mode === 'live' ? 'none' : 'inset 0 0 0 1px rgba(232,115,107,0.4)',
                }}
              >
                {ICON.bolt(14)} Live — real orders
              </button>
            </div>
          </div>

          {/* Live warning + typed confirm */}
          {mode === 'live' && (
            <div className="mb-5 rounded-xl border px-3.5 py-3" style={{ borderColor: 'rgba(232,115,107,0.4)', background: 'rgba(232,115,107,0.06)' }}>
              <div className="flex items-start gap-2 text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(15)}</span>
                <span>
                  <b style={{ color: C.red }}>LIVE places REAL orders on a connected broker.</b>{' '}
                  It still does nothing until the server flag{' '}
                  <code style={{ color: C.ink }}>FALCON_AUTOTRADE_ENABLED</code> is set — but
                  type <b>LIVE</b> below to confirm you intend a live session.
                </span>
              </div>
              <input
                value={liveConfirm}
                onChange={(e) => setLiveConfirm(e.target.value)}
                placeholder='Type "LIVE" to confirm'
                className="mt-2.5 w-full rounded-lg px-3 py-2 text-[12.5px] outline-none"
                style={{ ...inputStyle, borderColor: 'rgba(232,115,107,0.4)' }}
              />
            </div>
          )}

          {/* Config grid */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <Field label="Allocated capital" hint="Total capital this session may deploy.">
              <input
                type="number" min={0} step={10000}
                value={config.total_allocated_capital}
                onChange={(e) => set('total_allocated_capital', Number(e.target.value) || 0)}
                className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                style={inputStyle}
              />
              <div className="flex flex-wrap gap-1.5 mt-1">
                {CAPITAL_PRESETS.map((p) => (
                  <button key={p} type="button" onClick={() => set('total_allocated_capital', p)}
                    className="px-2 py-1 rounded-md text-[10.5px] transition-colors"
                    style={{ color: C.muted, border: `1px solid ${C.line}`, background: 'rgba(255,255,255,0.02)' }}>
                    {fmtCapital(p)}
                  </button>
                ))}
              </div>
            </Field>

            <Field label="Top N stocks" hint="How many of today's picks to trade.">
              <Segmented
                options={TOP_N_OPTIONS.map((n) => ({ id: n, label: String(n) }))}
                value={config.top_n_stocks}
                onChange={(v) => set('top_n_stocks', v)}
              />
            </Field>

            <Field label="Sizing mode" hint={SIZING_OPTIONS.find((s) => s.id === config.sizing_mode)?.hint}>
              <Segmented
                options={SIZING_OPTIONS.map((s) => ({ id: s.id, label: s.label }))}
                value={config.sizing_mode}
                onChange={(v) => set('sizing_mode', v)}
              />
            </Field>

            {config.sizing_mode === 'pct_cap' ? (
              <Field label="Max % per position" hint="Cap on any single position.">
                <input
                  type="number" min={1} max={100} step={1}
                  value={config.max_pct_per_position ?? 25}
                  onChange={(e) => set('max_pct_per_position', Number(e.target.value) || 0)}
                  className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                  style={inputStyle}
                />
              </Field>
            ) : (
              <Field label="Order product" hint="Broker product type for entries.">
                <Segmented
                  options={PRODUCT_OPTIONS.map((p) => ({ id: p, label: p }))}
                  value={config.order_product}
                  onChange={(v) => set('order_product', v)}
                />
              </Field>
            )}

            {config.sizing_mode === 'pct_cap' && (
              <Field label="Order product" hint="Broker product type for entries.">
                <Segmented
                  options={PRODUCT_OPTIONS.map((p) => ({ id: p, label: p }))}
                  value={config.order_product}
                  onChange={(v) => set('order_product', v)}
                />
              </Field>
            )}

            <Field label="Entry time (IST)" hint="When the session places entries.">
              <input
                type="time"
                value={config.entry_time}
                onChange={(e) => set('entry_time', e.target.value)}
                className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                style={inputStyle}
              />
            </Field>
          </div>

          {config.sizing_mode === 'manual' && (
            <div className="mt-3 rounded-xl border px-3.5 py-2.5 text-[11px] leading-snug"
              style={{ borderColor: C.line, background: 'rgba(255,255,255,0.015)', color: C.muted }}>
              Manual per-symbol amounts are sent only when configured by the backend
              preset (config/list). This UI creates the session with the chosen mode;
              per-symbol manual amounts are not edited here yet.
            </div>
          )}

          {/* Kill switch block */}
          <div className="mt-5 rounded-xl border p-3.5" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.015)' }}>
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span style={{ color: config.kill_switch_enabled ? C.red : C.faint }}>{ICON.shield(15)}</span>
                <span className="text-[12.5px] font-semibold" style={{ color: C.ink }}>Kill switch</span>
                <span className="text-[10px] font-mono uppercase tracking-[0.06em] rounded-full px-2 py-0.5"
                  style={config.kill_switch_enabled
                    ? { color: C.red, background: 'rgba(232,115,107,0.12)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }
                    : { color: C.faint, background: 'rgba(255,255,255,0.04)' }}>
                  {config.kill_switch_enabled ? 'ON' : 'OFF (default)'}
                </span>
              </div>
              <button
                type="button"
                onClick={() => set('kill_switch_enabled', !config.kill_switch_enabled)}
                className="relative w-11 h-6 rounded-full transition-colors"
                style={{ background: config.kill_switch_enabled ? C.red : 'rgba(255,255,255,0.12)' }}
                aria-pressed={config.kill_switch_enabled}
              >
                <span className="absolute top-0.5 w-5 h-5 rounded-full bg-white transition-all"
                  style={{ left: config.kill_switch_enabled ? '22px' : '2px' }} />
              </button>
            </div>

            {config.kill_switch_enabled && (
              <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 gap-4">
                <Field label="Trigger at (%)" hint="Auto-exits when this threshold is hit.">
                  <input
                    type="number" min={0} step={0.5}
                    value={config.kill_switch_pct}
                    onChange={(e) => set('kill_switch_pct', Number(e.target.value) || 0)}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                    style={inputStyle}
                  />
                </Field>
                <Field label="Direction">
                  <Segmented
                    options={KILL_DIR_OPTIONS}
                    value={config.kill_switch_direction}
                    onChange={(v) => set('kill_switch_direction', v)}
                  />
                </Field>
              </div>
            )}
          </div>

          {/* Create CTA */}
          <div className="mt-5 flex items-center gap-3">
            <button
              type="button"
              disabled={busy === 'create' || !liveReady}
              onClick={onCreate}
              className="flex items-center gap-2 px-5 py-2.5 rounded-xl text-[13px] font-semibold transition-opacity disabled:opacity-40"
              style={{ color: '#06130c', background: C.mint }}
            >
              {busy === 'create' ? 'Creating…' : <>Create {mode} session {ICON.arrow(13)}</>}
            </button>
            <span className="text-[11px]" style={{ color: C.muted }}>
              {mode === 'live' && !liveReady ? 'Type LIVE above to enable.' : `Creates a ${mode} session — no orders are placed yet.`}
            </span>
          </div>
        </div>
      )}

      {/* ── CREATED PHASE — confirm then Start ───────────────────────────────── */}
      {phase === 'created' && session && (
        <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
          <div className="flex items-center gap-2 mb-1">
            <span style={{ color: C.mint }}>{ICON.check(16)}</span>
            <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Session created</span>
            <ModePill mode={session.mode} />
          </div>
          <div className="text-[11.5px] mb-4" style={{ color: C.muted }}>
            ID <code style={{ color: C.ink2 }}>{session.session_id}</code> · status{' '}
            <span style={{ color: C.ink2 }}>{session.status}</span> · entry time{' '}
            <span style={{ color: C.ink2 }}>{config.entry_time} IST</span>
          </div>

          {/* TWO clear ways to begin — fire now, or arm for the entry time. */}
          <div className="flex flex-col sm:flex-row sm:items-stretch gap-3">
            {/* Start now → places immediately (RUNNING). */}
            <button
              type="button"
              disabled={busy === 'start'}
              onClick={() => onStart('now')}
              className="flex-1 flex flex-col items-start gap-1 px-4 py-3 rounded-xl text-left transition-opacity disabled:opacity-40"
              style={session.mode === 'live'
                ? { color: '#1a0908', background: C.red }
                : { color: '#06130c', background: C.mint }}
            >
              <span className="flex items-center gap-2 text-[13px] font-semibold">
                {ICON.bolt(14)} {busy === 'start' ? 'Starting…' : 'Start now'}
              </span>
              <span className="text-[10.5px] leading-snug opacity-80">
                Places {session.mode === 'paper' ? 'simulated' : 'real'} entries immediately.
              </span>
            </button>

            {/* Schedule → arms the session to auto-fire at entry_time (SCHEDULED). */}
            <button
              type="button"
              disabled={busy === 'start'}
              onClick={() => onStart('scheduled')}
              className="flex-1 flex flex-col items-start gap-1 px-4 py-3 rounded-xl text-left transition-colors disabled:opacity-40"
              style={{ color: C.mint, background: 'rgba(63,227,164,0.10)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}
            >
              <span className="flex items-center gap-2 text-[13px] font-semibold">
                {ICON.clock(14)} {busy === 'start' ? 'Scheduling…' : `Schedule for ${config.entry_time}`}
              </span>
              <span className="text-[10.5px] leading-snug" style={{ color: C.muted }}>
                Arms it to auto-fire at {config.entry_time} IST — nothing is placed until then.
              </span>
            </button>
          </div>

          <div className="mt-3 flex items-center gap-3">
            <button type="button" onClick={backToList}
              className="text-[12px] px-3 py-2 rounded-lg transition-colors"
              style={{ color: C.muted, border: `1px solid ${C.line}` }}>
              Discard
            </button>
            <p className="text-[11px] leading-snug" style={{ color: C.faint }}>
              {session.mode === 'paper'
                ? 'Paper simulates placement — no broker orders are sent.'
                : 'Live attempts real broker orders, but only if the server flag FALCON_AUTOTRADE_ENABLED is set; otherwise it reports skipped.'}
            </p>
          </div>
        </div>
      )}

      {/* ── RUNNING PHASE — start result + live status + kill ─────────────────── */}
      {phase === 'running' && session && (
        <div className="flex flex-col gap-4">
          {/* SCHEDULED — armed, waiting to fire. Places nothing until entry time. */}
          {scheduledNow && (
            <div className="rounded-2xl border p-4 sm:p-5"
              style={{ borderColor: 'rgba(63,227,164,0.32)', background: 'rgba(63,227,164,0.05)' }}>
              <div className="flex items-center gap-2 mb-3">
                <span style={{ color: C.mint }}>{ICON.clock(17)}</span>
                <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Scheduled — waiting to fire</span>
                {status && <ModePill mode={status.mode} />}
              </div>

              {/* Armed: show fire time + live countdown. Not armed: honest note. */}
              {status?.scheduler_armed === false ? (
                <div className="rounded-xl border px-3.5 py-3 mb-4"
                  style={{ borderColor: 'rgba(230,180,80,0.4)', background: 'rgba(230,180,80,0.06)' }}>
                  <div className="flex items-start gap-2 text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                    <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(15)}</span>
                    <span>
                      <b style={{ color: C.amber }}>Not armed.</b>{' '}
                      The backend restarted and lost this session&apos;s in-memory timer, so it
                      will NOT auto-fire. Re-schedule it to arm the timer again.
                    </span>
                  </div>
                  <button
                    type="button"
                    disabled={busy === 'start'}
                    onClick={() => onStart('scheduled')}
                    className="mt-3 inline-flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
                    style={{ color: '#06130c', background: C.mint }}
                  >
                    {ICON.clock(13)} {busy === 'start' ? 'Re-scheduling…' : 'Re-schedule'}
                  </button>
                </div>
              ) : (
                <div className="flex flex-wrap items-end gap-x-8 gap-y-3 mb-4">
                  <div>
                    <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>Fires at (IST)</div>
                    <div className="text-[15px] font-semibold mt-0.5" style={{ color: C.ink }}>
                      {status?.fires_at ?? `${config.entry_time}`}
                    </div>
                  </div>
                  <div>
                    <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>In</div>
                    <div className="text-[20px] font-semibold mt-0.5 tabular-nums" style={{ color: C.mint }}>
                      {countdown !== null
                        ? (countdown <= 0 ? 'firing…' : fmtCountdown(countdown))
                        : (typeof status?.seconds_remaining === 'number' ? fmtCountdown(status.seconds_remaining) : '—')}
                    </div>
                  </div>
                </div>
              )}

              <p className="text-[11px] leading-snug mb-3" style={{ color: C.faint }}>
                Nothing is placed yet. At the entry time the session auto-flips to RUNNING
                and places its entries — this view updates automatically.
              </p>

              {/* Cancel a SCHEDULED session — kill cancels it (places nothing). */}
              <button
                type="button"
                disabled={busy === 'kill'}
                onClick={onKill}
                className="inline-flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors disabled:opacity-40"
                style={{ color: C.red, background: 'rgba(232,115,107,0.12)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }}
              >
                {ICON.close(13)} {busy === 'kill' ? 'Cancelling…' : 'Cancel schedule'}
              </button>
            </div>
          )}

          {/* Placement result */}
          {!scheduledNow && startResult && (
            <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
              <div className="flex items-center gap-2 mb-3">
                <span style={{ color: C.mint }}>{ICON.bolt(15)}</span>
                <span className="text-[13.5px] font-semibold" style={{ color: C.ink }}>Placement result</span>
                <ModePill mode={startResult.mode} />
                <span className="ml-auto text-[11.5px]" style={{ color: C.muted }}>
                  {startResult.n_placed} placed · {startResult.orders?.length ?? 0} total
                </span>
              </div>
              {startResult.orders?.length ? (
                <div className="overflow-x-auto">
                  <table className="w-full text-[12px]">
                    <thead>
                      <tr style={{ color: C.faint }}>
                        <th className="text-left font-medium pb-2">Symbol</th>
                        <th className="text-left font-medium pb-2">Status</th>
                        <th className="text-left font-medium pb-2">Reason</th>
                      </tr>
                    </thead>
                    <tbody>
                      {startResult.orders.map((o, i) => {
                        const placed = /placed|complete|success|open/i.test(o.status)
                        return (
                          <tr key={`${o.symbol}-${i}`} style={{ borderTop: `1px solid ${C.line}` }}>
                            <td className="py-1.5 font-medium" style={{ color: C.ink }}>{o.symbol}</td>
                            <td className="py-1.5">
                              <span style={{ color: placed ? C.mint : C.amber }}>{o.status}</span>
                            </td>
                            <td className="py-1.5" style={{ color: C.muted }}>{o.reason ?? '—'}</td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-[12px]" style={{ color: C.muted }}>No orders reported.</p>
              )}
            </div>
          )}

          {/* Live status — the normal RUNNING view (hidden while SCHEDULED). */}
          {!scheduledNow && (
          <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
            <div className="flex items-center gap-2 mb-4">
              <Gear size={18} dir={1} />
              <span className="text-[13.5px] font-semibold" style={{ color: C.ink }}>Live status</span>
              {status && <ModePill mode={status.mode} />}
              <div className="ml-auto flex items-center gap-2">
                <label className="flex items-center gap-1.5 text-[11px] cursor-pointer" style={{ color: C.muted }}>
                  <input type="checkbox" checked={poll} onChange={(e) => setPoll(e.target.checked)} />
                  Auto-refresh
                </label>
                <button type="button" disabled={busy === 'status'} onClick={() => refreshStatus(false)}
                  className="text-[11.5px] px-2.5 py-1 rounded-lg transition-colors disabled:opacity-40"
                  style={{ color: C.mint, border: `1px solid rgba(63,227,164,0.3)` }}>
                  {busy === 'status' ? 'Refreshing…' : 'Refresh'}
                </button>
              </div>
            </div>

            {!status ? (
              <p className="text-[12px]" style={{ color: C.muted }}>Loading status…</p>
            ) : (
              <>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
                  <Stat label="Status" value={status.status} />
                  <Stat label="Gross return" value={fmtPct(status.gross_return)} valueColor={pctTone(status.gross_return)} />
                  <Stat label="Allocated" value={fmtCapital(status.total_allocated_capital)} />
                  <Stat label="Open positions" value={String(status.n_open_positions)} />
                </div>

                {/* Kill-switch state readout */}
                <div className="flex items-center gap-2 mb-4 text-[11.5px]" style={{ color: C.muted }}>
                  <span style={{ color: status.kill_switch_enabled ? C.red : C.faint }}>{ICON.shield(14)}</span>
                  Kill switch{' '}
                  <b style={{ color: status.kill_switch_enabled ? C.red : C.faint }}>
                    {status.kill_switch_enabled ? 'ARMED' : 'OFF'}
                  </b>
                  {status.kill_switch_enabled && (
                    <span>· {status.kill_switch_pct}% {status.kill_switch_direction}</span>
                  )}
                </div>

                {/* Open positions table */}
                {status.open_positions?.length ? (
                  <div className="overflow-x-auto">
                    <table className="w-full text-[12px]">
                      <thead>
                        <tr style={{ color: C.faint }}>
                          <th className="text-left font-medium pb-2">Symbol</th>
                          <th className="text-right font-medium pb-2">Qty</th>
                          <th className="text-right font-medium pb-2">Avg</th>
                          <th className="text-right font-medium pb-2">Last</th>
                          <th className="text-right font-medium pb-2">Return</th>
                        </tr>
                      </thead>
                      <tbody>
                        {status.open_positions.map((p, i) => {
                          const ret = typeof p.return_pct === 'number' ? p.return_pct : null
                          return (
                            <tr key={`${p.symbol ?? i}`} style={{ borderTop: `1px solid ${C.line}` }}>
                              <td className="py-1.5 font-medium" style={{ color: C.ink }}>{p.symbol ?? '—'}</td>
                              <td className="py-1.5 text-right" style={{ color: C.ink2 }}>{p.qty ?? '—'}</td>
                              <td className="py-1.5 text-right" style={{ color: C.ink2 }}>{p.avg_price ?? '—'}</td>
                              <td className="py-1.5 text-right" style={{ color: C.ink2 }}>{p.last_price ?? '—'}</td>
                              <td className="py-1.5 text-right" style={{ color: ret == null ? C.faint : pctTone(ret) }}>
                                {ret == null ? '—' : fmtPct(ret)}
                              </td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="text-[12px]" style={{ color: C.muted }}>No open positions.</p>
                )}
              </>
            )}
          </div>
          )}

          {/* KILL block — hidden while SCHEDULED (use "Cancel schedule" above). */}
          {!scheduledNow && (
          <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: 'rgba(232,115,107,0.32)', background: 'rgba(232,115,107,0.04)' }}>
            <div className="flex items-center gap-2 mb-2">
              <span style={{ color: C.red }}>{ICON.bolt(15)}</span>
              <span className="text-[13.5px] font-semibold" style={{ color: C.red }}>Kill session</span>
            </div>
            <p className="text-[11.5px] leading-snug mb-3" style={{ color: C.ink2 }}>
              Immediately exits all open positions and stops this session.
              {' '}This is irreversible for the session.
            </p>
            {!killArmed ? (
              <button type="button" onClick={() => setKillArmed(true)}
                className="px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors"
                style={{ color: C.red, background: 'rgba(232,115,107,0.12)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }}>
                Kill session…
              </button>
            ) : (
              <div className="flex flex-col sm:flex-row sm:items-center gap-2.5">
                <input
                  value={killConfirm}
                  onChange={(e) => setKillConfirm(e.target.value)}
                  placeholder='Type "KILL" to confirm'
                  className="rounded-lg px-3 py-2 text-[12.5px] outline-none sm:w-56"
                  style={{ ...inputStyle, borderColor: 'rgba(232,115,107,0.4)' }}
                />
                <button
                  type="button"
                  disabled={busy === 'kill' || killConfirm.trim().toUpperCase() !== 'KILL'}
                  onClick={onKill}
                  className="px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
                  style={{ color: '#1a0908', background: C.red }}
                >
                  {busy === 'kill' ? 'Killing…' : 'Confirm kill'}
                </button>
                <button type="button" onClick={() => { setKillArmed(false); setKillConfirm('') }}
                  className="text-[12px] px-3 py-2 rounded-lg" style={{ color: C.muted, border: `1px solid ${C.line}` }}>
                  Cancel
                </button>
              </div>
            )}
          </div>
          )}

          <div className="flex items-center gap-2">
            <button type="button" onClick={backToList}
              className="self-start text-[12px] px-3 py-2 rounded-lg transition-colors"
              style={{ color: C.muted, border: `1px solid ${C.line}` }}>
              ← Your sessions
            </button>
            <button type="button" onClick={openNewSession}
              className="self-start text-[12px] px-3 py-2 rounded-lg transition-colors"
              style={{ color: C.mint, border: `1px solid rgba(63,227,164,0.3)` }}>
              New session
            </button>
          </div>
        </div>
      )}

      {/* ── Error toast ──────────────────────────────────────────────────────── */}
      {error && (
        <div className="flex items-start gap-2 rounded-xl border px-3.5 py-2.5 text-[11.5px] leading-snug"
          style={{ borderColor: 'rgba(232,115,107,0.35)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
          <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(14)}</span>
          <span>{error}</span>
          <button type="button" onClick={() => setError(null)} className="ml-auto shrink-0" style={{ color: C.faint }}>
            {ICON.close(13)}
          </button>
        </div>
      )}

      {/* ── Read-only saved configs + brokers ────────────────────────────────── */}
      <ReferenceLists />
    </div>
  )
}

// ── Mode pill ────────────────────────────────────────────────────────────────
function ModePill({ mode }: { mode: Mode }) {
  const live = mode === 'live'
  return (
    <span className="text-[9px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
      style={live
        ? { color: C.red, background: 'rgba(232,115,107,0.12)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }
        : { color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
      {mode}
    </span>
  )
}

// RUNNING status pill — mint with a pulsing live dot so a running session that
// holds positions never reads as empty/"no orders" in the list.
function RunningPill() {
  return (
    <span className="inline-flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
      style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
      <span className="inline-block w-1.5 h-1.5 rounded-full live-dot" style={{ background: C.mint }} />
      Running
    </span>
  )
}

// SCHEDULED status pill — mint, distinct from RUNNING/CLOSED in the list.
function SchedPill() {
  return (
    <span className="inline-flex items-center gap-1 text-[10px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
      style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
      {ICON.clock(10)} Scheduled
    </span>
  )
}

function Stat({ label, value, valueColor }: { label: string; value: string; valueColor?: string }) {
  return (
    <div className="rounded-xl border px-3 py-2.5" style={{ borderColor: C.line, background: 'rgba(255,255,255,0.015)' }}>
      <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>{label}</div>
      <div className="text-[15px] font-semibold mt-0.5" style={{ color: valueColor ?? C.ink }}>{value}</div>
    </div>
  )
}

// ── Read-only saved configs + brokers (load on demand) ───────────────────────
function ReferenceLists() {
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [configs, setConfigs] = useState<SavedConfig[] | null>(null)
  const [brokers, setBrokers] = useState<Broker[] | null>(null)

  const load = useCallback(async () => {
    setLoading(true); setErr(null)
    const [c, b] = await Promise.allSettled([AutoTradeAPI.configList(), AutoTradeAPI.brokerList()])
    if (c.status === 'fulfilled') setConfigs(c.value.configs ?? [])
    if (b.status === 'fulfilled') setBrokers(b.value.brokers ?? [])
    if (c.status === 'rejected' && b.status === 'rejected') {
      setErr('Could not load presets or brokers.')
    }
    setLoading(false)
  }, [])

  const toggle = () => {
    const next = !open
    setOpen(next)
    if (next && configs === null && brokers === null && !loading) load()
  }

  return (
    <div className="rounded-2xl border" style={{ borderColor: C.line2, background: C.card2 }}>
      <button type="button" onClick={toggle}
        className="w-full flex items-center gap-2 px-4 py-3 text-left">
        <span style={{ color: C.muted }}>{ICON.book(15)}</span>
        <span className="text-[12.5px] font-semibold" style={{ color: C.ink }}>Saved presets &amp; brokers</span>
        <span className="ml-auto" style={{ color: C.faint }}>{open ? ICON.chevron(15) : ICON.chevronR(13)}</span>
      </button>

      {open && (
        <div className="px-4 pb-4 grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div>
            <div className="text-[10px] uppercase tracking-[0.05em] mb-2" style={{ color: C.faint }}>Presets (config/list)</div>
            {loading ? <Empty text="Loading…" />
              : err ? <Empty text={err} />
              : !configs?.length ? <Empty text="No saved presets." />
              : <ul className="flex flex-col gap-1.5">
                  {configs.map((c, i) => (
                    <li key={c.id ?? i} className="rounded-lg border px-3 py-2 text-[12px]"
                      style={{ borderColor: C.line, background: 'rgba(255,255,255,0.015)', color: C.ink2 }}>
                      {c.name ?? `Preset ${i + 1}`}
                    </li>
                  ))}
                </ul>}
          </div>
          <div>
            <div className="text-[10px] uppercase tracking-[0.05em] mb-2" style={{ color: C.faint }}>Brokers (broker/list)</div>
            {loading ? <Empty text="Loading…" />
              : !brokers?.length ? <Empty text="No brokers connected." />
              : <ul className="flex flex-col gap-1.5">
                  {brokers.map((b, i) => (
                    <li key={b.id ?? i} className="rounded-lg border px-3 py-2 text-[12px] flex items-center gap-2"
                      style={{ borderColor: C.line, background: 'rgba(255,255,255,0.015)', color: C.ink2 }}>
                      <span style={{ color: C.mint }}>{ICON.link(13)}</span>
                      {b.label ?? b.name ?? b.broker ?? `Broker ${i + 1}`}
                      {b.status && <span className="ml-auto text-[10px]" style={{ color: C.faint }}>{b.status}</span>}
                    </li>
                  ))}
                </ul>}
          </div>
        </div>
      )}
    </div>
  )
}

function Empty({ text }: { text: string }) {
  return <p className="text-[11.5px]" style={{ color: C.faint }}>{text}</p>
}
