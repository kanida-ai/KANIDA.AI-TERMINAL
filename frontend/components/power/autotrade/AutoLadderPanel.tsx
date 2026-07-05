'use client'

/**
 * AutoLadderPanel — the "Monthly Campaign" (Falcon Positional Auto-Ladder).
 *
 * TRADER-FACING "set once, run for a month": the trader sets total capital +
 * product + duration ONCE, hits Start, and the backend autonomously opens /
 * manages / rolls positional baskets every trading day for the duration. The
 * trader NEVER manages individual baskets.
 *
 * TRADER TERMS ONLY — this surface must NEVER print the word "sleeve" or any
 * internal term. It speaks: capital deployed · active baskets · open positions ·
 * daily P&L. A running campaign's child baskets are ordinary sessions (they carry
 * ladder_id) and stay visible in the Sessions tab; this is the higher-level view.
 *
 * HONESTY: every number comes from the LIVE ladderStatus poll — nothing is
 * fabricated; missing fields render "—". Live mode carries the same honest
 * "ships-disabled / PAPER default / real money" warnings the rest of the panel
 * uses. On a status failure we show a calm retry state (mirrors the Sessions tab).
 *
 * SAFETY: UI only. Places/manages no order itself; it only relays the trader's
 * explicit create/start/pause/resume/kill through the operator-token proxy.
 */
import { useCallback, useEffect, useRef, useState } from 'react'
import {
  C, ICON, Gear, MECHANISM_CSS, fmtCapital, fmtINR, signedINR, pctTone,
} from '@/components/power/shared/cotrade-kit'
import {
  AutoTradeAPI,
  type Mode,
  type LadderProduct, type LadderEndMode, type LadderKillMode, type LadderStatusName,
  type LadderStatus, type LadderSummary, type LadderSession,
} from '@/lib/autotrade-api'

// ── Trader-facing option sets ────────────────────────────────────────────────
// Product: CNC | MTF ONLY. No MIS — the backend 400s an intraday product for a
// multi-day campaign, so we never offer it.
const PRODUCT_OPTIONS: { id: LadderProduct; label: string; hint: string }[] = [
  { id: 'CNC', label: 'Cash (CNC)', hint: 'Delivery — your own funds' },
  { id: 'MTF', label: 'Margin (MTF)', hint: 'Broker-funded margin (leverage)' },
]
const DURATION_OPTIONS: { id: LadderEndMode; label: string; hint: string }[] = [
  { id: 'month_end', label: 'This month (auto)', hint: 'Runs to the end of this month, then stops' },
  { id: 'manual',    label: 'Until I stop',      hint: 'Runs every trading day until you stop it' },
]
const CAPITAL_PRESETS = [100_000, 500_000, 1_000_000, 3_000_000]

// The kill modes, explained in one line each (the confirm modal REQUIRES a choice).
const KILL_OPTIONS: { id: LadderKillMode; label: string; line: string }[] = [
  {
    id: 'flatten_now',
    label: 'Flatten everything now',
    line: 'Exit every open basket immediately and stop the campaign.',
  },
  {
    id: 'stop_new_let_finish',
    label: 'Stop opening new — let open baskets finish',
    line: 'Open no new baskets; let the already-open ones finish their normal exits.',
  },
]

const upper = (s?: string | null) => (s ?? '').toUpperCase()
const isRunning = (s?: string | null) => upper(s) === 'RUNNING'
const isPaused = (s?: string | null) => upper(s) === 'PAUSED'
const isLive = (s?: string | null) => isRunning(s) || isPaused(s)

// A ₹ value or "—" (honest empty for a missing field).
const rs = (v: number | undefined) => (typeof v === 'number' && Number.isFinite(v) ? fmtINR(v) : '—')
const signed = (v: number | undefined) => (typeof v === 'number' && Number.isFinite(v) ? signedINR(v) : '—')
// A backend FRACTION → signed percent string, or "—".
function fracPct(frac: number | undefined, dp = 2): string {
  if (frac == null || !Number.isFinite(frac)) return '—'
  const v = frac * 100
  return `${v >= 0 ? '+' : ''}${v.toFixed(dp).replace(/\.?0+$/, '')}%`
}

type Phase = 'list' | 'config' | 'running'

const inputStyle: React.CSSProperties = {
  background: 'rgba(255,255,255,0.03)',
  border: `1px solid ${C.line2}`,
  color: C.ink,
}

export function AutoLadderPanel({ userId }: { userId?: number | string }) {
  const [phase, setPhase] = useState<Phase>('list')

  // ── Your campaigns (list / re-open) ────────────────────────────────────────
  const [ladders, setLadders] = useState<LadderSummary[] | null>(null)
  const [laddersLoading, setLaddersLoading] = useState(false)
  const [laddersErr, setLaddersErr] = useState<string | null>(null)

  // ── Setup form ─────────────────────────────────────────────────────────────
  const [totalCapital, setTotalCapital] = useState(500_000)
  const [product, setProduct] = useState<LadderProduct>('CNC')
  const [endMode, setEndMode] = useState<LadderEndMode>('month_end')
  const [endDate, setEndDate] = useState('') // optional explicit stop
  const [mode, setMode] = useState<Mode>('paper')
  const [liveConfirm, setLiveConfirm] = useState('')

  // ── Live campaign ──────────────────────────────────────────────────────────
  const [ladderId, setLadderId] = useState<string>('')
  const [status, setStatus] = useState<LadderStatus | null>(null)
  const [statusErr, setStatusErr] = useState<string | null>(null)
  const [busy, setBusy] = useState<null | 'create' | 'pause' | 'resume' | 'kill'>(null)
  const [error, setError] = useState<string | null>(null)

  // Kill modal (mode is REQUIRED — starts unset so the trader must choose).
  const [killOpen, setKillOpen] = useState(false)
  const [killMode, setKillMode] = useState<LadderKillMode | null>(null)

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Derived — the live per-basket budget shown live under the capital input.
  const perBasket = totalCapital > 0 ? Math.floor(totalCapital / 3) : 0
  const liveReady = mode === 'paper' || liveConfirm.trim().toUpperCase() === 'LIVE'

  // ── List load (mount + refresh) ────────────────────────────────────────────
  const loadLadders = useCallback(async () => {
    if (userId == null) { setLadders([]); return }
    setLaddersLoading(true); setLaddersErr(null)
    try {
      const res = await AutoTradeAPI.ladders(userId)
      const list = (res.ladders ?? []).slice().sort((a, b) => {
        const ta = a.created_at ? Date.parse(a.created_at) : 0
        const tb = b.created_at ? Date.parse(b.created_at) : 0
        return tb - ta
      })
      setLadders(list)
    } catch (e) {
      setLaddersErr(e instanceof Error ? e.message : 'Could not load your campaigns.')
    } finally {
      setLaddersLoading(false)
    }
  }, [userId])

  useEffect(() => { loadLadders() }, [loadLadders])

  // ── Live status poll (~5s) ─────────────────────────────────────────────────
  const refreshStatus = useCallback(async (silent = false) => {
    if (!ladderId) return
    if (!silent) setStatusErr(null)
    try {
      const res = await AutoTradeAPI.ladderStatus(ladderId)
      setStatus(res)
      setStatusErr(null)
    } catch (e) {
      // Keep the last-good status visible; just surface the retry note.
      setStatusErr(e instanceof Error ? e.message : 'Could not load campaign status.')
    }
  }, [ladderId])

  useEffect(() => {
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null }
    if (phase === 'running' && ladderId) {
      refreshStatus(true)
      pollRef.current = setInterval(() => refreshStatus(true), 5_000)
    }
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [phase, ladderId, refreshStatus])

  // ── Actions ────────────────────────────────────────────────────────────────
  const openNew = () => {
    setPhase('config')
    setTotalCapital(500_000); setProduct('CNC'); setEndMode('month_end'); setEndDate('')
    setMode('paper'); setLiveConfirm(''); setError(null)
  }

  const backToList = () => {
    setPhase('list'); setLadderId(''); setStatus(null); setStatusErr(null)
    setError(null); setKillOpen(false); setKillMode(null)
    loadLadders()
  }

  // Create THEN start, then show the live panel for the campaign.
  const onStartCampaign = useCallback(async () => {
    if (mode === 'live' && !liveReady) {
      setError('Type LIVE to confirm a real-money campaign, or switch to Paper.')
      return
    }
    setError(null); setBusy('create')
    try {
      const created = await AutoTradeAPI.ladderCreate({
        total_capital: totalCapital,
        order_product: product,
        mode,
        end_date_mode: endMode,
        ...(endDate.trim() ? { end_date: endDate.trim() } : {}),
      })
      const id = created.ladder_id
      await AutoTradeAPI.ladderStart(id)
      setLadderId(id)
      setStatus(null); setStatusErr(null)
      setPhase('running')
      loadLadders()
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not start the campaign.')
    } finally {
      setBusy(null)
    }
  }, [mode, liveReady, totalCapital, product, endMode, endDate, loadLadders])

  const onResume = useCallback(async (l: LadderSummary) => {
    setError(null); setStatusErr(null)
    setLadderId(l.ladder_id)
    setStatus(null)
    setPhase('running')
    try {
      const res = await AutoTradeAPI.ladderStatus(l.ladder_id)
      setStatus(res)
    } catch {
      /* poll will retry */
    }
  }, [])

  const onPause = useCallback(async () => {
    if (!ladderId) return
    setError(null); setBusy('pause')
    try { await AutoTradeAPI.ladderPause(ladderId); await refreshStatus(true) }
    catch (e) { setError(e instanceof Error ? e.message : 'Could not pause the campaign.') }
    finally { setBusy(null) }
  }, [ladderId, refreshStatus])

  const onResumeCampaign = useCallback(async () => {
    if (!ladderId) return
    setError(null); setBusy('resume')
    try { await AutoTradeAPI.ladderResume(ladderId); await refreshStatus(true) }
    catch (e) { setError(e instanceof Error ? e.message : 'Could not resume the campaign.') }
    finally { setBusy(null) }
  }, [ladderId, refreshStatus])

  const onKill = useCallback(async () => {
    if (!ladderId || !killMode) return
    setError(null); setBusy('kill')
    try {
      await AutoTradeAPI.ladderKill(ladderId, killMode)
      setKillOpen(false); setKillMode(null)
      await refreshStatus(true)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not stop the campaign.')
    } finally {
      setBusy(null)
    }
  }, [ladderId, killMode, refreshStatus])

  // ════════════════════════════════════════════════════════════════════════════
  return (
    <div className="flex flex-col gap-4">
      <style>{MECHANISM_CSS}</style>
      <style>{`@keyframes al-live-pulse{0%,100%{opacity:1}50%{opacity:.35}}.al-live-dot{animation:al-live-pulse 1.6s ease-in-out infinite}@media (prefers-reduced-motion: reduce){.al-live-dot{animation:none}}`}</style>

      {/* Intro strip — the "set once, runs the month" mechanism */}
      <div className="rounded-2xl border overflow-hidden"
        style={{ borderColor: 'rgba(63,227,164,0.22)', background: 'linear-gradient(180deg, rgba(63,227,164,0.06), rgba(255,255,255,0.015))' }}>
        <div className="flex items-center gap-3 px-4 py-3">
          <div className="flex items-center -space-x-1 shrink-0">
            <Gear size={26} dir={1} />
            <Gear size={18} dir={-1} dim />
          </div>
          <div className="min-w-0">
            <div className="text-[13.5px] md:text-[14.5px] font-semibold leading-tight" style={{ color: C.ink }}>
              Set it once. Falcon runs your month.
            </div>
            <div className="text-[11px] md:text-[11.5px] leading-snug mt-0.5" style={{ color: C.muted }}>
              Choose your capital, product and duration once — Kanida.AI then opens, manages and
              rolls baskets every trading day. You never manage a basket.
            </div>
          </div>
        </div>
      </div>

      {/* Ships-disabled honesty banner (always on) */}
      <div className="flex items-start gap-2.5 rounded-2xl border px-4 py-3"
        style={{ borderColor: 'rgba(230,180,80,0.32)', background: 'rgba(230,180,80,0.06)' }}>
        <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.shield(16)}</span>
        <div className="text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
          <b style={{ color: C.amber }}>Ships disabled.</b>{' '}
          Campaigns default to <b>PAPER</b> mode (no real orders). Real LIVE orders ADDITIONALLY
          require the server flag <code style={{ color: C.ink }}>FALCON_AUTOTRADE_ENABLED</code> — until
          that is set on the backend, a LIVE campaign stays inert and places nothing.
        </div>
      </div>

      {/* Honest surface error */}
      {error && (
        <div className="flex items-start gap-2 rounded-xl border px-3.5 py-2.5 text-[11.5px] leading-snug"
          style={{ borderColor: 'rgba(232,115,107,0.35)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
          <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(14)}</span>
          <span>{error}</span>
          <button type="button" onClick={() => setError(null)} className="ml-auto shrink-0" style={{ color: C.faint }}>
            {ICON.close(12)}
          </button>
        </div>
      )}

      {/* ── LIST PHASE ─────────────────────────────────────────────────────── */}
      {phase === 'list' && (
        <div className="flex flex-col gap-4">
          <div className="flex items-center gap-2 flex-wrap">
            <span style={{ color: C.mint }}>{ICON.loop(16)}</span>
            <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Your campaigns</span>
            <div className="ml-auto flex items-center gap-2">
              <button type="button" disabled={laddersLoading} onClick={loadLadders}
                className="text-[11.5px] px-2.5 py-1.5 rounded-lg transition-colors disabled:opacity-40"
                style={{ color: C.muted, border: `1px solid ${C.line}` }}>
                {laddersLoading ? 'Refreshing…' : 'Refresh'}
              </button>
              <button type="button" onClick={openNew}
                className="flex items-center gap-1.5 text-[12px] font-semibold px-3.5 py-1.5 rounded-lg transition-opacity"
                style={{ color: '#06130c', background: C.mint }}>
                {ICON.loop(13)} New campaign
              </button>
            </div>
          </div>

          <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
            {laddersLoading && ladders === null ? (
              <p className="text-[12px]" style={{ color: C.muted }}>Loading your campaigns…</p>
            ) : laddersErr ? (
              <div className="flex items-start gap-2 text-[12px] leading-snug" style={{ color: C.ink2 }}>
                <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
                <span>{laddersErr}{' '}
                  <button type="button" onClick={loadLadders} className="underline" style={{ color: C.mint }}>Retry</button>
                </span>
              </div>
            ) : !ladders?.length ? (
              <div className="flex flex-col items-start gap-3">
                <p className="text-[12.5px]" style={{ color: C.muted }}>
                  No campaigns yet. Start one and Falcon runs baskets for you every trading day —
                  it stays here so you can re-open its live view anytime.
                </p>
                <button type="button" onClick={openNew}
                  className="flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold"
                  style={{ color: '#06130c', background: C.mint }}>
                  {ICON.loop(14)} Start your first campaign
                </button>
              </div>
            ) : (
              <ul className="flex flex-col gap-2">
                {ladders.map((l, i) => {
                  const live = isLive(l.status)
                  const day = typeof l.today_pnl === 'number' ? l.today_pnl : null
                  return (
                    <li key={l.ladder_id ?? i}>
                      <button type="button" onClick={() => onResume(l)}
                        className="w-full flex items-center gap-3 rounded-xl border px-3.5 py-3 text-left transition-colors"
                        style={{
                          borderColor: live ? 'rgba(63,227,164,0.32)' : C.line2,
                          background: 'rgba(255,255,255,0.015)',
                        }}>
                        <span className="shrink-0" style={{ color: C.mint }}>{ICON.loop(15)}</span>
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-2">
                            <StatusPill status={l.status} />
                            {l.order_product && <ProductPill product={l.order_product} />}
                            {typeof l.total_capital === 'number' && (
                              <span className="text-[11px] font-semibold" style={{ color: C.ink2 }}>
                                {fmtCapital(l.total_capital)}
                              </span>
                            )}
                          </div>
                          <div className="text-[10.5px] mt-0.5 font-mono truncate" style={{ color: C.faint }}>
                            {l.ladder_id}
                            {l.start_date ? ` · ${l.start_date}` : ''}
                            {l.end_date ? ` → ${l.end_date}` : ''}
                          </div>
                        </div>
                        <div className="shrink-0 text-right">
                          <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>Active baskets</div>
                          <div className="text-[13px] font-semibold tabular-nums" style={{ color: C.ink }}>
                            {typeof l.n_active_baskets === 'number' ? l.n_active_baskets : '—'}
                          </div>
                          {day != null && (
                            <div className="text-[10.5px] font-semibold tabular-nums mt-0.5" style={{ color: pctTone(day) }}>
                              {signed(day)}
                            </div>
                          )}
                        </div>
                        <span className="shrink-0" style={{ color: C.faint }}>{ICON.chevronR(14)}</span>
                      </button>
                    </li>
                  )
                })}
              </ul>
            )}
          </div>
        </div>
      )}

      {/* ── CONFIG PHASE (setup form) ──────────────────────────────────────── */}
      {phase === 'config' && (
        <div className="flex flex-col gap-4">
          <div className="flex items-center gap-2">
            <button type="button" onClick={backToList}
              className="flex items-center gap-1.5 text-[11.5px] px-2.5 py-1.5 rounded-lg transition-colors"
              style={{ color: C.muted, border: `1px solid ${C.line}` }}>
              {ICON.back(13)} Back
            </button>
            <span className="text-[14px] font-semibold" style={{ color: C.ink }}>New monthly campaign</span>
          </div>

          <div className="rounded-2xl border p-4 sm:p-5 flex flex-col gap-5" style={{ borderColor: C.line2, background: C.card }}>
            {/* Total capital */}
            <Field label="Total capital" hint={`Falcon splits this across ~3 baskets. Each basket ≈ ${fmtINR(perBasket)}`}>
              <div className="flex flex-col gap-2">
                <input
                  type="number" min={0} step={10_000} value={totalCapital}
                  onChange={(e) => setTotalCapital(Math.max(0, Number(e.target.value) || 0))}
                  className="w-full rounded-xl px-3 py-2 text-[13px] tabular-nums outline-none"
                  style={inputStyle}
                />
                <div className="flex flex-wrap gap-1.5">
                  {CAPITAL_PRESETS.map((p) => (
                    <button key={p} type="button" onClick={() => setTotalCapital(p)}
                      className="px-2.5 py-1 rounded-lg text-[11px] font-medium transition-colors"
                      style={totalCapital === p
                        ? { color: '#06130c', background: C.mint }
                        : { color: C.ink2, border: `1px solid ${C.line2}` }}>
                      {fmtCapital(p)}
                    </button>
                  ))}
                </div>
              </div>
            </Field>

            {/* Product — CNC | MTF only (no MIS) */}
            <Field label="Product" hint={PRODUCT_OPTIONS.find((o) => o.id === product)?.hint}>
              <Segmented options={PRODUCT_OPTIONS} value={product} onChange={setProduct} />
            </Field>

            {/* Duration */}
            <Field label="Duration">
              <div className="flex flex-col gap-2">
                {DURATION_OPTIONS.map((o) => {
                  const active = endMode === o.id
                  return (
                    <button key={o.id} type="button" onClick={() => setEndMode(o.id)}
                      className="flex items-start gap-2.5 rounded-xl border px-3.5 py-2.5 text-left transition-colors"
                      style={{
                        borderColor: active ? 'rgba(63,227,164,0.5)' : C.line2,
                        background: active ? 'rgba(63,227,164,0.06)' : 'rgba(255,255,255,0.015)',
                      }}>
                      <span className="shrink-0 mt-0.5 grid place-items-center w-4 h-4 rounded-full"
                        style={{ boxShadow: `inset 0 0 0 1.5px ${active ? C.mint : C.line2}` }}>
                        {active && <span className="w-2 h-2 rounded-full" style={{ background: C.mint }} />}
                      </span>
                      <span className="min-w-0">
                        <span className="block text-[12.5px] font-semibold" style={{ color: active ? C.ink : C.ink2 }}>{o.label}</span>
                        <span className="block text-[10.5px] leading-snug mt-0.5" style={{ color: C.faint }}>{o.hint}</span>
                      </span>
                    </button>
                  )
                })}
                {/* Optional explicit stop date */}
                <label className="flex items-center gap-2 text-[11px] mt-0.5" style={{ color: C.muted }}>
                  <span className="shrink-0">Or stop on a specific date (optional):</span>
                  <input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)}
                    className="rounded-lg px-2 py-1 text-[12px] outline-none" style={inputStyle} />
                </label>
              </div>
            </Field>

            {/* Mode */}
            <Field label="Mode" hint="Paper = simulated, no real orders. Live = real money on your broker.">
              <div className="flex flex-col gap-2.5">
                <Segmented
                  options={[{ id: 'paper' as Mode, label: 'Paper' }, { id: 'live' as Mode, label: 'Live' }]}
                  value={mode}
                  onChange={(m) => { setMode(m); if (m === 'paper') setLiveConfirm('') }}
                />
                {mode === 'live' && (
                  <div className="rounded-xl border px-3.5 py-3 flex flex-col gap-2"
                    style={{ borderColor: 'rgba(232,115,107,0.4)', background: 'rgba(232,115,107,0.06)' }}>
                    <div className="flex items-start gap-2 text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                      <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.shield(14)}</span>
                      <span>
                        <b style={{ color: C.red }}>Live places real orders</b> on your broker for the whole
                        month. Type <b style={{ color: C.ink }}>LIVE</b> to confirm.
                      </span>
                    </div>
                    <input value={liveConfirm} onChange={(e) => setLiveConfirm(e.target.value)}
                      placeholder="Type LIVE"
                      className="w-full rounded-lg px-3 py-2 text-[13px] outline-none" style={inputStyle} />
                  </div>
                )}
              </div>
            </Field>

            {/* Start */}
            <div className="flex items-center gap-3 pt-1">
              <button type="button" disabled={busy != null || !liveReady || totalCapital <= 0} onClick={onStartCampaign}
                className="flex items-center gap-2 px-5 py-2.5 rounded-xl text-[13px] font-semibold transition-opacity disabled:opacity-40"
                style={{ color: '#06130c', background: C.mint }}>
                {ICON.loop(15)} {busy === 'create' ? 'Starting…' : 'Start campaign'}
              </button>
              <span className="text-[10.5px]" style={{ color: C.faint }}>
                Creates the campaign and starts it — Falcon runs baskets daily until it ends.
              </span>
            </div>
          </div>
        </div>
      )}

      {/* ── RUNNING PHASE (live panel) ─────────────────────────────────────── */}
      {phase === 'running' && (
        <div className="flex flex-col gap-4">
          <div className="flex items-center gap-2 flex-wrap">
            <button type="button" onClick={backToList}
              className="flex items-center gap-1.5 text-[11.5px] px-2.5 py-1.5 rounded-lg transition-colors"
              style={{ color: C.muted, border: `1px solid ${C.line}` }}>
              {ICON.back(13)} Campaigns
            </button>
            <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Monthly campaign</span>
            <div className="ml-auto flex items-center gap-2">
              <StatusPill status={status?.status} />
              {status?.order_product && <ProductPill product={status.order_product} />}
            </div>
          </div>

          {/* Calm retry state (mirrors Sessions) — keeps last-good numbers visible */}
          {statusErr && (
            <div className="flex items-start gap-2 rounded-xl border px-3.5 py-2.5 text-[11.5px] leading-snug"
              style={{ borderColor: 'rgba(230,180,80,0.35)', background: 'rgba(230,180,80,0.06)', color: C.ink2 }}>
              <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
              <span>{statusErr}{' '}
                <button type="button" onClick={() => refreshStatus(false)} className="underline" style={{ color: C.mint }}>Retry</button>
              </span>
            </div>
          )}

          {status === null && !statusErr ? (
            <div className="rounded-2xl border p-6 text-center" style={{ borderColor: C.line2, background: C.card }}>
              <p className="text-[12px]" style={{ color: C.muted }}>Loading campaign…</p>
            </div>
          ) : (
            <>
              {/* Downturn alert — calm, informational, NOT an error */}
              {status?.alert?.active && (
                <div className="flex items-start gap-2.5 rounded-2xl border px-4 py-3"
                  style={{ borderColor: 'rgba(230,180,80,0.4)', background: 'rgba(230,180,80,0.06)' }}>
                  <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(15)}</span>
                  <div className="text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                    <div>{status.alert.message}</div>
                    {typeof status.alert.trailing_5d_avg_return === 'number' && (
                      <div className="text-[10.5px] mt-1" style={{ color: C.faint }}>
                        Trailing 5-day average return: <b style={{ color: pctTone(status.alert.trailing_5d_avg_return) }}>
                          {fracPct(status.alert.trailing_5d_avg_return)}</b>
                        {typeof status.alert.n_days_in_window === 'number' ? ` · over ${status.alert.n_days_in_window} days` : ''}
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Capital row */}
              <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 rounded-2xl border px-4 py-4"
                style={{ borderColor: C.line2, background: C.card }}>
                <Stat label="Capital deployed" value={rs(status?.capital_deployed)} />
                <Stat label="Capital free" value={rs(status?.capital_free)} />
                <Stat
                  label="Total capital"
                  value={rs(status?.total_capital)}
                  sub={typeof status?.per_basket_capital === 'number' ? `per basket ${fmtINR(status.per_basket_capital)}` : undefined}
                />
              </div>

              {/* Activity + P&L row */}
              <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
                <Stat label="Active baskets" value={typeof status?.n_active_baskets === 'number' ? String(status.n_active_baskets) : '—'} />
                <Stat label="Open positions" value={typeof status?.n_open_positions === 'number' ? String(status.n_open_positions) : '—'} />
                <Stat label="Daily P&L" value={signed(status?.today_pnl)} valueColor={typeof status?.today_pnl === 'number' ? pctTone(status.today_pnl) : C.faint} />
                <Stat label="Realized" value={signed(status?.realized_pnl)} valueColor={typeof status?.realized_pnl === 'number' ? pctTone(status.realized_pnl) : C.faint} />
                <Stat label="Unrealized" value={signed(status?.unrealized_pnl)} valueColor={typeof status?.unrealized_pnl === 'number' ? pctTone(status.unrealized_pnl) : C.faint} />
              </div>

              {/* Duration line */}
              {(status?.start_date || status?.end_date) && (
                <div className="flex items-center gap-2 text-[11px]" style={{ color: C.muted }}>
                  <span style={{ color: C.faint }}>{ICON.clock(13)}</span>
                  <span>
                    {status?.start_date ? `Started ${status.start_date}` : 'Running'}
                    {status?.end_date ? ` · runs to ${status.end_date}` : ''}
                  </span>
                </div>
              )}

              {/* Controls */}
              {isLive(status?.status) && (
                <div className="flex items-center gap-2.5 flex-wrap">
                  {isPaused(status?.status) ? (
                    <button type="button" disabled={busy != null} onClick={onResumeCampaign}
                      className="flex items-center gap-1.5 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
                      style={{ color: '#06130c', background: C.mint }}>
                      {ICON.bolt(14)} {busy === 'resume' ? 'Resuming…' : 'Resume'}
                    </button>
                  ) : (
                    <button type="button" disabled={busy != null} onClick={onPause}
                      className="flex items-center gap-1.5 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
                      style={{ color: C.ink2, border: `1px solid ${C.line2}` }}>
                      {ICON.clock(14)} {busy === 'pause' ? 'Pausing…' : 'Pause'}
                    </button>
                  )}
                  <button type="button" disabled={busy != null} onClick={() => { setKillMode(null); setKillOpen(true) }}
                    className="flex items-center gap-1.5 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
                    style={{ color: C.red, background: 'rgba(232,115,107,0.12)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }}>
                    {ICON.close(13)} Stop campaign
                  </button>
                  <button type="button" onClick={() => refreshStatus(false)}
                    className="ml-auto text-[11px] px-2.5 py-1.5 rounded-lg transition-colors"
                    style={{ color: C.muted, border: `1px solid ${C.line}` }}>
                    Refresh
                  </button>
                </div>
              )}

              {/* Child baskets */}
              <ChildBaskets sessions={status?.sessions} />
            </>
          )}
        </div>
      )}

      {/* ── KILL MODAL — mode is REQUIRED (no silent default) ──────────────── */}
      {killOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center px-4"
          style={{ background: 'rgba(0,0,0,0.6)' }} onClick={() => busy == null && setKillOpen(false)}>
          <div className="w-full max-w-md rounded-2xl border p-5"
            style={{ borderColor: 'rgba(232,115,107,0.4)', background: C.panel }} onClick={(e) => e.stopPropagation()}>
            <div className="flex items-center gap-2 mb-2">
              <span style={{ color: C.red }}>{ICON.shield(16)}</span>
              <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Stop the campaign</span>
              <button type="button" onClick={() => setKillOpen(false)} className="ml-auto shrink-0" style={{ color: C.faint }}>
                {ICON.close(14)}
              </button>
            </div>
            <p className="text-[11.5px] leading-snug mb-3" style={{ color: C.muted }}>
              Choose how to wind the campaign down — this is required.
            </p>
            <div className="flex flex-col gap-2">
              {KILL_OPTIONS.map((o) => {
                const active = killMode === o.id
                return (
                  <button key={o.id} type="button" onClick={() => setKillMode(o.id)}
                    className="flex items-start gap-2.5 rounded-xl border px-3.5 py-3 text-left transition-colors"
                    style={{
                      borderColor: active ? 'rgba(232,115,107,0.55)' : C.line2,
                      background: active ? 'rgba(232,115,107,0.07)' : 'rgba(255,255,255,0.015)',
                    }}>
                    <span className="shrink-0 mt-0.5 grid place-items-center w-4 h-4 rounded-full"
                      style={{ boxShadow: `inset 0 0 0 1.5px ${active ? C.red : C.line2}` }}>
                      {active && <span className="w-2 h-2 rounded-full" style={{ background: C.red }} />}
                    </span>
                    <span className="min-w-0">
                      <span className="block text-[12.5px] font-semibold" style={{ color: active ? C.ink : C.ink2 }}>{o.label}</span>
                      <span className="block text-[10.5px] leading-snug mt-0.5" style={{ color: C.faint }}>{o.line}</span>
                    </span>
                  </button>
                )
              })}
            </div>
            <div className="flex items-center gap-2.5 mt-4">
              <button type="button" disabled={busy != null || killMode == null} onClick={onKill}
                className="flex items-center gap-1.5 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
                style={{ color: '#fff', background: C.red }}>
                {busy === 'kill' ? 'Stopping…' : 'Confirm stop'}
              </button>
              <button type="button" disabled={busy != null} onClick={() => setKillOpen(false)}
                className="px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors disabled:opacity-40"
                style={{ color: C.ink2, border: `1px solid ${C.line2}` }}>
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// ── Child baskets list (from status.sessions[]) ──────────────────────────────
// Each is an ordinary session carrying ladder_id — deep-linked into the Sessions
// tab via a same-origin hash the shell reads (never surfaces "sleeve" etc.).
function ChildBaskets({ sessions }: { sessions?: LadderSession[] }) {
  if (!sessions || sessions.length === 0) {
    return (
      <div className="rounded-2xl border px-4 py-4" style={{ borderColor: C.line2, background: C.card }}>
        <div className="flex items-center gap-2 mb-1">
          <span style={{ color: C.mint }}>{ICON.bolt(14)}</span>
          <span className="text-[12.5px] font-semibold" style={{ color: C.ink }}>Baskets</span>
        </div>
        <p className="text-[11.5px]" style={{ color: C.muted }}>
          No baskets open yet — Falcon opens them on the next trading session.
        </p>
      </div>
    )
  }
  return (
    <div className="rounded-2xl border px-4 py-4" style={{ borderColor: C.line2, background: C.card }}>
      <div className="flex items-center gap-2 mb-2">
        <span style={{ color: C.mint }}>{ICON.bolt(14)}</span>
        <span className="text-[12.5px] font-semibold" style={{ color: C.ink }}>Baskets</span>
        <span className="text-[10.5px]" style={{ color: C.faint }}>({sessions.length}) — each also shows in the Sessions tab</span>
      </div>
      <ul className="flex flex-col gap-2">
        {sessions.map((s, i) => (
          <li key={s.session_id ?? i}>
            <a
              href={`?attab=sessions&session=${encodeURIComponent(s.session_id ?? '')}`}
              className="flex items-center gap-3 rounded-xl border px-3.5 py-2.5 transition-colors"
              style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.015)' }}>
              <span className="shrink-0" style={{ color: C.mint }}>{ICON.bolt(13)}</span>
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-2">
                  <span className="text-[12px] font-semibold truncate" style={{ color: C.ink }}>
                    {s.status ?? 'basket'}
                  </span>
                  {typeof s.total_allocated_capital === 'number' && (
                    <span className="text-[10.5px] font-semibold" style={{ color: C.ink2 }}>
                      {fmtINR(s.total_allocated_capital)}
                    </span>
                  )}
                </div>
                <div className="text-[10px] mt-0.5 font-mono truncate" style={{ color: C.faint }}>
                  {s.session_id}
                  {s.started_at ? ` · opened ${s.started_at}` : ''}
                  {s.closed_at ? ` · closed ${s.closed_at}` : ''}
                </div>
              </div>
              <span className="shrink-0 text-[10px]" style={{ color: C.mint }}>Open in Sessions →</span>
            </a>
          </li>
        ))}
      </ul>
    </div>
  )
}

// ── Shared small primitives (local — mirror PortfolioAutoTrade look) ─────────
function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1.5">
      <label className="text-[11px] font-semibold uppercase tracking-[0.05em]" style={{ color: C.muted }}>{label}</label>
      {children}
      {hint && <span className="text-[10.5px] leading-snug" style={{ color: C.faint }}>{hint}</span>}
    </div>
  )
}

function Segmented<T extends string>({
  options, value, onChange,
}: { options: { id: T; label: string }[]; value: T; onChange: (v: T) => void }) {
  return (
    <div className="inline-flex rounded-xl border p-0.5" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
      {options.map((o) => {
        const active = o.id === value
        return (
          <button key={o.id} type="button" onClick={() => onChange(o.id)}
            className="px-3 py-1.5 rounded-lg text-[12px] font-medium transition-colors"
            style={{ color: active ? '#06130c' : C.ink2, background: active ? C.mint : 'transparent' }}>
            {o.label}
          </button>
        )
      })}
    </div>
  )
}

function Stat({ label, value, valueColor, sub }: { label: string; value: string; valueColor?: string; sub?: string }) {
  return (
    <div className="rounded-xl border px-3 py-2.5" style={{ borderColor: C.line, background: 'rgba(255,255,255,0.015)' }}>
      <div className="text-[10px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>{label}</div>
      <div className="text-[15px] font-semibold mt-0.5 tabular-nums" style={{ color: valueColor ?? C.ink }}>{value}</div>
      {sub && <div className="text-[9.5px] mt-0.5" style={{ color: C.faint }}>{sub}</div>}
    </div>
  )
}

function StatusPill({ status }: { status?: LadderStatusName }) {
  const s = upper(status)
  const running = s === 'RUNNING'
  const paused = s === 'PAUSED'
  const done = s === 'COMPLETED'
  const tone = running ? C.mint : paused ? C.amber : done ? C.mint : C.muted
  const bg = running || done ? 'rgba(63,227,164,0.12)' : paused ? 'rgba(230,180,80,0.12)' : 'rgba(133,153,144,0.13)'
  const ring = running || done ? 'rgba(63,227,164,0.4)' : paused ? 'rgba(230,180,80,0.4)' : 'rgba(133,153,144,0.4)'
  const label = s || 'campaign'
  return (
    <span className="inline-flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
      style={{ color: tone, background: bg, boxShadow: `inset 0 0 0 1px ${ring}` }}>
      {running && <span className="inline-block w-1.5 h-1.5 rounded-full al-live-dot" style={{ background: C.mint }} />}
      {label}
    </span>
  )
}

function ProductPill({ product }: { product: LadderProduct }) {
  return (
    <span className="text-[9px] font-mono uppercase tracking-[0.07em] rounded-full px-2 py-0.5"
      style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
      {product}
    </span>
  )
}
