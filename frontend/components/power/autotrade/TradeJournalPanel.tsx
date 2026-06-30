'use client'

/**
 * TradeJournalPanel — daily trade journal for an AutoTrade session.
 *
 * Receives sessionId as a prop; fetches journal data on mount + on manual
 * Refresh. Includes:
 *   A. Session summary stat cards
 *   B. Positions table with expandable row detail
 *   C. Review flags section (visible only when review_items.length > 0)
 *   D. Export toolbar: Copy (plain text), CSV, Excel (CSV with .xlsx extension)
 *   E. Session date selector (lists recent sessions via GET /autotrade/sessions)
 *
 * Mint/F2 theme. Flat surfaces, no gradients/shadows. Dense 11-12px table type.
 * No emoji anywhere.
 */

import { useCallback, useEffect, useRef, useState } from 'react'
import { C, ICON, fmtINR, signedINR, fmtPct, pctTone } from '@/components/power/shared/cotrade-kit'
import {
  AutoTradeAPI,
  type TradeJournal,
  type JournalPosition,
  type SessionSummary,
} from '@/lib/autotrade-api'

// ── Colour constants used throughout ─────────────────────────────────────────
const RED    = 'var(--f2-red)'
const AMBER  = 'var(--f2-amber)'
const ORANGE = '#f97316'
const MINT   = 'var(--f2-mint)'

// ── Review-flag colours ───────────────────────────────────────────────────────
const FLAG_STYLE: Record<string, { color: string; bg: string }> = {
  DEEP_LOSS:              { color: RED,   bg: 'rgba(232,115,107,0.14)' },
  LARGE_WIN:              { color: MINT,  bg: 'rgba(63,227,164,0.14)'  },
  STOP_RECOVERED:         { color: AMBER, bg: 'rgba(230,180,80,0.14)'  },
  CONTINUED_DECLINE_OPEN: { color: ORANGE, bg: 'rgba(249,115,22,0.14)' },
  WINNER_OPEN:            { color: MINT,  bg: 'rgba(63,227,164,0.14)'  },
}
function flagStyle(flag: string | null): { color: string; bg: string } {
  if (!flag) return { color: C.faint, bg: 'rgba(255,255,255,0.05)' }
  return FLAG_STYLE[flag] ?? { color: C.muted, bg: 'rgba(255,255,255,0.06)' }
}

// ── Close-reason badge label ──────────────────────────────────────────────────
function reasonLabel(r: string | null): string {
  if (!r) return 'OPEN'
  switch (r.toUpperCase()) {
    case 'GTT_STOP':    return 'GTT-STOP'
    case 'GTT_TARGET':  return 'GTT-TARGET'
    case 'TRAIL':       return 'TRAIL'
    case 'TRAIL_EXIT':  return 'TRAIL'
    case 'SQUARE_OFF':  return 'SQUARE-OFF'
    case 'KILL':
    case 'KILL_SWITCH': return 'KILL'
    default:            return r.toUpperCase().replace(/_/g, '-')
  }
}
function reasonColor(r: string | null): { color: string; bg: string } {
  if (!r) return { color: MINT, bg: 'rgba(63,227,164,0.12)' }
  const u = r.toUpperCase()
  if (u.includes('STOP'))    return { color: RED,   bg: 'rgba(232,115,107,0.12)' }
  if (u.includes('TARGET'))  return { color: MINT,  bg: 'rgba(63,227,164,0.12)'  }
  if (u.includes('TRAIL'))   return { color: AMBER, bg: 'rgba(230,180,80,0.12)'  }
  if (u === 'SQUARE_OFF' || u === 'SQUARE-OFF') return { color: C.muted, bg: 'rgba(255,255,255,0.07)' }
  if (u.includes('KILL'))    return { color: RED,   bg: 'rgba(232,115,107,0.14)' }
  return { color: C.ink2, bg: 'rgba(255,255,255,0.06)' }
}

// ── Number formatters ─────────────────────────────────────────────────────────
function fmt2(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '—'
  return v.toFixed(2)
}
function fmtHold(min: number | null | undefined): string {
  if (min == null || !Number.isFinite(min)) return '—'
  if (min < 60) return `${Math.round(min)}m`
  const h = Math.floor(min / 60)
  const m = Math.round(min % 60)
  return `${h}h ${m}m`
}
function fmtPrice(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return '—'
  return v.toFixed(2)
}

// ── Plain-text journal builder (for Copy) ────────────────────────────────────
function buildPlainText(j: TradeJournal): string {
  const s = j.session_summary
  const lines: string[] = [
    `Trade Journal — ${j.trading_date}  |  Session: ${j.session_id}`,
    `Strategy: ${j.strategy}  |  Mode: ${j.mode.toUpperCase()}`,
    '',
    '== Session Summary ==',
    `Total P&L:        ${signedINR(s.total_pnl)}  (${s.total_pnl_pct_invested >= 0 ? '+' : ''}${s.total_pnl_pct_invested.toFixed(2)}% invested / ${s.total_pnl_pct_fund >= 0 ? '+' : ''}${s.total_pnl_pct_fund.toFixed(2)}% fund)`,
    `Realised P&L:     ${signedINR(s.total_realised_pnl)}`,
    `Unrealised P&L:   ${signedINR(s.total_unrealised_pnl)}`,
    `Capital:          ${fmtINR(s.total_allocated_capital)}  |  Invested basis: ${fmtINR(s.invested_basis)}  |  Leverage: ${s.leverage.toFixed(2)}x`,
    `Positions:        ${s.n_positions} total  |  ${s.n_open} open  |  ${s.n_closed} closed`,
    `Winners / Losers: ${s.n_winners}W / ${s.n_losers}L`,
    `Stops hit:        ${s.n_stop_hits}  |  Target hits: ${s.n_target_hits}  |  Trail exits: ${s.n_trail_exits}`,
    `Avg hold:         ${fmtHold(s.avg_hold_minutes)}`,
    s.best_trade  ? `Best trade:       ${s.best_trade.symbol}  ${signedINR(s.best_trade.pnl_rs)} (${s.best_trade.pnl_pct >= 0 ? '+' : ''}${s.best_trade.pnl_pct.toFixed(2)}%)` : '',
    s.worst_trade ? `Worst trade:      ${s.worst_trade.symbol}  ${signedINR(s.worst_trade.pnl_rs)} (${s.worst_trade.pnl_pct >= 0 ? '+' : ''}${s.worst_trade.pnl_pct.toFixed(2)}%)` : '',
    '',
    '== Positions ==',
    ['#', 'Symbol', 'Qty', 'Entry', 'Exit', 'P&L Rs', 'P&L %', 'Held', 'Reason', 'Flag'].join('\t'),
  ]
  j.positions.forEach((p, i) => {
    lines.push([
      i + 1,
      p.symbol,
      p.qty,
      fmtPrice(p.avg_price),
      fmtPrice(p.exit_price),
      p.pnl_rs.toFixed(0),
      `${p.pnl_pct >= 0 ? '+' : ''}${p.pnl_pct.toFixed(2)}%`,
      fmtHold(p.hold_minutes),
      reasonLabel(p.close_reason),
      p.review_flag ?? '',
    ].join('\t'))
  })
  if (j.review_items.length > 0) {
    lines.push('')
    lines.push('== Needs Review ==')
    j.review_items.forEach((r) => lines.push(`[${r.flag}] ${r.symbol}: ${r.note}`))
  }
  return lines.filter((l) => l !== undefined).join('\n')
}

// ── CSV builder (positions only) ─────────────────────────────────────────────
function buildCSV(j: TradeJournal): string {
  const header = 'Symbol,Qty,Entry,Exit,PnL_Rs,PnL_Pct,Hold_Min,Close_Reason,Review_Flag'
  const rows = j.positions.map((p) =>
    [
      p.symbol,
      p.qty,
      fmtPrice(p.avg_price),
      fmtPrice(p.exit_price),
      p.pnl_rs.toFixed(2),
      p.pnl_pct.toFixed(2),
      p.hold_minutes != null ? p.hold_minutes.toFixed(0) : '',
      p.close_reason ?? '',
      p.review_flag ?? '',
    ].join(','),
  )
  return [header, ...rows].join('\n')
}

// ── Download helper ───────────────────────────────────────────────────────────
function downloadText(content: string, filename: string, mime = 'text/csv;charset=utf-8;') {
  const blob = new Blob([content], { type: mime })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

// ── Tiny stat card ────────────────────────────────────────────────────────────
function StatCard({ label, value, color }: { label: string; value: React.ReactNode; color?: string }) {
  return (
    <div
      className="shrink-0 flex flex-col gap-1 rounded-xl border px-3 py-2.5 min-w-[100px]"
      style={{ borderColor: C.line2, background: C.card }}
    >
      <span className="text-[9.5px] font-semibold uppercase tracking-[0.06em]" style={{ color: C.muted }}>
        {label}
      </span>
      <span className="text-[14px] font-semibold leading-tight" style={{ color: color ?? C.ink }}>
        {value}
      </span>
    </div>
  )
}

// ── Expandable position row ───────────────────────────────────────────────────
function PositionRow({ pos, idx }: { pos: JournalPosition; idx: number }) {
  const [open, setOpen] = useState(false)
  const pnlColor = pos.pnl_rs >= 0 ? MINT : RED
  const rc = reasonColor(pos.close_reason)
  const fs = flagStyle(pos.review_flag)

  return (
    <>
      {/* Main row */}
      <tr
        onClick={() => setOpen((v) => !v)}
        className="cursor-pointer transition-colors"
        style={{ borderBottom: `1px solid ${C.line}` }}
      >
        <td className="py-2 pl-3 pr-2 text-[11px] tabular-nums" style={{ color: C.faint }}>{idx}</td>

        <td className="py-2 pr-3 text-[12px] font-semibold" style={{ color: C.ink }}>
          {pos.symbol}
          {pos.status === 'OPEN' && (
            <span className="ml-1.5 text-[9px] font-mono uppercase tracking-[0.05em] rounded-full px-1.5 py-0.5"
              style={{ color: MINT, background: 'rgba(63,227,164,0.12)' }}>
              OPEN
            </span>
          )}
        </td>

        <td className="py-2 pr-3 text-[11px] tabular-nums text-right" style={{ color: C.ink2 }}>{pos.qty}</td>

        <td className="py-2 pr-3 text-[11px] tabular-nums text-right font-mono" style={{ color: C.ink2 }}>
          {fmtPrice(pos.avg_price)}
        </td>

        <td className="py-2 pr-3 text-[11px] tabular-nums text-right font-mono" style={{ color: C.ink2 }}>
          {fmtPrice(pos.exit_price)}
        </td>

        <td className="py-2 pr-3 text-[11px] tabular-nums text-right font-semibold" style={{ color: pnlColor }}>
          {signedINR(pos.pnl_rs)}
        </td>

        <td className="py-2 pr-3 text-[11px] tabular-nums text-right font-semibold" style={{ color: pnlColor }}>
          {fmtPct(pos.pnl_pct)}
        </td>

        <td className="py-2 pr-3 text-[11px] tabular-nums text-right" style={{ color: C.muted }}>
          {fmtHold(pos.hold_minutes)}
        </td>

        <td className="py-2 pr-3 text-[10.5px]">
          <span className="inline-flex items-center rounded-full px-2 py-0.5 font-mono font-semibold"
            style={{ color: rc.color, background: rc.bg }}>
            {reasonLabel(pos.close_reason)}
          </span>
        </td>

        <td className="py-2 pr-3 text-[10.5px]">
          {pos.review_flag && (
            <span className="inline-flex items-center rounded-full px-2 py-0.5 font-semibold"
              style={{ color: fs.color, background: fs.bg }}>
              {pos.review_flag.replace(/_/g, ' ')}
            </span>
          )}
        </td>

        <td className="py-2 pr-3 text-[11px] text-right" style={{ color: C.faint }}>
          <span style={{ color: open ? MINT : C.faint }}>{ICON.chevron(13)}</span>
        </td>
      </tr>

      {/* Expanded detail row */}
      {open && (
        <tr style={{ background: C.card2 }}>
          <td colSpan={11} className="px-4 py-3">
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-x-6 gap-y-2 text-[11px]">
              <DetailPair label="Avg entry price" value={fmtPrice(pos.avg_price)} />
              <DetailPair label="Exit price"      value={fmtPrice(pos.exit_price)} />
              <DetailPair label="LTP"             value={fmtPrice(pos.ltp)} />
              <DetailPair label="Invested"        value={fmtINR(pos.invested_rs)} />
              <DetailPair label="SL level"        value={fmtPrice(pos.sl_level)} />
              <DetailPair label="Target"          value={fmtPrice(pos.target_price)} />
              <DetailPair label="SL %"            value={`${fmt2(pos.sl_pct)}%`} />
              <DetailPair label="Target %"        value={`${fmt2(pos.target_pct)}%`} />
              {pos.gtt_stop != null && (
                <DetailPair label="GTT stop"      value={fmtPrice(pos.gtt_stop)} />
              )}
              {pos.gtt_target != null && (
                <DetailPair label="GTT target"    value={fmtPrice(pos.gtt_target)} />
              )}
              <DetailPair label="Realised P&L"    value={pos.realised_pnl != null ? signedINR(pos.realised_pnl) : '—'} />
              <DetailPair label="Unrealised P&L"  value={pos.unrealised_pnl != null ? signedINR(pos.unrealised_pnl) : '—'} />
              <DetailPair label="Hold time"       value={fmtHold(pos.hold_minutes)} />
              <DetailPair label="Opened at"       value={pos.opened_at ? fmtTimestamp(pos.opened_at) : '—'} />
              <DetailPair label="Closed at"       value={pos.closed_at ? fmtTimestamp(pos.closed_at) : '—'} />
              <DetailPair label="Exchange"        value={pos.exchange} />
            </div>
            {pos.review_note && (
              <div className="mt-2.5 rounded-lg border px-3 py-2 text-[11px] leading-snug"
                style={{ borderColor: fs.color + '40', background: fs.bg, color: C.ink2 }}>
                <b style={{ color: fs.color }}>Review note:</b> {pos.review_note}
              </div>
            )}
          </td>
        </tr>
      )}
    </>
  )
}

function DetailPair({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-[9.5px] font-semibold uppercase tracking-[0.05em]" style={{ color: C.faint }}>{label}</span>
      <span className="font-mono" style={{ color: C.ink2 }}>{value}</span>
    </div>
  )
}

function fmtTimestamp(ts: string): string {
  // Show just HH:MM:SS IST if it looks like an ISO timestamp, else pass through
  try {
    const d = new Date(ts)
    if (isNaN(d.getTime())) return ts
    // Shift to IST (+5:30)
    const ist = new Date(d.getTime() + 330 * 60_000)
    return ist.toISOString().slice(11, 19) + ' IST'
  } catch {
    return ts
  }
}

// ── Skeleton loader ───────────────────────────────────────────────────────────
function Skeleton() {
  return (
    <div className="flex flex-col gap-3 animate-pulse">
      {/* Stat cards row */}
      <div className="flex gap-2 overflow-x-auto pb-1">
        {Array.from({ length: 8 }).map((_, i) => (
          <div key={i} className="shrink-0 h-14 w-28 rounded-xl" style={{ background: C.card }} />
        ))}
      </div>
      {/* Table skeleton */}
      <div className="rounded-xl border overflow-hidden" style={{ borderColor: C.line2, background: C.card }}>
        {Array.from({ length: 6 }).map((_, i) => (
          <div key={i} className="h-9 border-b" style={{ borderColor: C.line, background: i % 2 === 0 ? C.card : C.card2 }} />
        ))}
      </div>
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════════
// Main panel
// ═══════════════════════════════════════════════════════════════════════════════
export function TradeJournalPanel({ sessionId }: { sessionId: string }) {
  const [journal, setJournal]           = useState<TradeJournal | null>(null)
  const [loading, setLoading]           = useState(false)
  const [error, setError]               = useState<string | null>(null)
  const [sessions, setSessions]         = useState<SessionSummary[]>([])
  const [selectedId, setSelectedId]     = useState<string>(sessionId)
  const [copiedToast, setCopiedToast]   = useState(false)
  const toastRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Keep selectedId in sync when the prop changes (parent selects a session)
  useEffect(() => { setSelectedId(sessionId) }, [sessionId])

  // Load the recent sessions list for the date selector
  useEffect(() => {
    AutoTradeAPI.listSessions()
      .then((res) => {
        const list = (res.sessions ?? []).slice().sort((a, b) => {
          const ta = a.created_at ? Date.parse(a.created_at) : 0
          const tb = b.created_at ? Date.parse(b.created_at) : 0
          return tb - ta
        })
        setSessions(list)
      })
      .catch(() => { /* non-fatal — selector just won't populate */ })
  }, [])

  const fetchJournal = useCallback(async (id: string) => {
    if (!id) return
    setLoading(true)
    setError(null)
    try {
      const data = await AutoTradeAPI.journal(id)
      setJournal(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not load the journal.')
      setJournal(null)
    } finally {
      setLoading(false)
    }
  }, [])

  // Fetch on mount and whenever selected session changes
  useEffect(() => { fetchJournal(selectedId) }, [selectedId, fetchJournal])

  // ── Copy handler ────────────────────────────────────────────────────────────
  const onCopy = useCallback(() => {
    if (!journal) return
    const text = buildPlainText(journal)
    navigator.clipboard.writeText(text).then(() => {
      setCopiedToast(true)
      if (toastRef.current) clearTimeout(toastRef.current)
      toastRef.current = setTimeout(() => setCopiedToast(false), 2000)
    }).catch(() => { /* clipboard denied — silently ignore */ })
  }, [journal])

  // ── CSV download ────────────────────────────────────────────────────────────
  const onCSV = useCallback(() => {
    if (!journal) return
    const csv = buildCSV(journal)
    downloadText(csv, `journal_${journal.session_id}_${journal.trading_date}.csv`)
  }, [journal])

  // ── Excel download (CSV with .xlsx extension — opens in Excel) ─────────────
  const onExcel = useCallback(() => {
    if (!journal) return
    const csv = buildCSV(journal)
    downloadText(csv, `journal_${journal.session_id}_${journal.trading_date}.xlsx`)
  }, [journal])

  // ── Empty state (no session prop) ───────────────────────────────────────────
  if (!sessionId) {
    return (
      <div className="flex flex-col items-center justify-center gap-3 py-16 text-center">
        <span style={{ color: C.faint }}>{ICON.book(28)}</span>
        <p className="text-[13px]" style={{ color: C.muted }}>
          Select a session to view its trade journal.
        </p>
      </div>
    )
  }

  const s = journal?.session_summary
  const pnlPos = s != null && s.total_pnl >= 0

  return (
    <div className="flex flex-col gap-4">

      {/* ── Top bar: session selector + export toolbar ─────────────────────── */}
      <div className="flex flex-wrap items-center gap-2">

        {/* Session selector */}
        <div className="flex items-center gap-1.5 flex-1 min-w-0">
          <span className="text-[10.5px] font-semibold uppercase tracking-[0.06em] shrink-0" style={{ color: C.muted }}>
            Session
          </span>
          <select
            value={selectedId}
            onChange={(e) => setSelectedId(e.target.value)}
            className="min-w-0 flex-1 max-w-[280px] rounded-lg px-2.5 py-1.5 text-[11.5px] outline-none"
            style={{
              background: 'rgba(255,255,255,0.03)',
              border: `1px solid ${C.line2}`,
              color: C.ink,
            }}
          >
            {/* Always include the current prop as an option even if sessions list hasn't loaded */}
            {!sessions.find((s) => s.session_id === selectedId) && (
              <option value={selectedId} style={{ background: '#0b1410', color: C.ink }}>
                {selectedId}
              </option>
            )}
            {sessions.map((s) => (
              <option key={s.session_id} value={s.session_id} style={{ background: '#0b1410', color: C.ink }}>
                {s.session_id}
                {s.created_at ? ` · ${s.created_at.slice(0, 10)}` : ''}
                {s.status ? ` · ${s.status}` : ''}
              </option>
            ))}
          </select>
        </div>

        {/* Export toolbar (right side) */}
        <div className="flex items-center gap-1.5 shrink-0">
          {/* Copied toast */}
          {copiedToast && (
            <span className="text-[10.5px] px-2 py-1 rounded-lg font-semibold"
              style={{ color: MINT, background: 'rgba(63,227,164,0.12)' }}>
              Copied!
            </span>
          )}

          <SmallBtn onClick={onCopy} disabled={!journal || loading}>
            {ICON.loop(11)} Copy
          </SmallBtn>
          <SmallBtn onClick={onCSV} disabled={!journal || loading}>
            {ICON.book(11)} CSV
          </SmallBtn>
          <SmallBtn onClick={onExcel} disabled={!journal || loading} title="Downloads as .xlsx — opens in Excel">
            {ICON.trend(11)} Excel
          </SmallBtn>

          <SmallBtn onClick={() => fetchJournal(selectedId)} disabled={loading}>
            {loading ? 'Loading...' : 'Refresh'}
          </SmallBtn>
        </div>
      </div>

      {/* ── Loading state ───────────────────────────────────────────────────── */}
      {loading && <Skeleton />}

      {/* ── Error state ─────────────────────────────────────────────────────── */}
      {!loading && error && (
        <div className="flex items-start gap-2.5 rounded-xl border px-4 py-3 text-[12px] leading-snug"
          style={{ borderColor: 'rgba(232,115,107,0.35)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
          <span className="shrink-0 mt-0.5" style={{ color: RED }}>{ICON.info(14)}</span>
          <div className="flex-1 min-w-0">
            {error}
            <button type="button" onClick={() => fetchJournal(selectedId)}
              className="ml-2 underline" style={{ color: MINT }}>
              Retry
            </button>
          </div>
        </div>
      )}

      {/* ── Journal body (only when data loaded) ────────────────────────────── */}
      {!loading && !error && journal && s && (
        <>
          {/* Session header pill */}
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-[10.5px] font-mono" style={{ color: C.faint }}>{journal.trading_date}</span>
            <span className="text-[10.5px] uppercase font-semibold tracking-[0.06em] rounded-full px-2 py-0.5"
              style={{
                color: journal.mode === 'live' ? RED : MINT,
                background: journal.mode === 'live' ? 'rgba(232,115,107,0.12)' : 'rgba(63,227,164,0.12)',
              }}>
              {journal.mode}
            </span>
            <span className="text-[10.5px]" style={{ color: C.faint }}>{journal.strategy}</span>
            <span className="ml-auto text-[10.5px]" style={{ color: C.faint }}>
              Status: <b style={{ color: C.ink2 }}>{s.session_status}</b>
            </span>
          </div>

          {/* A. Stat cards — horizontally scrollable on mobile */}
          <div className="flex gap-2 overflow-x-auto pb-1 -mx-1 px-1">
            <StatCard
              label="Total P&L"
              color={pnlPos ? MINT : RED}
              value={
                <span>
                  {signedINR(s.total_pnl)}{' '}
                  <span className="text-[11px] font-normal" style={{ color: pnlPos ? MINT : RED }}>
                    {s.total_pnl_pct_invested >= 0 ? '+' : ''}
                    {s.total_pnl_pct_invested.toFixed(2)}%
                  </span>
                </span>
              }
            />
            <StatCard
              label="Fund P&L %"
              color={s.total_pnl_pct_fund >= 0 ? MINT : RED}
              value={`${s.total_pnl_pct_fund >= 0 ? '+' : ''}${s.total_pnl_pct_fund.toFixed(2)}%`}
            />
            <StatCard
              label="Leverage"
              value={`${s.leverage.toFixed(2)}x`}
            />
            <StatCard
              label="Positions"
              value={<span><b>{s.n_open}</b> open / <b>{s.n_closed}</b> closed</span>}
            />
            <StatCard
              label="Win / Loss"
              value={
                <span>
                  <b style={{ color: MINT }}>{s.n_winners}W</b>
                  {' / '}
                  <b style={{ color: RED }}>{s.n_losers}L</b>
                </span>
              }
            />
            <StatCard label="Stops hit" value={s.n_stop_hits} />
            <StatCard label="Trail exits" value={s.n_trail_exits} />
            <StatCard
              label="Best trade"
              color={MINT}
              value={
                s.best_trade
                  ? <span title={s.best_trade.symbol}>{s.best_trade.symbol} {signedINR(s.best_trade.pnl_rs)}</span>
                  : '—'
              }
            />
            <StatCard
              label="Worst trade"
              color={RED}
              value={
                s.worst_trade
                  ? <span title={s.worst_trade.symbol}>{s.worst_trade.symbol} {signedINR(s.worst_trade.pnl_rs)}</span>
                  : '—'
              }
            />
            <StatCard label="Avg hold" value={fmtHold(s.avg_hold_minutes)} />
          </div>

          {/* B. Positions table */}
          {journal.positions.length > 0 ? (
            <div className="rounded-xl border overflow-x-auto" style={{ borderColor: C.line2 }}>
              <table className="w-full min-w-[700px] border-collapse text-left">
                <thead>
                  <tr style={{ background: C.card, borderBottom: `1px solid ${C.line2}` }}>
                    {['#', 'Symbol', 'Qty', 'Entry', 'Exit', 'P&L Rs', 'P&L %', 'Held', 'Reason', 'Review', ''].map((h, i) => (
                      <th key={i}
                        className={`py-2 ${i === 0 ? 'pl-3 pr-2' : 'pr-3'} text-[10px] font-semibold uppercase tracking-[0.06em] ${i >= 5 && i <= 6 ? 'text-right' : ''}`}
                        style={{ color: C.muted }}>
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {journal.positions.map((pos, i) => (
                    <PositionRow key={pos.symbol + i} pos={pos} idx={i + 1} />
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="rounded-xl border px-4 py-8 text-center text-[12px]"
              style={{ borderColor: C.line2, background: C.card, color: C.muted }}>
              No positions in this session.
            </div>
          )}

          {/* C. Review flags section */}
          {journal.review_items.length > 0 && (
            <div className="rounded-xl border overflow-hidden" style={{ borderColor: AMBER + '44', background: 'rgba(230,180,80,0.04)' }}>
              <div className="flex items-center gap-2 px-4 py-2.5 border-b" style={{ borderColor: AMBER + '30' }}>
                <span style={{ color: AMBER }}>{ICON.info(14)}</span>
                <span className="text-[12px] font-semibold" style={{ color: C.ink }}>
                  Needs Review ({journal.review_items.length})
                </span>
              </div>
              <div className="flex flex-col divide-y" style={{ '--tw-divide-opacity': 1 } as React.CSSProperties}>
                {journal.review_items.map((item, i) => {
                  const fs2 = flagStyle(item.flag)
                  return (
                    <div key={i} className="flex items-start gap-3 px-4 py-2.5 text-[11.5px]">
                      <span className="shrink-0 font-semibold text-[11px] rounded-full px-2 py-0.5 mt-0.5"
                        style={{ color: fs2.color, background: fs2.bg }}>
                        {item.flag.replace(/_/g, ' ')}
                      </span>
                      <span className="font-semibold" style={{ color: C.ink }}>{item.symbol}</span>
                      <span style={{ color: C.ink2 }}>{item.note}</span>
                    </div>
                  )
                })}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}

// ── Small toolbar button ───────────────────────────────────────────────────────
function SmallBtn({
  onClick, disabled = false, children, title,
}: {
  onClick: () => void
  disabled?: boolean
  children: React.ReactNode
  title?: string
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      title={title}
      className="inline-flex items-center gap-1 h-7 px-2.5 rounded-lg text-[11px] font-medium transition-colors disabled:opacity-40"
      style={{
        color: C.ink2,
        border: `1px solid ${C.line2}`,
        background: 'rgba(255,255,255,0.03)',
      }}
    >
      {children}
    </button>
  )
}
