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
    case 'STOP_STOCK':  return 'STOP-STOCK'
    case 'FLOOR_EXIT':  return 'FLOOR-EXIT'
    case 'KILL':
    case 'KILL_SWITCH': return 'KILL'
    default:            return r.toUpperCase().replace(/_/g, '-')
  }
}

// ── Exit-reason explainer ─────────────────────────────────────────────────────
// A short, plain-English note per close reason so the log is self-explanatory.
// Keyed on the UPPERCASED raw reason; helper below normalises common aliases.
const REASON_EXPLAIN: Record<string, string> = {
  FLOOR_EXIT:  'Basket gave back to its locked profit floor — the whole basket was flattened.',
  TRAIL_EXIT:  'Basket retraced past its trailing giveback from the peak — whole basket exited.',
  TRAIL:       'Trailing exit — price pulled back from its peak past the giveback and this leg was closed.',
  STOP:        'Basket hit its hard stop (max loss) — the whole basket was flattened.',
  STOP_STOCK:  'This stock fell past its own per-position stop and was exited individually.',
  GTT_STOP:    'Broker-side GTT stop-loss triggered on this stock and closed the position.',
  GTT_TARGET:  'Broker-side GTT target triggered on this stock and booked the profit.',
  SQUARE_OFF:  'Session square-off time reached — the position was flattened intraday.',
  KILL_SWITCH: 'Portfolio kill switch fired (± basket P&L threshold) — every leg was flattened.',
  KILL:        'Portfolio kill switch fired (± basket P&L threshold) — every leg was flattened.',
  OPEN:        'Still open — live LTP and unrealised P&L shown; not yet closed.',
}
function reasonExplain(r: string | null): string {
  const key = (r ?? 'OPEN').toUpperCase()
  return REASON_EXPLAIN[key] ?? `Position closed (${key.replace(/_/g, ' ').toLowerCase()}).`
}
// The distinct reasons present in a journal (plus OPEN when any leg is live) —
// drives the compact inline legend so the operator sees only what applies.
function reasonsInJournal(j: TradeJournal): string[] {
  const seen = new Set<string>()
  let hasOpen = false
  for (const p of j.positions) {
    if (p.status === 'OPEN' || !p.close_reason) hasOpen = true
    else seen.add(p.close_reason.toUpperCase())
  }
  const out = Array.from(seen).sort()
  if (hasOpen) out.push('OPEN')
  return out
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
// Compact HH:MM IST for the table's Entry/Exit-time columns (full seconds live
// in the expandable detail via fmtTimestamp).
function fmtClock(ts: string | null | undefined): string {
  if (!ts) return '—'
  try {
    const d = new Date(ts)
    if (isNaN(d.getTime())) return ts
    const ist = new Date(d.getTime() + 330 * 60_000)
    return ist.toISOString().slice(11, 16)
  } catch {
    return ts
  }
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
    `Stops hit:        ${s.n_stop_hits}  |  Target hits: ${s.n_target_hits}  |  Trail exits: ${s.n_trail_exits}  |  Square-offs: ${s.n_square_off}`,
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

// ── CSV builder (per-stock table — ALL columns) ──────────────────────────────
// Escape a single CSV cell: wrap in quotes when it contains a comma, quote,
// newline, or leading =/+/-/@ (formula-injection guard), doubling inner quotes.
function csvCell(v: string | number | null | undefined): string {
  if (v == null) return ''
  let s = String(v)
  const needsQuote = /[",\n\r]/.test(s) || /^[=+\-@\t]/.test(s)
  if (/^[=+\-@\t]/.test(s)) s = `'${s}` // neutralise spreadsheet formula injection
  if (needsQuote) return `"${s.replace(/"/g, '""')}"`
  return s
}
function num(v: number | null | undefined, dp = 2): string {
  if (v == null || !Number.isFinite(v)) return ''
  return v.toFixed(dp)
}
// Full per-stock export — one row per position, every operator-requested column.
function buildCSV(j: TradeJournal): string {
  const header = [
    'Symbol', 'Exchange', 'Status', 'Qty',
    'Entry_Price', 'Invested_Rs', 'Entry_Time_IST',
    'Exit_Price', 'Exit_Time_IST', 'Hold_Min',
    'Close_Reason', 'Realised_PnL_Rs', 'Unrealised_PnL_Rs',
    'PnL_Rs', 'PnL_Pct',
    'SL_Level', 'SL_Pct', 'Target_Price', 'Target_Pct',
    'GTT_Stop', 'GTT_Target', 'LTP', 'Review_Flag', 'Review_Note',
  ].join(',')

  const rows = j.positions.map((p) =>
    [
      p.symbol,
      p.exchange,
      p.status,
      p.qty,
      num(p.avg_price),
      num(p.invested_rs),
      fmtClock(p.opened_at),
      num(p.exit_price),
      fmtClock(p.closed_at),
      p.hold_minutes != null ? p.hold_minutes.toFixed(0) : '',
      p.close_reason ?? '',
      num(p.realised_pnl),
      num(p.unrealised_pnl),
      num(p.pnl_rs),
      num(p.pnl_pct),
      num(p.sl_level),
      num(p.sl_pct),
      num(p.target_price),
      num(p.target_pct),
      num(p.gtt_stop),
      num(p.gtt_target),
      num(p.ltp),
      p.review_flag ?? '',
      p.review_note ?? '',
    ].map(csvCell).join(','),
  )

  // A short session-summary preamble as CSV comment-ish lines, then a blank
  // line, then the table — Excel/Sheets ignore the leading text rows cleanly.
  const s = j.session_summary
  const preamble = [
    `AutoTrade Trade Journal,${csvCell(j.session_id)}`,
    `Trading Date,${csvCell(j.trading_date)}`,
    `Strategy,${csvCell(j.strategy)},Mode,${csvCell(j.mode.toUpperCase())}`,
    `Total P&L (Rs),${num(s.total_pnl)},Total P&L % (invested),${num(s.total_pnl_pct_invested)},Total P&L % (fund),${num(s.total_pnl_pct_fund)}`,
    `Invested Basis (Rs),${num(s.invested_basis)},Fund (Rs),${num(s.total_allocated_capital)},Leverage,${num(s.leverage)}`,
    `Winners,${s.n_winners},Losers,${s.n_losers},Stops,${s.n_stop_hits},Trail Exits,${s.n_trail_exits},Square-offs,${s.n_square_off}`,
    '',
  ]
  return [...preamble, header, ...rows].join('\n')
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

// Column count for the positions table (keep in sync with header + colSpan).
const N_COLS = 17

// ── Expandable position row ───────────────────────────────────────────────────
function PositionRow({ pos, idx }: { pos: JournalPosition; idx: number }) {
  const [open, setOpen] = useState(false)
  const isOpen = pos.status === 'OPEN'
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

        <td className="py-2 pr-3 text-[12px] font-semibold whitespace-nowrap" style={{ color: C.ink }}>
          {pos.symbol}
          {isOpen && (
            <span className="ml-1.5 text-[9px] font-mono uppercase tracking-[0.05em] rounded-full px-1.5 py-0.5"
              style={{ color: MINT, background: 'rgba(63,227,164,0.12)' }}>
              OPEN
            </span>
          )}
        </td>

        <td className="py-2 pr-3 text-[11px] tabular-nums text-right" style={{ color: C.ink2 }}>{pos.qty}</td>

        {/* Entry price */}
        <td className="py-2 pr-3 text-[11px] tabular-nums text-right font-mono" style={{ color: C.ink2 }}>
          {fmtPrice(pos.avg_price)}
        </td>

        {/* Invested ₹ */}
        <td className="py-2 pr-3 text-[11px] tabular-nums text-right font-mono" style={{ color: C.muted }}>
          {pos.invested_rs != null ? fmtINR(pos.invested_rs) : '—'}
        </td>

        {/* Entry time */}
        <td className="py-2 pr-3 text-[10.5px] tabular-nums text-right font-mono whitespace-nowrap" style={{ color: C.muted }}>
          {fmtClock(pos.opened_at)}
        </td>

        {/* Exit price — for open rows show live LTP tagged */}
        <td className="py-2 pr-3 text-[11px] tabular-nums text-right font-mono whitespace-nowrap" style={{ color: C.ink2 }}>
          {isOpen
            ? (pos.ltp != null
                ? <span title="Live last price (position still open)">{fmtPrice(pos.ltp)}<span className="ml-1 text-[8.5px]" style={{ color: MINT }}>LTP</span></span>
                : '—')
            : fmtPrice(pos.exit_price)}
        </td>

        {/* Exit time */}
        <td className="py-2 pr-3 text-[10.5px] tabular-nums text-right font-mono whitespace-nowrap" style={{ color: C.muted }}>
          {isOpen ? '—' : fmtClock(pos.closed_at)}
        </td>

        {/* Hold */}
        <td className="py-2 pr-3 text-[11px] tabular-nums text-right whitespace-nowrap" style={{ color: C.muted }}>
          {fmtHold(pos.hold_minutes)}
        </td>

        {/* Exit reason */}
        <td className="py-2 pr-3 text-[10.5px]">
          <span className="inline-flex items-center rounded-full px-2 py-0.5 font-mono font-semibold whitespace-nowrap"
            title={reasonExplain(pos.close_reason)}
            style={{ color: rc.color, background: rc.bg }}>
            {reasonLabel(pos.close_reason)}
          </span>
        </td>

        {/* P&L ₹ — for open rows this is unrealised */}
        <td className="py-2 pr-3 text-[11px] tabular-nums text-right font-semibold whitespace-nowrap" style={{ color: pnlColor }}>
          {signedINR(pos.pnl_rs)}
          {isOpen && <span className="ml-1 text-[8.5px] font-normal" style={{ color: C.faint }}>unrl</span>}
        </td>

        {/* P&L % */}
        <td className="py-2 pr-3 text-[11px] tabular-nums text-right font-semibold" style={{ color: pnlColor }}>
          {fmtPct(pos.pnl_pct)}
        </td>

        {/* SL level */}
        <td className="py-2 pr-3 text-[11px] tabular-nums text-right font-mono" style={{ color: C.muted }}>
          {fmtPrice(pos.sl_level)}
        </td>

        {/* Target */}
        <td className="py-2 pr-3 text-[11px] tabular-nums text-right font-mono" style={{ color: C.muted }}>
          {fmtPrice(pos.target_price)}
        </td>

        {/* GTT stop / target */}
        <td className="py-2 pr-3 text-[10.5px] tabular-nums text-right font-mono whitespace-nowrap" style={{ color: C.faint }}>
          {pos.gtt_stop != null || pos.gtt_target != null
            ? <span title="GTT stop / target">{fmtPrice(pos.gtt_stop)} / {fmtPrice(pos.gtt_target)}</span>
            : '—'}
        </td>

        {/* Review flag */}
        <td className="py-2 pr-3 text-[10.5px]">
          {pos.review_flag && (
            <span className="inline-flex items-center rounded-full px-2 py-0.5 font-semibold whitespace-nowrap"
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
          <td colSpan={N_COLS} className="px-4 py-3">
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
            {/* Exit-reason explainer — makes the row self-explanatory */}
            <div className="mt-2.5 flex items-start gap-2 rounded-lg border px-3 py-2 text-[11px] leading-snug"
              style={{ borderColor: rc.color + '33', background: rc.bg, color: C.ink2 }}>
              <span className="inline-flex items-center rounded-full px-2 py-0.5 font-mono font-semibold shrink-0 text-[10px]"
                style={{ color: rc.color, background: 'rgba(255,255,255,0.04)' }}>
                {reasonLabel(pos.close_reason)}
              </span>
              <span>{reasonExplain(pos.close_reason)}</span>
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

// ── Exit-reason legend ────────────────────────────────────────────────────────
// Renders a self-explanatory note for each distinct close reason present in the
// journal, so the operator can read the log without prior knowledge.
function ReasonLegend({ journal }: { journal: TradeJournal }) {
  const [open, setOpen] = useState(false)
  const reasons = reasonsInJournal(journal)
  if (reasons.length === 0) return null
  return (
    <div className="rounded-xl border overflow-hidden" style={{ borderColor: C.line2, background: C.card }}>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center gap-2 px-4 py-2.5 text-left"
      >
        <span style={{ color: C.muted }}>{ICON.info(13)}</span>
        <span className="text-[11.5px] font-semibold" style={{ color: C.ink2 }}>
          What the exit reasons mean
        </span>
        <span className="ml-auto" style={{ color: open ? MINT : C.faint }}>
          {ICON.chevron(13)}
        </span>
      </button>
      {open && (
        <div className="flex flex-col gap-1.5 px-4 pb-3 pt-0.5">
          {reasons.map((r) => {
            const rc = reasonColor(r === 'OPEN' ? null : r)
            return (
              <div key={r} className="flex items-start gap-2.5 text-[11px] leading-snug">
                <span className="inline-flex items-center rounded-full px-2 py-0.5 font-mono font-semibold shrink-0 text-[10px] mt-0.5"
                  style={{ color: rc.color, background: rc.bg }}>
                  {reasonLabel(r === 'OPEN' ? null : r)}
                </span>
                <span style={{ color: C.muted }}>{reasonExplain(r === 'OPEN' ? null : r)}</span>
              </div>
            )
          })}
        </div>
      )}
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

  // ── CSV download (per-stock table, all columns) ─────────────────────────────
  const onCSV = useCallback(() => {
    if (!journal) return
    const csv = buildCSV(journal)
    downloadText(csv, `autotrade_journal_${journal.session_id}_${journal.trading_date}.csv`)
  }, [journal])

  // ── Excel download (CSV with .xlsx extension — opens in Excel) ─────────────
  const onExcel = useCallback(() => {
    if (!journal) return
    const csv = buildCSV(journal)
    downloadText(csv, `autotrade_journal_${journal.session_id}_${journal.trading_date}.xlsx`)
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

          <SmallBtn onClick={onCopy} disabled={!journal || loading} title="Copy the journal as tab-separated text">
            {ICON.loop(11)} Copy
          </SmallBtn>
          <SmallBtn onClick={onCSV} disabled={!journal || loading} title="Download the full per-stock trade log as CSV">
            {ICON.book(11)} Download CSV
          </SmallBtn>
          <SmallBtn onClick={onExcel} disabled={!journal || loading} title="Download as .xlsx — opens in Excel">
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
            <StatCard label="Square-offs" value={s.n_square_off} />
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
              <table className="w-full min-w-[1180px] border-collapse text-left">
                <thead>
                  <tr style={{ background: C.card, borderBottom: `1px solid ${C.line2}` }}>
                    {(
                      [
                        { h: '#',        r: false },
                        { h: 'Symbol',   r: false },
                        { h: 'Qty',      r: true  },
                        { h: 'Entry',    r: true  },
                        { h: 'Invested', r: true  },
                        { h: 'In',       r: true  },
                        { h: 'Exit',     r: true  },
                        { h: 'Out',      r: true  },
                        { h: 'Held',     r: true  },
                        { h: 'Reason',   r: false },
                        { h: 'P&L Rs',   r: true  },
                        { h: 'P&L %',    r: true  },
                        { h: 'SL',       r: true  },
                        { h: 'Target',   r: true  },
                        { h: 'GTT S/T',  r: true  },
                        { h: 'Review',   r: false },
                        { h: '',         r: false },
                      ] as { h: string; r: boolean }[]
                    ).map(({ h, r }, i) => (
                      <th key={i}
                        title={h === 'In' ? 'Entry time (IST)' : h === 'Out' ? 'Exit time (IST)' : h === 'GTT S/T' ? 'GTT stop / target' : undefined}
                        className={`py-2 ${i === 0 ? 'pl-3 pr-2' : 'pr-3'} text-[10px] font-semibold uppercase tracking-[0.06em] whitespace-nowrap ${r ? 'text-right' : ''}`}
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

          {/* B2. Exit-reason legend — only the reasons that appear in this journal */}
          {journal.positions.length > 0 && <ReasonLegend journal={journal} />}

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
