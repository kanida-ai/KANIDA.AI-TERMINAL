'use client'

/**
 * PlanSwitcher — a COMPACT, forward-looking multi-style switcher shared by both
 * Co-Trading and AutoTrade. When a user runs more than one trading style, each
 * style is a "plan" (style + capital + its own portfolio). This renders a slim
 * row of plan chips + "All plans" + "Add a style", designed to scale to N plans.
 *
 * HONESTY: only "Falcon Top 10 Swing" is LIVE. Adding any other style is
 * disabled with a "soon" tag — this switcher is mostly STRUCTURAL today (one
 * live plan), and says so. "All plans" shows a simple AGGREGATE (combined
 * capital + combined P&L + positions across styles); with one live plan it just
 * reflects that plan. No fabricated P&L — pnl is shown only when a plan supplies
 * a REAL value, else honest "—/pending".
 */
import { useState } from 'react'
import { C, ICON, fmtCapital, signedINR, pctTone } from './cotrade-kit'

/** A single plan a user is running. `pnl` is REAL or null (never fabricated). */
export type Plan = {
  id:        string
  styleId:   string
  styleName: string
  capital:   number
  /** REAL total P&L in ₹ if the tracking backend supplies it; else null. */
  pnl?:      number | null
  /** open positions count if known, else null. */
  open?:     number | null
  live:      boolean
}

/** The "all plans" aggregate sentinel id. */
export const ALL_PLANS = '__all__'

type AddableStyle = { id: string; name: string; live: boolean }

export function PlanSwitcher({
  plans, activeId, onSelect, addable, onAdd, accent = 'mint',
}: {
  plans:    Plan[]
  /** ALL_PLANS for the aggregate view, else a plan id. */
  activeId: string
  onSelect: (id: string) => void
  /** styles offered by "+ Add a style" (only live ones are addable). */
  addable:  AddableStyle[]
  onAdd:    (styleId: string) => void
  /** chip accent — mint for Co-Trading, amber for AutoTrade (launch-pending). */
  accent?:  'mint' | 'amber'
}) {
  const [pickerOpen, setPickerOpen] = useState(false)
  const accentColor = accent === 'amber' ? C.amber : C.mint
  const accentBg = accent === 'amber' ? 'rgba(230,180,80,0.10)' : 'rgba(63,227,164,0.09)'
  const accentRing = accent === 'amber' ? 'rgba(230,180,80,0.5)' : 'rgba(63,227,164,0.55)'

  const multi = plans.length > 1

  return (
    <div className="relative flex items-center gap-1.5 flex-wrap">
      {/* per-plan chips */}
      {plans.map(p => {
        const active = p.id === activeId
        return (
          <button key={p.id} type="button" onClick={() => onSelect(p.id)}
                  className="inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11.5px] transition-colors"
                  style={active
                    ? { borderColor: accentRing, color: accentColor, background: accentBg }
                    : { borderColor: C.line2, color: C.muted, background: 'rgba(255,255,255,0.02)' }}>
            <span className="font-semibold">{shortStyle(p.styleName)}</span>
            <span className="font-mono tabular-nums" style={{ color: C.faint }}>{fmtCapital(p.capital)}</span>
            {active && <span className="w-1.5 h-1.5 rounded-full shrink-0" style={{ background: accentColor, boxShadow: `0 0 7px ${accentColor}` }} />}
          </button>
        )
      })}

      {/* All plans (aggregate) — shown when more than one plan, OR always as the
          structural anchor so the design scales to N plans */}
      <button type="button" onClick={() => onSelect(ALL_PLANS)}
              className="inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11.5px] transition-colors"
              style={activeId === ALL_PLANS
                ? { borderColor: accentRing, color: accentColor, background: accentBg }
                : { borderColor: C.line2, color: C.muted, background: 'rgba(255,255,255,0.02)' }}>
        <span className="grid place-items-center" style={{ color: activeId === ALL_PLANS ? accentColor : C.faint }}>{ICON.trend(12)}</span>
        All plans
        {!multi && <span className="text-[8.5px] font-mono uppercase tracking-[0.06em]" style={{ color: C.faint }}>1</span>}
      </button>

      {/* + Add a style → opens the picker (only Swing live; others "soon") */}
      <div className="relative">
        <button type="button" onClick={() => setPickerOpen(o => !o)}
                className="inline-flex items-center gap-1 rounded-full border border-dashed px-2.5 py-1 text-[11.5px] transition-colors"
                style={{ borderColor: C.line2, color: C.muted }}>
          <span style={{ color: accentColor }}>+</span> Add a style
        </button>

        {pickerOpen && (
          <>
            <button type="button" aria-label="Close style picker" onClick={() => setPickerOpen(false)}
                    className="fixed inset-0 z-30" />
            <div className="absolute left-0 top-[calc(100%+6px)] z-40 w-[230px] rounded-xl border overflow-hidden shadow-[0_18px_50px_-20px_rgba(0,0,0,0.8)]"
                 style={{ borderColor: C.line2, background: C.panel }}>
              <div className="px-3 py-2 border-b text-[10px] uppercase tracking-[0.05em]" style={{ borderColor: C.line, color: C.faint }}>
                Run another style as its own plan
              </div>
              {addable.map(s => {
                const already = plans.some(p => p.styleId === s.id)
                const disabled = !s.live || already
                return (
                  <button key={s.id} type="button" disabled={disabled}
                          onClick={() => { if (!disabled) { onAdd(s.id); setPickerOpen(false) } }}
                          className="w-full flex items-center gap-2 px-3 py-2 text-left text-[12px] transition-colors disabled:cursor-not-allowed"
                          style={{ color: disabled ? C.faint : C.ink, opacity: disabled ? 0.6 : 1 }}>
                    <span className="font-medium">{s.name}</span>
                    {already
                      ? <span className="ml-auto text-[8.5px] font-mono uppercase tracking-[0.06em]" style={{ color: accentColor }}>added</span>
                      : !s.live && <span className="ml-auto text-[8.5px] font-mono uppercase tracking-[0.06em]" style={{ color: C.faint }}>soon</span>}
                  </button>
                )
              })}
              <div className="px-3 py-2 border-t text-[9.5px] leading-snug" style={{ borderColor: C.line, color: C.faint }}>
                Only Falcon Top 10 Swing is live today — the other engines are Launch-Pending and we won&apos;t fake their picks.
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  )
}

/**
 * AllPlansAggregate — a slim card shown when "All plans" is selected. Combines
 * capital across plans (REAL) and combined P&L / positions ONLY when each plan
 * supplies a real value; otherwise honest "pending". Scales to N plans.
 */
export function AllPlansAggregate({ plans, accent = 'mint' }: { plans: Plan[]; accent?: 'mint' | 'amber' }) {
  const totalCapital = plans.reduce((a, p) => a + (p.capital || 0), 0)
  const pnls = plans.map(p => p.pnl).filter((v): v is number => typeof v === 'number')
  const havePnl = pnls.length === plans.length && plans.length > 0
  const totalPnl = havePnl ? pnls.reduce((a, v) => a + v, 0) : null
  const opens = plans.map(p => p.open).filter((v): v is number => typeof v === 'number')
  const totalOpen = opens.length === plans.length && plans.length > 0 ? opens.reduce((a, v) => a + v, 0) : null
  const retPct = totalPnl != null && totalCapital > 0 ? (totalPnl / totalCapital) * 100 : null
  const accentColor = accent === 'amber' ? C.amber : C.mint

  return (
    <div className="rounded-2xl border overflow-hidden" style={{ borderColor: C.line2, background: C.panel }}>
      <div className="px-4 py-3 border-b flex items-center gap-2 flex-wrap" style={{ borderColor: C.line }}>
        <h3 className="text-[14px] font-semibold" style={{ color: C.ink }}>All plans · combined</h3>
        <span className="text-[10.5px]" style={{ color: C.faint }}>{plans.length} {plans.length === 1 ? 'plan' : 'plans'}</span>
        {plans.length === 1 && (
          <span className="ml-auto text-[10px] rounded-full px-2 py-0.5" style={{ color: accentColor, background: 'rgba(63,227,164,0.08)' }}>
            reflects your one live plan
          </span>
        )}
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-4">
        <Agg label="Combined capital" value={fmtCapital(totalCapital)} real />
        <Agg label="Combined P&L" value={totalPnl == null ? '—' : signedINR(totalPnl)} note={totalPnl == null ? 'pending' : undefined} real={totalPnl != null} tone={totalPnl == null ? undefined : pctTone(totalPnl)} />
        <Agg label="Return %" value={retPct == null ? '—' : `${retPct >= 0 ? '+' : ''}${retPct.toFixed(1)}%`} note={retPct == null ? 'pending' : undefined} real={retPct != null} tone={retPct == null ? undefined : pctTone(retPct)} />
        <Agg label="Open positions" value={totalOpen == null ? '—' : String(totalOpen)} note={totalOpen == null ? 'pending' : undefined} real={totalOpen != null} />
      </div>
      <div className="px-4 py-2 border-t text-[9.5px] leading-snug" style={{ borderColor: C.line, color: C.faint }}>
        Combined P&amp;L / positions aggregate across styles once each plan&apos;s live tracking is wired
        (a Backend need). With one live style today, this mirrors that plan.
      </div>
    </div>
  )
}

function Agg({ label, value, note, real, tone }: { label: string; value: string; note?: string; real?: boolean; tone?: string }) {
  return (
    <div className="px-3 py-2.5 border-l first:border-l-0 [&:nth-child(3)]:border-l-0 sm:[&:nth-child(3)]:border-l border-t sm:border-t-0 [&:nth-child(-n+2)]:border-t-0"
         style={{ borderColor: C.line, background: real ? 'rgba(63,227,164,0.04)' : undefined }}>
      <div className="text-[8.5px] uppercase tracking-[0.04em]" style={{ color: C.faint }}>{label}</div>
      <div className="text-[14px] font-mono font-semibold tabular-nums leading-tight mt-0.5" style={{ color: tone ?? (real ? C.ink : C.faint) }}>{value}</div>
      {note && <div className="text-[8px]" style={{ color: real ? C.mint : C.faint }}>{note}</div>}
    </div>
  )
}

/** "Swing · Falcon Top 10" → "Swing" for the compact chip. */
function shortStyle(name: string): string {
  if (/swing/i.test(name)) return 'Swing'
  return name.split(/\s+/).slice(0, 2).join(' ')
}
