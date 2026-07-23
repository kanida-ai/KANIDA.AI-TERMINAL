'use client'

/**
 * StrategyVisibilityPanel — ADMIN-only control for which AutoTrade strategies are
 * visible to power users. Lives inside the operator AutoTradePanel (the existing
 * admin/operator area) as its own sub-tab — NOT a new top-level portal mode.
 *
 * Contract (backend-provided):
 *   GET  /api/autotrade/admin/strategies          → { strategies: [{ strategy_id,
 *                                                      label, visible_to_power_users }] }
 *   POST /api/autotrade/admin/strategy-visibility  { strategy_id, visible }
 *
 * The main create-form strategy selector consumes the caller-appropriate list from
 * GET /api/autotrade/strategies (admin → all; power user → enabled only), so a
 * strategy toggled OFF here simply stops appearing for power users. New/experimental
 * strategies default OFF.
 *
 * SAFETY: UI only — toggles a visibility flag; touches no execution path. Mint/F2
 * theme; honest loading / empty / error states; nothing fabricated.
 */
import { useCallback, useEffect, useState } from 'react'
import { C, ICON } from '@/components/power/shared/cotrade-kit'
import { AutoTradeAPI, type StrategyInfo } from '@/lib/autotrade-api'

export function StrategyVisibilityPanel() {
  const [rows, setRows] = useState<StrategyInfo[] | null>(null)
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState<string | null>(null)
  // strategy_id currently being toggled (disables its switch + shows it's saving).
  const [savingId, setSavingId] = useState<string | null>(null)
  const [saveErr, setSaveErr] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true); setErr(null)
    try {
      const res = await AutoTradeAPI.adminStrategies()
      setRows(res.strategies ?? [])
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'Could not load strategies.')
      setRows(null)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  const toggle = useCallback(async (s: StrategyInfo) => {
    const next = !(s.visible_to_power_users ?? false)
    setSavingId(s.strategy_id); setSaveErr(null)
    // Optimistic: reflect the new state immediately, roll back on error.
    setRows((cur) => (cur ?? []).map((r) => r.strategy_id === s.strategy_id ? { ...r, visible_to_power_users: next } : r))
    try {
      await AutoTradeAPI.setStrategyVisibility(s.strategy_id, next)
    } catch (e) {
      setRows((cur) => (cur ?? []).map((r) => r.strategy_id === s.strategy_id ? { ...r, visible_to_power_users: !next } : r))
      setSaveErr(e instanceof Error ? e.message : 'Could not update visibility.')
    } finally {
      setSavingId(null)
    }
  }, [])

  return (
    <div className="flex flex-col gap-4">
      {/* Header + framing */}
      <div>
        <div className="flex items-center gap-2">
          <span style={{ color: C.mint }}>{ICON.bot(18)}</span>
          <h2 className="text-[16px] font-semibold" style={{ color: C.ink }}>Strategy visibility</h2>
        </div>
        <p className="text-[12px] leading-snug mt-1.5" style={{ color: C.muted }}>
          Choose which strategies power users can pick in the AutoTrade create form.
          You always see every strategy; power users see only the ones enabled here.{' '}
          <b style={{ color: C.ink2 }}>New / experimental strategies default OFF.</b>
        </p>
      </div>

      {/* Save error (non-fatal — the row was rolled back) */}
      {saveErr && (
        <div className="flex items-start gap-2 rounded-xl border px-3.5 py-2.5 text-[11.5px] leading-snug"
          style={{ borderColor: 'rgba(232,115,107,0.4)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
          <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(14)}</span>
          <span>{saveErr}</span>
        </div>
      )}

      {/* Loading */}
      {loading && (
        <div className="flex flex-col gap-2">
          {[0, 1, 2, 3].map((i) => (
            <div key={i} className="h-14 rounded-xl border animate-pulse"
              style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }} />
          ))}
        </div>
      )}

      {/* Error + retry */}
      {!loading && err && (
        <div className="rounded-xl border px-3.5 py-3" style={{ borderColor: 'rgba(232,115,107,0.4)', background: 'rgba(232,115,107,0.06)' }}>
          <div className="flex items-start gap-2 text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
            <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(14)}</span>
            <span>{err}</span>
          </div>
          <button type="button" onClick={load}
            className="mt-3 inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-[12px] font-semibold"
            style={{ color: '#06130c', background: C.mint }}>
            {ICON.loop(13)} Retry
          </button>
        </div>
      )}

      {/* Empty */}
      {!loading && !err && rows && rows.length === 0 && (
        <div className="flex flex-col items-center justify-center gap-2 py-16 text-center">
          <span style={{ color: C.faint }}>{ICON.bot(26)}</span>
          <p className="text-[13px]" style={{ color: C.muted }}>No strategies reported yet.</p>
        </div>
      )}

      {/* Per-strategy on/off list */}
      {!loading && !err && rows && rows.length > 0 && (
        <div className="flex flex-col gap-2">
          {rows.map((s) => {
            const on = s.visible_to_power_users ?? false
            const saving = savingId === s.strategy_id
            return (
              <div key={s.strategy_id}
                className="flex items-center justify-between gap-3 rounded-xl border px-3.5 py-3"
                style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.015)' }}>
                <div className="min-w-0">
                  <div className="text-[13px] font-semibold truncate" style={{ color: C.ink }}>
                    {s.label || s.strategy_id}
                  </div>
                  <div className="text-[10.5px] font-mono mt-0.5 truncate" style={{ color: C.faint }}>
                    {s.strategy_id}
                  </div>
                </div>
                <div className="flex items-center gap-2.5 shrink-0">
                  <span className="text-[10px] font-mono uppercase tracking-[0.06em] rounded-full px-2 py-0.5"
                    style={on
                      ? { color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }
                      : { color: C.faint, background: 'rgba(255,255,255,0.04)' }}>
                    {on ? 'Visible' : 'Hidden'}
                  </span>
                  <button type="button" onClick={() => toggle(s)} disabled={saving}
                    className="relative w-11 h-6 rounded-full transition-colors disabled:opacity-50 shrink-0"
                    style={{ background: on ? C.mint : 'rgba(255,255,255,0.12)' }}
                    aria-pressed={on} title="Visible to power users">
                    <span className="absolute top-0.5 w-5 h-5 rounded-full bg-white transition-all"
                      style={{ left: on ? '22px' : '2px' }} />
                  </button>
                </div>
              </div>
            )
          })}
          <p className="text-[10.5px] leading-snug mt-1" style={{ color: C.faint }}>
            &ldquo;Visible to Power Users&rdquo; — off means the strategy stays admin-only and never
            appears in a power user&apos;s strategy selector.
          </p>
        </div>
      )}
    </div>
  )
}
