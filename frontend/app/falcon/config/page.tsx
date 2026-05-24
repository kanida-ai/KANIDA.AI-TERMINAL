'use client'

// Falcon — Trail / Auto-Exit Config (Phase 2)
// Per-product (MTF, CNC) trail-stop parameters + auto-exit toggle.
// Values persist in falcon_trail_config table; consumed by the position monitor.

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { FalconAPI, type TrailConfig, type TrailConfigInput, type TrailMethod,
         type EngineConfig } from '../../../lib/falcon-api'

type DraftCfg = {
  activate_pct:      string
  lock_pct:          string
  trail_sl_pct:      string
  trail_profit_pct:  string
  initial_sl_pct:    string
  hold_days_max:     string
  auto_exit_enabled: boolean
  trail_method:      TrailMethod
  trail_lookback:    string
}

const cfgToDraft = (c: TrailConfig): DraftCfg => ({
  activate_pct:      String(c.activate_pct),
  lock_pct:          String(c.lock_pct),
  trail_sl_pct:      String(c.trail_sl_pct),
  trail_profit_pct:  String(c.trail_profit_pct),
  initial_sl_pct:    String(c.initial_sl_pct),
  hold_days_max:     String(c.hold_days_max),
  auto_exit_enabled: !!c.auto_exit_enabled,
  trail_method:      c.trail_method ?? 'percentage',
  trail_lookback:    String(c.trail_lookback ?? 10),
})

export default function TrailConfigPage() {
  const [drafts, setDrafts] = useState<Record<'MTF' | 'CNC', DraftCfg | null>>({ MTF: null, CNC: null })
  const [saving, setSaving] = useState<'MTF' | 'CNC' | null>(null)
  const [savedFlash, setSavedFlash] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    FalconAPI.tradeGetConfig()
      .then(r => {
        const next: Record<'MTF' | 'CNC', DraftCfg | null> = { MTF: null, CNC: null }
        for (const c of r.configs) next[c.product] = cfgToDraft(c)
        setDrafts(next)
      })
      .catch(e => setError(e instanceof Error ? e.message : String(e)))
  }, [])

  const update = (product: 'MTF' | 'CNC', field: keyof DraftCfg, value: string | boolean) => {
    setDrafts(prev => ({
      ...prev,
      [product]: { ...(prev[product] as DraftCfg), [field]: value },
    }))
  }

  const save = async (product: 'MTF' | 'CNC') => {
    const d = drafts[product]
    if (!d) return
    setSaving(product); setError(null); setSavedFlash(null)
    const payload: TrailConfigInput = {
      product,
      activate_pct:      Number(d.activate_pct),
      lock_pct:          Number(d.lock_pct),
      trail_sl_pct:      Number(d.trail_sl_pct),
      trail_profit_pct:  Number(d.trail_profit_pct),
      initial_sl_pct:    Number(d.initial_sl_pct),
      hold_days_max:     parseInt(d.hold_days_max, 10),
      auto_exit_enabled: d.auto_exit_enabled,
      trail_method:      d.trail_method,
      trail_lookback:    parseInt(d.trail_lookback, 10),
    }
    try {
      const r = await FalconAPI.tradeSaveConfig(payload)
      setDrafts(prev => ({ ...prev, [product]: cfgToDraft(r.config) }))
      setSavedFlash(`${product} config saved`)
      setTimeout(() => setSavedFlash(null), 3000)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(null)
    }
  }

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold">Trail / Auto-Exit Config</h1>
        <p className="text-sm text-neutral-400">
          Per-product (MTF + CNC) parameters for the position monitor's trail-stop
          and auto-exit logic. Saved values are picked up by the monitor on the next
          poll cycle (within {`<`} 60 seconds).
        </p>
      </header>

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded p-3 text-sm text-red-200">
          ✗ {error}
        </div>
      )}
      {savedFlash && (
        <div className="bg-green-500/10 border border-green-500/30 rounded p-3 text-sm text-green-200">
          ✓ {savedFlash}
        </div>
      )}

      {/* Engine Playbook — operator rules for staging NEW trades.
          Distinct from per-product trail config below (which manages
          already-placed positions). Editing this only affects future trades. */}
      <EnginePlaybookCard onError={setError} onSaved={msg => {
        setSavedFlash(msg)
        setTimeout(() => setSavedFlash(null), 3000)
      }} />

      <Explainer />

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {(['MTF', 'CNC'] as const).map(p => (
          <ProductCard
            key={p}
            product={p}
            draft={drafts[p]}
            saving={saving === p}
            onChange={(field, value) => update(p, field, value)}
            onSave={() => save(p)}
          />
        ))}
      </div>

      <div className="text-xs text-neutral-600 border-t border-neutral-900 pt-4">
        Monitor polls every 60s. Auto-exit toggle gates real-money actions —
        when OFF, monitor only logs events; when ON, it places trail SL and
        exits at trigger via the executor. External (non-Falcon) positions are
        never auto-exited regardless of toggle.
        See <Link href="/falcon/positions" className="text-amber-400 underline">/falcon/positions</Link> for live state.
      </div>
    </div>
  )
}

function Explainer() {
  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded p-4 text-sm">
      <h2 className="text-neutral-100 font-semibold mb-2">Trail method &amp; parameters</h2>
      <p className="text-xs text-neutral-400 mb-3">
        Choose <strong className="text-neutral-200">trail_method</strong> per product.
        Both methods share <code>activate_pct</code>, <code>lock_pct</code>,
        <code> initial_sl_pct</code>, and <code>hold_days_max</code> — they only
        differ in how the trailing SL value is computed once activated.
      </p>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-xs text-neutral-400">
        <div className="bg-neutral-950 border border-neutral-800 rounded p-3">
          <strong className="text-amber-300 block mb-1">percentage</strong>
          SL = current × (1 − <code>trail_sl_pct</code>/100). Fixed % distance below LTP.
          Simple, predictable. Tune <code>trail_sl_pct</code> per stock volatility.
        </div>
        <div className="bg-neutral-950 border border-neutral-800 rounded p-3">
          <strong className="text-amber-300 block mb-1">10d_low (Donchian)</strong>
          SL = max(<code>lock_floor</code>, lowest LOW of last <code>trail_lookback</code>{' '}
          completed trading sessions). Adapts to each stock's volatility automatically;
          today's intraday excluded. <code>trail_sl_pct</code> /
          <code> trail_profit_pct</code> ignored under this method.
        </div>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-xs text-neutral-400 mt-3">
        <div>
          <strong className="text-neutral-200">activate_pct</strong> — minimum profit %
          before trailing kicks in. Below this, SL stays at <code>initial_sl_pct</code>.
        </div>
        <div>
          <strong className="text-neutral-200">lock_pct</strong> — minimum profit floor
          once activated. SL never goes below entry × (1 + lock_pct/100).
        </div>
      </div>
      <div className="mt-3 text-xs text-amber-300/80">
        HFCL example (entry ₹140.96, qty 1200):
        activates at ₹157.88 (+12%). Under <strong>10d_low</strong> with 10d-low=₹148,
        SL parks at max(₹145.19 lock, ₹148) = ₹148. Climbs as 10d-low rolls forward;
        ratchets <em>up only</em>, never lowered.
      </div>
      <div className="mt-2 text-xs text-neutral-500">
        On Kite API failure during the 10d_low fetch, the existing Kite SL stays in place
        and the next successful poll re-evaluates. No percentage fallback.
      </div>
    </section>
  )
}

function ProductCard({ product, draft, saving, onChange, onSave }: {
  product: 'MTF' | 'CNC'
  draft: DraftCfg | null
  saving: boolean
  onChange: (field: keyof DraftCfg, value: string | boolean) => void
  onSave: () => void
}) {
  if (!draft) return <div className="bg-neutral-900 border border-neutral-800 rounded p-4 text-neutral-500">Loading {product}…</div>

  const inputCls = 'w-full bg-neutral-950 border border-neutral-800 rounded px-2 py-1.5 text-sm text-neutral-100'
  const inputClsDisabled = 'w-full bg-neutral-950/50 border border-neutral-900 rounded px-2 py-1.5 text-sm text-neutral-600 cursor-not-allowed'
  const isTenDayLow = draft.trail_method === '10d_low'

  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
      <div className="flex items-baseline justify-between mb-3">
        <h2 className="text-sm font-semibold text-amber-300">{product}</h2>
        <span className="text-xs text-neutral-500">
          {product === 'MTF' ? 'Margin Trading Facility (leveraged)' : 'Cash and Carry (delivery)'}
        </span>
      </div>

      {/* Method selector — drives which fields below are active */}
      <div className="grid grid-cols-2 gap-3 mb-3 pb-3 border-b border-neutral-800">
        <Field label="Trail method">
          <select
            value={draft.trail_method}
            onChange={e => onChange('trail_method', e.target.value as TrailMethod)}
            className={inputCls}>
            <option value="percentage">Percentage</option>
            <option value="10d_low">10-day Low (Donchian)</option>
          </select>
        </Field>
        <Field label={isTenDayLow ? 'Lookback (trading days)' : 'Lookback (n/a — percentage method)'}>
          <input type="number" min={1} max={100}
            value={draft.trail_lookback}
            disabled={!isTenDayLow}
            onChange={e => onChange('trail_lookback', e.target.value)}
            className={isTenDayLow ? inputCls : inputClsDisabled} />
        </Field>
      </div>

      <div className="grid grid-cols-2 gap-3 mb-3">
        <Field label="Initial SL (%)">
          <input type="number" step={0.5}
            value={draft.initial_sl_pct}
            onChange={e => onChange('initial_sl_pct', e.target.value)}
            className={inputCls} />
        </Field>
        <Field label="Hold days max">
          <input type="number" min={1} max={60}
            value={draft.hold_days_max}
            onChange={e => onChange('hold_days_max', e.target.value)}
            className={inputCls} />
        </Field>
        <Field label="Activate (%)">
          <input type="number" step={0.5} min={0}
            value={draft.activate_pct}
            onChange={e => onChange('activate_pct', e.target.value)}
            className={inputCls} />
        </Field>
        <Field label="Lock floor (%)">
          <input type="number" step={0.5} min={0}
            value={draft.lock_pct}
            onChange={e => onChange('lock_pct', e.target.value)}
            className={inputCls} />
        </Field>
        <Field label={isTenDayLow ? 'Trail SL (%) — ignored under 10d_low' : 'Trail SL (%)'}>
          <input type="number" step={0.5} min={0}
            value={draft.trail_sl_pct}
            disabled={isTenDayLow}
            onChange={e => onChange('trail_sl_pct', e.target.value)}
            className={isTenDayLow ? inputClsDisabled : inputCls} />
        </Field>
        <Field label={isTenDayLow ? 'Trail profit (%) — ignored under 10d_low' : 'Trail profit (%)'}>
          <input type="number" step={0.5} min={0}
            value={draft.trail_profit_pct}
            disabled={isTenDayLow}
            onChange={e => onChange('trail_profit_pct', e.target.value)}
            className={isTenDayLow ? inputClsDisabled : inputCls} />
        </Field>
      </div>

      <div className="flex items-center justify-between gap-3 border-t border-neutral-800 pt-3">
        <label className="flex items-center gap-2 text-sm cursor-pointer select-none">
          <input type="checkbox"
            checked={draft.auto_exit_enabled}
            onChange={e => onChange('auto_exit_enabled', e.target.checked)}
            className="accent-amber-500 w-4 h-4" />
          <span className={draft.auto_exit_enabled ? 'text-amber-300 font-medium' : 'text-neutral-400'}>
            Auto-exit ON for {product}
          </span>
        </label>
        <button onClick={onSave} disabled={saving}
          className={saving
            ? 'px-4 py-2 bg-neutral-800 text-neutral-500 rounded cursor-not-allowed text-sm'
            : 'px-4 py-2 bg-amber-500 text-neutral-950 rounded font-semibold text-sm'}>
          {saving ? 'Saving…' : 'Save ' + product}
        </button>
      </div>
    </section>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="block">
      <span className="block text-xs text-neutral-400 mb-1">{label}</span>
      {children}
    </label>
  )
}

// ─── Engine Playbook (singleton operator rules) ─────────────────────────────
// 4 knobs that drive NEW trade staging behavior in the /falcon/trade panel:
//   per_trade_pct        — per-trade cash = total capital × this % (default 6.0)
//   daily_picks_max      — hard cap on top-N picks per day (default 14)
//   skip_already_held    — if true, top-N filtered against kite.holdings()
//   mining_window_years  — B4 publish cutoff (default 4 — see tooltip on field)

function EnginePlaybookCard({ onSaved, onError }: {
  onSaved: (msg: string) => void
  onError: (err: string | null) => void
}) {
  const [draft, setDraft] = useState<{
    per_trade_pct:        string
    daily_picks_max:      string
    skip_already_held:    boolean
    mining_window_years:  string
    updated_at:           string
  } | null>(null)
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    FalconAPI.tradeGetEngineConfig()
      .then(r => setDraft({
        per_trade_pct:        String(r.config.per_trade_pct),
        daily_picks_max:      String(r.config.daily_picks_max),
        skip_already_held:    !!r.config.skip_already_held,
        mining_window_years:  String(r.config.mining_window_years ?? 4),
        updated_at:           r.config.updated_at,
      }))
      .catch(e => onError(e instanceof Error ? e.message : String(e)))
  }, [onError])

  if (!draft) return null

  const save = async () => {
    setSaving(true); onError(null)
    try {
      const r = await FalconAPI.tradeSaveEngineConfig({
        per_trade_pct:        Number(draft.per_trade_pct),
        daily_picks_max:      parseInt(draft.daily_picks_max, 10),
        skip_already_held:    draft.skip_already_held,
        mining_window_years:  parseInt(draft.mining_window_years, 10),
      })
      setDraft(d => d && ({ ...d, updated_at: r.config.updated_at }))
      onSaved('Engine Playbook saved — applies to next preview / next pattern publish')
    } catch (e: unknown) {
      onError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  const inputCls = 'w-full bg-neutral-950 border border-neutral-800 rounded px-2 py-1.5 text-sm text-neutral-100'

  return (
    <section className="bg-neutral-900 border border-amber-500/30 rounded p-4">
      <div className="flex items-baseline justify-between mb-1">
        <h2 className="text-base font-semibold text-amber-300">Engine Playbook</h2>
        <span className="text-[10px] text-neutral-500 font-mono uppercase">
          new-trade rules
        </span>
      </div>
      <p className="text-xs text-neutral-400 mb-4">
        Sizing, top-N cap, held-skip rule, and pattern-window cutoff for staging
        NEW trades. Existing positions are NOT affected — they keep running on
        whatever was set when they were adopted.
      </p>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3 mb-3">
        <Field label="Per-trade cash (% of total capital)">
          <input
            type="number" step={0.5} min={0.5} max={50}
            value={draft.per_trade_pct}
            onChange={e => setDraft(d => d && ({ ...d, per_trade_pct: e.target.value }))}
            className={inputCls} />
          <span className="block text-[10px] text-neutral-500 mt-1">
            e.g. 6.0 → ₹30L total × 6% = ₹1.8L per trade
          </span>
        </Field>
        <Field label="Daily picks (top N by score)">
          <input
            type="number" min={1} max={200}
            value={draft.daily_picks_max}
            onChange={e => setDraft(d => d && ({ ...d, daily_picks_max: e.target.value }))}
            className={inputCls} />
          <span className="block text-[10px] text-neutral-500 mt-1">
            Hard cap. Operator can de-select but not add beyond this.
          </span>
        </Field>
        <Field label="Skip already-held">
          <div className="flex items-center h-9 gap-2">
            <input
              type="checkbox"
              checked={draft.skip_already_held}
              onChange={e => setDraft(d => d && ({ ...d, skip_already_held: e.target.checked }))}
              className="accent-amber-500 w-4 h-4" />
            <span className="text-sm text-neutral-300">
              {draft.skip_already_held ? 'Skip if I own it' : 'Allow stacking'}
            </span>
          </div>
          <span className="block text-[10px] text-neutral-500 mt-1">
            Filters top-N picks against your Zerodha holdings.
          </span>
        </Field>
        <Field label="Mining-window cutoff (years)">
          <input
            type="number" min={1} max={20}
            value={draft.mining_window_years}
            onChange={e => setDraft(d => d && ({ ...d, mining_window_years: e.target.value }))}
            title={'Mining-window cutoff (years). '
                + 'Default 4 = optimal in calm trending regimes. '
                + '5 = trades ~3% return for ~7pp better drawdown in stress months. '
                + 'Flip to 5 after a losing month or when market volatility rises. '
                + 'Re-publish patterns after changing.'}
            className={inputCls} />
          <span className="block text-[10px] text-neutral-500 mt-1">
            B4 publish cutoff. <a href="/falcon/admin" className="underline">Re-publish</a> after changing.
          </span>
        </Field>
      </div>

      <div className="flex items-center justify-between border-t border-neutral-800 pt-3">
        <span className="text-[10px] text-neutral-500 font-mono">
          last saved: {draft.updated_at}
        </span>
        <button
          onClick={save}
          disabled={saving}
          className={saving
            ? 'px-4 py-2 bg-neutral-800 text-neutral-500 rounded cursor-not-allowed text-sm'
            : 'px-4 py-2 bg-amber-500 text-neutral-950 rounded font-semibold text-sm'}>
          {saving ? 'Saving…' : 'Save Playbook'}
        </button>
      </div>
    </section>
  )
}
