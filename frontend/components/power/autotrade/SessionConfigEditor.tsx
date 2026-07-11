'use client'

/**
 * SessionConfigEditor — the LIVE "Edit config" modal for a RUNNING AutoTrade
 * session OR a running Monthly campaign. It lets the operator re-tune the
 * risk/exit knobs POST-launch and Apply, hot-reloading the live session without
 * a restart and WITHOUT disturbing open positions.
 *
 * FLOW (dry-run → confirm → apply):
 *   1. Apply → PATCH …/config?dry_run=true. If it returns `warnings`
 *      (e.g. "AEGISLOG at −4.2% would exit next tick under the new 4% stop"),
 *      show them in a confirm step ("Apply anyway?").
 *   2. Confirm (or no warnings) → PATCH …/config?dry_run=false → success toast +
 *      the parent refreshes the card.
 *
 * UNITS: percent inputs are captured/displayed as PERCENTS and sent as FRACTIONS
 * (÷100) — the SAME convention as session create/preview + the status trail
 * readout. Only CHANGED fields are sent (a precise subset of the whitelist).
 *
 * SAFETY / SCOPE: UI only. It PATCHes the operator-authored risk knobs through
 * PowerAPI (Bearer power_jwt) and nothing else — it places/exits no order and
 * touches no execution path. LOCKED knobs (capital / product / picks / entry
 * time) are shown greyed, never editable here. Mint/F2 theme via cotrade-kit `C`.
 */
import { useMemo, useState } from 'react'
import { C, ICON } from '@/components/power/shared/cotrade-kit'
import {
  PowerAPI, PowerAPIError, isConfigVersionConflict,
  type AutotradeConfigPatch, type AutotradeConfigResult,
} from '@/lib/power-api'

export type ConfigEditorKind = 'session' | 'campaign'

// The editable knob set, held in the natural units the operator reads:
// pct fields as PERCENTS (e.g. "2.5"), times as "HH:MM:SS", capital as ₹ int,
// max-hold as an int of trading days. '' means "left blank / unchanged".
export type EditableValues = {
  arm_pct?: number | ''
  floor_pct?: number | ''
  trail_giveback_pct?: number | ''
  stop_pct?: number | ''
  per_position_stop_pct?: number | ''
  per_position_target_pct?: number | ''
  square_off_time?: string
  mis_square_off_time?: string
  max_hold_sessions?: number | ''
  // campaign-only
  per_basket_capital?: number | ''
  total_capital?: number | ''
  end_date?: string
}

// The read-only context shown greyed ("locked after launch").
export type LockedContext = {
  allocatedCapital?: number   // ₹ — session allocated / campaign total
  product?: string            // CNC / MIS / MTF
  picks?: string              // e.g. "Falcon Top 5" / universe label
  entryTime?: string          // "HH:MM:SS"
}

// ── Small local field primitives (kept here so the editor is self-contained) ──
function pctFrac(v: number | '' | undefined): number | undefined {
  if (v === '' || v == null || !Number.isFinite(Number(v))) return undefined
  return Number(v) / 100
}
function intOrU(v: number | '' | undefined): number | undefined {
  if (v === '' || v == null || !Number.isFinite(Number(v))) return undefined
  return Math.max(0, Math.floor(Number(v)))
}
function strOrU(v: string | undefined): string | undefined {
  const s = (v ?? '').trim()
  return s ? s : undefined
}

const inputStyle: React.CSSProperties = {
  background: 'rgba(255,255,255,0.03)',
  border: `1px solid ${C.line2}`,
  color: C.ink,
}

function Knob({
  label, hint, suffix, children,
}: { label: string; hint?: string; suffix?: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <label className="text-[10.5px] font-semibold uppercase tracking-[0.04em]" style={{ color: C.muted }}>{label}</label>
      <div className="relative flex items-center">
        {children}
        {suffix && (
          <span className="absolute right-3 text-[11px] pointer-events-none" style={{ color: C.faint }}>{suffix}</span>
        )}
      </div>
      {hint && <span className="text-[10px] leading-snug" style={{ color: C.faint }}>{hint}</span>}
    </div>
  )
}

function NumInput({
  value, onChange, suffix, placeholder, step,
}: {
  value: number | '' | undefined
  onChange: (v: number | '') => void
  suffix?: string
  placeholder?: string
  step?: string
}) {
  return (
    <input
      type="number"
      inputMode="decimal"
      step={step ?? 'any'}
      value={value ?? ''}
      placeholder={placeholder}
      onChange={(e) => onChange(e.target.value === '' ? '' : Number(e.target.value))}
      className="w-full rounded-lg px-3 py-2 text-[13px] tabular-nums outline-none"
      style={{ ...inputStyle, paddingRight: suffix ? '2.1rem' : undefined }}
    />
  )
}

function TimeInput({ value, onChange }: { value?: string; onChange: (v: string) => void }) {
  return (
    <input
      type="text"
      value={value ?? ''}
      placeholder="HH:MM:SS"
      onChange={(e) => onChange(e.target.value)}
      className="w-full rounded-lg px-3 py-2 text-[13px] tabular-nums outline-none"
      style={inputStyle}
    />
  )
}

function LockedChip({ label, value }: { label: string; value?: string }) {
  return (
    <div className="rounded-lg border px-3 py-2 opacity-60"
      style={{ borderColor: C.line, background: 'rgba(255,255,255,0.012)' }}>
      <div className="text-[9.5px] uppercase tracking-[0.05em]" style={{ color: C.faint }}>{label}</div>
      <div className="text-[12.5px] font-medium mt-0.5" style={{ color: C.ink2 }}>{value ?? '—'}</div>
    </div>
  )
}

const rs = (v?: number) => (typeof v === 'number' && Number.isFinite(v)
  ? `₹${v.toLocaleString('en-IN')}` : '—')

// What a reload returns: the FRESH editable values + the server's CURRENT
// config_version, so the editor can re-base an operator's edit after a conflict.
export type ReloadedConfig = { values: EditableValues; configVersion?: number }

export function SessionConfigEditor({
  kind, id, jwt, initial, initialConfigVersion, reloadConfig,
  locked, isPositional = false, isMis = false, isTrailing = true, onClose, onApplied,
}: {
  kind: ConfigEditorKind
  id: string
  jwt: string
  initial: EditableValues
  // The config_version captured when the editor opened (from the session/ladder
  // status). Sent as `expected_config_version` on every PATCH for optimistic
  // concurrency; a mismatch → 409 CONFIG_VERSION_CONFLICT → calm reload.
  initialConfigVersion?: number
  // Re-fetch the CURRENT config + version (used after a 409 conflict so the
  // operator re-applies against fresh state). Optional — when absent the conflict
  // banner falls back to "Close & reopen".
  reloadConfig?: () => Promise<ReloadedConfig | null>
  locked: LockedContext
  // isPositional: carry-overnight (max-hold applies). isMis: show MIS square-off.
  // isTrailing: the session runs the trailing basket (show arm/floor/give-back).
  isPositional?: boolean
  isMis?: boolean
  isTrailing?: boolean
  onClose: () => void
  onApplied: (msg: string) => void
}) {
  const [vals, setVals] = useState<EditableValues>(initial)
  // `baseline` is the last-known SERVER truth the diff is computed against. It
  // starts at `initial` and is rebased whenever we (re)load the server config.
  const [baseline, setBaseline] = useState<EditableValues>(initial)
  // The config_version the next PATCH will claim as `expected_config_version`.
  // Updated from every successful PATCH response and on a conflict reload.
  const [configVersion, setConfigVersion] = useState<number | undefined>(initialConfigVersion)
  const [phase, setPhase] = useState<'edit' | 'confirm' | 'applying'>('edit')
  const [warnings, setWarnings] = useState<string[]>([])
  const [error, setError] = useState<string | null>(null)
  // Set when a PATCH is rejected as stale; drives the calm reload banner.
  const [conflict, setConflict] = useState<string | null>(null)
  const [reloading, setReloading] = useState(false)

  const set = <K extends keyof EditableValues>(k: K, v: EditableValues[K]) =>
    setVals((s) => ({ ...s, [k]: v }))

  // Build the wire patch as a SUBSET of only the fields the operator CHANGED,
  // converting pct→fraction and dropping blanks. Times/capital/max-hold pass as-is.
  const buildPatch = useMemo(() => (): AutotradeConfigPatch => {
    const patch: AutotradeConfigPatch = {}
    const pctKeys: (keyof AutotradeConfigPatch & keyof EditableValues)[] = [
      'arm_pct', 'floor_pct', 'trail_giveback_pct', 'stop_pct',
      'per_position_stop_pct', 'per_position_target_pct',
    ]
    for (const k of pctKeys) {
      if (vals[k] !== baseline[k]) {
        const f = pctFrac(vals[k] as number | '' | undefined)
        if (f !== undefined) (patch as Record<string, number>)[k] = f
      }
    }
    if (vals.max_hold_sessions !== baseline.max_hold_sessions) {
      const n = intOrU(vals.max_hold_sessions)
      if (n !== undefined) patch.max_hold_sessions = n
    }
    if ((vals.square_off_time ?? '') !== (baseline.square_off_time ?? '')) {
      const s = strOrU(vals.square_off_time); if (s) patch.square_off_time = s
    }
    if ((vals.mis_square_off_time ?? '') !== (baseline.mis_square_off_time ?? '')) {
      const s = strOrU(vals.mis_square_off_time); if (s) patch.mis_square_off_time = s
    }
    if (kind === 'campaign') {
      if (vals.per_basket_capital !== baseline.per_basket_capital) {
        const n = intOrU(vals.per_basket_capital); if (n !== undefined) patch.per_basket_capital = n
      }
      if (vals.total_capital !== baseline.total_capital) {
        const n = intOrU(vals.total_capital); if (n !== undefined) patch.total_capital = n
      }
      if ((vals.end_date ?? '') !== (baseline.end_date ?? '')) {
        const s = strOrU(vals.end_date); if (s) patch.end_date = s
      }
    }
    return patch
  }, [vals, baseline, kind])

  const nChanged = Object.keys(buildPatch()).length

  // Always send the captured config_version as `expected_config_version` so a
  // concurrent edit (another operator / a tick) is caught with a 409 instead of
  // silently clobbering the newer config.
  const callConfig = (patch: AutotradeConfigPatch, dryRun: boolean): Promise<AutotradeConfigResult> =>
    kind === 'session'
      ? PowerAPI.autotradeUpdateSessionConfig(id, patch, { dryRun, expectedConfigVersion: configVersion }, jwt)
      : PowerAPI.autotradeUpdateLadderConfig(id, patch, { dryRun, expectedConfigVersion: configVersion }, jwt)

  // Fold the server's post-apply / post-dry-run config_version back into state so
  // the NEXT PATCH claims the fresh version. Ignores non-numeric / absent values.
  const absorbVersion = (res: AutotradeConfigResult) => {
    const v = Number(res.config_version)
    if (Number.isFinite(v)) setConfigVersion(v)
  }

  const humanErr = (e: unknown): string => {
    if (e instanceof PowerAPIError) {
      // NOT_RUNNING is the OTHER 409 — keep its distinct copy. The version
      // conflict is handled separately (never lands here).
      if (e.status === 409) return 'This is no longer RUNNING — reopen it to edit its config.'
      if (e.status === 404) return 'This session is not available to you.'
      if (e.status === 400) return e.message || 'One of the values is out of range.'
      return e.message || 'Could not update the config.'
    }
    return e instanceof Error ? e.message : 'Could not update the config.'
  }

  // 409 CONFIG_VERSION_CONFLICT — the config changed under us. Do NOT overwrite:
  // surface a calm banner and (if the parent supplied a re-fetch) reload the
  // latest config + its new version so the operator can re-apply their change.
  const onConflict = async (e: PowerAPIError) => {
    setPhase('edit')
    setWarnings([])
    setError(null)
    const noun = kind === 'campaign' ? "campaign's" : "session's"
    setConflict(`This ${noun} config changed since you opened it — reloading the latest. Re-enter your change and Apply again.`)
    // Prefer the server's authoritative current version off the 409 detail.
    const cur = Number(e.detail?.current_config_version)
    if (Number.isFinite(cur)) setConfigVersion(cur)
    await doReload()
  }

  // Re-fetch the current config + version (the "Reload & review" affordance). On
  // success rebases the baseline AND the working values to the fresh server truth.
  const doReload = async () => {
    if (!reloadConfig) return
    setReloading(true)
    try {
      const fresh = await reloadConfig()
      if (fresh) {
        setBaseline(fresh.values)
        setVals(fresh.values)
        if (fresh.configVersion != null && Number.isFinite(Number(fresh.configVersion))) {
          setConfigVersion(Number(fresh.configVersion))
        }
      }
    } catch {
      /* leave the conflict banner up; the operator can retry or close */
    } finally {
      setReloading(false)
    }
  }

  // Step 1 — dry_run. Warnings → confirm step; clean → apply straight through.
  const onApply = async () => {
    setError(null)
    const patch = buildPatch()
    if (Object.keys(patch).length === 0) { setError('Nothing changed yet.'); return }
    setConflict(null)
    setPhase('applying')
    try {
      const dry = await callConfig(patch, true)
      absorbVersion(dry)
      const w = (dry.warnings ?? []).filter(Boolean)
      if (w.length > 0) { setWarnings(w); setPhase('confirm'); return }
      await commit(patch)
    } catch (e) {
      if (isConfigVersionConflict(e)) { await onConflict(e); return }
      setError(humanErr(e)); setPhase('edit')
    }
  }

  // Step 2 — real apply (dry_run=false). Success → toast + refresh.
  const commit = async (patch: AutotradeConfigPatch) => {
    setConflict(null)
    setPhase('applying')
    try {
      const res = await callConfig(patch, false)
      absorbVersion(res)
      const base = 'Config updated — live within ~5s'
      const msg = kind === 'campaign'
        ? `${base} · updated ${res.children_updated?.length ?? 0} running ${(res.children_updated?.length ?? 0) === 1 ? 'child' : 'children'} + future spawns`
        : base
      onApplied(msg)
      onClose()
    } catch (e) {
      if (isConfigVersionConflict(e)) { await onConflict(e); return }
      setError(humanErr(e)); setPhase('edit')
    }
  }

  const applying = phase === 'applying'
  const idShort = `#${String(id).slice(0, 6)}`

  return (
    <div className="fixed inset-0 z-50 flex items-end sm:items-center justify-center px-3 sm:px-4"
      style={{ background: 'rgba(0,0,0,0.6)' }}
      onClick={() => !applying && onClose()}>
      <div
        className="w-full max-w-lg rounded-2xl border max-h-[92vh] overflow-y-auto"
        style={{ borderColor: 'rgba(63,227,164,0.3)', background: C.panel }}
        onClick={(e) => e.stopPropagation()}>

        {/* Header */}
        <div className="sticky top-0 z-10 flex items-center gap-2 px-5 pt-4 pb-3"
          style={{ background: C.panel, borderBottom: `1px solid ${C.line}` }}>
          <span style={{ color: C.mint }}>{SLIDERS(17)}</span>
          <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Edit config</span>
          <span className="text-[9px] font-mono rounded-full px-1.5 py-0.5"
            style={{ color: C.muted, background: 'rgba(255,255,255,0.05)' }}>
            {kind === 'campaign' ? 'campaign' : 'session'} {idShort}
          </span>
          <button type="button" onClick={() => !applying && onClose()} className="ml-auto shrink-0" style={{ color: C.faint }}>
            {ICON.close(15)}
          </button>
        </div>

        <div className="px-5 py-4 flex flex-col gap-4">
          {/* Reassurance */}
          <div className="flex items-start gap-2 rounded-xl border px-3.5 py-2.5"
            style={{ borderColor: 'rgba(63,227,164,0.28)', background: 'rgba(63,227,164,0.05)' }}>
            <span className="shrink-0 mt-0.5" style={{ color: C.mint }}>{ICON.shield(14)}</span>
            <p className="text-[11px] leading-snug" style={{ color: C.ink2 }}>
              Re-tune the risk/exit knobs on the fly. Changes <b style={{ color: C.ink }}>hot-reload the
              live {kind === 'campaign' ? 'campaign' : 'session'}</b> — <b style={{ color: C.ink }}>open positions are untouched</b>.
              {kind === 'campaign' ? ' Capital & end-date apply to future spawns.' : ''}
            </p>
          </div>

          {phase === 'confirm' ? (
            /* ── CONFIRM STEP — dry-run warnings ─────────────────────────────── */
            <div className="flex flex-col gap-3">
              <div className="flex items-center gap-2">
                <span style={{ color: C.amber }}>{ICON.info(15)}</span>
                <span className="text-[13px] font-semibold" style={{ color: C.ink }}>Before you apply</span>
              </div>
              <div className="rounded-xl border px-3.5 py-3"
                style={{ borderColor: 'rgba(230,180,80,0.4)', background: 'rgba(230,180,80,0.06)' }}>
                <ul className="flex flex-col gap-1.5">
                  {warnings.map((w, i) => (
                    <li key={i} className="flex items-start gap-2 text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                      <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>•</span>
                      <span>{w}</span>
                    </li>
                  ))}
                </ul>
              </div>
              <p className="text-[11px] leading-snug" style={{ color: C.faint }}>
                These are the effects of your new settings on what&apos;s already open. Apply anyway?
              </p>
              {error && <ErrLine error={error} />}
              <div className="flex items-center gap-2 pt-1">
                <button type="button" disabled={applying} onClick={() => commit(buildPatch())}
                  className="inline-flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
                  style={{ color: '#06130c', background: C.mint }}>
                  {applying ? 'Applying…' : 'Apply anyway'}
                </button>
                <button type="button" disabled={applying} onClick={() => { setPhase('edit'); setWarnings([]) }}
                  className="px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors disabled:opacity-40"
                  style={{ color: C.muted, border: `1px solid ${C.line}` }}>
                  Back
                </button>
              </div>
            </div>
          ) : (
            /* ── EDIT STEP ───────────────────────────────────────────────────── */
            <>
              {/* Optimistic-concurrency conflict — calm, never a silent overwrite */}
              {conflict && (
                <div className="flex items-start gap-2 rounded-xl border px-3.5 py-2.5"
                  style={{ borderColor: 'rgba(230,180,80,0.4)', background: 'rgba(230,180,80,0.06)' }}>
                  <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
                  <div className="flex flex-col gap-2">
                    <p className="text-[11.5px] leading-snug" style={{ color: C.ink2 }}>{conflict}</p>
                    {reloadConfig && (
                      <button type="button" disabled={reloading || applying} onClick={doReload}
                        className="self-start inline-flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-[11.5px] font-semibold transition-opacity disabled:opacity-40"
                        style={{ color: C.mint, border: `1px solid rgba(63,227,164,0.4)`, background: 'rgba(63,227,164,0.06)' }}>
                        {reloading ? 'Reloading…' : 'Reload & review'}
                      </button>
                    )}
                  </div>
                </div>
              )}

              {/* Editable knobs */}
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
                {isTrailing && (
                  <>
                    <Knob label="Arm" suffix="%" hint="Start trailing at +X% of capital">
                      <NumInput value={vals.arm_pct} onChange={(v) => set('arm_pct', v)} suffix="%" />
                    </Knob>
                    <Knob label="Floor" suffix="%" hint="Lock in +X% once armed">
                      <NumInput value={vals.floor_pct} onChange={(v) => set('floor_pct', v)} suffix="%" />
                    </Knob>
                    <Knob label="Give-back" suffix="%" hint="Exit if it falls X% from the peak">
                      <NumInput value={vals.trail_giveback_pct} onChange={(v) => set('trail_giveback_pct', v)} suffix="%" />
                    </Knob>
                  </>
                )}
                <Knob label="Basket stop" suffix="%" hint="Hard exit at −X% of capital">
                  <NumInput value={vals.stop_pct} onChange={(v) => set('stop_pct', v)} suffix="%" />
                </Knob>
                <Knob label="Per-stock stop" suffix="%" hint="Exit a position at −X%">
                  <NumInput value={vals.per_position_stop_pct} onChange={(v) => set('per_position_stop_pct', v)} suffix="%" />
                </Knob>
                <Knob label="Per-stock target" suffix="%" hint="Take a position at +X%">
                  <NumInput value={vals.per_position_target_pct} onChange={(v) => set('per_position_target_pct', v)} suffix="%" />
                </Knob>
                <Knob label="Square-off time" hint="Force basket square-off (IST)">
                  <TimeInput value={vals.square_off_time} onChange={(v) => set('square_off_time', v)} />
                </Knob>
                {isMis && (
                  <Knob label="MIS square-off" hint="Defensive intraday square-off (IST)">
                    <TimeInput value={vals.mis_square_off_time} onChange={(v) => set('mis_square_off_time', v)} />
                  </Knob>
                )}
                {isPositional && (
                  <Knob label="Max-hold days" hint="Force-close on the Nth trading day (0 = no cap)">
                    <NumInput value={vals.max_hold_sessions} onChange={(v) => set('max_hold_sessions', v)} step="1" />
                  </Knob>
                )}
              </div>

              {/* Campaign-only — future spawns */}
              {kind === 'campaign' && (
                <div className="flex flex-col gap-2.5">
                  <div className="flex items-center gap-2 text-[10.5px] uppercase tracking-[0.05em]" style={{ color: C.muted }}>
                    <span className="h-px flex-1" style={{ background: C.line }} />
                    Applies to future spawns
                    <span className="h-px flex-1" style={{ background: C.line }} />
                  </div>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-3.5">
                    <Knob label="Per-basket capital" hint="₹ each new daily basket deploys">
                      <NumInput value={vals.per_basket_capital} onChange={(v) => set('per_basket_capital', v)} step="1000" />
                    </Knob>
                    <Knob label="Total capital" hint="₹ campaign ceiling">
                      <NumInput value={vals.total_capital} onChange={(v) => set('total_capital', v)} step="1000" />
                    </Knob>
                    <Knob label="End date" hint="Last day it opens new baskets (YYYY-MM-DD)">
                      <input type="date" value={vals.end_date ?? ''} onChange={(e) => set('end_date', e.target.value)}
                        className="w-full rounded-lg px-3 py-2 text-[13px] outline-none" style={inputStyle} />
                    </Knob>
                  </div>
                </div>
              )}

              {/* Locked context */}
              <div className="flex flex-col gap-2">
                <div className="flex items-center gap-1.5 text-[10.5px]" style={{ color: C.faint }}>
                  <span>{LOCK(12)}</span>
                  Locked after launch — start a new {kind === 'campaign' ? 'campaign' : 'session'} to change these
                </div>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
                  <LockedChip label={kind === 'campaign' ? 'Total capital' : 'Allocated'} value={rs(locked.allocatedCapital)} />
                  <LockedChip label="Product" value={locked.product} />
                  <LockedChip label="Picks" value={locked.picks} />
                  <LockedChip label="Entry time" value={locked.entryTime} />
                </div>
              </div>

              {error && <ErrLine error={error} />}

              {/* Footer */}
              <div className="flex items-center gap-2 pt-1">
                <button type="button" disabled={applying || reloading || nChanged === 0} onClick={onApply}
                  className="inline-flex items-center gap-2 px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-opacity disabled:opacity-40"
                  style={{ color: '#06130c', background: C.mint }}>
                  {applying
                    ? 'Applying…'
                    : nChanged === 0 ? 'Apply' : `Apply ${nChanged} change${nChanged === 1 ? '' : 's'}`}
                </button>
                <button type="button" disabled={applying} onClick={onClose}
                  className="px-4 py-2 rounded-xl text-[12.5px] font-semibold transition-colors disabled:opacity-40"
                  style={{ color: C.muted, border: `1px solid ${C.line}` }}>
                  Cancel
                </button>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}

function ErrLine({ error }: { error: string }) {
  return (
    <div className="flex items-start gap-2 rounded-xl border px-3.5 py-2.5 text-[11.5px] leading-snug"
      style={{ borderColor: 'rgba(232,115,107,0.35)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
      <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(14)}</span>
      <span>{error}</span>
    </div>
  )
}

// Local sliders/gear + lock glyphs (cotrade-kit has no gear/lock icon in ICON).
const SLIDERS = (n: number) => (
  <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7">
    <path d="M4 8h9M17 8h3M4 16h3M11 16h9" strokeLinecap="round" />
    <circle cx="15" cy="8" r="2.2" /><circle cx="9" cy="16" r="2.2" />
  </svg>
)
const LOCK = (n: number) => (
  <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7">
    <rect x="5" y="11" width="14" height="9" rx="2" /><path d="M8 11V8a4 4 0 0 1 8 0v3" strokeLinecap="round" />
  </svg>
)
