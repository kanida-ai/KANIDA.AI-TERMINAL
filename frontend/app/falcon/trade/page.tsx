'use client'

// Falcon Auto-Trade — wired to live backend
// Spec: backend/falcon/trade/SPEC/Requirements.md + Design.md
// All data comes from /api/falcon/trade/*

import { useEffect, useMemo, useRef, useState } from 'react'
import Link from 'next/link'
import {
  FalconAPI,
  type FalconLiveSignal,
  type FalconSignalsToday,
  type TradePreviewResponse,
  type TradePlaceResponse,
  type TradeSmokeResponse,
  type TradeOrderSpec,
  type TradeExistingPosition,
} from '../../../lib/falcon-api'
import { fetchKiteStatus, type KiteStatus } from '../../../lib/admin-api'
import { PreflightBanner } from '../../../components/PreflightBanner'

const COST_MODEL = { mtfRate: 15.0, brokerage_bps: 30, slippage_bps: 5 }

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

type Step =
  | 'idle'
  | 'previewing'
  | 'smoke_running'
  | 'smoke_ok'
  | 'smoke_failed'
  | 'confirming_place'  // ConfirmModal open before /place fires
  | 'placing'           // calling /falcon/trade/place (real money)
  | 'placed'            // /place returned (per-order statuses on the table)
  | 'place_failed'
  | 'confirming_stage'  // ConfirmModal open before /premarket/stage-entries fires
  | 'staging'           // calling /premarket/stage-entries
  | 'staged'            // success — items now in pre-market
  | 'stage_failed'

type HoldAction = 'skip' | 'average'

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

const inr = (n: number) => '₹' + Math.round(n).toLocaleString('en-IN')

const inrLakh = (n: number) => {
  if (n >= 1e7) return `₹${(n / 1e7).toFixed(2)} Cr`
  if (n >= 1e5) return `₹${(n / 1e5).toFixed(2)} L`
  return inr(n)
}

// Returns the current IST hour/minute/second/weekday using Intl (timezone-safe).
function istParts(now: Date) {
  const fmt = new Intl.DateTimeFormat('en-IN', {
    timeZone: 'Asia/Kolkata',
    weekday: 'short',
    hour:    '2-digit',
    minute:  '2-digit',
    second:  '2-digit',
    hour12:  false,
  })
  const parts = fmt.formatToParts(now)
  const num = (t: string) => Number(parts.find(p => p.type === t)?.value ?? 0)
  let hour = num('hour'); if (hour === 24) hour = 0
  const min = num('minute')
  const sec = num('second')
  const weekday = parts.find(p => p.type === 'weekday')?.value ?? ''
  const isWeekday = !['Sat', 'Sun'].includes(weekday)
  return { hour, min, sec, isWeekday }
}

// ─────────────────────────────────────────────────────────────────────────────
// Page
// ─────────────────────────────────────────────────────────────────────────────

export default function FalconTradePage() {
  // — Engine Playbook (singleton, edited at /falcon/config) —
  // Drives default sizing %, top-N cap, skip-held filter for new trades.
  const [playbook, setPlaybook] = useState<{ per_trade_pct: number; daily_picks_max: number; skip_already_held: boolean } | null>(null)

  // — Section 1: Strategy / Signal —
  const [topN, setTopN] = useState(14)
  const [selectedSet, setSelectedSet] = useState<Set<string>>(new Set())

  // — Section 2: Sizing / Capital —
  const [totalCapital, setTotalCapital] = useState(5_000_000)
  const [perTrade, setPerTrade] = useState(300_000)
  const [perTradeOverridden, setPerTradeOverridden] = useState(false)  // false = auto from playbook %
  const [useLeverage, setUseLeverage] = useState(true)

  // — Section 3: Exit / Risk —
  const [holdDays, setHoldDays] = useState(7)
  const [slPct, setSlPct] = useState(-7)
  const [trailTrigger, setTrailTrigger] = useState(10)
  const [trailLookback, setTrailLookback] = useState(10)

  // — Hold-action per overlapping symbol —
  const [holdActions, setHoldActions] = useState<Record<string, HoldAction>>({})

  // — Step machine —
  const [step, setStep] = useState<Step>('idle')
  const [confirmText, setConfirmText] = useState('')
  const [errorMsg, setErrorMsg] = useState<string | null>(null)

  // — Backend data —
  const [tokenStatus, setTokenStatus] = useState<KiteStatus | null>(null)
  const [signalsData, setSignalsData] = useState<FalconSignalsToday | null>(null)
  const [heldBySymbol, setHeldBySymbol] = useState<Record<string, TradeExistingPosition>>({})
  const [mtfFlags, setMtfFlags] = useState<Record<string, boolean>>({})
  const [previewData, setPreviewData] = useState<TradePreviewResponse | null>(null)
  const [smokeResult, setSmokeResult] = useState<TradeSmokeResponse | null>(null)
  const [placeResult, setPlaceResult] = useState<TradePlaceResponse | null>(null)

  // — Loading / clock —
  const [loading, setLoading] = useState(true)
  const [now, setNow] = useState(new Date())
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000)
    return () => clearInterval(id)
  }, [])

  // — Initial data fetch —
  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const [signals, kite, playbookResp] = await Promise.all([
          FalconAPI.signalsToday(100).catch(() => null),
          fetchKiteStatus().catch(() => null),
          FalconAPI.tradeGetEngineConfig().catch(() => null),
        ])
        if (cancelled) return
        setSignalsData(signals)
        setTokenStatus(kite)

        // Apply playbook defaults (per_trade_pct, daily_picks_max, skip_held)
        const pb = playbookResp?.config
          ? {
              per_trade_pct:     playbookResp.config.per_trade_pct,
              daily_picks_max:   playbookResp.config.daily_picks_max,
              skip_already_held: !!playbookResp.config.skip_already_held,
            }
          : { per_trade_pct: 6, daily_picks_max: 14, skip_already_held: true }
        setPlaybook(pb)
        setTopN(pb.daily_picks_max)
        // Auto-compute per-trade from playbook %, unless user has overridden it
        setPerTrade(prev => perTradeOverridden ? prev : Math.round(totalCapital * pb.per_trade_pct / 100))

        // Default selection = top N (where N = playbook.daily_picks_max)
        if (signals?.picks?.length) {
          const topPicks = signals.picks.slice(0, pb.daily_picks_max).map(s => s.symbol)
          setSelectedSet(new Set(topPicks))
        }

        // Holdings + MTF flags depend on a valid token
        if (kite?.valid) {
          FalconAPI.tradePositions()
            .then(r => {
              if (cancelled) return
              const map: Record<string, TradeExistingPosition> = {}
              for (const p of r.positions) {
                map[p.symbol] = {
                  symbol:        p.symbol,
                  qty:           p.qty,
                  avg_entry:     p.avg_entry,
                  current_price: p.current_price,
                  product:       p.product,
                  entry_date:    p.entry_date,
                  days_held:     p.days_held,
                }
              }
              setHeldBySymbol(map)
            })
            .catch(() => {})

          if (signals?.picks?.length) {
            FalconAPI.tradeMtfCheck(signals.picks.map(s => s.symbol))
              .then(r => { if (!cancelled) setMtfFlags(r) })
              .catch(() => {})
          }
        }
      } catch (e) {
        if (!cancelled) setErrorMsg(String(e))
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    load()
    return () => { cancelled = true }
  }, [])

  // — Auto-recompute per_trade when totalCapital changes (unless overridden)
  useEffect(() => {
    if (!playbook || perTradeOverridden) return
    setPerTrade(Math.round(totalCapital * playbook.per_trade_pct / 100))
  }, [totalCapital, playbook, perTradeOverridden])

  // — Auto-mark overlap symbols as 'skip' when playbook.skip_already_held is on
  useEffect(() => {
    if (!playbook?.skip_already_held) return
    if (Object.keys(heldBySymbol).length === 0) return
    setHoldActions(prev => {
      const next = { ...prev }
      for (const sym of Object.keys(heldBySymbol)) {
        if (selectedSet.has(sym)) next[sym] = 'skip'
      }
      return next
    })
  }, [playbook, heldBySymbol, selectedSet])

  // — Re-poll token status every 10s while invalid —
  useEffect(() => {
    if (tokenStatus?.valid) return
    const id = setInterval(() => {
      fetchKiteStatus().then(setTokenStatus).catch(() => {})
    }, 10_000)
    return () => clearInterval(id)
  }, [tokenStatus?.valid])

  // — Derived: leverage math (no leverage assumption) —
  const selectedCount   = selectedSet.size
  const totalDeployed   = selectedCount * perTrade
  const utilizationPct  = totalCapital > 0 ? (totalDeployed / totalCapital) * 100 : 0
  const headroom        = totalCapital - totalDeployed
  const insufficientCap = totalDeployed > totalCapital
  const utilizationWarn = utilizationPct > 90 && !insufficientCap

  // — Derived: signals + overlaps —
  const allSignals: FalconLiveSignal[] = signalsData?.picks ?? []
  const selectedSignals = allSignals.filter(s => selectedSet.has(s.symbol))
  const eligibleSignals = selectedSignals.filter(s => mtfFlags[s.symbol] !== false)
  const skippedNonMtf   = selectedSignals.filter(s => mtfFlags[s.symbol] === false)

  const overlaps = useMemo(
    () => eligibleSignals.filter(s => heldBySymbol[s.symbol]),
    [eligibleSignals, heldBySymbol]
  )

  useEffect(() => {
    setHoldActions(prev => {
      const next = { ...prev }
      let changed = false
      overlaps.forEach(s => {
        if (!(s.symbol in next)) { next[s.symbol] = 'skip'; changed = true }
      })
      return changed ? next : prev
    })
  }, [overlaps])

  // — Market countdown (IST, timezone-safe) —
  const ist = istParts(now)
  const nowSecOfDay   = ist.hour * 3600 + ist.min * 60 + ist.sec
  const openSecOfDay  = 9 * 3600 + 15 * 60   // 09:15 IST
  const closeSecOfDay = 15 * 3600 + 30 * 60  // 15:30 IST
  const secToOpen     = openSecOfDay - nowSecOfDay
  const minutesToOpen = Math.floor(secToOpen / 60)
  const secondsToOpen = secToOpen - minutesToOpen * 60
  const marketIsOpen  = ist.isWeekday
    && nowSecOfDay >= openSecOfDay
    && nowSecOfDay < closeSecOfDay

  // ───────────────────────────────────────────────────────────────────────────
  // Actions (live API)
  // ───────────────────────────────────────────────────────────────────────────

  const reset = () => {
    setStep('idle')
    setConfirmText('')
    setPreviewData(null)
    setSmokeResult(null)
    setPlaceResult(null)
    setErrorMsg(null)
  }

  const refreshTokenAndData = async () => {
    const k = await fetchKiteStatus().catch(() => null)
    setTokenStatus(k)
    if (k?.valid && signalsData?.picks?.length) {
      FalconAPI.tradeMtfCheck(signalsData.picks.map(s => s.symbol))
        .then(setMtfFlags).catch(() => {})
      FalconAPI.tradePositions()
        .then(r => {
          const map: Record<string, TradeExistingPosition> = {}
          for (const p of r.positions) {
            map[p.symbol] = {
              symbol: p.symbol, qty: p.qty, avg_entry: p.avg_entry,
              current_price: p.current_price, product: p.product,
              entry_date: p.entry_date, days_held: p.days_held,
            }
          }
          setHeldBySymbol(map)
        })
        .catch(() => {})
    }
  }

  const onGeneratePreview = async () => {
    if (insufficientCap || selectedCount === 0) return
    setErrorMsg(null)
    setStep('previewing')
    try {
      const resp = await FalconAPI.tradePreview({
        signal_date:         signalsData?.signal_date ?? null,
        selected_symbols:    Array.from(selectedSet),
        total_capital:       totalCapital,
        per_trade:           perTrade,
        use_leverage:        useLeverage,
        hold_days:           holdDays,
        sl_pct:              slPct,
        trail_trigger_pct:   trailTrigger,
        trail_lookback_days: trailLookback,
        hold_actions:        holdActions,
      })
      setPreviewData(resp)
      // Refresh held map from preview's existing_positions (latest)
      const map: Record<string, TradeExistingPosition> = {}
      for (const p of resp.existing_positions) map[p.symbol] = p
      setHeldBySymbol(map)
    } catch (e: unknown) {
      setStep('idle')
      setErrorMsg(e instanceof Error ? e.message : String(e))
    }
  }

  const onSmokeTest = async () => {
    if (!previewData) return
    setErrorMsg(null)
    setStep('smoke_running')
    try {
      const r = await FalconAPI.tradeSmokeTest(previewData.preview_id)
      setSmokeResult(r)
      setStep(r.status === 'PLACED' ? 'smoke_ok' : 'smoke_failed')
      if (r.status !== 'PLACED' && r.error) setErrorMsg(r.error)
    } catch (e: unknown) {
      setStep('smoke_failed')
      setErrorMsg(e instanceof Error ? e.message : String(e))
    }
  }

  // ── Place Now (real money, market hours only) ─────────────────────────────
  // Operator preference 2026-05-11: bring back immediate-fire path for market
  // hours. Pre-market staging is the right default before 9:15 IST, but during
  // market hours "Buy" must mean "Buy now on Kite", not "schedule for tomorrow."
  // Two-step confirm: Preview ready → click Place Now → type CONFIRM → fire.
  // Smoke test is OPTIONAL (operator preference 2026-05-11) — Place Now and
  // Stage are now available directly from `previewing`, not gated behind smoke.
  const onPlaceNowClick = () => {
    if (!previewData) return
    _stepBeforeConfirm.current = step
    setConfirmText('')
    setErrorMsg(null)
    setStep('confirming_place')
  }
  // Where to return on Cancel: back to the screen we came from, so the buttons
  // remain visible (previewing, smoke_ok, smoke_failed, stage_failed, place_failed).
  const _stepBeforeConfirm = useRef<Step>('previewing')
  const onPlaceNowCancel = () => {
    setConfirmText('')
    setStep(_stepBeforeConfirm.current)
  }
  const onPlaceNowConfirm = async () => {
    if (!previewData) return
    if (confirmText !== 'CONFIRM') return
    setErrorMsg(null)
    setStep('placing')
    try {
      const r = await FalconAPI.tradePlace(previewData.preview_id, 'CONFIRM')
      setPlaceResult(r)
      setStep('placed')
      if (r.n_failed > 0) {
        const failed = r.orders.filter(o => o.role === 'ENTRY' && o.status !== 'PLACED')
        if (failed.length > 0) {
          setErrorMsg(`${failed.length} of ${r.n_attempted} ENTRY orders failed: ` +
                      failed.map(o => `${o.symbol} (${o.error || o.status})`).join('; '))
        }
      }
    } catch (e: unknown) {
      setStep('place_failed')
      setErrorMsg(e instanceof Error ? e.message : String(e))
    }
  }

  const [stageResult, setStageResult] = useState<{ n_staged: number; n_skipped: number; target_date: string } | null>(null)

  // Stage → opens CONFIRM modal first (same gate as Place Now). Operator
  // preference 2026-05-11: staging is a commitment to broker (deployer fires
  // it at 9:15 IST), so the gate should match Place Now visually + textually.
  const onStageClick = () => {
    if (!previewData) return
    _stepBeforeConfirm.current = step
    setConfirmText('')
    setErrorMsg(null)
    setStep('confirming_stage')
  }
  const onStageConfirm = async () => {
    if (!previewData) return
    if (confirmText !== 'CONFIRM') return
    setErrorMsg(null)
    await onStageEntries()
  }

  const onStageEntries = async () => {
    if (!previewData) return
    setErrorMsg(null)
    setStep('staging')
    try {
      const r = await FalconAPI.premarketStageEntries(previewData.preview_id)
      setStageResult({ n_staged: r.n_staged, n_skipped: r.n_skipped, target_date: r.target_date })
      setStep('staged')
      if (r.n_skipped > 0 && r.skipped.length > 0) {
        setErrorMsg(`${r.n_skipped} skipped: ` + r.skipped.map(s => `${s.symbol} (${s.reason})`).join(', '))
      }
    } catch (e: unknown) {
      setStep('stage_failed')
      setErrorMsg(e instanceof Error ? e.message : String(e))
    }
  }

  const onApplyTopN = () => {
    const top = allSignals
      .filter(s => mtfFlags[s.symbol] !== false)
      .slice(0, topN)
      .map(s => s.symbol)
    setSelectedSet(new Set(top))
  }

  const onSelectAllMtf = () => {
    setSelectedSet(new Set(allSignals.filter(s => mtfFlags[s.symbol] !== false).map(s => s.symbol)))
  }

  const onClearAll = () => setSelectedSet(new Set())

  const toggleSelected = (sym: string) => {
    setSelectedSet(prev => {
      const next = new Set(prev)
      if (next.has(sym)) next.delete(sym); else next.add(sym)
      return next
    })
  }

  // Header-checkbox handler: select-all-visible (all rows in the signals table)
  // when none/some are picked; clear when all picked. MTF-ineligible rows are
  // included on select-all (the planner will skip them with a visible reason),
  // matching the operator's intent of "tick everything I see".
  const onSelectAllVisible = (nextChecked: boolean) => {
    if (!nextChecked) {
      setSelectedSet(new Set())
      return
    }
    setSelectedSet(new Set(allSignals.map(s => s.symbol)))
  }

  const setHoldAction = (sym: string, action: HoldAction) => {
    setHoldActions(prev => ({ ...prev, [sym]: action }))
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Render
  // ───────────────────────────────────────────────────────────────────────────

  if (loading) {
    return <div className="text-neutral-400 text-sm">Loading signals + holdings…</div>
  }

  return (
    <div className="space-y-6">

      <header className="flex items-baseline justify-between">
        <div>
          <h1 className="text-2xl font-bold">Trade — Pre-Market Entry</h1>
          <p className="text-sm text-neutral-400">
            Falcon V7.1 → Zerodha MTF batch · operator-supervised · Phase 1
          </p>
        </div>
        <div className="text-xs text-neutral-500">
          {now.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour12: false })} IST
        </div>
      </header>

      {/* Preflight banner: blocks visually when any RED check fires (the
          backend /place endpoint enforces the gate independently — UI block
          is a redundant safety layer, not the only one). */}
      <PreflightBanner />

      <WorkflowStepper step={step} tokenValid={!!tokenStatus?.valid} />

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <TokenStatus
          status={tokenStatus}
          onRefresh={refreshTokenAndData}
        />
        <MarketCountdown
          marketIsOpen={marketIsOpen}
          minutes={minutesToOpen}
          seconds={secondsToOpen}
        />
      </div>

      {!signalsData && (
        <div className="bg-red-500/5 border border-red-500/30 rounded p-4 text-sm text-red-300">
          No signals available for today. Run the daily signal job (16:35 IST) first.
        </div>
      )}

      {Object.keys(heldBySymbol).length > 0 && (
        <ExistingPositionsBanner
          held={Object.values(heldBySymbol)}
          overlaps={overlaps}
        />
      )}

      <Card title="1. Strategy / Signal" hint="Rarely changes">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 items-end">
          <Field label="Strategy">
            <select className={inputCls} disabled value="V7.1">
              <option value="V7.1">Falcon V7.1</option>
            </select>
          </Field>
          <Field label={`Quick top-N preset (max ${allSignals.length || 100})`}>
            <div className="flex gap-2">
              <input
                type="number" min={1} max={allSignals.length || 500}
                value={topN}
                onChange={e => {
                  const cap = allSignals.length || 500
                  setTopN(Math.max(1, Math.min(cap, +e.target.value || 1)))
                }}
                className={inputCls + ' flex-1'}
              />
              <button type="button" onClick={onApplyTopN}
                      className="px-3 py-1.5 bg-amber-500/20 text-amber-300 border border-amber-500/40 rounded text-sm font-medium whitespace-nowrap">
                Apply
              </button>
            </div>
          </Field>
          <Field label="Selection">
            <div className="flex items-center gap-2">
              <span className={'px-2 py-1.5 rounded text-sm font-medium flex-1 text-center ' + (selectedCount === 0 ? 'bg-red-500/10 text-red-300 border border-red-500/40' : 'bg-amber-500/10 text-amber-300 border border-amber-500/40')}>
                {selectedCount} selected
              </span>
              <button type="button" onClick={onSelectAllMtf} className="text-xs text-neutral-400 hover:text-neutral-100 px-1">all MTF</button>
              <button type="button" onClick={onClearAll}    className="text-xs text-neutral-400 hover:text-neutral-100 px-1">clear</button>
            </div>
          </Field>
        </div>
        <div className="mt-3 text-xs text-neutral-500">
          Score method <code className="text-neutral-300">sum_oos_lift</code> ·
          Walk-forward <code className="text-neutral-300">auto</code> · Engine-enforced.
          Apply preset to fill top-N MTF-eligible, then fine-tune via per-row checkboxes below.
        </div>
      </Card>

      <Card title="2. Sizing / Capital" hint="The main user knobs">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Field label="Total capital (₹)">
            <input type="number" min={0} step={100000}
              value={totalCapital}
              onChange={e => setTotalCapital(+e.target.value || 0)}
              className={inputCls}
            />
          </Field>
          <Field label={
            playbook && !perTradeOverridden
              ? `Per-trade (auto: ${playbook.per_trade_pct}% of total) ₹`
              : 'Per-trade (override) ₹'
          }>
            <div className="flex items-center gap-2">
              <input
                type="number" min={0} step={50000}
                value={perTrade}
                disabled={!!playbook && !perTradeOverridden}
                onChange={e => setPerTrade(+e.target.value || 0)}
                className={(!!playbook && !perTradeOverridden)
                  ? inputCls + ' opacity-70 cursor-not-allowed'
                  : inputCls}
              />
              {playbook && (
                <button
                  type="button"
                  onClick={() => {
                    if (perTradeOverridden) {
                      // Reset to auto
                      setPerTradeOverridden(false)
                      setPerTrade(Math.round(totalCapital * playbook.per_trade_pct / 100))
                    } else {
                      setPerTradeOverridden(true)
                    }
                  }}
                  className="text-xs text-amber-400 hover:text-amber-300 underline whitespace-nowrap">
                  {perTradeOverridden ? 'reset' : 'override'}
                </button>
              )}
            </div>
            {playbook && !perTradeOverridden && (
              <span className="block text-[10px] text-neutral-500 mt-1">
                From Playbook · <a href="/falcon/config" className="underline">edit %</a>
              </span>
            )}
          </Field>
          <Field label="Use leverage (MTF)">
            <button type="button" onClick={() => setUseLeverage(v => !v)}
                    className={useLeverage ? toggleOnCls : toggleOffCls}>
              {useLeverage ? 'ON (MTF)' : 'OFF (CNC)'}
            </button>
          </Field>
        </div>
        <div className="mt-3 text-xs text-neutral-500">
          MAX_OPEN <span className="text-neutral-300">cash-bounded</span> — implicit max ≈ {perTrade > 0 ? Math.floor(totalCapital / perTrade) : 0} concurrent at this PER_TRADE.
          {useLeverage && (
            <> · MTF ON: <span className="text-amber-300">Zerodha determines actual margin per stock at order time</span>;
            with leverage your real cash usage is below the notional shown.</>
          )}
        </div>
      </Card>

      <Card title="3. Exit / Risk">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Field label="Hold days (HOLD_DAYS)">
            <input type="number" min={1} max={60}
              value={holdDays} onChange={e => setHoldDays(+e.target.value || 1)}
              className={inputCls} />
          </Field>
          <Field label="Initial stop loss (%)">
            <input type="number" step={0.5}
              value={slPct} onChange={e => setSlPct(+e.target.value)}
              className={inputCls} />
          </Field>
          <Field label="Trail trigger (%)">
            <input type="number" step={0.5}
              value={trailTrigger} onChange={e => setTrailTrigger(+e.target.value)}
              className={inputCls} />
          </Field>
          <Field label="Trail lookback (days)">
            <input type="number" min={1} max={30}
              value={trailLookback} onChange={e => setTrailLookback(+e.target.value || 1)}
              className={inputCls} />
          </Field>
        </div>
        <div className="mt-3 text-xs text-neutral-500">
          Phase 2 monitor enforces trail and time-stop. Phase 1 places initial SL only.
          See <Link href="/falcon/positions" className="text-amber-400 underline">/falcon/positions</Link> for live SL/target/trail levels per held position.
        </div>
      </Card>

      <Card title="4. Cost model" hint="Read-only — accounting display, not a strategy decision">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
          <ReadOnlyRow label="MTF interest rate (MTF_RATE)" value={`${COST_MODEL.mtfRate.toFixed(1)} % p.a.`} />
          <ReadOnlyRow label="Brokerage round-trip"          value={`${COST_MODEL.brokerage_bps} bps`} />
          <ReadOnlyRow label="Slippage each side"            value={`${COST_MODEL.slippage_bps} bps`} />
        </div>
      </Card>

      <DerivedStrip
        perTrade={perTrade}
        totalDeployed={totalDeployed}
        utilizationPct={utilizationPct}
        headroom={headroom}
        insufficientCap={insufficientCap}
        utilizationWarn={utilizationWarn}
        selectedCount={selectedCount}
        skippedCount={skippedNonMtf.length}
        overlapCount={overlaps.length}
      />

      {signalsData && (
        <Card title={`Today's signals — ${signalsData.n_picks} picks (signal_date ${signalsData.signal_date})`}
              hint="Check rows to include in batch · row hue: blue=held, red=non-MTF">
          <SignalsTable
            signals={allSignals}
            selectedSet={selectedSet}
            onToggleSelected={toggleSelected}
            onSelectAllVisible={onSelectAllVisible}
            heldBySymbol={heldBySymbol}
            holdActions={holdActions}
            onSetHoldAction={setHoldAction}
            mtfFlags={mtfFlags}
          />
        </Card>
      )}

      <div className="flex items-center gap-3 flex-wrap">
        <button type="button"
          onClick={onGeneratePreview}
          disabled={
            insufficientCap || !tokenStatus?.valid || step !== 'idle'
            || selectedCount === 0 || !signalsData
          }
          className={primaryBtn}>
          Generate Preview →
        </button>
        {selectedCount === 0 && (
          <span className="text-red-400 text-sm">Select at least one stock</span>
        )}
        {!tokenStatus?.valid && (
          <span className="text-red-400 text-sm">Token invalid — refresh required</span>
        )}
        {insufficientCap && (
          <span className="text-red-400 text-sm">
            INSUFFICIENT_CAPITAL — selected × PER_TRADE ({inrLakh(totalDeployed)}) exceeds Total ({inrLakh(totalCapital)})
          </span>
        )}
        {utilizationWarn && (
          <span className="text-yellow-400 text-sm">
            ⚠ Utilization {utilizationPct.toFixed(1)}% — above 90% threshold
          </span>
        )}
        <button type="button" onClick={reset} className={ghostBtn}>Reset</button>
      </div>

      {errorMsg && (
        <div className="bg-red-500/10 border border-red-500/30 rounded p-3 text-sm text-red-200">
          ✗ {errorMsg}
        </div>
      )}

      {(step === 'previewing' || step === 'smoke_running' || step === 'smoke_ok' ||
        step === 'smoke_failed' || step === 'staging' || step === 'staged' ||
        step === 'stage_failed' || step === 'confirming_place' || step === 'confirming_stage' ||
        step === 'placing' || step === 'placed' || step === 'place_failed') && previewData && (
        <PreviewBlock
          preview={previewData}
          smoke={smokeResult}
          place={placeResult}
          step={step}
          marketIsOpen={marketIsOpen}
          minutesToOpen={minutesToOpen}
          secondsToOpen={secondsToOpen}
          onSmokeTest={onSmokeTest}
          onStageClick={onStageClick}
          onPlaceNow={onPlaceNowClick}
          stageResult={stageResult}
        />
      )}

      {(step === 'confirming_place' || step === 'confirming_stage') && previewData && (
        <ConfirmModal
          preview={previewData}
          confirmText={confirmText}
          onConfirmTextChange={setConfirmText}
          onConfirm={step === 'confirming_place' ? onPlaceNowConfirm : onStageConfirm}
          onCancel={onPlaceNowCancel}
          kind={step === 'confirming_place' ? 'place' : 'stage'}
        />
      )}

      <div className="text-xs text-neutral-600 border-t border-neutral-900 pt-4">
        Backend: <code>{process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001'}</code> ·
        Token: <code>{tokenStatus?.valid ? '✓ valid' : '✗ invalid'}</code> ·
        Signals: <code>{signalsData?.signal_date ?? 'none'}</code> ·
        Held: <code>{Object.keys(heldBySymbol).length}</code>
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// Sub-components
// ─────────────────────────────────────────────────────────────────────────────

const inputCls   = 'w-full bg-neutral-950 border border-neutral-800 rounded px-2 py-1.5 text-sm text-neutral-100 disabled:opacity-50 disabled:cursor-not-allowed'
const toggleOnCls  = 'w-full bg-amber-500/20 text-amber-300 border border-amber-500/40 rounded px-3 py-1.5 text-sm font-medium'
const toggleOffCls = 'w-full bg-neutral-900 text-neutral-400 border border-neutral-800 rounded px-3 py-1.5 text-sm'
const primaryBtn = 'px-4 py-2 bg-amber-500 text-neutral-950 rounded font-semibold disabled:bg-neutral-800 disabled:text-neutral-600'
const ghostBtn   = 'px-3 py-2 text-sm text-neutral-400 hover:text-neutral-100'

function Card({ title, hint, children }: { title: string; hint?: string; children: React.ReactNode }) {
  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
      <div className="flex items-baseline justify-between mb-3">
        <h2 className="text-sm font-semibold text-neutral-100">{title}</h2>
        {hint && <span className="text-xs text-neutral-500">{hint}</span>}
      </div>
      {children}
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

function ReadOnlyRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between border-b border-neutral-800 pb-2 last:border-b-0">
      <span className="text-neutral-400">{label}</span>
      <span className="text-neutral-200 font-mono">{value}</span>
    </div>
  )
}

function ExistingPositionsBanner({ held, overlaps }: { held: TradeExistingPosition[]; overlaps: FalconLiveSignal[] }) {
  return (
    <div className="bg-blue-500/5 border border-blue-500/30 rounded p-4">
      <div className="flex items-center justify-between mb-2">
        <h3 className="text-sm font-semibold text-blue-300">
          📊 Existing positions: {held.length} held
        </h3>
        <Link href="/falcon/positions" className="text-blue-300 text-xs hover:text-blue-100 underline">
          See all positions →
        </Link>
      </div>
      {overlaps.length > 0 ? (
        <div className="text-sm text-blue-200">
          ⚠ <strong>{overlaps.length} of your selected picks overlap with held positions:</strong>{' '}
          {overlaps.map(o => (
            <span key={o.symbol} className="inline-block bg-blue-500/10 text-blue-200 px-2 py-0.5 rounded mr-1 font-mono">
              {o.symbol}
            </span>
          ))}
          <div className="text-xs text-blue-300/80 mt-2">
            Default action: <strong>Skip</strong> (hold existing position alone).
            Choose <strong>Average</strong> per pick in the signals table to add to the existing position,
            or uncheck the row to drop it from the batch entirely.
          </div>
        </div>
      ) : (
        <div className="text-sm text-neutral-400">No overlap with selected picks — held positions ride independently.</div>
      )}
    </div>
  )
}

function WorkflowStepper({ step, tokenValid }: { step: Step; tokenValid: boolean }) {
  const afterPreview = ['smoke_running','smoke_ok','smoke_failed','staging','staged','stage_failed',
                        'confirming_place','confirming_stage','placing','placed','place_failed']
  const done = step === 'staged' || step === 'placed'
  const steps = [
    { key: 'token',     label: 'Token',     done: tokenValid,                                                                  active: false },
    { key: 'configure', label: 'Configure', done: ['previewing', ...afterPreview].includes(step),                              active: step === 'idle' && tokenValid },
    { key: 'preview',   label: 'Preview',   done: afterPreview.includes(step),                                                  active: step === 'previewing' },
    { key: 'fire',      label: 'Place / Stage', done,                                                                            active: ['staging','confirming_place','confirming_stage','placing'].includes(step) },
    { key: 'done',      label: step === 'placed' ? 'Live on Kite' : 'In Pre-Market', done,                                       active: done },
  ]
  return (
    <div className="flex items-center gap-1 text-xs">
      {steps.map((s, i) => (
        <div key={s.key} className="flex items-center gap-1 flex-1">
          <div className={[
            'flex-1 px-3 py-2 rounded border text-center font-medium',
            s.done   ? 'bg-amber-500/10 border-amber-500/40 text-amber-300' :
            s.active ? 'bg-amber-500/20 border-amber-500   text-amber-100' :
                       'bg-neutral-900 border-neutral-800 text-neutral-500',
          ].join(' ')}>
            {i + 1}. {s.label}
          </div>
          {i < steps.length - 1 && (
            <span className={s.done ? 'text-amber-500' : 'text-neutral-700'}>→</span>
          )}
        </div>
      ))}
    </div>
  )
}

function TokenStatus({ status, onRefresh }: { status: KiteStatus | null; onRefresh: () => void }) {
  const valid = !!status?.valid
  return (
    <div className={[
      'rounded border p-3 flex items-center justify-between',
      valid ? 'bg-green-500/5 border-green-500/30' : 'bg-red-500/5 border-red-500/30',
    ].join(' ')}>
      <div>
        <div className="text-xs text-neutral-400">Kite token</div>
        <div className={'text-lg font-semibold ' + (valid ? 'text-green-300' : 'text-red-300')}>
          {valid ? '✓ Valid' : '✗ ' + (status?.reason ?? 'Expired or missing')}
        </div>
        <div className="text-xs text-neutral-500">
          {valid && status?.user
            ? <>{status.user}</>
            : <>Refresh at <Link href="/admin" className="text-amber-400 underline">/admin</Link>, then click Re-check below</>}
        </div>
      </div>
      <div className="flex gap-2">
        <Link href="/admin" target="_blank" className="px-3 py-1.5 bg-amber-500 text-neutral-950 rounded text-sm font-medium">
          Open /admin
        </Link>
        <button onClick={onRefresh} className="px-3 py-1.5 bg-neutral-800 text-neutral-300 rounded text-sm">
          Re-check
        </button>
      </div>
    </div>
  )
}

function MarketCountdown({ marketIsOpen, minutes, seconds }: { marketIsOpen: boolean; minutes: number; seconds: number }) {
  if (marketIsOpen) {
    return (
      <div className="rounded border bg-green-500/5 border-green-500/30 p-3">
        <div className="text-xs text-neutral-400">Market</div>
        <div className="text-lg font-semibold text-green-300">● OPEN</div>
        <div className="text-xs text-neutral-500">Place Orders enabled. Closes 15:30 IST.</div>
      </div>
    )
  }
  if (minutes < 0) {
    return (
      <div className="rounded border bg-neutral-900 border-neutral-800 p-3">
        <div className="text-xs text-neutral-400">Market</div>
        <div className="text-lg font-semibold text-neutral-500">CLOSED</div>
        <div className="text-xs text-neutral-500">Next open: 09:15 IST next weekday</div>
      </div>
    )
  }
  const hh = Math.floor(minutes / 60)
  const mm = minutes % 60
  return (
    <div className="rounded border bg-neutral-900 border-amber-500/30 p-3">
      <div className="text-xs text-neutral-400">Pre-market — opens at 09:15 IST</div>
      <div className="text-lg font-semibold text-amber-300 font-mono">
        {String(hh).padStart(2, '0')}:{String(mm).padStart(2, '0')}:{String(seconds).padStart(2, '0')}
      </div>
      <div className="text-xs text-neutral-500">Place Orders disabled until market open</div>
    </div>
  )
}

function DerivedStrip(props: {
  perTrade: number; totalDeployed: number;
  utilizationPct: number; headroom: number; insufficientCap: boolean; utilizationWarn: boolean;
  selectedCount: number; skippedCount: number; overlapCount: number;
}) {
  const { perTrade, totalDeployed, utilizationPct, headroom, insufficientCap, utilizationWarn, selectedCount, skippedCount, overlapCount } = props
  return (
    <div className="grid grid-cols-2 md:grid-cols-6 gap-3 text-sm bg-neutral-900 border border-neutral-800 rounded p-4">
      <Stat label="Selected (count)"  value={String(selectedCount)} accent={selectedCount > 0} err={selectedCount === 0} />
      <Stat label="PER_TRADE"         value={inrLakh(perTrade)} />
      <Stat label="Total deployed"    value={inrLakh(totalDeployed)} accent={!insufficientCap} warn={utilizationWarn} err={insufficientCap} />
      <Stat label="Utilization"       value={`${utilizationPct.toFixed(1)} %`} warn={utilizationWarn} err={insufficientCap} />
      <Stat label="Headroom"          value={inrLakh(Math.max(0, headroom))} err={insufficientCap} />
      <Stat label="Overlap with held" value={String(overlapCount)} warn={overlapCount > 0} />
    </div>
  )
}

function Stat({ label, value, accent, warn, err }: { label: string; value: string; accent?: boolean; warn?: boolean; err?: boolean }) {
  const colorCls = err ? 'text-red-400' : warn ? 'text-yellow-400' : accent ? 'text-amber-300' : 'text-neutral-100'
  return (
    <div>
      <div className="text-xs text-neutral-500">{label}</div>
      <div className={'text-base font-semibold ' + colorCls}>{value}</div>
    </div>
  )
}

function SignalsTable({ signals, selectedSet, onToggleSelected, onSelectAllVisible, heldBySymbol, holdActions, onSetHoldAction, mtfFlags }: {
  signals: FalconLiveSignal[];
  selectedSet: Set<string>;
  onToggleSelected: (s: string) => void;
  onSelectAllVisible: (nextChecked: boolean) => void;
  heldBySymbol: Record<string, TradeExistingPosition>;
  holdActions: Record<string, HoldAction>;
  onSetHoldAction: (sym: string, action: HoldAction) => void;
  mtfFlags: Record<string, boolean>;
}) {
  // Header checkbox: tristate over the currently-rendered signals.
  // - all selected → checked; click → clear all
  // - some selected → indeterminate; click → select all visible
  // - none selected → unchecked; click → select all visible
  const visibleSyms = signals.map(s => s.symbol)
  const visibleSelected = visibleSyms.filter(s => selectedSet.has(s)).length
  const allChecked  = visibleSyms.length > 0 && visibleSelected === visibleSyms.length
  const someChecked = visibleSelected > 0 && visibleSelected < visibleSyms.length
  const headerRef = useRef<HTMLInputElement | null>(null)
  useEffect(() => {
    if (headerRef.current) headerRef.current.indeterminate = someChecked
  }, [someChecked])

  return (
    <div className="overflow-x-auto -mx-4 px-4">
      <table className="w-full text-sm">
        <thead className="text-neutral-400 text-xs">
          <tr>
            <th className="text-center w-10">
              <input
                ref={headerRef}
                type="checkbox"
                checked={allChecked}
                onChange={() => onSelectAllVisible(!allChecked)}
                title={allChecked ? 'Deselect all' : someChecked ? `${visibleSelected}/${visibleSyms.length} selected — click to select all` : 'Select all'}
                className="cursor-pointer accent-amber-500"
              />
            </th>
            <th className="text-left py-2 w-10">#</th>
            <th className="text-left">Symbol</th>
            <th className="text-left">Sector</th>
            <th className="text-right">Close</th>
            <th className="text-right">Score</th>
            <th className="text-center">MTF</th>
            <th className="text-left">Held?</th>
            <th className="text-left">Action (if held)</th>
          </tr>
        </thead>
        <tbody>
          {signals.map((s) => {
            const picked = selectedSet.has(s.symbol)
            const held = heldBySymbol[s.symbol]
            const action = holdActions[s.symbol] ?? 'skip'
            // mtfFlags returns false only when explicitly checked; undefined = unknown (assume eligible)
            const mtf = mtfFlags[s.symbol] !== false
            return (
              <tr key={s.symbol}
                  className={[
                    'border-t border-neutral-800',
                    picked ? '' : 'opacity-50',
                    !mtf ? 'bg-red-500/5' : '',
                    held && picked ? 'bg-blue-500/5' : '',
                  ].join(' ')}>
                <td className="text-center py-1.5">
                  <input
                    type="checkbox"
                    checked={picked}
                    disabled={!mtf}
                    onChange={() => onToggleSelected(s.symbol)}
                    className="accent-amber-500 w-4 h-4 cursor-pointer disabled:cursor-not-allowed"
                  />
                </td>
                <td>{s.rank}</td>
                <td className="font-medium">{s.symbol}</td>
                <td className="text-neutral-400">{s.sector ?? '—'}</td>
                <td className="text-right font-mono text-neutral-300">{s.close_at_signal != null ? inr(s.close_at_signal) : '—'}</td>
                <td className="text-right text-amber-400">{s.score.toFixed(0)}</td>
                <td className="text-center">
                  {mtfFlags[s.symbol] === undefined
                    ? <span className="text-neutral-600">?</span>
                    : mtf ? <span className="text-green-400">✓</span> : <span className="text-red-400">✗</span>}
                </td>
                <td>
                  {held ? (
                    <div className="text-xs">
                      <span className="text-blue-300 font-mono">{held.qty.toLocaleString('en-IN')} sh</span>
                      <span className="text-neutral-500"> @ {inr(held.avg_entry)}</span>
                      <span className={held.current_price >= held.avg_entry ? ' text-green-400' : ' text-red-400'}>
                        {' '}({((held.current_price / held.avg_entry - 1) * 100).toFixed(1)}%)
                      </span>
                    </div>
                  ) : (
                    <span className="text-neutral-700">—</span>
                  )}
                </td>
                <td>
                  {!picked ? (
                    <span className="text-neutral-700">—</span>
                  ) : !mtf ? (
                    <span className="text-red-400 text-xs">non-MTF (skipped)</span>
                  ) : held ? (
                    <select
                      value={action}
                      onChange={e => onSetHoldAction(s.symbol, e.target.value as HoldAction)}
                      className="bg-neutral-950 border border-blue-500/40 text-blue-200 text-xs rounded px-2 py-1"
                    >
                      <option value="skip">Skip (hold existing)</option>
                      <option value="average">Average up</option>
                    </select>
                  ) : (
                    <span className="text-amber-300 text-xs">new entry</span>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function PreviewBlock(props: {
  preview: TradePreviewResponse;
  smoke:   TradeSmokeResponse | null;
  place:   TradePlaceResponse | null;
  step: Step;
  marketIsOpen: boolean; minutesToOpen: number; secondsToOpen: number;
  onSmokeTest: () => void;
  onStageClick: () => void;
  onPlaceNow: () => void;
  stageResult: { n_staged: number; n_skipped: number; target_date: string } | null;
}) {
  const { preview, smoke, place, step, marketIsOpen,
          onSmokeTest, onStageClick, onPlaceNow, stageResult } = props

  const orders = preview.orders
  const skipped = preview.skipped
  const avgCount = orders.filter(o => o.is_averaging).length

  // Map symbol → final status from place result if available
  const finalStatusBySym: Record<string, TradePlaceResponse['orders'][number]> = {}
  if (place?.orders) {
    for (const o of place.orders) {
      if (o.role === 'ENTRY') finalStatusBySym[o.symbol] = o
    }
  }

  return (
    <Card title="Order Preview" hint={`${orders.length} orders · ${avgCount} averaging into held positions`}>
      {skipped.length > 0 && (
        <div className="mb-3 text-xs text-neutral-400">
          <span className="text-red-400">Skipped:</span>{' '}
          {skipped.map(s => (
            <span key={s.symbol} className="inline-block bg-red-500/10 text-red-300 px-2 py-0.5 rounded mr-1 font-mono">
              {s.symbol} ({s.reason})
            </span>
          ))}
        </div>
      )}

      {orders.length === 0 ? (
        <div className="text-neutral-400 text-sm py-4">
          No orders to place. Adjust selection or hold actions.
        </div>
      ) : (
      <div className="overflow-x-auto -mx-4 px-4 mb-4">
        <table className="w-full text-xs">
          <thead className="text-neutral-400">
            <tr>
              <th className="text-left py-2">#</th>
              <th className="text-left">Symbol</th>
              <th className="text-right">Close</th>
              <th className="text-right">Margin %</th>
              <th className="text-right">Margin ₹</th>
              <th className="text-right">Qty</th>
              <th className="text-right">Notional</th>
              <th className="text-right">Lev</th>
              <th className="text-right">SL ₹</th>
              <th className="text-right">Target ₹</th>
              <th className="text-center">SL type</th>
              <th className="text-center">Action</th>
              <th className="text-center">Status</th>
            </tr>
          </thead>
          <tbody>
            {orders.map(o => {
              const live = finalStatusBySym[o.symbol]
              const rowCls = 'border-t border-neutral-800'
                + (o.is_averaging ? ' bg-blue-500/5' : '')
                + (o.margin_lookup_failed ? ' bg-amber-500/10' : '')
              return (
                <tr key={o.symbol} className={rowCls}>
                  <td className="py-1.5">{o.rank}</td>
                  <td className="font-medium">
                    {o.symbol}
                    {o.is_averaging && <span className="ml-1 text-blue-300 text-[10px]">+avg</span>}
                    {o.margin_lookup_failed && <span title="Kite margin API failed for this symbol — sized as cash (qty × close) instead of margin. Verify before placing." className="ml-1 text-amber-400 text-[10px]">⚠ cash-sized</span>}
                  </td>
                  <td className="text-right font-mono">{o.close.toFixed(2)}</td>
                  <td className="text-right font-mono text-neutral-400">
                    {o.margin_pct != null ? `${o.margin_pct.toFixed(1)}%` : '—'}
                  </td>
                  <td className="text-right font-mono">{inr(o.margin_required)}</td>
                  <td className="text-right font-mono">{o.qty.toLocaleString('en-IN')}</td>
                  <td className="text-right font-mono text-neutral-300">{inr(o.notional)}</td>
                  <td className="text-right font-mono text-amber-300">
                    {o.effective_leverage != null ? `${o.effective_leverage.toFixed(1)}×` : '—'}
                  </td>
                  <td className="text-right font-mono text-red-300">{o.sl_price.toFixed(2)}</td>
                  <td className="text-right font-mono text-green-300">{o.target_price.toFixed(2)}</td>
                  <td className="text-center font-mono">
                    <span className={o.sl_order_type === 'SL-M' ? 'text-amber-400' : 'text-neutral-400'}>{o.sl_order_type}</span>
                  </td>
                  <td className="text-center text-xs">
                    {o.is_averaging
                      ? <span className="text-blue-300">avg → {o.existing_qty.toLocaleString('en-IN')} sh existing</span>
                      : <span className="text-amber-300">new entry</span>}
                  </td>
                  <td className="text-center">
                    <PlaceStatusBadge status={live?.status} />
                  </td>
                </tr>
              )
            })}
          </tbody>
          <tfoot className="text-xs font-mono">
            <tr className="border-t-2 border-neutral-700">
              <td colSpan={4} className="py-2 text-right text-neutral-400">Total margin (≈ your cash) →</td>
              <td className="text-right text-green-300">{inr(preview.plan_total_margin ?? preview.plan_total_notional)}</td>
              <td colSpan={1}></td>
              <td className="text-right text-neutral-400">Notional →</td>
              <td className="text-right text-amber-300">
                {preview.plan_eff_leverage != null ? `${preview.plan_eff_leverage.toFixed(1)}×` : '—'}
              </td>
              <td colSpan={5} className="text-right text-neutral-500 pl-2">
                {preview.plan_eff_leverage != null
                  ? `${inr(preview.plan_total_notional)} notional via Zerodha MTF — your cash exposure is the margin column`
                  : 'broker-decided margin will be a fraction of this for MTF'}
              </td>
            </tr>
          </tfoot>
        </table>
      </div>
      )}

      {smoke && (
        <div className={[
          'rounded border p-3 mb-3 text-sm',
          step === 'smoke_running' ? 'bg-amber-500/10 border-amber-500/30 text-amber-200' :
          step === 'smoke_ok'      ? 'bg-green-500/10 border-green-500/30 text-green-200' :
                                     'bg-red-500/10 border-red-500/30 text-red-200',
        ].join(' ')}>
          {smoke.status === 'PLACED'
            ? <>✓ Smoke test PASSED. Bought 1 share of <strong>{smoke.smoke_symbol}</strong> @ ₹{smoke.smoke_price.toFixed(2)} · kite_order_id <code>{smoke.kite_order_id}</code></>
            : <>✗ Smoke test failed: {smoke.error}</>}
        </div>
      )}

      {step === 'staging' && (
        <div className="rounded bg-amber-500/10 border border-amber-500/30 p-3 mb-3">
          <div className="text-amber-200 text-sm font-medium animate-pulse">
            Staging {orders.length} order{orders.length !== 1 ? 's' : ''} to Pre-Market…
          </div>
        </div>
      )}

      {step === 'staged' && stageResult && (
        <div className="rounded bg-green-500/10 border border-green-500/30 p-3 mb-3 text-green-200 text-sm">
          ✓ Staged <strong>{stageResult.n_staged}</strong> order{stageResult.n_staged !== 1 ? 's' : ''} to Pre-Market for{' '}
          <code className="text-green-300">{stageResult.target_date}</code>.
          {stageResult.n_skipped > 0 && <span className="text-amber-300"> · {stageResult.n_skipped} skipped</span>}
          <div className="mt-1 text-green-300/80">
            Review and Confirm at{' '}
            <Link href="/falcon/premarket" className="underline font-medium">/falcon/premarket</Link>
            {' '}— deployer fires confirmed items at 9:15 IST.
          </div>
        </div>
      )}

      {step === 'stage_failed' && (
        <div className="rounded bg-red-500/10 border border-red-500/30 p-3 mb-3 text-red-200 text-sm">
          ✗ Staging failed. See error above.
        </div>
      )}

      {step === 'placing' && (
        <div className="rounded bg-amber-500/10 border border-amber-500/30 p-3 mb-3">
          <div className="text-amber-200 text-sm font-medium animate-pulse">
            Placing {orders.length} ENTRY + {orders.length} SL order{orders.length !== 1 ? 's' : ''} on Kite…
          </div>
        </div>
      )}

      {step === 'placed' && place && (
        <div className={[
          'rounded p-3 mb-3 text-sm border',
          place.n_failed === 0
            ? 'bg-green-500/10 border-green-500/30 text-green-200'
            : 'bg-amber-500/10 border-amber-500/40 text-amber-100',
        ].join(' ')}>
          {place.n_failed === 0
            ? <>✓ Placed <strong>{place.n_filled}</strong> of {place.n_attempted} orders on Kite. batch_id <code>{place.batch_id}</code></>
            : <>⚠ Placed <strong>{place.n_filled}</strong> of {place.n_attempted}; <strong>{place.n_failed}</strong> failed. See per-row status above and <Link href="/falcon/positions" className="underline">/falcon/positions</Link> for what landed.</>}
        </div>
      )}

      {step === 'place_failed' && (
        <div className="rounded bg-red-500/10 border border-red-500/30 p-3 mb-3 text-red-200 text-sm">
          ✗ Place Now failed. See error above. Nothing was placed on Kite.
        </div>
      )}

      {/* Buttons are available from `previewing` onwards. Smoke is OPTIONAL
          (operator preference 2026-05-11) — Place Now / Stage no longer require
          a successful smoke test. They're still recommended for a first batch
          on a new symbol / new token but skip-able for power users. */}
      <div className="flex items-center gap-3 flex-wrap">
        {orders.length > 0 && (step === 'previewing' || step === 'smoke_running' ||
                                step === 'smoke_ok' || step === 'smoke_failed' ||
                                step === 'stage_failed' || step === 'place_failed') && (
          <>
            {marketIsOpen && (
              <button onClick={onPlaceNow} className={primaryBtn}>
                ▶ Place Now ({orders.length}) — Market Open
              </button>
            )}
            <button onClick={onStageClick}
                    className={marketIsOpen
                      ? 'px-4 py-2 bg-blue-500/20 text-blue-200 border border-blue-500/40 rounded font-medium'
                      : primaryBtn}>
              {marketIsOpen ? `Stage for Tomorrow (${orders.length})` : `Stage to Pre-Market (${orders.length})`}
            </button>
            <button onClick={onSmokeTest}
                    className="px-3 py-2 text-sm text-neutral-400 hover:text-neutral-100 border border-neutral-800 rounded"
                    title="Optional — places 1 share of the cheapest symbol as a Kite connectivity probe. Skip if you've already tested today.">
              {step === 'smoke_ok'
                ? '✓ Smoke (re-run)'
                : 'Smoke Test (optional, 1 share)'}
            </button>
          </>
        )}
        {step === 'staged' && (
          <Link href="/falcon/premarket"
                className="px-4 py-2 bg-amber-500 text-neutral-950 rounded font-semibold">
            Open Pre-Market →
          </Link>
        )}
        {step === 'placed' && (
          <Link href="/falcon/positions"
                className="px-4 py-2 bg-amber-500 text-neutral-950 rounded font-semibold">
            Open Positions →
          </Link>
        )}
      </div>
    </Card>
  )
}

function PlaceStatusBadge({ status }: { status?: TradePlaceResponse['orders'][number]['status'] }) {
  if (!status)            return <span className="text-neutral-700">—</span>
  if (status === 'PLACING') return <span className="text-amber-400 animate-pulse">…</span>
  if (status === 'PLACED')  return <span className="text-green-400">✓</span>
  if (status === 'CANCELLED') return <span className="text-yellow-400">cancelled</span>
  if (status === 'REJECTED' || status === 'FAILED') return <span className="text-red-400">✗</span>
  return <span className="text-neutral-400">{status}</span>
}

function ConfirmModal({ preview, confirmText, onConfirmTextChange, onConfirm, onCancel, kind }: {
  preview: TradePreviewResponse;
  confirmText: string; onConfirmTextChange: (s: string) => void;
  onConfirm: () => void; onCancel: () => void;
  kind: 'place' | 'stage';     // 'place' = fires now on Kite; 'stage' = queues for 9:15 IST deploy
}) {
  const ok = confirmText === 'CONFIRM'
  const orders = preview.orders
  const avgCount = orders.filter(o => o.is_averaging).length
  const isPlace = kind === 'place'
  const title = isPlace ? 'Confirm Batch Placement (Live on Kite)'
                        : 'Confirm Pre-Market Staging'
  const verb  = isPlace ? 'place' : 'stage for tomorrow\'s 9:15 IST deploy window'
  const ctaLabel = isPlace ? 'Place Now' : 'Stage to Pre-Market'
  return (
    <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50">
      <div className={`bg-neutral-900 border rounded p-6 max-w-lg w-full ${
        isPlace ? 'border-amber-500/40' : 'border-blue-500/40'
      }`}>
        <h3 className={`text-lg font-bold mb-3 ${isPlace ? 'text-amber-300' : 'text-blue-300'}`}>
          {title}
        </h3>
        <p className="text-sm text-neutral-300 mb-4">
          You are about to {verb} <strong className={isPlace ? 'text-amber-300' : 'text-blue-300'}>{orders.length}</strong> BUY (MTF) + {' '}
          <strong className={isPlace ? 'text-amber-300' : 'text-blue-300'}>{orders.length}</strong> SL orders for total notional{' '}
          <strong className={isPlace ? 'text-amber-300' : 'text-blue-300'}>{inrLakh(preview.plan_total_notional)}</strong>.
          {!isPlace && (
            <span className="block mt-2 text-blue-200/90">
              Staging writes to <code>falcon_premarket_staging</code> (DB only). The pre-market deployer thread
              picks up QUEUED items at the next 9:14-9:30 IST window and calls <code>kite.place_order()</code> for
              each — that's when real Kite orders actually fire. You'll still need to <strong>Confirm</strong> in{' '}
              <code>/falcon/premarket</code> to flip STAGED → QUEUED.
            </span>
          )}
          {isPlace && (
            <span className="block mt-2">Zerodha decides the actual margin charged at order placement
              (typically a fraction of notional for MTF).</span>
          )}
          {avgCount > 0 && (
            <span className="block mt-2 text-blue-300">
              {avgCount} of these orders <strong>add to existing held positions</strong>.
            </span>
          )}
          <span className="block mt-2">
            This is real money. Type <code className={isPlace ? 'text-amber-300' : 'text-blue-300'}>CONFIRM</code> below to proceed.
          </span>
        </p>
        <input
          type="text"
          value={confirmText}
          onChange={e => onConfirmTextChange(e.target.value)}
          placeholder="Type CONFIRM"
          className="w-full bg-neutral-950 border border-neutral-700 rounded px-3 py-2 text-neutral-100 font-mono mb-4"
          autoFocus
        />
        <div className="flex gap-2 justify-end">
          <button onClick={onCancel} className="px-4 py-2 text-neutral-400 hover:text-neutral-100">Cancel</button>
          <button onClick={onConfirm} disabled={!ok}
                  className={ok
                    ? `px-4 py-2 ${isPlace ? 'bg-amber-500 text-neutral-950' : 'bg-blue-500 text-neutral-950'} rounded font-semibold`
                    : 'px-4 py-2 bg-neutral-800 text-neutral-600 rounded font-semibold cursor-not-allowed'}>
            {ctaLabel}
          </button>
        </div>
      </div>
    </div>
  )
}
