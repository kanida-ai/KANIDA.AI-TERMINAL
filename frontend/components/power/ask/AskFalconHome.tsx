'use client'

/**
 * AskFalconHome — the "Ask Falcon" home (F2 master-detail), 100% REAL data.
 *
 * LAYOUT INVARIANT (operator, 2026-06-21):
 *   • LEFT   — the AppShell 6-mode nav (not rendered here).
 *   • MIDDLE — focused control column: trading-style selector → Falcon Top 10
 *              FILTERS (universe / EOD date / sector) → Top 10 list → full
 *              Nifty-500 stock search → compact "Start Co-Trading" CTA.
 *   • RIGHT  — the ANALYSIS WORKSPACE. By default it explains the selected
 *              Top-10 pick; after a stock search it shows that stock's analysis.
 *
 * HARD RULE: no mock/static content. Real sources:
 *   • Top 10 list + explanation  — PowerAPI.falconTop20(universe, sector, signal_date)
 *     → Top20Response (seeded server-side, re-fetched client-side on filter change).
 *   • Greeting name              — real session (passed from page.tsx).
 *   • Universe search            — PowerAPI.universeSymbols() (GET /api/power/universe/
 *     symbols, LIVE — full ~477-name list, fetched once, filtered client-side). On
 *     failure → graceful fallback to the loaded Top-10 symbols (no crash).
 *   • Any-stock analysis         — PowerAPI.analyzeStock() (GET /api/power/ask/
 *     analyze-stock, LIVE). 404 NOT_COVERED → honest "not covered by Falcon".
 *     Top-10 stocks render from loaded data (REAL DetailCard).
 *   • Current / Prev-day LTP     — PowerAPI.quote() (GET /api/power/quote, LIVE).
 *     HONESTY: last_close is the LAST EOD CLOSE, labeled "last close · {as_of}",
 *     NEVER a live tick. Entry-day open stays genuinely "market open pending".
 *
 * Honesty: only "Falcon Top 10 Swing" is LIVE; other styles render Launch-Pending
 * (no faked re-ranking). Tiers are REAL engine bands (GOLD/PREMIUM/ENTERPRISE/
 * STANDARD/AVOID). The badge is a tier-derived SETUP-QUALITY label, not a buy call.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useRouter } from 'next/navigation'
import { PowerAPI, PowerAPIError } from '@/lib/power-api'
import type { Quote, AnalyzeStockResponse } from '@/lib/power-api'
import { CompassLogo } from '@/components/power/CompassLogo'
import type {
  Top20Pick, Top20Response, Top20Universe, Rating,
} from '@/lib/falcon-top20-types'

// ── F2 palette helpers (tokens defined in globals.css :root) ───────────────
const C = {
  panel: 'var(--f2-panel)', card: 'var(--f2-card)', card2: 'var(--f2-card-2)',
  mint: 'var(--f2-mint)', mintHi: 'var(--f2-mint-hi)', mintDim: 'var(--f2-mint-dim)',
  ink: 'var(--f2-ink)', ink2: 'var(--f2-ink-2)', muted: 'var(--f2-muted)', faint: 'var(--f2-faint)',
  line: 'var(--f2-line)', line2: 'var(--f2-line-2)', red: 'var(--f2-red)',
}

// ── Trading styles (was "personas"). ONLY swing is LIVE. ───────────────────
type StyleId = 'swing' | 'btst' | 'intraday' | 'weekly' | 'longterm'
type Style = { id: StyleId; name: string; hold: string; live: boolean; icon: keyof typeof ICON }
const STYLES: Style[] = [
  { id: 'swing',    name: 'Falcon Top 10 Swing', hold: '~7-day hold', live: true,  icon: 'flame' },
  { id: 'btst',     name: 'BTST Trader',         hold: '1–2 days',    live: false, icon: 'clock' },
  { id: 'intraday', name: 'Intraday Trader',     hold: 'Same day',    live: false, icon: 'bolt' },
  { id: 'weekly',   name: 'Weekly Swing',        hold: '1–4 weeks',   live: false, icon: 'trend' },
  { id: 'longterm', name: 'Long-Term Investor',  hold: '3+ months',   live: false, icon: 'shield' },
]

const UNIVERSE_OPTIONS: Array<{ key: Top20Universe; label: string }> = [
  { key: 'all500',   label: 'Nifty 500' },
  { key: 'nifty50',  label: 'Nifty 50'  },
  { key: 'nifty100', label: 'Nifty 100' },
  { key: 'nifty200', label: 'Nifty 200' },
  { key: 'fno',      label: 'F&O'       },
]

// ── Guided intents (the ORIGINAL composer-config defaults, restored). ──────
// Closed list only → an invalid/empty request is impossible. `needsStock` shows
// the Stock field; `live=false` intents are selectable but disabled ("soon")
// until their backend exists. Only "analyze-stock" + "explain-top10" run against
// the already-loaded REAL Top20Response.
type IntentId =
  | 'analyze-stock' | 'analyze-sector' | 'explain-top10' | 'find-setups'
  | 'compare-stocks' | 'why-tier' | 'review-portfolio' | 'sector-strength'
  | 'autotrade-readiness'
type Intent = { id: IntentId; label: string; needsStock: boolean; live: boolean }
const INTENTS: Intent[] = [
  { id: 'analyze-stock',       label: 'Analyze a stock',           needsStock: true,  live: true  },
  { id: 'analyze-sector',      label: 'Analyze a sector',          needsStock: false, live: false },
  { id: 'explain-top10',       label: "Explain today's Top 10",    needsStock: false, live: true  },
  { id: 'find-setups',         label: 'Find setups beyond Top 10', needsStock: false, live: false },
  { id: 'compare-stocks',      label: 'Compare two stocks',        needsStock: true,  live: false },
  { id: 'why-tier',            label: 'Why is X Gold/Enterprise',  needsStock: true,  live: false },
  { id: 'review-portfolio',    label: 'Review my portfolio',       needsStock: false, live: false },
  { id: 'sector-strength',     label: 'Market & sector strength',  needsStock: false, live: false },
  { id: 'autotrade-readiness', label: 'Check AutoTrade readiness', needsStock: false, live: false },
]

type Props = {
  firstName: string
  data:      Top20Response | null   // server-seeded initial Top20Response (null = backend down)
}

// What the right column is currently showing.
type Workspace =
  | { kind: 'pick' }                                  // explain the selected Top-10 pick
  | { kind: 'analysis'; symbol: string; name: string | null; sector: string | null }

export function AskFalconHome({ firstName, data: seed }: Props) {
  const router = useRouter()

  // ── Filter state (drives a REAL client re-fetch of falconTop20) ──────────
  const [universe, setUniverse]   = useState<Top20Universe>((seed?.universe as Top20Universe) ?? 'all500')
  const [sector, setSector]       = useState<string | null>(seed?.sector ?? null)
  const [signalDate, setSignalDate] = useState<string | null>(seed?.signal_date ?? null)

  const [data, setData]       = useState<Top20Response | null>(seed)
  const [loading, setLoading] = useState(false)
  const [fetchErr, setFetchErr] = useState(false)
  const firstRun = useRef(true)

  const picks = data?.picks ?? []

  const [style, setStyle] = useState<StyleId>('swing')
  const [styleOpen, setStyleOpen] = useState(false)
  const [sel, setSel] = useState<string | null>(null)
  const [ws, setWs] = useState<Workspace>({ kind: 'pick' })

  // EOD quotes for the loaded Top 10 (PowerAPI.quote, batched ≤60). Keyed by
  // symbol. last_close/prev_close are last EOD closes, NOT live ticks.
  const [quotes, setQuotes] = useState<Record<string, Quote>>({})

  const cur = STYLES.find(s => s.id === style)!

  // Fetch EOD quotes for the current Top-10 symbols whenever the picks change.
  // One batched call (≤60). Failure → leave quotes empty (cells show "—").
  useEffect(() => {
    const symbols = picks.map(p => p.symbol)
    if (symbols.length === 0) { setQuotes({}); return }
    let alive = true
    PowerAPI.quote(symbols)
      .then(res => { if (alive) setQuotes(res) })
      .catch(() => { if (alive) setQuotes({}) })
    return () => { alive = false }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data?.signal_date, data?.universe, data?.sector])

  // Re-fetch on filter change (skip the very first render — seed already loaded).
  useEffect(() => {
    if (firstRun.current) { firstRun.current = false; return }
    let alive = true
    setLoading(true); setFetchErr(false)
    PowerAPI.falconTop20(universe, sector, signalDate)
      .then(res => { if (alive) { setData(res); setSel(res.picks[0]?.symbol ?? null); setWs({ kind: 'pick' }) } })
      .catch(() => { if (alive) { setFetchErr(true) } })
      .finally(() => { if (alive) setLoading(false) })
    return () => { alive = false }
  }, [universe, sector, signalDate])

  // Selected pick (defaults to #1 when nothing chosen).
  const selectedSymbol = sel ?? picks[0]?.symbol ?? null
  const current: Top20Pick | null =
    picks.find(p => p.symbol === selectedSymbol) ?? picks[0] ?? null

  // Sector options come from the loaded picks (no hard-coded drift).
  const sectorOptions = useMemo(
    () => Array.from(new Set(picks.map(p => p.sector).filter(Boolean))).sort(),
    [picks],
  )
  const availableDates = data?.available_signal_dates ?? []
  const latestDate = data?.latest_signal_date ?? null

  function selectPick(symbol: string) {
    setSel(symbol)
    setWs({ kind: 'pick' })
  }
  function analyzeStock(symbol: string, name: string | null, sector: string | null) {
    // If the searched symbol is in the loaded Top 10, treat as a pick (REAL data).
    const inTop10 = picks.find(p => p.symbol === symbol)
    if (inTop10) { setSel(symbol); setWs({ kind: 'pick' }); return }
    setWs({ kind: 'analysis', symbol, name, sector })
  }

  return (
    <div className="flex flex-row min-h-screen md:min-h-0 md:h-full md:overflow-hidden"
         style={{ background: 'var(--f2-canvas)' }}>
      {/* ── MIDDLE: fixed-height flex column ──
          TOP (shrink-0): style selector + Falcon Top 10 heading + one-line filters
          + context line. MIDDLE (flex-1 min-h-0): the Top 10 list (all 10 fit; only
          a smaller viewport forces internal scroll). BOTTOM (shrink-0, sticky): the
          search composer — always visible, never pushed below the fold. */}
      <div className="hidden md:flex flex-col w-[360px] shrink-0 border-r md:h-screen md:min-h-0 md:overflow-hidden"
           style={{ borderColor: C.line, background: C.panel }}>
        {/* trading-style selector (TOP region — shrink-0) */}
        <div className="relative shrink-0 px-3.5 pt-2 pb-1.5 border-b" style={{ borderColor: C.line }}>
          <div className="text-[10px] uppercase tracking-[0.07em] mb-1" style={{ color: C.faint }}>
            Choose Your Trading Style
          </div>
          <button
            type="button"
            onClick={() => setStyleOpen(o => !o)}
            className="w-full flex items-center gap-2.5 rounded-[10px] px-2.5 py-1.5 border transition-colors"
            style={{ background: 'rgba(255,255,255,0.04)', borderColor: C.line2 }}
          >
            <span className="grid place-items-center w-6 h-6 rounded-lg shrink-0"
                  style={{ background: C.mintDim, color: C.mint }}>
              {ICON[cur.icon](14)}
            </span>
            <span className="text-left leading-tight">
              <span className="block text-[13px] font-semibold" style={{ color: C.ink }}>{cur.name}</span>
              <span className="block text-[10.5px]" style={{ color: C.faint }}>
                Top 10 · {cur.hold}{!cur.live && ' · soon'}
              </span>
            </span>
            <span className="ml-auto" style={{ color: C.faint }}>{ICON.chevron(14)}</span>
          </button>
          {styleOpen && (
            <div className="absolute left-3.5 right-3.5 top-[calc(100%-4px)] z-30 rounded-xl p-1.5 border
                            shadow-[0_22px_50px_-16px_rgba(0,0,0,0.7)]"
                 style={{ background: '#0e1713', borderColor: C.line2 }}
                 onMouseLeave={() => setStyleOpen(false)}>
              {STYLES.map(s => (
                <button
                  key={s.id}
                  type="button"
                  onClick={() => { setStyle(s.id); setStyleOpen(false); setWs({ kind: 'pick' }) }}
                  className="w-full flex items-center gap-2.5 rounded-lg px-2.5 py-2.5 text-left transition-colors"
                  style={s.id === style ? { background: 'rgba(63,227,164,0.08)' } : undefined}
                >
                  <span className="grid place-items-center w-6 h-6 rounded-lg shrink-0"
                        style={{ background: C.mintDim, color: C.mint }}>
                    {ICON[s.icon](13)}
                  </span>
                  <span className="leading-tight">
                    <span className="block text-[12.5px] font-semibold" style={{ color: C.ink }}>{s.name}</span>
                    <span className="block text-[11px]" style={{ color: C.faint }}>
                      {s.hold}{!s.live && ' · Launch-Pending'}
                    </span>
                  </span>
                  {s.live && <span className="ml-auto w-1.5 h-1.5 rounded-full"
                                   style={{ background: C.mint, boxShadow: `0 0 8px ${C.mint}` }} />}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* list header + FILTERS (swing only) — TOP region, shrink-0 */}
        <div className="shrink-0 px-3.5 pt-2 pb-1.5 border-b" style={{ borderColor: C.line }}>
          <div className="flex items-center justify-between mb-1.5">
            <span className="text-xs font-semibold" style={{ color: C.ink }}>Falcon Top 10</span>
            {style === 'swing' && (
              <span className="flex items-center gap-1.5 text-[11px]" style={{ color: C.muted }}>
                {loading
                  ? <span style={{ color: C.faint }}>refreshing…</span>
                  : <>
                      <span className="w-1.5 h-1.5 rounded-full"
                            style={{ background: C.mint, boxShadow: `0 0 8px ${C.mint}` }} />
                      {data?.signal_date ?? 'today'}
                    </>}
              </span>
            )}
          </div>

          {style === 'swing' && (
            <FilterBar
              universe={universe} onUniverse={setUniverse}
              sector={sector} onSector={setSector} sectorOptions={sectorOptions}
              signalDate={data?.signal_date ?? signalDate} onSignalDate={setSignalDate}
              availableDates={availableDates} latestDate={latestDate}
            />
          )}

          {/* context line: signal-date EOD + entry-day (REAL Top20Response fields) */}
          {style === 'swing' && data && (
            <div className="mt-1 text-[10.5px] leading-snug" style={{ color: C.faint }}>
              Engine emitted at{' '}
              <b style={{ color: C.muted, fontWeight: 600 }}>{data.signal_date ?? '—'}</b> EOD
              {' · '}qualified for entry on{' '}
              <b style={{ color: C.muted, fontWeight: 600 }}>{data.entry_date ?? data.next_trading_day ?? '—'}</b>
              {' · '}<span style={{ color: C.muted }}>{labelForUniverse(universe)}</span>
              {sector && <> · <span style={{ color: C.muted }}>{sector}</span></>}
            </div>
          )}
        </div>

        {/* list / launch-pending / empty — MIDDLE region (flex-1 min-h-0).
            All 10 rows fit between filters and composer at 1366×768; internal
            scroll engages ONLY when a smaller viewport forces it. */}
        <div className="flex-1 min-h-0 overflow-y-auto px-2 py-0.5 [scrollbar-width:none] [&::-webkit-scrollbar]:w-0">
          {style !== 'swing'
            ? <MiddleStylePending style={cur} />
            : fetchErr
              ? <MiddleFetchError />
              : picks.length === 0
                ? (loading ? <MiddleLoading /> : <MiddleEmpty />)
                : picks.map(p => (
                    <ListRow
                      key={p.symbol}
                      pick={p}
                      selected={ws.kind === 'pick' && current?.symbol === p.symbol}
                      onSelect={() => selectPick(p.symbol)}
                    />
                  ))}
        </div>

        {/* full Nifty-500 stock search — F2 guided composer */}
        <StockSearch
          picks={picks} current={current} onAnalyze={analyzeStock}
          onExplainTop10={() => { setSel(picks[0]?.symbol ?? null); setWs({ kind: 'pick' }) }}
        />
      </div>

      {/* ── RIGHT: analysis workspace ──
          Viewport-locked flex column on md+: a FIXED header (greeting + CTA row)
          then a SCROLLABLE analysis region — so the page itself never scrolls and
          the most important card content sits at the top of its own scroll pane. */}
      <div className="flex-1 min-w-0 flex flex-col md:h-screen md:min-h-0 md:overflow-hidden">
        {/* FIXED header block (greeting + CTA row) — shrink-0, does NOT scroll on
            md+. Aligned to the middle column's top with matching vertical padding. */}
        <div className="px-5 md:px-8 pt-2 md:pt-3 pb-2 max-w-[820px] shrink-0">
          <Greeting firstName={firstName} />

          {/* compact action CTAs — Co-Trading + AutoTrade (route to full pages) */}
          <RightCTARow onGo={(href) => router.push(href)} />
        </div>

        {/* SCROLLABLE analysis region (flex-1 min-h-0, overflow-y:auto). First view
            with no scroll shows: stock name+rank+tier, the compact metrics row,
            "Why we're picking this", and the start of "Historical track record". */}
        <div className="flex-1 min-h-0 md:overflow-y-auto px-5 md:px-8 pt-0 pb-6 max-w-[820px]
                        [scrollbar-width:thin]">
          {ws.kind === 'analysis' ? (
            <StockAnalysis key={ws.symbol} symbol={ws.symbol} name={ws.name} sector={ws.sector}
                           onBack={() => setWs({ kind: 'pick' })} />
          ) : style !== 'swing' ? (
            <RightStylePending style={cur} />
          ) : fetchErr ? (
            <RightEmpty hasData={false} />
          ) : current ? (
            <DetailCard key={current.symbol} pick={current} response={data!} quote={quotes[current.symbol] ?? null} />
          ) : (
            <RightEmpty hasData={data != null} />
          )}

          {/* Mobile fallback: the middle column is hidden on small screens. */}
          {style === 'swing' && picks.length > 0 && ws.kind === 'pick' && (
            <div className="md:hidden mt-8">
              <div className="text-[10px] uppercase tracking-[0.07em] mb-2" style={{ color: C.faint }}>
                Falcon Top 10 — tap to explain
              </div>
              <div className="rounded-xl border divide-y" style={{ borderColor: C.line }}>
                {picks.map(p => (
                  <button key={p.symbol} type="button" onClick={() => selectPick(p.symbol)}
                          className="w-full flex items-center gap-3 px-3 py-2.5 text-left"
                          style={{ borderColor: C.line }}>
                    <span className="w-6 text-center font-mono text-[11px]" style={{ color: C.faint }}>#{p.rank}</span>
                    <span className="font-semibold text-[13.5px]" style={{ color: C.ink }}>{p.symbol}</span>
                    <span className="ml-auto"><TierBadgeF2 pick={p} /></span>
                    <DayPct pct={p.flags?.day_return_pct ?? null} />
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Filter bar (REUSES Top20Filters semantics; wired to client refetch) ────
function FilterBar({
  universe, onUniverse, sector, onSector, sectorOptions,
  signalDate, onSignalDate, availableDates, latestDate,
}: {
  universe: Top20Universe; onUniverse: (u: Top20Universe) => void
  sector: string | null; onSector: (s: string | null) => void; sectorOptions: string[]
  signalDate: string | null; onSignalDate: (d: string | null) => void
  availableDates: string[]; latestDate: string | null
}) {
  // Compact: universe + EOD + sector all on ONE line so the Top-10 list stays
  // dominant. All three still drive the REAL falconTop20 refetch.
  const selCls = 'text-[11px] rounded-md px-1.5 py-1 border focus:outline-none min-w-0'
  const selStyle = { background: 'rgba(255,255,255,0.03)', borderColor: C.line2, color: C.ink } as const
  return (
    <div className="flex items-center gap-1.5 flex-nowrap">
      <select
        aria-label="Universe"
        value={universe}
        onChange={(e) => onUniverse(e.target.value as Top20Universe)}
        className={selCls + ' font-semibold'}
        style={selStyle}
      >
        {UNIVERSE_OPTIONS.map(opt => <option key={opt.key} value={opt.key}>{opt.label}</option>)}
      </select>
      <select
        aria-label="EOD date"
        value={signalDate ?? ''}
        onChange={(e) => onSignalDate(e.target.value || null)}
        className={selCls + ' font-mono'}
        style={selStyle}
      >
        {availableDates.length === 0 && <option value="">No data</option>}
        {availableDates.map(d => (
          <option key={d} value={d}>{d}{d === latestDate ? ' (latest)' : ''}</option>
        ))}
      </select>
      <select
        aria-label="Sector"
        value={sector ?? ''}
        onChange={(e) => onSector(e.target.value || null)}
        className={selCls + ' flex-1 max-w-[120px]'}
        style={selStyle}
      >
        <option value="">All sectors</option>
        {sectorOptions.map(s => <option key={s} value={s}>{s}</option>)}
      </select>
    </div>
  )
}

function labelForUniverse(u: Top20Universe): string {
  return UNIVERSE_OPTIONS.find(o => o.key === u)?.label ?? String(u)
}

// ── Full Nifty-500 stock search ────────────────────────────────────────────
// Calls PowerAPI.universeSymbols() (GET /api/power/universe/symbols — LIVE, the
// full ~477-name tradable universe). Fetched once on first focus, filtered
// client-side as the user types. On failure we fall back to the loaded Top-10
// symbols so the field never breaks. NEVER fabricated.
// (name is `string | null` here so the Top-10 fallback rows — which carry no
// company name — type-check; the live endpoint always sends a name string.)
type UniverseSymbol = { symbol: string; name: string | null; sector: string | null }

function StockSearch({
  picks, current, onAnalyze, onExplainTop10,
}: {
  picks: Top20Pick[]
  current: Top20Pick | null
  onAnalyze: (symbol: string, name: string | null, sector: string | null) => void
  onExplainTop10: () => void
}) {
  const [q, setQ] = useState('')
  const [open, setOpen] = useState(false)
  // Guided intent (closed list). Defaults to "Analyze a stock".
  const [intentId, setIntentId] = useState<IntentId>('analyze-stock')
  const [intentOpen, setIntentOpen] = useState(false)
  const intent = INTENTS.find(i => i.id === intentId)!
  // The stock the composer will Ask about: a chosen search result, else the
  // currently-selected Top-10 pick (so the bar always has a valid target).
  const [chosen, setChosen] = useState<UniverseSymbol | null>(null)
  const [universeList, setUniverseList] = useState<UniverseSymbol[] | null>(null)
  const [universeErr, setUniverseErr] = useState(false)
  const [loaded, setLoaded] = useState(false)

  // Fall back to the loaded Top-10 symbols until /universe/symbols exists.
  const fallback: UniverseSymbol[] = useMemo(
    () => picks.map(p => ({ symbol: p.symbol, name: null, sector: p.sector })),
    [picks],
  )
  const list = universeList ?? fallback
  const fullUniverse = universeList != null

  // Lazy-load the full universe on first focus (LIVE endpoint).
  const ensureLoaded = useCallback(() => {
    if (loaded) return
    setLoaded(true)
    PowerAPI.universeSymbols()
      .then(res => {
        if (Array.isArray(res?.symbols) && res.symbols.length > 0) setUniverseList(res.symbols)
        else setUniverseErr(true)
      })
      .catch(() => setUniverseErr(true))
  }, [loaded])

  const matches = useMemo(() => {
    const needle = q.trim().toUpperCase()
    if (!needle) return list.slice(0, 8)
    return list
      .filter(s =>
        s.symbol.toUpperCase().includes(needle) ||
        (s.name ?? '').toUpperCase().includes(needle))
      .slice(0, 8)
  }, [q, list])

  // The active Stock-field target.
  const target: UniverseSymbol | null =
    chosen ?? (current ? { symbol: current.symbol, name: null, sector: current.sector } : null)

  function pickSuggestion(s: UniverseSymbol) {
    setChosen(s); setQ(''); setOpen(false)
  }
  // Ask runs the chosen intent. Only the two LIVE intents do anything today;
  // non-live intents render a disabled menu item (can't be selected to Ask).
  function ask() {
    if (!intent.live) return
    if (intent.id === 'explain-top10') { onExplainTop10(); return }
    // analyze-stock
    if (target) onAnalyze(target.symbol, target.name, target.sector)
  }
  const askDisabled = !intent.live || (intent.needsStock && !target)

  // F2 composer visual: INTENT [dropdown ▾]  STOCK [field]  [Ask →]
  // BOTTOM region of the middle column — shrink-0 so it is ALWAYS visible at the
  // bottom of the viewport-locked column, never pushed below the fold.
  return (
    <div className="relative shrink-0 px-3.5 pt-2 pb-2.5 border-t" style={{ borderColor: C.line }}>
      <div className="flex flex-row items-stretch gap-1.5 rounded-xl p-1.5 border"
           style={{ background: 'rgba(255,255,255,0.022)', borderColor: C.line2 }}>
        {/* INTENT (guided, closed list — the full restored default set) */}
        <div className="relative flex flex-col" style={{ flex: '1.1 1 0' }}>
          <button
            type="button"
            onClick={() => setIntentOpen(o => !o)}
            className="flex flex-col justify-center gap-px rounded-[9px] px-2 py-1 border min-w-0 text-left w-full h-full"
            style={{ background: 'rgba(255,255,255,0.03)', borderColor: C.line }}
          >
            <span className="text-[9px] uppercase tracking-[0.05em] font-medium" style={{ color: C.faint }}>Intent</span>
            <span className="flex items-center gap-1 text-[11.5px] font-medium" style={{ color: C.ink }}>
              <span className="flex-1 min-w-0 truncate">{intent.label}</span>
              <span className="shrink-0" style={{ color: C.faint }}>{ICON.chevron(11)}</span>
            </span>
          </button>
          {intentOpen && (
            <div className="absolute left-0 right-0 bottom-[calc(100%+6px)] z-40 rounded-xl p-1.5 border
                            shadow-[0_22px_50px_-16px_rgba(0,0,0,0.7)] w-[230px] max-h-[300px] overflow-y-auto"
                 style={{ background: '#0e1713', borderColor: C.line2 }}
                 onMouseLeave={() => setIntentOpen(false)}>
              {INTENTS.map(it => (
                <button
                  key={it.id}
                  type="button"
                  disabled={!it.live}
                  onClick={() => { if (it.live) { setIntentId(it.id); setIntentOpen(false) } }}
                  className="w-full flex items-center gap-2 rounded-lg px-2.5 py-1.5 text-left transition-colors disabled:cursor-not-allowed"
                  style={it.id === intentId ? { background: 'rgba(63,227,164,0.08)' } : undefined}
                >
                  <span className="flex-1 text-[12px] font-medium truncate"
                        style={{ color: it.live ? C.ink : C.faint }}>{it.label}</span>
                  {!it.live
                    ? <span className="text-[8.5px] font-mono uppercase tracking-[0.08em] shrink-0" style={{ color: C.faint }}>soon</span>
                    : it.id === intentId && <span className="w-1.5 h-1.5 rounded-full shrink-0"
                                                  style={{ background: C.mint, boxShadow: `0 0 8px ${C.mint}` }} />}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* STOCK (full Nifty-500 searchable field — shown for stock-needing intents) */}
        {intent.needsStock && (
          <div className="flex flex-col justify-center gap-px rounded-[9px] px-2 py-1 border min-w-0"
               style={{ flex: '1.4 1 0', background: 'rgba(255,255,255,0.03)', borderColor: C.line }}>
            <span className="text-[9px] uppercase tracking-[0.05em] font-medium" style={{ color: C.faint }}>Stock</span>
            <input
              value={open ? q : (q || target?.symbol || '')}
              onFocus={() => { ensureLoaded(); setOpen(true); setQ('') }}
              onBlur={() => setTimeout(() => setOpen(false), 120)}
              onChange={e => { setQ(e.target.value); setOpen(true) }}
              placeholder="Search name or symbol…"
              className="w-full min-w-0 bg-transparent outline-none text-[11.5px] font-medium truncate"
              style={{ color: C.ink }}
            />
          </div>
        )}

        {/* ASK */}
        <button
          type="button"
          onClick={ask}
          disabled={askDisabled}
          className="flex items-center justify-center gap-1.5 rounded-[9px] px-3.5 text-[13px] font-semibold shrink-0 transition-colors disabled:opacity-50
                     shadow-[0_8px_22px_-10px_rgba(63,227,164,0.7)]"
          style={{ background: C.mint, color: '#06130c' }}
        >
          Ask{ICON.arrow(13)}
        </button>
      </div>

      {!fullUniverse && universeErr && (
        <p className="text-[10px] mt-1.5" style={{ color: C.faint }}>
          Couldn&apos;t load the full universe just now — searching today&apos;s Top 10 instead.
        </p>
      )}

      {open && matches.length > 0 && (
        <div className="absolute left-3.5 right-3.5 bottom-[calc(100%+6px)] z-30 rounded-xl p-1.5 border
                        shadow-[0_22px_50px_-16px_rgba(0,0,0,0.7)] max-h-[260px] overflow-y-auto"
             style={{ background: '#0e1713', borderColor: C.line2 }}>
          {matches.map(s => (
            <button key={s.symbol} type="button" onMouseDown={(e) => { e.preventDefault(); pickSuggestion(s) }}
                    className="w-full flex items-center gap-2.5 rounded-lg px-2.5 py-2 text-left transition-colors hover:bg-white/[0.04]">
              <span className="font-mono text-[12.5px] font-semibold" style={{ color: C.ink }}>{s.symbol}</span>
              {s.name && <span className="text-[11px] truncate" style={{ color: C.faint }}>{s.name}</span>}
              {s.sector && <span className="ml-auto text-[10.5px] shrink-0" style={{ color: C.muted }}>{s.sector}</span>}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Greeting (brand mark = CompassLogo, matches left nav; IST clock) ────────
function Greeting({ firstName }: { firstName: string }) {
  const now = new Date()
  const istMs = now.getTime() + (now.getTimezoneOffset() + 330) * 60_000
  const h = new Date(istMs).getHours()
  const part = h < 12 ? 'morning' : h < 17 ? 'afternoon' : 'evening'
  return (
    <div>
      <div className="flex items-center gap-2.5 text-[20px] font-semibold tracking-[-0.03em]" style={{ color: C.ink }}>
        <CompassLogo size={22} />
        <span>Good {part}, {firstName}</span>
      </div>
      <div className="text-[12.5px] mt-1 max-w-[560px] leading-snug" style={{ color: C.muted }}>
        Here&apos;s today&apos;s Falcon Top 10 — pick any name on the left for the full
        explanation, or search any Nifty 500 stock to analyze it.
      </div>
    </div>
  )
}

// ── Right-column compact CTA row (Co-Trading + AutoTrade) ──────────────────
function RightCTARow({ onGo }: { onGo: (href: string) => void }) {
  return (
    <div className="grid grid-cols-2 gap-2 mt-2.5">
      <RightCTACard
        icon="handshake" label="Start Co-Trading"
        sub="Mirror the Top 10" onClick={() => onGo('/power/co-trading')}
      />
      <RightCTACard
        icon="bot" label="Create AutoTrade"
        sub="Automate entries" onClick={() => onGo('/power/autotrade')}
      />
    </div>
  )
}
function RightCTACard({
  icon, label, sub, onClick,
}: { icon: keyof typeof ICON; label: string; sub: string; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="flex items-center gap-2 rounded-xl px-2.5 py-2 border text-left transition-colors hover:bg-white/[0.03]"
      style={{ borderColor: C.line2, background: 'rgba(63,227,164,0.05)' }}
    >
      <span className="grid place-items-center w-6 h-6 rounded-lg shrink-0"
            style={{ background: C.mintDim, color: C.mint }}>{ICON[icon](14)}</span>
      <span className="leading-tight min-w-0">
        <span className="block text-[12.5px] font-semibold truncate" style={{ color: C.ink }}>{label}</span>
        <span className="block text-[10.5px] truncate" style={{ color: C.faint }}>{sub}</span>
      </span>
      <span className="ml-auto shrink-0" style={{ color: C.mint }}>{ICON.arrow(13)}</span>
    </button>
  )
}

// ── Top 10 list row — SINGLE tight line so all 10 fit one screen, no scroll.
//    rank · symbol (sector as dim inline suffix) · tier badge · signal-day % ─
function ListRow({ pick, selected, onSelect }: { pick: Top20Pick; selected: boolean; onSelect: () => void }) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className="w-full grid grid-cols-[16px_1fr_auto] items-center gap-2 rounded-lg px-2 py-1 text-left transition-colors"
      style={selected ? { background: C.mintDim } : undefined}
    >
      <span className="text-center font-mono tabular-nums text-[11px]" style={{ color: selected ? C.mint : C.faint }}>
        {pick.rank}
      </span>
      <span className="min-w-0 flex items-baseline gap-1.5">
        <span className="text-[13px] font-[550] shrink-0" style={{ color: C.ink }}>{pick.symbol}</span>
        <span className="text-[10px] truncate" style={{ color: C.faint, opacity: 0.8 }}>{pick.sector}</span>
      </span>
      <span className="flex items-center gap-2">
        <TierBadgeF2 pick={pick} />
        <DayPct pct={pick.flags?.day_return_pct ?? null} />
      </span>
    </button>
  )
}

// ── Tier badge — REAL signal_tier band (qualitative band) ──────────────────
const TIER_STYLE: Record<string, { color: string; bg: string; ring: string }> = {
  amber: { color: 'var(--f2-amber)',      bg: 'rgba(230,180,80,0.14)',  ring: 'rgba(230,180,80,0.45)' },
  green: { color: 'var(--f2-tier-green)', bg: 'rgba(63,227,164,0.14)',  ring: 'rgba(63,227,164,0.45)' },
  teal:  { color: 'var(--f2-teal)',       bg: 'rgba(75,203,224,0.14)',  ring: 'rgba(75,203,224,0.45)' },
  red:   { color: 'var(--f2-red)',        bg: 'rgba(232,115,107,0.14)', ring: 'rgba(232,115,107,0.45)' },
  gray:  { color: 'var(--f2-slate)',      bg: 'rgba(133,153,144,0.13)', ring: 'rgba(133,153,144,0.40)' },
}
// Derive F2 color from the canonical BAND (never signal_tier_color — its
// yellow/amber semantics differ). GOLD=amber, PREMIUM=teal, ENTERPRISE=green.
const BAND_COLORKEY: Record<string, string> = {
  GOLD: 'amber', PREMIUM: 'teal', ENTERPRISE: 'green', STANDARD: 'gray', AVOID: 'red',
}
function tierBand(raw: string | null | undefined): string | null {
  if (!raw) return null
  return raw.split('-')[0].toUpperCase()
}

function TierBadgeF2({ pick }: { pick: Top20Pick }) {
  const band = tierBand(pick.signal_tier)
  if (!band) return null
  const s = TIER_STYLE[BAND_COLORKEY[band ?? ''] ?? 'gray'] ?? TIER_STYLE.gray
  return (
    <span
      title={pick.signal_tier_reason ?? undefined}
      className="inline-flex items-center rounded-md font-semibold uppercase tracking-[0.04em] text-[9.5px] px-1.5 py-0.5"
      style={{ color: s.color, background: s.bg, boxShadow: `inset 0 0 0 1px ${s.ring}` }}
    >
      {band}
    </span>
  )
}

function DayPct({ pct }: { pct: number | null }) {
  if (pct == null || Number.isNaN(pct)) return <span className="font-mono tabular-nums text-[12.5px]" style={{ color: C.faint }}>—</span>
  const up = pct >= 0
  return (
    <span className="font-mono tabular-nums text-[12.5px] font-semibold" style={{ color: up ? C.mint : C.red }}>
      {up ? '+' : ''}{pct.toFixed(1)}%
    </span>
  )
}

// ── Setup-quality label (tier-derived; NOT a buy call) ─────────────────────
function setupQuality(pick: Top20Pick): string {
  const band = tierBand(pick.signal_tier)
  if (band === 'ENTERPRISE') return 'Top-quality setup'
  if (band === 'PREMIUM')    return 'Premium setup'
  if (band === 'GOLD')       return 'High-quality setup'
  if (band === 'AVOID')      return 'Lower-conviction setup'
  return ratingToQuality(pick.rating)
}
function ratingToQuality(r: Rating): string {
  return r === 'Strong Buy' ? 'Strong setup' : 'Solid setup'
}

// ── Price breakdown (RIGHT detail card) — COMPACT one horizontal metrics ROW.
//    On wide widths: 4 cells in a single divided row (Current · Prev · Signal ·
//    Entry). On narrow widths: a clean 2×2 grid (NOT 4 tall cards).
//    Current LTP = quote.last_close (LAST EOD CLOSE, NOT a live tick — labeled as
//    such), Prev-day LTP = quote.prev_close, day-change derived. Signal-day % is
//    REAL. Entry-day open is genuinely "market open pending". ─
function PriceBreakdown({ pick, response, quote }: { pick: Top20Pick; response: Top20Response; quote: Quote | null }) {
  const sig = pick.flags?.day_return_pct ?? null
  const lc = quote?.last_close ?? null
  const pc = quote?.prev_close ?? null
  const chg = lc != null && pc != null && pc !== 0 ? (lc / pc - 1) * 100 : null
  const asOf = quote?.as_of ?? null
  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 mt-0.5 rounded-[10px] border overflow-hidden"
         style={{ borderColor: C.line, background: 'rgba(255,255,255,0.02)' }}>
      <PriceCell
        label="Current LTP"
        value={lc != null ? fmtRs(lc) : '—'}
        valueColor={lc == null ? C.faint : chg == null ? C.ink : chg >= 0 ? C.mint : C.red}
        note={lc != null
          ? `last close${asOf ? ` · ${asOf}` : ''}${chg != null ? ` · ${chg >= 0 ? '+' : ''}${chg.toFixed(1)}%` : ''}`
          : 'quote unavailable'}
        real={lc != null}
      />
      <PriceCell
        label="Prev-day LTP"
        value={pc != null ? fmtRs(pc) : '—'}
        note={pc != null ? 'prior EOD close' : 'quote unavailable'}
      />
      <PriceCell
        label={`AI signal day · ${response.signal_date ?? '—'}`}
        value={sig != null ? `${sig >= 0 ? '+' : ''}${sig.toFixed(1)}%` : '—'}
        valueColor={sig == null ? C.faint : sig >= 0 ? C.mint : C.red}
        note="signal-day move"
        real
      />
      <PriceCell label="Entry day" value={response.entry_date ?? response.next_trading_day ?? '—'}
                 note="market open pending" />
    </div>
  )
}
// ₹ formatter — two decimals, grouped. EOD closes only (never a live tick).
function fmtRs(n: number): string {
  return '₹' + n.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}
// Compact metric cell. Thin same-theme separators (left/top borders) keep the
// row visually one strip; the signal-day cell gets a faint mint wash (REAL).
function PriceCell({
  label, value, note, valueColor = C.ink, real = false,
}: { label: string; value: string; note: string; valueColor?: string; real?: boolean }) {
  return (
    <div className="px-2.5 py-1.5 border-t border-l first:border-l-0 sm:border-t-0 [&:nth-child(3)]:border-l-0 sm:[&:nth-child(3)]:border-l"
         style={{ background: real ? 'rgba(63,227,164,0.06)' : undefined, borderColor: C.line }}>
      <div className="text-[9px] uppercase tracking-[0.04em] leading-tight truncate" style={{ color: C.faint }}>{label}</div>
      <div className="text-[14px] font-mono font-semibold tabular-nums leading-tight mt-0.5" style={{ color: valueColor }}>{value}</div>
      <div className="text-[8.5px] leading-tight" style={{ color: C.faint }}>{note}</div>
    </div>
  )
}

// ── Detail card (RIGHT) — Top-10 explanation; every section maps to a bucket ─
function DetailCard({ pick, response, quote }: { pick: Top20Pick; response: Top20Response; quote: Quote | null }) {
  const band = tierBand(pick.signal_tier)
  const s = TIER_STYLE[BAND_COLORKEY[band ?? ''] ?? 'gray'] ?? TIER_STYLE.gray
  const fires = pick.bucket1?.total_fires_today ?? null

  return (
    <div className="rounded-[16px] border p-4 mt-1 shadow-[0_24px_70px_-34px_rgba(0,0,0,0.55)]"
         style={{ background: `linear-gradient(180deg, ${C.card}, ${C.card2})`, borderColor: C.line2 }}>
      {/* header */}
      <div className="flex items-center gap-3 flex-wrap">
        <span className="font-mono font-bold text-[15px]" style={{ color: C.mint }}>#{pick.rank}</span>
        <span className="text-[16px] font-semibold" style={{ color: C.ink }}>
          {pick.symbol}
          <span className="text-[13px] font-normal ml-2" style={{ color: C.faint }}>· {pick.sector}</span>
        </span>
      </div>

      {/* badges */}
      <div className="flex items-center gap-2 flex-wrap mt-2 mb-0.5">
        <span className="text-[10px] font-semibold uppercase tracking-[0.04em] rounded px-2.5 py-1"
              style={{ color: C.mint, background: C.mintDim, boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.3)' }}>
          {setupQuality(pick)}
        </span>
        {band && (
          <span className="text-[10px] font-semibold uppercase tracking-[0.04em] rounded-md px-2.5 py-1"
                style={{ color: s.color, background: s.bg, boxShadow: `inset 0 0 0 1px ${s.ring}` }}>
            {band}
          </span>
        )}
      </div>

      {/* PRICE BREAKDOWN — last EOD close + prev close (REAL), signal-day % REAL */}
      <Section title="Price">
        <PriceBreakdown pick={pick} response={response} quote={quote} />
      </Section>

      <Rule />

      {/* Why we're picking this — bucket1.synthesis (REAL) */}
      <Section title="Why we're picking this">
        <p className="text-[13.5px] leading-[1.6]" style={{ color: C.ink2 }}>
          {pick.bucket1?.synthesis ?? 'Explanation is still being generated for this pick.'}
          {fires != null && fires > 0 && (
            <> {' '}<b style={{ color: C.ink, fontWeight: 550 }}>{fires} signals</b> are
              firing on this stock right now, and most are saying the same thing.</>
          )}
        </p>
      </Section>

      <Rule />

      {/* Historical track record — per_stock_backtest (REAL; honest empty) */}
      <Section title="Historical track record">
        <TrackRecord pick={pick} response={response} />
      </Section>

      <Rule />

      {/* What the sector is doing — bucket3.narrative (REAL) */}
      <Section title="What the sector is doing">
        <p className="text-[13.5px] leading-[1.6]" style={{ color: C.ink2 }}>
          {pick.bucket3?.narrative ?? 'Sector context is unavailable for this pick.'}
        </p>
      </Section>
    </div>
  )
}

function TrackRecord({ pick, response }: { pick: Top20Pick; response: Top20Response }) {
  const bt = pick.per_stock_backtest
  if (!bt || bt.n_trades === 0) {
    return (
      <p className="text-[13.5px] leading-[1.6]" style={{ color: C.ink2 }}>
        <span style={{ color: C.mint, fontWeight: 600 }}>First-time Top 10 pick for {pick.symbol}.</span>{' '}
        It hasn&apos;t appeared in our Falcon Top 10 before under the locked rules
        ({response.hold_days}-day hold, −7% stop), so there&apos;s no per-stock track record yet.
      </p>
    )
  }
  const wr = bt.win_rate_pct
  const fy = bt.first_trade_date?.slice(0, 4)
  const ly = bt.last_trade_date?.slice(0, 4)
  const span = fy && ly ? (fy === ly ? fy : `${fy}–${ly}`) : 'before'
  const stopped = bt.worst_loss_pct != null && bt.worst_loss_pct <= -6.5
  const thin = bt.n_trades < 5
  return (
    <>
      <p className="text-[13.5px] leading-[1.6] mb-3" style={{ color: C.ink2 }}>
        When <b style={{ color: C.ink, fontWeight: 550 }}>{pick.symbol}</b> has appeared in our
        Falcon Top 10 {span === 'before' ? 'before' : `(${span})`}, it worked{' '}
        <b style={{ color: C.ink, fontWeight: 550 }}>{bt.n_wins} of {bt.n_trades}</b>
        {wr != null && <> ({Math.round(wr)}% win rate)</>}.
        {thin && (
          <span style={{ color: 'rgba(230,180,80,0.8)' }}>
            {' '}Thin sample — only {bt.n_trades} historical trade{bt.n_trades === 1 ? '' : 's'} so far.
          </span>
        )}
      </p>
      <ul className="flex flex-col gap-2 m-0 p-0 list-none">
        {(bt.avg_win_pct != null || bt.avg_loss_pct != null) && (
          <Bullet>
            {bt.avg_win_pct != null && bt.avg_win_pct !== 0 && (
              <>When it worked → <span style={{ color: C.mint, fontWeight: 600 }}>+{bt.avg_win_pct.toFixed(1)}%</span> per trade. </>
            )}
            {bt.avg_loss_pct != null && bt.avg_loss_pct !== 0 && (
              <>When it didn&apos;t → <span style={{ color: C.red, fontWeight: 600 }}>{bt.avg_loss_pct.toFixed(1)}%</span> per trade.</>
            )}
            {(bt.avg_loss_pct == null || bt.avg_loss_pct === 0) && bt.n_losses === 0 && (
              <span style={{ color: 'rgba(63,227,164,0.7)' }}> No losing trades on record yet.</span>
            )}
          </Bullet>
        )}
        {bt.worst_loss_pct != null && (
          <Bullet>
            Worst single trade:{' '}
            <span style={{ color: stopped ? '#f0d089' : C.red, fontWeight: 600 }}>{bt.worst_loss_pct.toFixed(1)}%</span>
            {stopped && <span style={{ color: C.faint }}> — it hit the stop, exactly what the stop is there to do.</span>}
          </Bullet>
        )}
      </ul>
    </>
  )
}

// ── Stock analysis workspace (RIGHT) — for a searched non-Top-10 stock ──────
// Wires to PowerAPI.analyzeStock() (GET /api/power/ask/analyze-stock — LIVE).
// Renders each REAL field as its own section. A 404 NOT_COVERED → an honest
// "not covered by Falcon" message (never fabricated). Top-10 stocks never reach
// here (they render the REAL DetailCard from loaded data).
function StockAnalysis({
  symbol, name, sector, onBack,
}: {
  symbol: string; name: string | null; sector: string | null; onBack: () => void
}) {
  const [state, setState] = useState<'loading' | 'ok' | 'not_covered' | 'error'>('loading')
  const [a, setA] = useState<AnalyzeStockResponse | null>(null)

  useEffect(() => {
    let alive = true
    setState('loading'); setA(null)
    PowerAPI.analyzeStock(symbol)
      .then(res => { if (alive) { setA(res); setState('ok') } })
      .catch((e: unknown) => {
        if (!alive) return
        // 404 NOT_COVERED is an EXPECTED, honest outcome — not an error toast.
        if (e instanceof PowerAPIError && (e.status === 404 || e.code === 'NOT_COVERED')) {
          setState('not_covered'); return
        }
        setState('error')
      })
    return () => { alive = false }
  }, [symbol])

  const headSector = a?.sector ?? sector
  const headName = a?.name ?? name

  return (
    <div className="rounded-[18px] border p-6 mt-6 shadow-[0_24px_70px_-34px_rgba(0,0,0,0.55)]"
         style={{ background: `linear-gradient(180deg, ${C.card}, ${C.card2})`, borderColor: C.line2 }}>
      <div className="flex items-center gap-3 flex-wrap">
        <button type="button" onClick={onBack}
                className="text-[12px] inline-flex items-center gap-1 rounded-md px-2 py-1 border"
                style={{ borderColor: C.line2, color: C.muted }}>
          {ICON.back(13)} Top 10
        </button>
        <span className="text-[18px] font-semibold" style={{ color: C.ink }}>
          {symbol}
          {headSector && <span className="text-[13px] font-normal ml-2" style={{ color: C.faint }}>· {headSector}</span>}
        </span>
        {a?.tier && (
          <span className="ml-auto"><AnalysisTierBadge tier={a.tier} reason={a.tier_reason} /></span>
        )}
      </div>
      {headName && headName !== symbol && (
        <div className="text-[12.5px] mt-1" style={{ color: C.faint }}>{headName}</div>
      )}

      {state === 'loading' && (
        <div className="mt-4 inline-flex items-center gap-1.5 text-[11px] font-mono uppercase tracking-wider rounded-full px-2.5 py-1"
             style={{ color: C.mint, background: C.mintDim, border: `1px solid rgba(63,227,164,0.3)` }}>
          <span className="w-1.5 h-1.5 rounded-full animate-pulse" style={{ background: C.mint }} />
          Analyzing {symbol}
        </div>
      )}

      {state === 'not_covered' && (
        <p className="text-[13.5px] leading-[1.6] mt-4" style={{ color: C.ink2 }}>
          <b style={{ color: C.ink }}>{symbol} isn&apos;t covered by Falcon yet.</b>{' '}
          It falls outside the universe our engine currently mines and tracks, so we
          don&apos;t have an honest analysis to show — and we won&apos;t invent one. Try a
          name from today&apos;s Top 10 on the left for a full, live explanation.
        </p>
      )}

      {state === 'error' && (
        <p className="text-[13.5px] leading-[1.6] mt-4" style={{ color: C.ink2 }}>
          We couldn&apos;t load the analysis for <b style={{ color: C.ink }}>{symbol}</b> just
          now. This is a live-data view — please try again in a moment.
        </p>
      )}

      {state === 'ok' && a && (
        <div className="mt-4">
          {/* lead plain-English explanation */}
          <p className="text-[13.5px] leading-[1.6]" style={{ color: C.ink2 }}>{a.explanation}</p>
          {a.tier_reason && (
            <p className="text-[12px] mt-2" style={{ color: C.faint }}>{a.tier_reason}</p>
          )}

          <Rule />
          <AnalysisSection title="Current trend"               body={a.current_trend} />
          <AnalysisSection title="Recent price movement"       body={a.recent_price_movement} />
          <AnalysisSection title="Recent volume behavior"      body={a.recent_volume_behavior} />
          <AnalysisSection title="Signal-day movement"         body={a.signal_day_movement} />
          <AnalysisSection title="Sector performance"          body={a.sector_performance} />
          <AnalysisSection title="Falcon pattern observations" body={a.falcon_pattern_observations} />
          <AnalysisSection title="Entry context"               body={a.entry_context} />

          {a.risk_warnings.length > 0 && (
            <>
              <Rule />
              <Section title="Risk warnings">
                <ul className="flex flex-col gap-2 m-0 p-0 list-none">
                  {a.risk_warnings.map((w, i) => (
                    <li key={i} className="flex gap-2.5 text-[13px] leading-[1.5]" style={{ color: C.ink2 }}>
                      <span className="mt-[7px] w-[5px] h-[5px] rounded-full shrink-0" style={{ background: 'rgba(230,180,80,0.8)' }} />
                      <span>{w}</span>
                    </li>
                  ))}
                </ul>
              </Section>
            </>
          )}

          <p className="text-[11px] mt-4" style={{ color: C.faint }}>
            As of {a.as_of} · explanatory analysis, not financial advice.
          </p>
        </div>
      )}
    </div>
  )
}

// Render a single analysis section only when the backend returned prose for it.
function AnalysisSection({ title, body }: { title: string; body: string | null | undefined }) {
  if (!body || !body.trim()) return null
  return (
    <Section title={title}>
      <p className="text-[13.5px] leading-[1.6]" style={{ color: C.ink2 }}>{body}</p>
    </Section>
  )
}

// Tier chip for the analyze-stock card — reuses the canonical band colours.
function AnalysisTierBadge({ tier, reason }: { tier: string; reason: string | null }) {
  const band = tierBand(tier)
  const s = TIER_STYLE[BAND_COLORKEY[band ?? ''] ?? 'gray'] ?? TIER_STYLE.gray
  return (
    <span
      title={reason ?? undefined}
      className="inline-flex items-center rounded-md font-semibold uppercase tracking-[0.04em] text-[10px] px-2 py-0.5"
      style={{ color: s.color, background: s.bg, boxShadow: `inset 0 0 0 1px ${s.ring}` }}
    >
      {band ?? tier}
    </span>
  )
}

// ── Launch-Pending states (non-swing styles — NO faked data) ───────────────
function MiddleStylePending({ style }: { style: Style }) {
  return (
    <div className="px-4 py-8 text-center">
      <div className="grid place-items-center w-10 h-10 rounded-xl mx-auto mb-3"
           style={{ background: C.mintDim, color: C.mint }}>{ICON[style.icon](18)}</div>
      <div className="text-[13px] font-semibold mb-1" style={{ color: C.ink }}>{style.name}</div>
      <div className="text-[11.5px] leading-relaxed" style={{ color: C.faint }}>
        Launch-Pending. This style has its own engine — we won&apos;t show ranked
        picks until it&apos;s validated. Only Falcon Top 10 Swing is live today.
      </div>
    </div>
  )
}

function RightStylePending({ style }: { style: Style }) {
  return (
    <div className="rounded-[18px] border p-6 mt-6"
         style={{ background: `linear-gradient(180deg, ${C.card}, ${C.card2})`, borderColor: C.line2 }}>
      <span className="inline-flex items-center gap-1.5 text-[11px] font-mono uppercase tracking-wider rounded-full px-2.5 py-1 mb-3"
            style={{ color: C.mint, background: C.mintDim, border: `1px solid rgba(63,227,164,0.3)` }}>
        <span className="w-1.5 h-1.5 rounded-full animate-pulse" style={{ background: C.mint }} />
        Launch Pending
      </span>
      <h2 className="text-[20px] font-semibold mb-2" style={{ color: C.ink }}>{style.name}</h2>
      <p className="text-[13.5px] leading-[1.6]" style={{ color: C.ink2 }}>
        A {style.hold} engine for the {style.name.replace(' Trader', '').replace(' Investor', '')} style.
        It isn&apos;t live yet — building and validating a separate engine for this horizon is in progress.
        We don&apos;t re-rank the Swing picks and pretend they&apos;re a different strategy.
      </p>
      <p className="text-[12.5px] mt-3" style={{ color: C.faint }}>
        For now, switch back to <b style={{ color: C.ink }}>Falcon Top 10 Swing</b> — our validated, live engine.
      </p>
    </div>
  )
}

function MiddleEmpty() {
  return <div className="px-4 py-10 text-center text-[12px]" style={{ color: C.faint }}>No picks available right now.</div>
}
function MiddleLoading() {
  return <div className="px-4 py-10 text-center text-[12px]" style={{ color: C.faint }}>Loading picks…</div>
}
function MiddleFetchError() {
  return (
    <div className="px-4 py-10 text-center text-[12px]" style={{ color: C.faint }}>
      Couldn&apos;t reach the engine for those filters. Try another date or universe.
    </div>
  )
}

function RightEmpty({ hasData }: { hasData: boolean }) {
  return (
    <div className="rounded-[18px] border p-6 mt-6"
         style={{ background: `linear-gradient(180deg, ${C.card}, ${C.card2})`, borderColor: C.line2 }}>
      <h2 className="text-[16px] font-semibold mb-2" style={{ color: C.ink }}>
        {hasData ? 'No picks for the latest signal date' : 'Couldn’t reach the Falcon engine'}
      </h2>
      <p className="text-[13.5px] leading-[1.6]" style={{ color: C.ink2 }}>
        {hasData
          ? 'The latest run returned no Top 10 picks. Check back after the next end-of-day cycle.'
          : 'We couldn’t load today’s Top 10 from the engine. This is a live-data view — refresh in a moment.'}
      </p>
    </div>
  )
}

// ── Tiny layout primitives ─────────────────────────────────────────────────
function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="mt-3">
      <h3 className="text-[13px] font-semibold tracking-[-0.01em] mb-1" style={{ color: C.ink }}>{title}</h3>
      {children}
    </section>
  )
}
function Rule() { return <div className="h-px my-2.5" style={{ background: C.line }} /> }
function Bullet({ children }: { children: React.ReactNode }) {
  return (
    <li className="flex gap-2.5 text-[13px] leading-[1.5]" style={{ color: C.ink2 }}>
      <span className="mt-[7px] w-[5px] h-[5px] rounded-full shrink-0" style={{ background: C.faint }} />
      <span>{children}</span>
    </li>
  )
}

// ── Inline stroke icons (1.6 stroke, currentColor) ─────────────────────────
const ICON = {
  flame:   (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M12 3c1 3 4 4.5 4 8a4 4 0 0 1-8 0c0-1.5.6-2.5 1.2-3.3C9.8 9 11.5 8 12 3z" strokeLinejoin="round"/></svg>,
  clock:   (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 2" strokeLinecap="round"/></svg>,
  bolt:    (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M13 3L5 13h6l-1 8 8-10h-6l1-8z" strokeLinejoin="round"/></svg>,
  trend:   (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M4 15l5-5 3 3 7-7" strokeLinecap="round" strokeLinejoin="round"/><path d="M16 6h4v4" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  shield:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M12 3l7 3v5c0 4.5-3 8-7 10-4-2-7-5.5-7-10V6l7-3z" strokeLinejoin="round"/></svg>,
  chevron: (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M6 9l6 6 6-6" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  arrow:   (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2"><path d="M5 12h14M13 6l6 6-6 6" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  back:    (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M15 18l-6-6 6-6" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  search:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7"><circle cx="11" cy="11" r="7"/><path d="M20 20l-3.2-3.2" strokeLinecap="round"/></svg>,
  handshake:(n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M12 7l2-2a2 2 0 0 1 3 0l3 3-5 5-1.5-1.5" strokeLinecap="round" strokeLinejoin="round"/><path d="M12 7L9.5 4.5a2 2 0 0 0-3 0l-3 3 4 4" strokeLinecap="round" strokeLinejoin="round"/><path d="M8 13l2 2M10 11l2.5 2.5" strokeLinecap="round"/></svg>,
  bot:     (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><rect x="4" y="8" width="16" height="11" rx="2.5"/><path d="M12 8V4.5M9 13h.01M15 13h.01" strokeLinecap="round"/><circle cx="12" cy="4" r="1"/></svg>,
}
