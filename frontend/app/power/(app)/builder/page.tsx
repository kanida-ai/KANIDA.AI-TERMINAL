'use client'

/**
 * /power/builder — Build an Agent.
 * Compose any strategy from indicators + conditions + exit, read it back in plain English, see the live
 * TOKEN cost (compute-metered, like Claude), run a real leak-free backtest across 13 years + 5 Market
 * Worlds, and read the evidence card. Wallet is charged by real compute. Matches the terminal-ui theme.
 *
 * v0.2 (falcon session): UX cleanup pass — grouped sections + hierarchy, a readable carded condition
 * builder, a live plain-English strategy sentence, starter presets, a clearer cost breakdown, and a
 * polished results panel with an honest verdict + Market-Worlds legend. SAME backend contract as v0.
 * Auth: demo user id via localStorage for the standalone backend; swap for power-auth JWT in prod.
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import { T } from '@/lib/theme'
import { pageShellStyle, panelStyle, chipStyle, SectionEyebrow, MetricCard, useBreakpoint } from '@/lib/terminal-ui'
import * as B from '@/lib/builder-api'

const WORLDS = ['Normal', 'High-Vol', 'Bear', 'Low-Liquidity', 'Unseen 18-19']
const WORLD_HINT: Record<string, string> = {
  'Normal': 'Uptrend, calm vol',
  'High-Vol': 'Top-quartile volatility',
  'Bear': 'Index below its 200-day',
  'Low-Liquidity': 'Thin-volume days',
  'Unseen 18-19': 'Held-out 2018–19',
}
const fe = (e?: number | null) => (e == null ? '—' : (e >= 0 ? '+' : '') + e.toFixed(2) + '%')
const wbg = (e?: number | null) => {
  if (e == null) return 'transparent'
  const a = Math.min(Math.abs(e) / 1.2, 1) * 0.28 + 0.06
  return e >= 0 ? `rgba(0,201,138,${a})` : `rgba(255,77,109,${a})`
}

// Starter strategies — honest starting points (not recommendations), all built from the live catalog.
type Preset = { key: string; label: string; blurb: string; s: Partial<B.Strategy> & { conditions: B.Condition[] } }
const PRESETS: Preset[] = [
  { key: 'oversold', label: 'Oversold bounce', blurb: 'Buy fear, exit in a week',
    s: { name: 'Oversold bounce', direction: 'long', conditions: [{ indicator: 'rsi', params: { period: 14 }, op: '<', value: 30 }] } },
  { key: 'breakout', label: 'Momentum breakout', blurb: 'Near 60-day high on volume',
    s: { name: 'Momentum breakout', direction: 'long', conditions: [
      { indicator: 'nd_high_dist', params: { period: 60 }, op: '>=', value: -1 },
      { indicator: 'vol_ratio', params: { period: 20 }, op: '>', value: 1.5 }] } },
  { key: 'gapfade', label: 'Gap fade', blurb: 'Short the exhaustion gap',
    s: { name: 'Gap fade', direction: 'short', conditions: [{ indicator: 'gap', params: {}, op: '>', value: 3 }] } },
  { key: 'pullback', label: 'Trend pullback', blurb: 'Dip inside an uptrend',
    s: { name: 'Trend pullback', direction: 'long', conditions: [
      { indicator: 'close_vs_sma', params: { period: 200 }, op: '>', value: 0 },
      { indicator: 'rsi', params: { period: 14 }, op: '<', value: 40 }] } },
]

function useUser() {
  const [u, setU] = useState('demo-user')
  useEffect(() => { try { const j = localStorage.getItem('kanida_power_jwt'); if (j) setU(j) } catch {} }, [])
  return u
}

export default function BuilderPage() {
  const user = useUser()
  const { isDesktop } = useBreakpoint()
  const [cat, setCat] = useState<B.Catalog | null>(null)
  const [name, setName] = useState('My Agent')
  const [direction, setDirection] = useState<'long' | 'short'>('long')
  const [logic, setLogic] = useState<'AND' | 'OR'>('AND')
  const [conds, setConds] = useState<B.Condition[]>([{ indicator: 'rsi', params: { period: 14 }, op: '<', value: 30 }])
  const [exit, setExit] = useState<B.ExitRule>({ type: 'horizon', days: 5 })
  const [gran, setGran] = useState<'daily' | '1min'>('daily')
  const [tokens, setTokens] = useState<B.TokenCost | null>(null)
  const [balance, setBalance] = useState<number>(0)
  const [result, setResult] = useState<B.BacktestResult | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [offline, setOffline] = useState(false)

  const strat: B.Strategy = useMemo(() => ({
    name, direction, entry: { logic, conditions: conds }, exit, cost_bps: 30, granularity: gran,
  }), [name, direction, logic, conds, exit, gran])

  useEffect(() => { B.fetchCatalog().then(c => { setCat(c); setOffline(false) }).catch(() => setOffline(true)) }, [])
  useEffect(() => { B.getWallet(user).then(w => setBalance(w.balance)).catch(() => {}) }, [user])
  useEffect(() => { const id = setTimeout(() => B.quote(strat).then(q => setTokens(q.tokens)).catch(() => {}), 250); return () => clearTimeout(id) }, [strat])

  const meta = useCallback((nm: string) => cat?.indicators.find(i => i.name === nm), [cat])
  const addCond = () => setConds([...conds, { indicator: cat?.indicators[0]?.name || 'rsi', params: meta(cat?.indicators[0]?.name || 'rsi')?.defaults || {}, op: '>', value: 0 }])
  const setCond = (i: number, patch: Partial<B.Condition>) => setConds(conds.map((c, j) => j === i ? { ...c, ...patch } : c))
  const rmCond = (i: number) => setConds(conds.filter((_, j) => j !== i))

  const applyPreset = (p: Preset) => {
    setName(p.s.name || 'My Agent'); setDirection(p.s.direction || 'long'); setLogic('AND')
    setConds(p.s.conditions); setExit(p.key === 'gapfade' ? { type: 'horizon', days: 1 } : p.key === 'breakout' ? { type: 'trail', pct: 8, max_days: 40 } : { type: 'horizon', days: p.key === 'pullback' ? 10 : 5 })
    setResult(null); setErr(null)
  }

  const run = useCallback(async () => {
    setBusy(true); setErr(null); setResult(null)
    try { const r = await B.runBacktest(strat, user); setResult(r); setBalance(r.wallet_balance) }
    catch (e: any) { setErr(e.message || 'backtest failed') } finally { setBusy(false) }
  }, [strat, user])

  // Plain-English read-back of the composed strategy (the canonical hypothesis, in a sentence).
  const humanCond = useCallback((c: B.Condition) => {
    const m = meta(c.indicator); const label = m?.label || c.indicator
    const ps = Object.values(c.params || {}); const pstr = ps.length ? `(${ps.join(', ')})` : ''
    return `${label}${pstr} ${c.op} ${c.value}`
  }, [meta])
  const exitText = exit.type === 'horizon' ? `exit after ${exit.days} trading days`
    : exit.type === 'target_stop' ? `exit at +${exit.target}% target or −${exit.stop}% stop (max ${exit.max_days ?? 20}d)`
    : `exit on a ${exit.pct}% trailing stop (max ${exit.max_days ?? 40}d)`
  const sentence = `Enter ${direction} when ${logic === 'AND' ? 'ALL' : 'ANY'} of [ ${conds.map(humanCond).join(logic === 'AND' ? '  ·  ' : '  or  ')} ] hold at the close — then ${exitText}.`

  const ov = result?.overall
  const verdict = ov ? ((ov.expct > 0 && (ov.edge ?? 0) > 0) ? { t: 'Positive net edge', tone: T.g }
    : ov.expct > 0 ? { t: 'Profitable, but no edge vs the market', tone: T.a }
    : { t: 'No edge — loses after costs', tone: T.r }) : null

  const box: React.CSSProperties = { background: T.s2, border: `1px solid ${T.b}`, borderRadius: 10, color: T.t, padding: '9px 10px', fontSize: 13, fontFamily: 'inherit', outlineColor: T.g }
  const fieldLabel: React.CSSProperties = { fontSize: 10, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', fontWeight: 700, marginBottom: 4, display: 'block' }
  const canRun = !!tokens && !offline && tokens.total <= balance

  return (
    <div style={{ ...pageShellStyle(), padding: isDesktop ? 28 : 18 }}>
      <div style={{ maxWidth: 1200, margin: '0 auto' }}>
        {/* ---------- HEADER ---------- */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 14 }}>
          <div>
            <SectionEyebrow>KANIDA · Build an Agent</SectionEyebrow>
            <h1 style={{ fontSize: isDesktop ? 30 : 24, fontWeight: 900, margin: '0 0 6px', letterSpacing: '-0.01em' }}>Compose a strategy. Backtest it instantly.</h1>
            <p style={{ color: T.t2, fontSize: 14.5, margin: 0, maxWidth: 660, lineHeight: 1.55 }}>
              Any indicators, any conditions, any exit. We backtest it leak-free across 13 years and 5 market worlds.
              Tokens are metered by real compute — like Claude.
              {cat && <> &nbsp;<b style={{ color: T.t }}>{cat.universe.stocks.toLocaleString()}</b> stocks × <b style={{ color: T.t }}>{cat.universe.bars.toLocaleString()}</b> bars.</>}
            </p>
          </div>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <span style={chipStyle('green')}>Wallet · {balance.toLocaleString()} tokens</span>
            <button onClick={() => B.topup(user, 5000).then(w => setBalance(w.balance)).catch(() => {})}
              style={{ ...chipStyle('neutral'), cursor: 'pointer' }}>+ Top up 5,000</button>
          </div>
        </div>

        {offline && (
          <div style={{ marginTop: 16, ...panelStyle(16), border: `1px solid ${T.r}`, background: 'rgba(255,77,109,0.06)', color: T.t2, fontSize: 13.5 }}>
            <b style={{ color: T.r }}>Backend offline.</b> Start the Agent Builder service on <code style={{ fontFamily: T.mono, color: T.a }}>:8001</code> (or set <code style={{ fontFamily: T.mono, color: T.a }}>BACKEND_ORIGIN</code>). The composer still works — you just can't price or run a backtest yet.
          </div>
        )}

        {/* ---------- PRESETS ---------- */}
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginTop: 18, flexWrap: 'wrap' }}>
          <span style={{ ...fieldLabel, margin: 0 }}>Start from</span>
          {PRESETS.map(p => (
            <button key={p.key} onClick={() => applyPreset(p)} title={p.blurb}
              style={{ ...chipStyle('neutral', true), cursor: 'pointer' }}>{p.label}</button>
          ))}
          <span style={{ color: T.t3, fontSize: 11.5 }}>— starting points, not recommendations</span>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: isDesktop ? 'minmax(0,1fr) minmax(0,1.05fr)' : '1fr', gap: 16, marginTop: 14, alignItems: 'start' }}>
          {/* ---------- COMPOSER ---------- */}
          <div style={panelStyle(20)}>
            {/* Identity */}
            <div style={{ display: 'flex', gap: 10, marginBottom: 18, alignItems: 'flex-end', flexWrap: 'wrap' }}>
              <div style={{ flex: '1 1 200px' }}>
                <label style={fieldLabel}>Agent name</label>
                <input value={name} onChange={e => setName(e.target.value)} style={{ ...box, width: '100%' }} />
              </div>
              <div>
                <label style={fieldLabel}>Direction</label>
                <div style={{ display: 'flex', gap: 6 }}>
                  {(['long', 'short'] as const).map(d => (
                    <button key={d} onClick={() => setDirection(d)}
                      style={{ ...chipStyle(d === direction ? (d === 'long' ? 'green' : 'red') : 'neutral'), cursor: 'pointer', textTransform: 'capitalize' }}>{d}</button>
                  ))}
                </div>
              </div>
            </div>

            {/* Entry */}
            <SectionEyebrow>Entry rules</SectionEyebrow>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10, flexWrap: 'wrap' }}>
              <span style={{ color: T.t2, fontSize: 12.5, fontWeight: 700 }}>WHEN</span>
              {(['AND', 'OR'] as const).map(lg => (
                <button key={lg} onClick={() => setLogic(lg)} style={{ ...chipStyle(lg === logic ? 'amber' : 'neutral', true), cursor: 'pointer' }}>{lg}</button>
              ))}
              <span style={{ color: T.t2, fontSize: 12.5 }}>of these hold at the close:</span>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {conds.map((c, i) => {
                const m = meta(c.indicator); const pkeys = Object.keys(m?.defaults || {})
                return (
                  <div key={i} style={{ background: T.s1, border: `1px solid ${T.b}`, borderRadius: 12, padding: '10px 10px 8px' }}>
                    <div style={{ display: 'flex', gap: 6, alignItems: 'flex-end', flexWrap: 'wrap' }}>
                      <div style={{ flex: '1 1 160px' }}>
                        <label style={fieldLabel}>Indicator</label>
                        <select value={c.indicator} onChange={e => setCond(i, { indicator: e.target.value, params: meta(e.target.value)?.defaults || {} })} style={{ ...box, width: '100%' }}>
                          {cat?.indicators.map(ind => <option key={ind.name} value={ind.name}>{ind.label}</option>)}
                        </select>
                      </div>
                      {pkeys.map(k => (
                        <div key={k}>
                          <label style={fieldLabel}>{k}</label>
                          <input type="number" title={k} value={c.params?.[k] ?? m?.defaults[k]} onChange={e => setCond(i, { params: { ...c.params, [k]: Number(e.target.value) } })} style={{ ...box, width: 66 }} />
                        </div>
                      ))}
                      <div>
                        <label style={fieldLabel}>is</label>
                        <select value={c.op} onChange={e => setCond(i, { op: e.target.value as any })} style={{ ...box, width: 58 }}>
                          {cat?.ops.map(o => <option key={o} value={o}>{o}</option>)}
                        </select>
                      </div>
                      <div>
                        <label style={fieldLabel}>value</label>
                        <input type="number" value={c.value} onChange={e => setCond(i, { value: Number(e.target.value) })} style={{ ...box, width: 78 }} />
                      </div>
                      <button onClick={() => rmCond(i)} disabled={conds.length === 1} title={conds.length === 1 ? 'Keep at least one condition' : 'Remove'}
                        style={{ ...box, cursor: conds.length === 1 ? 'not-allowed' : 'pointer', color: T.r, width: 36, opacity: conds.length === 1 ? 0.35 : 1 }}>×</button>
                    </div>
                  </div>
                )
              })}
            </div>
            <button onClick={addCond} style={{ ...chipStyle('neutral', true), cursor: 'pointer', marginTop: 10 }}>+ Add condition</button>

            {/* Exit */}
            <div style={{ borderTop: `1px solid ${T.b}`, marginTop: 18, paddingTop: 16 }}>
              <SectionEyebrow>Exit</SectionEyebrow>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
                <select value={exit.type} onChange={e => { const t = e.target.value as any; setExit(t === 'horizon' ? { type: t, days: 5 } : t === 'target_stop' ? { type: t, target: 10, stop: 5, max_days: 20 } : { type: t, pct: 8, max_days: 40 }) }} style={{ ...box }}>
                  {cat?.exits.map(x => <option key={x} value={x}>{x === 'horizon' ? 'Hold N days' : x === 'target_stop' ? 'Target / Stop' : 'Trailing stop'}</option>)}
                </select>
                {exit.type === 'horizon' && <label style={{ color: T.t2, fontSize: 12.5 }}>hold <input type="number" value={exit.days} onChange={e => setExit({ ...exit, days: Number(e.target.value) })} style={{ ...box, width: 60 }} /> days</label>}
                {exit.type === 'target_stop' && <>
                  <label style={{ color: T.t2, fontSize: 12.5 }}>target <input type="number" value={exit.target} onChange={e => setExit({ ...exit, target: Number(e.target.value) })} style={{ ...box, width: 60 }} />%</label>
                  <label style={{ color: T.t2, fontSize: 12.5 }}>stop <input type="number" value={exit.stop} onChange={e => setExit({ ...exit, stop: Number(e.target.value) })} style={{ ...box, width: 60 }} />%</label>
                  <label style={{ color: T.t2, fontSize: 12.5 }}>max <input type="number" value={exit.max_days} onChange={e => setExit({ ...exit, max_days: Number(e.target.value) })} style={{ ...box, width: 60 }} />d</label></>}
                {exit.type === 'trail' && <>
                  <label style={{ color: T.t2, fontSize: 12.5 }}>trail <input type="number" value={exit.pct} onChange={e => setExit({ ...exit, pct: Number(e.target.value) })} style={{ ...box, width: 60 }} />%</label>
                  <label style={{ color: T.t2, fontSize: 12.5 }}>max <input type="number" value={exit.max_days} onChange={e => setExit({ ...exit, max_days: Number(e.target.value) })} style={{ ...box, width: 60 }} />d</label></>}
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginTop: 14 }}>
                <span style={{ ...fieldLabel, margin: 0 }}>Data</span>
                {(['daily', '1min'] as const).map(g => <button key={g} onClick={() => setGran(g)} style={{ ...chipStyle(g === gran ? 'green' : 'neutral', true), cursor: 'pointer' }}>{g}</button>)}
                {gran === '1min' && <span style={{ color: T.t3, fontSize: 11.5 }}>~375× the compute of daily</span>}
              </div>
            </div>

            {/* Plain-English read-back */}
            <div style={{ marginTop: 16, background: T.gd, border: `1px solid ${T.gb}`, borderRadius: 12, padding: '11px 13px' }}>
              <div style={{ ...fieldLabel, color: T.g }}>Your agent, in plain English</div>
              <div style={{ color: T.t, fontSize: 13.5, lineHeight: 1.6 }}>{sentence}</div>
            </div>

            {/* Cost + Run */}
            <div style={{ borderTop: `1px solid ${T.b}`, marginTop: 16, paddingTop: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
              <div style={{ fontSize: 13, color: T.t2 }}>
                Cost: <b style={{ color: T.a, fontFamily: T.mono, fontSize: 15 }}>{tokens ? tokens.total.toLocaleString() : '…'}</b> tokens
                {tokens && <div style={{ fontSize: 11, color: T.t3, marginTop: 2 }}>input {tokens.input.toLocaleString()} + worlds {tokens.market_worlds.toLocaleString()} + output {tokens.output} · metered by compute</div>}
              </div>
              <button onClick={run} disabled={busy || !canRun}
                style={{ ...chipStyle('green'), cursor: busy ? 'wait' : canRun ? 'pointer' : 'not-allowed', fontSize: 14, padding: '12px 20px', opacity: canRun || busy ? 1 : 0.5 }}>
                {busy ? 'Backtesting…' : offline ? 'Backend offline' : tokens && tokens.total > balance ? 'Insufficient tokens' : `Run backtest → spend ${tokens?.total?.toLocaleString() ?? ''}`}
              </button>
            </div>
            {err && <div style={{ color: T.r, fontSize: 13, marginTop: 10 }}>{err}</div>}
          </div>

          {/* ---------- RESULTS ---------- */}
          <div style={panelStyle(20)}>
            {!result && !busy && (
              <div style={{ color: T.t2, fontSize: 14, textAlign: 'center', padding: '72px 20px' }}>
                <div style={{ fontSize: 34, marginBottom: 12, opacity: 0.5 }}>◇</div>
                Compose a strategy on the left and hit <b style={{ color: T.g }}>Run backtest</b>.
                <br />You'll see the evidence card and how it holds up across 5 market worlds.
              </div>
            )}
            {busy && (
              <div style={{ color: T.t2, fontSize: 14, textAlign: 'center', padding: '72px 20px' }}>
                <div style={{ fontSize: 15, color: T.g, fontWeight: 700, marginBottom: 8 }}>Backtesting across 13 years…</div>
                Running {name} leak-free over {cat?.universe.stocks.toLocaleString()} stocks and 5 market worlds.
              </div>
            )}
            {result && ov && <>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10, flexWrap: 'wrap', marginBottom: 12 }}>
                <SectionEyebrow>{result.strategy.name} · evidence</SectionEyebrow>
                {verdict && <span style={{ ...chipStyle('neutral', true), color: verdict.tone, borderColor: verdict.tone + '55' }}>{verdict.t}</span>}
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(112px,1fr))', gap: 10, marginBottom: 18 }}>
                <MetricCard label="Trades" value={ov.n.toLocaleString()} note="unique signals" />
                <MetricCard label="Win rate" value={ov.win + '%'} note="net of 0.30% cost" />
                <MetricCard label="Per trade" value={fe(ov.expct)} note="average net return" color={ov.expct >= 0 ? T.g : T.r} />
                <MetricCard label="Edge vs market" value={fe(ov.edge)} note="beat the universe by" color={(ov.edge ?? 0) >= 0 ? T.g : T.r} />
                <MetricCard label="Profit factor" value={ov.pf == null ? '∞' : String(ov.pf)} note="gross win ÷ loss" />
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
                <SectionEyebrow>Market Worlds</SectionEyebrow>
                <span style={{ color: T.t3, fontSize: 11 }}>net return / trade per regime</span>
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5,1fr)', gap: 6 }}>
                {WORLDS.map(w => {
                  const cw = result.market_worlds[w]; const e = cw?.expct
                  return <div key={w} title={`${WORLD_HINT[w]}${cw ? ` · ${cw.n.toLocaleString()} trades` : ' · too few trades'}`}
                    style={{ background: wbg(e), border: `1px solid ${T.b}`, borderRadius: 10, padding: '10px 6px', textAlign: 'center' }}>
                    <div style={{ fontSize: 10, color: T.t2, marginBottom: 4, lineHeight: 1.2 }}>{w}</div>
                    <div style={{ fontFamily: T.mono, fontWeight: 800, fontSize: 14, color: e == null ? T.t3 : e >= 0 ? T.g : T.r }}>{fe(e)}</div>
                  </div>
                })}
              </div>
              <div style={{ display: 'flex', gap: 14, marginTop: 10, color: T.t3, fontSize: 11 }}>
                <span><span style={{ color: T.g }}>■</span> positive expectancy</span>
                <span><span style={{ color: T.r }}>■</span> negative</span>
                <span>hover a tile for regime + sample size</span>
              </div>
              <div style={{ color: T.t2, fontSize: 12.5, marginTop: 14, borderTop: `1px solid ${T.b}`, paddingTop: 12, lineHeight: 1.6 }}>
                Charged <b style={{ color: T.a }}>{result.tokens_charged.toLocaleString()}</b> tokens · balance now <b style={{ color: T.g }}>{result.wallet_balance.toLocaleString()}</b>.
                Every number is leak-free (signal at close → enter next open), net of costs. Past performance is not a guarantee.
              </div>
            </>}
          </div>
        </div>
      </div>
    </div>
  )
}
