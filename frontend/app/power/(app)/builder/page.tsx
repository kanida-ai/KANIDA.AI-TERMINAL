'use client'

/**
 * /power/builder — Build an Agent.
 * Compose any strategy from indicators + conditions + exit, see the live TOKEN cost (compute-metered,
 * like Claude), run a real leak-free backtest on the whole universe, and see the evidence card + the
 * 5 Market Worlds. Wallet is charged by real compute. Matches the terminal-ui theme.
 *
 * Drop into: frontend/app/power/builder/page.tsx  (uses lib/builder-api.ts + lib/terminal-ui + lib/theme)
 * Auth: uses a demo user id via localStorage for the standalone backend; swap for power-auth JWT in prod.
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import { T } from '@/lib/theme'
import { pageShellStyle, panelStyle, chipStyle, SectionEyebrow, MetricCard } from '@/lib/terminal-ui'
import * as B from '@/lib/builder-api'

const WORLDS = ['Normal', 'High-Vol', 'Bear', 'Low-Liquidity', 'Unseen 18-19']
const fe = (e?: number | null) => (e == null ? '—' : (e >= 0 ? '+' : '') + e.toFixed(2) + '%')
const wbg = (e?: number | null) => {
  if (e == null) return 'transparent'
  const a = Math.min(Math.abs(e) / 1.2, 1) * 0.28 + 0.06
  return e >= 0 ? `rgba(0,201,138,${a})` : `rgba(255,77,109,${a})`
}
function useUser() {
  const [u, setU] = useState('demo-user')
  useEffect(() => { try { const j = localStorage.getItem('kanida_power_jwt'); if (j) setU(j) } catch {} }, [])
  return u
}

export default function BuilderPage() {
  const user = useUser()
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

  const strat: B.Strategy = useMemo(() => ({
    name, direction, entry: { logic, conditions: conds }, exit, cost_bps: 30, granularity: gran,
  }), [name, direction, logic, conds, exit, gran])

  useEffect(() => { B.fetchCatalog().then(setCat).catch(() => setErr('backend offline — start the agent_builder service')) }, [])
  useEffect(() => { B.getWallet(user).then(w => setBalance(w.balance)).catch(() => {}) }, [user])
  useEffect(() => { const id = setTimeout(() => B.quote(strat).then(q => setTokens(q.tokens)).catch(() => {}), 250); return () => clearTimeout(id) }, [strat])

  const addCond = () => setConds([...conds, { indicator: cat?.indicators[0]?.name || 'rsi', params: {}, op: '>', value: 0 }])
  const setCond = (i: number, patch: Partial<B.Condition>) => setConds(conds.map((c, j) => j === i ? { ...c, ...patch } : c))
  const rmCond = (i: number) => setConds(conds.filter((_, j) => j !== i))

  const run = useCallback(async () => {
    setBusy(true); setErr(null); setResult(null)
    try { const r = await B.runBacktest(strat, user); setResult(r); setBalance(r.wallet_balance) }
    catch (e: any) { setErr(e.message || 'backtest failed') } finally { setBusy(false) }
  }, [strat, user])

  const meta = (nm: string) => cat?.indicators.find(i => i.name === nm)
  const box: React.CSSProperties = { background: T.s2, border: `1px solid ${T.b}`, borderRadius: 10, color: T.t, padding: '8px 10px', fontSize: 13, fontFamily: 'inherit' }
  const ov = result?.overall

  return (
    <div style={{ ...pageShellStyle(), padding: 24 }}>
      <div style={{ maxWidth: 1180, margin: '0 auto' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 12 }}>
          <div>
            <SectionEyebrow>KANIDA · Build an Agent</SectionEyebrow>
            <h1 style={{ fontSize: 26, fontWeight: 900, margin: '0 0 4px' }}>Compose a strategy. Backtest it instantly.</h1>
            <p style={{ color: T.t2, fontSize: 14, margin: 0, maxWidth: 640 }}>
              Any indicators, any conditions, any exit. We backtest it leak-free across 13 years and 5 market worlds.
              Tokens are metered by real compute — like Claude. {cat && <>Universe: {cat.universe.stocks} stocks × {cat.universe.bars} bars.</>}
            </p>
          </div>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <span style={chipStyle('green')}>Wallet: {balance.toLocaleString()} tokens</span>
            <button onClick={() => B.topup(user, 5000).then(w => setBalance(w.balance))}
              style={{ ...chipStyle('neutral'), cursor: 'pointer' }}>+ Top up 5,000</button>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0,1fr) minmax(0,1.1fr)', gap: 16, marginTop: 18, alignItems: 'start' }}>
          {/* ---------- COMPOSER ---------- */}
          <div style={panelStyle(18)}>
            <div style={{ display: 'flex', gap: 10, marginBottom: 14 }}>
              <input value={name} onChange={e => setName(e.target.value)} style={{ ...box, flex: 1 }} />
              {(['long', 'short'] as const).map(d => (
                <button key={d} onClick={() => setDirection(d)} style={{ ...chipStyle(d === direction ? (d === 'long' ? 'green' : 'red') : 'neutral'), cursor: 'pointer' }}>{d}</button>
              ))}
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
              <span style={{ color: T.t2, fontSize: 12, fontWeight: 700 }}>WHEN</span>
              {(['AND', 'OR'] as const).map(lg => (
                <button key={lg} onClick={() => setLogic(lg)} style={{ ...chipStyle(lg === logic ? 'amber' : 'neutral', true), cursor: 'pointer' }}>{lg}</button>
              ))}
              <span style={{ color: T.t2, fontSize: 12 }}>of these hold at the close:</span>
            </div>

            {conds.map((c, i) => {
              const m = meta(c.indicator); const pkeys = Object.keys(m?.defaults || {})
              return (
                <div key={i} style={{ display: 'flex', gap: 6, marginBottom: 8, alignItems: 'center', flexWrap: 'wrap' }}>
                  <select value={c.indicator} onChange={e => setCond(i, { indicator: e.target.value, params: meta(e.target.value)?.defaults || {} })} style={{ ...box, flex: '1 1 150px' }}>
                    {cat?.indicators.map(ind => <option key={ind.name} value={ind.name}>{ind.label}</option>)}
                  </select>
                  {pkeys.map(k => (
                    <input key={k} type="number" title={k} value={c.params?.[k] ?? m?.defaults[k]} onChange={e => setCond(i, { params: { ...c.params, [k]: Number(e.target.value) } })} style={{ ...box, width: 64 }} />
                  ))}
                  <select value={c.op} onChange={e => setCond(i, { op: e.target.value as any })} style={{ ...box, width: 56 }}>
                    {cat?.ops.map(o => <option key={o} value={o}>{o}</option>)}
                  </select>
                  <input type="number" value={c.value} onChange={e => setCond(i, { value: Number(e.target.value) })} style={{ ...box, width: 74 }} />
                  <button onClick={() => rmCond(i)} style={{ ...box, cursor: 'pointer', color: T.r, width: 34 }}>×</button>
                </div>
              )
            })}
            <button onClick={addCond} style={{ ...chipStyle('neutral', true), cursor: 'pointer', marginTop: 2 }}>+ Add condition</button>

            <div style={{ borderTop: `1px solid ${T.b}`, marginTop: 14, paddingTop: 12 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                <span style={{ color: T.t2, fontSize: 12, fontWeight: 700 }}>EXIT</span>
                <select value={exit.type} onChange={e => { const t = e.target.value as any; setExit(t === 'horizon' ? { type: t, days: 5 } : t === 'target_stop' ? { type: t, target: 10, stop: 5, max_days: 20 } : { type: t, pct: 15, max_days: 60 }) }} style={{ ...box }}>
                  {cat?.exits.map(x => <option key={x} value={x}>{x}</option>)}
                </select>
                {exit.type === 'horizon' && <label style={{ color: T.t2, fontSize: 12 }}>hold <input type="number" value={exit.days} onChange={e => setExit({ ...exit, days: Number(e.target.value) })} style={{ ...box, width: 56 }} /> days</label>}
                {exit.type === 'target_stop' && <>
                  <label style={{ color: T.t2, fontSize: 12 }}>target <input type="number" value={exit.target} onChange={e => setExit({ ...exit, target: Number(e.target.value) })} style={{ ...box, width: 56 }} />%</label>
                  <label style={{ color: T.t2, fontSize: 12 }}>stop <input type="number" value={exit.stop} onChange={e => setExit({ ...exit, stop: Number(e.target.value) })} style={{ ...box, width: 56 }} />%</label></>}
                {exit.type === 'trail' && <label style={{ color: T.t2, fontSize: 12 }}>trail <input type="number" value={exit.pct} onChange={e => setExit({ ...exit, pct: Number(e.target.value) })} style={{ ...box, width: 56 }} />%</label>}
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 12 }}>
                <span style={{ color: T.t2, fontSize: 12, fontWeight: 700 }}>DATA</span>
                {(['daily', '1min'] as const).map(g => <button key={g} onClick={() => setGran(g)} style={{ ...chipStyle(g === gran ? 'green' : 'neutral', true), cursor: 'pointer' }}>{g}</button>)}
              </div>
            </div>

            <div style={{ borderTop: `1px solid ${T.b}`, marginTop: 14, paddingTop: 14, display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10 }}>
              <div style={{ fontSize: 13, color: T.t2 }}>
                Cost: <b style={{ color: T.a, fontFamily: T.mono }}>{tokens ? tokens.total.toLocaleString() : '…'} tokens</b>
                {tokens && <span style={{ fontSize: 11 }}> &nbsp;(input {tokens.input} + worlds {tokens.market_worlds} + output {tokens.output})</span>}
              </div>
              <button onClick={run} disabled={busy || !tokens || (tokens.total > balance)}
                style={{ ...chipStyle('green'), cursor: busy ? 'wait' : 'pointer', fontSize: 14, padding: '11px 18px', opacity: (!tokens || tokens.total > balance) ? 0.5 : 1 }}>
                {busy ? 'Backtesting…' : tokens && tokens.total > balance ? 'Insufficient tokens' : `Run backtest → spend ${tokens?.total ?? ''}`}
              </button>
            </div>
            {err && <div style={{ color: T.r, fontSize: 13, marginTop: 10 }}>{err}</div>}
          </div>

          {/* ---------- RESULTS ---------- */}
          <div style={panelStyle(18)}>
            {!result && <div style={{ color: T.t2, fontSize: 14, textAlign: 'center', padding: '60px 20px' }}>
              Compose a strategy on the left and hit <b style={{ color: T.g }}>Run backtest</b>.<br />You'll see the evidence card and how it holds up across 5 market worlds.
            </div>}
            {result && ov && <>
              <SectionEyebrow>{result.strategy.name} · evidence</SectionEyebrow>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(120px,1fr))', gap: 10, marginBottom: 16 }}>
                <MetricCard label="Trades" value={ov.n.toLocaleString()} note="unique signals" />
                <MetricCard label="Win rate" value={ov.win + '%'} note="net of 0.30% cost" />
                <MetricCard label="Per trade" value={fe(ov.expct)} note="average net return" color={ov.expct >= 0 ? T.g : T.r} />
                <MetricCard label="Edge vs market" value={fe(ov.edge)} note="beat the universe by" color={(ov.edge ?? 0) >= 0 ? T.g : T.r} />
                <MetricCard label="Profit factor" value={ov.pf == null ? '∞' : String(ov.pf)} note="gross win ÷ loss" />
              </div>
              <SectionEyebrow>Market Worlds</SectionEyebrow>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5,1fr)', gap: 6, marginBottom: 8 }}>
                {WORLDS.map(w => {
                  const cw = result.market_worlds[w]; const e = cw?.expct
                  return <div key={w} style={{ background: wbg(e), border: `1px solid ${T.b}`, borderRadius: 10, padding: '10px 8px', textAlign: 'center' }}>
                    <div style={{ fontSize: 10.5, color: T.t2, marginBottom: 4 }}>{w}</div>
                    <div style={{ fontFamily: T.mono, fontWeight: 800, color: e == null ? T.t2 : e >= 0 ? T.g : T.r }}>{fe(e)}</div>
                  </div>
                })}
              </div>
              <div style={{ color: T.t2, fontSize: 12, marginTop: 10 }}>
                Charged <b style={{ color: T.a }}>{result.tokens_charged}</b> tokens · balance now <b style={{ color: T.g }}>{result.wallet_balance.toLocaleString()}</b>.
                Every number is leak-free (signal at close → enter next open), net of costs.
              </div>
            </>}
          </div>
        </div>
      </div>
    </div>
  )
}
