'use client'

/**
 * /power/agents — the Agent Platform surface (Chart Agent #1, live).
 *
 * Renders the REAL Chart Agent output in an Agent → Story → Evidence 3-column view:
 *   LEFT   the agent + its pattern library (Horizontal Trendline = LIVE; Triangle/Channel = soon).
 *   MIDDLE a date + stock selector and the honest storyline (shows the WATCH and its reason).
 *   RIGHT  the evidence panel — strategy-replay stats + pattern-forward numbers + decision & basis.
 *
 * HONESTY: at the current sample sizes the decision is almost always WATCH — we never invent a
 * TRADE. G4 nested-coherence and Triangle/Channel are labelled SPEC. Every number comes straight
 * from the guarded read-only backend (/api/agents/chart/*); point-in-time as-of the chosen date.
 *
 * Matches the terminal-ui theme (lib/theme T tokens, lib/terminal-ui panels/chips).
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import { T } from '@/lib/theme'
import { pageShellStyle, panelStyle, chipStyle, SectionEyebrow } from '@/lib/terminal-ui'
import * as A from '@/lib/agents-api'

const DEFAULT_DATE = '2022-08-30'   // the vault-confirmed TITAN breakout day — a real, honest anchor
const DEFAULT_SYMBOL = 'TITAN'

const pct = (v?: number | null, dp = 2) => (v == null ? '—' : (v >= 0 ? '+' : '') + v.toFixed(dp) + '%')
const num = (v?: number | null) => (v == null ? '—' : String(v))
const rupee = (v?: number | null) => (v == null ? '—' : '₹' + v.toLocaleString('en-IN'))

function decisionTone(d?: string | null): 'green' | 'red' | 'amber' | 'neutral' {
  if (d === 'TRADE') return 'green'
  if (d === 'NO_TRADE') return 'red'
  if (d === 'WATCH') return 'amber'
  return 'neutral'
}
function stageTone(s?: string): 'green' | 'red' | 'amber' | 'neutral' {
  if (s === 'BREAKOUT') return 'green'
  if (s === 'RETEST') return 'green'
  if (s === 'APPROACHING') return 'amber'
  if (s === 'FAILED') return 'red'
  return 'neutral'
}

const box: React.CSSProperties = {
  background: T.s2, border: `1px solid ${T.b}`, borderRadius: 10, color: T.t,
  padding: '9px 11px', fontSize: 13, fontFamily: 'inherit',
}

function StatCell({ label, value, color = T.t, sub }: { label: string; value: string; color?: string; sub?: string }) {
  return (
    <div style={{ background: T.s2, border: `1px solid ${T.b}`, borderRadius: 12, padding: '11px 12px' }}>
      <div style={{ fontSize: 10.5, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', fontWeight: 700 }}>{label}</div>
      <div style={{ fontFamily: T.mono, fontSize: 20, fontWeight: 800, color, marginTop: 4 }}>{value}</div>
      {sub && <div style={{ color: T.t3, fontSize: 11, marginTop: 3 }}>{sub}</div>}
    </div>
  )
}

export default function AgentsPage() {
  const [date, setDate] = useState(DEFAULT_DATE)
  const [symbol, setSymbol] = useState(DEFAULT_SYMBOL)
  const [scan, setScan] = useState<A.ScanResp | null>(null)
  const [story, setStory] = useState<A.StorylineResp | null>(null)
  const [decision, setDecision] = useState<A.DecisionResp | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  const load = useCallback(async (sym: string, dt: string) => {
    setBusy(true); setErr(null)
    try {
      const [st, dc] = await Promise.all([A.fetchStoryline(sym, dt), A.fetchDecision(sym, dt)])
      setStory(st); setDecision(dc)
      if (!st.ok && st.error) setErr(st.error)
    } catch (e) {
      setErr((e as Error).message || 'agent backend offline')
    } finally { setBusy(false) }
  }, [])

  const runScan = useCallback(async (dt: string) => {
    try { setScan(await A.fetchScan(dt, 40)) } catch { setScan(null) }
  }, [])

  useEffect(() => { runScan(date); load(symbol, date) }, [])   // initial

  const pick = (sym: string) => { setSymbol(sym); load(sym, date) }
  const applyDate = () => { runScan(date); load(symbol, date) }

  const strat = decision?.strategy
  const fwd = decision?.pattern_forward
  const dec = decision?.decision
  const backendOffline = (scan && scan.ok === false && !scan.error) || decision?.note

  // symbols to offer in the picker: scan hits first (they have an active setup), then the universe.
  const symbolOptions = useMemo(() => {
    const hits = (scan?.occurrences || []).map(o => o.stock)
    return Array.from(new Set([...hits, ...A.DEFAULT_UNIVERSE]))
  }, [scan])

  return (
    <div style={{ ...pageShellStyle(), padding: 24 }}>
      <div style={{ maxWidth: 1320, margin: '0 auto' }}>
        {/* header */}
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 12 }}>
          <div>
            <SectionEyebrow>KANIDA · Agents</SectionEyebrow>
            <h1 style={{ fontSize: 26, fontWeight: 900, margin: '0 0 4px' }}>Agent → Story → Evidence</h1>
            <p style={{ color: T.t2, fontSize: 14, margin: 0, maxWidth: 720 }}>
              Autonomous research agents that observe the tape, decide with evidence, and explain themselves —
              point-in-time, honestly. This is the <b style={{ color: T.g }}>real</b> Chart Agent output:
              at today&apos;s sample sizes it mostly says <b style={{ color: T.a }}>WATCH</b>, and it never invents a trade.
            </p>
          </div>
          <span style={chipStyle('neutral')}>read-only · paper · point-in-time</span>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0,0.85fr) minmax(0,1.15fr) minmax(0,1.15fr)', gap: 16, marginTop: 18, alignItems: 'start' }}>

          {/* ================= LEFT · AGENT + PATTERNS ================= */}
          <div style={panelStyle(18)}>
            <SectionEyebrow>Agent</SectionEyebrow>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
              <div style={{ width: 40, height: 40, borderRadius: 12, background: T.gd, border: `1px solid ${T.gb}`, display: 'grid', placeItems: 'center', color: T.g, fontWeight: 900, fontFamily: T.mono }}>#1</div>
              <div>
                <div style={{ fontSize: 16, fontWeight: 800 }}>Chart Agent</div>
                <div style={{ color: T.t3, fontSize: 12 }}>chart-v1 · daily · observe</div>
              </div>
            </div>
            <p style={{ color: T.t2, fontSize: 12.5, lineHeight: 1.6, margin: '6px 0 14px' }}>
              Detects classic chart patterns, then scores each against its own resolved precedents under a
              governed exit policy (strategy-replay ETV). Emits paper intents only.
            </p>

            <SectionEyebrow>Patterns</SectionEyebrow>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
              {A.CHART_PATTERNS.map(p => (
                <div key={p.id} style={{ background: T.s2, border: `1px solid ${T.b}`, borderRadius: 12, padding: '11px 12px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 }}>
                  <div>
                    <div style={{ fontSize: 13.5, fontWeight: 700, color: T.t }}>{p.name}</div>
                    <div style={{ fontSize: 11, color: T.t3, marginTop: 2 }}>{p.note}</div>
                  </div>
                  <span style={chipStyle(p.status === 'live' ? 'green' : 'neutral', true)}>
                    {p.status === 'live' ? 'LIVE' : 'SOON'}
                  </span>
                </div>
              ))}
            </div>
            <div style={{ marginTop: 12, fontSize: 11, color: T.t3, lineHeight: 1.6 }}>
              SPEC (labelled, never faked): Triangle/Channel detectors, G4 nested-population coherence,
              bootstrap CI. Cloud feeds wiring is SPEC — data reads the R&D DB.
            </div>
          </div>

          {/* ================= MIDDLE · SELECTOR + STORYLINE ================= */}
          <div style={panelStyle(18)}>
            <SectionEyebrow>Story</SectionEyebrow>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center', marginBottom: 14 }}>
              <label style={{ color: T.t3, fontSize: 11 }}>Date</label>
              <input type="date" value={date} onChange={e => setDate(e.target.value)} style={{ ...box, width: 150 }} />
              <label style={{ color: T.t3, fontSize: 11 }}>Stock</label>
              <select value={symbol} onChange={e => pick(e.target.value)} style={{ ...box, minWidth: 130 }}>
                {symbolOptions.map(s => <option key={s} value={s}>{s}</option>)}
              </select>
              <button onClick={applyDate} disabled={busy} style={{ ...chipStyle('green', true), cursor: busy ? 'wait' : 'pointer' }}>
                {busy ? 'Reading…' : 'Read'}
              </button>
            </div>

            {/* scan hits for the date — a quick way to jump to an active setup */}
            {scan?.ok && scan.count > 0 && (
              <div style={{ marginBottom: 14 }}>
                <div style={{ fontSize: 11, color: T.t3, marginBottom: 6 }}>
                  {scan.count} live setup{scan.count === 1 ? '' : 's'} on {scan.date} (of {scan.universe_size} scanned):
                </div>
                <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                  {scan.occurrences.map(o => (
                    <button key={o.stock} onClick={() => pick(o.stock)}
                      style={{ ...chipStyle(o.stock === symbol ? stageTone(o.stage) : 'neutral', true), cursor: 'pointer' }}>
                      {o.stock} · {o.stage}
                    </button>
                  ))}
                </div>
              </div>
            )}
            {scan?.ok && scan.count === 0 && (
              <div style={{ fontSize: 12, color: T.t3, marginBottom: 12 }}>No live setups in the scanned universe on {scan.date}.</div>
            )}

            {/* the honest storyline */}
            {story?.ok && story.events.length > 0 ? (
              <div style={{ position: 'relative', paddingLeft: 6 }}>
                {story.events.map((e, i) => {
                  const isDecision = e.kind === 'decision'
                  const dotColor = isDecision ? (T[decisionTone(story.decision) === 'green' ? 'g' : decisionTone(story.decision) === 'red' ? 'r' : 'a'] ) : T.g
                  return (
                    <div key={i} style={{ display: 'flex', gap: 12, paddingBottom: i === story.events.length - 1 ? 0 : 16, position: 'relative' }}>
                      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                        <div style={{ width: 12, height: 12, borderRadius: 999, background: dotColor, marginTop: 3, flexShrink: 0 }} />
                        {i < story.events.length - 1 && <div style={{ width: 2, flex: 1, background: T.b, marginTop: 2 }} />}
                      </div>
                      <div style={{ paddingBottom: 2 }}>
                        <div style={{ fontSize: 13.5, fontWeight: 700, color: isDecision ? (T[decisionTone(story.decision) === 'green' ? 'g' : decisionTone(story.decision) === 'red' ? 'r' : 'a']) : T.t }}>
                          {e.title}
                        </div>
                        <div style={{ fontSize: 12.5, color: T.t2, lineHeight: 1.6, marginTop: 2 }}>{e.detail}</div>
                        {e.spec_note && <div style={{ fontSize: 10.5, color: T.t3, marginTop: 4, fontStyle: 'italic' }}>SPEC: {e.spec_note}</div>}
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <div style={{ color: T.t2, fontSize: 13, padding: '30px 6px' }}>
                {err ? <span style={{ color: T.r }}>{err}</span> : 'Pick a date + stock and hit Read to see the storyline.'}
              </div>
            )}
          </div>

          {/* ================= RIGHT · EVIDENCE ================= */}
          <div style={panelStyle(18)}>
            <SectionEyebrow>Evidence</SectionEyebrow>

            {/* decision banner */}
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8, marginBottom: 12, flexWrap: 'wrap' }}>
              <span style={{ ...chipStyle(decisionTone(dec)), fontSize: 14, padding: '10px 16px' }}>
                {dec || '—'}
              </span>
              {decision?.basis && <span style={chipStyle('neutral', true)}>basis: {decision.basis}</span>}
              {decision?.occurrence?.stage && <span style={chipStyle(stageTone(decision.occurrence.stage), true)}>{decision.occurrence.stage}</span>}
            </div>
            {decision?.reason && <div style={{ fontSize: 12.5, color: T.t2, lineHeight: 1.6, marginBottom: 14 }}>{decision.reason}</div>}

            {backendOffline && (
              <div style={{ fontSize: 12, color: T.a, background: 'rgba(255,209,102,0.08)', border: `1px solid rgba(255,209,102,0.25)`, borderRadius: 10, padding: '10px 12px', marginBottom: 14 }}>
                {decision?.note || scan?.note || 'Agent data source unavailable (SPEC: cloud feeds wiring).'}
              </div>
            )}

            {/* strategy-replay stats — what the §9 gates actually read */}
            {strat && (
              <>
                <div style={{ fontSize: 11, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', fontWeight: 700, marginBottom: 8 }}>
                  Strategy-replay ({strat.version || 'policy'}) · n={num(strat.n)}
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 8, marginBottom: 8 }}>
                  <StatCell label="Strategy-ETV" value={pct(strat.etv)} color={(strat.etv ?? 0) >= 0 ? T.g : T.r} />
                  <StatCell label="Win" value={strat.win == null ? '—' : strat.win + '%'} />
                  <StatCell label="Payoff" value={num(strat.payoff)} />
                  <StatCell label="CI-low" value={pct(strat.ci_low)} color={(strat.ci_low ?? -1) > 0 ? T.g : T.t2} sub="normal-approx SE" />
                  <StatCell label="Avg MAE" value={pct(strat.mae)} color={T.t2} />
                  <StatCell label="Avg hold" value={strat.avg_holding == null ? '—' : strat.avg_holding + 'd'} />
                </div>
                {strat.exits && (
                  <div style={{ fontSize: 11, color: T.t3, marginBottom: 14 }}>
                    Exits: {Object.entries(strat.exits).map(([k, v]) => `${k} ${v}`).join(' · ')}
                  </div>
                )}
              </>
            )}

            {/* pattern-forward numbers — the research family, kept alongside */}
            {fwd && Object.keys(fwd.horizons).length > 0 && (
              <>
                <div style={{ fontSize: 11, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', fontWeight: 700, marginBottom: 8 }}>
                  Pattern-forward (hold-to-close) · n={num(fwd.n)}
                </div>
                <div style={{ overflowX: 'auto', marginBottom: 12 }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12.5 }}>
                    <thead>
                      <tr style={{ color: T.t3 }}>
                        <th style={{ textAlign: 'left', padding: '4px 6px', fontWeight: 600 }}>H</th>
                        <th style={{ textAlign: 'right', padding: '4px 6px', fontWeight: 600 }}>Win</th>
                        <th style={{ textAlign: 'right', padding: '4px 6px', fontWeight: 600 }}>ETV</th>
                        <th style={{ textAlign: 'right', padding: '4px 6px', fontWeight: 600 }}>MFE</th>
                        <th style={{ textAlign: 'right', padding: '4px 6px', fontWeight: 600 }}>MAE</th>
                      </tr>
                    </thead>
                    <tbody style={{ fontFamily: T.mono }}>
                      {['1', '3', '5', '10'].filter(h => fwd.horizons[h]).map(h => {
                        const r = fwd.horizons[h]
                        return (
                          <tr key={h} style={{ borderTop: `1px solid ${T.b}` }}>
                            <td style={{ padding: '6px 6px', color: T.t2 }}>T+{h}</td>
                            <td style={{ padding: '6px 6px', textAlign: 'right', color: T.t2 }}>{r.win == null ? '—' : r.win + '%'}</td>
                            <td style={{ padding: '6px 6px', textAlign: 'right', color: (r.etv ?? 0) >= 0 ? T.g : T.r }}>{pct(r.etv)}</td>
                            <td style={{ padding: '6px 6px', textAlign: 'right', color: T.g }}>{pct(r.mfe)}</td>
                            <td style={{ padding: '6px 6px', textAlign: 'right', color: T.r }}>{pct(r.mae)}</td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </>
            )}

            {/* occurrence geometry */}
            {decision?.occurrence && (
              <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', fontSize: 11.5, color: T.t2, marginBottom: 12 }}>
                <span style={box}>level {rupee(decision.occurrence.level)}</span>
                <span style={box}>touches {decision.occurrence.touches?.length ?? '—'}</span>
                <span style={box}>dist {pct(decision.occurrence.context?.distance_to_level_pct)}</span>
                <span style={box}>vol {num(decision.occurrence.context?.volume_x)}×</span>
              </div>
            )}

            {/* gate stack */}
            {decision?.gates && decision.gates.length > 0 && (
              <>
                <div style={{ fontSize: 11, color: T.t3, textTransform: 'uppercase', letterSpacing: '.06em', fontWeight: 700, marginBottom: 8 }}>Gates (§9)</div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {decision.gates.map((g, i) => (
                    <div key={i} style={{ display: 'flex', gap: 8, alignItems: 'baseline', fontSize: 12 }}>
                      <span style={{ width: 16, textAlign: 'center', color: g.skipped ? T.t3 : g.pass ? T.g : T.r, fontWeight: 800 }}>
                        {g.skipped ? '–' : g.pass ? '✓' : '✕'}
                      </span>
                      <span style={{ color: T.t2 }}><b style={{ color: T.t }}>{g.gate}</b> — {g.reason}</span>
                    </div>
                  ))}
                </div>
              </>
            )}

            {decision?.spec_note && (
              <div style={{ fontSize: 10.5, color: T.t3, marginTop: 12, fontStyle: 'italic', lineHeight: 1.6 }}>
                SPEC note: {decision.spec_note}
              </div>
            )}

            {!strat && !fwd && !busy && (
              <div style={{ color: T.t2, fontSize: 13, padding: '20px 4px' }}>
                No resolved precedents to score yet for this setup — honest insufficient-evidence state.
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
