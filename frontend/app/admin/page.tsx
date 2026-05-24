'use client'
export const dynamic = 'force-dynamic'

import { useState, useEffect, useCallback, useRef } from 'react'
import {
  fetchUniverse, fetchUniverseStats, fetchDataAudit,
  fetchPipelineStatus, fetchDataFreshness, fetchKiteStatus,
  triggerPipeline, refreshKiteToken, seedUniverse, bulkImport,
  fetchIndices, refreshIndices, type IndexMembership,
  addStock, updateStock, deactivateStock, purgeYfinanceData,
  fetchStrategies, createStrategy, computeStrategyResults,
  promoteStrategy, deleteStrategy,
  type UniverseStock, type UniverseStats, type DataAudit,
  type PipelineStatus, type DataFreshness, type KiteStatus,
  type Strategy, type StrategyStatus,
} from '@/lib/admin-api'

// ── Design tokens (consistent with the rest of the app) ───────────────────────
const C = {
  bg:    '#07070d',
  s1:    '#0c0c18',
  s2:    '#101022',
  s3:    '#14142a',
  b:     'rgba(255,255,255,0.07)',
  b2:    'rgba(255,255,255,0.13)',
  g:     '#00c98a',
  gd:    'rgba(0,201,138,0.08)',
  r:     '#ff4d6d',
  rd:    'rgba(255,77,109,0.10)',
  a:     '#ffd166',
  ad:    'rgba(255,209,102,0.10)',
  t:     '#f4f4fc',
  t2:    '#d6d6ea',
  t3:    '#8888a8',
  indigo:'#6366f1',
}

const SECRET_KEY = 'kanida_admin_secret'
const KITE_API_KEY = process.env.NEXT_PUBLIC_KITE_API_KEY || ''

// ── Shared UI primitives ───────────────────────────────────────────────────────

function Pill({ color, children }: { color: string; children: React.ReactNode }) {
  return (
    <span style={{
      display: 'inline-block', padding: '2px 10px', borderRadius: 20,
      fontSize: 11, fontWeight: 700, letterSpacing: 0.5,
      background: color === 'green' ? C.gd : color === 'red' ? C.rd : C.ad,
      color:      color === 'green' ? C.g   : color === 'red' ? C.r   : C.a,
      border:     `1px solid ${color === 'green' ? 'rgba(0,201,138,0.3)' : color === 'red' ? 'rgba(255,77,109,0.3)' : 'rgba(255,209,102,0.3)'}`,
    }}>
      {children}
    </span>
  )
}

function Card({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) {
  return (
    <div style={{
      background: C.s1, border: `1px solid ${C.b}`, borderRadius: 10,
      padding: 20, ...style,
    }}>
      {children}
    </div>
  )
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ fontSize: 11, color: C.indigo, letterSpacing: 3, textTransform: 'uppercase', marginBottom: 16, fontWeight: 700 }}>
      {children}
    </div>
  )
}

function Btn({
  children, onClick, variant = 'primary', disabled = false, small = false, style,
}: {
  children: React.ReactNode
  onClick?: () => void
  variant?: 'primary' | 'danger' | 'ghost' | 'success'
  disabled?: boolean
  small?: boolean
  style?: React.CSSProperties
}) {
  const colors = {
    primary: { bg: C.indigo,  border: C.indigo,  text: '#fff' },
    danger:  { bg: C.r,       border: C.r,        text: '#fff' },
    success: { bg: C.g,       border: C.g,        text: '#07070d' },
    ghost:   { bg: 'transparent', border: C.b2,   text: C.t2 },
  }
  const { bg, border, text } = colors[variant]
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        background: disabled ? C.s2 : bg,
        border: `1px solid ${disabled ? C.b : border}`,
        color: disabled ? C.t3 : text,
        padding: small ? '5px 14px' : '9px 20px',
        borderRadius: 6,
        fontSize: small ? 12 : 13,
        fontWeight: 600,
        cursor: disabled ? 'not-allowed' : 'pointer',
        fontFamily: 'inherit',
        transition: 'opacity 0.15s',
        opacity: disabled ? 0.5 : 1,
        ...style,
      }}
    >
      {children}
    </button>
  )
}

function Input({ value, onChange, placeholder, type = 'text', style }: {
  value: string
  onChange: (v: string) => void
  placeholder?: string
  type?: string
  style?: React.CSSProperties
}) {
  return (
    <input
      type={type}
      value={value}
      onChange={e => onChange(e.target.value)}
      placeholder={placeholder}
      style={{
        background: C.s2, border: `1px solid ${C.b2}`, borderRadius: 6,
        padding: '9px 12px', color: C.t, fontSize: 13, fontFamily: 'inherit',
        outline: 'none', width: '100%', boxSizing: 'border-box', ...style,
      }}
    />
  )
}

function Spinner() {
  return <span style={{ display: 'inline-block', animation: 'spin 0.8s linear infinite' }}>⟳</span>
}

// ── Tab: Overview ──────────────────────────────────────────────────────────────

function OverviewTab({ secret }: { secret: string }) {
  const [fresh, setFresh]   = useState<DataFreshness | null>(null)
  const [kite, setKite]     = useState<KiteStatus | null>(null)
  const [pipe, setPipe]     = useState<PipelineStatus | null>(null)
  const [loading, setLoading] = useState(true)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const [f, k, p] = await Promise.all([
        fetchDataFreshness().catch(() => null),
        fetchKiteStatus().catch(() => null),
        fetchPipelineStatus().catch(() => null),
      ])
      setFresh(f as DataFreshness)
      setKite(k as KiteStatus)
      setPipe(p as PipelineStatus)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  const healthy = fresh?.overall_healthy
  const kiteOk  = kite?.valid

  return (
    <div>
      <SectionTitle>System overview</SectionTitle>

      {/* 3-column health row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 12, marginBottom: 16 }}>
        <Card>
          <div style={{ fontSize: 11, color: C.t3, marginBottom: 8 }}>DATA FRESHNESS</div>
          {loading
            ? <div style={{ color: C.t3 }}>Loading…</div>
            : <div>
                <Pill color={healthy ? 'green' : 'red'}>{healthy ? '✓ Fresh' : '✗ Stale'}</Pill>
                <div style={{ fontSize: 12, color: C.t3, marginTop: 8 }}>
                  Last trading day: {fresh?.last_trading_day ?? '—'}
                </div>
                <div style={{ fontSize: 12, color: C.t3 }}>
                  NSE tickers fresh: {fresh?.ohlcv.nse_fresh_tickers ?? '—'} / {fresh?.ohlcv.nse_total_tickers ?? '—'}
                </div>
              </div>
          }
        </Card>

        <Card>
          <div style={{ fontSize: 11, color: C.t3, marginBottom: 8 }}>ZERODHA TOKEN</div>
          {loading
            ? <div style={{ color: C.t3 }}>Loading…</div>
            : <div>
                <Pill color={kiteOk ? 'green' : 'red'}>{kiteOk ? `✓ Valid — ${kite?.user ?? ''}` : '✗ Expired'}</Pill>
                {!kiteOk && (
                  <div style={{ fontSize: 12, color: C.a, marginTop: 8 }}>
                    Go to Auth tab → re-authenticate to unblock the pipeline.
                  </div>
                )}
              </div>
          }
        </Card>

        <Card>
          <div style={{ fontSize: 11, color: C.t3, marginBottom: 8 }}>PIPELINE</div>
          {loading
            ? <div style={{ color: C.t3 }}>Loading…</div>
            : <div>
                <Pill color={pipe?.running ? 'amber' : pipe?.last_result === 'SUCCESS' ? 'green' : pipe?.last_result ? 'red' : 'amber'}>
                  {pipe?.running ? '⟳ Running' : pipe?.last_result ?? 'Never run'}
                </Pill>
                <div style={{ fontSize: 12, color: C.t3, marginTop: 8 }}>
                  Last: {pipe?.last_run ? new Date(pipe.last_run).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) : '—'}
                </div>
                <div style={{ fontSize: 12, color: C.t3 }}>
                  Next: {pipe?.next_run ? new Date(pipe.next_run).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) : '—'}
                </div>
              </div>
          }
        </Card>
      </div>

      {/* Pipeline step log */}
      {fresh?.pipeline_logs && (
        <Card style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 11, color: C.t3, marginBottom: 12 }}>PIPELINE STEP LOG</div>
          <div style={{ display: 'grid', gap: 8 }}>
            {Object.entries(fresh.pipeline_logs).map(([step, info]) => (
              <div key={step} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span style={{ fontSize: 12, color: C.t2, textTransform: 'capitalize', minWidth: 160 }}>
                  {step.replace(/_/g, ' ')}
                </span>
                <span style={{ fontSize: 11, color: info.last_run ? C.t3 : C.r, flex: 1 }}>
                  {info.last_run ?? 'Never run'}
                </span>
                <span style={{ fontSize: 11, color: C.t3, maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {info.last_line ?? '—'}
                </span>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Signal stats */}
      {fresh?.signals && (
        <Card>
          <div style={{ fontSize: 11, color: C.t3, marginBottom: 12 }}>SIGNAL ENGINE</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 16 }}>
            {[
              { label: 'Live Opportunities', value: fresh.signals.live_opportunities },
              { label: 'Last Snapshot',       value: fresh.signals.latest_snapshot_date ?? '—' },
              { label: 'Signals in Snapshot', value: fresh.signals.signal_count },
            ].map(({ label, value }) => (
              <div key={label}>
                <div style={{ fontSize: 11, color: C.t3 }}>{label}</div>
                <div style={{ fontSize: 20, fontWeight: 700, color: C.t, marginTop: 4 }}>{value}</div>
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* Environment / infrastructure status */}
      <Card style={{ marginBottom: 16, marginTop: 4 }}>
        <div style={{ fontSize: 11, color: C.t3, marginBottom: 12 }}>INFRASTRUCTURE</div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 12 }}>
          <div>
            <div style={{ fontSize: 11, color: C.t3, marginBottom: 4 }}>Environment</div>
            <Pill color="green">PRODUCTION</Pill>
          </div>
          <div>
            <div style={{ fontSize: 11, color: C.t3, marginBottom: 4 }}>DB Mode</div>
            <Pill color="amber">SQLite — bundled</Pill>
            <div style={{ fontSize: 11, color: C.t3, marginTop: 4 }}>Persists until Railway redeploy</div>
          </div>
          <div>
            <div style={{ fontSize: 11, color: C.t3, marginBottom: 4 }}>Backend</div>
            <Pill color={fresh ? 'green' : 'red'}>{fresh ? '✓ Connected' : loading ? '…' : '✗ Unreachable'}</Pill>
            <div style={{ fontSize: 11, color: C.t3, marginTop: 4 }}>web-production-50ff.up.railway.app</div>
          </div>
          <div>
            <div style={{ fontSize: 11, color: C.t3, marginBottom: 4 }}>Strategy Lab</div>
            <Pill color="amber">⚠ Scaffold only</Pill>
            <div style={{ fontSize: 11, color: C.t3, marginTop: 4 }}>Backtest engine not yet wired</div>
          </div>
        </div>
      </Card>

      <div style={{ textAlign: 'right', marginTop: 12 }}>
        <Btn variant="ghost" small onClick={load} disabled={loading}>
          {loading ? 'Refreshing…' : '↺ Refresh'}
        </Btn>
      </div>
    </div>
  )
}

// ── Tab: Universe ──────────────────────────────────────────────────────────────

function UniverseTab({ secret }: { secret: string }) {
  const [stocks, setStocks]     = useState<UniverseStock[]>([])
  const [stats, setStats]       = useState<UniverseStats | null>(null)
  const [total, setTotal]       = useState(0)
  const [search, setSearch]     = useState('')
  const [loading, setLoading]   = useState(true)
  const [msg, setMsg]           = useState<{ type: 'ok' | 'err'; text: string } | null>(null)

  // Add stock form
  const [showAdd, setShowAdd]   = useState(false)
  const [addSymbol, setAddSym]  = useState('')
  const [addSector, setAddSec]  = useState('')
  const [addName, setAddName]   = useState('')
  const [addSets, setAddSets]   = useState('FNO')
  const [addLoading, setAddLoading] = useState(false)

  // Import CSV
  const [showImport, setShowImport] = useState(false)
  const [csvText, setCsvText]       = useState('')
  const [importLoading, setImportLoading] = useState(false)

  // Index membership
  const [indices, setIndices]                 = useState<IndexMembership[]>([])
  const [refreshIdxLoading, setRefreshIdx]    = useState(false)
  const loadIndices = useCallback(async () => {
    try { const r = await fetchIndices(); setIndices(r.indices) } catch { /* silent */ }
  }, [])
  useEffect(() => { loadIndices() }, [loadIndices])

  async function doRefreshIndices() {
    if (!secret) { setMsg({ type: 'err', text: 'Enter admin secret first (Auth tab).' }); return }
    if (!confirm('Fetch all NSE index constituent lists from nsearchives.nseindia.com? Takes 10–30s.')) return
    setRefreshIdx(true)
    setMsg(null)
    try {
      const r = await refreshIndices(secret)
      const failed = r.indices_failed
      setMsg({
        type: failed === 0 ? 'ok' : 'err',
        text: failed === 0
          ? `✓ ${r.indices_attempted} indices, ${r.total_tickers} memberships loaded.`
          : `Refreshed with ${failed} failures — see admin logs.`,
      })
      loadIndices()
    } catch (e: any) {
      setMsg({ type: 'err', text: e.message })
    } finally {
      setRefreshIdx(false)
    }
  }

  const searchTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  const load = useCallback(async (q?: string) => {
    setLoading(true)
    try {
      const [u, s] = await Promise.all([
        fetchUniverse({ search: q, limit: 500 }),
        fetchUniverseStats(),
      ])
      setStocks(u.results)
      setTotal(u.total)
      setStats(s)
    } catch (e: any) {
      setMsg({ type: 'err', text: e.message })
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  function onSearch(v: string) {
    setSearch(v)
    if (searchTimer.current) clearTimeout(searchTimer.current)
    searchTimer.current = setTimeout(() => load(v), 300)
  }

  async function doAdd() {
    if (!addSymbol.trim()) return
    setAddLoading(true)
    setMsg(null)
    try {
      await addStock({
        symbol:        addSymbol.trim().toUpperCase(),
        sector:        addSector.trim() || undefined,
        company_name:  addName.trim() || undefined,
        universe_sets: addSets.split(',').map(s => s.trim()).filter(Boolean),
      })
      setMsg({ type: 'ok', text: `✓ ${addSymbol.toUpperCase()} added to universe.` })
      setAddSym(''); setAddSec(''); setAddName(''); setShowAdd(false)
      load()
    } catch (e: any) {
      setMsg({ type: 'err', text: e.message })
    } finally {
      setAddLoading(false)
    }
  }

  async function doDeactivate(symbol: string) {
    if (!confirm(`Deactivate ${symbol}? It won't be fetched or learned from. You can re-activate it later.`)) return
    try {
      await deactivateStock(symbol)
      setMsg({ type: 'ok', text: `✓ ${symbol} deactivated.` })
      load()
    } catch (e: any) {
      setMsg({ type: 'err', text: e.message })
    }
  }

  async function doReactivate(symbol: string) {
    try {
      await updateStock(symbol, { is_active: true })
      setMsg({ type: 'ok', text: `✓ ${symbol} reactivated.` })
      load()
    } catch (e: any) {
      setMsg({ type: 'err', text: e.message })
    }
  }

  async function doSeed() {
    if (!secret) { setMsg({ type: 'err', text: 'Enter admin secret first (Auth tab).' }); return }
    if (!confirm('Seed universe with built-in 188 F&O stocks? Existing rows will not be overwritten.')) return
    try {
      const r = await seedUniverse(secret)
      setMsg({ type: 'ok', text: r.message })
      load()
    } catch (e: any) {
      setMsg({ type: 'err', text: e.message })
    }
  }

  async function doImport() {
    if (!csvText.trim()) return
    setImportLoading(true)
    setMsg(null)
    try {
      const r = await bulkImport({ csv_text: csvText })
      setMsg({ type: 'ok', text: `✓ Imported ${r.inserted} stocks. ${r.errors} errors.` })
      setCsvText(''); setShowImport(false)
      load()
    } catch (e: any) {
      setMsg({ type: 'err', text: e.message })
    } finally {
      setImportLoading(false)
    }
  }

  return (
    <div>
      <SectionTitle>Stock universe</SectionTitle>

      {/* Stats row */}
      {stats && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 10, marginBottom: 16 }}>
          {[
            { label: 'Total Stocks',    value: stats.total },
            { label: 'Active',          value: stats.active },
            { label: 'Inactive',        value: stats.inactive },
            { label: 'Sectors',         value: stats.by_sector.length },
          ].map(({ label, value }) => (
            <Card key={label} style={{ padding: 14 }}>
              <div style={{ fontSize: 10, color: C.t3, marginBottom: 4 }}>{label}</div>
              <div style={{ fontSize: 22, fontWeight: 700, color: C.t }}>{value}</div>
            </Card>
          ))}
        </div>
      )}

      {/* Action bar */}
      <div style={{ display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' }}>
        <div style={{ flex: 1, minWidth: 200 }}>
          <Input value={search} onChange={onSearch} placeholder="Search symbol or company…" />
        </div>
        <Btn onClick={() => { setShowAdd(!showAdd); setShowImport(false) }} variant="primary">
          + Add Stock
        </Btn>
        <Btn onClick={() => { setShowImport(!showImport); setShowAdd(false) }} variant="ghost">
          ↑ Import CSV
        </Btn>
        <Btn onClick={doSeed} variant="ghost">
          Seed F&O list
        </Btn>
        <Btn onClick={doRefreshIndices} variant="ghost" disabled={refreshIdxLoading}>
          {refreshIdxLoading ? '⟳ Refreshing…' : '↻ Refresh NSE indices'}
        </Btn>
      </div>

      {/* Index membership status */}
      {indices.length > 0 && (
        <Card style={{ marginBottom: 14 }}>
          <div style={{ fontSize: 11, color: C.t3, marginBottom: 8 }}>
            INDEX MEMBERSHIP — {indices.length} indices loaded ({indices.reduce((a, b) => a + b.members, 0)} total memberships)
          </div>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            {indices.map(i => (
              <span key={i.index_name} style={{
                fontSize: 11, padding: '3px 8px', borderRadius: 4,
                background: `${C.indigo}18`, color: C.indigo,
                border: `1px solid ${C.indigo}33`,
              }}>
                {i.index_name} <span style={{ color: C.t3 }}>· {i.members}</span>
              </span>
            ))}
          </div>
        </Card>
      )}

      {/* Message */}
      {msg && (
        <div style={{
          padding: '10px 14px', borderRadius: 6, marginBottom: 12,
          background: msg.type === 'ok' ? C.gd : C.rd,
          border: `1px solid ${msg.type === 'ok' ? 'rgba(0,201,138,0.3)' : 'rgba(255,77,109,0.3)'}`,
          color: msg.type === 'ok' ? C.g : C.r, fontSize: 13,
        }}>
          {msg.text}
          <button onClick={() => setMsg(null)} style={{ float: 'right', background: 'none', border: 'none', color: 'inherit', cursor: 'pointer', fontSize: 16 }}>×</button>
        </div>
      )}

      {/* Add form */}
      {showAdd && (
        <Card style={{ marginBottom: 14, border: `1px solid ${C.indigo}` }}>
          <div style={{ fontSize: 12, color: C.indigo, marginBottom: 12, fontWeight: 700 }}>ADD NEW STOCK</div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr', gap: 10, marginBottom: 12 }}>
            <div>
              <div style={{ fontSize: 11, color: C.t3, marginBottom: 4 }}>SYMBOL *</div>
              <Input value={addSymbol} onChange={setAddSym} placeholder="e.g. HDFCBANK" />
            </div>
            <div>
              <div style={{ fontSize: 11, color: C.t3, marginBottom: 4 }}>SECTOR</div>
              <Input value={addSector} onChange={setAddSec} placeholder="e.g. Banks" />
            </div>
            <div>
              <div style={{ fontSize: 11, color: C.t3, marginBottom: 4 }}>COMPANY NAME</div>
              <Input value={addName} onChange={setAddName} placeholder="e.g. HDFC Bank Ltd" />
            </div>
            <div>
              <div style={{ fontSize: 11, color: C.t3, marginBottom: 4 }}>UNIVERSE SETS (comma-sep)</div>
              <Input value={addSets} onChange={setAddSets} placeholder="FNO,NIFTY500" />
            </div>
          </div>
          <div style={{ display: 'flex', gap: 10 }}>
            <Btn onClick={doAdd} disabled={addLoading || !addSymbol.trim()} variant="success">
              {addLoading ? 'Adding…' : 'Add Stock'}
            </Btn>
            <Btn onClick={() => setShowAdd(false)} variant="ghost">Cancel</Btn>
          </div>
        </Card>
      )}

      {/* Import form */}
      {showImport && (
        <Card style={{ marginBottom: 14, border: `1px solid ${C.indigo}` }}>
          <div style={{ fontSize: 12, color: C.indigo, marginBottom: 8, fontWeight: 700 }}>BULK IMPORT — CSV</div>
          <div style={{ fontSize: 12, color: C.t3, marginBottom: 10 }}>
            Paste CSV with columns: <code style={{ color: C.t2 }}>symbol, sector, exchange, industry</code>
            <br />Example: <code style={{ color: C.t2 }}>PAYTM,Fintech,NSE,Digital Payments</code>
          </div>
          <textarea
            value={csvText}
            onChange={e => setCsvText(e.target.value)}
            placeholder={'PAYTM,Fintech,NSE\nZOMATOQ,Internet,NSE\n...'}
            rows={6}
            style={{
              width: '100%', background: C.s2, border: `1px solid ${C.b2}`,
              borderRadius: 6, padding: 10, color: C.t, fontSize: 12,
              fontFamily: 'monospace', resize: 'vertical', boxSizing: 'border-box',
            }}
          />
          <div style={{ display: 'flex', gap: 10, marginTop: 10 }}>
            <Btn onClick={doImport} disabled={importLoading || !csvText.trim()} variant="success">
              {importLoading ? 'Importing…' : `Import ${csvText.trim().split('\n').filter(Boolean).length} rows`}
            </Btn>
            <Btn onClick={() => { setShowImport(false); setCsvText('') }} variant="ghost">Cancel</Btn>
          </div>
        </Card>
      )}

      {/* Table */}
      <Card style={{ padding: 0, overflow: 'hidden' }}>
        <div style={{ padding: '12px 16px', borderBottom: `1px solid ${C.b}`, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ fontSize: 12, color: C.t3 }}>
            {loading ? 'Loading…' : `${total} stocks${search ? ` matching "${search}"` : ''}`}
          </span>
        </div>
        <div style={{ overflowX: 'auto', maxHeight: 480 }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead>
              <tr style={{ background: C.s2, position: 'sticky', top: 0, zIndex: 1 }}>
                {['Symbol', 'Company', 'Sector', 'Universe Sets', 'Exchange', 'Added', 'Status', ''].map(h => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', color: C.t3, fontWeight: 600, letterSpacing: 0.5, fontSize: 11, whiteSpace: 'nowrap' }}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {stocks.map(s => (
                <tr key={`${s.symbol}:${s.exchange}`} style={{ borderBottom: `1px solid ${C.b}` }}
                  onMouseEnter={e => (e.currentTarget.style.background = C.s2)}
                  onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}>
                  <td style={{ padding: '9px 14px', color: C.t, fontWeight: 700, fontFamily: 'monospace' }}>{s.symbol}</td>
                  <td style={{ padding: '9px 14px', color: C.t2, maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{s.company_name ?? '—'}</td>
                  <td style={{ padding: '9px 14px', color: C.t3 }}>{s.sector ?? '—'}</td>
                  <td style={{ padding: '9px 14px' }}>
                    <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                      {s.universe_sets.map(us => (
                        <span key={us} style={{ background: C.s3, border: `1px solid ${C.b2}`, borderRadius: 4, padding: '1px 7px', fontSize: 10, color: C.t2 }}>{us}</span>
                      ))}
                    </div>
                  </td>
                  <td style={{ padding: '9px 14px', color: C.t3 }}>{s.exchange}</td>
                  <td style={{ padding: '9px 14px', color: C.t3 }}>{s.added_date}</td>
                  <td style={{ padding: '9px 14px' }}>
                    <Pill color={s.is_active ? 'green' : 'red'}>{s.is_active ? 'Active' : 'Inactive'}</Pill>
                  </td>
                  <td style={{ padding: '9px 14px' }}>
                    {s.is_active
                      ? <Btn small variant="ghost" onClick={() => doDeactivate(s.symbol)}>Deactivate</Btn>
                      : <Btn small variant="ghost" onClick={() => doReactivate(s.symbol)}>Reactivate</Btn>
                    }
                  </td>
                </tr>
              ))}
              {!loading && stocks.length === 0 && (
                <tr><td colSpan={8} style={{ padding: 32, textAlign: 'center', color: C.t3 }}>
                  No stocks found. Use "Seed F&O list" to populate the universe.
                </td></tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  )
}

// ── Tab: Pipeline ──────────────────────────────────────────────────────────────

function PipelineTab({ secret }: { secret: string }) {
  const [pipe, setPipe]   = useState<PipelineStatus | null>(null)
  const [loading, setLoading] = useState(true)
  const [triggering, setTriggering] = useState(false)
  const [msg, setMsg]     = useState<{ type: 'ok' | 'err'; text: string } | null>(null)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const load = useCallback(async () => {
    try {
      const p = await fetchPipelineStatus()
      setPipe(p)
    } catch {
      // silent
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    load()
    pollRef.current = setInterval(load, 5000)
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [load])

  async function doTrigger() {
    if (!secret) { setMsg({ type: 'err', text: 'Save your admin secret first (Auth tab → enter secret → it auto-saves).' }); return }
    setTriggering(true)
    setMsg(null)
    try {
      const r = await triggerPipeline(secret)
      setMsg({ type: 'ok', text: r.message })
    } catch (e: any) {
      setMsg({ type: 'err', text: e.message })
    } finally {
      setTriggering(false)
    }
  }

  const isRunning = pipe?.running

  const STEPS = [
    { name: 'OHLCV Fetch',         desc: 'Download latest daily + weekly candles from Zerodha for all active universe stocks' },
    { name: 'Pattern Learning',    desc: 'Mine recurring price behaviors across the F&O universe. Updates pattern_library table.' },
    { name: 'Backtest',            desc: 'Replay signals against historical data. Produces trade_log, win rates, bucket analysis.' },
    { name: 'Execution Analysis',  desc: 'Compares blind entry vs smart entry P&L. Produces execution_log + Execution IQ scores.' },
    { name: 'Pending Entries',     desc: "Identifies live setups for tomorrow's open. Populates live_opportunities table." },
  ]

  return (
    <div>
      <SectionTitle>Pipeline control</SectionTitle>

      {/* Status + trigger */}
      <Card style={{ marginBottom: 16, display: 'flex', gap: 24, alignItems: 'center' }}>
        <div>
          <div style={{ fontSize: 11, color: C.t3, marginBottom: 6 }}>CURRENT STATUS</div>
          <Pill color={isRunning ? 'amber' : pipe?.last_result === 'SUCCESS' ? 'green' : pipe?.last_result ? 'red' : 'amber'}>
            {isRunning ? '⟳ Running now' : pipe?.last_result ?? 'Never run'}
          </Pill>
          {pipe?.last_run && (
            <div style={{ fontSize: 12, color: C.t3, marginTop: 6 }}>
              Last run: {new Date(pipe.last_run).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })} IST
            </div>
          )}
          {pipe?.next_run && (
            <div style={{ fontSize: 12, color: C.t3, marginTop: 2 }}>
              Next run: {new Date(pipe.next_run).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })} IST
            </div>
          )}
        </div>

        <div style={{ marginLeft: 'auto' }}>
          <Btn
            onClick={doTrigger}
            disabled={isRunning || triggering}
            variant={isRunning ? 'ghost' : 'primary'}
          >
            {isRunning ? <><Spinner /> Running…</> : triggering ? 'Starting…' : '▶ Run Pipeline Now'}
          </Btn>
          <div style={{ fontSize: 11, color: C.t3, marginTop: 6, textAlign: 'right' }}>
            Auto-runs weekdays at 16:05 IST
          </div>
        </div>
      </Card>

      {msg && (
        <div style={{
          padding: '10px 14px', borderRadius: 6, marginBottom: 14,
          background: msg.type === 'ok' ? C.gd : C.rd,
          border: `1px solid ${msg.type === 'ok' ? 'rgba(0,201,138,0.3)' : 'rgba(255,77,109,0.3)'}`,
          color: msg.type === 'ok' ? C.g : C.r, fontSize: 13,
        }}>
          {msg.text}
        </div>
      )}

      {/* Steps */}
      <Card>
        <div style={{ fontSize: 11, color: C.t3, marginBottom: 14 }}>PIPELINE STEPS (run in order)</div>
        <div style={{ display: 'grid', gap: 12 }}>
          {STEPS.map((step, i) => (
            <div key={step.name} style={{ display: 'flex', gap: 14, alignItems: 'flex-start' }}>
              <div style={{
                width: 28, height: 28, borderRadius: '50%', display: 'flex',
                alignItems: 'center', justifyContent: 'center', flexShrink: 0,
                background: C.s3, border: `1px solid ${C.b2}`,
                color: C.indigo, fontSize: 12, fontWeight: 700,
              }}>{i + 1}</div>
              <div>
                <div style={{ fontSize: 13, color: C.t, fontWeight: 600 }}>{step.name}</div>
                <div style={{ fontSize: 12, color: C.t3, marginTop: 2 }}>{step.desc}</div>
              </div>
            </div>
          ))}
        </div>
      </Card>

      <div style={{ marginTop: 12, fontSize: 12, color: C.t3 }}>
        ℹ Requires a valid Zerodha token. If the pipeline aborts, check the Auth tab first.
      </div>
    </div>
  )
}

// ── Tab: Data Audit ────────────────────────────────────────────────────────────

function DataAuditTab({ secret }: { secret: string }) {
  const [audit, setAudit]     = useState<DataAudit | null>(null)
  const [loading, setLoading] = useState(true)
  const [purging, setPurging] = useState(false)
  const [purgeMsg, setPurgeMsg] = useState<{ type: 'ok' | 'err'; text: string } | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      setAudit(await fetchDataAudit())
    } catch {
      // silent
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  async function doPurge() {
    if (!secret) {
      setPurgeMsg({ type: 'err', text: 'Enter admin secret first (Auth tab).' })
      return
    }
    if (!confirm(
      `This will permanently delete ALL yfinance rows from ohlc_daily.\n\n` +
      `You must run the Kite OHLCV fetch pipeline after this to repopulate clean data.\n\n` +
      `Are you sure?`
    )) return

    setPurging(true)
    setPurgeMsg(null)
    try {
      const r = await purgeYfinanceData(secret)
      setPurgeMsg({ type: 'ok', text: r.message })
      load()
    } catch (e: any) {
      setPurgeMsg({ type: 'err', text: e.message })
    } finally {
      setPurging(false)
    }
  }

  return (
    <div>
      <SectionTitle>Data quality audit</SectionTitle>

      {loading && <div style={{ color: C.t3, padding: 32, textAlign: 'center' }}>Loading audit…</div>}

      {audit && (
        <>
          {/* Warnings */}
          {audit.warnings.length > 0 && (
            <div style={{ display: 'grid', gap: 10, marginBottom: 16 }}>
              {audit.warnings.map((w, i) => (
                <div key={i} style={{
                  padding: '12px 16px', borderRadius: 8,
                  background: w.level === 'critical' ? C.rd : w.level === 'warning' ? C.ad : C.gd,
                  border: `1px solid ${w.level === 'critical' ? 'rgba(255,77,109,0.3)' : w.level === 'warning' ? 'rgba(255,209,102,0.3)' : 'rgba(0,201,138,0.3)'}`,
                }}>
                  <div style={{ fontWeight: 700, color: w.level === 'critical' ? C.r : w.level === 'warning' ? C.a : C.g, fontSize: 13 }}>
                    {w.level === 'critical' ? '⚠ CRITICAL' : w.level === 'warning' ? '⚠ WARNING' : 'ℹ INFO'} — {w.message}
                  </div>
                  <div style={{ fontSize: 12, color: C.t2, marginTop: 4 }}>Action: {w.action}</div>
                </div>
              ))}
            </div>
          )}

          {/* Summary cards */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 12, marginBottom: 16 }}>
            {[
              { label: 'Total OHLCV Rows',    value: audit.total_ohlcv_rows.toLocaleString() },
              { label: 'Contaminated (yfinance)', value: audit.yfinance_rows.toLocaleString(), warn: audit.yfinance_rows > 0 },
              { label: 'Contamination %',     value: `${audit.contamination_pct}%`, warn: audit.contamination_pct > 0 },
            ].map(({ label, value, warn }) => (
              <Card key={label} style={{ border: warn ? `1px solid rgba(255,77,109,0.3)` : undefined }}>
                <div style={{ fontSize: 11, color: C.t3, marginBottom: 6 }}>{label}</div>
                <div style={{ fontSize: 22, fontWeight: 700, color: warn ? C.r : C.t }}>{value}</div>
              </Card>
            ))}
          </div>

          {/* Source breakdown */}
          <Card style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 11, color: C.t3, marginBottom: 12 }}>SOURCE BREAKDOWN</div>
            {audit.sources.length === 0
              ? <div style={{ color: C.t3, fontSize: 13 }}>No OHLCV data found in database.</div>
              : <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
                  <thead>
                    <tr>
                      {['Source', 'Rows', 'Tickers', 'Latest Date', 'Status'].map(h => (
                        <th key={h} style={{ padding: '8px 12px', textAlign: 'left', color: C.t3, borderBottom: `1px solid ${C.b}` }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {audit.sources.map(s => (
                      <tr key={s.source} style={{ borderBottom: `1px solid ${C.b}` }}>
                        <td style={{ padding: '8px 12px', fontFamily: 'monospace', color: s.source === 'yfinance' ? C.r : C.t }}>{s.source ?? 'null'}</td>
                        <td style={{ padding: '8px 12px', color: C.t2 }}>{s.rows.toLocaleString()}</td>
                        <td style={{ padding: '8px 12px', color: C.t2 }}>{s.tickers}</td>
                        <td style={{ padding: '8px 12px', color: C.t3 }}>{s.latest_date ?? '—'}</td>
                        <td style={{ padding: '8px 12px' }}>
                          <Pill color={s.source === 'kite' ? 'green' : 'red'}>
                            {s.source === 'kite' ? '✓ Clean' : '✗ Contaminated'}
                          </Pill>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
            }
          </Card>

          {/* Stale tickers */}
          {audit.stale_tickers.length > 0 && (
            <Card style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 11, color: C.a, marginBottom: 12 }}>
                ⚠ STALE TICKERS (no data in 5+ days)
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
                {audit.stale_tickers.map(t => (
                  <div key={t.ticker} style={{ background: C.s3, border: `1px solid ${C.b2}`, borderRadius: 6, padding: '4px 10px', fontSize: 12 }}>
                    <span style={{ color: C.t, fontWeight: 700 }}>{t.ticker}</span>
                    <span style={{ color: C.t3, marginLeft: 6 }}>{t.latest}</span>
                  </div>
                ))}
              </div>
            </Card>
          )}

          {/* Missing from universe */}
          {audit.universe_missing_data.length > 0 && (
            <Card>
              <div style={{ fontSize: 11, color: C.t3, marginBottom: 12 }}>
                ACTIVE UNIVERSE STOCKS WITH NO OHLCV DATA
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
                {audit.universe_missing_data.map(sym => (
                  <span key={sym} style={{ background: C.s3, border: `1px solid ${C.b2}`, borderRadius: 4, padding: '2px 8px', fontSize: 12, color: C.a }}>{sym}</span>
                ))}
              </div>
              <div style={{ fontSize: 12, color: C.t3, marginTop: 8 }}>
                Run the pipeline to fetch their history.
              </div>
            </Card>
          )}
        </>
      )}

      {/* Purge action */}
      {audit && audit.yfinance_rows > 0 && (
        <Card style={{ marginTop: 16, border: `1px solid rgba(255,77,109,0.3)` }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div>
              <div style={{ fontSize: 13, color: C.r, fontWeight: 700, marginBottom: 4 }}>
                ⚠ Purge yfinance Contamination
              </div>
              <div style={{ fontSize: 12, color: C.t3 }}>
                Permanently removes {audit.yfinance_rows.toLocaleString()} contaminated rows.
                Run the pipeline after to repopulate with clean Kite data.
              </div>
            </div>
            <Btn variant="danger" onClick={doPurge} disabled={purging}>
              {purging ? 'Purging…' : 'Purge yfinance Data'}
            </Btn>
          </div>
          {purgeMsg && (
            <div style={{
              marginTop: 10, padding: '8px 12px', borderRadius: 6,
              background: purgeMsg.type === 'ok' ? C.gd : C.rd,
              border: `1px solid ${purgeMsg.type === 'ok' ? 'rgba(0,201,138,0.3)' : 'rgba(255,77,109,0.3)'}`,
              color: purgeMsg.type === 'ok' ? C.g : C.r, fontSize: 12,
            }}>
              {purgeMsg.text}
            </div>
          )}
        </Card>
      )}

      <div style={{ textAlign: 'right', marginTop: 12 }}>
        <Btn variant="ghost" small onClick={load} disabled={loading}>
          {loading ? 'Refreshing…' : '↺ Refresh Audit'}
        </Btn>
      </div>
    </div>
  )
}

// ── Tab: Strategy Lab ──────────────────────────────────────────────────────────

const STATUS_COLOR: Record<StrategyStatus, string> = {
  draft:    'amber',
  sandbox:  'amber',
  staging:  'amber',
  prod:     'green',
  archived: 'red',
}

const STATUS_NEXT_LABEL: Partial<Record<StrategyStatus, string>> = {
  draft:   'Promote → Sandbox',
  sandbox: 'Promote → Staging',
  staging: 'Promote → Prod',
  prod:    'Archive',
}

// ── Strategy Lab scaffold banner (Sprint 1: backtest engine not yet wired) ─────
function StrategyLabScaffoldBanner() {
  return (
    <div style={{
      background: 'rgba(245,158,11,0.08)',
      border: '1px solid rgba(245,158,11,0.35)',
      borderRadius: 8,
      padding: '12px 16px',
      marginBottom: 20,
      display: 'flex',
      gap: 12,
      alignItems: 'flex-start',
    }}>
      <span style={{ fontSize: 18, lineHeight: 1 }}>⚠</span>
      <div>
        <div style={{ color: C.a, fontWeight: 700, fontSize: 13, marginBottom: 4 }}>
          Strategy Lab — Scaffold Only (Sprint 1)
        </div>
        <div style={{ color: '#d97706', fontSize: 12, lineHeight: 1.6 }}>
          Creating, promoting, and archiving strategies works.{' '}
          <strong style={{ color: C.a }}>Compute Backtest</strong> reads{' '}
          <code style={{ background: 'rgba(255,255,255,0.06)', padding: '1px 5px', borderRadius: 3 }}>trade_log</code>{' '}
          where <code style={{ background: 'rgba(255,255,255,0.06)', padding: '1px 5px', borderRadius: 3 }}>strategy_id</code> matches —
          the backtest engine does not yet tag trades with a strategy_id.
          Compute will return 0 trades until Sprint 2 refactors the pipeline.
          Do not promote strategies to prod based on Compute results yet.
        </div>
      </div>
    </div>
  )
}

function StrategyLabTab({ secret }: { secret: string }) {
  const [strategies, setStrategies] = useState<Strategy[]>([])
  const [loading, setLoading]       = useState(true)
  const [msg, setMsg]               = useState<{ type: 'ok' | 'err'; text: string } | null>(null)

  // Create form
  const [showCreate, setShowCreate] = useState(false)
  const [newName, setNewName]       = useState('')
  const [newDesc, setNewDesc]       = useState('')
  const [newNotes, setNewNotes]     = useState('')
  const [creating, setCreating]     = useState(false)

  // Per-row loading state
  const [computing, setComputing]   = useState<string | null>(null)
  const [promoting, setPromoting]   = useState<string | null>(null)
  const [deleting, setDeleting]     = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      setStrategies(await fetchStrategies())
    } catch (e: any) {
      setMsg({ type: 'err', text: e.message })
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])

  async function doCreate() {
    if (!newName.trim()) return
    setCreating(true)
    setMsg(null)
    try {
      const r = await createStrategy({ name: newName.trim(), description: newDesc.trim() || undefined, notes: newNotes.trim() || undefined })
      setMsg({ type: 'ok', text: `✓ Strategy "${r.name}" created (draft).` })
      setNewName(''); setNewDesc(''); setNewNotes(''); setShowCreate(false)
      load()
    } catch (e: any) {
      setMsg({ type: 'err', text: e.message })
    } finally {
      setCreating(false)
    }
  }

  async function doCompute(id: string) {
    setComputing(id)
    setMsg(null)
    try {
      const r = await computeStrategyResults(id)
      const res = r.result as any
      setMsg({ type: 'ok', text: `✓ Computed: ${res.trades} trades, win rate ${res.win_rate ?? '—'}%` })
      load()
    } catch (e: any) {
      setMsg({ type: 'err', text: e.message })
    } finally {
      setComputing(null)
    }
  }

  async function doPromote(strategy: Strategy) {
    if (!secret) { setMsg({ type: 'err', text: 'Enter admin secret first (Auth tab).' }); return }
    const nextStatus = STATUS_NEXT_LABEL[strategy.status]
    if (!nextStatus) return
    if (!confirm(`${nextStatus} "${strategy.name}"?`)) return

    setPromoting(strategy.id)
    setMsg(null)
    try {
      const r = await promoteStrategy(strategy.id, secret)
      setMsg({ type: 'ok', text: `✓ "${strategy.name}" moved to ${r.new_status}.` })
      load()
    } catch (e: any) {
      setMsg({ type: 'err', text: e.message })
    } finally {
      setPromoting(null)
    }
  }

  async function doDelete(strategy: Strategy) {
    if (!secret) { setMsg({ type: 'err', text: 'Enter admin secret first (Auth tab).' }); return }
    if (!confirm(`Delete draft "${strategy.name}"? This cannot be undone.`)) return

    setDeleting(strategy.id)
    setMsg(null)
    try {
      await deleteStrategy(strategy.id, secret)
      setMsg({ type: 'ok', text: `✓ Draft "${strategy.name}" deleted.` })
      load()
    } catch (e: any) {
      setMsg({ type: 'err', text: e.message })
    } finally {
      setDeleting(null)
    }
  }

  const prodStrategy = strategies.find(s => s.status === 'prod')

  return (
    <div>
      <StrategyLabScaffoldBanner />
      <SectionTitle>Strategy lab</SectionTitle>

      {/* Active prod strategy */}
      {prodStrategy ? (
        <Card style={{ marginBottom: 16, border: `1px solid rgba(0,201,138,0.3)` }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
            <div>
              <div style={{ fontSize: 11, color: C.g, marginBottom: 6, letterSpacing: 2, fontWeight: 700 }}>ACTIVE PROD STRATEGY</div>
              <div style={{ fontSize: 18, fontWeight: 700, color: C.t, marginBottom: 4 }}>{prodStrategy.name}</div>
              <div style={{ fontSize: 12, color: C.t3 }}>{prodStrategy.description ?? 'No description'}</div>
            </div>
            <div style={{ textAlign: 'right' }}>
              <div style={{ fontSize: 11, color: C.t3, marginBottom: 4 }}>Version {prodStrategy.version}</div>
              <div style={{ fontSize: 11, color: C.t3 }}>
                Promoted {prodStrategy.promoted_at ? new Date(prodStrategy.promoted_at).toLocaleDateString('en-IN') : '—'}
              </div>
            </div>
          </div>
          {prodStrategy.backtest_result && (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', gap: 12, marginTop: 14, paddingTop: 14, borderTop: `1px solid ${C.b}` }}>
              {[
                { label: 'Trades',    value: (prodStrategy.backtest_result as any).trades },
                { label: 'Win Rate',  value: `${(prodStrategy.backtest_result as any).win_rate ?? '—'}%` },
                { label: 'Avg Return',value: `${(prodStrategy.backtest_result as any).avg_return ?? '—'}%` },
                { label: 'Total PnL', value: `${(prodStrategy.backtest_result as any).total_pnl_pct ?? '—'}%` },
              ].map(({ label, value }) => (
                <div key={label}>
                  <div style={{ fontSize: 10, color: C.t3 }}>{label}</div>
                  <div style={{ fontSize: 17, fontWeight: 700, color: C.t, marginTop: 3 }}>{value}</div>
                </div>
              ))}
            </div>
          )}
        </Card>
      ) : (
        <Card style={{ marginBottom: 16, padding: '14px 18px' }}>
          <div style={{ fontSize: 13, color: C.t3 }}>
            No prod strategy active. Create a strategy, compute backtest results, and promote it through the lifecycle.
          </div>
        </Card>
      )}

      {/* Create button */}
      <div style={{ marginBottom: 14 }}>
        <Btn onClick={() => setShowCreate(!showCreate)} variant="primary">
          + New Strategy
        </Btn>
      </div>

      {/* Message */}
      {msg && (
        <div style={{
          padding: '10px 14px', borderRadius: 6, marginBottom: 14,
          background: msg.type === 'ok' ? C.gd : C.rd,
          border: `1px solid ${msg.type === 'ok' ? 'rgba(0,201,138,0.3)' : 'rgba(255,77,109,0.3)'}`,
          color: msg.type === 'ok' ? C.g : C.r, fontSize: 13,
        }}>
          {msg.text}
          <button onClick={() => setMsg(null)} style={{ float: 'right', background: 'none', border: 'none', color: 'inherit', cursor: 'pointer', fontSize: 16 }}>×</button>
        </div>
      )}

      {/* Create form */}
      {showCreate && (
        <Card style={{ marginBottom: 14, border: `1px solid ${C.indigo}` }}>
          <div style={{ fontSize: 12, color: C.indigo, marginBottom: 12, fontWeight: 700 }}>NEW DRAFT STRATEGY</div>
          <div style={{ display: 'grid', gap: 10, marginBottom: 12 }}>
            <div>
              <div style={{ fontSize: 11, color: C.t3, marginBottom: 4 }}>NAME *</div>
              <Input value={newName} onChange={setNewName} placeholder="e.g. Rally Breakout v1" />
            </div>
            <div>
              <div style={{ fontSize: 11, color: C.t3, marginBottom: 4 }}>DESCRIPTION</div>
              <Input value={newDesc} onChange={setNewDesc} placeholder="Brief strategy description" />
            </div>
            <div>
              <div style={{ fontSize: 11, color: C.t3, marginBottom: 4 }}>NOTES</div>
              <Input value={newNotes} onChange={setNewNotes} placeholder="Any additional context" />
            </div>
          </div>
          <div style={{ fontSize: 12, color: C.t3, marginBottom: 12 }}>
            Params default to: RR 2.0, min_overlap 0.65, max_hold 21d, smart entry, rally + pullback.
            Edit them after creation via the API or update endpoint.
          </div>
          <div style={{ display: 'flex', gap: 10 }}>
            <Btn onClick={doCreate} disabled={creating || !newName.trim()} variant="success">
              {creating ? 'Creating…' : 'Create Draft'}
            </Btn>
            <Btn onClick={() => setShowCreate(false)} variant="ghost">Cancel</Btn>
          </div>
        </Card>
      )}

      {/* Strategies table */}
      <Card style={{ padding: 0, overflow: 'hidden' }}>
        <div style={{ padding: '12px 16px', borderBottom: `1px solid ${C.b}` }}>
          <span style={{ fontSize: 12, color: C.t3 }}>
            {loading ? 'Loading…' : `${strategies.length} strategies`}
          </span>
        </div>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead>
              <tr style={{ background: C.s2 }}>
                {['Name', 'Status', 'Ver', 'Trades', 'Win%', 'Avg Return', 'Last Backtest', 'Actions'].map(h => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', color: C.t3, fontWeight: 600, fontSize: 11, whiteSpace: 'nowrap' }}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {strategies.map(s => {
                const br = s.backtest_result as any
                return (
                  <tr key={s.id} style={{ borderBottom: `1px solid ${C.b}` }}
                    onMouseEnter={e => (e.currentTarget.style.background = C.s2)}
                    onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}>
                    <td style={{ padding: '10px 14px' }}>
                      <div style={{ color: C.t, fontWeight: 600 }}>{s.name}</div>
                      {s.description && <div style={{ color: C.t3, fontSize: 11, marginTop: 2 }}>{s.description}</div>}
                    </td>
                    <td style={{ padding: '10px 14px' }}>
                      <Pill color={STATUS_COLOR[s.status]}>{s.status}</Pill>
                    </td>
                    <td style={{ padding: '10px 14px', color: C.t3 }}>v{s.version}</td>
                    <td style={{ padding: '10px 14px', color: C.t2 }}>{br?.trades ?? '—'}</td>
                    <td style={{ padding: '10px 14px', color: br?.win_rate != null ? (br.win_rate >= 50 ? C.g : C.r) : C.t3 }}>
                      {br?.win_rate != null ? `${br.win_rate}%` : '—'}
                    </td>
                    <td style={{ padding: '10px 14px', color: br?.avg_return != null ? (br.avg_return >= 0 ? C.g : C.r) : C.t3 }}>
                      {br?.avg_return != null ? `${br.avg_return}%` : '—'}
                    </td>
                    <td style={{ padding: '10px 14px', color: C.t3 }}>
                      {s.last_backtest_at ? new Date(s.last_backtest_at).toLocaleDateString('en-IN') : '—'}
                    </td>
                    <td style={{ padding: '10px 14px' }}>
                      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
                        {s.status !== 'archived' && (
                          <Btn small variant="ghost" onClick={() => doCompute(s.id)} disabled={computing === s.id}>
                            {computing === s.id ? '⟳' : '⊞ Compute'}
                          </Btn>
                        )}
                        {STATUS_NEXT_LABEL[s.status] && (
                          <Btn small variant="primary" onClick={() => doPromote(s)} disabled={promoting === s.id}>
                            {promoting === s.id ? '…' : STATUS_NEXT_LABEL[s.status]}
                          </Btn>
                        )}
                        {s.status === 'draft' && (
                          <Btn small variant="danger" onClick={() => doDelete(s)} disabled={deleting === s.id}>
                            {deleting === s.id ? '…' : 'Delete'}
                          </Btn>
                        )}
                      </div>
                    </td>
                  </tr>
                )
              })}
              {!loading && strategies.length === 0 && (
                <tr><td colSpan={8} style={{ padding: 32, textAlign: 'center', color: C.t3 }}>
                  No strategies yet. Create one to get started.
                </td></tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>

      <div style={{ marginTop: 10, fontSize: 12, color: C.t3 }}>
        ℹ Compute reads from <code style={{ color: C.t2 }}>trade_log</code> where strategy_id matches.
        Run the Backtest pipeline step to populate trades for a strategy.
      </div>
    </div>
  )
}

// ── Tab: Auth ──────────────────────────────────────────────────────────────────

function AuthTab({
  secret, setSecret, onSecretSaved,
}: {
  secret: string
  setSecret: (s: string) => void
  onSecretSaved: () => void
}) {
  const [requestToken, setRequestToken] = useState('')
  const [status, setStatus]             = useState<any>(null)
  const [loading, setLoading]           = useState(false)
  const [tokenStatus, setTokenStatus]   = useState<KiteStatus | null>(null)
  const [autoDetected, setAutoDetected] = useState(false)

  const loginUrl = KITE_API_KEY
    ? `https://kite.zerodha.com/connect/login?api_key=${KITE_API_KEY}&v=3`
    : 'https://kite.zerodha.com'

  useEffect(() => {
    fetchKiteStatus().then(setTokenStatus).catch(() => {})

    const params = new URLSearchParams(window.location.search)
    const rt = params.get('request_token')
    const ok = params.get('status')
    if (rt && ok === 'success') {
      setRequestToken(rt)
      setAutoDetected(true)
      window.history.replaceState({}, '', '/admin')
    }
  }, [])

  useEffect(() => {
    if (autoDetected && requestToken && secret) {
      doRefresh(requestToken, secret)
    }
  }, [autoDetected, requestToken, secret])

  async function doRefresh(rt: string, sec: string) {
    setLoading(true)
    setStatus(null)
    try {
      const data = await refreshKiteToken(rt, sec)
      setStatus({ ...data, status: 'ok' })
      localStorage.setItem(SECRET_KEY, sec)
      onSecretSaved()
      setRequestToken('')
      setAutoDetected(false)
      fetchKiteStatus().then(setTokenStatus).catch(() => {})
    } catch (e: any) {
      setStatus({ status: 'error', message: e.message })
    } finally {
      setLoading(false)
    }
  }

  const tokenValid = tokenStatus?.valid

  return (
    <div style={{ maxWidth: 520 }}>
      <SectionTitle>Zerodha authentication</SectionTitle>

      {/* Token status */}
      <Card style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <div style={{ fontSize: 11, color: C.t3, marginBottom: 6 }}>TOKEN STATUS</div>
          <Pill color={tokenValid ? 'green' : 'red'}>
            {tokenStatus == null ? 'Checking…' : tokenValid ? `✓ Valid — ${tokenStatus.user}` : '✗ Expired — refresh needed'}
          </Pill>
        </div>
        <Btn variant="ghost" small onClick={() => fetchKiteStatus().then(setTokenStatus)}>Recheck</Btn>
      </Card>

      {/* Admin secret */}
      <Card style={{ marginBottom: 14 }}>
        <div style={{ fontSize: 11, color: C.t3, marginBottom: 8 }}>ADMIN SECRET</div>
        <div style={{ display: 'flex', gap: 10 }}>
          <Input type="password" value={secret} onChange={setSecret} placeholder="Enter ADMIN_SECRET value" />
          <Btn variant="ghost" onClick={() => { localStorage.setItem(SECRET_KEY, secret); onSecretSaved() }}>
            Save
          </Btn>
        </div>
        <div style={{ fontSize: 11, color: C.t3, marginTop: 6 }}>
          Saved locally in your browser. Required to trigger the pipeline and manage the universe.
        </div>
      </Card>

      {/* One-click login */}
      {!tokenValid && !loading && (
        <Card style={{ marginBottom: 14 }}>
          <div style={{ fontSize: 13, color: C.t2, marginBottom: 14 }}>
            Click below — Zerodha will redirect back to this page and authenticate automatically.
          </div>
          <a href={loginUrl} style={{
            display: 'block', textAlign: 'center', background: C.indigo,
            color: '#fff', padding: 12, borderRadius: 6, fontSize: 14,
            textDecoration: 'none', fontWeight: 700,
          }}>
            Login with Zerodha →
          </a>
        </Card>
      )}

      {loading && (
        <Card style={{ textAlign: 'center', padding: 32 }}>
          <div style={{ fontSize: 15, color: C.indigo, marginBottom: 8 }}>⟳ Authenticating with Zerodha…</div>
          <div style={{ fontSize: 12, color: C.t3 }}>Exchanging request token for access token</div>
        </Card>
      )}

      {status?.status === 'ok' && (
        <Card style={{ border: `1px solid rgba(0,201,138,0.3)` }}>
          <div style={{ color: C.g, fontWeight: 700, fontSize: 14, marginBottom: 8 }}>✓ Authentication complete</div>
          <div style={{ color: C.t2, fontSize: 13 }}>
            Token saved to DB — all services will use it automatically.
            {status.railway_updated && ' Railway env also updated.'}
          </div>
          <div style={{ color: C.t3, fontSize: 12, marginTop: 6 }}>
            Preview: <code style={{ color: C.t2 }}>{status.token_preview}</code>
          </div>
        </Card>
      )}

      {status?.status === 'error' && (
        <Card style={{ border: `1px solid rgba(255,77,109,0.3)` }}>
          <div style={{ color: C.r, fontSize: 13 }}>✗ {status.message}</div>
        </Card>
      )}
    </div>
  )
}

// ── Root component ─────────────────────────────────────────────────────────────

type Tab = 'overview' | 'universe' | 'pipeline' | 'audit' | 'strategy' | 'auth'

const TABS: { key: Tab; label: string; icon: string }[] = [
  { key: 'overview',  label: 'Overview',    icon: '⬡' },
  { key: 'universe',  label: 'Universe',    icon: '◈' },
  { key: 'pipeline',  label: 'Pipeline',    icon: '▶' },
  { key: 'audit',     label: 'Data Audit',  icon: '◎' },
  { key: 'strategy',  label: 'Strategy Lab',icon: '⊞' },
  { key: 'auth',      label: 'Zerodha Auth',icon: '⚿' },
]

export default function AdminPage() {
  const [tab, setTab]       = useState<Tab>('overview')
  const [secret, setSecret] = useState('')

  useEffect(() => {
    const saved = localStorage.getItem(SECRET_KEY)
    if (saved) setSecret(saved)

    // Style for spinner animation
    const style = document.createElement('style')
    style.textContent = `@keyframes spin { to { transform: rotate(360deg); } }`
    document.head.appendChild(style)
    return () => { document.head.removeChild(style) }
  }, [])

  return (
    <div style={{ minHeight: '100vh', background: C.bg, color: C.t, fontFamily: 'system-ui, sans-serif', display: 'flex' }}>

      {/* Sidebar */}
      <div style={{
        width: 200, flexShrink: 0, background: C.s1,
        borderRight: `1px solid ${C.b}`, padding: '24px 0',
        display: 'flex', flexDirection: 'column',
      }}>
        {/* Logo */}
        <div style={{ padding: '0 20px 24px', borderBottom: `1px solid ${C.b}` }}>
          <div style={{ fontSize: 10, color: C.indigo, letterSpacing: 3, marginBottom: 4 }}>KANIDA.AI</div>
          <div style={{ fontSize: 15, fontWeight: 700, color: C.t }}>Admin Portal</div>
        </div>

        {/* Nav */}
        <nav style={{ padding: '16px 10px', flex: 1 }}>
          {TABS.map(t => (
            <button
              key={t.key}
              onClick={() => setTab(t.key)}
              style={{
                display: 'flex', alignItems: 'center', gap: 10,
                width: '100%', padding: '10px 12px', borderRadius: 8,
                border: 'none', cursor: 'pointer', fontFamily: 'inherit',
                background: tab === t.key ? C.s3 : 'transparent',
                color: tab === t.key ? C.t : C.t3,
                fontSize: 13, fontWeight: tab === t.key ? 600 : 400,
                marginBottom: 2, transition: 'all 0.1s',
              }}
            >
              <span style={{ fontSize: 15, width: 20, textAlign: 'center' }}>{t.icon}</span>
              {t.label}
            </button>
          ))}
        </nav>

        {/* Back link */}
        <div style={{ padding: '16px 20px', borderTop: `1px solid ${C.b}` }}>
          <a href="/terminal" style={{ fontSize: 12, color: C.t3, textDecoration: 'none' }}>
            ← Back to Terminal
          </a>
        </div>
      </div>

      {/* Main content */}
      <div style={{ flex: 1, padding: 32, overflowY: 'auto', maxHeight: '100vh' }}>
        <div style={{ maxWidth: 1100, margin: '0 auto' }}>

          {/* Page header */}
          <div style={{ marginBottom: 28 }}>
            <div style={{ fontSize: 20, fontWeight: 700, color: C.t }}>
              {TABS.find(t => t.key === tab)?.label}
            </div>
            <div style={{ fontSize: 13, color: C.t3, marginTop: 4 }}>
              {tab === 'overview'  && 'System health at a glance'}
              {tab === 'universe'  && 'Add, import, or deactivate stocks — changes take effect on the next pipeline run'}
              {tab === 'pipeline'  && 'Trigger or monitor the nightly data pipeline'}
              {tab === 'audit'     && 'Inspect data source quality and detect contamination'}
              {tab === 'strategy'  && 'Create strategies, compute backtest results, and promote to production'}
              {tab === 'auth'      && 'Zerodha token management — token expires daily at midnight IST'}
            </div>
          </div>

          {/* Tab content */}
          {tab === 'overview'  && <OverviewTab    secret={secret} />}
          {tab === 'universe'  && <UniverseTab   secret={secret} />}
          {tab === 'pipeline'  && <PipelineTab   secret={secret} />}
          {tab === 'audit'     && <DataAuditTab  secret={secret} />}
          {tab === 'strategy'  && <StrategyLabTab secret={secret} />}
          {tab === 'auth'      && (
            <AuthTab
              secret={secret}
              setSecret={setSecret}
              onSecretSaved={() => {}}
            />
          )}

        </div>
      </div>
    </div>
  )
}
