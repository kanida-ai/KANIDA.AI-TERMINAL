'use client'
import { useEffect, useState } from 'react'
import {
  FalconAPI, FalconPortfolioSummary, FalconPortfolioTrade,
} from '../../../lib/falcon-api'

const fmtINR = (x: number) => {
  const sign = x < 0 ? '-' : ''
  const a = Math.abs(x)
  if (a >= 1e7) return `${sign}₹${(a/1e7).toFixed(2)} Cr`
  if (a >= 1e5) return `${sign}₹${(a/1e5).toFixed(2)} L`
  return `${sign}₹${a.toFixed(0)}`
}

export default function FalconBacktestPage() {
  const [summary, setSummary] = useState<FalconPortfolioSummary | null>(null)
  const [trades,  setTrades]  = useState<FalconPortfolioTrade[]>([])
  const [err, setErr] = useState<string | null>(null)
  const [missing, setMissing] = useState(false)

  useEffect(() => {
    FalconAPI.portfolioSummary().then(setSummary).catch(e => {
      if (String(e).includes('404')) setMissing(true)
      else setErr(String(e))
    })
    FalconAPI.portfolioTrades(200).then(setTrades).catch(() => {})
  }, [])

  if (err) return <div className="text-red-400">{err}</div>
  if (missing) return <V71BacktestSummary />

  if (!summary) return <div className="text-neutral-500">Loading…</div>

  const winRate = summary.win_rate
  const wlr = summary.win_loss_ratio ?? 0

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-bold">Backtest — V{summary.engine_version}</h1>

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
        <Stat label="Starting" value={fmtINR(summary.starting_capital)} />
        <Stat label="Ending"   value={fmtINR(summary.ending_equity)} accent />
        <Stat label="P&L"      value={fmtINR(summary.total_pnl)} accent />
        <Stat label="Return"   value={`${summary.return_pct >= 0 ? '+' : ''}${summary.return_pct.toFixed(2)}%`} accent />
        <Stat label="Trades"   value={String(summary.trades_taken)} />
        <Stat label="Win rate" value={`${winRate.toFixed(1)}%`} />
        <Stat label="W/L ratio" value={wlr.toFixed(2)} />
        <Stat label="Max DD"   value={`${summary.max_drawdown_pct.toFixed(2)}%`} />
      </section>

      <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
        <h2 className="text-lg font-semibold mb-3">Yearly P&L</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
          {Object.entries(summary.yearly_pnl)
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([yr, pnl]) => (
            <div key={yr} className="bg-neutral-950 border border-neutral-800 rounded p-3">
              <div className="text-neutral-500 text-xs">{yr}</div>
              <div className={`text-base font-semibold ${pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                {fmtINR(pnl)}
              </div>
            </div>
          ))}
        </div>
      </section>

      <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
        <h2 className="text-lg font-semibold mb-3">Recent trades</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-neutral-400">
              <tr>
                <th className="text-left py-2">Symbol</th>
                <th className="text-left">Signal</th>
                <th className="text-left">Entry</th>
                <th className="text-left">Exit</th>
                <th className="text-left">Reason</th>
                <th className="text-right">Score</th>
                <th className="text-right">P&L</th>
                <th className="text-right pr-4">Ret %</th>
              </tr>
            </thead>
            <tbody>
              {trades.map((t, i) => (
                <tr key={i} className="border-t border-neutral-800">
                  <td className="py-2 font-medium">{t.symbol}</td>
                  <td>{t.signal_date}</td>
                  <td>{t.entry_date}</td>
                  <td>{t.exit_date}</td>
                  <td className="text-neutral-400">{t.exit_reason}</td>
                  <td className="text-right">{t.score.toFixed(0)}</td>
                  <td className={`text-right ${t.net_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                    {fmtINR(t.net_pnl)}
                  </td>
                  <td className={`text-right pr-4 ${t.ret_pct >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                    {t.ret_pct >= 0 ? '+' : ''}{t.ret_pct.toFixed(2)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}

function Stat({ label, value, accent }: { label: string; value: string; accent?: boolean }) {
  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-3">
      <div className="text-neutral-500 text-xs">{label}</div>
      <div className={`text-base font-semibold ${accent ? 'text-amber-400' : ''}`}>{value}</div>
    </div>
  )
}

// ── Empty-state: V7.1 reference summary ──────────────────────────────────────
// Until publish_backtest.py wires the offline portfolio runner output into the
// DB, this gives operators the headline V7.1 numbers from FALCON_V71_REPORT.md
// as a static reference. Once a real summary is published the live data takes
// over and this view never shows.
//
// Source of truth: universe_engine/reports/FALCON_V71_REPORT.md (3.3-yr walk-
// forward portfolio sim on Falcon V7 patterns).

function V71BacktestSummary() {
  const yearly = [
    { yr: '2023', pct: '+83%' },
    { yr: '2024', pct: '+91%' },
    { yr: '2025', pct: '+53%' },
    { yr: '2026', pct: '+23%' },
  ]
  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold">Backtest — Falcon V7.1 (offline reference)</h1>
        <p className="text-sm text-neutral-400">
          Portfolio walk-forward, 3.3 years, ₹30L starting capital, MTF deployment, V7 patterns
        </p>
      </header>

      <div className="bg-amber-500/10 border border-amber-500/30 rounded p-3 text-xs text-amber-200">
        ⓘ Showing reference numbers from <code className="text-amber-300">FALCON_V71_REPORT.md</code>.
        Live in-DB summary will appear here after the next R&D run pipes results in
        (forthcoming <code>publish_backtest.py</code> utility).
      </div>

      <section className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
        <Stat label="Starting"   value="₹30 L" />
        <Stat label="Ending"     value="₹1.05 Cr" accent />
        <Stat label="Total P&L"  value="+₹75 L"   accent />
        <Stat label="Return"     value="+250%"   accent />
        <Stat label="Max DD"     value="-10%" />
        <Stat label="Years +ve"  value="4 / 4" />
        <Stat label="Period"     value="2023→2026" />
        <Stat label="Engine"     value="V7.1"    accent />
      </section>

      <section className="bg-neutral-900 border border-neutral-800 rounded p-4">
        <h2 className="text-lg font-semibold mb-3">Yearly returns (on ₹30 L base)</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
          {yearly.map(y => (
            <div key={y.yr} className="bg-neutral-950 border border-neutral-800 rounded p-3">
              <div className="text-neutral-500 text-xs">{y.yr}</div>
              <div className="text-base font-semibold text-emerald-400">{y.pct}</div>
            </div>
          ))}
        </div>
        <p className="text-xs text-neutral-500 mt-3">
          Every year positive. 2023-2024 carried by trend regime; 2025-2026 modest but still positive.
          Engine independently rediscovered weekly_close_loc + weekly_range + ATR signature plus a new
          drawdown-bounce pattern class.
        </p>
      </section>

      <section className="bg-neutral-900 border border-neutral-800 rounded p-4 text-xs text-neutral-500 space-y-2">
        <div className="text-sm text-neutral-300 font-medium mb-2">How to refresh these numbers</div>
        <pre className="bg-neutral-950 border border-neutral-800 rounded p-3 overflow-x-auto text-neutral-300">
python universe_engine/scripts/falcon_v71_run.py        # offline portfolio sim
python backend/falcon/scripts/publish_backtest.py        # publishes summary + trades
        </pre>
        <div>
          (<code>publish_backtest.py</code> is a forthcoming admin utility — once it lands, the live
          summary above will replace this static reference card automatically.)
        </div>
      </section>
    </div>
  )
}
