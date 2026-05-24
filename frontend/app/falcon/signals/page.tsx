'use client'
import { useEffect, useMemo, useState } from 'react'
import { FalconAPI, FalconSignalDay, FalconSignalsToday } from '../../../lib/falcon-api'

export default function FalconSignalsPage() {
  const [days,    setDays]    = useState<FalconSignalDay[]>([])
  const [active,  setActive]  = useState<string | null>(null)
  const [signals, setSignals] = useState<FalconSignalsToday | null>(null)
  const [topN,    setTopN]    = useState(25)
  const [err,     setErr]     = useState<string | null>(null)

  useEffect(() => {
    FalconAPI.signalDates(60).then(d => {
      setDays(d)
      if (d.length) setActive(d[0].signal_date)
    }).catch(e => setErr(String(e)))
  }, [])

  useEffect(() => {
    if (!active) return
    FalconAPI.signalsByDate(active, topN).then(setSignals).catch(e => setErr(String(e)))
  }, [active, topN])

  const totalScore = useMemo(
    () => signals?.picks.reduce((s, p) => s + p.score, 0) ?? 0, [signals])

  if (err) return <div className="text-red-400 p-4">Error: {err}</div>

  return (
    <div className="space-y-4">
      <div className="flex items-baseline justify-between">
        <h1 className="text-2xl font-bold">Live Signals</h1>
        <div className="text-sm text-neutral-400">
          {days.length} dates · engine {signals?.engine_version ?? '…'}
        </div>
      </div>

      <div className="flex gap-3 items-center">
        <select className="bg-neutral-900 border border-neutral-800 rounded px-2 py-1 text-sm"
                value={active ?? ''} onChange={e => setActive(e.target.value)}>
          {days.map(d => (
            <option key={d.signal_date} value={d.signal_date}>
              {d.signal_date} ({d.n_picks} picks)
            </option>
          ))}
        </select>
        <label className="text-sm text-neutral-400">Top:</label>
        <select className="bg-neutral-900 border border-neutral-800 rounded px-2 py-1 text-sm"
                value={topN} onChange={e => setTopN(Number(e.target.value))}>
          {[10, 25, 50, 100].map(n => <option key={n}>{n}</option>)}
        </select>
        {signals && <span className="text-sm text-neutral-400">
          aggregate score: <span className="text-amber-400">{totalScore.toFixed(0)}</span>
        </span>}
      </div>

      {signals && (
        <div className="bg-neutral-900 border border-neutral-800 rounded">
          <div className="px-4 py-3 border-b border-neutral-800 text-sm">
            <span className="text-neutral-400">Trade on:</span>{' '}
            <strong className="text-amber-400">{signals.entry_date ?? '—'}</strong>
            <span className="text-neutral-500"> · </span>
            <span className="text-neutral-400">Signal date:</span> {signals.signal_date}
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="text-neutral-400 bg-neutral-950">
                <tr>
                  <th className="text-left px-4 py-2">#</th>
                  <th className="text-left">Symbol</th>
                  <th className="text-left">Sector</th>
                  <th className="text-right">Close</th>
                  <th className="text-right">Score</th>
                  <th className="text-right">n_fires</th>
                  <th className="text-right pr-4">Liquidity 60d</th>
                </tr>
              </thead>
              <tbody>
                {signals.picks.map(p => (
                  <tr key={p.symbol} className="border-t border-neutral-800 hover:bg-neutral-800/30">
                    <td className="px-4 py-2">{p.rank}</td>
                    <td className="font-medium">{p.symbol}</td>
                    <td className="text-neutral-400">{p.sector ?? '—'}</td>
                    <td className="text-right">₹{p.close_at_signal?.toFixed(2) ?? '—'}</td>
                    <td className="text-right text-amber-400">{p.score.toFixed(0)}</td>
                    <td className="text-right">{p.n_fires}</td>
                    <td className="text-right pr-4 text-neutral-400">
                      {p.avg_value_60d
                        ? `₹${(p.avg_value_60d / 1e7).toFixed(1)} Cr`
                        : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
