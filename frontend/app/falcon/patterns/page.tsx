'use client'
import { useEffect, useState } from 'react'
import { FalconAPI, FalconPattern } from '../../../lib/falcon-api'

export default function FalconPatternsPage() {
  const [patterns, setPatterns] = useState<FalconPattern[]>([])
  const [stats, setStats] = useState<{
    total_promoted: number
    by_classification: Array<{ classification: string; n: number }>
    by_target:         Array<{ outcome_target: string; n: number }>
  } | null>(null)
  const [classification, setClassification] = useState<string>('')
  const [target, setTarget] = useState<string>('')
  const [minLift, setMinLift] = useState<number>(5)
  const [sort, setSort] = useState<'oos_lift' | 'is_lift' | 'n_obs'>('oos_lift')

  useEffect(() => {
    FalconAPI.patternStats().then(setStats).catch(() => {})
  }, [])

  useEffect(() => {
    FalconAPI.patterns({
      limit: 200,
      classification: classification || undefined,
      target: target || undefined,
      minOosLift: minLift,
      sort,
    }).then(setPatterns).catch(() => {})
  }, [classification, target, minLift, sort])

  return (
    <div className="space-y-4">
      <h1 className="text-2xl font-bold">Promoted Patterns</h1>

      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
          <Stat label="Total promoted" value={String(stats.total_promoted)} accent />
          {stats.by_classification.map(b => (
            <Stat key={b.classification} label={b.classification} value={String(b.n)} />
          ))}
        </div>
      )}

      <div className="flex flex-wrap gap-3 items-center text-sm">
        <select className="bg-neutral-900 border border-neutral-800 rounded px-2 py-1"
                value={classification} onChange={e => setClassification(e.target.value)}>
          <option value="">All classifications</option>
          <option>universal</option>
          <option>regime_dependent</option>
          <option>sector_specific</option>
        </select>
        <select className="bg-neutral-900 border border-neutral-800 rounded px-2 py-1"
                value={target} onChange={e => setTarget(e.target.value)}>
          <option value="">All targets</option>
          <option>hit_10pc_20d</option>
          <option>hit_15pc_20d</option>
          <option>hit_25pc_30d</option>
          <option>hit_40pc_40d</option>
        </select>
        <label className="text-neutral-400">Min OOS lift (pp):</label>
        <input type="number" className="bg-neutral-900 border border-neutral-800 rounded px-2 py-1 w-20"
               value={minLift} onChange={e => setMinLift(Number(e.target.value))} />
        <select className="bg-neutral-900 border border-neutral-800 rounded px-2 py-1"
                value={sort} onChange={e => setSort(e.target.value as 'oos_lift' | 'is_lift' | 'n_obs')}>
          <option value="oos_lift">Sort: OOS lift</option>
          <option value="is_lift">Sort: IS lift</option>
          <option value="n_obs">Sort: n_obs</option>
        </select>
        <span className="text-neutral-400">{patterns.length} patterns</span>
      </div>

      <div className="bg-neutral-900 border border-neutral-800 rounded">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-neutral-400 bg-neutral-950">
              <tr>
                <th className="text-left px-4 py-2">PID</th>
                <th className="text-left">Class</th>
                <th className="text-left">Year</th>
                <th className="text-left">Scope</th>
                <th className="text-left">Target</th>
                <th className="text-right">n_obs</th>
                <th className="text-right">IS lift</th>
                <th className="text-right">OOS lift</th>
                <th className="text-right">yrs</th>
                <th className="text-left pl-4">Rule</th>
              </tr>
            </thead>
            <tbody>
              {patterns.map(p => (
                <tr key={p.pattern_id} className="border-t border-neutral-800">
                  <td className="px-4 py-2 text-neutral-500">{p.pattern_id}</td>
                  <td>
                    <span className={
                      p.classification === 'universal' ? 'text-emerald-400'
                        : p.classification === 'regime_dependent' ? 'text-amber-400'
                        : 'text-neutral-400'
                    }>{p.classification}</span>
                  </td>
                  <td>{p.mined_year}</td>
                  <td className="text-neutral-400">{p.scope}</td>
                  <td>{p.outcome_target}</td>
                  <td className="text-right">{p.n_obs}</td>
                  <td className="text-right">+{p.is_lift_pp.toFixed(1)}pp</td>
                  <td className="text-right text-amber-400">+{p.avg_oos_year_lift_pp.toFixed(1)}pp</td>
                  <td className="text-right">{p.n_years_passed}</td>
                  <td className="pl-4 font-mono text-xs text-neutral-300">{p.rule_text}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

function Stat({ label, value, accent }: { label: string; value: string; accent?: boolean }) {
  return (
    <div className="bg-neutral-900 border border-neutral-800 rounded p-3">
      <div className="text-neutral-500 text-xs">{label}</div>
      <div className={`text-lg font-semibold ${accent ? 'text-amber-400' : ''}`}>{value}</div>
    </div>
  )
}
