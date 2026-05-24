'use client'

// Falcon — Pre-Market Console (Phase 2.3)
// Workflow:
//   16:05 IST → EOD orchestrator stages NEW_ENTRY + BULK_ADOPT for next trading day
//   evening   → operator reviews + edits SLs + clicks Confirm & Schedule
//   9:14 IST  → operator refreshes Kite token (manual; banner shows status)
//   9:15 IST  → pre-market deployer fires QUEUED items, marks DEPLOYED/FAILED
//
// Single page, three sections: token banner / new entries / bulk adopts.

import { useCallback, useEffect, useRef, useState } from 'react'
import {
  FalconAPI,
  type PremarketItem,
  type PremarketListResponse,
  type PremarketStatus,
  type KiteTokenStatus,
} from '../../../lib/falcon-api'
import { PreflightBanner } from '../../../components/PreflightBanner'

const inr = (v: number | null | undefined) =>
  v == null ? '—' : new Intl.NumberFormat('en-IN', { maximumFractionDigits: 0 }).format(Math.round(v))

const fmt2 = (v: number | null | undefined) => v == null ? '—' : v.toFixed(2)

type ProductFilter = 'ALL' | 'MTF' | 'CNC'

export default function PremarketPage() {
  const [data, setData]       = useState<PremarketListResponse | null>(null)
  const [error, setError]     = useState<string | null>(null)
  const [busy, setBusy]       = useState<string | null>(null)
  const [flash, setFlash]     = useState<string | null>(null)
  const [productFilter, setProductFilter] = useState<ProductFilter>('ALL')
  const [selectedIds, setSelectedIds]     = useState<Set<number>>(new Set())
  // Operator preference 2026-05-11: DEPLOYED + FAILED rows are terminal —
  // they fired (or didn't) on Kite, the live state is /falcon/positions.
  // Default: hide them from the active queue so the page reflects "what
  // needs my attention NOW," not "what happened today." Toggle to reveal.
  const [showHistory, setShowHistory] = useState(false)

  // Which target_date is being viewed. Empty string = default (next deploy
  // window). Date picker lets operators inspect past/future days; the
  // preflight 'staging_no_stale' warning links here to clear leftover rows.
  const [viewDate, setViewDate] = useState<string>('')

  const refresh = useCallback(async () => {
    try {
      const r = await FalconAPI.premarketList(viewDate || undefined)
      setData(r)
      setError(null)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }, [viewDate])

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, 30000)
    return () => clearInterval(id)
  }, [refresh])

  const triggerStage = async () => {
    setBusy('stage')
    try {
      const r = await FalconAPI.premarketStageNow()
      setFlash(`Staged: ${r.total_staged} items for ${r.target_date}`)
      setTimeout(() => setFlash(null), 4000)
      await refresh()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(null)
    }
  }

  // Filter-aware confirm: only queues STAGED items matching current product filter.
  // No more confirm-all-by-date — that swept up CNC items when user only wanted MTF.
  const confirmFiltered = async () => {
    if (!data) return
    const itemProductLocal = (it: PremarketItem): string => {
      const p = it.payload as Record<string, unknown>
      return (p.product as string) || ''
    }
    const matchesProductLocal = (it: PremarketItem) => {
      if (productFilter === 'ALL') return true
      // NEW_ENTRY items don't carry product on payload — they default to MTF
      // per the trade-page config. Exclude them from CNC-only filter.
      const prod = itemProductLocal(it) || (it.kind === 'NEW_ENTRY' ? 'MTF' : '')
      return prod === productFilter
    }
    const ids = data.items
      .filter(it => it.status === 'STAGED' && matchesProductLocal(it))
      .map(it => it.id)
    if (ids.length === 0) {
      setFlash('No STAGED items match the current filter — nothing to confirm.')
      setTimeout(() => setFlash(null), 4000)
      return
    }
    setBusy('confirm-all')
    try {
      const r = await FalconAPI.premarketConfirm(ids)
      setFlash(`${r.n_confirmed} items queued for 9:15 IST (filter: ${productFilter})`)
      setTimeout(() => setFlash(null), 4000)
      await refresh()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(null)
    }
  }

  const cancelItems = async (ids: number[]) => {
    if (!ids.length) return
    setBusy(`cancel-${ids[0]}`)
    try {
      await FalconAPI.premarketCancel(ids)
      await refresh()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(null)
    }
  }

  const patchItem = async (id: number, patch: { sl_price?: number; qty?: number }) => {
    setBusy(`patch-${id}`)
    try {
      await FalconAPI.premarketPatchItem(id, patch)
      await refresh()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(null)
    }
  }

  const deployNow = async () => {
    if (!confirm('Force-deploy NOW? This bypasses the 9:14-9:30 IST window check. Use only for testing or recovery.')) return
    setBusy('deploy')
    try {
      const r = await FalconAPI.premarketDeployNow()
      setFlash(`Deploy cycle ran: ${JSON.stringify(r.result).slice(0, 200)}`)
      setTimeout(() => setFlash(null), 6000)
      await refresh()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(null)
    }
  }

  if (error && !data) {
    return (
      <div className="bg-red-500/10 border border-red-500/30 rounded p-4 text-red-200">
        ✗ {error}
        <button onClick={refresh} className="ml-3 underline">retry</button>
      </div>
    )
  }
  if (!data) return <div className="text-neutral-500">Loading…</div>

  // Filtering helpers
  const itemProduct = (it: PremarketItem): string => {
    const p = it.payload as Record<string, unknown>
    return (p.product as string) || ''
  }
  const matchesProduct = (it: PremarketItem) => {
    if (productFilter === 'ALL') return true
    return itemProduct(it) === productFilter
  }
  // Active queue = STAGED + QUEUED only. DEPLOYED + FAILED are terminal:
  //   DEPLOYED → live position now owned in Kite (visible at /falcon/positions)
  //   FAILED   → deployer hit a broker error, nothing on Kite, audit-only
  // Showing them in the "active queue" view forever makes the page look
  // stale when the user has already exited the live position. Toggle to reveal.
  // CANCELLED is hard-deleted at the API layer; defensive filter retained.
  const isTerminal = (i: PremarketItem) => i.status === 'DEPLOYED' || i.status === 'FAILED'
  const showRow    = (i: PremarketItem) => {
    if (i.status === 'CANCELLED') return false
    if (isTerminal(i) && !showHistory) return false
    return true
  }

  const newEntries = data.items.filter(i => i.kind === 'NEW_ENTRY' && showRow(i))
  const adopts     = data.items.filter(i => i.kind === 'BULK_ADOPT' && matchesProduct(i) && showRow(i))
  const hiddenTerminalCount =
      data.items.filter(i => isTerminal(i)).length - data.items.filter(i => isTerminal(i) && showRow(i)).length

  const counts = {
    staged:    data.items.filter(i => i.status === 'STAGED').length,
    queued:    data.items.filter(i => i.status === 'QUEUED').length,
    deployed:  data.items.filter(i => i.status === 'DEPLOYED').length,
    failed:    data.items.filter(i => i.status === 'FAILED').length,
    cancelled: data.items.filter(i => i.status === 'CANCELLED').length,
  }
  const productCounts = {
    MTF:  data.items.filter(i => i.kind === 'BULK_ADOPT' && itemProduct(i) === 'MTF').length,
    CNC:  data.items.filter(i => i.kind === 'BULK_ADOPT' && itemProduct(i) === 'CNC').length,
  }
  // STAGED items matching current product filter — what the Confirm button will queue.
  // Includes NEW_ENTRY items (default product = MTF) when MTF or ALL filter is active.
  const stagedInFilter = data.items.filter(it => {
    if (it.status !== 'STAGED') return false
    if (productFilter === 'ALL') return true
    const prod = itemProduct(it) || (it.kind === 'NEW_ENTRY' ? 'MTF' : '')
    return prod === productFilter
  }).length

  // Bulk-select state
  const visibleAdoptIds = adopts.filter(i => i.status === 'STAGED' || i.status === 'QUEUED').map(i => i.id)
  const allVisibleSelected = visibleAdoptIds.length > 0 && visibleAdoptIds.every(id => selectedIds.has(id))
  const toggleAllVisible = () => {
    setSelectedIds(prev => {
      const next = new Set(prev)
      if (allVisibleSelected) visibleAdoptIds.forEach(id => next.delete(id))
      else visibleAdoptIds.forEach(id => next.add(id))
      return next
    })
  }
  const toggleOne = (id: number) => {
    setSelectedIds(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      return next
    })
  }
  const clearSelection = () => setSelectedIds(new Set())

  const bulkConfirmSelected = async () => {
    const ids = [...selectedIds].filter(id => {
      const it = data.items.find(i => i.id === id)
      return it && it.status === 'STAGED'
    })
    if (!ids.length) return
    setBusy('bulk-confirm')
    try {
      const r = await FalconAPI.premarketConfirm(ids)
      setFlash(`${r.n_confirmed} items queued for 9:15 IST`)
      setTimeout(() => setFlash(null), 4000)
      clearSelection()
      await refresh()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(null)
    }
  }

  const bulkCancelSelected = async () => {
    const ids = [...selectedIds]
    if (!ids.length) return
    if (!confirm(`Cancel ${ids.length} selected item(s)?`)) return
    setBusy('bulk-cancel')
    try {
      const r = await FalconAPI.premarketCancel(ids)
      setFlash(`${r.n_cancelled} items cancelled`)
      setTimeout(() => setFlash(null), 4000)
      clearSelection()
      await refresh()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(null)
    }
  }

  return (
    <div className="space-y-6">
      <header className="flex items-baseline justify-between">
        <div className="flex-1">
          <h1 className="text-2xl font-bold">Pre-Market Console</h1>
          <div className="mt-1 flex flex-wrap items-center gap-3 text-sm">
            <span className="text-neutral-400">Viewing orders for</span>
            <input
              type="date"
              value={data.target_date}
              onChange={e => setViewDate(e.target.value)}
              className="bg-neutral-950 border border-neutral-700 rounded px-2 py-1 text-sm font-mono text-amber-300"
              title="Change to view past/future target dates. Default is the next 9:15 IST deploy window."
            />
            <DateRelativeBadge target={data.target_date} />
            {viewDate && viewDate !== _nextDeployTargetISTSync() && (
              <button
                onClick={() => setViewDate('')}
                className="text-xs text-neutral-500 hover:text-neutral-200 underline">
                reset to next deploy day
              </button>
            )}
            <span className="text-neutral-500">·</span>
            <span className="text-neutral-400">
              Items here fire on Kite at <span className="text-amber-300">9:15 IST that day</span>
            </span>
          </div>
        </div>
        <StageIndicator counts={counts} />
      </header>

      <PreflightBanner />
      <TokenBanner status={data.token_status} />

      {error && (
        <div className="bg-red-500/10 border border-red-500/30 rounded p-3 text-sm text-red-200">
          ✗ {error}
        </div>
      )}
      {flash && (
        <div className="bg-green-500/10 border border-green-500/30 rounded p-3 text-sm text-green-200">
          ✓ {flash}
        </div>
      )}

      {/* Top action bar */}
      <section className="bg-neutral-900 border border-neutral-800 rounded p-4 flex flex-wrap items-center gap-3">
        <button
          onClick={triggerStage}
          disabled={busy === 'stage'}
          title="Refresh tomorrow's NEW_ENTRY signals from the engine. Bulk adopts are NOT auto-staged — use /positions → Stage Bulk Adopt."
          className="px-3 py-1.5 bg-neutral-800 text-neutral-100 rounded text-sm hover:bg-neutral-700 disabled:opacity-50">
          {busy === 'stage' ? 'Staging…' : "Refresh tomorrow's signals"}
        </button>
        <button
          onClick={confirmFiltered}
          disabled={busy === 'confirm-all' || stagedInFilter === 0}
          title={productFilter === 'ALL'
            ? `Queue all ${stagedInFilter} STAGED items`
            : `Queue ${stagedInFilter} STAGED ${productFilter} items only`}
          className="px-3 py-1.5 bg-amber-500 text-neutral-950 rounded text-sm font-semibold hover:bg-amber-400 disabled:opacity-30 disabled:cursor-not-allowed">
          {busy === 'confirm-all'
            ? 'Confirming…'
            : `Confirm & Schedule for 9:15 IST (${stagedInFilter}${productFilter === 'ALL' ? '' : ' ' + productFilter})`}
        </button>
        <button
          onClick={deployNow}
          disabled={busy === 'deploy' || counts.queued === 0}
          className="px-3 py-1.5 border border-red-500/50 text-red-300 rounded text-sm hover:bg-red-500/10 disabled:opacity-30 disabled:cursor-not-allowed ml-auto">
          ⚠ Deploy now (force)
        </button>
        {/* Always show the toggle so operators can find it. Disabled when
            there's nothing to reveal (no DEPLOYED/FAILED rows for this date). */}
        <button
          onClick={() => setShowHistory(v => !v)}
          disabled={counts.deployed + counts.failed === 0}
          title="DEPLOYED + FAILED rows are terminal (already fired or failed on Kite). Hidden by default to keep this view focused on what needs action. Live state of held positions is /falcon/positions."
          className="px-3 py-1.5 text-xs border border-neutral-700 text-neutral-400 hover:text-neutral-100 rounded disabled:opacity-40 disabled:cursor-not-allowed">
          {showHistory
            ? `Hide history (${counts.deployed + counts.failed})`
            : `Show history (${counts.deployed + counts.failed})`}
        </button>
        <button
          onClick={refresh}
          className="px-3 py-1.5 text-neutral-400 hover:text-neutral-100 text-sm">
          Refresh
        </button>
      </section>

      {/* Terminal-state hint: empty active view + deployed-today count */}
      {!showHistory && hiddenTerminalCount > 0 && newEntries.length === 0 && adopts.length === 0 && (
        <div className="bg-neutral-900 border border-neutral-800 rounded p-4 text-sm text-neutral-400">
          Active queue is empty. <span className="text-green-300 font-semibold">{counts.deployed}</span> deployed
          {counts.failed > 0 && <> · <span className="text-red-300 font-semibold">{counts.failed}</span> failed</>}
          {' '}today. Click <strong>Show history</strong> above to inspect, or open{' '}
          <a href="/falcon/positions" className="text-amber-400 underline">/falcon/positions</a> for live state.
        </div>
      )}

      {/* New entries section */}
      <Section
        title={`New entries — tomorrow's signals (${newEntries.length})`}
        empty="No NEW_ENTRY signals staged. Check that the daily pipeline ran (Today page or Admin → Runs)."
      >
        {newEntries.length > 0 && (
          <NewEntriesTable items={newEntries} onCancel={cancelItems} />
        )}
      </Section>

      {/* Bulk adopt section */}
      {/* Filter + bulk-action bar — product filter only.
          Status filtering removed: the top-right summary chips already show
          the metro-style stage breakdown, and each row's status pill shows
          where that item is. Six redundant filter buttons added more noise
          than signal. */}
      <section className="bg-neutral-900 border border-neutral-800 rounded p-3 flex flex-wrap items-center gap-3 text-xs">
        <span className="text-neutral-500">Product:</span>
        {(['ALL', 'MTF', 'CNC'] as ProductFilter[]).map(p => (
          <button
            key={p}
            onClick={() => { setProductFilter(p); clearSelection() }}
            className={`px-2 py-0.5 rounded font-mono ${
              productFilter === p
                ? 'bg-amber-500/20 text-amber-300 border border-amber-500/40'
                : 'bg-neutral-800 text-neutral-400 hover:text-neutral-100'
            }`}>
            {p === 'ALL' ? `ALL (${productCounts.MTF + productCounts.CNC})` : `${p} (${productCounts[p]})`}
          </button>
        ))}
        {selectedIds.size > 0 && (
          <div className="ml-auto flex items-center gap-2">
            <span className="text-amber-300 font-semibold">{selectedIds.size} selected</span>
            <button
              onClick={bulkConfirmSelected}
              disabled={busy === 'bulk-confirm'}
              className="px-3 py-1 bg-amber-500 text-neutral-950 rounded font-semibold hover:bg-amber-400 disabled:opacity-50">
              Confirm selected
            </button>
            <button
              onClick={bulkCancelSelected}
              disabled={busy === 'bulk-cancel'}
              className="px-3 py-1 border border-red-500/50 text-red-300 rounded hover:bg-red-500/10 disabled:opacity-50">
              Cancel selected
            </button>
            <button onClick={clearSelection} className="text-neutral-500 hover:text-neutral-300">Clear</button>
          </div>
        )}
      </section>

      <Section
        title={`Bulk adopt — existing positions (${adopts.length}${data.items.filter(i => i.kind === 'BULK_ADOPT').length !== adopts.length ? ` of ${data.items.filter(i => i.kind === 'BULK_ADOPT').length}` : ''})`}
        empty="No BULK_ADOPT items match the current filter. Try ALL / ALL filters."
      >
        {adopts.length > 0 && (
          <BulkAdoptTable
            items={adopts}
            selectedIds={selectedIds}
            allVisibleSelected={allVisibleSelected}
            onToggleAll={toggleAllVisible}
            onToggleOne={toggleOne}
            onPatch={patchItem}
            onCancel={cancelItems}
            busy={busy}
          />
        )}
      </Section>

      <DeployerCard />
    </div>
  )
}

// ── Date helpers — clarify "tomorrow" vs "today" vs "Mon 12 May" ────────────

function _nextDeployTargetISTSync(): string {
  // Mirrors backend eod_orchestrator._next_deploy_target_ist(): if past 9:15
  // IST today, the next deploy window is tomorrow (weekday-only).
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata', year: 'numeric', month: '2-digit', day: '2-digit',
  })
  const istNow = new Date()
  const istHm = new Intl.DateTimeFormat('en-IN', {
    timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', hour12: false,
  }).formatToParts(istNow)
  const hh = Number(istHm.find(p => p.type === 'hour')?.value ?? 0)
  const mm = Number(istHm.find(p => p.type === 'minute')?.value ?? 0)
  const istToday = fmt.format(istNow)        // YYYY-MM-DD in IST
  const istWeekday = new Date(istToday + 'T00:00:00').getUTCDay()  // 0=Sun..6=Sat
  const before915 = hh < 9 || (hh === 9 && mm < 15)
  let d = new Date(istToday + 'T00:00:00Z')
  if (!(istWeekday >= 1 && istWeekday <= 5 && before915)) {
    d.setUTCDate(d.getUTCDate() + 1)
  }
  while ([0, 6].includes(d.getUTCDay())) d.setUTCDate(d.getUTCDate() + 1)
  return d.toISOString().slice(0, 10)
}

function DateRelativeBadge({ target }: { target: string }) {
  // Render a friendly tag: "today", "tomorrow", "Mon 12 May", or "(2d ago)"
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata', year: 'numeric', month: '2-digit', day: '2-digit',
  })
  const todayIST = fmt.format(new Date())
  const tgt = new Date(target + 'T00:00:00')
  const today = new Date(todayIST + 'T00:00:00')
  const diff = Math.round((tgt.getTime() - today.getTime()) / 86_400_000)
  let label: string, cls: string
  if (diff === 0)      { label = 'today';        cls = 'bg-amber-500/20 text-amber-200 border-amber-500/40' }
  else if (diff === 1) { label = 'tomorrow';     cls = 'bg-blue-500/20 text-blue-200 border-blue-500/40' }
  else if (diff > 1)   { label = `in ${diff}d`;  cls = 'bg-blue-500/10 text-blue-300 border-blue-500/30' }
  else if (diff === -1){ label = 'yesterday';    cls = 'bg-neutral-700/40 text-neutral-300 border-neutral-600' }
  else                 { label = `${-diff}d ago`; cls = 'bg-neutral-700/40 text-neutral-300 border-neutral-600' }
  const dow = tgt.toLocaleDateString('en-IN', { weekday: 'short', timeZone: 'Asia/Kolkata' })
  const md  = tgt.toLocaleDateString('en-IN', { day: 'numeric', month: 'short', timeZone: 'Asia/Kolkata' })
  return (
    <span className={`text-xs font-semibold px-2 py-0.5 rounded border ${cls}`}>
      {label} · {dow} {md}
    </span>
  )
}


function TokenBanner({ status }: { status: KiteTokenStatus }) {
  if (status.valid) {
    return (
      <div className="bg-green-500/10 border border-green-500/30 rounded p-3 flex items-center justify-between">
        <span className="text-sm text-green-200">
          ✓ Kite token <span className="font-mono">valid</span>
          {status.expires_at ? ` · expires ${status.expires_at}` : ''}
          {status.age_hours != null ? ` · age ${status.age_hours.toFixed(1)}h` : ''}
        </span>
        <span className="text-xs text-neutral-500">Pre-market deploy ready</span>
      </div>
    )
  }
  return (
    <div className="bg-red-500/10 border border-red-500/40 rounded p-3 flex items-center justify-between">
      <div>
        <div className="text-sm text-red-200 font-semibold">⚠ Kite token invalid — refresh required before 9:14 IST</div>
        <div className="text-xs text-red-300/70">{status.reason || 'Reason unknown — visit /admin/kite to refresh.'}</div>
      </div>
      <a href="/admin" className="px-3 py-1.5 bg-red-500 text-neutral-950 rounded text-sm font-semibold">Refresh now</a>
    </div>
  )
}

function Section({ title, empty, children }: { title: string; empty: string; children: React.ReactNode }) {
  const isEmpty = !children || (Array.isArray(children) && children.length === 0)
  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded">
      <div className="px-4 py-3 border-b border-neutral-800 flex items-baseline justify-between">
        <h2 className="text-sm font-semibold text-amber-300">{title}</h2>
      </div>
      <div className="p-4">
        {isEmpty
          ? <div className="text-sm text-neutral-500">{empty}</div>
          : children}
      </div>
    </section>
  )
}

// ── Status pill colour scheme ───────────────────────────────────────────────
// Single visual indicator on each row — like the "next station" LED on a metro
// map. Used in tables only; the page chrome stays neutral.
//   STAGED   → blue  (staged, awaiting your approval)
//   QUEUED   → amber (committed, will fire at 9:15 IST — armed)
//   DEPLOYED → green (success — Kite has the order)
//   FAILED   → red   (something went wrong, see error)

const STATUS_PILL_CLASS: Record<PremarketStatus, string> = {
  STAGED:    'bg-blue-500/20  text-blue-300   border border-blue-500/40',
  QUEUED:    'bg-amber-500/20 text-amber-300  border border-amber-500/40',
  DEPLOYED:  'bg-green-500/20 text-green-300  border border-green-500/40',
  FAILED:    'bg-red-500/20   text-red-300    border border-red-500/40',
  CANCELLED: 'bg-neutral-800  text-neutral-500',
}

function StatusPill({ status }: { status: PremarketStatus }) {
  return <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${STATUS_PILL_CLASS[status]}`}>{status}</span>
}

// ── Top-right metro-style stage indicator ───────────────────────────────────
// Train signal LEDs at the top of the page. Three states:
//   GREEN  solid       → items have passed/arrived at this stage (completed)
//   AMBER  blinking    → current "armed" stage waiting for the next event
//   GREY   dim         → empty / no items here yet
// Failed = red side-branch (only visible when count > 0).

function StageIndicator({ counts }: {
  counts: { staged: number; queued: number; deployed: number; failed: number; cancelled: number }
}) {
  const stages: Array<{ key: 'staged' | 'queued' | 'deployed'; label: string }> = [
    { key: 'staged',   label: 'STAGED' },
    { key: 'queued',   label: 'QUEUED' },
    { key: 'deployed', label: 'DEPLOYED' },
  ]
  return (
    <div className="flex items-center gap-3">
      {stages.map((s, i) => {
        const n = counts[s.key]
        const lit = n > 0
        // The "current waiting" stage is the latest non-empty one before deployed.
        // STAGED amber when items staged but none queued. QUEUED amber when items
        // queued but none deployed. After deploy, every stage flips green.
        const isPending = (s.key === 'queued' && counts.queued > 0 && counts.deployed === 0)
                       || (s.key === 'staged' && counts.staged > 0 && counts.queued === 0)

        let dotStyle = ''
        let labelCls = ''
        if (!lit) {
          dotStyle = 'bg-neutral-800 ring-1 ring-neutral-700'
          labelCls = 'text-neutral-600'
        } else if (isPending) {
          // Amber LED with glow + custom blink
          dotStyle = 'bg-amber-400 ring-2 ring-amber-300 shadow-[0_0_12px_rgba(251,191,36,0.85)] animate-[ledblink_1s_ease-in-out_infinite]'
          labelCls = 'text-amber-300 font-bold'
        } else {
          // Green LED with subtle glow
          dotStyle = 'bg-green-500 ring-2 ring-green-400 shadow-[0_0_8px_rgba(34,197,94,0.6)]'
          labelCls = 'text-green-400 font-semibold'
        }

        return (
          <span key={s.key} className="flex items-center gap-2 text-sm font-mono">
            {i > 0 && <span className="text-neutral-700 text-base">→</span>}
            <span className={`w-3 h-3 rounded-full ${dotStyle}`} />
            <span className={labelCls}>
              <span className="font-bold">{n}</span> <span className="text-[11px]">{s.label}</span>
            </span>
          </span>
        )
      })}
      {counts.failed > 0 && (
        <span className="flex items-center gap-2 ml-2 pl-3 border-l border-neutral-800 text-sm font-mono">
          <span className="w-3 h-3 rounded-full bg-red-500 ring-2 ring-red-400 shadow-[0_0_8px_rgba(239,68,68,0.7)]" />
          <span className="text-red-400 font-semibold">
            <span className="font-bold">{counts.failed}</span> <span className="text-[11px]">FAILED</span>
          </span>
        </span>
      )}
    </div>
  )
}

function OriginPill({ origin }: { origin: string | undefined }) {
  if (!origin) return <span className="text-[9px] text-neutral-600 font-mono">EOD</span>
  const map: Record<string, { label: string; cls: string }> = {
    TRADE_PAGE:     { label: 'from /trade',     cls: 'text-purple-300' },
    POSITIONS_PAGE: { label: 'from /positions', cls: 'text-blue-300' },
  }
  const m = map[origin]
  if (!m) return <span className="text-[9px] text-neutral-600 font-mono">{origin}</span>
  return <span className={`text-[9px] font-mono ${m.cls}`}>{m.label}</span>
}

function NewEntriesTable({ items, onCancel }: {
  items: PremarketItem[]
  onCancel: (ids: number[]) => Promise<void>
}) {
  // Local selection state (independent of bulk-adopt selection above).
  const [picked, setPicked] = useState<Set<number>>(new Set())
  const cancellableIds = items
    .filter(i => i.status === 'STAGED' || i.status === 'QUEUED')
    .map(i => i.id)
  const allSelected   = cancellableIds.length > 0 && cancellableIds.every(id => picked.has(id))
  const someSelected  = picked.size > 0 && !allSelected
  const headerRef     = useRef<HTMLInputElement | null>(null)
  useEffect(() => {
    if (headerRef.current) headerRef.current.indeterminate = someSelected
  }, [someSelected])

  const toggleAll = () => {
    setPicked(prev => {
      const next = new Set(prev)
      if (allSelected) cancellableIds.forEach(id => next.delete(id))
      else             cancellableIds.forEach(id => next.add(id))
      return next
    })
  }
  const toggleOne = (id: number) => {
    setPicked(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      return next
    })
  }
  const cancelSelected = async () => {
    const ids = [...picked].filter(id => cancellableIds.includes(id))
    if (ids.length === 0) return
    if (!confirm(`Cancel ${ids.length} STAGED/QUEUED new-entry items? This is a hard delete (audit history kept in falcon_trade_runs).`)) return
    await onCancel(ids)
    setPicked(new Set())
  }
  const cancelAll = async () => {
    if (cancellableIds.length === 0) return
    if (!confirm(`Cancel ALL ${cancellableIds.length} STAGED/QUEUED new-entry items?`)) return
    await onCancel(cancellableIds)
    setPicked(new Set())
  }

  return (
    <div className="overflow-x-auto -mx-4 px-4">
      {/* Bulk action bar */}
      {cancellableIds.length > 0 && (
        <div className="flex items-center gap-3 mb-2 text-xs">
          <span className={picked.size > 0 ? 'text-amber-300' : 'text-neutral-500'}>
            {picked.size} of {cancellableIds.length} selected
          </span>
          <button onClick={cancelSelected} disabled={picked.size === 0}
            className="px-2 py-1 bg-red-500/20 text-red-200 border border-red-500/40 rounded hover:bg-red-500/30 disabled:opacity-30 disabled:cursor-not-allowed">
            Cancel selected ({picked.size})
          </button>
          <button onClick={cancelAll}
            className="px-2 py-1 border border-red-500/60 text-red-300 rounded hover:bg-red-500/10">
            Cancel ALL ({cancellableIds.length})
          </button>
          {picked.size > 0 && (
            <button onClick={() => setPicked(new Set())}
              className="text-neutral-500 hover:text-neutral-200 underline">
              clear selection
            </button>
          )}
        </div>
      )}
      <table className="w-full text-xs">
        <thead className="text-neutral-400">
          <tr>
            <th className="text-center w-8">
              <input
                ref={headerRef}
                type="checkbox"
                checked={allSelected}
                onChange={toggleAll}
                title={allSelected ? 'Deselect all' : someSelected ? `${picked.size}/${cancellableIds.length} — click to select all` : 'Select all cancellable rows'}
                className="cursor-pointer accent-amber-500"
              />
            </th>
            <th className="text-left py-2">Symbol</th>
            <th className="text-left">Sector</th>
            <th className="text-right">Score</th>
            <th className="text-right">Fires</th>
            <th className="text-right">Close@signal</th>
            <th className="text-center">Status</th>
            <th className="text-right">Action</th>
          </tr>
        </thead>
        <tbody>
          {items.map(it => {
            const p = it.payload as Record<string, unknown>
            const cancellable = it.status === 'STAGED' || it.status === 'QUEUED'
            return (
              <tr key={it.id} className="border-t border-neutral-800">
                <td className="text-center">
                  {cancellable && (
                    <input type="checkbox" checked={picked.has(it.id)}
                      onChange={() => toggleOne(it.id)}
                      className="cursor-pointer accent-amber-500"
                    />
                  )}
                </td>
                <td className="py-1.5 font-medium font-mono">
                  {it.symbol}
                  <div className="mt-0.5"><OriginPill origin={p.origin as string | undefined} /></div>
                </td>
                <td className="text-left text-neutral-400">{(p.sector as string) || '—'}</td>
                <td className="text-right font-mono">{(p.score as number)?.toFixed(2) ?? '—'}</td>
                <td className="text-right font-mono">{(p.n_fires as number) ?? '—'}</td>
                <td className="text-right font-mono">{fmt2(p.close_at_signal as number)}</td>
                <td className="text-center"><StatusPill status={it.status} /></td>
                <td className="text-right">
                  {cancellable ? (
                    <button onClick={() => onCancel([it.id])}
                      className="text-red-400 hover:text-red-300 text-xs">
                      Cancel
                    </button>
                  ) : it.status === 'FAILED' ? (
                    <span className="text-red-400 text-xs" title={it.deploy_error || ''}>✗ failed</span>
                  ) : it.status === 'DEPLOYED' ? (
                    <span className="text-green-400 text-xs">✓ deployed</span>
                  ) : (
                    <span className="text-neutral-600 text-xs">—</span>
                  )}
                </td>
              </tr>
            )
          })}
        </tbody>
        <tfoot className="text-[10px] text-neutral-500">
          <tr><td colSpan={7} className="py-2">
            Per-trade ₹{inr((items[0]?.payload as Record<string, Record<string, number>>)?.config?.per_trade) || '50,000'}
            {' '}· SL {((items[0]?.payload as Record<string, Record<string, number>>)?.config?.sl_pct as unknown as number) ?? -7}%
            {' '}· hold {((items[0]?.payload as Record<string, Record<string, number>>)?.config?.hold_days as unknown as number) ?? 7}d
            {' '}· trail trigger +{((items[0]?.payload as Record<string, Record<string, number>>)?.config?.trail_trigger_pct as unknown as number) ?? 12}%
          </td></tr>
        </tfoot>
      </table>
    </div>
  )
}

function BulkAdoptTable({ items, selectedIds, allVisibleSelected, onToggleAll, onToggleOne, onPatch, onCancel, busy }: {
  items: PremarketItem[]
  selectedIds: Set<number>
  allVisibleSelected: boolean
  onToggleAll: () => void
  onToggleOne: (id: number) => void
  onPatch: (id: number, patch: { sl_price?: number; qty?: number }) => Promise<void>
  onCancel: (ids: number[]) => Promise<void>
  busy: string | null
}) {
  return (
    <div className="overflow-x-auto -mx-4 px-4">
      <table className="w-full text-xs">
        <thead className="text-neutral-400">
          <tr>
            <th className="text-center py-2 w-8">
              <input
                type="checkbox"
                checked={allVisibleSelected}
                onChange={onToggleAll}
                className="accent-amber-500 w-3.5 h-3.5"
                title="Select all visible (STAGED + QUEUED only)"
              />
            </th>
            <th className="text-left">Symbol</th>
            <th className="text-center">Product</th>
            <th className="text-right">Qty</th>
            <th className="text-right">Avg Entry</th>
            <th className="text-right">LTP</th>
            <th className="text-right">SL ₹ (editable)</th>
            <th className="text-center">Status</th>
            <th className="text-right">Action</th>
          </tr>
        </thead>
        <tbody>
          {items.map(it => {
            const p = it.payload as Record<string, number | string>
            const selectable = it.status === 'STAGED' || it.status === 'QUEUED'
            const checked = selectedIds.has(it.id)
            const rowCls = 'border-t border-neutral-800' + (checked ? ' bg-amber-500/5' : '')
            return (
              <tr key={it.id} className={rowCls}>
                <td className="text-center py-1.5">
                  <input
                    type="checkbox"
                    checked={checked}
                    disabled={!selectable}
                    onChange={() => selectable && onToggleOne(it.id)}
                    className="accent-amber-500 w-3.5 h-3.5 disabled:opacity-30 disabled:cursor-not-allowed"
                  />
                </td>
                <td className="font-medium font-mono">
                  {it.symbol}
                  <div className="mt-0.5"><OriginPill origin={p.origin as string | undefined} /></div>
                </td>
                <td className="text-center text-neutral-400 font-mono">{p.product as string}</td>
                <td className="text-right font-mono">{(p.qty as number)?.toLocaleString('en-IN')}</td>
                <td className="text-right font-mono">{fmt2(p.avg_entry as number)}</td>
                <td className="text-right font-mono text-neutral-400">{fmt2(p.current_price as number)}</td>
                <td className="text-right font-mono">
                  {it.status === 'STAGED'
                    ? <EditableSL initial={p.sl_price as number} onSave={v => onPatch(it.id, { sl_price: v })} disabled={busy === `patch-${it.id}`} />
                    : <span className="text-red-300">{fmt2(p.sl_price as number)}</span>}
                </td>
                <td className="text-center"><StatusPill status={it.status} /></td>
                <td className="text-right">
                  {(it.status === 'STAGED' || it.status === 'QUEUED') ? (
                    <button onClick={() => onCancel([it.id])}
                      className="text-red-400 hover:text-red-300 text-xs">
                      Cancel
                    </button>
                  ) : it.status === 'FAILED' ? (
                    <span className="text-red-400 text-xs" title={it.deploy_error || ''}>✗ failed</span>
                  ) : it.status === 'DEPLOYED' ? (
                    <span className="text-green-400 text-xs">✓ deployed</span>
                  ) : (
                    <span className="text-neutral-600 text-xs">—</span>
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

function EditableSL({ initial, onSave, disabled }: {
  initial: number
  onSave: (v: number) => Promise<void>
  disabled: boolean
}) {
  const [v, setV] = useState(String(initial))
  const [editing, setEditing] = useState(false)

  if (!editing) {
    return (
      <button
        onClick={() => setEditing(true)}
        className="text-red-300 font-mono hover:underline cursor-pointer">
        {Number(v).toFixed(2)}
      </button>
    )
  }

  return (
    <span className="inline-flex items-center gap-1">
      <input
        type="number" step={0.05} value={v}
        onChange={e => setV(e.target.value)}
        autoFocus
        className="w-20 bg-neutral-950 border border-neutral-700 rounded px-1.5 py-0.5 text-xs text-right font-mono" />
      <button
        disabled={disabled}
        onClick={async () => {
          const num = Number(v)
          if (!isFinite(num) || num <= 0) return
          await onSave(num)
          setEditing(false)
        }}
        className="text-green-400 hover:text-green-300 text-xs">✓</button>
      <button
        onClick={() => { setV(String(initial)); setEditing(false) }}
        className="text-neutral-500 hover:text-neutral-300 text-xs">✕</button>
    </span>
  )
}

function DeployerCard() {
  const [s, setS] = useState<{ started: boolean; last_cycle_at: string | null; last_deploy_at: string | null; n_deployed: number; n_failed: number; last_error: string | null } | null>(null)
  useEffect(() => {
    let alive = true
    const tick = () => FalconAPI.premarketDeployerStatus().then(r => alive && setS(r)).catch(() => {})
    tick()
    const id = setInterval(tick, 30000)
    return () => { alive = false; clearInterval(id) }
  }, [])
  if (!s) return null
  return (
    <section className="bg-neutral-900 border border-neutral-800 rounded p-3 text-xs text-neutral-500">
      Deployer thread: {s.started ? <span className="text-green-400">running</span> : <span className="text-red-400">not running</span>}
      {' · last cycle: '}
      <span className="font-mono">{s.last_cycle_at ?? 'never'}</span>
      {' · deployed: '}{s.n_deployed} · failed: {s.n_failed}
      {s.last_error && <span className="ml-3 text-red-400">✗ {s.last_error}</span>}
    </section>
  )
}
