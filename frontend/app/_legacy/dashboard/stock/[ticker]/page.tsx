'use client'

import { use, useCallback, useEffect, useState } from 'react'
import Link from 'next/link'
import { useSearchParams } from 'next/navigation'
import {
  getPromptAnswer,
  getPromptList,
  getStockDetail,
  TIER_COLORS,
  TIER_LABEL,
  type PromptAnswer,
  type PromptList,
  type StockDetail,
} from '@/lib/dashboard-api'

// ─────────────────────────────────────────────────────────────────────────────
// KANIDA.AI Terminal — Stock Detail
// Conversational exploration: Kanida.AI presents a menu of smart prompts.
// Clicking any prompt fetches a pre-computed, data-backed answer, plus
// follow-up questions so the user can keep drilling.
// ─────────────────────────────────────────────────────────────────────────────

const GROUP_ORDER = ['context', 'edge', 'structure', 'evidence', 'trade'] as const
const GROUP_LABEL: Record<string, string> = {
  context:   'What is this stock doing?',
  edge:      "Where's the edge?",
  structure: 'Price structure',
  evidence:  'Track record',
  trade:     'Trade execution',
}

export default function StockDetailPage({
  params,
}: {
  params: Promise<{ ticker: string }>
}) {
  const { ticker } = use(params)
  const searchParams = useSearchParams()
  const market = (searchParams.get('market') ?? 'NSE').toUpperCase()
  const initialPrompt = searchParams.get('prompt') ?? null

  const [stock, setStock]         = useState<StockDetail | null>(null)
  const [promptList, setPromptList] = useState<PromptList | null>(null)
  const [answers, setAnswers]     = useState<Record<string, PromptAnswer>>({})
  const [loadingIds, setLoadingIds] = useState<Set<string>>(new Set())
  const [openId, setOpenId]       = useState<string | null>(null)
  const [err, setErr]             = useState<string | null>(null)

  // Initial load — fetch the stock detail + the prompt menu in parallel.
  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const [s, pl] = await Promise.all([
          getStockDetail(ticker, market),
          getPromptList(ticker, market),
        ])
        if (cancelled) return
        setStock(s)
        setPromptList(pl)
      } catch (e) {
        if (!cancelled) setErr(e instanceof Error ? e.message : String(e))
      }
    }
    load()
    return () => { cancelled = true }
  }, [ticker, market])

  const loadAnswer = useCallback(async (promptId: string) => {
    if (answers[promptId] || loadingIds.has(promptId)) return
    setLoadingIds(prev => new Set(prev).add(promptId))
    try {
      const a = await getPromptAnswer(ticker, promptId, market)
      setAnswers(prev => ({ ...prev, [promptId]: a }))
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setLoadingIds(prev => {
        const next = new Set(prev); next.delete(promptId); return next
      })
    }
  }, [answers, loadingIds, ticker, market])

  // Auto-open the requested prompt if the URL carries ?prompt=...
  useEffect(() => {
    if (initialPrompt && promptList && !openId) {
      setOpenId(initialPrompt)
      loadAnswer(initialPrompt)
    }
  }, [initialPrompt, promptList, openId, loadAnswer])

  const togglePrompt = (id: string) => {
    const next = openId === id ? null : id
    setOpenId(next)
    if (next) loadAnswer(next)
  }

  // Group prompts by section
  const grouped: Record<string, PromptList['prompts']> = {}
  promptList?.prompts.forEach(p => {
    grouped[p.group] = grouped[p.group] ?? []
    grouped[p.group].push(p)
  })

  return (
    <main className="mx-auto max-w-[1100px] px-4 pb-16 pt-6 sm:px-6 lg:px-8">

      {/* Breadcrumb */}
      <nav className="mb-4 flex items-center gap-2 text-xs text-zinc-500">
        <Link href="/dashboard" className="hover:text-zinc-200">
          KANIDA<span className="text-emerald-400">.AI</span> Terminal
        </Link>
        <span className="text-zinc-700">/</span>
        <span className="text-zinc-400">{market}</span>
        <span className="text-zinc-700">/</span>
        <span className="text-zinc-200">{ticker}</span>
      </nav>

      {/* ════════════ STOCK HEADER ═══════════════════════════════ */}
      {stock ? <StockHeader stock={stock} /> : <HeaderSkeleton ticker={ticker} market={market} />}

      {err && (
        <div className="my-6 rounded-lg border border-rose-600/40 bg-rose-950/30 px-4 py-3 text-sm text-rose-300">
          {err}
        </div>
      )}

      {/* ════════════ FIRING BANNER ═══════════════════════════════ */}
      {stock && stock.firing_today.length > 0 && (
        <FiringBanner stock={stock} />
      )}

      {/* ════════════ SMART PROMPTS (conversation layer) ═══════════ */}
      <section className="mt-8">
        <div className="mb-3 flex items-baseline justify-between">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-zinc-400">
            Ask KANIDA.AI
          </h2>
          <span className="text-[11px] text-zinc-600">
            {promptList
              ? `${promptList.prompts.length} pre-computed answers · no wait time`
              : 'Loading prompts…'}
          </span>
        </div>

        {promptList ? (
          <div className="space-y-6">
            {GROUP_ORDER.filter(g => grouped[g]).map(group => (
              <div key={group}>
                <h3 className="mb-2 text-[11px] font-semibold uppercase tracking-wider text-zinc-500">
                  {GROUP_LABEL[group] ?? group}
                </h3>
                <ul className="divide-y divide-zinc-800 overflow-hidden rounded-xl border border-zinc-800 bg-zinc-900/40">
                  {grouped[group].map(p => (
                    <PromptCard
                      key={p.id}
                      question={p.question}
                      open={openId === p.id}
                      loading={loadingIds.has(p.id)}
                      answer={answers[p.id]}
                      onToggle={() => togglePrompt(p.id)}
                      onFollowUp={(pid) => {
                        setOpenId(pid)
                        loadAnswer(pid)
                        // bring the new question into view
                        setTimeout(() => {
                          document.getElementById(`prompt-${pid}`)?.scrollIntoView({
                            behavior: 'smooth', block: 'start',
                          })
                        }, 80)
                      }}
                      id={`prompt-${p.id}`}
                    />
                  ))}
                </ul>
              </div>
            ))}
          </div>
        ) : (
          <div className="grid gap-2">
            {[0,1,2,3,4].map(i => (
              <div key={i} className="h-12 animate-pulse rounded-lg border border-zinc-800 bg-zinc-900/40" />
            ))}
          </div>
        )}
      </section>

      <footer className="mt-12 text-center text-[11px] text-zinc-600">
        KANIDA<span className="text-emerald-400">.AI</span> · answers derived from {stock?.roster_active ?? '–'} calibrated setups · not financial advice.
      </footer>
    </main>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// HEADER
// ─────────────────────────────────────────────────────────────────────────────

function StockHeader({ stock }: { stock: StockDetail }) {
  const score = stock.predictability
  const scoreTone =
    score >= 75 ? 'text-emerald-300' :
    score >= 60 ? 'text-sky-300' :
    score >= 45 ? 'text-amber-300' :
    'text-zinc-400'

  return (
    <header className="rounded-2xl border border-zinc-800 bg-gradient-to-br from-zinc-900/80 to-zinc-900/40 p-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <div className="flex items-center gap-3">
            <h1 className="font-mono text-3xl font-bold tracking-tight text-zinc-100">
              {stock.ticker}
            </h1>
            <span className="rounded border border-zinc-700 bg-zinc-950 px-2 py-0.5 text-[11px] uppercase tracking-wider text-zinc-400">
              {stock.market}
            </span>
          </div>
          <div className="mt-1 text-sm text-zinc-300">
            {stock.company_name ?? '—'}
            {stock.sector && <span className="text-zinc-500"> · {stock.sector}</span>}
          </div>
        </div>

        <div className="flex flex-wrap items-baseline gap-6">
          <div>
            <div className={`font-mono text-3xl font-semibold tabular-nums ${scoreTone}`}>
              {score.toFixed(0)}
              <span className="ml-1 text-sm font-normal text-zinc-500">/ 100</span>
            </div>
            <div className="text-[10px] uppercase tracking-wider text-zinc-500">
              Predictability
            </div>
          </div>
          <div>
            <div className="font-mono text-2xl font-semibold tabular-nums text-zinc-200">
              {stock.roster_active}
            </div>
            <div className="text-[10px] uppercase tracking-wider text-zinc-500">
              Active setups
            </div>
          </div>
          <div>
            <div className="font-mono text-2xl font-semibold tabular-nums text-violet-300">
              {stock.roster_top}
            </div>
            <div className="text-[10px] uppercase tracking-wider text-zinc-500">
              Top-tier
            </div>
          </div>
        </div>
      </div>
    </header>
  )
}

function HeaderSkeleton({ ticker, market }: { ticker: string; market: string }) {
  return (
    <header className="rounded-2xl border border-zinc-800 bg-zinc-900/40 p-6">
      <h1 className="font-mono text-3xl font-bold text-zinc-100">{ticker}</h1>
      <p className="text-xs text-zinc-500">{market} · loading…</p>
    </header>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// FIRING BANNER
// ─────────────────────────────────────────────────────────────────────────────

function FiringBanner({ stock }: { stock: StockDetail }) {
  const bull = stock.firing_today.filter(e => e.bias === 'bullish').length
  const bear = stock.firing_today.filter(e => e.bias === 'bearish').length
  const parts: string[] = []
  if (bull) parts.push(`${bull} bullish`)
  if (bear) parts.push(`${bear} bearish`)
  return (
    <div className="mt-4 flex items-center gap-3 rounded-xl border border-amber-500/30 bg-amber-500/5 px-4 py-3">
      <span className="relative inline-flex">
        <span className="absolute inline-flex h-2.5 w-2.5 animate-ping rounded-full bg-amber-400 opacity-75" />
        <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-amber-400" />
      </span>
      <div className="flex-1">
        <div className="text-sm text-amber-200">
          {parts.join(' · ')} setup{stock.firing_today.length > 1 ? 's' : ''} firing today
        </div>
        <div className="text-[11px] text-amber-400/60">
          Latest scan · {stock.latest_signal_date ?? '—'}
        </div>
      </div>
      <div className="hidden gap-1 sm:flex">
        {stock.firing_today.slice(0, 4).map((e, i) => (
          <span key={i} className={`rounded border px-1.5 py-0.5 text-[10px]
            ${e.bias === 'bullish'
              ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-300'
              : 'border-rose-500/30 bg-rose-500/10 text-rose-300'}`}>
            {e.timeframe === '1D' ? 'D' : 'W'} · {e.strategy_name.slice(0, 18)}
          </span>
        ))}
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// PROMPT CARD
// ─────────────────────────────────────────────────────────────────────────────

function PromptCard({
  id, question, open, loading, answer, onToggle, onFollowUp,
}: {
  id: string
  question: string
  open: boolean
  loading: boolean
  answer?: PromptAnswer
  onToggle: () => void
  onFollowUp: (pid: string) => void
}) {
  return (
    <li id={id} className={open ? 'bg-zinc-900/60' : ''}>
      <button
        onClick={onToggle}
        className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left text-sm
                   text-zinc-200 transition hover:bg-zinc-800/40">
        <span className="flex items-center gap-2.5">
          <span className={`text-zinc-500 transition ${open ? 'rotate-90' : ''}`}>›</span>
          <span>{question}</span>
        </span>
        {loading && !answer && (
          <span className="text-[11px] text-zinc-500">thinking…</span>
        )}
      </button>

      {open && answer && (
        <div className="border-t border-zinc-800 bg-zinc-950/50 px-4 py-4">
          <p className="text-[14px] leading-relaxed text-zinc-200">{answer.answer}</p>

          {answer.evidence.length > 0 && (
            <div className="mt-3 rounded-lg border border-zinc-800/60 bg-zinc-900/40 p-2">
              <div className="px-1 pb-1 text-[10px] uppercase tracking-wider text-zinc-500">
                Evidence
              </div>
              <ul className="divide-y divide-zinc-800/50">
                {answer.evidence.slice(0, 6).map((e, i) => (
                  <li key={i} className="flex items-start gap-3 px-1 py-1.5 text-[12px]">
                    <span className="shrink-0 rounded bg-zinc-800/80 px-1.5 py-0.5 text-[10px] text-zinc-300">
                      {e.label}
                    </span>
                    <span className="flex-1 text-zinc-400">{e.detail}</span>
                    {e.when && (
                      <span className="shrink-0 font-mono text-[10px] text-zinc-600">
                        {e.when}
                      </span>
                    )}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {answer.next_prompts.length > 0 && (
            <div className="mt-4">
              <div className="mb-1.5 text-[10px] uppercase tracking-wider text-zinc-500">
                Follow up
              </div>
              <div className="flex flex-wrap gap-2">
                {answer.next_prompts.map(n => (
                  <button
                    key={n.id}
                    onClick={() => onFollowUp(n.id)}
                    className="rounded-full border border-emerald-500/30 bg-emerald-500/5 px-3 py-1
                               text-[12px] text-emerald-300 transition
                               hover:border-emerald-500/60 hover:bg-emerald-500/10">
                    {n.question}
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </li>
  )
}

// Keep the TIER_COLORS / TIER_LABEL imports referenced to avoid unused-import warnings
// in case future card tweaks want to use them.
void TIER_COLORS; void TIER_LABEL
