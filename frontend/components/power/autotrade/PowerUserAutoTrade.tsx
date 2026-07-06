'use client'

/**
 * PowerUserAutoTrade — the 4-tab AutoTrade shell for NON-operator (power) users.
 *
 * ATM/Apple-simple: one clear next action per tab, minimal scroll, the tab bar
 * always visible so the next step is one click. Only the ACTIVE tab's content
 * mounts (inactive tabs unmount → no duplicate polling, no scroll stacking).
 *
 * The four tabs (styled to match the operator AutoTradePanel tab bar):
 *   1. Connect Your Broker      → BrokerAccountsPanel (verbatim, isAdmin=false).
 *   2. Start Auto Trade         → PortfolioAutoTrade view='create' — mounts straight
 *                                 into the config → strategy → paper/live → Create →
 *                                 Start/Schedule flow. On a successful start it hands
 *                                 off to the Live tab; picking LIVE with no connected
 *                                 broker hands off to the Broker tab.
 *   3. Live & Scheduled         → PortfolioAutoTrade view='dashboard' — the sessions /
 *                                 campaigns list only. "New session" jumps to Start.
 *   4. Trade Journal            → TradeJournalPanel, with a session picker scoped to
 *                                 this user (defaults to their most recent session).
 *
 * This shell reuses the existing components entirely — it only owns navigation +
 * the guided handoffs. No trading logic lives here. Real-money gating (own ACTIVE
 * account for a LIVE start) is enforced inside PortfolioAutoTrade (isAdmin=false).
 */
import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react'
import { C, ICON, Gear, MECHANISM_CSS } from '@/components/power/shared/cotrade-kit'
import { PortfolioAutoTrade } from '@/components/power/autotrade/PortfolioAutoTrade'
import { BrokerAccountsPanel } from '@/components/power/autotrade/BrokerAccountsPanel'
import { TradeJournalPanel } from '@/components/power/autotrade/TradeJournalPanel'
import { AutoTradeAPI, type SessionSummary, type BrokerAccount } from '@/lib/autotrade-api'

type Tab = 'broker' | 'start' | 'live' | 'journal'

const TABS: { id: Tab; label: string; icon: (n: number) => ReactNode }[] = [
  { id: 'broker',  label: 'Connect Your Broker',      icon: ICON.link },
  { id: 'start',   label: 'Start Auto Trade',         icon: ICON.bolt },
  { id: 'live',    label: 'Live & Scheduled Campaigns', icon: ICON.clock },
  { id: 'journal', label: 'Trade Journal',            icon: ICON.book },
]

const VALID_TABS: Tab[] = ['broker', 'start', 'live', 'journal']

// Deep-link support — ?attab=start|live|broker|journal. Falls back to null so the
// account-count default (below) can decide the opening tab.
function deepLinkTab(): Tab | null {
  if (typeof window === 'undefined') return null
  const t = new URLSearchParams(window.location.search).get('attab')
  return t && (VALID_TABS as string[]).includes(t) ? (t as Tab) : null
}

// An account is "live-ready" only when ACTIVE (has a live daily token). PENDING /
// EXPIRED accounts are connected but cannot place a real order yet.
const isActive = (a: BrokerAccount) => String(a.status ?? '').toUpperCase() === 'ACTIVE'

export function PowerUserAutoTrade({ firstName, userId }: { firstName: string; userId: number | string }) {
  // tab is decided AFTER the first accounts load (default = broker when they have
  // 0 connected accounts, else live). A deep link, if present, always wins. Until
  // resolved we render nothing in the body (a brief spinner) so we never flash the
  // wrong tab. `resolved` guards the one-time default decision.
  const [tab, setTabState] = useState<Tab | null>(deepLinkTab())
  const [resolved, setResolved] = useState<boolean>(deepLinkTab() != null)

  const [accounts, setAccounts] = useState<BrokerAccount[] | null>(null)
  const [accountsErr, setAccountsErr] = useState<string | null>(null)

  // Handoff banner: after a broker goes ACTIVE we surface a "Next: Start Auto
  // Trade" nudge on the Broker tab and offer to advance. Tracks the ACTIVE count
  // so we only fire the nudge when it INCREASES (a new connection completed).
  const prevActiveCount = useRef<number | null>(null)
  const [justConnected, setJustConnected] = useState(false)

  const go = useCallback((next: Tab) => {
    setTabState(next)
    setResolved(true)
    // Keep the URL shareable/deep-linkable without a full navigation.
    if (typeof window !== 'undefined') {
      const url = new URL(window.location.href)
      url.searchParams.set('attab', next)
      window.history.replaceState(null, '', url.toString())
    }
  }, [])

  // Load the user's broker accounts. Runs on mount + is polled every 8s so a
  // connection completed inside the Broker tab is detected here (the reused
  // BrokerAccountsPanel has no top-level onConnected prop, so we observe the count
  // ourselves — the source of truth for both the default tab and the handoff).
  const loadAccounts = useCallback(async () => {
    if (userId == null) { setAccounts([]); return }
    try {
      const res = await AutoTradeAPI.brokerAccounts(userId)
      setAccounts(res.accounts ?? [])
      setAccountsErr(null)
    } catch (e) {
      setAccounts((prev) => prev ?? [])
      setAccountsErr(e instanceof Error ? e.message : 'Could not load your broker accounts.')
    }
  }, [userId])

  useEffect(() => {
    loadAccounts()
    const id = setInterval(loadAccounts, 8_000)
    return () => clearInterval(id)
  }, [loadAccounts])

  // One-time default-tab decision once accounts first resolve (unless a deep link
  // already set the tab): 0 connected → Broker; otherwise → Live.
  useEffect(() => {
    if (resolved || accounts == null) return
    setTabState(accounts.length === 0 ? 'broker' : 'live')
    setResolved(true)
  }, [resolved, accounts])

  // Detect a newly-ACTIVE account (a connection just completed). When the ACTIVE
  // count increases we raise the "Next: Start Auto Trade" nudge on the Broker tab.
  useEffect(() => {
    if (accounts == null) return
    const activeCount = accounts.filter(isActive).length
    if (prevActiveCount.current != null && activeCount > prevActiveCount.current) {
      setJustConnected(true)
    }
    prevActiveCount.current = activeCount
  }, [accounts])

  const activeCount = (accounts ?? []).filter(isActive).length

  return (
    <div className="h-full w-full flex flex-col" style={{ background: C.canvas, color: C.ink }}>
      <style>{MECHANISM_CSS}</style>

      {/* ── Header + tab bar (shrink-0, always visible) ──────────────────────── */}
      <div className="shrink-0 px-5 pt-7 pb-3 sm:px-8">
        <div className="mx-auto w-full max-w-4xl">
          <div className="flex items-start gap-3">
            <div className="flex items-center -space-x-1 shrink-0 mt-0.5">
              <Gear size={26} dir={1} />
              <Gear size={18} dir={-1} dim />
            </div>
            <div className="min-w-0">
              <h1 className="text-[20px] sm:text-[22px] font-semibold leading-tight" style={{ color: C.ink }}>
                AutoTrade
              </h1>
              <p className="text-[12.5px] leading-snug mt-1" style={{ color: C.muted }}>
                {firstName ? `${firstName}, connect` : 'Connect'} your broker, start an
                auto-trade on Falcon signals, and watch it run.{' '}
                <b style={{ color: C.ink2 }}>New sessions default to PAPER</b> — a live
                order needs your own connected broker account.
              </p>
            </div>
          </div>

          {/* Tab bar — same styling family as the operator AutoTradePanel. */}
          <div className="mt-5 inline-flex flex-wrap gap-0.5 rounded-xl border p-0.5"
            style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
            {TABS.map((t) => (
              <TabBtn
                key={t.id}
                active={tab === t.id}
                onClick={() => go(t.id)}
                icon={t.icon}
                // A small ACTIVE-broker count badge on the Broker tab so the user
                // can see at a glance whether they're connected.
                badge={t.id === 'broker' && activeCount > 0 ? activeCount : undefined}
              >
                {t.label}
              </TabBtn>
            ))}
          </div>
        </div>
      </div>

      {/* ── Body (flex-1, scrolls) — only the ACTIVE tab mounts ──────────────── */}
      <div className="flex-1 min-h-0 overflow-y-auto">
        {!resolved || tab == null ? (
          <div className="flex items-center justify-center py-24">
            <span className="text-[12.5px]" style={{ color: C.faint }}>Loading your AutoTrade…</span>
          </div>
        ) : (
          <>
            {tab === 'broker' && (
              <div className="mx-auto w-full max-w-4xl px-5 pb-10 sm:px-8">
                {/* Handoff nudge — surfaces once a broker becomes ACTIVE. */}
                {justConnected && (
                  <div className="mt-4 flex items-center gap-3 rounded-2xl border px-4 py-3"
                    style={{ borderColor: 'rgba(63,227,164,0.4)', background: 'rgba(63,227,164,0.07)' }}>
                    <span className="shrink-0" style={{ color: C.mint }}>{ICON.check(16)}</span>
                    <div className="min-w-0 flex-1">
                      <p className="text-[12.5px] font-semibold" style={{ color: C.ink }}>Broker connected</p>
                      <p className="text-[11.5px] leading-snug" style={{ color: C.muted }}>
                        You&apos;re ready to run an auto-trade on your own account.
                      </p>
                    </div>
                    <button type="button" onClick={() => { setJustConnected(false); go('start') }}
                      className="shrink-0 flex items-center gap-1.5 text-[12px] font-semibold px-3.5 py-2 rounded-lg"
                      style={{ color: '#06130c', background: C.mint }}>
                      Start Auto Trade {ICON.arrow(14)}
                    </button>
                  </div>
                )}
                <BrokerAccountsPanel userId={userId} isAdmin={false} />
              </div>
            )}

            {tab === 'start' && (
              <div className="mx-auto w-full max-w-3xl px-5 pb-10 sm:px-8 pt-4">
                <PortfolioAutoTrade
                  userId={userId}
                  isAdmin={false}
                  view="create"
                  onStarted={() => go('live')}
                  onNeedBroker={() => go('broker')}
                />
              </div>
            )}

            {tab === 'live' && (
              <div className="mx-auto w-full max-w-4xl px-5 pb-10 sm:px-8 pt-4">
                <PortfolioAutoTrade
                  userId={userId}
                  isAdmin={false}
                  view="dashboard"
                  onNewCampaign={() => go('start')}
                />
              </div>
            )}

            {tab === 'journal' && (
              <div className="mx-auto w-full max-w-4xl px-5 pb-10 sm:px-8 pt-4">
                <JournalTab userId={userId} onNewCampaign={() => go('start')} />
              </div>
            )}
          </>
        )}
      </div>

      {/* Silent: an accounts-load error never blocks the shell — the Broker tab
          surfaces its own detailed error. We only note it faintly if present. */}
      {accountsErr && tab === 'broker' && (
        <div className="shrink-0 px-5 pb-2 sm:px-8">
          <p className="mx-auto w-full max-w-4xl text-[10.5px]" style={{ color: C.faint }}>
            Broker status may be stale — {accountsErr}
          </p>
        </div>
      )}
    </div>
  )
}

/**
 * JournalTab — the Trade Journal tab. TradeJournalPanel takes a single sessionId;
 * for a power user we add a simple session picker (their own sessions, newest
 * first) and default to the most recent. Reuses TradeJournalPanel's analysis
 * verbatim.
 */
function JournalTab({ userId, onNewCampaign }: { userId: number | string; onNewCampaign: () => void }) {
  const [sessions, setSessions] = useState<SessionSummary[] | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [selectedId, setSelectedId] = useState<string>('')

  useEffect(() => {
    let alive = true
    AutoTradeAPI.listSessions(userId != null ? { user_id: userId } : undefined)
      .then((res) => {
        if (!alive) return
        const list = (res.sessions ?? []).slice().sort((a, b) => {
          const ta = a.created_at ? Date.parse(a.created_at) : 0
          const tb = b.created_at ? Date.parse(b.created_at) : 0
          return tb - ta
        })
        setSessions(list)
        // Default to the most recent session.
        if (list.length > 0) setSelectedId((cur) => cur || list[0].session_id)
        setErr(null)
      })
      .catch((e) => { if (alive) setErr(e instanceof Error ? e.message : 'Could not load your sessions.') })
    return () => { alive = false }
  }, [userId])

  // Empty state — no sessions yet.
  if (sessions != null && sessions.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center gap-3 py-20 text-center">
        <span style={{ color: C.faint }}>{ICON.book(28)}</span>
        <p className="text-[13px]" style={{ color: C.muted }}>No sessions yet.</p>
        <p className="text-[11.5px]" style={{ color: C.faint }}>
          Start an auto-trade and its trade journal — entries, exits, stops, trails and
          performance — shows up here.
        </p>
        <button type="button" onClick={onNewCampaign}
          className="mt-1 flex items-center gap-1.5 text-[12px] font-semibold px-3.5 py-2 rounded-lg"
          style={{ color: '#06130c', background: C.mint }}>
          {ICON.bolt(13)} Start Auto Trade
        </button>
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-4">
      {/* Session picker — this user's sessions, newest first. */}
      <div className="flex items-center gap-3 flex-wrap">
        <label className="text-[11px] font-semibold uppercase tracking-[0.05em]" style={{ color: C.muted }}>
          Session
        </label>
        <select
          value={selectedId}
          onChange={(e) => setSelectedId(e.target.value)}
          disabled={sessions == null || sessions.length === 0}
          className="rounded-lg px-3 py-2 text-[12.5px] outline-none disabled:opacity-50"
          style={{ background: 'rgba(255,255,255,0.03)', border: `1px solid ${C.line2}`, color: C.ink, minWidth: 260 }}
        >
          {sessions == null && <option style={{ background: '#0b1410', color: C.ink }}>Loading…</option>}
          {(sessions ?? []).map((s) => (
            <option key={s.session_id} value={s.session_id} style={{ background: '#0b1410', color: C.ink }}>
              {sessionLabel(s)}
            </option>
          ))}
        </select>
      </div>

      {err && (
        <div className="flex items-start gap-2 rounded-xl border px-3.5 py-2.5 text-[11.5px] leading-snug"
          style={{ borderColor: 'rgba(230,180,80,0.35)', background: 'rgba(230,180,80,0.06)', color: C.ink2 }}>
          <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
          <span>{err}</span>
        </div>
      )}

      {selectedId ? (
        <TradeJournalPanel sessionId={selectedId} />
      ) : (
        <p className="text-[12px] py-8 text-center" style={{ color: C.faint }}>Select a session to view its trade journal.</p>
      )}
    </div>
  )
}

// A readable one-line label for a session in the picker: date · mode · capital.
function sessionLabel(s: SessionSummary): string {
  const bits: string[] = []
  if (s.created_at) {
    const d = new Date(s.created_at)
    if (!Number.isNaN(d.getTime())) {
      bits.push(d.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', timeZone: 'Asia/Kolkata' }))
    }
  }
  if (s.mode) bits.push(String(s.mode).toUpperCase())
  if (s.ladder_id) bits.push('Campaign')
  if (typeof s.total_allocated_capital === 'number') bits.push(`₹${Math.round(s.total_allocated_capital).toLocaleString('en-IN')}`)
  if (s.status) bits.push(String(s.status))
  const head = bits.length ? bits.join(' · ') : s.session_id
  return `${head} — ${s.session_id.slice(0, 8)}`
}

function TabBtn({
  active, onClick, icon, children, badge,
}: {
  active: boolean
  onClick: () => void
  icon: (n: number) => ReactNode
  children: ReactNode
  badge?: number
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="flex items-center gap-2 px-4 py-2 rounded-lg text-[12.5px] font-semibold transition-colors"
      style={{
        color: active ? '#06130c' : C.ink2,
        background: active ? C.mint : 'transparent',
      }}
    >
      {icon(14)}
      {children}
      {badge != null && (
        <span className="inline-flex items-center justify-center min-w-[18px] h-[18px] px-1 rounded-full text-[10px] font-bold"
          style={{
            color: active ? '#06130c' : C.mint,
            background: active ? 'rgba(6,19,12,0.14)' : 'rgba(63,227,164,0.16)',
          }}>
          {badge}
        </span>
      )}
    </button>
  )
}
