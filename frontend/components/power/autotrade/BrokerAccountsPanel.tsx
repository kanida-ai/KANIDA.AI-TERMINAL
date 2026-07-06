'use client'

/**
 * BrokerAccountsPanel — AlgoTest-style guided broker onboarding (3 screens).
 *
 *   SCREEN 1 · GALLERY   — a searchable grid of broker cards (brand chip, name,
 *                          exchange chips, "Connected: N"). LIVE → Set up / Add
 *                          account; coming-soon → muted chip. A "Your connections"
 *                          count. This is the front door.
 *   SCREEN 2 · SETUP     — a two-pane guided card for the chosen broker. LEFT:
 *                          numbered setup.steps, a copyable callback/redirect URL,
 *                          a docs link, a token note, and the egress-IP allowlist
 *                          helper. RIGHT: a Connection name + the DYNAMIC field
 *                          schema (secret fields get an eye toggle) and a single
 *                          "Test & Connect" that runs the create → login-popup →
 *                          paste request_token → activate lifecycle with inline,
 *                          step-by-step status. Coming-soon brokers render the
 *                          steps but disable connect.
 *   SCREEN 3 · CONNECTED — per-account cards (brand chip + label + status pill;
 *                          "Last verified …"): Generate token / Reconnect,
 *                          Health-check, Delete. EXPIRED → Reconnect is the CTA.
 *
 *   ADMIN — when isAdmin, an "All users" toggle lists EVERY broker connection in a
 *   compact table (broker · user · label · status · last verified) with
 *   Health-check + Delete per row, for monitoring. (Backend returns all accounts
 *   for admin when no user_id filter is passed.)
 *
 * The API client + connect lifecycle in lib/autotrade-api.ts are UNCHANGED — this
 * file only reshapes the UX around them. Contract fields (brand/fields/setup) are
 * optional-safe: a partial backend falls back to a static registry + the legacy
 * API-key + API-secret inputs. api_secret stays WRITE-ONLY — snapshotted, sent
 * once, cleared from state the instant create resolves; never rendered.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { C, ICON } from '@/components/power/shared/cotrade-kit'
import {
  AutoTradeAPI,
  type BrokerAccount,
  type BrokerAccountHealth,
  type BrokerName,
  type FieldDef,
  type SupportedBroker,
} from '@/lib/autotrade-api'

const inputStyle: React.CSSProperties = {
  background: 'rgba(255,255,255,0.03)',
  border: `1px solid ${C.line2}`,
  color: C.ink,
}

// ════════════════════════════════════════════════════════════════════════════
// STATIC FALLBACK REGISTRY — used ONLY when GET /brokers/supported is missing or
// returns a partial shape (no display_name/brand/fields/setup). It carries just
// enough (display_name, brand chip, exchanges, the standard 2-field key/secret
// schema, and step copy) so the guided flow always works. Live/coming-soon still
// comes from the backend when present; here it mirrors the deployed backend
// (zerodha + rupeezy live). This is presentation metadata only.
// ════════════════════════════════════════════════════════════════════════════
const LEGACY_FIELDS: FieldDef[] = [
  { name: 'api_key',    label: 'API key',    type: 'text',     secret: false, required: true, placeholder: 'api_key',    maps_to: 'api_key' },
  { name: 'api_secret', label: 'API secret', type: 'password', secret: true,  required: true, placeholder: 'api_secret', maps_to: 'api_secret' },
]

const FALLBACK: Record<string, SupportedBroker> = {
  zerodha: {
    broker: 'zerodha', live: true, display_name: 'Zerodha (Kite Connect)',
    brand: { color: '#387ED1', initial: 'Z' },
    exchanges: ['NSE', 'BSE', 'NFO', 'MCX'],
    capabilities: { auth_kind: 'request_token', has_refresh_token: false, token_lifetime: 'daily ~06:00 IST', supports_gtt: true, supports_mtf: true },
    fields: LEGACY_FIELDS,
    setup: {
      docs_url: 'https://developers.kite.trade',
      steps: [
        'Create a Kite Connect app at developers.kite.trade (paid developer add-on).',
        'Set the app’s Redirect URL to the callback URL shown on the left.',
        'Copy your API Key + API Secret into the fields on the right.',
        'Add our egress IP (below) to the app’s Allowed IPs so live orders don’t error.',
        'Test & Connect → sign in on Kite → paste the returned request_token to finish.',
      ],
      token_note: 'Kite tokens expire every morning (~06:00 IST) with no refresh token — one-click Reconnect each trading day.',
    },
  },
  rupeezy: {
    broker: 'rupeezy', live: true, display_name: 'Rupeezy (Vortex API)',
    brand: { color: '#F04E30', initial: 'R' },
    exchanges: ['NSE', 'BSE', 'NFO'],
    capabilities: { auth_kind: 'oauth2_flow', token_lifetime: 'session' },
    fields: [
      { name: 'application_id', label: 'Application ID', type: 'text',     secret: false, required: true, placeholder: 'application_id', maps_to: 'api_key' },
      { name: 'x_api_key',      label: 'x-api-key',      type: 'password', secret: true,  required: true, placeholder: 'x-api-key',      maps_to: 'api_secret' },
    ],
    setup: {
      docs_url: 'https://vortex.rupeezy.in/docs/1.0/authentication/',
      steps: [
        'Register a Vortex app (free) at the Vortex developer portal.',
        'Set the Redirect URL to the callback URL shown on the left.',
        'Copy your application_id + x-api-key into the fields on the right.',
        'Test & Connect → log in at flow.rupeezy.in → paste the returned token.',
      ],
      token_note: 'Session-based token; the lifecycle layer detects expiry via a health ping and prompts a reconnect if needed.',
    },
  },
  fivepaisa: {
    broker: 'fivepaisa', live: false, display_name: 'FivePaisa (5paisa Xstream)',
    brand: { color: '#0072BC', initial: '5' },
    exchanges: ['NSE', 'BSE'],
    capabilities: { auth_kind: 'oauth_request_token', token_lifetime: 'daily' },
    fields: LEGACY_FIELDS,
    setup: {
      docs_url: 'https://xstream.5paisa.com/dev-docs/user-authentication-system/access-token',
      steps: [
        'Enable TOTP two-factor in your 5paisa account.',
        'Create an API app in the 5paisa developer portal to get your keys.',
        'Complete the OAuth login (with TOTP) when connecting.',
      ],
      token_note: 'Daily token (similar to Kite) — reconnect each trading day.',
    },
  },
  upstox: { broker: 'upstox', live: false, display_name: 'Upstox', brand: { color: '#5A31F4', initial: 'U' }, exchanges: ['NSE', 'BSE', 'NFO'] },
  angel:  { broker: 'angel',  live: false, display_name: 'Angel One', brand: { color: '#3E4C9A', initial: 'A' }, exchanges: ['NSE', 'BSE', 'NFO'] },
  dhan:   { broker: 'dhan',   live: false, display_name: 'Dhan', brand: { color: '#1FB574', initial: 'D' }, exchanges: ['NSE', 'BSE', 'NFO'] },
}
const FALLBACK_LIST: SupportedBroker[] = Object.values(FALLBACK)

// Merge the backend broker with our static fallback so every field is present.
// The backend ALWAYS wins where it provides a value; the fallback only fills gaps.
function enrich(b: SupportedBroker): SupportedBroker {
  const key = String(b.broker).toLowerCase()
  const fb = FALLBACK[key]
  if (!fb) return b
  return {
    ...fb, ...b,
    brand: { ...fb.brand, ...b.brand },
    capabilities: { ...fb.capabilities, ...b.capabilities },
    setup: { ...fb.setup, ...b.setup },
    exchanges: b.exchanges?.length ? b.exchanges : fb.exchanges,
    fields: b.fields?.length ? b.fields : fb.fields,
    display_name: b.display_name || fb.display_name,
  }
}

const displayName = (b: SupportedBroker | undefined, name?: BrokerName) =>
  b?.display_name || FALLBACK[String(name ?? b?.broker).toLowerCase()]?.display_name || String(name ?? b?.broker ?? '—')

// The credential field schema for a broker: the backend fields, else the merged
// fallback, else the legacy 2-field key/secret schema (never empty for live).
function fieldsFor(b: SupportedBroker | undefined): FieldDef[] {
  if (b?.fields?.length) return b.fields
  const fb = FALLBACK[String(b?.broker).toLowerCase()]
  if (fb?.fields?.length) return fb.fields
  return LEGACY_FIELDS
}

// ── Brand chip (CSS only — no external assets) ───────────────────────────────
function BrandChip({ broker, size = 40 }: { broker?: SupportedBroker; size?: number }) {
  const color = broker?.brand?.color || C.mint
  const initial = (broker?.brand?.initial || String(broker?.broker ?? '?')[0] || '?').toUpperCase().slice(0, 2)
  return (
    <span
      className="inline-grid place-items-center rounded-xl font-bold shrink-0"
      style={{
        width: size, height: size,
        background: `${color}22`,
        color, boxShadow: `inset 0 0 0 1px ${color}55`,
        fontSize: size * 0.42,
      }}
    >
      {initial}
    </span>
  )
}

// ── Status pill (ACTIVE mint / EXPIRED·ERROR·REVOKED red / PENDING amber) ─────
type Tone = { color: string; bg: string; ring: string; label: string }
function statusTone(status?: string): Tone {
  switch ((status ?? '').toUpperCase()) {
    case 'ACTIVE':  return { color: C.mint,  bg: 'rgba(63,227,164,0.12)',  ring: 'rgba(63,227,164,0.42)',  label: 'Active' }
    case 'EXPIRED': return { color: C.red,   bg: 'rgba(232,115,107,0.12)', ring: 'rgba(232,115,107,0.42)', label: 'Expired' }
    case 'REVOKED': return { color: C.red,   bg: 'rgba(232,115,107,0.12)', ring: 'rgba(232,115,107,0.42)', label: 'Revoked' }
    case 'ERROR':   return { color: C.red,   bg: 'rgba(232,115,107,0.12)', ring: 'rgba(232,115,107,0.42)', label: 'Error' }
    case 'PENDING': return { color: C.amber, bg: 'rgba(230,180,80,0.12)',  ring: 'rgba(230,180,80,0.42)',  label: 'Pending' }
    default:        return { color: C.muted, bg: 'rgba(255,255,255,0.04)', ring: C.line2, label: status || 'Unknown' }
  }
}
function StatusPill({ status }: { status?: string }) {
  const t = statusTone(status)
  return (
    <span className="inline-flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.06em] rounded-full px-2.5 py-1"
      style={{ color: t.color, background: t.bg, boxShadow: `inset 0 0 0 1px ${t.ring}` }}>
      <span className="w-1.5 h-1.5 rounded-full" style={{ background: t.color }} />
      {t.label}
    </span>
  )
}

function ExchangeChips({ list }: { list?: string[] }) {
  if (!list?.length) return null
  return (
    <div className="flex items-center gap-1 flex-wrap">
      {list.slice(0, 5).map((x) => (
        <span key={x} className="text-[9.5px] font-semibold uppercase tracking-[0.04em] rounded px-1.5 py-0.5"
          style={{ color: C.muted, background: 'rgba(255,255,255,0.03)', boxShadow: `inset 0 0 0 1px ${C.line2}` }}>
          {x}
        </span>
      ))}
    </div>
  )
}

// A copyable read-only value (callback URL / egress IP).
function CopyBox({ value, label }: { value: string; label?: string }) {
  const [copied, setCopied] = useState(false)
  const onCopy = useCallback(async () => {
    try { await navigator.clipboard.writeText(value); setCopied(true); setTimeout(() => setCopied(false), 1600) }
    catch { /* clipboard blocked — value is shown for manual copy */ }
  }, [value])
  return (
    <div className="flex items-stretch gap-2">
      <code className="flex-1 min-w-0 text-[11.5px] font-mono rounded-lg px-2.5 py-2 truncate"
        style={{ color: C.ink2, background: 'rgba(255,255,255,0.03)', border: `1px solid ${C.line2}` }}
        title={value}>
        {value}
      </code>
      <button type="button" onClick={onCopy}
        className="shrink-0 inline-flex items-center gap-1.5 text-[11.5px] font-semibold px-3 rounded-lg transition-colors"
        style={{ color: C.mint, border: '1px solid rgba(63,227,164,0.3)' }}
        aria-label={label ? `Copy ${label}` : 'Copy'}>
        {copied ? ICON.check(13) : ICON.book(13)} {copied ? 'Copied' : 'Copy'}
      </button>
    </div>
  )
}

function ErrorNote({ children, onClose }: { children: React.ReactNode; onClose?: () => void }) {
  return (
    <div className="flex items-start gap-2 rounded-lg border px-3 py-2 text-[11.5px] leading-snug"
      style={{ borderColor: 'rgba(232,115,107,0.35)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
      <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(13)}</span>
      <span className="min-w-0">{children}</span>
      {onClose && (
        <button type="button" onClick={onClose} className="ml-auto shrink-0" style={{ color: C.faint }} aria-label="Dismiss">
          {ICON.close(12)}
        </button>
      )}
    </div>
  )
}

// ── Egress / allowlist IP helper (self-contained; the exported one lives inside
//    PortfolioAutoTrade). Brokers need our outbound IP allowlisted for live orders.
function EgressIpHelper() {
  const [ip, setIp] = useState<string | null>(null)
  const [asOf, setAsOf] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState<string | null>(null)
  const load = useCallback(async () => {
    setLoading(true); setErr(null)
    try {
      const res = await AutoTradeAPI.egressIp()
      setIp(typeof res.ip === 'string' && res.ip ? res.ip : null)
      setAsOf(typeof res.as_of === 'string' ? res.as_of : null)
    } catch (e) {
      setIp(null); setErr(e instanceof Error ? e.message : 'unavailable')
    } finally { setLoading(false) }
  }, [])
  useEffect(() => { load() }, [load])
  return (
    <div className="rounded-xl border px-3.5 py-3" style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
      <div className="flex items-center gap-2 mb-1.5">
        <span style={{ color: C.mint }}>{ICON.shield(14)}</span>
        <span className="text-[11.5px] font-semibold" style={{ color: C.ink }}>Add our IP to your broker allowlist</span>
        {asOf && <span className="ml-auto text-[9.5px]" style={{ color: C.faint }}>as of {asOf}</span>}
      </div>
      {loading ? (
        <p className="text-[11px]" style={{ color: C.muted }}>Reading our egress IP…</p>
      ) : ip ? (
        <CopyBox value={ip} label="egress IP" />
      ) : (
        <div className="flex items-center gap-2 text-[11px]" style={{ color: C.faint }}>
          <span>Egress IP not reporting yet{err ? ` (${err})` : ''} — showing “—”.</span>
          <button type="button" onClick={load} className="underline" style={{ color: C.mint }}>Retry</button>
        </div>
      )}
      <p className="text-[10.5px] leading-snug mt-1.5" style={{ color: C.muted }}>
        Some brokers require the server’s outbound IP on their Allowed-IPs list or live orders error.
      </p>
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// The panel — a small screen state machine (gallery ⇄ setup) over one loaded list.
// ════════════════════════════════════════════════════════════════════════════
type Screen = { name: 'gallery' } | { name: 'setup'; broker: BrokerName }

export function BrokerAccountsPanel({ userId, isAdmin = false }: { userId: number | string; isAdmin?: boolean }) {
  // Registry (gallery source) + accounts + vault state.
  const [registry, setRegistry] = useState<SupportedBroker[]>(FALLBACK_LIST)
  const [accounts, setAccounts] = useState<BrokerAccount[] | null>(null)
  const [vaultEnabled, setVaultEnabled] = useState<boolean | null>(null)
  const [loading, setLoading] = useState(false)
  const [listErr, setListErr] = useState<string | null>(null)

  // Admin "All users" view (unscoped list) — only fetched when toggled on.
  const [allView, setAllView] = useState(false)
  const [allAccounts, setAllAccounts] = useState<BrokerAccount[] | null>(null)
  const [allErr, setAllErr] = useState<string | null>(null)
  const [allLoading, setAllLoading] = useState(false)

  const [screen, setScreen] = useState<Screen>({ name: 'gallery' })
  const [search, setSearch] = useState('')

  // ── Load the supported-broker registry (enriched with static fallback). ──────
  useEffect(() => {
    let alive = true
    AutoTradeAPI.supportedBrokers()
      .then((res) => {
        if (!alive) return
        const list = res.brokers?.length ? res.brokers.map(enrich) : FALLBACK_LIST
        setRegistry(list)
      })
      .catch(() => { if (alive) setRegistry(FALLBACK_LIST) })
    return () => { alive = false }
  }, [])

  const load = useCallback(async () => {
    setLoading(true); setListErr(null)
    try {
      const res = await AutoTradeAPI.brokerAccounts(userId)
      setAccounts(res.accounts ?? [])
      setVaultEnabled(res.vault_enabled ?? false)
    } catch (e) {
      setListErr(e instanceof Error ? e.message : 'Could not load your broker accounts.')
    } finally {
      setLoading(false)
    }
  }, [userId])
  useEffect(() => { load() }, [load])

  // Admin: fetch ALL accounts (no user_id filter → backend returns every account).
  const loadAll = useCallback(async () => {
    setAllLoading(true); setAllErr(null)
    try {
      // Passing '' → the q() helper drops the empty user_id → unscoped admin list.
      const res = await AutoTradeAPI.brokerAccounts('')
      setAllAccounts(res.accounts ?? [])
    } catch (e) {
      setAllErr(e instanceof Error ? e.message : 'Could not load all broker connections.')
    } finally {
      setAllLoading(false)
    }
  }, [])
  useEffect(() => { if (isAdmin && allView) loadAll() }, [isAdmin, allView, loadAll])

  const countFor = useCallback(
    (name: BrokerName) => (accounts ?? []).filter((a) => String(a.broker).toLowerCase() === String(name).toLowerCase()).length,
    [accounts],
  )

  const filtered = useMemo(() => {
    const s = search.trim().toLowerCase()
    if (!s) return registry
    return registry.filter((b) => displayName(b).toLowerCase().includes(s) || String(b.broker).toLowerCase().includes(s))
  }, [registry, search])

  const selectedBroker = screen.name === 'setup'
    ? (registry.find((b) => b.broker === screen.broker) ?? FALLBACK[String(screen.broker).toLowerCase()])
    : undefined

  // ── Vault-disabled friendly empty state ────────────────────────────────────
  if (vaultEnabled === false) {
    return (
      <div className="flex flex-col gap-4">
        <Header loading={loading} onRefresh={load} />
        <div className="rounded-2xl border p-5 flex items-start gap-3"
          style={{ borderColor: 'rgba(230,180,80,0.32)', background: 'rgba(230,180,80,0.06)' }}>
          <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.shield(18)}</span>
          <div className="text-[12.5px] leading-relaxed" style={{ color: C.ink2 }}>
            <b style={{ color: C.amber }}>Broker connect isn’t enabled yet.</b>{' '}
            Connecting a broker needs the server vault key so credentials store encrypted.
            Once the operator sets it, this panel will let you connect and activate accounts.
            <div className="mt-2 text-[11px]" style={{ color: C.faint }}>Nothing to do from the UI until then.</div>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-4">
      <Header loading={loading} onRefresh={load} />

      {screen.name === 'setup' ? (
        <GuidedSetup
          broker={selectedBroker}
          userId={userId}
          onBack={() => setScreen({ name: 'gallery' })}
          onConnected={load}
        />
      ) : (
        <>
          {/* ── SCREEN 1 · GALLERY ──────────────────────────────────────────── */}
          <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
            <div className="flex items-center gap-2 flex-wrap mb-3.5">
              <span className="text-[13px] font-semibold" style={{ color: C.ink }}>Choose your broker</span>
              <span className="text-[11px]" style={{ color: C.faint }}>
                {(accounts?.length ?? 0)} connection{(accounts?.length ?? 0) === 1 ? '' : 's'}
              </span>
              <div className="ml-auto relative">
                <span className="absolute left-2.5 top-1/2 -translate-y-1/2" style={{ color: C.faint }}>{ICON.info(13)}</span>
                <input value={search} onChange={(e) => setSearch(e.target.value)}
                  placeholder="Search brokers…"
                  className="rounded-lg pl-8 pr-3 py-2 text-[12.5px] outline-none w-[180px]" style={inputStyle} />
              </div>
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {filtered.map((b) => {
                const n = countFor(b.broker)
                return (
                  <button key={b.broker} type="button"
                    onClick={() => setScreen({ name: 'setup', broker: b.broker })}
                    className="text-left rounded-xl border p-3.5 flex flex-col gap-2.5 transition-colors hover:border-[rgba(63,227,164,0.4)]"
                    style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
                    <div className="flex items-start gap-3">
                      <BrandChip broker={b} />
                      <div className="min-w-0 flex-1">
                        <div className="text-[13px] font-semibold truncate" style={{ color: C.ink }}>{displayName(b)}</div>
                        <div className="mt-1"><ExchangeChips list={b.exchanges} /></div>
                      </div>
                    </div>
                    <div className="flex items-center gap-2 mt-auto">
                      {b.live ? (
                        <span className="text-[11px] font-semibold inline-flex items-center gap-1.5 rounded-lg px-2.5 py-1"
                          style={{ color: '#06130c', background: C.mint }}>
                          {ICON.link(12)} {n > 0 ? 'Add account' : 'Set up'}
                        </span>
                      ) : (
                        <span className="text-[10px] font-semibold uppercase tracking-[0.05em] rounded-full px-2.5 py-1"
                          style={{ color: C.amber, background: 'rgba(230,180,80,0.12)', boxShadow: 'inset 0 0 0 1px rgba(230,180,80,0.4)' }}>
                          Coming soon
                        </span>
                      )}
                      {n > 0 && (
                        <span className="ml-auto text-[10.5px] font-semibold" style={{ color: C.mint }}>Connected: {n}</span>
                      )}
                    </div>
                  </button>
                )
              })}
              {!filtered.length && (
                <p className="col-span-full text-[12px] py-6 text-center" style={{ color: C.muted }}>
                  No brokers match “{search}”.
                </p>
              )}
            </div>
          </div>

          {/* ── SCREEN 3 · YOUR CONNECTIONS ─────────────────────────────────── */}
          <YourConnections
            accounts={accounts} loading={loading} listErr={listErr} registry={registry}
            userId={userId} onReload={load} onRetry={load}
          />

          {/* ── ADMIN · all-users monitoring table ──────────────────────────── */}
          {isAdmin && (
            <AdminAllUsers
              on={allView} onToggle={() => setAllView((v) => !v)}
              accounts={allAccounts} loading={allLoading} err={allErr}
              registry={registry} onReload={loadAll}
            />
          )}
        </>
      )}
    </div>
  )
}

// ── Header ───────────────────────────────────────────────────────────────────
function Header({ loading, onRefresh }: { loading: boolean; onRefresh: () => void }) {
  return (
    <div className="flex items-center gap-2 flex-wrap">
      <span style={{ color: C.mint }}>{ICON.link(16)}</span>
      <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Broker accounts</span>
      <span className="text-[11px]" style={{ color: C.faint }}>
        Pick a broker, follow the steps, connect — as easy as eating a donut.
      </span>
      <button type="button" disabled={loading} onClick={onRefresh}
        className="ml-auto text-[11.5px] px-2.5 py-1.5 rounded-lg transition-colors disabled:opacity-40"
        style={{ color: C.muted, border: `1px solid ${C.line}` }}>
        {loading ? 'Refreshing…' : 'Refresh'}
      </button>
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// SCREEN 2 · GUIDED SETUP — two-pane. Runs the create → login → paste → activate
// lifecycle with step-by-step inline status. Coming-soon → steps shown, connect
// disabled. api_secret write-only: snapshot → send once → clear from state.
// ════════════════════════════════════════════════════════════════════════════
type Phase = 'idle' | 'saving' | 'awaiting_token' | 'activating'

function GuidedSetup({
  broker, userId, onBack, onConnected,
}: {
  broker?: SupportedBroker; userId: number | string; onBack: () => void; onConnected: () => void
}) {
  const live = broker?.live ?? false
  const fields = useMemo(() => fieldsFor(broker), [broker])
  const setup = broker?.setup

  const [label, setLabel] = useState('')
  const [values, setValues] = useState<Record<string, string>>({})
  const [reveal, setReveal] = useState<Record<string, boolean>>({})
  const [phase, setPhase] = useState<Phase>('idle')
  const [requestToken, setRequestToken] = useState('')
  const [err, setErr] = useState<string | null>(null)
  const [note, setNote] = useState<string | null>(null)
  // The just-created account id, used for the login-url + token-refresh steps.
  const acctIdRef = useRef<string | null>(null)

  const setVal = (name: string, v: string) => setValues((s) => ({ ...s, [name]: v }))

  const busy = phase === 'saving' || phase === 'activating'

  // Map the dynamic field values → the create-request api_key / api_secret keys.
  const mapped = useCallback(() => {
    let api_key = '', api_secret = ''
    for (const f of fields) {
      const v = (values[f.name] ?? '').trim()
      if (f.maps_to === 'api_key') api_key = api_key || v
      if (f.maps_to === 'api_secret') api_secret = api_secret || v
    }
    return { api_key, api_secret }
  }, [fields, values])

  const onConnect = useCallback(async () => {
    setErr(null); setNote(null)
    if (!broker) { setErr('Pick a broker first.'); return }
    if (!live) { setErr('This broker isn’t live yet — we’ll enable it soon.'); return }
    if (!label.trim()) { setErr('Give this connection a name first.'); return }
    for (const f of fields) {
      if (f.required && !(values[f.name] ?? '').trim()) { setErr(`${f.label} is required.`); return }
    }
    const { api_key, api_secret } = mapped()
    if (!api_key || !api_secret) { setErr('API key and secret are both required.'); return }

    setPhase('saving'); setNote('Saving your keys (encrypted)…')
    // Snapshot + CLEAR every secret field from state right away so it never
    // lingers in React state / the DOM after this tick.
    const secretNames = fields.filter((f) => f.secret).map((f) => f.name)
    setValues((s) => { const n = { ...s }; for (const k of secretNames) n[k] = ''; return n })
    const savedLabel = label.trim()
    try {
      const acct = await AutoTradeAPI.createBrokerAccount({
        user_id: userId, broker: broker.broker, account_label: savedLabel, api_key, api_secret,
      })
      acctIdRef.current = acct.broker_account_id
      // Non-secret fields cleared too (keys aren’t needed again).
      setValues((s) => { const n = { ...s }; for (const f of fields) n[f.name] = ''; return n })
      setNote('Opening broker login…')
      // Auto-capture handshake: record the pending connection so the redirect
      // landing at /power/autotrade/connect can finish it without copy-paste.
      // Written right before opening the login popup; the listener below clears
      // it on success. Contains no secret — just ids + broker name.
      try {
        localStorage.setItem('kanida.brokerConnect', JSON.stringify({
          broker_account_id: acct.broker_account_id,
          user_id: userId,
          broker: broker.broker,
        }))
      } catch { /* storage blocked — manual paste fallback still works */ }
      try {
        const res = await AutoTradeAPI.brokerLoginUrl(acct.broker_account_id, userId)
        if (res.login_url) window.open(res.login_url, '_blank', 'noopener,noreferrer,width=520,height=720')
        setPhase('awaiting_token')
        setNote('Sign in on the broker page — we’ll finish automatically. Or paste the request_token below if it doesn’t.')
      } catch {
        setPhase('awaiting_token')
        setNote('Saved. We couldn’t auto-open the login — the connection is below; paste your request_token to finish.')
      }
    } catch (e) {
      setPhase('idle')
      setErr(e instanceof Error ? e.message : 'Could not save the broker account.')
    }
  }, [broker, live, label, fields, values, mapped, userId])

  const onActivate = useCallback(async () => {
    if (!acctIdRef.current) { setErr('No pending connection to activate.'); return }
    if (!requestToken.trim()) { setErr('Paste the request_token from the broker login first.'); return }
    setErr(null); setPhase('activating'); setNote('Activating…')
    try {
      await AutoTradeAPI.refreshBrokerToken(acctIdRef.current, requestToken.trim(), userId)
      setPhase('idle'); setRequestToken(''); setNote(null)
      setLabel('')
      onConnected()
      onBack()  // back to the gallery → the new ACTIVE card shows in Your connections
    } catch (e) {
      setPhase('awaiting_token')
      setErr(e instanceof Error ? e.message : 'Could not activate. Check the request_token and try again.')
    }
  }, [requestToken, userId, onConnected, onBack])

  // ── Auto-capture handshake ─────────────────────────────────────────────────
  // The /power/autotrade/connect landing (opened as the broker popup's redirect
  // target) postMessages the result back here. We trust it ONLY when it comes
  // from our own origin. ok:true → run the same success path as the manual
  // Activate. ok:false → reveal the manual paste box as the safety net.
  // Refs keep this once-registered listener pointing at the latest handlers.
  const onConnectedRef = useRef(onConnected)
  const onBackRef = useRef(onBack)
  useEffect(() => { onConnectedRef.current = onConnected }, [onConnected])
  useEffect(() => { onBackRef.current = onBack }, [onBack])

  useEffect(() => {
    function onMessage(event: MessageEvent) {
      // SECURITY: reject anything not from our own origin.
      if (event.origin !== window.location.origin) return
      const data = event.data as { type?: string; ok?: boolean; error?: string } | null
      if (!data || data.type !== 'kanida-broker-connected') return
      if (data.ok) {
        try { localStorage.removeItem('kanida.brokerConnect') } catch { /* ignore */ }
        setPhase('idle'); setRequestToken(''); setErr(null); setNote(null); setLabel('')
        onConnectedRef.current()
        onBackRef.current()  // back to the gallery → new ACTIVE card shows
      } else {
        // Auto-activate failed → reveal the manual paste box as the fallback.
        setPhase('awaiting_token')
        setErr(data.error || 'Automatic connect didn’t complete — paste the request_token below to finish.')
      }
    }
    window.addEventListener('message', onMessage)
    return () => window.removeEventListener('message', onMessage)
  }, [])

  return (
    <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
      {/* header */}
      <div className="flex items-center gap-3 mb-4">
        <button type="button" onClick={onBack}
          className="inline-flex items-center gap-1 text-[11.5px] font-semibold px-2.5 py-1.5 rounded-lg transition-colors"
          style={{ color: C.muted, border: `1px solid ${C.line}` }}>
          {ICON.back(13)} Brokers
        </button>
        <BrandChip broker={broker} size={34} />
        <div className="min-w-0">
          <div className="text-[14px] font-semibold truncate" style={{ color: C.ink }}>{displayName(broker)}</div>
          <div className="mt-0.5"><ExchangeChips list={broker?.exchanges} /></div>
        </div>
        {!live && (
          <span className="ml-auto shrink-0 text-[10px] font-semibold uppercase tracking-[0.05em] rounded-full px-2.5 py-1"
            style={{ color: C.amber, background: 'rgba(230,180,80,0.12)', boxShadow: 'inset 0 0 0 1px rgba(230,180,80,0.4)' }}>
            Coming soon
          </span>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* ── LEFT: steps + callback + docs + token note + egress IP ──────────── */}
        <div className="flex flex-col gap-3">
          {setup?.steps?.length ? (
            <div>
              <div className="text-[10px] font-semibold uppercase tracking-[0.05em] mb-2" style={{ color: C.muted }}>Setup steps</div>
              <ol className="flex flex-col gap-2">
                {setup.steps.map((s, i) => (
                  <li key={i} className="flex items-start gap-2.5 text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
                    <span className="grid place-items-center w-5 h-5 rounded-full text-[10px] font-mono font-semibold shrink-0"
                      style={{ background: C.mintDim, color: C.mint }}>{i + 1}</span>
                    <span>{s}</span>
                  </li>
                ))}
              </ol>
            </div>
          ) : (
            <p className="text-[11.5px] leading-snug" style={{ color: C.muted }}>
              Enter your broker API key + secret on the right, then Test &amp; Connect to sign in.
            </p>
          )}

          {setup?.callback_url && (
            <div>
              <div className="text-[10px] font-semibold uppercase tracking-[0.05em] mb-1.5" style={{ color: C.muted }}>
                Redirect / Callback URL
              </div>
              <CopyBox value={setup.callback_url} label="callback URL" />
              <p className="text-[10.5px] leading-snug mt-1" style={{ color: C.faint }}>
                Set this as your Redirect URL on the broker’s developer console.
              </p>
            </div>
          )}

          <div className="flex items-center gap-3 flex-wrap">
            {setup?.docs_url && (
              <a href={setup.docs_url} target="_blank" rel="noopener noreferrer"
                className="inline-flex items-center gap-1.5 text-[11.5px] font-semibold" style={{ color: C.mint }}>
                {ICON.book(13)} View docs
              </a>
            )}
          </div>

          {setup?.token_note && (
            <div className="flex items-start gap-2 rounded-xl border px-3 py-2.5 text-[11px] leading-snug"
              style={{ borderColor: 'rgba(230,180,80,0.28)', background: 'rgba(230,180,80,0.05)', color: C.ink2 }}>
              <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.clock(13)}</span>
              <span>{setup.token_note}</span>
            </div>
          )}

          <EgressIpHelper />
        </div>

        {/* ── RIGHT: connection name + dynamic fields + Test & Connect ─────────── */}
        <div className="flex flex-col gap-3 rounded-xl border p-3.5"
          style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
          <Field label="Connection name" hint="A name to recognise this account — you can add several.">
            <input value={label} onChange={(e) => setLabel(e.target.value)}
              placeholder={`e.g. My ${displayName(broker)}`} disabled={!live || busy}
              className="w-full rounded-lg px-3 py-2 text-[13px] outline-none disabled:opacity-50" style={inputStyle} />
          </Field>

          {fields.map((f) => {
            const shown = !f.secret || reveal[f.name]
            return (
              <Field key={f.name} label={f.label + (f.required ? '' : ' (optional)')}
                hint={f.secret ? 'Write-only — sent once, never shown again.' : undefined}>
                <div className="relative">
                  <input
                    value={values[f.name] ?? ''}
                    onChange={(e) => setVal(f.name, e.target.value)}
                    type={shown ? 'text' : 'password'}
                    autoComplete={f.secret ? 'new-password' : 'off'}
                    placeholder={f.placeholder}
                    disabled={!live || busy}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none font-mono disabled:opacity-50"
                    style={{ ...inputStyle, paddingRight: f.secret ? 40 : undefined }}
                  />
                  {f.secret && (
                    <button type="button" onClick={() => setReveal((s) => ({ ...s, [f.name]: !s[f.name] }))}
                      className="absolute right-2 top-1/2 -translate-y-1/2 text-[10px] font-semibold uppercase"
                      style={{ color: C.muted }} aria-label={shown ? 'Hide' : 'Show'} tabIndex={-1}>
                      {shown ? 'Hide' : 'Show'}
                    </button>
                  )}
                </div>
              </Field>
            )
          })}

          {err && <ErrorNote onClose={() => setErr(null)}>{err}</ErrorNote>}
          {note && !err && (
            <div className="flex items-start gap-2 rounded-lg border px-3 py-2 text-[11.5px] leading-snug"
              style={{ borderColor: 'rgba(63,227,164,0.35)', background: 'rgba(63,227,164,0.06)', color: C.ink2 }}>
              <span className="shrink-0 mt-0.5" style={{ color: C.mint }}>
                {phase === 'awaiting_token' ? ICON.link(13) : ICON.check(13)}
              </span>
              <span>{note}</span>
            </div>
          )}

          {phase === 'awaiting_token' ? (
            <div className="flex flex-col gap-2">
              <span className="text-[11px] leading-snug" style={{ color: C.faint }}>
                Didn’t connect automatically? Paste the <code style={{ color: C.ink2 }}>request_token</code> from the broker page here.
              </span>
              <input value={requestToken} onChange={(e) => setRequestToken(e.target.value)}
                placeholder="request_token"
                className="w-full rounded-lg px-3 py-2 text-[12.5px] outline-none font-mono" style={inputStyle} />
              <div className="flex items-center gap-2">
                <button type="button" onClick={onActivate}
                  className="flex-1 flex items-center justify-center gap-1.5 text-[13px] font-semibold px-4 py-2.5 rounded-xl transition-opacity"
                  style={{ color: '#06130c', background: C.mint }}>
                  {ICON.check(14)} Finish &amp; connect
                </button>
                <button type="button" onClick={() => { setPhase('idle'); setNote(null); setRequestToken('') }}
                  className="text-[11.5px] px-3 py-2.5 rounded-xl" style={{ color: C.muted, border: `1px solid ${C.line}` }}>
                  Cancel
                </button>
              </div>
            </div>
          ) : (
            <button type="button" disabled={!live || busy} onClick={onConnect}
              className="flex items-center justify-center gap-2 px-5 py-2.5 rounded-xl text-[13px] font-semibold transition-opacity disabled:opacity-40 disabled:cursor-not-allowed"
              style={{ color: '#06130c', background: C.mint }}>
              {phase === 'saving' ? 'Saving…' : <>{ICON.link(14)} Test &amp; Connect</>}
            </button>
          )}

          {!live && (
            <p className="text-[11px] leading-snug" style={{ color: C.amber }}>
              This broker isn’t live yet — we’ll enable it soon.
            </p>
          )}
        </div>
      </div>
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// SCREEN 3 · YOUR CONNECTIONS — per-account cards with lifecycle actions.
// ════════════════════════════════════════════════════════════════════════════
function YourConnections({
  accounts, loading, listErr, registry, userId, onReload, onRetry,
}: {
  accounts: BrokerAccount[] | null; loading: boolean; listErr: string | null
  registry: SupportedBroker[]; userId: number | string; onReload: () => void; onRetry: () => void
}) {
  const [rowBusy, setRowBusy] = useState<string | null>(null)
  const [rowErr, setRowErr] = useState<string | null>(null)
  const [connectId, setConnectId] = useState<string | null>(null)
  const [requestToken, setRequestToken] = useState('')
  const [health, setHealth] = useState<Record<string, BrokerAccountHealth | { error: string }>>({})

  const metaFor = useCallback(
    (name: BrokerName) => registry.find((b) => String(b.broker).toLowerCase() === String(name).toLowerCase())
      ?? FALLBACK[String(name).toLowerCase()],
    [registry],
  )

  const onReconnect = useCallback(async (a: BrokerAccount) => {
    setRowErr(null); setRowBusy(a.broker_account_id); setRequestToken('')
    try {
      const res = await AutoTradeAPI.brokerLoginUrl(a.broker_account_id, userId)
      if (res.login_url) window.open(res.login_url, '_blank', 'noopener,noreferrer,width=520,height=720')
      setConnectId(a.broker_account_id)
    } catch (e) {
      setRowErr(e instanceof Error ? e.message : 'Could not get the broker login URL.')
    } finally { setRowBusy(null) }
  }, [userId])

  const onSubmitToken = useCallback(async (a: BrokerAccount) => {
    if (!requestToken.trim()) { setRowErr('Paste the request_token from the broker login first.'); return }
    setRowErr(null); setRowBusy(a.broker_account_id)
    try {
      await AutoTradeAPI.refreshBrokerToken(a.broker_account_id, requestToken.trim(), userId)
      setConnectId(null); setRequestToken(''); onReload()
    } catch (e) {
      setRowErr(e instanceof Error ? e.message : 'Could not activate. Check the request_token and try again.')
    } finally { setRowBusy(null) }
  }, [requestToken, userId, onReload])

  const onHealth = useCallback(async (a: BrokerAccount) => {
    setRowErr(null); setRowBusy(a.broker_account_id)
    try {
      const res = await AutoTradeAPI.brokerAccountHealth(a.broker_account_id, userId)
      setHealth((h) => ({ ...h, [a.broker_account_id]: res }))
    } catch (e) {
      setHealth((h) => ({ ...h, [a.broker_account_id]: { error: e instanceof Error ? e.message : 'Health check failed.' } }))
    } finally { setRowBusy(null) }
  }, [userId])

  const onDelete = useCallback(async (a: BrokerAccount) => {
    if (!window.confirm(
      `Remove "${a.account_label}" (${displayName(metaFor(a.broker), a.broker)})? ` +
      `This deletes its stored key/secret/token and cannot be undone.`,
    )) return
    setRowErr(null); setRowBusy(a.broker_account_id)
    try {
      await AutoTradeAPI.deleteBrokerAccount(a.broker_account_id, userId)
      if (connectId === a.broker_account_id) { setConnectId(null); setRequestToken('') }
      setHealth((h) => { const n = { ...h }; delete n[a.broker_account_id]; return n })
      onReload()
    } catch (e) {
      setRowErr(e instanceof Error ? e.message : 'Could not remove the account.')
    } finally { setRowBusy(null) }
  }, [userId, connectId, onReload, metaFor])

  return (
    <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
      <div className="flex items-center gap-2 mb-3">
        <span style={{ color: C.mint }}>{ICON.user(15)}</span>
        <span className="text-[13px] font-semibold" style={{ color: C.ink }}>Your connections</span>
      </div>

      {rowErr && <div className="mb-3"><ErrorNote onClose={() => setRowErr(null)}>{rowErr}</ErrorNote></div>}

      {loading && accounts === null ? (
        <p className="text-[12px]" style={{ color: C.muted }}>Loading your broker accounts…</p>
      ) : listErr ? (
        <div className="flex items-start gap-2 text-[12px] leading-snug" style={{ color: C.ink2 }}>
          <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
          <span>{listErr} <button type="button" onClick={onRetry} className="underline" style={{ color: C.mint }}>Retry</button></span>
        </div>
      ) : !accounts?.length ? (
        <p className="text-[12.5px]" style={{ color: C.muted }}>
          No connections yet. Pick a broker above to set one up.
        </p>
      ) : (
        <ul className="flex flex-col gap-2.5">
          {accounts.map((a) => {
            const meta = metaFor(a.broker)
            const busy = rowBusy === a.broker_account_id
            const st = (a.status ?? '').toUpperCase()
            const expired = st === 'EXPIRED' || st === 'REVOKED' || st === 'ERROR'
            const isConnecting = connectId === a.broker_account_id
            const h = health[a.broker_account_id]
            const lastVerified = typeof a.last_health_at === 'string' ? a.last_health_at
              : typeof a.last_verified_at === 'string' ? a.last_verified_at : null
            // Token lifecycle (all optional): last refresh, expiry, last live check.
            const refreshedAt = (typeof a.token_date === 'string' && a.token_date)
              || (typeof a.last_login_at === 'string' && a.last_login_at) || null
            const expiresRaw = (typeof a.token_expiry === 'string' && a.token_expiry)
              || (typeof a.token_expires_at === 'string' && a.token_expires_at)
              || (typeof meta?.capabilities?.token_lifetime === 'string' ? meta.capabilities.token_lifetime : null)
            // ISO IST (…+05:30) → "2026-07-06 06:00" (kept in IST, no tz conversion).
            const fmtWhen = (s: string) => /^\d{4}-\d\d-\d\dT/.test(s) ? `${s.slice(0, 16).replace('T', ' ')} IST` : s
            const expiresLabel = expiresRaw ? fmtWhen(expiresRaw) : null
            return (
              <li key={a.broker_account_id} className="rounded-xl border p-3.5"
                style={{ borderColor: expired ? 'rgba(232,115,107,0.3)' : C.line2, background: 'rgba(255,255,255,0.02)' }}>
                <div className="flex items-start gap-3 flex-wrap">
                  <BrandChip broker={meta} size={38} />
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-[13px] font-semibold" style={{ color: C.ink }}>{a.account_label || '—'}</span>
                      <span className="text-[11px]" style={{ color: C.muted }}>· {displayName(meta, a.broker)}</span>
                      <StatusPill status={a.status} />
                    </div>
                    {/* Token lifecycle — validity · last refresh · expiry · last live check */}
                    <div className="mt-1 flex items-center gap-x-3 gap-y-0.5 flex-wrap text-[11px]" style={{ color: C.faint }}>
                      <span>key <code style={{ color: C.ink2 }}>{a.api_key_masked || '—'}</code></span>
                      <span>token{' '}
                        {a.has_token
                          ? <b style={{ color: expired ? C.red : C.mint }}>{expired ? 'expired' : 'valid'}</b>
                          : <b style={{ color: C.muted }}>none</b>}
                      </span>
                      {refreshedAt && <span>refreshed <b style={{ color: C.ink2 }}>{fmtWhen(refreshedAt)}</b></span>}
                      {expiresLabel && <span>expires <b style={{ color: C.ink2 }}>{expiresLabel}</b></span>}
                      {lastVerified && (
                        <span>checked {fmtWhen(lastVerified)}
                          {typeof a.last_health_status === 'string' && a.last_health_status
                            ? <> · <b style={{ color: expired ? C.amber : C.mint }}>{a.last_health_status}</b></>
                            : null}
                        </span>
                      )}
                    </div>
                    {expired && (
                      <p className="mt-1.5 text-[11px]" style={{ color: C.red }}>
                        Token expired — one click to renew.
                      </p>
                    )}
                    {h && (
                      <div className="mt-1.5 flex items-start gap-1.5 text-[11px] leading-snug">
                        {'error' in h ? (
                          <>
                            <span className="shrink-0 mt-px" style={{ color: C.red }}>{ICON.info(12)}</span>
                            <span style={{ color: C.ink2 }}>Health: {String((h as { error: string }).error)}</span>
                          </>
                        ) : (
                          <>
                            <span className="shrink-0 mt-px" style={{ color: h.ok ? C.mint : C.amber }}>
                              {h.ok ? ICON.check(12) : ICON.info(12)}
                            </span>
                            <span style={{ color: C.ink2 }}>
                              Health: <b style={{ color: h.ok ? C.mint : C.amber }}>{h.ok ? 'OK' : (h.status || 'not ready')}</b>
                              {h.detail ? ` — ${h.detail}` : ''}
                            </span>
                          </>
                        )}
                      </div>
                    )}
                  </div>
                  <div className="flex items-center gap-2 shrink-0 flex-wrap justify-end">
                    <button type="button" disabled={busy} onClick={() => onReconnect(a)}
                      className="flex items-center gap-1.5 text-[11.5px] font-semibold px-3 py-1.5 rounded-lg transition-opacity disabled:opacity-40"
                      style={expired
                        ? { color: '#06130c', background: C.mint }
                        : { color: C.mint, background: 'rgba(63,227,164,0.10)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.35)' }}>
                      {ICON.link(12)} {busy && !isConnecting ? 'Working…' : (a.has_token ? 'Reconnect' : 'Generate token')}
                    </button>
                    <button type="button" disabled={busy} onClick={() => onHealth(a)}
                      className="flex items-center gap-1.5 text-[11.5px] font-semibold px-3 py-1.5 rounded-lg transition-opacity disabled:opacity-40"
                      style={{ color: C.ink2, background: 'rgba(255,255,255,0.03)', boxShadow: `inset 0 0 0 1px ${C.line2}` }}>
                      {ICON.shield(12)} Health-check
                    </button>
                    <button type="button" disabled={busy} onClick={() => onDelete(a)}
                      className="flex items-center gap-1.5 text-[11.5px] font-semibold px-3 py-1.5 rounded-lg transition-opacity disabled:opacity-40"
                      style={{ color: C.red, background: 'rgba(232,115,107,0.10)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }}>
                      {ICON.close(12)} Delete
                    </button>
                  </div>
                </div>

                {isConnecting && (
                  <div className="mt-3 rounded-lg border px-3 py-3"
                    style={{ borderColor: 'rgba(63,227,164,0.3)', background: 'rgba(63,227,164,0.05)' }}>
                    <p className="text-[11.5px] leading-snug mb-2" style={{ color: C.ink2 }}>
                      Signed in on the broker page? Copy the{' '}
                      <code style={{ color: C.ink }}>request_token=…</code> from that page’s address bar and paste it here.
                    </p>
                    <div className="flex items-center gap-2 flex-wrap">
                      <input value={requestToken} onChange={(e) => setRequestToken(e.target.value)}
                        placeholder="request_token"
                        className="flex-1 min-w-[180px] rounded-lg px-3 py-2 text-[12.5px] outline-none font-mono" style={inputStyle} />
                      <button type="button" disabled={busy} onClick={() => onSubmitToken(a)}
                        className="flex items-center gap-1.5 text-[12px] font-semibold px-3.5 py-2 rounded-lg transition-opacity disabled:opacity-40"
                        style={{ color: '#06130c', background: C.mint }}>
                        {ICON.check(13)} {busy ? 'Activating…' : 'Activate'}
                      </button>
                      <button type="button" onClick={() => { setConnectId(null); setRequestToken('') }}
                        className="text-[11.5px] px-3 py-2 rounded-lg" style={{ color: C.muted, border: `1px solid ${C.line}` }}>
                        Cancel
                      </button>
                    </div>
                  </div>
                )}
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// ADMIN · ALL-USERS — compact monitoring table (broker · user · label · status ·
// last verified) with Health-check + Delete per row. Backend returns all accounts
// for admin when no user_id filter is passed.
// ════════════════════════════════════════════════════════════════════════════
function AdminAllUsers({
  on, onToggle, accounts, loading, err, registry, onReload,
}: {
  on: boolean; onToggle: () => void
  accounts: BrokerAccount[] | null; loading: boolean; err: string | null
  registry: SupportedBroker[]; onReload: () => void
}) {
  const [rowBusy, setRowBusy] = useState<string | null>(null)
  const [rowErr, setRowErr] = useState<string | null>(null)
  const [health, setHealth] = useState<Record<string, boolean | string>>({})

  const metaFor = useCallback(
    (name: BrokerName) => registry.find((b) => String(b.broker).toLowerCase() === String(name).toLowerCase())
      ?? FALLBACK[String(name).toLowerCase()],
    [registry],
  )
  const ownerOf = (a: BrokerAccount) =>
    a.user_id != null ? String(a.user_id) : a.owner_user_id != null ? String(a.owner_user_id) : '—'

  const onHealth = useCallback(async (a: BrokerAccount) => {
    setRowErr(null); setRowBusy(a.broker_account_id)
    const uid = a.user_id ?? a.owner_user_id ?? ''
    try {
      const res = await AutoTradeAPI.brokerAccountHealth(a.broker_account_id, uid as number | string)
      setHealth((h) => ({ ...h, [a.broker_account_id]: res.ok ? true : (res.status || 'not ready') }))
    } catch (e) {
      setHealth((h) => ({ ...h, [a.broker_account_id]: e instanceof Error ? e.message : 'failed' }))
    } finally { setRowBusy(null) }
  }, [])

  const onDelete = useCallback(async (a: BrokerAccount) => {
    if (!window.confirm(`ADMIN: remove "${a.account_label}" (${a.broker}, user ${ownerOf(a)})? This cannot be undone.`)) return
    setRowErr(null); setRowBusy(a.broker_account_id)
    const uid = a.user_id ?? a.owner_user_id ?? ''
    try {
      await AutoTradeAPI.deleteBrokerAccount(a.broker_account_id, uid as number | string)
      onReload()
    } catch (e) {
      setRowErr(e instanceof Error ? e.message : 'Could not remove the account.')
    } finally { setRowBusy(null) }
  }, [onReload])

  return (
    <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
      <div className="flex items-center gap-2 flex-wrap">
        <span style={{ color: C.amber }}>{ICON.shield(15)}</span>
        <span className="text-[13px] font-semibold" style={{ color: C.ink }}>All users</span>
        <span className="text-[10px] font-mono uppercase tracking-[0.06em] rounded-full px-2 py-0.5"
          style={{ color: C.amber, background: 'rgba(230,180,80,0.12)', boxShadow: 'inset 0 0 0 1px rgba(230,180,80,0.4)' }}>
          Admin
        </span>
        <span className="text-[11px]" style={{ color: C.faint }}>Every broker connection, for monitoring.</span>
        <button type="button" onClick={onToggle}
          className="ml-auto text-[11.5px] font-semibold px-3 py-1.5 rounded-lg transition-colors"
          style={on ? { color: '#06130c', background: C.mint } : { color: C.muted, border: `1px solid ${C.line}` }}>
          {on ? 'Hide' : 'Show all connections'}
        </button>
      </div>

      {on && (
        <div className="mt-3">
          {rowErr && <div className="mb-3"><ErrorNote onClose={() => setRowErr(null)}>{rowErr}</ErrorNote></div>}
          {loading && accounts === null ? (
            <p className="text-[12px]" style={{ color: C.muted }}>Loading all connections…</p>
          ) : err ? (
            <div className="flex items-start gap-2 text-[12px]" style={{ color: C.ink2 }}>
              <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
              <span>{err} <button type="button" onClick={onReload} className="underline" style={{ color: C.mint }}>Retry</button></span>
            </div>
          ) : !accounts?.length ? (
            <p className="text-[12.5px]" style={{ color: C.muted }}>No broker connections across users yet.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-[11.5px]" style={{ color: C.ink2 }}>
                <thead>
                  <tr className="text-left" style={{ color: C.muted }}>
                    <th className="font-semibold uppercase tracking-[0.04em] text-[10px] py-1.5 pr-3">Broker</th>
                    <th className="font-semibold uppercase tracking-[0.04em] text-[10px] py-1.5 pr-3">User</th>
                    <th className="font-semibold uppercase tracking-[0.04em] text-[10px] py-1.5 pr-3">Label</th>
                    <th className="font-semibold uppercase tracking-[0.04em] text-[10px] py-1.5 pr-3">Status</th>
                    <th className="font-semibold uppercase tracking-[0.04em] text-[10px] py-1.5 pr-3">Last verified</th>
                    <th className="font-semibold uppercase tracking-[0.04em] text-[10px] py-1.5">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {accounts.map((a) => {
                    const busy = rowBusy === a.broker_account_id
                    const hv = health[a.broker_account_id]
                    const lastVerified = typeof a.last_health_at === 'string' ? a.last_health_at
                      : typeof a.last_verified_at === 'string' ? a.last_verified_at : '—'
                    return (
                      <tr key={a.broker_account_id} className="border-t" style={{ borderColor: C.line2 }}>
                        <td className="py-2 pr-3">
                          <span className="inline-flex items-center gap-1.5">
                            <BrandChip broker={metaFor(a.broker)} size={20} />
                            {displayName(metaFor(a.broker), a.broker)}
                          </span>
                        </td>
                        <td className="py-2 pr-3 font-mono">{ownerOf(a)}</td>
                        <td className="py-2 pr-3">{a.account_label || '—'}</td>
                        <td className="py-2 pr-3"><StatusPill status={a.status} /></td>
                        <td className="py-2 pr-3">{lastVerified}</td>
                        <td className="py-2">
                          <div className="flex items-center gap-1.5">
                            <button type="button" disabled={busy} onClick={() => onHealth(a)}
                              className="text-[10.5px] font-semibold px-2 py-1 rounded-md transition-opacity disabled:opacity-40"
                              style={{ color: C.ink2, background: 'rgba(255,255,255,0.03)', boxShadow: `inset 0 0 0 1px ${C.line2}` }}>
                              Health
                            </button>
                            <button type="button" disabled={busy} onClick={() => onDelete(a)}
                              className="text-[10.5px] font-semibold px-2 py-1 rounded-md transition-opacity disabled:opacity-40"
                              style={{ color: C.red, background: 'rgba(232,115,107,0.10)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }}>
                              Delete
                            </button>
                            {hv !== undefined && (
                              <span className="text-[10px]" style={{ color: hv === true ? C.mint : C.amber }}>
                                {hv === true ? 'OK' : String(hv)}
                              </span>
                            )}
                          </div>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1.5">
      <label className="text-[11px] font-semibold uppercase tracking-[0.05em]" style={{ color: C.muted }}>{label}</label>
      {children}
      {hint && <span className="text-[10.5px] leading-snug" style={{ color: C.faint }}>{hint}</span>}
    </div>
  )
}
