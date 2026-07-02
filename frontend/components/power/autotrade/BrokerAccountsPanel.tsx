'use client'

/**
 * BrokerAccountsPanel — Tradetron-style, per-user "connect your broker" flow.
 *
 * The guided onboarding for the (now-deployed) broker-AGNOSTIC AutoTrade backend.
 * A user picks a broker → sees THAT broker's step guide + capability badges →
 * enters credentials once → authorizes on the broker's own site → pastes the
 * returned request_token → the account goes PENDING → ACTIVE. Daily-expiry
 * brokers (Kite) surface a one-click Reconnect each morning; every account has a
 * live Health-check.
 *
 * HARD HONESTY / SECURITY:
 *   • api_secret is WRITE-ONLY — sent ONCE on connect, consumed server-side,
 *     never read back by any endpoint, and CLEARED from React state the instant
 *     the create call resolves (success or fail). We never display a secret.
 *   • We only ever show the masked api_key, whether a secret/token is on file, the
 *     account status, and a live health string. No secret ever lives in the DOM.
 *   • The broker dropdown is sourced from GET /brokers/supported — live brokers
 *     are selectable, non-live are shown disabled with a "coming soon" tag +
 *     their capability badges. A backend miss falls back to a static registry.
 *   • vault_enabled=false → a calm, friendly empty state (the operator must set
 *     the server vault key). NOT an error.
 *   • Every backend field degrades to "—"; nothing is fabricated. This panel
 *     places NO order and changes NO trading logic — it manages broker-account
 *     records + their daily login token only. Live trading stays gated
 *     server-side by FALCON_AUTOTRADE_ENABLED.
 *
 * Transport: lib/autotrade-api.ts → same-origin /api/falcon-proxy (the proxy
 * forwards the user's power_jwt / injects the operator token server-side).
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import { C, ICON } from '@/components/power/shared/cotrade-kit'
import {
  AutoTradeAPI,
  type BrokerAccount,
  type BrokerAccountHealth,
  type BrokerCapabilities,
  type BrokerName,
  type SupportedBroker,
} from '@/lib/autotrade-api'

const inputStyle: React.CSSProperties = {
  background: 'rgba(255,255,255,0.03)',
  border: `1px solid ${C.line2}`,
  color: C.ink,
}

// ════════════════════════════════════════════════════════════════════════════
// PER-BROKER ONBOARDING GUIDES — mirrors docs/design/BROKER_ONBOARDING_GUIDES.md.
// A small config map keyed by broker name: friendly label, whether API access is
// paid/required, how to get creds, the connect steps, the daily-login/refresh
// note, how to confirm active, and MTF/GTT limits. Rendered after broker select.
// This is STATIC explanatory content; the live/disabled state + capability
// badges come from the backend registry (GET /brokers/supported).
// ════════════════════════════════════════════════════════════════════════════
type BrokerGuide = {
  label: string
  // The developer console where creds are created.
  console?: { label: string; url: string }
  // API-access note (paid? free? enable where?).
  apiAccess: string
  // How to obtain api_key / api_secret.
  getCreds: string
  // Ordered connect/authorize steps (rendered as a numbered list).
  connectSteps: string[]
  // Daily-login / token-refresh behaviour.
  refreshNote: string
  // How the user confirms the account is ACTIVE.
  confirmActive: string
  // Capability / limitation prose (MTF / GTT / order types).
  limits: string
}

const GUIDES: Record<string, BrokerGuide> = {
  zerodha: {
    label: 'Zerodha (Kite Connect)',
    console: { label: 'developers.kite.trade', url: 'https://developers.kite.trade' },
    apiAccess: 'Required. Kite Connect is a paid developer add-on (subscription per app).',
    getCreds: 'Create a Kite Connect app → note the API Key and API Secret. Set the app’s redirect URL to our callback.',
    connectSteps: [
      'Enter your API Key + API Secret below and Connect the account (it starts PENDING).',
      'Click Connect on the account card → we open the Kite login in a new tab.',
      'Log in + authorize on Kite → it redirects back with a request_token in the URL.',
      'Copy that request_token, paste it here → we exchange it for the daily access token.',
    ],
    refreshNote: 'Daily login required. Kite access tokens expire every morning (~06:00 IST) and there is no refresh token — reconnect each trading day (one click). The card shows EXPIRED → Reconnect each morning.',
    confirmActive: 'Green ACTIVE badge on the account card, backed by a live kite.profile() ping (use Health-check any time).',
    limits: 'MTF ✓, GTT-OCO ✓ (broker-held stop/target backup), SL/SL-M ✓. Live orders also require our SEBI-registered static egress IP on the app’s allowed-IPs. AutoTrade fully supported.',
  },
  fivepaisa: {
    label: 'FivePaisa (5paisa Xstream)',
    console: { label: '5paisa developer portal', url: 'https://xstream.5paisa.com/dev-docs/user-authentication-system/access-token' },
    apiAccess: 'Required. 5paisa Developer / Xstream API — enable it in your 5paisa account and create an API app.',
    getCreds: 'From the 5paisa developer portal, obtain App/User Key + Encryption Key + Client/User ID. TOTP two-factor must be enabled (Security Settings → Enable TOTP).',
    connectSteps: [
      'Enable TOTP in 5paisa and create your API app to get your keys.',
      'Enter your credentials below and Connect the account.',
      'Click Connect on the card → complete the OAuth login (with TOTP) on 5paisa.',
      'A request token is returned → paste it here → we exchange it for the access token.',
    ],
    refreshNote: 'Daily. The access token is valid through the day (expires daily), so a daily reconnect is required (similar to Kite). No long-lived refresh token (to be certified).',
    confirmActive: 'ACTIVE badge, backed by a lightweight authenticated call (e.g. margin/holdings).',
    limits: 'Orders reference ScripCode (numeric), not symbol — the adapter maps symbols → 5paisa ScripCodes. MTF / GTT / basket / SL support to be certified.',
  },
  rupeezy: {
    label: 'Rupeezy (Vortex API)',
    console: { label: 'Vortex developer portal', url: 'https://vortex.rupeezy.in/docs/1.0/authentication/' },
    apiAccess: 'Required. Vortex API is a free trading API — register an application on the Vortex developer portal.',
    getCreds: 'Create your app → get application_id and x-api-key (keep the x-api-key secret). Configure the redirect URL (and optionally a postback/webhook URL).',
    connectSteps: [
      'Register a Vortex app and note application_id + x-api-key.',
      'Enter your credentials below and Connect the account.',
      'Click Connect on the card → you’re sent to flow.rupeezy.in to log in.',
      'We receive the auth artifact → paste the returned token here → we exchange it for the access token.',
    ],
    refreshNote: 'Session-based token in the Authorization header; exact lifetime/refresh to be certified — the lifecycle layer detects expiry via a health ping and prompts reconnect if needed.',
    confirmActive: 'ACTIVE badge, backed by a Portfolio/positions ping. Rupeezy also pushes postbacks (webhooks) for fills.',
    limits: 'Order management supports all exchanges + order types incl. stop-loss. MTF / GTT / basket support to be certified.',
  },
  upstox:  { label: 'Upstox',   apiAccess: 'Coming soon — integration not yet live.', getCreds: '', connectSteps: [], refreshNote: '', confirmActive: '', limits: '' },
  angel:   { label: 'Angel One', apiAccess: 'Coming soon — integration not yet live.', getCreds: '', connectSteps: [], refreshNote: '', confirmActive: '', limits: '' },
  dhan:    { label: 'Dhan',     apiAccess: 'Coming soon — integration not yet live.', getCreds: '', connectSteps: [], refreshNote: '', confirmActive: '', limits: '' },
}

// Static fallback registry — used ONLY if GET /brokers/supported fails, so the
// panel still works. Matches the deployed backend (zerodha + rupeezy live).
const FALLBACK_BROKERS: SupportedBroker[] = [
  { broker: 'zerodha',   live: true,  capabilities: { auth_kind: 'request_token', has_refresh_token: false, token_lifetime: 'daily ~06:00 IST', supports_gtt: true, supports_mtf: true } },
  { broker: 'rupeezy',   live: true,  capabilities: { auth_kind: 'oauth2_flow', token_lifetime: 'session' } },
  { broker: 'fivepaisa', live: false, capabilities: { auth_kind: 'oauth_request_token', token_lifetime: 'daily' } },
  { broker: 'upstox',    live: false },
  { broker: 'angel',     live: false },
  { broker: 'dhan',      live: false },
]

const guideFor = (b: BrokerName): BrokerGuide =>
  GUIDES[String(b).toLowerCase()] ?? { label: String(b), apiAccess: 'Coming soon.', getCreds: '', connectSteps: [], refreshNote: '', confirmActive: '', limits: '' }
const brokerLabel = (b: BrokerName) => guideFor(b).label

type StatusTone = { color: string; bg: string; ring: string; label: string }
function statusTone(status?: string): StatusTone {
  switch ((status ?? '').toUpperCase()) {
    case 'ACTIVE':
      return { color: C.mint, bg: 'rgba(63,227,164,0.12)', ring: 'rgba(63,227,164,0.42)', label: 'Active' }
    case 'EXPIRED':
      return { color: C.red, bg: 'rgba(232,115,107,0.12)', ring: 'rgba(232,115,107,0.42)', label: 'Expired' }
    case 'REVOKED':
    case 'ERROR':
      return { color: C.red, bg: 'rgba(232,115,107,0.12)', ring: 'rgba(232,115,107,0.42)', label: (status || 'Error') }
    case 'PENDING':
      return { color: C.amber, bg: 'rgba(230,180,80,0.12)', ring: 'rgba(230,180,80,0.42)', label: 'Pending' }
    default:
      return { color: C.muted, bg: 'rgba(255,255,255,0.04)', ring: C.line2, label: status || 'Unknown' }
  }
}

function StatusBadge({ status }: { status?: string }) {
  const t = statusTone(status)
  return (
    <span
      className="inline-flex items-center gap-1.5 text-[10px] font-mono uppercase tracking-[0.06em] rounded-full px-2.5 py-1"
      style={{ color: t.color, background: t.bg, boxShadow: `inset 0 0 0 1px ${t.ring}` }}
    >
      <span className="w-1.5 h-1.5 rounded-full" style={{ background: t.color }} />
      {t.label}
    </span>
  )
}

// A capability pill (MTF / GTT / F&O / refresh). Present = mint, absent = faint.
function CapBadge({ on, label, title }: { on: boolean | undefined; label: string; title?: string }) {
  return (
    <span title={title}
      className="inline-flex items-center gap-1 text-[9.5px] font-semibold uppercase tracking-[0.05em] rounded px-1.5 py-0.5"
      style={on
        ? { color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.35)' }
        : { color: C.faint, background: 'rgba(255,255,255,0.03)', boxShadow: `inset 0 0 0 1px ${C.line2}` }}>
      {label}
    </span>
  )
}

function CapabilityBadges({ caps }: { caps?: BrokerCapabilities }) {
  if (!caps) return null
  return (
    <div className="flex items-center gap-1.5 flex-wrap">
      <CapBadge on={caps.supports_mtf} label="MTF" title="Margin Trading Facility" />
      <CapBadge on={caps.supports_gtt} label="GTT" title="Broker-held Good-Till-Triggered stop/target" />
      {caps.fno !== undefined && <CapBadge on={caps.fno} label="F&O" title="Futures & Options" />}
      <CapBadge on={caps.has_refresh_token} label="refresh"
        title={caps.has_refresh_token ? 'Long-lived refresh token' : 'No refresh token — daily reconnect'} />
      {caps.token_lifetime && (
        <span className="text-[9.5px]" style={{ color: C.faint }} title="Token lifetime">· {caps.token_lifetime}</span>
      )}
    </div>
  )
}

export function BrokerAccountsPanel({ userId }: { userId: number | string }) {
  // Supported-broker registry (dropdown source)
  const [registry, setRegistry] = useState<SupportedBroker[]>(FALLBACK_BROKERS)

  // List + vault state
  const [accounts, setAccounts] = useState<BrokerAccount[] | null>(null)
  const [vaultEnabled, setVaultEnabled] = useState<boolean | null>(null)
  const [loading, setLoading] = useState(false)
  const [listErr, setListErr] = useState<string | null>(null)

  // Connect form
  const [broker, setBroker] = useState<BrokerName>('zerodha')
  const [label, setLabel] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [apiSecret, setApiSecret] = useState('')  // WRITE-ONLY — cleared after submit
  const [creating, setCreating] = useState(false)
  const [createErr, setCreateErr] = useState<string | null>(null)
  const [createOk, setCreateOk] = useState<string | null>(null)
  const [showGuide, setShowGuide] = useState(false)

  // Per-row connect (login URL → paste request_token → refresh)
  const [connectId, setConnectId] = useState<string | null>(null)  // which row is mid-connect
  const [requestToken, setRequestToken] = useState('')
  const [rowBusy, setRowBusy] = useState<string | null>(null)       // id currently working
  const [rowErr, setRowErr] = useState<string | null>(null)
  // Per-row health results (id → last health), so cards show a live liveness read.
  const [health, setHealth] = useState<Record<string, BrokerAccountHealth | { error: string }>>({})

  // ── Load the supported-broker registry (dropdown). Falls back on error. ──────
  useEffect(() => {
    let alive = true
    AutoTradeAPI.supportedBrokers()
      .then((res) => {
        if (!alive) return
        const list = res.brokers?.length ? res.brokers : FALLBACK_BROKERS
        setRegistry(list)
        // Default the dropdown to the first LIVE broker.
        const firstLive = list.find((b) => b.live)
        if (firstLive) setBroker(firstLive.broker)
      })
      .catch(() => { if (alive) setRegistry(FALLBACK_BROKERS) })
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

  const selected = useMemo(
    () => registry.find((b) => b.broker === broker),
    [registry, broker],
  )
  const selectedLive = selected?.live ?? false
  const guide = guideFor(broker)

  // ── Connect a new account — api_secret is WRITE-ONLY and cleared immediately ──
  const onCreate = useCallback(async () => {
    setCreateErr(null); setCreateOk(null)
    if (!selectedLive) { setCreateErr('This broker isn’t live yet — pick a live broker to connect.'); return }
    if (!label.trim() || !apiKey.trim() || !apiSecret.trim()) {
      setCreateErr('Account label, API key and API secret are all required.')
      return
    }
    setCreating(true)
    // Snapshot the secret locally, then CLEAR it from state right away so it never
    // lingers in React state / the DOM after this tick.
    const secret = apiSecret
    setApiSecret('')
    const savedLabel = label.trim()
    try {
      const acct = await AutoTradeAPI.createBrokerAccount({
        user_id: userId,
        broker,
        account_label: savedLabel,
        api_key: apiKey.trim(),
        api_secret: secret,
      })
      setLabel(''); setApiKey('')  // secret already cleared
      await load()
      // SEAMLESS: immediately open the broker login for the just-created account
      // and reveal the paste-token step — so it's one flow (enter keys → sign in
      // → paste token) instead of a separate "Connect" click on the card.
      try {
        const res = await AutoTradeAPI.brokerLoginUrl(acct.broker_account_id, userId)
        if (res.login_url) {
          window.open(res.login_url, '_blank', 'noopener,noreferrer,width=520,height=720')
        }
        setConnectId(acct.broker_account_id)
        setRequestToken('')
        setCreateOk(`Saved ${brokerLabel(broker)}. A login window opened — sign in there, then paste the token below to finish.`)
      } catch {
        // Login-URL fetch failed — the account is still saved; the user can use
        // “Log in” on its card. Don't lose their work over this.
        setCreateOk(`Saved ${brokerLabel(broker)} · ${savedLabel}. Use “Log in” on the account card below to sign in and activate.`)
      }
    } catch (e) {
      setCreateErr(e instanceof Error ? e.message : 'Could not connect the broker account.')
    } finally {
      setCreating(false)
    }
  }, [userId, broker, label, apiKey, apiSecret, selectedLive, load])

  // ── Per-row: open the broker login in a popup, then paste request_token ───────
  const onConnect = useCallback(async (acct: BrokerAccount) => {
    setRowErr(null); setRowBusy(acct.broker_account_id); setRequestToken('')
    try {
      const res = await AutoTradeAPI.brokerLoginUrl(acct.broker_account_id, userId)
      if (res.login_url) {
        // Open the broker's login in a new tab/popup; the user logs in there and
        // copies the request_token from the redirect URL back into the input.
        window.open(res.login_url, '_blank', 'noopener,noreferrer,width=520,height=720')
      }
      setConnectId(acct.broker_account_id)
    } catch (e) {
      setRowErr(e instanceof Error ? e.message : 'Could not get the broker login URL.')
    } finally {
      setRowBusy(null)
    }
  }, [userId])

  const onSubmitToken = useCallback(async (acct: BrokerAccount) => {
    if (!requestToken.trim()) { setRowErr('Paste the request_token from the broker login first.'); return }
    setRowErr(null); setRowBusy(acct.broker_account_id)
    try {
      await AutoTradeAPI.refreshBrokerToken(acct.broker_account_id, requestToken.trim(), userId)
      setConnectId(null); setRequestToken('')
      await load()
    } catch (e) {
      setRowErr(e instanceof Error ? e.message : 'Could not activate the account. Check the request_token and try again.')
    } finally {
      setRowBusy(null)
    }
  }, [requestToken, userId, load])

  // ── Per-row: live health ping (does not mutate the account). ─────────────────
  const onHealth = useCallback(async (acct: BrokerAccount) => {
    setRowErr(null); setRowBusy(acct.broker_account_id)
    try {
      const res = await AutoTradeAPI.brokerAccountHealth(acct.broker_account_id, userId)
      setHealth((h) => ({ ...h, [acct.broker_account_id]: res }))
    } catch (e) {
      setHealth((h) => ({ ...h, [acct.broker_account_id]: { error: e instanceof Error ? e.message : 'Health check failed.' } }))
    } finally {
      setRowBusy(null)
    }
  }, [userId])

  const onDelete = useCallback(async (acct: BrokerAccount) => {
    if (!window.confirm(
      `Remove the broker account "${acct.account_label}" (${brokerLabel(acct.broker)})? ` +
      `This deletes its stored key/secret/token. This cannot be undone.`,
    )) return
    setRowErr(null); setRowBusy(acct.broker_account_id)
    try {
      await AutoTradeAPI.deleteBrokerAccount(acct.broker_account_id, userId)
      if (connectId === acct.broker_account_id) { setConnectId(null); setRequestToken('') }
      setHealth((h) => { const n = { ...h }; delete n[acct.broker_account_id]; return n })
      await load()
    } catch (e) {
      setRowErr(e instanceof Error ? e.message : 'Could not remove the account.')
    } finally {
      setRowBusy(null)
    }
  }, [userId, connectId, load])

  return (
    <div className="flex flex-col gap-4">
      {/* ── Header ───────────────────────────────────────────────────────────── */}
      <div className="flex items-center gap-2 flex-wrap">
        <span style={{ color: C.mint }}>{ICON.link(16)}</span>
        <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Broker accounts</span>
        <span className="text-[11px]" style={{ color: C.faint }}>
          Enter your broker keys once, sign in, and you&apos;re connected.
        </span>
        <button type="button" disabled={loading} onClick={load}
          className="ml-auto text-[11.5px] px-2.5 py-1.5 rounded-lg transition-colors disabled:opacity-40"
          style={{ color: C.muted, border: `1px solid ${C.line}` }}>
          {loading ? 'Refreshing…' : 'Refresh'}
        </button>
      </div>

      {/* ── Vault-disabled friendly empty state ──────────────────────────────── */}
      {vaultEnabled === false ? (
        <div className="rounded-2xl border p-5 flex items-start gap-3"
          style={{ borderColor: 'rgba(230,180,80,0.32)', background: 'rgba(230,180,80,0.06)' }}>
          <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.shield(18)}</span>
          <div className="text-[12.5px] leading-relaxed" style={{ color: C.ink2 }}>
            <b style={{ color: C.amber }}>Broker connect isn&apos;t enabled yet.</b>{' '}
            Connecting a broker account needs the server vault key so credentials can
            be stored encrypted. The operator must set the vault key on the backend —
            once that&apos;s done, this panel will let you connect and activate accounts here.
            <div className="mt-2 text-[11px]" style={{ color: C.faint }}>
              Nothing to do from the UI until then.
            </div>
          </div>
        </div>
      ) : (
        <>
          {/* ── Connect form + per-broker onboarding guide ───────────────────── */}
          <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
            <div className="flex items-center gap-2 mb-3">
              <span style={{ color: C.mint }}>{ICON.bolt(15)}</span>
              <span className="text-[13px] font-semibold" style={{ color: C.ink }}>Connect a broker account</span>
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <Field label="Broker" hint={selectedLive
                ? 'Live integration — selectable and production-wired.'
                : 'Coming soon — not selectable yet.'}>
                <div className="flex items-center gap-2 flex-wrap">
                  <select
                    value={broker}
                    onChange={(e) => setBroker(e.target.value as BrokerName)}
                    className="flex-1 min-w-[160px] rounded-lg px-3 py-2 text-[13px] outline-none"
                    style={inputStyle}
                  >
                    {registry.map((b) => (
                      <option key={b.broker} value={b.broker} disabled={!b.live}
                        style={{ background: '#0b1410', color: b.live ? C.ink : C.faint }}>
                        {brokerLabel(b.broker)}{b.live ? '' : ' — coming soon'}
                      </option>
                    ))}
                  </select>
                  {selectedLive ? (
                    <span className="shrink-0 inline-flex items-center gap-1 text-[10px] font-semibold rounded-full px-2 py-1"
                      style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
                      {ICON.check(11)} live
                    </span>
                  ) : (
                    <span className="shrink-0 inline-flex items-center text-[10px] font-semibold rounded-full px-2 py-1"
                      style={{ color: C.amber, background: 'rgba(230,180,80,0.12)', boxShadow: 'inset 0 0 0 1px rgba(230,180,80,0.4)' }}>
                      coming soon
                    </span>
                  )}
                </div>
                {selected?.capabilities && (
                  <div className="mt-1.5"><CapabilityBadges caps={selected.capabilities} /></div>
                )}
              </Field>

              <Field label="Account label" hint="A name to recognise this account.">
                <input value={label} onChange={(e) => setLabel(e.target.value)}
                  placeholder="e.g. My Zerodha" disabled={!selectedLive}
                  className="w-full rounded-lg px-3 py-2 text-[13px] outline-none disabled:opacity-50" style={inputStyle} />
              </Field>

              <Field label="API key" hint="From your broker's developer console.">
                <input value={apiKey} onChange={(e) => setApiKey(e.target.value)}
                  autoComplete="off" disabled={!selectedLive}
                  placeholder="api_key"
                  className="w-full rounded-lg px-3 py-2 text-[13px] outline-none font-mono disabled:opacity-50" style={inputStyle} />
              </Field>

              <Field label="API secret" hint="Write-only — sent once, never shown again or read back.">
                <input value={apiSecret} onChange={(e) => setApiSecret(e.target.value)}
                  type="password" autoComplete="new-password" disabled={!selectedLive}
                  placeholder="api_secret"
                  className="w-full rounded-lg px-3 py-2 text-[13px] outline-none font-mono disabled:opacity-50" style={inputStyle} />
              </Field>
            </div>

            {/* Per-broker onboarding guide (collapsible) */}
            <div className="mt-4">
              <button type="button" onClick={() => setShowGuide((v) => !v)}
                className="flex items-center gap-2 text-[12px] font-semibold"
                style={{ color: C.mint }}>
                {ICON.book(14)} {showGuide ? 'Hide' : 'Show'} the {guide.label} setup guide
                <span style={{ color: C.faint }}>{showGuide ? ICON.chevron(13) : ICON.chevronR(13)}</span>
              </button>
              {showGuide && <OnboardingGuide guide={guide} live={selectedLive} />}
            </div>

            {createErr && (
              <div className="mt-3 flex items-start gap-2 rounded-lg border px-3 py-2 text-[11.5px] leading-snug"
                style={{ borderColor: 'rgba(232,115,107,0.35)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
                <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(13)}</span>
                <span>{createErr}</span>
              </div>
            )}
            {createOk && (
              <div className="mt-3 flex items-start gap-2 rounded-lg border px-3 py-2 text-[11.5px] leading-snug"
                style={{ borderColor: 'rgba(63,227,164,0.35)', background: 'rgba(63,227,164,0.06)', color: C.ink2 }}>
                <span className="shrink-0 mt-0.5" style={{ color: C.mint }}>{ICON.check(13)}</span>
                <span>{createOk}</span>
              </div>
            )}

            <div className="mt-4 flex items-center gap-3 flex-wrap">
              <button type="button" disabled={creating || !selectedLive} onClick={onCreate}
                className="flex items-center gap-2 px-5 py-2.5 rounded-xl text-[13px] font-semibold transition-opacity disabled:opacity-40 disabled:cursor-not-allowed"
                style={{ color: '#06130c', background: C.mint }}>
                {creating ? 'Connecting…' : <>{ICON.link(14)} Connect &amp; log in</>}
              </button>
              <span className="text-[11px]" style={{ color: C.faint }}>
                One step: we save your keys (encrypted), then open your broker login to finish.
              </span>
            </div>
          </div>

          {/* ── Accounts list ────────────────────────────────────────────────── */}
          <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
            <div className="flex items-center gap-2 mb-3">
              <span style={{ color: C.mint }}>{ICON.user(15)}</span>
              <span className="text-[13px] font-semibold" style={{ color: C.ink }}>Your connected accounts</span>
            </div>

            {rowErr && (
              <div className="mb-3 flex items-start gap-2 rounded-lg border px-3 py-2 text-[11.5px] leading-snug"
                style={{ borderColor: 'rgba(232,115,107,0.35)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}>
                <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(13)}</span>
                <span>{rowErr}</span>
                <button type="button" onClick={() => setRowErr(null)} className="ml-auto shrink-0" style={{ color: C.faint }}>
                  {ICON.close(12)}
                </button>
              </div>
            )}

            {loading && accounts === null ? (
              <p className="text-[12px]" style={{ color: C.muted }}>Loading your broker accounts…</p>
            ) : listErr ? (
              <div className="flex items-start gap-2 text-[12px] leading-snug" style={{ color: C.ink2 }}>
                <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
                <span>{listErr} <button type="button" onClick={load} className="underline" style={{ color: C.mint }}>Retry</button></span>
              </div>
            ) : !accounts?.length ? (
              <p className="text-[12.5px]" style={{ color: C.muted }}>
                No broker accounts connected yet. Use the form above to connect one.
              </p>
            ) : (
              <ul className="flex flex-col gap-2.5">
                {accounts.map((a) => {
                  const isConnecting = connectId === a.broker_account_id
                  const busy = rowBusy === a.broker_account_id
                  const st = (a.status ?? '').toUpperCase()
                  const expired = st === 'EXPIRED' || st === 'REVOKED' || st === 'ERROR'
                  const h = health[a.broker_account_id]
                  return (
                    <li key={a.broker_account_id}
                      className="rounded-xl border p-3.5"
                      style={{ borderColor: expired ? 'rgba(232,115,107,0.3)' : C.line2, background: 'rgba(255,255,255,0.02)' }}>
                      <div className="flex items-start gap-3 flex-wrap">
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-2 flex-wrap">
                            <span className="text-[13px] font-semibold" style={{ color: C.ink }}>{a.account_label || '—'}</span>
                            <span className="text-[11px]" style={{ color: C.muted }}>· {brokerLabel(a.broker)}</span>
                            <StatusBadge status={a.status} />
                          </div>
                          <div className="mt-1 flex items-center gap-3 flex-wrap text-[11px]" style={{ color: C.faint }}>
                            <span>key <code style={{ color: C.ink2 }}>{a.api_key_masked || '—'}</code></span>
                            <span>secret {a.has_secret ? <b style={{ color: C.mint }}>on file</b> : '—'}</span>
                            <span>token {a.has_token ? <b style={{ color: C.mint }}>active</b> : '—'}</span>
                          </div>
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
                          <button type="button" disabled={busy} onClick={() => onConnect(a)}
                            className="flex items-center gap-1.5 text-[11.5px] font-semibold px-3 py-1.5 rounded-lg transition-opacity disabled:opacity-40"
                            style={{ color: '#06130c', background: C.mint }}>
                            {ICON.link(12)} {busy && !isConnecting ? 'Working…' : (a.has_token ? 'Reconnect' : 'Log in')}
                          </button>
                          <button type="button" disabled={busy} onClick={() => onHealth(a)}
                            className="flex items-center gap-1.5 text-[11.5px] font-semibold px-3 py-1.5 rounded-lg transition-opacity disabled:opacity-40"
                            style={{ color: C.mint, background: 'rgba(63,227,164,0.10)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.35)' }}>
                            {ICON.shield(12)} Health-check
                          </button>
                          <button type="button" disabled={busy} onClick={() => onDelete(a)}
                            className="flex items-center gap-1.5 text-[11.5px] font-semibold px-3 py-1.5 rounded-lg transition-opacity disabled:opacity-40"
                            style={{ color: C.red, background: 'rgba(232,115,107,0.10)', boxShadow: 'inset 0 0 0 1px rgba(232,115,107,0.4)' }}>
                            {ICON.close(12)} Delete
                          </button>
                        </div>
                      </div>

                      {/* Paste-back request_token step (after the login popup opened) */}
                      {isConnecting && (
                        <div className="mt-3 rounded-lg border px-3 py-3"
                          style={{ borderColor: 'rgba(63,227,164,0.3)', background: 'rgba(63,227,164,0.05)' }}>
                          <p className="text-[11.5px] leading-snug mb-2" style={{ color: C.ink2 }}>
                            Signed in on the broker page? Copy the{' '}
                            <code style={{ color: C.ink }}>token</code> from that page&apos;s address bar
                            (the <code style={{ color: C.ink }}>request_token=…</code> part) and paste it here to finish.
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
        </>
      )}
    </div>
  )
}

// ── The per-broker onboarding guide (Tradetron-style step list). ──────────────
function OnboardingGuide({ guide, live }: { guide: BrokerGuide; live: boolean }) {
  if (!live) {
    return (
      <div className="mt-2.5 rounded-xl border px-3.5 py-3 text-[11.5px] leading-snug"
        style={{ borderColor: 'rgba(230,180,80,0.35)', background: 'rgba(230,180,80,0.05)', color: C.ink2 }}>
        <b style={{ color: C.amber }}>{guide.label}</b> — {guide.apiAccess}
      </div>
    )
  }
  return (
    <div className="mt-2.5 rounded-xl border px-3.5 py-3.5"
      style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)' }}>
      <dl className="flex flex-col gap-2.5 text-[11.5px] leading-snug" style={{ color: C.ink2 }}>
        <GuideRow term="API access">{guide.apiAccess}{guide.console && (
          <> {' '}<a href={guide.console.url} target="_blank" rel="noopener noreferrer"
            className="underline" style={{ color: C.mint }}>{guide.console.label}</a>.</>
        )}</GuideRow>
        {guide.getCreds && <GuideRow term="Get credentials">{guide.getCreds}</GuideRow>}
        {guide.connectSteps.length > 0 && (
          <div>
            <dt className="text-[10px] font-semibold uppercase tracking-[0.05em] mb-1" style={{ color: C.muted }}>Connect steps</dt>
            <ol className="flex flex-col gap-1 list-none">
              {guide.connectSteps.map((s, i) => (
                <li key={i} className="flex items-start gap-2">
                  <span className="grid place-items-center w-4 h-4 rounded-full text-[9px] font-mono font-semibold shrink-0 mt-px"
                    style={{ background: C.mintDim, color: C.mint }}>{i + 1}</span>
                  <span>{s}</span>
                </li>
              ))}
            </ol>
          </div>
        )}
        {guide.refreshNote && <GuideRow term="Daily login / refresh">{guide.refreshNote}</GuideRow>}
        {guide.confirmActive && <GuideRow term="Confirm active">{guide.confirmActive}</GuideRow>}
        {guide.limits && <GuideRow term="MTF / GTT / limits">{guide.limits}</GuideRow>}
      </dl>
    </div>
  )
}

function GuideRow({ term, children }: { term: string; children: React.ReactNode }) {
  return (
    <div>
      <dt className="text-[10px] font-semibold uppercase tracking-[0.05em] mb-0.5" style={{ color: C.muted }}>{term}</dt>
      <dd>{children}</dd>
    </div>
  )
}

function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1.5">
      <label className="text-[11px] font-semibold uppercase tracking-[0.05em]" style={{ color: C.muted }}>
        {label}
      </label>
      {children}
      {hint && <span className="text-[10.5px] leading-snug" style={{ color: C.faint }}>{hint}</span>}
    </div>
  )
}
