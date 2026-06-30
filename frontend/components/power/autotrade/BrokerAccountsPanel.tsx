'use client'

/**
 * BrokerAccountsPanel — Phase-2 multi-tenant "connect your broker account".
 *
 * Lets the operator (and, when per-user auth exists, a user) connect one or more
 * broker accounts and obtain a daily login token per account, so AutoTrade
 * sessions can run on a CHOSEN account rather than only the global/operator one.
 *
 * HARD HONESTY / SECURITY:
 *   • api_secret is WRITE-ONLY — it is sent ONCE on connect, consumed server-side,
 *     never read back by any endpoint, and CLEARED from React state the instant
 *     the create call resolves (success or fail). We never display a secret.
 *   • We only ever show the masked api_key, whether a secret/token is on file, and
 *     the account status. No secret material ever lives in the rendered DOM.
 *   • vault_enabled=false → a calm, friendly empty state (the operator must set
 *     the server vault key). NOT an error.
 *   • Every backend field degrades to "—"; nothing is fabricated.
 *
 * Transport: lib/autotrade-api.ts → same-origin /api/falcon-proxy (operator token
 * injected server-side). This component places no order and changes no trading
 * logic; it only manages broker-account records + their daily login token.
 */
import { useCallback, useEffect, useState } from 'react'
import { C, ICON } from '@/components/power/shared/cotrade-kit'
import {
  AutoTradeAPI,
  type BrokerAccount,
  type BrokerName,
} from '@/lib/autotrade-api'

const inputStyle: React.CSSProperties = {
  background: 'rgba(255,255,255,0.03)',
  border: `1px solid ${C.line2}`,
  color: C.ink,
}

// Brokers — Zerodha is the verified live integration; the rest are shown as
// "beta — unverified" so the user knows they're not production-validated yet.
const BROKERS: { id: BrokerName; label: string; verified: boolean }[] = [
  { id: 'zerodha', label: 'Zerodha', verified: true },
  { id: 'upstox',  label: 'Upstox',  verified: false },
  { id: 'angel',   label: 'Angel One', verified: false },
  { id: 'dhan',    label: 'Dhan',    verified: false },
  { id: 'fyers',   label: 'Fyers',   verified: false },
]

const brokerLabel = (b: BrokerName) => BROKERS.find((x) => x.id === b)?.label ?? b

type StatusTone = { color: string; bg: string; ring: string; label: string }
function statusTone(status?: string): StatusTone {
  switch ((status ?? '').toUpperCase()) {
    case 'ACTIVE':
      return { color: C.mint, bg: 'rgba(63,227,164,0.12)', ring: 'rgba(63,227,164,0.42)', label: 'Active' }
    case 'EXPIRED':
      return { color: C.red, bg: 'rgba(232,115,107,0.12)', ring: 'rgba(232,115,107,0.42)', label: 'Expired' }
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

export function BrokerAccountsPanel({ userId }: { userId: number | string }) {
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

  // Per-row connect (login URL → paste request_token → refresh)
  const [connectId, setConnectId] = useState<string | null>(null)  // which row is mid-connect
  const [requestToken, setRequestToken] = useState('')
  const [rowBusy, setRowBusy] = useState<string | null>(null)       // id currently working
  const [rowErr, setRowErr] = useState<string | null>(null)

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

  // ── Connect a new account — api_secret is WRITE-ONLY and cleared immediately ──
  const onCreate = useCallback(async () => {
    setCreateErr(null); setCreateOk(null)
    if (!label.trim() || !apiKey.trim() || !apiSecret.trim()) {
      setCreateErr('Account label, API key and API secret are all required.')
      return
    }
    setCreating(true)
    // Snapshot the secret locally, then CLEAR it from state right away so it never
    // lingers in React state / the DOM after this tick.
    const secret = apiSecret
    setApiSecret('')
    try {
      await AutoTradeAPI.createBrokerAccount({
        user_id: userId,
        broker,
        account_label: label.trim(),
        api_key: apiKey.trim(),
        api_secret: secret,
      })
      setCreateOk(`Connected ${brokerLabel(broker)} · ${label.trim()}. Now use "Connect" to log in and activate.`)
      setLabel(''); setApiKey('')  // secret already cleared
      await load()
    } catch (e) {
      setCreateErr(e instanceof Error ? e.message : 'Could not connect the broker account.')
    } finally {
      setCreating(false)
    }
  }, [userId, broker, label, apiKey, apiSecret, load])

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

  const onDelete = useCallback(async (acct: BrokerAccount) => {
    if (!window.confirm(
      `Remove the broker account "${acct.account_label}" (${brokerLabel(acct.broker)})? ` +
      `This deletes its stored key/secret/token. This cannot be undone.`,
    )) return
    setRowErr(null); setRowBusy(acct.broker_account_id)
    try {
      await AutoTradeAPI.deleteBrokerAccount(acct.broker_account_id, userId)
      if (connectId === acct.broker_account_id) { setConnectId(null); setRequestToken('') }
      await load()
    } catch (e) {
      setRowErr(e instanceof Error ? e.message : 'Could not remove the account.')
    } finally {
      setRowBusy(null)
    }
  }, [userId, connectId, load])

  const selectedBrokerVerified = BROKERS.find((b) => b.id === broker)?.verified ?? false

  return (
    <div className="flex flex-col gap-4">
      {/* ── Header ───────────────────────────────────────────────────────────── */}
      <div className="flex items-center gap-2 flex-wrap">
        <span style={{ color: C.mint }}>{ICON.link(16)}</span>
        <span className="text-[14px] font-semibold" style={{ color: C.ink }}>Broker accounts</span>
        <span className="text-[11px]" style={{ color: C.faint }}>
          Connect a broker, then activate it daily with a login token.
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
            <b style={{ color: C.amber }}>Broker-account vault isn&apos;t enabled yet.</b>{' '}
            Connecting a broker account needs the server vault key so credentials can
            be stored encrypted. The operator must set the vault key on the backend —
            once that&apos;s done, this panel will let you connect and activate accounts here.
            <div className="mt-2 text-[11px]" style={{ color: C.faint }}>
              Nothing to do from the UI until then. AutoTrade sessions keep running on the
              global operator account as today.
            </div>
          </div>
        </div>
      ) : (
        <>
          {/* ── Connect form ─────────────────────────────────────────────────── */}
          <div className="rounded-2xl border p-4 sm:p-5" style={{ borderColor: C.line2, background: C.card }}>
            <div className="flex items-center gap-2 mb-3">
              <span style={{ color: C.mint }}>{ICON.bolt(15)}</span>
              <span className="text-[13px] font-semibold" style={{ color: C.ink }}>Connect a broker account</span>
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <Field label="Broker" hint={selectedBrokerVerified
                ? 'Zerodha (Kite) is the verified live integration.'
                : 'Beta integration — not yet production-verified.'}>
                <div className="flex items-center gap-2">
                  <select
                    value={broker}
                    onChange={(e) => setBroker(e.target.value as BrokerName)}
                    className="w-full rounded-lg px-3 py-2 text-[13px] outline-none"
                    style={inputStyle}
                  >
                    {BROKERS.map((b) => (
                      <option key={b.id} value={b.id} style={{ background: '#0b1410', color: C.ink }}>
                        {b.label}{b.verified ? '' : ' (beta)'}
                      </option>
                    ))}
                  </select>
                  {selectedBrokerVerified ? (
                    <span className="shrink-0 inline-flex items-center gap-1 text-[10px] font-semibold rounded-full px-2 py-1"
                      style={{ color: C.mint, background: 'rgba(63,227,164,0.12)', boxShadow: 'inset 0 0 0 1px rgba(63,227,164,0.4)' }}>
                      {ICON.check(11)} verified
                    </span>
                  ) : (
                    <span className="shrink-0 inline-flex items-center text-[10px] font-semibold rounded-full px-2 py-1"
                      style={{ color: C.amber, background: 'rgba(230,180,80,0.12)', boxShadow: 'inset 0 0 0 1px rgba(230,180,80,0.4)' }}>
                      beta — unverified
                    </span>
                  )}
                </div>
              </Field>

              <Field label="Account label" hint="A name to recognise this account.">
                <input value={label} onChange={(e) => setLabel(e.target.value)}
                  placeholder="e.g. My Zerodha"
                  className="w-full rounded-lg px-3 py-2 text-[13px] outline-none" style={inputStyle} />
              </Field>

              <Field label="API key" hint="From your broker's developer console.">
                <input value={apiKey} onChange={(e) => setApiKey(e.target.value)}
                  autoComplete="off"
                  placeholder="api_key"
                  className="w-full rounded-lg px-3 py-2 text-[13px] outline-none font-mono" style={inputStyle} />
              </Field>

              <Field label="API secret" hint="Write-only — sent once, never shown again or read back.">
                <input value={apiSecret} onChange={(e) => setApiSecret(e.target.value)}
                  type="password" autoComplete="new-password"
                  placeholder="api_secret"
                  className="w-full rounded-lg px-3 py-2 text-[13px] outline-none font-mono" style={inputStyle} />
              </Field>
            </div>

            {!selectedBrokerVerified && (
              <div className="mt-3 flex items-start gap-2 rounded-lg border px-3 py-2 text-[11px] leading-snug"
                style={{ borderColor: 'rgba(230,180,80,0.35)', background: 'rgba(230,180,80,0.06)', color: C.ink2 }}>
                <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(13)}</span>
                <span>{brokerLabel(broker)} is a <b>beta, unverified</b> integration — the connect/activate flow may not be fully wired yet.</span>
              </div>
            )}

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

            <div className="mt-4 flex items-center gap-3">
              <button type="button" disabled={creating} onClick={onCreate}
                className="flex items-center gap-2 px-5 py-2.5 rounded-xl text-[13px] font-semibold transition-opacity disabled:opacity-40"
                style={{ color: '#06130c', background: C.mint }}>
                {creating ? 'Connecting…' : <>{ICON.link(14)} Connect account</>}
              </button>
              <span className="text-[11px]" style={{ color: C.faint }}>
                The secret is stored encrypted on the server and never returned to the browser.
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
                  const expired = (a.status ?? '').toUpperCase() === 'EXPIRED'
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
                        </div>
                        <div className="flex items-center gap-2 shrink-0">
                          <button type="button" disabled={busy} onClick={() => onConnect(a)}
                            className="flex items-center gap-1.5 text-[11.5px] font-semibold px-3 py-1.5 rounded-lg transition-opacity disabled:opacity-40"
                            style={{ color: '#06130c', background: C.mint }}>
                            {ICON.link(12)} {busy && !isConnecting ? 'Working…' : (a.has_token ? 'Re-connect' : 'Connect')}
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
                            A broker login opened in a new tab. After you log in, copy the{' '}
                            <code style={{ color: C.ink }}>request_token</code> from the redirect URL and paste it here.
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
