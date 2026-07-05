'use client'

/**
 * /power/autotrade/connect — broker OAuth auto-capture landing page.
 *
 * This is the target of the broker's Redirect / Callback URL
 * (https://www.kanida.ai/power/autotrade/connect). After the user signs in at
 * their broker, the broker redirects HERE with the token in the query string.
 * We auto-activate the pending broker connection so the user no longer copies /
 * pastes the request_token by hand.
 *
 * FLOW (happy path):
 *   1. Read the token from the URL (request_token | auth | code | token).
 *   2. Read the pending connection from localStorage 'kanida.brokerConnect'
 *      ({ broker_account_id, user_id, broker }) written by BrokerAccountsPanel
 *      right before it opened this popup.
 *   3. POST refreshBrokerToken → on success postMessage the opener and close.
 *
 * FALLBACK (auto-activate fails, or opened directly / missing pending id):
 *   Show the captured token in a read-only Copy box + guidance to paste it back
 *   on the AutoTrade broker card. The panel's manual paste box stays as the net.
 *
 * This page is DELIBERATELY placed OUTSIDE the app/power/(app)/* route group so
 * it does NOT inherit the AppShell left-rail or the hard login redirect — an
 * OAuth popup landing must be a calm, self-contained page that runs, reports to
 * its opener, and closes. The parent app/power/layout.tsx already passes
 * /power/autotrade/* through without the public TopBar/Footer.
 *
 * SECURITY: we postMessage to the EXPLICIT window.location.origin (never '*'),
 * and we never render or log any secret beyond the one-time token needed to
 * finish the connection.
 */

import { useCallback, useEffect, useRef, useState } from 'react'
import { AutoTradeAPI } from '@/lib/autotrade-api'
import { C, ICON } from '@/components/power/shared/cotrade-kit'

const LS_KEY = 'kanida.brokerConnect'

type Pending = { broker_account_id?: string; user_id?: number | string; broker?: string }

// Grab whichever token param the broker used, in priority order.
function readToken(sp: URLSearchParams): string {
  for (const k of ['request_token', 'auth', 'code', 'token']) {
    const v = sp.get(k)
    if (v && v.trim()) return v.trim()
  }
  return ''
}

function readPending(): Pending {
  try {
    const raw = localStorage.getItem(LS_KEY)
    if (!raw) return {}
    const p = JSON.parse(raw) as Pending
    return p && typeof p === 'object' ? p : {}
  } catch {
    return {}
  }
}

function clearPending() {
  try { localStorage.removeItem(LS_KEY) } catch { /* ignore */ }
}

// Post the result back to the opener ONLY, with the explicit target origin.
function notifyOpener(payload: Record<string, unknown>) {
  try {
    if (typeof window !== 'undefined' && window.opener) {
      window.opener.postMessage(
        { type: 'kanida-broker-connected', ...payload },
        window.location.origin,
      )
    }
  } catch { /* opener gone / cross-origin — the fallback UI still recovers it */ }
}

type Stage =
  | { name: 'working' }
  | { name: 'success'; hasOpener: boolean }
  | { name: 'error'; message: string; token: string }
  | { name: 'missing'; reason: string; token: string }

export default function BrokerConnectPage() {
  const [stage, setStage] = useState<Stage>({ name: 'working' })
  const ran = useRef(false)

  useEffect(() => {
    if (ran.current) return   // StrictMode double-invoke guard
    ran.current = true

    const sp = new URLSearchParams(
      typeof window !== 'undefined' ? window.location.search : '',
    )
    const token = readToken(sp)
    const status = sp.get('status') // broker-reported status, if any
    const pending = readPending()
    const id = pending.broker_account_id
    const userId = pending.user_id

    // Broker explicitly reported a non-success status (e.g. Zerodha ?status=error).
    const brokerRejected = typeof status === 'string' && status && status.toLowerCase() !== 'success'

    if (!token || !id) {
      const reason = !token
        ? 'We didn’t receive a login token from your broker.'
        : 'We couldn’t find a pending broker connection on this device.'
      setStage({ name: 'missing', reason, token })
      notifyOpener({ ok: false, error: reason })
      return
    }

    if (brokerRejected) {
      const message = `Your broker reported: ${status}. Please try connecting again.`
      setStage({ name: 'error', message, token })
      notifyOpener({ ok: false, error: message, broker_account_id: id })
      return
    }

    let cancelled = false
    ;(async () => {
      try {
        await AutoTradeAPI.refreshBrokerToken(id, token, userId)
        if (cancelled) return
        const hasOpener = typeof window !== 'undefined' && !!window.opener
        clearPending()
        notifyOpener({ ok: true, broker_account_id: id })
        setStage({ name: 'success', hasOpener })
        if (hasOpener) {
          setTimeout(() => { try { window.close() } catch { /* ignore */ } }, 1200)
        }
      } catch (e) {
        if (cancelled) return
        const message = e instanceof Error ? e.message
          : 'We couldn’t finish the connection automatically.'
        notifyOpener({ ok: false, error: message, broker_account_id: id })
        setStage({ name: 'error', message, token })
      }
    })()

    return () => { cancelled = true }
  }, [])

  return (
    <div
      className="min-h-screen w-full grid place-items-center px-4 py-10"
      style={{ background: C.canvas, color: C.ink }}
    >
      <div
        className="w-full max-w-md rounded-2xl border p-6 sm:p-7 flex flex-col gap-4"
        style={{ borderColor: C.line2, background: C.card }}
      >
        <Brand />
        {stage.name === 'working' && <Working />}
        {stage.name === 'success' && <Success hasOpener={stage.hasOpener} />}
        {stage.name === 'error' && <Failure message={stage.message} token={stage.token} />}
        {stage.name === 'missing' && <Missing reason={stage.reason} token={stage.token} />}
      </div>
    </div>
  )
}

// ── Header ────────────────────────────────────────────────────────────────────
function Brand() {
  return (
    <div className="flex items-center gap-2.5">
      <span style={{ color: C.mint }}>{ICON.link(18)}</span>
      <span className="text-[14px] font-semibold" style={{ color: C.ink }}>
        Connecting your broker
      </span>
    </div>
  )
}

// ── Working spinner ─────────────────────────────────────────────────────────
function Working() {
  return (
    <div className="flex items-center gap-3 py-2">
      <span
        className="inline-block w-4 h-4 rounded-full border-2 animate-spin"
        style={{ borderColor: C.line2, borderTopColor: C.mint }}
      />
      <span className="text-[12.5px]" style={{ color: C.ink2 }}>
        Finishing the connection — this only takes a moment…
      </span>
    </div>
  )
}

// ── Success ─────────────────────────────────────────────────────────────────
function Success({ hasOpener }: { hasOpener: boolean }) {
  return (
    <div className="flex flex-col gap-3">
      <div
        className="flex items-start gap-2.5 rounded-xl border px-3.5 py-3 text-[12.5px] leading-snug"
        style={{ borderColor: 'rgba(63,227,164,0.35)', background: 'rgba(63,227,164,0.06)', color: C.ink2 }}
      >
        <span className="shrink-0 mt-0.5" style={{ color: C.mint }}>{ICON.check(15)}</span>
        <span>
          <b style={{ color: C.mint }}>Connected ✓</b>
          {hasOpener
            ? ' — you can close this window. We’ve taken you back to AutoTrade.'
            : ' — your broker is now linked.'}
        </span>
      </div>
      {!hasOpener && (
        <a
          href="/power/autotrade"
          className="inline-flex items-center justify-center gap-1.5 text-[13px] font-semibold px-4 py-2.5 rounded-xl transition-opacity"
          style={{ color: '#06130c', background: C.mint }}
        >
          {ICON.link(14)} Back to AutoTrade
        </a>
      )}
    </div>
  )
}

// ── Error — auto-activate failed; offer the manual fallback ─────────────────
function Failure({ message, token }: { message: string; token: string }) {
  return (
    <div className="flex flex-col gap-3">
      <div
        className="flex items-start gap-2 rounded-xl border px-3.5 py-3 text-[12px] leading-snug"
        style={{ borderColor: 'rgba(232,115,107,0.35)', background: 'rgba(232,115,107,0.06)', color: C.ink2 }}
      >
        <span className="shrink-0 mt-0.5" style={{ color: C.red }}>{ICON.info(14)}</span>
        <span>{message}</span>
      </div>
      {token ? (
        <TokenFallback token={token} />
      ) : (
        <BackLink />
      )}
    </div>
  )
}

// ── Missing token / pending id — friendly guidance, no crash ────────────────
function Missing({ reason, token }: { reason: string; token: string }) {
  return (
    <div className="flex flex-col gap-3">
      <div
        className="flex items-start gap-2 rounded-xl border px-3.5 py-3 text-[12px] leading-snug"
        style={{ borderColor: C.line2, background: 'rgba(255,255,255,0.02)', color: C.ink2 }}
      >
        <span className="shrink-0 mt-0.5" style={{ color: C.amber }}>{ICON.info(14)}</span>
        <span>
          {reason}{' '}
          Open this from the <b style={{ color: C.ink }}>Add-broker</b> flow in AutoTrade,
          then sign in at your broker to finish.
        </span>
      </div>
      {token ? <TokenFallback token={token} /> : <BackLink />}
    </div>
  )
}

// ── Read-only token box + Copy — the manual recovery path ───────────────────
function TokenFallback({ token }: { token: string }) {
  const [copied, setCopied] = useState(false)
  const onCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(token)
      setCopied(true)
      setTimeout(() => setCopied(false), 1600)
    } catch { /* clipboard blocked — value is shown for manual selection */ }
  }, [token])

  return (
    <div className="flex flex-col gap-2">
      <span className="text-[11px] font-semibold uppercase tracking-[0.05em]" style={{ color: C.muted }}>
        Your request token
      </span>
      <div className="flex items-stretch gap-2">
        <code
          className="flex-1 min-w-0 text-[11.5px] font-mono rounded-lg px-2.5 py-2 truncate"
          style={{ color: C.ink2, background: 'rgba(255,255,255,0.03)', border: `1px solid ${C.line2}` }}
          title={token}
        >
          {token}
        </code>
        <button
          type="button"
          onClick={onCopy}
          className="shrink-0 inline-flex items-center gap-1.5 text-[11.5px] font-semibold px-3 rounded-lg transition-colors"
          style={{ color: C.mint, border: '1px solid rgba(63,227,164,0.3)' }}
          aria-label="Copy request token"
        >
          {copied ? ICON.check(13) : ICON.book(13)} {copied ? 'Copied' : 'Copy'}
        </button>
      </div>
      <p className="text-[11px] leading-snug" style={{ color: C.faint }}>
        Paste this back on the broker card in AutoTrade to finish connecting.
      </p>
      <BackLink />
    </div>
  )
}

function BackLink() {
  return (
    <a
      href="/power/autotrade"
      className="inline-flex items-center justify-center gap-1.5 text-[12.5px] font-semibold px-4 py-2.5 rounded-xl transition-colors"
      style={{ color: C.mint, border: '1px solid rgba(63,227,164,0.3)' }}
    >
      {ICON.link(13)} Back to AutoTrade
    </a>
  )
}
