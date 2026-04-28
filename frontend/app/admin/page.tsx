'use client'
import { useState, useEffect } from 'react'

const API          = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
const KITE_API_KEY = process.env.NEXT_PUBLIC_KITE_API_KEY || ''
const SECRET_KEY   = 'kanida_admin_secret'

export default function AdminPage() {
  const [requestToken, setRequestToken] = useState('')
  const [secret, setSecret]             = useState('')
  const [status, setStatus]             = useState<any>(null)
  const [loading, setLoading]           = useState(false)
  const [tokenStatus, setTokenStatus]   = useState<any>(null)
  const [autoDetected, setAutoDetected] = useState(false)
  const [secretSaved, setSecretSaved]   = useState(false)

  // On mount: check token status, restore saved secret, auto-detect request_token from URL
  useEffect(() => {
    checkToken()
    const saved = localStorage.getItem(SECRET_KEY)
    if (saved) { setSecret(saved); setSecretSaved(true) }

    const params = new URLSearchParams(window.location.search)
    const rt = params.get('request_token')
    const ok = params.get('status')
    if (rt && ok === 'success') {
      setRequestToken(rt)
      setAutoDetected(true)
      // Clean URL without reloading
      window.history.replaceState({}, '', '/admin')
    }
  }, [])

  // Auto-submit when both token and secret are ready after redirect
  useEffect(() => {
    if (autoDetected && requestToken && secret) {
      doRefresh(requestToken, secret)
    }
  }, [autoDetected, requestToken, secret])

  async function checkToken() {
    try {
      const r = await fetch(`${API}/api/admin/token-status`)
      setTokenStatus(await r.json())
    } catch {}
  }

  async function doRefresh(rt: string, sec: string) {
    setLoading(true)
    setStatus(null)
    try {
      const r = await fetch(`${API}/api/admin/refresh-token`, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ request_token: rt, secret: sec }),
      })
      const data = await r.json()
      setStatus(data)
      if (data.status === 'ok') {
        localStorage.setItem(SECRET_KEY, sec)
        setSecretSaved(true)
        setRequestToken('')
        setAutoDetected(false)
        checkToken()
      }
    } catch (e: any) {
      setStatus({ status: 'error', message: e.message })
    } finally {
      setLoading(false)
    }
  }

  const loginUrl = KITE_API_KEY
    ? `https://kite.zerodha.com/connect/login?api_key=${KITE_API_KEY}&v=3`
    : 'https://kite.zerodha.com'

  const tokenValid = tokenStatus?.valid

  return (
    <div style={{ minHeight: '100vh', background: '#0a0a0f', color: '#e2e8f0', fontFamily: 'monospace', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 24 }}>
      <div style={{ width: '100%', maxWidth: 500 }}>

        <div style={{ marginBottom: 28 }}>
          <div style={{ fontSize: 11, color: '#6366f1', letterSpacing: 3, textTransform: 'uppercase', marginBottom: 6 }}>KANIDA.AI</div>
          <h1 style={{ fontSize: 20, fontWeight: 700, margin: 0 }}>Zerodha Auth</h1>
          <p style={{ color: '#64748b', fontSize: 13, marginTop: 4 }}>Token expires daily at midnight IST.</p>
        </div>

        {/* Token status banner */}
        <div style={{ padding: '12px 16px', borderRadius: 8, marginBottom: 24, background: tokenValid ? '#052e16' : '#1a0a0a', border: `1px solid ${tokenValid ? '#166534' : '#3f1515'}`, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ fontSize: 13, color: tokenValid ? '#4ade80' : '#f87171' }}>
            {tokenStatus == null
              ? 'Checking...'
              : tokenValid
                ? `✓ Valid — ${tokenStatus.user}`
                : `✗ Expired — refresh needed`}
          </span>
          <button onClick={checkToken} style={{ fontSize: 11, background: 'transparent', border: '1px solid #334155', color: '#64748b', padding: '3px 10px', borderRadius: 4, cursor: 'pointer' }}>
            Refresh status
          </button>
        </div>

        {/* Loading state when auto-processing */}
        {loading && (
          <div style={{ textAlign: 'center', padding: 32, background: '#111827', borderRadius: 8, marginBottom: 16 }}>
            <div style={{ fontSize: 15, color: '#6366f1', marginBottom: 8 }}>⟳ Authenticating with Zerodha...</div>
            <div style={{ fontSize: 12, color: '#475569' }}>Exchanging request token for access token</div>
          </div>
        )}

        {/* Success result */}
        {status?.status === 'ok' && (
          <div style={{ padding: '16px', borderRadius: 8, background: '#052e16', border: '1px solid #166534', marginBottom: 16 }}>
            <div style={{ color: '#4ade80', fontWeight: 700, fontSize: 15, marginBottom: 6 }}>✓ Authentication complete</div>
            {status.railway_updated
              ? <div style={{ color: '#86efac', fontSize: 13 }}>✓ Railway env updated automatically — token will persist after restarts</div>
              : <>
                  <div style={{ color: '#fbbf24', fontSize: 13, marginBottom: 8 }}>Token active for this session. Copy & save in Railway → Variables → KITE_ACCESS_TOKEN to persist:</div>
                  <div style={{ display: 'flex', gap: 8 }}>
                    <code style={{ background: '#0f172a', padding: '6px 10px', borderRadius: 4, color: '#e2e8f0', fontSize: 11, flex: 1, wordBreak: 'break-all' }}>{status.access_token}</code>
                    <button onClick={() => navigator.clipboard.writeText(status.access_token)}
                      style={{ background: '#1e293b', border: 'none', color: '#94a3b8', padding: '6px 12px', borderRadius: 4, cursor: 'pointer', fontSize: 12, whiteSpace: 'nowrap' }}>
                      Copy
                    </button>
                  </div>
                </>
            }
          </div>
        )}

        {status?.status !== 'ok' && !loading && (
          <>
            {/* One-click login */}
            <div style={{ background: '#111827', border: '1px solid #1e293b', borderRadius: 8, padding: 20, marginBottom: 16 }}>
              <div style={{ fontSize: 13, color: '#94a3b8', marginBottom: 14 }}>
                Click below — Zerodha will redirect back here and authenticate automatically.
              </div>
              <a href={loginUrl}
                style={{ display: 'block', textAlign: 'center', background: '#6366f1', color: '#fff', padding: '12px', borderRadius: 6, fontSize: 14, textDecoration: 'none', fontWeight: 700 }}>
                Login with Zerodha →
              </a>
            </div>

            {/* Secret field (only shown if not saved) */}
            {!secretSaved && (
              <div style={{ background: '#111827', border: '1px solid #1e293b', borderRadius: 8, padding: 16 }}>
                <label style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 6 }}>Admin Secret <span style={{ color: '#475569' }}>(saved after first use)</span></label>
                <input type="password" value={secret} onChange={e => setSecret(e.target.value)}
                  placeholder="Enter ADMIN_SECRET"
                  style={{ width: '100%', background: '#0f172a', border: '1px solid #334155', borderRadius: 6, padding: '10px 12px', color: '#e2e8f0', fontSize: 13, boxSizing: 'border-box' }} />
              </div>
            )}
          </>
        )}

        {status?.status === 'error' && (
          <div style={{ padding: '12px 16px', borderRadius: 8, background: '#2d0a0a', border: '1px solid #7f1d1d', fontSize: 13, color: '#f87171' }}>
            ✗ {status.detail || status.message}
          </div>
        )}

        {/* Try again after success */}
        {status?.status === 'ok' && (
          <button onClick={() => { setStatus(null) }}
            style={{ marginTop: 12, background: 'transparent', border: '1px solid #334155', color: '#64748b', padding: '8px 16px', borderRadius: 6, cursor: 'pointer', fontSize: 12, width: '100%' }}>
            Refresh again
          </button>
        )}

      </div>
    </div>
  )
}
