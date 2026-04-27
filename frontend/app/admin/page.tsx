'use client'
import { useState } from 'react'

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
const KITE_API_KEY = process.env.NEXT_PUBLIC_KITE_API_KEY || ''

export default function AdminPage() {
  const [requestToken, setRequestToken] = useState('')
  const [secret, setSecret]             = useState('')
  const [status, setStatus]             = useState<any>(null)
  const [loading, setLoading]           = useState(false)
  const [tokenStatus, setTokenStatus]   = useState<any>(null)

  async function checkToken() {
    const r = await fetch(`${API}/api/admin/token-status`)
    setTokenStatus(await r.json())
  }

  async function refreshToken() {
    if (!requestToken || !secret) return
    setLoading(true)
    setStatus(null)
    try {
      const r = await fetch(`${API}/api/admin/refresh-token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ request_token: requestToken, secret }),
      })
      setStatus(await r.json())
      setRequestToken('')
    } catch (e: any) {
      setStatus({ status: 'error', message: e.message })
    } finally {
      setLoading(false)
    }
  }

  const loginUrl = KITE_API_KEY
    ? `https://kite.zerodha.com/connect/login?api_key=${KITE_API_KEY}&v=3`
    : 'https://kite.zerodha.com'

  return (
    <div style={{ minHeight: '100vh', background: '#0a0a0f', color: '#e2e8f0', fontFamily: 'monospace', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 24 }}>
      <div style={{ width: '100%', maxWidth: 520 }}>

        <div style={{ marginBottom: 32 }}>
          <div style={{ fontSize: 11, color: '#6366f1', letterSpacing: 3, textTransform: 'uppercase', marginBottom: 6 }}>KANIDA.AI</div>
          <h1 style={{ fontSize: 22, fontWeight: 700, margin: 0 }}>Zerodha Token Refresh</h1>
          <p style={{ color: '#64748b', fontSize: 13, marginTop: 6 }}>Zerodha access tokens expire daily at midnight IST. Use this page to refresh.</p>
        </div>

        {/* Token status */}
        <div style={{ background: '#111827', border: '1px solid #1e293b', borderRadius: 8, padding: 16, marginBottom: 24 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: tokenStatus ? 10 : 0 }}>
            <span style={{ fontSize: 12, color: '#94a3b8' }}>Current token status</span>
            <button onClick={checkToken} style={{ fontSize: 11, background: '#1e293b', border: 'none', color: '#94a3b8', padding: '4px 10px', borderRadius: 4, cursor: 'pointer' }}>
              Check
            </button>
          </div>
          {tokenStatus && (
            <div style={{ fontSize: 13, marginTop: 8, padding: '8px 12px', borderRadius: 6, background: tokenStatus.valid ? '#052e16' : '#2d0a0a', color: tokenStatus.valid ? '#4ade80' : '#f87171' }}>
              {tokenStatus.valid
                ? `✓ Valid — logged in as ${tokenStatus.user} (${tokenStatus.email})`
                : `✗ ${tokenStatus.reason}`}
            </div>
          )}
        </div>

        {/* Step 1 */}
        <div style={{ background: '#111827', border: '1px solid #1e293b', borderRadius: 8, padding: 16, marginBottom: 16 }}>
          <div style={{ fontSize: 11, color: '#6366f1', marginBottom: 8, letterSpacing: 1 }}>STEP 1</div>
          <p style={{ fontSize: 13, color: '#94a3b8', margin: '0 0 12px' }}>Log in to Zerodha. After login, copy the <code style={{ color: '#fbbf24' }}>request_token</code> from the redirect URL.</p>
          <a href={loginUrl} target="_blank" rel="noreferrer"
            style={{ display: 'inline-block', background: '#6366f1', color: '#fff', padding: '8px 18px', borderRadius: 6, fontSize: 13, textDecoration: 'none', fontWeight: 600 }}>
            Open Zerodha Login →
          </a>
          <p style={{ fontSize: 11, color: '#475569', marginTop: 10, marginBottom: 0 }}>
            The redirect URL looks like: <code>...?request_token=<span style={{color:'#fbbf24'}}>AbCdEfGhIj...</span>&action=login&status=success</code>
          </p>
        </div>

        {/* Step 2 */}
        <div style={{ background: '#111827', border: '1px solid #1e293b', borderRadius: 8, padding: 16, marginBottom: 24 }}>
          <div style={{ fontSize: 11, color: '#6366f1', marginBottom: 8, letterSpacing: 1 }}>STEP 2</div>
          <div style={{ marginBottom: 12 }}>
            <label style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Request Token</label>
            <input
              value={requestToken}
              onChange={e => setRequestToken(e.target.value)}
              placeholder="Paste request_token from URL"
              style={{ width: '100%', background: '#0f172a', border: '1px solid #334155', borderRadius: 6, padding: '10px 12px', color: '#e2e8f0', fontSize: 13, boxSizing: 'border-box' }}
            />
          </div>
          <div style={{ marginBottom: 16 }}>
            <label style={{ fontSize: 12, color: '#94a3b8', display: 'block', marginBottom: 4 }}>Admin Secret</label>
            <input
              type="password"
              value={secret}
              onChange={e => setSecret(e.target.value)}
              placeholder="Enter ADMIN_SECRET"
              style={{ width: '100%', background: '#0f172a', border: '1px solid #334155', borderRadius: 6, padding: '10px 12px', color: '#e2e8f0', fontSize: 13, boxSizing: 'border-box' }}
            />
          </div>
          <button
            onClick={refreshToken}
            disabled={loading || !requestToken || !secret}
            style={{ width: '100%', background: loading ? '#334155' : '#6366f1', border: 'none', color: '#fff', padding: '11px', borderRadius: 6, fontSize: 14, fontWeight: 700, cursor: loading ? 'not-allowed' : 'pointer' }}>
            {loading ? 'Refreshing...' : 'Refresh Token'}
          </button>
        </div>

        {status && (
          <div style={{ padding: '12px 16px', borderRadius: 8, background: status.status === 'ok' ? '#052e16' : '#2d0a0a', border: `1px solid ${status.status === 'ok' ? '#166534' : '#7f1d1d'}`, fontSize: 13 }}>
            {status.status === 'ok' ? (
              <>
                <div style={{ color: '#4ade80', fontWeight: 700, marginBottom: 4 }}>✓ Token refreshed successfully</div>
                <div style={{ color: '#86efac' }}>Token: <code>{status.token_preview}</code></div>
                <div style={{ color: '#86efac' }}>{status.railway_updated ? '✓ Railway env var updated automatically' : '⚠ Railway env not updated — set RAILWAY_TOKEN in Railway vars'}</div>
              </>
            ) : (
              <div style={{ color: '#f87171' }}>✗ {status.detail || status.message}</div>
            )}
          </div>
        )}

      </div>
    </div>
  )
}
