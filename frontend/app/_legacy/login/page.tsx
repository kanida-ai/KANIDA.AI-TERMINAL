'use client'
import { useState, Suspense } from 'react'
import { useRouter, useSearchParams } from 'next/navigation'

function LoginForm() {
  const [password, setPassword] = useState('')
  const [error, setError]       = useState('')
  const [loading, setLoading]   = useState(false)
  const router                  = useRouter()
  const params                  = useSearchParams()
  const from                    = params.get('from') || '/analysis'

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setError('')
    const res = await fetch(`/api/auth?from=${encodeURIComponent(from)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ password }),
    })
    if (res.ok) {
      const { redirect } = await res.json()
      router.push(redirect)
    } else {
      setError('Incorrect password')
      setLoading(false)
    }
  }

  return (
    <div style={{ minHeight: '100vh', background: '#0a0a0f', display: 'flex', alignItems: 'center', justifyContent: 'center', fontFamily: 'monospace' }}>
      <div style={{ width: 360 }}>
        <div style={{ textAlign: 'center', marginBottom: 40 }}>
          <div style={{ fontSize: 11, color: '#6366f1', letterSpacing: 4, textTransform: 'uppercase', marginBottom: 8 }}>KANIDA.AI</div>
          <div style={{ fontSize: 22, fontWeight: 700, color: '#e2e8f0' }}>Quant Intelligence Engine</div>
          <div style={{ fontSize: 13, color: '#475569', marginTop: 6 }}>Private access only</div>
        </div>

        <form onSubmit={handleSubmit}>
          <input
            type="password"
            value={password}
            onChange={e => setPassword(e.target.value)}
            placeholder="Enter password"
            autoFocus
            style={{ width: '100%', background: '#111827', border: `1px solid ${error ? '#7f1d1d' : '#1e293b'}`, borderRadius: 8, padding: '12px 16px', color: '#e2e8f0', fontSize: 14, boxSizing: 'border-box', outline: 'none', marginBottom: 12 }}
          />
          {error && <div style={{ color: '#f87171', fontSize: 13, marginBottom: 12 }}>{error}</div>}
          <button
            type="submit"
            disabled={loading || !password}
            style={{ width: '100%', background: loading ? '#334155' : '#6366f1', border: 'none', color: '#fff', padding: 13, borderRadius: 8, fontSize: 14, fontWeight: 700, cursor: loading ? 'not-allowed' : 'pointer' }}>
            {loading ? 'Signing in...' : 'Sign In'}
          </button>
        </form>
      </div>
    </div>
  )
}

export default function LoginPage() {
  return (
    <Suspense>
      <LoginForm />
    </Suspense>
  )
}
