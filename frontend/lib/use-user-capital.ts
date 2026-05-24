'use client'

/**
 * useUserCapital — shared state for "what capital would I run this at?"
 *
 * Persists to localStorage so the choice survives a refresh and follows the
 * user across portfolio pages. Default: ₹5,00,000 (operator-specified).
 *
 * Used by /power/portfolios/[slug] to scale all rupee values displayed on
 * the dashboard. Percent returns are NEVER scaled — they're capital-
 * agnostic by construction.
 */
import { useCallback, useEffect, useState } from 'react'

const STORAGE_KEY  = 'kanida_user_capital_rs'
export const DEFAULT_CAPITAL_RS = 500_000   // ₹5 L per operator spec

export const QUICK_CAPITALS_RS: Array<{ label: string; value: number }> = [
  { label: '₹2 L',  value:   200_000 },
  { label: '₹5 L',  value:   500_000 },
  { label: '₹10 L', value: 1_000_000 },
  { label: '₹25 L', value: 2_500_000 },
  { label: '₹50 L', value: 5_000_000 },
  { label: '₹1 Cr', value: 10_000_000 },
]


export function useUserCapital(): {
  capital:      number
  setCapital:   (n: number) => void
  hydrated:     boolean
} {
  // Start with DEFAULT so SSR + first client render agree. After hydration
  // we replace from localStorage in an effect.
  const [capital, setCapitalState] = useState<number>(DEFAULT_CAPITAL_RS)
  const [hydrated, setHydrated]    = useState<boolean>(false)

  useEffect(() => {
    if (typeof window === 'undefined') return
    try {
      const raw = window.localStorage.getItem(STORAGE_KEY)
      const v   = raw ? parseInt(raw, 10) : 0
      if (Number.isFinite(v) && v >= 10_000) setCapitalState(v)
    } catch { /* private mode etc. */ }
    setHydrated(true)
  }, [])

  const setCapital = useCallback((n: number) => {
    const safe = Math.max(10_000, Math.min(100_000_000, Math.round(n)))
    setCapitalState(safe)
    try { window.localStorage.setItem(STORAGE_KEY, String(safe)) } catch { /* ignore */ }
  }, [])

  return { capital, setCapital, hydrated }
}
