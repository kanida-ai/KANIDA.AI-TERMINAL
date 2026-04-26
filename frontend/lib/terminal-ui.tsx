'use client'

import React, { useEffect, useState } from 'react'
import { T } from '@/lib/theme'

export type Tone = 'neutral' | 'green' | 'red' | 'amber'

export function useBreakpoint() {
  const [width, setWidth] = useState(1280)

  useEffect(() => {
    const update = () => setWidth(window.innerWidth)
    update()
    window.addEventListener('resize', update)
    return () => window.removeEventListener('resize', update)
  }, [])

  return {
    width,
    isMobile: width < 760,
    isTablet: width >= 760 && width < 1100,
    isDesktop: width >= 1100,
    isWide: width >= 1440,
  }
}

export function pageShellStyle(): React.CSSProperties {
  return {
    minHeight: '100vh',
    background:
      'radial-gradient(circle at top left, rgba(0,201,138,0.10), transparent 22%), radial-gradient(circle at 84% 10%, rgba(255,209,102,0.09), transparent 18%), linear-gradient(180deg, #080812, #06060d)',
    color: T.t,
    fontFamily: 'var(--font-geist-sans), system-ui, -apple-system, sans-serif',
  }
}

export function panelStyle(padding = 20): React.CSSProperties {
  return {
    background: 'linear-gradient(180deg, rgba(20,20,40,0.98), rgba(10,10,20,0.99))',
    border: `1px solid ${T.b}`,
    borderRadius: 24,
    padding,
    boxShadow: '0 24px 54px rgba(0,0,0,0.34)',
    boxSizing: 'border-box',
  }
}

export function chipStyle(tone: Tone = 'neutral', small = false): React.CSSProperties {
  const map = {
    neutral: { bg: 'rgba(255,255,255,0.05)', border: T.b, color: T.t2 },
    green: { bg: 'rgba(0,201,138,0.10)', border: 'rgba(0,201,138,0.25)', color: T.g },
    red: { bg: 'rgba(255,77,109,0.10)', border: 'rgba(255,77,109,0.25)', color: T.r },
    amber: { bg: 'rgba(255,209,102,0.10)', border: 'rgba(255,209,102,0.25)', color: T.a },
  }[tone]

  return {
    display: 'inline-flex',
    alignItems: 'center',
    gap: 7,
    padding: small ? '6px 11px' : '9px 14px',
    borderRadius: 999,
    border: `1px solid ${map.border}`,
    background: map.bg,
    color: map.color,
    fontSize: small ? 12 : 13,
    fontWeight: 700,
    letterSpacing: '.02em',
    boxSizing: 'border-box',
    whiteSpace: 'nowrap',
  }
}

export function SectionEyebrow({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        fontSize: 13,
        color: T.g,
        textTransform: 'uppercase',
        letterSpacing: '.12em',
        fontWeight: 800,
        marginBottom: 12,
      }}
    >
      {children}
    </div>
  )
}

export function MetricCard({
  label,
  value,
  note,
  color = T.t,
}: {
  label: string
  value: string
  note: string
  color?: string
}) {
  return (
    <div style={{ background: T.s2, border: `1px solid ${T.b}`, borderRadius: 18, padding: 16 }}>
      <div style={{ fontSize: 12, color: T.t2, textTransform: 'uppercase', letterSpacing: '.07em', marginBottom: 10, fontWeight: 700 }}>
        {label}
      </div>
      <div style={{ fontFamily: T.mono, fontSize: 30, fontWeight: 900, color }}>{value}</div>
      <div style={{ marginTop: 10, color: T.t2, fontSize: 14, lineHeight: 1.65 }}>{note}</div>
    </div>
  )
}

export function AmbientOrb({
  color,
  top,
  right,
  size,
}: {
  color: string
  top: number
  right: number
  size: number
}) {
  return (
    <div
      style={{
        position: 'absolute',
        top,
        right,
        width: size,
        height: size,
        borderRadius: '50%',
        background: color,
        filter: 'blur(48px)',
        opacity: 0.12,
        pointerEvents: 'none',
      }}
    />
  )
}

export function SearchBar({
  value,
  onChange,
  onSubmit,
  placeholder,
  buttonLabel = 'Analyze',
  readOnly = false,
}: {
  value: string
  onChange?: (value: string) => void
  onSubmit?: () => void
  placeholder: string
  buttonLabel?: string
  readOnly?: boolean
}) {
  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: 12,
        background: 'linear-gradient(180deg, rgba(18,18,36,0.98), rgba(10,10,20,0.99))',
        border: `1px solid ${T.b2}`,
        borderRadius: 20,
        padding: '14px 14px 14px 20px',
        boxShadow: '0 12px 34px rgba(0,0,0,0.34)',
      }}
    >
      <div style={{ color: T.t2, fontSize: 17, fontWeight: 700 }}>Search</div>
      <input
        readOnly={readOnly}
        value={value}
        onChange={(e) => onChange?.(e.target.value)}
        onKeyDown={(e) => e.key === 'Enter' && onSubmit?.()}
        placeholder={placeholder}
        style={{
          flex: 1,
          minWidth: 0,
          background: 'transparent',
          border: 'none',
          outline: 'none',
          color: T.t,
          fontSize: 17,
          fontFamily: 'inherit',
        }}
      />
      <button
        onClick={onSubmit}
        style={{
          border: 'none',
          background: T.g,
          color: '#000',
          padding: '13px 24px',
          borderRadius: 12,
          fontSize: 14,
          fontWeight: 800,
          cursor: 'pointer',
        }}
      >
        {buttonLabel}
      </button>
    </div>
  )
}

export function TinyWave({ bullish }: { bullish: boolean }) {
  const points = bullish
    ? ['18,50', '44,36', '68,40', '92,22', '118,26', '142,12']
    : ['18,16', '44,24', '68,18', '92,34', '118,28', '142,42']

  return (
    <svg width="160" height="56" viewBox="0 0 160 56" fill="none" aria-hidden="true">
      <path d={`M${points.join(' L')}`} stroke={bullish ? T.g : T.r} strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
      <path d={`M${points.join(' L')}`} stroke={bullish ? 'rgba(0,201,138,0.22)' : 'rgba(255,77,109,0.22)'} strokeWidth="9" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

export function MiniBars({
  bullish,
  bars,
  height = 120,
}: {
  bullish: boolean
  bars: number[]
  height?: number
}) {
  return (
    <div style={{ display: 'flex', alignItems: 'flex-end', gap: 8, height }}>
      {bars.map((bar, i) => (
        <div
          key={i}
          style={{
            flex: 1,
            height: `${bar}%`,
            borderRadius: '12px 12px 4px 4px',
            background:
              i > Math.floor(bars.length * 0.66)
                ? bullish
                  ? 'linear-gradient(180deg, rgba(0,201,138,0.95), rgba(0,201,138,0.22))'
                  : 'linear-gradient(180deg, rgba(255,77,109,0.95), rgba(255,77,109,0.22))'
                : 'linear-gradient(180deg, rgba(255,255,255,0.26), rgba(255,255,255,0.08))',
          }}
        />
      ))}
    </div>
  )
}

export function GlowDivider() {
  return (
    <div
      style={{
        height: 1,
        width: '100%',
        background: 'linear-gradient(90deg, transparent, rgba(255,255,255,0.12), transparent)',
      }}
    />
  )
}
