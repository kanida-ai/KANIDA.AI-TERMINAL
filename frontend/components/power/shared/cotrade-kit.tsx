'use client'

/**
 * cotrade-kit — SHARED primitives for the Co-Trading + AutoTrade flows so the two
 * pages feel like one product family (DRY). Extracted from CoTradingExperience so
 * AutoTradeExperience can reuse the EXACT same F2 palette, icon set, formatters,
 * tier-band colouring and the running-machine "mechanism strip" gear visual.
 *
 * HONESTY: nothing here fabricates numbers. The MechanismStrip is 100% static
 * explanatory content; tier colours derive from the BAND name (never
 * signal_tier_color); formatters render "—" for non-finite inputs.
 */
import { Fragment } from 'react'

// ── F2 palette (tokens in globals.css :root — same helper as AskFalconHome) ──
export const C = {
  canvas: 'var(--f2-canvas)', panel: 'var(--f2-panel)', card: 'var(--f2-card)', card2: 'var(--f2-card-2)',
  mint: 'var(--f2-mint)', mintHi: 'var(--f2-mint-hi)', mintDim: 'var(--f2-mint-dim)',
  ink: 'var(--f2-ink)', ink2: 'var(--f2-ink-2)', muted: 'var(--f2-muted)', faint: 'var(--f2-faint)',
  line: 'var(--f2-line)', line2: 'var(--f2-line-2)', red: 'var(--f2-red)', amber: 'var(--f2-amber)',
}

// ── Tier band colouring — derive from the BAND name, NEVER signal_tier_color ─
export const TIER_STYLE: Record<string, { color: string; bg: string; ring: string }> = {
  amber: { color: 'var(--f2-amber)',      bg: 'rgba(230,180,80,0.14)',  ring: 'rgba(230,180,80,0.45)' },
  green: { color: 'var(--f2-tier-green)', bg: 'rgba(63,227,164,0.14)',  ring: 'rgba(63,227,164,0.45)' },
  teal:  { color: 'var(--f2-teal)',       bg: 'rgba(75,203,224,0.14)',  ring: 'rgba(75,203,224,0.45)' },
  red:   { color: 'var(--f2-red)',        bg: 'rgba(232,115,107,0.14)', ring: 'rgba(232,115,107,0.45)' },
  gray:  { color: 'var(--f2-slate)',      bg: 'rgba(133,153,144,0.13)', ring: 'rgba(133,153,144,0.40)' },
}
export const BAND_COLORKEY: Record<string, string> = {
  GOLD: 'amber', PREMIUM: 'teal', ENTERPRISE: 'green', STANDARD: 'gray', AVOID: 'red',
}
export function tierBand(raw: string | null | undefined): string | null {
  if (!raw) return null
  return raw.split('-')[0].toUpperCase()
}

// ════════════════════════════════════════════════════════════════════════════
// MECHANISM STRIP — "How it works" gear-train banner. STATIC explanatory content
//   (no numbers/P&L). Gently-rotating inline-SVG gears read as a running machine:
//   set it ONCE, Kanida.AI then runs continuously. Compact, mint/F2.
//   `slim` renders a 1-line version for a result header.
//   `variant` swaps the copy: 'cotrade' (virtual) vs 'autotrade' (real broker,
//   launch-pending). Animation is disabled under prefers-reduced-motion (scoped
//   <style>; globals.css untouched).
// ════════════════════════════════════════════════════════════════════════════
export type MechanismVariant = 'cotrade' | 'autotrade'

type Step = { you?: boolean; title: string; body: string }
const STEPS: Record<MechanismVariant, Step[]> = {
  cotrade: [
    { you: true, title: 'You',       body: 'Choose your trading style + capital' },
    {            title: 'Kanida.AI', body: 'Picks the daily Top 10 stocks' },
    {            title: 'Kanida.AI', body: 'Decides entry, stop-loss & exit' },
    {            title: 'Kanida.AI', body: 'Reports performance' },
  ],
  autotrade: [
    { you: true, title: 'You',       body: 'Connect your broker + set capital, once' },
    {            title: 'Kanida.AI', body: 'Picks the daily Top 10 stocks' },
    {            title: 'Kanida.AI', body: 'Places & manages the real orders' },
    {            title: 'Kanida.AI', body: 'Trails stops, exits & reports' },
  ],
}
const HEADLINE: Record<MechanismVariant, { title: string; sub: string }> = {
  cotrade: {
    title: 'Set it once. Falcon runs the machine.',
    sub: 'You choose the style and capital — Kanida.AI handles the picks, entry, exits and reporting, every trading day.',
  },
  autotrade: {
    title: 'Falcon trades for you, automatically, with your real broker.',
    sub: 'Connect once — Kanida.AI then places, manages and exits the daily Top 10 on your account, every trading day.',
  },
}
const SLIM_LINE: Record<MechanismVariant, React.ReactNode> = {
  cotrade: (
    <span>Set once · Falcon picks, enters, manages &amp; exits — <b style={{ color: C.mint }}>every trading day, automatically</b>.</span>
  ),
  autotrade: (
    <span>Connect once · Falcon places, manages &amp; exits real orders — <b style={{ color: C.mint }}>automatically</b>.</span>
  ),
}

/** A single gently-rotating inline-SVG cog. `dir` flips spin so meshing reads. */
export function Gear({ size = 22, dir = 1, color = C.mint, dim = false }: { size?: number; dir?: 1 | -1; color?: string; dim?: boolean }) {
  const teeth = Array.from({ length: 8 }, (_, i) => i * 45)
  return (
    <span
      className="ct-gear inline-grid place-items-center shrink-0"
      style={{ width: size, height: size, animationDirection: dir === -1 ? 'reverse' : 'normal', opacity: dim ? 0.85 : 1 }}
    >
      <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke={color} strokeWidth="1.5" aria-hidden>
        {teeth.map(a => (
          <rect key={a} x="10.7" y="0.7" width="2.6" height="4.4" rx="0.8" fill={color} stroke="none"
                transform={`rotate(${a} 12 12)`} />
        ))}
        <circle cx="12" cy="12" r="7" fill="none" />
        <circle cx="12" cy="12" r="2.5" fill={color} stroke="none" />
      </svg>
    </span>
  )
}

export const MECHANISM_CSS = `
@keyframes ct-gear-spin { to { transform: rotate(360deg); } }
.ct-gear { animation: ct-gear-spin 11s linear infinite; transform-origin: 50% 50%; }
@media (prefers-reduced-motion: reduce) { .ct-gear { animation: none; } }
`

/**
 * MechanismStrip — the running-machine banner.
 * - `variant`     : 'cotrade' | 'autotrade' (copy + the bridge line)
 * - `slim`        : 1-line variant for a result header
 * - `onBridge`    : click handler for the secondary bridge button
 * - `launching`   : show the honest "Launching soon for your account" badge
 *                   (used by AutoTrade — the feature isn't live for users yet)
 */
export function MechanismStrip({
  variant = 'cotrade', slim = false, onBridge, launching = false,
}: {
  variant?: MechanismVariant; slim?: boolean; onBridge?: () => void; launching?: boolean
}) {
  if (slim) {
    return (
      <div className="flex items-center gap-2 text-[11px]" style={{ color: C.muted }}>
        <style>{MECHANISM_CSS}</style>
        <Gear size={15} dir={1} />
        <Gear size={12} dir={-1} dim />
        <span>{SLIM_LINE[variant]}</span>
        {launching && (
          <span className="ml-auto shrink-0 text-[8.5px] font-mono uppercase tracking-[0.06em] rounded-full px-2 py-0.5"
                style={{ color: C.amber, background: 'rgba(230,180,80,0.12)', boxShadow: 'inset 0 0 0 1px rgba(230,180,80,0.4)' }}>
            Launching soon
          </span>
        )}
      </div>
    )
  }

  const steps = STEPS[variant]
  const head = HEADLINE[variant]

  return (
    <div className="rounded-2xl border overflow-hidden"
         style={{ borderColor: 'rgba(63,227,164,0.22)', background: 'linear-gradient(180deg, rgba(63,227,164,0.06), rgba(255,255,255,0.015))' }}>
      <style>{MECHANISM_CSS}</style>

      {/* headline row: gear cluster + copy (+ launch badge for AutoTrade) */}
      <div className="flex items-center gap-3 px-4 pt-3 pb-2.5">
        <div className="flex items-center -space-x-1 shrink-0">
          <Gear size={26} dir={1} />
          <Gear size={18} dir={-1} dim />
        </div>
        <div className="min-w-0">
          <div className="text-[13.5px] md:text-[14.5px] font-semibold leading-tight" style={{ color: C.ink }}>
            {head.title}
          </div>
          <div className="text-[11px] md:text-[11.5px] leading-snug mt-0.5" style={{ color: C.muted }}>
            {head.sub}
          </div>
        </div>
        {launching && (
          <span className="ml-auto shrink-0 inline-flex items-center gap-1.5 text-[9px] font-mono uppercase tracking-[0.07em] rounded-full px-2.5 py-1"
                style={{ color: C.amber, background: 'rgba(230,180,80,0.12)', boxShadow: 'inset 0 0 0 1px rgba(230,180,80,0.42)' }}>
            <span className="w-1.5 h-1.5 rounded-full animate-pulse" style={{ background: C.amber }} />
            Launching soon for your account
          </span>
        )}
      </div>

      {/* the 4-step gear-train — fills the strip width; arrows BETWEEN cells.
          The ↻ loop-back chip wraps to its own full-width line below. */}
      <div className="px-3 pb-3">
        <div className="grid grid-cols-2 sm:grid-cols-[1fr_auto_1fr_auto_1fr_auto_1fr] items-stretch gap-1.5">
          {steps.map((s, i) => (
            <Fragment key={i}>
              <div className="flex flex-col rounded-xl border px-2.5 py-2 justify-center min-w-0"
                   style={{
                     borderColor: s.you ? 'rgba(63,227,164,0.5)' : C.line2,
                     background: s.you ? 'rgba(63,227,164,0.08)' : 'rgba(255,255,255,0.02)',
                   }}>
                <div className="flex items-center gap-1.5 mb-1">
                  {s.you
                    ? <span className="grid place-items-center w-4 h-4 rounded-md shrink-0" style={{ background: C.mint, color: '#06130c' }}>{ICON.user(10)}</span>
                    : <Gear size={14} dir={i % 2 === 0 ? 1 : -1} />}
                  <span className="text-[10px] font-semibold uppercase tracking-[0.05em]"
                        style={{ color: s.you ? C.mint : C.ink }}>{s.title}</span>
                  {s.you && (
                    <span className="ml-auto text-[7.5px] font-mono uppercase tracking-[0.06em] shrink-0" style={{ color: C.mint }}>input</span>
                  )}
                </div>
                <div className="text-[10.5px] leading-tight" style={{ color: s.you ? C.ink2 : C.muted }}>{s.body}</div>
              </div>
              {i < steps.length - 1 && (
                <span className="hidden sm:flex items-center justify-center shrink-0 px-0.5" style={{ color: C.faint }}>{ICON.arrow(13)}</span>
              )}
            </Fragment>
          ))}
        </div>

        {/* loop-back: repeats continuously — own full-width line, never clipped */}
        <div className="flex items-center justify-center gap-2 mt-1.5 rounded-full border px-3 py-1.5"
             style={{ borderColor: 'rgba(63,227,164,0.32)', background: 'rgba(63,227,164,0.05)' }}>
          <span className="shrink-0 grid place-items-center w-5 h-5 rounded-full" style={{ color: C.mint }}>
            {ICON.loop(14)}
          </span>
          <span className="text-[10.5px] leading-tight text-center" style={{ color: C.muted }}>
            Repeats every trading day — <b style={{ color: C.mint }}>automatically</b>
          </span>
        </div>
      </div>

      {/* secondary bridge line (compact) */}
      {onBridge && (
        <button type="button" onClick={onBridge}
                className="w-full flex items-center gap-1.5 px-4 py-2 border-t text-[11px] transition-colors text-left"
                style={{ borderColor: 'rgba(63,227,164,0.18)', color: C.muted, background: 'rgba(63,227,164,0.03)' }}>
          <span style={{ color: C.mint }}>{ICON.bot(13)}</span>
          {variant === 'cotrade'
            ? <>AutoTrade does exactly this with your <b style={{ color: C.ink }}>real broker</b></>
            : <>Try it risk-free first with <b style={{ color: C.ink }}>virtual capital</b> in Co-Trading</>}
          <span className="ml-auto shrink-0" style={{ color: C.mint }}>{ICON.arrow(12)}</span>
        </button>
      )}
    </div>
  )
}

// ════════════════════════════════════════════════════════════════════════════
// Formatting helpers (shared)
// ════════════════════════════════════════════════════════════════════════════
export function fmtINR(v: number): string {
  if (!Number.isFinite(v)) return '—'
  return '₹' + Math.round(v).toLocaleString('en-IN')
}
export function signedINR(v: number): string {
  if (!Number.isFinite(v)) return '—'
  return (v >= 0 ? '+' : '−') + '₹' + Math.abs(Math.round(v)).toLocaleString('en-IN')
}
export function fmtNum(v: number): string {
  return v.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}
export function fmtCapital(v: number): string {
  if (v >= 1e7) return `₹${(v / 1e7).toFixed(v % 1e7 === 0 ? 0 : 1)} Cr`
  if (v >= 1e5) return `₹${(v / 1e5).toFixed(v % 1e5 === 0 ? 0 : 1)} L`
  return '₹' + v.toLocaleString('en-IN')
}
export function pctTone(v: number | null | undefined): string {
  if (v == null) return C.faint
  return v >= 0 ? C.mint : C.red
}
export function fmtPct(v: number | null | undefined, dp = 1): string {
  if (v == null || !Number.isFinite(v)) return '—'
  return `${v >= 0 ? '+' : ''}${v.toFixed(dp)}%`
}
export function istTodayISO(): string {
  const now = new Date()
  const istMs = now.getTime() + (now.getTimezoneOffset() + 330) * 60_000
  return new Date(istMs).toISOString().slice(0, 10)
}

// ── Inline icons (shared with AskFalconHome / CoTrading style) ───────────────
export const ICON = {
  flame:   (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M12 3c1 3 4 4.5 4 8a4 4 0 0 1-8 0c0-1.5.6-2.5 1.2-3.3C9.8 9 11.5 8 12 3z" strokeLinejoin="round"/></svg>,
  clock:   (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 2" strokeLinecap="round"/></svg>,
  bolt:    (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M13 3L5 13h6l-1 8 8-10h-6l1-8z" strokeLinejoin="round"/></svg>,
  trend:   (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M4 15l5-5 3 3 7-7" strokeLinecap="round" strokeLinejoin="round"/><path d="M16 6h4v4" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  shield:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M12 3l7 3v5c0 4.5-3 8-7 10-4-2-7-5.5-7-10V6l7-3z" strokeLinejoin="round"/></svg>,
  chevron: (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M6 9l6 6 6-6" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  chevronR:(n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M9 6l6 6-6 6" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  wallet:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><rect x="3" y="6" width="18" height="13" rx="2.5"/><path d="M16 12h3" strokeLinecap="round"/><path d="M3 9h13a2 2 0 0 1 2 2" /></svg>,
  arrow: (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2"><path d="M5 12h14M13 6l6 6-6 6" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  back:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M15 18l-6-6 6-6" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  info:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7"><circle cx="12" cy="12" r="9"/><path d="M12 11v5M12 8h.01" strokeLinecap="round"/></svg>,
  bot:   (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><rect x="4" y="8" width="16" height="11" rx="2.5"/><path d="M12 8V4.5M9 13h.01M15 13h.01" strokeLinecap="round"/><circle cx="12" cy="4" r="1"/></svg>,
  book:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6"><path d="M4 5.5A2.5 2.5 0 0 1 6.5 3H20v15H6.5A2.5 2.5 0 0 0 4 20.5z" strokeLinejoin="round"/><path d="M4 20.5A2.5 2.5 0 0 1 6.5 18H20" /><path d="M8 7.5h7M8 11h7" strokeLinecap="round"/></svg>,
  close: (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9"><path d="M6 6l12 12M18 6L6 18" strokeLinecap="round"/></svg>,
  user:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="8" r="3.4"/><path d="M5.5 19a6.5 6.5 0 0 1 13 0" strokeLinecap="round"/></svg>,
  loop:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"><path d="M4 12a8 8 0 0 1 13.5-5.8L20 8" strokeLinecap="round" strokeLinejoin="round"/><path d="M20 4v4h-4" strokeLinecap="round" strokeLinejoin="round"/><path d="M20 12a8 8 0 0 1-13.5 5.8L4 16" strokeLinecap="round" strokeLinejoin="round"/><path d="M4 20v-4h4" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  link:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7"><path d="M9 15l6-6" strokeLinecap="round"/><path d="M11 7l1-1a3.5 3.5 0 0 1 5 5l-1 1M13 17l-1 1a3.5 3.5 0 0 1-5-5l1-1" strokeLinecap="round" strokeLinejoin="round"/></svg>,
  bell:  (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7"><path d="M6 9a6 6 0 0 1 12 0c0 5 2 6 2 6H4s2-1 2-6z" strokeLinejoin="round"/><path d="M10 19a2 2 0 0 0 4 0" strokeLinecap="round"/></svg>,
  check: (n: number) => <svg width={n} height={n} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2"><path d="M5 12l5 5L20 7" strokeLinecap="round" strokeLinejoin="round"/></svg>,
}
