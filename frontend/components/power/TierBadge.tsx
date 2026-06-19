/**
 * TierBadge — visual treatment for the Pick v1 `tier` enum.
 *
 * Maps the backend's `tier_color` string (locked in PICK_SCHEMA_VERSION=1) to
 * a Tailwind class set. Decoupling the styling here means designers can rework
 * colors without touching every consumer.
 *
 * Two sizes: 'sm' for inline (compact rows), 'md' for cards.
 */
import type { Tier, TierColor } from '@/lib/power-api'

const TIER_COLOR_CLASSES: Record<TierColor, string> = {
  amber:  'bg-mint-500/15  text-mint-300  border-mint-500/40',
  green:  'bg-green-500/15  text-green-300  border-green-500/40',
  yellow: 'bg-yellow-500/15 text-yellow-300 border-yellow-500/40',
  orange: 'bg-orange-500/15 text-orange-300 border-orange-500/40',
  gray:   'bg-neutral-700/40 text-neutral-300 border-neutral-600',
  red:    'bg-red-500/15    text-red-300    border-red-500/40',
}

const SIZE_CLASSES = {
  sm: 'text-[10px] px-1.5 py-0.5 gap-0.5',
  md: 'text-xs px-2 py-1 gap-1',
} as const

type Props = {
  tier:       Tier
  tierColor:  TierColor
  tierIcon?:  string         // emoji from backend (⭐🟢🟡🟠⚪)
  size?:      'sm' | 'md'
  title?:     string         // tooltip text
  className?: string
}

export function TierBadge({ tier, tierColor, tierIcon, size = 'md', title, className = '' }: Props) {
  return (
    <span
      title={title}
      className={[
        'inline-flex items-center font-semibold rounded border font-mono',
        SIZE_CLASSES[size],
        TIER_COLOR_CLASSES[tierColor],
        className,
      ].join(' ')}
    >
      {tierIcon && <span aria-hidden>{tierIcon}</span>}
      <span>{tier}</span>
    </span>
  )
}
