/**
 * Single source of truth for the Falcon AppShell navigation.
 *
 * 6 primary modes (Ask Falcon home → Plans) + footer items (Account / Learn /
 * Admin). Each mode names its route, icon, and an honesty `live` flag the nav
 * uses to render a subtle "Soon" tag on Launch-Pending modes — so the nav never
 * over-promises. Admin is operator-only (gated by `adminOnly`).
 *
 * Routes live under the additive route group app/power/(app)/* — the legacy
 * /power/today, /power/live, /power/replay, etc. are untouched.
 */
import type { ComponentType } from 'react'
import {
  IconSpark, IconSignal, IconHandshake, IconBolt, IconChart, IconLock,
  IconUser, IconBook, IconAdmin,
} from './icons'

export type NavItem = {
  key:        string
  label:      string
  href:       string
  Icon:       ComponentType<{ size?: number }>
  /** false = Launch-Pending (nav shows a subtle "Soon" tag). */
  live:       boolean
  adminOnly?: boolean
}

/** The 6 primary modes, in order. */
export const PRIMARY_NAV: NavItem[] = [
  { key: 'ask',         label: 'Ask Falcon',  href: '/power/ask',         Icon: IconSpark,     live: true  },
  { key: 'signals',     label: 'Signals',     href: '/power/signals',     Icon: IconSignal,    live: true  },
  { key: 'co-trading',  label: 'Co-Trading',  href: '/power/co-trading',  Icon: IconHandshake, live: true  },
  { key: 'autotrade',   label: 'AutoTrade',   href: '/power/autotrade',   Icon: IconBolt,      live: true  },
  { key: 'performance', label: 'Performance', href: '/power/performance', Icon: IconChart,     live: true  },
  { key: 'plans',       label: 'Plans',       href: '/power/plans',       Icon: IconLock,      live: false },
]

/** Footer / account section (not primary modes). */
export const FOOTER_NAV: NavItem[] = [
  { key: 'account', label: 'Account',      href: '/power/account', Icon: IconUser,  live: true },
  { key: 'learn',   label: 'Learn Falcon', href: '/power/learn',   Icon: IconBook,  live: true },
  { key: 'admin',   label: 'Admin',        href: '/power/admin',   Icon: IconAdmin, live: true, adminOnly: true },
]
