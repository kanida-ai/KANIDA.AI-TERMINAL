/**
 * KANIDA design tokens — the visual system from docs/FRONTEND_SPEC.md § "Design DNA".
 *
 *   Ground   near-black, slightly cool
 *   Cards    a touch lighter, rounded 16-20, hairline border, soft depth
 *   Accents  mint/green = active | positive | primary CTA
 *            red        = negative | short
 *            amber      = watch | caution
 *   Agents   Trader = blue · Investor = green · Pathfinder = violet
 *            (agent identity colour is SEPARATE from semantic colour)
 *
 * One token set, two palettes. Dark is primary; light must stay accessible
 * (FRONTEND_SPEC § States). Every component reads `useTheme()` — no component
 * ever hard-codes a hex.
 */
import { Platform } from 'react-native';

export type Palette = {
  /** page ground */
  bg: string;
  /** ground for a sunken well (inputs, inset rows) */
  bgSunken: string;
  /** card surface */
  surface: string;
  /** card surface, one step up (nested card, active sub-tab) */
  surfaceRaised: string;
  /** hairline card border */
  border: string;
  /** stronger divider */
  borderStrong: string;

  text: string;
  textSecondary: string;
  textMuted: string;
  /** text for data the sample size says not to trust yet (n < 20) */
  textGreyed: string;

  positive: string;
  positiveSoft: string;
  negative: string;
  negativeSoft: string;
  caution: string;
  cautionSoft: string;
  neutral: string;
  neutralSoft: string;

  /** agent identity colours */
  trader: string;
  investor: string;
  pathfinder: string;
  pathfinderSoft: string;
  pathfinderDeep: string;

  /** focus ring — accessibility, both themes */
  focus: string;
  /** scrim behind sheets */
  scrim: string;
};

const dark: Palette = {
  bg: '#08090C',
  bgSunken: '#050609',
  surface: '#101218',
  surfaceRaised: '#181B23',
  border: 'rgba(255,255,255,0.08)',
  borderStrong: 'rgba(255,255,255,0.16)',

  text: '#F2F4F8',
  textSecondary: '#9BA3B2',
  textMuted: '#6B7383',
  textGreyed: '#5A6070',

  positive: '#3FE0A5',
  positiveSoft: 'rgba(63,224,165,0.14)',
  negative: '#FF6B7A',
  negativeSoft: 'rgba(255,107,122,0.14)',
  caution: '#F5B54B',
  cautionSoft: 'rgba(245,181,75,0.14)',
  neutral: '#7DA2FF',
  neutralSoft: 'rgba(125,162,255,0.14)',

  trader: '#4C9AFF',
  investor: '#3FE0A5',
  pathfinder: '#A78BFA',
  pathfinderSoft: 'rgba(167,139,250,0.14)',
  pathfinderDeep: '#6D4EE0',

  focus: '#A78BFA',
  scrim: 'rgba(0,0,0,0.72)',
};

const light: Palette = {
  bg: '#F7F8FA',
  bgSunken: '#EEF0F4',
  surface: '#FFFFFF',
  surfaceRaised: '#F4F5F8',
  border: 'rgba(10,12,20,0.10)',
  borderStrong: 'rgba(10,12,20,0.18)',

  text: '#0C0E14',
  textSecondary: '#4B5364',
  textMuted: '#6B7383',
  textGreyed: '#9AA1AF',

  positive: '#0E8F63',
  positiveSoft: 'rgba(14,143,99,0.12)',
  negative: '#C6303F',
  negativeSoft: 'rgba(198,48,63,0.12)',
  caution: '#9A6608',
  cautionSoft: 'rgba(154,102,8,0.14)',
  neutral: '#2C5BD6',
  neutralSoft: 'rgba(44,91,214,0.12)',

  trader: '#2C5BD6',
  investor: '#0E8F63',
  pathfinder: '#5B3FD1',
  pathfinderSoft: 'rgba(91,63,209,0.12)',
  pathfinderDeep: '#4227B0',

  focus: '#5B3FD1',
  scrim: 'rgba(12,14,20,0.45)',
};

export const palettes = { dark, light } as const;

/** 4px base rhythm — calm spacing, generous padding. */
export const space = {
  xs: 4,
  sm: 8,
  md: 12,
  lg: 16,
  xl: 20,
  xxl: 24,
  xxxl: 32,
  huge: 48,
} as const;

export const radius = {
  sm: 8,
  md: 12,
  /** the card radius from the mockups */
  card: 18,
  lg: 22,
  pill: 999,
} as const;

/** Soft depth — the mockups' cards float, they do not glow. */
export const elevation = {
  card: Platform.select({
    web: { boxShadow: '0 1px 2px rgba(0,0,0,0.24), 0 8px 24px rgba(0,0,0,0.18)' } as object,
    ios: {
      shadowColor: '#000',
      shadowOpacity: 0.28,
      shadowRadius: 18,
      shadowOffset: { width: 0, height: 8 },
    },
    default: { elevation: 4 },
  }) as object,
} as const;

/**
 * Type scale. Geometric-sans feel via the platform system face, and TABULAR
 * NUMERALS on every figure (FRONTEND_SPEC § Type) so columns of numbers line up.
 */
export const fontFamily = Platform.select({
  ios: { sans: 'system-ui', mono: 'ui-monospace' },
  android: { sans: 'sans-serif', mono: 'monospace' },
  default: {
    sans: '-apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, sans-serif',
    mono: 'ui-monospace, SFMono-Regular, Menlo, monospace',
  },
}) as { sans: string; mono: string };

/** Applied to every numeric string in the app. */
export const tabularNums = { fontVariant: ['tabular-nums' as const] };

export const type = {
  display: { fontSize: 30, lineHeight: 36, fontWeight: '700' as const, letterSpacing: -0.6 },
  title: { fontSize: 22, lineHeight: 28, fontWeight: '700' as const, letterSpacing: -0.3 },
  heading: { fontSize: 18, lineHeight: 24, fontWeight: '650' as const, letterSpacing: -0.2 },
  subheading: { fontSize: 16, lineHeight: 22, fontWeight: '600' as const },
  body: { fontSize: 15, lineHeight: 23, fontWeight: '400' as const },
  bodyStrong: { fontSize: 15, lineHeight: 23, fontWeight: '600' as const },
  small: { fontSize: 13, lineHeight: 19, fontWeight: '400' as const },
  smallStrong: { fontSize: 13, lineHeight: 19, fontWeight: '600' as const },
  caption: { fontSize: 11.5, lineHeight: 16, fontWeight: '500' as const },
  /** small UPPERCASE labels with letter-spacing */
  label: {
    fontSize: 10.5,
    lineHeight: 14,
    fontWeight: '700' as const,
    letterSpacing: 1.1,
    textTransform: 'uppercase' as const,
  },
  /** the hero number */
  metric: { fontSize: 26, lineHeight: 30, fontWeight: '700' as const, letterSpacing: -0.5 },
  metricSm: { fontSize: 18, lineHeight: 22, fontWeight: '700' as const, letterSpacing: -0.3 },
} as const;

export const motion = {
  fast: 140,
  base: 220,
  slow: 360,
} as const;
