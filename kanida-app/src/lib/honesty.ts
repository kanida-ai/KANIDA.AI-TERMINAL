/**
 * The product's honesty rules, in one place, so no screen can quietly soften one.
 *
 * From CLAUDE.md (non-negotiables) and docs/sessions/PATHFINDER.md:
 *   - Expectancy is the hero metric, never win-rate.
 *   - Every return is shown WITH its drawdown.
 *   - n < 50 flagged, n < 20 greyed. The SERVER derives `sample_flag`; the app
 *     renders it. We never re-derive it from n -- one rule, one owner.
 *   - Losers first. Died experiments are shown, not hidden.
 *   - No return promises, no target prices.
 */
import type { ExperimentStatus, PerformanceBlock, SampleFlag } from '@/api/types';
import type { Palette } from '@/design/tokens';

/** How much confidence the sample earns. Copy shown next to n. */
export const sampleCopy: Record<SampleFlag, { short: string; long: string }> = {
  ok: { short: 'n ≥ 50', long: 'Sample large enough to read normally.' },
  flagged: {
    short: 'small sample',
    long: 'Fewer than 50 closed trades. Treat the number as provisional.',
  },
  greyed: {
    short: 'too few to read',
    long: 'Fewer than 20 closed trades. Shown greyed because it is not yet evidence.',
  },
  unknown: { short: 'no sample yet', long: 'Nothing has been tested yet, so there is no number.' },
};

export function sampleTone(flag: SampleFlag, c: Palette): string {
  switch (flag) {
    case 'ok':
      return c.textSecondary;
    case 'flagged':
      return c.caution;
    case 'greyed':
    case 'unknown':
      return c.textGreyed;
  }
}

/** Text colour for a figure whose sample says it should not be read confidently. */
export function figureTone(flag: SampleFlag, value: number | null | undefined, c: Palette): string {
  if (flag === 'greyed' || flag === 'unknown') return c.textGreyed;
  if (value === null || value === undefined || Number.isNaN(value)) return c.textMuted;
  if (value > 0) return c.positive;
  if (value < 0) return c.negative;
  return c.textSecondary;
}

/** Status badge palette. VALIDATING blue - PROMISING green - TESTING violet - DIED red. */
export function statusTone(status: ExperimentStatus, c: Palette): { fg: string; bg: string } {
  switch (status) {
    case 'died':
      return { fg: c.negative, bg: c.negativeSoft };
    case 'promising':
      return { fg: c.positive, bg: c.positiveSoft };
    case 'promoted':
      return { fg: c.investor, bg: c.positiveSoft };
    case 'validating':
      return { fg: c.neutral, bg: c.neutralSoft };
    case 'testing':
      return { fg: c.pathfinder, bg: c.pathfinderSoft };
    case 'queued':
      return { fg: c.textMuted, bg: 'transparent' };
  }
}

/** The word shown on the badge. `died` is published as REJECTED, never hidden. */
export const statusLabel: Record<ExperimentStatus, string> = {
  queued: 'QUEUED',
  testing: 'EXPERIMENTING',
  validating: 'VALIDATING',
  promising: 'PROMISING',
  promoted: 'GRADUATED',
  died: 'REJECTED',
};

/** One plain-English line per status -- the pipeline stage in words. */
export const statusMeaning: Record<ExperimentStatus, string> = {
  queued: 'A question is written. Nothing has been computed yet.',
  testing: 'The deterministic replay is running over history.',
  validating: 'Holding virtual capital forward, sample still building.',
  promising: 'Forward evidence agrees with history so far.',
  promoted: 'Cleared the gate and is now a versioned strategy.',
  died: 'Killed and published, with the reason and what we kept.',
};

/** The edge-discovery pipeline, in the order it flows. */
export const PIPELINE: { status: ExperimentStatus; label: string }[] = [
  { status: 'queued', label: 'New ideas' },
  { status: 'testing', label: 'Backtesting' },
  { status: 'validating', label: 'Virtual trading' },
  { status: 'promising', label: 'Validating' },
  { status: 'promoted', label: 'Graduated' },
];

/**
 * The 2x-slippage gate: an edge that does not survive doubled slippage is not an
 * edge. Shown next to expectancy, never hidden behind a tap.
 */
export function survives2xSlippage(p: PerformanceBlock): boolean {
  return p.expectancy_2x_slippage_pct_per_trade > 0;
}

/** Human label for the two performance bases. */
export function basisLabel(basis: PerformanceBlock['basis']): string {
  return basis === 'historical_replay' ? 'Historical replay' : 'Forward virtual book';
}

/**
 * `improved: null` means "not yet enough forward evidence to say" -- it is NOT
 * a "no". The change-log renders all three states distinctly.
 */
export function improvedCopy(improved: boolean | null | undefined): {
  text: string;
  tone: 'positive' | 'negative' | 'neutral';
} {
  if (improved === true) return { text: 'Performance improved', tone: 'positive' };
  if (improved === false) return { text: 'Performance did not improve', tone: 'negative' };
  return { text: 'Too early to say — not enough forward evidence yet', tone: 'neutral' };
}
