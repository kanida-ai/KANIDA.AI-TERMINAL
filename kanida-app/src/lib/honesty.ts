/**
 * The product's honesty rules, in one place, so no screen can quietly soften one.
 *
 * From CLAUDE.md (non-negotiables), docs/sessions/PATHFINDER.md and the S1/S2 hand-backs:
 *   - Every number is a computed fact with provenance. The app renders; it never derives.
 *   - n < 50 flagged, n < 20 greyed. The SERVER derives `sample_flag`; the app renders it.
 *   - A backfill is labelled a backfill — on the edition, on every graded card, on the
 *     scoreboard. The label is the engine's string, rendered verbatim.
 *   - Died / buried / void / Wrong are shown as fully as Right.
 *   - No entry, target, stop or execution on the research surface (spec addendum 6).
 *     The engine enforces it in the schema; the client lints the same words as a belt to
 *     the braces, and withholds a string that would read like an instruction.
 *   - No return promises, no target prices.
 */
import type {
  ComparisonCategory,
  Decision,
  EvidenceLevel,
  ExperimentBeat,
  ExperimentState,
  Finding,
  GradingStatus,
  SampleFlag,
  Verdict,
} from '@/api/types';
import type { Palette } from '@/design/tokens';

// ── samples ──────────────────────────────────────────────────────────────────

/** How much confidence the sample earns. Copy shown next to n. */
export const sampleCopy: Record<SampleFlag, { short: string; long: string }> = {
  ok: { short: 'n ≥ 50', long: 'Sample large enough to read normally.' },
  flagged: {
    short: 'small sample',
    long: 'Fewer than fifty cases. Treat the number as provisional.',
  },
  greyed: {
    short: 'too few to read',
    long: 'Fewer than twenty cases. Shown greyed because it is not yet evidence.',
  },
  unknown: { short: 'no sample', long: 'A count or a label, not a statistic — there is no sample to size.' },
  not_applicable: {
    short: 'observation',
    long: 'A parameter the engine was configured with, or a single observation (today’s move) — not a statistic, so no sample size applies.',
  },
};

export function sampleTone(flag: SampleFlag, c: Palette): string {
  switch (flag) {
    case 'ok':
      return c.textSecondary;
    case 'flagged':
      return c.caution;
    case 'greyed':
      return c.textGreyed;
    case 'unknown':
    case 'not_applicable':
      return c.textMuted;
  }
}

/** True when a figure's sample says it must not be read confidently. */
export function isGreyed(flag: SampleFlag): boolean {
  return flag === 'greyed';
}

/** Text colour for a signed figure: direction from its sign, confidence from its sample. */
export function figureTone(flag: SampleFlag, value: number | null | undefined, c: Palette): string {
  if (flag === 'greyed') return c.textGreyed;
  if (value === null || value === undefined || Number.isNaN(value)) return c.textMuted;
  if (value > 0) return c.positive;
  if (value < 0) return c.negative;
  return c.textSecondary;
}

// ── the research decision vocabulary ─────────────────────────────────────────

export type ToneKey = 'positive' | 'negative' | 'caution' | 'neutral' | 'pathfinder' | 'muted';

export const decisionCopy: Record<Decision, { label: string; meaning: string; tone: ToneKey }> = {
  virtual_long: {
    label: 'Testing long · virtual money',
    meaning: 'History cleared the cost hurdle; a virtual long is being tracked forward. Research, not a recommendation.',
    tone: 'pathfinder',
  },
  virtual_short: {
    label: 'Testing short · virtual money',
    meaning: 'History cleared the cost hurdle; a virtual short is being tracked forward. Research, not a recommendation.',
    tone: 'pathfinder',
  },
  watch: {
    label: 'Watching, not calling',
    meaning: 'Interesting, but the evidence does not clear the bar for a call. Graded on whether NOT calling was right.',
    tone: 'caution',
  },
  no_trade: {
    label: 'No trade',
    meaning: 'History says this does not pay after costs. Graded on whether the avoided trade would have lost.',
    tone: 'neutral',
  },
  new_experiment: {
    label: 'Worth an experiment',
    meaning: 'A real lift over the control. The question moves to the experiment loop with virtual capital.',
    tone: 'positive',
  },
  continue: {
    label: 'Still open',
    meaning: 'The same claim as an earlier card whose horizon is still running. Served, not counted, graded once through the original.',
    tone: 'muted',
  },
  reject: {
    label: 'Rejected · usually noise',
    meaning: 'History rejects the popular reading of this move. Graded on whether the rejection held.',
    tone: 'negative',
  },
};

export function toneColor(tone: ToneKey, c: Palette): string {
  switch (tone) {
    case 'positive':
      return c.positive;
    case 'negative':
      return c.negative;
    case 'caution':
      return c.caution;
    case 'neutral':
      return c.neutral;
    case 'pathfinder':
      return c.pathfinder;
    case 'muted':
      return c.textMuted;
  }
}

export function toneSoft(tone: ToneKey, c: Palette): string {
  switch (tone) {
    case 'positive':
      return c.positiveSoft;
    case 'negative':
      return c.negativeSoft;
    case 'caution':
      return c.cautionSoft;
    case 'neutral':
      return c.neutralSoft;
    case 'pathfinder':
      return c.pathfinderSoft;
    case 'muted':
      return 'transparent';
  }
}

/** A debunk is a card whose decision rejects the popular reading of a striking day. */
export function isDebunk(f: Pick<Finding, 'decision'>): boolean {
  return f.decision === 'no_trade' || f.decision === 'reject';
}

// ── verdicts ─────────────────────────────────────────────────────────────────

export const verdictCopy: Record<Verdict, { label: string; tone: ToneKey }> = {
  right: { label: 'Right', tone: 'positive' },
  wrong: { label: 'Wrong', tone: 'negative' },
  inconclusive: { label: 'Inconclusive', tone: 'caution' },
  void: { label: 'Void', tone: 'muted' },
};

export const gradingStatusCopy: Record<GradingStatus, string> = {
  pending: 'Verdict pending — the horizon has not completed',
  graded: 'Graded under the rule frozen at publication',
  continued: 'Graded once, through the card it continues',
  void: 'Closed without a verdict — the outcome could not be measured',
};

// ── evidence levels ──────────────────────────────────────────────────────────

export const levelCopy: Record<EvidenceLevel, { short: string; long: string }> = {
  same_stock: { short: 'Same stock', long: 'This stock’s (or this pair’s) own history.' },
  peer_group: { short: 'Peers', long: 'Its sector peers.' },
  sector: { short: 'Sector', long: 'The sector as a group.' },
  whole_market: { short: 'Whole market', long: 'Every stock in the universe.' },
};

// ── story types ──────────────────────────────────────────────────────────────

export type StoryKind = 'theme' | 'stock' | 'volume' | 'pair' | 'market' | 'finding';

/** The story type a finding renders as. Derived from the engine's template, never guessed from prose. */
export function storyKindFor(f: Pick<Finding, 'template_id' | 'subject_kind'>): StoryKind {
  switch (f.template_id) {
    case 'theme_cycle':
      return 'theme';
    case 'dip':
    case 'surge':
      return 'stock';
    case 'volume_anomaly':
      return 'volume';
    case 'relationship':
      return 'pair';
    case 'market_regime':
      return 'market';
  }
  if (f.subject_kind === 'sector') return 'theme';
  if (f.subject_kind === 'pair') return 'pair';
  if (f.subject_kind === 'market') return 'market';
  return 'finding';
}

export const storyKindLabel: Record<StoryKind, string> = {
  theme: 'Theme in play',
  stock: 'Stock behaviour',
  volume: 'Volume anomaly',
  pair: 'Relationship',
  market: 'The market',
  finding: 'Finding',
};

/** A debunk overrides the kind label: the story is that the popular reading is wrong. */
export function storyLabelFor(f: Pick<Finding, 'template_id' | 'subject_kind' | 'decision'>): string {
  const kind = storyKindLabel[storyKindFor(f)];
  return isDebunk(f) ? `${kind} · Debunk` : kind;
}

// ── experiments ──────────────────────────────────────────────────────────────

export const experimentStateCopy: Record<ExperimentState, { label: string; meaning: string; tone: ToneKey }> = {
  testing: { label: 'TESTING', meaning: 'A version is open and being tracked forward with virtual money.', tone: 'pathfinder' },
  buried: { label: 'BURIED', meaning: 'Retired. The idea is dead and the post-mortem is public.', tone: 'negative' },
  proposed: {
    label: 'PROPOSED',
    meaning: 'Every gate the engine can measure passed. A human decides; nothing promotes itself.',
    tone: 'positive',
  },
};

export const comparisonCopy: Record<ComparisonCategory, { label: string; tone: ToneKey }> = {
  stronger: { label: 'Right — at least as strong as history', tone: 'positive' },
  weaker: { label: 'Right — but weaker than history', tone: 'caution' },
  inconclusive: { label: 'Inconclusive', tone: 'caution' },
  failed: { label: 'Wrong — the edge failed', tone: 'negative' },
  void: { label: 'Void — nothing closed', tone: 'muted' },
  pending: { label: 'Pending', tone: 'muted' },
};

/** The seven beats, in the agent's own voice — the customer narrative from the spec. */
export const BEAT_LABEL: Record<ExperimentBeat, string> = {
  noticed: 'I noticed',
  researched: 'I tested',
  history_showed: 'History showed',
  decided: 'I decided to test it with virtual money',
  happened: 'What happened',
  learned: 'What I learned',
  next: 'What I’m testing next',
};

export const BEAT_ORDER: ExperimentBeat[] = [
  'noticed',
  'researched',
  'history_showed',
  'decided',
  'happened',
  'learned',
  'next',
];

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

// ── compliance lint (spec addendum 6) ────────────────────────────────────────

/**
 * Mirrors the engine's `PUBLIC_CARD_BANNED_RE`. The server refuses these words on
 * every public experiment text; the client checks the same list on anything it is
 * about to put on the research surface, so a contract slip cannot become a trade
 * instruction on a phone.
 */
export const PUBLIC_BANNED_RE =
  /\b(entry|entries|exit price|target|targets|stop[- ]?loss|stoploss|execute|execution|place an order|buy|buying|sell|selling|take profit|book profit)\b/i;

export function readsLikeInstruction(text: string): boolean {
  return PUBLIC_BANNED_RE.test(text);
}

/**
 * The narrower lint for S1 RESEARCH cards. The S1 engine's own debunks say
 * "history says do not buy the dip" and "buying big surges lost money after
 * costs" — those are findings about what does NOT pay, not instructions, and the
 * S1 contract does not run them through the S2 regex. What must never reach a
 * research card is the shape of an order: an entry, an exit price, a target, a
 * stop, an execution. That is what this list is.
 */
export const ORDER_RE =
  /\b(entry|entries|exit price|target|targets|stop[- ]?loss|stoploss|execute|execution|place an order|take profit|book profit)\b/i;

export function readsLikeOrder(text: string): boolean {
  return ORDER_RE.test(text);
}

export const WITHHELD = '[withheld — this line would read like a trade instruction]';

/** The text, or a visible withholding. Never a silent drop, never the instruction. (experiment surfaces) */
export function publicSafe(text: string): string {
  return readsLikeInstruction(text) ? WITHHELD : text;
}

/** The same, under the research-card lint. (S1 finding surfaces) */
export function researchSafe(text: string): string {
  return readsLikeOrder(text) ? WITHHELD : text;
}

// ── records ──────────────────────────────────────────────────────────────────

export function recordShort(backfilled: boolean): string {
  return backfilled ? 'Simulated backfill' : 'Forward record';
}
