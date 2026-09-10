/**
 * One story, one screen.
 *
 * Every slide follows the same rhythm so the feed reads as one voice:
 *
 *   kicker        what kind of story this is, and where it sits (What matters now · 1 of 3)
 *   subject       who it is about
 *   headline      the engine's headline -- digit-free, readable from across the room
 *   the numbers   two to four key facts, each tappable for its provenance
 *   the lede      the first sentences of the narrative, every figure a fact
 *   the decision  what Pathfinder decided, and the one-line reason
 *   the grading   pending / graded, the date that matters, the record it is published under
 *   provenance    level · n · period · regime · cost hurdle -- the chips that make it trustworthy
 *   deeper        the evidence, one tap or one swipe away
 *
 * Metrics support the story; they never dominate it. Nothing on a slide is a
 * raw token, nothing is a synthesised number, and nothing is an entry, target,
 * stop or execution.
 */
import { useEffect, useState, type ReactNode } from 'react';
import { Animated, Easing, View } from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';

import type { ApiError } from '@/api/client';
import type { ExperimentCard, Fact, RejectedCandidate, ScoreCounts } from '@/api/types';
import { useLayout } from '@/design/responsive';
import { useTheme } from '@/design/theme';
import { motion, radius, space } from '@/design/tokens';
import { count, dateShort, factValue, pct, pctAbs, rangeLabel } from '@/lib/format';
import {
  BEAT_LABEL,
  comparisonCopy,
  experimentStateCopy,
  isGreyed,
  publicSafe,
  researchSafe,
  sampleCopy,
  storyLabelFor,
  toneColor,
} from '@/lib/honesty';
import { keyFacts, ledeOf, type Story } from '@/lib/stories';

import { DecisionPill, EngineFigure, GradingLine, LevelChip, RecordChip, RecordLine, SampleChip, StatePill } from '../chips';
import { FactPress, FactsProvider, RichText } from '../facts';
import { Skeleton } from '../states';
import { Label, Pill, Row, Stack, Touchable, Txt } from '../ui';

const CHROME = 88;

export type SlideContext = {
  index: number;
  active: boolean;
  height: number;
  width: number;
  /** a short viewport: fewer lines, fewer tiles */
  compact: boolean;
  /** a phone-width column: two tiles, tighter type */
  phone: boolean;
  onDeeper: () => void;
  /** the experiments registry request, for the "nothing cleared the gate" story */
  registryError?: ApiError | null;
  registryLoading?: boolean;
  onOpenStory?: (findingId: string) => void;
};

export function StorySlide({ story, ctx }: { story: Story; ctx: SlideContext }) {
  switch (story.kind) {
    case 'finding':
      return (
        <FactsProvider facts={story.finding.facts}>
          <FindingSlide story={story} ctx={ctx} />
        </FactsProvider>
      );
    case 'experiment':
      return (
        <FactsProvider facts={story.card.facts}>
          <ExperimentSlide card={story.card} ctx={ctx} />
        </FactsProvider>
      );
    case 'no_experiment':
      return <NoExperimentSlide story={story} ctx={ctx} />;
    case 'scoreboard':
      return <ScoreboardSlide story={story} ctx={ctx} />;
    case 'next':
      return <NextSlide story={story} ctx={ctx} />;
    case 'empty_edition':
      return <EmptyEditionSlide story={story} ctx={ctx} />;
  }
}

// ── the frame every slide shares ────────────────────────────────────────────

function Frame({ ctx, children }: { ctx: SlideContext; children: ReactNode }) {
  const insets = useSafeAreaInsets();
  const layout = useLayout();
  const [anim] = useState(() => new Animated.Value(ctx.active ? 1 : 0));

  useEffect(() => {
    Animated.timing(anim, {
      toValue: ctx.active ? 1 : 0.55,
      duration: motion.slow,
      easing: Easing.out(Easing.cubic),
      useNativeDriver: true,
    }).start();
  }, [anim, ctx.active]);

  const top = (layout.isWide ? 0 : insets.top) + CHROME;
  const bottom = (layout.isWide ? 0 : insets.bottom) + space.lg;

  return (
    <View style={{ height: ctx.height, width: ctx.width, paddingTop: top, paddingBottom: bottom, paddingHorizontal: space.xl }}>
      <Animated.View
        style={{
          flex: 1,
          opacity: anim,
          transform: [{ translateY: anim.interpolate({ inputRange: [0, 1], outputRange: [14, 0] }) }],
        }}>
        {children}
      </Animated.View>
    </View>
  );
}

/** The part of a slide that may give way: it shrinks and clips so the footer (decision, chips, deeper) always fits. */
function Body({ children, gap }: { children: ReactNode; gap: number }) {
  return <View style={{ flexShrink: 1, minHeight: 0, overflow: 'hidden', gap }}>{children}</View>;
}

function Kicker({ children, tone }: { children: ReactNode; tone?: string }) {
  const { c } = useTheme();
  return (
    <Txt variant="label" color={tone ?? c.pathfinder}>
      {children}
    </Txt>
  );
}

/** The bottom-of-slide affordance into depth. */
function Deeper({ label, onPress, hint = true }: { label: string; onPress: () => void; hint?: boolean }) {
  const { c } = useTheme();
  return (
    <Row style={{ justifyContent: 'space-between', marginTop: 'auto', paddingTop: space.md, gap: space.sm }}>
      <Touchable
        accessibilityRole="button"
        accessibilityLabel={label}
        onPress={onPress}
        style={{
          paddingHorizontal: space.lg,
          paddingVertical: space.sm + 2,
          borderRadius: radius.pill,
          backgroundColor: c.pathfinderSoft,
          borderWidth: 1,
          borderColor: c.pathfinder,
          flexDirection: 'row',
          alignItems: 'center',
          gap: space.sm,
        }}>
        <Txt variant="smallStrong" color={c.pathfinder}>
          {label}
        </Txt>
        <Txt variant="smallStrong" color={c.pathfinder}>
          →
        </Txt>
      </Touchable>
      {hint ? (
        <Txt variant="caption" tone="muted">
          swipe ↑ next · → deeper
        </Txt>
      ) : null}
    </Row>
  );
}

function regimeShort(regime: string): string {
  const i = regime.indexOf(' (');
  return i === -1 ? regime : regime.slice(0, i);
}

// ── a finding ────────────────────────────────────────────────────────────────

function FindingSlide({ story, ctx }: { story: Extract<Story, { kind: 'finding' }>; ctx: SlideContext }) {
  const { c } = useTheme();
  const f = story.finding;
  const tall = ctx.height >= 780;
  const facts = keyFacts(f, ctx.compact || (ctx.phone && !tall) ? 2 : 4);
  const wmn = f.tier === 'what_matters_now';
  const lines = ctx.compact ? 3 : ctx.phone ? (tall ? 5 : 4) : ctx.height > 860 ? 7 : 5;
  const gap = ctx.compact ? space.sm : space.md;
  const heroStyle = ctx.compact ? { fontSize: 22, lineHeight: 28 } : ctx.phone ? { fontSize: 25, lineHeight: 30 } : undefined;

  return (
    <Frame ctx={ctx}>
      <Stack gap={gap} style={{ flex: 1 }}>
        <Row style={{ justifyContent: 'space-between', gap: space.sm, flexWrap: 'wrap' }}>
          <Kicker tone={wmn ? c.pathfinder : c.textMuted}>
            {wmn ? `What matters now · ${story.tierIndex} of ${story.tierTotal}` : `Discovery ${story.tierIndex}`}
          </Kicker>
          <Txt variant="caption" tone="muted">
            {storyLabelFor(f)}
          </Txt>
        </Row>

        <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
          <Txt variant="subheading" tone="secondary" numberOfLines={1} style={{ flexShrink: 1 }}>
            {f.subject}
          </Txt>
          <Pill fg={c.textMuted} bordered>
            {f.subject_kind}
          </Pill>
          {f.continues ? (
            <Pill fg={c.textMuted} bordered>
              still open
            </Pill>
          ) : null}
        </Row>

        <Touchable accessibilityRole="button" accessibilityLabel="Open the evidence" onPress={ctx.onDeeper}>
          <RichText variant="hero" style={heroStyle} numberOfLines={ctx.compact ? 3 : 4}>
            {f.narrative.headline}
          </RichText>
        </Touchable>

        <Body gap={gap}>
          {facts.length > 0 ? <KeyFacts facts={facts} phone={ctx.phone} /> : null}
          <RichText variant={ctx.compact || ctx.phone ? 'body' : 'lede'} tone="secondary" numberOfLines={lines}>
            {ledeOf(f.narrative.body, ctx.compact ? 2 : 3)}
          </RichText>
        </Body>

        <Stack gap={space.xs}>
          <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
            <DecisionPill decision={f.decision} />
          </Row>
          <Txt variant="small" tone="secondary" numberOfLines={2}>
            {researchSafe(f.decision_reason)}
          </Txt>
        </Stack>

        <GradingLine grading={f.grading} horizonSessions={f.grading_rule.horizon_sessions} editionDate={f.edition_date} />

        <Row gap={space.xs} style={{ flexWrap: 'wrap' }}>
          <LevelChip level={f.provenance.level} />
          <SampleChip n={f.provenance.n} flag={f.provenance.sample_flag} />
          <Pill fg={c.textMuted} bordered>
            {rangeLabel(f.provenance.period)}
          </Pill>
          <Pill fg={c.textMuted} bordered>
            {regimeShort(f.provenance.regime)}
          </Pill>
          <Pill fg={c.textMuted} bordered>
            hurdle {pctAbs(f.provenance.cost_hurdle_pct, 2)}
          </Pill>
        </Row>

        <Deeper label="Evidence & provenance" onPress={ctx.onDeeper} hint={!ctx.phone} />
      </Stack>
    </Frame>
  );
}

/** Two-up tiles of the facts the engine flagged as key. Each opens its provenance. */
function KeyFacts({ facts, phone }: { facts: Fact[]; phone: boolean }) {
  const { c } = useTheme();
  return (
    <View style={{ flexDirection: 'row', flexWrap: 'wrap', gap: space.sm }}>
      {facts.map((fact) => {
        const grey = isGreyed(fact.sample_flag);
        return (
          <FactPress key={fact.id} fact={fact} style={{ width: '48%', flexGrow: 1, flexShrink: 1 }}>
            <View
              style={{
                flex: 1,
                padding: phone ? space.sm + 2 : space.md,
                borderRadius: radius.md,
                backgroundColor: c.surface,
                borderWidth: 1,
                borderColor: c.border,
                gap: 2,
              }}>
              <Txt variant="metricSm" numeric color={grey ? c.textGreyed : c.text}>
                {factValue(fact.value, fact.unit)}
              </Txt>
              <Txt variant="caption" tone="muted" numberOfLines={phone ? 1 : 2}>
                {fact.label}
              </Txt>
              {fact.n !== null && fact.n !== undefined ? (
                <Txt variant="caption" color={grey ? c.textGreyed : c.textMuted} numeric>
                  n = {count(fact.n)}
                  {fact.sample_flag === 'ok' ? '' : ` · ${sampleCopy[fact.sample_flag].short}`}
                </Txt>
              ) : null}
            </View>
          </FactPress>
        );
      })}
    </View>
  );
}

// ── an experiment (the seven beats) ─────────────────────────────────────────

function ExperimentSlide({ card, ctx }: { card: ExperimentCard; ctx: SlideContext }) {
  const { c } = useTheme();
  const state = experimentStateCopy[card.state];
  const cmp = comparisonCopy[card.latest_comparison];
  const s = card.score;
  return (
    <Frame ctx={ctx}>
      <Stack gap={ctx.compact ? space.sm : space.md} style={{ flex: 1 }}>
        <Row style={{ justifyContent: 'space-between', gap: space.sm, flexWrap: 'wrap' }}>
          <Kicker>Experiment · {card.family.replace(/_/g, ' ')}</Kicker>
          <StatePill state={card.state} />
        </Row>

        <Txt variant="hero" style={ctx.compact ? { fontSize: 22, lineHeight: 28 } : { fontSize: 24, lineHeight: 30 }} numberOfLines={3}>
          {capitalise(publicSafe(card.theme))}
        </Txt>

        <Body gap={ctx.compact ? 4 : space.sm}>
          {card.story.map((line) => (
            <Row key={line.beat} gap={space.sm} style={{ alignItems: 'flex-start' }}>
              <Txt variant="caption" color={c.pathfinder} style={{ width: 96, paddingTop: 2 }} numberOfLines={2}>
                {BEAT_LABEL[line.beat]}
              </Txt>
              <RichText variant="smallStrong" style={{ flex: 1 }} numberOfLines={ctx.compact ? 1 : 2}>
                {publicSafe(line.headline)}
              </RichText>
            </Row>
          ))}
        </Body>

        <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
          <ScoreInline counts={s} voidCount={s.void} />
          <Pill fg={toneColor(cmp.tone, c)} bordered>
            {cmp.label}
          </Pill>
        </Row>

        <Txt variant="caption" tone="muted" numeric>
          v{card.versions_count} · {count(card.trials_total)} variants counted · {count(card.periods_graded)} periods graded · opened{' '}
          {dateShort(card.opened_edition)} · {state.meaning}
        </Txt>

        <RecordLine label={card.record_label} backfilled={card.backfilled} />

        <Deeper label="Versions, trials, expected vs actual" onPress={ctx.onDeeper} hint={!ctx.phone} />
      </Stack>
    </Frame>
  );
}

function capitalise(s: string): string {
  return s.length === 0 ? s : s[0].toUpperCase() + s.slice(1);
}

/** Right · Wrong · Inconclusive · n, inline, with void apart. */
export function ScoreInline({ counts, voidCount }: { counts: ScoreCounts; voidCount?: number }) {
  const { c } = useTheme();
  return (
    <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
      <Txt variant="smallStrong" color={c.positive} numeric>
        Right {counts.right}
      </Txt>
      <Txt variant="smallStrong" color={c.negative} numeric>
        Wrong {counts.wrong}
      </Txt>
      <Txt variant="smallStrong" color={c.caution} numeric>
        Inconclusive {counts.inconclusive}
      </Txt>
      <Txt variant="small" tone="muted" numeric>
        n = {counts.n}
        {voidCount ? ` · void ${voidCount}` : ''}
        {counts.sample_flag === 'ok' ? '' : ` · ${sampleCopy[counts.sample_flag].short}`}
      </Txt>
    </Row>
  );
}

// ── nothing cleared the gate ────────────────────────────────────────────────

function NoExperimentSlide({ story, ctx }: { story: Extract<Story, { kind: 'no_experiment' }>; ctx: SlideContext }) {
  const { c } = useTheme();
  const reg = story.registry;
  const sb = story.scoreboard;
  const closest = story.closest.slice(0, ctx.compact ? 1 : 2);
  const researched = reg ? reg.not_opened.filter((r) => r.trials_evaluated > 0).length : null;

  return (
    <Frame ctx={ctx}>
      <Stack gap={ctx.compact ? space.sm : space.md} style={{ flex: 1 }}>
        <Row style={{ justifyContent: 'space-between', gap: space.sm, flexWrap: 'wrap' }}>
          <Kicker>Experiments</Kicker>
          <Pill fg={c.textMuted} bordered>
            none open
          </Pill>
        </Row>

        <Txt variant="hero" style={ctx.compact ? { fontSize: 23, lineHeight: 29 } : undefined}>
          {ctx.registryError && !reg
            ? 'The experiment registry could not be reached'
            : !reg && ctx.registryLoading
              ? 'Checking the experiment gate'
              : 'Nothing cleared the gate'}
        </Txt>

        <Body gap={ctx.compact ? space.sm : space.md}>
        {reg && sb ? (
          <Txt variant={ctx.compact || ctx.phone ? 'body' : 'lede'} tone="secondary">
            No hypothesis passed every gate on this edition. Pathfinder evaluated{' '}
            <EngineFigure
              value={count(sb.trials_total)}
              label="variants evaluated, every one a counted trial"
              engine={reg.engine_version}
              asOf={sb.as_of}
              what="The total number of rule variants the experiment loop has evaluated — every one is a counted trial, so the record cannot be quietly p-hacked."
              variant="bodyStrong"
              color={c.text}
            />{' '}
            counted variants across{' '}
            <EngineFigure
              value={researched === null ? '—' : count(researched)}
              label="findings researched at the gate"
              engine={reg.engine_version}
              asOf={sb.as_of}
              what="Findings whose hypothesis family was actually researched against the closed variant set (the rest were declined for a stated reason before any variant ran)."
              variant="bodyStrong"
              color={c.text}
            />{' '}
            findings and declined{' '}
            <EngineFigure
              value={count(sb.candidates_not_opened)}
              label="candidates declined, on the record"
              engine={reg.engine_version}
              asOf={sb.as_of}
              what="S1 findings the worth-testing gate did not open, each with its reason and trial count on the record."
              variant="bodyStrong"
              color={c.text}
            />{' '}
            candidates on the record. Declining is the result, not a gap.
          </Txt>
        ) : ctx.registryError && !reg ? (
          <Stack gap={space.xs}>
            <Txt variant="body" tone="secondary">
              {ctx.registryError.message}
            </Txt>
            <Txt variant="caption" tone="muted" mono numeric>
              {ctx.registryError.url}
            </Txt>
            <Txt variant="caption" tone="muted">
              Nothing is shown in its place — an experiment story you cannot source is not a story.
            </Txt>
          </Stack>
        ) : (
          <Stack gap={space.sm}>
            <Skeleton width="90%" height={14} />
            <Skeleton width="70%" height={14} />
          </Stack>
        )}

        {reg && closest.length > 0 ? (
          <Stack gap={space.sm}>
            <Label>The closest, and why it did not open</Label>
            {closest.map((r) => (
              <ClosestCandidate key={r.finding_id} r={r} engine={reg.engine_version} asOf={reg.as_of} compact={ctx.compact || ctx.phone} />
            ))}
          </Stack>
        ) : null}
        </Body>

        {sb ? <RecordLine label={sb.record_label} backfilled={sb.forward.n === 0} /> : null}

        <Deeper label="Why nothing opened" onPress={ctx.onDeeper} hint={!ctx.phone} />
      </Stack>
    </Frame>
  );
}

export function ClosestCandidate({ r, engine, asOf, compact = false }: { r: RejectedCandidate; engine: string; asOf: string; compact?: boolean }) {
  const { c } = useTheme();
  return (
    <View style={{ padding: space.md, borderRadius: radius.md, backgroundColor: c.surface, borderWidth: 1, borderColor: c.border, gap: 6 }}>
      <Row style={{ justifyContent: 'space-between', gap: space.sm, flexWrap: 'wrap' }}>
        <Txt variant="caption" tone="muted" numeric>
          {r.template_id} · {dateShort(r.edition_date)} · {count(r.trials_evaluated)} trials
        </Txt>
        {r.best_expectancy_net_pct !== null && r.best_expectancy_net_pct !== undefined ? (
          <Row gap={4}>
            <Txt variant="caption" tone="muted">
              expectancy net
            </Txt>
            <EngineFigure
              value={pct(r.best_expectancy_net_pct)}
              label="expectancy per trade, net, of the closest variant"
              engine={engine}
              asOf={asOf}
              what="The closest variant's mean net P&L per trade on the whole sealed history — reported on the decline record; it cleared every gate but the one named below."
              variant="small"
              color={c.textSecondary}
            />
          </Row>
        ) : null}
      </Row>
      <Txt variant="small" numberOfLines={compact ? 2 : 4}>
        {publicSafe(r.best_rule_text ?? '')}
      </Txt>
      <Txt variant="caption" tone="muted">
        A research hypothesis as the engine defines it — not an instruction.
      </Txt>
      {r.best_failed_gates.length > 0 ? (
        <Txt variant="caption" color={c.caution} mono numeric>
          failed: {r.best_failed_gates.join(' · ')}
        </Txt>
      ) : null}
    </View>
  );
}

// ── the scoreboard ──────────────────────────────────────────────────────────

function ScoreboardSlide({ story, ctx }: { story: Extract<Story, { kind: 'scoreboard' }>; ctx: SlideContext }) {
  const { c } = useTheme();
  const sb = story.scoreboard;
  const xs = story.experiments;
  const engine = `scoreboard as of ${sb.as_of}`;
  const big = ctx.compact ? 'metric' : 'metricLg';
  const templates = Object.entries(sb.by_template).slice(0, ctx.compact ? 3 : ctx.phone ? 4 : 6);

  return (
    <Frame ctx={ctx}>
      <Stack gap={ctx.compact ? space.sm : space.md} style={{ flex: 1 }}>
        <Row style={{ justifyContent: 'space-between', gap: space.sm, flexWrap: 'wrap' }}>
          <Kicker>The record</Kicker>
          <RecordChip backfilled={story.backfilled} />
        </Row>

        <Txt variant="hero" style={ctx.compact ? { fontSize: 23, lineHeight: 29 } : undefined}>
          Right, wrong, and inconclusive — graded in public
        </Txt>

        <Row style={{ gap: space.lg, alignItems: 'flex-end' }}>
          <BigCount label="Right" value={sb.right} color={c.positive} variant={big} engine={engine} asOf={sb.as_of} />
          <BigCount label="Wrong" value={sb.wrong} color={c.negative} variant={big} engine={engine} asOf={sb.as_of} />
          <BigCount label="Inconclusive" value={sb.inconclusive} color={c.caution} variant={big} engine={engine} asOf={sb.as_of} />
        </Row>

        <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
          <SampleChip n={sb.n} flag={sb.sample_flag} />
          <Pill fg={c.textMuted} bordered>
            independent grades
          </Pill>
          <Pill fg={c.textMuted} bordered>
            pending {sb.pending}
          </Pill>
          {sb.void > 0 ? (
            <Pill fg={c.textMuted} bordered>
              void {sb.void}
            </Pill>
          ) : null}
        </Row>

        <RecordLine label={story.recordLabel} backfilled={story.backfilled} />

        <View style={{ flexDirection: 'row', gap: space.sm }}>
          <SplitTile title="Forward" counts={sb.forward} note="generated on the session date, before the outcome" />
          <SplitTile title="Backfilled" counts={sb.backfilled} note="generated after the fact — simulated" />
        </View>

        <Body gap={ctx.compact ? space.sm : space.md}>
        {!ctx.compact && templates.length > 0 ? (
          <Stack gap={4}>
            <Label>By question type</Label>
            {templates.map(([k, v]) => (
              <Row key={k} style={{ justifyContent: 'space-between', gap: space.sm }}>
                <Txt variant="caption" tone="secondary" numberOfLines={1} style={{ flexShrink: 1 }}>
                  {k.replace(/_/g, ' ')}
                </Txt>
                <Txt variant="caption" tone="muted" numeric>
                  R {v.right} · W {v.wrong} · I {v.inconclusive} · n {v.n}
                  {v.sample_flag === 'ok' ? '' : ` · ${sampleCopy[v.sample_flag].short}`}
                </Txt>
              </Row>
            ))}
          </Stack>
        ) : null}

        {xs ? (
          <Txt variant="caption" tone="muted" numeric numberOfLines={3}>
            Experiments: Right {xs.right} · Wrong {xs.wrong} · Inconclusive {xs.inconclusive} · n {xs.n} · void {xs.void} · testing{' '}
            {xs.experiments_testing} · buried {xs.experiments_buried} · proposed {xs.experiments_proposed} · not opened{' '}
            {xs.candidates_not_opened} · {xs.trials_total} trials counted
          </Txt>
        ) : null}
        </Body>

        <Deeper label="Full scoreboard" onPress={ctx.onDeeper} hint={!ctx.phone} />
      </Stack>
    </Frame>
  );
}

function BigCount({ label, value, color, variant, engine, asOf }: { label: string; value: number; color: string; variant: 'metric' | 'metricLg'; engine: string; asOf: string }) {
  return (
    <Stack gap={2} style={{ flexGrow: 1, flexBasis: 0 }}>
      <EngineFigure
        value={count(value)}
        label={`${label} — independent grades`}
        engine={engine}
        asOf={asOf}
        what="One grade per claim per non-overlapping horizon, judged under the rule frozen when the card was published. Continuations are folded into the claim they continue; void outcomes are counted apart."
        variant={variant}
        color={color}
      />
      <Txt variant="caption" tone="muted">
        {label}
      </Txt>
    </Stack>
  );
}

function SplitTile({ title, counts, note }: { title: string; counts: ScoreCounts; note: string }) {
  const { c } = useTheme();
  const empty = counts.n === 0;
  return (
    <View style={{ flex: 1, padding: space.md, borderRadius: radius.md, backgroundColor: c.surface, borderWidth: 1, borderColor: c.border, gap: 2 }}>
      <Label>{title}</Label>
      <Txt variant="metricSm" numeric color={empty ? c.textGreyed : c.text}>
        n = {counts.n}
      </Txt>
      <Txt variant="caption" tone="muted" numeric>
        {empty ? 'nothing yet' : `R ${counts.right} · W ${counts.wrong} · I ${counts.inconclusive}`}
      </Txt>
      <Txt variant="caption" tone="muted" numberOfLines={2}>
        {note}
      </Txt>
    </View>
  );
}

// ── what I'm testing next ───────────────────────────────────────────────────

function NextSlide({ story, ctx }: { story: Extract<Story, { kind: 'next' }>; ctx: SlideContext }) {
  const { c } = useTheme();
  const follow = story.followUps.slice(0, ctx.compact ? 2 : ctx.phone ? 3 : 5);
  const pending = story.pending.slice(0, ctx.compact ? 2 : 4);
  return (
    <Frame ctx={ctx}>
      <Stack gap={ctx.compact ? space.sm : space.md} style={{ flex: 1 }}>
        <Kicker>What I{'’'}m testing next</Kicker>
        <Txt variant="hero" style={ctx.compact ? { fontSize: 23, lineHeight: 29 } : undefined}>
          The questions today{'’'}s findings raised
        </Txt>
        <Txt variant="small" tone="secondary">
          Each follow-up comes from a card{'’'}s own template, not from a model{'’'}s imagination. A question
          becomes an experiment only through the worth-testing gate.
        </Txt>

        <Body gap={ctx.compact ? space.sm : space.md}>
        {follow.length > 0 ? (
          <Stack gap={space.sm}>
            {follow.map((q, i) => (
              <Touchable
                key={`${q.findingId}:${i}`}
                accessibilityRole="button"
                accessibilityLabel={`Open ${q.subject}`}
                onPress={() => ctx.onOpenStory?.(q.findingId)}
                style={{ padding: space.md, borderRadius: radius.md, backgroundColor: c.surface, borderWidth: 1, borderColor: c.border, gap: 2 }}>
                <Txt variant="caption" color={c.pathfinder}>
                  {q.subject}
                </Txt>
                <Txt variant="bodyStrong" numberOfLines={2}>
                  {researchSafe(q.question)}
                </Txt>
              </Touchable>
            ))}
            {story.followUps.length > follow.length ? (
              <Txt variant="caption" tone="muted" numeric>
                + {story.followUps.length - follow.length} more on the cards themselves
              </Txt>
            ) : null}
          </Stack>
        ) : null}

        {pending.length > 0 ? (
          <Stack gap={4}>
            <Label>Verdicts still pending</Label>
            {pending.map((p) => (
              <Row key={p.findingId} style={{ justifyContent: 'space-between', gap: space.sm }}>
                <Txt variant="small" tone="secondary" numberOfLines={1} style={{ flexShrink: 1 }}>
                  {p.subject}
                </Txt>
                <Txt variant="caption" tone="muted" numeric>
                  {p.due ? `due ${dateShort(p.due)}` : `due ${p.horizonSessions} session${p.horizonSessions === 1 ? '' : 's'} after ${dateShort(p.editionDate)}`}
                </Txt>
              </Row>
            ))}
          </Stack>
        ) : null}

        </Body>

        <Row style={{ marginTop: 'auto', paddingTop: space.md }}>
          <Txt variant="caption" tone="muted">
            End of this edition · swipe ↓ to go back up
          </Txt>
        </Row>
      </Stack>
    </Frame>
  );
}

// ── an edition that published nothing ───────────────────────────────────────

function EmptyEditionSlide({ story, ctx }: { story: Extract<Story, { kind: 'empty_edition' }>; ctx: SlideContext }) {
  const { c } = useTheme();
  const engine = `edition ${story.editionDate} header · generated ${story.generatedAt}`;
  return (
    <Frame ctx={ctx}>
      <Stack gap={space.md} style={{ flex: 1 }}>
        <Kicker>This edition</Kicker>
        <Txt variant="hero" style={ctx.compact ? { fontSize: 23, lineHeight: 29 } : undefined}>
          Nothing cleared the usefulness threshold on {dateShort(story.editionDate)}
        </Txt>
        <Txt variant="lede" tone="secondary">
          <EngineFigure
            value={count(story.candidatesConsidered)}
            label="candidate questions computed"
            engine={engine}
            asOf={story.dataAsOf}
            what="Every question the library asked of this close, scored for usefulness before anything was written."
            variant="bodyStrong"
            color={c.text}
          />{' '}
          candidate questions were computed across{' '}
          <EngineFigure
            value={count(story.universeScanned)}
            label="stocks scanned"
            engine={engine}
            asOf={story.dataAsOf}
            what="The size of the universe the edition scanned."
            variant="bodyStrong"
            color={c.text}
          />{' '}
          stocks, and none scored above the threshold. That is a valid edition. Pathfinder publishes only
          when the usefulness bar is met — it never pads a quiet day.
        </Txt>
        <Row gap={space.xs} style={{ flexWrap: 'wrap' }}>
          <Pill fg={c.textMuted} bordered>
            {regimeShort(story.regime)}
          </Pill>
          <Pill fg={c.textMuted} bordered>
            data to {dateShort(story.dataAsOf)}
          </Pill>
        </Row>
        <Txt variant="caption" tone="muted">
          The candidates and their scores are on the record; the scoreboard follows.
        </Txt>
      </Stack>
    </Frame>
  );
}
