/**
 * The experiment RECORD, section by section: the seven beats in full, every
 * version with its frozen expectation and its periods (expected vs actual,
 * the learning), every counted trial, the worth-testing gates, the basket
 * (constituents withheld pending RA review), the proposal, the post-mortem.
 *
 * Nothing here is an instruction. A rule text is the engine's definition of a
 * research hypothesis and is labelled so; the client lints it against the same
 * banned words the server refuses, and withholds rather than prints.
 */
import { View } from 'react-native';

import type {
  BasketView,
  Expectation,
  ExperimentStoryLine,
  GateView,
  PeriodView,
  PostMortem,
  ProposalView,
  TrialView,
  VersionView,
} from '@/api/types';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { EMPTY, count, dateShort, dateTimeIST, humanise, inr, pct, pctAbs, rangeWithSpan } from '@/lib/format';
import { BEAT_LABEL, comparisonCopy, figureTone, gateCopy, publicSafe, sampleCopy, toneColor, toneSoft, verdictCopy } from '@/lib/honesty';

import { LevelPill } from './ChangeLog';
import { RecordLine, SampleChip, VerdictPill } from './chips';
import { FactsProvider, RichText } from './facts';
import { Attribution } from './narrative';
import { Card, Divider, Field, Label, Pill, Row, Stack, Txt } from './ui';

// ── the seven beats, in full ─────────────────────────────────────────────────

export function BeatsFull({ story }: { story: ExperimentStoryLine[] }) {
  const { c } = useTheme();
  return (
    <Stack gap={space.md}>
      {story.map((line, i) => (
        <Row key={line.beat} style={{ alignItems: 'stretch', gap: space.md }}>
          <View style={{ width: 18, alignItems: 'center' }}>
            <View style={{ width: 11, height: 11, borderRadius: radius.pill, backgroundColor: c.pathfinder, marginTop: space.xl }} />
            {i < story.length - 1 ? <View style={{ flex: 1, width: 2, backgroundColor: c.border, marginTop: 4 }} /> : null}
          </View>
          <Card style={{ flex: 1 }}>
            <Stack gap={space.md}>
              <Label tone="inherit" style={{ color: c.pathfinder }}>
                {BEAT_LABEL[line.beat]}
              </Label>
              <RichText variant="heading">{publicSafe(line.headline)}</RichText>
              <RichText variant="body" tone="secondary">
                {publicSafe(line.body)}
              </RichText>
              <Attribution line={line} />
            </Stack>
          </Card>
        </Row>
      ))}
    </Stack>
  );
}

// ── a version ────────────────────────────────────────────────────────────────

export function VersionCard({ v, isCurrent }: { v: VersionView; isCurrent: boolean }) {
  const { c } = useTheme();
  const statusTone = v.status === 'buried' ? c.negative : v.status === 'superseded' ? c.textMuted : c.pathfinder;
  return (
    <Card accent={statusTone}>
      <Stack gap={space.lg}>
        <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
          <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
            <Txt variant="heading" numeric>
              v{v.version}
            </Txt>
            <Pill fg={statusTone} bordered>
              {v.status}
              {isCurrent ? ' · current' : ''}
            </Pill>
            <LevelPill level={v.level} />
          </Row>
          <Txt variant="caption" tone="muted" numeric>
            created {dateShort(v.created_edition)}
          </Txt>
        </Row>

        <Field label="The rule under test (a research definition, not an instruction)">
          <Txt variant="body">{publicSafe(v.rule_text)}</Txt>
        </Field>

        {v.conditions.length > 0 ? (
          <Row gap={space.xs} style={{ flexWrap: 'wrap' }}>
            {v.conditions.map((cond) => (
              <Pill key={cond} fg={c.textSecondary} bordered>
                {publicSafe(cond)}
              </Pill>
            ))}
            <Pill fg={c.textMuted} bordered>
              horizon {v.horizon_sessions} session{v.horizon_sessions === 1 ? '' : 's'}
            </Pill>
          </Row>
        ) : null}

        <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
          <Field label="What changed" style={{ flexGrow: 1, flexBasis: 0, minWidth: 160 }}>
            <Txt variant="small">{publicSafe(v.change)}</Txt>
          </Field>
          <Field label="Why" style={{ flexGrow: 1, flexBasis: 0, minWidth: 160 }}>
            <Txt variant="small" tone="secondary">
              {publicSafe(v.why)}
            </Txt>
          </Field>
        </Row>

        {v.validation ? (
          <Field label="Validation before it could replace anything">
            <Txt variant="small" tone="secondary">
              {v.validation}
            </Txt>
          </Field>
        ) : null}

        <Txt variant="caption" tone="muted" numeric>
          {count(v.trials_for_this_version)} variant{v.trials_for_this_version === 1 ? '' : 's'} evaluated to arrive at this version — every one a
          counted trial.
        </Txt>

        <Divider />
        <ExpectationBlock e={v.expectation} />

        <Divider />
        <Stack gap={space.md}>
          <Label>Forward periods</Label>
          {v.periods.length === 0 ? (
            <Txt variant="small" tone="greyed">
              No period has started yet.
            </Txt>
          ) : (
            v.periods.map((p) => <PeriodCard key={p.period_no} p={p} />)
          )}
        </Stack>
      </Stack>
    </Card>
  );
}

const isNum = (v: number | null | undefined): v is number => v !== null && v !== undefined;

/**
 * The historical expectation, frozen up front. The block IS its provenance.
 *
 * S2 re-audit N1: the FROZEN figure is the book-selected one — measured over the trades the
 * virtual book would actually have taken under its limits, i.e. the strategy that is traded.
 * The equal-weighted figure over every firing is served as context only and is labelled so;
 * it is the S1 card's population, most of which the book cannot take.
 * N3: the concentration facts say how much of the window rode on its best days.
 */
export function ExpectationBlock({ e }: { e: Expectation }) {
  const { c } = useTheme();
  const grey = e.sample_flag === 'greyed';
  const hasContext = isNum(e.equal_weighted_expectancy_net_pct) || isNum(e.signals_fired);
  const hasConcentration = isNum(e.top3_days_share_pct) || isNum(e.expectancy_without_best_day_net_pct) || isNum(e.trailing_expectancy_without_best_day_net_pct);
  return (
    <Stack gap={space.md}>
      <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
        <Label>Historical expectation — frozen {dateShort(e.frozen_at)}, sealed at {dateShort(e.seal)}</Label>
        <SampleChip n={e.n} flag={e.sample_flag} />
      </Row>
      <Row style={{ gap: space.lg, flexWrap: 'wrap', alignItems: 'flex-start' }}>
        <Metric label="Expectancy / trade · book-selected" value={pct(e.expectancy_net_pct)} tone={figureTone(e.sample_flag, e.expectancy_net_pct, c)} size="lg" hint="net of costs, on the trades the book would take — the frozen figure that decides" />
        <Metric label="At 2× slippage" value={pct(e.expectancy_2x_slippage_net_pct)} tone={figureTone(e.sample_flag, e.expectancy_2x_slippage_net_pct, c)} hint={e.expectancy_2x_slippage_net_pct > 0 ? 'survives the gate' : 'does NOT survive the gate'} />
        <Metric label="Median / trade" value={pct(e.median_net_pct)} tone={grey ? c.textGreyed : c.text} />
        <Metric label="Hit rate (supporting)" value={pctAbs(e.hit_rate_pct)} tone={c.textSecondary} />
      </Row>
      {e.population ? (
        <Txt variant="caption" tone="muted">
          Population: {e.population}
        </Txt>
      ) : null}
      {hasContext ? (
        <Row style={{ gap: space.lg, flexWrap: 'wrap', alignItems: 'flex-start' }}>
          <Metric
            label="Equal-weighted, every firing"
            value={isNum(e.equal_weighted_expectancy_net_pct) ? pct(e.equal_weighted_expectancy_net_pct) : EMPTY}
            tone={c.textSecondary}
            size="sm"
            hint={`context: all signals, untakeable by the book${e.equal_weighted_n ? ` · n = ${count(e.equal_weighted_n)}` : ''} — not the expectation`}
          />
          <Metric
            label="Signals fired · skipped"
            value={`${isNum(e.signals_fired) ? count(e.signals_fired) : EMPTY} · ${isNum(e.signals_skipped) ? count(e.signals_skipped) : EMPTY}`}
            tone={c.textSecondary}
            size="sm"
            hint="firings the book's limits could not take are skipped, not imagined"
          />
        </Row>
      ) : null}
      <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
        <Metric label="Signal days" value={count(e.signal_days)} tone={c.textSecondary} size="sm" hint="independent clusters" />
        <Metric label="Trailing window" value={isNum(e.trailing_expectancy_net_pct) ? pct(e.trailing_expectancy_net_pct) : EMPTY} tone={c.textSecondary} size="sm" hint={e.trailing_period ? `n = ${count(e.trailing_n)} · ${rangeWithSpan(e.trailing_period)} · a persistence check, not a holdout` : undefined} />
        <Metric label="Edge vs baseline" value={isNum(e.edge_vs_baseline_pct) ? pct(e.edge_vs_baseline_pct) : EMPTY} tone={c.textSecondary} size="sm" hint={`pool n = ${count(e.baseline_n)} · same kind of day`} />
        <Metric label="Placebo p" value={isNum(e.placebo_p) ? e.placebo_p.toFixed(3) : EMPTY} tone={c.textSecondary} size="sm" hint={`${count(e.placebo_draws)} day-blocked draws${isNum(e.placebo_se) ? ` · se ${e.placebo_se.toFixed(3)}` : ''}${e.placebo_convention ? ` · ${e.placebo_convention}` : ''}`} />
        <Metric label="Cluster t" value={isNum(e.cluster_t) ? e.cluster_t.toFixed(2) : EMPTY} tone={c.textSecondary} size="sm" hint={e.cluster_t_kind ?? undefined} />
        <Metric label="Discovery window · 2×" value={isNum(e.discovery_expectancy_2x_slippage_net_pct) ? pct(e.discovery_expectancy_2x_slippage_net_pct) : EMPTY} tone={c.textSecondary} size="sm" hint={`n = ${count(e.discovery_n)} · advisory`} />
      </Row>
      {hasConcentration ? (
        <Stack gap={space.xs}>
          <Label>Concentration — how much rode on the best days</Label>
          <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
            <Metric label="Top-3 days' share of P&L" value={isNum(e.top3_days_share_pct) ? pctAbs(e.top3_days_share_pct) : EMPTY} tone={c.textSecondary} size="sm" hint="of the whole window's net P&L; above 100% means the rest lost" />
            <Metric label="Without the best day" value={isNum(e.expectancy_without_best_day_net_pct) ? pct(e.expectancy_without_best_day_net_pct) : EMPTY} tone={isNum(e.expectancy_without_best_day_net_pct) ? figureTone(e.sample_flag, e.expectancy_without_best_day_net_pct, c) : c.textSecondary} size="sm" hint="expectancy with the single best signal day removed" />
            <Metric label="Trailing · top-3 share" value={isNum(e.trailing_top3_days_share_pct) ? pctAbs(e.trailing_top3_days_share_pct) : EMPTY} tone={c.textSecondary} size="sm" />
            <Metric label="Trailing · without best day" value={isNum(e.trailing_expectancy_without_best_day_net_pct) ? pct(e.trailing_expectancy_without_best_day_net_pct) : EMPTY} tone={isNum(e.trailing_expectancy_without_best_day_net_pct) ? figureTone(e.sample_flag, e.trailing_expectancy_without_best_day_net_pct, c) : c.textSecondary} size="sm" hint="the persistence check with its best day removed" />
          </Row>
        </Stack>
      ) : null}
      <Stack gap={2}>
        <Label>How this was counted</Label>
        <Txt variant="caption" tone="muted" numeric>
          n = {count(e.n)} trades · {rangeWithSpan(e.period)} · hurdle {pctAbs(e.hurdle_pct, 2)} round trip
        </Txt>
        <Txt variant="caption" tone="muted">
          {e.metric}
        </Txt>
        <Txt variant="caption" tone="muted" mono>
          {e.computed_by}
        </Txt>
      </Stack>
    </Stack>
  );
}

export function Metric({ label, value, tone, hint, size = 'md' }: { label: string; value: string; tone?: string; hint?: string; size?: 'lg' | 'md' | 'sm' }) {
  return (
    <Stack gap={3} style={{ flexGrow: 1, flexBasis: 0, minWidth: 96 }}>
      <Label>{label}</Label>
      <Txt variant={size === 'lg' ? 'metric' : size === 'md' ? 'metricSm' : 'bodyStrong'} numeric color={tone}>
        {value}
      </Txt>
      {hint ? (
        <Txt variant="caption" tone="muted">
          {hint}
        </Txt>
      ) : null}
    </Stack>
  );
}

// ── a period ─────────────────────────────────────────────────────────────────

export function PeriodCard({ p }: { p: PeriodView }) {
  const { c } = useTheme();
  const f = p.forward;
  const eva = p.expected_vs_actual;
  const cmp = eva ? comparisonCopy[eva.category] : null;
  const greyed = f.sample_flag === 'greyed';
  const hasResult = f.mean_net_pct !== null && f.mean_net_pct !== undefined;
  return (
    <FactsProvider facts={p.realized_facts}>
      <Card raised accent={p.verdict ? toneColor(verdictCopy[p.verdict].tone, c) : c.border}>
        <Stack gap={space.md}>
          <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
            <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
              <Txt variant="subheading" numeric>
                Period {p.period_no}
              </Txt>
              {p.verdict ? <VerdictPill verdict={p.verdict} /> : null}
              <Pill fg={c.textMuted} bordered>
                {p.status}
              </Pill>
            </Row>
            <Txt variant="caption" tone="muted" numeric>
              {p.start ? `${dateShort(p.start)} → ${p.end ? dateShort(p.end) : 'open'}` : 'not started'}
              {p.due ? ` · due ${dateShort(p.due)}` : ''}
            </Txt>
          </Row>

          <Txt variant="caption" tone="muted">
            {f.label}
          </Txt>

          <Row style={{ gap: space.lg, flexWrap: 'wrap', alignItems: 'flex-start' }}>
            <Metric label="Mean net / trade" value={hasResult ? pct(f.mean_net_pct) : EMPTY} tone={figureTone(f.sample_flag, f.mean_net_pct, c)} size="lg" hint={hasResult ? `vs expected ${eva ? pct(eva.expected_net_pct) : EMPTY}` : 'nothing closed yet'} />
            <Metric label="Book return" value={f.book_return_pct === null || f.book_return_pct === undefined ? EMPTY : pct(f.book_return_pct)} tone={greyed ? c.textGreyed : figureTone(f.sample_flag, f.book_return_pct, c)} hint={`worst drawdown ${pctAbs(f.max_drawdown_pct)} · now ${pctAbs(f.current_drawdown_pct)}`} />
            <Metric label="Closed · open" value={`${count(f.n_closed)} · ${count(f.n_open)}`} tone={c.textSecondary} size="sm" hint={`${count(f.signals_taken)} of ${count(f.signals_seen)} signals taken · ${count(f.signal_days)} signal day${f.signal_days === 1 ? '' : 's'}${f.n_unresolved ? ` · ${count(f.n_unresolved)} unresolved (not graded)` : ''}`} />
            <Metric label="Hit rate (supporting)" value={f.hit_rate_pct === null || f.hit_rate_pct === undefined ? EMPTY : pctAbs(f.hit_rate_pct)} tone={c.textSecondary} size="sm" />
          </Row>

          <Txt variant="caption" tone="muted" numeric>
            {inr(f.capital_inr)} virtual capital · {sampleCopy[f.sample_flag].short} · marked to {dateShort(f.as_of)}
            {f.drawdown_convention ? ` · drawdown: ${f.drawdown_convention}` : ''}
          </Txt>

          {eva && cmp ? (
            <View style={{ backgroundColor: toneSoft(cmp.tone, c), borderRadius: radius.md, padding: space.md, gap: space.xs }}>
              <Label tone="inherit" style={{ color: toneColor(cmp.tone, c) }}>
                Expected vs actual · {cmp.label}
              </Label>
              <RichText variant="body">{eva.statement}</RichText>
              {eva.gap_pct !== null && eva.gap_pct !== undefined ? (
                <Txt variant="caption" tone="muted" numeric>
                  gap {pct(eva.gap_pct)}
                </Txt>
              ) : null}
            </View>
          ) : null}

          {p.learning ? (
            <Stack gap={space.xs}>
              <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                <Label>What I learned</Label>
                <LevelPill level={p.learning.level} />
                <Pill fg={p.learning.buried ? c.negative : c.textMuted} bordered>
                  next: {p.learning.next_action}
                </Pill>
              </Row>
              <RichText variant="body" tone="secondary">
                {publicSafe(p.learning.statement)}
              </RichText>
              <Txt variant="caption" tone="muted" numeric>
                {count(p.learning.trials_evaluated)} candidate revision{p.learning.trials_evaluated === 1 ? '' : 's'} evaluated · {count(p.learning.trials_passing)} cleared
                {p.learning.adopted_rule ? ' · one adopted' : ''}
              </Txt>
              {p.learning.adopted_rule ? (
                <Txt variant="small">{publicSafe(p.learning.adopted_rule)}</Txt>
              ) : null}
            </Stack>
          ) : null}

          <Stack gap={4}>
            <Txt variant="caption" tone="muted">
              {p.status === 'graded' || p.status === 'void'
                ? `Judged under ${p.grading_rule.rule_version}${p.grader_version ? ` by ${p.grader_version}` : ''}${p.graded_at ? ` on ${dateTimeIST(p.graded_at)}` : ''}${p.data_as_of ? ` · data to ${dateShort(p.data_as_of)}` : ''}`
                : `Will be judged under ${p.grading_rule.rule_version}: ${p.grading_rule.right}`}
            </Txt>
            <RecordLine label={p.record} backfilled={p.backfilled} />
          </Stack>
        </Stack>
      </Card>
    </FactsProvider>
  );
}

// ── trials ───────────────────────────────────────────────────────────────────

/**
 * The p-hacking ledger. `familyTrialsAllTime` (S2 re-audit N4) is the FAMILY's count across
 * every finding, retry and revision — the number the family-wise significance bar divides by.
 * It never restarts, so it is shown beside this experiment's own count, never in its place.
 */
export function TrialsLedger({ trials, familyTrialsAllTime }: { trials: TrialView[]; familyTrialsAllTime?: number | null }) {
  const { c } = useTheme();
  return (
    <Card>
      <Stack gap={space.md}>
        <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
          <Label>Every counted trial</Label>
          <Txt variant="caption" tone="muted" numeric>
            {count(trials.length)} on the record · the p-hacking ledger
          </Txt>
        </Row>
        {familyTrialsAllTime !== null && familyTrialsAllTime !== undefined ? (
          <Txt variant="caption" tone="muted" numeric>
            Family trials, all time: {count(familyTrialsAllTime)} — every trial this hypothesis family has ever had, across every finding, retry and
            revision. The family-wise significance bar divides by this count; it never restarts.
          </Txt>
        ) : null}
        {trials.map((t) => (
          <View key={t.trial_no} style={{ gap: 4, paddingVertical: space.sm, borderTopWidth: 1, borderTopColor: c.border }}>
            <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
              <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                <Txt variant="smallStrong" numeric>
                  #{t.trial_no}
                </Txt>
                <Pill fg={t.adopted ? c.positive : t.passed ? c.neutral : c.textMuted} bordered>
                  {t.adopted ? 'adopted' : t.passed ? 'cleared' : 'failed'}
                </Pill>
                <Txt variant="caption" tone="muted">
                  {t.context}
                </Txt>
              </Row>
              <Txt variant="caption" tone="muted" numeric>
                expectancy {t.expectancy_net_pct === null || t.expectancy_net_pct === undefined ? EMPTY : pct(t.expectancy_net_pct)} · trailing{' '}
                {t.trailing_expectancy_net_pct === null || t.trailing_expectancy_net_pct === undefined ? EMPTY : pct(t.trailing_expectancy_net_pct)} · n = {count(t.n)}
              </Txt>
            </Row>
            <Txt variant="small" tone="secondary">
              {publicSafe(t.rule_text)}
            </Txt>
            <Txt variant="caption" color={t.passed ? c.textMuted : c.caution}>
              {t.reason}
            </Txt>
          </View>
        ))}
      </Stack>
    </Card>
  );
}

// ── gates ────────────────────────────────────────────────────────────────────

export function GatesList({ gates, title }: { gates: GateView[]; title: string }) {
  const { c } = useTheme();
  return (
    <Card>
      <Stack gap={space.sm}>
        <Label>{title}</Label>
        {gates.map((g) => {
          const copy = gateCopy(g);
          return (
          <Row key={g.name} style={{ alignItems: 'flex-start', gap: space.sm, paddingVertical: 4 }}>
            <Txt variant="smallStrong" color={toneColor(copy.tone, c)} style={{ width: 18 }}>
              {copy.glyph}
            </Txt>
            <Stack gap={2} style={{ flex: 1 }}>
              <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                <Txt variant="smallStrong" mono>
                  {g.name}
                </Txt>
                {g.insufficient ? (
                  <Pill fg={toneColor('neutral', c)} bordered>
                    not enough data
                  </Pill>
                ) : null}
                {!g.fatal ? (
                  <Pill fg={c.textMuted} bordered>
                    advisory
                  </Pill>
                ) : null}
                {g.value !== null && g.value !== undefined ? (
                  <Txt variant="caption" tone="muted" numeric>
                    {formatGate(g.value)}
                    {g.bar !== null && g.bar !== undefined ? ` vs ${formatGate(g.bar)}` : ''}
                  </Txt>
                ) : null}
              </Row>
              <Txt variant="caption" tone="secondary">
                {g.statement}
              </Txt>
              {g.insufficient ? (
                <Txt variant="caption" tone="muted">
                  Its statistic could not be computed on the record it has (too few signal days) — not passed, not a measured failure.
                </Txt>
              ) : null}
            </Stack>
          </Row>
          );
        })}
      </Stack>
    </Card>
  );
}

function formatGate(v: number): string {
  if (Number.isInteger(v)) return count(v);
  return Math.abs(v) < 1 ? v.toFixed(3) : v.toFixed(2);
}

// ── basket, proposal, post-mortem ───────────────────────────────────────────

export function BasketCard({ basket }: { basket: BasketView }) {
  const { c } = useTheme();
  const withheld = basket.constituents_visibility === 'withheld_pending_ra_review';
  return (
    <Card accent={withheld ? c.caution : c.positive}>
      <Stack gap={space.sm}>
        <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
          <Label>The basket</Label>
          <Pill fg={withheld ? c.caution : c.positive} bordered>
            {withheld ? 'constituents withheld' : 'RA-reviewed'}
          </Pill>
        </Row>
        <Txt variant="body" tone="secondary">
          {publicSafe(basket.description)}
        </Txt>
        <Txt variant="caption" tone="muted">
          {basket.ra_review_state}
        </Txt>
        {!withheld && basket.constituents.length > 0 ? (
          <Row gap={space.xs} style={{ flexWrap: 'wrap' }}>
            {basket.constituents.map((s) => (
              <Pill key={s} fg={c.textSecondary} bordered>
                {s}
              </Pill>
            ))}
          </Row>
        ) : null}
        <Txt variant="caption" tone="muted">
          Research experiment, not a recommendation. No entry, exit or stop is implied for anyone.
        </Txt>
      </Stack>
    </Card>
  );
}

export function ProposalCard({ proposal }: { proposal: ProposalView }) {
  const { c } = useTheme();
  const blocked = proposal.status === 'blocked_unsigned_constitution';
  return (
    <Stack gap={space.md}>
      <Card accent={blocked ? c.caution : c.positive}>
        <Stack gap={space.sm}>
          <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
            <Label tone="inherit" style={{ color: blocked ? c.caution : c.positive }}>
              Graduation proposal · v{proposal.version} → {proposal.target_agent}
            </Label>
            <Pill fg={blocked ? c.caution : c.positive} bordered>
              {humanise(proposal.status)}
            </Pill>
          </Row>
          <Txt variant="body" tone="secondary">
            {proposal.human_gate}
          </Txt>
          <Txt variant="caption" tone="muted" numeric>
            proposed {dateShort(proposal.proposed_edition)} · incumbent {proposal.incumbent} · decided by {proposal.decided_by}
          </Txt>
        </Stack>
      </Card>
      <GatesList gates={proposal.gates} title="Graduation gates — on the forward record only" />
    </Stack>
  );
}

export function PostMortemCard({ postMortem }: { postMortem: PostMortem }) {
  const { c } = useTheme();
  return (
    <Card accent={c.negative}>
      <Stack gap={space.lg}>
        <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
          <Label tone="inherit" style={{ color: c.negative }}>
            Post-mortem
          </Label>
          <Pill fg={c.negative} bg={c.negativeSoft}>
            {humanise(postMortem.cause)}
          </Pill>
        </Row>
        <RichText variant="heading">{publicSafe(postMortem.summary.headline)}</RichText>
        <RichText variant="body" tone="secondary">
          {publicSafe(postMortem.summary.body)}
        </RichText>
        <Attribution line={postMortem.summary} />
        <Divider />
        <View style={{ backgroundColor: c.positiveSoft, borderRadius: radius.md, padding: space.lg, gap: space.xs }}>
          <Label tone="inherit" style={{ color: c.positive }}>
            What we kept
          </Label>
          <Txt variant="body">{publicSafe(postMortem.what_we_kept)}</Txt>
        </View>
        <Txt variant="caption" tone="muted">
          Retired {postMortem.retired_version} on {dateTimeIST(postMortem.died_at)}
          {postMortem.approved_by ? ` · approved by ${postMortem.approved_by}` : ''}.
        </Txt>
      </Stack>
    </Card>
  );
}
