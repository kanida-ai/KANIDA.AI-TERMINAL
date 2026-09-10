import { useLocalSearchParams, useRouter } from 'expo-router';
import { useMemo } from 'react';
import { View } from 'react-native';

import { api } from '@/api/client';
import type { Finding } from '@/api/types';
import { useResource } from '@/api/useResource';
import { AgentHeader, Disclosure } from '@/components/agent';
import { DecisionPill, GradingLine, LevelChip, RecordLine, SampleChip, VerdictPill } from '@/components/chips';
import { FactPress, FactsProvider, RichText } from '@/components/facts';
import { Attribution } from '@/components/narrative';
import { Page } from '@/components/Page';
import { EmptyState, Resourced, SkeletonCard } from '@/components/states';
import { Card, Divider, Field, Label, Pill, Row, Stack, Touchable, Txt } from '@/components/ui';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { count, dateShort, dateTimeIST, factValue, pctAbs, rangeWithSpan } from '@/lib/format';
import { decisionCopy, levelCopy, researchSafe, sampleCopy, sampleTone, storyLabelFor } from '@/lib/honesty';
import { findingsOf } from '@/lib/stories';

/**
 * DEPTH for one finding — everything the story slide summarised:
 *
 *   the full narrative · the decision and its reason · the evidence (level, n,
 *   period, regime, comparison group, cost hurdle, data source, universe,
 *   disclosures) · every fact, tappable · how it will be graded (the rule frozen
 *   at publication) · how it was graded · related stocks · the follow-up
 *   questions · why it was published (the usefulness score against the threshold).
 *
 * Back returns to the same story in the feed (the pager remembers its position).
 */
export default function StoryDepthScreen() {
  const { id, date } = useLocalSearchParams<{ id: string; date?: string }>();
  const router = useRouter();
  const edition = typeof date === 'string' && date ? date : null;
  const feed = useResource((s) => api.feed(edition, s), [edition]);

  const finding = useMemo<Finding | null>(() => {
    if (!feed.data) return null;
    return findingsOf(feed.data).find((f) => f.id === id) ?? null;
  }, [feed.data, id]);

  const back = () =>
    router.canGoBack()
      ? router.back()
      : router.replace({ pathname: '/pathfinder', params: edition ? { date: edition, story: String(id) } : { story: String(id) } });

  return (
    <View style={{ flex: 1 }}>
      <AgentHeader name="Pathfinder" mandate="One finding — the evidence behind the story" onBack={back} />
      <Page refreshing={feed.refreshing} onRefresh={feed.refresh} contentStyle={{ paddingTop: space.xl }}>
        <Resourced
          data={feed.data}
          error={feed.error}
          loading={feed.loading}
          onRetry={feed.refresh}
          skeleton={
            <Stack gap={space.lg}>
              <SkeletonCard lines={3} />
              <SkeletonCard lines={5} />
            </Stack>
          }>
          {() =>
            finding ? (
              <FactsProvider facts={[...finding.facts, ...finding.grading.realized_facts]}>
                <Depth f={finding} />
              </FactsProvider>
            ) : (
              <EmptyState title="That story is not on this edition" body="The link may point at a different close. Open the feed for the latest edition." />
            )
          }
        </Resourced>
      </Page>
    </View>
  );
}

function Depth({ f }: { f: Finding }) {
  const { c } = useTheme();
  const router = useRouter();
  const d = decisionCopy[f.decision];
  const p = f.provenance;
  const u = f.usefulness;

  return (
    <Stack gap={space.xxl}>
      {/* ── the story, in full ─────────────────────────────────────── */}
      <Stack gap={space.md}>
        <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
          <Txt variant="label" tone="pathfinder">
            {f.tier === 'what_matters_now' ? 'What matters now' : 'Discovery'} · {storyLabelFor(f)}
          </Txt>
          <Txt variant="caption" tone="muted" numeric>
            rank {f.rank} · {dateShort(f.edition_date)}
          </Txt>
        </Row>
        <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
          <Txt variant="subheading" tone="secondary">
            {f.subject}
          </Txt>
          <Pill fg={c.textMuted} bordered>
            {f.subject_kind}
          </Pill>
        </Row>
        <RichText variant="title">{f.narrative.headline}</RichText>
        <Card>
          <Stack gap={space.md}>
            <Field label="The question the library asked">
              <Txt variant="body" tone="secondary">
                {researchSafe(f.question)}
              </Txt>
            </Field>
            <Divider />
            <RichText variant="body">{f.narrative.body}</RichText>
            <Attribution line={f.narrative} />
          </Stack>
        </Card>
      </Stack>

      {/* ── the decision ───────────────────────────────────────────── */}
      <Section kicker="The decision" title={d.label}>
        <Card raised>
          <Stack gap={space.sm}>
            <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
              <DecisionPill decision={f.decision} />
            </Row>
            <Txt variant="body">{researchSafe(f.decision_reason)}</Txt>
            <Txt variant="caption" tone="muted">
              {d.meaning}
            </Txt>
            {f.continues ? (
              <Txt variant="caption" tone="muted">
                Continues {f.continues} — the same claim while its horizon is still running. Served, not counted, graded once through the original.
              </Txt>
            ) : null}
          </Stack>
        </Card>
      </Section>

      {/* ── the evidence ───────────────────────────────────────────── */}
      <Section kicker="The evidence" title="Where the numbers come from">
        <Card raised>
          <Stack gap={space.lg}>
            <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
              <LevelChip level={p.level} />
              <SampleChip n={p.n} flag={p.sample_flag} />
              <Pill fg={c.textMuted} bordered>
                hurdle {pctAbs(p.cost_hurdle_pct, 2)}
              </Pill>
            </Row>
            <Field label="Evidence level">
              <Txt variant="body">{levelCopy[p.level].long}</Txt>
            </Field>
            <Field label="Compared against">
              <Txt variant="body" tone="secondary">
                {p.comparison_group}
              </Txt>
            </Field>
            <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
              <Field label="Historical period" style={{ flexGrow: 1, flexBasis: 0, minWidth: 160 }}>
                <Txt variant="body" numeric>
                  {rangeWithSpan(p.period)}
                </Txt>
              </Field>
              <Field label="Regime it was computed in" style={{ flexGrow: 1, flexBasis: 0, minWidth: 160 }}>
                <Txt variant="body">{p.regime}</Txt>
              </Field>
            </Row>
            <Field label="Cost hurdle used to judge meaningfulness">
              <Txt variant="body" numeric>
                {pctAbs(p.cost_hurdle_pct, 2)} round trip · {p.cost_convention}
              </Txt>
            </Field>
            <Divider />
            <Stack gap={space.xs}>
              <Label>Data</Label>
              <Txt variant="caption" tone="muted" mono>
                {p.data_source} · {p.universe}
              </Txt>
              <Txt variant="caption" tone="muted" mono>
                {p.computed_by} · point-in-time as of {dateShort(p.as_of)} · computed {dateTimeIST(p.computed_at)}
              </Txt>
            </Stack>
            {p.disclosures.length > 0 ? (
              <Stack gap={space.xs}>
                <Label>Disclosures</Label>
                {p.disclosures.map((line, i) => (
                  <Txt key={i} variant="caption" tone="secondary">
                    · {line}
                  </Txt>
                ))}
              </Stack>
            ) : null}
          </Stack>
        </Card>
      </Section>

      {/* ── every number ───────────────────────────────────────────── */}
      <Section kicker="The numbers" title="Every figure on this card, tappable">
        <Card raised padded={false}>
          {f.facts.map((fact, i) => {
            const grey = fact.sample_flag === 'greyed';
            const key = f.key_fact_refs.includes(fact.id);
            return (
              <FactPress key={fact.id} fact={fact}>
                <View
                  style={{
                    flexDirection: 'row',
                    alignItems: 'center',
                    gap: space.md,
                    paddingHorizontal: space.xl,
                    paddingVertical: space.md,
                    borderTopWidth: i === 0 ? 0 : 1,
                    borderTopColor: c.border,
                  }}>
                  <Stack gap={2} style={{ flex: 1 }}>
                    <Txt variant="small" tone={key ? 'default' : 'secondary'} style={key ? { fontWeight: '600' } : undefined}>
                      {fact.label}
                    </Txt>
                    <Txt variant="caption" color={sampleTone(fact.sample_flag, c)} numeric>
                      {fact.n !== null && fact.n !== undefined ? `n = ${count(fact.n)} · ` : ''}
                      {sampleCopy[fact.sample_flag].short}
                      {fact.provenance.level ? ` · ${levelCopy[fact.provenance.level].short}` : ''}
                    </Txt>
                  </Stack>
                  <Txt variant="bodyStrong" numeric color={grey ? c.textGreyed : c.text} style={{ maxWidth: '45%' }} numberOfLines={typeof fact.value === 'string' ? 3 : 1}>
                    {factValue(fact.value, fact.unit)}
                  </Txt>
                </View>
              </FactPress>
            );
          })}
        </Card>
      </Section>

      {/* ── how it is graded ───────────────────────────────────────── */}
      <Section kicker="How it is graded" title="The rule was frozen at publication">
        <Card raised>
          <Stack gap={space.md}>
            <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
              <Pill fg={c.pathfinder} bordered>
                {f.grading_rule.kind.replace(/_/g, ' ')}
              </Pill>
              <Pill fg={c.textMuted} bordered>
                horizon {f.grading_rule.horizon_sessions} session{f.grading_rule.horizon_sessions === 1 ? '' : 's'}
              </Pill>
              <Pill fg={c.textMuted} bordered>
                hurdle {pctAbs(f.grading_rule.hurdle_pct, 2)}
              </Pill>
            </Row>
            <Field label="What is measured">
              <Txt variant="small" tone="secondary">
                {f.grading_rule.metric}
              </Txt>
            </Field>
            <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
              <RuleLeg label="Right" text={f.grading_rule.right} color={c.positive} />
              <RuleLeg label="Wrong" text={f.grading_rule.wrong} color={c.negative} />
              <RuleLeg label="Inconclusive" text={f.grading_rule.inconclusive} color={c.caution} />
            </Row>
            <Txt variant="caption" tone="muted" mono numeric>
              frozen {dateTimeIST(f.grading_rule.frozen_at)} · {f.grading_rule.rule_version}
            </Txt>
          </Stack>
        </Card>

        <Card raised accent={f.grading.verdict ? undefined : c.border}>
          <Stack gap={space.md}>
            <Label>The grade</Label>
            <GradingLine grading={f.grading} horizonSessions={f.grading_rule.horizon_sessions} editionDate={f.edition_date} />
            {f.grading.verdict ? (
              <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                <VerdictPill verdict={f.grading.verdict} />
                <Txt variant="caption" tone="muted" numeric>
                  {f.grading.graded_at ? `graded ${dateTimeIST(f.grading.graded_at)}` : ''}
                </Txt>
              </Row>
            ) : null}
            {f.grading.void_reason ? (
              <Txt variant="small" tone="secondary">
                {f.grading.void_reason}
              </Txt>
            ) : null}
            {f.grading.realized_facts.length > 0 ? (
              <Stack gap={space.xs}>
                <Label>What actually happened</Label>
                {f.grading.realized_facts.map((fact) => (
                  <FactPress key={fact.id} fact={fact}>
                    <Row style={{ justifyContent: 'space-between', gap: space.md }}>
                      <Txt variant="small" tone="secondary" style={{ flex: 1 }}>
                        {fact.label}
                      </Txt>
                      <Txt variant="bodyStrong" numeric>
                        {factValue(fact.value, fact.unit)}
                      </Txt>
                    </Row>
                  </FactPress>
                ))}
              </Stack>
            ) : null}
            <RecordLine label={f.grading.record} backfilled={f.grading.backfilled} />
          </Stack>
        </Card>
      </Section>

      {/* ── related, follow-ups ────────────────────────────────────── */}
      {f.related_symbols.length > 0 ? (
        <Section kicker="Related stocks" title="Named on this card">
          <Row gap={space.xs} style={{ flexWrap: 'wrap' }}>
            {f.related_symbols.map((s) => (
              <Pill key={s} fg={c.textSecondary} bordered>
                {s}
              </Pill>
            ))}
          </Row>
          <Txt variant="caption" tone="muted">
            Research items, not instructions. No entry, target, stop or execution is implied.
          </Txt>
        </Section>
      ) : null}

      {f.follow_up_questions.length > 0 ? (
        <Section kicker="Pathfinder’s follow-up questions" title="What this card asks next">
          <Card raised>
            <Stack gap={space.md}>
              {f.follow_up_questions.map((q, i) => (
                <Row key={i} gap={space.md} style={{ alignItems: 'flex-start' }}>
                  <Txt variant="metricSm" tone="muted" numeric style={{ width: 26 }}>
                    {i + 1}
                  </Txt>
                  <Txt variant="bodyStrong" style={{ flex: 1 }}>
                    {researchSafe(q)}
                  </Txt>
                </Row>
              ))}
              <Txt variant="caption" tone="muted">
                A follow-up becomes an experiment only through the worth-testing gate, with every variant counted.
              </Txt>
            </Stack>
          </Card>
        </Section>
      ) : null}

      {/* ── why it was published ───────────────────────────────────── */}
      <Section kicker="Why it was published" title="Usefulness against the threshold">
        <Card raised>
          <Stack gap={space.md}>
            <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
              <Score label="Total" value={u.total} strong />
              <Score label="Evidence" value={u.evidence_strength} />
              <Score label="Novelty" value={u.novelty} />
              <Score label="Relevance" value={u.trader_relevance} />
              <Score label="Magnitude" value={u.magnitude} />
            </Row>
            <Txt variant="caption" tone="muted" numeric>
              threshold {u.threshold.toFixed(2)} · {u.version} · nothing below the threshold is served, continuation or not
            </Txt>
          </Stack>
        </Card>
      </Section>

      <Touchable
        accessibilityRole="button"
        onPress={() => router.push({ pathname: '/pathfinder/scoreboard', params: { date: f.edition_date } })}
        style={{ alignSelf: 'flex-start', paddingHorizontal: space.lg, paddingVertical: space.sm, borderRadius: radius.pill, borderWidth: 1, borderColor: c.borderStrong }}>
        <Txt variant="smallStrong">The scoreboard as of this edition ›</Txt>
      </Touchable>

      <Disclosure text={f.disclosure} />
    </Stack>
  );
}

function Section({ kicker, title, children }: { kicker: string; title: string; children: React.ReactNode }) {
  const { c } = useTheme();
  return (
    <Stack gap={space.md}>
      <Stack gap={space.xs}>
        <Label tone="inherit" style={{ color: c.pathfinder }}>
          {kicker}
        </Label>
        <Txt variant="title">{title}</Txt>
      </Stack>
      {children}
    </Stack>
  );
}

function RuleLeg({ label, text, color }: { label: string; text: string; color: string }) {
  return (
    <Stack gap={3} style={{ flexGrow: 1, flexBasis: 0, minWidth: 160 }}>
      <Txt variant="label" color={color}>
        {label}
      </Txt>
      <Txt variant="small" tone="secondary">
        {text}
      </Txt>
    </Stack>
  );
}

function Score({ label, value, strong = false }: { label: string; value: number; strong?: boolean }) {
  const { c } = useTheme();
  return (
    <Stack gap={3} style={{ flexGrow: 1, flexBasis: 0, minWidth: 72 }}>
      <Label>{label}</Label>
      <Txt variant={strong ? 'metricSm' : 'bodyStrong'} numeric color={strong ? c.pathfinder : c.textSecondary}>
        {value.toFixed(2)}
      </Txt>
    </Stack>
  );
}
