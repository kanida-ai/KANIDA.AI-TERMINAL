import { useLocalSearchParams, useRouter } from 'expo-router';
import { View } from 'react-native';

import { api } from '@/api/client';
import type { ExperimentRecord } from '@/api/types';
import { useResource } from '@/api/useResource';
import { AgentHeader, Disclosure } from '@/components/agent';
import { ChangeLog } from '@/components/ChangeLog';
import { LevelChip, RecordLine, SampleChip, StatePill } from '@/components/chips';
import { BasketCard, BeatsFull, GatesList, PostMortemCard, ProposalCard, TrialsLedger, VersionCard } from '@/components/experiment';
import { FactsProvider } from '@/components/facts';
import { ScoreInline } from '@/components/feed/StorySlide';
import { Page } from '@/components/Page';
import { Resourced, SkeletonCard } from '@/components/states';
import { Card, Field, Label, Pill, Row, Stack, Txt } from '@/components/ui';
import { useTheme } from '@/design/theme';
import { space } from '@/design/tokens';
import { count, dateShort, pctAbs, rangeWithSpan } from '@/lib/format';
import { comparisonCopy, experimentStateCopy, publicSafe, toneColor } from '@/lib/honesty';

/**
 * EXPERIMENT DEPTH — the full record for one hypothesis:
 *
 *   the seven beats in full · the versions (v1 → v2 → v3: what changed, why,
 *   trial counts, the frozen expectation, every period with expected vs actual
 *   and the learning) · every counted trial · the worth-testing gates · the
 *   basket (constituents withheld pending RA review) · the change-log · the
 *   post-mortem when buried · the graduation proposal when one exists.
 *
 * Died / buried is shown as fully as a win. Constituents appear only when the
 * API marks them RA-reviewed. No entry, target, stop or execution anywhere.
 */
export default function ExperimentDepthScreen() {
  const { id } = useLocalSearchParams<{ id: string }>();
  const router = useRouter();
  const res = useResource((s) => api.experiment(String(id), s), [id]);

  return (
    <View style={{ flex: 1 }}>
      <AgentHeader
        name="Pathfinder"
        mandate="One experiment — versions, trials, expected vs actual"
        onBack={() => (router.canGoBack() ? router.back() : router.replace('/pathfinder/experiments'))}
      />
      <Page refreshing={res.refreshing} onRefresh={res.refresh} contentStyle={{ paddingTop: space.xl }}>
        <Resourced
          data={res.data}
          error={res.error}
          loading={res.loading}
          onRetry={res.refresh}
          skeleton={
            <Stack gap={space.lg}>
              <SkeletonCard lines={2} />
              <SkeletonCard lines={5} />
              <SkeletonCard lines={4} />
            </Stack>
          }>
          {(rec) => (
            <FactsProvider facts={rec.facts}>
              <RecordBody rec={rec} />
            </FactsProvider>
          )}
        </Resourced>
      </Page>
    </View>
  );
}

function RecordBody({ rec }: { rec: ExperimentRecord }) {
  const { c } = useTheme();
  const state = experimentStateCopy[rec.state];
  const cmp = comparisonCopy[rec.latest_comparison];
  const current = rec.versions.find((v) => v.status === 'open') ?? rec.versions[rec.versions.length - 1];

  return (
    <Stack gap={space.xxl}>
      {/* ── masthead ─────────────────────────────────────────────── */}
      <Stack gap={space.md}>
        <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
          <Txt variant="label" tone="pathfinder">
            Experiment · {rec.family.replace(/_/g, ' ')}
          </Txt>
          <StatePill state={rec.state} />
        </Row>
        <Txt variant="display" style={{ fontSize: 26, lineHeight: 33 }}>
          {publicSafe(rec.theme)}
        </Txt>
        <Txt variant="small" tone="secondary">
          {state.meaning}
        </Txt>
        <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
          <ScoreInline counts={rec.score} voidCount={rec.score.void} />
          <Pill fg={toneColor(cmp.tone, c)} bordered>
            {cmp.label}
          </Pill>
        </Row>
        <Txt variant="caption" tone="muted" numeric>
          {rec.id} · opened {dateShort(rec.opened_edition)} · news {dateShort(rec.news_edition)} · v{rec.versions_count} ·{' '}
          {count(rec.trials_total)} trials · {count(rec.periods_graded)} periods graded · {rec.direction} · {rec.constitution_version}
        </Txt>
        <RecordLine label={rec.record_label} backfilled={rec.backfilled} />
        {rec.opened_backfilled !== rec.backfilled ? (
          <Txt variant="caption" tone="muted">
            The experiment was opened in a {rec.opened_backfilled ? 'backfill' : 'forward run'}; this card was written on a{' '}
            {rec.backfilled ? 'backfilled' : 'forward'} edition.
          </Txt>
        ) : null}

        <Card raised>
          <Stack gap={space.md}>
            <Field label="The question">{publicSafe(rec.question)}</Field>
            <Field label="The rule under test (a research definition, not an instruction)">
              <Txt variant="body" tone="secondary">
                {publicSafe(rec.current_rule_text)}
              </Txt>
            </Field>
            <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
              <LevelChip level={rec.evidence.level} />
              <SampleChip n={rec.evidence.n} flag={rec.evidence.sample_flag} />
              <Pill fg={c.textMuted} bordered>
                {rangeWithSpan(rec.evidence.period)}
              </Pill>
              <Pill fg={c.textMuted} bordered>
                hurdle {pctAbs(rec.evidence.cost_hurdle_pct, 2)}
              </Pill>
            </Row>
            <Txt variant="caption" tone="muted">
              From finding {rec.source_finding_id} · {rec.evidence.comparison_group}
            </Txt>
          </Stack>
        </Card>
      </Stack>

      {/* ── the seven beats ───────────────────────────────────────── */}
      <Section kicker="The story" title="I noticed → I tested → history showed → I decided → what happened → what I learned → what’s next">
        <BeatsFull story={rec.story} />
      </Section>

      {/* ── post-mortem first when buried: losers first ───────────── */}
      {rec.post_mortem ? (
        <Section kicker="Buried" title="The post-mortem, in full">
          <PostMortemCard postMortem={rec.post_mortem} />
        </Section>
      ) : null}

      {rec.proposal ? (
        <Section kicker="Graduation" title="A proposal, not a promotion">
          <ProposalCard proposal={rec.proposal} />
        </Section>
      ) : null}

      {/* ── versions ─────────────────────────────────────────────── */}
      <Section kicker="Versions" title={`${rec.id.replace(/^exp_/, '#')} → ${rec.versions.map((v) => `v${v.version}`).join(' → ')}`}>
        {rec.versions.map((v) => (
          <VersionCard key={v.version} v={v} isCurrent={current?.version === v.version} />
        ))}
      </Section>

      {/* ── the gate, the trials, the basket ─────────────────────── */}
      <Section kicker="The worth-testing gate" title="What v1 had to clear">
        <GatesList gates={rec.worth_testing_gates} title="Gates on the whole sealed history and the trailing window" />
      </Section>

      <Section kicker="Trials" title="Every variant ever evaluated, counted">
        <TrialsLedger trials={rec.trials} />
      </Section>

      <Section kicker="The basket" title="Sector / theme and evidence — names under RA review only">
        <BasketCard basket={rec.basket} />
      </Section>

      <Section kicker="Change-log" title="What changed, why, on what evidence">
        {rec.change_log.length > 0 ? (
          <ChangeLog entries={rec.change_log} />
        ) : (
          <Card>
            <Txt variant="small" tone="greyed">
              Nothing has changed yet.
            </Txt>
          </Card>
        )}
      </Section>

      <Stack gap={space.md}>
        <Txt variant="caption" tone="muted" numeric>
          Narrated by {rec.llm_provider === 'none' ? 'the engine (no model in the loop)' : rec.llm_provider}
        </Txt>
        <Disclosure text={rec.disclosure} />
      </Stack>
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
