import { useLocalSearchParams, useRouter } from 'expo-router';
import { View } from 'react-native';

import { api } from '@/api/client';
import type { ExperimentDetail } from '@/api/types';
import { useResource } from '@/api/useResource';
import { AgentHeader, AsOfStamp, Disclosure } from '@/components/agent';
import { ChangeLog } from '@/components/ChangeLog';
import { EvidenceCard, LlmUsageCard, PostMortemCard } from '@/components/Evidence';
import { FactsProvider } from '@/components/facts';
import { LedgerCard } from '@/components/Ledger';
import { Page } from '@/components/Page';
import { PerformancePair } from '@/components/Performance';
import { Sparkline } from '@/components/Sparkline';
import { Resourced, SkeletonCard } from '@/components/states';
import { BEAT_LABEL, StoryBeatCard, TriggerCard } from '@/components/Story';
import { Card, Divider, Field, Label, Pill, Row, Stack, Txt } from '@/components/ui';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { count, dateTimeIST, humanise } from '@/lib/format';
import { sampleCopy, sampleTone, statusLabel, statusMeaning, statusTone } from '@/lib/honesty';
import { shortExperimentId } from '@/lib/tokens';

/**
 * EXPERIMENT DETAIL — the full loop story for one hypothesis. The heart.
 *
 * The page reads top to bottom exactly as the loop runs, and each beat is
 * followed immediately by the deterministic material that beat is talking about:
 *
 *   I noticed              -> the trigger that fired (engine, never a clock)
 *   I am testing because   -> the rulebook + the historical evidence
 *   the virtual experiment -> the virtual book, losers-first ledger
 *   what happened          -> forward vs historical, side by side
 *   what I learned/changed -> the L1-L3 change-log, and the post-mortem if dead
 *   what I test next       -> the condition that will next wake the model
 *
 * A reader can follow one idea from the moment it was noticed to the moment it
 * was killed or graduated, and can tap any number to see where it came from.
 */
export default function ExperimentDetailScreen() {
  const { id } = useLocalSearchParams<{ id: string }>();
  const router = useRouter();
  const res = useResource((s) => api.experiment(String(id), s), [id]);

  return (
    <View style={{ flex: 1 }}>
      <AgentHeader
        name="Pathfinder Agent"
        mandate="One experiment, start to finish"
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
          {(exp) => (
            <FactsProvider facts={exp.facts} evidence={exp.evidence}>
              <DetailBody exp={exp} />
            </FactsProvider>
          )}
        </Resourced>
      </Page>
    </View>
  );
}

function DetailBody({ exp }: { exp: ExperimentDetail }) {
  const { c } = useTheme();
  const badge = statusTone(exp.status, c);
  const flagTone = sampleTone(exp.sample_flag, c);

  const beat = (b: (typeof exp.story)[number]['beat']) => exp.story.find((s) => s.beat === b);

  return (
    <Stack gap={space.xxl}>
      {/* ── Masthead ─────────────────────────────────────────────── */}
      <Stack gap={space.lg}>
        <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
          <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
            <Txt variant="smallStrong" tone="muted" numeric>
              {shortExperimentId(exp.id)}
            </Txt>
            <Pill fg={badge.fg} bg={badge.bg} bordered={exp.status === 'queued'}>
              {statusLabel[exp.status]}
            </Pill>
            <Pill fg={c.textMuted} bordered>
              stage: {humanise(exp.stage)}
            </Pill>
          </Row>
          {exp.spark ? <Sparkline spark={exp.spark} width={120} height={38} muted={exp.sample_flag === 'greyed'} /> : null}
        </Row>

        <Txt variant="display" style={{ fontSize: 26, lineHeight: 33 }}>
          “{exp.hypothesis}”
        </Txt>

        <Txt variant="small" tone="secondary">
          {statusMeaning[exp.status]}
        </Txt>

        <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
          <Pill fg={flagTone} bordered>
            {exp.n === null || exp.n === undefined ? 'n —' : `n = ${count(exp.n)}`}
            {exp.sample_flag === 'ok' ? '' : ` · ${sampleCopy[exp.sample_flag].short}`}
          </Pill>
          {exp.occurrences !== null && exp.occurrences !== undefined ? (
            <Pill fg={c.textMuted} bordered>
              {count(exp.occurrences)} occurrences
            </Pill>
          ) : null}
          {exp.strategy_version ? (
            <Pill fg={c.textMuted} bordered>
              {exp.strategy_version}
            </Pill>
          ) : null}
          <Pill fg={c.textMuted} bordered>
            {exp.constitution_version}
          </Pill>
        </Row>

        <Card raised>
          <Stack gap={space.md}>
            <Field label="The question I asked myself">{exp.question}</Field>
            <Divider />
            <Field label="Why I thought it was worth asking">{exp.rationale}</Field>
          </Stack>
        </Card>
      </Stack>

      {/* ── Beat 1: I noticed ────────────────────────────────────── */}
      <Section title={BEAT_LABEL.noticed} kicker="Step 1 of the loop">
        {beat('noticed') ? <StoryBeatCard line={beat('noticed')!} showRail={false} showLabel={false} /> : null}
        {exp.triggers.map((t) => (
          <TriggerCard key={t.id} trigger={t} />
        ))}
      </Section>

      {/* ── Beat 2: I am testing this because ────────────────────── */}
      <Section title={BEAT_LABEL.hypothesis} kicker="Step 2 — the rule, then the evidence">
        {beat('hypothesis') ? <StoryBeatCard line={beat('hypothesis')!} showRail={false} showLabel={false} /> : null}
        <RulebookCard exp={exp} />
        {exp.evidence
          .filter((e) => e.kind === 'historical_replay' || e.kind === 'novelty_check')
          .map((e) => (
            <EvidenceCard key={e.id} evidence={e} />
          ))}
      </Section>

      {/* ── Beat 3: the virtual experiment ───────────────────────── */}
      <Section title={BEAT_LABEL.experiment} kicker="Step 3 — virtual capital, real rules">
        {beat('experiment') ? <StoryBeatCard line={beat('experiment')!} showRail={false} showLabel={false} /> : null}
        {exp.virtual_book ? (
          <LedgerCard book={exp.virtual_book} />
        ) : (
          <Card>
            <Txt variant="small" tone="greyed">
              No virtual capital has been committed yet. Nothing has been traded, so there is nothing
              to show.
            </Txt>
          </Card>
        )}
      </Section>

      {/* ── Beat 4: what happened ────────────────────────────────── */}
      <Section title={BEAT_LABEL.outcome} kicker="Step 4 — history against forward">
        {beat('outcome') ? <StoryBeatCard line={beat('outcome')!} showRail={false} showLabel={false} /> : null}
        <PerformancePair historical={exp.historical_return} virtual={exp.virtual_return} />
        {exp.evidence
          .filter((e) => e.kind !== 'historical_replay' && e.kind !== 'novelty_check')
          .map((e) => (
            <EvidenceCard key={e.id} evidence={e} />
          ))}
      </Section>

      {/* ── Beat 5: what I learned and changed ───────────────────── */}
      <Section
        title={BEAT_LABEL.learning}
        kicker="Step 5 — the governance record: what changed, why, on what evidence, and whether it helped">
        {beat('learning') ? <StoryBeatCard line={beat('learning')!} showRail={false} showLabel={false} /> : null}
        {exp.post_mortem ? <PostMortemCard postMortem={exp.post_mortem} /> : null}
        {exp.change_log.length > 0 ? (
          <ChangeLog entries={exp.change_log} />
        ) : (
          <Card>
            <Txt variant="small" tone="greyed">
              Nothing has been changed yet. The change-log stays empty until there is evidence to
              change something for.
            </Txt>
          </Card>
        )}
      </Section>

      {/* ── Beat 6: what I will test next ────────────────────────── */}
      <Section title={BEAT_LABEL.next} kicker="Step 6 — and what will wake me for it">
        {beat('next') ? <StoryBeatCard line={beat('next')!} showRail={false} showLabel={false} /> : null}
        {exp.next_review ? <TriggerCard trigger={exp.next_review} title="What will wake me next" /> : null}
      </Section>

      {exp.llm_usage ? <LlmUsageCard usage={exp.llm_usage} /> : null}

      <Stack gap={space.md}>
        <AsOfStamp asOf={dateTimeIST(exp.as_of)} constitutionVersion={exp.constitution_version} />
        <Disclosure text={exp.disclosure} />
      </Stack>
    </Stack>
  );
}

function Section({
  title,
  kicker,
  children,
}: {
  title: string;
  kicker: string;
  children: React.ReactNode;
}) {
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

/** The deterministic rule under test. No target price — an invalidation instead. */
function RulebookCard({ exp }: { exp: ExperimentDetail }) {
  const { c } = useTheme();
  const r = exp.rulebook;
  return (
    <Card raised>
      <Stack gap={space.lg}>
        <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
          <Label>The rulebook</Label>
          <Row gap={space.sm}>
            <Pill fg={r.direction === 'long' ? c.positive : c.negative} bordered>
              {r.direction.toUpperCase()}
            </Pill>
            <Pill fg={c.textMuted} bordered>
              {r.universe}
            </Pill>
          </Row>
        </Row>

        <Field label="Entry">{r.entry}</Field>
        <Field label="What makes it wrong (invalidation)">
          <Txt variant="body">{r.invalidation}</Txt>
        </Field>
        <Field label="Exit">{r.exit}</Field>

        <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
          <Field label="Horizon" style={{ flexGrow: 1, flexBasis: 0, minWidth: 120 }}>
            <Txt variant="body" numeric>
              {r.horizon_sessions} session{r.horizon_sessions === 1 ? '' : 's'}
            </Txt>
          </Field>
          <Field label="Sizing" style={{ flexGrow: 2, flexBasis: 0, minWidth: 180 }}>
            {r.sizing}
          </Field>
        </Row>

        <View style={{ backgroundColor: c.bgSunken, borderRadius: radius.md, padding: space.md, gap: 4 }}>
          <Label>Cost convention applied to every simulated trade</Label>
          <Txt variant="caption" tone="secondary">
            {r.cost_convention}
          </Txt>
        </View>

        <Txt variant="caption" tone="muted">
          There is no price target anywhere in this rulebook, by design. A rule states what would
          make it wrong; it does not promise where a price will go.
        </Txt>
      </Stack>
    </Card>
  );
}
