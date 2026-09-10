import { useLocalSearchParams, useRouter } from 'expo-router';
import { View } from 'react-native';

import { api } from '@/api/client';
import type { ExperimentScoreboard, ScoreCounts, Scoreboard } from '@/api/types';
import { useResource } from '@/api/useResource';
import { AgentHeader, Disclosure } from '@/components/agent';
import { RecordChip, RecordLine, SampleChip } from '@/components/chips';
import { ScoreInline } from '@/components/feed/StorySlide';
import { Page } from '@/components/Page';
import { Resourced, SkeletonCard } from '@/components/states';
import { Card, Divider, Label, Pill, Row, Stack, Txt } from '@/components/ui';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { count, dateShort } from '@/lib/format';
import { sampleCopy } from '@/lib/honesty';

/**
 * THE SCOREBOARD, in full: Right · Wrong · Inconclusive · n (independent),
 * forward vs backfilled, void, regraded, continued, pending, by question type;
 * and the experiment scoreboard beside it. The record label is the first thing
 * on the page and the last.
 */
export default function ScoreboardScreen() {
  const { date } = useLocalSearchParams<{ date?: string }>();
  const router = useRouter();
  const edition = typeof date === 'string' && date ? date : null;
  const feed = useResource((s) => api.feed(edition, s), [edition]);

  return (
    <View style={{ flex: 1 }}>
      <AgentHeader
        name="Pathfinder"
        mandate="The public record — right, wrong, inconclusive, with n"
        onBack={() => (router.canGoBack() ? router.back() : router.replace('/pathfinder'))}
      />
      <Page refreshing={feed.refreshing} onRefresh={feed.refresh} contentStyle={{ paddingTop: space.xl }}>
        <Resourced data={feed.data} error={feed.error} loading={feed.loading} onRetry={feed.refresh} skeleton={<SkeletonCard lines={5} />}>
          {(data) => (
            <Stack gap={space.xxl}>
              <Stack gap={space.md}>
                <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                  <Txt variant="label" tone="pathfinder">
                    Scoreboard as of {dateShort(data.scoreboard.as_of)}
                  </Txt>
                  <RecordChip backfilled={data.backfilled} />
                </Row>
                <Txt variant="title">Findings graded in public</Txt>
                <RecordLine label={data.scoreboard.record_label} backfilled={data.backfilled} />
              </Stack>

              <FindingsBoard sb={data.scoreboard} />

              {data.experiments_scoreboard ? <ExperimentsBoard xs={data.experiments_scoreboard} /> : null}

              <Disclosure text={data.disclosure} />
            </Stack>
          )}
        </Resourced>
      </Page>
    </View>
  );
}

function Big({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <Stack gap={2} style={{ flexGrow: 1, flexBasis: 0 }}>
      <Txt variant="metricLg" numeric color={color}>
        {count(value)}
      </Txt>
      <Txt variant="caption" tone="muted">
        {label}
      </Txt>
    </Stack>
  );
}

function SplitCard({ title, counts, note }: { title: string; counts: ScoreCounts; note: string }) {
  const { c } = useTheme();
  const empty = counts.n === 0;
  return (
    <View style={{ flex: 1, minWidth: 150, padding: space.lg, borderRadius: radius.md, backgroundColor: c.bgSunken, gap: space.xs }}>
      <Label>{title}</Label>
      <Txt variant="metric" numeric color={empty ? c.textGreyed : c.text}>
        n = {count(counts.n)}
      </Txt>
      <Txt variant="small" tone={empty ? 'greyed' : 'secondary'} numeric>
        {empty ? 'nothing yet' : `Right ${counts.right} · Wrong ${counts.wrong} · Inconclusive ${counts.inconclusive}`}
      </Txt>
      <Txt variant="caption" tone="muted">
        {note} · {sampleCopy[counts.sample_flag].short}
      </Txt>
    </View>
  );
}

function FindingsBoard({ sb }: { sb: Scoreboard }) {
  const { c } = useTheme();
  const templates = Object.entries(sb.by_template);
  return (
    <Card>
      <Stack gap={space.lg}>
        <Row style={{ gap: space.lg, alignItems: 'flex-end' }}>
          <Big label="Right" value={sb.right} color={c.positive} />
          <Big label="Wrong" value={sb.wrong} color={c.negative} />
          <Big label="Inconclusive" value={sb.inconclusive} color={c.caution} />
        </Row>
        <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
          <SampleChip n={sb.n} flag={sb.sample_flag} />
          <Pill fg={c.textMuted} bordered>
            independent · one grade per claim per non-overlapping horizon
          </Pill>
        </Row>

        <View style={{ flexDirection: 'row', flexWrap: 'wrap', gap: space.md }}>
          <SplitCard title="Forward" counts={sb.forward} note="generated on the session date, before the outcome" />
          <SplitCard title="Backfilled" counts={sb.backfilled} note="generated after the fact — simulated" />
        </View>

        <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
          <Small label="Pending" value={sb.pending} hint="horizon not complete" />
          <Small label="Void" value={sb.void} hint="unmeasurable, not counted" />
          <Small label="Regraded" value={sb.regraded} hint="folded into an earlier grade" />
          <Small label="Continued" value={sb.continued} hint="served, not counted" />
          <Small label="All grade rows" value={sb.n_total} hint="including re-grades" />
        </Row>

        <Divider />
        <Stack gap={space.sm}>
          <Label>By question type</Label>
          {templates.length === 0 ? (
            <Txt variant="small" tone="greyed">
              Nothing graded yet.
            </Txt>
          ) : (
            templates.map(([k, v]) => (
              <Row key={k} style={{ justifyContent: 'space-between', gap: space.md, flexWrap: 'wrap' }}>
                <Txt variant="small" tone="secondary" style={{ minWidth: 120 }}>
                  {k.replace(/_/g, ' ')}
                </Txt>
                <ScoreInline counts={v} />
              </Row>
            ))
          )}
        </Stack>

        <Txt variant="caption" tone="muted">
          A card that says “x% of cases bounced” is graded on its own subject’s single outcome, so the per-card verdict is noisy by construction. The per-type split is the number to read.
        </Txt>
      </Stack>
    </Card>
  );
}

function ExperimentsBoard({ xs }: { xs: ExperimentScoreboard }) {
  const { c } = useTheme();
  const families = Object.entries(xs.by_family);
  return (
    <Stack gap={space.md}>
      <Stack gap={space.xs}>
        <Label tone="inherit" style={{ color: c.pathfinder }}>
          Experiments
        </Label>
        <Txt variant="title">Graded periods, virtual money</Txt>
      </Stack>
      <Card>
        <Stack gap={space.lg}>
          <Row style={{ gap: space.lg, alignItems: 'flex-end' }}>
            <Big label="Right" value={xs.right} color={c.positive} />
            <Big label="Wrong" value={xs.wrong} color={c.negative} />
            <Big label="Inconclusive" value={xs.inconclusive} color={c.caution} />
          </Row>
          <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
            <SampleChip n={xs.n} flag={xs.sample_flag} />
            <Pill fg={c.textMuted} bordered>
              void {xs.void}
            </Pill>
            <Pill fg={c.textMuted} bordered>
              pending {xs.pending}
            </Pill>
          </Row>
          <RecordLine label={xs.record_label} backfilled={xs.forward.n === 0} />
          <View style={{ flexDirection: 'row', flexWrap: 'wrap', gap: space.md }}>
            <SplitCard title="Forward" counts={xs.forward} note="stepped on the session date" />
            <SplitCard title="Backfilled" counts={xs.backfilled} note="stepped after the fact — simulated" />
          </View>
          <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
            <Small label="Testing" value={xs.experiments_testing} />
            <Small label="Buried" value={xs.experiments_buried} />
            <Small label="Proposed" value={xs.experiments_proposed} hint="human decides" />
            <Small label="Not opened" value={xs.candidates_not_opened} hint="declined on the record" />
            <Small label="Trials" value={xs.trials_total} hint="every variant counted" />
          </Row>
          {families.length > 0 ? (
            <>
              <Divider />
              <Stack gap={space.sm}>
                <Label>By family</Label>
                {families.map(([k, v]) => (
                  <Row key={k} style={{ justifyContent: 'space-between', gap: space.md, flexWrap: 'wrap' }}>
                    <Txt variant="small" tone="secondary" style={{ minWidth: 120 }}>
                      {k.replace(/_/g, ' ')}
                    </Txt>
                    <ScoreInline counts={v} />
                  </Row>
                ))}
              </Stack>
            </>
          ) : null}
        </Stack>
      </Card>
    </Stack>
  );
}

function Small({ label, value, hint }: { label: string; value: number; hint?: string }) {
  return (
    <Stack gap={2} style={{ flexGrow: 1, flexBasis: 0, minWidth: 90 }}>
      <Label>{label}</Label>
      <Txt variant="metricSm" numeric>
        {count(value)}
      </Txt>
      {hint ? (
        <Txt variant="caption" tone="muted">
          {hint}
        </Txt>
      ) : null}
    </Stack>
  );
}
