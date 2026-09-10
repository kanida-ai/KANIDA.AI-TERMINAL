import { useRouter } from 'expo-router';
import { View } from 'react-native';

import { api } from '@/api/client';
import type { Confidence } from '@/api/types';
import { useResource } from '@/api/useResource';
import { AsOfStamp, Disclosure } from '@/components/agent';
import { LevelPill, LEVEL_MEANING } from '@/components/ChangeLog';
import { FactsProvider, RichText } from '@/components/facts';
import { Page } from '@/components/Page';
import { SampleChip } from '@/components/Performance';
import { EmptyState, Resourced, SkeletonCard } from '@/components/states';
import { Attribution, TriggerCard } from '@/components/Story';
import { Card, Divider, Label, Pill, Row, Stack, Touchable, Txt } from '@/components/ui';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { dateShort, dateTimeIST } from '@/lib/format';
import { shortExperimentId } from '@/lib/tokens';

/**
 * LEARNINGS — what Pathfinder now believes, and what it will ask next.
 *
 * Two halves, both required by the brief: `learned[]` (a statement, its level in
 * the learning hierarchy, its confidence, the experiments and evidence that
 * produced it, and the strategy versions it changed) and `testing_next[]` (the
 * question, why NOW, the deterministic test planned, and what is blocked).
 *
 * "Blocked" is shown, not hidden. A research lab with nothing blocked is a
 * research lab that is not telling you everything.
 */
const CONFIDENCE_COPY: Record<Confidence, string> = {
  provisional: 'Provisional — one line of evidence, small sample.',
  supported: 'Supported — holds across the evidence gathered so far.',
  strong: 'Strong — holds out of sample and after costs.',
};

export default function LearningsScreen() {
  const { c } = useTheme();
  const router = useRouter();
  const res = useResource((s) => api.learnings(s), []);

  return (
    <Page refreshing={res.refreshing} onRefresh={res.refresh} bottomInset={72} contentStyle={{ paddingTop: space.xl }}>
      <Resourced
        data={res.data}
        error={res.error}
        loading={res.loading}
        onRetry={res.refresh}
        skeleton={
          <Stack gap={space.lg}>
            <SkeletonCard lines={3} />
            <SkeletonCard lines={3} />
          </Stack>
        }>
        {(data) => (
          <FactsProvider facts={data.facts}>
            <Stack gap={space.xxl}>
              <Stack gap={space.md}>
                <Stack gap={space.xs}>
                  <Label>Key learnings</Label>
                  <Txt variant="title">What I now believe</Txt>
                  <Txt variant="small" tone="secondary">
                    Each one names the experiments that produced it and the level of the learning
                    hierarchy it sits at. Nothing here changes the Constitution — that is human-only.
                  </Txt>
                </Stack>

                {data.learned.length === 0 ? (
                  <EmptyState
                    title="Nothing has been learned yet"
                    body="No experiment has produced enough evidence to change what Pathfinder believes. That is a normal state for a research lab, and it is shown rather than filled."
                  />
                ) : (
                  data.learned.map((l, i) => (
                    <Card key={l.id}>
                      <Stack gap={space.md}>
                        <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
                          <Row gap={space.sm}>
                            <Txt variant="metricSm" tone="muted" numeric>
                              {i + 1}
                            </Txt>
                            <LevelPill level={l.level} />
                            <Pill fg={c.textMuted} bordered>
                              {l.confidence}
                            </Pill>
                          </Row>
                          <SampleChip n={l.n} flag={l.sample_flag} />
                        </Row>

                        <RichText variant="heading">{l.statement.headline}</RichText>
                        <RichText variant="body" tone="secondary">
                          {l.statement.body}
                        </RichText>
                        <Attribution line={l.statement} />

                        <Divider />

                        <Stack gap={space.sm}>
                          <Txt variant="caption" tone="muted">
                            {CONFIDENCE_COPY[l.confidence]} {LEVEL_MEANING[l.level]}
                          </Txt>
                          <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                            <Txt variant="caption" tone="muted">
                              Learned {dateShort(l.learned_at)} from
                            </Txt>
                            {l.from_experiments.map((id) => (
                              <Touchable
                                key={id}
                                accessibilityRole="link"
                                onPress={() => router.push(`/pathfinder/experiment/${id}`)}>
                                <Txt variant="caption" color={c.pathfinder} numeric style={{ fontWeight: '700' }}>
                                  {shortExperimentId(id)}
                                </Txt>
                              </Touchable>
                            ))}
                          </Row>
                          {l.applied_in.length > 0 ? (
                            <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                              <Txt variant="caption" tone="muted">
                                Changed
                              </Txt>
                              {l.applied_in.map((v) => (
                                <Txt key={v} variant="caption" tone="secondary" mono numeric>
                                  {v}
                                </Txt>
                              ))}
                            </Row>
                          ) : null}
                        </Stack>
                      </Stack>
                    </Card>
                  ))
                )}
              </Stack>

              <Stack gap={space.md}>
                <Stack gap={space.xs}>
                  <Label>What I am testing next</Label>
                  <Txt variant="title">The questions in the queue</Txt>
                  <Txt variant="small" tone="secondary">
                    Each one says why NOW rather than why eventually — the observer found the
                    condition, not the calendar.
                  </Txt>
                </Stack>

                {data.testing_next.length === 0 ? (
                  <EmptyState
                    title="Nothing queued"
                    body="No condition has come up that justifies a new question yet."
                  />
                ) : (
                  data.testing_next.map((next, i) => (
                    <Card key={next.id} accent={next.blocked_by ? c.caution : undefined}>
                      <Stack gap={space.md}>
                        <Row gap={space.md} style={{ alignItems: 'flex-start' }}>
                          <Txt variant="metricSm" tone="muted" numeric style={{ width: 26 }}>
                            {i + 1}
                          </Txt>
                          <Stack gap={space.sm} style={{ flex: 1 }}>
                            <Txt variant="heading">{next.question}</Txt>
                            <RichText variant="bodyStrong" tone="secondary">
                              {next.why_now.headline}
                            </RichText>
                            <RichText variant="body" tone="secondary">
                              {next.why_now.body}
                            </RichText>
                            <Attribution line={next.why_now} />
                          </Stack>
                        </Row>

                        {next.triggered_by ? <TriggerCard trigger={next.triggered_by} title="What raised it" /> : null}

                        <View style={{ backgroundColor: c.bgSunken, borderRadius: radius.md, padding: space.md, gap: 4 }}>
                          <Label>The deterministic test that will run</Label>
                          <Txt variant="small" tone="secondary">
                            {next.planned_test}
                          </Txt>
                        </View>

                        {next.blocked_by ? (
                          <View style={{ backgroundColor: c.cautionSoft, borderRadius: radius.md, padding: space.md, gap: 4 }}>
                            <Label tone="inherit" style={{ color: c.caution }}>
                              Blocked
                            </Label>
                            <Txt variant="small" color={c.caution}>
                              {next.blocked_by}
                            </Txt>
                          </View>
                        ) : null}

                        {next.queued_experiment_id ? (
                          <Touchable
                            accessibilityRole="link"
                            onPress={() => router.push(`/pathfinder/experiment/${next.queued_experiment_id}`)}
                            style={{
                              alignSelf: 'flex-start',
                              paddingHorizontal: space.lg,
                              paddingVertical: space.sm,
                              borderRadius: radius.pill,
                              borderWidth: 1,
                              borderColor: c.borderStrong,
                            }}>
                            <Txt variant="smallStrong">
                              Open {shortExperimentId(next.queued_experiment_id)}
                            </Txt>
                          </Touchable>
                        ) : null}
                      </Stack>
                    </Card>
                  ))
                )}
              </Stack>

              <Stack gap={space.md}>
                <AsOfStamp
                  asOf={dateTimeIST(data.as_of)}
                  constitutionVersion={data.constitution_version}
                />
                <Disclosure text={data.disclosure} />
              </Stack>
            </Stack>
          </FactsProvider>
        )}
      </Resourced>
    </Page>
  );
}
