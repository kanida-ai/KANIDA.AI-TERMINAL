import { useRouter } from 'expo-router';
import { View } from 'react-native';

import { api } from '@/api/client';
import { useResource } from '@/api/useResource';
import { AsOfStamp, Disclosure } from '@/components/agent';
import { LlmUsageCard } from '@/components/Evidence';
import { ExperimentCard } from '@/components/ExperimentCard';
import { FactsProvider, RichText } from '@/components/facts';
import { Page } from '@/components/Page';
import { PipelineBar } from '@/components/Pipeline';
import { Resourced, SkeletonCard } from '@/components/states';
import { StoryThread, TriggerCard } from '@/components/Story';
import { Card, Label, Row, Stack, Touchable, Txt } from '@/components/ui';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { dateTimeIST, humanise } from '@/lib/format';

/**
 * OVERVIEW — today's activity as a STORY.
 *
 * The order on this page is the UX law from docs/sessions/PATHFINDER.md: the
 * loop narrative first, at full width and full weight; the counters and the
 * pipeline underneath it, supporting. A dashboard would put the numbers on top.
 * This is not a dashboard.
 */
export default function OverviewScreen() {
  const { c } = useTheme();
  const router = useRouter();

  const loop = useResource((s) => api.loop(s), []);
  const list = useResource((s) => api.experiments(null, s), []);
  const learnings = useResource((s) => api.learnings(s), []);

  const refreshing = loop.refreshing || list.refreshing || learnings.refreshing;
  const refreshAll = () => {
    loop.refresh();
    list.refresh();
    learnings.refresh();
  };

  return (
    <Page refreshing={refreshing} onRefresh={refreshAll} bottomInset={72} contentStyle={{ paddingTop: space.xl }}>
      <Resourced
        data={loop.data}
        error={loop.error}
        loading={loop.loading}
        onRetry={loop.refresh}
        skeleton={
          <Stack gap={space.lg}>
            <SkeletonCard lines={2} />
            <SkeletonCard lines={4} />
            <SkeletonCard lines={4} />
          </Stack>
        }>
        {(data) => (
          <FactsProvider facts={data.facts}>
            <Stack gap={space.xl}>
              {/* Today's activity — three counters, honestly labelled. */}
              <Card>
                <Stack gap={space.lg}>
                  <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
                    <Label>Today{'’'}s activity</Label>
                    <Txt variant="caption" tone="muted">
                      Cycle {data.cycle_id} · stage: {humanise(data.stage)}
                    </Txt>
                  </Row>
                  <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
                    <Counter
                      label="Experiments running"
                      value={
                        (data.counts.testing ?? 0) +
                        (data.counts.validating ?? 0) +
                        (data.counts.promising ?? 0)
                      }
                    />
                    <Counter label="Promising" value={data.counts.promising ?? 0} tone={c.positive} />
                    <Counter label="Rejected · published" value={data.counts.died ?? 0} tone={c.negative} />
                  </Row>
                  <View style={{ backgroundColor: c.bgSunken, borderRadius: radius.md, padding: space.md }}>
                    <Txt variant="caption" tone="muted">
                      There is no blended {'“'}average virtual return{'”'} here. The engine does not
                      compute one, and averaging books with different rules, horizons and sample
                      sizes in the app would be a number Kanida invented rather than measured. Each
                      experiment carries its own expectancy, drawdown and n below.
                    </Txt>
                  </View>
                </Stack>
              </Card>

              {/* What woke the model. */}
              {data.trigger ? <TriggerCard trigger={data.trigger} /> : null}

              {/* THE LOOP — the reason this screen exists. */}
              <Stack gap={space.md}>
                <Stack gap={space.xs}>
                  <Label>The loop, right now</Label>
                  <Txt variant="title">What I am doing and why</Txt>
                </Stack>
                <StoryThread story={data.story} />
              </Stack>

              <PipelineBar counts={data.counts} />
            </Stack>
          </FactsProvider>
        )}
      </Resourced>

      {/* Today's experiments — preview, losers first as the engine ordered them. */}
      <Resourced
        data={list.data}
        error={list.error}
        loading={list.loading}
        onRetry={list.refresh}
        skeleton={<SkeletonCard lines={4} />}>
        {(data) => (
          <Stack gap={space.md}>
            <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
              <Stack gap={space.xs}>
                <Label>Today{'’'}s experiments</Label>
                <Txt variant="title">Losers first</Txt>
              </Stack>
              <Touchable
                accessibilityRole="link"
                onPress={() => router.push('/pathfinder/experiments')}
                style={{
                  paddingHorizontal: space.lg,
                  paddingVertical: space.sm,
                  borderRadius: radius.pill,
                  borderWidth: 1,
                  borderColor: c.borderStrong,
                }}>
                <Txt variant="smallStrong">View all {data.count}</Txt>
              </Touchable>
            </Row>
            {data.items.slice(0, 3).map((item) => (
              <ExperimentCard key={item.id} item={item} />
            ))}
          </Stack>
        )}
      </Resourced>

      {/* What I learned today + what I'm testing tomorrow. */}
      <Resourced
        data={learnings.data}
        error={learnings.error}
        loading={learnings.loading}
        onRetry={learnings.refresh}
        skeleton={<SkeletonCard lines={3} />}>
        {(data) => (
          <FactsProvider facts={data.facts}>
            <Stack gap={space.xl}>
              {data.learned.length > 0 ? (
                <Card accent={c.positive}>
                  <Stack gap={space.md}>
                    <Row gap={space.sm}>
                      <Txt style={{ fontSize: 16 }}>💡</Txt>
                      <Label tone="inherit" style={{ color: c.positive }}>
                        What I learned
                      </Label>
                    </Row>
                    <RichText variant="heading">{data.learned[0].statement.headline}</RichText>
                    <RichText variant="body" tone="secondary">
                      {data.learned[0].statement.body}
                    </RichText>
                    <Txt variant="caption" tone="muted">
                      Level {data.learned[0].level} · {data.learned[0].confidence} · from{' '}
                      {data.learned[0].from_experiments.join(', ')}
                    </Txt>
                  </Stack>
                </Card>
              ) : null}

              {data.testing_next.length > 0 ? (
                <Card>
                  <Stack gap={space.lg}>
                    <Label>What I am testing next</Label>
                    {data.testing_next.map((next, i) => (
                      <Row key={next.id} gap={space.md} style={{ alignItems: 'flex-start' }}>
                        <Txt variant="metricSm" tone="muted" numeric style={{ width: 26 }}>
                          {i + 1}
                        </Txt>
                        <Stack gap={4} style={{ flex: 1 }}>
                          <Txt variant="bodyStrong">{next.question}</Txt>
                          <RichText variant="small" tone="secondary">
                            {next.why_now.body}
                          </RichText>
                          {next.blocked_by ? (
                            <Txt variant="caption" tone="caution">
                              Blocked: {next.blocked_by}
                            </Txt>
                          ) : null}
                        </Stack>
                      </Row>
                    ))}
                  </Stack>
                </Card>
              ) : null}
            </Stack>
          </FactsProvider>
        )}
      </Resourced>

      {loop.data?.llm_usage ? <LlmUsageCard usage={loop.data.llm_usage} /> : null}

      {loop.data ? (
        <Stack gap={space.md}>
          <AsOfStamp
            asOf={dateTimeIST(loop.data.as_of)}
            constitutionVersion={loop.data.constitution_version}
          />
          <Disclosure text={loop.data.disclosure} />
        </Stack>
      ) : null}
    </Page>
  );
}

function Counter({ label, value, tone }: { label: string; value: number; tone?: string }) {
  return (
    <Stack gap={2} style={{ flexGrow: 1, flexBasis: 0, minWidth: 78 }}>
      <Txt variant="metric" numeric color={tone}>
        {value}
      </Txt>
      <Txt variant="caption" tone="muted">
        {label}
      </Txt>
    </Stack>
  );
}
