import { useRouter } from 'expo-router';
import { View } from 'react-native';

import { api } from '@/api/client';
import type { ExperimentsResponse, RejectedCandidate } from '@/api/types';
import { useResource } from '@/api/useResource';
import { AgentHeader, Disclosure } from '@/components/agent';
import { RecordLine, StatePill } from '@/components/chips';
import { FactsProvider, RichText } from '@/components/facts';
import { ClosestCandidate, ScoreInline } from '@/components/feed/StorySlide';
import { Page } from '@/components/Page';
import { Resourced, SkeletonCard } from '@/components/states';
import { Card, Divider, Label, Pill, Row, Stack, Touchable, Txt } from '@/components/ui';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { count, dateShort } from '@/lib/format';
import { BEAT_LABEL, comparisonCopy, publicSafe, toneColor } from '@/lib/honesty';

/**
 * THE REGISTRY — every experiment (losers first: buried, then testing, proposed
 * last), then every finding the gate did NOT open, grouped by the reason on the
 * record, with the researched ones first and their closest variant shown.
 *
 * When nothing has opened, this is the page that says why, in full.
 */
export default function ExperimentsScreen() {
  const router = useRouter();
  const res = useResource((s) => api.experiments(s), []);

  return (
    <View style={{ flex: 1 }}>
      <AgentHeader
        name="Pathfinder"
        mandate="The experiment registry — and what it declined"
        onBack={() => (router.canGoBack() ? router.back() : router.replace('/pathfinder'))}
      />
      <Page refreshing={res.refreshing} onRefresh={res.refresh} contentStyle={{ paddingTop: space.xl }}>
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
          {(data) => <Registry data={data} />}
        </Resourced>
      </Page>
    </View>
  );
}

function Registry({ data }: { data: ExperimentsResponse }) {
  const { c } = useTheme();
  const router = useRouter();
  const sb = data.scoreboard;
  const researched = data.not_opened
    .filter((r) => r.trials_evaluated > 0)
    .sort((a, b) => a.best_failed_gates.length - b.best_failed_gates.length || b.edition_date.localeCompare(a.edition_date));
  const declined = groupByReason(data.not_opened.filter((r) => r.trials_evaluated === 0));

  return (
    <Stack gap={space.xxl}>
      <Stack gap={space.md}>
        <Txt variant="label" tone="pathfinder">
          Registry as of {dateShort(data.as_of)}
        </Txt>
        <Txt variant="title">
          {data.count === 0 ? 'No experiment is open' : `${count(data.count)} experiment${data.count === 1 ? '' : 's'}, losers first`}
        </Txt>
        <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
          <ScoreInline counts={sb} voidCount={sb.void} />
        </Row>
        <Txt variant="caption" tone="muted" numeric>
          testing {sb.experiments_testing} · buried {sb.experiments_buried} · proposed {sb.experiments_proposed} · not opened {sb.candidates_not_opened} ·{' '}
          {count(sb.trials_total)} trials counted · engine {data.engine_version}
        </Txt>
        <RecordLine label={sb.record_label} backfilled={sb.forward.n === 0} />
      </Stack>

      {data.items.length > 0 ? (
        <Stack gap={space.md}>
          {data.items.map((card) => {
            const cmp = comparisonCopy[card.latest_comparison];
            return (
              <FactsProvider key={card.id} facts={card.facts}>
                <Touchable
                  accessibilityRole="link"
                  accessibilityLabel={`Open experiment ${card.id}`}
                  onPress={() => router.push({ pathname: '/pathfinder/experiment/[id]', params: { id: card.id } })}>
                  <Card accent={card.state === 'buried' ? c.negative : card.state === 'proposed' ? c.positive : c.pathfinder}>
                    <Stack gap={space.md}>
                      <Row style={{ justifyContent: 'space-between', gap: space.sm, flexWrap: 'wrap' }}>
                        <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                          <StatePill state={card.state} />
                          <Txt variant="caption" tone="muted">
                            {card.family.replace(/_/g, ' ')}
                          </Txt>
                        </Row>
                        <Txt variant="heading" tone="muted">
                          ›
                        </Txt>
                      </Row>
                      <Txt variant="subheading">{publicSafe(card.theme)}</Txt>
                      <Stack gap={4}>
                        {card.story.map((line) => (
                          <Row key={line.beat} gap={space.sm} style={{ alignItems: 'flex-start' }}>
                            <Txt variant="caption" color={c.pathfinder} style={{ width: 96 }} numberOfLines={2}>
                              {BEAT_LABEL[line.beat]}
                            </Txt>
                            <RichText variant="small" style={{ flex: 1 }} numberOfLines={2}>
                              {publicSafe(line.headline)}
                            </RichText>
                          </Row>
                        ))}
                      </Stack>
                      <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                        <ScoreInline counts={card.score} voidCount={card.score.void} />
                        <Pill fg={toneColor(cmp.tone, c)} bordered>
                          {cmp.label}
                        </Pill>
                      </Row>
                      <Txt variant="caption" tone="muted" numeric>
                        v{card.versions_count} · {count(card.trials_total)} trials · {count(card.periods_graded)} periods graded · opened {dateShort(card.opened_edition)}
                      </Txt>
                      <RecordLine label={card.record_label} backfilled={card.backfilled} />
                    </Stack>
                  </Card>
                </Touchable>
              </FactsProvider>
            );
          })}
        </Stack>
      ) : null}

      <Stack gap={space.md}>
        <Stack gap={space.xs}>
          <Label tone="inherit" style={{ color: c.pathfinder }}>
            Researched at the gate, and declined
          </Label>
          <Txt variant="title">
            {researched.length === 0 ? 'Nothing was researched yet' : `${count(researched.length)} finding${researched.length === 1 ? '' : 's'} researched, none cleared`}
          </Txt>
          <Txt variant="small" tone="secondary">
            Each ran the closed set of variants — every one a counted trial. The closest variant and the one gate it failed are on the record.
          </Txt>
        </Stack>
        {researched.map((r) => (
          <ClosestCandidate key={r.finding_id} r={r} engine={data.engine_version} asOf={data.as_of} />
        ))}
      </Stack>

      <Stack gap={space.md}>
        <Stack gap={space.xs}>
          <Label tone="inherit" style={{ color: c.pathfinder }}>
            Declined before any variant ran
          </Label>
          <Txt variant="title">The reasons, with counts</Txt>
        </Stack>
        <Card>
          <Stack gap={space.md}>
            {declined.map((g, i) => (
              <View key={g.reason} style={{ gap: space.xs, paddingTop: i === 0 ? 0 : space.md, borderTopWidth: i === 0 ? 0 : 1, borderTopColor: c.border }}>
                <Row style={{ justifyContent: 'space-between', gap: space.md }}>
                  <Txt variant="small" style={{ flex: 1 }}>
                    {g.reason}
                  </Txt>
                  <Txt variant="metricSm" numeric>
                    {count(g.items.length)}
                  </Txt>
                </Row>
                <Txt variant="caption" tone="muted" numberOfLines={2}>
                  {summariseTemplates(g.items)}
                </Txt>
              </View>
            ))}
            {declined.length === 0 ? (
              <Txt variant="small" tone="greyed">
                Nothing declined without research.
              </Txt>
            ) : null}
          </Stack>
        </Card>
      </Stack>

      <Divider />
      <Stack gap={space.md}>
        <Touchable
          accessibilityRole="button"
          onPress={() => router.push('/pathfinder/scoreboard')}
          style={{ alignSelf: 'flex-start', paddingHorizontal: space.lg, paddingVertical: space.sm, borderRadius: radius.pill, borderWidth: 1, borderColor: c.borderStrong }}>
          <Txt variant="smallStrong">The scoreboard ›</Txt>
        </Touchable>
        <Disclosure text={data.disclosure} />
      </Stack>
    </Stack>
  );
}

/** Group by the engine's reason, stripped of the per-finding tail so the count is meaningful. */
function groupByReason(rows: RejectedCandidate[]): { reason: string; items: RejectedCandidate[] }[] {
  const groups = new Map<string, RejectedCandidate[]>();
  for (const r of rows) {
    const key = r.reason.replace(/\s+on \d{4}-\d{2}-\d{2}/, '').replace(/—.*$/, '').trim();
    const arr = groups.get(key) ?? [];
    arr.push(r);
    groups.set(key, arr);
  }
  return [...groups.entries()].map(([reason, items]) => ({ reason, items })).sort((a, b) => b.items.length - a.items.length);
}

function summariseTemplates(items: RejectedCandidate[]): string {
  const counts = new Map<string, number>();
  for (const r of items) counts.set(r.template_id, (counts.get(r.template_id) ?? 0) + 1);
  return [...counts.entries()].map(([k, v]) => `${k.replace(/_/g, ' ')} ${v}`).join(' · ');
}
