import { useRouter } from 'expo-router';
import { useCallback } from 'react';
import { View } from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';

import { api } from '@/api/client';
import type { FeedResponse } from '@/api/types';
import { useResource } from '@/api/useResource';
import { ActiveBadge, AsOfStamp, PathfinderAvatar } from '@/components/agent';
import { RecordChip } from '@/components/chips';
import { FactsProvider, RichText } from '@/components/facts';
import { Page } from '@/components/Page';
import { Resourced, SkeletonCard } from '@/components/states';
import { Card, Label, Row, Stack, Touchable, Txt } from '@/components/ui';
import { useLayout } from '@/design/responsive';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { dateShort, dateTimeIST } from '@/lib/format';
import { findingsOf } from '@/lib/stories';

/**
 * Home. Only Pathfinder is built in this slice, so only Pathfinder shows
 * numbers. Trader and Investor render as explicitly unbuilt rather than as
 * plausible-looking cards -- a placeholder that looks like data is a lie with
 * good typography.
 */
export default function HomeScreen() {
  const insets = useSafeAreaInsets();
  const layout = useLayout();
  const feed = useResource((signal) => api.feed(null, signal), []);

  const skeleton = useCallback(
    () => (
      <Stack gap={space.lg}>
        <SkeletonCard lines={2} />
        <SkeletonCard lines={4} />
      </Stack>
    ),
    [],
  );

  return (
    <Page
      refreshing={feed.refreshing}
      onRefresh={feed.refresh}
      bottomInset={layout.isWide ? 0 : 72}
      contentStyle={{ paddingTop: layout.isWide ? space.xxl : insets.top + space.lg }}>
      <Stack gap={space.xs}>
        <Label>Kanida research</Label>
        <Txt variant="display">Three managers.{'\n'}One honest record.</Txt>
        <Txt variant="body" tone="secondary">
          Every number below is computed by a deterministic engine, point-in-time, after costs. No
          returns are promised.
        </Txt>
      </Stack>

      <Resourced data={feed.data} error={feed.error} loading={feed.loading} onRetry={feed.refresh} skeleton={skeleton()}>
        {(data) => <PathfinderHomeCard feed={data} />}
      </Resourced>

      <UnbuiltAgentCard
        name="Trader"
        mandate="1–3 day holds · Falcon rulebook"
        reason="Not in this slice. It will use the same frame, contract and honesty rules as Pathfinder."
        accentKey="trader"
      />
      <UnbuiltAgentCard
        name="Investor"
        mandate="3–12 month holds · 10–15 names · monthly rebalance"
        reason="Not in this slice. It will use the same frame, contract and honesty rules as Pathfinder."
        accentKey="investor"
      />
    </Page>
  );
}

function PathfinderHomeCard({ feed }: { feed: FeedResponse }) {
  const { c } = useTheme();
  const router = useRouter();
  const findings = findingsOf(feed);
  const first = feed.what_matters_now[0] ?? feed.discoveries[0] ?? null;
  const sb = feed.scoreboard;

  return (
    <Touchable
      accessibilityRole="link"
      accessibilityLabel="Open the Pathfinder feed"
      onPress={() => router.push('/pathfinder')}>
      <Card accent={c.pathfinder}>
        <Stack gap={space.lg}>
          <Row style={{ justifyContent: 'space-between', gap: space.md }}>
            <Row gap={space.md} style={{ flex: 1 }}>
              <PathfinderAvatar size={44} />
              <Stack gap={2} style={{ flex: 1 }}>
                <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                  <Txt variant="subheading">Pathfinder</Txt>
                  <ActiveBadge />
                </Row>
                <Txt variant="small" tone="secondary">
                  What matters now, in thirty seconds · then depth
                </Txt>
              </Stack>
            </Row>
            <Txt variant="heading" tone="muted">
              ›
            </Txt>
          </Row>

          <View style={{ backgroundColor: c.bgSunken, borderRadius: radius.md, padding: space.md, gap: 4 }}>
            <Row style={{ justifyContent: 'space-between', gap: space.sm, flexWrap: 'wrap' }}>
              <Label>Edition {dateShort(feed.edition_date)}</Label>
              <RecordChip backfilled={feed.backfilled} />
            </Row>
            {first ? (
              <FactsProvider facts={first.facts}>
                <RichText variant="bodyStrong" numberOfLines={3}>
                  {first.narrative.headline}
                </RichText>
              </FactsProvider>
            ) : (
              <Txt variant="bodyStrong">Nothing cleared the usefulness threshold on this close — a valid edition.</Txt>
            )}
          </View>

          <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
            <HomeStat label="Stories today" value={findings.length} tone={c.text} />
            <HomeStat label="Right · Wrong · Incl." value={`${sb.right} · ${sb.wrong} · ${sb.inconclusive}`} tone={c.text} />
            <HomeStat label={`n = ${sb.n} · forward ${sb.forward.n}`} value={feed.experiments_scoreboard ? `${feed.experiments_scoreboard.experiments_testing} testing` : '—'} tone={c.textSecondary} />
          </Row>

          <Txt variant="caption" color={feed.backfilled ? c.caution : c.positive}>
            {feed.record_label}
          </Txt>

          <AsOfStamp asOf={dateTimeIST(feed.generated_at)} />
        </Stack>
      </Card>
    </Touchable>
  );
}

function HomeStat({ label, value, tone }: { label: string; value: number | string; tone: string }) {
  return (
    <Stack gap={2} style={{ flexGrow: 1, flexBasis: 0, minWidth: 96 }}>
      <Txt variant="metricSm" numeric color={tone}>
        {value}
      </Txt>
      <Txt variant="caption" tone="muted">
        {label}
      </Txt>
    </Stack>
  );
}

function UnbuiltAgentCard({
  name,
  mandate,
  reason,
  accentKey,
}: {
  name: string;
  mandate: string;
  reason: string;
  accentKey: 'trader' | 'investor';
}) {
  const { c } = useTheme();
  return (
    <Card style={{ opacity: 0.72 }}>
      <Stack gap={space.sm}>
        <Row gap={space.md}>
          <View
            style={{
              width: 44,
              height: 44,
              borderRadius: radius.md,
              borderWidth: 1,
              borderColor: c.border,
              alignItems: 'center',
              justifyContent: 'center',
            }}>
            <Txt color={c[accentKey]} style={{ fontSize: 20 }}>
              ◇
            </Txt>
          </View>
          <Stack gap={2} style={{ flex: 1 }}>
            <Txt variant="subheading" tone="secondary">
              {name}
            </Txt>
            <Txt variant="small" tone="muted">
              {mandate}
            </Txt>
          </Stack>
          <Txt variant="label" tone="muted">
            Not built
          </Txt>
        </Row>
        <Txt variant="caption" tone="muted">
          {reason}
        </Txt>
      </Stack>
    </Card>
  );
}
