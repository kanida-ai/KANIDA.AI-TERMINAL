import { useRouter } from 'expo-router';
import { useCallback } from 'react';
import { View } from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';

import { api } from '@/api/client';
import { useResource } from '@/api/useResource';
import { AsOfStamp, PathfinderAvatar, ActiveBadge } from '@/components/agent';
import { FactsProvider, RichText } from '@/components/facts';
import { Page } from '@/components/Page';
import { Resourced, SkeletonCard } from '@/components/states';
import { Card, Label, Row, Stack, Touchable, Txt } from '@/components/ui';
import { useLayout } from '@/design/responsive';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { dateTimeIST } from '@/lib/format';

/**
 * Home. In this slice only Pathfinder is built, so only Pathfinder shows
 * numbers. Trader and Investor render as explicitly unbuilt rather than as
 * plausible-looking cards -- a placeholder that looks like data is a lie with
 * good typography.
 */
export default function HomeScreen() {
  const insets = useSafeAreaInsets();
  const layout = useLayout();
  const loop = useResource((signal) => api.loop(signal), []);

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
      refreshing={loop.refreshing}
      onRefresh={loop.refresh}
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

      <Resourced
        data={loop.data}
        error={loop.error}
        loading={loop.loading}
        onRetry={loop.refresh}
        skeleton={skeleton()}>
        {(data) => (
          <FactsProvider facts={data.facts}>
            <PathfinderHomeCard
              counts={data.counts}
              headline={data.story[0]?.headline ?? ''}
              asOf={data.as_of}
            />
          </FactsProvider>
        )}
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

function PathfinderHomeCard({
  counts,
  headline,
  asOf,
}: {
  counts: Partial<Record<string, number>>;
  headline: string;
  asOf: string;
}) {
  const { c } = useTheme();
  const router = useRouter();

  const live = (counts.testing ?? 0) + (counts.validating ?? 0) + (counts.promising ?? 0);
  const promising = counts.promising ?? 0;
  const rejected = counts.died ?? 0;

  return (
    <Touchable
      accessibilityRole="link"
      accessibilityLabel="Open the Pathfinder agent"
      onPress={() => router.push('/pathfinder/overview')}>
      <Card accent={c.pathfinder}>
        <Stack gap={space.lg}>
          <Row style={{ justifyContent: 'space-between', gap: space.md }}>
            <Row gap={space.md} style={{ flex: 1 }}>
              <PathfinderAvatar size={44} />
              <Stack gap={2} style={{ flex: 1 }}>
                <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                  <Txt variant="subheading">Pathfinder Agent</Txt>
                  <ActiveBadge />
                </Row>
                <Txt variant="small" tone="secondary">
                  Discover repeatable market edges
                </Txt>
              </Stack>
            </Row>
            <Txt variant="heading" tone="muted">
              ›
            </Txt>
          </Row>

          {headline ? (
            <View style={{ backgroundColor: c.bgSunken, borderRadius: radius.md, padding: space.md }}>
              <Label>Right now</Label>
              <RichText variant="bodyStrong" style={{ marginTop: 4 }}>
                {headline}
              </RichText>
            </View>
          ) : null}

          <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
            <HomeStat label="Live experiments" value={live} tone={c.text} />
            <HomeStat label="Promising" value={promising} tone={promising > 0 ? c.positive : c.text} />
            <HomeStat label="Rejected · published" value={rejected} tone={c.negative} />
          </Row>

          <Txt variant="caption" tone="muted">
            No blended return is shown across experiments. Averaging books with different rules,
            horizons and sample sizes would be a number this app invented rather than measured — each
            experiment carries its own.
          </Txt>

          <AsOfStamp asOf={dateTimeIST(asOf)} />
        </Stack>
      </Card>
    </Touchable>
  );
}

function HomeStat({ label, value, tone }: { label: string; value: number; tone: string }) {
  return (
    <Stack gap={2} style={{ flexGrow: 1, flexBasis: 0, minWidth: 78 }}>
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
