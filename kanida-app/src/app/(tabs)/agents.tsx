import { useRouter } from 'expo-router';
import { View } from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';

import { ActiveBadge, PathfinderAvatar } from '@/components/agent';
import { Page } from '@/components/Page';
import { Card, Label, Row, Stack, Touchable, Txt } from '@/components/ui';
import { useLayout } from '@/design/responsive';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';

/** The agent directory. One built agent, two declared-but-unbuilt. */
export default function AgentsScreen() {
  const { c } = useTheme();
  const router = useRouter();
  const insets = useSafeAreaInsets();
  const layout = useLayout();

  return (
    <Page
      bottomInset={layout.isWide ? 0 : 72}
      contentStyle={{ paddingTop: layout.isWide ? space.xxl : insets.top + space.lg }}>
      <Stack gap={space.xs}>
        <Label>Agents</Label>
        <Txt variant="title">Three managers, each with a rulebook</Txt>
      </Stack>

      <Touchable
        accessibilityRole="link"
        accessibilityLabel="Open the Pathfinder agent"
        onPress={() => router.push('/pathfinder')}>
        <Card accent={c.pathfinder}>
          <Row gap={space.md}>
            <PathfinderAvatar size={44} />
            <Stack gap={3} style={{ flex: 1 }}>
              <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                <Txt variant="subheading">Pathfinder Agent</Txt>
                <ActiveBadge />
              </Row>
              <Txt variant="small" tone="secondary">
                Discover repeatable market edges · Test → Validate → Evolve · the swipeable feed
              </Txt>
            </Stack>
            <Txt variant="heading" tone="muted">
              ›
            </Txt>
          </Row>
        </Card>
      </Touchable>

      {[
        { name: 'Trader', mandate: '1–3 day holds · Falcon rulebook', key: 'trader' as const },
        {
          name: 'Investor',
          mandate: '3–12 month holds · 10–15 names · monthly rebalance',
          key: 'investor' as const,
        },
      ].map((a) => (
        <Card key={a.name} style={{ opacity: 0.72 }}>
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
              <Txt color={c[a.key]} style={{ fontSize: 20 }}>
                ◇
              </Txt>
            </View>
            <Stack gap={3} style={{ flex: 1 }}>
              <Txt variant="subheading" tone="secondary">
                {a.name}
              </Txt>
              <Txt variant="small" tone="muted">
                {a.mandate}
              </Txt>
            </Stack>
            <Txt variant="label" tone="muted">
              Not built
            </Txt>
          </Row>
        </Card>
      ))}
    </Page>
  );
}
