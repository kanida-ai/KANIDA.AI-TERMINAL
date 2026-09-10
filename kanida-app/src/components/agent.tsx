/**
 * The shared agent-screen chrome from FRONTEND_SPEC:
 *
 *   back arrow · agent avatar · name · green Active badge · one-line mandate
 *   ------------------------------------------------------------------------
 *   sub-tabs
 *
 * Trader and Investor will reuse this exact frame; Pathfinder is the first
 * build, so the component is written agent-agnostic from the start.
 */
import { Link, usePathname, useRouter } from 'expo-router';
import type { ComponentProps } from 'react';
import { ScrollView, View } from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';

import { useTheme } from '@/design/theme';
import { useLayout } from '@/design/responsive';
import { radius, space } from '@/design/tokens';

import { Row, Stack, Touchable, Txt } from './ui';

type Href = ComponentProps<typeof Link>['href'];

/** The violet flask. Drawn, not imported -- no asset pipeline for one glyph. */
export function PathfinderAvatar({ size = 40 }: { size?: number }) {
  const { c } = useTheme();
  return (
    <View
      accessibilityLabel="Pathfinder"
      style={{
        width: size,
        height: size,
        borderRadius: radius.md,
        backgroundColor: c.pathfinderSoft,
        borderWidth: 1,
        borderColor: c.pathfinder,
        alignItems: 'center',
        justifyContent: 'center',
      }}>
      <Txt style={{ fontSize: size * 0.5, lineHeight: size * 0.62 }} color={c.pathfinder}>
        ⚗
      </Txt>
    </View>
  );
}

export function ActiveBadge() {
  const { c } = useTheme();
  return (
    <Row gap={5}>
      <View style={{ width: 6, height: 6, borderRadius: 3, backgroundColor: c.positive }} />
      <Txt variant="caption" color={c.positive} style={{ fontWeight: '700' }}>
        Active
      </Txt>
    </Row>
  );
}

export function AgentHeader({
  name,
  mandate,
  onBack,
}: {
  name: string;
  mandate: string;
  onBack?: () => void;
}) {
  const { c } = useTheme();
  const router = useRouter();
  const insets = useSafeAreaInsets();
  const layout = useLayout();

  return (
    <View
      style={{
        backgroundColor: c.bg,
        paddingTop: insets.top + space.sm,
        paddingHorizontal: layout.gutter,
        paddingBottom: space.md,
        borderBottomWidth: 1,
        borderBottomColor: c.border,
      }}>
      <View style={{ width: '100%', maxWidth: layout.contentWidth, alignSelf: 'center' }}>
        <Row gap={space.md} style={{ alignItems: 'flex-start' }}>
          <Touchable
            accessibilityRole="button"
            accessibilityLabel="Back"
            onPress={() => (onBack ? onBack() : router.canGoBack() ? router.back() : router.replace('/'))}
            style={{ paddingVertical: 2, paddingRight: space.xs }}>
            <Txt variant="title" tone="secondary">
              ‹
            </Txt>
          </Touchable>

          <PathfinderAvatar />

          <Stack gap={2} style={{ flex: 1 }}>
            <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
              <Txt variant="heading">{name}</Txt>
              <ActiveBadge />
            </Row>
            <Txt variant="small" tone="secondary">
              {mandate}
            </Txt>
          </Stack>
        </Row>
      </View>
    </View>
  );
}

export type SubTab = { key: string; label: string; href: Href };

/** Horizontally scrollable sub-tabs. Real routes, so every tab is deep-linkable. */
export function SubTabs({ tabs }: { tabs: SubTab[] }) {
  const { c } = useTheme();
  const pathname = usePathname();
  const layout = useLayout();

  return (
    <View style={{ backgroundColor: c.bg, borderBottomWidth: 1, borderBottomColor: c.border }}>
      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        contentContainerStyle={{
          paddingHorizontal: layout.gutter,
          gap: space.xs,
          paddingVertical: space.sm,
          width: '100%',
          maxWidth: layout.contentWidth + layout.gutter * 2,
          alignSelf: 'center',
        }}>
        {tabs.map((t) => {
          const active = pathname.endsWith(`/${t.key}`);
          return (
            <Link key={t.key} href={t.href} asChild>
              <Touchable
                accessibilityRole="tab"
                accessibilityState={{ selected: active }}
                style={{
                  paddingHorizontal: space.lg,
                  paddingVertical: space.sm,
                  borderRadius: radius.pill,
                  backgroundColor: active ? c.pathfinderSoft : 'transparent',
                }}>
                <Txt
                  variant="smallStrong"
                  color={active ? c.pathfinder : c.textSecondary}>
                  {t.label}
                </Txt>
              </Touchable>
            </Link>
          );
        })}
      </ScrollView>
    </View>
  );
}

/**
 * The research disclosure, verbatim from the engine payload. It is rendered on
 * every Pathfinder surface and is never truncated or restyled into fine print.
 */
export function Disclosure({ text }: { text: string }) {
  const { c } = useTheme();
  return (
    <View
      style={{
        borderRadius: radius.md,
        borderWidth: 1,
        borderColor: c.border,
        padding: space.lg,
        gap: space.xs,
      }}>
      <Txt variant="label" tone="muted">
        Research disclosure
      </Txt>
      <Txt variant="small" tone="secondary">
        {text}
      </Txt>
    </View>
  );
}

/** The footer stamp: what this page is as of, and which Constitution governs it. */
export function AsOfStamp({
  asOf,
  constitutionVersion,
}: {
  asOf: string;
  constitutionVersion?: string;
}) {
  return (
    <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
      <Txt variant="caption" tone="muted" numeric>
        As of {asOf}
      </Txt>
      {constitutionVersion ? (
        <Txt variant="caption" tone="muted" mono numeric>
          {constitutionVersion}
        </Txt>
      ) : null}
    </Row>
  );
}
