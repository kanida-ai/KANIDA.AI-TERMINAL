import { Tabs } from 'expo-router';
import { Platform, View } from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';

import { useLayout } from '@/design/responsive';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { Touchable, Txt } from '@/components/ui';

/**
 * The app chrome from FRONTEND_SPEC: Home · Agents · AutoTrade · Insights ·
 * Profile, active tab tinted.
 *
 * Responsive without a fork: on a phone the bar sits at the bottom in the
 * thumb zone; from the `lg` breakpoint up the same bar moves to the top and
 * becomes a desktop nav rail. One component, one set of tokens.
 */

const GLYPH: Record<string, string> = {
  index: '⌂',
  agents: '◇',
  autotrade: '⇄',
  insights: '◔',
  profile: '◯',
};

const TITLE: Record<string, string> = {
  index: 'Home',
  agents: 'Agents',
  autotrade: 'AutoTrade',
  insights: 'Insights',
  profile: 'Profile',
};

export default function TabsLayout() {
  const { c } = useTheme();
  const layout = useLayout();
  const insets = useSafeAreaInsets();
  const atTop = layout.isWide;

  return (
    <Tabs
      screenOptions={{
        headerShown: false,
        sceneStyle: { backgroundColor: c.bg },
        tabBarPosition: atTop ? 'top' : 'bottom',
      }}
      tabBar={({ state, navigation }) => (
        <View
          style={{
            backgroundColor: c.bg,
            borderTopWidth: atTop ? 0 : 1,
            borderBottomWidth: atTop ? 1 : 0,
            borderColor: c.border,
            paddingTop: atTop ? insets.top + space.sm : space.sm,
            paddingBottom: atTop ? space.sm : insets.bottom + space.sm,
            paddingHorizontal: layout.gutter,
          }}>
          <View
            style={{
              flexDirection: 'row',
              gap: atTop ? space.xs : 0,
              width: '100%',
              maxWidth: atTop ? layout.contentWidth : undefined,
              alignSelf: 'center',
              justifyContent: atTop ? 'flex-start' : 'space-around',
              alignItems: 'center',
            }}>
            {atTop ? (
              <Txt variant="subheading" style={{ marginRight: space.xl, letterSpacing: 0.5 }}>
                KANIDA<Txt variant="subheading" tone="pathfinder">.AI</Txt>
              </Txt>
            ) : null}

            {state.routes.map((route, i) => {
              const focused = state.index === i;
              const tint = focused ? c.pathfinder : c.textMuted;
              return (
                <Touchable
                  key={route.key}
                  accessibilityRole="tab"
                  accessibilityState={{ selected: focused }}
                  accessibilityLabel={TITLE[route.name] ?? route.name}
                  onPress={() => {
                    const event = navigation.emit({
                      type: 'tabPress',
                      target: route.key,
                      canPreventDefault: true,
                    });
                    if (!focused && !event.defaultPrevented) navigation.navigate(route.name);
                  }}
                  style={{
                    flexDirection: atTop ? 'row' : 'column',
                    alignItems: 'center',
                    justifyContent: 'center',
                    gap: atTop ? space.sm : 3,
                    paddingHorizontal: atTop ? space.lg : space.sm,
                    paddingVertical: space.sm,
                    borderRadius: radius.pill,
                    backgroundColor: focused && atTop ? c.pathfinderSoft : 'transparent',
                    minWidth: atTop ? undefined : 56,
                  }}>
                  <Txt
                    color={tint}
                    style={{
                      fontSize: atTop ? 15 : 19,
                      lineHeight: atTop ? 18 : 22,
                      ...Platform.select({ web: { fontFamily: 'inherit' }, default: {} }),
                    }}>
                    {GLYPH[route.name] ?? '•'}
                  </Txt>
                  <Txt variant={atTop ? 'smallStrong' : 'caption'} color={tint}>
                    {TITLE[route.name] ?? route.name}
                  </Txt>
                </Touchable>
              );
            })}
          </View>
        </View>
      )}>
      <Tabs.Screen name="index" options={{ title: 'Home' }} />
      <Tabs.Screen name="agents" options={{ title: 'Agents' }} />
      <Tabs.Screen name="autotrade" options={{ title: 'AutoTrade' }} />
      <Tabs.Screen name="insights" options={{ title: 'Insights' }} />
      <Tabs.Screen name="profile" options={{ title: 'Profile' }} />
    </Tabs>
  );
}
