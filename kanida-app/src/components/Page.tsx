/**
 * The page shell: safe areas, the responsive reading column, pull-to-refresh.
 *
 * Phone  -> one column, bottom tab bar, 16px gutter.
 * Tablet -> 940px column, 2-up grids.
 * Desktop-> 1180px column, top nav rail, story + supporting columns.
 *
 * Same components, same tokens; only the frame changes.
 */
import type { ReactNode } from 'react';
import { RefreshControl, ScrollView, View, type StyleProp, type ViewStyle } from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';

import { useTheme } from '@/design/theme';
import { useLayout } from '@/design/responsive';
import { space } from '@/design/tokens';

export function Page({
  children,
  refreshing = false,
  onRefresh,
  /** extra bottom padding so content clears the tab bar */
  bottomInset = 0,
  contentStyle,
}: {
  children: ReactNode;
  refreshing?: boolean;
  onRefresh?: () => void;
  bottomInset?: number;
  contentStyle?: StyleProp<ViewStyle>;
}) {
  const { c } = useTheme();
  const layout = useLayout();
  const insets = useSafeAreaInsets();

  return (
    <ScrollView
      style={{ flex: 1, backgroundColor: c.bg }}
      contentContainerStyle={{
        paddingHorizontal: layout.gutter,
        paddingBottom: insets.bottom + bottomInset + space.huge,
        alignItems: 'center',
      }}
      refreshControl={
        onRefresh ? (
          <RefreshControl refreshing={refreshing} onRefresh={onRefresh} tintColor={c.pathfinder} />
        ) : undefined
      }
      keyboardShouldPersistTaps="handled">
      <View style={[{ width: '100%', maxWidth: layout.contentWidth, gap: space.lg }, contentStyle]}>
        {children}
      </View>
    </ScrollView>
  );
}

/**
 * A responsive grid. On a phone it is a plain stack; from `md` up it wraps into
 * `layout.columns` columns without any per-screen breakpoint code.
 */
export function Grid({
  children,
  gap = space.lg,
  minWidth = 280,
}: {
  children: ReactNode[];
  gap?: number;
  minWidth?: number;
}) {
  const layout = useLayout();
  const cols = layout.width < minWidth * 2 + gap ? 1 : layout.columns;
  if (cols === 1) return <View style={{ gap }}>{children}</View>;
  return (
    <View style={{ flexDirection: 'row', flexWrap: 'wrap', gap }}>
      {children.map((child, i) => (
        <View key={i} style={{ flexGrow: 1, flexBasis: `${100 / cols - 1}%`, minWidth }}>
          {child}
        </View>
      ))}
    </View>
  );
}
