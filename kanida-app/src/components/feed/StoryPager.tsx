/**
 * The vertical story pager -- one story per screen, thumb-swipe up for the next.
 *
 * Plain React Native `ScrollView` with `pagingEnabled`, which react-native-web
 * renders as CSS scroll-snap (`scroll-snap-type: y mandatory`) and native renders
 * as real paging. Every story is a direct child sized to the viewport, so the
 * same file serves iOS, Android, desktop web and mobile web with no fork.
 *
 * Chrome (top): the Instagram-style segmented progress rail -- one segment per
 * story, filled up to the current one, each segment tappable -- plus the edition
 * and a back affordance. On a wide screen the same pager sits in a phone-width
 * column with an index rail on the left and arrow keys / arrow buttons.
 *
 * Swipe DEEPER: a horizontal swipe on a story (or ArrowRight on a keyboard, or
 * the story's own "Evidence" affordance) opens its depth. Back returns to the same
 * story because the pager remembers its position per edition (see `feedMemory`).
 */
import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react';
import {
  Animated,
  Easing,
  Pressable,
  ScrollView,
  View,
  type GestureResponderEvent,
  type LayoutChangeEvent,
  type NativeScrollEvent,
  type NativeSyntheticEvent,
} from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';

import { useLayout } from '@/design/responsive';
import { useTheme } from '@/design/theme';
import { motion, radius, space } from '@/design/tokens';
import { clampIndex } from '@/lib/stories';

import { Row, Touchable, Txt } from '../ui';

/** Where the reader was, per feed key, so "back" lands on the same story. */
const feedMemory = new Map<string, number>();

export function rememberPosition(key: string, index: number): void {
  feedMemory.set(key, index);
}

export function recallPosition(key: string): number | undefined {
  return feedMemory.get(key);
}

export type SlideRender = (args: { index: number; active: boolean; height: number; width: number; compact: boolean; phone: boolean }) => ReactNode;

export function StoryPager({
  count,
  memoryKey,
  initialIndex,
  renderSlide,
  titles,
  onDeeper,
  headerLeft,
  headerRight,
}: {
  count: number;
  memoryKey: string;
  /** an explicit start (a deep link); otherwise the remembered position, else 0 */
  initialIndex?: number | null;
  renderSlide: SlideRender;
  /** short names for the desktop index rail and accessibility */
  titles: string[];
  /** swipe / ArrowRight -> depth for the story at index */
  onDeeper: (index: number) => void;
  headerLeft?: ReactNode;
  headerRight?: ReactNode;
}) {
  const { c } = useTheme();
  const layout = useLayout();
  const insets = useSafeAreaInsets();
  const scrollRef = useRef<ScrollView>(null);

  const start = clampIndex(initialIndex ?? recallPosition(memoryKey) ?? 0, count);
  const [index, setIndex] = useState(start);
  const [frame, setFrame] = useState<{ width: number; height: number } | null>(null);
  const [hasScrolledToStart, setHasScrolledToStart] = useState(false);

  const onLayout = useCallback((e: LayoutChangeEvent) => {
    const { width, height } = e.nativeEvent.layout;
    setFrame((prev) => (prev && prev.width === width && prev.height === height ? prev : { width, height }));
  }, []);

  const goTo = useCallback(
    (i: number, animated = true) => {
      if (!frame) return;
      const target = clampIndex(i, count);
      scrollRef.current?.scrollTo({ y: target * frame.height, animated });
      setIndex(target);
      rememberPosition(memoryKey, target);
    },
    [count, frame, memoryKey],
  );

  // Land on the remembered / requested story once the frame is measured.
  useEffect(() => {
    if (!frame || hasScrolledToStart) return;
    scrollRef.current?.scrollTo({ y: start * frame.height, animated: false });
    // Marking the initial jump done is the point of this effect (a one-time
    // synchronisation with the ScrollView after layout).
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setHasScrolledToStart(true);
  }, [frame, hasScrolledToStart, start]);

  // The current index, readable from event handlers without re-binding them.
  const indexRef = useRef(index);
  useEffect(() => {
    indexRef.current = index;
  }, [index]);

  const onScroll = useCallback(
    (e: NativeSyntheticEvent<NativeScrollEvent>) => {
      if (!frame) return;
      const i = clampIndex(e.nativeEvent.contentOffset.y / frame.height, count);
      if (i === indexRef.current) return;
      rememberPosition(memoryKey, i);
      setIndex(i);
    },
    [count, frame, memoryKey],
  );

  // Keyboard on desktop / web: ↑ ↓ (or j / k, PageUp / PageDown, Space) to page, → for depth.
  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.addEventListener !== 'function') return;
    const onKey = (ev: KeyboardEvent) => {
      const tag = (ev.target as { tagName?: string } | null)?.tagName ?? '';
      if (tag === 'INPUT' || tag === 'TEXTAREA') return;
      switch (ev.key) {
        case 'ArrowDown':
        case 'PageDown':
        case 'j':
        case ' ':
          ev.preventDefault();
          goTo(indexRef.current + 1);
          break;
        case 'ArrowUp':
        case 'PageUp':
        case 'k':
          ev.preventDefault();
          goTo(indexRef.current - 1);
          break;
        case 'ArrowRight':
        case 'Enter':
          ev.preventDefault();
          onDeeper(indexRef.current);
          break;
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [goTo, onDeeper]);

  // Horizontal swipe = deeper. A vertical swipe is the ScrollView's.
  const touchStart = useRef<{ x: number; y: number } | null>(null);
  const onTouchStart = (e: GestureResponderEvent) => {
    touchStart.current = { x: e.nativeEvent.pageX, y: e.nativeEvent.pageY };
  };
  const onTouchEnd = (e: GestureResponderEvent) => {
    const s = touchStart.current;
    touchStart.current = null;
    if (!s) return;
    const dx = e.nativeEvent.pageX - s.x;
    const dy = e.nativeEvent.pageY - s.y;
    if (dx < -56 && Math.abs(dy) < 40) onDeeper(indexRef.current);
  };

  const compact = (frame?.height ?? 800) < 720;
  const wide = layout.isWide;

  const pager = (
    <View style={{ flex: 1 }} onLayout={onLayout}>
      {frame ? (
        <ScrollView
          ref={scrollRef}
          pagingEnabled
          decelerationRate="fast"
          showsVerticalScrollIndicator={false}
          scrollEventThrottle={16}
          onScroll={onScroll}
          onTouchStart={onTouchStart}
          onTouchEnd={onTouchEnd}
          style={{ flex: 1 }}
          contentContainerStyle={{ height: frame.height * count }}>
          {Array.from({ length: count }).map((_, i) => (
            <View key={i} style={{ height: frame.height, width: frame.width }}>
              {Math.abs(i - index) <= 1
                ? renderSlide({ index: i, active: i === index, height: frame.height, width: frame.width, compact, phone: frame.width < 700 })
                : null}
            </View>
          ))}
        </ScrollView>
      ) : null}

      {/* Top chrome: progress rail + edition + back. Overlaid, never part of the scroll. */}
      <View
        pointerEvents="box-none"
        style={{
          position: 'absolute',
          top: 0,
          left: 0,
          right: 0,
          paddingTop: wide ? space.md : insets.top + space.sm,
          paddingHorizontal: space.lg,
          gap: space.sm,
        }}>
        <ProgressRail count={count} index={index} onJump={goTo} titles={titles} />
        <Row style={{ justifyContent: 'space-between', gap: space.sm }}>
          <View style={{ flexShrink: 1 }}>{headerLeft}</View>
          <Row gap={space.sm}>
            <Txt variant="caption" tone="muted" numeric>
              {index + 1} / {count}
            </Txt>
            {headerRight}
          </Row>
        </Row>
      </View>
    </View>
  );

  if (!wide) return pager;

  // Desktop: a phone-width column, an index rail on the left, arrows on the right.
  const columnWidth = Math.min(480, Math.max(360, layout.width * 0.36));
  return (
    <View style={{ flex: 1, flexDirection: 'row', justifyContent: 'center', gap: space.xxl, paddingHorizontal: layout.gutter }}>
      <View style={{ width: 260, paddingTop: space.huge, gap: space.xs }}>
        <Txt variant="label" tone="muted" style={{ marginBottom: space.sm }}>
          This edition
        </Txt>
        {titles.map((t, i) => (
          <Touchable
            key={i}
            accessibilityRole="button"
            accessibilityState={{ selected: i === index }}
            onPress={() => goTo(i)}
            style={{
              paddingVertical: 6,
              paddingHorizontal: space.md,
              borderRadius: radius.md,
              backgroundColor: i === index ? c.pathfinderSoft : 'transparent',
              flexDirection: 'row',
              alignItems: 'center',
              gap: space.sm,
            }}>
            <Txt variant="caption" tone="muted" numeric style={{ width: 18 }}>
              {i + 1}
            </Txt>
            <Txt variant="small" color={i === index ? c.pathfinder : c.textSecondary} numberOfLines={1} style={{ flex: 1 }}>
              {t}
            </Txt>
          </Touchable>
        ))}
        <Txt variant="caption" tone="muted" style={{ marginTop: space.lg }}>
          ↑ ↓ to browse · → for the evidence
        </Txt>
      </View>

      <View
        style={{
          width: columnWidth,
          marginVertical: space.lg,
          borderRadius: radius.lg,
          overflow: 'hidden',
          borderWidth: 1,
          borderColor: c.border,
          backgroundColor: c.bg,
        }}>
        {pager}
      </View>

      <View style={{ width: 56, justifyContent: 'center', gap: space.md }}>
        <ArrowButton label="Previous story" glyph="↑" disabled={index === 0} onPress={() => goTo(index - 1)} />
        <ArrowButton label="Next story" glyph="↓" disabled={index >= count - 1} onPress={() => goTo(index + 1)} />
        <ArrowButton label="Open the evidence" glyph="→" onPress={() => onDeeper(index)} />
      </View>
    </View>
  );
}

function ArrowButton({ label, glyph, onPress, disabled = false }: { label: string; glyph: string; onPress: () => void; disabled?: boolean }) {
  const { c } = useTheme();
  return (
    <Touchable
      accessibilityRole="button"
      accessibilityLabel={label}
      disabled={disabled}
      onPress={onPress}
      style={{
        width: 44,
        height: 44,
        borderRadius: radius.pill,
        borderWidth: 1,
        borderColor: c.borderStrong,
        alignItems: 'center',
        justifyContent: 'center',
        opacity: disabled ? 0.3 : 1,
      }}>
      <Txt variant="subheading" tone="secondary">
        {glyph}
      </Txt>
    </Touchable>
  );
}

/** The segmented progress rail. Filled up to the current story; each segment jumps. */
function ProgressRail({ count, index, onJump, titles }: { count: number; index: number; onJump: (i: number) => void; titles: string[] }) {
  const { c } = useTheme();
  return (
    <Row gap={4} style={{ height: 14, alignItems: 'center' }}>
      {Array.from({ length: count }).map((_, i) => (
        <Pressable
          key={i}
          accessibilityRole="button"
          accessibilityLabel={`Story ${i + 1} of ${count}: ${titles[i] ?? ''}`}
          onPress={() => onJump(i)}
          hitSlop={6}
          style={{ flex: 1, height: 14, justifyContent: 'center' }}>
          <Segment filled={i < index} active={i === index} track={c.borderStrong} fill={c.pathfinder} />
        </Pressable>
      ))}
    </Row>
  );
}

function Segment({ filled, active, track, fill }: { filled: boolean; active: boolean; track: string; fill: string }) {
  // The active segment fills in calmly; the others are either full or empty.
  const [progress] = useState(() => new Animated.Value(filled ? 1 : 0));
  useEffect(() => {
    Animated.timing(progress, {
      toValue: filled || active ? 1 : 0,
      duration: active ? motion.slow * 2 : motion.fast,
      easing: Easing.out(Easing.cubic),
      useNativeDriver: false,
    }).start();
  }, [active, filled, progress]);
  return (
    <View style={{ height: 3, borderRadius: 2, backgroundColor: track, overflow: 'hidden' }}>
      <Animated.View
        style={{
          height: '100%',
          backgroundColor: fill,
          width: progress.interpolate({ inputRange: [0, 1], outputRange: ['0%', '100%'] }),
          opacity: active ? 1 : filled ? 0.85 : 0,
        }}
      />
    </View>
  );
}
