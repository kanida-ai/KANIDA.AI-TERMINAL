/**
 * Loading, empty and error states.
 *
 * These are first-class screens, not afterthoughts: a research lab that has had
 * a quiet day must look as considered as one that had a discovery. "No
 * experiment reached a decision today" is a real answer, and the design says so
 * without apology or filler.
 */
import { useEffect, useState, type ReactNode } from 'react';
import { Animated, Easing, Pressable, View, type StyleProp, type ViewStyle } from 'react-native';

import { ApiError, API_BASE_URL } from '@/api/client';
import { useTheme } from '@/design/theme';
import { motion, radius, space } from '@/design/tokens';

import { Card, Row, Stack, Txt } from './ui';

/** A shimmering placeholder block. Same rhythm as the content it stands in for. */
export function Skeleton({
  width = '100%',
  height = 14,
  style,
}: {
  width?: number | `${number}%`;
  height?: number;
  style?: StyleProp<ViewStyle>;
}) {
  const { c } = useTheme();
  // Lazy useState, not useRef: a ref's `.current` must not be read during render.
  const [pulse] = useState(() => new Animated.Value(0.45));

  useEffect(() => {
    const loop = Animated.loop(
      Animated.sequence([
        Animated.timing(pulse, {
          toValue: 1,
          duration: motion.slow * 2,
          easing: Easing.inOut(Easing.quad),
          useNativeDriver: true,
        }),
        Animated.timing(pulse, {
          toValue: 0.45,
          duration: motion.slow * 2,
          easing: Easing.inOut(Easing.quad),
          useNativeDriver: true,
        }),
      ]),
    );
    loop.start();
    return () => loop.stop();
  }, [pulse]);

  return (
    <Animated.View
      accessibilityLabel="Loading"
      style={[
        { width, height, borderRadius: radius.sm, backgroundColor: c.surfaceRaised, opacity: pulse },
        style,
      ]}
    />
  );
}

/** The skeleton of a story beat / experiment card, so the page does not jump. */
export function SkeletonCard({ lines = 3 }: { lines?: number }) {
  return (
    <Card>
      <Stack gap={space.md}>
        <Skeleton width="40%" height={11} />
        <Skeleton width="85%" height={18} />
        {Array.from({ length: lines }).map((_, i) => (
          <Skeleton key={i} width={i === lines - 1 ? '62%' : '100%'} height={12} />
        ))}
      </Stack>
    </Card>
  );
}

export function EmptyState({
  title,
  body,
  icon = '·',
}: {
  title: string;
  body: string;
  icon?: string;
}) {
  const { c } = useTheme();
  return (
    <Card style={{ alignItems: 'center', paddingVertical: space.huge }}>
      <Stack gap={space.sm} style={{ alignItems: 'center', maxWidth: 420 }}>
        <View
          style={{
            width: 44,
            height: 44,
            borderRadius: radius.pill,
            backgroundColor: c.pathfinderSoft,
            alignItems: 'center',
            justifyContent: 'center',
          }}>
          <Txt variant="heading" tone="pathfinder">
            {icon}
          </Txt>
        </View>
        <Txt variant="subheading" style={{ textAlign: 'center' }}>
          {title}
        </Txt>
        <Txt variant="small" tone="secondary" style={{ textAlign: 'center' }}>
          {body}
        </Txt>
      </Stack>
    </Card>
  );
}

/**
 * The error state names the server it could not reach. When the base URL is
 * swapped from the P0 mock to the P1 engine, this is the screen that tells you
 * which one you are actually pointed at.
 */
export function ErrorState({ error, onRetry }: { error: ApiError; onRetry: () => void }) {
  const { c } = useTheme();
  const unreachable = error.isUnreachable;
  return (
    <Card accent={c.caution}>
      <Stack gap={space.md}>
        <Txt variant="label" tone="caution">
          {unreachable ? 'Research server unreachable' : 'Could not load'}
        </Txt>
        <Txt variant="subheading">{error.message}</Txt>
        <Stack gap={2}>
          <Txt variant="caption" tone="muted" mono numeric>
            {API_BASE_URL}
          </Txt>
          <Txt variant="caption" tone="muted" mono>
            {error.code}
            {error.status ? ` · HTTP ${error.status}` : ''}
            {error.requestId ? ` · ${error.requestId}` : ''}
          </Txt>
        </Stack>
        {unreachable ? (
          <Txt variant="small" tone="secondary">
            Nothing is shown rather than something stale — a research result you cannot date is not a
            result.
          </Txt>
        ) : null}
        <Row>
          <Pressable
            onPress={onRetry}
            accessibilityRole="button"
            style={{
              paddingHorizontal: space.lg,
              paddingVertical: space.sm,
              borderRadius: radius.pill,
              borderWidth: 1,
              borderColor: c.borderStrong,
            }}>
            <Txt variant="smallStrong">Try again</Txt>
          </Pressable>
        </Row>
      </Stack>
    </Card>
  );
}

/** Wraps a resource: skeleton while loading, error state on failure, else children. */
export function Resourced<T>({
  data,
  error,
  loading,
  onRetry,
  skeleton,
  children,
}: {
  data: T | null;
  error: ApiError | null;
  loading: boolean;
  onRetry: () => void;
  skeleton: ReactNode;
  children: (data: T) => ReactNode;
}) {
  if (error && !data) return <ErrorState error={error} onRetry={onRetry} />;
  if (loading && !data) return <>{skeleton}</>;
  if (!data) return null;
  return <>{children(data)}</>;
}
