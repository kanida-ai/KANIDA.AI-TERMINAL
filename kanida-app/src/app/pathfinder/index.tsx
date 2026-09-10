import { useLocalSearchParams, useRouter } from 'expo-router';
import { useCallback, useMemo } from 'react';
import { View } from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';

import { api, invalidate } from '@/api/client';
import { useResource } from '@/api/useResource';
import { RecordChip } from '@/components/chips';
import { StoryPager } from '@/components/feed/StoryPager';
import { StorySlide } from '@/components/feed/StorySlide';
import { ErrorState, SkeletonCard } from '@/components/states';
import { Row, Stack, Touchable, Txt } from '@/components/ui';
import { useLayout } from '@/design/responsive';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { dateShort } from '@/lib/format';
import { buildStories, indexOfStory, storyTitle, type Story } from '@/lib/stories';

/**
 * THE FEED — one finding per screen, swipe up for the next.
 *
 *   /pathfinder            the latest edition
 *   /pathfinder?date=      a specific close (deep-linkable, shareable)
 *   /pathfinder?story=     land on one story (used by "back" from depth and by the Home card)
 *
 * The order is the engine's: what matters now, then discoveries, then the
 * experiments (or the honest story of why none opened), then the scoreboard,
 * then what is being tested next. Nothing is padded; a four-finding day is four
 * finding stories and an edition that published nothing says so.
 */
export default function FeedScreen() {
  const { c } = useTheme();
  const router = useRouter();
  const insets = useSafeAreaInsets();
  const layout = useLayout();
  const params = useLocalSearchParams<{ date?: string; story?: string }>();
  const date = typeof params.date === 'string' && params.date ? params.date : null;
  const wanted = typeof params.story === 'string' && params.story ? params.story : null;

  const feed = useResource((s) => api.feed(date, s), [date]);
  const registry = useResource((s) => api.experiments(s), []);

  const stories = useMemo<Story[]>(() => (feed.data ? buildStories(feed.data, registry.data) : []), [feed.data, registry.data]);
  const titles = useMemo(() => stories.map(storyTitle), [stories]);
  const memoryKey = `feed:${feed.data?.edition_date ?? date ?? 'latest'}`;
  const initialIndex = indexOfStory(stories, wanted);

  const openDepth = useCallback(
    (index: number) => {
      const s = stories[index];
      if (!s || !feed.data) return;
      const ed = feed.data.edition_date;
      switch (s.kind) {
        case 'finding':
          router.push({ pathname: '/pathfinder/story/[id]', params: { id: s.finding.id, date: ed } });
          return;
        case 'experiment':
          router.push({ pathname: '/pathfinder/experiment/[id]', params: { id: s.card.id } });
          return;
        case 'no_experiment':
          router.push('/pathfinder/experiments');
          return;
        case 'scoreboard':
          router.push({ pathname: '/pathfinder/scoreboard', params: { date: ed } });
          return;
        case 'next':
        case 'empty_edition':
          router.push({ pathname: '/pathfinder/scoreboard', params: { date: ed } });
          return;
      }
    },
    [feed.data, router, stories],
  );

  const openStory = useCallback(
    (findingId: string) => {
      if (!feed.data) return;
      router.push({ pathname: '/pathfinder/story/[id]', params: { id: findingId, date: feed.data.edition_date } });
    },
    [feed.data, router],
  );

  const refreshAll = () => {
    invalidate();
    feed.refresh();
    registry.refresh();
  };

  const back = () => (router.canGoBack() ? router.back() : router.replace('/'));

  if (feed.error && !feed.data) {
    return (
      <View style={{ flex: 1, backgroundColor: c.bg, paddingTop: insets.top + space.lg, paddingHorizontal: layout.gutter, alignItems: 'center' }}>
        <Stack gap={space.lg} style={{ width: '100%', maxWidth: 640 }}>
          <BackRow onPress={back} />
          <ErrorState error={feed.error} onRetry={refreshAll} />
        </Stack>
      </View>
    );
  }

  if (!feed.data) {
    return (
      <View style={{ flex: 1, backgroundColor: c.bg, paddingTop: insets.top + space.lg, paddingHorizontal: layout.gutter, alignItems: 'center' }}>
        <Stack gap={space.lg} style={{ width: '100%', maxWidth: 480 }}>
          <BackRow onPress={back} />
          <SkeletonCard lines={2} />
          <SkeletonCard lines={5} />
          <SkeletonCard lines={3} />
        </Stack>
      </View>
    );
  }

  const data = feed.data;

  return (
    <View style={{ flex: 1, backgroundColor: c.bg }}>
      <StoryPager
        key={memoryKey}
        count={stories.length}
        memoryKey={memoryKey}
        initialIndex={initialIndex}
        titles={titles}
        onDeeper={openDepth}
        headerLeft={
          <Row gap={space.sm}>
            <Touchable accessibilityRole="button" accessibilityLabel="Back" onPress={back} style={{ paddingVertical: 2, paddingRight: space.xs }}>
              <Txt variant="title" tone="secondary">
                ‹
              </Txt>
            </Touchable>
            <Stack gap={0}>
              <Txt variant="smallStrong">Pathfinder</Txt>
              <Txt variant="caption" tone="muted" numeric>
                {dateShort(data.edition_date)} · {regimeShort(data.regime)}
              </Txt>
            </Stack>
          </Row>
        }
        headerRight={<RecordChip backfilled={data.backfilled} />}
        renderSlide={({ index, active, height, width, compact, phone }) => (
          <StorySlide
            story={stories[index]}
            ctx={{
              index,
              active,
              height,
              width,
              compact,
              phone,
              onDeeper: () => openDepth(index),
              registryError: registry.error,
              registryLoading: registry.loading,
              onOpenStory: openStory,
            }}
          />
        )}
      />
    </View>
  );
}

function regimeShort(regime: string): string {
  const i = regime.indexOf(' (');
  return i === -1 ? regime : regime.slice(0, i);
}

function BackRow({ onPress }: { onPress: () => void }) {
  const { c } = useTheme();
  return (
    <Row gap={space.sm}>
      <Touchable
        accessibilityRole="button"
        accessibilityLabel="Back"
        onPress={onPress}
        style={{ paddingHorizontal: space.md, paddingVertical: space.xs, borderRadius: radius.pill, borderWidth: 1, borderColor: c.borderStrong }}>
        <Txt variant="smallStrong" tone="secondary">
          ‹ Back
        </Txt>
      </Touchable>
      <Txt variant="smallStrong">Pathfinder</Txt>
    </Row>
  );
}
