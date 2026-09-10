import { useLocalSearchParams, useRouter } from 'expo-router';
import { ScrollView, View } from 'react-native';

import { api } from '@/api/client';
import type { ExperimentStatus } from '@/api/types';
import { useResource } from '@/api/useResource';
import { Disclosure } from '@/components/agent';
import { ExperimentCard } from '@/components/ExperimentCard';
import { Page } from '@/components/Page';
import { EmptyState, Resourced, SkeletonCard } from '@/components/states';
import { Label, Row, Stack, Touchable, Txt } from '@/components/ui';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { statusLabel, statusMeaning, statusTone } from '@/lib/honesty';

/**
 * EXPERIMENTS — the full filterable list.
 *
 * The engine returns the list LOSERS FIRST (dead experiments lead, graduated
 * ones last). This screen renders `items` in the order received and offers no
 * "sort by best". Filtering by stage is allowed; re-ranking by result is not.
 */
const FILTERS: { key: ExperimentStatus | 'all'; label: string }[] = [
  { key: 'all', label: 'All' },
  { key: 'died', label: 'Rejected' },
  { key: 'testing', label: 'Experimenting' },
  { key: 'validating', label: 'Validating' },
  { key: 'promising', label: 'Promising' },
  { key: 'promoted', label: 'Graduated' },
  { key: 'queued', label: 'Queued' },
];

export default function ExperimentsScreen() {
  const { c } = useTheme();
  const router = useRouter();
  const params = useLocalSearchParams<{ status?: string }>();
  const active = (FILTERS.find((f) => f.key === params.status)?.key ?? 'all') as
    | ExperimentStatus
    | 'all';

  const list = useResource(
    (s) => api.experiments(active === 'all' ? null : active, s),
    [active],
  );

  return (
    <Page refreshing={list.refreshing} onRefresh={list.refresh} bottomInset={72} contentStyle={{ paddingTop: space.xl }}>
      <Stack gap={space.xs}>
        <Label>Experiments</Label>
        <Txt variant="title">Every hypothesis, including the dead ones</Txt>
        <Txt variant="small" tone="secondary">
          Ordered worst result first. Nothing is hidden for looking bad — that ordering is the point.
        </Txt>
      </Stack>

      <View style={{ marginHorizontal: -space.xs }}>
        <ScrollView
          horizontal
          showsHorizontalScrollIndicator={false}
          contentContainerStyle={{ gap: space.sm, paddingHorizontal: space.xs }}>
          {FILTERS.map((f) => {
            const isActive = f.key === active;
            const tone = f.key === 'all' ? { fg: c.text, bg: c.surfaceRaised } : statusTone(f.key, c);
            return (
              <Touchable
                key={f.key}
                accessibilityRole="button"
                accessibilityState={{ selected: isActive }}
                onPress={() =>
                  router.setParams({ status: f.key === 'all' ? undefined : f.key })
                }
                style={{
                  paddingHorizontal: space.lg,
                  paddingVertical: space.sm,
                  borderRadius: radius.pill,
                  borderWidth: 1,
                  borderColor: isActive ? tone.fg : c.border,
                  backgroundColor: isActive ? tone.bg : 'transparent',
                }}>
                <Txt variant="smallStrong" color={isActive ? tone.fg : c.textSecondary}>
                  {f.label}
                </Txt>
              </Touchable>
            );
          })}
        </ScrollView>
      </View>

      {active !== 'all' ? (
        <Txt variant="small" tone="secondary">
          {statusLabel[active]} — {statusMeaning[active]}
        </Txt>
      ) : null}

      <Resourced
        data={list.data}
        error={list.error}
        loading={list.loading}
        onRetry={list.refresh}
        skeleton={
          <Stack gap={space.lg}>
            <SkeletonCard lines={3} />
            <SkeletonCard lines={3} />
            <SkeletonCard lines={3} />
          </Stack>
        }>
        {(data) =>
          data.items.length === 0 ? (
            <EmptyState
              title="No experiment is at this stage"
              body="Nothing is queued here right now. The list will fill as the observer finds conditions worth asking a question about."
            />
          ) : (
            <Stack gap={space.lg}>
              <Row style={{ justifyContent: 'space-between' }}>
                <Txt variant="caption" tone="muted" numeric>
                  {data.count} experiment{data.count === 1 ? '' : 's'}
                </Txt>
                <Txt variant="caption" tone="muted">
                  worst first
                </Txt>
              </Row>
              {data.items.map((item) => (
                <ExperimentCard key={item.id} item={item} />
              ))}
              <Disclosure text={data.disclosure} />
            </Stack>
          )
        }
      </Resourced>
    </Page>
  );
}
