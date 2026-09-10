import { api } from '@/api/client';
import { useResource } from '@/api/useResource';
import { Disclosure } from '@/components/agent';
import { ExperimentCard } from '@/components/ExperimentCard';
import { Page } from '@/components/Page';
import { EmptyState, Resourced, SkeletonCard } from '@/components/states';
import { Card, Label, Stack, Txt } from '@/components/ui';
import { space } from '@/design/tokens';

/**
 * STRATEGY LAB — graduated strategies and their versions (L3 learning).
 *
 * The brief scopes this as a stub for P2, and the P0 contract has no
 * `/strategies` endpoint yet. Rather than mock one, this renders what the
 * contract DOES serve: the experiments that graduated, each carrying its
 * strategy version. The per-version diff view lands when P1 exposes strategy
 * versions as a resource — noted in docs/handbacks/P2.md § contract gaps.
 */
export default function StrategyLabScreen() {
  const res = useResource((s) => api.experiments('promoted', s), []);

  return (
    <Page refreshing={res.refreshing} onRefresh={res.refresh} bottomInset={72} contentStyle={{ paddingTop: space.xl }}>
      <Stack gap={space.xs}>
        <Label>Strategy lab</Label>
        <Txt variant="title">What graduated, and at which version</Txt>
        <Txt variant="small" tone="secondary">
          An experiment graduates only when forward evidence agrees with history after costs. Each
          version change is recorded in that experiment{'’'}s change-log.
        </Txt>
      </Stack>

      <Resourced
        data={res.data}
        error={res.error}
        loading={res.loading}
        onRetry={res.refresh}
        skeleton={<SkeletonCard lines={3} />}>
        {(data) =>
          data.items.length === 0 ? (
            <EmptyState
              title="Nothing has graduated yet"
              body="No experiment has cleared the gate. This screen stays empty until one does — it will not be filled with candidates."
            />
          ) : (
            <Stack gap={space.lg}>
              {data.items.map((item) => (
                <ExperimentCard key={item.id} item={item} />
              ))}
              <Card>
                <Stack gap={space.xs}>
                  <Label>Not built yet</Label>
                  <Txt variant="small" tone="secondary">
                    Version-to-version diffs across strategies need a strategy-versions resource that
                    the API does not serve yet. Until it does, the full version history for a
                    strategy lives in its experiment{'’'}s change-log rather than being reassembled
                    here from parts.
                  </Txt>
                </Stack>
              </Card>
              <Disclosure text={data.disclosure} />
            </Stack>
          )
        }
      </Resourced>
    </Page>
  );
}
