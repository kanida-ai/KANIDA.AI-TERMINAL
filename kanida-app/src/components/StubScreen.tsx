import { useSafeAreaInsets } from 'react-native-safe-area-context';

import { Page } from '@/components/Page';
import { Card, Label, Stack, Txt } from '@/components/ui';
import { useLayout } from '@/design/responsive';
import { space } from '@/design/tokens';

/**
 * An honestly-empty tab.
 *
 * This slice (session P2) builds Pathfinder. The rest of the chrome exists so
 * the navigation is real, but it shows nothing rather than a mock: a screen full
 * of invented numbers is the exact failure mode this product is built against.
 */
export function StubScreen({ title, body }: { title: string; body: string }) {
  const insets = useSafeAreaInsets();
  const layout = useLayout();
  return (
    <Page
      bottomInset={layout.isWide ? 0 : 72}
      contentStyle={{ paddingTop: layout.isWide ? space.xxl : insets.top + space.lg }}>
      <Stack gap={space.xs}>
        <Label>{title}</Label>
        <Txt variant="title">Not built in this slice</Txt>
      </Stack>
      <Card>
        <Txt variant="body" tone="secondary">
          {body}
        </Txt>
      </Card>
    </Page>
  );
}
