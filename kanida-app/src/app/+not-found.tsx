import { Link, Stack } from 'expo-router';

import { Page } from '@/components/Page';
import { Card, Stack as VStack, Txt } from '@/components/ui';
import { space } from '@/design/tokens';

export default function NotFound() {
  return (
    <>
      <Stack.Screen options={{ title: 'Not found' }} />
      <Page contentStyle={{ paddingTop: space.huge }}>
        <Card>
          <VStack gap={space.md}>
            <Txt variant="title">That page does not exist</Txt>
            <Txt variant="body" tone="secondary">
              The link may be from an older build of the app.
            </Txt>
            <Link href="/">
              <Txt variant="bodyStrong" tone="pathfinder">
                Go to Home
              </Txt>
            </Link>
          </VStack>
        </Card>
      </Page>
    </>
  );
}
