/**
 * The edge-discovery pipeline: New ideas -> Backtesting -> Virtual trading ->
 * Validating -> Graduated, with live counts.
 *
 * The rejected column sits OUTSIDE the funnel, on its own row, because it is not
 * a stage things pass through -- it is where most ideas end, and the product
 * says so out loud. Tapping any stage filters the experiments list.
 */
import { useRouter } from 'expo-router';
import { View } from 'react-native';

import type { ExperimentStatus } from '@/api/types';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { PIPELINE, statusTone } from '@/lib/honesty';

import { Card, Label, Row, Stack, Touchable, Txt } from './ui';

export function PipelineBar({ counts }: { counts: Partial<Record<ExperimentStatus, number>> }) {
  const { c } = useTheme();
  const router = useRouter();
  const died = counts.died ?? 0;

  return (
    <Card>
      <Stack gap={space.lg}>
        <Label>Edge discovery pipeline</Label>

        <Row style={{ gap: space.sm, alignItems: 'stretch', flexWrap: 'wrap' }}>
          {PIPELINE.map((stage, i) => {
            const tone = statusTone(stage.status, c);
            const n = counts[stage.status] ?? 0;
            return (
              <Touchable
                key={stage.status}
                accessibilityRole="link"
                accessibilityLabel={`${stage.label}: ${n} experiments`}
                onPress={() => router.push(`/pathfinder/experiments?status=${stage.status}`)}
                style={{ flexGrow: 1, flexBasis: 0, minWidth: 92 }}>
                <View
                  style={{
                    backgroundColor: n > 0 ? tone.bg : 'transparent',
                    borderRadius: radius.md,
                    borderWidth: 1,
                    borderColor: n > 0 ? 'transparent' : c.border,
                    paddingVertical: space.md,
                    paddingHorizontal: space.sm,
                    gap: 4,
                    minHeight: 78,
                  }}>
                  <Txt variant="metricSm" numeric color={n > 0 ? tone.fg : c.textGreyed}>
                    {n}
                  </Txt>
                  <Txt variant="caption" tone={n > 0 ? 'secondary' : 'greyed'}>
                    {stage.label}
                  </Txt>
                </View>
                {i < PIPELINE.length - 1 ? null : null}
              </Touchable>
            );
          })}
        </Row>

        <Touchable
          accessibilityRole="link"
          accessibilityLabel={`Rejected and published: ${died} experiments`}
          onPress={() => router.push('/pathfinder/experiments?status=died')}>
          <Row
            style={{
              justifyContent: 'space-between',
              backgroundColor: c.negativeSoft,
              borderRadius: radius.md,
              padding: space.md,
            }}>
            <Stack gap={2} style={{ flex: 1 }}>
              <Txt variant="smallStrong" color={c.negative}>
                {died} rejected — published, not hidden
              </Txt>
              <Txt variant="caption" tone="secondary">
                Most ideas end here. Each one carries its post-mortem and what we kept.
              </Txt>
            </Stack>
            <Txt variant="heading" tone="muted">
              ›
            </Txt>
          </Row>
        </Touchable>
      </Stack>
    </Card>
  );
}
