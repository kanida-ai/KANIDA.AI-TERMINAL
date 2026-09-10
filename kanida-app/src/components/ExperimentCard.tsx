/**
 * The experiment card -- the atom of the Pathfinder UI.
 *
 * The mockup's anatomy: `#id` · status badge · the hypothesis in quotes ·
 * historical · virtual · occurrences / n · sparkline · chevron.
 *
 * ONE DELIBERATE ELEVATION over the mockup: where the mockup shows a bare
 * "return" for each side, this shows EXPECTANCY PER TRADE as the figure, with
 * the cumulative return and its drawdown on the supporting line beneath. Two
 * reasons, both non-negotiables from CLAUDE.md: expectancy is the hero metric,
 * and no return is ever shown without its drawdown. The layout, weight and
 * rhythm of the mockup are unchanged -- only what the big number MEANS is.
 *
 * A `died` experiment renders exactly like a live one, with a red badge. It is
 * never dimmed, collapsed or moved to a secondary surface: the graveyard is a
 * feature.
 */
import { useRouter } from 'expo-router';
import { View } from 'react-native';

import type { ExperimentSummary, PerformanceBlock } from '@/api/types';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { EMPTY, count, pct, pctAbs } from '@/lib/format';
import { figureTone, sampleCopy, sampleTone, statusLabel, statusTone } from '@/lib/honesty';
import { shortExperimentId } from '@/lib/tokens';

import { Sparkline } from './Sparkline';
import { Card, Label, Pill, Row, Stack, Touchable, Txt } from './ui';

function SideMetric({
  side,
  block,
  greyed,
}: {
  side: string;
  block: PerformanceBlock | null | undefined;
  greyed: boolean;
}) {
  const { c } = useTheme();
  if (!block) {
    return (
      <Stack gap={2} style={{ flexGrow: 1, flexBasis: 0, minWidth: 104 }}>
        <Label>{side}</Label>
        <Txt variant="metricSm" tone="greyed" numeric>
          {EMPTY}
        </Txt>
        <Txt variant="caption" tone="muted">
          nothing computed yet
        </Txt>
      </Stack>
    );
  }
  const tone = greyed ? c.textGreyed : figureTone(block.sample_flag, block.expectancy_pct_per_trade, c);
  const hasReturn = block.total_return_pct !== null && block.total_return_pct !== undefined;
  return (
    <Stack gap={2} style={{ flexGrow: 1, flexBasis: 0, minWidth: 104 }}>
      <Label>{side}</Label>
      <Txt variant="metricSm" numeric color={tone}>
        {pct(block.expectancy_pct_per_trade)}
      </Txt>
      <Txt variant="caption" tone={greyed ? 'greyed' : 'muted'} numeric>
        {hasReturn ? `${pct(block.total_return_pct, 1)} · ` : ''}
        DD {pctAbs(block.max_drawdown_pct)}
      </Txt>
    </Stack>
  );
}

export function ExperimentCard({ item }: { item: ExperimentSummary }) {
  const { c } = useTheme();
  const router = useRouter();
  const badge = statusTone(item.status, c);
  const greyed = item.sample_flag === 'greyed' || item.sample_flag === 'unknown';
  const flagTone = sampleTone(item.sample_flag, c);

  return (
    <Touchable
      accessibilityRole="link"
      accessibilityLabel={`Experiment ${shortExperimentId(item.id)}, ${statusLabel[item.status]}. ${item.hypothesis}`}
      onPress={() => router.push(`/pathfinder/experiment/${item.id}`)}>
      <Card accent={item.status === 'died' ? c.negative : undefined}>
        <Stack gap={space.lg}>
          <Row style={{ justifyContent: 'space-between', gap: space.sm }}>
            <Row gap={space.sm}>
              <Txt variant="smallStrong" tone="muted" numeric>
                {shortExperimentId(item.id)}
              </Txt>
              <Pill fg={badge.fg} bg={badge.bg} bordered={item.status === 'queued'}>
                {statusLabel[item.status]}
              </Pill>
            </Row>
            <Txt variant="heading" tone="muted">
              ›
            </Txt>
          </Row>

          {/* The hypothesis, in quotes -- the sentence is the product. */}
          <Txt variant="subheading" style={{ lineHeight: 23 }}>
            “{item.hypothesis}”
          </Txt>

          <Row style={{ gap: space.lg, alignItems: 'flex-end', flexWrap: 'wrap' }}>
            <SideMetric side="Historical" block={item.historical_return} greyed={greyed} />
            <SideMetric side="Forward · virtual" block={item.virtual_return} greyed={greyed} />
            {item.spark ? (
              <View style={{ paddingBottom: 2 }}>
                <Sparkline spark={item.spark} muted={greyed} />
              </View>
            ) : null}
          </Row>

          <Row style={{ gap: space.sm, flexWrap: 'wrap' }}>
            <Pill fg={flagTone} bordered>
              {item.n === null || item.n === undefined ? 'n —' : `n = ${count(item.n)}`}
              {item.sample_flag === 'ok' ? '' : ` · ${sampleCopy[item.sample_flag].short}`}
            </Pill>
            {item.occurrences !== null && item.occurrences !== undefined ? (
              <Pill fg={c.textMuted} bordered>
                {count(item.occurrences)} occurrences
              </Pill>
            ) : null}
            {item.strategy_version ? (
              <Pill fg={c.textMuted} bordered>
                {item.strategy_version}
              </Pill>
            ) : null}
          </Row>

          {greyed ? (
            <View style={{ backgroundColor: c.bgSunken, borderRadius: radius.md, padding: space.md }}>
              <Txt variant="caption" tone="greyed">
                {sampleCopy[item.sample_flag].long}
              </Txt>
            </View>
          ) : null}
        </Stack>
      </Card>
    </Touchable>
  );
}
