/**
 * Rendering a PerformanceBlock.
 *
 * This component is where three non-negotiables physically live:
 *
 *   1. EXPECTANCY IS THE HERO. It is the large figure, at the top, always. Win
 *      rate appears only in the supporting row, at supporting size, and only
 *      when the engine sent one.
 *   2. EVERY RETURN IS SHOWN WITH ITS DRAWDOWN. `total_return_pct` is never
 *      rendered without `max_drawdown_pct` next to it, at equal weight.
 *   3. EVERY NUMBER CARRIES n + DATE RANGE + DATA SOURCE + COST CONVENTION.
 *      The provenance strip is part of the block, not a tooltip.
 *
 * Plus the gate that decides whether any of it counts: the same expectancy
 * re-run at 2x slippage. An edge that dies at doubled slippage is not an edge,
 * and it is shown right under the hero figure rather than buried.
 */
import { View } from 'react-native';

import type { PerformanceBlock, Provenance } from '@/api/types';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { EMPTY, pct, pctAbs, count, rangeWithSpan, dateShort } from '@/lib/format';
import { basisLabel, figureTone, sampleCopy, sampleTone, survives2xSlippage } from '@/lib/honesty';

import { Card, Divider, Label, Pill, Row, Stack, Txt } from './ui';

/** The n-flag chip. Shown wherever a figure is. */
export function SampleChip({ n, flag }: { n: number | null | undefined; flag: PerformanceBlock['sample_flag'] }) {
  const { c } = useTheme();
  const tone = sampleTone(flag, c);
  return (
    <Pill fg={tone} bordered>
      {n === null || n === undefined ? 'n —' : `n = ${count(n)}`}
      {flag === 'ok' ? '' : ` · ${sampleCopy[flag].short}`}
    </Pill>
  );
}

/** A labelled figure. `paired` renders it at half width beside a sibling. */
export function Metric({
  label,
  value,
  tone,
  hint,
  size = 'md',
}: {
  label: string;
  value: string;
  tone?: string;
  hint?: string;
  size?: 'lg' | 'md' | 'sm';
}) {
  return (
    <Stack gap={3} style={{ flexGrow: 1, flexBasis: 0, minWidth: 84 }}>
      <Label>{label}</Label>
      <Txt variant={size === 'lg' ? 'metric' : size === 'md' ? 'metricSm' : 'bodyStrong'} numeric color={tone}>
        {value}
      </Txt>
      {hint ? (
        <Txt variant="caption" tone="muted">
          {hint}
        </Txt>
      ) : null}
    </Stack>
  );
}

/**
 * The full block. `compact` is the card variant (hero + return/drawdown pair);
 * the full variant adds the supporting row and the provenance strip.
 */
export function PerformanceView({
  block,
  compact = false,
  title,
}: {
  block: PerformanceBlock;
  compact?: boolean;
  title?: string;
}) {
  const { c } = useTheme();
  const grey = block.sample_flag === 'greyed' || block.sample_flag === 'unknown';
  const expTone = figureTone(block.sample_flag, block.expectancy_pct_per_trade, c);
  const slipTone = figureTone(block.sample_flag, block.expectancy_2x_slippage_pct_per_trade, c);
  const survives = survives2xSlippage(block);

  return (
    <Stack gap={space.lg}>
      <Stack gap={space.xs}>
        <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
          <Label>{title ?? basisLabel(block.basis)}</Label>
          <SampleChip n={block.n} flag={block.sample_flag} />
        </Row>
        {/* The honesty label, verbatim from the engine. Never paraphrased. */}
        <Txt variant="caption" tone="muted">
          {block.label}
        </Txt>
      </Stack>

      {/* 1. HERO: expectancy. */}
      <Row style={{ alignItems: 'flex-start', gap: space.lg, flexWrap: 'wrap' }}>
        <Metric
          size="lg"
          label="Expectancy / trade"
          value={pct(block.expectancy_pct_per_trade)}
          tone={expTone}
          hint="net of costs — the metric that decides"
        />
        <Metric
          size="md"
          label="At 2× slippage"
          value={pct(block.expectancy_2x_slippage_pct_per_trade)}
          tone={slipTone}
          hint={survives ? 'survives the gate' : 'does NOT survive the gate'}
        />
      </Row>

      {!survives ? (
        <View
          style={{
            backgroundColor: c.negativeSoft,
            borderRadius: radius.md,
            padding: space.md,
          }}>
          <Txt variant="small" color={c.negative}>
            The edge does not survive doubled slippage. Whatever the headline number says, this is
            not tradeable as written.
          </Txt>
        </View>
      ) : null}

      {/* 2. Return, always paired with drawdown. */}
      <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
        <Metric
          label="Return to date"
          value={block.total_return_pct === null || block.total_return_pct === undefined ? EMPTY : pct(block.total_return_pct, 1)}
          tone={grey ? c.textGreyed : figureTone(block.sample_flag, block.total_return_pct, c)}
          hint={block.total_return_pct === null || block.total_return_pct === undefined ? 'not reported for this basis' : undefined}
        />
        <Metric
          label="Worst drawdown"
          value={pctAbs(block.max_drawdown_pct)}
          tone={grey ? c.textGreyed : c.text}
          hint="peak to trough"
        />
        <Metric
          label="Drawdown now"
          value={pctAbs(block.current_drawdown_pct)}
          tone={grey ? c.textGreyed : c.text}
          hint={`as of ${dateShort(block.provenance.as_of)}`}
        />
      </Row>

      {!compact ? (
        <>
          <Divider />
          {/* 3. Supporting only. Win rate lives here and nowhere else. */}
          <Row style={{ gap: space.lg, flexWrap: 'wrap' }}>
            <Metric size="sm" label="Win rate (supporting)" value={block.win_rate_pct === null || block.win_rate_pct === undefined ? EMPTY : pctAbs(block.win_rate_pct)} tone={c.textSecondary} />
            <Metric size="sm" label="Avg win" value={block.avg_win_pct === null || block.avg_win_pct === undefined ? EMPTY : pct(block.avg_win_pct)} tone={c.textSecondary} />
            <Metric size="sm" label="Avg loss" value={block.avg_loss_pct === null || block.avg_loss_pct === undefined ? EMPTY : pct(block.avg_loss_pct)} tone={c.textSecondary} />
            <Metric size="sm" label="Payoff" value={block.payoff_ratio === null || block.payoff_ratio === undefined ? EMPTY : `${block.payoff_ratio.toFixed(2)}×`} tone={c.textSecondary} />
            <Metric size="sm" label="Occurrences" value={block.occurrences === null || block.occurrences === undefined ? EMPTY : count(block.occurrences)} tone={c.textSecondary} hint={block.occurrences ? `${count(block.n)} were tradeable` : undefined} />
          </Row>
          <ProvenanceStrip provenance={block.provenance} n={block.n} />
        </>
      ) : null}
    </Stack>
  );
}

/**
 * n + date range + data source + cost convention, on the number, always.
 *
 * `n` is OPTIONAL here on purpose: an evidence bundle can carry provenance
 * without a trade count, and printing "n = 0 closed trades" in that case would
 * be inventing a number rather than reporting one.
 */
export function ProvenanceStrip({ provenance, n }: { provenance: Provenance; n?: number | null }) {
  const p = provenance;
  return (
    <Stack gap={space.xs}>
      <Label>How this was counted</Label>
      <Txt variant="caption" tone="muted" numeric>
        {n === null || n === undefined ? '' : `n = ${count(n)} closed trades · `}
        {rangeWithSpan(p.date_range)} · point-in-time as of {dateShort(p.as_of)}
      </Txt>
      <Txt variant="caption" tone="muted" mono>
        {p.data_source}
        {p.universe ? ` · ${p.universe}` : ''} · {p.computed_by}
      </Txt>
      <Txt variant="caption" tone="muted">
        {p.cost_convention}
      </Txt>
    </Stack>
  );
}

/** Both bases side by side -- history against forward. The comparison IS the story. */
export function PerformancePair({
  historical,
  virtual,
}: {
  historical?: PerformanceBlock | null;
  virtual?: PerformanceBlock | null;
}) {
  const { c } = useTheme();
  return (
    <Stack gap={space.lg}>
      {historical ? (
        <Card raised>
          <PerformanceView block={historical} />
        </Card>
      ) : null}
      {virtual ? (
        <Card raised accent={c.pathfinder}>
          <PerformanceView block={virtual} />
        </Card>
      ) : null}
      {historical && virtual ? (
        <Txt variant="small" tone="secondary">
          Forward is the number that counts. History is where the idea came from, not proof that it
          works.
        </Txt>
      ) : null}
    </Stack>
  );
}
