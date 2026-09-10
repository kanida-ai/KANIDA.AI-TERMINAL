/**
 * Evidence bundles, the post-mortem, and the model's own running cost.
 *
 * An evidence bundle is a DETERMINISTIC result the model is allowed to read and
 * interpret. So each card shows the computed block first and the model's reading
 * second, visibly attributed -- never the other way round.
 */
import { View } from 'react-native';

import type { Evidence, EvidenceKind, LlmUsage, PostMortem } from '@/api/types';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { count, dateTimeIST, humanise } from '@/lib/format';

import { RichText } from './facts';
import { PerformanceView, ProvenanceStrip } from './Performance';
import { Attribution } from './Story';
import { Card, Divider, Label, Pill, Row, Stack, Txt } from './ui';

const KIND_COPY: Record<EvidenceKind, string> = {
  historical_replay: 'Replay of the exact rule over history — not hold-to-close.',
  forward_virtual: 'Forward virtual money, marked daily.',
  regime_split: 'The same rule, split by market regime.',
  cost_sensitivity: 'What costs and slippage do to the edge.',
  placebo: 'The same test on a signal that should not work.',
  novelty_check: 'Whether this question has already been asked.',
};

export function EvidenceCard({ evidence }: { evidence: Evidence }) {
  const { c } = useTheme();
  return (
    <Card raised>
      <Stack gap={space.lg}>
        <Stack gap={space.xs}>
          <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
            <Label>{humanise(evidence.kind)}</Label>
            <Pill fg={c.textMuted} bordered>
              deterministic
            </Pill>
          </Row>
          <Txt variant="subheading">{evidence.title}</Txt>
          <Txt variant="caption" tone="muted">
            {KIND_COPY[evidence.kind]}
          </Txt>
        </Stack>

        {evidence.performance ? (
          <>
            <Divider />
            <PerformanceView block={evidence.performance} />
          </>
        ) : (
          // A provenance-only bundle (e.g. a novelty check). It has no trade
          // count, so none is shown — a zero here would be an invented number.
          <ProvenanceStrip provenance={evidence.provenance} />
        )}

        {evidence.interpretation ? (
          <>
            <Divider />
            <Stack gap={space.sm}>
              <Label>How I read it</Label>
              <RichText variant="bodyStrong">{evidence.interpretation.headline}</RichText>
              <RichText variant="body" tone="secondary">
                {evidence.interpretation.body}
              </RichText>
              <Attribution line={evidence.interpretation} />
            </Stack>
          </>
        ) : null}
      </Stack>
    </Card>
  );
}

/**
 * The post-mortem. Published on every dead experiment, in full, with what we
 * kept -- the learning that outlives the idea.
 */
export function PostMortemCard({ postMortem }: { postMortem: PostMortem }) {
  const { c } = useTheme();
  return (
    <Card accent={c.negative}>
      <Stack gap={space.lg}>
        <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
          <Label tone="inherit" style={{ color: c.negative }}>
            Post-mortem
          </Label>
          <Pill fg={c.negative} bg={c.negativeSoft}>
            {humanise(postMortem.cause)}
          </Pill>
        </Row>

        <RichText variant="heading">{postMortem.summary.headline}</RichText>
        <RichText variant="body" tone="secondary">
          {postMortem.summary.body}
        </RichText>
        <Attribution line={postMortem.summary} />

        <Divider />

        <View style={{ backgroundColor: c.positiveSoft, borderRadius: radius.md, padding: space.lg, gap: space.xs }}>
          <Label tone="inherit" style={{ color: c.positive }}>
            What we kept
          </Label>
          <Txt variant="body">{postMortem.what_we_kept}</Txt>
        </View>

        <Txt variant="caption" tone="muted">
          Retired {postMortem.retired_version} on {dateTimeIST(postMortem.died_at)}
          {postMortem.approved_by ? ` · approved by ${postMortem.approved_by}` : ''}.
        </Txt>
      </Stack>
    </Card>
  );
}

/**
 * Real token instrumentation from day one, and the hard daily cap.
 *
 * Showing the model's own running cost is part of the honesty: the reader can
 * see that the expensive thing (the model) runs rarely, and the cheap thing (the
 * deterministic observer) runs continuously.
 */
export function LlmUsageCard({ usage }: { usage: LlmUsage }) {
  const { c } = useTheme();
  const pctUsed = usage.budget_used_pct ?? null;
  return (
    <Card>
      <Stack gap={space.md}>
        <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
          <Label>{usage.window === 'today_ist' ? 'Model usage today (IST)' : 'Model usage, this experiment'}</Label>
          <Txt variant="smallStrong" numeric>
            ${usage.total_cost_usd.toFixed(2)}
            {usage.daily_budget_usd ? ` / $${usage.daily_budget_usd.toFixed(2)} cap` : ''}
          </Txt>
        </Row>

        {pctUsed !== null ? (
          <View style={{ height: 6, borderRadius: 3, backgroundColor: c.bgSunken, overflow: 'hidden' }}>
            <View
              style={{
                width: `${Math.min(100, Math.max(0, pctUsed))}%`,
                height: '100%',
                backgroundColor: pctUsed > 85 ? c.caution : c.pathfinder,
              }}
            />
          </View>
        ) : null}

        <Stack gap={space.xs}>
          {usage.by_model.map((m) => (
            <Row key={m.model} style={{ justifyContent: 'space-between', gap: space.sm }}>
              <Txt variant="caption" tone="secondary" mono>
                {m.model}
              </Txt>
              <Txt variant="caption" tone="muted" numeric>
                {count(m.calls)} calls · {count(m.input_tokens + m.output_tokens)} tokens · $
                {m.cost_usd.toFixed(2)}
              </Txt>
            </Row>
          ))}
        </Stack>

        <Txt variant="caption" tone="muted">
          The deterministic observer runs continuously and costs nothing to think with. The model is
          woken only when the observer finds something worth waking it for.
        </Txt>
      </Stack>
    </Card>
  );
}
