/**
 * The loop story -- the thing the whole product is about.
 *
 * Six beats, always in this order:
 *   I noticed -> I am testing this because -> the virtual experiment ->
 *   what happened -> what I learned and changed -> what I will test next
 *
 * UX law (docs/sessions/PATHFINDER.md): the STORY dominates, metrics support.
 * So a beat is a full-width block with a headline you can read from across the
 * room, and every figure inside it is a tappable deterministic fact.
 *
 * Each beat also states WHO wrote it. That is not a footnote -- it is the core
 * principle made visible: the engine computes, the model narrates, and the
 * reader can always tell which they are looking at.
 */
import { View } from 'react-native';

import type { StoryBeat, StoryLine, Trigger } from '@/api/types';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { dateTimeIST, humanise, timeIST } from '@/lib/format';

import { RichText } from './facts';
import { Card, Label, Pill, Row, Stack, Txt } from './ui';

/** The beat headings, in the agent's own voice. */
export const BEAT_LABEL: Record<StoryBeat, string> = {
  noticed: 'I noticed',
  hypothesis: 'I am testing this because',
  experiment: 'The virtual experiment',
  outcome: 'What happened',
  learning: 'What I learned and changed',
  next: 'What I will test next',
};

export const BEAT_ORDER: StoryBeat[] = [
  'noticed',
  'hypothesis',
  'experiment',
  'outcome',
  'learning',
  'next',
];

function beatColor(beat: StoryBeat, c: ReturnType<typeof useTheme>['c']): string {
  switch (beat) {
    case 'noticed':
      return c.neutral;
    case 'hypothesis':
      return c.pathfinder;
    case 'experiment':
      return c.pathfinderDeep;
    case 'outcome':
      return c.caution;
    case 'learning':
      return c.positive;
    case 'next':
      return c.textMuted;
  }
}

/** Model ids are internal; the reader sees a plain name. */
function modelName(model: string | null | undefined): string {
  if (!model) return '';
  if (model.startsWith('claude-sonnet')) return 'Claude Sonnet 5';
  if (model.startsWith('claude-haiku')) return 'Claude Haiku 4.5';
  if (model.startsWith('claude-opus')) return 'Claude Opus 5';
  return model;
}

/** "Written by ... / Computed by ..." -- who authored this beat. */
export function Attribution({ line }: { line: StoryLine }) {
  const { c } = useTheme();
  if (line.produced_by === 'engine') {
    return (
      <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
        <Pill fg={c.textMuted} bordered>
          Deterministic engine
        </Pill>
        <Txt variant="caption" tone="muted">
          Computed, not written. {timeIST(line.at)}
        </Txt>
      </Row>
    );
  }
  if (line.produced_by === 'human') {
    return (
      <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
        <Pill fg={c.caution} bordered>
          Human decision
        </Pill>
        <Txt variant="caption" tone="muted">
          {timeIST(line.at)}
        </Txt>
      </Row>
    );
  }
  return (
    <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
      <Pill fg={c.pathfinder} bg={c.pathfinderSoft}>
        {modelName(line.model)}
      </Pill>
      <Txt variant="caption" tone="muted">
        Interpreted the engine{'’'}s numbers · every figure above is computed, not written ·{' '}
        {timeIST(line.at)}
      </Txt>
    </Row>
  );
}

/** One beat of the loop. */
export function StoryBeatCard({
  line,
  index,
  total,
  showRail = true,
  showLabel = true,
}: {
  line: StoryLine;
  index?: number;
  total?: number;
  showRail?: boolean;
  /** the detail screen already names the beat in its section heading */
  showLabel?: boolean;
}) {
  const { c } = useTheme();
  const color = beatColor(line.beat, c);
  const isLast = index !== undefined && total !== undefined && index === total - 1;

  return (
    <Row style={{ alignItems: 'stretch', gap: space.md }}>
      {showRail ? (
        <View style={{ width: 18, alignItems: 'center' }}>
          <View
            style={{
              width: 11,
              height: 11,
              borderRadius: radius.pill,
              backgroundColor: color,
              marginTop: space.xl,
            }}
          />
          {!isLast ? (
            <View style={{ flex: 1, width: 2, backgroundColor: c.border, marginTop: 4 }} />
          ) : null}
        </View>
      ) : null}

      <Card style={{ flex: 1 }} accent={showRail ? undefined : color}>
        <Stack gap={space.md}>
          {showLabel ? (
            <Label tone="inherit" style={{ color }}>
              {BEAT_LABEL[line.beat]}
            </Label>
          ) : null}
          <RichText variant="heading">{line.headline}</RichText>
          <RichText variant="body" tone="secondary">
            {line.body}
          </RichText>
          <Attribution line={line} />
        </Stack>
      </Card>
    </Row>
  );
}

/** The whole story, in beat order, as one connected thread. */
export function StoryThread({ story, rail = true }: { story: StoryLine[]; rail?: boolean }) {
  const ordered = [...story].sort(
    (a, b) => BEAT_ORDER.indexOf(a.beat) - BEAT_ORDER.indexOf(b.beat),
  );
  return (
    <Stack gap={space.md}>
      {ordered.map((line, i) => (
        <StoryBeatCard
          key={`${line.beat}-${i}`}
          line={line}
          index={i}
          total={ordered.length}
          showRail={rail}
        />
      ))}
    </Stack>
  );
}

/**
 * What woke the model up. Autonomy here means continuous cheap observation and
 * INTELLIGENT ACTIVATION -- so the trigger is shown as a first-class fact, and
 * it always says `fired_by: engine`. A clock never wakes the model.
 */
export function TriggerCard({ trigger, title = 'What woke me' }: { trigger: Trigger; title?: string }) {
  const { c } = useTheme();
  return (
    <Card raised accent={c.neutral}>
      <Stack gap={space.sm}>
        <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
          <Label tone="inherit" style={{ color: c.neutral }}>
            {title}
          </Label>
          <Pill fg={c.textMuted} bordered>
            {humanise(trigger.type)}
          </Pill>
        </Row>
        <RichText variant="body" tone="secondary">
          {trigger.description}
        </RichText>
        <Txt variant="caption" tone="muted">
          Fired by the deterministic observer at {dateTimeIST(trigger.fired_at)}. The language model
          is woken by evidence, never by a clock.
        </Txt>
      </Stack>
    </Card>
  );
}
