/**
 * The change-log -- the governance record, rendered as customer-facing copy.
 *
 * Every entry answers, in this order and without exception:
 *
 *   what changed -> why -> on what evidence -> previous version -> new version
 *   -> did performance actually improve?
 *
 * The learning HIERARCHY is on every entry, because the level is the promise:
 *   L1 evidence     appended automatically
 *   L2 parameter    tuned inside a human-approved range
 *   L3 strategy     a new version -- must backtest AND forward-validate to replace
 *   L4 constitution HUMAN ONLY. The agent learns inside it; it cannot rewrite it.
 *
 * `improved: null` renders as "too early to say", never as a quiet success.
 */
import { View } from 'react-native';

import type { ChangeLogEntry, LearningLevel } from '@/api/types';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { dateTimeIST, pct } from '@/lib/format';
import { improvedCopy } from '@/lib/honesty';

import { Card, Divider, Label, Pill, Row, Stack, Txt } from './ui';

export const LEVEL_MEANING: Record<LearningLevel, string> = {
  L1: 'Evidence — a new observation appended automatically.',
  L2: 'Parameter — tuned inside a range a human approved.',
  L3: 'Strategy — a new version. It must backtest and forward-validate before it may replace anything.',
  L4: 'Constitution — human-controlled. The agent can learn inside it; it cannot rewrite it.',
};

function levelTone(level: LearningLevel, c: ReturnType<typeof useTheme>['c']) {
  switch (level) {
    case 'L1':
      return { fg: c.textSecondary, bg: 'transparent' };
    case 'L2':
      return { fg: c.neutral, bg: c.neutralSoft };
    case 'L3':
      return { fg: c.pathfinder, bg: c.pathfinderSoft };
    case 'L4':
      return { fg: c.caution, bg: c.cautionSoft };
  }
}

export function LevelPill({ level }: { level: LearningLevel }) {
  const { c } = useTheme();
  const tone = levelTone(level, c);
  return (
    <Pill fg={tone.fg} bg={tone.bg} bordered={level === 'L1'}>
      {level}
    </Pill>
  );
}

function VersionArrow({ from, to }: { from?: string | null; to: string }) {
  const { c } = useTheme();
  return (
    <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
      <Txt variant="small" tone="muted" mono numeric>
        {from ?? 'first version'}
      </Txt>
      <Txt variant="small" tone="muted">
        →
      </Txt>
      <Txt variant="small" mono numeric color={c.text} style={{ fontWeight: '700' }}>
        {to}
      </Txt>
    </Row>
  );
}

export function ChangeLogItem({ entry }: { entry: ChangeLogEntry }) {
  const { c } = useTheme();
  const verdict = improvedCopy(entry.improved);
  const verdictColor =
    verdict.tone === 'positive' ? c.positive : verdict.tone === 'negative' ? c.negative : c.caution;

  const before = entry.performance_before?.expectancy_pct_per_trade;
  const after = entry.performance_after?.expectancy_pct_per_trade;

  return (
    <Card raised>
      <Stack gap={space.md}>
        <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
          <Row gap={space.sm}>
            <Txt variant="caption" tone="muted" numeric>
              #{entry.seq}
            </Txt>
            <LevelPill level={entry.level} />
            {entry.decided_by === 'human' ? (
              <Pill fg={c.caution} bordered>
                Human
              </Pill>
            ) : entry.decided_by === 'engine' ? (
              <Pill fg={c.textMuted} bordered>
                Engine
              </Pill>
            ) : null}
          </Row>
          <Txt variant="caption" tone="muted">
            {dateTimeIST(entry.at)}
          </Txt>
        </Row>

        <Stack gap={space.xs}>
          <Label>What changed</Label>
          <Txt variant="bodyStrong">{entry.what_changed}</Txt>
        </Stack>

        <Stack gap={space.xs}>
          <Label>Why</Label>
          <Txt variant="body" tone="secondary">
            {entry.why}
          </Txt>
        </Stack>

        <Stack gap={space.xs}>
          <Label>Version</Label>
          <VersionArrow from={entry.previous_version} to={entry.new_version} />
        </Stack>

        {entry.validation ? (
          <Stack gap={space.xs}>
            <Label>Validation before it could replace anything</Label>
            <Txt variant="small" tone="secondary">
              {entry.validation}
            </Txt>
          </Stack>
        ) : null}

        <Divider />

        <View style={{ backgroundColor: c.bgSunken, borderRadius: radius.md, padding: space.md, gap: space.sm }}>
          <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
            <Txt variant="smallStrong" color={verdictColor}>
              {verdict.text}
            </Txt>
            {before !== undefined || after !== undefined ? (
              <Row gap={space.sm}>
                <Txt variant="small" tone="muted" numeric>
                  {before === undefined ? '—' : pct(before)}
                </Txt>
                <Txt variant="small" tone="muted">
                  →
                </Txt>
                <Txt variant="small" numeric tone="secondary">
                  {after === undefined ? '—' : pct(after)}
                </Txt>
                <Txt variant="caption" tone="muted">
                  expectancy / trade
                </Txt>
              </Row>
            ) : null}
          </Row>
          <Txt variant="caption" tone="muted">
            {LEVEL_MEANING[entry.level]}
            {entry.approved_by ? ` Approved by ${entry.approved_by}.` : ''} Recorded against{' '}
            {entry.constitution_version}.
          </Txt>
        </View>
      </Stack>
    </Card>
  );
}

export function ChangeLog({ entries }: { entries: ChangeLogEntry[] }) {
  // Append-only: the engine guarantees strictly increasing `seq`. We render in
  // the order received and never re-sort.
  return (
    <Stack gap={space.md}>
      {entries.map((e) => (
        <ChangeLogItem key={e.seq} entry={e} />
      ))}
    </Stack>
  );
}
