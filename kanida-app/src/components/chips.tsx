/**
 * The small honest chips every story and every depth screen is built from:
 * the n-flag, the evidence level, the decision, the verdict, the record label,
 * the experiment state -- and the engine-reported figure that has no fact card.
 */
import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react';
import { Modal, Pressable, StyleSheet, View } from 'react-native';

import type {
  Decision,
  EvidenceLevel,
  ExperimentState,
  GradingState,
  SampleFlag,
  Verdict,
} from '@/api/types';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { count, dateShort } from '@/lib/format';
import {
  decisionCopy,
  dueBasisShort,
  experimentStateCopy,
  gradingStatusCopy,
  levelCopy,
  recordShort,
  sampleCopy,
  sampleTone,
  toneColor,
  toneSoft,
  verdictCopy,
} from '@/lib/honesty';

import { Card, Divider, Label, Pill, Row, Stack, Txt } from './ui';

/** The n-flag chip. Shown wherever a statistic is. */
export function SampleChip({ n, flag }: { n: number | null | undefined; flag: SampleFlag }) {
  const { c } = useTheme();
  const tone = sampleTone(flag, c);
  return (
    <Pill fg={tone} bordered>
      {n === null || n === undefined ? 'n —' : `n = ${count(n)}`}
      {flag === 'ok' ? '' : ` · ${sampleCopy[flag].short}`}
    </Pill>
  );
}

/** same stock / peers / sector / whole market */
export function LevelChip({ level }: { level: EvidenceLevel }) {
  const { c } = useTheme();
  return (
    <Pill fg={c.pathfinder} bordered>
      {levelCopy[level].short}
    </Pill>
  );
}

export function DecisionPill({ decision }: { decision: Decision }) {
  const { c } = useTheme();
  const d = decisionCopy[decision];
  return (
    <Pill fg={toneColor(d.tone, c)} bg={toneSoft(d.tone, c)} bordered={d.tone === 'muted'}>
      {d.label}
    </Pill>
  );
}

export function VerdictPill({ verdict }: { verdict: Verdict }) {
  const { c } = useTheme();
  const v = verdictCopy[verdict];
  return (
    <Pill fg={toneColor(v.tone, c)} bg={toneSoft(v.tone, c)} bordered={v.tone === 'muted'}>
      {v.label}
    </Pill>
  );
}

export function StatePill({ state }: { state: ExperimentState }) {
  const { c } = useTheme();
  const s = experimentStateCopy[state];
  return (
    <Pill fg={toneColor(s.tone, c)} bg={toneSoft(s.tone, c)}>
      {s.label}
    </Pill>
  );
}

/**
 * The record label -- "simulated backfill" or "forward record". The engine's
 * string is the truth; the chip is the short form, the full sentence sits next
 * to it wherever there is room. Amber for a backfill so it can never pass for a
 * track record at a glance.
 */
export function RecordChip({ backfilled }: { backfilled: boolean }) {
  const { c } = useTheme();
  return (
    <Pill fg={backfilled ? c.caution : c.positive} bg={backfilled ? c.cautionSoft : c.positiveSoft}>
      {recordShort(backfilled)}
    </Pill>
  );
}

/** The full record sentence, verbatim from the engine. */
export function RecordLine({ label, backfilled }: { label: string; backfilled: boolean }) {
  const { c } = useTheme();
  return (
    <Row gap={space.sm} style={{ alignItems: 'flex-start' }}>
      <View
        style={{
          width: 3,
          alignSelf: 'stretch',
          borderRadius: 2,
          backgroundColor: backfilled ? c.caution : c.positive,
        }}
      />
      <Txt variant="caption" color={backfilled ? c.caution : c.positive} style={{ flex: 1 }}>
        {label}
      </Txt>
    </Row>
  );
}

/**
 * One line: the grading status, the date that matters, and the record it is published under.
 * When the engine has not stamped a due session on a pending card, the frozen rule's own
 * horizon is stated instead ("due N sessions after 29 Jul") — a parameter of the rule, not a
 * number the app invented.
 */
export function GradingLine({ grading, horizonSessions, editionDate }: { grading: GradingState; horizonSessions?: number; editionDate?: string }) {
  const { c } = useTheme();
  const when =
    grading.status === 'pending'
      ? grading.due_session
        ? `due ${dateShort(grading.due_session)}${dueBasisShort(grading.due_session_basis) ? ' (projected)' : ''}`
        : horizonSessions && editionDate
          ? `due ${horizonSessions} session${horizonSessions === 1 ? '' : 's'} after ${dateShort(editionDate)}`
          : 'due session not stamped by the engine'
      : grading.status === 'graded' || grading.status === 'void'
        ? grading.data_as_of
          ? `on data to ${dateShort(grading.data_as_of)}`
          : ''
        : '';
  return (
    <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
      {grading.verdict ? <VerdictPill verdict={grading.verdict} /> : null}
      <Txt variant="caption" tone="muted" style={{ flexShrink: 1 }}>
        {gradingStatusCopy[grading.status]}
        {when ? ` · ${when}` : ''}
      </Txt>
      <Pill fg={grading.backfilled ? c.caution : c.positive} bordered>
        {recordShort(grading.backfilled)}
      </Pill>
    </Row>
  );
}

// ── engine-reported figures ─────────────────────────────────────────────────

export type FigureDetails = {
  value: string;
  label: string;
  engine: string;
  asOf: string;
  /** what the figure is, in one sentence */
  what: string;
};

const FigureContext = createContext<(d: FigureDetails) => void>(() => {});

/**
 * Owns the sheet for engine-reported figures. Mounted once at the root, so an
 * `EngineFigure` can sit inline inside any Text on any platform.
 */
export function FigureSheetProvider({ children }: { children: ReactNode }) {
  const [active, setActive] = useState<FigureDetails | null>(null);
  const open = useCallback((d: FigureDetails) => setActive(d), []);
  const value = useMemo(() => open, [open]);
  return (
    <FigureContext.Provider value={value}>
      {children}
      <FigureSheet details={active} onClose={() => setActive(null)} />
    </FigureContext.Provider>
  );
}

/**
 * A figure the engine reported WITHOUT a fact card -- the counts and the
 * declined candidates' expectancies on the experiments registry, the scoreboard
 * tallies. It is still tappable, and the sheet says exactly what is and is not
 * known about it: which engine, as of which date, and that no n / window card
 * was served. It never pretends to be a Fact.
 */
export function EngineFigure({
  value,
  label,
  engine,
  asOf,
  what,
  variant = 'bodyStrong',
  color,
}: FigureDetails & {
  variant?: 'bodyStrong' | 'metricSm' | 'metric' | 'metricLg' | 'small';
  color?: string;
}) {
  const { c } = useTheme();
  const open = useContext(FigureContext);
  return (
    <Txt
      variant={variant}
      numeric
      color={color}
      onPress={() => open({ value, label, engine, asOf, what })}
      accessibilityRole="button"
      accessibilityLabel={`${label}: ${value}. Tap for the source.`}
      style={{ textDecorationLine: 'underline', textDecorationStyle: 'dotted', textDecorationColor: c.pathfinder }}>
      {value}
    </Txt>
  );
}

function FigureSheet({ details, onClose }: { details: FigureDetails | null; onClose: () => void }) {
  const { c } = useTheme();
  if (!details) return null;
  return (
    <Modal visible transparent animationType="fade" onRequestClose={onClose}>
      <Pressable accessibilityLabel="Close" onPress={onClose} style={[StyleSheet.absoluteFill, { backgroundColor: c.scrim }]} />
      <View style={figure.wrap} pointerEvents="box-none">
        <Card style={figure.card}>
          <Stack gap={space.lg}>
            <Stack gap={space.xs}>
              <Label>Engine-reported figure</Label>
              <Txt variant="metric" numeric>
                {details.value}
              </Txt>
              <Txt variant="small" tone="secondary">
                {details.label}
              </Txt>
            </Stack>
            <Txt variant="small" tone="secondary">
              {details.what}
            </Txt>
            <Divider />
            <Stack gap={space.md}>
              <Stack gap={2}>
                <Label>Reported by</Label>
                <Txt variant="small" tone="secondary" mono numeric>
                  {details.engine}
                </Txt>
              </Stack>
              <Stack gap={2}>
                <Label>As of</Label>
                <Txt variant="small" tone="secondary" numeric>
                  {dateShort(details.asOf)}
                </Txt>
              </Stack>
            </Stack>
            <Divider />
            <Txt variant="caption" tone="muted">
              This figure was reported by the deterministic engine as a count or a summary on its own
              record. No separate fact card (n, window, cost convention) was served for it — the app
              shows what it was given and says so, rather than inventing a provenance.
            </Txt>
            <Pressable onPress={onClose} accessibilityRole="button" style={[figure.close, { borderColor: c.borderStrong }]}>
              <Txt variant="smallStrong">Close</Txt>
            </Pressable>
          </Stack>
        </Card>
      </View>
    </Modal>
  );
}

const figure = StyleSheet.create({
  wrap: { flex: 1, justifyContent: 'flex-end', alignItems: 'center', padding: space.md },
  card: { width: '100%', maxWidth: 520, borderRadius: radius.lg },
  close: {
    alignSelf: 'flex-start',
    paddingHorizontal: space.lg,
    paddingVertical: space.sm,
    borderRadius: radius.pill,
    borderWidth: 1,
  },
});
