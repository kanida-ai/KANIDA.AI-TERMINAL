/**
 * Fact rendering -- the client side of "the LLM never calculates".
 *
 * The engine sends narrative with NO numerals in it, plus a `facts[]` array of
 * deterministic numbers. This module substitutes each `{{fact:...}}` token back
 * into the sentence as a tappable figure, and lets the reader open the number's
 * full provenance: n, date range, data source, cost convention, and the
 * deterministic component that computed it.
 *
 * DESIGN DECISION -- inline facts are rendered in a NEUTRAL emphasis, not
 * green/red. A fact carries no semantic direction: `12.7%` is a good number when
 * it is a win rate and a bad one when it is a drawdown, and the sentence around
 * it already says which. Colour is reserved for the metric components, where the
 * field name tells us what "up" means. Miscolouring a drawdown green would be a
 * dishonest chart of one number.
 */
import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react';
import { Modal, Pressable, ScrollView, StyleSheet, View } from 'react-native';
import { useRouter } from 'expo-router';

import type { Evidence, Fact, StoryLine } from '@/api/types';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import {
  EMPTY,
  dateShort,
  dateTimeIST,
  factValue,
  rangeWithSpan,
  unitHint,
} from '@/lib/format';
import { sampleCopy, sampleTone } from '@/lib/honesty';
import { parseSegments, shortExperimentId, shortVersion } from '@/lib/tokens';

import { Card, Divider, Label, Pill, Row, Stack, Txt } from './ui';

type FactsValue = {
  facts: Map<string, Fact>;
  evidence: Map<string, Evidence>;
  open: (fact: Fact) => void;
};

const FactsContext = createContext<FactsValue>({
  facts: new Map(),
  evidence: new Map(),
  open: () => {},
});

export function FactsProvider({
  facts,
  evidence = [],
  children,
}: {
  facts: Fact[];
  evidence?: Evidence[];
  children: ReactNode;
}) {
  const [active, setActive] = useState<Fact | null>(null);
  const open = useCallback((f: Fact) => setActive(f), []);
  const value = useMemo<FactsValue>(
    () => ({
      facts: new Map(facts.map((f) => [f.id, f])),
      evidence: new Map(evidence.map((e) => [e.id, e])),
      open,
    }),
    [facts, evidence, open],
  );
  return (
    <FactsContext.Provider value={value}>
      {children}
      <FactSheet fact={active} onClose={() => setActive(null)} />
    </FactsContext.Provider>
  );
}

export function useFacts() {
  return useContext(FactsContext);
}

/**
 * Prose with reference tokens resolved.
 *
 * If a fact id is missing from `facts[]` we do NOT print the raw token and we do
 * not silently drop the sentence -- we show a visible placeholder, because a
 * number that cannot be traced is a bug we want to see, not hide.
 */
export function RichText({
  children,
  variant = 'body',
  tone = 'default',
  style,
}: {
  children: string;
  variant?: 'body' | 'small' | 'bodyStrong' | 'heading' | 'subheading' | 'title';
  tone?: 'default' | 'secondary' | 'muted';
  style?: object;
}) {
  const { c } = useTheme();
  const { facts, evidence, open } = useFacts();
  const router = useRouter();
  const segments = parseSegments(children);

  return (
    <Txt variant={variant} tone={tone} style={style}>
      {segments.map((seg, i) => {
        if (seg.kind === 'text') return seg.text;

        if (seg.kind === 'fact') {
          const fact = facts.get(seg.id);
          if (!fact) {
            return (
              <Txt key={i} variant={variant} color={c.caution}>
                [missing figure]
              </Txt>
            );
          }
          const grey = fact.sample_flag === 'greyed';
          return (
            <Txt
              key={i}
              variant={variant}
              numeric
              onPress={() => open(fact)}
              accessibilityRole="button"
              accessibilityLabel={`${fact.label}: ${factValue(fact.value, fact.unit)}. Tap for provenance.`}
              color={grey ? c.textGreyed : c.text}
              style={{
                fontWeight: '700',
                textDecorationLine: 'underline',
                textDecorationStyle: 'dotted',
                textDecorationColor: c.pathfinder,
              }}>
              {factValue(fact.value, fact.unit)}
            </Txt>
          );
        }

        if (seg.kind === 'exp') {
          return (
            <Txt
              key={i}
              variant={variant}
              numeric
              color={c.pathfinder}
              accessibilityRole="link"
              onPress={() => router.push(`/pathfinder/experiment/${seg.id}`)}
              style={{ fontWeight: '700' }}>
              {shortExperimentId(seg.id)}
            </Txt>
          );
        }

        if (seg.kind === 'evd') {
          const ev = evidence.get(seg.id);
          return (
            <Txt key={i} variant={variant} tone="secondary" style={{ fontWeight: '600' }}>
              {ev ? ev.title : seg.id}
            </Txt>
          );
        }

        return (
          <Txt key={i} variant={variant} numeric mono color={c.textSecondary}>
            {shortVersion(seg.id)}
          </Txt>
        );
      })}
    </Txt>
  );
}

/** Headline of a story beat, with tokens resolved. */
export function RichHeadline({ line, style }: { line: StoryLine; style?: object }) {
  return (
    <RichText variant="heading" style={style}>
      {line.headline}
    </RichText>
  );
}

/**
 * The provenance sheet. This is where "every number carries n + date range +
 * data source + cost convention" actually lands for the customer.
 */
function FactSheet({ fact, onClose }: { fact: Fact | null; onClose: () => void }) {
  const { c } = useTheme();
  if (!fact) return null;
  const p = fact.provenance;
  const flagTone = sampleTone(fact.sample_flag, c);
  const isStatistic =
    fact.n !== null && fact.n !== undefined
      ? true
      : ['pct', 'pct_per_trade', 'ratio', 'x'].includes(fact.unit);

  return (
    <Modal visible transparent animationType="fade" onRequestClose={onClose}>
      <Pressable
        accessibilityLabel="Close"
        onPress={onClose}
        style={[StyleSheet.absoluteFill, { backgroundColor: c.scrim }]}
      />
      <View style={sheet.wrap} pointerEvents="box-none">
        <Card style={sheet.card}>
          <ScrollView contentContainerStyle={{ padding: space.xl, gap: space.lg }}>
            <Stack gap={space.xs}>
              <Label>Deterministic figure</Label>
              <Txt variant="metric" numeric>
                {factValue(fact.value, fact.unit)}
              </Txt>
              <Txt variant="small" tone="secondary">
                {fact.label} · {unitHint(fact.unit)}
              </Txt>
            </Stack>

            {/*
              n only means something for a STATISTIC. A raw count ("conditions
              evaluated today") has no sample behind it, and stamping it
              "no sample yet" would invent a caveat rather than remove one.
            */}
            {isStatistic ? (
              <>
                <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                  <Pill fg={flagTone} bordered>
                    {fact.n === null || fact.n === undefined ? 'n —' : `n = ${fact.n}`}
                  </Pill>
                  <Pill fg={flagTone} bordered>
                    {sampleCopy[fact.sample_flag].short}
                  </Pill>
                </Row>
                {fact.sample_flag !== 'ok' ? (
                  <Txt variant="small" tone="secondary">
                    {sampleCopy[fact.sample_flag].long}
                  </Txt>
                ) : null}
              </>
            ) : (
              <Txt variant="small" tone="secondary">
                A direct observation, not a statistic — there is no sample behind it to size.
              </Txt>
            )}

            <Divider />

            <Stack gap={space.md}>
              <SheetRow label="Window" value={rangeWithSpan(p.date_range)} />
              <SheetRow label="Point-in-time as of" value={dateShort(p.as_of)} />
              <SheetRow label="Data source" value={p.data_source} mono />
              <SheetRow label="Universe" value={p.universe ?? EMPTY} mono />
              <SheetRow label="Cost convention" value={p.cost_convention} />
              <SheetRow label="Computed by" value={p.computed_by} mono />
              <SheetRow label="Computed at" value={dateTimeIST(p.computed_at)} />
            </Stack>

            {fact.note ? (
              <>
                <Divider />
                <Txt variant="small" tone="secondary">
                  {fact.note}
                </Txt>
              </>
            ) : null}

            <Divider />
            <Txt variant="caption" tone="muted">
              Computed by the deterministic engine. The language model reads this number; it never
              produces one.
            </Txt>

            <Pressable
              onPress={onClose}
              accessibilityRole="button"
              style={[sheet.close, { borderColor: c.borderStrong }]}>
              <Txt variant="smallStrong">Close</Txt>
            </Pressable>
          </ScrollView>
        </Card>
      </View>
    </Modal>
  );
}

function SheetRow({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <Stack gap={2}>
      <Label>{label}</Label>
      <Txt variant="small" tone="secondary" mono={mono} numeric={mono}>
        {value}
      </Txt>
    </Stack>
  );
}

const sheet = StyleSheet.create({
  wrap: { flex: 1, justifyContent: 'flex-end', alignItems: 'center', padding: space.md },
  card: { width: '100%', maxWidth: 520, maxHeight: '86%', padding: 0, borderRadius: radius.lg },
  close: {
    alignSelf: 'flex-start',
    paddingHorizontal: space.lg,
    paddingVertical: space.sm,
    borderRadius: radius.pill,
    borderWidth: 1,
  },
});
