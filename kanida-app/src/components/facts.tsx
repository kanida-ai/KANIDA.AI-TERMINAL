/**
 * Fact rendering -- the client side of "the engine computes, the model narrates".
 *
 * The engine sends narrative with NO numerals in it, plus a `facts[]` array of
 * deterministic numbers. This module substitutes each `{{fact:...}}` token back
 * into the sentence as a tappable figure, and lets the reader open the number's
 * full provenance: n, sample flag, evidence level, window, data source, cost
 * convention, and the deterministic component that computed it.
 *
 * DESIGN DECISION -- inline facts are rendered in a NEUTRAL emphasis, not
 * green/red. A fact carries no semantic direction: `12.7%` is a good number when
 * it is a win rate and a bad one when it is a drawdown, and the sentence around
 * it already says which. Colour is reserved for the labelled metrics.
 */
import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react';
import { Modal, Pressable, ScrollView, StyleSheet, View } from 'react-native';
import { useRouter } from 'expo-router';

import type { Fact } from '@/api/types';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { EMPTY, dateShort, dateTimeIST, factValue, rangeWithSpan, unitHint } from '@/lib/format';
import { levelCopy, sampleCopy, sampleTone } from '@/lib/honesty';
import { parseSegments, shortExperimentId, shortVersion } from '@/lib/tokens';

import { Card, Divider, Label, Pill, Row, Stack, Txt } from './ui';

type FactsValue = {
  facts: Map<string, Fact>;
  open: (fact: Fact) => void;
};

const FactsContext = createContext<FactsValue>({
  facts: new Map(),
  open: () => {},
});

/**
 * Provides the facts a subtree may reference, and owns the provenance sheet.
 * Nest freely: an inner provider sees its own facts first, then the outer ones.
 */
export function FactsProvider({ facts, children }: { facts: Fact[]; children: ReactNode }) {
  const parent = useContext(FactsContext);
  const [active, setActive] = useState<Fact | null>(null);
  const open = useCallback((f: Fact) => setActive(f), []);
  const value = useMemo<FactsValue>(() => {
    const merged = new Map(parent.facts);
    for (const f of facts) merged.set(f.id, f);
    return { facts: merged, open };
  }, [facts, open, parent.facts]);
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

type Variant = 'body' | 'lede' | 'small' | 'smallStrong' | 'bodyStrong' | 'heading' | 'subheading' | 'title' | 'hero';

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
  numberOfLines,
  color,
}: {
  children: string;
  variant?: Variant;
  tone?: 'default' | 'secondary' | 'muted';
  style?: object;
  numberOfLines?: number;
  color?: string;
}) {
  const { c } = useTheme();
  const { facts, open } = useFacts();
  const router = useRouter();
  const segments = parseSegments(children);
  const factColor = color ?? c.text;

  return (
    <Txt variant={variant} tone={tone} color={color} style={style} numberOfLines={numberOfLines}>
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
              color={grey ? c.textGreyed : factColor}
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
          return (
            <Txt key={i} variant={variant} tone="secondary" style={{ fontWeight: '600' }}>
              {seg.id}
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

/** A tappable figure standing on its own (a key-fact tile, a row in the numbers table). */
export function FactPress({ fact, children, style }: { fact: Fact; children: ReactNode; style?: object }) {
  const { open } = useFacts();
  return (
    <Pressable
      onPress={() => open(fact)}
      accessibilityRole="button"
      accessibilityLabel={`${fact.label}: ${factValue(fact.value, fact.unit)}. Tap for provenance.`}
      style={({ pressed }) => [{ opacity: pressed ? 0.7 : 1 }, style]}>
      {children}
    </Pressable>
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
  const hasN = fact.n !== null && fact.n !== undefined;
  const isStatistic = hasN || ['pct', 'pct_per_trade', 'ratio', 'x'].includes(fact.unit);
  const isParameter = fact.sample_flag === 'not_applicable';

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

            {isParameter ? (
              <Txt variant="small" tone="secondary">
                {sampleCopy.not_applicable.long}
              </Txt>
            ) : isStatistic ? (
              <>
                <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                  <Pill fg={flagTone} bordered>
                    {hasN ? `n = ${fact.n}` : 'n —'}
                  </Pill>
                  <Pill fg={flagTone} bordered>
                    {sampleCopy[fact.sample_flag].short}
                  </Pill>
                  {p.level ? (
                    <Pill fg={c.pathfinder} bordered>
                      {levelCopy[p.level].short}
                    </Pill>
                  ) : null}
                </Row>
                {fact.sample_flag !== 'ok' ? (
                  <Txt variant="small" tone="secondary">
                    {sampleCopy[fact.sample_flag].long}
                  </Txt>
                ) : null}
              </>
            ) : (
              <Row gap={space.sm} style={{ flexWrap: 'wrap' }}>
                <Txt variant="small" tone="secondary" style={{ flexShrink: 1 }}>
                  A count or a label, not a statistic — there is no sample behind it to size.
                </Txt>
                {p.level ? (
                  <Pill fg={c.pathfinder} bordered>
                    {levelCopy[p.level].short}
                  </Pill>
                ) : null}
              </Row>
            )}

            <Divider />

            <Stack gap={space.md}>
              {p.level ? <SheetRow label="Evidence from" value={levelCopy[p.level].long} /> : null}
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
              Computed by the deterministic engine before it was written about. The language model
              may read this number; it never produces one.
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
