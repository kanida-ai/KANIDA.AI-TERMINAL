/**
 * The virtual book: capital, open positions, and the LOSERS-FIRST ledger.
 *
 * The engine sends `ledger_losers_first` already sorted ascending by net P&L.
 * This component renders it IN THE ORDER RECEIVED and offers no sort control.
 * That is deliberate: a "sort by best" button is how a losers-first track record
 * quietly becomes a winners-first one.
 *
 * Every row shows the signal bar and the entry bar separately, because entry is
 * the NEXT OPEN after the signal -- the point-in-time rule, visible in the data
 * rather than asserted in a footnote.
 */
import { useState } from 'react';
import { View } from 'react-native';

import type { VirtualBook, VirtualTrade } from '@/api/types';
import { useTheme } from '@/design/theme';
import { radius, space } from '@/design/tokens';
import { EMPTY, dateShort, inr, pct, pctAbs } from '@/lib/format';

import { PerformanceView } from './Performance';
import { Card, Divider, Label, Pill, Row, Stack, Touchable, Txt } from './ui';

function TradeRow({ trade, open = false }: { trade: VirtualTrade; open?: boolean }) {
  const { c } = useTheme();
  const net = trade.pnl_pct_net;
  const tone = net === null || net === undefined ? c.textMuted : net > 0 ? c.positive : net < 0 ? c.negative : c.textSecondary;

  return (
    <View style={{ paddingVertical: space.md, gap: 6 }}>
      <Row style={{ justifyContent: 'space-between', gap: space.sm }}>
        <Row gap={space.sm} style={{ flex: 1, flexWrap: 'wrap' }}>
          <Txt variant="bodyStrong" numeric>
            {trade.symbol}
          </Txt>
          <Pill fg={trade.direction === 'long' ? c.positive : c.negative} bordered>
            {trade.direction.toUpperCase()}
          </Pill>
          {open ? (
            <Pill fg={c.caution} bordered>
              OPEN
            </Pill>
          ) : null}
        </Row>
        <Txt variant="bodyStrong" numeric color={tone}>
          {open ? EMPTY : pct(net)}
        </Txt>
      </Row>

      <Row style={{ gap: space.md, flexWrap: 'wrap' }}>
        <Txt variant="caption" tone="muted" numeric>
          signal {dateShort(trade.signal_date)} → entry {dateShort(trade.entry_date)} @{' '}
          {trade.entry_price.toFixed(2)}
        </Txt>
        {trade.exit_date ? (
          <Txt variant="caption" tone="muted" numeric>
            exit {dateShort(trade.exit_date)}
            {trade.exit_price ? ` @ ${trade.exit_price.toFixed(2)}` : ''}
            {trade.exit_reason ? ` · ${trade.exit_reason}` : ''}
          </Txt>
        ) : null}
      </Row>

      {!open ? (
        <Row style={{ gap: space.md, flexWrap: 'wrap' }}>
          <Txt variant="caption" tone="muted" numeric>
            gross {pct(trade.pnl_pct_gross)}
          </Txt>
          <Txt variant="caption" tone="muted" numeric>
            costs {trade.costs_pct === null || trade.costs_pct === undefined ? EMPTY : pctAbs(trade.costs_pct, 2)}
          </Txt>
          <Txt variant="caption" tone="muted" numeric>
            slippage {trade.slippage_bps ?? EMPTY} bps
          </Txt>
          {trade.holding_sessions !== null && trade.holding_sessions !== undefined ? (
            <Txt variant="caption" tone="muted" numeric>
              held {trade.holding_sessions} session{trade.holding_sessions === 1 ? '' : 's'}
            </Txt>
          ) : null}
        </Row>
      ) : null}
    </View>
  );
}

const INITIAL_ROWS = 5;

export function LedgerCard({ book }: { book: VirtualBook }) {
  const { c } = useTheme();
  const [expanded, setExpanded] = useState(false);
  const closed = book.ledger_losers_first;
  const shown = expanded ? closed : closed.slice(0, INITIAL_ROWS);

  return (
    <Stack gap={space.lg}>
      <Card raised accent={c.pathfinder}>
        <Stack gap={space.lg}>
          <Row style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: space.sm }}>
            <Label>Virtual capital at work</Label>
            <Txt variant="metricSm" numeric>
              {inr(book.capital_inr)}
            </Txt>
          </Row>
          <Txt variant="caption" tone="muted">
            Virtual money. Kanida never places an order on your behalf.
          </Txt>
          <Divider />
          <PerformanceView block={book.metrics} />
        </Stack>
      </Card>

      {book.open_positions.length > 0 ? (
        <Card>
          <Stack gap={space.sm}>
            <Label>Open right now ({book.open_positions.length})</Label>
            {book.open_positions.map((t, i) => (
              <View key={t.id}>
                {i > 0 ? <Divider /> : null}
                <TradeRow trade={t} open />
              </View>
            ))}
          </Stack>
        </Card>
      ) : null}

      <Card>
        <Stack gap={space.sm}>
          <Stack gap={2}>
            <Label>Closed trades — worst first</Label>
            <Txt variant="caption" tone="muted">
              Every closed trade, sorted by net result, worst at the top. There is no sort control:
              the losses lead by design.
            </Txt>
          </Stack>

          {closed.length === 0 ? (
            <Txt variant="small" tone="greyed" style={{ paddingVertical: space.md }}>
              No trade has closed yet.
            </Txt>
          ) : (
            shown.map((t, i) => (
              <View key={t.id}>
                {i > 0 ? <Divider /> : null}
                <TradeRow trade={t} />
              </View>
            ))
          )}

          {closed.length > INITIAL_ROWS ? (
            <Touchable
              accessibilityRole="button"
              onPress={() => setExpanded((v) => !v)}
              style={{
                alignSelf: 'flex-start',
                paddingHorizontal: space.lg,
                paddingVertical: space.sm,
                borderRadius: radius.pill,
                borderWidth: 1,
                borderColor: c.borderStrong,
              }}>
              <Txt variant="smallStrong">
                {expanded ? 'Show fewer' : `Show all ${closed.length} closed trades`}
              </Txt>
            </Touchable>
          ) : null}
        </Stack>
      </Card>
    </Stack>
  );
}
