/**
 * Unit tests for the pure logic the screens depend on.
 *
 *   node --test        (Node >= 22 strips the TypeScript types natively)
 *
 * These cover the parts where a quiet bug would produce a DISHONEST screen:
 * token resolution, sign handling, the n-flag copy, and the compliance lint.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { factValue, pct, pctAbs, rangeLabel, EMPTY } from '../src/lib/format.ts';
import {
  decisionCopy,
  improvedCopy,
  isDebunk,
  publicSafe,
  readsLikeInstruction,
  readsLikeOrder,
  researchSafe,
  recordShort,
  sampleCopy,
  storyKindFor,
  storyLabelFor,
  verdictCopy,
  WITHHELD,
} from '../src/lib/honesty.ts';
import {
  factIdsIn,
  hasUnrenderedTokens,
  parseSegments,
  shortExperimentId,
  shortVersion,
} from '../src/lib/tokens.ts';

test('parseSegments splits prose around every reference token, in order', () => {
  const segs = parseSegments(
    'It delivered {{fact:fct_e7_virt_exp}} across {{fact:fct_e7_virt_n}} trades in {{exp:exp_0007}}.',
  );
  assert.deepEqual(
    segs.map((s) => s.kind),
    ['text', 'fact', 'text', 'fact', 'text', 'exp', 'text'],
  );
  assert.equal((segs[1] as { id: string }).id, 'fct_e7_virt_exp');
  assert.equal((segs[5] as { id: string }).id, 'exp_0007');
});

test('parseSegments handles prose with no tokens and empty prose', () => {
  assert.deepEqual(parseSegments('plain'), [{ kind: 'text', text: 'plain' }]);
  assert.deepEqual(parseSegments(''), [{ kind: 'text', text: '' }]);
});

test('a token at the very start or end is not swallowed', () => {
  const a = parseSegments('{{fact:fct_a}} led.');
  assert.equal(a[0].kind, 'fact');
  const b = parseSegments('It ended at {{fact:fct_b}}');
  assert.equal(b[b.length - 1].kind, 'fact');
});

test('factIdsIn returns every fact referenced, and only facts', () => {
  assert.deepEqual(factIdsIn('{{fact:fct_a}} and {{exp:exp_1}} and {{fact:fct_b}}'), [
    'fct_a',
    'fct_b',
  ]);
});

test('hasUnrenderedTokens catches a raw token reaching the screen', () => {
  assert.equal(hasUnrenderedTokens('expectancy of {{fact:fct_x}}'), true);
  assert.equal(hasUnrenderedTokens('expectancy of +0.24%'), false);
});

test('experiment ids render as the mockup chip', () => {
  assert.equal(shortExperimentId('exp_0007'), '#0007');
  assert.equal(shortExperimentId('exp_dip_bounce_20260504'), '#dip_bounce_20260504');
});

test('versions shorten without losing the number', () => {
  assert.equal(shortVersion('strategy@v2.1'), 'v2.1');
  assert.equal(shortVersion('constitution@1.3.0'), '1.3.0');
  assert.equal(shortVersion('v1'), 'v1');
});

test('a signed percentage never loses its sign', () => {
  assert.equal(pct(0.24), '+0.24%');
  assert.equal(pct(-0.03), '−0.03%'); // true minus, not a hyphen
  assert.equal(pct(0), '0.00%');
  assert.equal(pct(null), EMPTY);
});

test('drawdowns render as unsigned magnitudes — never a plus on a drawdown', () => {
  assert.equal(pctAbs(12.7), '12.7%');
  assert.equal(pctAbs(-12.7), '12.7%');
  assert.equal(pctAbs(0.5, 2), '0.50%');
  assert.ok(!pctAbs(12.7).includes('+'));
});

test('a fact renders in its declared unit, never a guessed one', () => {
  assert.equal(factValue(0.24, 'pct_per_trade'), '0.24%');
  assert.equal(factValue(0.24, 'pct_per_trade', 'always'), '+0.24%');
  assert.equal(factValue(-0.11, 'pct_per_trade'), '−0.11%'); // a loss always shows its minus
  assert.equal(factValue(12.7, 'pct'), '12.7%'); // a drawdown is never rendered as a gain
  assert.equal(factValue(5, 'sessions'), '5 sessions');
  assert.equal(factValue(1, 'sessions'), '1 session');
  assert.equal(factValue(1.45, 'x'), '1.45×');
  assert.equal(factValue(10, 'bps'), '10 bps');
  assert.equal(factValue('NEUTRAL', 'text'), 'NEUTRAL');
});

test('large counts use Indian digit grouping', () => {
  assert.equal(factValue(1482000, 'count'), '14,82,000');
});

test('a date range renders as month-year to month-year', () => {
  assert.equal(rangeLabel({ start: '2018-01-01', end: '2025-12-31' }), 'Jan 2018 – Dec 2025');
});

test('improved:null reads as "too early to say", never as a pass', () => {
  assert.equal(improvedCopy(null).tone, 'neutral');
  assert.match(improvedCopy(null).text, /too early/i);
  assert.equal(improvedCopy(false).tone, 'negative');
  assert.equal(improvedCopy(true).tone, 'positive');
});

test('every sample flag has copy, and the small ones say so', () => {
  assert.match(sampleCopy.flagged.short, /small/i);
  assert.match(sampleCopy.greyed.short, /too few/i);
  assert.match(sampleCopy.not_applicable.long, /not a statistic/i);
});

test('a backfill is named a backfill, a forward record a forward record', () => {
  assert.equal(recordShort(true), 'Simulated backfill');
  assert.equal(recordShort(false), 'Forward record');
});

test('every decision and every verdict has customer copy that is not a promise', () => {
  for (const d of Object.values(decisionCopy)) {
    assert.ok(d.label.length > 0 && d.meaning.length > 0);
    assert.ok(!/guarantee|will return|target price/i.test(d.meaning));
  }
  assert.equal(verdictCopy.wrong.label, 'Wrong');
  assert.equal(verdictCopy.void.label, 'Void');
});

test('a no_trade or reject is a debunk; the story type follows the engine template', () => {
  assert.equal(isDebunk({ decision: 'no_trade' }), true);
  assert.equal(isDebunk({ decision: 'reject' }), true);
  assert.equal(isDebunk({ decision: 'watch' }), false);
  assert.equal(storyKindFor({ template_id: 'theme_cycle', subject_kind: 'sector' }), 'theme');
  assert.equal(storyKindFor({ template_id: 'dip', subject_kind: 'stock' }), 'stock');
  assert.equal(storyKindFor({ template_id: 'surge', subject_kind: 'stock' }), 'stock');
  assert.equal(storyKindFor({ template_id: 'volume_anomaly', subject_kind: 'stock' }), 'volume');
  assert.equal(storyKindFor({ template_id: 'relationship', subject_kind: 'pair' }), 'pair');
  assert.equal(storyKindFor({ template_id: 'market_regime', subject_kind: 'market' }), 'market');
  assert.equal(storyLabelFor({ template_id: 'surge', subject_kind: 'stock', decision: 'reject' }), 'Stock behaviour · Debunk');
  assert.equal(storyLabelFor({ template_id: 'theme_cycle', subject_kind: 'sector', decision: 'watch' }), 'Theme in play');
});

test('the compliance lint withholds a trade instruction and passes research prose', () => {
  assert.equal(readsLikeInstruction('Buy the dip at the open'), true);
  assert.equal(readsLikeInstruction('place a stop-loss below the low'), true);
  assert.equal(readsLikeInstruction('the target is the prior high'), true);
  assert.equal(readsLikeInstruction('History says a one-day flip like this is usually noise; not chasing it.'), false);
  assert.equal(
    readsLikeInstruction('a virtual long position from the next session\'s open, closed at the close 5 sessions later'),
    false,
  );
  assert.equal(publicSafe('Sell everything'), WITHHELD);
  // the S1 research-card lint: the engine's own debunks say "do not buy the dip" — a finding, not an order
  assert.equal(readsLikeOrder('The day’s hardest fall: history says do not buy the dip'), false);
  assert.equal(readsLikeOrder('Buying big surges lost money after costs, in median and in expectancy'), false);
  assert.equal(readsLikeOrder('entry at the open with a stop-loss below the low'), true);
  assert.equal(readsLikeOrder('the target is the prior high'), true);
  assert.equal(researchSafe('execute at the close'), WITHHELD);
  assert.equal(publicSafe('A real drop, but the bounce does not beat costs.'), 'A real drop, but the bounce does not beat costs.');
});
