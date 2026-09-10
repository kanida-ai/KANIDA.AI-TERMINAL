/**
 * Unit tests for the pure logic the screens depend on.
 *
 *   node --test        (Node >= 22 strips the TypeScript types natively)
 *
 * These cover the parts where a quiet bug would produce a DISHONEST screen:
 * token resolution, sign handling, and the n-flag copy.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import { factValue, pct, pctAbs, rangeLabel, EMPTY } from '../src/lib/format.ts';
import { improvedCopy, PIPELINE, statusLabel, survives2xSlippage } from '../src/lib/honesty.ts';
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
  assert.equal(shortExperimentId('exp_0015'), '#0015');
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

test('drawdowns render as unsigned magnitudes', () => {
  assert.equal(pctAbs(12.7), '12.7%');
  assert.equal(pctAbs(-12.7), '12.7%');
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
  assert.equal(factValue('regime-dependent', 'text'), 'regime-dependent');
});

test('large counts use Indian digit grouping', () => {
  assert.equal(factValue(1482000, 'count'), '14,82,000');
});

test('a date range renders as month-year to month-year', () => {
  assert.equal(rangeLabel({ start: '2018-01-01', end: '2025-12-31' }), 'Jan 2018 – Dec 2025');
});

test('the 2x-slippage gate is strictly positive', () => {
  const base = {
    basis: 'historical_replay' as const,
    label: '',
    expectancy_pct_per_trade: 0.24,
    max_drawdown_pct: 0,
    current_drawdown_pct: 0,
    n: 1,
    sample_flag: 'ok' as const,
    provenance: {} as never,
  };
  assert.equal(survives2xSlippage({ ...base, expectancy_2x_slippage_pct_per_trade: 0.09 }), true);
  assert.equal(survives2xSlippage({ ...base, expectancy_2x_slippage_pct_per_trade: -0.03 }), false);
  // exactly zero is NOT an edge
  assert.equal(survives2xSlippage({ ...base, expectancy_2x_slippage_pct_per_trade: 0 }), false);
});

test('improved:null reads as "too early to say", never as a pass', () => {
  assert.equal(improvedCopy(null).tone, 'neutral');
  assert.match(improvedCopy(null).text, /too early/i);
  assert.equal(improvedCopy(false).tone, 'negative');
  assert.equal(improvedCopy(true).tone, 'positive');
});

test('a died experiment is labelled REJECTED, not hidden or softened', () => {
  assert.equal(statusLabel.died, 'REJECTED');
  // the pipeline is the funnel only -- rejected is rendered separately, on purpose
  assert.equal(
    PIPELINE.some((s) => s.status === 'died'),
    false,
  );
});
