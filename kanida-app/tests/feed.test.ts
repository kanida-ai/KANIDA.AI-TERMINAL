/**
 * The feed as stories: composition, paging and honesty -- pure, no server.
 *
 * Fixtures below are SHAPED like the S1/S2 contract but are synthetic: they
 * exist to pin how the client composes and orders, never to stand for results.
 */
import assert from 'node:assert/strict';
import test from 'node:test';

import type {
  ExperimentCard,
  ExperimentsResponse,
  Fact,
  FeedResponse,
  Finding,
  Scoreboard,
} from '../src/api/types.ts';
import { BACKFILL_LABEL, FORWARD_LABEL } from '../src/api/types.ts';
import { readsLikeInstruction, readsLikeOrder } from '../src/lib/honesty.ts';
import {
  buildStories,
  clampIndex,
  closestDeclined,
  findingsOf,
  indexOfStory,
  keyFacts,
  ledeOf,
  storyTitle,
} from '../src/lib/stories.ts';
import { hasUnrenderedTokens, parseSegments } from '../src/lib/tokens.ts';

// ── fixtures ─────────────────────────────────────────────────────────────────

const PROV = {
  data_source: 'kanida.db (synthetic fixture)',
  date_range: { start: '2013-01-01', end: '2026-07-29' },
  as_of: '2026-07-29',
  cost_convention: 'pf_cost_hurdle_v2',
  computed_by: 'pathfinder_research@1.3.0+code.test',
  computed_at: '2026-09-10T18:00:00+05:30',
  universe: 'nifty500_today',
  level: 'whole_market' as const,
};

function fact(id: string, value: number | string, unit: Fact['unit'], n: number | null = null): Fact {
  return {
    id,
    label: id,
    value,
    unit,
    n,
    sample_flag: n === null ? 'unknown' : n < 20 ? 'greyed' : n < 50 ? 'flagged' : 'ok',
    provenance: PROV,
  };
}

function finding(id: string, tier: Finding['tier'], rank: number, over: Partial<Finding> = {}): Finding {
  return {
    id,
    edition_date: '2026-07-29',
    rank,
    tier,
    template_id: 'dip',
    question: 'A stock just had a hard one-day fall. Does the dip bounce after costs?',
    subject: id.toUpperCase(),
    subject_kind: 'stock',
    decision: 'no_trade',
    decision_reason: 'A real drop, but the bounce does not beat costs.',
    narrative: {
      headline: 'The day’s hardest fall: history says do not buy the dip',
      body: `{{fact:${id}_subject}} fell {{fact:${id}_move}} today. Across {{fact:${id}_cases}} cases the bounce was {{fact:${id}_hit}}. It did not clear costs.`,
      produced_by: 'engine',
      at: '2026-09-10T18:00:00+05:30',
      fact_refs: [`${id}_subject`, `${id}_move`, `${id}_cases`, `${id}_hit`],
    },
    facts: [
      fact(`${id}_subject`, id.toUpperCase(), 'text'),
      fact(`${id}_move`, -12.8, 'pct'),
      fact(`${id}_cases`, 12368, 'count'),
      fact(`${id}_hit`, 50.6, 'pct', 12368),
      fact(`${id}_own_hit`, 55, 'pct', 12),
    ],
    key_fact_refs: [`${id}_subject`, `${id}_move`, `${id}_hit`, `${id}_own_hit`],
    provenance: {
      level: 'whole_market',
      n: 12368,
      sample_flag: 'ok',
      period: { start: '2013-01-01', end: '2026-07-29' },
      regime: 'NEUTRAL (risk score 46/100)',
      comparison_group: 'every stock on sessions it fell at least 6%',
      cost_hurdle_pct: 0.5,
      cost_convention: 'pf_cost_hurdle_v2',
      data_source: 'kanida.db (synthetic fixture)',
      universe: 'nifty500_today',
      as_of: '2026-07-29',
      computed_by: 'pathfinder_research@1.3.0+code.test',
      computed_at: '2026-09-10T18:00:00+05:30',
      disclosures: ['survivorship: today’s members on history'],
    },
    grading_rule: {
      kind: 'no_trade_call',
      horizon_sessions: 1,
      hurdle_pct: 0.5,
      metric: 'the move a long trade would have made',
      right: 'Right if the avoided trade would have lost more than the hurdle',
      wrong: 'Wrong if it would have cleared the hurdle',
      inconclusive: 'Inconclusive inside the hurdle',
      frozen_at: '2026-09-10T18:00:00+05:30',
      rule_version: 'grading_rules@1.3.0+code.test',
      spec: {},
    },
    grading: {
      status: 'pending',
      due_session: '2026-07-30',
      realized_facts: [],
      backfilled: true,
      record: BACKFILL_LABEL,
    },
    usefulness: { total: 0.7, evidence_strength: 0.6, novelty: 0.8, trader_relevance: 0.7, magnitude: 0.7, threshold: 0.65, version: 'ranking@1' },
    related_symbols: [id.toUpperCase()],
    follow_up_questions: ['Does the bounce depend on whether the whole market fell that day?'],
    disclosure: 'Research item, not a recommendation.',
    backfilled: true,
    ...over,
  };
}

const SCOREBOARD: Scoreboard = {
  right: 12,
  wrong: 2,
  inconclusive: 7,
  n: 21,
  sample_flag: 'flagged',
  pending: 4,
  by_template: { dip: { right: 2, wrong: 0, inconclusive: 1, n: 3, sample_flag: 'greyed' } },
  as_of: '2026-07-29',
  forward: { right: 0, wrong: 0, inconclusive: 0, n: 0, sample_flag: 'greyed' },
  backfilled: { right: 12, wrong: 2, inconclusive: 7, n: 21, sample_flag: 'flagged' },
  n_total: 21,
  n_independent: 21,
  continued: 4,
  regraded: 0,
  void: 0,
  record_label: BACKFILL_LABEL,
};

function feed(over: Partial<FeedResponse> = {}): FeedResponse {
  return {
    edition_date: '2026-07-29',
    data_as_of: '2026-07-29',
    generated_at: '2026-09-10T18:00:00+05:30',
    regime: 'NEUTRAL (risk score 46/100)',
    universe_scanned: 500,
    candidates_considered: 7,
    published_count: 4,
    continued_count: 0,
    usefulness_threshold: 0.65,
    what_matters_now: [finding('fnd_a', 'what_matters_now', 1), finding('fnd_b', 'what_matters_now', 2), finding('fnd_c', 'what_matters_now', 3)],
    discoveries: [finding('fnd_d', 'discovery', 4)],
    scoreboard: SCOREBOARD,
    llm_provider: 'none',
    disclosure: 'Pathfinder is a research lab.',
    backfilled: true,
    record_label: BACKFILL_LABEL,
    experiment_cards: [],
    experiments_scoreboard: {
      right: 0,
      wrong: 0,
      inconclusive: 0,
      n: 0,
      void: 0,
      pending: 0,
      forward: { right: 0, wrong: 0, inconclusive: 0, n: 0, sample_flag: 'greyed' },
      backfilled: { right: 0, wrong: 0, inconclusive: 0, n: 0, sample_flag: 'greyed' },
      by_family: {},
      experiments_testing: 0,
      experiments_buried: 0,
      experiments_proposed: 0,
      candidates_not_opened: 131,
      trials_total: 72,
      as_of: '2026-07-29',
      record_label: BACKFILL_LABEL,
      sample_flag: 'greyed',
    },
    ...over,
  };
}

const REGISTRY: ExperimentsResponse = {
  as_of: '2026-07-29',
  count: 0,
  items: [],
  not_opened: [
    { finding_id: 'fnd_x', edition_date: '2026-05-04', template_id: 'theme_cycle', family: null, trials_evaluated: 0, reason: 'no closed hypothesis family for template theme_cycle', best_rule_text: null, best_expectancy_net_pct: null, best_failed_gates: [] },
    { finding_id: 'fnd_y', edition_date: '2026-05-04', template_id: 'surge', family: 'surge_fade', trials_evaluated: 18, reason: 'not worth testing: none of 18 variants cleared the gate', best_rule_text: 'a virtual short position from the next session’s open, closed at the close 5 sessions later', best_expectancy_net_pct: 0.331, best_failed_gates: ['implementable_under_cost_convention'] },
    { finding_id: 'fnd_z', edition_date: '2026-07-29', template_id: 'dip', family: 'dip_bounce', trials_evaluated: 18, reason: 'not worth testing: none of 18 variants cleared the gate', best_rule_text: 'a virtual long position from the next session’s open, closed at the close 5 sessions later', best_expectancy_net_pct: 0.242, best_failed_gates: ['history_placebo=0.093 vs 0.05'] },
  ],
  scoreboard: feed().experiments_scoreboard!,
  llm_provider: 'none',
  engine_version: 'pathfinder_experiments@1.0.0+code.test',
  disclosure: 'Research experiment with virtual money.',
};

function card(over: Partial<ExperimentCard> = {}): ExperimentCard {
  const beats = ['noticed', 'researched', 'history_showed', 'decided', 'happened', 'learned', 'next'] as const;
  return {
    id: 'exp_dip_bounce_20260504',
    opened_edition: '2026-05-04',
    news_edition: '2026-07-29',
    state: 'testing',
    family: 'dip_bounce',
    theme: 'the bounce after a hard one-day fall, across the whole market',
    source_finding_id: 'fnd_a',
    evidence: finding('fnd_a', 'what_matters_now', 1).provenance,
    story: beats.map((beat) => ({
      beat,
      headline: `${beat} headline`,
      body: `${beat} body with {{fact:fct_x_n}} trades`,
      produced_by: 'engine' as const,
      at: '2026-09-10T18:00:00+05:30',
      fact_refs: ['fct_x_n'],
    })),
    facts: [fact('fct_x_n', 27, 'count')],
    versions_count: 1,
    trials_total: 18,
    periods_graded: 3,
    score: { right: 1, wrong: 0, inconclusive: 2, n: 3, sample_flag: 'greyed', void: 2 },
    latest_comparison: 'weaker',
    backfilled: true,
    opened_backfilled: true,
    record_label: BACKFILL_LABEL,
    llm_provider: 'none',
    disclosure: 'Research experiment with virtual money.',
    ...over,
  };
}

// ── composition ──────────────────────────────────────────────────────────────

test('the feed order is clarity first: what matters now, discoveries, experiments, scoreboard, next', () => {
  const stories = buildStories(feed(), REGISTRY);
  assert.deepEqual(
    stories.map((s) => s.kind),
    ['finding', 'finding', 'finding', 'finding', 'no_experiment', 'scoreboard', 'next'],
  );
  assert.deepEqual(stories.slice(0, 4).map((s) => s.id), ['fnd_a', 'fnd_b', 'fnd_c', 'fnd_d']);
  const first = stories[0];
  assert.equal(first.kind === 'finding' && first.tierIndex, 1);
  assert.equal(first.kind === 'finding' && first.tierTotal, 3);
  const disc = stories[3];
  assert.equal(disc.kind === 'finding' && disc.finding.tier, 'discovery');
});

test('a four-finding edition renders four finding stories — no padding', () => {
  const stories = buildStories(feed(), REGISTRY);
  assert.equal(stories.filter((s) => s.kind === 'finding').length, 4);
  assert.equal(findingsOf(feed()).length, 4);
});

test('an edition that published nothing renders ONE honest story, then the scoreboard — never a blank', () => {
  const stories = buildStories(feed({ what_matters_now: [], discoveries: [], published_count: 0, candidates_considered: 6 }), REGISTRY);
  assert.deepEqual(
    stories.map((s) => s.kind),
    ['empty_edition', 'no_experiment', 'scoreboard'],
  );
  const e = stories[0];
  assert.ok(e.kind === 'empty_edition' && e.candidatesConsidered === 6 && e.editionDate === '2026-07-29');
});

test('when no experiment opened, the story says so and names the closest variant and its failed gate', () => {
  const stories = buildStories(feed(), REGISTRY);
  const s = stories.find((x) => x.kind === 'no_experiment');
  assert.ok(s && s.kind === 'no_experiment');
  assert.equal(s.closest.length, 2, 'only the researched candidates are "closest"');
  assert.equal(s.closest[0].finding_id, 'fnd_z', 'fewest failed gates first; a statistical near-miss before an unimplementable one; then the latest edition');
  assert.equal(s.closest[1].finding_id, 'fnd_y');
  assert.deepEqual(s.closest[0].best_failed_gates, ['history_placebo=0.093 vs 0.05']);
  assert.equal(s.scoreboard?.trials_total, 72);
  assert.equal(s.scoreboard?.candidates_not_opened, 131);
});

test('the no-experiment story survives a registry that has not loaded, without inventing anything', () => {
  const stories = buildStories(feed(), null);
  const s = stories.find((x) => x.kind === 'no_experiment');
  assert.ok(s && s.kind === 'no_experiment');
  assert.equal(s.registry, null);
  assert.deepEqual(s.closest, []);
  assert.deepEqual(closestDeclined(null), []);
});

test('an experiment card on the edition becomes an experiment story in the feed, after the findings', () => {
  const stories = buildStories(feed({ experiment_cards: [card()] }), REGISTRY);
  assert.deepEqual(
    stories.map((s) => s.kind),
    ['finding', 'finding', 'finding', 'finding', 'experiment', 'scoreboard', 'next'],
  );
  const x = stories[4];
  assert.ok(x.kind === 'experiment' && x.card.story.length === 7);
  assert.equal(storyTitle(x), 'the bounce after a hard one-day fall, across the whole market');
});

test('the scoreboard story carries the record label and the forward/backfilled split verbatim', () => {
  const s = buildStories(feed(), REGISTRY).find((x) => x.kind === 'scoreboard');
  assert.ok(s && s.kind === 'scoreboard');
  assert.equal(s.recordLabel, BACKFILL_LABEL);
  assert.equal(s.backfilled, true);
  assert.equal(s.scoreboard.forward.n, 0);
  assert.equal(s.scoreboard.backfilled.n, 21);
  assert.equal(s.experiments?.trials_total, 72);
});

test('a forward edition is labelled forward, not backfilled', () => {
  const fwd = feed({ backfilled: false, record_label: FORWARD_LABEL });
  const s = buildStories(fwd, REGISTRY).find((x) => x.kind === 'scoreboard');
  assert.ok(s && s.kind === 'scoreboard' && s.backfilled === false && s.recordLabel === FORWARD_LABEL);
});

test('the next story collects every follow-up question and every pending horizon from the edition’s cards', () => {
  const s = buildStories(feed(), REGISTRY).find((x) => x.kind === 'next');
  assert.ok(s && s.kind === 'next');
  assert.equal(s.followUps.length, 4);
  assert.equal(s.pending.length, 4);
  assert.equal(s.pending[0].due, '2026-07-30');
  const graded = finding('fnd_g', 'discovery', 5, {
    follow_up_questions: [],
    grading: { status: 'graded', verdict: 'right', due_session: '2026-07-20', graded_at: '2026-09-10T18:00:00+05:30', data_as_of: '2026-07-21', realized_facts: [], backfilled: true, record: BACKFILL_LABEL },
  });
  const s2 = buildStories(feed({ what_matters_now: [graded], discoveries: [] }), REGISTRY);
  assert.equal(s2.some((x) => x.kind === 'next'), false, 'no next story when there is nothing to test next');
});

// ── paging / depth navigation ────────────────────────────────────────────────

test('the index is clamped inside the feed and a story can be found by id for back-navigation', () => {
  const stories = buildStories(feed(), REGISTRY);
  assert.equal(clampIndex(-3, stories.length), 0);
  assert.equal(clampIndex(99, stories.length), stories.length - 1);
  assert.equal(clampIndex(2.4, stories.length), 2);
  assert.equal(clampIndex(0, 0), 0);
  assert.equal(indexOfStory(stories, 'fnd_c'), 2);
  assert.equal(indexOfStory(stories, `scoreboard:2026-07-29`), 5);
  assert.equal(indexOfStory(stories, 'nope'), null);
  assert.equal(indexOfStory(stories, null), null);
});

// ── honesty rendering ────────────────────────────────────────────────────────

test('key facts are numeric, in the engine’s order, and greyed samples stay flagged', () => {
  const f = finding('fnd_a', 'what_matters_now', 1);
  const ks = keyFacts(f);
  assert.deepEqual(
    ks.map((k) => k.id),
    ['fnd_a_move', 'fnd_a_hit', 'fnd_a_own_hit'],
    'the text subject is skipped; numbers only',
  );
  assert.equal(ks[2].sample_flag, 'greyed');
  assert.equal(keyFacts(f, 1).length, 1);
});

test('the lede keeps whole sentences and never splits a fact token', () => {
  const body = 'A fell {{fact:fct_a}} today. Across {{fact:fct_b}} cases the bounce was {{fact:fct_c}}. It did not clear costs.';
  const lede = ledeOf(body, 2);
  assert.equal(lede, 'A fell {{fact:fct_a}} today. Across {{fact:fct_b}} cases the bounce was {{fact:fct_c}}.');
  for (const seg of parseSegments(lede)) {
    if (seg.kind === 'text') assert.ok(!seg.text.includes('{{') && !seg.text.includes('}}'));
  }
  assert.equal(ledeOf('One sentence only', 2), 'One sentence only');
});

test('every fact token in every story narrative resolves against the story’s own facts (no raw token can reach the screen)', () => {
  const stories = buildStories(feed({ experiment_cards: [card()] }), REGISTRY);
  for (const s of stories) {
    if (s.kind === 'finding') {
      const known = new Set(s.finding.facts.map((f) => f.id));
      for (const seg of parseSegments(s.finding.narrative.body)) if (seg.kind === 'fact') assert.ok(known.has(seg.id));
      assert.equal(hasUnrenderedTokens(s.finding.narrative.headline), false, 'a headline never carries a token');
    }
    if (s.kind === 'experiment') {
      const known = new Set(s.card.facts.map((f) => f.id));
      for (const line of s.card.story) for (const seg of parseSegments(line.body)) if (seg.kind === 'fact') assert.ok(known.has(seg.id));
    }
  }
});

test('no composed copy on the research surface reads like a trade instruction', () => {
  const stories = buildStories(feed({ experiment_cards: [card()] }), REGISTRY);
  for (const s of stories) {
    if (s.kind === 'finding') {
      assert.equal(readsLikeOrder(s.finding.decision_reason), false);
      assert.equal(readsLikeOrder(s.finding.narrative.headline), false);
      for (const q of s.finding.follow_up_questions) assert.equal(readsLikeOrder(q), false);
    }
    if (s.kind === 'experiment') {
      assert.equal(readsLikeInstruction(s.card.theme), false);
      for (const line of s.card.story) assert.equal(readsLikeInstruction(line.headline), false);
    }
    if (s.kind === 'no_experiment') {
      for (const r of s.closest) assert.equal(readsLikeInstruction(r.best_rule_text ?? ''), false);
    }
    assert.equal(readsLikeOrder(storyTitle(s)), false);
  }
});

test('the backfill label is present on every graded card and on the scoreboard, verbatim', () => {
  const f = feed();
  for (const x of findingsOf(f)) {
    assert.equal(x.grading.backfilled, f.backfilled);
    assert.equal(x.grading.record, BACKFILL_LABEL);
  }
  assert.equal(f.scoreboard.record_label, BACKFILL_LABEL);
  assert.equal(f.experiments_scoreboard?.record_label, BACKFILL_LABEL);
});

// ── integration polish: additive fields propagate, never invented ────────────

test('the next story carries each pending card\'s due-session basis verbatim, and the empty edition names the engine version when served', () => {
  const projected = finding('fnd_p', 'what_matters_now', 1, {
    grading: { status: 'pending', due_session: '2026-08-05', due_session_basis: 'projected: weekdays after the edition net of NSE closures', realized_facts: [], backfilled: true, record: BACKFILL_LABEL },
  });
  const stamped = finding('fnd_s', 'what_matters_now', 2);
  const s = buildStories(feed({ what_matters_now: [projected, stamped], discoveries: [], published_count: 2 }), REGISTRY).find((x) => x.kind === 'next');
  assert.ok(s && s.kind === 'next');
  assert.equal(s.pending[0].due, '2026-08-05');
  assert.match(String(s.pending[0].dueBasis), /^projected/);
  assert.equal(s.pending[1].dueBasis, null, 'no basis served means no basis claimed');
  const empty = buildStories(feed({ what_matters_now: [], discoveries: [], published_count: 0, engine_version: 'pathfinder_research@1.3.0+code.abc' }), REGISTRY)[0];
  assert.ok(empty.kind === 'empty_edition' && empty.engineVersion === 'pathfinder_research@1.3.0+code.abc');
  const bare = buildStories(feed({ what_matters_now: [], discoveries: [], published_count: 0 }), REGISTRY)[0];
  assert.ok(bare.kind === 'empty_edition' && bare.engineVersion === null);
});
