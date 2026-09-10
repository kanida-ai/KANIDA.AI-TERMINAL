/**
 * Contract tests — run against a LIVE research server, not against fixtures.
 *
 *   1) cd backend && KANIDA_PATHFINDER_SOURCE=research uvicorn pathfinder.mock_app:app --port 8010
 *   2) npm run test:contract
 *
 * `EXPO_PUBLIC_API_BASE_URL` picks the server. Each test asserts an assumption
 * the feed actually makes. If one fails, a story would be lying, not merely
 * broken:
 *
 *   - a `{{fact:...}}` the app cannot resolve would print "[missing figure]"
 *   - a digit inside a narrative would mean a number originated outside `facts[]`
 *   - a served card below the threshold would be padding
 *   - a scoreboard whose forward + backfilled != n would hide a backfill
 *   - a word like "entry" or "stop-loss" on a public card would be a trade instruction
 */
import assert from 'node:assert/strict';
import test, { before } from 'node:test';

import { readsLikeInstruction, readsLikeOrder } from '../src/lib/honesty.ts';
import { parseSegments } from '../src/lib/tokens.ts';

const BASE = (process.env.EXPO_PUBLIC_API_BASE_URL || 'http://127.0.0.1:8010').replace(/\/+$/, '');

type Json = Record<string, unknown>;

async function get(path: string): Promise<Json> {
  const res = await fetch(`${BASE}${path}`);
  assert.equal(res.status, 200, `GET ${path} -> ${res.status}`);
  return (await res.json()) as Json;
}

type Fact = { id: string; unit: string; n: number | null; sample_flag: string; value: unknown; provenance: Record<string, unknown> };
type Narrative = { headline: string; body: string; produced_by: string; model?: string | null; fact_refs: string[] };
type Finding = {
  id: string;
  rank: number;
  tier: string;
  template_id: string;
  question: string;
  subject: string;
  decision: string;
  decision_reason: string;
  narrative: Narrative;
  facts: Fact[];
  key_fact_refs: string[];
  provenance: Record<string, unknown> & { period: { start: string; end: string }; as_of: string; n: number; sample_flag: string; disclosures: string[] };
  grading_rule: Record<string, unknown> & { frozen_at: string; horizon_sessions: number; right: string; wrong: string; inconclusive: string };
  grading: { status: string; verdict: string | null; backfilled: boolean; record: string; realized_facts: Fact[]; continues: string | null; void_reason: string | null; due_session: string | null };
  usefulness: { total: number; threshold: number };
  backfilled: boolean;
  continues: string | null;
  follow_up_questions: string[];
};
type Counts = { right: number; wrong: number; inconclusive: number; n: number; sample_flag: string };
type Feed = Json & {
  edition_date: string;
  data_as_of: string;
  candidates_considered: number;
  published_count: number;
  continued_count: number;
  usefulness_threshold: number;
  what_matters_now: Finding[];
  discoveries: Finding[];
  backfilled: boolean;
  record_label: string;
  scoreboard: Counts & { forward: Counts; backfilled: Counts; n_total: number; n_independent: number; regraded: number; void: number; pending: number; record_label: string; by_template: Record<string, Counts> };
  experiment_cards: Card[];
  experiments_scoreboard: (Counts & { void: number; forward: Counts; backfilled: Counts; trials_total: number; candidates_not_opened: number; record_label: string }) | null;
};
type Card = {
  id: string;
  news_edition: string;
  state: string;
  theme: string;
  story: (Narrative & { beat: string })[];
  facts: Fact[];
  backfilled: boolean;
  record_label: string;
  trials_total: number;
  versions_count: number;
};
type Registry = Json & {
  count: number;
  items: Card[];
  not_opened: { finding_id: string; trials_evaluated: number; reason: string; best_rule_text: string | null; best_failed_gates: string[] }[];
  scoreboard: Counts & { void: number; forward: Counts; backfilled: Counts; trials_total: number; candidates_not_opened: number; record_label: string };
  engine_version: string;
  disclosure: string;
};

let feed: Feed;
let registry: Registry;
let editions: Feed[] = [];

const BACKFILL_LABEL = 'simulated backfill — generated after the fact; not a forward track record';
const FORWARD_LABEL = 'forward record — generated on its own session date, before the outcome';

function findings(f: Feed): Finding[] {
  return [...f.what_matters_now, ...f.discoveries];
}

function bareText(s: string): string {
  return parseSegments(s)
    .filter((x) => x.kind === 'text')
    .map((x) => (x as { text: string }).text)
    .join('');
}

/** Walk back a few sessions from the latest edition to collect a handful of real editions. */
async function priorEditions(latest: string, want: number): Promise<Feed[]> {
  const out: Feed[] = [];
  const d = new Date(latest + 'T00:00:00Z');
  for (let i = 0; i < 40 && out.length < want; i += 1) {
    d.setUTCDate(d.getUTCDate() - 1);
    const iso = d.toISOString().slice(0, 10);
    const res = await fetch(`${BASE}/api/pathfinder/feed?date=${iso}`);
    if (res.status === 200) out.push((await res.json()) as Feed);
  }
  return out;
}

before(async () => {
  try {
    feed = (await get('/api/pathfinder/feed')) as Feed;
  } catch (err) {
    throw new Error(
      `Could not reach the Pathfinder research API at ${BASE}. Start it with:\n` +
        `  cd backend && KANIDA_PATHFINDER_SOURCE=research uvicorn pathfinder.mock_app:app --port 8010\n` +
        `(original error: ${(err as Error).message})`,
    );
  }
  registry = (await get('/api/pathfinder/experiments')) as Registry;
  editions = [feed, ...(await priorEditions(feed.edition_date, 6))];
});

// ── the feed ─────────────────────────────────────────────────────────────────

test('the latest edition serves clarity first: at most three in what_matters_now, ranks strictly increasing, counts honest', () => {
  assert.ok(feed.what_matters_now.length <= 3, 'what matters now is two or three, never a list');
  for (const f of feed.what_matters_now) assert.equal(f.tier, 'what_matters_now');
  for (const f of feed.discoveries) assert.equal(f.tier, 'discovery');
  const ranks = findings(feed).map((f) => f.rank);
  assert.deepEqual(ranks, [...ranks].sort((a, b) => a - b));
  assert.equal(new Set(ranks).size, ranks.length);
  assert.equal(findings(feed).length, feed.published_count + feed.continued_count, 'nothing served that was not counted');
  assert.equal(findings(feed).filter((f) => f.continues).length, feed.continued_count);
});

test('nothing served is below the usefulness threshold — the feed is never padded', () => {
  for (const ed of editions) {
    for (const f of findings(ed)) {
      assert.ok(f.usefulness.total >= ed.usefulness_threshold, `${f.id} at ${f.usefulness.total} below ${ed.usefulness_threshold}`);
    }
  }
});

test('every narrative is digit-free outside its fact tokens, and every token resolves to a served fact', () => {
  let checked = 0;
  for (const ed of editions) {
    for (const f of findings(ed)) {
      checked += 1;
      assert.ok(!/\d/.test(f.narrative.headline), `digit in headline of ${f.id}`);
      assert.ok(!/\d/.test(bareText(f.narrative.body)), `digit outside a token in the body of ${f.id}`);
      assert.ok(!/\d/.test(bareText(f.decision_reason)), `digit in decision_reason of ${f.id}`);
      const known = new Set([...f.facts.map((x) => x.id), ...f.grading.realized_facts.map((x) => x.id)]);
      for (const seg of [...parseSegments(f.narrative.body), ...parseSegments(f.narrative.headline)]) {
        if (seg.kind === 'fact') assert.ok(known.has(seg.id), `unresolvable fact ${seg.id} on ${f.id} — the app would render "[missing figure]"`);
      }
      for (const id of f.key_fact_refs) assert.ok(known.has(id), `key fact ${id} not served on ${f.id}`);
      assert.ok(f.narrative.fact_refs.length >= 1, 'a narrative cites at least one fact');
      if (f.narrative.produced_by === 'llm') assert.ok(f.narrative.model, 'a model-narrated body names its model');
      else assert.ok(!f.narrative.model, 'an engine-narrated body names no model');
    }
  }
  assert.ok(checked >= 1, 'no findings served across the editions probed');
});

test('every fact carries provenance with a window that ends no later than its as_of, and no model computed it', () => {
  let n = 0;
  for (const ed of editions) {
    for (const f of findings(ed)) {
      for (const fact of [...f.facts, ...f.grading.realized_facts]) {
        n += 1;
        const p = fact.provenance as { data_source: string; date_range: { start: string; end: string }; as_of: string; cost_convention: string; computed_by: string; computed_at: string };
        for (const key of ['data_source', 'date_range', 'as_of', 'cost_convention', 'computed_by', 'computed_at']) {
          assert.ok(p[key as keyof typeof p], `fact ${fact.id}: provenance.${key} missing`);
        }
        assert.ok(p.date_range.end <= p.as_of, `fact ${fact.id}: window ends after as_of`);
        assert.ok(!/claude|gpt|gemini|sonnet|haiku|opus|llm/i.test(p.computed_by), `fact ${fact.id}: computed_by names a model`);
        if (fact.sample_flag === 'not_applicable') assert.equal(fact.n, null, 'a parameter / observation carries no n');
        if (['pct', 'pct_per_trade', 'ratio', 'x'].includes(fact.unit) && fact.sample_flag !== 'not_applicable') {
          assert.equal(typeof fact.n, 'number', `statistic ${fact.id} has no n`);
        }
      }
    }
  }
  assert.ok(n >= 10, `expected many facts, saw ${n}`);
});

test('every card carries the addendum-7 provenance: level, n, period, regime, comparison group, cost hurdle, disclosures', () => {
  for (const ed of editions) {
    for (const f of findings(ed)) {
      const p = f.provenance;
      assert.ok(['same_stock', 'peer_group', 'sector', 'whole_market'].includes(String(p.level)), `${f.id}: level`);
      assert.equal(typeof p.n, 'number');
      assert.ok(p.period.start <= p.period.end && p.period.end <= p.as_of, `${f.id}: period / as_of`);
      assert.ok(String(p.regime).length > 0);
      assert.ok(String(p.comparison_group).length > 10, `${f.id}: comparison group missing`);
      assert.equal(typeof p.cost_hurdle_pct, 'number');
      assert.ok(Number(p.cost_hurdle_pct) > 0, 'a cost hurdle of zero would be no hurdle');
      assert.ok(Array.isArray(p.disclosures) && p.disclosures.length >= 1, `${f.id}: data disclosures missing`);
      const expected = p.n < 20 ? 'greyed' : p.n < 50 ? 'flagged' : 'ok';
      assert.equal(p.sample_flag, expected, `${f.id}: sample flag disagrees with n`);
    }
  }
});

test('the grading rule was frozen at publication and states Right / Wrong / Inconclusive; the grading state is consistent', () => {
  for (const ed of editions) {
    for (const f of findings(ed)) {
      const r = f.grading_rule;
      assert.ok(r.right && r.wrong && r.inconclusive, `${f.id}: rule legs missing`);
      assert.ok(r.horizon_sessions >= 1);
      assert.ok(String(r.frozen_at).length > 0 && String(r.rule_version).length > 0);
      const g = f.grading;
      if (g.status === 'graded') assert.ok(['right', 'wrong', 'inconclusive'].includes(String(g.verdict)), `${f.id}: graded without a verdict`);
      if (g.status === 'void') assert.ok(g.verdict === 'void' && g.void_reason, `${f.id}: void without a reason`);
      if (g.status === 'pending') assert.equal(g.verdict, null);
      if (g.status === 'continued') assert.ok(g.continues, `${f.id}: continued without the finding it continues`);
      assert.equal(g.backfilled, f.backfilled, `${f.id}: card and grade disagree on backfilled`);
      assert.equal(g.record, g.backfilled ? BACKFILL_LABEL : FORWARD_LABEL, `${f.id}: record label does not match backfilled`);
    }
  }
});

test('the backfill label is on the edition, on every card and on the scoreboard — and the split sums', () => {
  for (const ed of editions) {
    assert.equal(ed.record_label, ed.backfilled ? BACKFILL_LABEL : FORWARD_LABEL);
    for (const f of findings(ed)) assert.equal(f.backfilled, ed.backfilled);
    const sb = ed.scoreboard;
    assert.equal(sb.right + sb.wrong + sb.inconclusive, sb.n, 'counts must sum to n');
    assert.equal(sb.forward.n + sb.backfilled.n, sb.n, 'forward + backfilled must equal n');
    assert.equal(sb.n_independent, sb.n);
    assert.ok(sb.n_total >= sb.n);
    assert.equal(sb.regraded, sb.n_total - sb.n);
    assert.ok(sb.record_label.length > 0);
    if (sb.forward.n === 0 && sb.n > 0) assert.equal(sb.record_label, BACKFILL_LABEL, 'a scoreboard with no forward grades must say it is a backfill');
    for (const v of Object.values(sb.by_template)) assert.equal(v.right + v.wrong + v.inconclusive, v.n);
    const xs = ed.experiments_scoreboard;
    if (xs) {
      assert.equal(xs.right + xs.wrong + xs.inconclusive, xs.n);
      assert.equal(xs.forward.n + xs.backfilled.n, xs.n);
      assert.ok(xs.record_label.length > 0);
    }
  }
});

test('no card, question, reason or follow-up on the research surface reads like an order (entry / target / stop / execution)', () => {
  // S1 debunks legitimately say "do not buy the dip"; what must never appear is the shape of an order.
  for (const ed of editions) {
    for (const f of findings(ed)) {
      assert.equal(readsLikeOrder(f.narrative.headline), false, `${f.id}: headline`);
      assert.equal(readsLikeOrder(f.narrative.body), false, `${f.id}: body`);
      assert.equal(readsLikeOrder(f.decision_reason), false, `${f.id}: decision_reason`);
      assert.equal(readsLikeOrder(f.question), false, `${f.id}: question`);
      for (const q of f.follow_up_questions) assert.equal(readsLikeOrder(q), false, `${f.id}: follow-up`);
    }
  }
  const banned = /"(target|target_price|stop_loss|stop|entry_price|expected_return|projection|price_target)"\s*:/i;
  for (const ed of editions) assert.ok(!banned.test(JSON.stringify(ed)), 'a promise- or instruction-shaped field reached the client');
});

test('an edition that published nothing is still a valid, honest edition (when one exists in the window probed)', () => {
  const empty = editions.filter((ed) => findings(ed).length === 0);
  for (const ed of empty) {
    assert.equal(ed.published_count, 0);
    assert.ok(ed.candidates_considered >= 0);
    assert.ok(ed.scoreboard, 'the scoreboard still rides on an empty edition');
    assert.ok(ed.record_label.length > 0);
  }
});

test('the feed guards its errors: a malformed date is a 400, an unknown edition a 404, no internals leak', async () => {
  const bad = await fetch(`${BASE}/api/pathfinder/feed?date=DROP`);
  assert.equal(bad.status, 400);
  const missing = await fetch(`${BASE}/api/pathfinder/feed?date=1999-01-01`);
  assert.equal(missing.status, 404);
  for (const res of [bad, missing]) {
    const body = (await res.json()) as { error?: { code: string; message: string } };
    assert.ok(body.error?.code, 'the guarded error shape is missing');
    assert.ok(!/Traceback|SELECT |sqlite|psycopg/i.test(body.error!.message), 'error leaks internals');
  }
});

// ── the experiments registry ─────────────────────────────────────────────────

test('the registry counts agree with its items, lists losers first, and carries the record label', () => {
  assert.equal(registry.count, registry.items.length);
  const order: Record<string, number> = { buried: 0, testing: 1, proposed: 2 };
  const ranks = registry.items.map((i) => order[i.state]);
  assert.deepEqual(ranks, [...ranks].sort((a, b) => a - b), 'losers first: buried, then testing, proposed last');
  const sb = registry.scoreboard;
  assert.equal(sb.right + sb.wrong + sb.inconclusive, sb.n);
  assert.equal(sb.forward.n + sb.backfilled.n, sb.n);
  assert.ok(sb.record_label.length > 0);
  assert.ok(String(registry.engine_version).startsWith('pathfinder_experiments@'));
  assert.match(registry.disclosure, /not a recommendation/i);
});

test('every declined candidate is on the record with a reason; the researched ones name the closest variant and its failed gates', () => {
  assert.ok(registry.not_opened.length + registry.count >= 1, 'a research product with nothing on the record');
  assert.equal(registry.scoreboard.candidates_not_opened, registry.not_opened.length);
  let trials = 0;
  for (const r of registry.not_opened) {
    assert.ok(r.reason.length > 10, `${r.finding_id}: no reason`);
    trials += r.trials_evaluated;
    if (r.trials_evaluated > 0) {
      assert.ok(r.best_rule_text, `${r.finding_id}: researched but no closest variant`);
      assert.ok(r.best_failed_gates.length >= 1, `${r.finding_id}: declined but no failed gate named`);
      assert.equal(readsLikeInstruction(r.best_rule_text!), false, `${r.finding_id}: rule text reads like an instruction`);
    }
  }
  assert.ok(registry.scoreboard.trials_total >= trials, 'trials_total counts at least every declined trial');
});

test('every public experiment card tells the seven beats in order, digit-free, with resolvable facts and no instruction words', () => {
  const beats = ['noticed', 'researched', 'history_showed', 'decided', 'happened', 'learned', 'next'];
  const cards = [...registry.items, ...editions.flatMap((ed) => ed.experiment_cards)];
  for (const c of cards) {
    assert.deepEqual(
      c.story.map((s) => s.beat),
      beats,
    );
    const known = new Set(c.facts.map((f) => f.id));
    for (const line of c.story) {
      assert.ok(!/\d/.test(line.headline), `${c.id}: digit in a beat headline`);
      assert.ok(!/\d/.test(bareText(line.body)), `${c.id}: digit outside a token in beat ${line.beat}`);
      for (const seg of parseSegments(line.body)) if (seg.kind === 'fact') assert.ok(known.has(seg.id), `${c.id}: unresolvable ${seg.id}`);
      assert.equal(readsLikeInstruction(line.headline), false);
      assert.equal(readsLikeInstruction(line.body), false);
    }
    assert.equal(readsLikeInstruction(c.theme), false);
    assert.equal(c.record_label, c.backfilled ? BACKFILL_LABEL : FORWARD_LABEL);
    assert.ok(!('constituents' in c) && !('basket' in c), 'a public card has no field for a constituent list');
    assert.ok(c.trials_total >= 1 && c.versions_count >= 1);
  }
  for (const ed of editions) for (const c of ed.experiment_cards) assert.equal(c.news_edition, ed.edition_date, 'a card is served on the edition its story was written on');
});

test('an experiment record, when one exists, carries versions, trials, periods and a withheld or RA-reviewed basket', async () => {
  for (const item of registry.items) {
    const rec = (await get(`/api/pathfinder/experiment/${item.id}`)) as Json & {
      versions: { version: number; periods: { status: string; verdict: string | null; backfilled: boolean; record: string }[]; expectation: { period: { end: string }; seal: string; computed_by: string } }[];
      trials: unknown[];
      trials_total: number;
      basket: { constituents_visibility: string; constituents: string[] };
      state: string;
      post_mortem: unknown;
    };
    assert.ok(rec.versions.length >= 1);
    assert.equal(rec.trials.length, rec.trials_total);
    for (const v of rec.versions) {
      assert.ok(v.expectation.period.end <= v.expectation.seal, 'an expectation never sees past its seal');
      assert.ok(!/claude|gpt|gemini|llm/i.test(v.expectation.computed_by));
      for (const p of v.periods) {
        if (p.status === 'graded') assert.ok(['right', 'wrong', 'inconclusive'].includes(String(p.verdict)));
        assert.equal(p.record, p.backfilled ? BACKFILL_LABEL : FORWARD_LABEL);
      }
    }
    if (rec.basket.constituents.length > 0) assert.equal(rec.basket.constituents_visibility, 'in_app_ra_reviewed');
    assert.equal(rec.state === 'buried', rec.post_mortem !== null && rec.post_mortem !== undefined, 'buried <=> post-mortem');
  }
});

test('the experiment endpoint guards its errors: a malformed id is a 400, an unknown id a 404', async () => {
  const bad = await fetch(`${BASE}/api/pathfinder/experiment/DROP`);
  assert.equal(bad.status, 400);
  const missing = await fetch(`${BASE}/api/pathfinder/experiment/exp_nope`);
  assert.equal(missing.status, 404);
  for (const res of [bad, missing]) {
    const body = (await res.json()) as { error?: { code: string; message: string } };
    assert.ok(body.error?.code);
    assert.ok(!/Traceback|SELECT |sqlite|psycopg/i.test(body.error!.message));
  }
});

test('every payload carries its research disclosure', () => {
  for (const ed of editions) assert.match(String(ed.disclosure), /research lab/i);
  for (const ed of editions) for (const f of findings(ed)) assert.match(String((f as unknown as { disclosure: string }).disclosure), /not a recommendation/i);
  assert.match(registry.disclosure, /research/i);
});
