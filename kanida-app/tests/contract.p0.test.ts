/**
 * Contract tests — run against a LIVE server, not against fixtures.
 *
 *   1) cd backend && uvicorn pathfinder.mock_app:app --port 8010
 *   2) node --test tests/contract.test.ts
 *
 * Point `EXPO_PUBLIC_API_BASE_URL` at the P1 engine and this same file becomes
 * the acceptance suite for the swap: if the real engine passes it, every screen
 * in this app renders honestly against real data with no code change.
 *
 * Each test asserts an assumption the UI actually makes. If one fails, a screen
 * is lying, not merely broken:
 *
 *   - a `{{fact:...}}` the app cannot resolve would print "[missing figure]"
 *   - a numeral inside LLM prose would mean the model computed something
 *   - an unsorted ledger would put winners first
 *   - a performance block without a drawdown would show a naked return
 */
import assert from 'node:assert/strict';
import test, { before } from 'node:test';

import { parseSegments } from '../src/lib/tokens.ts';

const BASE = (process.env.EXPO_PUBLIC_API_BASE_URL || 'http://127.0.0.1:8010').replace(/\/+$/, '');

type Json = Record<string, unknown>;

async function get(path: string): Promise<Json> {
  const res = await fetch(`${BASE}${path}`);
  assert.equal(res.status, 200, `GET ${path} -> ${res.status}`);
  return (await res.json()) as Json;
}

let loop: Json;
let list: Json;
let learnings: Json;
let details: Json[] = [];

before(async () => {
  try {
    loop = await get('/api/pathfinder/loop');
  } catch (err) {
    throw new Error(
      `Could not reach the Pathfinder API at ${BASE}. Start it with:\n` +
        `  cd backend && uvicorn pathfinder.mock_app:app --port 8010\n` +
        `(original error: ${(err as Error).message})`,
    );
  }
  list = await get('/api/pathfinder/experiments');
  learnings = await get('/api/pathfinder/learnings');
  const items = list.items as { id: string }[];
  details = await Promise.all(items.map((i) => get(`/api/pathfinder/experiment/${i.id}`)));
  assert.ok(details.length > 0, 'no experiments served');
});

// ── helpers ──────────────────────────────────────────────────────────────────

type Fact = { id: string; unit: string; n: number | null; sample_flag: string };
type Line = { headline: string; body: string; produced_by: string; model?: string | null; beat: string };
type Perf = Record<string, unknown> & { provenance: Record<string, unknown> };

function storyLinesOf(payload: Json): Line[] {
  const out: Line[] = [];
  const push = (l: unknown) => {
    if (l && typeof l === 'object' && 'produced_by' in l) out.push(l as Line);
  };
  (payload.story as Line[] | undefined)?.forEach(push);
  (payload.evidence as { interpretation?: Line }[] | undefined)?.forEach((e) => push(e.interpretation));
  push((payload.post_mortem as { summary?: Line } | undefined)?.summary);
  (payload.learned as { statement?: Line }[] | undefined)?.forEach((l) => push(l.statement));
  (payload.testing_next as { why_now?: Line }[] | undefined)?.forEach((n) => push(n.why_now));
  return out;
}

function performanceBlocksOf(node: unknown, acc: Perf[] = []): Perf[] {
  if (!node || typeof node !== 'object') return acc;
  if (Array.isArray(node)) {
    node.forEach((n) => performanceBlocksOf(n, acc));
    return acc;
  }
  const obj = node as Record<string, unknown>;
  if ('expectancy_pct_per_trade' in obj && 'provenance' in obj) acc.push(obj as Perf);
  Object.values(obj).forEach((v) => performanceBlocksOf(v, acc));
  return acc;
}

// ── the tests ────────────────────────────────────────────────────────────────

test('every fact token in every payload resolves to a served fact', () => {
  for (const payload of [loop, learnings, ...details]) {
    const known = new Set((payload.facts as Fact[]).map((f) => f.id));
    for (const line of storyLinesOf(payload)) {
      for (const seg of [...parseSegments(line.headline), ...parseSegments(line.body)]) {
        if (seg.kind === 'fact') {
          assert.ok(
            known.has(seg.id),
            `unresolvable fact ${seg.id} in beat "${line.beat}" — the app would render "[missing figure]"`,
          );
        }
      }
    }
  }
});

test('LLM-authored prose contains no literal numeral — the model never computes', () => {
  let checked = 0;
  for (const payload of [loop, learnings, ...details]) {
    for (const line of storyLinesOf(payload)) {
      if (line.produced_by !== 'llm') continue;
      checked += 1;
      assert.ok(line.model, 'an llm-authored line must name its model');
      for (const field of ['headline', 'body'] as const) {
        const bare = parseSegments(line[field])
          .filter((s) => s.kind === 'text')
          .map((s) => (s as { text: string }).text)
          .join('');
        assert.ok(
          !/\d/.test(bare),
          `numeral in LLM-authored ${field} ("${line[field]}") — a number originated in the model`,
        );
      }
    }
  }
  assert.ok(checked >= 20, `expected a meaningful number of LLM lines, saw ${checked}`);
});

test('the models used are only the ones the gateway routes to', () => {
  const allowed = new Set(['claude-sonnet-5', 'claude-haiku-4-5', 'claude-opus-5']);
  for (const payload of [loop, learnings, ...details]) {
    for (const line of storyLinesOf(payload)) {
      if (line.model) assert.ok(allowed.has(line.model), `unexpected model ${line.model}`);
    }
  }
});

test('no provenance names a model as the thing that computed a number', () => {
  for (const payload of [loop, learnings, ...details]) {
    for (const block of performanceBlocksOf(payload)) {
      const by = String(block.provenance.computed_by);
      assert.ok(!/claude|gpt|gemini|llm|model/i.test(by), `computed_by looks like a model: ${by}`);
    }
    for (const f of payload.facts as { provenance: { computed_by: string } }[]) {
      assert.ok(!/claude|gpt|gemini|llm/i.test(f.provenance.computed_by));
    }
  }
});

test('every performance block carries expectancy AND both drawdowns AND full provenance', () => {
  let n = 0;
  for (const payload of [loop, list, learnings, ...details]) {
    for (const b of performanceBlocksOf(payload)) {
      n += 1;
      assert.equal(typeof b.expectancy_pct_per_trade, 'number', 'expectancy is the hero and is required');
      assert.equal(typeof b.expectancy_2x_slippage_pct_per_trade, 'number');
      assert.equal(typeof b.max_drawdown_pct, 'number', 'no return without its drawdown');
      assert.equal(typeof b.current_drawdown_pct, 'number');
      const p = b.provenance;
      for (const key of ['data_source', 'date_range', 'as_of', 'cost_convention', 'computed_by']) {
        assert.ok(p[key], `provenance.${key} missing — the number has no source`);
      }
      // point-in-time: nothing in the window may post-date the as_of
      const range = p.date_range as { end: string };
      assert.ok(range.end <= String(p.as_of), `window ends after as_of (${range.end} > ${p.as_of})`);
    }
  }
  assert.ok(n >= 15, `expected many performance blocks, saw ${n}`);
});

test('no payload contains a target price, projection or promised return', () => {
  const banned = /"(target|target_price|expected_return|projection|projected_return|price_target)"/i;
  for (const payload of [loop, list, learnings, ...details]) {
    const raw = JSON.stringify(payload);
    assert.ok(!banned.test(raw), 'a promise-shaped field reached the client');
  }
});

test('the experiments list leads with the dead ones (losers first)', () => {
  const items = list.items as { status: string }[];
  assert.equal(items[0].status, 'died', 'the list must lead with a rejected experiment');
  assert.equal(items[items.length - 1].status, 'promoted', 'graduated experiments come last');
});

test('every virtual ledger is ascending by net P&L, worst trade first', () => {
  let books = 0;
  for (const d of details) {
    const book = d.virtual_book as { ledger_losers_first: { pnl_pct_net: number }[] } | null;
    if (!book) continue;
    books += 1;
    const pnl = book.ledger_losers_first.map((t) => t.pnl_pct_net);
    assert.deepEqual(pnl, [...pnl].sort((a, b) => a - b), 'ledger is not losers-first');
  }
  assert.ok(books >= 3, `expected several virtual books, saw ${books}`);
});

test('every simulated trade enters on a LATER session than its signal, with costs charged', () => {
  let trades = 0;
  for (const d of details) {
    const book = d.virtual_book as {
      ledger_losers_first: Record<string, string | number | null>[];
      open_positions: Record<string, string | number | null>[];
    } | null;
    if (!book) continue;
    for (const t of [...book.ledger_losers_first, ...book.open_positions]) {
      trades += 1;
      assert.ok(
        String(t.entry_date) > String(t.signal_date),
        `entry ${t.entry_date} is not after signal ${t.signal_date}`,
      );
      if (t.exit_date) {
        assert.ok(Number(t.costs_pct) >= 0, 'a closed trade must be charged costs');
        assert.ok(Number(t.slippage_bps) >= 0, 'a closed trade must be charged slippage');
      }
    }
  }
  assert.ok(trades >= 20, `expected a real ledger, saw ${trades} trades`);
});

test('sample_flag matches the product rule the UI renders (n<20 greyed, n<50 flagged)', () => {
  const expected = (n: number | null | undefined) =>
    n === null || n === undefined ? 'unknown' : n < 20 ? 'greyed' : n < 50 ? 'flagged' : 'ok';
  let checked = 0;
  for (const payload of [loop, learnings, ...details]) {
    for (const f of payload.facts as Fact[]) {
      checked += 1;
      assert.equal(f.sample_flag, expected(f.n), `fact ${f.id} flag disagrees with its n`);
    }
    for (const b of performanceBlocksOf(payload)) {
      assert.equal(b.sample_flag, expected(b.n as number));
    }
  }
  assert.ok(checked > 30);
});

test('a died experiment publishes a post-mortem with what was kept', () => {
  const dead = details.filter((d) => d.status === 'died');
  assert.ok(dead.length >= 1, 'the graveyard must not be empty in a research product');
  for (const d of dead) {
    const pm = d.post_mortem as { what_we_kept: string; cause: string; evidence_refs: string[] };
    assert.ok(pm, 'a dead experiment without a post-mortem is a hidden failure');
    assert.ok(pm.what_we_kept.length > 20);
    assert.ok(pm.cause);
    assert.ok(pm.evidence_refs.length >= 1);
  }
});

test('every change-log entry answers what/why/evidence/versions, and L4 is human-only', () => {
  let entries = 0;
  for (const d of details) {
    const log = d.change_log as {
      seq: number;
      level: string;
      what_changed: string;
      why: string;
      evidence_refs: string[];
      new_version: string;
      previous_version: string | null;
      decided_by: string;
      approved_by: string | null;
      validation: string | null;
    }[];
    const seqs = log.map((e) => e.seq);
    assert.deepEqual(seqs, [...seqs].sort((a, b) => a - b), 'change-log must be append-only');
    for (const e of log) {
      entries += 1;
      assert.ok(e.what_changed && e.why && e.new_version);
      assert.ok(e.evidence_refs.length >= 1, 'a change without evidence is an opinion');
      assert.notEqual(e.previous_version, e.new_version);
      if (e.level === 'L4') {
        assert.equal(e.decided_by, 'human', 'the Constitution is human-only');
        assert.ok(e.approved_by, 'an L4 change needs a named human approver');
      }
      if (e.level === 'L3') {
        assert.ok(e.validation, 'an L3 change must state its backtest + forward validation');
      }
    }
  }
  assert.ok(entries >= 5, `expected a real change-log, saw ${entries} entries`);
});

test('every trigger was fired by the deterministic engine, never a clock or the model', () => {
  const triggers: { fired_by: string }[] = [];
  if (loop.trigger) triggers.push(loop.trigger as { fired_by: string });
  for (const d of details) {
    (d.triggers as { fired_by: string }[]).forEach((t) => triggers.push(t));
    if (d.next_review) triggers.push(d.next_review as { fired_by: string });
  }
  assert.ok(triggers.length >= 4);
  for (const t of triggers) assert.equal(t.fired_by, 'engine');
});

test('the status filter is honoured, so the pipeline links cannot mislead', async () => {
  for (const status of ['died', 'testing', 'validating', 'promising', 'promoted', 'queued']) {
    const res = (await get(`/api/pathfinder/experiments?status=${status}`)) as {
      items: { status: string }[];
      status_filter: string;
    };
    assert.equal(res.status_filter, status);
    for (const item of res.items) assert.equal(item.status, status);
  }
});

test('an unknown experiment id returns a guarded error the app can render', async () => {
  const res = await fetch(`${BASE}/api/pathfinder/experiment/exp_9999`);
  assert.equal(res.status, 404);
  const body = (await res.json()) as { error?: { code: string; message: string } };
  assert.ok(body.error?.code, "the error shape the client parses is missing");
  const message = body.error!.message;
  assert.ok(!/Traceback|SELECT |sqlite|psycopg/i.test(message), 'error leaks internals');
});

test('every payload carries its research disclosure', () => {
  for (const payload of [loop, list, learnings, ...details]) {
    assert.match(String(payload.disclosure), /research lab/i);
  }
});
