// Generates the shared drift fixture from the TYPESCRIPT walk — the one the panel uses for the selected
// instrument. The Python pass is judged against this, never the other way round.
const fs = require('fs'), path = require('path'), vm = require('node:vm'), ts = require('typescript');
const ROOT = 'C:/Users/SPS/Documents/Kanida_Falcon/kanida-app';
const load = (rel, req) => {
  const code = ts.transpileModule(fs.readFileSync(path.join(ROOT, rel), 'utf8'),
    { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
  const ctx = { exports: {}, require: req, process: { env: {} } };
  vm.runInNewContext(code, ctx);
  return ctx.exports;
};
const L = load('src/derivative/logic.ts', n => { throw Error('unexpected import: ' + n); });
const SUM = load('src/derivative/summary.ts', n => /\/logic$/.test(n) ? L : (() => { throw Error(n); })());

const MARKS = ['09:15', '09:30', '09:45', '10:00', '10:15', '10:30', '10:45', '11:00']
  .map(t => `2026-09-18 ${t}:00`);
const slot = (k, type, row, deltas, prices) => ({
  present: true, row, option_type: type, strike: k, instrument_token: k,
  tradingsymbol: `X${k}${type}`,
  points: MARKS.map((at, i) => ({ at, delta_oi: deltas[i], price: prices[i] })),
});
const FLAT_D = [0, 0, 0, 0, 0, 0, 0, 0], FLAT_P = [20, 20, 20, 20, 20, 20, 20, 20];
const SLOTS = [
  slot(23350, 'CE', 'calls', [0, 0, 0, 0, 20000, 40000, 60000, 80000], [80, 80, 80, 80, 95, 110, 125, 140]),
  slot(23400, 'CE', 'calls', [0, 0, 0, 0, 0, 25000, 50000, 50000], [55, 55, 55, 55, 55, 66, 78, 56]),
  slot(23450, 'CE', 'calls', [0, 0, 0, 0, 0, 0, 30000, 30500], [40, 40, 40, 40, 40, 40, 52, 40]),
  slot(23500, 'CE', 'calls', FLAT_D, FLAT_P),
  slot(23550, 'CE', 'calls', FLAT_D, FLAT_P),
  // a put side that DOES move, so the walk is exercised on both rows
  slot(23300, 'PE', 'puts', [0, 0, 0, 0, -15000, -30000, -45000, -60000], [70, 70, 70, 70, 60, 50, 40, 30]),
  slot(23250, 'PE', 'puts', FLAT_D, FLAT_P),
  slot(23200, 'PE', 'puts', FLAT_D, FLAT_P),
  slot(23150, 'PE', 'puts', FLAT_D, FLAT_P),
  slot(23100, 'PE', 'puts', FLAT_D, FLAT_P),
];

// only the fields both implementations produce, so the comparison is exact and not a shape argument
const pick = side => ({
  behaviour: side.behaviour, state: side.state, strikes: side.strikes,
  lead: side.lead, joined: side.joined, left: side.left,
  lead_stable: side.lead_stable, first_at: side.first_at,
  last_confirmed_at: side.last_confirmed_at, elapsed_minutes: side.elapsed_minutes,
  scans: side.scans, measured: side.measured, slots: side.slots,
});
const walk = SUM.observe({ rows: SLOTS });
const expected = walk.map(o => ({
  at: o.at, from: o.from, window_minutes: o.window_minutes, previous_at: o.previous_at,
  covered: o.covered, calls: pick(o.calls), puts: pick(o.puts),
  headline: (SUM.narrate(o, { underlying: 'X' }) || {}).headline,
}));

const out = path.join(ROOT, 'server/tests/fixtures/session_walk.json');
fs.mkdirSync(path.dirname(out), { recursive: true });
fs.writeFileSync(out, JSON.stringify({ slots: SLOTS, expected }, null, 1));
console.log('fixture written:', expected.length, 'readings ->', path.relative(ROOT, out));
