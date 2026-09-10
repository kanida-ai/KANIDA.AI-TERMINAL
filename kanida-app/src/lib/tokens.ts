/**
 * Reference-token parsing -- the client half of "the LLM never calculates".
 *
 * The engine guarantees that LLM-authored prose contains no literal numeral.
 * Every number is a token that points at a deterministic record:
 *
 *   {{fact:fct_e7_virt_exp}}   a Fact -- a computed number with full provenance
 *   {{exp:exp_0007}}           an experiment
 *   {{evd:evd_e7_costs}}       an evidence bundle
 *   {{ver:strategy@v2.1}}      a strategy / constitution version
 *
 * If the app rendered the raw string the customer would read
 * "it delivered {{fact:fct_e7_virt_exp}}". So this parser is not a nicety --
 * it is the seam where the deterministic number is substituted back into the
 * narrative, carrying its n, window, source and cost convention with it.
 *
 * Pure and dependency-free so it can be unit-tested outside React.
 */

export type TokenKind = 'fact' | 'exp' | 'evd' | 'ver';

export type Segment =
  | { kind: 'text'; text: string }
  | { kind: TokenKind; id: string; raw: string };

const TOKEN_RE = /\{\{(fact|exp|evd|ver):([A-Za-z0-9_.@\-]+)\}\}/g;

/** Split prose into plain-text runs and reference tokens, in order. */
export function parseSegments(input: string): Segment[] {
  const out: Segment[] = [];
  let last = 0;
  TOKEN_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = TOKEN_RE.exec(input)) !== null) {
    if (m.index > last) out.push({ kind: 'text', text: input.slice(last, m.index) });
    out.push({ kind: m[1] as TokenKind, id: m[2], raw: m[0] });
    last = m.index + m[0].length;
  }
  if (last < input.length) out.push({ kind: 'text', text: input.slice(last) });
  if (out.length === 0) out.push({ kind: 'text', text: '' });
  return out;
}

/** Every fact id referenced by a piece of prose. */
export function factIdsIn(input: string): string[] {
  const out: string[] = [];
  for (const seg of parseSegments(input)) {
    if (seg.kind === 'fact') out.push(seg.id);
  }
  return out;
}

/** True if any unresolved reference token would reach the screen. */
export function hasUnrenderedTokens(input: string): boolean {
  return /\{\{[a-z]+:[^}]+\}\}/.test(input);
}

/**
 * Experiment ids read as "#0007" in the UI (the mockups' `#id` chip), while the
 * API id stays `exp_0007`. One place converts.
 */
export function shortExperimentId(id: string): string {
  const m = /^exp_0*([0-9]+)$/.exec(id);
  return m ? `#${m[1].padStart(4, '0')}` : `#${id.replace(/^exp_/, '')}`;
}

/** `strategy@v2.1` -> `v2.1`; `constitution@1.3.0` -> `1.3.0`. */
export function shortVersion(v: string): string {
  const at = v.indexOf('@');
  return at === -1 ? v : v.slice(at + 1);
}
