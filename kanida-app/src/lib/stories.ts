/**
 * The feed, as a list of STORIES -- one per screen.
 *
 * Pure and dependency-free so the composition can be unit-tested outside React.
 *
 * The order is the spec's (docs/sessions/PATHFINDER.md § The feed): front-load
 * clarity, preserve depth, keep discovery alive.
 *
 *   1. what_matters_now          the first two or three -- clarity in thirty seconds
 *   2. discoveries               keep swiping
 *   3. experiments               every experiment card with news on this edition; or,
 *                                when none opened, the honest story of what came closest
 *   4. the scoreboard            Right · Wrong · Inconclusive · n, forward vs backfilled
 *   5. what I'm testing next     the follow-up questions the cards raised + the horizons pending
 *
 * NO PADDING. A four-finding edition is four finding stories. An edition that
 * published nothing renders ONE story that says so, with the numbers that say
 * why -- never a blank, never a filler.
 */
import type {
  ExperimentCard,
  ExperimentScoreboard,
  ExperimentsResponse,
  Fact,
  FeedResponse,
  Finding,
  RejectedCandidate,
  Scoreboard,
} from '@/api/types';

export type FollowUp = { findingId: string; subject: string; question: string };
export type PendingHorizon = { findingId: string; subject: string; headline: string; due: string | null; horizonSessions: number; editionDate: string };

export type Story =
  | {
      kind: 'finding';
      id: string;
      finding: Finding;
      /** position inside its tier, 1-based, and the tier's size */
      tierIndex: number;
      tierTotal: number;
    }
  | {
      kind: 'empty_edition';
      id: string;
      editionDate: string;
      dataAsOf: string;
      generatedAt: string;
      candidatesConsidered: number;
      universeScanned: number;
      threshold: number;
      regime: string;
    }
  | { kind: 'experiment'; id: string; card: ExperimentCard }
  | {
      kind: 'no_experiment';
      id: string;
      /** null while the registry is still loading or unreachable */
      registry: ExperimentsResponse | null;
      /** the researched-and-declined candidates, closest first (fewest failed gates) */
      closest: RejectedCandidate[];
      scoreboard: ExperimentScoreboard | null;
    }
  | {
      kind: 'scoreboard';
      id: string;
      scoreboard: Scoreboard;
      experiments: ExperimentScoreboard | null;
      editionDate: string;
      backfilled: boolean;
      recordLabel: string;
    }
  | {
      kind: 'next';
      id: string;
      followUps: FollowUp[];
      pending: PendingHorizon[];
    };

/** Every finding on the edition, in the order the engine ranked them. */
export function findingsOf(feed: FeedResponse): Finding[] {
  return [...feed.what_matters_now, ...feed.discoveries];
}

/**
 * The declined candidates that were actually researched, closest first: fewest
 * failed gates; a variant that failed only because it cannot be traded under the
 * cost convention ranks after one that failed a statistical gate (it was never a
 * candidate a retail book could hold); then the latest edition.
 */
export function closestDeclined(registry: ExperimentsResponse | null): RejectedCandidate[] {
  if (!registry) return [];
  const unimplementable = (r: RejectedCandidate) =>
    r.best_failed_gates.length > 0 && r.best_failed_gates.every((g) => g.startsWith('implementable')) ? 1 : 0;
  return registry.not_opened
    .filter((r) => r.trials_evaluated > 0 && r.best_rule_text)
    .sort(
      (a, b) =>
        a.best_failed_gates.length - b.best_failed_gates.length ||
        unimplementable(a) - unimplementable(b) ||
        b.edition_date.localeCompare(a.edition_date),
    );
}

export function buildStories(feed: FeedResponse, registry: ExperimentsResponse | null): Story[] {
  const out: Story[] = [];
  const findings = findingsOf(feed);

  if (findings.length === 0) {
    out.push({
      kind: 'empty_edition',
      id: `empty:${feed.edition_date}`,
      editionDate: feed.edition_date,
      dataAsOf: feed.data_as_of,
      generatedAt: feed.generated_at,
      candidatesConsidered: feed.candidates_considered,
      universeScanned: feed.universe_scanned,
      threshold: feed.usefulness_threshold,
      regime: feed.regime,
    });
  }

  feed.what_matters_now.forEach((f, i) =>
    out.push({ kind: 'finding', id: f.id, finding: f, tierIndex: i + 1, tierTotal: feed.what_matters_now.length }),
  );
  feed.discoveries.forEach((f, i) =>
    out.push({ kind: 'finding', id: f.id, finding: f, tierIndex: i + 1, tierTotal: feed.discoveries.length }),
  );

  if (feed.experiment_cards.length > 0) {
    feed.experiment_cards.forEach((card) => out.push({ kind: 'experiment', id: card.id, card }));
  } else {
    out.push({
      kind: 'no_experiment',
      id: `no-experiment:${feed.edition_date}`,
      registry,
      closest: closestDeclined(registry),
      scoreboard: feed.experiments_scoreboard ?? registry?.scoreboard ?? null,
    });
  }

  out.push({
    kind: 'scoreboard',
    id: `scoreboard:${feed.edition_date}`,
    scoreboard: feed.scoreboard,
    experiments: feed.experiments_scoreboard ?? null,
    editionDate: feed.edition_date,
    backfilled: feed.backfilled,
    recordLabel: feed.record_label,
  });

  const followUps: FollowUp[] = [];
  const pending: PendingHorizon[] = [];
  for (const f of findings) {
    for (const q of f.follow_up_questions) followUps.push({ findingId: f.id, subject: f.subject, question: q });
    if (f.grading.status === 'pending') {
      pending.push({
        findingId: f.id,
        subject: f.subject,
        headline: f.narrative.headline,
        due: f.grading.due_session ?? null,
        horizonSessions: f.grading_rule.horizon_sessions,
        editionDate: f.edition_date,
      });
    }
  }
  if (followUps.length > 0 || pending.length > 0) {
    out.push({ kind: 'next', id: `next:${feed.edition_date}`, followUps, pending });
  }

  return out;
}

/** The facts a story leads with: `key_fact_refs` in the engine's order, numeric only, at most `max`. */
export function keyFacts(f: Finding, max = 4): Fact[] {
  const byId = new Map(f.facts.map((x) => [x.id, x]));
  const out: Fact[] = [];
  for (const id of f.key_fact_refs) {
    const fact = byId.get(id);
    if (fact && typeof fact.value === 'number') out.push(fact);
    if (out.length >= max) break;
  }
  return out;
}

/**
 * The first `sentences` sentences of a narrative body -- the lede a story shows
 * before the reader chooses depth. Tokens are never split: a `{{fact:…}}` id
 * contains no ". " so a sentence boundary cannot fall inside one.
 */
export function ledeOf(body: string, sentences = 2): string {
  const parts = body.split(/(?<=[.!?])\s+/);
  if (parts.length <= sentences) return body;
  return parts.slice(0, sentences).join(' ');
}

/** Keep an index inside the feed. */
export function clampIndex(i: number, length: number): number {
  if (length <= 0) return 0;
  return Math.min(Math.max(0, Math.round(i)), length - 1);
}

/** The index of a story by id, or null when it is not in this feed. */
export function indexOfStory(stories: Story[], id: string | null | undefined): number | null {
  if (!id) return null;
  const i = stories.findIndex((s) => s.id === id);
  return i === -1 ? null : i;
}

/** A short name for the progress rail / desktop index. */
export function storyTitle(s: Story): string {
  switch (s.kind) {
    case 'finding':
      return s.finding.subject;
    case 'empty_edition':
      return 'Nothing published';
    case 'experiment':
      return s.card.theme;
    case 'no_experiment':
      return 'Nothing cleared the gate';
    case 'scoreboard':
      return 'The record';
    case 'next':
      return 'Testing next';
  }
}
