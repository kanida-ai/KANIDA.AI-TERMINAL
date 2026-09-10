/**
 * The Pathfinder API client.
 *
 * ============================================================================
 *  THE BASE URL IS THE ONLY ENVIRONMENT SEAM. NOTHING ELSE CHANGES.
 * ============================================================================
 *
 *   EXPO_PUBLIC_API_BASE_URL=http://127.0.0.1:8010     # the research engine, locally
 *   EXPO_PUBLIC_API_BASE_URL=https://api.kanida.ai     # the same contract, deployed
 *
 * Set it in `.env` (see `.env.example`) or in the EAS build profile. Paths,
 * types, parsing and every screen stay identical (docs/openapi.yaml).
 *
 * This module is the ONLY place in the app that knows a hostname exists.
 *
 * Three reads, all under the research source (`KANIDA_PATHFINDER_SOURCE=research`):
 *
 *   GET /api/pathfinder/feed?date=          the clarity-first edition (S1) + experiment cards (S2)
 *   GET /api/pathfinder/experiments         the registry: cards, the declined, the scoreboard (S2)
 *   GET /api/pathfinder/experiment/{id}     one experiment's full record (S2)
 */
import Constants from 'expo-constants';
import { Platform } from 'react-native';

import type { ExperimentRecord, ExperimentsResponse, FeedResponse } from './types';

/** Where the research engine listens locally (backend/pathfinder/mock_app.py). */
const LOCAL_PORT = 8010;

/**
 * On a physical phone `127.0.0.1` is the phone, not the dev laptop. When no
 * base URL is configured we fall back to the Metro host that is already serving
 * this bundle, so `npm start` -> scan QR -> the app reaches the engine with no
 * extra configuration. Explicit config always wins.
 */
function inferredDevBaseUrl(): string {
  const hostUri =
    (Constants.expoConfig as { hostUri?: string } | null)?.hostUri ??
    (Constants.expoGoConfig as { debuggerHost?: string } | null)?.debuggerHost ??
    '';
  const host = hostUri.split(':')[0];
  if (host && host !== 'localhost' && host !== '127.0.0.1') {
    return `http://${host}:${LOCAL_PORT}`;
  }
  if (Platform.OS === 'android') {
    // Android emulator loopback to the host machine.
    return `http://10.0.2.2:${LOCAL_PORT}`;
  }
  return `http://127.0.0.1:${LOCAL_PORT}`;
}

export const API_BASE_URL: string = (
  process.env.EXPO_PUBLIC_API_BASE_URL || inferredDevBaseUrl()
).replace(/\/+$/, '');

/** True when we are pointed at something that is not a local engine. */
export const IS_LIVE_ENGINE = !/(^|\/\/)(127\.0\.0\.1|localhost|10\.0\.2\.2|192\.168\.|10\.)/.test(
  API_BASE_URL,
);

const DEFAULT_TIMEOUT_MS = 12_000;

export class ApiError extends Error {
  readonly code: string;
  readonly status: number | null;
  readonly requestId: string | null;
  readonly url: string;

  constructor(args: {
    message: string;
    code: string;
    status?: number | null;
    requestId?: string | null;
    url: string;
  }) {
    super(args.message);
    this.name = 'ApiError';
    this.code = args.code;
    this.status = args.status ?? null;
    this.requestId = args.requestId ?? null;
    this.url = args.url;
  }

  /** Offline / DNS / server-not-running -- the state the UI shows a retry for. */
  get isUnreachable(): boolean {
    return this.code === 'network_unreachable' || this.code === 'timeout';
  }
}

async function get<T>(path: string, signal?: AbortSignal): Promise<T> {
  const url = `${API_BASE_URL}${path}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);
  const onOuterAbort = () => controller.abort();
  signal?.addEventListener('abort', onOuterAbort);

  let res: Response;
  try {
    res = await fetch(url, {
      signal: controller.signal,
      headers: { Accept: 'application/json' },
    });
  } catch {
    const aborted = signal?.aborted === true;
    throw new ApiError({
      url,
      code: aborted ? 'aborted' : controller.signal.aborted ? 'timeout' : 'network_unreachable',
      message: aborted
        ? 'Request cancelled.'
        : controller.signal.aborted
          ? 'The research server did not answer in time.'
          : `Could not reach the research server at ${API_BASE_URL}.`,
    });
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort', onOuterAbort);
  }

  const raw = await res.text();
  let body: unknown = null;
  if (raw) {
    try {
      body = JSON.parse(raw);
    } catch {
      body = null;
    }
  }

  if (!res.ok) {
    // The contract's guarded error shape: { error: { code, message, request_id } }
    const err = (body as { error?: { code?: string; message?: string; request_id?: string } })
      ?.error;
    throw new ApiError({
      url,
      status: res.status,
      code: err?.code ?? `http_${res.status}`,
      requestId: err?.request_id ?? null,
      message: err?.message ?? `The research server returned ${res.status}.`,
    });
  }

  if (body === null) {
    throw new ApiError({ url, status: res.status, code: 'bad_payload', message: 'Unreadable response from the research server.' });
  }
  return body as T;
}

/**
 * A small per-session memo. The feed is read-only and an edition never changes
 * once published (the store is append-only), so the same edition served twice
 * is the same bytes. The memo makes "open a story's depth, come back" instant
 * and keeps the pager where it was. `invalidate()` on a manual refresh.
 */
const memo = new Map<string, Promise<unknown>>();

function remembered<T>(key: string, fetcher: () => Promise<T>): Promise<T> {
  const hit = memo.get(key) as Promise<T> | undefined;
  if (hit) return hit;
  const p = fetcher().catch((err: unknown) => {
    memo.delete(key);
    throw err;
  });
  memo.set(key, p);
  return p;
}

export function invalidate(): void {
  memo.clear();
}

/** A date must look like 2026-07-29 before it goes into a query string. */
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

export const api = {
  /** GET /api/pathfinder/feed?date= -- the edition for one close; latest when omitted. */
  feed: (date?: string | null, signal?: AbortSignal) => {
    const q = date && DATE_RE.test(date) ? `?date=${encodeURIComponent(date)}` : '';
    return remembered(`feed${q}`, () => get<FeedResponse>(`/api/pathfinder/feed${q}`, signal));
  },

  /** GET /api/pathfinder/experiments -- the registry, losers first, with the declined and the scoreboard. */
  experiments: (signal?: AbortSignal) =>
    remembered('experiments', () => get<ExperimentsResponse>('/api/pathfinder/experiments', signal)),

  /** GET /api/pathfinder/experiment/{id} -- versions, trials, periods, learning, proposal, post-mortem. */
  experiment: (id: string, signal?: AbortSignal) =>
    remembered(`experiment:${id}`, () =>
      get<ExperimentRecord>(`/api/pathfinder/experiment/${encodeURIComponent(id)}`, signal),
    ),
};
