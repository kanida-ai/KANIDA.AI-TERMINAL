/**
 * The Pathfinder API client.
 *
 * ============================================================================
 *  SWAPPING THE P0 MOCK FOR THE P1 ENGINE IS A BASE-URL CHANGE. NOTHING ELSE.
 * ============================================================================
 *
 *   EXPO_PUBLIC_API_BASE_URL=http://127.0.0.1:8010     # P0 mock  (default)
 *   EXPO_PUBLIC_API_BASE_URL=https://api.kanida.ai     # P1 engine
 *
 * Set it in `.env` (see `.env.example`) or in the EAS build profile. Paths,
 * types, parsing and every screen stay identical -- P0 and P1 serve the same
 * contract (docs/openapi.yaml).
 *
 * This module is the ONLY place in the app that knows a hostname exists.
 */
import Constants from 'expo-constants';
import { Platform } from 'react-native';

import type {
  ExperimentDetail,
  ExperimentListResponse,
  ExperimentStatus,
  LearningsResponse,
  LoopResponse,
} from './types';

/** Where the P0 mock listens (backend/pathfinder/mock_app.py). */
const MOCK_PORT = 8010;

/**
 * On a physical phone `127.0.0.1` is the phone, not the dev laptop. When no
 * base URL is configured we fall back to the Metro host that is already serving
 * this bundle, so `npm start` -> scan QR -> the app reaches the mock with no
 * extra configuration. Explicit config always wins.
 */
function inferredDevBaseUrl(): string {
  const hostUri =
    (Constants.expoConfig as { hostUri?: string } | null)?.hostUri ??
    (Constants.expoGoConfig as { debuggerHost?: string } | null)?.debuggerHost ??
    '';
  const host = hostUri.split(':')[0];
  if (host && host !== 'localhost' && host !== '127.0.0.1') {
    return `http://${host}:${MOCK_PORT}`;
  }
  if (Platform.OS === 'android') {
    // Android emulator loopback to the host machine.
    return `http://10.0.2.2:${MOCK_PORT}`;
  }
  return `http://127.0.0.1:${MOCK_PORT}`;
}

export const API_BASE_URL: string = (
  process.env.EXPO_PUBLIC_API_BASE_URL || inferredDevBaseUrl()
).replace(/\/+$/, '');

/** True when we are pointed at something that is not the local P0 mock. */
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

export const api = {
  /** GET /api/pathfinder/loop -- the live loop, as a story. */
  loop: (signal?: AbortSignal) => get<LoopResponse>('/api/pathfinder/loop', signal),

  /** GET /api/pathfinder/experiments?status= -- losers first, died leads. */
  experiments: (status?: ExperimentStatus | null, signal?: AbortSignal) =>
    get<ExperimentListResponse>(
      `/api/pathfinder/experiments${status ? `?status=${encodeURIComponent(status)}` : ''}`,
      signal,
    ),

  /** GET /api/pathfinder/experiment/{id} -- the full journey incl. the change-log. */
  experiment: (id: string, signal?: AbortSignal) =>
    get<ExperimentDetail>(`/api/pathfinder/experiment/${encodeURIComponent(id)}`, signal),

  /** GET /api/pathfinder/learnings -- learned + testing next. */
  learnings: (signal?: AbortSignal) => get<LearningsResponse>('/api/pathfinder/learnings', signal),
};
