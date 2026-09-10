/**
 * A tiny fetch-state hook. Deliberately dependency-free: one codebase shipping
 * to iOS + Android + web is easier to keep identical with less machinery, and
 * four read-only GETs do not need a cache library.
 *
 * Every screen gets the same four states, so the honest ones (empty, error,
 * offline) are as designed as the happy one.
 */
import { useCallback, useEffect, useState } from 'react';

import { ApiError } from './client';

export type Resource<T> = {
  data: T | null;
  error: ApiError | null;
  /** first load, nothing on screen yet -> skeletons */
  loading: boolean;
  /** a manual refresh over data that is already on screen -> spinner only */
  refreshing: boolean;
  refresh: () => void;
};

export function useResource<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
  /** The caller's declared identity for `fetcher` — e.g. `[id]`, or `[]`. */
  deps: readonly unknown[],
): Resource<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<ApiError | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [nonce, setNonce] = useState(0);
  const [hasData, setHasData] = useState(false);

  const key = JSON.stringify(deps);

  useEffect(() => {
    const controller = new AbortController();
    let live = true;
    // Marking the request pending as it starts is the point of this effect — it
    // is the "synchronise with an external system" case the rule exempts in
    // spirit. It costs one extra render when a screen mounts or the key changes,
    // and it is what makes the skeleton appear before the network answers.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    if (hasData) setRefreshing(true);
    else setLoading(true);

    fetcher(controller.signal)
      .then((value) => {
        if (!live) return;
        setHasData(true);
        setData(value);
        setError(null);
      })
      .catch((err: unknown) => {
        if (!live) return;
        if (err instanceof ApiError && err.code === 'aborted') return;
        setError(
          err instanceof ApiError
            ? err
            : new ApiError({ url: '', code: 'unknown', message: 'Something went wrong.' }),
        );
      })
      .finally(() => {
        if (!live) return;
        setLoading(false);
        setRefreshing(false);
      });

    return () => {
      live = false;
      controller.abort();
    };
    // `fetcher` is a fresh closure on every render, so it cannot be a dependency;
    // `key` is the caller's declared identity for it, and `nonce` is a manual
    // refresh. `hasData` only picks which spinner to show, so it must not re-run
    // the fetch.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, nonce]);

  const refresh = useCallback(() => setNonce((n) => n + 1), []);

  return { data, error, loading, refreshing, refresh };
}
