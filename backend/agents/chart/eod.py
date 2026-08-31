"""
Chart Agent · POST-MARKET EOD job (glue over the built pieces).

After market close this runs ONE loop for a date D:

  1. DAILY REFRESH (guarded, optional):  bring the daily OHLC store current up to D. This does NOT
     ship its own Kite client (agents never touch a broker/feed directly). It REUSES an injected
     ``fetch_fn`` (or ``AGENT_CHART_FETCH_FN='module:function'``). When no fetcher is wired in the
     running environment, the step is SKIPPED with an honest logged note — never fabricated — and the
     scan still runs on whatever the prod daily pipeline has already written. A fetch failure is
     recorded in the result and never crashes the scan (additive + guarded).
  2. SCAN (the real work):  ``screener.build_screen(D)`` runs every REGISTERED pattern's live-stage
     classifier across the FULL universe, strictly point-in-time (build_screen enforces it). The
     GOTCHA baked in below: ``patterns.load_builtin()`` MUST have run first or the registry is empty
     and the scan silently returns 0 setups — so run_eod loads it unconditionally.
  3. PUBLISH:  build_screen writes the small per-date artifact via the existing store (Local default,
     or S3 when AGENT_CHART_SCREEN_URI is set). run_eod confirms it by reading it back.

Execution boundary: this emits/serves a SCREEN artifact only. It never routes an order, and the
downstream chart agent stays paper-default. No git/shell/deploy here.

CLI (scheduled post-market):
    python -m agents.chart.eod --date 2026-07-31            # refresh (if wired) + scan + publish
    python -m agents.chart.eod --date 2026-07-31 --no-fetch # scan + publish only
"""
from __future__ import annotations
import argparse
import importlib
import logging
import os
import time
from datetime import date

log = logging.getLogger("agents.chart.eod")

# Honest note surfaced (and logged) when no daily fetcher is wired into the running environment.
_NO_FETCHER_NOTE = "daily feed not refreshed here — prod daily pipeline must be current"


def _resolve_fetch_fn(fetch_fn):
    """Resolve the daily-refresh callable, in precedence order:
        1. an explicit ``fetch_fn`` argument (injection — used by tests / the cloud wiring);
        2. env ``AGENT_CHART_FETCH_FN='module:function'`` (wire a reused fetcher with no code change).
    Returns (callable_or_None, note). A bad spec yields (None, <reason>) — never raises."""
    if fetch_fn is not None:
        return fetch_fn, None
    spec = os.environ.get("AGENT_CHART_FETCH_FN", "").strip()
    if not spec:
        return None, None
    try:
        mod_name, _, fn_name = spec.partition(":")
        fn = getattr(importlib.import_module(mod_name), fn_name)
        if not callable(fn):
            return None, f"AGENT_CHART_FETCH_FN {spec!r} is not callable"
        return fn, None
    except Exception as e:  # noqa: BLE001 — a bad hook must not crash the EOD run
        return None, f"AGENT_CHART_FETCH_FN {spec!r} not importable ({type(e).__name__}: {e})"


def run_eod(as_of_date=None, fetch: bool = True, fetch_fn=None) -> dict:
    """Post-market EOD loop for ``as_of_date`` (default: today). Refresh (guarded/optional) -> scan the
    full universe over all registered patterns -> publish the screen artifact. Returns:

        {as_of_date, fetched, fetch_seconds?, scanned, count, by_pattern, scan_seconds,
         stored, published, trading_day, note, universe_size}

    ``fetched`` is True (refreshed), False (fetch disabled), or a "skipped: …"/"error: …" string —
    honest, never fabricated. Nothing here can crash the scan: the fetch step is fully guarded."""
    # Imported lazily so a screener/data import problem can't break module import (additive + guarded).
    from . import screener as scr
    from .patterns import registry as patterns

    as_of = str(as_of_date) if as_of_date else date.today().isoformat()

    # THE GOTCHA: populate the pattern registry BEFORE scanning, or build_screen finds 0 patterns and
    # silently returns 0 setups. load_builtin() is idempotent + guarded (a broken detector is skipped).
    patterns.load_builtin()
    n_patterns = len(patterns.all_patterns())

    result: dict = {"as_of_date": as_of, "fetched": None, "scanned": 0, "count": 0,
                    "by_pattern": {}, "scan_seconds": None, "stored": None, "published": False,
                    "n_patterns": n_patterns}

    # ---------------------------------------------------------------- 1) DAILY REFRESH (guarded)
    if not fetch:
        result["fetched"] = False
        log.info("run_eod(%s): fetch disabled (--no-fetch) — scanning existing daily store.", as_of)
    else:
        fn, note = _resolve_fetch_fn(fetch_fn)
        if fn is None:
            msg = note or _NO_FETCHER_NOTE
            result["fetched"] = f"skipped: {msg}"
            log.warning("run_eod(%s): fetch skipped — %s", as_of, msg)
        else:
            try:
                t = time.perf_counter()
                out = fn(as_of)                     # reused fetcher brings the daily store current -> D
                result["fetched"] = True
                result["fetch_seconds"] = round(time.perf_counter() - t, 3)
                if isinstance(out, dict):
                    result["fetch_detail"] = out
                log.info("run_eod(%s): daily store refreshed via %s in %.2fs", as_of,
                         getattr(fn, "__name__", repr(fn)), result["fetch_seconds"])
            except Exception as e:  # noqa: BLE001 — a fetch failure must NOT sink the scan
                result["fetched"] = f"error: {type(e).__name__}: {e}"
                log.warning("run_eod(%s): daily refresh failed (non-fatal, scanning stale store): %s",
                            as_of, e)

    # ---------------------------------------------------------------- 2) SCAN (full universe)
    # ONE post-market pass does BOTH: enrich=True attaches the per-setup storyline (tier/quality/hook)
    # the middle column needs, AND precompute_setups=True writes a self-contained per-setup bundle for
    # every setup so a column-3 click serves in MS (fast-path /setup + /bars). Off-gateway (RunTask) so
    # the added build_setup+bars cost per setup is fine here (measured in payload['setup_precompute']).
    t0 = time.perf_counter()
    payload = scr.build_screen(as_of, enrich=True, precompute_setups=True)
    result["scan_seconds"] = round(time.perf_counter() - t0, 3)
    result["setup_precompute"] = payload.get("setup_precompute")

    result["scanned"] = payload.get("scanned", 0)
    result["count"] = payload.get("count", 0)
    result["universe_size"] = payload.get("universe_size", 0)
    result["trading_day"] = payload.get("trading_day")
    result["note"] = payload.get("note")

    by_pattern: dict = {}
    for s in payload.get("setups", []):
        p = s.get("pattern")
        by_pattern[p] = by_pattern.get(p, 0) + 1
    result["by_pattern"] = by_pattern

    # ---------------------------------------------------------------- 3) PUBLISH (confirm artifact)
    result["stored"] = payload.get("stored_at") or payload.get("store_error")
    loaded = scr.load_screen(as_of)                 # read the artifact back through the same store
    result["published"] = bool(loaded) and loaded.get("count") == result["count"]
    if not result["published"]:
        log.warning("run_eod(%s): screen artifact not confirmed on read-back (stored=%s).",
                    as_of, result["stored"])

    log.info("run_eod(%s): %d setups over %d/%d symbols in %.2fs | fetched=%s | stored=%s",
             as_of, result["count"], result["scanned"], result["universe_size"],
             result["scan_seconds"], result["fetched"], result["stored"])
    return result


def _main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="agents.chart.eod", description="Chart Agent post-market EOD job (refresh + scan + publish)")
    ap.add_argument("--date", default=None, help="as-of date YYYY-MM-DD (default: today)")
    ap.add_argument("--no-fetch", action="store_true",
                    help="skip the daily refresh; scan the existing daily store")
    args = ap.parse_args(argv)

    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    res = run_eod(as_of_date=args.date, fetch=not args.no_fetch)

    print("\n=== Chart Agent EOD ===")
    print(f"  as_of_date : {res['as_of_date']}  (trading_day={res.get('trading_day')})")
    print(f"  fetched    : {res['fetched']}")
    print(f"  scanned    : {res['scanned']}/{res.get('universe_size')} symbols "
          f"in {res['scan_seconds']}s  ({res.get('n_patterns')} patterns loaded)")
    print(f"  count      : {res['count']} setups")
    print(f"  by_pattern : {res['by_pattern']}")
    print(f"  stored     : {res['stored']}  (published={res['published']})")
    sp = res.get("setup_precompute") or {}
    if sp:
        print(f"  setups B   : precomputed {sp.get('precomputed')} bundles in {sp.get('elapsed_sec')}s "
              f"(errors={sp.get('errors')})")
    if res.get("note"):
        print(f"  note       : {res['note']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
