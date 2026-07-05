"""SELF-UPDATING NSE holiday source (CM / equity segment) — fetch + cache.

WHY THIS EXISTS (real-money safety):
  trading_calendar.NSE_HOLIDAYS is a hand-maintained set seeded only with
  2025/2026. For any FUTURE date OUT of ohlc_daily coverage, is_trading_day
  returned "iso not in holidays" — so in an un-seeded year (e.g. 2027) EVERY NSE
  holiday would SILENTLY read as a TRADING day and the scheduler could fire real
  trades on a real holiday. This module adds the authoritative, auto-updating
  layer: the official NSE holiday list, fetched and cached to disk, merged into
  the calendar per-call (no restart needed).

SOURCE (authoritative, auto-updating):
  GET https://www.nseindia.com/api/holiday-master?type=trading
      header User-Agent: Mozilla/5.0
  → JSON keyed by SEGMENT. We use the "CM" (Capital Market / equity) segment.
  Each item: {"tradingDate":"15-Jan-2026","weekDay":"Thursday","description":...}.
  tradingDate is parsed with "%d-%b-%Y" → "YYYY-MM-DD". NSE returns whatever
  year(s) it has published (typically the current year, and next year once the
  circular is out) — that is exactly the coverage extension we need.

  If a bare GET ever 401s in prod (NSE occasionally requires a cookie handshake),
  we first GET the NSE home page to obtain cookies via a shared cookiejar opener,
  then retry the API. STANDARD LIBRARY ONLY (urllib / json / http.cookiejar) — no
  pip dependency.

SAFETY / BEST-EFFORT:
  * NOTHING here ever raises to the caller. Network / parse / disk errors log a
    warning and leave the last good cache in place (or return None / empty).
  * The cache is the ONLY persisted state (data/config/nse_holidays_cache.json).
    trading_calendar reads it per-call (cheap, small file) like the operator
    override, so a background refresh takes effect with no restart.
  * refresh_if_stale(max_age_days) only re-fetches when the cache is missing or
    older than the given age — so boot / a daily tick can call it cheaply.
"""
from __future__ import annotations

import http.cookiejar
import json
import logging
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

log = logging.getLogger("kanida.autotrade.nse_holiday_source")

IST = timezone(timedelta(hours=5, minutes=30))

_HOLIDAY_API = "https://www.nseindia.com/api/holiday-master?type=trading"
_NSE_HOME = "https://www.nseindia.com/"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
_SEGMENT = "CM"  # Capital Market = equity
_TIMEOUT = 12  # seconds — best-effort; never blocks boot (called in a bg thread)


# ── cache location ──────────────────────────────────────────────────────────────
def _cache_path() -> Path:
    """data/config/nse_holidays_cache.json under the repo (backend/.. /data/...).

    Patched in tests to a temp file. Mirrors trading_calendar._override_holidays'
    repo-relative resolution (parents[2] from backend/autotrade/<file>)."""
    return (Path(__file__).resolve().parents[2]
            / "data" / "config" / "nse_holidays_cache.json")


# ── raw network fetch (patched in tests) ────────────────────────────────────────
def _raw_fetch_cm() -> Optional[Dict[str, Any]]:
    """GET the NSE holiday-master JSON. Returns the parsed dict (segment-keyed) or
    None on any failure. Never raises. Uses a cookiejar opener + a home-page
    priming GET fallback if the bare API call is rejected."""
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    headers = {"User-Agent": _UA, "Accept": "application/json"}

    def _get(url: str) -> Optional[bytes]:
        req = urllib.request.Request(url, headers=headers)
        with opener.open(req, timeout=_TIMEOUT) as resp:
            return resp.read()

    try:
        try:
            body = _get(_HOLIDAY_API)
        except Exception as e:  # 401 / cookie wall → prime cookies then retry.
            log.info("NSE holiday API direct GET failed (%s) — priming cookies", e)
            try:
                _get(_NSE_HOME)  # populates the cookiejar
            except Exception as e2:  # pragma: no cover - defensive
                log.warning("NSE home-page cookie prime failed: %s", e2)
            body = _get(_HOLIDAY_API)
        if not body:
            return None
        return json.loads(body.decode("utf-8", "replace"))
    except Exception as e:  # pragma: no cover - network path, never raises
        log.warning("NSE holiday fetch failed (best-effort, keeping cache): %s", e)
        return None


# ── parse ───────────────────────────────────────────────────────────────────────
def _parse_cm_holidays(payload: Dict[str, Any]) -> Set[str]:
    """Extract the CM (equity) segment holidays as a set of ISO YYYY-MM-DD.

    Robust to missing keys / garbled rows: a row that doesn't parse is skipped.
    Never raises."""
    out: Set[str] = set()
    if not isinstance(payload, dict):
        return out
    rows = payload.get(_SEGMENT) or []
    if not isinstance(rows, list):
        return out
    for item in rows:
        try:
            raw = (item or {}).get("tradingDate")
            if not raw:
                continue
            d = datetime.strptime(str(raw).strip(), "%d-%b-%Y").date()
            out.add(d.isoformat())
        except Exception:
            continue  # skip a garbled row, never abort the batch
    return out


# ── cache read/write ────────────────────────────────────────────────────────────
def _read_cache() -> Optional[Dict[str, Any]]:
    p = _cache_path()
    try:
        if not p.exists():
            return None
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:  # pragma: no cover - corrupt cache = treat as absent
        log.warning("NSE holiday cache unreadable (%s) — ignoring", e)
        return None


def _write_cache(holidays: Set[str]) -> None:
    p = _cache_path()
    years = sorted({int(h[:4]) for h in holidays if len(h) >= 4 and h[:4].isdigit()})
    payload = {
        "fetched_at": datetime.now(IST).isoformat(),
        "holidays": sorted(holidays),
        "years": years,
    }
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        # Atomic-ish write: temp file then replace (avoids a torn read).
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(p)
    except Exception as e:  # pragma: no cover - disk error, best-effort
        log.warning("NSE holiday cache write failed (%s) — cache not updated", e)


# ── public API ──────────────────────────────────────────────────────────────────
def fetch_and_cache() -> Optional[Dict[str, Any]]:
    """Fetch the CM holiday list and persist it to the cache. Returns the parsed
    cache payload on success, or None if the fetch/parse produced nothing (in
    which case the previous cache, if any, is LEFT IN PLACE). Never raises."""
    try:
        payload = _raw_fetch_cm()
        if payload is None:
            return None
        holidays = _parse_cm_holidays(payload)
        if not holidays:
            log.warning(
                "NSE holiday fetch parsed 0 holidays — keeping existing cache")
            return None
        _write_cache(holidays)
        return _read_cache()
    except Exception as e:  # best-effort: never raise, keep the last good cache
        log.warning("NSE holiday fetch/cache failed (%s) — keeping cache", e)
        return None


def load_cached_holidays() -> Set[str]:
    """The fetched holiday ISO dates from the on-disk cache (empty set if none).
    Read per-call by trading_calendar so a refresh needs no restart. Never raises."""
    data = _read_cache()
    if not data:
        return set()
    hols = data.get("holidays") or []
    return {str(h) for h in hols if h}


def load_cached_years() -> Set[int]:
    """The set of YEARS present in the fetched cache (empty if none). Feeds the
    calendar coverage guard. Never raises."""
    data = _read_cache()
    if not data:
        return set()
    yrs = data.get("years")
    if yrs:
        try:
            return {int(y) for y in yrs}
        except Exception:  # pragma: no cover
            pass
    # Derive from the holiday dates if "years" is missing/garbled.
    return {int(h[:4]) for h in load_cached_holidays()
            if len(h) >= 4 and h[:4].isdigit()}


def cache_age_days() -> Optional[float]:
    """Age of the cache in days, or None if there is no (readable) cache."""
    data = _read_cache()
    if not data:
        return None
    try:
        fetched = datetime.fromisoformat(str(data.get("fetched_at")))
        if fetched.tzinfo is None:
            fetched = fetched.replace(tzinfo=IST)
        return (datetime.now(IST) - fetched).total_seconds() / 86400.0
    except Exception:  # pragma: no cover
        return None


def refresh_if_stale(max_age_days: float = 7) -> bool:
    """Re-fetch ONLY when the cache is missing or older than max_age_days.

    Returns True if a fetch was performed AND succeeded (cache updated), False
    otherwise (fresh cache, or a failed fetch that left the prior cache intact).
    Never raises — safe to call at boot / on a daily tick."""
    try:
        age = cache_age_days()
        if age is not None and age <= float(max_age_days):
            return False  # still fresh
        return fetch_and_cache() is not None
    except Exception as e:  # pragma: no cover - belt-and-braces
        log.warning("refresh_if_stale failed (best-effort): %s", e)
        return False


def ensure_years_covered(years: List[int], max_age_days: float = 7) -> Set[int]:
    """Best-effort: ensure the cache covers each requested year. Refreshes if the
    cache is stale OR a requested year is absent. Returns the set of requested
    years that are STILL uncovered after the attempt (empty = all covered). Never
    raises — the caller logs a loud alert if the returned set is non-empty."""
    want = {int(y) for y in years}
    have = load_cached_years()
    missing = want - have
    if missing or cache_age_days() is None:
        fetch_and_cache()
        have = load_cached_years()
    return want - have
