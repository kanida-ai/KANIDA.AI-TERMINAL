"""Falcon preflight checks — structural gate for auto-trade.

Philosophy:
  Every auto-trade failure we've shipped has fallen into one of 4 classes:
    1. External contract we didn't validate (token, IP, tick size, MTF rules)
    2. Internal invariant violated silently (entry_date NULL, force-flag scope)
    3. Configuration drift between code paths (tick=0.05 in planner, dynamic elsewhere)
    4. Silent failures (0 rows = "success", default arg captured at import time)

  This module is the structural fix. Every check is a NAMED invariant. If any
  RED check fires, all auto-trade paths refuse to call broker APIs. The
  operator sees exactly which invariant is broken + the remediation hint.

Hot paths that gate on preflight:
  * Backend boot                       → log + cache result
  * Token refresh                      → re-run + cache
  * Pre-market deployer cycle          → check before kite.place_order()
  * Trade /place endpoint (Place Now)  → check before kite.place_order()
  * /api/falcon/preflight              → UI banner + ad-hoc operator query

Speed budget:
  * Full run (cold): under 3 seconds — kite.instruments() is the expensive one
  * Hot path (cached): under 200ms — token + signals checks only
  * Cache TTL: 60s for kite-side reads, 5s for DB-side
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("kanida.falcon.preflight")
IST = timezone(timedelta(hours=5, minutes=30))

# ── Result types ─────────────────────────────────────────────────────────────

GREEN  = "green"
YELLOW = "yellow"
RED    = "red"


@dataclass
class CheckResult:
    name:        str        # canonical name (snake-case)
    status:      str        # GREEN / YELLOW / RED
    detail:      str        # 1-line observation, includes the numeric measurement
    remediation: str        # operator instruction if not green
    elapsed_ms:  int        # how long the check took

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PreflightResult:
    ok:           bool          # True iff zero RED checks
    has_warnings: bool          # True iff any YELLOW
    target_date:  str           # IST date this preflight was computed for
    ran_at:       str           # IST ISO timestamp
    elapsed_ms:   int           # total wall-clock
    checks:       List[CheckResult]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok":           self.ok,
            "has_warnings": self.has_warnings,
            "target_date":  self.target_date,
            "ran_at":       self.ran_at,
            "elapsed_ms":   self.elapsed_ms,
            "red":          [c.name for c in self.checks if c.status == RED],
            "yellow":       [c.name for c in self.checks if c.status == YELLOW],
            "green":        [c.name for c in self.checks if c.status == GREEN],
            "checks":       [c.to_dict() for c in self.checks],
        }


# ── Cache + lock ─────────────────────────────────────────────────────────────

_cache_lock = threading.Lock()
_cached: Optional[PreflightResult] = None
_cached_at: Optional[datetime] = None
CACHE_TTL = timedelta(seconds=60)


def get_cached() -> Optional[PreflightResult]:
    """Return the most recent preflight if it's < CACHE_TTL old. Else None."""
    with _cache_lock:
        if _cached is None or _cached_at is None:
            return None
        if datetime.now(IST) - _cached_at > CACHE_TTL:
            return None
        return _cached


def is_blocking() -> bool:
    """Quick gate: True if the most recent preflight had any RED check.
    Hot path on the deployer + /place — must NOT call into kite or DB."""
    p = get_cached()
    if p is None:
        return False  # no cached result = fail-open (preflight not yet run)
    return not p.ok


# ── The 12 checks ────────────────────────────────────────────────────────────
# Each function takes (kite, ctx) and returns CheckResult.
# ctx is a dict for sharing computed values across checks (e.g. signal rows).

def _now_ist() -> datetime:
    return datetime.now(IST)


def _check(name: str, fn: Callable[[], tuple]) -> CheckResult:
    """Wrap a check function: time it, catch exceptions."""
    t0 = datetime.now()
    try:
        status, detail, remediation = fn()
    except Exception as e:
        log.exception("preflight check %s raised", name)
        status, detail, remediation = RED, f"check raised: {e}", "see backend.log for stack trace"
    elapsed_ms = int((datetime.now() - t0).total_seconds() * 1000)
    return CheckResult(name=name, status=status, detail=detail,
                       remediation=remediation, elapsed_ms=elapsed_ms)


# ── External (Kite) contracts ────────────────────────────────────────────────

def check_kite_token_valid(kite, _ctx: Dict[str, Any]) -> CheckResult:
    """Token decodes and kite.profile() returns. Catches expired/missing tokens."""
    def _fn():
        if kite is None:
            return RED, "kite client not initialised", "refresh token at /admin/kite"
        p = kite.profile()
        user = p.get("user_name") or p.get("user_id") or "?"
        return GREEN, f"profile() OK — user={user}", ""
    return _check("kite_token_valid", _fn)


def check_kite_ip_allowed(kite, _ctx: Dict[str, Any]) -> CheckResult:
    """Probe place_order with intentionally-invalid params. We're looking for
    a validation error, NOT an IP error. If we get IP-not-allowed, the kite
    app's IP allowlist is blocking this machine's egress IP."""
    def _fn():
        if kite is None:
            return RED, "kite client not initialised", "refresh token at /admin/kite"
        try:
            # qty=0 reliably triggers a validation reject post-IP-check.
            kite.place_order(
                variety='regular', exchange='NSE', tradingsymbol='IFCI',
                transaction_type='BUY', quantity=0, product='MTF',
                order_type='MARKET',
            )
            # If we somehow get past validation, that's also passing the IP gate.
            return GREEN, "place_order accepted (qty=0 didn't reject as expected, but IP passed)", ""
        except Exception as e:
            msg = str(e)
            lc = msg.lower()
            if 'ip' in lc and 'not allowed' in lc:
                return RED, f"IP blocked: {msg}", "clear Allowed IPs at https://developers.kite.trade/apps"
            # Any other error means IP passed — broker just rejected the bad order.
            return GREEN, f"IP allowed (validation-layer reject as expected: {msg[:80]})", ""
    return _check("kite_ip_allowed", _fn)


def check_kite_instruments_fresh(kite, ctx: Dict[str, Any]) -> CheckResult:
    """kite.instruments('NSE') returns >5000 rows. Catches stale instrument cache."""
    def _fn():
        from .trade.services import mtf_eligibility
        if kite is None:
            return RED, "kite client not initialised", "refresh token at /admin/kite"
        # Force a refresh — cheap if cache is hot.
        try:
            n = mtf_eligibility._refresh(kite) if hasattr(mtf_eligibility, '_refresh') else None
        except Exception:
            n = None
        if n is None:
            # Cache size as fallback (mtf_eligibility exposes _cache)
            n = len(getattr(mtf_eligibility, '_cache', {}) or {})
        ctx["n_instruments"] = n
        if n < 1000:
            return RED, f"only {n} NSE instruments cached", "check kite.instruments() output, restart backend"
        if n < 4000:
            return YELLOW, f"only {n} instruments — NSE usually has 5000+", "verify exchange filter, instrument refresh"
        return GREEN, f"{n} NSE instruments cached", ""
    return _check("kite_instruments_fresh", _fn)


def check_kite_margins_live(kite, _ctx: Dict[str, Any]) -> CheckResult:
    """kite.order_margins() returns total > 0 for a known-tradable symbol.
    Catches MTF API outage / token-scope issues."""
    def _fn():
        from .trade.services import margin_calc
        if kite is None:
            return RED, "kite client not initialised", "refresh token at /admin/kite"
        result = margin_calc.fetch_margins_batch(kite, [("HFCL", "MTF")], force_refresh=True)
        m = result.get("HFCL")
        if m is None or m <= 0:
            return RED, f"order_margins returned no margin for HFCL: {m}", "verify MTF auth scope, check kite logs"
        return GREEN, f"HFCL MTF margin=Rs.{m:.2f}/sh", ""
    return _check("kite_margins_live", _fn)


# ── Internal (DB) invariants ─────────────────────────────────────────────────

def _falcon_conn():
    from .db import falcon_conn
    return falcon_conn()


def check_ohlc_fresh(_kite, _ctx: Dict[str, Any]) -> CheckResult:
    """MAX(trade_date) in ohlc_daily is recent enough to mine signals from."""
    def _fn():
        with _falcon_conn() as con:
            row = con.execute("SELECT MAX(trade_date) FROM ohlc_daily").fetchone()
            latest = row[0] if row else None
        if not latest:
            return RED, "ohlc_daily is empty", "run daily_data_refresh job"
        today = _now_ist().date()
        latest_d = datetime.strptime(latest, "%Y-%m-%d").date()
        gap = (today - latest_d).days
        # On weekends/holidays the bar can lag — Mon morning sees Fri's bar (gap=3)
        if gap > 5:
            return RED, f"latest bar {latest} ({gap}d old)", "run daily_data_refresh; check data source"
        if gap > 3:
            return YELLOW, f"latest bar {latest} ({gap}d old)", "if weekend/holiday this is fine"
        return GREEN, f"latest bar {latest} ({gap}d old)", ""
    return _check("ohlc_fresh", _fn)


def check_signals_have_entry_date(_kite, ctx: Dict[str, Any]) -> CheckResult:
    """Every row in falcon_signals_live for latest signal_date has entry_date set.
    Catches the NO_SIGNALS_FOR_DATE class — deployer queries by entry_date."""
    def _fn():
        with _falcon_conn() as con:
            row = con.execute("SELECT MAX(signal_date) FROM falcon_signals_live").fetchone()
            latest_sd = row[0] if row else None
            if not latest_sd:
                return RED, "no signals in falcon_signals_live", "run daily_signals job"
            r = con.execute(
                "SELECT COUNT(*) AS total, SUM(CASE WHEN entry_date IS NULL THEN 1 ELSE 0 END) AS nulls "
                "FROM falcon_signals_live WHERE signal_date=?", (latest_sd,)
            ).fetchone()
            total = r[0] or 0
            nulls = r[1] or 0
        ctx["latest_signal_date"] = latest_sd
        if total == 0:
            return RED, f"signal_date={latest_sd} has 0 rows", "run daily_signals job"
        if nulls > 0:
            return RED, f"signal_date={latest_sd}: {nulls}/{total} rows have entry_date=NULL", \
                       "backfill: UPDATE falcon_signals_live SET entry_date=<next_weekday> WHERE entry_date IS NULL"
        return GREEN, f"signal_date={latest_sd} all {total} rows have entry_date", ""
    return _check("signals_have_entry_date", _fn)


def check_signals_recent(_kite, _ctx: Dict[str, Any]) -> CheckResult:
    """Latest signal_date is from the previous trading day.

    This is the bug that bit us on 2026-05-08, 2026-05-12 and 2026-05-13:
    the daily 16:05 IST cron silently missed (backend restart, bad token,
    or scheduler thread blocked) → no signals for today → operator opens
    /trade in the morning and sees yesterday's data, confused why the
    deployer queue is empty.

    Rule: previous trading day's bar must produce signals before today's
    market open. If today_IST is a weekday AND we're past 09:14 IST AND
    latest signal_date is not yesterday-or-today, that's a RED block.
    Soft on weekends / pre-EOD.
    """
    def _fn():
        with _falcon_conn() as con:
            row = con.execute("SELECT MAX(signal_date) FROM falcon_signals_live").fetchone()
            latest = row[0] if row else None
            # Also pull the latest OHLC bar — we expect signals to match it
            ohlc_row = con.execute("SELECT MAX(trade_date) FROM ohlc_daily").fetchone()
            latest_ohlc = ohlc_row[0] if ohlc_row else None
        if not latest:
            return RED, "no signals", "run daily_signals job"
        if latest_ohlc and latest < latest_ohlc:
            # OHLC has Tue's bar but signals are still based on Mon's. Pipeline
            # is mid-run OR daily_signals ran BEFORE daily_data_refresh (ordering bug).
            return RED, (f"signals lag OHLC: latest_signal={latest} < latest_ohlc={latest_ohlc}"),\
                       "run daily_signals job — daily_data_refresh has fresh bars but signals are stale"
        today = _now_ist().date()
        latest_d = datetime.strptime(latest, "%Y-%m-%d").date()
        gap = (today - latest_d).days
        # On a weekday morning past the deploy-ready window, signals must be <=1 day old.
        n = _now_ist()
        past_market_prep = n.weekday() < 5 and (n.hour, n.minute) >= (9, 0)
        if past_market_prep and gap > 1:
            return RED, (f"signals are {gap}d old (latest: {latest}); today is a trading day "
                         f"past 09:00 IST"),\
                       "run pipeline: daily_data_refresh -> daily_features -> daily_signals"
        if gap > 4:
            return RED, f"signals are {gap}d old (latest: {latest})", "run daily_signals job"
        if gap > 2:
            return YELLOW, f"signals are {gap}d old", "consider rerunning daily_signals if not weekend"
        return GREEN, f"signals from {latest} ({gap}d ago)", ""
    return _check("signals_recent", _fn)


def check_promoted_patterns_present(_kite, _ctx: Dict[str, Any]) -> CheckResult:
    """falcon_promoted_patterns has rows. Without these, daily_signals emits nothing."""
    def _fn():
        with _falcon_conn() as con:
            n = con.execute("SELECT COUNT(*) FROM falcon_promoted_patterns").fetchone()[0]
        if n == 0:
            return RED, "0 promoted patterns", "run weekly_remine (publish R&D->PROD)"
        if n < 100:
            return YELLOW, f"only {n} promoted patterns", "expected 500+; check publish_patterns output"
        return GREEN, f"{n} promoted patterns", ""
    return _check("promoted_patterns_present", _fn)


def check_trail_config_present(_kite, _ctx: Dict[str, Any]) -> CheckResult:
    """falcon_trail_config has MTF + CNC rows. Deployer uses these for SL pct."""
    def _fn():
        with _falcon_conn() as con:
            try:
                rows = con.execute("SELECT product FROM falcon_trail_config").fetchall()
            except sqlite3.OperationalError:
                return RED, "falcon_trail_config table missing", "run schema migration"
        products = {r[0] for r in rows}
        missing = {"MTF", "CNC"} - products
        if missing:
            return RED, f"missing rows for: {sorted(missing)}", "insert defaults via /trail-config UI"
        return GREEN, f"products configured: {sorted(products)}", ""
    return _check("trail_config_present", _fn)


def check_no_active_batch(_kite, _ctx: Dict[str, Any]) -> CheckResult:
    """No PENDING/PLACING trade run for today. Prevents double-fire of a batch."""
    def _fn():
        today = _now_ist().date().isoformat()
        with _falcon_conn() as con:
            row = con.execute(
                "SELECT COUNT(*) FROM falcon_trade_runs "
                "WHERE entry_date=? AND status IN ('PENDING','PLACING')", (today,)
            ).fetchone()
        n = row[0] if row else 0
        if n > 0:
            return YELLOW, f"{n} active batch(es) for today", \
                           "wait for current batch to complete OR force-finalize if stuck"
        return GREEN, "no active batch", ""
    return _check("no_active_batch", _fn)


def check_autotrade_flag(_kite, _ctx: Dict[str, Any]) -> CheckResult:
    """FALCON_AUTOTRADE_ENABLED=true in env. Required for any place_order call."""
    def _fn():
        v = os.environ.get("FALCON_AUTOTRADE_ENABLED", "").lower()
        if v != "true":
            return RED, f"FALCON_AUTOTRADE_ENABLED={v!r}", \
                       "set FALCON_AUTOTRADE_ENABLED=true in config/.env and restart backend"
        return GREEN, "FALCON_AUTOTRADE_ENABLED=true", ""
    return _check("autotrade_flag", _fn)


def check_staging_no_stale(_kite, _ctx: Dict[str, Any]) -> CheckResult:
    """No STAGED|QUEUED rows with target_date in the past. Stale staging breaks
    the user's mental model — they think it'll fire and it never will."""
    def _fn():
        today = _now_ist().date().isoformat()
        with _falcon_conn() as con:
            try:
                row = con.execute(
                    "SELECT COUNT(*) FROM falcon_premarket_staging "
                    "WHERE status IN ('STAGED','QUEUED') AND target_date < ?", (today,)
                ).fetchone()
            except sqlite3.OperationalError:
                return YELLOW, "falcon_premarket_staging table not found", "schema migration"
        n = row[0] if row else 0
        if n > 0:
            return YELLOW, f"{n} STAGED/QUEUED rows with target_date < today", \
                          "review /falcon/premarket and Cancel or let them expire"
        return GREEN, "no stale staging rows", ""
    return _check("staging_no_stale", _fn)


def check_top_n_consistent(_kite, _ctx: Dict[str, Any]) -> CheckResult:
    """Latest signal_date has at least TOP_N_DEFAULT picks. Catches the case
    where config was bumped but a pipeline ran with the old default."""
    def _fn():
        from .config import TOP_N_DEFAULT
        with _falcon_conn() as con:
            row = con.execute("SELECT MAX(signal_date) FROM falcon_signals_live").fetchone()
            latest = row[0] if row else None
            if not latest:
                return RED, "no signals", "run daily_signals job"
            n = con.execute(
                "SELECT COUNT(*) FROM falcon_signals_live WHERE signal_date=?", (latest,)
            ).fetchone()[0]
        if n < TOP_N_DEFAULT:
            return YELLOW, f"signal_date={latest} has {n} picks (TOP_N_DEFAULT={TOP_N_DEFAULT})", \
                          "rerun daily_signals with top_n=TOP_N_DEFAULT"
        return GREEN, f"signal_date={latest}: {n} picks (>= TOP_N_DEFAULT={TOP_N_DEFAULT})", ""
    return _check("top_n_consistent", _fn)


# ── The runner ───────────────────────────────────────────────────────────────

_ALL_CHECKS: List[Callable] = [
    check_kite_token_valid,
    check_kite_ip_allowed,
    check_kite_instruments_fresh,
    check_kite_margins_live,
    check_ohlc_fresh,
    check_signals_recent,
    check_signals_have_entry_date,
    check_promoted_patterns_present,
    check_trail_config_present,
    check_no_active_batch,
    check_autotrade_flag,
    check_staging_no_stale,
    check_top_n_consistent,
]


def run(force: bool = False, include_kite: bool = True) -> PreflightResult:
    """Run all checks. Cached for 60s unless force=True.

    Args:
        force: skip cache, re-run everything
        include_kite: if False, skip the 4 kite-dependent checks (boot-time
                      before kite client exists). DB-only checks still run.
    """
    global _cached, _cached_at
    if not force:
        c = get_cached()
        if c is not None:
            return c

    t0 = datetime.now()
    kite = None
    if include_kite:
        try:
            from services.kite_auth import get_kite_client
            kite = get_kite_client(check=False)
        except Exception as e:
            log.warning("preflight: cannot get kite client: %s", e)
            kite = None

    ctx: Dict[str, Any] = {}
    results: List[CheckResult] = []
    for fn in _ALL_CHECKS:
        is_kite_check = fn.__name__.startswith("check_kite_")
        if is_kite_check and not include_kite:
            results.append(CheckResult(
                name=fn.__name__.replace("check_", ""),
                status=YELLOW, detail="skipped (no kite client)",
                remediation="run preflight with kite client available",
                elapsed_ms=0,
            ))
            continue
        results.append(fn(kite, ctx))

    elapsed_ms = int((datetime.now() - t0).total_seconds() * 1000)
    has_red    = any(r.status == RED for r in results)
    has_yellow = any(r.status == YELLOW for r in results)
    target_iso = _now_ist().date().isoformat()
    result = PreflightResult(
        ok=not has_red,
        has_warnings=has_yellow,
        target_date=target_iso,
        ran_at=_now_ist().isoformat(),
        elapsed_ms=elapsed_ms,
        checks=results,
    )
    with _cache_lock:
        _cached = result
        _cached_at = _now_ist()

    n_red = sum(1 for r in results if r.status == RED)
    n_yel = sum(1 for r in results if r.status == YELLOW)
    log.info("preflight: %s (%d ms) red=%d yellow=%d green=%d",
             "OK" if result.ok else "BLOCKED", elapsed_ms,
             n_red, n_yel, len(results) - n_red - n_yel)
    return result


def gate(action: str) -> None:
    """Raise RuntimeError if the most recent preflight had any RED check.

    Hot path on the deployer + /place — fails fast with the specific
    check name so the operator sees what's broken.
    """
    r = get_cached()
    if r is None:
        # No cached preflight yet — be conservative: run it now.
        r = run()
    if r.ok:
        return
    reds = [c for c in r.checks if c.status == RED]
    names = ", ".join(c.name for c in reds)
    msg = f"PREFLIGHT_BLOCKED ({action}): {names}"
    log.error("preflight gate: %s — %d red checks", action, len(reds))
    raise RuntimeError(msg)
