"""Live tier decisions — 9:30:30 / 9:45 / 10:00 IST scheduler core.

Ported from `universe_engine/.../live_tier_tool_v2.py`. Refactored as importable
functions that the scheduler thread (services/scheduler.py) drives.

Tier rules (rank → action when intraday data is available):
  ELITE     (1-3)    : ENTER if first_15_ret > +0.2%
  HIGH      (4-7)    : ENTER if first_15_ret > +0.5% AND vol > 10% of yest day
  MID       (8-14)   : ENTER if first_15_close_loc > 0.66 AND vol > 6%
  LOWER     (15-25)  : ENTER if first_15_ret > +0.5% AND vol > 10%
  TAIL      (26-50)  : ENTER if first_15_ret > +0.5%, else SKIP
  DEEP-TAIL (51-100) : ENTER if very strong (ret > +0.8% AND vol > 15%), else SKIP

Six tier buckets drive the decision rule. The Pick payload collapses 26+ to
"TAIL" for display (Pick v1 contract).

Failure mode:
  Per-symbol Kite error → write action='WAIT' with reason='kite_unavailable'.
  Never poisons the table; never blocks the remaining cycle.

Idempotency:
  UPSERT keyed on (entry_date, cycle, rank). Replace-in-place if cycle re-runs.

Read by: routers/picks_router.py → /api/power/picks/live (sub-200ms read).
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from .explainer import compute_top_n, load_patterns

log = logging.getLogger("kanida.power_user.live_tier")
IST = timezone(timedelta(hours=5, minutes=30))

TOP_N = 100

# Cycle name → (hour, minute, second) IST. The scheduler thread reads this
# constant and wakes accordingly. Tests can monkeypatch.
CYCLES: List[Tuple[str, Tuple[int, int, int]]] = [
    ("0930", (9, 30, 30)),
    ("0945", (9, 45,  0)),
    ("1000", (10, 0,  0)),
]

# Earlier cycles to consult for the LOCK rule (operator policy 2026-05-14).
# When running cycle X, we look up rows from prior cycles for the same rank.
# If the prior cycle's action is ENTER or SKIP, we COPY that decision forward
# instead of re-evaluating. Only WAIT rows get a fresh Kite probe.
EARLIER_CYCLES: Dict[str, List[str]] = {
    "0930": [],                  # first cycle of the day — always evaluates
    "0945": ["0930"],            # consult 0930 first
    "1000": ["0945", "0930"],    # consult most-recent 0945 first, then 0930
}

LOCKED_ACTIONS = frozenset({"ENTER", "SKIP"})   # final once decided


# ──────────────────────────────────────────────────────────────────────────
#  TIER + DECISION RULE
# ──────────────────────────────────────────────────────────────────────────

def tier_of(rank: int) -> str:
    """Internal 6-tier classification (drives the decision rule).

    Note: this differs from explainer.tier_info which returns 5 tiers for
    display. DEEP-TAIL collapses to TAIL in user-facing payloads.
    """
    if rank <= 3:   return "ELITE"
    if rank <= 7:   return "HIGH"
    if rank <= 14:  return "MID"
    if rank <= 25:  return "LOWER"
    if rank <= 50:  return "TAIL"
    return "DEEP-TAIL"


def apply_tier_rule(rank: int,
                    ret_15:    Optional[float],
                    vol_pct:   Optional[float],
                    close_loc: Optional[float]) -> Tuple[str, str, str]:
    """Apply the tier-specific decision rule.

    Returns:
        (action, tier, reason) where action ∈ {ENTER, WAIT, SKIP}.

    If any of ret_15 / vol_pct / close_loc is None (Kite hadn't returned the
    bar yet), action is WAIT — never poison the table with stale data.
    """
    tier = tier_of(rank)
    if any(v is None for v in (ret_15, vol_pct, close_loc)):
        return ("WAIT", tier, "Need first 15 min of market data")

    if tier == "ELITE":
        if ret_15 > 0.2:
            return ("ENTER", tier, f"ret={ret_15:+.2f}% > +0.2%")
        return ("WAIT", tier, f"ret={ret_15:+.2f}% (need > +0.2%)")

    if tier == "HIGH":
        if ret_15 > 0.5 and vol_pct > 10:
            return ("ENTER", tier, f"ret={ret_15:+.2f}% AND vol={vol_pct:.1f}%")
        return ("WAIT", tier,
                f"ret={ret_15:+.2f}%, vol={vol_pct:.1f}% (need ret>+0.5% AND vol>10%)")

    if tier == "MID":
        if close_loc > 0.66 and vol_pct > 6:
            return ("ENTER", tier, f"close_loc={close_loc:.2f} AND vol={vol_pct:.1f}%")
        return ("WAIT", tier, f"close_loc={close_loc:.2f}, vol={vol_pct:.1f}%")

    if tier == "LOWER":
        if ret_15 > 0.5 and vol_pct > 10:
            return ("ENTER", tier, f"ret={ret_15:+.2f}% AND vol={vol_pct:.1f}%")
        return ("SKIP", tier, f"ret={ret_15:+.2f}%, vol={vol_pct:.1f}%")

    if tier == "TAIL":
        if ret_15 > 0.5:
            return ("ENTER", tier, f"ret={ret_15:+.2f}% > +0.5%")
        return ("SKIP", tier, f"ret={ret_15:+.2f}%")

    # DEEP-TAIL (rank > 50)
    if ret_15 > 0.8 and vol_pct > 15:
        return ("ENTER", tier, f"ret={ret_15:+.2f}% AND vol={vol_pct:.1f}% (very strong)")
    return ("SKIP", tier, f"ret={ret_15:+.2f}%, vol={vol_pct:.1f}%")


# ──────────────────────────────────────────────────────────────────────────
#  KITE BAR FETCH
# ──────────────────────────────────────────────────────────────────────────

def _compute_15min_metrics(today_bars: List[Dict[str, Any]],
                            yest_total_vol: float
                            ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """From 15 1-min bars of today + yesterday's total volume → (ret_15, vol_pct, close_loc).

    Returns (None, None, None) if input is insufficient.
    """
    if len(today_bars) < 15 or yest_total_vol <= 0:
        return (None, None, None)
    f15 = today_bars[:15]
    t_open = f15[0]["open"]
    c      = f15[-1]["close"]
    h      = max(b["high"] for b in f15)
    l      = min(b["low"]  for b in f15)
    rng    = h - l
    v15    = sum(b["volume"] for b in f15)
    if t_open <= 0:
        return (None, None, None)
    ret_15    = (c / t_open - 1) * 100
    close_loc = (c - l) / rng if rng > 0 else 0.5
    vol_pct   = v15 / yest_total_vol * 100
    return (ret_15, vol_pct, close_loc)


def _fetch_symbol_15min(kite,
                         tradingsymbol: str,
                         sym_to_token: Dict[str, int],
                         yesterday_str: str,
                         today_str: str
                         ) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
    """Fetch first 15-min of today + yesterday's total vol for one symbol.

    Returns (ret_15, vol_pct, close_loc, error_msg).
    error_msg is None on success; populated on Kite failure.
    """
    tok = sym_to_token.get(tradingsymbol)
    if not tok:
        return (None, None, None, "no_token")
    try:
        bars = kite.historical_data(
            instrument_token=tok,
            from_date=f"{yesterday_str} 09:15:00",
            to_date=f"{today_str} 15:30:00",
            interval="minute",
        )
    except Exception as e:
        return (None, None, None, f"kite_error:{str(e)[:80]}")

    yest_bars  = [b for b in bars if str(b["date"])[:10] == yesterday_str]
    today_bars = [b for b in bars if str(b["date"])[:10] == today_str]
    yest_total_vol = sum(b["volume"] for b in yest_bars) if yest_bars else 0

    ret_15, vol_pct, close_loc = _compute_15min_metrics(today_bars, yest_total_vol)
    return (ret_15, vol_pct, close_loc, None)


# ──────────────────────────────────────────────────────────────────────────
#  CYCLE RUNNER — writes to falcon_live_decisions
# ──────────────────────────────────────────────────────────────────────────

def _now_ist() -> datetime:
    return datetime.now(IST)


def _lookup_prior_decision(con: sqlite3.Connection,
                            entry_date: str,
                            rank: int,
                            current_cycle: str,
                            ) -> Optional[Dict[str, Any]]:
    """For cycle 0945 / 1000: look up earlier cycles for this rank.

    Returns the prior row dict if it's a LOCKED action (ENTER or SKIP),
    else None (meaning: re-evaluate fresh).

    LOCK SEMANTICS (operator policy 2026-05-14):
      Once 0930 decides ENTER or SKIP for rank N, 0945 and 1000 cycles
      copy that decision forward — never flip it. Traders who acted on
      a 9:30 ENTER will not see the app retroactively say "we changed
      our mind." Only WAIT rows are eligible for re-evaluation.
    """
    for prior_cycle in EARLIER_CYCLES.get(current_cycle, []):
        row = con.execute("""
            SELECT cycle, action, reason, ret_15, vol_pct, close_loc, decided_at_cycle, tier
              FROM falcon_live_decisions
             WHERE entry_date = ? AND cycle = ? AND rank = ?
        """, (entry_date, prior_cycle, rank)).fetchone()
        if row is None:
            continue
        cyc, action, reason, ret_15, vol_pct, close_loc, decided_at, tier = row
        if action in LOCKED_ACTIONS:
            return {
                "action":           action,
                "reason":           reason,
                "ret_15":           ret_15,
                "vol_pct":          vol_pct,
                "close_loc":        close_loc,
                "decided_at_cycle": decided_at or cyc,
                "tier":             tier,
            }
        # Was WAIT in the prior cycle — keep looking at even earlier ones
        # (though in practice 0930 is the only one before 0945)
    return None


def run_cycle(cycle_name: str,
              kite: Optional[Any] = None,
              con:  Optional[sqlite3.Connection] = None,
              top_n: int = TOP_N,
              sleep_between_symbols: float = 0.35,
              ) -> Dict[str, Any]:
    """Run one cycle: compute top-N → fetch 15-min per symbol → apply rule → UPSERT.

    Args:
        cycle_name — one of {'0930','0945','1000'}; tagged into the DB row
        kite       — KiteConnect client; if None, fetched via get_kite_client()
        con        — open SQLite connection; if None, opened from kanida_universe.db
        top_n      — number of picks to evaluate (default 100; smaller for tests)

    Returns:
        {
          "cycle":         "0930",
          "signal_date":   "2026-05-14",
          "entry_date":    "2026-05-15",
          "computed_at":   IST ISO,
          "n_evaluated":   100,
          "n_enter":       12,
          "n_wait":        8,
          "n_skip":        80,
          "n_kite_errors": 0,
          "elapsed_sec":   34.5,
        }
    """
    t0 = time.time()
    valid_cycles = {c[0] for c in CYCLES}
    if cycle_name not in valid_cycles:
        raise ValueError(f"unknown cycle_name {cycle_name!r}; expected one of {sorted(valid_cycles)}")

    # ── Set up DB + Kite client ────────────────────────────────────
    own_con = False
    if con is None:
        from ..config import POWER_DB_PATH    # noqa: WPS433 — lazy import
        con = sqlite3.connect(POWER_DB_PATH, timeout=30.0)
        con.row_factory = sqlite3.Row
        own_con = True

    if kite is None:
        try:
            from services.kite_auth import get_kite_client   # noqa: WPS433
            kite = get_kite_client(check=True)
        except Exception as e:
            log.error("live_tier: cannot get Kite client: %s", e)
            if own_con:
                con.close()
            return {"cycle": cycle_name, "error": f"NO_KITE_CLIENT: {e}",
                    "elapsed_sec": time.time() - t0}

    # ── Compute top-N from features (re-use explainer's compute_top_n) ──
    signal_date = con.execute("SELECT MAX(trade_date) FROM falcon_features").fetchone()[0]
    if not signal_date:
        if own_con: con.close()
        return {"cycle": cycle_name, "error": "NO_SIGNAL_DATE", "elapsed_sec": time.time() - t0}

    today_ist = _now_ist().date().isoformat()
    patterns  = load_patterns(con)
    picks     = compute_top_n(con, signal_date, top_n=top_n, patterns=patterns)
    if not picks:
        if own_con: con.close()
        return {"cycle": cycle_name, "error": "NO_PICKS", "signal_date": signal_date,
                "elapsed_sec": time.time() - t0}

    # ── Build symbol → instrument_token map (one Kite call) ────────────
    try:
        instruments = kite.instruments("NSE")
        sym_to_token = {i["tradingsymbol"]: i["instrument_token"]
                        for i in instruments if i["segment"] == "NSE"}
    except Exception as e:
        log.error("live_tier: kite.instruments() failed: %s", e)
        if own_con: con.close()
        return {"cycle": cycle_name, "error": f"INSTRUMENTS_FAILED: {e}",
                "elapsed_sec": time.time() - t0}

    # ── Per-symbol fetch + decision + UPSERT ───────────────────────────
    # Lock semantics applied here for cycle 0945 / 1000:
    #   For each rank, check prior cycles via _lookup_prior_decision.
    #   If a LOCKED action (ENTER/SKIP) exists → copy forward, mark
    #   decided_at_cycle = the earlier cycle name. Skip the Kite probe.
    #   Else → fresh Kite fetch + apply_tier_rule.
    n_enter = 0; n_wait = 0; n_skip = 0; n_kite_errors = 0; n_locked = 0
    computed_at = _now_ist().isoformat()

    for rank_idx, pick in enumerate(picks, start=1):
        sym = pick["symbol"]

        # ── LOCK CHECK ─────────────────────────────────────────────
        locked = _lookup_prior_decision(con, today_ist, rank_idx, cycle_name)
        if locked is not None:
            n_locked += 1
            action    = locked["action"]
            tier      = locked["tier"]
            reason    = f"locked from {locked['decided_at_cycle']}: {locked['reason']}"
            ret_15    = locked["ret_15"]
            vol_pct   = locked["vol_pct"]
            close_loc = locked["close_loc"]
            decided_at = locked["decided_at_cycle"]
        else:
            # ── FRESH EVALUATION ────────────────────────────────────
            ret_15, vol_pct, close_loc, err = _fetch_symbol_15min(
                kite, sym, sym_to_token, signal_date, today_ist
            )
            if err is not None:
                n_kite_errors += 1
                action, tier, reason = "WAIT", tier_of(rank_idx), f"kite_unavailable: {err}"
                ret_15 = vol_pct = close_loc = None
            else:
                action, tier, reason = apply_tier_rule(rank_idx, ret_15, vol_pct, close_loc)
            decided_at = cycle_name

        if   action == "ENTER": n_enter += 1
        elif action == "WAIT":  n_wait  += 1
        else:                   n_skip  += 1

        con.execute("""
            INSERT INTO falcon_live_decisions
              (signal_date, entry_date, cycle, rank, symbol, sector, score, tier,
               action, reason, decided_at_cycle, ret_15, vol_pct, close_loc, computed_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(entry_date, cycle, rank) DO UPDATE SET
              symbol           = excluded.symbol,
              sector           = excluded.sector,
              score            = excluded.score,
              tier             = excluded.tier,
              action           = excluded.action,
              reason           = excluded.reason,
              decided_at_cycle = excluded.decided_at_cycle,
              ret_15           = excluded.ret_15,
              vol_pct          = excluded.vol_pct,
              close_loc        = excluded.close_loc,
              computed_at      = excluded.computed_at
        """, (
            signal_date, today_ist, cycle_name, rank_idx,
            sym, pick["sector"] or None, round(pick["score"], 2),
            tier, action, reason, decided_at,
            ret_15, vol_pct, close_loc, computed_at,
        ))

        # Courtesy throttle — only when we actually hit Kite
        if sleep_between_symbols > 0 and locked is None:
            time.sleep(sleep_between_symbols)

    con.commit()

    elapsed = round(time.time() - t0, 2)
    summary = {
        "cycle":          cycle_name,
        "signal_date":    signal_date,
        "entry_date":     today_ist,
        "computed_at":    computed_at,
        "n_evaluated":    len(picks),
        "n_locked":       n_locked,     # rows copied forward from earlier cycle
        "n_enter":        n_enter,
        "n_wait":         n_wait,
        "n_skip":         n_skip,
        "n_kite_errors":  n_kite_errors,
        "elapsed_sec":    elapsed,
    }
    log.info("live_tier cycle=%s evaluated=%d locked=%d enter=%d wait=%d skip=%d kite_err=%d (%.1fs)",
             cycle_name, len(picks), n_locked, n_enter, n_wait, n_skip, n_kite_errors, elapsed)
    if own_con:
        con.close()
    return summary


def get_decisions(con: sqlite3.Connection,
                   entry_date: Optional[str] = None,
                   cycle: str = "latest",
                   ) -> Dict[str, Any]:
    """Read pre-computed decisions for a given entry_date + cycle.

    Sub-millisecond response — single indexed SELECT, no Kite call.
    Powers /api/power/picks/live.

    Args:
        con        — open SQLite connection
        entry_date — defaults to today IST
        cycle      — '0930' | '0945' | '1000' | 'latest' (most recent for the day)
    """
    if entry_date is None:
        entry_date = _now_ist().date().isoformat()

    if cycle == "latest":
        # most recent cycle that has rows for this entry_date
        row = con.execute(
            "SELECT cycle FROM falcon_live_decisions "
            "WHERE entry_date = ? ORDER BY cycle DESC LIMIT 1",
            (entry_date,)
        ).fetchone()
        if not row:
            return {"entry_date": entry_date, "cycle": None,
                    "summary": {"enter": 0, "wait": 0, "skip": 0}, "decisions": []}
        cycle = row[0]

    rows = con.execute(
        """SELECT rank, symbol, sector, score, tier, action, reason,
                  decided_at_cycle,
                  ret_15, vol_pct, close_loc, computed_at, signal_date
             FROM falcon_live_decisions
            WHERE entry_date = ? AND cycle = ?
            ORDER BY rank ASC""",
        (entry_date, cycle)
    ).fetchall()

    decisions: List[Dict[str, Any]] = []
    n_enter = n_wait = n_skip = n_locked = 0
    computed_at = None
    signal_date = None
    for r in rows:
        d = dict(zip(
            ("rank","symbol","sector","score","tier","action","reason",
             "decided_at_cycle",
             "ret_15","vol_pct","close_loc","computed_at","signal_date"),
            r
        ))
        decisions.append(d)
        if   d["action"] == "ENTER": n_enter += 1
        elif d["action"] == "WAIT":  n_wait  += 1
        else:                        n_skip  += 1
        # Locked = decided in an earlier cycle than the one being read
        if d["decided_at_cycle"] and d["decided_at_cycle"] != cycle:
            n_locked += 1
        computed_at = d["computed_at"]
        signal_date = d["signal_date"]

    return {
        "entry_date":  entry_date,
        "signal_date": signal_date,
        "cycle":       cycle,
        "computed_at": computed_at,
        "summary":     {
            "enter":  n_enter,
            "wait":   n_wait,
            "skip":   n_skip,
            "locked": n_locked,   # how many decisions carried forward from earlier cycle
        },
        "decisions":   decisions,
    }
