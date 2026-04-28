"""
KANIDA Signals — Signal Roster Management
==========================================
Derives signal_roster status from stock_signal_fitness scores.

TIER SYSTEM (2-axis: quality × frequency)
─────────────────────────────────────────
  core_active     : high quality + high frequency  (size 1.00)
  high_conviction : high quality + low frequency   (size 0.75)
  steady          : mid  quality + high frequency  (size 0.40)
  emerging        : mid  quality + any frequency   (size 0.15)
  experimental    : low  sample, promising         (size 0.05)
  retired         : failed evidence OR stale       (gated out)

Back-compat legacy `status` column is still written:
  core_active / high_conviction → 'active'
  steady                        → 'watchlist'
  emerging / experimental       → 'test'
  retired                       → 'retired'

Demotion is logged with a reason string.
Promotions and demotions are tracked via status_changed_at.
"""

import sqlite3
import time
from datetime import datetime, timezone, date
from typing import Optional

from .db import get_conn, SIGNALS_DB_PATH

# ── Legacy thresholds (kept for back-compat / derive_status) ──────
ACTIVE_THRESHOLD    = 55.0
WATCHLIST_THRESHOLD = 38.0
RETIRE_THRESHOLD    = 20.0
MIN_APPEARANCES = {"active": 20, "watchlist": 12, "test": 5}
MIN_RECENT_FOR_ACTIVE = 8

# ── New tier thresholds ───────────────────────────────────────────
# Quality axis
CORE_FITNESS_MIN      = 55.0
CORE_WIN15_MIN        = 0.55
HC_FITNESS_MIN        = 60.0
HC_WILSON_MIN         = 0.55
HC_WIN15_MIN          = 0.60
STEADY_FITNESS_MIN    = 45.0
STEADY_WILSON_MIN     = 0.45
EMERGING_FITNESS_MIN  = 45.0
EXPERIMENTAL_FIT_MIN  = 35.0

# Sample axis
CORE_TOTAL_MIN        = 20
STEADY_TOTAL_MIN      = 20
EMERGING_TOTAL_MIN    = 12
EXPERIMENTAL_TOTAL_MIN = 5

# Frequency axis (recent_appearances in last 90d, timeframe-aware)
RECENT_HIGH_1D        = 6
RECENT_HIGH_1W        = 3
RECENT_MED_1D         = 4
RECENT_MED_1W         = 2

# Retire gates
RETIRE_FITNESS_BELOW  = 20.0
RETIRE_WIN15_BELOW    = 0.40
RETIRE_WIN_MIN_N      = 12
STALE_DAYS_MAX        = 365

# Sizing
TIER_SIZE = {
    "core_active":     1.00,
    "high_conviction": 0.75,
    "steady":          0.40,
    "emerging":        0.15,
    "experimental":    0.05,
    "retired":         0.00,
}

# Legacy status map
TIER_TO_STATUS = {
    "core_active":     "active",
    "high_conviction": "active",
    "steady":          "watchlist",
    "emerging":        "test",
    "experimental":    "test",
    "retired":         "retired",
}


def _is_high_freq(recent: int, timeframe: str) -> bool:
    thr = RECENT_HIGH_1W if timeframe == "1W" else RECENT_HIGH_1D
    return recent >= thr


def _is_med_freq(recent: int, timeframe: str) -> bool:
    thr = RECENT_MED_1W if timeframe == "1W" else RECENT_MED_1D
    return recent >= thr


def _days_since(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    try:
        return (date.today() - date.fromisoformat(date_str[:10])).days
    except Exception:
        return None


# ── Bias-aware calibration offset ─────────────────────────────────
# Bearish 15d win-rates structurally sit below bullish (markets rise more
# than fall). Without this offset, bearish signals are permanently stuck
# in `emerging`/`experimental` because they cannot clear bullish-calibrated
# win-rate and wilson floors — which makes the bearish ranking look dead
# on the dashboard even when high-quality bearish setups are firing.
#
# Observed global distributions (recent_appearances >= 3):
#   bullish: avg_wr15=0.613 max=1.000  avg_wilson=0.537 max=0.785
#   bearish: avg_wr15=0.406 max=0.450  avg_wilson=0.322 max=0.414
# The bearish mean sits ~0.21 below bullish mean on both axes, so we
# shift bearish floors down by 0.20 to place bearish at the same
# percentile of its own distribution that bullish floors occupy.
BIAS_WIN15_OFFSET  = {"bullish": 0.00, "bearish": -0.20, "neutral": 0.00}
BIAS_WILSON_OFFSET = {"bullish": 0.00, "bearish": -0.20, "neutral": 0.00}


def derive_tier(
    fitness: float,
    wilson_lower: Optional[float],
    win_rate_15d: Optional[float],
    total: int,
    recent: int,
    timeframe: str,
    last_signal_date: Optional[str],
    bias: str = "bullish",
) -> tuple[str, str, str, str, Optional[str]]:
    """
    Returns (tier, quality_grade, frequency_grade, legacy_status, reason).
    Evaluated in strictest-first order. Thresholds are bias-aware:
    bearish floors are calibrated to the structurally lower bearish
    win-rate distribution so both biases populate the tier ladder.
    """
    f    = fitness or 0.0
    wl   = wilson_lower if wilson_lower is not None else 0.0
    w15  = win_rate_15d if win_rate_15d is not None else 0.0
    tot  = total or 0
    rec  = recent or 0

    # Bias-aware win/wilson floors
    w_off  = BIAS_WIN15_OFFSET.get(bias, 0.0)
    wl_off = BIAS_WILSON_OFFSET.get(bias, 0.0)
    core_w    = CORE_WIN15_MIN    + w_off
    hc_w      = HC_WIN15_MIN      + w_off
    hc_wl     = HC_WILSON_MIN     + wl_off
    steady_wl = STEADY_WILSON_MIN + wl_off
    retire_w  = RETIRE_WIN15_BELOW + w_off

    # Frequency grade (independent axis for reporting)
    if _is_high_freq(rec, timeframe):
        freq_grade = "high"
    elif _is_med_freq(rec, timeframe):
        freq_grade = "medium"
    else:
        freq_grade = "low"

    # Quality grade (independent axis for reporting, bias-aware)
    if f >= HC_FITNESS_MIN and wl >= hc_wl and w15 >= hc_w:
        quality_grade = "high"
    elif f >= STEADY_FITNESS_MIN and wl >= steady_wl:
        quality_grade = "medium"
    elif f >= EXPERIMENTAL_FIT_MIN:
        quality_grade = "low"
    else:
        quality_grade = "poor"

    # Stale / hard-retire checks first
    days_stale = _days_since(last_signal_date)
    if days_stale is not None and days_stale > STALE_DAYS_MAX:
        return ("retired", quality_grade, freq_grade, "retired",
                f"stale: no signals in {days_stale}d")
    if f < RETIRE_FITNESS_BELOW:
        return ("retired", quality_grade, freq_grade, "retired",
                f"fitness {f:.1f} below {RETIRE_FITNESS_BELOW}")
    if tot >= RETIRE_WIN_MIN_N and w15 < retire_w:
        return ("retired", quality_grade, freq_grade, "retired",
                f"win15 {w15:.2f} < {retire_w:.2f} on n={tot}")

    # CORE_ACTIVE: top quality + high frequency + proven win rate
    if (f >= CORE_FITNESS_MIN and w15 >= core_w
            and tot >= CORE_TOTAL_MIN and _is_high_freq(rec, timeframe)):
        return ("core_active", quality_grade, freq_grade, "active", None)

    # HIGH_CONVICTION: exceptional quality, any frequency
    if (f >= HC_FITNESS_MIN and wl >= hc_wl
            and w15 >= hc_w and tot >= CORE_TOTAL_MIN):
        return ("high_conviction", quality_grade, freq_grade, "active", None)

    # STEADY: reliable quality, high frequency
    if (f >= STEADY_FITNESS_MIN and wl >= steady_wl
            and tot >= STEADY_TOTAL_MIN and _is_med_freq(rec, timeframe)):
        return ("steady", quality_grade, freq_grade, "watchlist", None)

    # EMERGING: mid quality, building track record
    if f >= EMERGING_FITNESS_MIN and tot >= EMERGING_TOTAL_MIN:
        return ("emerging", quality_grade, freq_grade, "test", None)

    # EXPERIMENTAL: low sample, promising fitness
    if f >= EXPERIMENTAL_FIT_MIN and tot >= EXPERIMENTAL_TOTAL_MIN:
        return ("experimental", quality_grade, freq_grade, "test", None)

    return ("retired", quality_grade, freq_grade, "retired",
            f"below experimental floor (f={f:.1f}, n={tot})")


def derive_trend_gate(
    overall_wr: float,
    wr_up: Optional[float],
    wr_dn: Optional[float],
    wr_rg: Optional[float],
    n_up: int,
    n_dn: int,
    n_rg: int,
) -> Optional[str]:
    """
    Returns comma-separated allowed trend states, or None for no gate.

    A gate is set when one or more states are significantly better than
    the overall win rate (>= MIN_EDGE advantage, >= MIN_STATE_N samples)
    AND at least one state is genuinely worse (< overall - 0.05).
    This ensures gating only activates when the signal is clearly
    regime-dependent, not just broadly strong.
    """
    MIN_STATE_N = 8
    MIN_EDGE    = 0.15   # state win_rate must beat overall by this much

    candidates: dict[str, float] = {}
    for state, wr, n in [
        ("UPTREND",   wr_up, n_up),
        ("DOWNTREND", wr_dn, n_dn),
        ("RANGE",     wr_rg, n_rg),
    ]:
        if wr is not None and n >= MIN_STATE_N:
            candidates[state] = wr

    if not candidates:
        return None

    strong = [s for s, wr in candidates.items() if wr >= overall_wr + MIN_EDGE]
    if not strong:
        return None

    # Only gate if there are genuinely weak states to gate out
    weak = [s for s, wr in candidates.items() if s not in strong and wr < overall_wr - 0.05]
    if not weak:
        return None

    return ",".join(sorted(strong))


def derive_status(
    fitness_score: float,
    total_appearances: int,
    recent_appearances: int,
    current_status: Optional[str] = None,
) -> tuple[str, Optional[str]]:
    """
    Returns (new_status, demotion_reason).
    demotion_reason is None unless the signal is being demoted/retired.
    """
    # Hard retire: score too low regardless of history
    if fitness_score < RETIRE_THRESHOLD:
        return "retired", f"fitness {fitness_score:.1f} below retire threshold {RETIRE_THRESHOLD}"

    # Sample gate: not enough history for any meaningful status
    if total_appearances < MIN_APPEARANCES["test"]:
        return "test", None

    # Active requires both fitness AND recent activity
    if (fitness_score >= ACTIVE_THRESHOLD
            and total_appearances >= MIN_APPEARANCES["active"]
            and recent_appearances >= MIN_RECENT_FOR_ACTIVE):
        return "active", None

    # Watchlist
    if (fitness_score >= WATCHLIST_THRESHOLD
            and total_appearances >= MIN_APPEARANCES["watchlist"]):
        return "watchlist", None

    # Demote active → watchlist if fitness dropped or insufficient recent firings
    if current_status == "active":
        if recent_appearances < MIN_RECENT_FOR_ACTIVE:
            return "watchlist", f"insufficient recent firings ({recent_appearances} < {MIN_RECENT_FOR_ACTIVE})"
        if fitness_score < WATCHLIST_THRESHOLD:
            return "watchlist", f"fitness {fitness_score:.1f} dropped below watchlist threshold"

    return "test", None


def update_roster_for_stock(
    ticker: str,
    market: str,
    conn: sqlite3.Connection,
) -> dict:
    """
    Read fitness scores for all strategies of this stock and
    upsert signal_roster. Returns change counts.
    """
    fitness_rows = conn.execute(
        """SELECT timeframe, strategy_name, bias,
                  fitness_score, total_appearances, recent_appearances,
                  win_rate_15d,
                  win_rate_in_uptrend,   appearances_in_uptrend,
                  win_rate_in_downtrend, appearances_in_downtrend,
                  win_rate_in_range,     appearances_in_range,
                  wilson_lower_15d, last_signal_date
           FROM stock_signal_fitness
           WHERE ticker=? AND market=?""",
        (ticker, market),
    ).fetchall()

    if not fitness_rows:
        return {"evaluated": 0}

    existing = {
        (r[0], r[1], r[2]): dict(r)
        for r in conn.execute(
            """SELECT timeframe, strategy_name, bias, status,
                      fitness_score, fitness_at_promotion, appearances_since_promotion,
                      trend_gate
               FROM signal_roster WHERE ticker=? AND market=?""",
            (ticker, market),
        ).fetchall()
    }

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    changes = {"promoted_to_active": 0, "demoted_from_active": 0,
               "promoted_to_watchlist": 0, "newly_retired": 0,
               "unchanged": 0, "evaluated": 0}

    upsert_rows = []
    for r in fitness_rows:
        (tf, strat, bias, fitness, n, n_recent,
         overall_wr,
         wr_up, n_up, wr_dn, n_dn, wr_rg, n_rg,
         wilson_lower, last_signal_date) = (
            r[0], r[1], r[2], r[3], r[4], r[5],
            r[6], r[7], r[8], r[9], r[10], r[11], r[12],
            r[13], r[14]
        )
        key = (tf, strat, bias)
        prev = existing.get(key)
        prev_status = prev["status"] if prev else None

        tier, q_grade, f_grade, new_status, reason = derive_tier(
            fitness, wilson_lower, overall_wr, n, n_recent, tf, last_signal_date,
            bias,
        )
        size_mult = TIER_SIZE[tier]

        trend_gate = derive_trend_gate(
            overall_wr or 0.0,
            wr_up, wr_dn, wr_rg,
            n_up or 0, n_dn or 0, n_rg or 0,
        )

        # Track changes (keyed off legacy status for back-compat summary)
        changes["evaluated"] += 1
        if prev_status != new_status:
            if new_status == "active":
                changes["promoted_to_active"] += 1
            elif new_status == "retired":
                changes["newly_retired"] += 1
            elif prev_status == "active" and new_status != "active":
                changes["demoted_from_active"] += 1
            elif new_status == "watchlist":
                changes["promoted_to_watchlist"] += 1
        else:
            changes["unchanged"] += 1

        upsert_rows.append((
            ticker, market, tf, strat, bias,
            new_status,
            round(fitness, 2),
            round(fitness, 2) if prev_status != new_status else (
                prev["fitness_at_promotion"] if prev else round(fitness, 2)
            ),
            0 if prev_status != new_status else (prev["appearances_since_promotion"] or 0),
            None,           # win_rate_since_promotion (updated by paper_trades tracker)
            trend_gate,
            prev_status,
            now_str,
            reason,
            now_str,
            tier, q_grade, f_grade, size_mult,
        ))

    with conn:
        conn.executemany(
            """INSERT OR REPLACE INTO signal_roster (
                ticker, market, timeframe, strategy_name, bias,
                status, fitness_score, fitness_at_promotion,
                appearances_since_promotion, win_rate_since_promotion,
                trend_gate,
                previous_status, status_changed_at, demotion_reason,
                last_calibrated_at,
                tier, quality_grade, frequency_grade, size_multiplier
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            upsert_rows,
        )

    return changes


def run_all(
    market: str = "NSE",
    db_path: str = SIGNALS_DB_PATH,
) -> None:
    conn = get_conn(db_path)
    tickers = [r[0] for r in conn.execute(
        "SELECT DISTINCT ticker FROM stock_signal_fitness WHERE market=? ORDER BY ticker",
        (market,)
    ).fetchall()]
    conn.close()

    if not tickers:
        print("No fitness data found. Run fitness computation first.")
        return

    print(f"\nRoster Update — {market} — {len(tickers)} tickers")
    t0 = time.time()

    totals = {"promoted_to_active": 0, "demoted_from_active": 0,
              "promoted_to_watchlist": 0, "newly_retired": 0,
              "unchanged": 0, "evaluated": 0}

    conn = get_conn(db_path)
    for ticker in tickers:
        ch = update_roster_for_stock(ticker, market, conn)
        for k in totals:
            totals[k] += ch.get(k, 0)
    conn.close()

    print(f"  Done in {time.time()-t0:.1f}s")
    print(f"\n  Evaluated        : {totals['evaluated']:,}")
    print(f"  → Active         : {totals['promoted_to_active']:,}")
    print(f"  → Watchlist      : {totals['promoted_to_watchlist']:,}")
    print(f"  → Retired        : {totals['newly_retired']:,}")
    print(f"  → Demoted        : {totals['demoted_from_active']:,}")
    print(f"  Unchanged        : {totals['unchanged']:,}")

    # Summary by status
    conn = get_conn(db_path)
    print("\n── Roster by Status ────────────────────────────────────────")
    status_rows = conn.execute("""
        SELECT status, COUNT(*) AS cnt,
               ROUND(AVG(fitness_score), 1) AS avg_fitness
        FROM signal_roster WHERE market=?
        GROUP BY status ORDER BY cnt DESC
    """, (market,)).fetchall()
    for r in status_rows:
        print(f"  {r[0]:<12} {r[1]:>6,} signals   avg fitness: {r[2]}")

    print("\n── Roster by Tier ──────────────────────────────────────────")
    tier_rows = conn.execute("""
        SELECT tier, COUNT(*) AS cnt,
               ROUND(AVG(fitness_score), 1) AS avg_fitness,
               ROUND(AVG(size_multiplier), 2) AS avg_size
        FROM signal_roster WHERE market=?
        GROUP BY tier ORDER BY avg_size DESC
    """, (market,)).fetchall()
    for r in tier_rows:
        print(f"  {(r[0] or 'NULL'):<16} {r[1]:>6,}   avg fitness: {r[2]}   avg size: {r[3]}")

    print("\n── Roster by Quality × Frequency ───────────────────────────")
    qf_rows = conn.execute("""
        SELECT quality_grade, frequency_grade, COUNT(*) AS cnt
        FROM signal_roster WHERE market=?
        GROUP BY quality_grade, frequency_grade
        ORDER BY quality_grade, frequency_grade
    """, (market,)).fetchall()
    for r in qf_rows:
        print(f"  quality={(r[0] or 'NULL'):<8} freq={(r[1] or 'NULL'):<8} → {r[2]:>6,}")
    conn.close()
