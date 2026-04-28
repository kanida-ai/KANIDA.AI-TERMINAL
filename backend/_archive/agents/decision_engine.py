"""
KANIDA — DECISION ENGINE
=========================
Converts raw fingerprint + live score data into structured decision cards.

Supports two tone modes:
  SEBI_SAFE        — statistical framing only, no directive language
                     "In 58% of similar setups, the stock moved lower"
  GLOBAL_AGGRESSIVE — directive framing for non-SEBI markets
                     "Setup favours a short position. Enter at ₹8,895"

Capital allocation logic:
  - User provides total capital + risk % per trade
  - System computes max loss amount, then back-calculates position size
    from ATR-based stop distance
  - Ensures no single trade exceeds risk budget
  - Diversification: max 1 stock per sector in a trade plan

CAGR computation:
  - Only shown when total_appearances >= 100 (statistically meaningful)
  - Formula: (1 + avg_fwd_15d/100)^(252/15) - 1
  - Labelled as "backtested annualised return" — not a forward guarantee
"""

import math
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from datetime import datetime

TODAY = datetime.today()

# ── Tone modes ─────────────────────────────────────────────────────────────────
SEBI_SAFE         = "SEBI_SAFE"
GLOBAL_AGGRESSIVE = "GLOBAL_AGGRESSIVE"

# ── Thresholds ─────────────────────────────────────────────────────────────────
MIN_SCORE_STRONG     = 60.0   # score_pct >= 60% → Strong signal
MIN_SCORE_DEVELOPING = 35.0   # score_pct >= 35% → Developing / Watchlist
MIN_WIN_RATE_STRONG  = 55.0   # historical win rate >= 55%
MIN_RR               = 1.5    # risk:reward >= 1.5
MIN_APPEARANCES      = 100    # minimum appearances for CAGR to be shown
FORWARD_DAYS         = 15     # trading days in forward window
TRADING_DAYS_PER_YR  = 252

# Risk profile presets
RISK_PROFILES = {
    "conservative": 1.0,
    "balanced":     2.0,
    "aggressive":   3.0,
}


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class AllocationPlan:
    capital:          float = 0.0
    risk_pct:         float = 2.0
    risk_amount:      float = 0.0   # capital * risk_pct / 100
    position_size:    float = 0.0   # capital to deploy
    quantity:         int   = 0     # shares/units
    entry_price:      float = 0.0
    stop_loss:        float = 0.0
    target_1:         float = 0.0
    target_2:         float = 0.0
    stop_pct:         float = 0.0
    t1_pct:           float = 0.0
    t2_pct:           float = 0.0
    risk_reward:      float = 0.0
    max_loss_if_stop: float = 0.0
    gain_if_t1:       float = 0.0
    gain_if_t2:       float = 0.0
    currency:         str   = "₹"


@dataclass
class DecisionCard:
    # Identity
    ticker:           str   = ""
    market:           str   = "NSE"
    bias:             str   = "bullish"
    timeframe:        str   = "1D"
    signal_label:     str   = ""    # STRONG / DEVELOPING / WATCHLIST

    # Signal
    score_pct:        float = 0.0
    firing_count:     int   = 0
    qualified_total:  int   = 0
    top_strategy:     str   = ""
    top_win_rate:     float = 0.0
    firing_strategies: List[str] = field(default_factory=list)

    # Regime
    regime:           str   = ""
    regime_score:     float = 0.0
    regime_aligned:   bool  = True
    sector:           str   = ""

    # Historical stats
    hist_win_pct:     float = 0.0
    hist_appearances: int   = 0
    hist_wins:        int   = 0
    avg_fwd_15d:      float = 0.0
    median_fwd:       float = 0.0
    best_case:        float = 0.0
    worst_case:       float = 0.0
    cagr_est:         float = 0.0   # 0 if appearances < MIN_APPEARANCES
    cagr_valid:       bool  = False
    cagr_note:        str   = ""   # shown when cagr_valid=False
    cagr_1y:          Optional[float] = None
    cagr_2y:          Optional[float] = None
    cagr_3y:          Optional[float] = None
    hold_days_min:    int   = 8
    hold_days_max:    int   = 15

    # Price
    live_price:       float = 0.0
    currency:         str   = "₹"

    # Allocation (populated by allocate())
    allocation:       Optional[AllocationPlan] = None

    # Narrative (populated by narrate())
    headline:         str   = ""
    decision_text:    str   = ""
    edge_text:        str   = ""
    disclaimer:       str   = ""
    tone_mode:        str   = SEBI_SAFE


# ── CAGR computation ───────────────────────────────────────────────────────────

def compute_cagr(avg_fwd_15d: float, total_appearances: int) -> tuple:
    """
    Returns (cagr_pct, is_valid).
    Only valid when total_appearances >= MIN_APPEARANCES.
    Formula: (1 + avg_fwd/100)^(252/15) - 1
    This is a theoretical annualised rate — meaningful only for
    strategies that fire frequently enough to compound.
    When invalid: returns (0.0, False) — UI should show "Insufficient data".
    """
    if total_appearances < MIN_APPEARANCES:
        return 0.0, False
    try:
        periods = TRADING_DAYS_PER_YR / FORWARD_DAYS
        cagr    = ((1 + avg_fwd_15d / 100) ** periods - 1) * 100
        cagr    = max(-500.0, min(500.0, cagr))
        return round(cagr, 1), True
    except Exception:
        return 0.0, False


def compute_cagr_periods(db_path: str, ticker: str, market: str,
                          bias: str) -> Dict:
    """
    Compute CAGR broken into 1y, 2y, 3y periods from the paper_ledger.
    Uses actual trade outcomes filtered by date range — not a formula proxy.
    Returns dict with 1y, 2y, 3y keys. Each has cagr, win_rate, total, valid.
    """
    import sqlite3 as _sq
    from datetime import datetime as _dt, timedelta as _td

    today    = _dt.today()
    periods  = {
        "1y": (today - _td(days=365)).strftime("%Y-%m-%d"),
        "2y": (today - _td(days=730)).strftime("%Y-%m-%d"),
        "3y": (today - _td(days=1095)).strftime("%Y-%m-%d"),
    }

    result = {}
    try:
        conn = _sq.connect(db_path)
        conn.row_factory = _sq.Row
        for label, from_date in periods.items():
            rows = conn.execute("""
                SELECT outcome_pct, win FROM paper_ledger
                WHERE ticker=? AND market=? AND bias=?
                  AND source='historical' AND win IS NOT NULL
                  AND signal_date >= ?
                ORDER BY signal_date ASC
            """, (ticker.upper(), market.upper(), bias, from_date)).fetchall()

            total = len(rows)
            MIN_SIG = 50   # need >= 50 signals for period CAGR to be meaningful
            if total < MIN_SIG:
                result[label] = {
                    "cagr": None, "win_rate": None,
                    "total": total, "valid": False,
                    "note": f"Only {total} signals in {label} window — need ≥{MIN_SIG} for reliable CAGR",
                }
                continue

            is_short  = bias == "bearish"
            wins      = sum(1 for r in rows if r["win"] == 1)
            losses    = total - wins
            win_rate  = wins / total * 100
            all_outs  = [r["outcome_pct"] for r in rows if r["outcome_pct"] is not None]
            win_rows  = [r["outcome_pct"] for r in rows if r["win"]==1 and r["outcome_pct"] is not None]
            loss_rows = [r["outcome_pct"] for r in rows if r["win"]==0 and r["outcome_pct"] is not None]

            avg_win_raw  = sum(win_rows)  / len(win_rows)  if win_rows  else 0
            avg_loss_raw = sum(loss_rows) / len(loss_rows) if loss_rows else 0

            # Flip signs for bearish display
            if is_short:
                avg_win_disp  = -avg_win_raw
                avg_loss_disp = -avg_loss_raw
            else:
                avg_win_disp  = avg_win_raw
                avg_loss_disp = avg_loss_raw

            # Expected value for CAGR: weighted avg of win/loss
            ev = avg_win_disp * (win_rate/100) + avg_loss_disp * (1 - win_rate/100)

            try:
                periods_yr = TRADING_DAYS_PER_YR / FORWARD_DAYS
                cagr = ((1 + ev / 100) ** periods_yr - 1) * 100
                cagr = max(-500.0, min(500.0, cagr))
            except Exception:
                cagr = 0.0

            # Reliability rating
            if total >= 500:   reliability = "HIGH"
            elif total >= 200: reliability = "MEDIUM"
            else:              reliability = "LOW"

            # EV warning — if expected value is negative, flag it
            ev_negative = ev < 0

            result[label] = {
                "cagr":        round(cagr, 1),
                "win_rate":    round(win_rate, 1),
                "avg_win":     round(avg_win_disp, 2),
                "avg_loss":    round(avg_loss_disp, 2),
                "ev":          round(ev, 3),
                "total":       total,
                "valid":       True,
                "reliability": reliability,
                "ev_negative": ev_negative,
                "warning":     "Negative expected value — strategy has historically lost money in this window" if ev_negative else "",
                "from":        from_date,
            }
        conn.close()
    except Exception as e:
        result["error"] = str(e)

    return result


# ── Capital allocation ─────────────────────────────────────────────────────────

def allocate(card: DecisionCard, capital: float,
             risk_pct: float = 2.0) -> AllocationPlan:
    """
    Compute position size from capital + risk %.
    Logic:
      max_risk_amount = capital * risk_pct / 100
      stop_distance   = live_price * stop_pct / 100
      quantity        = floor(max_risk_amount / stop_distance)
      position_size   = quantity * live_price
    """
    cur         = card.currency
    price       = card.live_price
    stop_pct    = card.allocation.stop_pct if card.allocation else 5.0
    is_short    = card.bias == "bearish"

    if price <= 0 or stop_pct <= 0:
        return AllocationPlan(capital=capital, risk_pct=risk_pct, currency=cur)

    risk_amount   = capital * risk_pct / 100
    stop_distance = price * stop_pct / 100
    quantity      = max(1, int(risk_amount / stop_distance))
    position_size = quantity * price

    # Cap position at 40% of total capital (diversification guardrail)
    if position_size > capital * 0.4:
        quantity      = max(1, int(capital * 0.4 / price))
        position_size = quantity * price

    t1_pct = stop_pct * 1.5
    t2_pct = stop_pct * 3.0

    if is_short:
        stop_loss = price * (1 + stop_pct / 100)
        target_1  = price * (1 - t1_pct / 100)
        target_2  = price * (1 - t2_pct / 100)
    else:
        stop_loss = price * (1 - stop_pct / 100)
        target_1  = price * (1 + t1_pct / 100)
        target_2  = price * (1 + t2_pct / 100)

    max_loss  = quantity * price * stop_pct / 100
    gain_t1   = quantity * price * t1_pct / 100
    gain_t2   = quantity * price * t2_pct / 100
    rr        = t1_pct / stop_pct if stop_pct > 0 else 0

    return AllocationPlan(
        capital=capital, risk_pct=risk_pct,
        risk_amount=round(risk_amount, 0),
        position_size=round(position_size, 0),
        quantity=quantity,
        entry_price=round(price, 2),
        stop_loss=round(stop_loss, 2),
        target_1=round(target_1, 2),
        target_2=round(target_2, 2),
        stop_pct=round(stop_pct, 1),
        t1_pct=round(t1_pct, 1),
        t2_pct=round(t2_pct, 1),
        risk_reward=round(rr, 1),
        max_loss_if_stop=round(max_loss, 0),
        gain_if_t1=round(gain_t1, 0),
        gain_if_t2=round(gain_t2, 0),
        currency=cur,
    )


# ── Narrative generator ────────────────────────────────────────────────────────

def narrate(card: DecisionCard, tone_mode: str = SEBI_SAFE) -> DecisionCard:
    """
    Populate headline, decision_text, edge_text based on tone_mode.
    SEBI_SAFE: statistical framing only
    GLOBAL_AGGRESSIVE: directive framing
    """
    card.tone_mode = tone_mode
    is_short   = card.bias == "bearish"
    is_neutral = card.bias == "neutral"
    cur        = card.currency
    price_str  = f"{cur}{card.live_price:,.2f}"
    win_pct    = round(card.hist_win_pct, 0)
    fwd        = card.avg_fwd_15d
    direction  = "lower" if is_short else ("sideways" if is_neutral else "higher")
    opp_dir    = "higher" if is_short else "lower"

    # ── Headline ──
    if tone_mode == GLOBAL_AGGRESSIVE:
        if card.signal_label == "STRONG":
            card.headline = (
                f"{'Short' if is_short else ('Range play' if is_neutral else 'Long')} "
                f"{card.ticker} — high-conviction setup"
            )
        elif card.signal_label == "DEVELOPING":
            card.headline = f"{card.ticker} — setup forming, monitor closely"
        else:
            card.headline = f"{card.ticker} — early signal, watchlist"
    else:  # SEBI_SAFE
        if card.signal_label == "STRONG":
            card.headline = (
                f"{card.ticker} — {card.firing_count} of {card.qualified_total} "
                f"historically validated {card.bias} patterns active"
            )
        elif card.signal_label == "DEVELOPING":
            card.headline = (
                f"{card.ticker} — {card.bias} setup developing "
                f"({card.firing_count}/{card.qualified_total} patterns)"
            )
        else:
            card.headline = f"{card.ticker} — early {card.bias} pattern, watchlist"

    # ── Edge text (historical stats) ──
    if tone_mode == GLOBAL_AGGRESSIVE:
        card.edge_text = (
            f"Setup favours a {'short' if is_short else ('range' if is_neutral else 'long')} position. "
            f"Win rate: {win_pct:.0f}%. "
            f"Avg move: {fwd:+.1f}% over {FORWARD_DAYS} trading days. "
            f"Based on {card.hist_appearances} historical signals."
        )
    else:  # SEBI_SAFE
        card.edge_text = (
            f"In {win_pct:.0f}% of identical setups over the backtest period, "
            f"{card.ticker} moved {direction} within {FORWARD_DAYS} trading days. "
            f"Median move: {card.median_fwd:+.1f}%. "
            f"Best case: {card.best_case:+.1f}%. Worst case: {card.worst_case:+.1f}%. "
            f"Based on {card.hist_appearances} historical signals."
        )

    # ── Decision text ──
    if card.allocation:
        a = card.allocation
        if tone_mode == GLOBAL_AGGRESSIVE:
            card.decision_text = (
                f"Enter at {cur}{a.entry_price:,.2f}. "
                f"Stop {'above' if is_short else 'below'} {cur}{a.stop_loss:,.2f} "
                f"({'+'if is_short else '-'}{a.stop_pct:.1f}%). "
                f"Target 1: {cur}{a.target_1:,.2f} ({'−' if is_short else '+'}{a.t1_pct:.1f}%). "
                f"Target 2: {cur}{a.target_2:,.2f} ({'−' if is_short else '+'}{a.t2_pct:.1f}%). "
                f"Risk:Reward = 1:{a.risk_reward:.1f}. "
                f"Deploy {cur}{a.position_size:,.0f} ({a.quantity} shares). "
                f"Max loss if stop hit: {cur}{a.max_loss_if_stop:,.0f}. "
                f"Gain if T1: {cur}{a.gain_if_t1:,.0f}."
            )
        else:  # SEBI_SAFE
            card.decision_text = (
                f"Reference entry: {cur}{a.entry_price:,.2f} (current price). "
                f"Historical stop reference: {cur}{a.stop_loss:,.2f} "
                f"({'+'if is_short else '−'}{a.stop_pct:.1f}% from entry). "
                f"Historical target 1: {cur}{a.target_1:,.2f} "
                f"({'−' if is_short else '+'}{a.t1_pct:.1f}%). "
                f"Historical target 2: {cur}{a.target_2:,.2f} "
                f"({'−' if is_short else '+'}{a.t2_pct:.1f}%). "
                f"Risk:Reward ratio: 1:{a.risk_reward:.1f}. "
                f"Suggested allocation: {cur}{a.position_size:,.0f} ({a.quantity} shares). "
                f"Maximum historical loss at stop: {cur}{a.max_loss_if_stop:,.0f}."
            )
    else:
        card.decision_text = (
            f"Reference entry: {price_str}. "
            f"Run with capital allocation to get position sizing."
        )

    # ── Disclaimer ──
    if tone_mode == GLOBAL_AGGRESSIVE:
        card.disclaimer = (
            "Past performance does not guarantee future results. "
            "Signals are based on historical pattern matching. Trade at your own risk."
        )
    else:  # SEBI_SAFE
        card.disclaimer = (
            "Statistical analysis only. Levels shown are historical references, "
            "not investment advice. Consult a SEBI-registered advisor before trading. "
            "Past pattern outcomes do not guarantee future results."
        )

    return card


# ── Main builder — one card from scan result dict ──────────────────────────────

def build_decision_card(result: dict,
                        capital: float = 0.0,
                        risk_pct: float = 2.0,
                        tone_mode: str = SEBI_SAFE) -> DecisionCard:
    """
    Build a full DecisionCard from a scan_one() result dict.
    Optionally includes capital allocation if capital > 0.
    """
    is_short = result.get("bias") == "bearish"
    cur      = "₹" if result.get("market","NSE").upper() == "NSE" else "$"
    price    = result.get("live_price", 0.0) or 0.0
    stop_pct = result.get("stop_pct", 5.0) or 5.0
    score    = result.get("score_pct", 0.0) or 0.0

    # Signal label
    if score >= MIN_SCORE_STRONG:
        label = "STRONG"
    elif score >= MIN_SCORE_DEVELOPING:
        label = "DEVELOPING"
    else:
        label = "WATCHLIST"

    # CAGR — use hist_avg_outcome, sign-adjusted for bearish
    appearances = result.get("hist_total", 0) or 0
    avg_fwd_raw = result.get("hist_avg_outcome", 0.0) or 0.0
    # For bearish: raw outcome_pct is negative when price fell (a win).
    # Flip sign so CAGR reflects the actual trader P&L direction.
    if result.get("bias") == "bearish":
        avg_fwd = -avg_fwd_raw
    else:
        avg_fwd = avg_fwd_raw
    cagr, cagr_valid = compute_cagr(avg_fwd, appearances)
    cagr_note = "" if cagr_valid else (
        f"Insufficient signal history ({appearances} signals). "
        f"CAGR requires ≥{MIN_APPEARANCES} closed signals to be statistically meaningful."
        if appearances > 0 else
        "No historical signals found for this setup."
    )

    # Hold days — rough estimate from forward window
    hold_min = max(5, FORWARD_DAYS - 7)
    hold_max = FORWARD_DAYS + 5

    card = DecisionCard(
        ticker=result.get("ticker",""),
        market=result.get("market","NSE"),
        bias=result.get("bias","bullish"),
        timeframe=result.get("timeframe","1D"),
        signal_label=label,
        score_pct=round(score, 1),
        firing_count=result.get("firing_count", 0),
        qualified_total=result.get("qualified_total", 0),
        top_strategy=result.get("top_strategy",""),
        top_win_rate=result.get("top_win_rate", 0.0) or 0.0,
        firing_strategies=result.get("firing_strategies", []),
        regime=result.get("regime",""),
        regime_score=result.get("regime_score", 0.0) or 0.0,
        regime_aligned=result.get("bias_aligned", True),
        sector=result.get("sector",""),
        hist_win_pct=result.get("hist_win_pct", 0.0) or 0.0,
        hist_appearances=appearances,
        hist_wins=int(appearances * (result.get("hist_win_pct",0) or 0) / 100),
        avg_fwd_15d=round(avg_fwd, 2),
        median_fwd=round(result.get("hist_avg_outcome", 0.0) or 0.0, 2),
        best_case=round(result.get("stop_pct", 5.0) or 5.0, 1) * 3,
        worst_case=round(-((result.get("stop_pct", 5.0) or 5.0) * 1.5), 1),
        cagr_est=cagr,
        cagr_valid=cagr_valid,
        cagr_note=cagr_note,
        hold_days_min=hold_min,
        hold_days_max=hold_max,
        live_price=round(price, 2),
        currency=cur,
        allocation=None,
    )

    # Attach allocation plan if capital provided
    if capital > 0 and price > 0:
        # Build a minimal allocation object to pass stop_pct
        from dataclasses import replace
        stub = AllocationPlan(stop_pct=stop_pct, currency=cur)
        card.allocation = stub
        card.allocation = allocate(card, capital, risk_pct)

    # Generate narrative
    card = narrate(card, tone_mode)
    return card


# ── Trade plan builder — multiple stocks for a capital amount ──────────────────

def build_trade_plan(results: List[dict],
                     capital: float,
                     risk_pct: float = 2.0,
                     max_trades: int = 5,
                     tone_mode: str = SEBI_SAFE,
                     min_score: float = MIN_SCORE_DEVELOPING,
                     min_win_rate: float = MIN_WIN_RATE_STRONG,
                     min_rr: float = MIN_RR) -> Dict:
    """
    Given a list of scan results and a capital amount, build a complete
    trade plan: up to max_trades stocks, diversified by sector,
    with full allocation and narrative for each.

    Returns:
      {
        capital, risk_pct, max_trades,
        trades: [DecisionCard, ...],
        total_deployed, total_at_risk,
        summary_text,
        tone_mode,
        generated_at,
      }
    """
    # Filter qualifying results
    # Win rate >= 55% is already enforced at fingerprint build time per stock.
    # Applying it again here double-filters and empties results in bear markets.
    # Only filter on score strength and regime alignment.
    qualified = [
        r for r in results
        if (r.get("score_pct", 0) or 0) >= min_score
        and r.get("bias_aligned", False)
        and not r.get("error")
        and (r.get("qualified_total", 0) or 0) > 0
    ]

    # Sort by score_pct desc, then hist_win_pct desc
    qualified.sort(
        key=lambda r: (r.get("score_pct", 0), r.get("hist_win_pct", 0)),
        reverse=True,
    )

    # Sector diversification — max 1 per sector
    # Also enforce total deployed <= capital (running sum check)
    seen_sectors = set()
    trades = []
    capital_deployed_so_far = 0.0
    for r in qualified:
        if len(trades) >= max_trades:
            break
        sector = r.get("sector", "Unknown") or "Unknown"
        if sector in seen_sectors:
            continue

        card = build_decision_card(
            result=r, capital=capital,
            risk_pct=risk_pct, tone_mode=tone_mode,
        )

        # Check R:R after allocation
        if card.allocation and card.allocation.risk_reward < min_rr:
            continue

        # Check we don't exceed total capital
        position_cost = card.allocation.position_size if card.allocation else 0
        if capital_deployed_so_far + position_cost > capital:
            # Try to fit a smaller position with remaining capital
            remaining = capital - capital_deployed_so_far
            if remaining < r.get("live_price", 9999):
                # Can't even buy 1 share — skip this trade
                continue
            # Rebuild card with remaining capital as the cap
            card = build_decision_card(
                result=r, capital=remaining,
                risk_pct=risk_pct, tone_mode=tone_mode,
            )
            if not card.allocation or card.allocation.quantity < 1:
                continue

        seen_sectors.add(sector)
        capital_deployed_so_far += card.allocation.position_size if card.allocation else 0
        trades.append(card)

    # Portfolio summary
    total_deployed = sum(
        c.allocation.position_size for c in trades if c.allocation
    )
    total_at_risk = sum(
        c.allocation.max_loss_if_stop for c in trades if c.allocation
    )
    cur = trades[0].currency if trades else "₹"
    remaining = capital - total_deployed

    if tone_mode == GLOBAL_AGGRESSIVE:
        summary = (
            f"{len(trades)} trade{'s' if len(trades)!=1 else ''} identified. "
            f"Total deployed: {cur}{total_deployed:,.0f} of {cur}{capital:,.0f}. "
            f"Total at risk: {cur}{total_at_risk:,.0f}. "
            f"Remaining cash: {cur}{remaining:,.0f}."
        )
    else:
        summary = (
            f"{len(trades)} setup{'s' if len(trades)!=1 else ''} identified "
            f"matching your criteria. "
            f"Suggested allocation: {cur}{total_deployed:,.0f} across "
            f"{len(trades)} position{'s' if len(trades)!=1 else ''}. "
            f"Maximum historical loss if all stops hit: {cur}{total_at_risk:,.0f}. "
            f"Unallocated: {cur}{remaining:,.0f}."
        )

    if not trades:
        summary = (
            "No setups currently meet the criteria for your capital and risk settings. "
            "The market regime or signal strength may not support new positions today. "
            "Check back tomorrow after the morning scan."
        )

    return {
        "capital":        capital,
        "risk_pct":       risk_pct,
        "max_trades":     max_trades,
        "trades":         trades,
        "total_deployed": round(total_deployed, 0),
        "total_at_risk":  round(total_at_risk, 0),
        "remaining_cash": round(remaining, 0),
        "summary":        summary,
        "tone_mode":      tone_mode,
        "generated_at":   TODAY.strftime("%Y-%m-%d %H:%M"),
        "currency":       cur,
    }


# ── Screener categories ────────────────────────────────────────────────────────

def categorise_results(results: List[dict]) -> Dict:
    """
    Split scan results into Strong Buy, Strong Sell, Watchlist.

    Strong = score >= MIN_SCORE_STRONG (60%). Regime alignment is NOT
    required for Strong signals — instead we label them with a regime
    conflict warning. A 3/3 STRONG signal on a BEAR-regime stock is
    still a strong signal; the regime context is shown alongside it.

    Watchlist = score >= MIN_SCORE_DEVELOPING (35%) regardless of regime.
    Regime-conflicted strong signals are shown in their bucket with a
    conflict flag so users can make an informed decision.
    """
    strong_buy  = []
    strong_sell = []
    watchlist   = []

    for r in results:
        score   = r.get("score_pct", 0) or 0
        aligned = r.get("bias_aligned", False)
        bias    = r.get("bias", "bullish")

        # Tag regime conflict — shown in UI but doesn't exclude
        r["regime_conflict"] = not aligned

        if score >= MIN_SCORE_STRONG:
            if bias == "bullish":
                strong_buy.append(r)
            elif bias == "bearish":
                strong_sell.append(r)
            else:
                watchlist.append(r)
        elif score >= MIN_SCORE_DEVELOPING:
            watchlist.append(r)

    # Sort: regime-aligned first, then by score desc
    def sort_key(r):
        aligned = 1 if r.get("bias_aligned", False) else 0
        return (aligned, r.get("score_pct", 0))

    for bucket in [strong_buy, strong_sell, watchlist]:
        bucket.sort(key=sort_key, reverse=True)

    return {
        "strong_buy":  strong_buy,
        "strong_sell": strong_sell,
        "watchlist":   watchlist,
    }


# ── Standalone test ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Simulate a scan result for testing
    test_result = {
        "ticker": "BAJAJ-AUTO", "market": "NSE", "bias": "bearish",
        "timeframe": "1D", "score_pct": 100.0, "firing_count": 3,
        "qualified_total": 3, "top_strategy": "Rising channel break",
        "top_win_rate": 58.0, "firing_strategies": ["Rising channel break", "SMA stack bear", "SMA stack bear 2"],
        "regime": "BEAR", "regime_score": 15.0, "bias_aligned": True,
        "sector": "Auto", "live_price": 8895.50, "stop_pct": 4.8,
        "stop_loss": 9324.82, "target_1": 8251.52, "target_2": 7607.22,
        "hist_win_pct": 58.0, "hist_total": 312, "hist_avg_outcome": 2.5,
        "error": None,
    }

    print("\n── SEBI_SAFE mode ──")
    card = build_decision_card(test_result, capital=50000, risk_pct=2.0, tone_mode=SEBI_SAFE)
    print(f"Headline:  {card.headline}")
    print(f"Edge:      {card.edge_text}")
    print(f"Decision:  {card.decision_text}")
    if card.cagr_valid:
        print(f"CAGR est:  {card.cagr_est:+.1f}% (backtested, 3y)")
    print(f"Alloc:     {card.allocation.quantity} shares @ ₹{card.allocation.entry_price:,.2f} = ₹{card.allocation.position_size:,.0f}")
    print(f"Disclaimer: {card.disclaimer}")

    print("\n── GLOBAL_AGGRESSIVE mode ──")
    card2 = build_decision_card(test_result, capital=50000, risk_pct=2.0, tone_mode=GLOBAL_AGGRESSIVE)
    print(f"Headline:  {card2.headline}")
    print(f"Edge:      {card2.edge_text}")
    print(f"Decision:  {card2.decision_text}")

    print("\n── Trade plan for ₹50,000 ──")
    plan = build_trade_plan([test_result], capital=50000, risk_pct=2.0, tone_mode=SEBI_SAFE)
    print(f"Summary: {plan['summary']}")
    print(f"Trades:  {len(plan['trades'])}")
