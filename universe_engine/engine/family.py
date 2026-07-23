"""
Strategy family classifier — groups the 107 long strategies into semantic families
for the report leaderboard.
"""
from __future__ import annotations
from typing import Dict


# Family map by strategy name. Add new strategies here.
FAMILY_MAP: Dict[str, str] = {
    # Candlestick reversals
    "Bull engulfing": "candlestick_reversal",
    "Morning star": "candlestick_reversal",
    "Hammer": "candlestick_reversal",
    "Bullish harami": "candlestick_reversal",
    "Bullish marubozu": "candlestick_reversal",
    "Doji to bull": "candlestick_reversal",
    "Spring pattern": "candlestick_reversal",
    "Wick rejection low": "candlestick_reversal",
    "Three white soldiers": "candlestick_reversal",
    "Morning doji star": "candlestick_reversal",
    "Trend reversal bull": "candlestick_reversal",
    "Three-bar reversal bull": "candlestick_reversal",
    "Exhaustion bottom": "candlestick_reversal",
    "Reversal vol spike bull": "candlestick_reversal",
    "High vol reversal bull": "candlestick_reversal",
    "Small body big bull": "candlestick_reversal",

    # Breakouts
    "Range breakout": "breakout",
    "Flag & pole": "breakout",
    "Cup & handle": "breakout",
    "VCP pattern": "breakout",
    "Inside bar breakout": "breakout",
    "Ascending triangle": "breakout",
    "Pivot breakout": "breakout",
    "Week high breakout": "breakout",
    "Accumulation breakout": "breakout",
    "Multi-bar base break": "breakout",
    "52W high breakout": "breakout",
    "52W high momentum": "breakout",
    "New month high": "breakout",
    "Vol confirmed break": "breakout",
    "Vol dry-up breakout": "breakout",
    "Thrust consolidation": "breakout",
    "Thrust bar bull": "breakout",
    "Bull outside day": "breakout",
    "Range expansion bull": "breakout",
    "Breakaway gap": "breakout",
    "Bull continuation gap": "breakout",
    "Gap and go bull": "breakout",
    "Open above prev high": "breakout",
    "Above prev mid bull": "breakout",
    "Reclaim prev close": "breakout",
    "Higher low retest": "breakout",
    "Retest break level": "breakout",
    "Close above OR": "breakout",
    "Bear gap fill bull": "breakout",
    "Gap fill continue bull": "breakout",
    "High tight flag": "breakout",
    "Strong open close bull": "breakout",
    "Close vs prev range": "breakout",
    "Wide range bar bull": "breakout",
    "First bar continuation": "breakout",
    "Strong open bar": "breakout",

    # MA / trend
    "Golden cross": "ma_trend",
    "EMA breakout": "ma_trend",
    "SMA50 support hold": "ma_trend",
    "SMA200 reclaim": "ma_trend",
    "EMA ribbon bull": "ma_trend",
    "EMA50 pullback": "ma_trend",
    "SMA stack bull": "ma_trend",
    "Pullback to SMA20": "ma_trend",
    "SMA20 pullback": "ma_trend",
    "EMA slope positive": "ma_trend",
    "SMA20 slope bull": "ma_trend",
    "Rising SMA20 above": "ma_trend",
    "MACD cross bull": "ma_trend",
    "Above VWAP anchor": "ma_trend",
    "VWAP bounce": "ma_trend",
    "Anchor VWAP above": "ma_trend",
    "Support cluster bull": "ma_trend",
    "Trend line bounce": "ma_trend",
    "Demand zone bounce": "ma_trend",
    "Fibonacci R2→R3": "ma_trend",
    "Higher highs / lows": "ma_trend",
    "Rising lows": "ma_trend",
    "Swing low higher": "ma_trend",
    "Higher open/close": "ma_trend",
    "Higher low after retest": "ma_trend",
    "Sector RS bull": "ma_trend",
    "RSI oversold reversal": "ma_trend",

    # Volume confirmation
    "Volume surge": "volume",
    "Bull power candle": "volume",
    "Volume trend bull": "volume",
    "Up-day vol increase": "volume",
    "Vol price confirm bull": "volume",
    "Vol spike pullback buy": "volume",
    "Decreasing sell pressure": "volume",
    "Sell pressure decrease": "volume",
    "Narrow spread accum": "volume",
    "Low vol tight range": "volume",
    "Low-vol tight range": "volume",

    # Momentum / continuation
    "Consecutive bull bars": "momentum",
    "Momentum persistence": "momentum",
    "Trend day bull": "momentum",
    "Tightening closes": "momentum",
    "PA momentum bull": "momentum",
    "PA acceleration bull": "momentum",
    "High price in trend": "momentum",
    "Power hour close": "momentum",
    "Body to range bull": "momentum",
    "Strong close midpoint cross": "momentum",
    "Midpoint cross bull": "momentum",
    "Close near high": "momentum",
    "Upper quartile series": "momentum",
    "Wicks shortening bull": "momentum",
    "Wicks shorter bull": "momentum",
    "Low wick series": "momentum",
    "Low ATR coil bull": "momentum",
    "Candle expansion bull": "momentum",
    "Price compression bull": "momentum",
    "Price density above SMA": "momentum",
    "Price near VWAP bounce": "momentum",
    "Tight consolidation bull": "momentum",
    "Round number above": "momentum",
    "Open equals low": "momentum",
    "Green close red open": "momentum",
    "Expansion follow-thru": "momentum",
    "Follow through day": "momentum",
    "Double bottom": "momentum",

    # Catch-all
}


def family_for(strategy_name: str) -> str:
    return FAMILY_MAP.get(strategy_name, "other")


# ── Deflated Sharpe Ratio (López de Prado 2014) ───────────────────────────────

def deflated_sharpe_ratio(observed_sharpe: float, n_trials: int,
                          skew: float = 0.0, kurt: float = 3.0,
                          n_obs: int = 250) -> float:
    """
    Approximate Deflated Sharpe: adjusts a strategy's reported Sharpe for the
    number of strategies tested. A strategy must beat the *expected best of N*
    null-distribution Sharpe to count as real.

    Returns deflated Sharpe (probability-style score). Values > 0 = real edge.
    Values <= 0 = consistent with selection bias on N strategies.

    Reference: Bailey & López de Prado (2014), "The Deflated Sharpe Ratio:
    Correcting for Selection Bias, Backtest Overfitting, and Non-Normality".

    Simplified form for our use:
        DSR ≈ (SR - SR0) / sqrt((1 - skew*SR + (kurt-1)/4 * SR^2) / (n_obs-1))
    where SR0 = expected max Sharpe under N independent N(0,1) noise trials.
    """
    import math
    if n_trials <= 1:
        return observed_sharpe
    # Expected max of N standard normals (approximation)
    gamma = 0.5772156649  # Euler-Mascheroni
    sr0 = (1.0 - gamma) * _normal_inv(1.0 - 1.0/n_trials) \
          + gamma * _normal_inv(1.0 - 1.0/(n_trials * 2.71828))
    var = (1.0 - skew * observed_sharpe + (kurt - 1.0)/4.0 * observed_sharpe**2) / max(n_obs - 1, 1)
    if var <= 0: var = 1e-9
    dsr = (observed_sharpe - sr0) / math.sqrt(var)
    return dsr


def _normal_inv(p: float) -> float:
    """Approximate inverse CDF of standard normal."""
    import math
    if p <= 0 or p >= 1:
        return 0.0
    # Beasley-Springer-Moro algorithm (simplified)
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    p_low = 0.02425; p_high = 1 - p_low
    if p < p_low:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    if p <= p_high:
        q = p - 0.5; r = q*q
        return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / \
               (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)
    q = math.sqrt(-2 * math.log(1 - p))
    return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
            ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
