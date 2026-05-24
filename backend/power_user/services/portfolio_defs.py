"""Locked specs for the 5 Co-Trader personas (V3 audit-ready, 2026-05-16).

Source of truth for the public-facing track record. Numbers below come from
the operator's locked V3 simulator runs:
  persona1_V3_fixed35k_2021_2026.xlsx
  persona2_V3_fixed35k_2024_2026.xlsx
  persona3_V3_variantA2_fixed_50k_2021_2026.xlsx
  persona4_V3_locked_top14_sl10_tgt30.xlsx
  persona5_V3_locked_top15_sl5_tgt7.xlsx
  PERSONA_1_LOCKED.md (consolidated lock doc)

Methodology (verbatim, applied to every persona file):
  - Integer shares only (no fractional)
  - No leverage; cash only
  - Idle cash tracked separately
  - Gap-down stops honoured (exit at gap price if worse)
  - Fixed rupees per trade (NO compounding within year)
  - Each calendar year resets to ₹5,00,000 in the backtest
  - Walk-forward: any point in the test can only use patterns mined in the
    prior 4 years (mirrors live publish_patterns.py cadence)
  - All 12-14 QA checks PASS on every persona; P&L reconciles year-by-year
    to ₹0 gap.

DO NOT edit numbers without re-running the V3 simulator + bumping a version
marker. These are the public, audited, GTM-ready figures.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional

VIRTUAL_CAPITAL_START = 500_000.0       # ₹5 L — locked per V3 audit
START_DATE_LABEL      = "2026-01-01"    # internal live-book start; surfaced as "Backtested through 2026-05-14"


# ──────────────────────────────────────────────────────────────────────────
# Param + def structures
# ──────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PortfolioParams:
    fixed_per_trade_rs:  int               # ₹ per trade at the ₹5 L base — locked
    top_n:               int               # top-N picks by engine score
    entry_cadence:       str               # 'daily' | 'tuesday_only' | 'first_of_month'
    entry_time_label:    str               # human description for UI
    hold_days_max:       int               # max trading sessions held
    sl_pct:              float             # initial stop loss (negative)
    target_pct:          Optional[float]   # fixed target (None → trail-only)
    trail_trigger_pct:   Optional[float]   # arm trail at +N% (None → no trail)
    trail_method:        Optional[str]     # 'donchian_10d' | None
    intraday_filter:     Optional[str]     # 'wait_15min_volume_check' (P2 only)
    skip_if_held:        bool              # don't re-enter symbols already open

@dataclass(frozen=True)
class PortfolioDef:
    slug:                 str
    name:                 str
    tagline:              str                       # one-line quote
    persona_number:       int                       # P1..P5
    risk_tier:            str                       # 'Medium' | 'Low-Medium' | 'Medium-High' | 'Lowest' | 'Highest'
    risk_tier_color:      str                       # tailwind palette key
    display_order:        int
    backtest_window:      str                       # e.g. "2021-2026 (5.4 years)"

    params:               PortfolioParams
    backtest_metrics:     Dict[str, object]         # V3 audited locked numbers

    what_it_does:         str                       # 3-4 sentence trader voice
    trading_cycle:        List[str]                 # bullet list for UI
    best_fit:             str
    not_for:              str
    what_to_expect:       str

    # Machine-readable rule keys (audit + DB columns). The engine dispatches on
    # entry_cadence + intraday_filter directly — these are documentation IDs.
    entry_rule:           str = "engine_v3"
    exit_rule:            str = "engine_v3"

    disclosure:           Optional[str] = None      # prominent warning (only P5)
    execution_guidance:   str = ""                   # Fix 6: "How to execute this strategy"

    def parameters_json(self) -> str:
        return json.dumps({
            "fixed_per_trade_rs": self.params.fixed_per_trade_rs,
            "top_n":              self.params.top_n,
            "entry_cadence":      self.params.entry_cadence,
            "entry_time_label":   self.params.entry_time_label,
            "hold_days_max":      self.params.hold_days_max,
            "sl_pct":             self.params.sl_pct,
            "target_pct":         self.params.target_pct,
            "trail_trigger_pct":  self.params.trail_trigger_pct,
            "trail_method":       self.params.trail_method,
            "intraday_filter":    self.params.intraday_filter,
            "skip_if_held":       self.params.skip_if_held,
        }, separators=(",", ":"))

    def backtest_metrics_json(self) -> str:
        out = dict(self.backtest_metrics)
        out["backtest_window"] = self.backtest_window
        out["risk_tier"]       = self.risk_tier
        out["risk_tier_color"] = self.risk_tier_color
        out["persona_number"]  = self.persona_number
        return json.dumps(out, separators=(",", ":"))

    def narrative_json(self) -> str:
        return json.dumps({
            "what_it_does":       self.what_it_does,
            "trading_cycle":      self.trading_cycle,
            "best_fit":           self.best_fit,
            "not_for":            self.not_for,
            "what_to_expect":     self.what_to_expect,
            "disclosure":         self.disclosure,
            "execution_guidance": self.execution_guidance,
        }, separators=(",", ":"))


# ──────────────────────────────────────────────────────────────────────────
# P1 — THE DAILY TRADER  (Medium risk)
# ──────────────────────────────────────────────────────────────────────────

DAILY_TRADER = PortfolioDef(
    slug          = "daily-trader",
    name          = "The Daily Trader",
    tagline       = "Active trader who doesn't miss a single day's opportunity.",
    persona_number= 1,
    risk_tier     = "Medium",
    risk_tier_color = "amber",
    display_order = 1,
    backtest_window = "2021-2026 (5.4 years)",
    params = PortfolioParams(
        fixed_per_trade_rs = 35_000,
        top_n              = 14,
        entry_cadence      = "daily",
        entry_time_label   = "9:15 IST (open)",
        hold_days_max      = 7,
        sl_pct             = -7.0,
        target_pct         = None,
        trail_trigger_pct  = 12.0,
        trail_method       = "donchian_10d",
        intraday_filter    = None,
        skip_if_held       = True,
    ),
    backtest_metrics = {
        "avg_yearly_return_pct":    207.0,
        "median_yearly_return_pct": 183.0,
        "best_year_pct":            442.0,
        "best_year_label":          "2024",
        "worst_year_pct":           -23.0,
        "worst_year_label":         "2021",
        "positive_years":           "5 of 6",
        "win_rate_pct":             62.0,
        "avg_max_drawdown_pct":    -19.0,
        "trades_per_year":          700,
        "total_pnl_rs_over_window": 6_200_000,    # ₹62 lakh
        "stop_loss_pct":           -7.0,
    },
    what_it_does = (
        "Every market morning, the engine identifies its top 14 highest-conviction "
        "stocks. The portfolio enters all 14 that aren't already held. Each gets "
        "exactly ₹35,000. After 7 trading days, the position auto-exits — unless "
        "the smart trailing stop fires earlier. As old positions exit, the cash is "
        "recycled into the next day's top picks. The portfolio is always near-fully invested."
    ),
    trading_cycle = [
        "9:15 IST: enter today's top 14 picks (skip stocks already held)",
        "Through the day: the smart trailing stop protects profit once a stock rises +12%",
        "Day 7: position exits at close if neither stop loss nor trail has fired",
        "Cash from exits → re-deployed next morning into fresh top 14",
    ],
    best_fit       = "Active traders comfortable watching the market daily, can stomach a one-year drawdown.",
    not_for        = "Beginners, traders who hate frequent alerts, anyone who can't survive a −23% year.",
    what_to_expect = "~14 entries every day, near-full deployment, busiest log of all personas.",
    execution_guidance = (
        "Best with automated trade monitoring. 14 entries every morning + smart trailing stop = "
        "active management. Auto-trade or auto-watch dramatically improves execution quality. "
        "Manual execution is possible but tedious."
    ),
)


# ──────────────────────────────────────────────────────────────────────────
# P2 — THE PATIENT TRADER  (Low-Medium risk)
# ──────────────────────────────────────────────────────────────────────────

PATIENT_TRADER = PortfolioDef(
    slug          = "patient-trader",
    name          = "The Patient Trader",
    tagline       = "Same engine as Daily Trader, but waits 15 minutes for the morning to confirm.",
    persona_number= 2,
    risk_tier     = "Low-Medium",
    risk_tier_color = "green",
    display_order = 2,
    backtest_window = "2024-2026 (2.0 years — 1-min data limit)",
    params = PortfolioParams(
        fixed_per_trade_rs = 35_000,
        top_n              = 14,
        entry_cadence      = "daily",
        entry_time_label   = "9:30 IST (after 15-min confirmation)",
        hold_days_max      = 7,
        sl_pct             = -7.0,
        target_pct         = None,
        trail_trigger_pct  = 12.0,
        trail_method       = "donchian_10d",
        intraday_filter    = "wait_15min_volume_check",
        skip_if_held       = True,
    ),
    backtest_metrics = {
        "avg_yearly_return_pct":    136.0,
        "median_yearly_return_pct": 176.0,
        "best_year_pct":            179.0,
        "best_year_label":          "2024",
        "worst_year_pct":           53.0,
        "worst_year_label":         "2026 partial — no stress year in window",
        "positive_years":           "3 of 3",
        "win_rate_pct":             59.0,
        "avg_max_drawdown_pct":    -10.0,
        "trades_per_year":          700,
        "total_pnl_rs_over_window": 2_000_000,    # ₹20 lakh
        "stop_loss_pct":           -7.0,
        "backtest_caveat": (
            "P2 tested only 2024-2026 because 1-minute data didn't exist before May 2024. "
            "We do NOT know how the 15-minute filter would have behaved in 2021's regime shift."
        ),
    },
    what_it_does = (
        "Engine generates the top 100 picks at EOD. At 9:30 IST, the portfolio walks "
        "down the list checking each candidate — does it have positive 15-minute return "
        "AND morning volume running hot? Only the first 14 that pass get entered at the "
        "9:30 price. Stocks that fail are skipped (they may pass tomorrow)."
    ),
    trading_cycle = [
        "9:15 IST: market opens, NO trades placed yet",
        "9:30 IST: 15-minute confirmation rule runs on top 100 candidates",
        "Walk down the ranked list, take first 14 that pass: positive 15-min return AND first-15-min volume > 5% of yesterday's full-day volume",
        "Failed candidates: skipped today (may pass tomorrow)",
        "Rest of trade: identical to Daily Trader (7-day hold, −7% SL, +12% smart trail)",
    ],
    best_fit       = "Risk-aware traders who prefer fewer trades with morning confirmation.",
    not_for        = "Traders who want maximum exposure or maximum returns.",
    what_to_expect = "~10-14 trades per day after filtering, smoother ride than Daily, lower returns but lower drawdowns.",
    execution_guidance = (
        "Same execution profile as Daily, but you need to be at the screen at 9:30 IST for "
        "the 15-min confirmation check. Auto-trade strongly recommended — the 9:30 cutoff is precise."
    ),
)


# ──────────────────────────────────────────────────────────────────────────
# P3 — THE WEEKLY TRADER  (Medium-High risk)
# ──────────────────────────────────────────────────────────────────────────

WEEKLY_TRADER = PortfolioDef(
    slug          = "weekly-trader",
    name          = "The Weekly Trader",
    tagline       = "One day a week — Tuesday. Top 10 high-conviction picks. Hold to next Monday close.",
    persona_number= 3,
    risk_tier     = "Medium-High",
    risk_tier_color = "orange",
    display_order = 3,
    backtest_window = "2021-2026 (5.4 years)",
    params = PortfolioParams(
        fixed_per_trade_rs = 50_000,
        top_n              = 10,
        entry_cadence      = "tuesday_only",
        entry_time_label   = "Tuesday 9:15 IST",
        hold_days_max      = 5,
        sl_pct             = -7.0,
        target_pct         = 12.0,
        trail_trigger_pct  = None,
        trail_method       = None,
        intraday_filter    = None,
        skip_if_held       = True,
    ),
    backtest_metrics = {
        "avg_yearly_return_pct":    194.0,
        "median_yearly_return_pct": 206.0,
        "best_year_pct":            349.0,
        "best_year_label":          "2025",
        "worst_year_pct":           -17.0,
        "worst_year_label":         "2021",
        "positive_years":           "5 of 6",
        "win_rate_pct":             74.0,        # HIGHEST of all 5
        "avg_max_drawdown_pct":    -10.0,
        "trades_per_year":          420,
        "total_pnl_rs_over_window": 5_800_000,
        "stop_loss_pct":           -7.0,
        "target_pct":               12.0,
    },
    what_it_does = (
        "The simplest active style. Every Tuesday morning, the engine's top 10 stocks "
        "for that day are entered, each at exactly ₹50,000. Each position is held until "
        "either (a) it hits the −7% stop loss, (b) it hits the +12% target, or (c) Monday "
        "EOD close. Then Tuesday starts fresh."
    ),
    trading_cycle = [
        "Tuesday 9:15 IST: enter top 10 picks (₹50,000 each = ₹5 lakh deployed)",
        "Wed / Thu / Fri: positions held passively, exit if stop or target hits",
        "Monday EOD: any remaining positions close at Monday's close",
        "Next Tuesday 9:15: cycle repeats",
    ],
    best_fit       = "Working professionals with weekly availability, traders who prefer concentrated bets.",
    not_for        = "Traders who want daily action or many small positions.",
    what_to_expect = "10 trades every Tuesday, 100% deployed all week, highest win rate, set on Tuesday and check Friday EOD.",
    execution_guidance = (
        "Easiest to execute manually. Set 10 orders Tuesday 9:15 IST, then check positions "
        "Friday EOD. One active day a week."
    ),
)


# ──────────────────────────────────────────────────────────────────────────
# P4 — THE MONTHLY TRADER  (Lowest risk — only persona that never lost a year)
# ──────────────────────────────────────────────────────────────────────────

MONTHLY_TRADER = PortfolioDef(
    # 2026-05-17 — switched to Alt G ("two-batch monthly cadence", source script
    # sim_p4_alts_fullyear.py). Engine wiring in persona_engine_core.simulate_year
    # with run_cfg.split_15_mode=True. All numbers below reflect the verified
    # Alt G backtest (yearly P&L gap = ₹0.00 on every year, see V3 audit).
    slug          = "monthly-trader",
    name          = "The Monthly Trader",
    tagline       = "Twice a month — top 14 on day 1 and mid-month. Capital never sits idle.",
    persona_number= 4,
    risk_tier     = "Lowest",
    risk_tier_color = "green",
    display_order = 4,
    backtest_window = "2021-2026 (5.4 years)",
    params = PortfolioParams(
        fixed_per_trade_rs = 35_714,
        top_n              = 14,
        # entry_cadence stays "first_of_month" because the frontend type and
        # portfolio_engine dispatcher are constrained to a 3-value enum. The
        # human-readable entry_time_label below carries the actual cadence.
        entry_cadence      = "first_of_month",
        entry_time_label   = "1st & 16th trading day of month, 9:15 IST",
        hold_days_max      = 15,                # batch-1 cap; batch-2 forced to month-end (≤ ~8 TDs)
        sl_pct             = -10.0,
        target_pct         = 30.0,
        trail_trigger_pct  = None,
        trail_method       = None,
        intraday_filter    = None,
        skip_if_held       = True,
    ),
    backtest_metrics = {
        "avg_yearly_return_pct":    59.5,       # was 36.0
        "median_yearly_return_pct": 38.7,       # was 30.0
        "best_year_pct":           147.0,       # was 99.0 — 2023 +146.74%
        "best_year_label":          "2023",
        "worst_year_pct":           15.9,       # was 4.0 — 2021 +15.91%, now ABOVE FD
        "worst_year_label":         "2021 — still positive ✓",
        "positive_years":           "6 of 6",   # still the only persona that never lost a year
        "win_rate_pct":             59.7,       # was 56.0 (avg WR across 6 years)
        "avg_max_drawdown_pct":    -11.8,       # was -11.0 (avg of yearly MDDs: -12.86, -17.34, -7.35, -10.23, -14.81, -8.18)
        "trades_per_year":          275,        # was 120 — 1,653 / 6yr ≈ 275
        "total_pnl_rs_over_window": 1_784_000,  # was 11L; now ~17.8L (sum of yearly P&L on fresh ₹5L base each year)
        "stop_loss_pct":           -10.0,
        "target_pct":               30.0,
        "headline_callout":         "The only persona that NEVER had a losing calendar year.",
    },
    what_it_does = (
        "Twice-monthly cadence. On the 1st trading day of each calendar month, the engine's "
        "top 14 stocks are entered (batch 1) at ₹35,714 each and held up to 15 trading days. "
        "On the 16th trading day of the same month, batch 1 has cleared and a fresh top 14 "
        "is entered (batch 2), held until the month's final trading day. Cash recycles inside "
        "the month — at most ~1 trading day idle between batches. Wide stop (−10%) and a +30% "
        "fixed target apply to both batches; whichever fires first wins."
    ),
    trading_cycle = [
        "1st trading day of month, 9:15 IST: enter top 14 picks (batch 1, ₹35,714 each = ₹5 lakh)",
        "Days 1-15: batch 1 held; exit early on SL (−10%) or target (+30%)",
        "Day 15 close: any remaining batch-1 positions close",
        "16th trading day, 9:15 IST: enter top 14 again (batch 2, same sizing)",
        "Days 16 → month-end: batch 2 held; exit early on SL or target",
        "Month-end close: any remaining batch-2 positions close",
        "Next month's 1st trading day: cycle repeats",
    ],
    best_fit       = "Set-and-twice-a-month traders — two confirmation moments per month, capital always working.",
    not_for        = "Active traders who want daily action; those uncomfortable with mid-month rebalancing.",
    what_to_expect = "~14 trades on 2 days per month (day 1 + day 16). Only persona that never lost a calendar year — including 2021's regime shift.",
    execution_guidance = (
        "Easiest after the daily personas. Place 14 orders on the 1st AND 16th trading day of "
        "each month, then walk away. Manual execution is fine; set a calendar reminder for both "
        "dates."
    ),
)


# ──────────────────────────────────────────────────────────────────────────
# P5 — THE BTST TRADER  (HIGHEST risk — mandatory disclosure)
# ──────────────────────────────────────────────────────────────────────────

BTST_TRADER = PortfolioDef(
    slug          = "btst-trader",
    name          = "The BTST Trader",
    tagline       = "Highest absolute returns. Buy at 9:15. Sell at next-day close. Repeat every day.",
    persona_number= 5,
    risk_tier     = "Highest",
    risk_tier_color = "red",
    display_order = 5,
    backtest_window = "2021-2026 (5.4 years)",
    params = PortfolioParams(
        fixed_per_trade_rs = 33_333,
        top_n              = 15,
        entry_cadence      = "daily",
        entry_time_label   = "9:15 IST — bot-only (~13 trades per day)",
        hold_days_max      = 2,                   # buy today, sell tomorrow EOD
        sl_pct             = -5.0,
        target_pct         = 7.0,
        trail_trigger_pct  = None,
        trail_method       = None,
        intraday_filter    = None,
        skip_if_held       = True,
    ),
    backtest_metrics = {
        "avg_yearly_return_pct":    306.0,    # HIGHEST
        "median_yearly_return_pct": 346.0,
        "best_year_pct":            564.0,
        "best_year_label":          "2024",
        "worst_year_pct":          -54.0,     # ⚠️ HIGHEST drawdown of any persona
        "worst_year_label":         "2021 ⚠️",
        "positive_years":           "5 of 6",
        "win_rate_pct":             60.0,
        "avg_max_drawdown_pct":    -22.0,    # worst of all 5
        "trades_per_year":          3_266,    # ~13 per day
        "total_pnl_rs_over_window": 9_200_000, # highest absolute
        "stop_loss_pct":           -5.0,
        "target_pct":               7.0,
    },
    what_it_does = (
        "The fastest cycle — true Buy Today, Sell Tomorrow. Every market day, the "
        "engine's top 15 stocks are entered at 9:15 IST. Each position is held for "
        "exactly 2 sessions — bought today, sold tomorrow's EOD close (unless stop "
        "loss or target fires first). Fast capital recycling means winners compound "
        "rapidly in good regimes — but losses compound just as fast in bad regimes."
    ),
    trading_cycle = [
        "9:15 IST: enter today's top 15 picks (₹33,333 each = ₹5 lakh deployed)",
        "Through today + tomorrow: positions monitored for stop or target",
        "Tomorrow's EOD close: any remaining positions exit",
        "Next day 9:15: new top 15 enter — cycle repeats every market day",
    ],
    best_fit       = "High-octane traders comfortable with severe drawdowns, automated traders.",
    not_for        = "ANY risk-averse trader, beginners, anyone who cannot stomach a −54% year.",
    what_to_expect = "~13 trades per day, automated execution required, highest possible returns AND highest possible drawdowns.",
    disclosure     = (
        "This strategy lost 54% in 2021's regime shift. The next 4 years recovered "
        "strongly (+79%, +554%, +564%, +506%), but a user starting at the wrong time "
        "can lose more than half their capital in a single year. Bot-only — not manually "
        "executable due to ~13 trades per day."
    ),
    execution_guidance = (
        "Bot-only — not manually executable. ~13 trades per day. Requires automated "
        "entry + next-day exit logic. Recommended only after Auto Trade feature ships."
    ),
)


# ──────────────────────────────────────────────────────────────────────────
# P6 — FALCON TOP 10  (Highest-quality daily picks — full capital deployment)
# 2026-05-21 — new persona ranked by avg_lift (quality-per-fire), gated at
# min_fires=10. Same SL/trail as Daily Trader but bigger per-trade size (5L/10).
# ──────────────────────────────────────────────────────────────────────────

FALCON_TOP_10 = PortfolioDef(
    slug          = "falcon-top-10",
    name          = "Falcon Top 10",
    tagline       = "Engine's 10 highest-quality daily picks. Full ₹5L deployed, 7-day hold.",
    persona_number= 6,
    risk_tier     = "Medium",
    risk_tier_color = "amber",
    display_order = 6,
    backtest_window = "2021-2026 (5.4 years, walk-forward)",
    params = PortfolioParams(
        fixed_per_trade_rs = 50_000,
        top_n              = 10,
        entry_cadence      = "daily",
        entry_time_label   = "Every trading day, 9:15 IST",
        hold_days_max      = 7,
        sl_pct             = -7.0,
        target_pct         = None,
        trail_trigger_pct  = 12.0,
        trail_method       = "donchian_low_10d",
        intraday_filter    = None,
        skip_if_held       = True,
    ),
    backtest_metrics = {
        "avg_yearly_return_pct":    258.34,
        "median_yearly_return_pct": 217.39,
        "best_year_pct":            564.47,
        "best_year_label":          "2023",
        "worst_year_pct":             5.35,
        "worst_year_label":         "2021 — still positive ✓",
        "positive_years":           "6 of 6",
        "win_rate_pct":              69.25,
        "avg_max_drawdown_pct":      -9.96,
        "trades_per_year":          474,
        "total_pnl_rs_over_window": 7_750_107,
        "stop_loss_pct":             -7.0,
        "target_pct":                None,
        "headline_callout":         "Highest-conviction picks: ranked by avg_lift × 10-pattern gate. 6/6 positive years.",
        "backtest_caveat":          (
            "6-year window only (2021-2026); not stress-tested against 2008/2015/2020-style "
            "crashes. 2021 result built on a thin 15-trade sample (min_fires=10 cut the "
            "eligible pool in a quiet year). 100% capital deployment every day — intra-year "
            "drawdowns to -15.59% (2025) are real."
        ),
    },
    what_it_does = (
        "The engine's 10 highest-conviction daily setups, ranked by per-fire pattern quality "
        "(avg_lift = sum_lift / n_fires) and gated by a minimum 10 confluent patterns. Each "
        "pick gets ₹50,000 — the full ₹5L base is always deployed across 10 names. Same "
        "exit discipline as the Daily Trader: −7% initial stop, trail arms at +12% gain "
        "(10-day Donchian low, no +3% lock floor), 7-day max hold."
    ),
    trading_cycle = [
        "Every trading day, 9:15 IST: top 10 picks by avg_lift (₹50,000 each = ₹5 lakh)",
        "Initial stop: −7% from entry, exits at gap price if stock gaps down past stop",
        "Trail: arms after close at +12% above entry; thereafter SL = max(entry, 10-day low)",
        "Time stop: 7 trading days from entry",
        "Skip already-held symbols; no leverage; integer shares only",
    ],
    best_fit       = "Confidence-first traders: fewer, higher-quality setups over breadth.",
    not_for        = "Operators who prefer broader diversification or lower per-trade size.",
    what_to_expect = "~474 trades/year on average. 6/6 positive years. Worst single year +5.35% (thin sample); best +564%. Avg max DD −9.96%, worst single-year DD −15.59%.",
    execution_guidance = (
        "Daily execution. Place 10 orders at 9:15 IST. Auto-trade integration follows the "
        "same flow as Daily Trader — operator-staged at 16:10 IST, deployed at 09:15 IST."
    ),
)


ALL_PORTFOLIOS: List[PortfolioDef] = [
    DAILY_TRADER,
    PATIENT_TRADER,
    WEEKLY_TRADER,
    MONTHLY_TRADER,
    BTST_TRADER,
    FALCON_TOP_10,
]

# Slugs that existed in the DB before this lock — we DELETE these on reseed so
# orphaned data doesn't linger. Listed here as documentation.
OBSOLETE_SLUGS: List[str] = ["champion", "confirmed", "expiry-trader", "concentrated", "smooth"]


def by_slug(slug: str) -> Optional[PortfolioDef]:
    for p in ALL_PORTFOLIOS:
        if p.slug == slug:
            return p
    return None


# ──────────────────────────────────────────────────────────────────────────
# Methodology footnote — VERBATIM on every persona detail page (operator
# request). Centralised here so it can't drift.
# ──────────────────────────────────────────────────────────────────────────

METHODOLOGY_FOOTNOTE = (
    "How these numbers were calculated. Every persona was tested using our V3 "
    "audit-ready Indian cash-equity simulator. Rules: integer shares only (no "
    "fractional), no leverage, idle cash tracked separately, gap-down stops "
    "honoured (exit at gap price if worse than stop). Capital model: fixed rupees "
    "per trade (no compounding within year). Each calendar year resets to "
    "₹5,00,000 — so returns shown are what a fresh ₹5 L account would have made "
    "each year independently, not a compounding multi-year track record. "
    "Walk-forward: at any point during the backtest, the engine could only use "
    "patterns mined in the prior 4 years (mirrors how live production refreshes "
    "weekly via publish_patterns.py). Pattern source: same production database "
    "the live engine uses. All P&L reconciles year-by-year to ₹0 gap. All 12-14 "
    "QA checks PASS on every persona file."
)
