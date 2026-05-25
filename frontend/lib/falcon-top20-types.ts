/**
 * Falcon Top 20 payload contract — mirrors
 * backend/power_user/services/falcon_top20_explainer.py.
 *
 * SEPARATE from the legacy `Pick` (v1 tier-based) type in power-api.ts.
 * That older shape is still used by the replay/today preview surfaces and
 * is locked behind `_version: 1`. The 3-bucket Top 20 payload below is the
 * new institutional-grade explainability shape introduced on 2026-05-23.
 *
 * Three buckets per pick:
 *   bucket1 — WHY  : patterns that fired, plain English, lift per pattern
 *   bucket2 — EVIDENCE : historical track record on THIS specific stock
 *   bucket3 — SECTOR  : sector rank, peer firings, momentum, rotation
 *
 * If the backend ever changes a field, this file is the single source of
 * truth on the frontend side and must be updated in lockstep.
 */

export type Regime =
  | 'breakout'
  | 'momentum'
  | 'compression'
  | 'reversal'
  | 'mean_reversion'
  | 'capitulation'
  | 'other'

export type SignalType =
  | 'Breakout'
  | 'Momentum'
  | 'Reversal'
  | 'Compression'
  | 'Mean-Reversion'
  | 'Capitulation'

/** Legacy field — kept on the type for back-compat but UI no longer renders it.
 *  Replaced by `rating` ('Strong Buy' | 'Good Setup') + position_size_rs. */
export type RiskLevel = 'Low' | 'Medium' | 'High' | '—'

export type Rating = 'Strong Buy' | 'Good Setup'

export type SmallerPositionReason =
  | 'weak_history'    // today HR ≥10pp below stock's lifetime baseline
  | 'severe_dd'       // worst drawdown during a trade ≤ -30%
  | 'recent_loss'     // most recent similar setup lost money
  | 'oversold'        // ≥30% of firing patterns are oversold-bounce


/** Per-stock historical track record from the locked falcon-top-10 persona
 *  backtest. All numbers reflect the -7% stop and 7-day hold rules already
 *  applied — worst_loss_pct is the worst realised trade outcome (near -7%
 *  when the stop hit), NOT a 20-day MAE. */
export type Top20PerStockBacktest = {
  n_trades:          number
  n_wins:            number
  n_losses:          number
  /** Null when n_trades == 0 (this symbol never appeared in the historical Top 10). */
  win_rate_pct:      number | null
  avg_win_pct:       number | null
  avg_loss_pct:      number | null
  best_win_pct:      number | null
  worst_loss_pct:    number | null
  first_trade_date:  string | null
  last_trade_date:   string | null
}

export type RotationDirection = 'inflow' | 'outflow' | 'neutral'

export type Top20Pattern = {
  pattern_id:  number
  regime:      Regime
  blurb:       string
  english:     string
  lift_pp:     number
}

export type Top20LastSimilar = {
  date:             string      // YYYY-MM-DD
  return_pct:       number
  days_to_outcome:  number
}

export type Top20RegimeGroup = {
  regime:             Regime
  /** Most-common blurb in this regime group (or human regime name as fallback). */
  headline:           string
  count:              number
  sum_lift_pp:        number
  avg_lift_pp:        number
  avg_oos_hit_rate:   number | null
  /** Lift-weighted OOS hit rate across patterns in this regime. */
  weighted_hit_rate:  number | null
}

export type Top20Bucket1Why = {
  total_fires_today:        number
  pattern_count:            number
  /** Internal — Bucket 1 v4 (2026-05-24) no longer renders today HR on the card.
   *  The headline number is now the strategy-level per-trade win rate from the
   *  locked falcon-top-10 backtest (Top20Response.strategy_win_rate_pct). */
  today_weighted_hit_rate:  number | null
  /** Dominant pattern regime — used for the rating chip color, never shown as text. */
  dominant_regime:          Regime
  /** ONE plain-English paragraph: "Stock is breaking out — pushing toward
   *  X-month highs. 94 different signals from our library are firing on this
   *  stock right now, and most say the same thing: this is a clean breakout setup." */
  synthesis:                string
}

export type Top20Bucket2Evidence = {
  /** Total (pattern, fire_date) pairs found via rule replay since lookback_start. */
  n_historical_fires:    number
  /** Subset of n_historical_fires that have realised ret_20d in falcon_outcomes. */
  n_with_outcome:        number
  /** % of n_with_outcome that hit +10% within 20 trading days. */
  hit_rate_pct:          number
  /** Mean ret_20d on winning fires. NULL when no outcomes have realised. */
  avg_win_pct:           number | null
  /** Mean ret_20d on losing fires (negative). NULL when no outcomes have realised. */
  avg_loss_pct:          number | null
  /** Worst observed mae_20d (negative %). NULL when not available. */
  worst_drawdown_pct:    number | null
  /** Stock's all-time hit_10pc_20d rate, computed across falcon_outcomes. */
  lifetime_baseline_pct: number
  /** hit_rate_pct - lifetime_baseline_pct */
  kaynes_delta_pp:       number
  last_similar:          Top20LastSimilar | null
  first_fire_date:       string | null
  last_fire_date:        string | null
  /** YYYY-MM-DD lookback floor used for the replay (default '2021-01-01'). */
  lookback_start:        string
}

export type Top20Bucket3Sector = {
  sector_name:               string
  sector_rank:               number
  /** Rank 5 trading days ago. NULL when no prior data. */
  sector_rank_5d_ago:        number | null
  /** Positive = improved (rotating in); negative = worsened (rotating out). */
  rotation_positions:        number
  total_sectors:             number
  n_peers_firing:            number
  /** How many sector mates are in today's top 50 picks (includes this stock). */
  peer_count_in_top50:       number
  sector_20d_return_pct:     number
  rotation_direction:        RotationDirection
  /** Legacy field — derived from rotation_direction for back-compat. */
  rotation_sessions_of_10:   number
  /** Plain-English paragraph the UI renders verbatim. Source of truth for the
   *  "What the sector is doing" section. (F6, 2026-05-24: tailwind/neutral/
   *  headwind verdict tag removed — narrative conveys the same info in prose.) */
  narrative:                 string
}

export type Top20Action = {
  /** "Buy at tomorrow's open (~₹161.50)" */
  entry_text:         string
  /** "Position size: ₹50,000 (~310 shares)" */
  position_text:      string
  /** "Exit if it drops to ₹150 (-7% stop)" */
  stop_text:          string
  /** "We'll hold for up to 7 trading days" */
  hold_text:          string
  // Raw fields for the calculator / Excel export
  entry_price_rs:     number | null
  stop_loss_rs:       number
  stop_loss_pct:      number
  time_horizon_days:  number
  /** Back-compat — same string as entry_text */
  entry_label:        string
}

export type Top20Flags = {
  /** cap_share > 30% of firing patterns are capitulation-regime. */
  capitulation_warning: boolean
  /** cap_share > 50% AND day return < 0 AND sector_rank > 10. */
  falling_knife:        boolean
  /** Symbol NOT in top-50 of any of the prior 5 trading days. */
  fresh_entry:          boolean
  /** % of fired patterns that are capitulation-regime (0-100). */
  cap_share_pct:        number
  /** Signal-day return % from OHLC (close/prev_close - 1)*100. */
  day_return_pct:       number | null
}

export type Top20Pick = {
  rank:                       number
  symbol:                     string
  sector:                     string
  signal_type:                SignalType
  /** NEW — plain-English rating shown as a chip. Both values mean BUY. */
  rating:                     Rating
  /** ₹50,000 (standard) or ₹25,000 (smaller — when qualified). */
  position_size_rs:           number
  /** floor(position_size_rs / entry_price). 0 if entry_price missing. */
  shares_recommended:         number
  /** True when any of the smaller-position triggers fired. */
  smaller_position:           boolean
  /** Subset of {weak_history, severe_dd, recent_loss, oversold} that fired. */
  smaller_position_reasons:   SmallerPositionReason[]
  /** Plain-English heads-up rendered above the action block. */
  weaker_history_message:     string | null
  /** Per-stock historical record under the locked Top 10 rules. */
  per_stock_backtest:         Top20PerStockBacktest
  /** Legacy fields — UI no longer renders these. */
  risk_level:                 RiskLevel
  warning:                    string | null
  bucket1:                    Top20Bucket1Why
  bucket2:                    Top20Bucket2Evidence
  bucket3:                    Top20Bucket3Sector
  action:                     Top20Action
  flags:                      Top20Flags
}

export type Top20Universe = 'all500' | 'nifty50' | 'nifty100' | 'nifty200' | 'fno'

export type Top20Response = {
  signal_date:        string | null
  entry_date:         string | null
  next_trading_day:   string | null
  universe:           Top20Universe | string
  sector:             string | null
  validation_status:  'pending' | 'validated' | 'invalidated' | 'pre_market'
  validated_at:       string | null
  picks:              Top20Pick[]
  /** YYYY-MM-DD list of every signal date that has data, newest first. */
  available_signal_dates: string[]
  /** Newest available signal date (== available_signal_dates[0]). */
  latest_signal_date:     string | null
  /** True when the response is for the latest available date (default view). */
  is_latest:              boolean
  /** Strategy-level per-trade win rate (~70% — same on every card).
   *  Sourced from the locked falcon-top-10 persona backtest. */
  strategy_win_rate_pct:  number
  /** Total closed trades in the backtest that produced strategy_win_rate_pct. */
  strategy_n_trades:      number
  /** Locked hold horizon — 7 trading days for Falcon Top 10. */
  hold_days:              number
  /** Standard per-trade size from the persona (₹50,000). */
  standard_position_rs:   number
  /** Smaller per-trade size when a weaker-history trigger fired (₹25,000). */
  smaller_position_rs:    number
  _built_in_ms?:          number
}
