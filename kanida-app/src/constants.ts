// Shared app constants (4.3). Values repeated across screens live here once.
// This module has NO imports on purpose, so any file (including model.ts / decision.ts) can use it without circular-import risk.

// Capital: preset chip values and the default starting capital (₹).
export const CAPITAL_CHOICES:number[]=[10000,30000,50000];
export const DEFAULT_CAPITAL:number=10000;

// Chart timeframes, in display order.
export const TIMEFRAMES:string[]=['1H','4H','1D','1W'];

// Simulation study defaults. DEFAULT_STUDY_START/END mirror the study engine's own fallbacks.
export const DEFAULT_STUDY_START='2020-01-01',DEFAULT_STUDY_END='2026-07-31';
export const DEFAULT_STUDY_PATTERNS:string[]=['cup_handle'],DEFAULT_STUDY_TIMEFRAMES:string[]=['1D'],DEFAULT_STUDY_UNIVERSE='nifty50';

// Research round-trip cost assumption, in percent of notional (0.40%).
export const ROUND_TRIP_COST_PCT=0.4;

// Evidence: default minimum historical trades for browsing filters.
export const MIN_TRADES_DEFAULT=10;
// Stored market data older than this many calendar days cannot seed a trade plan.
export const DATA_STALE_DAYS=3;
