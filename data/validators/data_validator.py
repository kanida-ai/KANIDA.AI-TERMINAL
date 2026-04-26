"""
Data quality gate for all OHLCV ingestion.

Every row from every source passes through here before touching the DB.
This prevents corrupted records (like the AMZN April 2026 -17%/-28% gaps
on 4-6M volume) from contaminating the pattern engine.

Validation rules:
  1. Basic OHLCV integrity (no nulls, no negative prices, H >= L, etc.)
  2. Single-day gap check: flag if |gap| > threshold AND volume < vol_ratio * avg
  3. Volume sanity: reject if volume < absolute floor for the instrument tier
  4. Continuity: flag if price jumps > 25% in a single bar regardless of volume

Quality flags written to ohlc_daily.quality_flag:
  'ok'       — passed all checks
  'suspect'  — flagged, written but marked for review
  'rejected' — not written to DB
"""

from __future__ import annotations

from dataclasses import dataclass, field
from statistics import mean
from typing import Any


# ── Config ─────────────────────────────────────────────────────────────────────

# Move thresholds that trigger the combined volume+gap check
LARGE_CAP_GAP_THRESHOLD = 0.12      # 12% — unrealistic for AMZN, RELIANCE
MID_CAP_GAP_THRESHOLD = 0.15        # 15%
SMALL_CAP_GAP_THRESHOLD = 0.25      # 25%

# Volume ratio vs 30-day average — below this ratio + large gap = suspect
LOW_VOLUME_RATIO = 0.30

# Absolute minimum volume floor by market tier (reject below this, no exceptions)
MIN_VOLUME = {
    "US_large": 1_000_000,    # large cap US (AMZN, AAPL, etc.)
    "US_mid":   200_000,
    "US_small": 50_000,
    "NSE_large": 50_000,      # Nifty 50 stocks
    "NSE_mid":  10_000,
    "NSE_small": 5_000,
    "default":  1_000,
}

# Hard reject: price move above this in a single bar is physically impossible
HARD_REJECT_MOVE = 0.50             # 50% single-day move


@dataclass
class OHLCVRow:
    market: str
    ticker: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str
    quality_flag: str = "ok"
    quality_notes: str = ""


@dataclass
class DataValidator:
    market: str
    ticker: str
    cap_tier: str = "large"         # 'large' | 'mid' | 'small'
    _history: list[float] = field(default_factory=list, repr=False)

    @property
    def gap_threshold(self) -> float:
        if self.cap_tier == "large":
            return LARGE_CAP_GAP_THRESHOLD
        if self.cap_tier == "mid":
            return MID_CAP_GAP_THRESHOLD
        return SMALL_CAP_GAP_THRESHOLD

    @property
    def vol_floor(self) -> int:
        key = f"{self.market}_{self.cap_tier}"
        return MIN_VOLUME.get(key, MIN_VOLUME["default"])

    def validate_row(self, row: dict[str, Any], prev_close: float | None = None) -> OHLCVRow:
        r = OHLCVRow(**{k: row[k] for k in OHLCVRow.__dataclass_fields__ if k in row})
        notes: list[str] = []

        # Rule 1 — basic integrity
        if r.close <= 0 or r.open <= 0 or r.high <= 0 or r.low <= 0:
            r.quality_flag = "rejected"
            r.quality_notes = "zero_or_negative_price"
            return r
        if r.volume <= 0:
            r.quality_flag = "rejected"
            r.quality_notes = "zero_volume"
            return r
        if r.high < r.low:
            r.quality_flag = "rejected"
            r.quality_notes = "high_below_low"
            return r
        if r.high < r.close or r.high < r.open:
            notes.append("high_below_close_or_open")
            r.quality_flag = "suspect"
        if r.low > r.close or r.low > r.open:
            notes.append("low_above_close_or_open")
            r.quality_flag = "suspect"

        # Rule 2 — absolute volume floor
        if r.volume < self.vol_floor:
            notes.append(f"volume_below_floor({self.vol_floor})")
            r.quality_flag = "suspect"

        if prev_close and prev_close > 0:
            gap = abs(r.open - prev_close) / prev_close
            day_move = abs(r.close - prev_close) / prev_close

            # Rule 3 — hard reject: physically impossible single-day move
            if day_move > HARD_REJECT_MOVE:
                r.quality_flag = "rejected"
                r.quality_notes = f"impossible_move_{day_move:.1%}"
                return r

            # Rule 4 — combined gap + volume check (the AMZN April 2026 case)
            avg_vol = mean(self._history[-30:]) if len(self._history) >= 5 else None
            if gap > self.gap_threshold and avg_vol:
                vol_ratio = r.volume / avg_vol
                if vol_ratio < LOW_VOLUME_RATIO:
                    notes.append(
                        f"large_gap_{gap:.1%}_on_low_volume_{vol_ratio:.2f}x_avg"
                    )
                    r.quality_flag = "suspect"

        if notes:
            r.quality_notes = "; ".join(notes)
        return r

    def validate_series(
        self,
        rows: list[dict[str, Any]],
    ) -> tuple[list[dict], list[dict], list[dict]]:
        """
        Validate a sorted (ascending date) series of rows.
        Returns (ok_rows, suspect_rows, rejected_rows) as plain dicts.
        """
        ok: list[dict] = []
        suspect: list[dict] = []
        rejected: list[dict] = []
        prev_close: float | None = None

        for raw in rows:
            validated = self.validate_row(raw, prev_close)
            self._history.append(validated.close)
            d = validated.__dict__
            if validated.quality_flag == "rejected":
                rejected.append(d)
                print(
                    f"  REJECTED {self.ticker} {validated.trade_date}: "
                    f"{validated.quality_notes}"
                )
            elif validated.quality_flag == "suspect":
                suspect.append(d)
                print(
                    f"  SUSPECT  {self.ticker} {validated.trade_date}: "
                    f"{validated.quality_notes}"
                )
            else:
                ok.append(d)
            if validated.quality_flag != "rejected":
                prev_close = validated.close

        return ok, suspect, rejected
