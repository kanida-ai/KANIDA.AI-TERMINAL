#!/usr/bin/env python3
"""build_signal_day_context.py — S-CONTEXT: signal-day PRE-ENTRY context layer.

ADDITIVE study layer on top of the existing per-signal-day study. It does NOT
modify `falcon_signal_day_study` or its v3 Excel. For every study row it computes
the SIGNAL-DAY (pre-entry) market context — what the stock did ON the signal day
and the day before, relative to its own prior closes and 20-day volume — and
stores it in a NEW RND table `falcon_signal_day_context` keyed by the study row
`id`. It also:
  (1) writes the new context table (idempotent, RND-only),
  (2) regenerates the study Excel WITH the context columns appended (a NEW file,
      out/v8/falcon_signal_day_study_with_context.xlsx — v3 is NOT overwritten),
  (3) writes a separate 7-sheet analysis workbook
      out/v8/falcon_signal_day_context_analysis.xlsx (RESOLVED rows only),
  (4) appends "extension" rows for the recent signal days that post-date the
      study (2026-06-13..latest) so the file is current, computed via the SAME
      engine/sim path as the study (REUSED, never re-implemented),
  (5) runs parity checks and prints a threshold report.

────────────────────────────────────────────────────────────────────────────
WHY READ THE STUDY ROWS FROM THE RND DB (not the v3 Excel)
  The v3 Excel's 'All Signal-Day Trades' sheet has NO `id` column (its header is
  exactly build_signal_day_study._COLS, which excludes the DB PK). The context
  table is keyed by `id`, so we read the canonical, id-bearing study rows from
  the RND `falcon_signal_day_study` table (persona='falcon_top10_daily'). Those
  rows hold the IDENTICAL data the v3 Excel holds (same builder, same columns) —
  so the regenerated Excel preserves every original column AND gains a true join
  key. The original v3 file is never read or written.

OHLC SOURCE = PROD (verified): RND ohlc_daily is stale to 2026-05-07. All OHLCV
  lookups (signal-day bar, prev/prev-prev close, 20-day volume) come from PROD
  ohlc_daily, which covers 2016-01-01..2026-06-18 with volume populated. The
  per-symbol trading calendar is derived from PROD distinct trade_dates (handles
  weekends/holidays): "prev trading day" = latest trade_date < signal_date for
  that symbol; "prev-prev" = the one before that.

NULL DISCIPLINE: every context field is NULL (never imputed) when a needed bar is
  missing — e.g. a stock's first appearance with no prior close, or <20 prior
  trading days for the 20-day volume average.

CLI (mirrors sibling builders):
  python build_signal_day_context.py --rnd-db <p> --prod-db <p> [--out <dir>]
  python build_signal_day_context.py --dry-run     # compute + print, write NOTHING

Constraints honored: RND-only writes; PROD read-only; never modifies
falcon_signal_day_study or out/v3; idempotent (DROP+CREATE the context table in
one txn); no Kite; no prod-tree / shared-engine mutation.
"""
from __future__ import annotations

import argparse
import math
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Windows consoles default to cp1252 → force utf-8 so box-drawing / ₹ never crash.
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# ── Import root (mirror build_signal_day_study.py exactly). ──────────────────
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent.parent
_BACKEND_ROOT = _REPO_ROOT / "backend"
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

from power_user.services.persona_simulator import (  # noqa: E402
    _resolve_rnd_db_path,
    PROD_DB,
)

# REUSE the study builder's engine path for the extension rows — never
# re-implement selection / simulation / row-building (parity guaranteed).
import build_signal_day_study as study  # noqa: E402

STUDY_PERSONA_TAG = "falcon_top10_daily"
CONTEXT_TABLE = "falcon_signal_day_context"

# Circuit proxy thresholds (NSE limits actually vary 2/5/10/20% — this is an
# APPROXIMATE flag; documented as such).
CIRCUIT_UP_MULT = 1.18    # close >= prev_close * 1.18  → 'UPPER'
CIRCUIT_DOWN_MULT = 0.82  # close <= prev_close * 0.82  → 'LOWER'

# entry_context priority order (HIGH → LOW). First satisfied flag wins. Documented.
ENTRY_CONTEXT_PRIORITY = [
    "SIGNAL_DAY_CIRCUIT",   # is_signal_day_circuit
    "HIGH_VOL_CONFIRM",     # is_high_vol_confirm
    "EXTENDED_MOVE",        # is_extended_move
    "FRESH_BREAKOUT",       # is_fresh_breakout
    "GAP_UP_ENTRY",         # is_gap_up_entry
    "GAP_DOWN_ENTRY",       # is_gap_down_entry
    "LOW_VOL_WARNING",      # is_low_vol_warning
    # else NORMAL
]


# ════════════════════════════════════════════════════════════════════════════
# 1. PROD OHLCV LOADER  (with VOLUME — the shared engine's load_all_bars omits it)
# ════════════════════════════════════════════════════════════════════════════

def load_ohlcv(prod_db: str, start: str, end: str) -> Dict[str, List[Dict[str, Any]]]:
    """{symbol -> [bar,...]} sorted by trade_date, each bar with OHLC + volume.

    We load our OWN OHLCV (the engine's load_all_bars drops volume, and we must
    not modify it). PROD ohlc_daily is the verified OHLC source."""
    con = sqlite3.connect(prod_db, timeout=120.0)
    try:
        rows = con.execute(
            "SELECT symbol, trade_date, open, high, low, close, volume "
            "  FROM ohlc_daily WHERE trade_date BETWEEN ? AND ?",
            (start, end),
        ).fetchall()
    finally:
        con.close()
    bars: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for sym, td, o, h, l, c, v in rows:
        bars[sym].append({"date": td, "open": o, "high": h, "low": l,
                          "close": c, "volume": v})
    return {s: sorted(bs, key=lambda b: b["date"]) for s, bs in bars.items()}


def _date_index(bars_sym: List[Dict[str, Any]]) -> Dict[str, int]:
    return {b["date"]: i for i, b in enumerate(bars_sym)}


# ════════════════════════════════════════════════════════════════════════════
# 2. CONTEXT COMPUTATION  (per study row — all Part-1 formulas)
# ════════════════════════════════════════════════════════════════════════════
#
# Column → formula mapping (every NULL is a true "bar missing", never imputed):
#   signal_day_open/high/low/close = the signal_date bar's raw OHLC
#   prev_close       = close at the prev trading day (< signal_date)
#   prev_prev_close  = close 2 trading days before signal_date
#   signal_day_ret_pct      = (sd_close - prev_close)/prev_close*100
#   signal_day_high_pct     = (sd_high  - prev_close)/prev_close*100
#   signal_day_low_pct      = (sd_low   - prev_close)/prev_close*100
#   signal_day_range_pct    = (sd_high - sd_low)/prev_close*100
#   signal_day_circuit      = 'UPPER' if sd_close>=prev_close*1.18,
#                             'LOWER' if sd_close<=prev_close*0.82, else 'NONE'  (PROXY)
#   prev_day_ret_pct        = (prev_close - prev_prev_close)/prev_prev_close*100
#   prev_day_high_pct       = (prev_high  - prev_prev_close)/prev_prev_close*100
#   prev_day_low_pct        = (prev_low   - prev_prev_close)/prev_prev_close*100
#   prev_day_volume         = volume at prev trading day
#   signal_day_volume       = volume at signal_date
#   avg_20d_volume          = mean volume over the 20 trading days STRICTLY before
#                             signal_date (NULL if <20 available)
#   signal_day_vol_ratio    = signal_day_volume / avg_20d_volume
#   entry_gap_pct           = (entry_price - sd_close)/sd_close*100   (entry from study row)
#   two_day_ret_pct         = (sd_close - prev_prev_close)/prev_prev_close*100
#
#   Booleans (multiple may be true):
#     is_fresh_breakout  = signal_day_ret>5 AND prev_day_ret in [-1,1]
#     is_extended_move   = two_day_ret>10
#     is_gap_up_entry    = entry_gap_pct>2
#     is_gap_down_entry  = entry_gap_pct<-2
#     is_signal_day_circuit = signal_day_circuit != 'NONE'
#     is_high_vol_confirm   = vol_ratio>2 AND signal_day_ret>3
#     is_low_vol_warning    = vol_ratio<0.8
#   entry_context = priority-resolved single label (ENTRY_CONTEXT_PRIORITY), else NORMAL.

_CTX_FLOAT_COLS = [
    "signal_day_open", "signal_day_high", "signal_day_low", "signal_day_close",
    "prev_close", "prev_prev_close",
    "signal_day_ret_pct", "signal_day_high_pct", "signal_day_low_pct",
    "signal_day_range_pct",
    "prev_day_ret_pct", "prev_day_high_pct", "prev_day_low_pct",
    "prev_day_volume", "signal_day_volume", "avg_20d_volume",
    "signal_day_vol_ratio", "entry_gap_pct", "two_day_ret_pct",
]
_CTX_BOOL_COLS = [
    "is_fresh_breakout", "is_extended_move", "is_gap_up_entry",
    "is_gap_down_entry", "is_signal_day_circuit", "is_high_vol_confirm",
    "is_low_vol_warning",
]
# Full ordered column set for the context table / Excel (id + keys + all fields).
CONTEXT_COLS = (
    ["id", "signal_date", "symbol"]
    + _CTX_FLOAT_COLS
    + ["signal_day_circuit"]
    + _CTX_BOOL_COLS
    + ["entry_context", "is_extension"]
)


def _null_context(study_id: Any, signal_date: str, symbol: str,
                  is_extension: int) -> Dict[str, Any]:
    row: Dict[str, Any] = {c: None for c in CONTEXT_COLS}
    row["id"] = study_id
    row["signal_date"] = signal_date
    row["symbol"] = symbol
    row["is_extension"] = is_extension
    row["entry_context"] = "NORMAL"   # default label when context can't be built
    return row


def compute_context(
    study_id: Any,
    signal_date: str,
    symbol: str,
    entry_price: Optional[float],
    bars_sym: Optional[List[Dict[str, Any]]],
    didx: Optional[Dict[str, int]],
    is_extension: int,
) -> Dict[str, Any]:
    """Build one context row. Anything that needs a missing bar → NULL."""
    row = _null_context(study_id, signal_date, symbol, is_extension)
    if not bars_sym or didx is None:
        return row

    # Signal-day bar index. If signal_date itself isn't a trading bar for this
    # symbol (rare data gap), fall back to NULL context (can't anchor).
    sd_i = didx.get(signal_date)
    if sd_i is None:
        return row
    sd = bars_sym[sd_i]
    row["signal_day_open"] = sd["open"]
    row["signal_day_high"] = sd["high"]
    row["signal_day_low"] = sd["low"]
    row["signal_day_close"] = sd["close"]
    row["signal_day_volume"] = sd["volume"]

    # prev / prev-prev trading bars (strictly before signal_date for THIS symbol).
    prev = bars_sym[sd_i - 1] if sd_i - 1 >= 0 else None
    prev_prev = bars_sym[sd_i - 2] if sd_i - 2 >= 0 else None
    prev_close = prev["close"] if prev else None
    prev_prev_close = prev_prev["close"] if prev_prev else None
    row["prev_close"] = prev_close
    row["prev_prev_close"] = prev_prev_close
    if prev is not None:
        row["prev_day_volume"] = prev["volume"]

    sd_close = sd["close"]

    # signal-day returns vs prev_close.
    if prev_close not in (None, 0):
        row["signal_day_ret_pct"] = (sd_close - prev_close) / prev_close * 100.0
        row["signal_day_high_pct"] = (sd["high"] - prev_close) / prev_close * 100.0
        row["signal_day_low_pct"] = (sd["low"] - prev_close) / prev_close * 100.0
        row["signal_day_range_pct"] = (sd["high"] - sd["low"]) / prev_close * 100.0
        # circuit PROXY (approximate — NSE limits vary 2/5/10/20%).
        if sd_close >= prev_close * CIRCUIT_UP_MULT:
            row["signal_day_circuit"] = "UPPER"
        elif sd_close <= prev_close * CIRCUIT_DOWN_MULT:
            row["signal_day_circuit"] = "LOWER"
        else:
            row["signal_day_circuit"] = "NONE"
    # else: leave signal_day_circuit NULL (no prev_close to judge against)

    # prev-day returns vs prev_prev_close.
    if prev is not None and prev_prev_close not in (None, 0):
        row["prev_day_ret_pct"] = (prev_close - prev_prev_close) / prev_prev_close * 100.0
        row["prev_day_high_pct"] = (prev["high"] - prev_prev_close) / prev_prev_close * 100.0
        row["prev_day_low_pct"] = (prev["low"] - prev_prev_close) / prev_prev_close * 100.0

    # two-day return vs prev_prev_close.
    if prev_prev_close not in (None, 0):
        row["two_day_ret_pct"] = (sd_close - prev_prev_close) / prev_prev_close * 100.0

    # 20-day average volume STRICTLY before signal_date (need >=20 prior bars).
    if sd_i >= 20:
        window = bars_sym[sd_i - 20: sd_i]   # exactly the 20 bars before signal_date
        vols = [b["volume"] for b in window if b["volume"] is not None]
        if len(vols) == 20:
            avg20 = sum(vols) / 20.0
            row["avg_20d_volume"] = avg20
            if avg20 not in (None, 0) and sd["volume"] is not None:
                row["signal_day_vol_ratio"] = sd["volume"] / avg20
    # else: <20 prior days → avg_20d_volume + vol_ratio stay NULL (no impute)

    # entry gap vs signal-day close (entry_price from the study row).
    if entry_price is not None and sd_close not in (None, 0):
        row["entry_gap_pct"] = (entry_price - sd_close) / sd_close * 100.0

    # ── Boolean flags (multiple may be true). A flag is NULL-safe: it can only be
    #    True when ALL its inputs are non-NULL; otherwise it is 0 (not asserted).
    sdr = row["signal_day_ret_pct"]
    pdr = row["prev_day_ret_pct"]
    twr = row["two_day_ret_pct"]
    egp = row["entry_gap_pct"]
    vr = row["signal_day_vol_ratio"]
    circ = row["signal_day_circuit"]

    row["is_fresh_breakout"] = 1 if (sdr is not None and pdr is not None
                                     and sdr > 5 and -1 <= pdr <= 1) else 0
    row["is_extended_move"] = 1 if (twr is not None and twr > 10) else 0
    row["is_gap_up_entry"] = 1 if (egp is not None and egp > 2) else 0
    row["is_gap_down_entry"] = 1 if (egp is not None and egp < -2) else 0
    row["is_signal_day_circuit"] = 1 if (circ is not None and circ != "NONE") else 0
    row["is_high_vol_confirm"] = 1 if (vr is not None and sdr is not None
                                       and vr > 2 and sdr > 3) else 0
    row["is_low_vol_warning"] = 1 if (vr is not None and vr < 0.8) else 0

    # ── entry_context — priority-resolved single label.
    flag_for_label = {
        "SIGNAL_DAY_CIRCUIT": "is_signal_day_circuit",
        "HIGH_VOL_CONFIRM": "is_high_vol_confirm",
        "EXTENDED_MOVE": "is_extended_move",
        "FRESH_BREAKOUT": "is_fresh_breakout",
        "GAP_UP_ENTRY": "is_gap_up_entry",
        "GAP_DOWN_ENTRY": "is_gap_down_entry",
        "LOW_VOL_WARNING": "is_low_vol_warning",
    }
    label = "NORMAL"
    for lab in ENTRY_CONTEXT_PRIORITY:
        if row.get(flag_for_label[lab]) == 1:
            label = lab
            break
    row["entry_context"] = label
    return row


# ════════════════════════════════════════════════════════════════════════════
# 3. LOAD EXISTING STUDY ROWS (from RND DB — they carry `id`)
# ════════════════════════════════════════════════════════════════════════════

def load_study_rows(rnd_db: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    """All persona='falcon_top10_daily' rows from falcon_signal_day_study, as
    dicts including `id`. These are the canonical, id-bearing study rows (the v3
    Excel content minus the absent id column)."""
    con = sqlite3.connect(rnd_db, timeout=120.0)
    con.row_factory = sqlite3.Row
    try:
        cur = con.execute(
            "SELECT * FROM falcon_signal_day_study WHERE persona = ? ORDER BY id",
            (STUDY_PERSONA_TAG,),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        con.close()
    return rows, cols


# ════════════════════════════════════════════════════════════════════════════
# 4. EXTENSION ROWS (recent signal days post-dating the study) — REUSE the engine
# ════════════════════════════════════════════════════════════════════════════

def build_extension_rows(
    rnd_db: str,
    prod_db: str,
    study_max_signal_date: str,
    top_n: int,
) -> Tuple[List[Dict[str, Any]], str]:
    """Recompute every top-N pick whose signal_date > study_max_signal_date using
    the STUDY's own engine/sim path (study._build_run_context,
    study._year_signals_by_signal_date, study.simulate_independent_pick,
    study.build_row). Returns (rows_shaped_like_study_rows, mode_note).

    These rows are IDENTICAL in computation to the study's rows; recent picks
    whose 7-day hold hasn't fully elapsed come out OPEN_AT_BACKTEST_END_MTM with
    NULL net_ret_pct (exactly like the study's edge rows). On any failure we fall
    back to falcon_signals_live and document it.
    """
    try:
        ctx = study._build_run_context(rnd_db, prod_db)
    except Exception as e:  # pragma: no cover — defensive
        return _extension_fallback(prod_db, study_max_signal_date, top_n,
                                   reason=f"run-context build failed: {e}")

    run_cfg = ctx["run_cfg"]
    hold_days = run_cfg.hold_days
    bars = ctx["bars"]
    sector_map = ctx["sector_map"]
    years = ctx["years_in_window"]

    # Only re-run years that could contain signal_dates after the study cutoff.
    cutoff_year = int(study_max_signal_date[:4])
    cand_years = [y for y in years if y >= cutoff_year]

    ext_trades: List[Tuple[int, Dict[str, Any]]] = []  # (rank, trade)
    ext_keys: List[Tuple[str, str]] = []
    for Y in cand_years:
        sigs_by_sd = study._year_signals_by_signal_date(ctx, Y)
        for sd, group in sigs_by_sd.items():
            if sd <= study_max_signal_date:
                continue   # already in the study
            ranked = sorted(group, key=lambda s: s["score"], reverse=True)[:top_n]
            for pos, sig in enumerate(ranked, start=1):
                sym = sig["symbol"]
                bs = bars.get(sym)
                if not bs:
                    continue
                sd_idx = next((i for i, b in enumerate(bs) if b["date"] > sd), None)
                if sd_idx is None:
                    continue
                trade = study.simulate_independent_pick(sig, bs, sd_idx, run_cfg)
                if trade is None:
                    continue
                ext_trades.append((pos, trade))
                ext_keys.append((sd, sym))

    if not ext_trades:
        return [], ("no new signal_dates after %s (study already current)"
                    % study_max_signal_date)

    prior_idx = study.build_prior_appearances_index(ext_keys)
    rows: List[Dict[str, Any]] = []
    for rank, trade in ext_trades:
        sym = trade["symbol"]
        bs = bars[sym]
        prior_30d = prior_idx.get((trade["signal_date"], sym), 0)
        r = study.build_row(trade, rank, sector_map, bs, hold_days, prior_30d)
        rows.append(r)
    return rows, ("engine-reuse (study sim path); %d extension picks" % len(rows))


def _extension_fallback(
    prod_db: str,
    study_max_signal_date: str,
    top_n: int,
    reason: str,
) -> Tuple[List[Dict[str, Any]], str]:
    """Fallback: read recent top-N from PROD falcon_signals_live, set
    entry_price = entry-day open, compute d1_close_ret directly from OHLC, leave
    full-hold outcome columns NULL. Documented when used."""
    con = sqlite3.connect(prod_db, timeout=120.0)
    try:
        has = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='falcon_signals_live'"
        ).fetchone()
        if not has:
            return [], reason + " | falcon_signals_live absent — NO extension rows"
        sig_rows = con.execute(
            "SELECT signal_date, symbol, rank FROM falcon_signals_live "
            "WHERE signal_date > ? ORDER BY signal_date, rank",
            (study_max_signal_date,),
        ).fetchall()
    finally:
        con.close()
    if not sig_rows:
        return [], reason + " | falcon_signals_live has no rows after cutoff"

    ohlcv = load_ohlcv(prod_db, "2020-12-01", "2099-12-31")
    rows: List[Dict[str, Any]] = []
    for sd, sym, rank in sig_rows:
        if rank is not None and rank > top_n:
            continue
        bs = ohlcv.get(sym)
        if not bs:
            continue
        didx = _date_index(bs)
        e_i = next((i for i, b in enumerate(bs) if b["date"] > sd), None)
        if e_i is None:
            continue
        entry_open = bs[e_i]["open"]
        d1 = None
        if e_i < len(bs) and entry_open:
            d1 = (bs[e_i]["close"] / entry_open - 1.0) * 100.0
        r = {c: None for c in study._COLS}
        r["persona"] = STUDY_PERSONA_TAG
        r["signal_date"] = sd
        r["entry_date"] = bs[e_i]["date"]
        r["symbol"] = sym
        r["engine_rank"] = rank
        r["entry_price"] = entry_open
        r["exit_reason"] = "OPEN_AT_BACKTEST_END_MTM"
        r["d1_close_ret"] = d1
        rows.append(r)
    return rows, reason + (" | FALLBACK via falcon_signals_live: %d rows" % len(rows))


# ════════════════════════════════════════════════════════════════════════════
# 5. WRITE CONTEXT TABLE (idempotent, single txn, RND-only)
# ════════════════════════════════════════════════════════════════════════════

def _ctx_sqlite_type(col: str) -> str:
    if col == "id":
        return "INTEGER"
    if col in ("signal_date", "symbol", "signal_day_circuit", "entry_context"):
        return "TEXT"
    if col in _CTX_BOOL_COLS or col == "is_extension":
        return "INTEGER"
    return "REAL"


def write_context_table(rnd_db: str, ctx_rows: List[Dict[str, Any]]) -> None:
    con = sqlite3.connect(rnd_db, timeout=120.0)
    try:
        con.execute("BEGIN")
        con.execute(f"DROP TABLE IF EXISTS {CONTEXT_TABLE}")
        col_defs = ",\n  ".join(f"{c} {_ctx_sqlite_type(c)}" for c in CONTEXT_COLS)
        con.execute(
            f"CREATE TABLE {CONTEXT_TABLE} (\n  {col_defs},\n"
            f"  created_at TEXT DEFAULT (datetime('now'))\n)"
        )
        con.execute(
            f"CREATE INDEX idx_fsdc_signal_date ON {CONTEXT_TABLE}(signal_date)")
        con.execute(
            f"CREATE INDEX idx_fsdc_entry_context ON {CONTEXT_TABLE}(entry_context)")
        placeholders = ", ".join("?" for _ in CONTEXT_COLS)
        con.executemany(
            f"INSERT INTO {CONTEXT_TABLE} ({', '.join(CONTEXT_COLS)}) "
            f"VALUES ({placeholders})",
            [[r.get(c) for c in CONTEXT_COLS] for r in ctx_rows],
        )
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


# ════════════════════════════════════════════════════════════════════════════
# 6. ANALYSIS BUCKETS / STATS  (Part 3 — RESOLVED rows only, no extension)
# ════════════════════════════════════════════════════════════════════════════

def _rate(num: int, den: int) -> Optional[float]:
    return (num / den * 100.0) if den else None


def _avg(vals: List[float]) -> Optional[float]:
    v = [x for x in vals if x is not None]
    return (sum(v) / len(v)) if v else None


def _bucket(value: Optional[float], edges: List[Tuple[str, Any, Any]]) -> Optional[str]:
    """edges = list of (label, lo, hi); lo/hi may be None for open ends.
    Interval is [lo, hi) — lo inclusive, hi exclusive — matching the prompt's
    contiguous bands. Returns the first matching label, or None if value is None."""
    if value is None:
        return None
    for label, lo, hi in edges:
        if (lo is None or value >= lo) and (hi is None or value < hi):
            return label
    return None


def _grp_stats(rows: List[Dict[str, Any]], fields: List[str]) -> Dict[str, Any]:
    """Generic aggregate over a row group. `fields` controls which metrics are
    computed (keeps each sheet's columns to exactly what the spec asks)."""
    n = len(rows)
    out: Dict[str, Any] = {"N": n}
    if n == 0:
        for f in fields:
            out[f] = None
        return out
    rets = [r["net_ret_pct"] for r in rows if r.get("net_ret_pct") is not None]
    if "win_rate" in fields:
        out["win_rate"] = _rate(sum(1 for x in rets if x > 0), len(rets))
    if "avg_net_ret" in fields:
        out["avg_net_ret"] = _avg(rets)
    if "avg_d1" in fields:
        out["avg_d1"] = _avg([r.get("d1_close_ret") for r in rows])
    if "avg_d7" in fields:
        out["avg_d7"] = _avg([r.get("d7_close_ret") for r in rows])
    if "avg_d30" in fields:
        out["avg_d30"] = _avg([r.get("d30_close_ret") for r in rows])
    if "avg_signal_day_ret" in fields:
        out["avg_signal_day_ret"] = _avg([r.get("signal_day_ret_pct") for r in rows])
    if "avg_entry_gap" in fields:
        out["avg_entry_gap"] = _avg([r.get("entry_gap_pct") for r in rows])
    if "stop_rate" in fields:
        out["stop_rate"] = _rate(sum(1 for r in rows if r.get("exit_reason") == "INIT_STOP"), n)
    if "trail_rate" in fields:
        out["trail_rate"] = _rate(sum(1 for r in rows if r.get("exit_reason") == "TRAIL_GIVEBACK"), n)
    if "avg_post_hold_peak_high_ret" in fields:
        # D30 post-hold peak window proxy: prefer post_hold_peak_high_ret, else d30 high.
        vals = []
        for r in rows:
            v = r.get("post_hold_peak_high_ret")
            if v is None:
                v = r.get("d30_high_ret")
            if v is not None:
                vals.append(v)
        out["avg_post_hold_peak_high_ret"] = _avg(vals) if vals else None
    return out


# Bucket edge definitions (label, lo_inclusive, hi_exclusive).
SIGNAL_RET_BUCKETS = [
    ("<-5", None, -5), ("-5..-2", -5, -2), ("-2..0", -2, 0), ("0..2", 0, 2),
    ("2..5", 2, 5), ("5..7", 5, 7), ("7..10", 7, 10), ("10..15", 10, 15),
    (">15", 15, None),
]
ENTRY_GAP_BUCKETS = [
    ("<-3", None, -3), ("-3..-1", -3, -1), ("-1..0", -1, 0), ("0..1", 0, 1),
    ("1..3", 1, 3), ("3..5", 3, 5), (">5", 5, None),
]
TWO_DAY_BUCKETS = [
    ("<0", None, 0), ("0..3", 0, 3), ("3..7", 3, 7), ("7..10", 7, 10),
    ("10..15", 10, 15), ("15..20", 15, 20), (">20", 20, None),
]
VOL_RATIO_BUCKETS = [
    ("<0.5", None, 0.5), ("0.5..1", 0.5, 1), ("1..2", 1, 2), ("2..3", 2, 3),
    (">3", 3, None),
]


def build_analysis(joined: List[Dict[str, Any]]) -> Dict[str, Any]:
    """`joined` = study rows merged with their context, RESOLVED & non-extension
    only (net_ret_pct not None, is_extension != 1). Returns the 7 sheets' data."""
    res: Dict[str, Any] = {}

    # S1 — Summary by entry_context.
    s1 = []
    by_ec: Dict[str, List[Dict]] = defaultdict(list)
    for r in joined:
        by_ec[r.get("entry_context") or "NORMAL"].append(r)
    for ec in sorted(by_ec):
        st = _grp_stats(by_ec[ec], ["win_rate", "avg_net_ret", "avg_d1",
                                    "avg_signal_day_ret", "avg_entry_gap",
                                    "stop_rate", "avg_post_hold_peak_high_ret"])
        st = {"entry_context": ec, **st}
        s1.append(st)
    res["S1"] = s1

    # S2 — Signal-Day Return buckets.
    s2 = _bucketed(joined, "signal_day_ret_pct", SIGNAL_RET_BUCKETS,
                   ["win_rate", "avg_net_ret", "avg_d1", "avg_d7", "avg_d30",
                    "stop_rate", "trail_rate"], "signal_day_ret_bucket")
    res["S2"] = s2

    # S3 — Entry Gap buckets.
    res["S3"] = _bucketed(joined, "entry_gap_pct", ENTRY_GAP_BUCKETS,
                          ["win_rate", "avg_net_ret", "avg_d1", "stop_rate"],
                          "entry_gap_bucket")

    # S4 — Two-Day Momentum buckets.
    res["S4"] = _bucketed(joined, "two_day_ret_pct", TWO_DAY_BUCKETS,
                          ["win_rate", "avg_net_ret", "stop_rate"],
                          "two_day_ret_bucket")

    # S5 — Volume Confirmation buckets.
    res["S5"] = _bucketed(joined, "signal_day_vol_ratio", VOL_RATIO_BUCKETS,
                          ["win_rate", "avg_net_ret", "stop_rate"],
                          "vol_ratio_bucket")

    # S6 — Cross-tab: signal_day_ret buckets (rows) × vol_ratio buckets (cols) → WR.
    res["S6"] = _crosstab_wr(joined)

    # S7 — Combined Gate: CONFIRMED proxy (d1_close_ret>0.5) by entry_context;
    # engine_rank<=3 tagged enterprise-ish (documented proxies).
    s7 = []
    confirmed = [r for r in joined if (r.get("d1_close_ret") is not None
                                       and r["d1_close_ret"] > 0.5)]
    by_ec2: Dict[str, List[Dict]] = defaultdict(list)
    for r in confirmed:
        by_ec2[r.get("entry_context") or "NORMAL"].append(r)
    for ec in sorted(by_ec2):
        grp = by_ec2[ec]
        st = _grp_stats(grp, ["win_rate", "avg_net_ret"])
        ent = [r for r in grp if (r.get("engine_rank") or 99) <= 3]
        st = {"entry_context": ec, **st,
              "N_enterprise_rank<=3": len(ent),
              "enterprise_win_rate": _grp_stats(ent, ["win_rate"]).get("win_rate"),
              "enterprise_avg_net_ret": _grp_stats(ent, ["avg_net_ret"]).get("avg_net_ret")}
        s7.append(st)
    res["S7"] = s7
    return res


def _bucketed(rows: List[Dict[str, Any]], field: str,
              edges: List[Tuple[str, Any, Any]], metrics: List[str],
              label_key: str) -> List[Dict[str, Any]]:
    by_b: Dict[str, List[Dict]] = defaultdict(list)
    for r in rows:
        b = _bucket(r.get(field), edges)
        if b is not None:
            by_b[b].append(r)
    out = []
    for label, _lo, _hi in edges:   # preserve declared bucket order
        st = _grp_stats(by_b.get(label, []), metrics)
        out.append({label_key: label, **st})
    return out


def _crosstab_wr(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Win-rate matrix: rows = SIGNAL_RET_BUCKETS, cols = VOL_RATIO_BUCKETS."""
    cells: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
    for r in rows:
        rb = _bucket(r.get("signal_day_ret_pct"), SIGNAL_RET_BUCKETS)
        cb = _bucket(r.get("signal_day_vol_ratio"), VOL_RATIO_BUCKETS)
        if rb is not None and cb is not None:
            cells[(rb, cb)].append(r)
    row_labels = [e[0] for e in SIGNAL_RET_BUCKETS]
    col_labels = [e[0] for e in VOL_RATIO_BUCKETS]
    matrix = []
    for rb in row_labels:
        line = {"signal_day_ret_bucket": rb}
        for cb in col_labels:
            grp = cells.get((rb, cb), [])
            rets = [x["net_ret_pct"] for x in grp if x.get("net_ret_pct") is not None]
            wr = _rate(sum(1 for v in rets if v > 0), len(rets))
            line[f"{cb} (N={len(grp)})"] = wr
        matrix.append(line)
    return {"row_labels": row_labels, "col_labels": col_labels, "matrix": matrix}


# ════════════════════════════════════════════════════════════════════════════
# 7. EXCEL WRITERS  (regenerated study + analysis) — openpyxl, CSV fallback
# ════════════════════════════════════════════════════════════════════════════

def _try_openpyxl():
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
        return Workbook, Font
    except Exception:
        return None, None


def write_study_with_context(
    out_dir: Path,
    study_cols: List[str],
    study_rows: List[Dict[str, Any]],     # original (has id), with is_extension=0
    ext_rows: List[Dict[str, Any]],       # extension (has id assigned), is_extension=1
    ctx_by_id: Dict[Any, Dict[str, Any]],
    per_year_sheet: List[List[Any]],
    overall_sheet: List[List[Any]],
) -> Path:
    """out/v8/falcon_signal_day_study_with_context.xlsx.

    Sheet 'All Signal-Day Trades' = id + ALL original study columns + appended
    context columns, original rows first then extension rows. 'Per-Year Efficacy'
    and 'Overall' copied unchanged (read back from the original table aggregates).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "falcon_signal_day_study_with_context.xlsx"
    Workbook, Font = _try_openpyxl()

    # Header = id + original study columns (minus id/created_at if present) +
    # context-only columns (exclude id/signal_date/symbol already implied).
    base_cols = ["id"] + [c for c in study_cols if c not in ("id", "created_at")]
    ctx_extra = [c for c in CONTEXT_COLS if c not in ("id", "signal_date", "symbol")]
    header = base_cols + ctx_extra

    def row_values(r: Dict[str, Any]) -> List[Any]:
        rid = r.get("id")
        c = ctx_by_id.get(rid, {})
        return ([rid] + [r.get(col) for col in base_cols if col != "id"]
                + [c.get(col) for col in ctx_extra])

    all_rows = study_rows + ext_rows

    if Workbook is None:
        return _csv_fallback_study(out_dir, header, all_rows, row_values,
                                   per_year_sheet, overall_sheet)

    wb = Workbook()
    ws1 = wb.active
    ws1.title = "All Signal-Day Trades"
    ws1.append(header)
    for cell in ws1[1]:
        cell.font = Font(bold=True)
    for r in all_rows:
        ws1.append(row_values(r))

    ws2 = wb.create_sheet("Per-Year Efficacy")
    for line in per_year_sheet:
        ws2.append(line)
    if per_year_sheet:
        for cell in ws2[1]:
            cell.font = Font(bold=True)

    ws3 = wb.create_sheet("Overall")
    for line in overall_sheet:
        ws3.append(line)
    if overall_sheet:
        for cell in ws3[1]:
            cell.font = Font(bold=True)

    wb.save(path)
    return path


def _csv_fallback_study(out_dir, header, all_rows, row_values, per_year, overall):
    import csv
    p = out_dir / "falcon_signal_day_study_with_context.csv"
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in all_rows:
            w.writerow(row_values(r))
    with open(out_dir / "falcon_study_per_year.csv", "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(per_year)
    with open(out_dir / "falcon_study_overall.csv", "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(overall)
    print(f"[context] openpyxl missing — wrote CSV fallback: {p.name}")
    return p


def write_analysis(out_dir: Path, an: Dict[str, Any]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "falcon_signal_day_context_analysis.xlsx"
    Workbook, Font = _try_openpyxl()
    if Workbook is None:
        return _csv_fallback_analysis(out_dir, an)

    wb = Workbook()
    first = True

    def add_sheet(title: str, header: List[str], rows: List[List[Any]]):
        nonlocal first
        ws = wb.active if first else wb.create_sheet(title)
        if first:
            ws.title = title
            first = False
        ws.append(header)
        for cell in ws[1]:
            cell.font = Font(bold=True)
        for r in rows:
            ws.append(r)

    # S1
    s1_cols = ["entry_context", "N", "win_rate", "avg_net_ret", "avg_d1",
               "avg_signal_day_ret", "avg_entry_gap", "stop_rate",
               "avg_post_hold_peak_high_ret"]
    add_sheet("S1 by entry_context", s1_cols,
              [[d.get(c) for c in s1_cols] for d in an["S1"]])
    # S2
    s2_cols = ["signal_day_ret_bucket", "N", "win_rate", "avg_net_ret", "avg_d1",
               "avg_d7", "avg_d30", "stop_rate", "trail_rate"]
    add_sheet("S2 signal-day ret buckets", s2_cols,
              [[d.get(c) for c in s2_cols] for d in an["S2"]])
    # S3
    s3_cols = ["entry_gap_bucket", "N", "win_rate", "avg_net_ret", "avg_d1", "stop_rate"]
    add_sheet("S3 entry-gap buckets", s3_cols,
              [[d.get(c) for c in s3_cols] for d in an["S3"]])
    # S4
    s4_cols = ["two_day_ret_bucket", "N", "win_rate", "avg_net_ret", "stop_rate"]
    add_sheet("S4 two-day momentum", s4_cols,
              [[d.get(c) for c in s4_cols] for d in an["S4"]])
    # S5
    s5_cols = ["vol_ratio_bucket", "N", "win_rate", "avg_net_ret", "stop_rate"]
    add_sheet("S5 volume confirmation", s5_cols,
              [[d.get(c) for c in s5_cols] for d in an["S5"]])
    # S6 cross-tab
    ct = an["S6"]
    # matrix always has one row per SIGNAL_RET_BUCKET; derive col headers from the
    # first row's vol-ratio keys (every row shares the same keys).
    if ct["matrix"]:
        col_keys = [k for k in ct["matrix"][0].keys() if k != "signal_day_ret_bucket"]
    else:
        col_keys = []
    s6_header = ["signal_day_ret_bucket \\ vol_ratio"] + col_keys
    s6_rows = []
    for line in ct["matrix"]:
        s6_rows.append([line.get("signal_day_ret_bucket")] +
                       [v for k, v in line.items() if k != "signal_day_ret_bucket"])
    add_sheet("S6 ret x vol WR matrix", s6_header, s6_rows)
    # S7
    s7_cols = ["entry_context", "N", "win_rate", "avg_net_ret",
               "N_enterprise_rank<=3", "enterprise_win_rate", "enterprise_avg_net_ret"]
    add_sheet("S7 combined gate (d1>0.5)", s7_cols,
              [[d.get(c) for c in s7_cols] for d in an["S7"]])

    wb.save(path)
    return path


def _csv_fallback_analysis(out_dir: Path, an: Dict[str, Any]) -> Path:
    import csv, json
    p = out_dir / "falcon_signal_day_context_analysis.json"
    with open(p, "w", encoding="utf-8") as f:
        json.dump(an, f, indent=2, default=str)
    print(f"[context] openpyxl missing — wrote JSON fallback: {p.name}")
    return p


# ════════════════════════════════════════════════════════════════════════════
# 8. PER-YEAR / OVERALL SHEETS for the regenerated study (copy of original logic)
# ════════════════════════════════════════════════════════════════════════════

def build_efficacy_sheets(study_rows: List[Dict[str, Any]]):
    """Reproduce the v3 'Per-Year Efficacy' + 'Overall' sheets from the original
    study rows (resolved only) using the study builder's compute_efficacy — so
    the two copied sheets match v3 exactly."""
    rows_by_year: Dict[int, List[Dict]] = defaultdict(list)
    for r in study_rows:
        rows_by_year[int(r["signal_date"][:4])].append(r)
    py_cols = ["year", "n_picks", "win_rate", "avg_ret", "median_ret",
               "big_winner_rate", "big_loser_rate", "avg_post_hold_high_ret",
               "early_exit_rate", "avg_post_hold_peak_high_ret", "circuit_hit_rate"]
    per_year_sheet = [py_cols]
    for Y in sorted(rows_by_year):
        st = study.compute_efficacy(rows_by_year[Y])
        st["year"] = Y
        per_year_sheet.append([st.get(c) for c in py_cols])
    overall = study.compute_efficacy(study_rows)
    overall_sheet = [["metric", "value"]]
    for k in ["n_picks", "win_rate", "avg_ret", "median_ret", "big_winner_rate",
              "big_loser_rate", "avg_post_hold_high_ret", "early_exit_rate",
              "avg_post_hold_peak_high_ret", "circuit_hit_rate"]:
        overall_sheet.append([k, overall.get(k)])
    return per_year_sheet, overall_sheet


# ════════════════════════════════════════════════════════════════════════════
# 9. PARITY CHECKS + THRESHOLD REPORT
# ════════════════════════════════════════════════════════════════════════════

def parity_checks(study_ids: List[Any], ctx_rows: List[Dict[str, Any]],
                  study_rows: List[Dict[str, Any]]) -> List[str]:
    msgs: List[str] = []
    orig_ctx = [c for c in ctx_rows if c.get("is_extension") != 1]

    # (a) all original study ids present in context.
    sid = set(study_ids)
    cid = {c["id"] for c in orig_ctx}
    missing = sid - cid
    msgs.append(f"(a) original study ids in context: {len(cid & sid)}/{len(sid)}"
                + ("" if not missing else f"  MISSING {len(missing)} (sample {list(missing)[:5]})"))

    # (b) entry_gap_pct in [-10,10] for >95%.
    gaps = [c["entry_gap_pct"] for c in orig_ctx if c.get("entry_gap_pct") is not None]
    in_band = sum(1 for g in gaps if -10 <= g <= 10)
    pct = (in_band / len(gaps) * 100.0) if gaps else None
    msgs.append(f"(b) entry_gap_pct in [-10,10]: {in_band}/{len(gaps)}"
                + (f" = {pct:.2f}% ({'PASS' if (pct or 0) > 95 else 'REVIEW'})"
                   if pct is not None else " (no gaps)"))

    # (c) signal_day_ret_pct null count < 100 (true first-appearances only).
    nulls = sum(1 for c in orig_ctx if c.get("signal_day_ret_pct") is None)
    msgs.append(f"(c) signal_day_ret_pct NULLs: {nulls} ({'PASS' if nulls < 100 else 'REVIEW'} <100)")

    # (d) entry_date == signal_date + 1..5 calendar days for all rows.
    bad = 0
    for r in study_rows:
        try:
            sd = datetime.fromisoformat(r["signal_date"]).date()
            ed = datetime.fromisoformat(r["entry_date"]).date()
            if not (1 <= (ed - sd).days <= 5):
                bad += 1
        except Exception:
            bad += 1
    msgs.append(f"(d) entry_date within signal_date+1..5 cal days: "
                f"{len(study_rows) - bad}/{len(study_rows)} ({'PASS' if bad == 0 else f'{bad} OUTSIDE'})")
    return msgs


def threshold_report(s2: List[Dict[str, Any]]) -> List[str]:
    """From S2, find the signal_day_ret bucket where the edge (WR / avg net_ret)
    breaks down. Heuristic: scan buckets in ascending return order; the first
    'high run-up' bucket (lower bound >= 5) whose win_rate falls below the
    all-bucket baseline WR is reported as the pre-entry filter threshold."""
    lines: List[str] = []
    # baseline WR across all resolved rows (weighted by N).
    tot_n = sum((d.get("N") or 0) for d in s2)
    tot_win = 0.0
    for d in s2:
        if d.get("win_rate") is not None and d.get("N"):
            tot_win += d["win_rate"] / 100.0 * d["N"]
    baseline_wr = (tot_win / tot_n * 100.0) if tot_n else None
    lines.append(f"baseline WR (all signal_day_ret buckets, N={tot_n}): "
                 f"{baseline_wr:.1f}%" if baseline_wr is not None else "baseline WR: n/a")

    order = {e[0]: e[1] for e in SIGNAL_RET_BUCKETS}  # label -> lo bound
    # consider the high run-up buckets in ascending lower-bound order.
    high = [d for d in s2 if (order.get(d["signal_day_ret_bucket"]) or -999) >= 5
            and d.get("win_rate") is not None and (d.get("N") or 0) >= 20]
    high.sort(key=lambda d: order.get(d["signal_day_ret_bucket"]) or 0)
    breakdown = None
    for d in high:
        if baseline_wr is not None and d["win_rate"] < baseline_wr:
            breakdown = d
            break
    if breakdown is None and high:
        # none below baseline — report the lowest-WR high bucket as the soft edge.
        breakdown = min(high, key=lambda d: d["win_rate"])
    if breakdown:
        lo = order.get(breakdown["signal_day_ret_bucket"])
        lines.append(
            f"PRE-ENTRY FILTER: entering next morning after the stock ran "
            f">{lo:.0f}% on the signal day (bucket '{breakdown['signal_day_ret_bucket']}') "
            f"drops WR to {breakdown['win_rate']:.1f}% / avg net "
            f"{(breakdown.get('avg_net_ret') or 0):.2f}% (N={breakdown.get('N')}). "
            f"That is the edge-breakdown threshold."
        )
    else:
        lines.append("PRE-ENTRY FILTER: no high run-up bucket had N>=20 — "
                     "insufficient sample to call a threshold.")
    return lines


# ════════════════════════════════════════════════════════════════════════════
# 10. ORCHESTRATION
# ════════════════════════════════════════════════════════════════════════════

def run(rnd_db: str, prod_db: str, out_dir: Path, top_n: int, dry_run: bool) -> int:
    print(f"[context] RND DB : {rnd_db}")
    print(f"[context] PROD DB: {prod_db}")
    print(f"[context] mode   : {'DRY-RUN (no writes)' if dry_run else 'APPLY'}")

    # ── 1. Load canonical study rows (with id). ──
    study_rows, study_cols = load_study_rows(rnd_db)
    if not study_rows:
        print("[context] ERROR: no falcon_signal_day_study rows for persona "
              f"'{STUDY_PERSONA_TAG}'. Run build_signal_day_study.py first.")
        return 2
    for r in study_rows:
        r["is_extension"] = 0
    study_ids = [r["id"] for r in study_rows]
    study_max_id = max(study_ids)
    study_max_sd = max(r["signal_date"] for r in study_rows)
    print(f"[context] loaded {len(study_rows)} study rows "
          f"(id {min(study_ids)}..{study_max_id}, signal_date max {study_max_sd})")

    # ── 2. Extension rows (recent signal days post-dating the study). ──
    ext_rows, ext_note = build_extension_rows(rnd_db, prod_db, study_max_sd, top_n)
    # assign synthetic ids beyond the study max so the context join key is unique.
    next_id = study_max_id
    for r in ext_rows:
        next_id += 1
        r["id"] = next_id
        r["is_extension"] = 1
    print(f"[context] extension: {ext_note}")
    if ext_rows:
        ext_sds = sorted({r["signal_date"] for r in ext_rows})
        print(f"[context]   extension signal_dates: {ext_sds}  ({len(ext_rows)} rows)")

    # ── 3. OHLCV (with volume) for every symbol we need. ──
    needed_syms = {r["symbol"] for r in study_rows} | {r["symbol"] for r in ext_rows}
    print(f"[context] loading PROD OHLCV for {len(needed_syms)} symbols...")
    ohlcv = load_ohlcv(prod_db, "2015-01-01", "2099-12-31")
    didx_cache: Dict[str, Dict[str, int]] = {}

    def _didx(sym: str):
        if sym not in didx_cache:
            didx_cache[sym] = _date_index(ohlcv.get(sym, []))
        return didx_cache.get(sym)

    # ── 4. Compute context for every row (study + extension). ──
    ctx_rows: List[Dict[str, Any]] = []
    all_rows = [(r, 0) for r in study_rows] + [(r, 1) for r in ext_rows]
    for r, is_ext in all_rows:
        sym = r["symbol"]
        c = compute_context(
            study_id=r["id"], signal_date=r["signal_date"], symbol=sym,
            entry_price=r.get("entry_price"),
            bars_sym=ohlcv.get(sym), didx=_didx(sym), is_extension=is_ext,
        )
        ctx_rows.append(c)
    ctx_by_id = {c["id"]: c for c in ctx_rows}
    print(f"[context] computed {len(ctx_rows)} context rows "
          f"({len(study_rows)} study + {len(ext_rows)} extension)")

    # entry_context distribution.
    ec_dist: Dict[str, int] = defaultdict(int)
    for c in ctx_rows:
        if c.get("is_extension") != 1:
            ec_dist[c.get("entry_context") or "NORMAL"] += 1
    print("[context] entry_context distribution (resolved+unresolved study rows):")
    for k in sorted(ec_dist, key=lambda x: -ec_dist[x]):
        print(f"            {k:>22}: {ec_dist[k]}")

    # ── 5. Analysis (RESOLVED, non-extension only). ──
    joined: List[Dict[str, Any]] = []
    for r in study_rows:
        if r.get("net_ret_pct") is None:
            continue   # unresolved edge rows excluded from outcome analysis
        merged = dict(r)
        merged.update({k: v for k, v in ctx_by_id[r["id"]].items()
                       if k not in ("id", "signal_date", "symbol", "is_extension")})
        joined.append(merged)
    print(f"[context] analysis cohort (resolved, non-extension): {len(joined)} rows")
    an = build_analysis(joined)

    # ── 6. Parity checks. ──
    print("\n[context] ── PARITY CHECKS ──")
    for m in parity_checks(study_ids, ctx_rows, study_rows):
        print(f"    {m}")
    print(f"    row counts: original={len(study_rows)}  "
          f"+extension={len(ext_rows)}  total={len(study_rows) + len(ext_rows)}")

    # ── 7. Report. ──
    print("\n[context] ── ENTRY-CONTEXT EFFICACY (S1, resolved) ──")
    s1_sorted = [d for d in an["S1"] if (d.get("N") or 0) >= 20
                 and d.get("win_rate") is not None]
    if s1_sorted:
        best = max(s1_sorted, key=lambda d: d["win_rate"])
        worst = min(s1_sorted, key=lambda d: d["win_rate"])
        print(f"    BEST  by WR : {best['entry_context']} "
              f"WR={best['win_rate']:.1f}% avg={best.get('avg_net_ret'):.2f}% (N={best['N']})")
        print(f"    WORST by WR : {worst['entry_context']} "
              f"WR={worst['win_rate']:.1f}% avg={worst.get('avg_net_ret'):.2f}% (N={worst['N']})")
    print("\n[context] ── THRESHOLD REPORT (S2) ──")
    for line in threshold_report(an["S2"]):
        print(f"    {line}")

    if dry_run:
        print("\n[context] DRY-RUN — nothing written (no DB table, no Excel).")
        return 0

    # ── 8. APPLY: write context table + both Excels. ──
    write_context_table(rnd_db, ctx_rows)
    print(f"\n[context] wrote table {CONTEXT_TABLE} ({len(ctx_rows)} rows) to RND DB.")

    per_year_sheet, overall_sheet = build_efficacy_sheets(study_rows)
    study_xlsx = write_study_with_context(
        out_dir, study_cols, study_rows, ext_rows, ctx_by_id,
        per_year_sheet, overall_sheet,
    )
    print(f"[context] wrote study+context workbook: {study_xlsx}")

    an_xlsx = write_analysis(out_dir, an)
    print(f"[context] wrote analysis workbook: {an_xlsx}")
    print("[context] APPLY complete.")
    return 0


# ════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════

def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Signal-day pre-entry CONTEXT layer for the per-signal-day "
                    "study: new RND table falcon_signal_day_context (keyed by "
                    "study id), regenerated study Excel with context columns, a "
                    "7-sheet analysis workbook, parity checks + threshold report."
    )
    p.add_argument("--rnd-db", default=None,
                   help="RND research DB (default: persona resolver).")
    p.add_argument("--prod-db", default=None,
                   help="PROD DB for OHLCV (default: config.POWER_DB_PATH).")
    p.add_argument("--out", default=str(_HERE / "out" / "v8"),
                   help="Output dir for the workbooks (default: ./out/v8).")
    p.add_argument("--top-n", type=int, default=10,
                   help="Picks per signal_date for extension rows (default 10).")
    p.add_argument("--dry-run", action="store_true",
                   help="Compute + print everything; write NOTHING.")
    args = p.parse_args(argv)

    rnd_db = args.rnd_db or _resolve_rnd_db_path()
    prod_db = args.prod_db or PROD_DB
    return run(rnd_db=rnd_db, prod_db=prod_db, out_dir=Path(args.out),
               top_n=args.top_n, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
