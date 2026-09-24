"""
Falcon Intraday Portfolio Backtest
==================================

Simulates buying the Falcon Top-10 (ranked EOD signal list) at 09:15 the NEXT
trading day, tracking combined portfolio P&L minute-by-minute, and exiting the
ENTIRE basket the moment portfolio return >= +1.0% (executed at the OPEN of the
following 1-min candle) or at the 15:29 CLOSE if the target is never reached.
No overnight positions, ever.

Data sources (RND DB, universe_engine/data/db/kanida_universe.db):
  - ohlc_1min               : 1-minute OHLCV (symbol, bar_time, open..volume)
  - falcon_signal_day_study : ranked Top-10 history (persona falcon_top10_daily,
                              signal_date, entry_date, engine_rank, symbol)

Everything below the CONFIG dataclass is parameterised: date range, baskets,
target %, capital, persona, symbol aliases. Re-run with different inputs without
touching the logic.

Usage:
    python scripts/falcon_intraday_backtest.py
    python scripts/falcon_intraday_backtest.py --capital 3000000 --target 1.0 \
        --start 2024-05-14 --end 2026-06-15
"""
from __future__ import annotations

import argparse
import math
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
OUT_XLSX = ROOT / "outputs" / "Falcon_Intraday_Backtest_Results.xlsx"

SESSION_OPEN = "09:15:00"
SESSION_CLOSE = "15:29:00"

# Basket label -> set of engine ranks to include. Order in the output respects rank.
DEFAULT_BASKETS: dict[str, list[int]] = {
    "Top 3":          [1, 2, 3],
    "Top 5":          [1, 2, 3, 4, 5],
    "Top 7":          [1, 2, 3, 4, 5, 6, 7],
    "Top 10":         [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "Ranks 1-3 only": [1, 2, 3],
    "Ranks 1-5 only": [1, 2, 3, 4, 5],
    "Ranks 3-7":      [3, 4, 5, 6, 7],
    "Ranks 6-10":     [6, 7, 8, 9, 10],
    "Ranks 4-10":     [4, 5, 6, 7, 8, 9, 10],
}

# Symbol renames: study symbol -> symbol as stored in ohlc_1min.
DEFAULT_ALIASES: dict[str, str] = {"ZOMATO": "ETERNAL"}

# Time-of-day buckets (label, start_inclusive, end_exclusive) for hit analysis.
TOD_BUCKETS = [
    ("09:15-09:45", "09:15", "09:45"),
    ("09:45-10:15", "09:45", "10:15"),
    ("10:15-11:00", "10:15", "11:00"),
    ("11:00-12:00", "11:00", "12:00"),
    ("12:00-13:00", "12:00", "13:00"),
    ("13:00-14:00", "13:00", "14:00"),
    ("14:00-15:00", "14:00", "15:00"),
    ("15:00-15:29", "15:00", "15:30"),
]


@dataclass
class Config:
    db_path: Path = DEFAULT_DB
    persona: str = "falcon_top10_daily"
    start: str | None = None          # entry_date >= start (auto = first usable)
    end: str | None = None            # entry_date <= end   (auto = last usable)
    capital: float = 500_000.0
    target_pct: float = 1.0
    baskets: dict[str, list[int]] = field(default_factory=lambda: dict(DEFAULT_BASKETS))
    aliases: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_ALIASES))


# --------------------------------------------------------------------------- #
# Loaders
# --------------------------------------------------------------------------- #
def load_signals(con: sqlite3.Connection, cfg: Config) -> dict[str, list[tuple[int, str]]]:
    """entry_date -> [(rank, study_symbol), ...] for ranks 1..10, rank-ordered."""
    sql = """
        SELECT entry_date, engine_rank, symbol
        FROM falcon_signal_day_study
        WHERE persona = ? AND engine_rank BETWEEN 1 AND 10
        ORDER BY entry_date, engine_rank
    """
    out: dict[str, list[tuple[int, str]]] = {}
    for ed, rk, sym in con.execute(sql, (cfg.persona,)):
        out.setdefault(ed, []).append((int(rk), sym))
    return out


def load_ohlc_1min_day(con: sqlite3.Connection, day: str,
                       study_symbols: list[str], aliases: dict[str, str]
                       ) -> dict[str, pd.DataFrame]:
    """
    Load 1-min bars for `day` for the given study symbols.

    Returns study_symbol -> DataFrame indexed by 'HH:MM' with columns open/close,
    reindexed onto the day's master minute grid and forward-filled on close so the
    portfolio mark is always defined. Symbols with no 09:15 bar are omitted (caller
    drops + reallocates).
    """
    fetch_syms = {aliases.get(s, s): s for s in study_symbols}   # ohlc symbol -> study symbol
    if not fetch_syms:
        return {}
    ph = ",".join("?" * len(fetch_syms))
    rows = con.execute(
        f"""SELECT symbol, substr(bar_time,12,5) AS hm, open, close
            FROM ohlc_1min
            WHERE bar_time BETWEEN ? AND ? AND symbol IN ({ph})""",
        [f"{day} {SESSION_OPEN}", f"{day} {SESSION_CLOSE}", *fetch_syms.keys()],
    ).fetchall()
    if not rows:
        return {}

    raw = pd.DataFrame(rows, columns=["ohlc_sym", "hm", "open", "close"])
    # Master minute grid = every minute that trades anywhere in this basket-day.
    grid = sorted(raw["hm"].unique())
    out: dict[str, pd.DataFrame] = {}
    for ohlc_sym, g in raw.groupby("ohlc_sym"):
        study_sym = fetch_syms[ohlc_sym]
        g = g.set_index("hm").sort_index()
        g = g[~g.index.duplicated(keep="first")]
        df = g.reindex(grid)
        df["close"] = df["close"].ffill()       # carry last trade forward
        # open used only for the next-candle exit; leave NaN where no bar traded
        out[study_sym] = df[["open", "close"]]
    return out


# --------------------------------------------------------------------------- #
# Core simulation
# --------------------------------------------------------------------------- #
def simulate_day(basket_picks: list[tuple[int, str]],
                 day_ohlc: dict[str, pd.DataFrame],
                 capital: float, target_pct: float) -> dict | None:
    """
    Simulate one basket on one entry day.

    basket_picks : [(rank, study_symbol), ...] rank-ordered (subset of the day's 10)
    day_ohlc     : study_symbol -> DataFrame(index='HH:MM', cols open/close), ffill'd

    Returns a result dict, or None if no pick had tradeable data that day.
    """
    # --- Entry: keep only picks with a real 09:15 OPEN; equal-allocate over them.
    present = []
    for rk, sym in basket_picks:
        df = day_ohlc.get(sym)
        if df is None or "09:15" not in df.index:
            continue
        entry_open = df.at["09:15", "open"]
        if entry_open is None or not np.isfinite(entry_open) or entry_open <= 0:
            continue
        present.append((rk, sym, float(entry_open)))
    if not present:
        return None

    alloc = capital / len(present)
    legs = []          # per-stock leg dicts
    for rk, sym, entry_open in present:
        qty = int(math.floor(alloc / entry_open))
        if qty <= 0:
            continue
        legs.append({"rank": rk, "symbol": sym, "entry_price": entry_open, "qty": qty})
    if not legs:
        return None

    syms = [l["symbol"] for l in legs]
    qty = np.array([l["qty"] for l in legs], dtype=float)
    entry_px = np.array([l["entry_price"] for l in legs], dtype=float)
    deployed = float((qty * entry_px).sum())

    # --- Build aligned close matrix over the master minute grid (from 09:15).
    grid = list(day_ohlc[syms[0]].index)
    for s in syms[1:]:
        if len(day_ohlc[s].index) > len(grid):
            grid = list(day_ohlc[s].index)
    grid = [m for m in grid if m >= "09:15"]
    close_mat = np.column_stack([
        day_ohlc[s]["close"].reindex(grid).ffill().bfill().to_numpy(dtype=float)
        for s in syms
    ])                                              # shape (minutes, stocks)
    port_val = close_mat @ qty                       # portfolio value per minute
    port_ret = (port_val - deployed) / deployed * 100.0

    # --- Trigger detection on candle CLOSE; execute at NEXT candle OPEN.
    hit_idx = None
    for i in range(len(grid) - 1):                   # need a next candle to execute
        if port_ret[i] >= target_pct:
            hit_idx = i
            break

    if hit_idx is not None:
        hit_time = grid[hit_idx]
        nxt = grid[hit_idx + 1]
        exit_px = np.array([
            _exit_price(day_ohlc[s], nxt) for s in syms
        ], dtype=float)
        exit_reason = "TARGET_HIT"
        exit_time = hit_time
        trigger_mark_return = float(port_ret[hit_idx])
    else:
        exit_time = grid[-1]                          # 15:29 (or last available)
        exit_px = np.array([
            float(day_ohlc[s]["close"].reindex(grid).ffill().bfill().iloc[-1])
            for s in syms
        ], dtype=float)
        exit_reason = "CLOSE_EXIT"
        trigger_mark_return = float(port_ret[-1])

    exit_val = float((qty * exit_px).sum())
    realized_return = (exit_val - deployed) / deployed * 100.0

    for k, l in enumerate(legs):
        l["exit_price"] = float(exit_px[k])
        l["stock_return_pct"] = (exit_px[k] - entry_px[k]) / entry_px[k] * 100.0

    return {
        "legs": legs,
        "symbols": syms,
        "entry_prices": entry_px.tolist(),
        "exit_prices": exit_px.tolist(),
        "exit_time": exit_time,
        "exit_reason": exit_reason,
        "deployed": deployed,
        "portfolio_return_pct": realized_return,
        "trigger_mark_return_pct": trigger_mark_return,
        "hit_1pct": exit_reason == "TARGET_HIT",
        "n_stocks": len(legs),
    }


def _exit_price(df: pd.DataFrame, minute: str) -> float:
    """Executable exit = OPEN of the next candle; fall back to its ffill'd close."""
    o = df.at[minute, "open"] if minute in df.index else None
    if o is not None and np.isfinite(o) and o > 0:
        return float(o)
    c = df["close"].reindex([minute]).ffill()
    val = c.iloc[0] if len(c) else np.nan
    if not np.isfinite(val):                          # last resort: last known close
        val = df["close"].ffill().iloc[-1]
    return float(val)


# --------------------------------------------------------------------------- #
# Backtest driver
# --------------------------------------------------------------------------- #
def run_backtest(cfg: Config):
    con = sqlite3.connect(str(cfg.db_path))
    signals = load_signals(con, cfg)

    # Usable entry days = signal days whose entry_date has 1-min data.
    min_1m, max_1m = con.execute(
        "SELECT min(substr(bar_time,1,10)), max(substr(bar_time,1,10)) FROM ohlc_1min"
    ).fetchone()
    start = cfg.start or min_1m
    end = cfg.end or max_1m
    entry_days = sorted(d for d in signals if start <= d <= end)

    trade_rows: list[dict] = []          # Output 2 (one row per basket per day)
    per_stock_rows: list[dict] = []      # backing for Output 5
    skipped: list[tuple[str, str]] = []  # (basket, day) with no tradeable data

    for day in entry_days:
        picks_all = signals[day]                       # ranks 1..10 that day
        study_syms = [s for _, s in picks_all]
        day_ohlc = load_ohlc_1min_day(con, day, study_syms, cfg.aliases)

        for label, ranks in cfg.baskets.items():
            rankset = set(ranks)
            basket_picks = [(rk, sym) for rk, sym in picks_all if rk in rankset]
            if not basket_picks:
                continue
            res = simulate_day(basket_picks, day_ohlc, cfg.capital, cfg.target_pct)
            if res is None:
                skipped.append((label, day))
                continue

            trade_rows.append({
                "entry_date": day,
                "basket_label": label,
                "stocks": ", ".join(res["symbols"]),
                "entry_prices": ", ".join(f"{p:.2f}" for p in res["entry_prices"]),
                "exit_prices": ", ".join(f"{p:.2f}" for p in res["exit_prices"]),
                "exit_time": res["exit_time"][:5],
                "exit_reason": res["exit_reason"],
                "portfolio_return_pct": round(res["portfolio_return_pct"], 4),
                "hit_1pct": res["hit_1pct"],
                "trigger_mark_return_pct": round(res["trigger_mark_return_pct"], 4),
                "n_stocks": res["n_stocks"],
                "deployed": round(res["deployed"], 2),
            })
            if label == "Top 10":
                for leg in res["legs"]:
                    per_stock_rows.append({
                        "entry_date": day,
                        "rank": leg["rank"],
                        "symbol": leg["symbol"],
                        "stock_return_pct": leg["stock_return_pct"],
                        "day_hit": res["hit_1pct"],
                    })

    con.close()
    trades = pd.DataFrame(trade_rows)
    per_stock = pd.DataFrame(per_stock_rows)
    trades, per_stock, dropped_days = align_common_days(trades, per_stock, cfg.baskets)
    return cfg, trades, per_stock, entry_days, skipped, dropped_days


def align_common_days(trades: pd.DataFrame, per_stock: pd.DataFrame, baskets: dict):
    """
    Enforce parity #1: every basket must cover the IDENTICAL set of trading days.
    Keep only entry_dates that produced a valid simulation in ALL baskets
    (intersection). Returns (trades, per_stock, dropped_days).
    """
    if trades.empty:
        return trades, per_stock, []
    n_baskets = len(baskets)
    per_day = trades.groupby("entry_date")["basket_label"].nunique()
    common = set(per_day[per_day == n_baskets].index)
    all_days = set(trades["entry_date"])
    dropped = sorted(all_days - common)
    trades = trades[trades["entry_date"].isin(common)].reset_index(drop=True)
    if not per_stock.empty:
        per_stock = per_stock[per_stock["entry_date"].isin(common)].reset_index(drop=True)
    return trades, per_stock, dropped


# --------------------------------------------------------------------------- #
# Output builders
# --------------------------------------------------------------------------- #
def _hit_minute_to_bucket(hhmm: str) -> str | None:
    for label, lo, hi in TOD_BUCKETS:
        if lo <= hhmm < hi:
            return label
    return None


def compute_summary(trades: pd.DataFrame, baskets: dict) -> pd.DataFrame:
    rows = []
    for label in baskets:
        d = trades[trades["basket_label"] == label]
        if d.empty:
            continue
        hits = d[d["hit_1pct"]]
        no_hit = d[~d["hit_1pct"]]
        pos_no_hit = no_hit[no_hit["portfolio_return_pct"] > 0]
        neg = no_hit[no_hit["portfolio_return_pct"] <= 0]
        avg_hit_time = (
            pd.to_datetime(hits["exit_time"], format="%H:%M").dt.strftime("%H:%M")
            if not hits.empty else pd.Series(dtype=str)
        )
        avg_hit_str = (
            pd.to_datetime(hits["exit_time"], format="%H:%M").mean().strftime("%H:%M")
            if not hits.empty else "—"
        )
        rows.append({
            "Basket": label,
            "Trading days": len(d),
            "Days hit +1%": len(hits),
            "Hit rate %": round(len(hits) / len(d) * 100, 1),
            "Avg hit time": avg_hit_str,
            "Days positive (no hit)": len(pos_no_hit),
            "Days negative": len(neg),
            "Avg return": round(d["portfolio_return_pct"].mean(), 3),
            "Median return": round(d["portfolio_return_pct"].median(), 3),
            "Best day": round(d["portfolio_return_pct"].max(), 3),
            "Worst day": round(d["portfolio_return_pct"].min(), 3),
        })
    return pd.DataFrame(rows)


def compute_time_of_day(trades: pd.DataFrame, baskets: dict) -> pd.DataFrame:
    rows = []
    for label in baskets:
        hits = trades[(trades["basket_label"] == label) & (trades["hit_1pct"])]
        rec = {"Basket": label, "Total hits": len(hits)}
        counts = {b[0]: 0 for b in TOD_BUCKETS}
        for t in hits["exit_time"]:
            b = _hit_minute_to_bucket(t)
            if b:
                counts[b] += 1
        rec.update(counts)
        rows.append(rec)
    return pd.DataFrame(rows)


def compute_year_breakdown(trades: pd.DataFrame, baskets: dict) -> pd.DataFrame:
    rows = []
    t = trades.copy()
    t["year"] = t["entry_date"].str[:4]
    for label in baskets:
        d = t[t["basket_label"] == label]
        for yr, g in d.groupby("year"):
            rows.append({
                "Basket": label,
                "Year": yr,
                "Days": len(g),
                "Hit rate %": round(g["hit_1pct"].mean() * 100, 1),
                "Avg return": round(g["portfolio_return_pct"].mean(), 3),
            })
    return pd.DataFrame(rows)


def compute_rank_contribution(per_stock: pd.DataFrame) -> pd.DataFrame:
    """Output 5 — Top-10 only: avg individual stock return by rank, hit vs miss days,
    plus how often each rank itself crossed +1% on hit days."""
    rows = []
    if per_stock.empty:
        return pd.DataFrame(rows)
    for rk in range(1, 11):
        r = per_stock[per_stock["rank"] == rk]
        hit = r[r["day_hit"]]
        miss = r[~r["day_hit"]]
        rows.append({
            "Rank": rk,
            "Appearances": len(r),
            "Avg return (hit days) %": round(hit["stock_return_pct"].mean(), 3) if not hit.empty else None,
            "Avg return (miss days) %": round(miss["stock_return_pct"].mean(), 3) if not miss.empty else None,
            "Avg return (all days) %": round(r["stock_return_pct"].mean(), 3),
            "Own >=+1% on hit days %": round((hit["stock_return_pct"] >= 1.0).mean() * 100, 1) if not hit.empty else None,
        })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Parity checks
# --------------------------------------------------------------------------- #
def parity_checks(cfg: Config, trades: pd.DataFrame, entry_days: list[str]) -> list[tuple[str, bool, str]]:
    checks = []

    # 1) Total trading days identical across baskets (same date range).
    per_basket_days = trades.groupby("basket_label")["entry_date"].nunique()
    uniq = set(per_basket_days.values)
    checks.append((
        "All baskets share the same trading-day count",
        len(uniq) == 1,
        f"day counts per basket = {dict(per_basket_days)}",
    ))

    # 2) TARGET_HIT trigger-mark must be >= target (the true invariant).
    th = trades[trades["exit_reason"] == "TARGET_HIT"]
    bad_trigger = th[th["trigger_mark_return_pct"] < cfg.target_pct - 1e-9]
    checks.append((
        "Every TARGET_HIT fired on a trigger-mark >= +1.0%",
        bad_trigger.empty,
        f"{len(bad_trigger)} violations",
    ))

    # 2b) Diagnostic: TARGET_HIT realized return below +1% (execution-lag cost).
    lag = th[th["portfolio_return_pct"] < cfg.target_pct - 1e-9]
    checks.append((
        "[diagnostic] TARGET_HIT realized < +1% due to next-open execution lag",
        True,
        f"{len(lag)} of {len(th)} hit-days ({(len(lag)/len(th)*100 if len(th) else 0):.1f}%) "
        f"slipped below +1% at the fill (expected, not a bug)",
    ))

    # 3) No overnight positions: exit_time always <= 15:29.
    bad_time = trades[trades["exit_time"] > "15:29"]
    checks.append((
        "No exit_time after 15:29 (no overnight positions)",
        bad_time.empty,
        f"{len(bad_time)} violations",
    ))

    # 4) CLOSE_EXIT rows must NOT be flagged hit; TARGET rows must be flagged hit.
    mis = trades[(trades["exit_reason"] == "TARGET_HIT") != trades["hit_1pct"]]
    checks.append((
        "exit_reason and hit_1pct are consistent",
        mis.empty,
        f"{len(mis)} mismatches",
    ))
    return checks


def spot_check_entry_prices(cfg: Config, trades: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Parity: entry price for a stock == its 09:15 OPEN in ohlc_1min. Spot-check n rows."""
    con = sqlite3.connect(str(cfg.db_path))
    top10 = trades[trades["basket_label"] == "Top 10"]
    sample = top10.sample(min(n, len(top10)), random_state=7)
    rows = []
    for _, r in sample.iterrows():
        syms = [s.strip() for s in r["stocks"].split(",")]
        pxs = [float(x) for x in r["entry_prices"].split(",")]
        k = 0                                            # first stock in the row
        sym, px = syms[k], pxs[k]
        ohlc_sym = cfg.aliases.get(sym, sym)
        db_open = con.execute(
            "SELECT open FROM ohlc_1min WHERE symbol=? AND bar_time=?",
            (ohlc_sym, f"{r['entry_date']} {SESSION_OPEN}"),
        ).fetchone()
        db_open = float(db_open[0]) if db_open else None
        rows.append({
            "entry_date": r["entry_date"], "symbol": sym,
            "backtest_entry": round(px, 2),
            "db_0915_open": round(db_open, 2) if db_open is not None else None,
            "match": (db_open is not None and abs(db_open - px) < 1e-6),
        })
    con.close()
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Excel writer
# --------------------------------------------------------------------------- #
def write_outputs(path: Path, summary, trades, tod, year, rank_contrib,
                  parity, spotcheck, meta: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        summary.to_excel(xl, sheet_name="1_Summary", index=False)
        trades.to_excel(xl, sheet_name="2_Daily_Trade_Log", index=False)
        tod.to_excel(xl, sheet_name="3_Time_Of_Day", index=False)
        year.to_excel(xl, sheet_name="4_Year_Breakdown", index=False)
        rank_contrib.to_excel(xl, sheet_name="5_Rank_Contribution", index=False)

        pc = pd.DataFrame(
            [{"check": c, "pass": ("PASS" if ok else "FAIL"), "detail": d} for c, ok, d in parity]
        )
        pc.to_excel(xl, sheet_name="6_Parity_Checks", index=False)
        spotcheck.to_excel(xl, sheet_name="6b_EntryPrice_SpotCheck", index=False)

        pd.DataFrame([{"key": k, "value": str(v)} for k, v in meta.items()]).to_excel(
            xl, sheet_name="0_Run_Info", index=False)
    return path


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description="Falcon intraday portfolio backtest")
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--persona", default="falcon_top10_daily")
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--capital", type=float, default=500_000.0)
    ap.add_argument("--target", type=float, default=1.0)
    ap.add_argument("--out", default=str(OUT_XLSX))
    args = ap.parse_args()

    cfg = Config(db_path=Path(args.db), persona=args.persona, start=args.start,
                 end=args.end, capital=args.capital, target_pct=args.target)

    print(f"[*] DB        : {cfg.db_path}")
    print(f"[*] persona   : {cfg.persona}")
    print(f"[*] capital   : Rs {cfg.capital:,.0f}   target: +{cfg.target_pct}%")
    print(f"[*] baskets   : {len(cfg.baskets)}")
    print("[*] running backtest ...", flush=True)

    cfg, trades, per_stock, entry_days, skipped, dropped_days = run_backtest(cfg)
    common_days = trades["entry_date"].nunique()
    print(f"[*] entry days simulated : {len(entry_days)}  "
          f"({entry_days[0]} .. {entry_days[-1]})")
    print(f"[*] common trading days  : {common_days} (identical across all baskets)")
    print(f"[*] dropped for parity   : {len(dropped_days)} day(s) "
          f"(>=1 basket had no tradeable pick) -> {dropped_days}")
    print(f"[*] trade rows           : {len(trades)}   skipped basket-days: {len(skipped)}")

    summary = compute_summary(trades, cfg.baskets)
    tod = compute_time_of_day(trades, cfg.baskets)
    year = compute_year_breakdown(trades, cfg.baskets)
    rank_contrib = compute_rank_contribution(per_stock)
    parity = parity_checks(cfg, trades, entry_days)
    spotcheck = spot_check_entry_prices(cfg, trades, n=10)

    print("\n=== PARITY CHECKS ===")
    for c, ok, d in parity:
        print(f"  [{'PASS' if ok else 'FAIL'}] {c}  ::  {d}")
    print("\n=== ENTRY-PRICE SPOT CHECK (10 rows) ===")
    print(spotcheck.to_string(index=False))
    print(f"  all match: {bool(spotcheck['match'].all())}")

    print("\n=== SUMMARY ===")
    print(summary.to_string(index=False))

    meta = {
        "db_path": cfg.db_path, "persona": cfg.persona, "capital": cfg.capital,
        "target_pct": cfg.target_pct, "window_start": entry_days[0],
        "window_end": entry_days[-1], "common_trading_days": common_days,
        "dropped_for_parity": ", ".join(dropped_days) if dropped_days else "none",
        "aliases": cfg.aliases, "missing_handling": "drop & equal-reallocate",
        "target_marked_on": "1-min candle CLOSE; exit at next-candle OPEN",
    }
    out = write_outputs(Path(args.out), summary, trades, tod, year, rank_contrib,
                        parity, spotcheck, meta)
    print(f"\n[*] wrote {out}")


if __name__ == "__main__":
    main()
