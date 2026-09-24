"""
Live Intraday Falcon Signal-Timing Research
===========================================

Question: at what time during the SIGNAL DAY could the EOD Falcon Top-10 have
been identified from live 1-minute data, so a user could enter same-day instead
of next morning?

Method (no lookahead): for each signal_date that has 1-min data, replay from 09:15.
At each checkpoint T, score EVERY stock by an intraday "Falcon-likeness" PROXY
computed only from 09:15->T data, rank the live Top-10, and measure overlap with
the stored EOD Falcon Top-10 (falcon_signal_day_study, persona falcon_top10_daily).
Then backtest entering the live Top-10 at T (next-candle open) with the SAME
trailing exit as the agents, vs the next-morning baseline.

Proxy features at T (z-scored cross-sectionally that minute):
  ret_T        intraday return 09:15->T
  close_loc_T  (price - running low)/(running high - running low)   [close-in-range]
  vol_ratio_T  cumvol@T / (ADV_shares * minutes_elapsed/375)        [volume surge pace]
  dist_high_T  price@T / prior-20d high - 1                          [breakout]
  range_T      (running high - running low)/open                    [range/vol so far]
score = z(ret)+z(close_loc)+z(vol_ratio)+z(dist_high)+z(range)   (equal weight, interpretable)
"""
from __future__ import annotations
import sqlite3
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import numpy as np
import pandas as pd

from intraday_gainers_research import load_day, DB     # reuse full-OHLCV 1-min loader

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs" / "Live_Signal_Timing.xlsx"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
WIN_S, WIN_E = "2024-05-14", "2026-06-25"
ALIAS = {"ZOMATO": "ETERNAL"}
CHECKPOINTS = ["09:16", "09:18", "09:20", "09:25", "09:30", "09:45", "10:00", "10:15",
               "10:30", "10:45", "11:00", "11:30", "12:00", "13:00", "14:00", "15:00", "15:28"]
TARGET, STOP, GIVEBACK = 1.0, -1.5, 0.75
CAPITAL, BASKET = 500_000.0, 10


def _z(a):
    a = np.asarray(a, float)
    m = np.nanmean(a); s = np.nanstd(a)
    return (a - m) / s if s > 0 else np.zeros_like(a)


def proxy_rank(dd, it, adv_sh, prior_hi):
    """Return list of (score, symbol) sorted desc, using only 09:15->it data."""
    rows = []
    elapsed = it + 1
    for sym, d in dd["syms"].items():
        c = d["close"][it]
        if not np.isfinite(c) or d["o0"] <= 0:
            continue
        rh, rl = d["runhigh"][it], d["runlow"][it]
        ret = (c / d["o0"] - 1) * 100
        cl = (c - rl) / (rh - rl) if rh > rl else 0.5
        adv = adv_sh.get(sym, 0.0)
        vr = d["cumvol"][it] / (adv * elapsed / 375.0) if adv > 0 else 0.0
        ph = prior_hi.get(sym, 0.0)
        dh = (c / ph - 1) * 100 if ph > 0 else 0.0
        rg = (rh - rl) / d["o0"] * 100
        rows.append((sym, ret, cl, vr, dh, rg))
    if not rows:
        return []
    syms = [r[0] for r in rows]
    arr = np.array([r[1:] for r in rows], float)
    score = _z(arr[:, 0]) + _z(arr[:, 1]) + _z(arr[:, 2]) + _z(arr[:, 3]) + _z(arr[:, 4])
    order = np.argsort(-score)
    return [(float(score[i]), syms[i]) for i in order]


def sim_trail(dd, picks, it):
    """Enter picks at open of it+1, equal capital, lock+1%/trail/stop/15:29. Returns ret% or None."""
    grid = dd["grid"]; n = len(grid); e = it + 1
    if e >= n or not picks:
        return None
    legs = []
    for s in picks:
        d = dd["syms"].get(s)
        if d is None:
            continue
        ep = d["open"][e]
        if not np.isfinite(ep) or ep <= 0:
            continue
        legs.append((s, ep))
    if not legs:
        return None
    alloc = CAPITAL / len(legs)
    qty = np.array([np.floor(alloc / ep) for _, ep in legs], float)
    keep = qty > 0
    legs = [l for l, k in zip(legs, keep) if k]; qty = qty[keep]
    if not legs:
        return None
    entry = np.array([ep for _, ep in legs]); deployed = float((qty * entry).sum())
    close = np.column_stack([dd["syms"][s]["close"][e:] for s, _ in legs])
    openx = np.column_stack([dd["syms"][s]["open"][e:] for s, _ in legs])
    port = close @ qty
    ret = (port - deployed) / deployed * 100
    nxt = np.empty_like(port); nxt[:-1] = (openx[1:] @ qty); nxt[-1] = port[-1]
    armed = False; peak = None
    for i in range(len(ret) - 1):
        r = ret[i]
        if r <= STOP:
            return (nxt[i] - deployed) / deployed * 100
        if not armed:
            if r >= TARGET:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        if r <= max(TARGET, peak - GIVEBACK):
            return (nxt[i] - deployed) / deployed * 100
    return (float(port[-1]) - deployed) / deployed * 100


def process_chunk(payload):
    days_chunk, eod10, adv_sh, prior_hi = payload
    con = sqlite3.connect(str(DB))
    overlaps = []      # (day, T, overlap, n_label)
    bt = []            # (day, T, port_ret)
    for day in days_chunk:
        label = eod10.get(day)
        if not label:
            continue
        dd = load_day(con, day)
        if dd is None:
            continue
        gi = dd["gi"]
        labset = set(label)
        for T in CHECKPOINTS:
            if T not in gi:
                continue
            it = gi[T]
            ranked = proxy_rank(dd, it, adv_sh, prior_hi.get(day, {}))
            if not ranked:
                continue
            live10 = [s for _, s in ranked[:10]]
            overlaps.append((day, T, len(set(live10) & labset), len(labset)))
            r = sim_trail(dd, live10, it)
            if r is not None:
                bt.append((day, T, r))
    con.close()
    return overlaps, bt


def main():
    con = sqlite3.connect(str(DB))
    # EOD label: top-10 per signal_date (map study symbol -> ohlc symbol via alias)
    sig = pd.read_sql_query(
        """SELECT signal_date, engine_rank, symbol FROM falcon_signal_day_study
           WHERE persona='falcon_top10_daily' AND engine_rank<=10
             AND signal_date BETWEEN ? AND ?""", con, params=(WIN_S, WIN_E))
    eod10 = {d: [ALIAS.get(s, s) for s in g.sort_values("engine_rank")["symbol"]]
             for d, g in sig.groupby("signal_date")}
    # ADV (shares) per symbol
    adv = pd.read_sql_query(
        "SELECT symbol, AVG(volume) v FROM ohlc_daily WHERE trade_date BETWEEN ? AND ? GROUP BY symbol",
        con, params=(WIN_S, WIN_E))
    adv_sh = dict(zip(adv["symbol"], adv["v"]))
    # prior-20d high per (date, symbol)
    od = pd.read_sql_query(
        "SELECT symbol, trade_date, high FROM ohlc_daily WHERE trade_date BETWEEN date(?, '-40 days') AND ?",
        con, params=(WIN_S, WIN_E))
    con.close()
    od = od.sort_values(["symbol", "trade_date"])
    od["p20"] = od.groupby("symbol")["high"].transform(lambda s: s.rolling(20, min_periods=5).max().shift(1))
    prior_hi = defaultdict(dict)
    for r in od.itertuples(index=False):
        if pd.notna(r.p20):
            prior_hi[r.trade_date][r.symbol] = float(r.p20)

    import argparse
    ap = argparse.ArgumentParser(); ap.add_argument("--days", type=int, default=0)
    a = ap.parse_args()
    days = sorted(d for d in eod10 if WIN_S <= d <= WIN_E)
    if a.days:
        days = days[:a.days]
    print(f"[*] signal days with EOD top-10: {len(days)}", flush=True)

    nw = 10
    chunks = [days[i::nw] for i in range(nw)]
    payloads = [(ch, eod10, adv_sh, dict(prior_hi)) for ch in chunks if ch]
    all_ov, all_bt = [], []
    with ProcessPoolExecutor(max_workers=nw) as ex:
        for ov, bt in ex.map(process_chunk, payloads):
            all_ov.extend(ov); all_bt.extend(bt)

    ov = pd.DataFrame(all_ov, columns=["date", "T", "overlap", "n_label"])
    bt = pd.DataFrame(all_bt, columns=["date", "T", "port_ret"])

    # Sheet 1: overlap by checkpoint
    s1 = ov.groupby("T").agg(days=("date", "size"), avg_overlap=("overlap", "mean"),
                             avg_n_label=("n_label", "mean")).reset_index()
    s1["avg_overlap_pct"] = (s1["avg_overlap"] / s1["avg_n_label"] * 100).round(1)
    s1["avg_overlap"] = s1["avg_overlap"].round(2)
    s1 = s1.sort_values("T")
    # order by clock
    s1 = s1.set_index("T").reindex([t for t in CHECKPOINTS if t in s1["T"].values
                                    or t in set(ov["T"])]).reset_index()

    # close-of-day ceiling (15:28)
    ceil = s1[s1["T"] == "15:28"]["avg_overlap_pct"].iloc[0] if "15:28" in set(s1["T"]) else None

    # Sheet 2: backtest by checkpoint (live Top-10 entry) + baseline reference
    s2 = bt.groupby("T").agg(days=("date", "size"), avg_return=("port_ret", "mean"),
                             win_rate=("port_ret", lambda x: (x > 0).mean() * 100)).reset_index()
    s2["avg_return"] = s2["avg_return"].round(3); s2["win_rate"] = s2["win_rate"].round(1)
    s2 = s2.set_index("T").reindex([t for t in CHECKPOINTS if t in set(bt["T"])]).reset_index()

    # best checkpoint = highest overlap among T <= 13:00 (need time to trade)
    cand = s1[s1["T"] <= "13:00"]
    best_T = cand.sort_values("avg_overlap_pct", ascending=False)["T"].iloc[0] if not cand.empty else None

    # Sheet 4: daily log for best checkpoint
    s4 = bt[bt["T"] == best_T].copy().sort_values("date") if best_T else pd.DataFrame()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        s1.to_excel(xl, "1_Overlap_by_Checkpoint", index=False)
        s2.to_excel(xl, "2_Backtest_by_Checkpoint", index=False)
        pd.DataFrame([{"best_checkpoint(<=13:00)": best_T,
                       "overlap_at_best_%": s1[s1.T == best_T]["avg_overlap_pct"].iloc[0] if best_T else None,
                       "close_ceiling_15:28_%": ceil,
                       "live_entry_avg_return@best": s2[s2.T == best_T]["avg_return"].iloc[0] if best_T and best_T in set(bt["T"]) else None,
                       "live_entry_win_rate@best": s2[s2.T == best_T]["win_rate"].iloc[0] if best_T and best_T in set(bt["T"]) else None,
                       "baseline_next_morning": "Trailing agent ~75% WR, ~+1.0%/day (enter EOD Top-10 next 09:15)"}]
                     ).to_excel(xl, "3_Best_vs_Baseline", index=False)
        s4.to_excel(xl, "4_Best_Checkpoint_DailyLog", index=False)

    pd.set_option("display.width", 160)
    print("\n=== OVERLAP BY CHECKPOINT (avg of EOD Top-10 already identifiable) ===")
    print(s1.to_string(index=False))
    print("\n=== BACKTEST: live Top-10 entry at checkpoint (trailing exit) ===")
    print(s2.to_string(index=False))
    print(f"\n[*] best checkpoint (<=13:00): {best_T}  close-ceiling 15:28: {ceil}%")
    print(f"[*] wrote {OUT}")
    if DESK.exists():
        import shutil; shutil.copy(OUT, DESK / OUT.name)


if __name__ == "__main__":
    main()
