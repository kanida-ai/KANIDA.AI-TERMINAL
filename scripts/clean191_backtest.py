"""
clean191_backtest.py  — RESEARCH BACKTEST (read-only)
=====================================================

Question
--------
How much of Falcon's edge survives once the look-ahead-leaked patterns are
removed?  Falcon's 4 weekly features (weekly_close_loc, weekly_range_pct,
weekly_close_vs_sma20, weekly_breakout_20w) are LOOK-AHEAD: falcon_features
stamps the full (Friday-inclusive) week's bar onto every weekday, so a Monday
row already "knows" Friday's close.  674 of 865 promoted patterns reference >=1
weekly feature (leaked); 191 are pure-daily (clean, point-in-time correct).

This script rebuilds the daily signal using ONLY the 191 clean patterns and runs
two strategies over 2026, month-by-month:
  - BTST Oscillator (1x CNC, daily bars, 2-session hold)
  - Magnifier (intraday MIS, real 1-min trail)

READ-ONLY: both DBs are opened mode=ro.  No writes anywhere.  This is a
backtest of a HYPOTHETICAL clean-only signal, NOT the live system.

Run:
    C:/Users/SPS/anaconda3/python.exe scripts/clean191_backtest.py
"""
from __future__ import annotations
import sqlite3, json, sys
from collections import defaultdict
import numpy as np

# ---------------------------------------------------------------- paths
ROOT = __import__("pathlib").Path(__file__).resolve().parent.parent
SERVING_DB = ROOT / "data" / "db" / "kanida_universe.db"          # patterns, daily features, daily OHLC
MIN_DB     = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"  # 1-min bars

WEEKLY = {"weekly_close_loc", "weekly_range_pct", "weekly_close_vs_sma20", "weekly_breakout_20w"}

# Feature column order — MUST match falcon_features schema / live signal_runner.py
FEATURE_COLS = [
    "range_pct","close_loc","gap_pct","body_pct","upper_wick_pct","lower_wick_pct",
    "dist_sma_20","dist_sma_50","dist_sma_200","slope_sma_20","slope_sma_50",
    "rsi_14","roc_5","roc_20","roc_60",
    "vol_vs_20d","vol_5d_vs_20d","n_sub_75v_7d","n_sub_75v_20d",
    "atr_20_pct","atr_5_vs_20","n_sub_2_5_range_7d","n_sub_3_range_7d",
    "n_higher_lows_5d","n_higher_highs_5d",
    "dist_high_10","dist_high_20","dist_high_60","dist_high_120","dist_high_252",
    "weekly_close_vs_sma20","weekly_breakout_20w","weekly_range_pct","weekly_close_loc",
    "rs_sector_20d","rs_sector_60d","rs_market_20d","rs_market_60d",
]
FEATURE_IDX = {c: i for i, c in enumerate(FEATURE_COLS)}

TOP_N = 15
DISASTER_STOP = -0.06          # BTST basket disaster stop at D+1 close
MIN_1MIN_CUTOFF = "2026-07-10" # last date with 1-min data

# Magnifier trail params (CAPITAL basis, all in percentage-points of basket P&L)
MAG_ARM      = 6.0   # arm at +6%
MAG_FLOOR    = 2.0   # once armed, never exit below +2%
MAG_GIVEBACK = 5.0   # trailing giveback 5pp from peak (once armed)
MAG_HARDSTOP = -3.0  # hard stop -3%
MAG_SQUAREOFF = "15:12:00"
MAG_ENTRY_TIME = "09:15:00"


def ro(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


# ---------------------------------------------------------------- Step 1: clean patterns
def load_clean_patterns(con):
    rows = con.execute("""
        SELECT c.pattern_id, c.rule_json, p.avg_oos_year_lift_pp
        FROM falcon_promoted_patterns p
        JOIN falcon_pattern_candidates c ON p.pattern_id = c.pattern_id
    """).fetchall()
    clean = []
    for pid, rj, lift in rows:
        rule = [(f, op, float(th)) for f, op, th in json.loads(rj)]
        feats = {f for f, _, _ in rule}
        if feats & WEEKLY:
            continue
        clean.append({"pattern_id": pid, "rule": rule, "oos_lift": float(lift)})
    return clean


# ---------------------------------------------------------------- Step 2: daily signal
def rule_mask(rule, X):
    mask = np.ones(X.shape[0], dtype=bool)
    for f, op, th in rule:
        idx = FEATURE_IDX.get(f)
        if idx is None:
            return np.zeros(X.shape[0], dtype=bool)
        col = X[:, idx]
        if op == "<=":
            mask &= (col <= th) & ~np.isnan(col)
        else:
            mask &= (col > th) & ~np.isnan(col)
    return mask


def build_signals(con, patterns):
    """Return dict[signal_date] -> list of top-15 dicts {symbol,score,n_fires}."""
    feat_select = ", ".join(FEATURE_COLS)
    rows = con.execute(f"""
        SELECT symbol, trade_date, {feat_select}
        FROM falcon_features
        WHERE trade_date >= '2026-01-01'
        ORDER BY trade_date
    """).fetchall()
    by_date = defaultdict(list)
    for r in rows:
        by_date[r[1]].append((r[0], r[2:]))

    signals = {}
    for d, recs in by_date.items():
        syms = [s for s, _ in recs]
        n = len(syms)
        X = np.full((n, len(FEATURE_COLS)), np.nan)
        for i, (_, fv) in enumerate(recs):
            X[i] = [np.nan if v is None else v for v in fv]
        fire = np.zeros(n, dtype=np.int32)
        score = np.zeros(n)
        for p in patterns:
            m = rule_mask(p["rule"], X)
            if not m.any():
                continue
            fire += m.astype(np.int32)
            score += m * p["oos_lift"]
        cands = [{"symbol": syms[i], "score": float(score[i]), "n_fires": int(fire[i])}
                 for i in range(n) if fire[i] >= 1]
        cands.sort(key=lambda c: -c["score"])
        signals[d] = cands[:TOP_N]
    return signals


# ---------------------------------------------------------------- daily OHLC
def load_daily(con):
    """dict[symbol][date] -> (open, close); plus sorted trading calendar."""
    rows = con.execute("""
        SELECT symbol, trade_date, open, close FROM ohlc_daily
        WHERE trade_date >= '2025-12-15'
    """).fetchall()
    px = defaultdict(dict)
    dates = set()
    for s, d, o, cl in rows:
        px[s][d] = (o, cl)
        dates.add(d)
    return px, sorted(dates)


# ---------------------------------------------------------------- Step 4: BTST Oscillator
def run_btst(signals, px, cal):
    cal_idx = {d: i for i, d in enumerate(cal)}
    results = []  # per basket: {entry_date, entry_month, ret, size, full15, names:[...]}
    for D, basket in sorted(signals.items()):
        if not basket:
            continue
        i = cal_idx.get(D)
        if i is None or i + 2 >= len(cal):
            continue
        D1, D2 = cal[i + 1], cal[i + 2]
        names = []
        for b in basket:
            s = b["symbol"]
            if D1 not in px[s] or D2 not in px[s]:
                continue
            o1 = px[s][D1][0]
            c1 = px[s][D1][1]
            c2 = px[s][D2][1]
            if not o1 or o1 <= 0:
                continue
            names.append({"symbol": s, "score": b["score"], "o1": o1, "c1": c1, "c2": c2,
                          "r_d1": c1 / o1 - 1.0, "r_hold": c2 / o1 - 1.0})
        if not names:
            continue
        mark_d1 = float(np.mean([nm["r_d1"] for nm in names]))
        if mark_d1 <= DISASTER_STOP:
            basket_ret = mark_d1
            stopped = True
        else:
            basket_ret = float(np.mean([nm["r_hold"] for nm in names]))
            stopped = False
        results.append({
            "entry_date": D1, "entry_month": D1[:7], "ret": basket_ret,
            "size": len(names), "full15": len(names) == TOP_N, "stopped": stopped,
            "names": names, "signal_date": D,
        })
    return results


# ---------------------------------------------------------------- Step 5: Magnifier
def run_magnifier(signals, cal, mindb):
    cal_idx = {d: i for i, d in enumerate(cal)}
    con = ro(mindb)
    results = []
    for D, basket in sorted(signals.items()):
        if not basket:
            continue
        i = cal_idx.get(D)
        if i is None or i + 1 >= len(cal):
            continue
        D1 = cal[i + 1]
        if D1 > MIN_1MIN_CUTOFF:
            continue
        syms = [b["symbol"] for b in basket]
        ph = "(" + ",".join("?" * len(syms)) + ")"
        rows = con.execute(f"""
            SELECT symbol, substr(bar_time,12,8) AS t, open, close
            FROM ohlc_1min
            WHERE bar_time >= ? AND bar_time <= ? AND symbol IN {ph}
            ORDER BY t
        """, [f"{D1} 00:00:00", f"{D1} 23:59:59"] + syms).fetchall()
        # per-symbol time->(open,close)
        sd = defaultdict(dict)
        times = set()
        for s, t, o, cl in rows:
            sd[s][t] = (o, cl)
            times.add(t)
        # entry: 09:15 open per name (skip name w/o 1-min that day)
        entry = {}
        for s in syms:
            if MAG_ENTRY_TIME in sd[s] and sd[s][MAG_ENTRY_TIME][0]:
                entry[s] = sd[s][MAG_ENTRY_TIME][0]
        if not entry:
            continue
        held = list(entry.keys())
        minute_list = sorted(t for t in times if MAG_ENTRY_TIME <= t <= MAG_SQUAREOFF)
        # walk minutes, compute equal-weight basket P&L% on capital
        armed = False
        peak = -1e9
        exit_pl = None
        exit_reason = "squareoff_1512"
        last_price = dict(entry)  # forward-fill last known close
        for t in minute_list:
            for s in held:
                if t in sd[s]:
                    last_price[s] = sd[s][t][1]  # close of that minute
            # basket P&L%
            pls = [(last_price[s] / entry[s] - 1.0) * 100.0 for s in held]
            pl = float(np.mean(pls))
            if pl > peak:
                peak = pl
            if not armed and pl >= MAG_ARM:
                armed = True
            # exit checks (evaluated each minute)
            if pl <= MAG_HARDSTOP:
                exit_pl, exit_reason = pl, "hardstop_-3"
                break
            if armed:
                trail = max(peak - MAG_GIVEBACK, MAG_FLOOR)
                if pl <= trail:
                    exit_pl, exit_reason = pl, "trail_giveback"
                    break
        if exit_pl is None:
            # square-off at 15:12 (or last available minute <= 15:12)
            for s in held:
                t1512 = MAG_SQUAREOFF if MAG_SQUAREOFF in sd[s] else None
                if t1512:
                    last_price[s] = sd[s][MAG_SQUAREOFF][1]
            pls = [(last_price[s] / entry[s] - 1.0) * 100.0 for s in held]
            exit_pl = float(np.mean(pls))
        results.append({
            "entry_date": D1, "entry_month": D1[:7], "ret": exit_pl / 100.0,
            "size": len(held), "full15": len(held) == TOP_N, "reason": exit_reason,
            "armed": armed, "peak": peak, "signal_date": D, "syms": held, "entry": entry,
        })
    con.close()
    return results


# ---------------------------------------------------------------- reporting
def monthly_table(results, label, leverage=1.0):
    """Compound month-over-month; report per-month mean basket return, dd, coverage."""
    months = sorted(set(r["entry_month"] for r in results))
    print(f"\n{'='*92}\n{label}   (leverage x{leverage:g})\n{'='*92}")
    print(f"{'Month':<9}{'Return%':>10}{'MaxDD%':>10}{'#baskets':>10}{'AvgSize':>9}{'%full15':>9}")
    equity = 1.0
    peak_eq = 1.0
    agg = []
    monthly_rets = []
    for m in months:
        rs = [r for r in results if r["entry_month"] == m]
        rets = [r["ret"] * leverage for r in rs]
        # per-month: mean basket return compounded across baskets in the month
        # equity walk basket-by-basket (chronological) to get intra-month DD
        mstart_eq = equity
        mpeak = equity
        mmaxdd = 0.0
        for r in sorted(rs, key=lambda x: x["entry_date"]):
            equity *= (1 + r["ret"] * leverage)
            mpeak = max(mpeak, equity)
            dd = (equity - mpeak) / mpeak
            mmaxdd = min(mmaxdd, dd)
            peak_eq = max(peak_eq, equity)
        mret = equity / mstart_eq - 1.0
        monthly_rets.append(mret)
        sizes = [r["size"] for r in rs]
        pct_full = 100.0 * sum(1 for r in rs if r["full15"]) / len(rs)
        print(f"{m:<9}{mret*100:>10.2f}{mmaxdd*100:>10.2f}{len(rs):>10}{np.mean(sizes):>9.1f}{pct_full:>9.0f}")
        agg.append((m, mret, mmaxdd, len(rs)))
    total = equity - 1.0
    worst = min(monthly_rets) if monthly_rets else 0.0
    print("-"*57)
    print(f"Total 2026 return: {total*100:+.2f}%   |   Mean monthly: {np.mean(monthly_rets)*100:+.2f}%   |   Worst month: {worst*100:+.2f}%")
    return agg


def coverage_stats(signals):
    days = sorted(signals.keys())
    sizes = [len(signals[d]) for d in days]
    n_days = len(days)
    n_ge1 = sum(1 for s in sizes if s >= 1)
    n_zero = sum(1 for s in sizes if s == 0)
    n_full = sum(1 for s in sizes if s == TOP_N)
    print(f"\n{'='*92}\nCOVERAGE (2026 signal-days, clean-191 signal)\n{'='*92}")
    print(f"Total 2026 signal-days (feature dates): {n_days}")
    print(f"  days with >=1 name : {n_ge1} ({100*n_ge1/n_days:.0f}%)")
    print(f"  days with 0 names  : {n_zero} ({100*n_zero/n_days:.0f}%)")
    print(f"  days with full 15  : {n_full} ({100*n_full/n_days:.0f}%)")
    from collections import Counter
    dist = Counter(sizes)
    print("  basket-size distribution (size: #days):")
    for sz in sorted(dist):
        print(f"     {sz:>2}: {dist[sz]}")


def worked_example_btst(btst):
    # pick a signal-day with a full-15 basket that was NOT disaster-stopped
    cands = [r for r in btst if r["full15"] and not r["stopped"]]
    r = cands[len(cands)//2] if cands else (btst[0] if btst else None)
    if not r:
        print("no BTST basket to show"); return
    print(f"\n{'='*92}\nWORKED EXAMPLE — BTST Oscillator\n{'='*92}")
    print(f"Signal day D = {r['signal_date']}  -> entry D+1 = {r['entry_date']}  (hold to D+2 close)")
    print(f"{'symbol':<14}{'score':>9}{'openD+1':>10}{'closeD+2':>10}{'ret%':>9}")
    for nm in r["names"]:
        print(f"{nm['symbol']:<14}{nm['score']:>9.2f}{nm['o1']:>10.2f}{nm['c2']:>10.2f}{nm['r_hold']*100:>9.2f}")
    print(f"basket size {r['size']}, disaster-stop {'YES' if r['stopped'] else 'no'}")
    print(f"BASKET RETURN = mean(per-name) = {r['ret']*100:+.3f}%")


def worked_example_mag(mag, mindb):
    cands = [r for r in mag if r["full15"]]
    r = cands[len(cands)//2] if cands else (mag[0] if mag else None)
    if not r:
        print("no Magnifier basket to show"); return
    print(f"\n{'='*92}\nWORKED EXAMPLE — Magnifier (intraday MIS 1-min trail)\n{'='*92}")
    print(f"Signal day D = {r['signal_date']}  -> entry D+1 = {r['entry_date']} @ 09:15 open")
    print(f"exit reason: {r['reason']}   armed: {r['armed']}   peak basket P&L: {r['peak']:+.2f}%")
    # recompute per-name entry->1512 to show arithmetic
    con = ro(mindb)
    syms = r["syms"]; entry = r["entry"]; D1 = r["entry_date"]
    ph = "(" + ",".join("?"*len(syms)) + ")"
    rows = con.execute(f"""SELECT symbol, substr(bar_time,12,8) t, close FROM ohlc_1min
        WHERE bar_time>=? AND bar_time<=? AND symbol IN {ph}""",
        [f"{D1} 00:00:00", f"{D1} 23:59:59"]+syms).fetchall()
    con.close()
    lastp = {}
    for s, t, cl in rows:
        if t <= MAG_SQUAREOFF:
            lastp.setdefault(s, {})[t] = cl
    print(f"{'symbol':<14}{'entry@0915':>12}{'close@1512':>12}{'ret%':>9}")
    for s in syms:
        # last close <= 15:12
        ts = sorted(lastp.get(s, {}).keys())
        px1512 = lastp[s][ts[-1]] if ts else entry[s]
        print(f"{s:<14}{entry[s]:>12.2f}{px1512:>12.2f}{(px1512/entry[s]-1)*100:>9.2f}")
    print(f"basket size {r['size']}")
    print(f"BASKET RETURN (1x) = {r['ret']*100:+.3f}%   |  (5x) = {r['ret']*5*100:+.3f}%")
    print("  NOTE: table shows @15:12 marks for illustration; the engine may have")
    print(f"        exited earlier via '{r['reason']}' at basket P&L {r['ret']*100:+.2f}%.")


def main():
    print("RESEARCH BACKTEST — clean-191 (leak-free) Falcon signal, 2026")
    print(f"Serving DB : {SERVING_DB}")
    print(f"1-min DB   : {MIN_DB}")

    scon = ro(SERVING_DB)
    patterns = load_clean_patterns(scon)
    print(f"\nClean patterns loaded: {len(patterns)} (expected 191)")
    # confirm zero weekly refs
    leaked_refs = sum(1 for p in patterns for f, _, _ in p["rule"] if f in WEEKLY)
    print(f"Weekly-feature references across the 191 rules: {leaked_refs} (must be 0)")

    signals = build_signals(scon, patterns)
    px, cal = load_daily(scon)
    scon.close()

    coverage_stats(signals)

    btst = run_btst(signals, px, cal)
    mag = run_magnifier(signals, cal, MIN_DB)

    print(f"\nMagnifier is limited to signal-days whose D+1 <= {MIN_1MIN_CUTOFF} (1-min data cutoff).")
    print(f"BTST baskets: {len(btst)}   |   Magnifier baskets: {len(mag)}")

    monthly_table(btst, "BTST OSCILLATOR — 1x CNC, 2-session hold", 1.0)
    monthly_table(mag, "MAGNIFIER — intraday MIS", 1.0)
    monthly_table(mag, "MAGNIFIER — intraday MIS", 5.0)

    worked_example_btst(btst)
    worked_example_mag(mag, MIN_DB)


if __name__ == "__main__":
    main()
