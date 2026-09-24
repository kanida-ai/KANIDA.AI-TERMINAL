"""
LIVE INTRADAY FALCON  (Falcon-context + intraday confirmation)
==============================================================
Can we identify the day's strongest Falcon-type movers EARLY (same session) instead
of waiting for EOD + next-day entry?

At each checkpoint T (09:30..13:00), for trading day D:
  pool   = top-K stocks by PRIOR-day (D-1) Falcon score   (known at the open of D)
  confirm= each pool stock's intraday return open(09:15)->T   (from 1-min)
  live   = rank pool by  z(prior_falcon_score) + z(intraday_ret)  -> live Top-10/Top-5

Then measure, per checkpoint:
  (a) overlap of live Top-10 with D's OWN regenerated EOD Falcon Top-10 (the true target)
  (b) same-day TRAILING backtest: enter live Top-5 just after T, trail (+1 lock / 0.75
      giveback / -1.5 stop) to 15:29  -> same-day return
  (c) raw continuation: live Top-5 buy&hold (T+1 open -> 15:29)

Compare best checkpoint's same-day result to the next-day agents.
Read-only. Output: outputs/Live_Falcon_Intraday.xlsx
"""
import sqlite3, bisect, argparse
from pathlib import Path
import numpy as np
import pandas as pd

from falcon_signal_replay import load_patterns, FEATURE_COLS, FIDX, rule_mask
from falcon_intraday_backtest import load_ohlc_1min_day, DEFAULT_ALIASES

ROOT = Path(__file__).resolve().parent.parent
SLIM = ROOT / "data" / "db" / "kanida_universe.db"
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
OUT = ROOT / "outputs" / "Live_Falcon_Intraday.xlsx"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
CAP, TARGET, STOP, GIVEBACK = 500_000.0, 1.0, -1.5, 0.75
POOL_K = 60
CHECKPOINTS = ["09:30", "09:45", "10:00", "10:15", "10:30", "11:00", "11:30", "12:00", "13:00"]
WIN_S, WIN_E = "2024-05-13", "2026-06-24"


def per_stock_scores(con, patterns, date):
    """Falcon score + n_fires for EVERY stock with features on `date` (no filter)."""
    rows = con.execute(f"SELECT symbol, {', '.join(FEATURE_COLS)} FROM falcon_features "
                       f"WHERE trade_date=?", (date,)).fetchall()
    if not rows:
        return None
    syms = [r[0] for r in rows]
    X = np.full((len(syms), len(FEATURE_COLS)), np.nan)
    for i, r in enumerate(rows):
        X[i] = [v if v is not None else np.nan for v in r[1:]]
    yr = int(date[:4])
    elig = [p for p in patterns if int(p["mined_year"]) < yr]
    fire = np.zeros(len(syms), dtype=np.int32); score = np.zeros(len(syms))
    for p in elig:
        m = rule_mask(p["rule"], X)
        if m.any():
            fire += m.astype(np.int32); score += m.astype(np.float64) * p["oos_lift"]
    return syms, score, fire


def eod_top10(syms, score, fire, min_fires=10):
    cand = [(syms[i], score[i], fire[i]) for i in range(len(syms)) if fire[i] >= min_fires]
    cand.sort(key=lambda c: -c[1])
    top = cand[:100]
    top.sort(key=lambda c: -(c[1] / max(c[2], 1)))
    return [s for s, _, _ in top[:10]]


def trail_from(day_ohlc, picks, entry_time):
    """Enter at first bar open >= entry_time, trail to 15:29. Returns (ret%, n)."""
    grid = None
    for s in picks:
        df = day_ohlc.get(s)
        if df is None:
            continue
        g = [m for m in df.index if m >= entry_time]
        if g and (grid is None or len(g) > len(grid)):
            grid = g
    if not grid:
        return None
    cc, oo, ep = [], [], []
    for s in picks:
        df = day_ohlc.get(s)
        if df is None or grid[0] not in df.index:
            continue
        eo = df.at[grid[0], "open"]
        if not np.isfinite(eo) or eo <= 0:
            continue
        close = df["close"].reindex(grid).ffill().bfill().to_numpy(float)
        o = df["open"].reindex(grid).to_numpy(float)
        oo.append(np.where(np.isfinite(o) & (o > 0), o, close)); cc.append(close); ep.append(float(eo))
    if not ep:
        return None
    ep = np.array(ep); qty = np.floor((CAP / len(ep)) / ep); keep = qty > 0
    if not keep.any():
        return None
    close = np.column_stack(cc)[:, keep]; openexec = np.column_stack(oo)[:, keep]
    qty = qty[keep]; ep = ep[keep]
    deployed = float((qty * ep).sum())
    port = close @ qty; ret = (port - deployed) / deployed * 100.0
    openval = openexec @ qty
    nxt = np.empty_like(openval); nxt[:-1] = openval[1:]; nxt[-1] = port[-1]
    armed = False; peak = None
    for i in range(len(ret) - 1):
        r = ret[i]
        if r <= STOP:
            return (nxt[i] - deployed) / deployed * 100.0, int(keep.sum())
        if not armed:
            if r >= TARGET:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        if r <= max(TARGET, peak - GIVEBACK):
            return (nxt[i] - deployed) / deployed * 100.0, int(keep.sum())
    return (float(port[-1]) - deployed) / deployed * 100.0, int(keep.sum())


def buyhold(day_ohlc, picks, entry_time):
    grid = None
    for s in picks:
        df = day_ohlc.get(s)
        if df is None:
            continue
        g = [m for m in df.index if m >= entry_time]
        if g and (grid is None or len(g) > len(grid)):
            grid = g
    if not grid:
        return None
    rs = []
    for s in picks:
        df = day_ohlc.get(s)
        if df is None or grid[0] not in df.index:
            continue
        eo = df.at[grid[0], "open"]
        last = df["close"].reindex(grid).ffill().bfill().to_numpy(float)[-1]
        if np.isfinite(eo) and eo > 0:
            rs.append((last / eo - 1) * 100)
    return float(np.mean(rs)) if rs else None


def z(a):
    a = np.asarray(a, float); mu = np.nanmean(a); sd = np.nanstd(a)
    return (a - mu) / sd if sd > 0 else np.zeros_like(a)


def minute_after(t):
    h, m = int(t[:2]), int(t[3:]); m += 1
    if m == 60:
        h += 1; m = 0
    return f"{h:02d}:{m:02d}"


def main():
    sc = sqlite3.connect(str(SLIM)); rc = sqlite3.connect(str(RND))
    patterns = load_patterns(sc)
    print(f"[*] patterns {len(patterns)}", flush=True)
    feat_days = [r[0] for r in sc.execute(
        "SELECT DISTINCT trade_date FROM falcon_features WHERE trade_date BETWEEN ? AND ? ORDER BY 1",
        (WIN_S, WIN_E))]
    onemin = set(r[0] for r in rc.execute("SELECT DISTINCT substr(bar_time,1,10) FROM ohlc_1min"))

    # accumulate per-checkpoint stats
    recs = {t: [] for t in CHECKPOINTS}
    overlap = {t: [] for t in CHECKPOINTS}
    prior_syms = prior_score = prior_fire = None; prior_date = None
    for k, D in enumerate(feat_days):
        # prior-day falcon (context) = previous feature day
        if prior_date is not None and D in onemin and prior_syms is not None:
            # build pool = top-K by prior score
            order = np.argsort(-prior_score)
            pool = [prior_syms[i] for i in order[:POOL_K]]
            prior_lookup = {prior_syms[i]: prior_score[i] for i in range(len(prior_syms))}
            day_ohlc = load_ohlc_1min_day(rc, D, pool, DEFAULT_ALIASES)
            avail = [s for s in pool if day_ohlc.get(s) is not None and "09:15" in day_ohlc[s].index]
            if len(avail) >= 10:
                # EOD target = D's own regenerated top-10
                tgt = per_stock_scores(sc, patterns, D)
                eod10 = set(eod_top10(*tgt)) if tgt else set()
                opens = {s: day_ohlc[s].at["09:15", "open"] for s in avail}
                for t in CHECKPOINTS:
                    rets = []
                    for s in avail:
                        df = day_ohlc[s]
                        bars = [m for m in df.index if m <= t and m >= "09:15"]
                        if not bars or not np.isfinite(opens[s]) or opens[s] <= 0:
                            rets.append(np.nan); continue
                        rets.append((df.at[bars[-1], "close"] / opens[s] - 1) * 100)
                    rets = np.array(rets, float)
                    pri = np.array([prior_lookup.get(s, 0) for s in avail], float)
                    live_score = z(pri) + z(rets)
                    live_score[~np.isfinite(rets)] = -1e9
                    rank = np.argsort(-live_score)
                    live10 = [avail[i] for i in rank[:10]]
                    live5 = [avail[i] for i in rank[:5]]
                    if eod10:
                        overlap[t].append(len(set(live10) & eod10))
                    et = minute_after(t)
                    tr = trail_from(day_ohlc, live5, et)
                    bh = buyhold(day_ohlc, live5, et)
                    if tr is not None:
                        recs[t].append({"date": D, "stocks": ", ".join(live5),
                                        "trail_ret": round(tr[0], 4),
                                        "bh_ret": round(bh, 4) if bh is not None else None})
        # advance prior
        ps = per_stock_scores(sc, patterns, D)
        if ps:
            prior_syms, prior_score, prior_fire = ps; prior_date = D
        if (k + 1) % 100 == 0:
            print(f"  [{k+1}/{len(feat_days)}] {D}", flush=True)
    sc.close(); rc.close()

    # summarize
    rows = []
    for t in CHECKPOINTS:
        df = pd.DataFrame(recs[t])
        ov = overlap[t]
        if df.empty:
            continue
        rows.append({
            "checkpoint": t, "days": len(df),
            "overlap_w_EOD_/10": round(np.mean(ov), 2) if ov else None,
            "trail_win%": round((df.trail_ret > 0).mean() * 100, 1),
            "trail_avg_day%": round(df.trail_ret.mean(), 3),
            "trail_sum%": round(df.trail_ret.sum(), 1),
            "buyhold_avg%": round(df.bh_ret.mean(), 3),
        })
    summary = pd.DataFrame(rows)

    best = summary.sort_values("trail_avg_day%", ascending=False).iloc[0]["checkpoint"] if not summary.empty else None
    comp = pd.DataFrame([
        {"strategy": f"LIVE intraday Falcon (same-day entry, best checkpoint {best})",
         "avg_day%": summary.set_index("checkpoint").loc[best, "trail_avg_day%"] if best else None,
         "win%": summary.set_index("checkpoint").loc[best, "trail_win%"] if best else None,
         "entry": "same session (intraday)"},
        {"strategy": "Trailing/Intraday agent (next-day 9:15 on EOD Top-5, true signals)",
         "avg_day%": 0.956, "win%": 74.9, "entry": "next trading day 09:15"},
    ])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        summary.to_excel(xl, "1_By_Checkpoint", index=False)
        comp.to_excel(xl, "2_LiveSameDay_vs_NextDay", index=False)
        if best:
            pd.DataFrame(recs[best]).to_excel(xl, "3_BestCheckpoint_log", index=False)
    pd.set_option("display.width", 200)
    print("\n=== LIVE INTRADAY FALCON — by checkpoint ===")
    print(summary.to_string(index=False))
    print("\n=== same-day (best) vs next-day agent ===")
    print(comp.to_string(index=False))
    print(f"\n[*] wrote {OUT}")
    if DESK.exists():
        import shutil; shutil.copy(OUT, DESK / OUT.name); print(f"[*] copied to {DESK/OUT.name}")


if __name__ == "__main__":
    main()
