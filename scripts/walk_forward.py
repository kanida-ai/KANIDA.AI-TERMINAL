"""
KANIDA — WALK-FORWARD SELECTOR. The honest question: can we pick the tradeable stocks A-PRIORI
(before the test year), or is the per-stock edge only visible in hindsight?

Method (no look-ahead):
  * Patterns = TRAIN-GATED only (mined on <=2024; every stored mined_patterns row passed the train
    gate). We do NOT use the 'promoted' flag here (promotion used 2025 -> would leak into the 2025
    selection year).
  * SELECT year = 2025: trade those train-gated patterns on 2025 -> each stock's 2025 ROC/PF.
    This is a genuine OOS year, known BEFORE 2026 -> a legitimate selection signal.
  * TEST year = 2026 (sealed): trade the SAME patterns on 2026 -> each stock's 2026 ROC/PF.
  * Analysis: does 2025 rank predict 2026? Spearman corr; quintiles; and the money test —
    a rule decided ONLY on 2025 (roc2025>0 AND pf2025>=1.2) -> is the SELECTED set's 2026 ROC
    materially better than the unselected / whole universe?

Parallel (8 workers, own DBs, merge). Writes KANIDA_SNR.db.walkforward + prints the verdict.
Run: PYTHONIOENCODING=utf-8 python walk_forward.py [N]      (logs db/walkforward.out)
"""
import os
os.environ["OMP_NUM_THREADS"] = "1"; os.environ["OPENBLAS_NUM_THREADS"] = "1"; os.environ["MKL_NUM_THREADS"] = "1"
import sys, sqlite3, time, json, multiprocessing as mp
from pathlib import Path
from datetime import datetime
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\scripts")))
from mine_phase1 import features, apply_rule, TARGETS
from confirm_and_trade import route, simulate, LEV, COST, CAPITAL

KDB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
SNR = r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db"
WK = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\wf_dbs")
OUT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\walkforward.out")
TGT = {n: (d, p, w) for n, d, p, w in TARGETS}
MIN_N = 10                                                # need >=10 trades in a year to rank a stock


def load_cash(stock, start="2024-12-15"):
    con = sqlite3.connect(KDB)
    df = pd.read_sql("SELECT bar_time,open,high,low,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? ORDER BY bar_time",
                     con, params=[stock, start]); con.close()
    df["day"] = df["bar_time"].str[:10]; df["tm"] = df["bar_time"].str[11:19]
    return {d: list(zip(g["tm"], g["open"], g["high"], g["low"], g["close"])) for d, g in df.groupby("day", sort=False)}


def load_fut(stock):
    con = sqlite3.connect(KDB)
    df = pd.read_sql("SELECT bar_time,expiry,open,high,low,close FROM ohlc_futures_1min WHERE symbol=? ORDER BY bar_time",
                     con, params=[stock]); con.close()
    if df.empty: return {}
    df["day"] = df["bar_time"].str[:10]; df["tm"] = df["bar_time"].str[11:19]; df["exp"] = pd.to_datetime(df["expiry"])
    out = {}
    for day, g in df.groupby("day", sort=False):
        dd = pd.to_datetime(day); v = g[g["exp"] >= dd]; v = v if not v.empty else g
        fm = v["exp"].min(); gg = v[v["exp"] == fm]
        out[day] = list(zip(gg["tm"], gg["open"], gg["high"], gg["low"], gg["close"]))
    return out


def trade_year(pats, F, is_fno, cash, fut, year):
    Fy = F[F["year"] == year]; days = list(Fy.index)
    if len(days) < 2 or not pats: return []
    lifts = np.array([p["lift"] for p in pats])
    FIRE = np.zeros((len(days), len(pats)), dtype=bool)          # [days x patterns], vectorized once
    for pi, p in enumerate(pats):
        FIRE[:, pi] = apply_rule(Fy, p["conds"]).values
    pnls = []; i = 0
    while i < len(days) - 1:
        fired = np.where(FIRE[i])[0]
        if fired.size == 0:
            i += 1; continue
        fired = fired[np.argsort(-lifts[fired])]                 # highest-lift first
        ed = days[i + 1]; ed_str = ed.strftime("%Y-%m-%d"); chosen = None
        for pi in fired:
            p = pats[pi]; prod, inst, needs = route(p["dir"], p["w"])
            if needs and (not is_fno or ed_str not in fut): continue
            chosen = (p, prod, inst); break
        if chosen is None:
            i += 1; continue
        p, prod, inst = chosen
        long = (p["dir"] == "up"); wdays = days[i + 1: i + 1 + p["w"]]; bars = fut if inst == "FUT" else cash
        r = simulate(bars, wdays, long, p["pct"], prod, "fixed_target")
        if r is None:
            i += 1; continue
        entry, ex = r["entry"], r["exit"]; lev = LEV[prod]; notional = CAPITAL * lev; qty = int(notional // entry)
        pnl = qty * ((ex - entry) if long else (entry - ex)) - notional * COST[prod] / 100
        pnls.append(pnl); i = days.index(r["exit_day"]) + 1
    return pnls


def metrics(pnls):
    n = len(pnls)
    if n == 0: return (0, 0.0, 0.0, 0.0)
    s = sum(pnls); g = sum(p for p in pnls if p > 0); l = -sum(p for p in pnls if p < 0)
    pf = (g / l) if l > 0 else (999.0 if g > 0 else 0.0)
    win = sum(1 for p in pnls if p > 0) / n * 100
    return (n, round(s / CAPITAL * 100, 2), round(pf, 2), round(win, 1))


def process_chunk(arg):
    wid, stocks = arg
    WK.mkdir(exist_ok=True); wpath = WK / f"wf_{wid}.db"
    if wpath.exists(): wpath.unlink()
    wcon = sqlite3.connect(str(wpath))
    wcon.execute("""CREATE TABLE walkforward(Stock TEXT PRIMARY KEY, is_fno INTEGER,
        n2025 INTEGER, roc2025 REAL, pf2025 REAL, win2025 REAL,
        n2026 INTEGER, roc2026 REAL, pf2026 REAL, win2026 REAL)"""); wcon.commit()
    ro = sqlite3.connect(f"file:{SNR}?mode=ro", uri=True)
    kc = sqlite3.connect(KDB)
    fno = {s: (kc.execute("SELECT is_fno FROM instrument_labels WHERE symbol=?", (s,)).fetchone() or [0])[0] for s in stocks}
    kc.close()
    for s in stocks:
        try:
            rows = ro.execute("SELECT outcome_target,rule_json,lift_pct FROM mined_patterns "
                              "WHERE Stock=? AND scope='stock_specific'", (s,)).fetchall()
            if not rows: continue
            pats = [{"target": t, "dir": TGT[t][0], "pct": TGT[t][1], "w": TGT[t][2],
                     "conds": [tuple(c) for c in json.loads(rj)], "lift": lf or 0} for t, rj, lf in rows]
            F = features(s); cash = load_cash(s); fut = load_fut(s) if fno[s] else {}
            m25 = metrics(trade_year(pats, F, bool(fno[s]), cash, fut, 2025))
            m26 = metrics(trade_year(pats, F, bool(fno[s]), cash, fut, 2026))
            wcon.execute("INSERT OR REPLACE INTO walkforward VALUES (?,?,?,?,?,?,?,?,?,?)",
                         (s, fno[s], *m25, *m26)); wcon.commit()
        except Exception as e:
            with open(WK / f"wf_{wid}.log", "a", encoding="utf-8") as f: f.write(f"{s} ERR {str(e)[:140]}\n")
    ro.close(); wcon.close(); return str(wpath)


def log(m):
    line = f"{datetime.now():%H:%M:%S} {m}"; print(line, flush=True)
    with open(OUT, "a", encoding="utf-8") as f: f.write(line + "\n")


def analyze():
    con = sqlite3.connect(SNR)
    df = pd.read_sql("SELECT * FROM walkforward", con); con.close()
    df.to_csv(r"C:\Users\SPS\Documents\Kanida_Falcon\reports\walkforward.csv", index=False)
    q = df[(df.n2025 >= MIN_N) & (df.n2026 >= MIN_N)].copy()
    log(f"stocks with >= {MIN_N} trades in BOTH years: {len(q)} (of {len(df)})")
    if len(q) < 20:
        log("too few for a stable read"); return
    sp = q[["roc2025", "roc2026"]].corr(method="spearman").iloc[0, 1]
    pe = q[["roc2025", "roc2026"]].corr(method="pearson").iloc[0, 1]
    log(f"CORR 2025->2026 stock ROC:  Spearman {sp:+.3f} | Pearson {pe:+.3f}")
    # quintiles by 2025 ROC -> mean 2026 ROC
    q["quint25"] = pd.qcut(q.roc2025, 5, labels=["Q1(worst)", "Q2", "Q3", "Q4", "Q5(best)"])
    qt = q.groupby("quint25", observed=True).agg(n=("Stock", "size"), mean_roc2025=("roc2025", "mean"),
                                                 mean_roc2026=("roc2026", "mean"),
                                                 pct_pos_2026=("roc2026", lambda s: (s > 0).mean() * 100)).round(1)
    log("QUINTILES by 2025 ROC -> 2026 outcome:\n" + qt.to_string())
    # the money test: rule decided ONLY on 2025
    sel = q[(q.roc2025 > 0) & (q.pf2025 >= 1.2)]; non = q.drop(sel.index)
    log(f"\nSELECTION RULE (roc2025>0 AND pf2025>=1.2), decided WITHOUT 2026:")
    log(f"  selected: {len(sel)} stocks | 2026 mean ROC {sel.roc2026.mean():+.1f}% | median {sel.roc2026.median():+.1f}% | %pos {(sel.roc2026>0).mean()*100:.0f}%")
    log(f"  NOT sel : {len(non)} stocks | 2026 mean ROC {non.roc2026.mean():+.1f}% | median {non.roc2026.median():+.1f}% | %pos {(non.roc2026>0).mean()*100:.0f}%")
    log(f"  whole set: {len(q)} stocks | 2026 mean ROC {q.roc2026.mean():+.1f}%")
    edge = sel.roc2026.mean() - q.roc2026.mean()
    log(f"  >>> selector edge on 2026 = {edge:+.1f}pp vs universe. "
        f"{'WORKS — a-priori selection adds value.' if edge > 5 and sel.roc2026.mean() > 0 else 'WEAK/does not work — 2025 does not reliably predict 2026.'}")
    # top-30 by 2025 as a concrete portfolio
    top = q.nlargest(30, "roc2025")
    log(f"\nTOP-30 by 2025 ROC -> their 2026: mean ROC {top.roc2026.mean():+.1f}% | %pos {(top.roc2026>0).mean()*100:.0f}%")


def main():
    OUT.write_text("", encoding="utf-8"); WK.mkdir(exist_ok=True)
    N = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    con = sqlite3.connect(KDB)
    cov = {r[0]: r[1][:10] for r in con.execute("SELECT symbol,max(bar_time) FROM ohlc_daily GROUP BY symbol")}
    con.close()
    snr = sqlite3.connect(SNR)
    stocks = [r[0] for r in snr.execute("SELECT DISTINCT Stock FROM mined_patterns WHERE scope='stock_specific'").fetchall()]
    snr.close()
    stocks = sorted([s for s in stocks if cov.get(s, "") >= "2026-07-25"])
    chunks = [[] for _ in range(N)]
    for i, s in enumerate(stocks): chunks[i % N].append(s)
    args = [(i, c) for i, c in enumerate(chunks) if c]
    log(f"=== WALK-FORWARD | {len(stocks)} stocks | {len(args)} workers | select 2025 -> test 2026 ===")
    t0 = time.time()
    with mp.get_context("spawn").Pool(len(args)) as pool:
        outs = pool.map(process_chunk, args)
    main_con = sqlite3.connect(SNR, timeout=180)
    main_con.execute("""CREATE TABLE IF NOT EXISTS walkforward(Stock TEXT PRIMARY KEY, is_fno INTEGER,
        n2025 INTEGER, roc2025 REAL, pf2025 REAL, win2025 REAL,
        n2026 INTEGER, roc2026 REAL, pf2026 REAL, win2026 REAL)""")
    main_con.execute("DELETE FROM walkforward"); main_con.commit()
    for wp in outs:
        posix = wp.replace("\\", "/"); main_con.execute(f"ATTACH DATABASE '{posix}' AS wk")
        main_con.execute("INSERT OR REPLACE INTO walkforward SELECT * FROM wk.walkforward")
        main_con.commit(); main_con.execute("DETACH DATABASE wk")
    main_con.close()
    log(f"workers+merge done in {time.time()-t0:.0f}s")
    analyze()
    log("WALK-FORWARD COMPLETE")


if __name__ == "__main__":
    main()
