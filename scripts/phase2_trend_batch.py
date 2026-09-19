"""
KANIDA Phase-2 (A + C): TREND-CONTINUATION pattern family across a batch of F&O stocks.

A) New pattern family using TRIPLE-BARRIER first-touch labels (Lopez de Prado):
     up trade wins if +TP% is hit BEFORE -SL% within N days (else loss / no follow-through).
   These reward sustained follow-through, so STOP/TRAILING exits finally MATCH the signal
   (unlike the reversion touch-targets in mine_phase1, where a fixed target was optimal).
   Mine per stock (RF leaf-rules over point-in-time features) -> promote if holds val 2025.

C) Roll the leverage-correct, product-type-accurate engine over a 12-stock F&O batch:
     Rs1,00,000 = margin deployed. Multi-day trades on F&O names -> NRML FUTURES (5x, ~20% margin);
     no futures data -> long falls back to CNC cash (1x), short = NOT EXECUTABLE.
   Confirm on SEALED 2026, tag Keep/Watch/Retire, trade with fixed_target / target_stop / trailing,
   exact 1-min entry/exit times. Writes mined_patterns(scope='trend') + trade_log_trend/journal_trend.

Run: PYTHONIOENCODING=utf-8 python phase2_trend_batch.py   (logs to db/phase2_trend.log)
"""
import sys, json, sqlite3
from datetime import datetime
from pathlib import Path
import numpy as np, pandas as pd
from sklearn.ensemble import RandomForestClassifier
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\scripts")))
from mine_phase1 import features, leaf_rules, apply_rule
from confirm_and_trade import (load_cash_1min, load_fut_1min_frontmonth, LEV, COST, CAPITAL,
                               MIS_CUTOFF, SL_MULT, ACT_MULT, TRAIL_MULT)

KDB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
SNR = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db")
BATCH = sys.argv[1:] or ["ADANIENT", "CARTRADE"]     # scoped: trend family for ADANIENT + CARTRADE
VAULT_YEAR = 2026
# barrier targets: (name, direction, TP%, SL%, horizon_days)
BARRIERS = [("up_tb3x1_5d", "up", 3.0, 1.0, 5), ("up_tb5x2_10d", "up", 5.0, 2.0, 10),
            ("dn_tb3x1_5d", "dn", 3.0, 1.0, 5), ("dn_tb5x2_10d", "dn", 5.0, 2.0, 10)]
BAR = {b[0]: b for b in BARRIERS}
TRAIN_MAX, VAL_YEAR = 2024, 2025
MIN_TR_OBS, MIN_TR_LIFT = 40, 8.0
MIN_VA_OBS, MIN_VA_LIFT = 12, 5.0
PID_OFFSET = 500000                                   # keep trend pattern_ids clear of reversion
POLICIES = ["fixed_target", "target_stop", "trailing"]
LOG = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\phase2_trend.log")


def log(m):
    line = f"{datetime.now():%H:%M:%S} {m}"
    print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ---------- triple-barrier first-touch label ----------
def label_barrier(F, direction, tp, sl, N):
    C = F["_close"].values; H = F["_high"].values; L = F["_low"].values
    n = len(C)
    if direction == "up":
        tp_lvl = C * (1 + tp / 100); sl_lvl = C * (1 - sl / 100)
    else:
        tp_lvl = C * (1 - tp / 100); sl_lvl = C * (1 + sl / 100)
    res = np.full(n, np.nan); resolved = np.zeros(n, bool)
    for k in range(1, N + 1):
        hi = np.concatenate([H[k:], np.full(k, np.nan)])
        lo = np.concatenate([L[k:], np.full(k, np.nan)])
        valid = ~np.isnan(hi)
        if direction == "up":
            hit_t = hi >= tp_lvl; hit_s = lo <= sl_lvl
        else:
            hit_t = lo <= tp_lvl; hit_s = hi >= sl_lvl
        newly = (~resolved) & valid & (hit_t | hit_s)
        win = hit_t & ~hit_s                            # both same day -> treat as stop (conservative)
        res[newly & win] = 1.0
        res[newly & ~win] = 0.0
        resolved |= newly
    enough = np.arange(n) < (n - N)                     # unresolved w/ full horizon known -> no follow-through
    res[np.isnan(res) & enough] = 0.0
    return pd.Series(res, index=F.index)


def stats(y, mask):
    n = int(mask.sum())
    if n == 0: return 0, 0, 0.0
    hits = int(y[mask].sum()); return n, hits, hits / n * 100


# ---------- (A) mine trend patterns ----------
def mine_trend(stock, F, con):
    feats = [c for c in F.columns if not c.startswith("_") and c != "year"]
    yrs = F["year"]; tr = yrs <= TRAIN_MAX; va = yrs == VAL_YEAR
    pid = PID_OFFSET; promoted = 0; mined = 0
    for name, d, tp, sl, N in BARRIERS:
        y = label_barrier(F, d, tp, sl, N)
        m = y.notna() & F[feats].notna().all(axis=1)
        Xtr, ytr = F.loc[tr & m, feats], y[tr & m]
        if len(ytr) < 100 or ytr.nunique() < 2: continue
        base_tr = ytr.mean() * 100; base_va = y[va & m].mean() * 100 if (va & m).sum() else 0
        rf = RandomForestClassifier(n_estimators=60, max_depth=3, min_samples_leaf=25,
                                    random_state=42, n_jobs=-1)
        rf.fit(Xtr, ytr)
        seen = set()
        for conds in leaf_rules(rf, feats):
            key = tuple(sorted(conds))
            if key in seen: continue
            seen.add(key)
            mtr = apply_rule(F.loc[tr & m], conds); ntr, htr, ptr = stats(ytr, mtr)
            if ntr < MIN_TR_OBS or (ptr - base_tr) < MIN_TR_LIFT: continue
            mva = apply_rule(F.loc[va & m], conds); nva, hva, pva = stats(y[va & m], mva)
            prom = 1 if (nva >= MIN_VA_OBS and (pva - base_va) >= MIN_VA_LIFT) else 0
            pid += 1; mined += 1; promoted += prom
            rule_text = " AND ".join(f"{f}{op}{round(thr,4)}" for f, op, thr in conds)
            con.execute("""INSERT OR REPLACE INTO mined_patterns
                (Stock,pattern_id,mined_year,scope,outcome_target,n_obs,n_hits,precision_pct,
                 base_rate_pct,lift_pct,depth,rule_text,rule_json,promoted)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (stock, pid, TRAIN_MAX, "trend", name, ntr, htr, round(ptr, 2), round(base_tr, 2),
                 round(ptr - base_tr, 2), len(conds), rule_text, json.dumps(conds), prom))
    con.commit()
    log(f"  [{stock}] trend mined={mined} promoted={promoted}")
    return promoted


def confirm_trend(stock, F, con):
    rows = con.execute("SELECT pattern_id,outcome_target,rule_json,promoted FROM mined_patterns "
                       "WHERE Stock=? AND scope='trend'", (stock,)).fetchall()
    yr = F["year"] == VAULT_YEAR; counts = {"Keep": 0, "Watch": 0, "Retire": 0, "Test": 0}
    for pid, tname, rjson, promoted in rows:
        if not promoted:
            con.execute("UPDATE mined_patterns SET status='Test' WHERE Stock=? AND pattern_id=? AND outcome_target=?",
                        (stock, pid, tname)); counts["Test"] += 1; continue
        _, d, tp, sl, N = BAR[tname]; y = label_barrier(F, d, tp, sl, N); sub = yr & y.notna()
        base = y[sub].mean() * 100
        conds = [tuple(c) for c in json.loads(rjson)]; m = apply_rule(F[sub], conds)
        n = int(m.sum()); hits = int(y[sub][m].sum()) if n else 0
        prec = hits / n * 100 if n else 0.0; lift = prec - base
        status = "Keep" if (n >= 8 and lift >= 3 and prec > base) else ("Watch" if (n >= 3 and lift > 0) else "Retire")
        counts[status] += 1
        con.execute("UPDATE mined_patterns SET status=?, oos2026_n=?, oos2026_hits=?, oos2026_precision_pct=?, "
                    "oos2026_base_pct=?, oos2026_lift_pct=? WHERE Stock=? AND pattern_id=? AND outcome_target=?",
                    (status, n, hits, round(prec, 2), round(base, 2), round(lift, 2), stock, pid, tname))
    con.commit()
    log(f"  [{stock}] trend tagged: " + " ".join(f"{k}={v}" for k, v in counts.items()))


# ---------- barrier trade sim (exact 1-min) ----------
def simulate_barrier(bars, window_days, long, tp, sl, product, policy):
    d0 = window_days[0].strftime("%Y-%m-%d"); b0 = bars.get(d0)
    if not b0: return None
    entry_tm, entry = b0[0][0], float(b0[0][1])
    if entry <= 0: return None
    target = entry * (1 + tp / 100) if long else entry * (1 - tp / 100)
    hardstop = entry * (1 - sl / 100) if long else entry * (1 + sl / 100)
    mfe, mae, last = -1e9, 1e9, None
    act = tp * ACT_MULT; trailgap = sl                    # trailing: activate at act%, trail sl% behind peak
    stop = hardstop; init = hardstop; peak = entry; trough = entry

    def pack(exd, di, tm, ex, reason):
        return dict(entry=entry, entry_tm=entry_tm, exit_day=exd, hold=di + 1, exit_tm=tm,
                    exit=ex, reason=reason, mfe=mfe, mae=mae, target=target, stop=hardstop)

    for di, day in enumerate(window_days):
        for (tm, o, h, l, c) in bars.get(day.strftime("%Y-%m-%d"), []):
            o, h, l, c = float(o), float(h), float(l), float(c); last = (day, di, tm, c)
            if long:
                mfe = max(mfe, (h / entry - 1) * 100); mae = min(mae, (l / entry - 1) * 100)
                if policy != "fixed_target" and l <= stop:
                    ex = o if o < stop else stop
                    return pack(day, di, tm, ex, "hard_stop" if abs(stop - init) < 1e-6 else "trail_stop")
                if policy != "trailing" and h >= target:
                    return pack(day, di, tm, target, "target")
                if policy == "trailing":
                    peak = max(peak, h)
                    if (peak / entry - 1) * 100 >= act: stop = max(stop, peak * (1 - trailgap / 100))
            else:
                mfe = max(mfe, (entry / l - 1) * 100); mae = min(mae, (entry / h - 1) * 100)
                if policy != "fixed_target" and h >= stop:
                    ex = o if o > stop else stop
                    return pack(day, di, tm, ex, "hard_stop" if abs(stop - init) < 1e-6 else "trail_stop")
                if policy != "trailing" and l <= target:
                    return pack(day, di, tm, target, "target")
                if policy == "trailing":
                    trough = min(trough, l)
                    if (entry / trough - 1) * 100 >= act: stop = min(stop, trough * (1 + trailgap / 100))
    if last is None: return None
    day, di, tm, c = last; return pack(day, di, tm, c, "time")


def route_trend(direction, is_fno, fut, entry_day_str):
    """Multi-day trend: on F&O w/ futures data -> NRML futures (5x, both sides). Else long->CNC(1x); short->None."""
    if is_fno and entry_day_str in fut:
        return "NRML", "FUT", True
    if direction == "up":
        return "CNC", "CASH", True
    return None, None, False                               # multi-day short w/o futures = not executable


def trade_trend(con, stock, F, is_fno, policy, cash, fut):
    pr = con.execute("SELECT pattern_id,outcome_target,rule_json,lift_pct FROM mined_patterns "
                     "WHERE Stock=? AND scope='trend' AND promoted=1", (stock,)).fetchall()
    pats = [{"pid": pid, "target": t, "dir": BAR[t][1], "tp": BAR[t][2], "sl": BAR[t][3], "N": BAR[t][4],
             "conds": [tuple(c) for c in json.loads(rj)], "lift": lf or 0} for pid, t, rj, lf in pr]
    days = list(F[F["year"] == VAULT_YEAR].index)
    trades = []; dropped = 0; i = 0
    while i < len(days) - 1:
        t = days[i]
        firing = [pt for pt in pats if apply_rule(F.loc[[t]], pt["conds"]).iloc[0]]
        if not firing:
            i += 1; continue
        ed = days[i + 1]; ed_str = ed.strftime("%Y-%m-%d"); execs = []
        for pt in firing:
            prod, inst, ok = route_trend(pt["dir"], is_fno, fut, ed_str)
            if not ok: dropped += 1; continue
            execs.append((pt, prod, inst))
        if not execs:
            i += 1; continue
        pt, prod, inst = max(execs, key=lambda x: x[0]["lift"])
        long = (pt["dir"] == "up"); wdays = days[i + 1: i + 1 + pt["N"]]
        bars = fut if inst == "FUT" else cash
        r = simulate_barrier(bars, wdays, long, pt["tp"], pt["sl"], prod, policy)
        if r is None:
            i += 1; continue
        entry, ex = r["entry"], r["exit"]; lev = LEV[prod]; notional = CAPITAL * lev
        gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
        cpct = COST[prod]; roc = (gross - cpct) * lev; qty = int(notional // entry)
        pnl = qty * ((ex - entry) if long else (entry - ex)) - notional * cpct / 100
        trades.append((stock, "trend", policy, pt["pid"], pt["target"], "LONG" if long else "SHORT",
                       pt["tp"], pt["sl"], pt["N"], prod, inst, lev, round(notional, 0),
                       t.strftime("%Y-%m-%d"), ed_str, r["entry_tm"], round(entry, 2), round(r["target"], 2),
                       round(r["stop"], 2), r["exit_day"].strftime("%Y-%m-%d"), r["exit_tm"], round(ex, 2),
                       r["reason"], r["hold"], round(gross, 3), cpct, round(roc, 3), CAPITAL, qty,
                       round(pnl, 1), round(r["mfe"], 2), round(r["mae"], 2), 1 if roc > 0 else 0))
        i = days.index(r["exit_day"]) + 1
    con.executemany("INSERT INTO trade_log_trend VALUES (" + ",".join("?" * 33) + ")", trades)
    con.commit()
    return trades, dropped


def journal(con, stock, policy, trades, dropped):
    if not trades:
        log(f"  [{stock}/{policy:12}] no executable trend trades (dropped {dropped})"); return
    df = pd.DataFrame(trades, columns=["Stock", "scope", "policy", "pid", "tgt", "dir", "tp", "sl", "N", "prod",
        "inst", "lev", "notional", "sig", "entry_d", "entry_t", "entry", "target", "stop", "exit_d", "exit_t",
        "exit", "reason", "hold", "gross", "cost", "roc", "cap", "qty", "pnl", "mfe", "mae", "win"])
    eq = df["pnl"].cumsum(); dd = (eq - eq.cummax()).min()
    gains = df.loc[df.pnl > 0, "pnl"].sum(); losses = -df.loc[df.pnl < 0, "pnl"].sum()
    pf = gains / losses if losses > 0 else float("inf")
    row = (stock, policy, len(df), int(df.win.sum()), round(df.win.mean() * 100, 1),
           round(df.roc.mean(), 3), round(df.pnl.sum(), 0), round(df.pnl.sum() / CAPITAL * 100, 1),
           round(pf, 2), round(df.mfe.mean(), 2), round(df.mae.mean(), 2), round(df.hold.mean(), 1),
           round(dd, 0), round(df.roc.max(), 2), round(df.roc.min(), 2), dropped)
    con.execute("INSERT INTO trade_journal_trend VALUES (" + ",".join("?" * 16) + ")", row); con.commit()
    log(f"  [{stock}/{policy:12}] n={row[2]:>2} win={row[4]:>4}% avgROC={row[5]:+.2f}% "
        f"P&L=Rs{row[6]:>9,.0f} ({row[7]:+.1f}%) PF={row[8]:>4} MFE={row[9]}% MAE={row[10]}% "
        f"hold={row[11]}d maxDD=Rs{row[12]:,.0f} dropped={dropped}")


def ensure(con):
    con.execute("DROP TABLE IF EXISTS trade_log_trend")
    con.execute("""CREATE TABLE trade_log_trend(
        Stock TEXT, scope TEXT, policy TEXT, pattern_id INTEGER, outcome_target TEXT, direction TEXT,
        tp REAL, sl REAL, horizon INTEGER, product_type TEXT, instrument TEXT, leverage REAL, notional REAL,
        signal_date TEXT, entry_date TEXT, entry_time TEXT, entry_px REAL, target_px REAL, stop_px REAL,
        exit_date TEXT, exit_time TEXT, exit_px REAL, exit_reason TEXT, holding_days INTEGER,
        gross_move_pct REAL, cost_pct REAL, net_roc_pct REAL, capital REAL, qty INTEGER, pnl_rs REAL,
        mfe_pct REAL, mae_pct REAL, win INTEGER)""")
    con.execute("DROP TABLE IF EXISTS trade_journal_trend")
    con.execute("""CREATE TABLE trade_journal_trend(
        Stock TEXT, policy TEXT, trades INTEGER, wins INTEGER, win_rate_pct REAL, avg_net_roc_pct REAL,
        total_pnl_rs REAL, return_on_capital_pct REAL, profit_factor REAL, avg_mfe_pct REAL, avg_mae_pct REAL,
        avg_hold_days REAL, max_drawdown_rs REAL, best_trade_roc_pct REAL, worst_trade_roc_pct REAL,
        dropped_not_executable INTEGER)""")
    con.execute("DELETE FROM mined_patterns WHERE scope='trend'")
    con.commit()


def main():
    LOG.write_text("", encoding="utf-8")
    con = sqlite3.connect(str(SNR), timeout=120); ensure(con)
    kc = sqlite3.connect(KDB)
    fno = {s: (kc.execute("SELECT is_fno FROM instrument_labels WHERE symbol=?", (s,)).fetchone() or [0])[0] for s in BATCH}
    kc.close()
    log(f"=== Phase-2 A+C | batch of {len(BATCH)} F&O stocks | trend/barrier family ===")
    log("  pre-flight (F&O + futures-data => leveraged multi-day lane):")
    for s in BATCH:
        log(f"    {s:11} is_fno={fno[s]}")
    for s in BATCH:
        log(f"--- {s} ---")
        F = features(s)
        mine_trend(s, F, con)
        confirm_trend(s, F, con)
        cash = load_cash_1min(s); fut = load_fut_1min_frontmonth(s) if fno[s] else {}
        futdays = len(fut)
        log(f"  [{s}] futures 1-min days available in window: {futdays}")
        for policy in POLICIES:
            tr, dropped = trade_trend(con, s, F, bool(fno[s]), policy, cash, fut)
            journal(con, s, policy, tr, dropped)
    con.close()
    log("PHASE-2 A+C COMPLETE")


if __name__ == "__main__":
    main()
