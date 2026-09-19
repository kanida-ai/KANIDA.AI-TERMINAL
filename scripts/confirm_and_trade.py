"""
KANIDA Phase-1b (v3 — leverage-correct + trailing-stop exits):
 (1) confirm promoted patterns on SEALED 2026 -> tag Keep/Watch/Retire/Test in mined_patterns;
 (2) product-type-accurate OOS trade log on 2026 with EXACT 1-min entry/exit times, comparing
     TWO exit policies (fixed_target vs trailing) so we can see the effect on return & drawdown.

 CAPITAL (Rs1,00,000/stock) = MARGIN / CAPITAL DEPLOYED (not notional). Leverage by product:
   MIS  = 5x (intraday equity margin; 15:20 square-off)          [cash 1-min]
   CNC  = 1x (delivery long, or CNC-intraday no-leverage)        [cash 1-min]
   NRML = 5x on stock FUTURES (~20% SPAN+exposure margin; cash overnight short illegal) [fut 1-min]
          * ASSUMPTION: 20% margin -> 5x. Replace with the live broker margin API before real sizing.
          * not F&O / no futures data -> NOT EXECUTABLE, dropped.

 Exit policies:
   fixed_target : exit at nominal target touch, else time / MIS square-off  (baseline)
   trailing     : hard initial stop = 0.6x target; after +0.5x target favorable, trail 0.5x target
                  behind the peak; NO profit cap (let winners run); gap-aware fills; else time / 15:20.

 Run: PYTHONIOENCODING=utf-8 python confirm_and_trade.py
"""
import sys, json, sqlite3
from datetime import datetime
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\scripts")))
from mine_phase1 import features, apply_rule, label, TARGETS

KDB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
SNR = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db")
STOCKS = ["ADANIENT", "CARTRADE"]
VAULT_YEAR = 2026
CAPITAL = 100_000.0                                   # margin / capital deployed per stock
MIS_CUTOFF = "15:20:00"
LEV = {"MIS": 5.0, "CNC": 1.0, "NRML": 5.0}          # leverage by product type
FUT_MARGIN_PCT = 20.0                                 # NRML fut margin assumption -> 5x (flag: use broker API)
COST = {"MIS": 0.08, "CNC": 0.15, "NRML": 0.05}      # round-trip % of NOTIONAL (turnover)
SL_MULT, ACT_MULT, TRAIL_MULT = 0.6, 0.5, 0.5        # trailing params as multiples of target %
STOP_MULT = 1.0                                       # target_stop: protective hard stop = 1.0x target (1:1 R:R)
POLICIES = ["fixed_target", "target_stop", "trailing"]
TGT = {n: (d, p, w) for n, d, p, w in TARGETS}


def log(m): print(f"{datetime.now():%H:%M:%S} {m}", flush=True)


def ensure_cols(con):
    have = [c[1] for c in con.execute("PRAGMA table_info(mined_patterns)").fetchall()]
    for col, typ in [("status", "TEXT"), ("oos2026_n", "INTEGER"), ("oos2026_hits", "INTEGER"),
                     ("oos2026_precision_pct", "REAL"), ("oos2026_base_pct", "REAL"), ("oos2026_lift_pct", "REAL")]:
        if col not in have:
            con.execute(f"ALTER TABLE mined_patterns ADD COLUMN {col} {typ}")
    con.execute("DROP TABLE IF EXISTS trade_log")
    con.execute("""CREATE TABLE trade_log(
        Stock TEXT, policy TEXT, pattern_id INTEGER, outcome_target TEXT, direction TEXT,
        product_type TEXT, instrument TEXT, leverage REAL, notional REAL,
        signal_date TEXT, entry_date TEXT, entry_time TEXT, entry_px REAL, target_px REAL,
        exit_date TEXT, exit_time TEXT, exit_px REAL, exit_reason TEXT, holding_days INTEGER,
        gross_move_pct REAL, cost_pct REAL, net_roc_pct REAL, capital REAL, qty INTEGER, pnl_rs REAL,
        mfe_pct REAL, mae_pct REAL, win INTEGER)""")
    con.execute("DROP TABLE IF EXISTS trade_journal")
    con.execute("""CREATE TABLE trade_journal(
        Stock TEXT, policy TEXT, trades INTEGER, wins INTEGER, win_rate_pct REAL,
        avg_net_roc_pct REAL, total_pnl_rs REAL, return_on_capital_pct REAL, profit_factor REAL,
        avg_mfe_pct REAL, avg_mae_pct REAL, avg_hold_days REAL, max_drawdown_rs REAL,
        best_trade_roc_pct REAL, worst_trade_roc_pct REAL, dropped_not_executable INTEGER)""")
    con.commit()


def confirm_and_tag(con, stock, F):
    rows = con.execute("SELECT pattern_id,outcome_target,rule_json,promoted FROM mined_patterns WHERE Stock=?",
                       (stock,)).fetchall()
    yr = F["year"] == VAULT_YEAR
    counts = {"Keep": 0, "Watch": 0, "Retire": 0, "Test": 0}
    for pid, tname, rjson, promoted in rows:
        if not promoted:
            con.execute("UPDATE mined_patterns SET status='Test' WHERE Stock=? AND pattern_id=?", (stock, pid))
            counts["Test"] += 1; continue
        d, p, w = TGT[tname]; y = label(F, d, p, w); sub = yr & y.notna()
        base = y[sub].mean() * 100
        conds = [tuple(c) for c in json.loads(rjson)]; m = apply_rule(F[sub], conds)
        n = int(m.sum()); hits = int(y[sub][m].sum()) if n else 0
        prec = hits / n * 100 if n else 0.0; lift = prec - base
        status = "Keep" if (n >= 10 and lift >= 3 and prec > base) else ("Watch" if (n >= 3 and lift > 0) else "Retire")
        counts[status] += 1
        con.execute("UPDATE mined_patterns SET status=?, oos2026_n=?, oos2026_hits=?, oos2026_precision_pct=?, "
                    "oos2026_base_pct=?, oos2026_lift_pct=? WHERE Stock=? AND pattern_id=?",
                    (status, n, hits, round(prec, 2), round(base, 2), round(lift, 2), stock, pid))
    con.commit()
    log(f"  [{stock}] tagged: " + " ".join(f"{k}={v}" for k, v in counts.items()))


def load_cash_1min(stock):
    con = sqlite3.connect(KDB)
    df = pd.read_sql("SELECT bar_time,open,high,low,close FROM ohlc_1min WHERE symbol=? AND bar_time>=? ORDER BY bar_time",
                     con, params=[stock, "2025-12-15"]); con.close()
    df["day"] = df["bar_time"].str[:10]; df["tm"] = df["bar_time"].str[11:19]
    return {d: list(zip(g["tm"], g["open"], g["high"], g["low"], g["close"]))
            for d, g in df.groupby("day", sort=False)}


def load_fut_1min_frontmonth(stock):
    con = sqlite3.connect(KDB)
    df = pd.read_sql("SELECT bar_time,expiry,open,high,low,close FROM ohlc_futures_1min WHERE symbol=? ORDER BY bar_time",
                     con, params=[stock]); con.close()
    if df.empty: return {}
    df["day"] = df["bar_time"].str[:10]; df["tm"] = df["bar_time"].str[11:19]; df["exp"] = pd.to_datetime(df["expiry"])
    out = {}
    for day, g in df.groupby("day", sort=False):
        dd = pd.to_datetime(day); valid = g[g["exp"] >= dd]
        if valid.empty: valid = g
        fm = valid["exp"].min(); gg = valid[valid["exp"] == fm]
        out[day] = list(zip(gg["tm"], gg["open"], gg["high"], gg["low"], gg["close"]))
    return out


def simulate(bars, window_days, long, pct, product, policy):
    """Exact-time 1-min exit engine. Returns micro-detail dict or None."""
    d0 = window_days[0].strftime("%Y-%m-%d"); b0 = bars.get(d0)
    if not b0: return None
    entry_tm, entry = b0[0][0], float(b0[0][1])
    if entry <= 0: return None
    mis = (product == "MIS"); days = window_days[:1] if mis else window_days
    mfe, mae, last = -1e9, 1e9, None

    def pack(exd, di, tm, ex, reason):
        return dict(entry=entry, entry_tm=entry_tm, exit_day=exd, hold=di + 1, exit_tm=tm,
                    exit=ex, reason=reason, mfe=mfe, mae=mae)

    if policy == "fixed_target":
        target = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
        for di, day in enumerate(days):
            for (tm, o, h, l, c) in bars.get(day.strftime("%Y-%m-%d"), []):
                if mis and tm > MIS_CUTOFF: continue
                h, l, c = float(h), float(l), float(c); last = (day, di, tm, c)
                if long:
                    mfe = max(mfe, (h / entry - 1) * 100); mae = min(mae, (l / entry - 1) * 100)
                    if h >= target: return pack(day, di, tm, target, "target")
                else:
                    mfe = max(mfe, (entry / l - 1) * 100); mae = min(mae, (entry / h - 1) * 100)
                    if l <= target: return pack(day, di, tm, target, "target")
        if last is None: return None
        day, di, tm, c = last; return pack(day, di, tm, c, "mis_squareoff" if mis else "time")

    if policy == "target_stop":                          # fixed target + protective hard stop (1:1 R:R)
        target = entry * (1 + pct / 100) if long else entry * (1 - pct / 100)
        sl = pct * STOP_MULT
        stop = entry * (1 - sl / 100) if long else entry * (1 + sl / 100)
        for di, day in enumerate(days):
            for (tm, o, h, l, c) in bars.get(day.strftime("%Y-%m-%d"), []):
                if mis and tm > MIS_CUTOFF: continue
                o, h, l, c = float(o), float(h), float(l), float(c); last = (day, di, tm, c)
                if long:
                    mfe = max(mfe, (h / entry - 1) * 100); mae = min(mae, (l / entry - 1) * 100)
                    if l <= stop: return pack(day, di, tm, o if o < stop else stop, "hard_stop")
                    if h >= target: return pack(day, di, tm, target, "target")
                else:
                    mfe = max(mfe, (entry / l - 1) * 100); mae = min(mae, (entry / h - 1) * 100)
                    if h >= stop: return pack(day, di, tm, o if o > stop else stop, "hard_stop")
                    if l <= target: return pack(day, di, tm, target, "target")
        if last is None: return None
        day, di, tm, c = last; return pack(day, di, tm, c, "mis_squareoff" if mis else "time")

    # trailing
    sl, act, trail = pct * SL_MULT, pct * ACT_MULT, pct * TRAIL_MULT
    if long:
        stop = entry * (1 - sl / 100); peak = entry
    else:
        stop = entry * (1 + sl / 100); trough = entry
    init_stop = stop
    for di, day in enumerate(days):
        for (tm, o, h, l, c) in bars.get(day.strftime("%Y-%m-%d"), []):
            if mis and tm > MIS_CUTOFF: continue
            o, h, l, c = float(o), float(h), float(l), float(c); last = (day, di, tm, c)
            if long:
                mfe = max(mfe, (h / entry - 1) * 100); mae = min(mae, (l / entry - 1) * 100)
                if l <= stop:                                    # stop hit (gap-aware fill)
                    ex = o if o < stop else stop
                    return pack(day, di, tm, ex, "hard_stop" if abs(stop - init_stop) < 1e-6 else "trail_stop")
                peak = max(peak, h)
                if (peak / entry - 1) * 100 >= act:
                    stop = max(stop, peak * (1 - trail / 100))
            else:
                mfe = max(mfe, (entry / l - 1) * 100); mae = min(mae, (entry / h - 1) * 100)
                if h >= stop:
                    ex = o if o > stop else stop
                    return pack(day, di, tm, ex, "hard_stop" if abs(stop - init_stop) < 1e-6 else "trail_stop")
                trough = min(trough, l)
                if (entry / trough - 1) * 100 >= act:
                    stop = min(stop, trough * (1 + trail / 100))
    if last is None: return None
    day, di, tm, c = last; return pack(day, di, tm, c, "mis_squareoff" if mis else "time")


def route(direction, w):
    if w == 1: return "MIS", "CASH", False
    if direction == "up": return "CNC", "CASH", False
    return "NRML", "FUT", True


def trade_2026(con, stock, F, is_fno, policy):
    pr = con.execute("SELECT pattern_id,outcome_target,rule_json,lift_pct FROM mined_patterns "
                     "WHERE Stock=? AND promoted=1", (stock,)).fetchall()
    pats = [{"pid": pid, "target": t, "dir": TGT[t][0], "pct": TGT[t][1], "w": TGT[t][2],
             "conds": [tuple(c) for c in json.loads(rj)], "lift": lf or 0} for pid, t, rj, lf in pr]
    cash = load_cash_1min(stock); fut = load_fut_1min_frontmonth(stock) if is_fno else {}
    days = list(F[F["year"] == VAULT_YEAR].index)
    trades = []; dropped = {"no_fno": 0, "no_fut_data": 0}; i = 0
    while i < len(days) - 1:
        t = days[i]
        firing = [pt for pt in pats if apply_rule(F.loc[[t]], pt["conds"]).iloc[0]]
        if not firing:
            i += 1; continue
        ed = days[i + 1]; ed_str = ed.strftime("%Y-%m-%d"); execs = []
        for pt in firing:
            prod, inst, needs_fut = route(pt["dir"], pt["w"])
            if needs_fut:
                if not is_fno: dropped["no_fno"] += 1; continue
                if ed_str not in fut: dropped["no_fut_data"] += 1; continue
            execs.append((pt, prod, inst))
        if not execs:
            i += 1; continue
        pt, prod, inst = max(execs, key=lambda x: x[0]["lift"])
        long = (pt["dir"] == "up"); wdays = days[i + 1: i + 1 + pt["w"]]
        bars = fut if inst == "FUT" else cash
        r = simulate(bars, wdays, long, pt["pct"], prod, policy)
        if r is None:
            i += 1; continue
        entry, ex = r["entry"], r["exit"]; lev = LEV[prod]; notional = CAPITAL * lev
        gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
        cpct = COST[prod]; roc = (gross - cpct) * lev                    # return on capital deployed
        qty = int(notional // entry)
        pnl = qty * ((ex - entry) if long else (entry - ex)) - notional * cpct / 100
        tgt_px = entry * (1 + pt["pct"] / 100) if long else entry * (1 - pt["pct"] / 100)
        trades.append((stock, policy, pt["pid"], pt["target"], "LONG" if long else "SHORT", prod, inst, lev,
                       round(notional, 0), t.strftime("%Y-%m-%d"), ed_str, r["entry_tm"], round(entry, 2),
                       round(tgt_px, 2), r["exit_day"].strftime("%Y-%m-%d"), r["exit_tm"], round(ex, 2), r["reason"],
                       r["hold"], round(gross, 3), cpct, round(roc, 3), CAPITAL, qty, round(pnl, 1),
                       round(r["mfe"], 2), round(r["mae"], 2), 1 if roc > 0 else 0))
        i = days.index(r["exit_day"]) + 1
    con.executemany("INSERT INTO trade_log VALUES (" + ",".join("?" * 28) + ")", trades)
    con.commit()
    return trades, dropped


def journal(con, stock, policy, trades, dropped):
    ndrop = sum(dropped.values())
    if not trades:
        log(f"  [{stock}/{policy}] no executable trades (dropped {ndrop})"); return
    cols = ["Stock", "policy", "pid", "target", "dir", "prod", "inst", "lev", "notional", "sig", "entry_d",
            "entry_t", "entry", "tgt", "exit_d", "exit_t", "exit", "reason", "hold", "gross", "cost", "roc",
            "cap", "qty", "pnl", "mfe", "mae", "win"]
    df = pd.DataFrame(trades, columns=cols)
    eq = df["pnl"].cumsum(); dd = (eq - eq.cummax()).min()
    gains = df.loc[df.pnl > 0, "pnl"].sum(); losses = -df.loc[df.pnl < 0, "pnl"].sum()
    pf = gains / losses if losses > 0 else float("inf")
    row = (stock, policy, len(df), int(df.win.sum()), round(df.win.mean() * 100, 1),
           round(df.roc.mean(), 3), round(df.pnl.sum(), 0), round(df.pnl.sum() / CAPITAL * 100, 1),
           round(pf, 2), round(df.mfe.mean(), 2), round(df.mae.mean(), 2), round(df.hold.mean(), 1),
           round(dd, 0), round(df.roc.max(), 2), round(df.roc.min(), 2), ndrop)
    con.execute("INSERT INTO trade_journal VALUES (" + ",".join("?" * 16) + ")", row); con.commit()
    log(f"  [{stock}/{policy:12}] trades={row[2]:>2} win={row[4]:>4}% avgROC={row[5]:+.2f}% "
        f"P&L=Rs{row[6]:>9,.0f} (ROC {row[7]:+.1f}%) PF={row[8]:>4} MFE={row[9]}% MAE={row[10]}% "
        f"hold={row[11]}d maxDD=Rs{row[12]:,.0f}")
    return df


def main():
    con = sqlite3.connect(str(SNR), timeout=90); ensure_cols(con)
    kc = sqlite3.connect(KDB)
    fno = {s: (kc.execute("SELECT is_fno FROM instrument_labels WHERE symbol=?", (s,)).fetchone() or [0])[0] for s in STOCKS}
    kc.close()
    log("=== (1) confirm on sealed 2026 + tag lifecycle ===")
    feats = {}
    for s in STOCKS:
        F = features(s); feats[s] = F; confirm_and_tag(con, s, F)
    log(f"\n=== (2) OOS trade log 2026 | Rs1L=margin | LEV MIS5x/CNC1x/NRML5x | fixed vs trailing ===")
    for s in STOCKS:
        log(f"  [{s}] is_fno={fno[s]}")
        for policy in POLICIES:
            tr, dropped = trade_2026(con, s, feats[s], bool(fno[s]), policy)
            journal(con, s, policy, tr, dropped)
    con.close()
    log("\nDONE")


if __name__ == "__main__":
    main()
