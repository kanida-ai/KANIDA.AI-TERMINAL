"""
KANIDA Phase-2 (C, reversion): roll the PROVEN reversion touch-target family + leverage-correct,
product-type-accurate, fixed_target exit engine across the 12-stock F&O batch, to test whether the
ADANIENT +81% edge GENERALIZES or is stock-specific. Scoped to scope='stock_specific' so it never
touches the trend-family rows. Writes trade_log_rev / trade_journal_rev + tags status on 2026.

Run: PYTHONIOENCODING=utf-8 python phase2_reversion_batch.py   (logs to db/phase2_rev.log)
"""
import sys, json, sqlite3
from datetime import datetime
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\scripts")))
from mine_phase1 import features, mine_stock, apply_rule, label, TARGETS
from confirm_and_trade import (load_cash_1min, load_fut_1min_frontmonth, route, simulate,
                               LEV, COST, CAPITAL)

KDB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
SNR = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db")
BATCH = ["ADANIENT", "RELIANCE", "ICICIBANK", "BHARTIARTL", "INFY", "M&M",
         "BEL", "HAL", "DIXON", "TRENT", "MARUTI", "TATASTEEL"]
VAULT_YEAR = 2026
POLICY = "fixed_target"                               # proven best for reversion touch-targets
TGT = {n: (d, p, w) for n, d, p, w in TARGETS}
LOG = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\phase2_rev.log")


def log(m):
    line = f"{datetime.now():%H:%M:%S} {m}"; print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f: f.write(line + "\n")


def ensure(con):
    con.execute("DROP TABLE IF EXISTS trade_log_rev")
    con.execute("""CREATE TABLE trade_log_rev(
        Stock TEXT, policy TEXT, pattern_id INTEGER, outcome_target TEXT, direction TEXT,
        product_type TEXT, instrument TEXT, leverage REAL, notional REAL,
        signal_date TEXT, entry_date TEXT, entry_time TEXT, entry_px REAL, target_px REAL,
        exit_date TEXT, exit_time TEXT, exit_px REAL, exit_reason TEXT, holding_days INTEGER,
        gross_move_pct REAL, cost_pct REAL, net_roc_pct REAL, capital REAL, qty INTEGER, pnl_rs REAL,
        mfe_pct REAL, mae_pct REAL, win INTEGER)""")
    con.execute("DROP TABLE IF EXISTS trade_journal_rev")
    con.execute("""CREATE TABLE trade_journal_rev(
        Stock TEXT, policy TEXT, trades INTEGER, wins INTEGER, win_rate_pct REAL, avg_net_roc_pct REAL,
        total_pnl_rs REAL, return_on_capital_pct REAL, profit_factor REAL, avg_mfe_pct REAL,
        avg_mae_pct REAL, avg_hold_days REAL, max_drawdown_rs REAL, best_roc_pct REAL, worst_roc_pct REAL,
        keep_patterns INTEGER, dropped_not_executable INTEGER)""")
    con.commit()


def confirm(con, stock, F):
    rows = con.execute("SELECT pattern_id,outcome_target,rule_json,promoted FROM mined_patterns "
                       "WHERE Stock=? AND scope='stock_specific'", (stock,)).fetchall()
    yr = F["year"] == VAULT_YEAR; keep = 0
    for pid, tname, rjson, promoted in rows:
        if not promoted:
            con.execute("UPDATE mined_patterns SET status='Test' WHERE Stock=? AND pattern_id=? AND outcome_target=?",
                        (stock, pid, tname)); continue
        d, p, w = TGT[tname]; y = label(F, d, p, w); sub = yr & y.notna(); base = y[sub].mean() * 100
        conds = [tuple(c) for c in json.loads(rjson)]; m = apply_rule(F[sub], conds)
        n = int(m.sum()); hits = int(y[sub][m].sum()) if n else 0
        prec = hits / n * 100 if n else 0.0; lift = prec - base
        status = "Keep" if (n >= 10 and lift >= 3 and prec > base) else ("Watch" if (n >= 3 and lift > 0) else "Retire")
        keep += (status == "Keep")
        con.execute("UPDATE mined_patterns SET status=?, oos2026_n=?, oos2026_hits=?, oos2026_precision_pct=?, "
                    "oos2026_base_pct=?, oos2026_lift_pct=? WHERE Stock=? AND pattern_id=? AND outcome_target=?",
                    (status, n, hits, round(prec, 2), round(base, 2), round(lift, 2), stock, pid, tname))
    con.commit(); return keep


def trade(con, stock, F, is_fno, cash, fut):
    pr = con.execute("SELECT pattern_id,outcome_target,rule_json,lift_pct FROM mined_patterns "
                     "WHERE Stock=? AND scope='stock_specific' AND promoted=1", (stock,)).fetchall()
    pats = [{"pid": pid, "target": t, "dir": TGT[t][0], "pct": TGT[t][1], "w": TGT[t][2],
             "conds": [tuple(c) for c in json.loads(rj)], "lift": lf or 0} for pid, t, rj, lf in pr]
    days = list(F[F["year"] == VAULT_YEAR].index); trades = []; dropped = 0; i = 0
    while i < len(days) - 1:
        t = days[i]
        firing = [pt for pt in pats if apply_rule(F.loc[[t]], pt["conds"]).iloc[0]]
        if not firing:
            i += 1; continue
        ed = days[i + 1]; ed_str = ed.strftime("%Y-%m-%d"); execs = []
        for pt in firing:
            prod, inst, needs_fut = route(pt["dir"], pt["w"])
            if needs_fut:
                if not is_fno or ed_str not in fut: dropped += 1; continue
            execs.append((pt, prod, inst))
        if not execs:
            i += 1; continue
        pt, prod, inst = max(execs, key=lambda x: x[0]["lift"])
        long = (pt["dir"] == "up"); wdays = days[i + 1: i + 1 + pt["w"]]
        bars = fut if inst == "FUT" else cash
        r = simulate(bars, wdays, long, pt["pct"], prod, POLICY)
        if r is None:
            i += 1; continue
        entry, ex = r["entry"], r["exit"]; lev = LEV[prod]; notional = CAPITAL * lev
        gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
        cpct = COST[prod]; roc = (gross - cpct) * lev; qty = int(notional // entry)
        pnl = qty * ((ex - entry) if long else (entry - ex)) - notional * cpct / 100
        tgt_px = entry * (1 + pt["pct"] / 100) if long else entry * (1 - pt["pct"] / 100)
        trades.append((stock, POLICY, pt["pid"], pt["target"], "LONG" if long else "SHORT", prod, inst, lev,
                       round(notional, 0), t.strftime("%Y-%m-%d"), ed_str, r["entry_tm"], round(entry, 2),
                       round(tgt_px, 2), r["exit_day"].strftime("%Y-%m-%d"), r["exit_tm"], round(ex, 2), r["reason"],
                       r["hold"], round(gross, 3), cpct, round(roc, 3), CAPITAL, qty, round(pnl, 1),
                       round(r["mfe"], 2), round(r["mae"], 2), 1 if roc > 0 else 0))
        i = days.index(r["exit_day"]) + 1
    con.executemany("INSERT INTO trade_log_rev VALUES (" + ",".join("?" * 28) + ")", trades)
    con.commit(); return trades, dropped


def journal(con, stock, trades, dropped, keep):
    if not trades:
        log(f"  [{stock:11}] no executable reversion trades (Keep={keep}, dropped {dropped})"); return
    df = pd.DataFrame(trades, columns=["Stock", "policy", "pid", "target", "dir", "prod", "inst", "lev", "notional",
        "sig", "entry_d", "entry_t", "entry", "tgt", "exit_d", "exit_t", "exit", "reason", "hold", "gross",
        "cost", "roc", "cap", "qty", "pnl", "mfe", "mae", "win"])
    eq = df["pnl"].cumsum(); dd = (eq - eq.cummax()).min()
    gains = df.loc[df.pnl > 0, "pnl"].sum(); losses = -df.loc[df.pnl < 0, "pnl"].sum()
    pf = gains / losses if losses > 0 else float("inf")
    row = (stock, POLICY, len(df), int(df.win.sum()), round(df.win.mean() * 100, 1), round(df.roc.mean(), 3),
           round(df.pnl.sum(), 0), round(df.pnl.sum() / CAPITAL * 100, 1), round(pf, 2), round(df.mfe.mean(), 2),
           round(df.mae.mean(), 2), round(df.hold.mean(), 1), round(dd, 0), round(df.roc.max(), 2),
           round(df.roc.min(), 2), keep, dropped)
    con.execute("INSERT INTO trade_journal_rev VALUES (" + ",".join("?" * 17) + ")", row); con.commit()
    log(f"  [{stock:11}] n={row[2]:>2} win={row[4]:>4}% avgROC={row[5]:+.2f}% P&L=Rs{row[6]:>9,.0f} "
        f"({row[7]:+.1f}%) PF={row[8]:>4} MFE={row[9]}% MAE={row[10]}% hold={row[11]}d "
        f"maxDD=Rs{row[12]:,.0f} Keep={keep} dropped={dropped}")


def main():
    LOG.write_text("", encoding="utf-8")
    con = sqlite3.connect(str(SNR), timeout=120); ensure(con)
    kc = sqlite3.connect(KDB)
    fno = {s: (kc.execute("SELECT is_fno FROM instrument_labels WHERE symbol=?", (s,)).fetchone() or [0])[0] for s in BATCH}
    kc.close()
    log(f"=== Phase-2 C (reversion) | {len(BATCH)} stocks | Rs1L margin | MIS5x/CNC1x/NRML5x | {POLICY} ===")
    for s in BATCH:
        log(f"--- {s} (is_fno={fno[s]}) ---")
        F = features(s)
        con.execute("DELETE FROM mined_patterns WHERE Stock=? AND scope='stock_specific'", (s,)); con.commit()
        mine_stock(s, con)                                # writes scope='stock_specific'
        keep = confirm(con, s, F)
        cash = load_cash_1min(s); fut = load_fut_1min_frontmonth(s) if fno[s] else {}
        tr, dropped = trade(con, s, F, bool(fno[s]), cash, fut)
        journal(con, s, tr, dropped, keep)
    con.close(); log("PHASE-2 C (reversion) COMPLETE")


if __name__ == "__main__":
    main()
