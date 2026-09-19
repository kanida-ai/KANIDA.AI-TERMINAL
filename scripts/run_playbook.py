"""
KANIDA — canonical per-stock PLAYBOOK executor (v1, dual order-type).
Runs the frozen playbook (docs/KANIDA_STOCK_PLAYBOOK.md) for each stock, independently:
  mine reversion touch-targets (<=2024) -> promote (2025) -> confirm on SEALED 2026 + tag
  Keep/Watch/Retire/Test -> trade fixed_target with real product-type routing, leverage-correct,
  exact 1-min timing -> DUAL intraday lane (MIS 5x AND CNC 1x shown side-by-side) -> verdict.

Rs1,00,000 = margin deployed. Intraday(1d)=MIS or CNC (both computed). long>1d=CNC(1x).
short>1d=NRML futures(5x); not F&O/no futures -> NOT EXECUTABLE. Never uses 2026 for selection.

Usage:
  python run_playbook.py ADANIENT CARTRADE      # run just these (replaces their rows)
  python run_playbook.py                        # full F&O universe (full history), resumable
  python run_playbook.py --fresh                # recreate tables then run full universe
Logs to db/run_playbook.log
"""
import sys, json, sqlite3
from datetime import datetime
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(r"C:\Users\SPS\Documents\Kanida_Falcon\scripts")))
from mine_phase1 import features, mine_stock, apply_rule, label, TARGETS
from confirm_and_trade import load_cash_1min, load_fut_1min_frontmonth, route, simulate, LEV, COST, CAPITAL

KDB = r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db"
SNR = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db")
VAULT_YEAR = 2026
POLICY = "fixed_target"
TGT = {n: (d, p, w) for n, d, p, w in TARGETS}
LOG = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\run_playbook.log")
TRADEABLE_PF, TRADEABLE_ROC = 1.2, 0.0            # verdict thresholds on sealed-2026 MIS lane


def log(m):
    line = f"{datetime.now():%H:%M:%S} {m}"; print(line, flush=True)
    with open(LOG, "a", encoding="utf-8") as f: f.write(line + "\n")


def ensure(con, fresh):
    if fresh:
        for t in ("trade_log_rev", "trade_journal_rev", "stock_verdict"):
            con.execute(f"DROP TABLE IF EXISTS {t}")
    con.execute("""CREATE TABLE IF NOT EXISTS trade_log_rev(
        Stock TEXT, pattern_id INTEGER, outcome_target TEXT, direction TEXT,
        signal_date TEXT, entry_date TEXT, entry_time TEXT, entry_px REAL, target_px REAL,
        exit_date TEXT, exit_time TEXT, exit_px REAL, exit_reason TEXT, holding_days INTEGER,
        gross_move_pct REAL, mfe_pct REAL, mae_pct REAL,
        laneA_product TEXT, laneA_leverage REAL, laneA_cost_pct REAL, laneA_net_roc_pct REAL,
        laneA_qty INTEGER, laneA_pnl_rs REAL, laneA_win INTEGER,
        laneB_product TEXT, laneB_leverage REAL, laneB_cost_pct REAL, laneB_net_roc_pct REAL,
        laneB_qty INTEGER, laneB_pnl_rs REAL)""")
    con.execute("""CREATE TABLE IF NOT EXISTS trade_journal_rev(
        Stock TEXT PRIMARY KEY, trades INTEGER, intraday_n INTEGER, multiday_n INTEGER,
        intraday_mis_pnl_rs REAL, intraday_mis_roc_pct REAL, intraday_cnc_pnl_rs REAL, intraday_cnc_roc_pct REAL,
        multiday_pnl_rs REAL, multiday_roc_pct REAL, total_mis_lane_roc_pct REAL, total_cnc_lane_roc_pct REAL,
        win_rate_pct REAL, profit_factor REAL, avg_mfe_pct REAL, avg_mae_pct REAL, avg_hold_days REAL,
        max_drawdown_rs REAL, keep_patterns INTEGER, dropped_not_executable INTEGER, verdict TEXT)""")
    con.execute("""CREATE TABLE IF NOT EXISTS stock_verdict(
        Stock TEXT PRIMARY KEY, is_fno INTEGER, verdict TEXT, total_mis_lane_roc_pct REAL,
        total_cnc_lane_roc_pct REAL, profit_factor REAL, win_rate_pct REAL, keep_patterns INTEGER,
        trades INTEGER, evaluated_on TEXT)""")
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

    def lane(prod, entry, ex, long, gross):
        lev = LEV[prod]; cost = COST[prod]; notional = CAPITAL * lev; qty = int(notional // entry)
        roc = (gross - cost) * lev
        pnl = qty * ((ex - entry) if long else (entry - ex)) - notional * cost / 100
        return (prod, lev, cost, round(roc, 3), qty, round(pnl, 1))

    while i < len(days) - 1:
        t = days[i]
        firing = [pt for pt in pats if apply_rule(F.loc[[t]], pt["conds"]).iloc[0]]
        if not firing:
            i += 1; continue
        ed = days[i + 1]; ed_str = ed.strftime("%Y-%m-%d"); execs = []
        for pt in firing:
            prod, inst, needs_fut = route(pt["dir"], pt["w"])
            if needs_fut and (not is_fno or ed_str not in fut): dropped += 1; continue
            execs.append((pt, prod, inst))
        if not execs:
            i += 1; continue
        pt, prod, inst = max(execs, key=lambda x: x[0]["lift"])
        long = (pt["dir"] == "up"); wdays = days[i + 1: i + 1 + pt["w"]]
        bars = fut if inst == "FUT" else cash
        r = simulate(bars, wdays, long, pt["pct"], prod, POLICY)
        if r is None:
            i += 1; continue
        entry, ex = r["entry"], r["exit"]
        gross = (ex / entry - 1) * 100 if long else (1 - ex / entry) * 100
        if prod == "MIS":                                 # intraday -> DUAL lanes
            A = lane("MIS", entry, ex, long, gross); B = lane("CNC", entry, ex, long, gross)
        else:                                             # multi-day: single lane
            A = lane(prod, entry, ex, long, gross); B = (None, None, None, None, None, None)
        tgt_px = entry * (1 + pt["pct"] / 100) if long else entry * (1 - pt["pct"] / 100)
        trades.append((stock, pt["pid"], pt["target"], "LONG" if long else "SHORT",
                       t.strftime("%Y-%m-%d"), ed_str, r["entry_tm"], round(entry, 2), round(tgt_px, 2),
                       r["exit_day"].strftime("%Y-%m-%d"), r["exit_tm"], round(ex, 2), r["reason"], r["hold"],
                       round(gross, 3), round(r["mfe"], 2), round(r["mae"], 2),
                       A[0], A[1], A[2], A[3], A[4], A[5], 1 if A[3] > 0 else 0,
                       B[0], B[1], B[2], B[3], B[4], B[5]))
        i = days.index(r["exit_day"]) + 1
    con.execute("DELETE FROM trade_log_rev WHERE Stock=?", (stock,))
    con.executemany("INSERT INTO trade_log_rev VALUES (" + ",".join("?" * 30) + ")", trades)
    con.commit(); return trades, dropped


def journal(con, stock, is_fno, trades, dropped, keep):
    con.execute("DELETE FROM trade_journal_rev WHERE Stock=?", (stock,))
    con.execute("DELETE FROM stock_verdict WHERE Stock=?", (stock,))
    if not trades:
        con.execute("INSERT INTO stock_verdict VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (stock, is_fno, "Park", 0, 0, 0, 0, keep, 0, str(VAULT_YEAR)))
        con.commit(); log(f"  [{stock:11}] no executable trades -> Park (Keep={keep}, dropped {dropped})"); return
    C = ["Stock", "pid", "tgt", "dir", "sig", "entry_d", "entry_t", "entry", "tgtpx", "exit_d", "exit_t",
         "exit", "reason", "hold", "gross", "mfe", "mae", "Ap", "Alev", "Acost", "Aroc", "Aqty", "Apnl",
         "Awin", "Bp", "Blev", "Bcost", "Broc", "Bqty", "Bpnl"]
    df = pd.DataFrame(trades, columns=C)
    intra = df[df.Ap == "MIS"]; multi = df[df.Ap != "MIS"]
    im_pnl = intra.Apnl.sum(); ic_pnl = intra.Bpnl.sum(); m_pnl = multi.Apnl.sum()
    tot_mis = im_pnl + m_pnl; tot_cnc = ic_pnl + m_pnl
    eq = df.Apnl.cumsum(); dd = (eq - eq.cummax()).min()
    gains = df.loc[df.Apnl > 0, "Apnl"].sum(); losses = -df.loc[df.Apnl < 0, "Apnl"].sum()
    pf = round(gains / losses, 2) if losses > 0 else 999.0
    win = round(df.Awin.mean() * 100, 1)
    verdict = "Tradeable" if (tot_mis / CAPITAL * 100 > TRADEABLE_ROC and pf >= TRADEABLE_PF) else "Park"
    row = (stock, len(df), len(intra), len(multi), round(im_pnl, 0), round(im_pnl / CAPITAL * 100, 1),
           round(ic_pnl, 0), round(ic_pnl / CAPITAL * 100, 1), round(m_pnl, 0), round(m_pnl / CAPITAL * 100, 1),
           round(tot_mis / CAPITAL * 100, 1), round(tot_cnc / CAPITAL * 100, 1), win, pf,
           round(df.mfe.mean(), 2), round(df.mae.mean(), 2), round(df.hold.mean(), 1), round(dd, 0),
           keep, dropped, verdict)
    con.execute("INSERT INTO trade_journal_rev VALUES (" + ",".join("?" * 21) + ")", row)
    con.execute("INSERT INTO stock_verdict VALUES (?,?,?,?,?,?,?,?,?,?)",
                (stock, is_fno, verdict, row[10], row[11], pf, win, keep, len(df), str(VAULT_YEAR)))
    con.commit()
    log(f"  [{stock:11}] n={row[1]:>2} (intra {row[2]}/multi {row[3]}) | INTRADAY MIS {row[5]:+.1f}% vs CNC {row[7]:+.1f}% "
        f"| multiday {row[9]:+.1f}% | TOTAL MIS-lane {row[10]:+.1f}% / CNC-lane {row[11]:+.1f}% | "
        f"win {win}% PF {pf} DD Rs{row[17]:,.0f} Keep={keep} -> {verdict}")


def universe(con):
    kc = sqlite3.connect(KDB)
    fno = [r[0] for r in kc.execute("SELECT symbol FROM instrument_labels WHERE is_fno=1").fetchall()]
    cov = {r[0]: (r[1], r[2]) for r in kc.execute(
        "SELECT symbol,min(bar_time),max(bar_time) FROM ohlc_daily GROUP BY symbol").fetchall()}
    kc.close()
    return sorted([s for s in fno if s in cov and cov[s][0][:10] <= "2020-01-02" and cov[s][1][:10] >= "2026-07-25"])


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    fresh = "--fresh" in sys.argv
    LOG.write_text("", encoding="utf-8")
    con = sqlite3.connect(str(SNR), timeout=120); ensure(con, fresh or bool(args))
    kc = sqlite3.connect(KDB)
    stocks = args or universe(con)
    fno = {s: (kc.execute("SELECT is_fno FROM instrument_labels WHERE symbol=?", (s,)).fetchone() or [0])[0] for s in stocks}
    kc.close()
    done = set() if (args or fresh) else set(r[0] for r in con.execute("SELECT Stock FROM stock_verdict").fetchall())
    todo = [s for s in stocks if s not in done]
    log(f"=== PLAYBOOK run | {len(todo)} stocks (skipping {len(done)} done) | Rs1L margin | dual MIS/CNC intraday ===")
    for n, s in enumerate(todo, 1):
        try:
            log(f"--- [{n}/{len(todo)}] {s} (is_fno={fno[s]}) ---")
            F = features(s)
            con.execute("DELETE FROM mined_patterns WHERE Stock=? AND scope='stock_specific'", (s,)); con.commit()
            mine_stock(s, con)
            keep = confirm(con, s, F)
            cash = load_cash_1min(s); fut = load_fut_1min_frontmonth(s) if fno[s] else {}
            tr, dropped = trade(con, s, F, bool(fno[s]), cash, fut)
            journal(con, s, fno[s], tr, dropped, keep)
        except Exception as e:
            log(f"  [{s}] ERROR: {str(e)[:160]}")
    # summary
    v = pd.read_sql("SELECT verdict,count(*) n FROM stock_verdict GROUP BY verdict", con)
    log("VERDICT TALLY: " + " ".join(f"{r.verdict}={r.n}" for _, r in v.iterrows()))
    con.close(); log("PLAYBOOK RUN COMPLETE")


if __name__ == "__main__":
    main()
