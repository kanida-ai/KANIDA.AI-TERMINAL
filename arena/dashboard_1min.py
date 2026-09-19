"""
Rebuild the Worker Performance dashboard on the 1-MINUTE tick engine (new source of truth).
Parallel 1-min backtest over the whole universe -> roll legs into positions -> reuse worker_cards.kpis for
the card + worker_cards.render_html for the page. Sellability (sustained net expectancy, +1%/trade) is
RE-EVALUATED on the 1-min book. Targets the sub-minute milestone for the sellable set; full 441 ~2 min.
Run: PYTHONIOENCODING=utf-8 python arena/dashboard_1min.py
"""
import os, sys, json, sqlite3, time
os.environ.setdefault("OMP_NUM_THREADS", "1")
from pathlib import Path
from collections import OrderedDict
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "arena")); sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "kanida_engine"))
import backtest_1min as B
import worker_cards as W

KDB = str(ROOT / "db" / "kanida.db"); SNR = str(ROOT / "db" / "KANIDA_SNR.db"); REP = ROOT / "reports"
MARGIN = B.MARGIN; MIN_OCC_EVAL = 8


def card_from_legs(sym, legs, keptn):
    # INVESTED-CASH basis: each daily order (MIS-Daily leg or single trade) IS a trade — so return,
    # drawdown, monthly and trade-count are all at the DAILY level the trader actually experiences.
    trades = [{"entry": pd.Timestamp(str(r["entry_date"])), "date": pd.Timestamp(str(r["exit_date"])),
               "roc": r["net_pnl_rs"] / MARGIN * 100, "order": r["order_type"],
               "direction": r["direction"], "hold": int(r.get("hold", 1))} for r in legs]
    if len(trades) < 20: return None
    tr = pd.DataFrame(trades); tr["yr"] = pd.to_datetime(tr.date).dt.year
    y25 = tr[tr.yr == 2025].roc; y26 = tr[tr.yr == 2026].roc
    sustained = (len(y25) >= 20 and len(y26) >= 12 and float(y25.sum()) > 0 and float(y26.sum()) > 0)
    card = W.kpis(trades, keptn, sustained, 1.0)
    if not card: return None
    card["symbol"] = sym
    card["ret_dd"] = round(card["total_return"] / -card["max_dd"], 1) if card["max_dd"] < 0 else 99.0
    card["status"] = "Healthy" if (sustained and card["ret_dd"] >= 10) else ("Watch" if sustained else "Caution")
    return card


def main():
    T0 = time.time()
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]; con.close()
    tasks = [(s, int(fno.get(s, 0))) for s in syms]
    cards = []
    import multiprocessing as mp
    with mp.get_context("spawn").Pool(8) as pool:
        for sym, legs, keptn in pool.imap_unordered(B.bt_worker, tasks):
            if not legs: continue
            c = card_from_legs(sym, legs, keptn)
            if c: cards.append(c)
    cards.sort(key=lambda c: (c["sustained"] and (c["min_netexp"] or 0) >= 1.0, c["net_exp"]), reverse=True)
    (REP / "worker_cards.json").write_text(json.dumps(cards), encoding="utf-8")
    W.render_html(cards)
    sell = sum(1 for c in cards if c["min_netexp"] is not None and c["sustained"] and c["min_netexp"] >= 1.0)
    dt = time.time() - T0
    print(f"1-MIN DASHBOARD: {len(cards)} worker cards | sellable(>=+1%): {sell} | {dt:.1f}s "
          f"[{'PASS' if dt < 300 else 'OVER'} 5-min ceiling]")
    print("  written: docs/kanida_workers.html + reports/worker_cards.json (now 1-min source of truth)")


if __name__ == "__main__":
    main()
