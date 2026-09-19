"""
SPS_V3 cross-sectional SCANNER — runs the proven pipeline over the whole universe
to find WHICH stocks carry a robust, vault-confirmable intraday edge.

Focused, theory-motivated grid (from the CARTRADE discovery): opening-range /
VWAP-reversion / dry-up families x ATR-regime & VWAP-side filters x a few
targets/stops. Per stock we keep the champion = highest VALIDATION t-stat among
hypotheses that clear train AND val t>=2.5 (net positive). Vault (2026) stays sealed
here; FDR correction + vault confirmation happen in finalize.py.

Resumable + time-boxed:  python scan.py <budget_seconds>
"""
import sys, time, json, sqlite3
from pathlib import Path
from datetime import datetime
import numpy as np
import lab, continuous
from lab import load_min, load_atr, day_pack, TRAIN, VAL

HERE = Path(__file__).resolve().parent
UNIV = (HERE / "universe_scan.txt").read_text().split()

FOCUS_TRIGGERS = ["ORB15", "ORB30", "ORB45", "ORBlate", "VWAPREV", "DRYREV"]
FILTER_COMBOS = [(), ("ATRHI",), ("WITHVWAP",), ("AM",), ("ATRHI", "WITHVWAP"), ("ATRHI", "AM")]
TARGETS = [0.01, 0.015, 0.02]
STOPS = [None, 0.01]


def grid():
    for t in FOCUS_TRIGGERS:
        for fc in FILTER_COMBOS:
            for tg in TARGETS:
                for st in STOPS:
                    yield {"trigger": t, "filters": list(fc), "target": tg, "stop": st, "horizon": "intraday"}


def scan_stock(sym):
    atr = load_atr(sym)
    tr = day_pack(load_min(sym, *TRAIN)); va = day_pack(load_min(sym, *VAL))
    atrs = [atr[d][0] for d in tr if atr.get(d) and atr[d][0] == atr[d][0]]
    if not atrs:
        return None, 0.0
    thr = float(np.nanmedian(atrs))
    best = None
    for hyp in grid():
        rtr = continuous.simulate(sym, tr, atr, hyp, thr)
        if not rtr or rtr["trades"] < 100 or rtr["net_1x"] <= 0 or rtr["tstat"] < 2.5:
            continue
        rva = continuous.simulate(sym, va, atr, hyp, thr)
        if not rva or rva["trades"] < 40 or rva["net_1x"] <= 0 or rva["tstat"] < 2.5:
            continue
        if best is None or rva["tstat"] > best[2]["tstat"]:
            best = (hyp, rtr, rva)
    return best, thr


def main(budget):
    con = sqlite3.connect(str(HERE / "scan_results.db"))
    con.execute("""CREATE TABLE IF NOT EXISTS champ(
        sym TEXT PRIMARY KEY, spec TEXT, thr REAL, tr_json TEXT, va_json TEXT, has_edge INT, ts TEXT)""")
    done = {r[0] for r in con.execute("SELECT sym FROM champ").fetchall()}
    t0 = time.time(); now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for sym in UNIV:
        if sym in done:
            continue
        if time.time() - t0 > budget:
            break
        best, thr = scan_stock(sym)
        if best:
            h, rtr, rva = best
            con.execute("INSERT OR REPLACE INTO champ VALUES(?,?,?,?,?,?,?)",
                        (sym, json.dumps(h), thr, json.dumps(rtr), json.dumps(rva), 1, now))
            print(f"{sym:12} EDGE  {h['trigger']:8} {','.join(h['filters']) or '-':16} "
                  f"tgt{h['target']*100:.1f} stop{h['stop']} | val_t={rva['tstat']:.2f} "
                  f"net1x={rva['net_1x']*100:+.3f}% win={rva['win']*100:.0f}%", flush=True)
        else:
            con.execute("INSERT OR REPLACE INTO champ VALUES(?,?,?,?,?,?,?)",
                        (sym, None, thr, None, None, 0, now))
            print(f"{sym:12} no edge", flush=True)
        con.commit()
    tot = con.execute("SELECT count(*) FROM champ").fetchone()[0]
    ed = con.execute("SELECT count(*) FROM champ WHERE has_edge=1").fetchone()[0]
    print(f"\n--- scanned {tot}/{len(UNIV)} · with train&val t>=2.5 edge: {ed} ---", flush=True)
    con.close()


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 300)
