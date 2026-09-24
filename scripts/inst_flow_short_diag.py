"""What does a PROFITABLE short look like? Slice short-trade market-relative alpha by
family combo and by whether the stock was UP or DOWN vs its open at entry.
Goal: find the subset of shorts that CONTINUE down (positive short-alpha) vs the falling
knives that bounce (negative alpha)."""
import sys, statistics as st
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flow_paper_engine import _con, _load_day
from inst_flow_engine import InstEngine
from inst_flow_diag import market_series, nearest


def run(date):
    con = _con(ro=True); minutes = _load_day(con, date); con.close()
    nt = _con(ro=True); has_tick = nt.execute("SELECT count(*) FROM mkt_trades_1min WHERE substr(bar_time,1,10)=?", (date,)).fetchone()[0] > 0; nt.close()
    eng = InstEngine(date, has_tick)
    for m, of, tk in minutes:
        eng.on_minute(m, of, tk)
    mkt = market_series(minutes)
    for t in eng.trades:
        ne, nx = nearest(mkt, t["entry_time"]), nearest(mkt, t["exit_time"])
        if ne and nx:
            sr = t["exit_px"] / t["entry_px"] - 1; mr = nx / ne - 1
            t["alpha"] = (sr - mr) * 100 if t["side"] == "long" else (mr - sr) * 100
        else:
            t["alpha"] = None

    def show(title, rows):
        rows = [r for r in rows if r.get("alpha") is not None]
        if not rows:
            print(f"    {title:34s} n=0"); return
        a = [r["alpha"] for r in rows]
        net = sum(r["net_pnl_rs"] for r in rows)
        print(f"    {title:34s} n={len(rows):>4d}  alpha={st.mean(a):>+6.3f}%  a>0={sum(1 for x in a if x>0)/len(rows)*100:>3.0f}%  net=Rs{net:>11,.0f}")

    sh = [t for t in eng.trades if t["side"] == "short"]
    lo = [t for t in eng.trades if t["side"] == "long"]
    print(f"\n===== {date}  SHORT anatomy  (n_short={len(sh)}) =====")
    print("  By whether the stock was UP or DOWN vs open at entry:")
    show("short a STOCK THAT WAS UP (>+0.3%)", [t for t in sh if t["entry_move_open_pct"] > 0.3])
    show("short a FLAT stock (-0.3..+0.3%)", [t for t in sh if -0.3 <= t["entry_move_open_pct"] <= 0.3])
    show("short a STOCK ALREADY DOWN (<-0.3%)", [t for t in sh if t["entry_move_open_pct"] < -0.3])
    print("  By family combo (top combos):")
    by = defaultdict(list)
    for t in sh:
        by[t["signal_families"]].append(t)
    for fam, rows in sorted(by.items(), key=lambda kv: -len(kv[1]))[:8]:
        show(fam, rows)
    print("  Does ABSORPTION (distribution) in the short families help?")
    show("short WITH absorption", [t for t in sh if "ABSORPTION" in t["signal_families"]])
    show("short WITHOUT absorption", [t for t in sh if "ABSORPTION" not in t["signal_families"]])
    print("  (LONG reference — by whether stock was up/down at entry:)")
    show("long a STOCK ALREADY UP (>+0.3%)", [t for t in lo if t["entry_move_open_pct"] > 0.3])
    show("long a STOCK THAT WAS DOWN (<-0.3%)", [t for t in lo if t["entry_move_open_pct"] < -0.3])


if __name__ == "__main__":
    for d in sys.argv[1:] or ["2026-07-08", "2026-07-09"]:
        run(d)
