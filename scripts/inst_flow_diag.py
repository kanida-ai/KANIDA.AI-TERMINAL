"""Troubleshoot the inst-flow engine: is the long/short call BETA (random per-stock) or does it
carry stock-specific edge that the market move is drowning? Measure market-RELATIVE alpha per trade.

For each trade over its hold window:
  stock_raw   = exit_px/entry_px - 1                (actual price change)
  mkt_raw     = nifty_exit/nifty_entry - 1          (NIFTY 50 over the same minutes)
  long_alpha  = stock_raw - mkt_raw                 (>0 = outperformed the market)
  short_alpha = mkt_raw - stock_raw                 (>0 = underperformed -> good for a short)
If BOTH sides show +alpha, the signal picks the right stocks relatively and a market-NEUTRAL
entry (relative strength) will make both sides pay. If alpha ~0 or negative, the signal is random.
"""
import sys, statistics as st
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flow_paper_engine import _con, _load_day
from inst_flow_engine import InstEngine

NIFTY = "NSE:NIFTY 50"


def market_series(minutes):
    s = {}
    for m, of_rows, _ in minutes:
        r = of_rows.get(NIFTY)
        if r and r["close"]:
            s[m] = r["close"]
    return s


def nearest(series, minute):
    if minute in series:
        return series[minute]
    ks = [k for k in series if k <= minute]
    return series[max(ks)] if ks else None


def analyze(date, rs_gate=False, rs_thr=0.002, mode="v1"):
    con = _con(ro=True); minutes = _load_day(con, date); con.close()
    nt = _con(ro=True); has_tick = nt.execute("SELECT count(*) FROM mkt_trades_1min WHERE substr(bar_time,1,10)=?", (date,)).fetchone()[0] > 0; nt.close()
    eng = InstEngine(date, has_tick, rs_gate=rs_gate, rs_thr=rs_thr, mode=mode)
    for m, of, tk in minutes:
        eng.on_minute(m, of, tk)
    eng.post_exit()
    mkt = market_series(minutes)
    day_mkt = (list(mkt.values())[-1] / list(mkt.values())[0] - 1) * 100 if mkt else 0
    for t in eng.trades:
        ne, nx = nearest(mkt, t["entry_time"]), nearest(mkt, t["exit_time"])
        if ne and nx:
            stock_raw = t["exit_px"] / t["entry_px"] - 1
            mkt_raw = nx / ne - 1
            t["mkt_raw_pct"] = mkt_raw * 100
            t["alpha_pct"] = (stock_raw - mkt_raw) * 100 if t["side"] == "long" else (mkt_raw - stock_raw) * 100
        else:
            t["alpha_pct"] = None

    def blk(name, rows):
        rows = [r for r in rows if r.get("alpha_pct") is not None]
        if not rows:
            return f"  {name:12s} n=0"
        a = [r["alpha_pct"] for r in rows]
        pos = sum(1 for x in a if x > 0)
        raw_wr = sum(1 for r in rows if r["net_pnl_rs"] > 0) / len(rows) * 100
        return (f"  {name:12s} n={len(rows):>4d} | RAW WR={raw_wr:>5.1f}%  net=Rs{sum(r['net_pnl_rs'] for r in rows):>11,.0f} "
                f"|| ALPHA mean={st.mean(a):>+6.3f}%  median={st.median(a):>+6.3f}%  alpha>0={pos/len(rows)*100:>4.0f}%")

    tag = f"MODE {mode}" + (f" +RS{rs_thr:+.1%}" if rs_gate else "")
    print(f"\n===== {date}  (NIFTY 50 day move {day_mkt:+.2f}%)  |  {tag} =====")
    print("  [RAW = absolute P&L | ALPHA = market-relative: did the pick beat the market in its own direction?]")
    print(blk("ALL", eng.trades))
    print(blk("LONG", [t for t in eng.trades if t["side"] == "long"]))
    print(blk("SHORT", [t for t in eng.trades if t["side"] == "short"]))
    print(f"  skips: {len(eng.skips)}  (weak-RS: {sum(1 for s in eng.skips if s['reason']=='weak-RS')})")
    return eng.trades


if __name__ == "__main__":
    days = [a for a in sys.argv[1:] if a.startswith("2026")] or ["2026-07-08", "2026-07-09"]
    for d in days:
        analyze(d, mode="v1")
        analyze(d, mode="v2")
