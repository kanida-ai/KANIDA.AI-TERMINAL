"""
INTRADAY TRAILING LAB — for MIS 5x shorts (fixed 5x; the trail is the ONLY drawdown lever, not leverage).
Uses 1-minute bars to simulate, for each short day, what happens AFTER the 09:15 entry and which trailing
exit is best. Compares: no-trail baseline (open->15:20 close) vs the Falcon capital-trail benchmark vs
ATR-trail vs Donchian-trail. All returns are CAPITAL basis at 5x (capital recycles daily under MIS).
Run: PYTHONIOENCODING=utf-8 python arena/intraday_trail.py [SYMBOL]   (default NEWGEN)
"""
import os, sys, csv, sqlite3
from pathlib import Path
from collections import OrderedDict
import numpy as np
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
KDB = str(ROOT / "db" / "kanida.db"); REP = ROOT / "reports"
LEV = 5.0; COST_CAP = 0.30                        # ~Zerodha MIS round-trip on capital basis (price cost x5)
SLIP_CAP = float(os.environ.get("SLIP_CAP", "0"))  # capital-basis slippage per round-trip (env, for stress test)
SQOFF = "15:20"


def short_days(symbol):
    f = REP / "sellable_tradelogs_1min.csv"
    days = sorted({r["entry_date"] for r in csv.DictReader(open(f))
                   if r["symbol"] == symbol and r["order_type"] == "MIS-Daily"})
    return days


def load_days(symbol):
    con = sqlite3.connect(KDB)
    rows = con.execute("SELECT bar_time,open,high,low,close FROM ohlc_1min WHERE symbol=? "
                       "AND bar_time>=? AND bar_time<? ORDER BY bar_time", (symbol, "2025-01-01", "2027-01-01")).fetchall()
    con.close()
    out = {}
    for bt, o, h, l, c in rows:
        d = bt[:10]; out.setdefault(d, {"hm": [], "o": [], "h": [], "l": [], "c": []})
        g = out[d]; g["hm"].append(bt[11:16]); g["o"].append(o); g["h"].append(h); g["l"].append(l); g["c"].append(c)
    for d in out:
        for k in ("o", "h", "l", "c"): out[d][k] = np.array(out[d][k], float)
        out[d]["hm"] = np.array(out[d]["hm"])
    return out


def capshort(E, p): return LEV * (E - p) / E * 100.0        # capital % for a short entered at E, price p


def sim_day(g, method, params):
    """Simulate the day's 5x short under a trailing method (entry=09:15 open). Returns
    (capital_pct_net, exit_price, exit_hm, reason) — net of costs+slippage — or None."""
    hm, o, h, l, c = g["hm"], g["o"], g["h"], g["l"], g["c"]
    if len(o) < 5: return None
    E = float(o[0]); n = len(o); sq = np.where(hm == SQOFF)[0]; end = sq[0] if len(sq) else n - 1
    ex_p = float(c[end]); ex_hm = hm[end]; reason = "square-off"          # default: 15:20
    if method == "capital":
        A, F, G, H = params; armed = False; peak = -1e9
        for i in range(1, end + 1):                                       # arm/trail from 09:16
            cap_hi = capshort(E, h[i])                                    # worst-in-bar (adverse = price up)
            if not armed:
                if cap_hi <= -H: ex_p = E * (1 + H / 500); ex_hm = hm[i]; reason = "hard-stop"; break
            else:
                th = max(F, peak - G)
                if cap_hi <= th: ex_p = E * (1 - th / 500); ex_hm = hm[i]; reason = "trail"; break
            cap_lo = capshort(E, l[i])                                    # best-in-bar
            if cap_lo >= A: armed = True
            peak = max(peak, cap_lo)
    elif method == "atr":
        K, win = params; tr = h - l; atr = np.copy(tr)
        for i in range(1, n): atr[i] = (atr[i - 1] * (win - 1) + tr[i]) / win
        run_low = E
        for i in range(1, end + 1):
            run_low = min(run_low, l[i]); stop = run_low + K * atr[i]     # short trailing stop above the low
            if h[i] >= stop: ex_p = stop; ex_hm = hm[i]; reason = "atr-trail"; break
    elif method == "donchian":
        N = params[0]
        for i in range(1, end + 1):
            lo = max(0, i - N); stop = float(h[lo:i].max()) if i > lo else E
            if h[i] >= stop and i > N: ex_p = stop; ex_hm = hm[i]; reason = "donchian"; break
    cap = capshort(E, float(ex_p)) - COST_CAP - SLIP_CAP                  # net of Zerodha cost + slippage
    return (cap, round(float(ex_p), 2), str(ex_hm), reason)


def stats(daily):
    r = np.array([x[0] if isinstance(x, tuple) else x for x in daily if x is not None])
    eq = np.concatenate([[0.0], np.cumsum(r)]); peak = np.maximum.accumulate(eq)
    dd = (eq - peak)                                          # additive % drawdown (capital basis)
    return dict(n=len(r), total=r.sum(), maxdd=dd.min(), win=(r > 0).mean() * 100,
                avgwin=r[r > 0].mean() if (r > 0).any() else 0, avgloss=r[r <= 0].mean() if (r <= 0).any() else 0)


def main():
    sym = sys.argv[1] if len(sys.argv) > 1 else "NEWGEN"
    days = short_days(sym); data = load_days(sym)
    days = [d for d in days if d in data]
    methods = [("baseline (no trail)", "baseline", None),
               ("capital-trail 6/2/5/3 (Falcon)", "capital", (6, 2, 5, 3)),
               ("capital-trail 8/3/4/2 (tighter)", "capital", (8, 3, 4, 2)),
               ("ATR-trail K=2.0 w=14", "atr", (2.0, 14)),
               ("ATR-trail K=1.2 w=14", "atr", (1.2, 14)),
               ("Donchian-trail N=20", "donchian", (20,)),
               ("Donchian-trail N=40", "donchian", (40,))]
    print(f"=== {sym} · intraday 5x-short trailing lab · {len(days)} short-days · capital basis ===")
    print(f"{'method':<34}{'total%':>9}{'maxDD%':>9}{'ret/DD':>8}{'win%':>7}{'avgWin':>8}{'avgLoss':>8}")
    for name, m, p in methods:
        daily = [sim_day(data[d], m, p) for d in days]
        s = stats(daily); rdd = s["total"] / -s["maxdd"] if s["maxdd"] < 0 else 0
        print(f"{name:<34}{s['total']:>+9.0f}{s['maxdd']:>+9.0f}{rdd:>8.2f}{s['win']:>7.0f}{s['avgwin']:>+8.2f}{s['avgloss']:>+8.2f}")
    print("\n(total% = sum of daily 5x-capital returns; maxDD% on the additive equity; capital recycles daily.)")


if __name__ == "__main__":
    main()
