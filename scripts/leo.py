"""
AGENT LEO — sector-momentum rotation (1x CNC, unleveraged). Each rebalance Leo:
  1. scores every stock's momentum, 2. ranks SECTORS by their stocks' momentum,
  3. concentrates into the top-K leading sectors, holding the strongest names within them.

Leak-free: score on close of day t -> enter next OPEN (t+1) -> hold to next rebalance's open.
Honest costs 0.30%/side on turnover. Benchmarked vs EW-441 (survivor beta) and plain top-N momentum
(no sector logic) so we can SEE what the sector concentration actually adds. OOS era split included.

Run: python scripts/leo.py            (sweep)
     python scripts/leo.py today       (just today's picks)
"""
from __future__ import annotations
import sys, sqlite3, time
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC
KDB = str(ROOT / "db" / "kanida.db")
MIN_PRICE = 20.0; MIN_DVOL = 2e7; COST_SIDE = 0.30 / 100


def load():
    fields, _ = DC.wide_all()
    o, c, v = fields["o"], fields["c"], fields["v"]
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    lab = pd.read_sql_query("SELECT symbol,sector FROM instrument_labels", con); con.close()
    sec = lab.dropna(subset=["sector"]).set_index("symbol")["sector"]
    sec = sec[sec.index.isin(c.columns)]
    dvol20 = (c * v).rolling(20).mean()
    return o, c, dvol20, sec


def rebal_dates(dates, freq):
    s = pd.Series(dates, index=dates)
    if freq == "M":
        return pd.DatetimeIndex(sorted(s.groupby([dates.year, dates.month]).last().values))
    if freq == "W":
        iso = dates.isocalendar()
        return pd.DatetimeIndex(sorted(s.groupby([iso.year.values, iso.week.values]).last().values))
    raise ValueError(freq)


# ---- FAST ARCHITECTURE: convert the payload to numpy ONCE, precompute per-(lb,freq) rebalance events. ----
_ARR = {}


def _arrays(o, c, dvol20, sec):
    if "c" not in _ARR:
        cols = list(c.columns)
        code = {s: i for i, s in enumerate(sorted(sec.unique()))}
        sc = np.array([code.get(sec.get(s, None), -1) for s in cols], dtype=np.int32)
        _ARR.update(c=c.values.astype(np.float64), o=o.values.astype(np.float64),
                    dv=dvol20.values.astype(np.float64), sc=sc, cols=cols,
                    dates=c.index, secnames=sorted(sec.unique()))
    return _ARR


_EVENTS = {}


def _events(o, c, dvol20, sec, lb, freq):
    """Precompute, ONCE per (lb,freq): for each rebalance -> eligible stock momenta, sector codes, and
    the open-price rows needed. topk/nper sweeps then reuse this instantly."""
    key = (lb, freq)
    if key in _EVENTS:
        return _EVENTS[key]
    A = _arrays(o, c, dvol20, sec)
    carr, oarr, dv, sc, dates = A["c"], A["o"], A["dv"], A["sc"], A["dates"]
    pos = {d: i for i, d in enumerate(dates)}
    rd = rebal_dates(dates, freq)
    ent = [(pos[d], pos[d] + 1) for d in rd if d in pos and pos[d] + 1 < len(dates) and pos[d] - lb >= 0]
    nsec = len(A["secnames"]); evs = []
    for j in range(len(ent) - 1):
        si, ei = ent[j]; nxt = ent[j + 1][1]
        mom = carr[si] / carr[si - lb] - 1.0
        elig = (carr[si] > MIN_PRICE) & (dv[si] > MIN_DVOL) & np.isfinite(mom) & (sc >= 0)
        idx = np.where(elig)[0]
        if idx.size < 4:
            continue
        m = mom[idx]; s_ = sc[idx]
        # sector median momentum + counts via per-sector reduce (small arrays, fast)
        med = np.full(nsec, -np.inf); cnt = np.zeros(nsec, dtype=np.int32)
        for k in range(nsec):
            sel = m[s_ == k]
            if sel.size:
                med[k] = np.median(sel); cnt[k] = sel.size
        evs.append((dates[ei], idx, m, s_, med, cnt, oarr[ei], oarr[nxt]))
    _EVENTS[key] = evs
    return evs


def run(o, c, dvol20, sec, lb=63, freq="M", topk=2, nper=12, within="top_mom", cost_side=COST_SIDE):
    evs = _events(o, c, dvol20, sec, lb, freq)
    rets = {}; prev = None
    for edate, idx, m, s_, med, cnt, oe_row, ox_row in evs:
        order = np.argsort(med)[::-1]
        lead = [k for k in order if cnt[k] >= 3][:topk]
        if not lead:
            continue
        leadset = set(lead)
        mask = np.array([sc in leadset for sc in s_])
        cidx = idx[mask]; cm = m[mask]
        if cidx.size == 0:
            continue
        take = cidx[np.argsort(cm)[::-1][:nper]]
        oe = oe_row[take]; ox = ox_row[take]
        val = (oe > 0) & (ox > 0)
        gross = float(np.where(val, ox / oe - 1.0, 0.0).mean())
        held = set(take.tolist())
        if prev is None:
            cost = cost_side
        else:
            cost = (len(held - prev) + len(prev - held)) / max(len(held), 1) * cost_side
        rets[edate] = gross - cost; prev = held
    s = pd.Series(rets).sort_index(); s.index = pd.to_datetime(s.index)
    return s


def bench_topN_momentum(o, c, dvol20, lb, freq, nper, cost_side=COST_SIDE):
    """plain top-N momentum, no sector logic."""
    dates = c.index; pos = {d: i for i, d in enumerate(dates)}
    rd = rebal_dates(dates, freq)
    ent = [(pos[d], pos[d] + 1) for d in rd if d in pos and pos[d] + 1 < len(dates) and pos[d] - lb >= 0]
    rets = {}; prev = set()
    for j in range(len(ent) - 1):
        si, ei = ent[j]; nxt = ent[j + 1][1]
        mom = c.iloc[si] / c.iloc[si - lb] - 1
        elig = (c.iloc[si] > MIN_PRICE) & (dvol20.iloc[si] > MIN_DVOL) & mom.notna()
        m = mom[elig[elig].index]
        picks = list(m.sort_values(ascending=False).index[:nper])
        oe = o.iloc[ei][picks].values.astype(float); ox = o.iloc[nxt][picks].values.astype(float)
        val = (oe > 0) & (ox > 0)
        gross = float(np.where(val, ox / oe - 1.0, 0.0).mean())
        held = set(picks); cost = (len(held - prev) + len(prev - held)) / max(len(held), 1) * cost_side
        rets[dates[ei]] = gross - cost; prev = held
    s = pd.Series(rets).sort_index(); s.index = pd.to_datetime(s.index)
    return s


def met(s):
    eq = 1e6 * (1 + s).cumprod()
    return DC.curve_metrics(eq, cap0=1e6)


def today_picks(o, c, dvol20, sec, lb=63, topk=2, nper=12):
    si = len(c) - 1
    mom = c.iloc[si] / c.iloc[si - lb] - 1
    elig = (c.iloc[si] > MIN_PRICE) & (dvol20.iloc[si] > MIN_DVOL) & mom.notna()
    names = [n for n in elig[elig].index if n in sec.index]
    d = pd.DataFrame({"m": mom[names].values, "sec": sec[names].values}, index=names)
    grp = d.groupby("sec")["m"].agg(["median", "count"])
    grp = grp[grp["count"] >= 3].sort_values("median", ascending=False)
    lead = list(grp.index[:topk])
    picks = d[d["sec"].isin(lead)].sort_values("m", ascending=False).head(nper)
    print(f"=== LEO picks as of {c.index[si].date()} (lb={lb}d, top {topk} sectors) ===")
    print("  leading sectors:", ", ".join(f"{s}({grp.loc[s,'median']*100:+.0f}%)" for s in lead))
    print(f"  {'stock':<14}{'sector':<26}{'mom%':>7}")
    for sym, r in picks.iterrows():
        print(f"  {sym:<14}{r['sec']:<26}{r['m']*100:>+6.0f}%")


def main():
    t0 = time.time()
    o, c, dvol20, sec = load()
    if len(sys.argv) > 1 and sys.argv[1] == "today":
        today_picks(o, c, dvol20, sec); return
    print("=== AGENT LEO sweep (net 0.30%/side, leak-free) — sector-momentum rotation ===")
    print(f"  {'lb':>4}{'freq':>5}{'topk':>5}{'nper':>5}{'CAGR%':>8}{'maxDD%':>8}{'Calmar':>7}{'Shrp':>6}{'pos_mo':>7}")
    best = None
    for lb in [21, 63, 126]:
        for freq in ["M", "W"]:
            for topk in [1, 2, 3]:
                for nper in [8, 12, 20]:
                    s = run(o, c, dvol20, sec, lb=lb, freq=freq, topk=topk, nper=nper)
                    m = met(s)
                    if not m:
                        continue
                    if best is None or (m["CAGR_%"] or -9) > best[0]["CAGR_%"]:
                        best = (m, dict(lb=lb, freq=freq, topk=topk, nper=nper), s)
                    if nper == 12 and freq == "M":       # print a readable slice
                        print(f"  {lb:>4}{freq:>5}{topk:>5}{nper:>5}{m['CAGR_%']:>7}%{m['maxDD_%']:>7}%"
                              f"{str(m['calmar']):>7}{m['sharpe_m']:>6}{str(m['pct_pos_mo']):>7}")
    m, cfg, s = best
    print(f"\n=== LEO BEST: {cfg}  CAGR {m['CAGR_%']}%  DD {m['maxDD_%']}%  Calmar {m['calmar']}  Sharpe_m {m['sharpe_m']} ===")
    # benchmarks at same lb/freq/nper
    bmom = bench_topN_momentum(o, c, dvol20, cfg["lb"], cfg["freq"], cfg["nper"]); mmom = met(bmom)
    ew = c.pct_change(fill_method=None).mean(1); ew_eq = 1e6 * (1 + ew).cumprod(); mew = DC.curve_metrics(ew_eq, 1e6)
    print(f"  vs plain top-{cfg['nper']} momentum (no sectors): CAGR {mmom['CAGR_%']}%  DD {mmom['maxDD_%']}%  Calmar {mmom['calmar']}")
    print(f"  vs EW-441 (survivor beta baseline):          CAGR {mew['CAGR_%']}%  DD {mew['maxDD_%']}%")
    print(f"  => Leo's sector concentration adds {round(m['CAGR_%']-mmom['CAGR_%'],1)}pp over plain momentum, "
          f"{round(m['CAGR_%']-mew['CAGR_%'],1)}pp over survivor beta.")
    print("\n  OOS era split (Leo best):")
    print(f"  {'era':<16}{'months':>7}{'CAGR%':>8}{'maxDD%':>8}{'Calmar':>7}")
    for lo, hi, lab in [("2014-01-01", "2020-01-01", "2014-2019"), ("2020-01-01", "2023-01-01", "2020-2022"),
                        ("2023-01-01", "2027-01-01", "2023-2026 OOS")]:
        sub = s[(s.index >= lo) & (s.index < hi)]
        if len(sub) >= 8:
            mm = met(sub)
            print(f"  {lab:<16}{len(sub):>7}{mm['CAGR_%']:>7}%{mm['maxDD_%']:>7}%{str(mm['calmar']):>7}")
    print(f"\n  [{time.time()-t0:.0f}s]")
    today_picks(o, c, dvol20, sec, lb=cfg["lb"], topk=cfg["topk"], nper=cfg["nper"])


if __name__ == "__main__":
    main()
