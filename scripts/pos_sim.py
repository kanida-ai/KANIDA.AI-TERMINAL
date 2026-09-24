"""Positional simulation engine — consumes _pos_dataset.pkl (per signal day: Top-5
concatenated 1-min paths across D..D+7). Everything works on the 1-MINUTE basket path,
so the trail sees intraday favorable/adverse movement on every hold day and overnight
gaps are the natural jump at each day's first bar.

Conventions (match the validated intraday work):
  entry = 09:15 open on D; qty = floor((5L/nstk)/entry); return on DEPLOYED capital.
  basket ret%(t) = (C[t] @ qty - dep)/dep*100  (close path, for decisions)
  fills = next-bar OPEN (retO[i+1]); across a day boundary that IS next session's
          09:15 open -> overnight gaps are filled at the real gap price.
  charges = (deployed + exit_value) * CHARGE_SIDE  ->  ~0.10% round-trip, netted.
"""
import pickle
from pathlib import Path
import numpy as np

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
DS = ROOT / "docs" / "ops" / "_pos_dataset.pkl"
ASSIGNED = 500000.0
CHARGE_SIDE = 0.0005


def load():
    return pickle.load(open(DS, "rb"))


def paths(M):
    """Return (retC, retO) basket return% paths on close and open."""
    q = M["qty"].astype(np.float64); dep = M["dep"]
    retC = (M["C"].astype(np.float64) @ q - dep) / dep * 100.0
    retO = (M["O"].astype(np.float64) @ q - dep) / dep * 100.0
    return retC, retO


def _net(gross_ret, dep):
    """Convert a GROSS return% on deployed to NET% after round-trip charges."""
    exit_val = dep * (1 + gross_ret / 100.0)
    charges = (dep + exit_val) * CHARGE_SIDE
    return gross_ret - charges / dep * 100.0


def mfe_mae_by_day(M):
    """Per hold-day: intraday MFE (max favorable) / MAE (max adverse) / day-open / day-close
    of the basket return%, plus the overnight gap into that day."""
    retC, retO = paths(M)
    day = M["day"]; out = []
    prev_close = 0.0
    for k in range(int(day.max()) + 1):
        m = day == k
        sl = retC[m]; slo = retO[m]
        gap = float(slo[0] - prev_close)          # overnight gap (ret pts) into day k (0 for day0)
        out.append(dict(hold_day=k, day_open=float(slo[0]), day_close=float(sl[-1]),
                        mfe=float(sl.max()), mae=float(sl.min()),
                        gap=(gap if k > 0 else 0.0)))
        prev_close = float(sl[-1])
    return out


def fixed_hold(M, N):
    """Exit at the CLOSE of hold-day N (or the last available day). No trail. NET%."""
    retC, _ = paths(M)
    day = M["day"]
    kmax = int(day.max())
    k = min(N, kmax)
    idx = np.where(day == k)[0][-1]
    return _net(float(retC[idx]), M["dep"]), k


def basket_trail(M, arm, floor, give, stop, max_hold):
    """Basket arm-and-trail across the multi-day 1-min path, capped at max_hold sessions.
    Returns dict(net, gross, reason, exit_day, exit_idx)."""
    retC, retO = paths(M)
    day = M["day"]; eod = M["eod"]
    kcap = min(max_hold, int(day.max()))
    last = int(eod[kcap])                       # last actionable bar (square-off point)

    def fill(i):
        j = i + 1
        return float(retO[j]) if j <= last else float(retC[i])

    armed = False; peak = None
    for i in range(last):
        r = float(retC[i])
        if r <= -stop:
            g = fill(i); return dict(net=_net(g, M["dep"]), gross=g, reason="STOP",
                                     exit_day=int(day[i]), exit_idx=i)
        if not armed:
            if r >= arm:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        thr = max(floor, peak - give)
        if r <= thr:
            g = fill(i); reason = "FLOOR" if thr == floor else "TRAIL"
            return dict(net=_net(g, M["dep"]), gross=g, reason=reason,
                        exit_day=int(day[i]), exit_idx=i)
    g = float(retC[last])                        # time exit (max hold) at square-off close
    return dict(net=_net(g, M["dep"]), gross=g, reason="TIME", exit_day=int(day[last]), exit_idx=last)


def perstock_trail(M, pstop, arm, floor, give, stop, max_hold):
    """Two-layer: each stock has a software stop at -pstop% (fires when its 1-min close
    breaches; filled at that bar's close, cut loss frozen into the numerator); the basket
    arm/trail runs on the FROZEN notional (denominator = dep, never shrinks). Remaining
    legs exit at next-open on a basket trigger or at time-exit close."""
    q = M["qty"].astype(np.float64); dep = M["dep"]; e = M["entry"].astype(np.float64)
    C = M["C"].astype(np.float64); O = M["O"].astype(np.float64)
    day = M["day"]; eod = M["eod"]
    kcap = min(max_hold, int(day.max())); last = int(eod[kcap])
    lvl = e * (1 - pstop / 100.0)
    mask = np.ones(len(e), bool); realized = 0.0
    armed = False; peak = None
    for i in range(last):
        # per-stock stop fills first (on close breach)
        for j in np.where(mask)[0]:
            if C[i, j] <= lvl[j]:
                realized += (C[i, j] - e[j]) * q[j]; mask[j] = False
        unreal = float(((C[i] - e) * q * mask).sum())
        G = (realized + unreal) / dep * 100.0
        trig = (not armed and G <= -stop) or (armed and G <= max(floor, (peak if peak else -1e9) - give))
        if trig:
            j2 = i + 1
            px = O[j2] if j2 <= last else C[i]
            realized += float(((px - e) * q * mask).sum())
            return dict(net=_net(realized / dep * 100.0, dep), gross=realized / dep * 100.0,
                        reason="STOP" if not armed else "TRAIL", exit_day=int(day[i]), exit_idx=i)
        if not armed and G >= arm:
            armed = True; peak = G
        if armed:
            peak = max(peak, G)
    realized += float(((C[last] - e) * q * mask).sum())
    return dict(net=_net(realized / dep * 100.0, dep), gross=realized / dep * 100.0,
                reason="TIME", exit_day=int(day[last]), exit_idx=last)


def summarize(nets, dates):
    """Per-trade distribution stats + additive drawdown + monthly-grew count."""
    from collections import defaultdict
    a = np.array(nets, float)
    eq = np.cumsum(a); dd = float((np.maximum.accumulate(eq) - eq).max())
    md = defaultdict(list)
    for r, d in zip(nets, dates):
        md[d[:7]].append(r)
    grew = sum(1 for v in md.values() if sum(v) >= 0)
    wins = a[a > 0]; losses = a[a < 0]
    return dict(n=len(a), mean=round(float(a.mean()), 3), median=round(float(np.median(a)), 3),
                pos=round(float((a > 0).mean() * 100), 1), ge1=round(float((a >= 1).mean() * 100), 1),
                ge2=round(float((a >= 2).mean() * 100), 1), worst=round(float(a.min()), 2),
                best=round(float(a.max()), 2), total=round(float(a.sum()), 1), maxdd=round(dd, 1),
                avgwin=round(float(wins.mean()), 2) if len(wins) else 0.0,
                avgloss=round(float(losses.mean()), 2) if len(losses) else 0.0,
                months_grew=f"{grew}/{len(md)}")


if __name__ == "__main__":
    ds = load()
    print(f"[*] loaded {len(ds)} signal days  {ds[0]['signal_date']}..{ds[-1]['signal_date']}")
    m = ds[0]
    print("    sample day0:", m["signal_date"], "syms", m["syms"], "hold-days", len(m["dates"]),
          "bars", m["C"].shape[0])
    print("    mfe/mae by day:", mfe_mae_by_day(m)[:3])
