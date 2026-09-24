"""Falcon intraday KPI baseline + the capital-rotation viability question.
Computes the KPIs from the user's list on the CURRENT strategy (535 days, _opt_dataset),
and — critically — WHEN the basket peaks and how fast it captures 80% of its peak, which
determines whether freed capital could be rotated intraday."""
import pickle
from pathlib import Path
from collections import defaultdict
import numpy as np
ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
CH = 0.0005
ARM, FLOOR, GIVE, STOP = 2.5, 1.0, 1.5, 3.0  # current live config


def _net(g):
    return g - (2 + g / 100.0) * CH * 100.0


def cur_exit(rc, n):
    armed = False; peak = None
    for i in range(n - 1):
        r = rc[i]
        if r <= -STOP:
            return i, -STOP, "STOP"
        if not armed:
            if r >= ARM:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        if r <= max(FLOOR, peak - GIVE):
            return i, r, "TRAIL"
    return n - 1, rc[n - 1], "EOD"


def main():
    mats = pickle.load(open(ROOT / "docs" / "ops" / "_opt_dataset.pkl", "rb"))
    hours_to_peak = []; hours_to_80 = []; hold_hours = []; nets = []; gb = []
    peak_time_bucket = defaultdict(int); p2l = 0; peakvals = []
    for d, M in mats:
        e = M["entry"].astype(float); q = np.floor((500000.0 / M["nstocks"]) / e); dep = float((e * q).sum())
        rc = (M["close"].astype(float) @ q - dep) / dep * 100.0
        n = M["n"]; grid = M["grid"]
        pk_idx = int(np.argmax(rc)); pk = float(rc[pk_idx])
        peakvals.append(max(pk, 0))
        htp = pk_idx / 60.0; hours_to_peak.append(htp)
        pt = grid[pk_idx] if pk_idx < len(grid) else "15:29"
        b = ("09:15-10:30" if pt < "10:30" else "10:30-12:00" if pt < "12:00" else
             "12:00-14:00" if pt < "14:00" else "14:00-15:29")
        peak_time_bucket[b] += 1
        if pk > 0.1:
            t80 = int(np.argmax(rc >= 0.8 * pk)); hours_to_80.append(t80 / 60.0)
        xi, xr, rs = cur_exit(rc, n)
        hold_hours.append(xi / 60.0)
        net = _net(xr); nets.append(net)
        gb.append(max(0.0, float(rc[:xi + 1].max()) - xr))
        if float(rc[:xi + 1].max()) > 0.5 and net < 0:
            p2l += 1
    nets = np.array(nets); hh = np.array(hold_hours); htp = np.array(hours_to_peak)
    wins = nets[nets > 0]; los = nets[nets < 0]
    print("FALCON INTRADAY — KPI BASELINE (current config, 535 days, Rs5L, net)\n")
    print(f"  Winning-day ratio         : {(nets>0).mean()*100:.1f}%")
    print(f"  Avg winning day           : +{wins.mean():.2f}%   Avg losing day: {los.mean():.2f}%   asym {wins.mean()/abs(los.mean()):.1f}x")
    print(f"  Tail-loss freq (< -2%)    : {(nets<-2).mean()*100:.1f}% of days")
    print(f"  Avg holding time (to exit): {hh.mean():.2f} h   median {np.median(hh):.2f} h  (session = 6.23 h)")
    print(f"  Daily capital rotation    : 1.00x  (deploy once at 09:15, hold; no intraday redeploy today)")
    print(f"  Return per capital-HOUR   : {nets.mean()/hh.mean():.3f}%/h   (= {nets.mean()/hh.mean()*5:.2f}%/h on capital @5x)")
    print(f"  Peak-to-exit giveback     : {np.mean(gb):.3f}%")
    print(f"  Profit-to-loss reversal   : {p2l}/{len(nets)} = {p2l/len(nets)*100:.1f}% of days (was >+0.5%, closed red)")
    print(f"\n  === THE ROTATION QUESTION: when does the basket PEAK? ===")
    print(f"  Time-to-peak (from 09:15) : mean {htp.mean():.2f} h   median {np.median(htp):.2f} h")
    print(f"  Time-to-80%-of-peak       : mean {np.mean(hours_to_80):.2f} h   median {np.median(hours_to_80):.2f} h")
    print(f"  WHEN the intraday peak occurs:")
    for b in ["09:15-10:30", "10:30-12:00", "12:00-14:00", "14:00-15:29"]:
        print(f"     {b}: {peak_time_bucket[b]:4d} days  {peak_time_bucket[b]/len(mats)*100:5.1f}%")
    # idle-capital estimate: capital is 'done' after 80%-peak; hours idle = session - hours_to_80
    idle = 6.23 - np.array(hours_to_80)
    print(f"\n  Est. idle capital-hours after 80%-of-peak: mean {idle.mean():.2f} h of the 6.23h session "
          f"({idle.mean()/6.23*100:.0f}% of the day)")


if __name__ == "__main__":
    main()
