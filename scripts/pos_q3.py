"""Q3 — positional TRAIL sweep. Fixed-hold-3 gives mean ~5.3%/trade but no downside
management (worst -10, and DD balloons if you hold longer). Can a basket trail — with
params WIDENED for multi-day/overnight noise — match the day-3 return while cutting the
tail? Sweep arm/floor/giveback/hard-stop x max_hold, rank by mean and by return/DD.
"""
import numpy as np
import pos_sim as P


def main():
    ds = P.load()
    dates = [m["signal_date"] for m in ds]
    ARMS = [2.0, 3.0]
    FLOORS = [1.0]
    GIVES = [2.0, 3.0, 4.0]
    STOPS = [4.0, 5.0, 6.0]
    HOLDS = [3, 4, 5]
    rows = []
    for arm in ARMS:
        for fl in FLOORS:
            for gv in GIVES:
                for st in STOPS:
                    for mh in HOLDS:
                        nets = []; days = []; reasons = {}
                        for m in ds:
                            r = P.basket_trail(m, arm, fl, gv, st, mh)
                            nets.append(r["net"]); days.append(r["exit_day"])
                            reasons[r["reason"]] = reasons.get(r["reason"], 0) + 1
                        s = P.summarize(nets, dates)
                        rd = s["total"] / s["maxdd"] if s["maxdd"] else 0
                        rows.append(dict(arm=arm, fl=fl, gv=gv, st=st, mh=mh,
                                         mean=s["mean"], pos=s["pos"], ge1=s["ge1"], ge2=s["ge2"],
                                         worst=s["worst"], total=s["total"], maxdd=s["maxdd"],
                                         rd=round(rd, 1), avgday=round(float(np.mean(days)), 2),
                                         mo=s["months_grew"]))
    hdr = f"{'arm':>4}{'fl':>4}{'give':>5}{'stop':>5}{'maxH':>5}{'mean%':>7}{'%pos':>6}{'%>=2':>6}" \
          f"{'worst':>7}{'total%':>8}{'maxDD':>7}{'ret/DD':>7}{'avgHd':>6}{'moGrew':>7}"
    print("Q3 — POSITIONAL TRAIL SWEEP (net per trade). Baseline fixed-hold-3: mean 5.30 / DD 16.6\n")
    print("TOP 12 by MEAN return:")
    print(hdr)
    for r in sorted(rows, key=lambda x: -x["mean"])[:12]:
        print(f"{r['arm']:>4}{r['fl']:>4}{r['gv']:>5}{r['st']:>5}{r['mh']:>5}{r['mean']:>7.3f}"
              f"{r['pos']:>5.1f}%{r['ge2']:>5.1f}%{r['worst']:>7.2f}{r['total']:>8.0f}"
              f"{r['maxdd']:>7.1f}{r['rd']:>7.1f}{r['avgday']:>6.2f}{r['mo']:>7}")
    print("\nTOP 12 by RETURN/DRAWDOWN (efficiency):")
    print(hdr)
    for r in sorted(rows, key=lambda x: -x["rd"])[:12]:
        print(f"{r['arm']:>4}{r['fl']:>4}{r['gv']:>5}{r['st']:>5}{r['mh']:>5}{r['mean']:>7.3f}"
              f"{r['pos']:>5.1f}%{r['ge2']:>5.1f}%{r['worst']:>7.2f}{r['total']:>8.0f}"
              f"{r['maxdd']:>7.1f}{r['rd']:>7.1f}{r['avgday']:>6.2f}{r['mo']:>7}")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
    main()
