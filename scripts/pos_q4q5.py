"""Q4 (per-stock stop) + Q5 (is the time-exit necessary?) on the chosen positional
basket trail: arm3 / floor1 / giveback4 / hardstop6.

Q4: does adding a per-stock software stop at -pstop% help or (as intraday) hurt via
    whipsaw? Compare basket-only vs two-layer at pstop = 3/4/5/6.
Q5: hold the trail config fixed and vary the max-hold cap 2..7 (7 = effectively no cap,
    dataset horizon). If mean plateaus by day 3 while DD grows, the 3-day time-exit
    earns its place; if a longer cap keeps adding return, relax it.
"""
import numpy as np
import pos_sim as P

ARM, FL, GV, ST = 3.0, 1.0, 4.0, 6.0


def run(fn):
    ds = P.load(); dates = [m["signal_date"] for m in ds]
    nets = []; days = []
    for m in ds:
        r = fn(m); nets.append(r["net"]); days.append(r["exit_day"])
    s = P.summarize(nets, dates)
    s["avgday"] = round(float(np.mean(days)), 2)
    return s


def main():
    print("Q4 — PER-STOCK STOP added to the basket trail (arm3/fl1/give4/stop6, max-hold 3)\n")
    print(f"{'variant':<26}{'mean%':>7}{'%pos':>6}{'%>=2':>6}{'worst':>7}{'total%':>8}{'maxDD':>7}{'avgHd':>6}{'moGrew':>8}")
    base = run(lambda m: P.basket_trail(m, ARM, FL, GV, ST, 3))
    print(f"{'BASKET-ONLY':<26}{base['mean']:>7.3f}{base['pos']:>5.1f}%{base['ge2']:>5.1f}%"
          f"{base['worst']:>7.2f}{base['total']:>8.0f}{base['maxdd']:>7.1f}{base['avgday']:>6.2f}{base['months_grew']:>8}")
    for ps in [3.0, 4.0, 5.0, 6.0]:
        s = run(lambda m, ps=ps: P.perstock_trail(m, ps, ARM, FL, GV, ST, 3))
        print(f"{'TWO-LAYER per-stock -'+str(ps):<26}{s['mean']:>7.3f}{s['pos']:>5.1f}%{s['ge2']:>5.1f}%"
              f"{s['worst']:>7.2f}{s['total']:>8.0f}{s['maxdd']:>7.1f}{s['avgday']:>6.2f}{s['months_grew']:>8}")

    print("\nQ5 — TIME-EXIT (max-hold cap) on the same trail (arm3/fl1/give4/stop6)\n")
    print(f"{'max-hold':>9}{'mean%':>7}{'%pos':>6}{'%>=2':>6}{'worst':>7}{'total%':>8}{'maxDD':>7}{'ret/DD':>7}{'avgHd':>6}{'moGrew':>8}")
    for mh in [2, 3, 4, 5, 7]:
        s = run(lambda m, mh=mh: P.basket_trail(m, ARM, FL, GV, ST, mh))
        rd = s["total"] / s["maxdd"] if s["maxdd"] else 0
        print(f"{mh:>9}{s['mean']:>7.3f}{s['pos']:>5.1f}%{s['ge2']:>5.1f}%{s['worst']:>7.2f}"
              f"{s['total']:>8.0f}{s['maxdd']:>7.1f}{rd:>7.1f}{s['avgday']:>6.2f}{s['months_grew']:>8}")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
    main()
