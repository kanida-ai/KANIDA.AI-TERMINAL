"""Q1 (MFE/MAE by hold-day) + Q2 (fixed-hold base curve) — the core hypothesis test:
does giving Falcon Top-5 winners multiple days of room beat closing intraday?

Q1: aggregate the basket's intraday MFE / MAE / close / overnight-gap for each hold-day
    k = 0..7 across all signal days. If favorable excursion keeps growing past day 0,
    positional has headroom; if only adverse heat grows, it does not.
Q2: fixed-hold-N net return per trade for N = 0..7 (N=0 == intraday same-day 15:29 close,
    the baseline). Mean/median/%pos/%>=1%/%>=2% + additive total + maxDD.
"""
import numpy as np
import pos_sim as P


def main():
    ds = P.load()
    dates = [m["signal_date"] for m in ds]
    print(f"[*] {len(ds)} signal days  {dates[0]}..{dates[-1]}\n")

    # ---------- Q1: MFE/MAE by hold-day ----------
    H = 7
    agg = {k: dict(mfe=[], mae=[], close=[], gap=[], alive=0) for k in range(H + 1)}
    for m in ds:
        rows = P.mfe_mae_by_day(m)
        for r in rows:
            k = r["hold_day"]
            agg[k]["mfe"].append(r["mfe"]); agg[k]["mae"].append(r["mae"])
            agg[k]["close"].append(r["day_close"]); agg[k]["gap"].append(r["gap"])
            agg[k]["alive"] += 1
    print("Q1 — BASKET MFE / MAE / CLOSE by hold-day (avg across signal days, GROSS ret pts)")
    print(f"{'hold_day':>8}{'n':>6}{'avgMFE':>8}{'avgMAE':>8}{'avgClose':>9}{'avgGap':>8}"
          f"{'medMFE':>8}{'medMAE':>8}{'%dayUp':>8}")
    for k in range(H + 1):
        a = agg[k]
        if not a["mfe"]:
            continue
        mfe = np.array(a["mfe"]); mae = np.array(a["mae"]); cl = np.array(a["close"]); gp = np.array(a["gap"])
        print(f"{k:>8}{len(mfe):>6}{mfe.mean():>8.2f}{mae.mean():>8.2f}{cl.mean():>9.2f}{gp.mean():>8.2f}"
              f"{np.median(mfe):>8.2f}{np.median(mae):>8.2f}{(cl>0).mean()*100:>7.1f}%")

    # ---------- Q2: fixed-hold-N base curve ----------
    print("\nQ2 — FIXED-HOLD-N net return per trade (N=0 = intraday same-day close baseline)")
    print(f"{'holdN':>6}{'n':>6}{'mean%':>8}{'median%':>9}{'%pos':>7}{'%>=1':>7}{'%>=2':>7}"
          f"{'worst':>8}{'best':>8}{'total%':>9}{'maxDD':>8}{'moGrew':>9}")
    for N in range(H + 1):
        nets = []
        for m in ds:
            net, _ = P.fixed_hold(m, N)
            nets.append(net)
        s = P.summarize(nets, dates)
        print(f"{N:>6}{s['n']:>6}{s['mean']:>8.3f}{s['median']:>9.3f}{s['pos']:>6.1f}%"
              f"{s['ge1']:>6.1f}%{s['ge2']:>6.1f}%{s['worst']:>8.2f}{s['best']:>8.2f}"
              f"{s['total']:>9.0f}{s['maxdd']:>8.1f}{s['months_grew']:>9}")

    # ---------- intraday baseline sanity (N=0 detail) ----------
    print("\nNote: N=0 mean is the intraday no-trail baseline. If mean% rises with N, "
          "multi-day holding adds return; the MFE column shows whether the room exists.")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
    main()
