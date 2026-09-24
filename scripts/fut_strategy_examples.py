"""Compute the day-by-day long/short momentum example trades on real futures 1-min data,
for the liquid window (trade dates 2026-06-16 .. 2026-07-03). Signal = prior-day close-to-
close return; long the top-quintile winners, short the bottom-quintile losers; OI-buildup
confirm; enter 09:15 open, exit 15:29 close (open->close on the trade day). Prints for
inspection; also importable (build_examples) for the doc generator.
"""
import numpy as np
import pandas as pd
import fut_eda as E

WIN_START, WIN_END = "2026-06-16", "2026-07-03"


def build_examples():
    p = E.build_panel()
    p["liq"] = p.groupby("date").turnover.transform(lambda s: s >= s.median())
    p = p.sort_values(["symbol", "date"])
    p["sig_ret"] = p.groupby("symbol").ret_co.shift(1)      # prior-day close-to-close (the signal)
    p["sig_oichg"] = p.groupby("symbol").oi_chg.shift(1)    # prior-day OI change
    days = [d for d in sorted(p.date.unique()) if WIN_START <= d <= WIN_END]
    out = []
    for T in days:
        g = p[(p.date == T) & p.liq & p.sig_ret.notna() & p.day_ret.notna()].copy()
        if len(g) < 20:
            continue
        g["q"] = pd.qcut(g.sig_ret.rank(method="first"), 5, labels=False)
        longs = g[g.q == 4].copy(); shorts = g[g.q == 0].copy()
        longs["oi_build"] = (longs.sig_ret > 0) & (longs.sig_oichg > 0)
        shorts["oi_build"] = (shorts.sig_ret < 0) & (shorts.sig_oichg > 0)
        lt = longs[longs.oi_build]; st = shorts[shorts.oi_build]     # OI-confirmed traded set
        out.append(dict(
            date=T,
            long_top=longs.sort_values("sig_ret", ascending=False).head(6),
            short_top=shorts.sort_values("sig_ret").head(6),
            long_leg=float(lt.day_ret.mean() * 100) if len(lt) else float(longs.day_ret.mean() * 100),
            short_leg=float(-st.day_ret.mean() * 100) if len(st) else float(-shorts.day_ret.mean() * 100),
            n_long=len(lt) if len(lt) else len(longs), n_short=len(st) if len(st) else len(shorts),
        ))
    return out


def main():
    ex = build_examples()
    cum_l = cum_s = 0.0
    print(f"{'trade date':>11}{'longs':>7}{'long leg%':>10}{'shorts':>8}{'short leg%':>11}{'combo%':>8}{'cum%':>9}")
    for e in ex:
        combo = e["long_leg"] + e["short_leg"]; cum_l += e["long_leg"]; cum_s += e["short_leg"]
        print(f"{e['date']:>11}{e['n_long']:>7}{e['long_leg']:>10.3f}{e['n_short']:>8}{e['short_leg']:>11.3f}{combo:>8.3f}{cum_l+cum_s:>9.2f}")
    print(f"\nPeriod: long {cum_l:+.2f}% | short {cum_s:+.2f}% | combined {cum_l+cum_s:+.2f}% over {len(ex)} days (GROSS)")
    # sample one day's picks
    e = ex[len(ex)//2]
    print(f"\nSample trade day {e['date']} — LONGS (yesterday's winners, entered 09:15 open, exit 15:29):")
    for _, r in e["long_top"].iterrows():
        print(f"  {r['symbol']:<12} prior {r['sig_ret']*100:+.2f}%  OIbuild={'Y' if (r['sig_ret']>0 and r['sig_oichg']>0) else 'n'}  "
              f"entry {r['opn']:.1f} exit {r['cls']:.1f}  trade {r['day_ret']*100:+.2f}%")
    print(f"SHORTS (yesterday's losers):")
    for _, r in e["short_top"].iterrows():
        print(f"  {r['symbol']:<12} prior {r['sig_ret']*100:+.2f}%  OIbuild={'Y' if (r['sig_ret']<0 and r['sig_oichg']>0) else 'n'}  "
              f"entry {r['opn']:.1f} exit {r['cls']:.1f}  trade {-r['day_ret']*100:+.2f}% (short)")


if __name__ == "__main__":
    main()
