"""
Jan-2026 path experiment (vectorized, NaN-safe). Every stock, every day: enter at
09:15 open, +1% target / -1% stop. Classify each LONG and SHORT trade by first-touch
order of the +1% and -1% levels from the 1-min candles.
"""
import sqlite3
from pathlib import Path
import numpy as np, pandas as pd

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
HERE = Path(__file__).resolve().parent
CAP = 30_000.0

con = sqlite3.connect(str(DB))
df = pd.read_sql("SELECT symbol,bar_time,open,high,low FROM ohlc_1min "
                 "WHERE bar_time>='2026-01-01' AND bar_time<='2026-01-31 23:59'", con)
con.close()
df["dt"] = pd.to_datetime(df["bar_time"]); df["date"] = df["dt"].dt.date
df["mins"] = df["dt"].dt.hour * 60 + df["dt"].dt.minute - (9 * 60 + 15)
df = df[df["mins"] >= 0]

# entry = 09:15 open per (symbol,date)
entry = df[df["mins"] == 0].groupby(["symbol", "date"])["open"].first().rename("entry")
df = df.merge(entry, on=["symbol", "date"])
up = df["high"] >= df["entry"] * 1.01           # +1% touched
dn = df["low"] <= df["entry"] * 0.99            # -1% touched
t_up = df[up].groupby(["symbol", "date"])["mins"].min()        # first minute +1% touched (NaN if never)
t_dn = df[dn].groupby(["symbol", "date"])["mins"].min()
R = pd.DataFrame(index=entry.index)
R["t_up"] = t_up; R["t_dn"] = t_dn


def classify(t_tgt, t_stp):
    tt = not pd.isna(t_tgt); ts = not pd.isna(t_stp)
    if not tt and not ts: return "no_touch"
    if tt and not ts: return "target_clean"
    if ts and not tt: return "stop_clean"
    return "target_then_revert" if t_tgt < t_stp else "stop_then_target"


R["long"] = [classify(u, d) for u, d in zip(R["t_up"], R["t_dn"])]     # long: tgt=up, stop=dn
R["short"] = [classify(d, u) for u, d in zip(R["t_up"], R["t_dn"])]    # short: tgt=dn, stop=up

order = ["target_clean", "target_then_revert", "stop_clean", "stop_then_target", "no_touch"]
labels = {"target_clean": "1. Met target & held (clean +1%)",
          "target_then_revert": "2. Met target, then reversed to stop",
          "stop_clean": "3. Hit stop loss (clean -1%)",
          "stop_then_target": "4. Hit stop, then reverted to target",
          "no_touch": "5. Neither (moved <1% all day)"}
n = len(R); days = len(set(i[1] for i in R.index)); syms = len(set(i[0] for i in R.index))
lc = R["long"].value_counts(); sc = R["short"].value_counts()
print("=" * 74)
print(f"JAN-2026 PATH EXPERIMENT · {days} trading days · {syms} stocks · {n:,} stock-days")
print(f"entry 09:15 open · +1% target / -1% stop · Rs{CAP:,.0f}/side")
print("=" * 74)
print(f"{'bucket':40}{'LONG cnt':>9}{'LONG%':>8}{'SHORT cnt':>10}{'SHORT%':>8}")
for k in order:
    lo = int(lc.get(k, 0)); sh = int(sc.get(k, 0))
    print(f"{labels[k]:40}{lo:>9}{lo/n*100:>7.1f}%{sh:>10}{sh/n*100:>7.1f}%")
print("-" * 74)
print(f"{'TOTAL':40}{n:>9}{'100%':>8}{n:>10}{'100%':>8}")
tt = lc.get("target_clean", 0) + lc.get("target_then_revert", 0)
ss = lc.get("stop_clean", 0) + lc.get("stop_then_target", 0)
print(f"\nLONG: {tt/n*100:.0f}% of trades touched +1% at some point · {ss/n*100:.0f}% touched -1% · "
      f"{lc.get('no_touch',0)/n*100:.0f}% moved <1% all day")
print(f"WHIPSAW (stopped, then price DID reach target): {lc.get('stop_then_target',0)/n*100:.1f}%")
print(f"FAKEOUT (target hit, then reversed to stop):    {lc.get('target_then_revert',0)/n*100:.1f}%")
# net of a real 1%/1% bracket (first touch wins): win = target_clean + stop_then... no: bracket exits at FIRST touch
long_win = lc.get("target_clean", 0) + lc.get("target_then_revert", 0)   # target touched first OR only
long_win = lc.get("target_clean", 0) + (R[(R["long"] == "target_then_revert")].shape[0])
first_target = lc.get("target_clean", 0) + lc.get("target_then_revert", 0)  # target before stop
first_stop = lc.get("stop_clean", 0) + lc.get("stop_then_target", 0)        # stop before target
print(f"\nReal 1%/1% bracket (exit at FIRST touch):")
print(f"  LONG  wins(+1%): {first_target} ({first_target/n*100:.1f}%) · losses(-1%): {first_stop} ({first_stop/n*100:.1f}%) · "
      f"no-hit: {lc.get('no_touch',0)} ({lc.get('no_touch',0)/n*100:.1f}%)")
sfirst_t = sc.get("target_clean", 0) + sc.get("target_then_revert", 0)
sfirst_s = sc.get("stop_clean", 0) + sc.get("stop_then_target", 0)
print(f"  SHORT wins(-1%): {sfirst_t} ({sfirst_t/n*100:.1f}%) · losses(+1%): {sfirst_s} ({sfirst_s/n*100:.1f}%) · "
      f"no-hit: {sc.get('no_touch',0)} ({sc.get('no_touch',0)/n*100:.1f}%)")

try:
    import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(11, 5)); x = np.arange(len(order)); w = 0.4
    lv = [lc.get(k, 0) / n * 100 for k in order]; sv = [sc.get(k, 0) / n * 100 for k in order]
    b1 = ax.bar(x - w / 2, lv, w, label="LONG", color="#2e7d32"); b2 = ax.bar(x + w / 2, sv, w, label="SHORT", color="#c62828")
    ax.set_xticks(x); ax.set_xticklabels([labels[k].split(". ", 1)[1] for k in order], rotation=18, ha="right", fontsize=8)
    ax.set_ylabel("% of stock-day trades"); ax.set_title(f"Jan-2026 · +1% tgt / -1% stop · {n:,} stock-days · intraday path outcome")
    for b in (b1, b2):
        for r in b: ax.text(r.get_x() + r.get_width() / 2, r.get_height() + 0.4, f"{r.get_height():.0f}", ha="center", fontsize=7)
    ax.legend(); fig.tight_layout(); fig.savefig(HERE / "jan26_experiment.png", dpi=120); plt.close(fig)
    print(f"\n[chart -> {HERE/'jan26_experiment.png'}]")
except Exception as e:
    print("plot skipped:", e)
