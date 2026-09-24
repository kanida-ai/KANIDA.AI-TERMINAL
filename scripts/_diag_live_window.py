"""Diagnose the live-signal window: which days/picks dragged it. Read-only."""
import pandas as pd
from collections import Counter
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
p = ROOT / "outputs" / "Live_Signal_SpotCheck.xlsx"
pd.set_option("display.width", 220); pd.set_option("display.max_colwidth", 70)

live = pd.read_excel(p, "2_LIVE_daily_log")
cmp = pd.read_excel(p, "4_DayByDay_compare")

print("=== LIVE daily log sorted worst -> best ===")
print(live.sort_values("return_pct")[["entry_date", "stocks", "return_pct", "result"]].to_string(index=False))

print(f"\n=== LIVE window: {len(live)} days, {(live.return_pct>0).sum()} win / {(live.return_pct<0).sum()} loss ===")
print(f"  mean {live.return_pct.mean():.3f}  sum {live.return_pct.sum():.2f}")
print(f"  if we DROP the 3 worst days: mean {live.sort_values('return_pct').iloc[3:].return_pct.mean():.3f}")

# per-stock contribution proxy: appearances on loss vs win days
loss_stocks = Counter()
win_stocks = Counter()
for _, r in live.iterrows():
    for s in [x.strip() for x in str(r["stocks"]).split(",")]:
        (loss_stocks if r["return_pct"] < 0 else win_stocks)[s] += 1
print("\n=== stocks most often on LOSS days (live) ===")
for s, n in loss_stocks.most_common(12):
    print(f"  {s:14} loss-days={n}  win-days={win_stocks.get(s,0)}")

print("\n=== day-by-day LIVE vs RESEARCH (biggest gaps where research won) ===")
cmp2 = cmp.sort_values("return_diff")
print(cmp2[["entry_date", "stocks_live", "return_pct_live", "stocks_research",
            "return_pct_research", "return_diff"]].head(10).to_string(index=False))
print(f"\n  days research > live: {(cmp.return_diff<0).sum()}/{len(cmp)}; "
      f"mean diff (live-research): {cmp.return_diff.mean():.3f}")
