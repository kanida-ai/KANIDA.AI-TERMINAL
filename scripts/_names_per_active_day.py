"""How many Top-10 names pass the best filters per ACTIVE day? Features-only, fast."""
import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
con = sqlite3.connect(str(DB))
f = pd.read_sql_query("SELECT * FROM intraday_bt_mtf_features", con)
con.close()

FILTERS = {
    "Aggressive (96.5% hit)": lambda d: (d.d0_vol_ratio >= 1.6032) & (d.w2_ret_pct >= 7.6964) & (d.w_vol_vs_w1 <= 1.0952),
    "Balanced (92.2% hit)":   lambda d: (d.d1_ret_pct >= -1.418) & (d.m_ret_pct <= 2.3585) & (d.w2_ret_pct >= 7.6964),
    "Robust (88.0% hit)":     lambda d: (d.m_ret_pct <= 2.3585) & (d.w2_ret_pct >= 7.6964),
}
for name, fn in FILTERS.items():
    sub = f[fn(f)]
    per_day = sub.groupby("entry_date").size()
    print(f"\n=== {name} ===")
    print(f"  active days        : {per_day.size}")
    print(f"  names/active-day   : mean {per_day.mean():.2f}, median {per_day.median():.0f}, "
          f"min {per_day.min()}, max {per_day.max()}")
    print(f"  active days w/ >=3 : {int((per_day>=3).sum())} ({(per_day>=3).mean()*100:.0f}%)")
    print(f"  active days w/ >=5 : {int((per_day>=5).sum())} ({(per_day>=5).mean()*100:.0f}%)")
    print(f"  distribution       : " + ", ".join(f"{k}:{v}" for k, v in per_day.value_counts().sort_index().items()))
