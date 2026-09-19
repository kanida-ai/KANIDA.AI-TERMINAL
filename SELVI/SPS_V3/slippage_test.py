"""
Slippage-at-size test for the vault-confirmed basket.
1. Capacity: per-stock 5x notional (Rs150k) vs avg daily turnover -> market-impact headroom.
2. Stress: sweep ADDITIONAL round-trip slippage on top of the modeled 0.13% and find the
   BREAK-EVEN slippage (where net edge -> 0), plus net return under realistic bands.

Run:  PYTHONIOENCODING=utf-8 python slippage_test.py
"""
import json, sqlite3
from pathlib import Path
import numpy as np
import finalize
from finalize import load_min, load_atr, day_pack, VAULT

HERE = Path(__file__).resolve().parent
CAP = 30_000.0
LEV = 5.0

port = json.load(open(HERE / "portfolio.json")); syms = port["list"]
con = sqlite3.connect(str(HERE / "scan_results.db"))
specs, thrs = {}, {}
for s in syms:
    row = con.execute("SELECT spec, thr FROM champ WHERE sym=?", (s,)).fetchone()
    specs[s] = json.loads(row[0]); thrs[s] = row[1]
con.close()
kdb = sqlite3.connect(str(HERE.parents[1] / "db" / "kanida.db"))
adv = {}
for s in syms:
    adv[s] = kdb.execute("SELECT AVG(close*volume) FROM ohlc_daily WHERE symbol=? AND bar_time<'2026'", (s,)).fetchone()[0] or 0
kdb.close()

# per-trade net returns (already net of modeled 0.13%) on the sealed vault
nets = []
for s in syms:
    atr = load_atr(s); vault = day_pack(load_min(s, *VAULT, unlock=True))
    ser = finalize.sim_series(s, vault, atr, specs[s], thrs[s])
    nets.extend(ser.values())
nets = np.array(nets)
n_trades = len(nets)

# ---- 1. capacity ----
part = {s: (CAP * LEV) / adv[s] for s in syms if adv[s] > 0}   # 5x notional as fraction of ADV
worst = sorted(part.items(), key=lambda x: -x[1])[:6]

print("=" * 74)
print("SLIPPAGE-AT-SIZE TEST — vault-confirmed basket")
print(f"{len(syms)} stocks · {n_trades} vault trades · base per-trade net {nets.mean()*100:+.4f}% (after modeled 0.13%)")
print("=" * 74)
print("\n--- 1. CAPACITY (5x notional Rs150k as % of avg daily turnover) ---")
print(f"  worst (least liquid): " + ", ".join(f"{s} {p*100:.3f}%" for s, p in worst))
print(f"  => even the thinnest name trades Rs150k = {worst[0][1]*100:.3f}% of its daily turnover")
print(f"     (market impact negligible at this size; slippage = spread + breakout adverse fill, not impact)")

# ---- 2. stress sweep ----
print("\n--- 2. STRESS: additional round-trip slippage on top of modeled 0.13% ---")
print(f"{'add slip':>10}{'net/trade 1x':>14}{'net/trade 5x':>14}{'% trades +':>12}{'basket/day 5x*':>16}")
tdays = 141
for s in [0.0, 0.0005, 0.0008, 0.0010, 0.0015, 0.0020, 0.0025, 0.0030]:
    st = nets - s
    perday5 = st.mean() * 5 * (n_trades / tdays) * 100 / (n_trades / tdays)  # per-trade 5x; day handled below
    # basket avg return/day on deployed approx = per-trade net * (trades/day)/(stocks-live/day) ~ per-trade net
    print(f"{s*100:>9.2f}%{st.mean()*100:>13.4f}%{st.mean()*5*100:>13.4f}%{(st>0).mean()*100:>11.1f}%"
          f"{st.mean()*5*100:>15.4f}%")
breakeven = nets.mean()
print(f"\n  BREAK-EVEN additional slippage (net -> 0): {breakeven*100:.4f}%  "
      f"(i.e. total round-trip tolerance ~ {0.13+breakeven*100:.2f}%)")

# realistic bands
print("\n--- realistic slippage bands (liquid mid-cap MIS breakout) ---")
for lbl, s in [("optimistic  +0.03%", 0.0003), ("base        +0.08%", 0.0008), ("conservative +0.15%", 0.0015)]:
    st = nets - s
    surv = st.mean() / nets.mean() * 100
    print(f"  {lbl}: net/trade 1x {st.mean()*100:+.4f}% (5x {st.mean()*5*100:+.4f}%) · "
          f"{surv:.0f}% of edge survives · {(st>0).mean()*100:.0f}% trades +")
