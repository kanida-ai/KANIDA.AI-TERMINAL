"""STRICT ABSORPTION FOOTPRINT scan (the exact FORCEMOT / HFCL sequence), 2024-2026.
The 4-stage pattern, ALL required:
  1. DRY-UP      : volume contracts before the low (recent-20-40d median vol < longer-120d baseline).
  2. ABSORPTION  : peak volume AT the price low — a genuine local bottom L where a high-volume spike
                   (>= 2.5x the dry-up baseline) prints AT/near the low, and price refuses to stay down.
  3. HIGHER-LOWS : after L the low HOLDS and lows rise over the next ~4 weeks (float changing hands).
  4. EXPANSION   : (outcome) does price then mark up? measured FORWARD from the confirm day (L+20).
Bottom L identified as a local min (40 back / 15 fwd); OUTCOME measured strictly forward from L+20 (no lookahead
in the outcome). Reports matched names + whether expansion followed, hit-rate vs base rate. READ-ONLY."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
FWD, TGT = 130, 0.40

def scan():
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT symbol, trade_date, high, low, close, volume FROM ohlc_daily WHERE trade_date>='2023-01-01' ORDER BY symbol, trade_date", oc)
    oc.close()
    hits = []
    base_hit = base_n = 0
    for s, g in df.groupby("symbol"):
        g = g.reset_index(drop=True); c = g.close.values; hi = g.high.values; lo = g.low.values; v = g.volume.values.astype(float); dt = g.trade_date.values
        n = len(g)
        if n < 200 or np.median(c * v) < 2e7 or np.median(c) < 10: continue
        # base-rate reference: any day in 2024-26 that later makes +40% within 130d
        for i in range(60, n - FWD):
            if "2024" <= str(dt[i])[:4] <= "2026":
                base_n += 1; base_hit += int(hi[i + 1:i + 1 + FWD].max() >= c[i] * (1 + TGT))
        for L in range(140, n - 25 - FWD):
            if not ("2024" <= str(dt[L])[:4] <= "2026"): continue
            # 1) local bottom
            if lo[L] > lo[L - 40:L + 16].min() * 1.001: continue
            base_line = np.median(v[L - 120:L - 40]); dry = np.median(v[L - 40:L - 15])
            if not (base_line > 0 and dry < 0.90 * base_line): continue          # 2) DRY-UP before
            # 3) ABSORPTION: a high-volume spike AT/near the low
            win = slice(L - 8, L + 6); spikex = v[win].max() / max(dry, 1)
            if spikex < 2.5: continue                                            # peak volume >= 2.5x dry-up
            splo = lo[win].min()
            if splo > lo[L - 40:L + 16].min() * 1.03: continue                   # the spike is AT the low
            # 4) HIGHER-LOWS / hold after L
            if lo[L + 1:L + 21].min() < lo[L] * 0.97: continue                   # low holds 20d
            if lo[L + 11:L + 26].min() < lo[L + 1:L + 11].min() * 0.99: continue # rising lows
            ei = L + 20; epx = c[ei]                                             # confirm/entry ~4wk after low
            fwd = hi[ei + 1:ei + 1 + FWD].max() / epx - 1
            run = c[ei + 1:ei + 1 + FWD].max() / epx - 1
            hits.append(dict(symbol=s, low_date=str(dt[L]), low=lo[L], spike_x=spikex,
                             entry=str(dt[ei]), entry_px=epx, ran=run * 100, broke_out=int(fwd >= TGT)))
    return pd.DataFrame(hits), (base_hit / base_n if base_n else np.nan)

if __name__ == "__main__":
    H, base_rate = scan()
    H = H.sort_values("ran", ascending=False).reset_index(drop=True)
    print(f"STRICT ABSORPTION FOOTPRINT (dry-up -> peak-vol-at-low -> higher-lows -> expansion), bottoms in 2024-2026")
    print(f"matches: {len(H)}   |   distinct stocks: {H.symbol.nunique()}")
    print(f"HIT-RATE (reached +40% within 26wk from confirm): {H.broke_out.mean():.0%}   vs   base rate {base_rate:.0%}"
          f"   ->  LIFT {H.broke_out.mean()/base_rate:.2f}x")
    print(f"median forward run: {H.ran.median():+.0f}%   mean {H.ran.mean():+.0f}%")
    print("\nALL MATCHES (by forward run):")
    print(f"{'#':<3}{'symbol':<13}{'low_date':<12}{'spikeX':>7}{'entry':<12}{'run%':>7}{'+40%?':>6}")
    for i, r in H.iterrows():
        print(f"{i+1:<3}{r.symbol:<13}{r.low_date:<12}{r.spike_x:>6.1f}x{r.entry:<12}{r.ran:>+7.0f}{'Y' if r.broke_out else '.':>6}")
    for chk in ["FORCEMOT", "HFCL"]:
        got = "YES" if chk in set(H.symbol) else "no"
        print(f"  [sanity] {chk} detected: {got}")
