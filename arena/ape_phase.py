"""Phase-based accumulation detector, calibrated to the REAL skeleton both archetypes share:
  DRY-UP (25d avg vol falls to <=0.65x the prior 120d baseline)  ->  extended BASE at the lows  ->
  the low HOLDS with HIGHER-LOWS  ->  recent VOLUME EXPANSION as it turns up (demand appearing).
ARM the day the higher-lows + volume-pickup confirm, while price is still near the base low (not extended).
HARD GATE: it MUST fire on FORCEMOT (Feb-2025 base) and HFCL (Dec-2025-Feb-2026 base) or the detector is wrong.
Only after it passes do we scan + measure lift vs base rate. READ-ONLY."""
import os, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
BASE, FWD, TGT = 75, 130, 0.40
DRY, VUP, NEAR = 0.65, 1.3, 1.30

def arms_for(g):
    """Return list of arm indices (deduped one per base) for a single stock's daily frame."""
    c = g.close.values; lo = g.low.values; hi = g.high.values; v = g.volume.values.astype(float); n = len(g)
    va25 = pd.Series(v).rolling(25).mean().values
    out = []; last_arm = -999
    for A in range(BASE + 130, n - 1):
        if A - last_arm < 40: continue
        b0 = A - BASE; bl = lo[b0:A].min(); bli = b0 + int(np.argmin(lo[b0:A]))
        if bli > b0 + BASE * 0.7: continue                                  # low in first ~70% -> room to base after
        if lo[bli + 1:A].min() < bl * 0.96: continue                        # low HOLDS (allow 4% undercut/spring)
        if lo[A - 15:A].min() < bl * 1.005: continue                        # recent lows ABOVE the base low (higher-lows)
        baseline = np.median(v[b0 - 120:b0 - 20]); dryv = np.min(va25[b0:A])
        if not (baseline > 0 and dryv <= DRY * baseline): continue          # DRY-UP happened
        if v[A - 15:A].mean() < VUP * dryv: continue                        # recent VOLUME PICKUP vs dry-up
        if c[A] > bl * NEAR: continue                                        # still near the base (not extended)
        out.append(A); last_arm = A
    return out, (c, lo, hi)

def load(w0="2016-01-01"):
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT symbol, trade_date, high, low, close, volume FROM ohlc_daily WHERE trade_date>=? ORDER BY symbol, trade_date", oc, params=(w0,))
    oc.close(); return df

if __name__ == "__main__":
    df = load()
    # ---- HARD GATE: verify both archetypes ----
    print("GATE — does the detector fire on the archetypes?")
    for s in ["FORCEMOT", "HFCL"]:
        g = df[df.symbol == s].reset_index(drop=True)
        arms, (c, lo, hi) = arms_for(g)
        a25 = [g.trade_date.iloc[a] for a in arms if "2024" <= g.trade_date.iloc[a][:4] <= "2026"]
        print(f"  {s}: arm dates {a25}")
        for a in arms:
            if "2025" <= g.trade_date.iloc[a][:7] <= "2026-03":
                fwd = hi[a + 1:a + 1 + FWD].max() / c[a] - 1
                print(f"      armed {g.trade_date.iloc[a]} @ {c[a]:.1f}  -> fwd run +{fwd*100:.0f}%")
    # ---- SCAN + measure ----
    print("\nSCAN 2024-2026 ...")
    hits = []; base_hit = base_n = 0
    for s, g in df.groupby("symbol"):
        g = g.reset_index(drop=True); c = g.close.values; hi = g.high.values; v = g.volume.values.astype(float)
        if len(g) < 300 or np.median(c * v) < 2e7 or np.median(c) < 10: continue
        for i in range(200, len(g) - FWD):
            if "2024" <= g.trade_date.iloc[i][:4] <= "2026":
                base_n += 1; base_hit += int(hi[i + 1:i + 1 + FWD].max() >= c[i] * (1 + TGT))
        arms, _ = arms_for(g)
        for a in arms:
            if not ("2024" <= g.trade_date.iloc[a][:4] <= "2026") or a >= len(g) - FWD: continue
            fwd = hi[a + 1:a + 1 + FWD].max() / c[a] - 1
            hits.append(dict(symbol=s, arm=g.trade_date.iloc[a], px=c[a], ran=fwd * 100, broke=int(fwd >= TGT)))
    H = pd.DataFrame(hits); br = base_hit / base_n
    print(f"matches {len(H)} | stocks {H.symbol.nunique()} | HIT +40% {H.broke.mean():.0%} vs base {br:.0%} -> LIFT {H.broke.mean()/br:.2f}x")
    print(f"median run {H.ran.median():+.0f}%  mean {H.ran.mean():+.0f}%  |  >=100%: {(H.ran>=100).mean():.0%}  <0%: {(H.ran<0).mean():.0%}")
    print("\ntop 20 by forward run:")
    for i, r in H.sort_values("ran", ascending=False).head(20).reset_index(drop=True).iterrows():
        print(f"  {i+1:<3}{r.symbol:<13}{r.arm:<12}{r.ran:>+6.0f}%")
