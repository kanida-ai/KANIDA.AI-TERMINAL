"""
REGIME / MARKET-INTERNALS BRAIN (Carter Ch.5 idea, India edition). A standalone agent that reads the
tape's internals daily and outputs a RISK state. It does NOT trade — it GATES the other agents.

Signals built from db/kanida.db (all leak-free: each day's state uses only data up to that day's close):
  1. nifty_trend : NIFTY 50 close > its 200-DMA (and 50-DMA)               -> primary trend
  2. breadth200  : % of the 441 universe trading above their own 200-DMA   -> participation
  3. breadth50   : % above 50-DMA                                          -> short-term thrust
  4. vix_pct     : INDIA VIX percentile over trailing 252d (high = fear)   -> stress
  5. adv_dec10   : 10-day mean of (advancers - decliners)/total           -> momentum of breadth

Composite RISK-ON score 0..100; state = RISK_ON / NEUTRAL / RISK_OFF.
VALIDATION: bucket FORWARD 20-day returns of NIFTY and of EW-441 by today's state. A real brain shows
monotonic forward return & lower drawdown in RISK_ON vs RISK_OFF. Also ranks SECTORS by 3-month strength.

Run: python scripts/regime.py
"""
from __future__ import annotations
import sys, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts"))
import daily_core as DC
KDB = str(ROOT / "db" / "kanida.db")


def _series(sym):
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT bar_time,close FROM ohlc_daily WHERE symbol=? ORDER BY bar_time", con, params=(sym,))
    con.close()
    df["date"] = pd.to_datetime(df["bar_time"].str[:10])
    return df.drop_duplicates("date").set_index("date")["close"].astype(float)


def build_regime():
    """Return a DataFrame indexed by date with all internals + composite score + state (all point-in-time)."""
    fields, _ = DC.wide_all()
    c = fields["c"]                                   # 441 universe closes (index NIFTY already dropped)
    nifty = _series("NIFTY 50"); vix = _series("INDIA VIX")
    idx = c.index

    # breadth: % above own 200/50 DMA
    ma200 = c.rolling(200).mean(); ma50 = c.rolling(50).mean()
    above200 = (c > ma200); above50 = (c > ma50)
    valid200 = c.notna() & ma200.notna(); valid50 = c.notna() & ma50.notna()
    breadth200 = (above200 & valid200).sum(1) / valid200.sum(1).clip(lower=1) * 100
    breadth50 = (above50 & valid50).sum(1) / valid50.sum(1).clip(lower=1) * 100

    # advance/decline (of the universe) and its 10d mean
    chg = c.pct_change(fill_method=None)
    adv = (chg > 0).sum(1); dec = (chg < 0).sum(1); tot = (adv + dec).clip(lower=1)
    adline = ((adv - dec) / tot * 100)
    adv_dec10 = adline.rolling(10).mean()

    nifty = nifty.reindex(idx).ffill(); vix = vix.reindex(idx).ffill()
    nifty_ma200 = nifty.rolling(200).mean(); nifty_ma50 = nifty.rolling(50).mean()
    nifty_trend = (nifty > nifty_ma200).astype(float)
    nifty_trend50 = (nifty > nifty_ma50).astype(float)
    vix_pct = vix.rolling(252).apply(lambda w: (w[-1] >= w).mean() * 100, raw=True)   # 0..100, high=fear

    df = pd.DataFrame({"nifty": nifty, "nifty_trend": nifty_trend, "nifty_trend50": nifty_trend50,
                       "breadth200": breadth200, "breadth50": breadth50, "vix": vix, "vix_pct": vix_pct,
                       "adv_dec10": adv_dec10})
    # composite RISK-ON score (0..100): weighted, higher = safer to be long
    df["score"] = (
        25 * df["nifty_trend"] + 10 * df["nifty_trend50"]
        + 0.25 * df["breadth200"] + 0.10 * df["breadth50"]
        + 0.20 * (100 - df["vix_pct"])
        + 0.10 * (df["adv_dec10"].clip(-50, 50) + 50)
    )
    # normalize to 0..100 (max possible weight sum): 25+10+25+10+20+10 = 100
    df["state"] = pd.cut(df["score"], [-1, 45, 65, 101], labels=["RISK_OFF", "NEUTRAL", "RISK_ON"])
    return df, c


def validate(df, c):
    # forward 20-trading-day returns of NIFTY and EW-441
    fwd_nifty = df["nifty"].shift(-20) / df["nifty"] - 1
    ew = c.pct_change(fill_method=None).mean(1)                       # daily EW-441 return
    ew_eq = (1 + ew).cumprod()
    fwd_ew = ew_eq.shift(-20) / ew_eq - 1
    g = pd.DataFrame({"state": df["state"], "fwd_nifty": fwd_nifty * 100, "fwd_ew": fwd_ew * 100}).dropna()
    print("=== VALIDATION: forward 20-day % return by TODAY's regime state (leak-free) ===")
    print(f"  {'state':<10}{'days':>7}{'NIFTY fwd20 avg':>18}{'hitrate>0':>11}{'EW441 fwd20 avg':>18}{'hitrate>0':>11}")
    for st in ["RISK_ON", "NEUTRAL", "RISK_OFF"]:
        s = g[g.state == st]
        if len(s):
            print(f"  {st:<10}{len(s):>7}{s.fwd_nifty.mean():>17.2f}%{(s.fwd_nifty>0).mean()*100:>10.0f}%"
                  f"{s.fwd_ew.mean():>17.2f}%{(s.fwd_ew>0).mean()*100:>10.0f}%")
    # also worst-case: forward min (drawdown proxy) by state
    print("\n  downside check: std of NIFTY fwd20 by state (risk-off should be most volatile)")
    for st in ["RISK_ON", "NEUTRAL", "RISK_OFF"]:
        s = g[g.state == st]
        if len(s):
            print(f"    {st:<10} std {s.fwd_nifty.std():.2f}%   5th-pctile {np.percentile(s.fwd_nifty,5):.2f}%")


def sector_strength(topn=6):
    con = sqlite3.connect("file:" + Path(KDB).as_posix() + "?mode=ro", uri=True)
    lab = pd.read_sql_query("SELECT symbol,sector FROM instrument_labels", con); con.close()
    fields, _ = DC.wide_all()
    c = fields["c"]
    sec = lab.dropna(subset=["sector"]).set_index("symbol")["sector"].to_dict()
    ret63 = c.iloc[-1] / c.iloc[-64] - 1                              # 3-month per stock
    byname = pd.Series({s: sec.get(s) for s in c.columns})
    d = pd.DataFrame({"sector": byname, "r": ret63}).dropna()
    strength = d.groupby("sector")["r"].median().sort_values(ascending=False) * 100
    print(f"\n=== SECTOR STRENGTH (median 3-month return, as of {c.index[-1].date()}) ===")
    for i, (s, v) in enumerate(strength.items()):
        tag = "  <= leaders" if i < topn else ("  <= laggards" if i >= len(strength) - topn else "")
        print(f"  {s:<34}{v:>+7.1f}%{tag}")


def main():
    df, c = build_regime()
    cur = df.dropna().iloc[-1]
    print(f"=== CURRENT REGIME as of {df.dropna().index[-1].date()} ===")
    print(f"  state={cur['state']}  score={cur['score']:.0f}/100 | NIFTY>200DMA={int(cur['nifty_trend'])} "
          f"breadth200={cur['breadth200']:.0f}% breadth50={cur['breadth50']:.0f}% "
          f"VIX={cur['vix']:.1f}(pct {cur['vix_pct']:.0f}) adv/dec10={cur['adv_dec10']:.0f}\n")
    # time in each state
    vc = df["state"].value_counts(normalize=True) * 100
    print("  time spent:", {k: f"{vc.get(k,0):.0f}%" for k in ["RISK_ON", "NEUTRAL", "RISK_OFF"]}, "\n")
    validate(df, c)
    sector_strength()


if __name__ == "__main__":
    main()
