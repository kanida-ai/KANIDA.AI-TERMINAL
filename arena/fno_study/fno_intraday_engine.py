"""STAGE 2 — intraday MFE/MAE engine over 1-min bars. For each F&O stock x trading day:
  Long book & Short book, Rs5L each: Rs2.5L @ 09:15 open + Rs2.5L @ 09:16 open -> blended avg cost, hold to 15:29 close.
  Records per side: MFE, MAE, EOD return (gross & net of real intraday cost stack), intraday vol, day range.
  Plus single-shot @09:15 benchmark (gross). Attaches Stage-1 Falcon tier/rank/score/pattern tags (signal_date = prev trading day).
Processes month-by-month, checkpoints arena/fno_study/records/<YYYY-MM>.csv. Read-only sources; Falcon untouched.
Usage: python fno_intraday_engine.py 2024-05 [2024-06 ...]   (no args = all months 2024-05..2026-07)"""
import os, sys, sqlite3
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UEDB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")   # ohlc_1min + fo_stock_master
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")                        # ohlc_daily (trading calendar)
OUT = os.path.join(ROOT, "arena", "fno_study", "records"); os.makedirs(OUT, exist_ok=True)
CAP = 5e5   # Rs5L per book

# F&O tradable universe
fc = sqlite3.connect("file:" + UEDB.replace("\\", "/") + "?mode=ro", uri=True)
FO = set(s for (s,) in fc.execute("SELECT DISTINCT symbol FROM fo_stock_master WHERE fo_eligible=1")
         if s not in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"))
fc.close()
# trading calendar -> prev trading day (for signal_date join)
dc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
CALL = [d for (d,) in dc.execute("SELECT DISTINCT trade_date FROM ohlc_daily WHERE trade_date>='2024-01-01' ORDER BY trade_date")]
dc.close()
PREV = {CALL[i]: CALL[i-1] for i in range(1, len(CALL))}
# Stage-1 tags keyed (symbol, signal_date)
TAGS = pd.read_csv(os.path.join(ROOT, "arena", "fno_study", "tier_tags.csv"))
TAGS = TAGS.set_index(["symbol", "signal_date"])

def cost_rs(buy_val, sell_val):
    brok = min(20, 0.0003*buy_val) + min(20, 0.0003*sell_val)
    stt = 0.00025 * sell_val; txn = 0.0000297*(buy_val+sell_val); stamp = 0.00003*buy_val
    sebi = 0.000001*(buy_val+sell_val); gst = 0.18*(brok+txn+sebi)
    return brok + stt + txn + stamp + sebi + gst

def process_month(ym):
    y, m = map(int, ym.split("-")); nxt = f"{y+(m//12):04d}-{(m%12)+1:02d}-01"
    con = sqlite3.connect("file:" + UEDB.replace("\\", "/") + "?mode=ro", uri=True)
    df = pd.read_sql_query("SELECT symbol,bar_time,open,high,low,close FROM ohlc_1min "
                           "WHERE bar_time>=? AND bar_time<?", con, params=(f"{ym}-01 00:00:00", f"{nxt} 00:00:00"))
    con.close()
    if df.empty: return None
    df = df[df.symbol.isin(FO)].copy()
    df["date"] = df.bar_time.str[:10]; df["hhmm"] = df.bar_time.str[11:16]
    df = df.sort_values(["symbol", "date", "bar_time"])
    key = ["symbol", "date"]
    p1 = df[df.hhmm == "09:15"].groupby(key).open.first()
    p2 = df[df.hhmm == "09:16"].groupby(key).open.first()
    path = df[df.hhmm >= "09:16"]                                 # blended entry complete at 09:16
    ph = path.groupby(key).high.max(); pl = path.groupby(key).low.min()
    lastc = df.groupby(key).close.last()                          # 15:29 close
    dayhi = df.groupby(key).high.max(); daylo = df.groupby(key).low.min()   # single-shot path (from 09:15)
    df["ret"] = df.groupby(key).close.pct_change()
    vol = df.groupby(key).ret.std() * 100
    B = pd.DataFrame({"p1": p1, "p2": p2, "pathHigh": ph, "pathLow": pl, "lastClose": lastc,
                      "dayHigh": dayhi, "dayLow": daylo, "vol": vol}).dropna(subset=["p1", "p2", "lastClose"])
    B = B.reset_index()
    # blended avg entry
    Q = CAP/2/B.p1 + CAP/2/B.p2; B["avg"] = CAP / Q
    a = B.avg
    B["L_mfe"] = (B.pathHigh - a)/a*100; B["L_mae"] = (B.pathLow - a)/a*100; B["L_eod_g"] = (B.lastClose - a)/a*100
    B["S_mfe"] = (a - B.pathLow)/a*100; B["S_mae"] = (a - B.pathHigh)/a*100; B["S_eod_g"] = (a - B.lastClose)/a*100
    B["range_pct"] = (B.dayHigh - B.dayLow)/a*100
    # single-shot @09:15 gross (entry=p1, exit=lastClose)
    B["SS_L_eod_g"] = (B.lastClose - B.p1)/B.p1*100; B["SS_S_eod_g"] = (B.p1 - B.lastClose)/B.p1*100
    # net of costs (per book): long buy=CAP sell=CAP*(1+eod); short sell=CAP buy=CAP*(1-S_eod... use values)
    lc = np.array([cost_rs(CAP, CAP*(1+e/100)) for e in B.L_eod_g])
    sc = np.array([cost_rs(CAP*(1 - s/100), CAP) for s in B.S_eod_g])   # short: buy-to-cover value, sell value=CAP
    B["L_eod_n"] = B.L_eod_g - lc/CAP*100; B["S_eod_n"] = B.S_eod_g - sc/CAP*100
    # attach tags (signal_date = prev trading day of `date`)
    B["signal_date"] = B.date.map(PREV)
    B = B.merge(TAGS, left_on=["symbol", "signal_date"], right_index=True, how="left")
    B["month"] = ym
    cols = ["symbol", "date", "month", "signal_date", "tier", "rank", "score", "avg_lift", "sret", "twoday", "rng",
            "entry_context", "regime", "sector", "p1", "p2", "avg", "lastClose", "vol", "range_pct",
            "L_mfe", "L_mae", "L_eod_g", "L_eod_n", "S_mfe", "S_mae", "S_eod_g", "S_eod_n", "SS_L_eod_g", "SS_S_eod_g"]
    return B[cols]

if __name__ == "__main__":
    months = sys.argv[1:]
    if not months:
        months = [f"{y}-{m:02d}" for y in (2024, 2025, 2026) for m in range(1, 13) if (y, m) >= (2024, 5) and (y, m) <= (2026, 7)]
    for ym in months:
        B = process_month(ym)
        if B is None or B.empty: print(f"{ym}: no data"); continue
        p = os.path.join(OUT, f"{ym}.csv"); B.to_csv(p, index=False)
        print(f"{ym}: {B.symbol.nunique()} stocks x {B.date.nunique()} days = {len(B):,} recs | "
              f"L_eod_net {B.L_eod_n.mean():+.3f}% S_eod_net {B.S_eod_n.mean():+.3f}% | tiers={B.tier.notna().mean()*100:.0f}% tagged -> {os.path.basename(p)}")
