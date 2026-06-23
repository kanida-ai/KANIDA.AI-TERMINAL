"""What happened May-12 -> 2026-06-23: market summary + volatility TOUCH screen on the
freshly fetched window (date-filtered queries -> fast)."""
from __future__ import annotations
import numpy as np, pandas as pd
from persona_engine import db, universe

START = "2026-05-12"
NPICK = 5


def run(con, fo):
    qmarks = ",".join("?"*len(fo))
    # ---- market context (daily) ----
    nifty = pd.read_sql_query("SELECT trade_date, close FROM ohlc_daily WHERE symbol='NIFTY50' "
                              "AND trade_date>=? ORDER BY trade_date", con, params=[START])
    dly = pd.read_sql_query(f"SELECT symbol,trade_date,open,close FROM ohlc_daily WHERE symbol IN ({qmarks}) "
                            "AND trade_date>=?", con, params=fo+[START])
    dly["ret"] = (dly["close"]/dly["open"]-1)*100
    mkt = dly.groupby("trade_date")["ret"].agg(["mean", lambda s: (s > 0).mean()*100]).reset_index()
    mkt.columns = ["date", "univ_avg_oc%", "univ_%up"]
    print("=== MARKET (F&O universe, open->close daily) May-12 -> today ===")
    if len(nifty) >= 2:
        print(f"NIFTY50: {nifty['close'].iloc[0]:.0f} -> {nifty['close'].iloc[-1]:.0f} "
              f"({(nifty['close'].iloc[-1]/nifty['close'].iloc[0]-1)*100:+.1f}% over window)")
    print(f"universe avg open->close/day: {mkt['univ_avg_oc%'].mean():+.2f}%   "
          f"avg %stocks up/day: {mkt['univ_%up'].mean():.0f}%   trading days: {mkt.shape[0]}")

    # ---- morning volatility (9:15-10:00) ----
    m = pd.DataFrame(con.execute(
        f"SELECT symbol, bar_time, close FROM ohlc_1min WHERE bar_time>=? "
        f"AND substr(bar_time,12,5)>='09:15' AND substr(bar_time,12,5)<='10:00' AND symbol IN ({qmarks})",
        [START+" 00:00:00"]+fo).fetchall(), columns=["symbol", "bar_time", "c"])
    m["date"] = m["bar_time"].str[:10]
    m["ret"] = m.groupby(["symbol", "date"])["c"].pct_change()
    volat = m.groupby(["symbol", "date"])["ret"].std().mul(100).rename("m_volat").reset_index()

    # ---- touch window (10:00-15:15) ----
    rows = con.execute(
        f"SELECT symbol, substr(bar_time,1,10) d, "
        f"MIN(CASE WHEN substr(bar_time,12,5)='10:00' THEN open END) entry, "
        f"MAX(high) hi, MIN(low) lo FROM ohlc_1min WHERE bar_time>=? "
        f"AND substr(bar_time,12,5)>='10:00' AND substr(bar_time,12,5)<='15:15' AND symbol IN ({qmarks}) "
        f"GROUP BY symbol, d", [START+" 00:00:00"]+fo).fetchall()
    t = pd.DataFrame(rows, columns=["symbol", "date", "entry", "hi", "lo"]).dropna(subset=["entry"])
    t = t.merge(volat, on=["symbol", "date"], how="inner")
    t["up"] = (t["hi"]/t["entry"]-1)*100
    t["dn"] = (t["lo"]/t["entry"]-1)*100
    t["rk"] = t.groupby("date")["m_volat"].rank(ascending=False, method="first")
    picks = t[t["rk"] <= NPICK].copy()
    for X in (1, 2, 3):
        picks[f"t{X}"] = ((picks["up"] >= X) | (picks["dn"] <= -X)).astype(int)

    daily = picks.groupby("date").agg(t1=("t1", "sum"), t2=("t2", "sum"), t3=("t3", "sum")).reset_index()
    print(f"\n=== TOUCH SCREEN on the new window ({daily.shape[0]} days) ===")
    print(f"avg of 5 touching: +/-1%={daily['t1'].mean():.2f}  +/-2%={daily['t2'].mean():.2f}  +/-3%={daily['t3'].mean():.2f}")
    print(f"% days >=4/5 @1%: {(daily['t1']>=4).mean()*100:.0f}%   >=3/5 @2%: {(daily['t2']>=3).mean()*100:.0f}%   "
          f">=2/5 @3%: {(daily['t3']>=2).mean()*100:.0f}%")
    print("\ndate        @1 @2 @3 | top-5 (max_up% / max_dn%)")
    for _, r in daily.iterrows():
        d = picks[picks["date"] == r["date"]].sort_values("rk")
        names = ", ".join(f"{x.symbol}({x.up:+.1f}/{x.dn:+.1f})" for x in d.itertuples())
        print(f"{r['date']}  {int(r['t1'])}  {int(r['t2'])}  {int(r['t3'])} | {names}")


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("RECENTONLY_DONE")
