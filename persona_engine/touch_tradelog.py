"""
Complete TOUCH-based trade log + consistency/repeatability breakdown.

Strategy: each day rank F&O stocks by morning (9:15-10:00) volatility, take top-5.
Enter 10:00, set +/-X% intraday targets. A pick HITS X% if its 10:00->15:15 high
reaches +X% (long touch) or low reaches -X% (short touch); 'either' = magnitude touch.
No training (pure rule) -> evaluated on ALL intraday days for true repeatability.

Outputs (outputs/persona_findings/):
  TouchTradeLog.xlsx -> trades, daily_summary, monthly, yearly, consistency
"""
from __future__ import annotations
from pathlib import Path
import numpy as np, pandas as pd
from persona_engine import db, universe
from persona_engine.intraday_eod import morning_features

NPICK = 5
OUT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\outputs\persona_findings")


def touch_window(con, fo):
    rows = con.execute(
        "SELECT symbol, substr(bar_time,1,10) d, "
        "MIN(CASE WHEN substr(bar_time,12,5)='10:00' THEN open END) entry, "
        "MAX(high) hi, MIN(low) lo, "
        "MAX(CASE WHEN substr(bar_time,12,5)='15:15' THEN close END) c1515 "
        "FROM ohlc_1min WHERE substr(bar_time,12,5) >= '10:00' AND substr(bar_time,12,5) <= '15:15' "
        "AND symbol IN (%s) GROUP BY symbol, d" % ",".join("?"*len(fo)), list(fo)).fetchall()
    return pd.DataFrame(rows, columns=["symbol", "date", "entry", "hi", "lo", "c1515"]).dropna(subset=["entry"])


def build(con, fo):
    mf = morning_features(con, fo)[["symbol", "date", "m_volat"]]
    t = touch_window(con, fo).merge(mf, on=["symbol", "date"], how="inner").dropna(subset=["m_volat", "entry"])
    t["up_touch"] = (t["hi"]/t["entry"]-1)*100
    t["dn_touch"] = (t["lo"]/t["entry"]-1)*100
    t["close_move"] = (t["c1515"]/t["entry"]-1)*100
    # rank by morning volatility per day, keep top-5
    t["rk"] = t.groupby("date")["m_volat"].rank(ascending=False, method="first")
    picks = t[t["rk"] <= NPICK].copy()
    for X in (1, 2, 3):
        picks[f"touch_{X}"] = ((picks["up_touch"] >= X) | (picks["dn_touch"] <= -X)).astype(int)
        picks[f"up_{X}"] = (picks["up_touch"] >= X).astype(int)
        picks[f"dn_{X}"] = (picks["dn_touch"] <= -X).astype(int)
    picks["dir_first"] = np.where(picks["up_touch"].abs() >= picks["dn_touch"].abs(), "UP", "DOWN")
    return picks.sort_values(["date", "rk"])


def summarize(picks):
    g = picks.groupby("date")
    daily = g.agg(
        n_picks=("symbol", "count"),
        touch1=("touch_1", "sum"), touch2=("touch_2", "sum"), touch3=("touch_3", "sum"),
        up1=("up_1", "sum"), dn1=("dn_1", "sum"),
    ).reset_index()
    daily["year"] = daily["date"].str[:4]; daily["month"] = daily["date"].str[:7]

    def brk(df, key):
        r = df.groupby(key).agg(
            days=("date", "count"),
            avg_touch1=("touch1", "mean"), avg_touch2=("touch2", "mean"), avg_touch3=("touch3", "mean"),
            pct_days_4of5_at1=("touch1", lambda s: (s >= 4).mean()*100),
            pct_days_3of5_at2=("touch2", lambda s: (s >= 3).mean()*100),
            pct_days_2of5_at3=("touch3", lambda s: (s >= 2).mean()*100),
        ).round(2).reset_index()
        return r
    monthly = brk(daily, "month"); yearly = brk(daily, "year")
    overall = pd.DataFrame([{
        "scope": "ALL", "trading_days": len(daily),
        "avg_touch1_of5": round(daily["touch1"].mean(), 2),
        "avg_touch2_of5": round(daily["touch2"].mean(), 2),
        "avg_touch3_of5": round(daily["touch3"].mean(), 2),
        "days_>=4of5_@1%": int((daily["touch1"] >= 4).sum()),
        "pct_days_>=4of5_@1%": round((daily["touch1"] >= 4).mean()*100, 1),
        "days_>=3of5_@2%": int((daily["touch2"] >= 3).sum()),
        "pct_days_>=3of5_@2%": round((daily["touch2"] >= 3).mean()*100, 1),
        "days_>=2of5_@3%": int((daily["touch3"] >= 2).sum()),
        "pct_days_>=2of5_@3%": round((daily["touch3"] >= 2).mean()*100, 1),
    }])
    return daily, monthly, yearly, overall


def run(con, fo):
    picks = build(con, fo)
    daily, monthly, yearly, overall = summarize(picks)
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "TouchTradeLog.xlsx"
    log = picks[["date", "rk", "symbol", "entry", "hi", "lo",
                 "up_touch", "dn_touch", "close_move", "dir_first",
                 "touch_1", "touch_2", "touch_3"]].rename(columns={
        "rk": "rank", "hi": "day_high_1015", "lo": "day_low_1015",
        "up_touch": "max_up_%", "dn_touch": "max_down_%", "close_move": "close_%"})
    for c in ["entry", "day_high_1015", "day_low_1015", "max_up_%", "max_down_%", "close_%"]:
        log[c] = log[c].round(2)
    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        overall.T.to_excel(xl, sheet_name="overall")
        yearly.to_excel(xl, sheet_name="yearly", index=False)
        monthly.to_excel(xl, sheet_name="monthly", index=False)
        daily.to_excel(xl, sheet_name="daily_summary", index=False)
        log.to_excel(xl, sheet_name="trades", index=False)
    # console
    print("=== OVERALL ==="); print(overall.T.to_string(header=False))
    print("\n=== YEARLY ==="); print(yearly.to_string(index=False))
    print("\n=== MONTHLY ==="); print(monthly.to_string(index=False))
    print("\nXLSX ->", path)
    return path


if __name__ == "__main__":
    con = db.connect()
    fo, _ = universe.get_universes(con, as_of_date="2026-06-23")
    run(con, fo)
    con.close()
    print("TOUCHLOG_DONE")
