"""
BIG-MOVER FEASIBILITY (foundation for the live +5%-by-EOD predictor)
====================================================================
Question: can we identify by ~10:00 which liquid stocks will CLOSE >=+5% today,
with enough move still left to capture?

Per stock-day (broad liquid universe), from 1-min bars, record state at 09:30/09:45/
10:00 (return from 09:15 open, intraday high so far, cumulative volume) and the
full-day outcome (09:15 open -> 15:29 close) + the capturable move (checkpoint->close).

Outputs the base rate of +5% closers, how much of their move is gone by each
checkpoint, and P(close>=+5%) conditioned on early-session return — i.e. whether
early state actually predicts the close. Also dumps the per-stock-day dataset
(parquet) for the predictor step. Read-only.
"""
import sqlite3
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
OUTX = ROOT / "outputs" / "BigMover_Feasibility.xlsx"
DATASET = ROOT / "outputs" / "_bigmover_1000_dataset.parquet"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
CHECKS = ["09:30", "09:45", "10:00"]
ADV_MIN = 50e7          # >= Rs 50 Cr average daily traded value
ALIASES = {"ZOMATO": "ETERNAL"}


def liquid_symbols():
    c = sqlite3.connect(str(RND))
    rows = c.execute("""SELECT symbol, AVG(close*volume) adv FROM ohlc_daily
                        WHERE trade_date>='2024-05-01' GROUP BY symbol HAVING adv>=?""",
                     (ADV_MIN,)).fetchall()
    c.close()
    return sorted(s for s, _ in rows)


def trading_days():
    c = sqlite3.connect(str(RND))
    d = [r[0] for r in c.execute(
        "SELECT DISTINCT substr(bar_time,1,10) FROM ohlc_1min WHERE substr(bar_time,1,10)>='2024-05-13' ORDER BY 1")]
    c.close()
    return d


def worker(day, syms):
    c = sqlite3.connect(str(RND))
    ph = ",".join("?" * len(syms))
    q = f"""SELECT symbol, substr(bar_time,12,5) hm, open, high, low, close, volume
            FROM ohlc_1min WHERE substr(bar_time,1,10)=? AND symbol IN ({ph})"""
    df = pd.read_sql_query(q, c, params=[day, *syms])
    c.close()
    if df.empty:
        return []
    # collapse aliases (ZOMATO->ETERNAL etc.)
    df["symbol"] = df["symbol"].replace(ALIASES)
    out = []
    for sym, g in df.groupby("symbol"):
        g = g.sort_values("hm")
        gi = g.set_index("hm")
        if "09:15" not in gi.index:
            continue
        o = gi.at["09:15", "open"]
        if isinstance(o, pd.Series):
            o = o.iloc[0]
        if not np.isfinite(o) or o <= 0:
            continue
        bars = list(gi.index)
        last_close = gi["close"].iloc[-1]
        rec = {"date": day, "symbol": sym, "open": float(o),
               "ret_full": (last_close / o - 1) * 100.0}
        for t in CHECKS:
            upto = [b for b in bars if b <= t and b >= "09:15"]
            if not upto:
                rec[f"ret_{t}"] = np.nan; rec[f"hi_{t}"] = np.nan
                rec[f"px_{t}"] = np.nan; rec[f"vol_{t}"] = np.nan
                continue
            sub = gi.loc[upto]
            px = sub["close"].iloc[-1]
            hi = sub["high"].max()
            rec[f"px_{t}"] = float(px)
            rec[f"ret_{t}"] = (px / o - 1) * 100.0
            rec[f"hi_{t}"] = (hi / o - 1) * 100.0          # best intraday so far
            rec[f"vol_{t}"] = float(sub["volume"].sum())
        # capturable move checkpoint->close
        for t in CHECKS:
            if np.isfinite(rec.get(f"px_{t}", np.nan)) and rec[f"px_{t}"] > 0:
                rec[f"cap_{t}_close"] = (last_close / rec[f"px_{t}"] - 1) * 100.0
            else:
                rec[f"cap_{t}_close"] = np.nan
        out.append(rec)
    return out


def main():
    syms = liquid_symbols()
    days = trading_days()
    print(f"[*] liquid universe: {len(syms)} symbols, {len(days)} trading days", flush=True)
    rows = []
    with ProcessPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(worker, d, syms): d for d in days}
        done = 0
        for f in as_completed(futs):
            rows.extend(f.result())
            done += 1
            if done % 100 == 0:
                print(f"  [{done}/{len(days)}] scanned", flush=True)
    df = pd.DataFrame(rows)
    df.to_parquet(DATASET)
    print(f"[*] dataset rows: {len(df):,}  ->  {DATASET}", flush=True)

    n = len(df)
    win5 = df["ret_full"] >= 5.0
    base = win5.mean() * 100
    print(f"\n=== BASE RATE ===")
    print(f"  stock-days: {n:,}   close>=+5%: {win5.sum():,}  ({base:.2f}% base rate)")
    print(f"  avg +5%-closers/day: {win5.sum()/df.date.nunique():.1f}")

    # for +5% closers: how much of the move is gone by each checkpoint?
    w = df[win5]
    rows_done = []
    for t in CHECKS:
        frac_done = (w[f"ret_{t}"] / w["ret_full"]).clip(-1, 2)   # share of full move complete
        cap = w[f"cap_{t}_close"]
        rows_done.append({"checkpoint": t,
                          "median_ret_by_t%": round(w[f"ret_{t}"].median(), 2),
                          "median_%move_done": round(frac_done.median() * 100, 1),
                          "median_capturable_to_close%": round(cap.median(), 2),
                          "still_>3%_left_share": round((cap >= 3).mean() * 100, 1)})
    done_df = pd.DataFrame(rows_done)
    print(f"\n=== For +5% closers: how much move is left? ===")
    print(done_df.to_string(index=False))

    # conditional: P(close>=+5%) by early-session return bucket at 10:00
    t = "10:00"
    df["_b"] = pd.cut(df[f"ret_{t}"], [-100, 0, 1, 2, 3, 4, 5, 100],
                      labels=["<0", "0-1", "1-2", "2-3", "3-4", "4-5", ">5"])
    cond = df.groupby("_b", observed=True).apply(
        lambda g: pd.Series({
            "n": len(g),
            "P(close>=5%)%": round((g.ret_full >= 5).mean() * 100, 1),
            "avg_capture_to_close%": round(g[f"cap_{t}_close"].mean(), 2),
        }), include_groups=False).reset_index()
    print(f"\n=== P(close>=+5%) conditioned on return by 10:00 ===")
    print(cond.to_string(index=False))

    OUTX.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUTX, engine="openpyxl") as xl:
        pd.DataFrame([{"stock_days": n, "plus5_closers": int(win5.sum()),
                       "base_rate_%": round(base, 2),
                       "avg_plus5_per_day": round(win5.sum()/df.date.nunique(), 2)}]).to_excel(
            xl, "1_BaseRate", index=False)
        done_df.to_excel(xl, "2_MoveLeft_for5pc", index=False)
        cond.to_excel(xl, "3_Pclose_by_1000_return", index=False)
    print(f"\n[*] wrote {OUTX}")
    if DESK.exists():
        import shutil; shutil.copy(OUTX, DESK / OUTX.name); print(f"[*] copied to {DESK/OUTX.name}")


if __name__ == "__main__":
    main()
