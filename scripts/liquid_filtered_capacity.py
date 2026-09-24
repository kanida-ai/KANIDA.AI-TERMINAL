"""
Liquidity-filtered Falcon Top-5 — return + capacity
====================================================

Draw the daily basket from the Top-10 pool, keep only names with ADV >= ₹100 Cr,
take the top 5 by engine rank, run the SAME intraday backtest (+1% portfolio
target, 09:15 entry, exit next candle / 15:29). Then re-run the square-root
market-impact capacity model on this liquid basket.

Answers: (1) does the edge survive on liquid-only names? (2) how much higher is
the capacity ceiling? Compared head-to-head with the unfiltered Top-5 baseline.
"""
from __future__ import annotations
import math, sqlite3
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import numpy as np
import pandas as pd

from falcon_intraday_backtest import (
    Config, DEFAULT_DB, load_signals, load_ohlc_1min_day, simulate_day, DEFAULT_ALIASES)

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs" / "Falcon_Liquid_Filtered_Capacity.xlsx"
DESKTOP = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
ADV_MIN_CR = 100.0          # liquidity filter
BASE_CAPITAL = 500_000.0
TARGET = 1.0
BASKET = 5
N_USERS = [100, 500, 1_000, 2_000, 5_000, 10_000, 25_000, 50_000, 100_000]
CAPITALS = [25_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000, 10_000_000]
CAP_LABEL = {25_000: "₹25K", 50_000: "₹50K", 100_000: "₹1L", 500_000: "₹5L",
             1_000_000: "₹10L", 5_000_000: "₹50L", 10_000_000: "₹1Cr"}
EF = {"A": 1.0, "C": 1/math.sqrt(15)}     # entry timing: A=simultaneous, C=15min stagger


def adv_map():
    con = sqlite3.connect(str(DEFAULT_DB))
    syms = [r[0] for r in con.execute(
        "SELECT DISTINCT symbol FROM falcon_signal_day_study "
        "WHERE persona='falcon_top10_daily' AND engine_rank<=10")]
    osyms = sorted(set(DEFAULT_ALIASES.get(s, s) for s in syms))
    ph = ",".join("?" * len(osyms))
    rows = con.execute(
        f"SELECT symbol, AVG(close*volume) FROM ohlc_daily "
        f"WHERE trade_date BETWEEN '2024-05-14' AND '2026-06-15' AND symbol IN ({ph}) "
        f"GROUP BY symbol", osyms).fetchall()
    con.close()
    m = {s: (v or 0) for s, v in rows}
    return {s: m.get(DEFAULT_ALIASES.get(s, s), 0.0) for s in syms}


def process_chunk(payload):
    db, days_chunk, signals, advs = payload
    cfg = Config(db_path=Path(db))
    con = sqlite3.connect(db)
    out = []
    for day in days_chunk:
        picks = signals.get(day, [])
        liquid = [(rk, s) for rk, s in picks if advs.get(s, 0) / 1e7 >= ADV_MIN_CR][:BASKET]
        if not liquid:
            continue
        day_ohlc = load_ohlc_1min_day(con, day, [s for _, s in liquid], DEFAULT_ALIASES)
        res = simulate_day(liquid, day_ohlc, BASE_CAPITAL, TARGET)
        if res is None:
            continue
        out.append({
            "date": day, "syms": res["symbols"],
            "ent": [l["entry_price"] for l in res["legs"]],
            "exi": [l["exit_price"] for l in res["legs"]],
            "port_ret": res["portfolio_return_pct"], "n": res["n_stocks"],
            "advrs": [advs.get(s, 0) for s in res["symbols"]],
        })
    con.close()
    return out


def coef_of(adv_cr):
    return 0.05 if adv_cr >= 500 else 0.10      # all liquid names are >=100Cr


def prep_days(records):
    days = []
    for r in records:
        ent = np.array(r["ent"]); exi = np.array(r["exi"])
        qty = np.floor((BASE_CAPITAL / len(ent)) / ent)
        w = (qty * ent) / (qty * ent).sum()
        coef = np.array([coef_of(a / 1e7) for a in r["advrs"]])
        days.append({"w": w, "ent": ent, "exi": exi,
                     "r_base": (exi - ent) / ent * 100.0,
                     "advrs": np.array(r["advrs"]), "coef": coef,
                     "base_port": r["port_ret"], "date": r["date"]})
    return days


def sim_impact(days, n_users, capital, ef, xf=1.0):
    flow = n_users * capital / BASKET
    rets = []
    for d in days:
        part = np.where(d["advrs"] > 0, flow / d["advrs"], 0.0)
        ei = d["coef"] * np.sqrt(part) * ef * 100.0
        xi = d["coef"] * np.sqrt(part) * xf * 100.0
        se = d["ent"] * (1 + ei / 100); sx = d["exi"] * (1 - xi / 100)
        r = (sx - se) / se * 100.0
        rets.append(float((d["w"] * r).sum()))
    return np.array(rets)


def avg_at(days, n, cap, ef):
    return sim_impact(days, n, cap, ef).mean()


def max_users(days, cap, thresh, ef):
    lo, hi = 1, 20_000_000
    if avg_at(days, lo, cap, ef) < thresh: return 0
    if avg_at(days, hi, cap, ef) >= thresh: return hi
    while hi - lo > max(1, lo * 0.01):
        m = (lo + hi) // 2
        if avg_at(days, m, cap, ef) >= thresh: lo = m
        else: hi = m
    return lo


def fmt_cr(x): return f"₹{x/1e7:,.2f} Cr"


def main():
    cfg = Config()
    con = sqlite3.connect(str(cfg.db_path))
    signals = load_signals(con, cfg)
    min1, max1 = con.execute(
        "SELECT min(substr(bar_time,1,10)),max(substr(bar_time,1,10)) FROM ohlc_1min").fetchone()
    con.close()
    advs = adv_map()
    print(f"[*] ADV map {len(advs)} syms; liquid>=₹{ADV_MIN_CR:.0f}Cr", flush=True)
    days_list = sorted(d for d in signals if min1 <= d <= max1)

    nw = 10
    chunks = [days_list[i::nw] for i in range(nw)]
    payloads = [(str(cfg.db_path), ch, signals, advs) for ch in chunks if ch]
    records = []
    with ProcessPoolExecutor(max_workers=nw) as ex:
        for part in ex.map(process_chunk, payloads):
            records.extend(part)
    records.sort(key=lambda r: r["date"])
    print(f"[*] liquid trading days: {len(records)}", flush=True)

    days = prep_days(records)
    base_avg = float(np.mean([d["base_port"] for d in days]))
    base_wr = float(np.mean([d["base_port"] > 0 for d in days]) * 100)
    nnames = np.array([len(d["w"]) for d in days])
    print(f"[*] LIQUID baseline: avg {base_avg:.3f}%/day, WR {base_wr:.1f}%, "
          f"avg names/day {nnames.mean():.2f}, days<5 names {int((nnames<5).sum())}", flush=True)

    # capacity ceiling (Model A worst case + Model C 15-min stagger)
    t50, t20 = base_avg * 0.5, base_avg * 0.2
    rows = []
    for cap in CAPITALS:
        rows.append({
            "capital": CAP_LABEL[cap],
            "A_users_50%": max_users(days, cap, t50, EF["A"]),
            "A_breakeven_users": max_users(days, cap, 1e-9, EF["A"]),
            "A_breakeven_AUM": fmt_cr(max_users(days, cap, 1e-9, EF["A"]) * cap),
            "C15_users_50%": max_users(days, cap, t50, EF["C"]),
            "C15_breakeven_AUM": fmt_cr(max_users(days, cap, 1e-9, EF["C"]) * cap),
        })
    s_ceiling = pd.DataFrame(rows)

    # comparison vs unfiltered baseline (from prior capacity sim)
    unfilt = {"strategy": "Unfiltered Top-5 (mostly mid/small-cap)",
              "avg_per_day_%": 0.607, "win_rate_%": 80.3, "avg_names_day": 5.0,
              "50%_erosion_AUM_modelA": "₹0.1 Cr", "breakeven_AUM_modelA": "₹0.3 Cr"}
    a50_aum_50K = max_users(days, 50_000, t50, EF["A"]) * 50_000
    c50_aum_50K = max_users(days, 50_000, t50, EF["C"]) * 50_000
    liq = {"strategy": f"Liquid-only ≥₹{ADV_MIN_CR:.0f}Cr (Top-5 from Top-10)",
           "avg_per_day_%": round(base_avg, 3), "win_rate_%": round(base_wr, 1),
           "avg_names_day": round(float(nnames.mean()), 2),
           "50%_erosion_AUM_modelA": fmt_cr(a50_aum_50K),
           "breakeven_AUM_modelA": fmt_cr(max_users(days, 50_000, 1e-9, EF["A"]) * 50_000)}
    s_comp = pd.DataFrame([unfilt, liq])

    # master per (users x capital) Model A
    master = []
    for cap in CAPITALS:
        for n in N_USERS:
            rA = sim_impact(days, n, cap, EF["A"]); rC = sim_impact(days, n, cap, EF["C"])
            master.append({
                "n_users": n, "capital": CAP_LABEL[cap], "total_aum": fmt_cr(n * cap),
                "flow_per_stock": fmt_cr(n * cap / BASKET),
                "simA_avg_return": round(rA.mean(), 3), "simA_win_rate": round((rA > 0).mean() * 100, 1),
                "simC15_avg_return": round(rC.mean(), 3),
                "degradation_A_%": round((base_avg - rA.mean()) / base_avg * 100, 1) if base_avg else None})
    s_master = pd.DataFrame(master)

    # daily log — COMPLETE
    s_daily = pd.DataFrame([{
        "entry_date": d["date"],
        "stocks": ", ".join(records[i]["syms"]),
        "n_names": len(d["w"]),
        "entry_prices": ", ".join(f"{x:.2f}" for x in d["ent"]),
        "exit_prices": ", ".join(f"{x:.2f}" for x in d["exi"]),
        "per_stock_return_pct": ", ".join(f"{x:.2f}" for x in d["r_base"]),
        "adv_cr": ", ".join(f"{a/1e7:.0f}" for a in d["advrs"]),
        "portfolio_return_pct": round(d["base_port"], 3),
        "win": "WIN" if d["base_port"] > 0 else "LOSS"} for i, d in enumerate(days)])
    s_daily.to_csv(ROOT / "outputs" / "Falcon_Liquid_TradeLog.csv", index=False)
    nwin = int((s_daily["win"] == "WIN").sum())
    print(f"[*] trade log: {len(s_daily)} days, {nwin} wins, {len(s_daily)-nwin} losses "
          f"({nwin/len(s_daily)*100:.1f}% WR)", flush=True)

    # EXPLODED: one row per stock per day, with qty + ₹ P&L (capital ₹5L/day equal-split)
    ex = []
    for i, d in enumerate(days):
        n = len(d["ent"]); alloc = BASE_CAPITAL / n
        for j in range(n):
            entp, exip = float(d["ent"][j]), float(d["exi"][j])
            qty = math.floor(alloc / entp)
            deployed = qty * entp; exit_val = qty * exip
            ex.append({
                "entry_date": d["date"], "stock": records[i]["syms"][j],
                "adv_cr": round(d["advrs"][j] / 1e7, 0), "qty": qty,
                "entry_price": round(entp, 2), "exit_price": round(exip, 2),
                "deployed_rs": round(deployed, 0), "exit_value_rs": round(exit_val, 0),
                "pnl_rs": round(exit_val - deployed, 0),
                "stock_return_pct": round((exip - entp) / entp * 100, 2),
                "day_portfolio_return_pct": round(d["base_port"], 3),
                "day_result": "WIN" if d["base_port"] > 0 else "LOSS"})
    s_explode = pd.DataFrame(ex)
    s_explode.to_csv(ROOT / "outputs" / "Falcon_Liquid_TradeLog_Exploded.csv", index=False)
    print(f"[*] exploded log: {len(s_explode)} stock-day rows, "
          f"total ₹P&L {s_explode['pnl_rs'].sum():,.0f} on ₹5L/day", flush=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        s_comp.to_excel(xl, sheet_name="1_Compare_vs_Unfiltered", index=False)
        s_ceiling.to_excel(xl, sheet_name="2_Capacity_Ceiling", index=False)
        s_master.to_excel(xl, sheet_name="3_Master_Users_x_Capital", index=False)
        s_daily.to_excel(xl, sheet_name="4_Daily_Log", index=False)
        s_explode.to_excel(xl, sheet_name="5_TradeLog_Exploded", index=False)

    print("\n=== COMPARISON ===")
    print(s_comp.to_string(index=False))
    print("\n=== CAPACITY CEILING (liquid-only) ===")
    print(s_ceiling.to_string(index=False))
    print(f"\n[*] wrote {OUT}")
    if DESKTOP.exists():
        import shutil; shutil.copy(OUT, DESKTOP / OUT.name)
        print(f"[*] copied to {DESKTOP / OUT.name}")


if __name__ == "__main__":
    main()
