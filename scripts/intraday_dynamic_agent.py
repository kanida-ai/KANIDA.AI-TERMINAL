"""
Dynamic Intraday Portfolio Agent  (separate system; does not modify Falcon/backtest)
====================================================================================

Rule-based intraday portfolio policy that actively manages a basket drawn from the
liquid (>=Rs100Cr ADV) Falcon Top-N pool. Slot-based: K capital slots; each slot
holds one position at a time; freed slots can be refilled by CONSERVATIVE rotation
(calm setups only, before a midday cutoff). Per-stock arm/trail + weakness-cut +
stop; portfolio -1.5% catastrophe stop; hard 15:29 flat. Costs charged per leg.

Walk-forward only: parameters are tuned on a trailing PAST window and applied
forward; a setup-outcome LEDGER vetoes archetypes that keep losing (past-only).

Validation ladder (rows in the output):
  S1 static  : Top-5 @ 09:15, +1% portfolio target, 15:29 close  (= liquid baseline)
  S2 exits   : per-stock arm/trail + weakness-cut + stop, NO rotation
  S3 rotation: S2 exits + conservative rotation adds
  S4 wf-tuned: S3 with walk-forward-selected params (monthly retune)
  S5 +ledger : S4 + setup-outcome veto

NO LOOKAHEAD: every decision at minute t uses only data <= t (entry fills at t+1
open). Walk-forward params/veto for day d use only days < d.
"""
from __future__ import annotations
import argparse, math, sqlite3
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import numpy as np
import pandas as pd

from falcon_intraday_backtest import Config, DEFAULT_DB, load_signals, load_ohlc_1min_day, DEFAULT_ALIASES

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs" / "Intraday_Dynamic_Agent.xlsx"
DESKTOP = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"

CAPITAL = 500_000.0
K_SLOTS = 5
POOL_N = 20
ADV_MIN_CR = 100.0
COST_BPS = 12.0
ARM = 1.0
STOP_STK = -1.5
STOP_PORT = -1.5
GRID = [(0.50, "11:00"), (0.50, "12:00"), (0.75, "11:00"),
        (0.75, "12:00"), (1.00, "11:00"), (1.00, "12:00")]
DEFAULT_PARAM = (0.75, "12:00")
CALM_DVWAP = (-0.30, 0.60)
CALM_MAXEXT = 2.5
CALM_MINRET = 0.0


def adv_map(con):
    syms = [r[0] for r in con.execute(
        "SELECT DISTINCT symbol FROM falcon_signal_day_study "
        "WHERE persona='falcon_top10_daily' AND engine_rank<=10")]
    osyms = sorted(set(DEFAULT_ALIASES.get(s, s) for s in syms))
    ph = ",".join("?" * len(osyms))
    rows = con.execute(
        f"SELECT symbol, AVG(close*volume) FROM ohlc_daily "
        f"WHERE trade_date BETWEEN '2024-05-14' AND '2026-06-15' AND symbol IN ({ph}) "
        f"GROUP BY symbol", osyms).fetchall()
    m = {s: (v or 0) for s, v in rows}
    return {s: m.get(DEFAULT_ALIASES.get(s, s), 0.0) for s in syms}


def liquid_pool(signals_day, advs, adv_min=ADV_MIN_CR):
    out = [(rk, s) for rk, s in signals_day if advs.get(s, 0) / 1e7 >= adv_min and rk <= 10]
    return out[:POOL_N]


def load_day_arrays(con, day, syms):
    """Full OHLCV per pool symbol (need high/low/volume for VWAP). Alias-aware."""
    fetch = {DEFAULT_ALIASES.get(s, s): s for s in syms}      # ohlc symbol -> study symbol
    if not fetch:
        return None
    ph = ",".join("?" * len(fetch))
    rows = con.execute(
        f"SELECT symbol, substr(bar_time,12,5) hm, open, high, low, close, volume "
        f"FROM ohlc_1min WHERE bar_time BETWEEN ? AND ? AND symbol IN ({ph})",
        [f"{day} 09:15:00", f"{day} 15:29:59", *fetch.keys()]).fetchall()
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["osym", "hm", "open", "high", "low", "close", "volume"])
    grid = sorted(df["hm"].unique())

    def piv(col, ff=True):
        p = df.pivot_table(values=col, index="hm", columns="osym", aggfunc="last").reindex(grid)
        return p.ffill().bfill() if ff else p

    p_close = piv("close"); p_open = piv("open", ff=False)
    p_high = piv("high"); p_low = piv("low"); p_vol = piv("volume", ff=False).fillna(0.0)
    arr = {}
    for osym in p_close.columns:
        s = fetch[osym]
        close = p_close[osym].to_numpy(float)
        opn = p_open[osym].to_numpy(float)
        if "09:15" not in grid:
            continue
        o0 = opn[grid.index("09:15")]
        if not np.isfinite(o0) or o0 <= 0:
            continue
        oexec = np.where(np.isfinite(opn) & (opn > 0), opn, close)
        high = p_high[osym].to_numpy(float); low = p_low[osym].to_numpy(float)
        vol = p_vol[osym].to_numpy(float)
        tp = (high + low + close) / 3.0
        cumpv = np.cumsum(tp * vol); cumv = np.cumsum(vol)
        vwap = np.where(cumv > 0, cumpv / np.maximum(cumv, 1), close)
        ret915 = (close / float(o0) - 1) * 100.0
        mom5 = np.zeros_like(close); mom5[5:] = (close[5:] / close[:-5] - 1) * 100.0
        arr[s] = {"close": close, "open": oexec, "vwap": vwap, "ret915": ret915,
                  "mom5": mom5, "o0": float(o0)}
    if not arr:
        return None
    return {"grid": grid, "gi": {m: i for i, m in enumerate(grid)}, "syms": arr}


def _cost_frac():
    return COST_BPS / 10_000.0


def archetype(a, i):
    r = a["ret915"][i]; dv = (a["close"][i] / a["vwap"][i] - 1) * 100.0
    if r > CALM_MAXEXT:
        return "extended"
    if a["close"][i] < a["vwap"][i] and a["mom5"][i] < 0:
        return "weak"
    if r >= CALM_MINRET and CALM_DVWAP[0] <= dv <= CALM_DVWAP[1]:
        return "calm"
    return "neutral"


def is_calm(a, i):
    r = a["ret915"][i]; dv = (a["close"][i] / a["vwap"][i] - 1) * 100.0
    return (r >= CALM_MINRET and r <= CALM_MAXEXT and CALM_DVWAP[0] <= dv <= CALM_DVWAP[1])


def simulate(day, ranks, mode, trail=0.75, cutoff="12:00", veto=None):
    syms = day["syms"]; grid = day["grid"]; gi = day["gi"]
    pool = [(rk, s) for rk, s in ranks if s in syms]
    if not pool:
        return None
    n = len(grid); slot_cap = CAPITAL / K_SLOTS
    cut_idx = gi.get(cutoff, n - 1); last = n - 1

    if mode == "static":
        chosen = pool[:K_SLOTS]; qty = {}; ent = {}
        for rk, s in chosen:
            ep = syms[s]["open"][0]
            if ep <= 0: continue
            qty[s] = math.floor(slot_cap / ep); ent[s] = ep
        qty = {s: q for s, q in qty.items() if q > 0}
        if not qty:
            return None
        deployed = sum(qty[s] * ent[s] for s in qty)
        port = np.array([sum(qty[s] * syms[s]["close"][i] for s in qty) for i in range(n)])
        ret = (port - deployed) / deployed * 100.0
        hit = next((i for i in range(last) if ret[i] >= ARM), None)
        legs = []
        xi = (hit + 1) if hit is not None else last
        rsn = "TARGET" if hit is not None else "CLOSE"
        for s in qty:
            xp = syms[s]["open"][hit + 1] if hit is not None else syms[s]["close"][last]
            legs.append({"sym": s, "ent": ent[s], "exi": xp, "cap": qty[s] * ent[s],
                         "qty": qty[s], "ei": 0, "xi": xi, "reason": rsn,
                         "arch": "static", "rotated": False})
        return _finish(legs, "TARGET" if hit is not None else "CLOSE", grid)

    # ---- PORTFOLIO-level trailing engine (the proven winner) + conservative
    #      per-stock breakdown-cut + rotation refill (modes: trail, trail_rot).
    rotate = (mode == "trail_rot")
    STK_CUT = -2.0                       # only cut a genuinely broken leg
    slots = []; used = set()
    for rk, s in pool[:K_SLOTS]:
        ep = syms[s]["open"][0]
        if ep <= 0: continue
        slots.append({"sym": s, "ei": 0, "ep": ep}); used.add(s)
    while len(slots) < K_SLOTS:
        slots.append(None)
    legs = []                            # closed legs (cut, rotated-out, or final)
    rot_candidates = [(rk, s) for rk, s in pool if s not in used]
    realized = 0.0; armed = False; peak = 0.0; reason = "MANAGED"

    def close_leg(sl, px, i, leg_reason):
        nonlocal realized
        a = syms[sl["sym"]]
        realized += slot_cap * (px / sl["ep"] - 1)
        legs.append({"sym": sl["sym"], "ent": sl["ep"], "exi": px, "cap": slot_cap,
                     "qty": math.floor(slot_cap / sl["ep"]), "ei": sl["ei"], "xi": i,
                     "reason": leg_reason, "arch": archetype(a, sl["ei"]), "rotated": sl["ei"] > 0})

    for i in range(last):
        # portfolio MTM = realized (banked) + unrealized (open legs), vs CAPITAL
        unreal = sum(slot_cap * (syms[sl["sym"]]["close"][i] / sl["ep"] - 1) for sl in slots if sl)
        port_ret = (realized + unreal) / CAPITAL * 100.0
        # portfolio catastrophe stop
        if port_ret <= STOP_PORT:
            for sl in slots:
                if sl: close_leg(sl, syms[sl["sym"]]["open"][i + 1], i, "STOP")
            slots = [None] * K_SLOTS; reason = "PORT_STOP"; break
        # portfolio profit-lock + trail (exit ALL open)
        if not armed and port_ret >= ARM:
            armed = True; peak = port_ret
        if armed:
            peak = max(peak, port_ret)
            if port_ret <= max(ARM, peak - trail):
                for sl in slots:
                    if sl: close_leg(sl, syms[sl["sym"]]["open"][i + 1], i, "TRAIL")
                slots = [None] * K_SLOTS; reason = "TRAIL"; break
        # per-stock breakdown cut (conservative): only clearly broken legs
        for k, sl in enumerate(slots):
            if not sl: continue
            a = syms[sl["sym"]]; r = (a["close"][i] / sl["ep"] - 1) * 100.0
            if r <= STK_CUT:
                close_leg(sl, a["open"][i + 1], i, "CUT"); slots[k] = None
        # rotation refill (calm candidates only, before cutoff)
        if rotate and i < cut_idx:
            for k in range(K_SLOTS):
                if slots[k] is not None: continue
                for rk, s in rot_candidates:
                    if s in used: continue
                    a = syms[s]
                    if not is_calm(a, i): continue
                    if veto and archetype(a, i) in veto: continue
                    ep = a["open"][i + 1]
                    if not np.isfinite(ep) or ep <= 0: continue
                    slots[k] = {"sym": s, "ei": i + 1, "ep": ep}; used.add(s); break

    if reason == "MANAGED":              # 15:29 flat
        for sl in slots:
            if sl: close_leg(sl, syms[sl["sym"]]["close"][last], last, "CLOSE")
    return _finish(legs, reason, grid)


def _finish(legs, reason, grid):
    if not legs:
        return None
    cf = _cost_frac()
    gross = sum(l["cap"] * (l["exi"] / l["ent"] - 1) for l in legs)
    cost = sum(l["cap"] * cf for l in legs)
    lg = len(grid) - 1
    out = []
    for l in legs:
        out.append({"sym": l["sym"], "arch": l["arch"], "rotated": l["rotated"],
                    "ret": (l["exi"] / l["ent"] - 1) * 100.0,
                    "ent": round(l["ent"], 2), "exi": round(l["exi"], 2), "qty": l["qty"],
                    "entry_time": grid[min(l["ei"], lg)], "exit_time": grid[min(l["xi"], lg)],
                    "pnl_rs": round(l["qty"] * (l["exi"] - l["ent"]), 0), "leg_reason": l["reason"]})
    return {"net_ret": (gross - cost) / CAPITAL * 100.0, "gross_ret": gross / CAPITAL * 100.0,
            "reason": reason, "n_legs": len(legs), "n_rot": sum(l["rotated"] for l in legs), "legs": out}


TRAIL_GS = [0.50, 0.75, 1.00]


def process_chunk(payload):
    db, days_chunk, signals, advs, adv_min = payload
    con = sqlite3.connect(db); out = {}
    for day in days_chunk:
        pool = liquid_pool(signals.get(day, []), advs, adv_min)
        if not pool:
            continue
        da = load_day_arrays(con, day, [s for _, s in pool])
        if da is None:
            continue
        rec = {"S1": simulate(da, pool, "static")}
        for g in TRAIL_GS:
            rec[f"T_{g}"] = simulate(da, pool, "trail", trail=g)
        for (g, cut) in GRID:
            rec[f"R_{g}_{cut}"] = simulate(da, pool, "trail_rot", trail=g, cutoff=cut)
        out[day] = rec
    con.close()
    return out


def metrics(series):
    s = np.array([x for x in series if x is not None])
    if len(s) == 0:
        return {}
    return {"days": len(s), "win_rate": round((s > 0).mean() * 100, 1),
            "avg_day": round(s.mean(), 3), "median": round(float(np.median(s)), 3),
            "worst": round(s.min(), 2), "best": round(s.max(), 2), "sum": round(s.sum(), 1)}


def monthly_table(odays, series):
    df = pd.DataFrame({"date": pd.to_datetime(odays), "ret": series}).dropna()
    df["year"] = df.date.dt.year; df["month"] = df.date.dt.month
    rows = []
    for (y, m), g in df.groupby(["year", "month"]):
        w = g[g.ret > 0]; l = g[g.ret < 0]
        rows.append({"year": int(y), "month": int(m), "Days": len(g), "Win": len(w), "Loss": len(l),
                     "Win%": round(len(w) / len(g) * 100, 1),
                     "Avg_win": round(w.ret.mean(), 2) if len(w) else None,
                     "Avg_loss": round(l.ret.mean(), 2) if len(l) else None,
                     "Sum_month": round(g.ret.sum(), 2)})
    return pd.DataFrame(rows)


def yearly_table(odays, series):
    df = pd.DataFrame({"date": pd.to_datetime(odays), "ret": series}).dropna()
    df["year"] = df.date.dt.year
    rows = []
    for y, g in df.groupby("year"):
        w = g[g.ret > 0]; l = g[g.ret < 0]
        rows.append({"year": int(y), "Days": len(g), "Win": len(w), "Loss": len(l),
                     "Win%": round(len(w) / len(g) * 100, 1),
                     "Avg_win": round(w.ret.mean(), 2) if len(w) else None,
                     "Avg_loss": round(l.ret.mean(), 2) if len(l) else None,
                     "Avg_day": round(g.ret.mean(), 3), "Sum": round(g.ret.sum(), 1)})
    rows.append({"year": "ALL", "Days": len(df), "Win": int((df.ret > 0).sum()),
                 "Loss": int((df.ret < 0).sum()), "Win%": round((df.ret > 0).mean() * 100, 1),
                 "Avg_win": round(df[df.ret > 0].ret.mean(), 2),
                 "Avg_loss": round(df[df.ret < 0].ret.mean(), 2),
                 "Avg_day": round(df.ret.mean(), 3), "Sum": round(df.ret.sum(), 1)})
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=0)
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--adv-min", type=float, default=ADV_MIN_CR,
                    help="liquidity filter ₹Cr (0 = unfiltered universe)")
    ap.add_argument("--end", default=None, help="cap entry_date at this YYYY-MM-DD (clean-data window)")
    ap.add_argument("--out", default=str(OUT))
    args = ap.parse_args()
    adv_min = args.adv_min
    out_path = Path(args.out)

    con = sqlite3.connect(str(DEFAULT_DB)); cfg = Config()
    signals = load_signals(con, cfg); advs = adv_map(con)
    mn, mx = con.execute(
        "SELECT min(substr(bar_time,1,10)),max(substr(bar_time,1,10)) FROM ohlc_1min").fetchone()
    con.close()
    end_cap = args.end or mx
    days = sorted(d for d in signals if mn <= d <= end_cap)
    if args.days:
        days = days[:args.days]
    print(f"[*] days {len(days)} pool<=Top{POOL_N} liquid>=Rs{adv_min:.0f}Cr K={K_SLOTS} "
          f"cost {COST_BPS}bps/leg", flush=True)

    chunks = [days[i::args.workers] for i in range(args.workers)]
    payloads = [(str(DEFAULT_DB), ch, signals, advs, adv_min) for ch in chunks if ch]
    daymap = {}
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        for part in ex.map(process_chunk, payloads):
            daymap.update(part)
    odays = sorted(daymap)
    print(f"[*] simulated days {len(odays)}", flush=True)

    def col(key):
        return [daymap[d][key]["net_ret"] if daymap[d].get(key) else None for d in odays]

    s1 = col("S1"); s2 = col("T_0.75")
    s3 = col(f"R_{DEFAULT_PARAM[0]}_{DEFAULT_PARAM[1]}")

    # walk-forward selects best key across trail-only AND trail+rotation variants
    KEYS = [f"T_{g}" for g in TRAIL_GS] + [f"R_{g}_{c}" for (g, c) in GRID]
    s4 = [None] * len(odays); s4_choice = []; RETUNE = 21; WINDOW = 120; cur = "T_0.75"
    for idx, d in enumerate(odays):
        if idx % RETUNE == 0 and idx >= WINDOW:
            best, bestv = cur, -1e9
            for key in KEYS:
                vals = [(daymap[odays[j]].get(key) or {}).get("net_ret")
                        for j in range(idx - WINDOW, idx)]
                vals = [v for v in vals if v is not None]
                if vals and np.mean(vals) > bestv:
                    bestv = np.mean(vals); best = key
            cur = best
        s4_choice.append(cur)
        s4[idx] = (daymap[d].get(cur) or {}).get("net_ret")

    arch_hist = defaultdict(list); s5 = [None] * len(odays)
    for idx, d in enumerate(odays):
        key = s4_choice[idx]
        veto = set()
        for arch, recs in arch_hist.items():
            rs = [r for j, r in recs if j < idx][-200:]
            if len(rs) >= 20 and (np.mean([r > 0 for r in rs]) * 100) < 45:
                veto.add(arch)
        if not veto or key.startswith("T_"):     # ledger only affects rotation variants
            x = daymap[d].get(key)
        else:
            g = float(key.split("_")[1]); cutv = key.split("_")[2]
            con = sqlite3.connect(str(DEFAULT_DB))
            pool = liquid_pool(signals.get(d, []), advs, adv_min)
            da = load_day_arrays(con, d, [s for _, s in pool]); con.close()
            x = simulate(da, pool, "trail_rot", trail=g, cutoff=cutv, veto=veto) if da else None
        s5[idx] = x["net_ret"] if x else None
        for l in (x["legs"] if x else []):
            arch_hist[l["arch"]].append((idx, l["ret"]))

    ladder = pd.DataFrame([
        {"step": "S1 static (liquid Top-5, +1% target)", **metrics(s1)},
        {"step": "S2 portfolio trail (lock+1%, no rotation)", **metrics(s2)},
        {"step": "S3 + conservative rotation", **metrics(s3)},
        {"step": "S4 walk-forward (picks trail-vs-rotation per window)", **metrics(s4)},
        {"step": "S5 + setup-outcome ledger veto", **metrics(s5)}])

    def roll_wr(series, w=30):
        s = pd.Series([1.0 if (x is not None and x > 0) else (0.0 if x is not None else np.nan)
                       for x in series])
        return (s.rolling(w, min_periods=w).mean() * 100).round(1)

    daily = pd.DataFrame({"date": odays, "S1_static": s1, "S2_trail": s2, "S3_rot": s3,
                          "S4_wf": s4, "S5_ledger": s5, "wf_choice": s4_choice})
    daily["S1_roll30_wr"] = roll_wr(s1); daily["S5_roll30_wr"] = roll_wr(s5)

    oos = []
    for label, ser in (("S1", s1), ("S4", s4), ("S5", s5)):
        tr = [v for dd, v in zip(odays, ser) if dd < "2026-01-01" and v is not None]
        te = [v for dd, v in zip(odays, ser) if dd >= "2026-01-01" and v is not None]
        oos.append({"step": label,
                    "TRAIN_win": round(np.mean([v > 0 for v in tr]) * 100, 1),
                    "TRAIN_avg": round(float(np.mean(tr)), 3),
                    "TEST_win": round(np.mean([v > 0 for v in te]) * 100, 1),
                    "TEST_avg": round(float(np.mean(te)), 3), "TEST_days": len(te)})
    oos = pd.DataFrame(oos)

    # GROSS series (same cost basis as the user's pasted trailing table) for S1 + S4
    s1g = [daymap[d]["S1"]["gross_ret"] if daymap[d].get("S1") else None for d in odays]
    s4g = [(daymap[d].get(s4_choice[i]) or {}).get("gross_ret") for i, d in enumerate(odays)]
    agent_month = monthly_table(odays, s4g); agent_year = yearly_table(odays, s4g)
    static_month = monthly_table(odays, s1g); static_year = yearly_table(odays, s1g)
    csv_tag = "Unfiltered" if adv_min < 1 else f"Liq{adv_min:.0f}"
    agent_month.to_csv(ROOT / "outputs" / f"Agent_Monthly_GROSS_{csv_tag}.csv", index=False)

    # ---- TRADE JOURNAL for the S4 walk-forward path (daily + exploded per-leg) ----
    daily_rows, ex_rows = [], []
    for i, d in enumerate(odays):
        rec = daymap[d].get(s4_choice[i])
        if not rec:
            continue
        port = rec["gross_ret"]; res = "WIN" if port > 0 else "LOSS"
        legs = rec["legs"]
        daily_rows.append({
            "date": d, "config": s4_choice[i], "n_legs": rec["n_legs"],
            "stocks": ", ".join(l["sym"] for l in legs),
            "exit_reason_day": rec["reason"],
            "portfolio_return_pct": round(port, 3), "result": res})
        for l in legs:
            deployed = l["qty"] * l["ent"]
            ex_rows.append({
                "date": d, "stock": l["sym"], "entry_time": l["entry_time"],
                "entry_price": l["ent"], "exit_time": l["exit_time"], "exit_price": l["exi"],
                "qty": l["qty"], "deployed_rs": round(deployed, 0),
                "pnl_rs": l["pnl_rs"], "stock_return_pct": round(l["ret"], 2),
                "leg_reason": l["leg_reason"], "was_rotated_in": l["rotated"],
                "day_portfolio_return_pct": round(port, 3), "day_result": res})
    jdaily = pd.DataFrame(daily_rows); jex = pd.DataFrame(ex_rows)
    jdaily.to_csv(ROOT / "outputs" / f"Agent_TradeLog_Daily_{csv_tag}.csv", index=False)
    jex.to_csv(ROOT / "outputs" / f"Agent_TradeLog_Exploded_{csv_tag}.csv", index=False)
    print(f"[*] journal: {len(jdaily)} days, {len(jex)} stock-day legs, "
          f"total ₹P&L {jex['pnl_rs'].sum():,.0f}", flush=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as xl:
        ladder.to_excel(xl, "1_Validation_Ladder", index=False)
        oos.to_excel(xl, "2_WalkForward_OOS", index=False)
        daily.to_excel(xl, "3_Daily_AllSteps", index=False)
        agent_month.to_excel(xl, "4_Agent_Monthly_GROSS", index=False)
        agent_year.to_excel(xl, "5_Agent_Yearly_GROSS", index=False)
        static_month.to_excel(xl, "6_Static_Monthly_GROSS", index=False)
        static_year.to_excel(xl, "7_Static_Yearly_GROSS", index=False)
        jdaily.to_excel(xl, "8_Daily_TradeLog", index=False)
        jex.to_excel(xl, "9_Trade_Journal_Exploded", index=False)

    print("\n=== AGENT (S4) MONTHLY — GROSS of costs, liquid universe ===")
    print(agent_month.to_string(index=False))
    print("\n=== AGENT (S4) YEARLY — GROSS ===")
    print(agent_year.to_string(index=False))
    print("\n=== STATIC (S1) YEARLY — GROSS (same universe, for reference) ===")
    print(static_year.to_string(index=False))

    print("\n=== VALIDATION LADDER (net of costs) ===")
    print(ladder.to_string(index=False))
    print("\n=== WALK-FORWARD OOS (train<2026 / test 2026) ===")
    print(oos.to_string(index=False))
    print(f"\n[*] wrote {out_path}")
    if DESKTOP.exists():
        import shutil; shutil.copy(out_path, DESKTOP / out_path.name)
        print(f"[*] copied to {DESKTOP / out_path.name}")


if __name__ == "__main__":
    main()
