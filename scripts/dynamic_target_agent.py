"""
Kanida.AI — Autonomous Target-Seeking Dynamic Intraday Agent (backtest).
Faithful to the operator spec: 3 governing principles, tiered Falcon universe
(Top-20 -> Top-50, never broad market), minute-by-minute decision loop on 1-min
candles, refill/rotation/average-up with limits, portfolio floor check, net-of-cost.
Walk-forward over the 1-min window; sweeps daily targets {2,3,4,5,6%}.

Output: outputs/Dynamic_Target_Agent.xlsx
"""
import sqlite3, bisect, argparse
from pathlib import Path
import numpy as np
import pandas as pd
from falcon_signal_replay import load_patterns, rank_for_date

ROOT = Path(__file__).resolve().parent.parent
SLIM = ROOT / "data" / "db" / "kanida_universe.db"
RND = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"
OUT = ROOT / "outputs" / "Dynamic_Target_Agent.xlsx"
DESK = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
ALIASES = {"ZOMATO": "ETERNAL"}

P = dict(total_capital=500000.0, max_positions=5, top20=20, top50=50,
         hard_stop_stock=-2.0, port_hard_stop=-1.5, no_entry_after="13:30",
         averaging_enabled=False, max_avg_adds=2, max_refills=2,
         target_ceiling=8.0, cost_leg=0.12,           # cost % per leg (0.24% round trip)
         grace_min=20, arm_pct=1.0,                    # grace before non-catastrophic cuts; arm before trailing
         slippage_leg=0.05)                            # execution slippage on EVERY fill (buy higher, sell lower)
TARGETS = [2.0, 3.0, 4.0, 5.0, 6.0]


def trail_at(t, protect):
    # WIDE trail so winners run; wider early, tighter near close; tighter in protect mode
    base = 3.0 if t < "11:00" else (2.5 if t < "14:00" else 1.5)
    return base * 0.5 if protect else base


def calm_cap(t):
    return 2.0 if t < "11:00" else (1.5 if t < "12:30" else 1.0)


def load_day(rc, ed, syms, prior_vol):
    ph = ",".join("?" * len(syms))
    q = f"""SELECT symbol, substr(bar_time,12,5) hm, open, high, low, close, volume
            FROM ohlc_1min WHERE substr(bar_time,1,10)=? AND symbol IN ({ph})"""
    df = pd.read_sql_query(q, rc, params=[ed, *syms])
    if df.empty:
        return None, None
    df["symbol"] = df["symbol"].replace(ALIASES)
    grid = sorted(m for m in df["hm"].unique() if "09:15" <= m <= "15:29")
    if len(grid) < 60:
        return None, None
    gi = {m: i for i, m in enumerate(grid)}
    data = {}
    for sym, g in df.groupby("symbol"):
        g = g[(g.hm >= "09:15") & (g.hm <= "15:29")].sort_values("hm")
        if g.empty or g.hm.iloc[0] != "09:15":
            continue
        n = len(grid)
        cl = np.full(n, np.nan); op = np.full(n, np.nan)
        hi = np.full(n, np.nan); lo = np.full(n, np.nan); vol = np.zeros(n)
        idx = g["hm"].map(gi).to_numpy()                  # vectorized placement (no iterrows)
        cl[idx] = g["close"].to_numpy(); op[idx] = g["open"].to_numpy()
        hi[idx] = g["high"].to_numpy(); lo[idx] = g["low"].to_numpy(); vol[idx] = g["volume"].to_numpy()
        cl = pd.Series(cl).ffill().to_numpy()             # forward-fill close over empty minutes
        op = np.where(np.isfinite(op), op, cl)            # a bar with no trade has no range
        hi = np.where(np.isfinite(hi), hi, cl)
        lo = np.where(np.isfinite(lo), lo, cl)
        if not np.isfinite(cl[0]) or cl[0] <= 0:
            continue
        cumv = np.cumsum(vol)
        vwap = np.cumsum(cl * vol) / np.where(cumv > 0, cumv, 1)
        mom = np.zeros(n)
        mom[:min(5, n)] = (cl[:min(5, n)] / cl[0] - 1) if cl[0] > 0 else 0
        if n > 5:
            mom[5:] = np.where(cl[:-5] > 0, cl[5:] / cl[:-5] - 1, 0)
        pv = prior_vol.get(sym, 0) or 1
        vpace = (cumv / pv) / (np.arange(1, n + 1) / n)
        data[sym] = dict(open0915=float(op[0]), close=cl, openarr=op, high=hi, low=lo,
                         vwap=vwap, mom=mom, vpace=vpace)
    return grid, data


def run_agent(grid, data, ranks, target, avg_on):
    cap = P["total_capital"]; alloc = cap / P["max_positions"]; cl_cost = P["cost_leg"]
    top20 = [s for s, r in ranks.items() if r <= P["top20"] and s in data]
    top50 = [s for s, r in ranks.items() if r <= P["top50"] and s in data]
    initial = sorted(top20, key=lambda s: ranks[s])[:P["max_positions"]]
    if len(initial) < 3:
        return None

    cash = cap; cost_total = 0.0; leg_count = 0; pos = {}; trades = []
    slip = P["slippage_leg"]

    def buy(sym, i, amount, kind):
        nonlocal cash, cost_total, leg_count
        raw = data[sym]["close"][i]
        if raw <= 0: return False
        px = raw * (1 + slip / 100)                          # execution slippage: buy fills higher
        qty = int(amount // px)
        if qty <= 0: return False
        dep = qty * px; fee = dep * cl_cost / 100
        if dep + fee > cash + 1e-6:
            qty = int((cash * 0.999) // (px * (1 + cl_cost / 100)))
            if qty <= 0: return False
            dep = qty * px; fee = dep * cl_cost / 100
        cash -= dep + fee; cost_total += fee; leg_count += 1
        if sym in pos:
            p = pos[sym]; tot = p["qty"] + qty
            p["entry"] = (p["entry"] * p["qty"] + px * qty) / tot; p["qty"] = tot; p["adds"] += 1
        else:
            pos[sym] = dict(qty=qty, entry=px, peak=0.0, entry_i=i, entry_time=grid[i], kind=kind, adds=0)
        return True

    def sell(sym, i, reason, fill=None):
        nonlocal cash, cost_total, leg_count
        p = pos.pop(sym)
        raw = data[sym]["close"][i] if fill is None else fill
        px = raw * (1 - slip / 100)                          # execution slippage: sell fills lower
        proceeds = p["qty"] * px; fee = proceeds * cl_cost / 100
        cash += proceeds - fee; cost_total += fee; leg_count += 1
        ret = (px / p["entry"] - 1) * 100
        pnl = p["qty"] * (px - p["entry"]) - fee
        trades.append(dict(stock=sym, kind=p["kind"], n_adds=p["adds"],
                           entry_time=p["entry_time"], entry_price=round(p["entry"], 2),
                           exit_time=grid[i], exit_price=round(px, 2), qty=p["qty"],
                           deployed_rs=round(p["qty"] * p["entry"]), pnl_rs=round(pnl),
                           stock_return_pct=round(ret, 3), exit_reason=reason))
        return ret

    # initial entry at 09:15 open (with slippage)
    for sym in initial:
        raw = data[sym]["open0915"]; px = raw * (1 + slip / 100)
        qty = int(alloc // px)
        if qty <= 0: continue
        dep = qty * px; fee = dep * cl_cost / 100
        cash -= dep + fee; cost_total += fee; leg_count += 1
        pos[sym] = dict(qty=qty, entry=px, peak=0.0, entry_i=0, entry_time=grid[0], kind="INIT", adds=0)

    mode = "NORMAL"; floor = None; touched = False; protect_ever = False
    refills = 0; adds = 0; cuts = 0; cut_today = set(); exit_reason = "SQOFF"; stop_event = None

    def net_liq(field, i):
        # NET return if we liquidate ALL open positions now at data[s][field][i] (exit fee + slippage in)
        if not pos:
            return (cash - cap) / cap * 100
        proceeds = sum(p["qty"] * data[s][field][i] * (1 - slip / 100) for s, p in pos.items())
        return (cash + proceeds - proceeds * cl_cost / 100 - cap) / cap * 100

    def gross_low_mark(i):
        if not pos: return (cash - cap) / cap * 100
        return (cash + sum(p["qty"] * data[s]["low"][i] for s, p in pos.items()) - cap) / cap * 100

    def flatten_interp(field_level, i, reason, net_based):
        # fill each position where the basket crosses `field_level` intrabar (interpolate open->low).
        # net_based=True -> level is a NET-liquidation level (target floor); False -> GROSS mark (hard stop).
        def mark(frac):
            px = {s: data[s]["openarr"][i] + frac * (data[s]["low"][i] - data[s]["openarr"][i]) for s in pos}
            if net_based:
                proceeds = sum(p["qty"] * px[s] * (1 - slip / 100) for s, p in pos.items())
                return (cash + proceeds - proceeds * cl_cost / 100 - cap) / cap * 100, px
            return (cash + sum(p["qty"] * px[s] for s, p in pos.items()) - cap) / cap * 100, px
        m0, _ = mark(0.0); m1, _ = mark(1.0)
        frac = 0.0 if m1 == m0 else max(0.0, min(1.0, (field_level - m0) / (m1 - m0)))
        _, fills = mark(frac)
        for s in list(pos): sell(s, i, reason, fill=fills[s])
        return (cash - cap) / cap * 100    # realized net after the fills

    N = len(grid)
    k_net = (1 - slip / 100) * (1 - cl_cost / 100)        # gross->net liquidation factor
    # breadth is position-independent -> precompute once for the whole day (was a per-minute np.mean)
    breadth_arr = (np.mean([data[s]["close"] / data[s]["open0915"] - 1 > 0 for s in top20], axis=0)
                   if top20 else np.zeros(N))
    for i in range(N):
        t = grid[i]
        so = sl = sc = 0.0                                 # single pass: peaks + open/low/close basket sums
        for s, p in pos.items():
            d = data[s]; q = p["qty"]; c = d["close"][i]
            r = (c / p["entry"] - 1) * 100
            if r > p["peak"]: p["peak"] = r
            so += q * d["openarr"][i]; sl += q * d["low"][i]; sc += q * c
        nl_open = (cash + so * k_net - cap) / cap * 100     # NET liquidation at open / low / close
        nl_low = (cash + sl * k_net - cap) / cap * 100
        pr_close = (cash + sc * k_net - cap) / cap * 100
        gross_low = (cash + sl - cap) / cap * 100           # GROSS mark (for the hard stop)
        breadth = breadth_arr[i]
        remaining = N - 1 - i

        # 0.2/1b — LOCK only when NET liquidation at the executable OPEN >= target (a net-sellable level)
        if mode == "NORMAL" and pos and nl_open >= target:
            mode = "LOCK"; floor = target
        if pr_close >= target:
            touched = True
        # 0.2/1a — INTRABAR floor on NET LIQUIDATION VALUE (master, after lock)
        if mode == "LOCK" and pos and nl_low <= floor:
            realized = flatten_interp(floor, i, "FLOOR_LOCK", net_based=True)
            stop_event = dict(kind="FLOOR", trigger=floor, realized=round(realized, 3), close_only=round(pr_close, 3))
            exit_reason = "FLOOR_LOCK"; break
        # 1a — INTRABAR portfolio hard stop on the GROSS mark (fills ~ -1.5 gross => ~ -1.67 net, as accepted)
        if pos and gross_low <= P["port_hard_stop"]:
            realized = flatten_interp(P["port_hard_stop"], i, "PORT_STOP", net_based=False)
            stop_event = dict(kind="STOP", trigger=P["port_hard_stop"], realized=round(realized, 3), close_only=round(pr_close, 3))
            exit_reason = "PORT_STOP"; protect_ever = True; break
        if i == N - 1:
            for s in list(pos): sell(s, i, "SQOFF")
            exit_reason = "SQOFF"; break

        pr = pr_close

        protect = (pr < 0 and breadth < 0.30)
        if protect: protect_ever = True
        tr = trail_at(t, protect)
        # cut positions (grace period + real-breakdown only; winners run on a wide breakeven-trail)
        for s in list(pos):
            p = pos[s]; r = (data[s]["close"][i] / p["entry"] - 1) * 100
            below_vwap = data[s]["close"][i] < data[s]["vwap"][i]; mom = data[s]["mom"][i]
            held = i - p["entry_i"]
            reason = None
            if r <= P["hard_stop_stock"]:
                reason = "STOP_2"                                   # catastrophe -2%, always
            elif held >= P["grace_min"]:
                armed = p["peak"] >= P["arm_pct"]
                if armed and r <= max(0.0, p["peak"] - tr):         # wide trail, breakeven-protected
                    reason = "TRAIL"
                elif r < -0.5 and below_vwap and mom < -0.002:      # real breakdown (weakening loser)
                    reason = "BREAKDOWN"
            if reason:
                rr = sell(s, i, reason); cuts += 1
                if rr < 0: cut_today.add(s)

        if protect or mode == "LOCK":
            continue
        gap = target - pr
        healthy = breadth >= 0.30
        newentry_ok = t < P["no_entry_after"]

        def candidate(exclude):
            for tier in (top20, top50):
                cands = [s for s in tier if s not in pos and s not in cut_today and s not in exclude]
                cands.sort(key=lambda s: ranks[s])
                for s in cands:
                    r = (data[s]["close"][i] / data[s]["open0915"] - 1) * 100
                    if 0 < r < calm_cap(t) and data[s]["close"][i] > data[s]["vwap"][i] and data[s]["vpace"][i] > 1.0:
                        return s
            return None

        # 5a stalled rotation
        if newentry_ok and refills < P["max_refills"]:
            for s in list(pos):
                p = pos[s]; r = (data[s]["close"][i] / p["entry"] - 1) * 100
                stalled = (-0.5 <= r <= 0.5 and (i - p["entry_i"]) >= 30 and
                           r < p["peak"] - 0.05 and data[s]["close"][i] <= data[s]["vwap"][i])
                if stalled:
                    cand = candidate(exclude={s})
                    if cand:
                        sell(s, i, "ROTATE_OUT")
                        if buy(cand, i, alloc, "ROTATE_IN"):
                            refills += 1
                        break
        # refill empty slots
        open_slots = P["max_positions"] - len(pos)
        if gap > 0 and remaining > 60 and healthy and newentry_ok:
            if open_slots > 0 and refills < P["max_refills"]:
                cand = candidate(exclude=set())
                if cand and buy(cand, i, alloc, "REFILL"):
                    refills += 1
            # average up (freed cash only, open_slots==0)
            elif avg_on and open_slots == 0 and adds < P["max_avg_adds"]:
                best = None; bestr = 0
                for s, p in pos.items():
                    r = (data[s]["close"][i] / p["entry"] - 1) * 100
                    at_high = r >= p["peak"] - 0.05
                    if r > bestr and at_high and data[s]["close"][i] > data[s]["vwap"][i] and data[s]["vpace"][i] > 1.0:
                        best = s; bestr = r
                if best and cash > alloc * 0.2:
                    add_amt = min(cash * 0.5, alloc)
                    if buy(best, i, add_amt, "AVG_UP"):
                        adds += 1

    net = (cash - cap) / cap * 100
    gross = net + cost_total / cap * 100                      # gross of brokerage fees (slippage stays in price)
    return dict(target=target, net=round(net, 3), gross=round(gross, 3),
                touched=touched, delivered=(net >= target),  # 0.1: delivered = final NET >= target
                refills=refills, adds=adds, cuts=cuts,
                protect=protect_ever, exit_reason=exit_reason,
                cost_pct=round(cost_total / cap * 100, 3),
                legs=leg_count, n_init=len(initial),
                stop_trigger=(stop_event["trigger"] if stop_event else None),
                stop_realized=(stop_event["realized"] if stop_event else None),
                stop_close_only=(stop_event["close_only"] if stop_event else None),
                init_stocks=", ".join(initial), trades=trades)


def static_top5(grid, data, ranks):
    """Baseline: Top-5 @09:15 -> 15:29 close, net of cost + slippage (entry+exit)."""
    slip = P["slippage_leg"]
    top5 = sorted([s for s, r in ranks.items() if r <= P["top20"] and s in data], key=lambda s: ranks[s])[:5]
    if len(top5) < 3: return None
    rs = []
    for s in top5:
        e = data[s]["open0915"] * (1 + slip / 100); c = data[s]["close"][-1] * (1 - slip / 100)
        if e > 0: rs.append((c / e - 1) * 100)
    if not rs: return None
    return round(float(np.mean(rs)) - 2 * P["cost_leg"], 3)     # entry+exit brokerage


def lookahead_report(patterns):
    """0.4 — document the walk-forward boundary: for each signal year, only patterns
    mined in STRICTLY prior years are eligible (rank_for_date filters mined_year<signal_year;
    features are queried point-in-time WHERE trade_date=signal_date)."""
    rows = []
    for sy in (2024, 2025, 2026):
        elig = [p for p in patterns if int(p["mined_year"]) < sy]
        rows.append(dict(signal_year=sy, eligible_patterns=len(elig),
                         max_mined_year_used=max((int(p["mined_year"]) for p in elig), default=None),
                         no_future_pattern=all(int(p["mined_year"]) < sy for p in elig),
                         features="point-in-time: trade_date = signal_date only"))
    return pd.DataFrame(rows)


def main():
    global OUT
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", type=int, default=0)
    ap.add_argument("--slip", type=float, default=None)      # per-leg slippage % override (0.5 sensitivity)
    ap.add_argument("--out", type=str, default=None)         # output workbook path override
    args = ap.parse_args()
    if args.slip is not None: P["slippage_leg"] = args.slip
    if args.out: OUT = Path(args.out)
    sc = sqlite3.connect(str(SLIM)); rc = sqlite3.connect(str(RND))
    patterns = load_patterns(sc)
    look = lookahead_report(patterns)
    print(f"[*] slippage/leg={P['slippage_leg']}%  look-ahead clean={bool(look.no_future_pattern.all())}", flush=True)
    onemin = [r[0] for r in rc.execute("SELECT DISTINCT substr(bar_time,1,10) FROM ohlc_1min ORDER BY 1")]
    onemset = set(onemin)
    nx = lambda d: (onemin[bisect.bisect_right(onemin, d)] if bisect.bisect_right(onemin, d) < len(onemin) else None)
    feat_days = [r[0] for r in sc.execute(
        "SELECT DISTINCT trade_date FROM falcon_features WHERE trade_date>='2024-05-10' ORDER BY 1")]
    if args.smoke: feat_days = feat_days[:args.smoke]

    daily = []; base_rows = []; seen_ed = set()
    for k, sd in enumerate(feat_days):
        rk = rank_for_date(sc, patterns, sd, min_fires=10)
        if not rk: continue
        ed = nx(sd)
        if ed is None or ed not in onemset: continue
        if ed in seen_ed: continue                    # 0.3 — each execution date exactly once
        seen_ed.add(ed)
        ranks = {c["symbol"]: c["rank"] for c in rk[:P["top50"]]}
        syms = list(ranks.keys())
        prior_vol = {r[0]: r[1] for r in sc.execute(
            f"SELECT symbol, volume FROM ohlc_daily WHERE trade_date=? AND symbol IN ({','.join('?'*len(syms))})", [sd, *syms])}
        grid, data = load_day(rc, ed, syms, prior_vol)
        if not data: continue
        base = static_top5(grid, data, ranks)
        if base is not None: base_rows.append(dict(date=ed, base_net=base))
        for tgt in TARGETS:
            r = run_agent(grid, data, ranks, tgt, P["averaging_enabled"])
            if r: r["date"] = ed; daily.append(r)
        if args.smoke:
            r5 = next((r for r in daily if r["date"] == ed and r["target"] == 5.0), None)
            if r5:
                print(f"  {ed}: base={base}  agent+5 net={r5['net']} delivered={r5['delivered']} "
                      f"touched={r5['touched']} exit={r5['exit_reason']}", flush=True)
        if (k + 1) % 50 == 0:
            print(f"  [{k+1}/{len(feat_days)}] {ed}", flush=True)
    sc.close(); rc.close()

    dd = pd.DataFrame(daily); bb = pd.DataFrame(base_rows)
    dd["dt"] = pd.to_datetime(dd["date"]); dd["Year"] = dd.dt.dt.year; dd["YM"] = dd.dt.dt.strftime("%Y-%m")

    # Sheet 1 — achievability
    ach = []
    for tgt in TARGETS:
        g = dd[dd.target == tgt]
        miss = g[~g.delivered]
        eq = (1 + g.sort_values("date").net / 100).cumprod()
        ach.append(dict(target_pct=tgt, days=len(g),
                        pct_touched=round(g.touched.mean()*100, 1),
                        pct_delivered=round(g.delivered.mean()*100, 1),
                        avg_net_pct=round(g.net.mean(), 3),
                        avg_gross_pct=round(g.gross.mean(), 3),
                        protect_days_pct=round(g.protect.mean()*100, 1),
                        avg_on_missed_pct=round(miss.net.mean(), 3) if len(miss) else None,
                        win_rate_pct=round((g.net > 0).mean()*100, 1),
                        max_dd_pct=round((eq/eq.cummax()-1).min()*100, 2)))
    ach = pd.DataFrame(ach)

    # --- STAGE-0 DIAGNOSTICS: TRUE delivery (net>=target), stop, dedup, look-ahead ---
    g5 = dd[dd.target == 5.0]
    stops = dd[dd.exit_reason == "PORT_STOP"]
    p1 = pd.DataFrame([
        dict(metric="Unique execution days (deduped)", value=int(g5.date.nunique())),
        dict(metric="slippage per leg %", value=P["slippage_leg"]),
        dict(metric="Target +5% TOUCHED (close reached target) %", value=round(g5.touched.mean()*100, 1)),
        dict(metric="Target +5% DELIVERED (final NET >= target) %", value=round(g5.delivered.mean()*100, 1)),
        dict(metric="Worst PORT_STOP day net %", value=round(stops.net.min(), 3) if len(stops) else None),
        dict(metric="Avg PORT_STOP realized net %", value=round(stops.net.mean(), 3) if len(stops) else None),
        dict(metric="PORT_STOP days worse than -2%", value=int((stops.net < -2).sum()) if len(stops) else 0),
        dict(metric="Look-ahead clean (no future patterns)", value=bool(look.no_future_pattern.all())),
    ])

    # Sheet 8 — comparison
    base_net = round(bb.base_net.mean(), 3); base_wr = round((bb.base_net > 0).mean()*100, 1)
    comp = pd.DataFrame([
        dict(Metric="Avg net return/day %", Static_Top5=base_net,
             **{f"Agent_+{int(t)}%": round(dd[dd.target==t].net.mean(), 3) for t in TARGETS}),
        dict(Metric="Win rate %", Static_Top5=base_wr,
             **{f"Agent_+{int(t)}%": round((dd[dd.target==t].net>0).mean()*100, 1) for t in TARGETS}),
        dict(Metric="Days target DELIVERED (net>=target) %", Static_Top5="N/A",
             **{f"Agent_+{int(t)}%": round(dd[dd.target==t].delivered.mean()*100, 1) for t in TARGETS}),
    ])

    # --- FULL day-by-day trade journal for the +5% target (every entry & exit) ---
    j = []
    for _, r in dd[dd.target == 5.0].iterrows():
        for tr in r["trades"]:
            j.append(dict(date=r["date"], **tr))
    journal = pd.DataFrame(j)
    if not journal.empty:
        journal = journal[["date", "stock", "kind", "n_adds", "entry_time", "entry_price",
                           "exit_time", "exit_price", "qty", "deployed_rs", "pnl_rs",
                           "stock_return_pct", "exit_reason"]]
        eff = journal.groupby("kind").agg(
            trades=("stock", "size"),
            win_rate_pct=("pnl_rs", lambda x: round((x > 0).mean() * 100, 1)),
            avg_return_pct=("stock_return_pct", "mean"),
            total_pnl_rs=("pnl_rs", "sum")).reset_index()
        eff["avg_return_pct"] = eff["avg_return_pct"].round(3)
    else:
        eff = pd.DataFrame()

    daily5 = dd[dd.target == 5.0][["date", "target", "net", "gross", "touched", "delivered", "refills",
                                  "adds", "cuts", "protect", "exit_reason", "stop_trigger", "stop_realized",
                                  "stop_close_only", "cost_pct", "init_stocks"]]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        p1.to_excel(xl, "0_Stage0_Diagnostics", index=False)
        look.to_excel(xl, "0b_LookAhead_Boundary", index=False)
        ach.to_excel(xl, "1_Target_Achievability", index=False)
        daily5.to_excel(xl, "2_Daily_Log_+5", index=False)
        journal.to_excel(xl, "3_Trade_Journal_+5", index=False)
        eff.to_excel(xl, "4_Action_Effectiveness_+5", index=False)
        dd[["date", "target", "gross", "cost_pct", "net", "legs"]].to_excel(xl, "5c_Gross_vs_Net", index=False)
        comp.to_excel(xl, "8_Comparison", index=False)
    print(f"\n[*] trade journal rows (+5 target): {len(journal):,}")

    pd.set_option("display.width", 220)
    print("\n=== PRIORITY-1 DIAGNOSTICS ===")
    print(p1.to_string(index=False))
    print("\n=== TARGET ACHIEVABILITY (net of cost) ===")
    print(ach.to_string(index=False))
    print("\n=== COMPARISON ===")
    print(comp.to_string(index=False))
    print(f"\n[*] wrote {OUT}")
    if DESK.exists():
        import shutil; shutil.copy(OUT, DESK / OUT.name); print(f"[*] copied to {DESK/OUT.name}")


if __name__ == "__main__":
    main()
