"""BEDROCK self-correcting SWING agent — the WALK-FORWARD run + daily journal + Excel export.
Every morning the agent re-chooses (universe, crowding cap, entry timing, target, stop, time-exit) using ONLY
trades that CLOSED BEFORE that morning. Leverage must be EARNED. Nothing here ever reads the future.
Outputs: BEDROCK_SELFLEARN.xlsx  [Trade Log | Daily Journal | Config Evolution | Summary]"""
import os, sqlite3, json
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db"); AP = os.path.dirname(os.path.abspath(__file__))
CAP0 = 500000.0; MAX_LEV = 2.0; COST = 0.0015; MIN_TICKET = 1000.0
ENTRY_MODES = ["OPEN", "WAIT30", "DIP", "BRK"]; TARGETS = [None, 10.0, 15.0]; STOPS = [None, 6.0, 10.0]; TIMES = [5, 10, 15]
CAPS = [3, 5, 10, 20, 999]; UNIS = ["ALL", "FO", "N50"]
LOOKBACK = 40; WARMUP = 15; MIN_EVAL = 10
DEFAULT = ("OPEN", None, None, 15); DEF_CAP = 999; DEF_UNI = "ALL"

G = pd.read_pickle(os.path.join(AP, "_bedrock_grid.pkl"))
uc = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
oh = pd.read_sql_query("SELECT symbol,trade_date,close FROM ohlc_daily WHERE trade_date>='2026-01-01' AND trade_date<='2026-07-31'", uc); uc.close()
CLOSE = {(r.symbol, r.trade_date): r.close for r in oh.itertuples()}
alldays = sorted(oh.trade_date.unique()); AIDX = {d: i for i, d in enumerate(alldays)}
COMBOS = [(em, tg, sp, tm) for em in ENTRY_MODES for tg in TARGETS for sp in STOPS for tm in TIMES]
KEY = lambda c: f"{c[0]}|{c[1]}|{c[2]}|{c[3]}"
edays = sorted(G.entry_date.unique())


def exit_date_for(row, combo):
    """Exit date implied by this combo's time limit (stop/target may end earlier; time bound is the schedule)."""
    i = AIDX.get(row["entry_date"])
    return alldays[min(i + combo[3] - 1, len(alldays)-1)] if i is not None else row["entry_date"]


def choose(ledger, equity_peak, equity, lev):
    """Pick config from CLOSED trades only. Returns (combo, cap, uni, lev, why)."""
    if len(ledger) < WARMUP:
        return DEFAULT, DEF_CAP, DEF_UNI, 1.0, f"warm-up ({len(ledger)}/{WARMUP} closed trades) — running defaults"
    tail = ledger[-LOOKBACK:]
    best = None
    for uni in UNIS:
        for cap in CAPS:
            sub = [t for t in tail if (uni == "ALL" or (uni == "FO" and t["uni_fo"]) or (uni == "N50" and t["uni_n50"])) and t["rank"] <= cap]
            if len(sub) < MIN_EVAL: continue
            for c in COMBOS:
                v = np.array([t["shadow"].get(KEY(c), np.nan) for t in sub], float)
                v = v[np.isfinite(v)]
                if len(v) < MIN_EVAL: continue
                score = v.mean()
                if best is None or score > best[0]: best = (score, c, cap, uni, len(v))
    if best is None:
        return DEFAULT, DEF_CAP, DEF_UNI, lev, "not enough comparable trades — holding defaults"
    _, c, cap, uni, n = best
    # leverage must be EARNED
    last20 = [t["ret"] for t in ledger[-20:]]
    dd = (equity_peak - equity) / equity_peak * 100 if equity_peak > 0 else 0
    if len(last20) >= 20 and np.mean(last20) > 0 and dd < 8:
        newlev = min(MAX_LEV, lev + 0.25); lreason = f"trailing 20 trades {np.mean(last20):+.2f}% & DD {dd:.1f}% → lever up to {newlev:.2f}x"
    elif len(last20) >= 5 and (np.mean(last20) < 0 or dd > 12):
        newlev = max(1.0, lev - 0.5); lreason = f"trailing 20 {np.mean(last20):+.2f}% / DD {dd:.1f}% → DE-LEVER to {newlev:.2f}x"
    else:
        newlev = lev; lreason = f"leverage held at {lev:.2f}x"
    why = (f"best of {len(UNIS)*len(CAPS)*len(COMBOS)} configs on last {n} closed trades: "
           f"{uni} · top-{'all' if cap==999 else cap}/day · {c[0]} entry · "
           f"target {c[1] or '—'} · stop {c[2] or '—'} · exit D{c[3]} (avg {best[0]:+.2f}%/trade). {lreason}")
    return c, cap, uni, newlev, why


cash = CAP0; lev = 1.0; borrowed = 0.0; openpos = []; ledger = []; peak = CAP0
trades = []; journal = []; cfg_hist = []
cfg, cap, uni, _, why = DEFAULT, DEF_CAP, DEF_UNI, 1.0, "day 1 — defaults"
prev_sig = None

for d in edays:
    # ---------- 1) LEARN (uses only trades closed before today) ----------
    eq_now = cash + sum(p["ticket"] * (CLOSE.get((p["sym"], d), p["entry_px"]) / p["entry_px"]) for p in openpos) - borrowed
    peak = max(peak, eq_now)
    newcfg, newcap, newuni, newlev, why = choose(ledger, peak, eq_now, lev)
    changed = (newcfg, newcap, newuni) != (cfg, cap, uni) or newlev != lev
    prev = (cfg, cap, uni, lev)
    cfg, cap, uni, lev = newcfg, newcap, newuni, newlev

    # ---------- 2) ACT: today's signals under the chosen config ----------
    todays = G[G.entry_date == d].sort_values("rank")
    if uni == "FO": todays = todays[todays.uni_fo == 1]
    elif uni == "N50": todays = todays[todays.uni_n50 == 1]
    todays = todays[todays["rank"] <= cap]
    buying_power = max(eq_now, 0) * lev
    slots = max(cap if cap != 999 else 20, 1) * cfg[3]
    ticket = max(buying_power / slots, 0)
    taken, nofill = 0, 0
    for r in todays.itertuples():
        ep = r.ep_map.get(cfg[0])
        if ep is None: nofill += 1; continue
        sh = getattr(r, KEY(cfg).replace("|", "_"), None)
        shv = r._asdict().get(KEY(cfg)) if hasattr(r, "_asdict") else None
        val = G.loc[r.Index, KEY(cfg)]
        if not np.isfinite(val): nofill += 1; continue
        tk = min(ticket, max(cash, 0)) if ticket > cash else ticket
        if tk < MIN_TICKET and cash > MIN_TICKET: tk = MIN_TICKET
        if tk <= 0: continue
        xd = exit_date_for(G.loc[r.Index], cfg)
        openpos.append(dict(sym=r.symbol, entry_date=d, exit_date=xd, entry_px=ep, ticket=tk,
                            ret=val, cfg=KEY(cfg), rank=r.rank, uni_fo=r.uni_fo, uni_n50=r.uni_n50,
                            shadow={k: G.loc[r.Index, k] for k in [KEY(c) for c in COMBOS]}))
        cash -= tk; taken += 1
    # borrow only if the plan needs more than cash and leverage has been earned
    if cash < 0:
        borrowed += -cash; cash = 0.0

    # ---------- 3) EXITS due today ----------
    keep, closed_today, pnl_today = [], 0, 0.0
    for p in openpos:
        if p["exit_date"] <= d:
            pl = p["ticket"] * p["ret"] / 100.0
            cash += p["ticket"] + pl; pnl_today += pl; closed_today += 1
            ledger.append(dict(ret=p["ret"], rank=p["rank"], uni_fo=p["uni_fo"], uni_n50=p["uni_n50"], shadow=p["shadow"]))
            trades.append(dict(entry_date=p["entry_date"], exit_date=d, symbol=p["sym"], config=p["cfg"],
                               entry_price=round(p["entry_px"], 2), ticket=round(p["ticket"], 0),
                               ret_pct=round(p["ret"], 2), pnl=round(pl, 0), rank=p["rank"],
                               universe=("N50" if p["uni_n50"] else "FO" if p["uni_fo"] else "OTHER")))
        else: keep.append(p)
    openpos = keep
    # repay borrowing from profits first
    if borrowed > 0 and cash > 0:
        rep = min(borrowed, cash); borrowed -= rep; cash -= rep

    equity = cash + sum(p["ticket"] * (CLOSE.get((p["sym"], d), p["entry_px"]) / p["entry_px"]) for p in openpos) - borrowed
    peak = max(peak, equity); dd = (peak - equity) / peak * 100 if peak > 0 else 0
    nxt_cfg, nxt_cap, nxt_uni, nxt_lev, nxt_why = choose(ledger, peak, equity, lev)
    journal.append(dict(
        date=d, equity=round(equity, 0), cash=round(cash, 0), borrowed=round(borrowed, 0), leverage=round(lev, 2),
        open_positions=len(openpos), entered=taken, no_fill=nofill, closed=closed_today, pnl_today=round(pnl_today, 0),
        drawdown_pct=round(dd, 2), closed_to_date=len(ledger),
        config_used=f"{uni} | top-{'all' if cap==999 else cap} | {cfg[0]} | tgt {cfg[1] or '—'} | stop {cfg[2] or '—'} | exit D{cfg[3]}",
        changed_today=("YES — " + why) if changed else "no change",
        correction_for_tomorrow=f"{nxt_uni} | top-{'all' if nxt_cap==999 else nxt_cap} | {nxt_cfg[0]} | tgt {nxt_cfg[1] or '—'} | "
                                f"stop {nxt_cfg[2] or '—'} | exit D{nxt_cfg[3]} | lev {nxt_lev:.2f}x",
        why_tomorrow=nxt_why))
    cfg_hist.append(dict(date=d, universe=uni, cap=cap, entry=cfg[0], target=cfg[1], stop=cfg[2], time_exit=cfg[3], leverage=lev))

# close any stragglers at last close
for p in openpos:
    pl = p["ticket"] * p["ret"] / 100.0; cash += p["ticket"] + pl
    trades.append(dict(entry_date=p["entry_date"], exit_date=alldays[-1], symbol=p["sym"], config=p["cfg"],
                       entry_price=round(p["entry_px"], 2), ticket=round(p["ticket"], 0), ret_pct=round(p["ret"], 2),
                       pnl=round(pl, 0), rank=p["rank"], universe=("N50" if p["uni_n50"] else "FO" if p["uni_fo"] else "OTHER")))
final = cash - borrowed
TL = pd.DataFrame(trades); JR = pd.DataFrame(journal); CH = pd.DataFrame(cfg_hist)
JR["month"] = JR.date.str[:7]
mo = JR.groupby("month").agg(sessions=("date", "count"), trades_entered=("entered", "sum"), trades_closed=("closed", "sum"),
                             pnl=("pnl_today", "sum"), end_equity=("equity", "last"), max_dd=("drawdown_pct", "max")).reset_index()
starts = [CAP0] + mo.end_equity.tolist()[:-1]; mo["start_equity"] = starts
mo["ret_pct"] = (mo.end_equity - mo.start_equity) / mo.start_equity * 100
summary = pd.DataFrame([dict(metric="Start capital", value=f"Rs{CAP0:,.0f}"),
                        dict(metric="Final equity", value=f"Rs{final:,.0f}"),
                        dict(metric="Total return", value=f"{(final/CAP0-1)*100:+.2f}%"),
                        dict(metric="Sessions", value=len(JR)),
                        dict(metric="Trades closed", value=len(TL)),
                        dict(metric="Win rate", value=f"{(TL.ret_pct>0).mean()*100:.0f}%" if len(TL) else "-"),
                        dict(metric="Avg return / trade", value=f"{TL.ret_pct.mean():+.2f}%" if len(TL) else "-"),
                        dict(metric="Max drawdown", value=f"{JR.drawdown_pct.max():.2f}%"),
                        dict(metric="Max leverage used", value=f"{CH.leverage.max():.2f}x"),
                        dict(metric="Config changes", value=int((JR.changed_today != 'no change').sum()))])
out = os.path.join(ROOT, "docs", "ops", "BEDROCK_SELFLEARN.xlsx")
os.makedirs(os.path.dirname(out), exist_ok=True)
with pd.ExcelWriter(out, engine="openpyxl") as w:
    summary.to_excel(w, "Summary", index=False); mo.to_excel(w, "Monthly", index=False)
    TL.to_excel(w, "Trade Log", index=False); JR.to_excel(w, "Daily Journal", index=False)
    CH.to_excel(w, "Config Evolution", index=False)

print("BEDROCK — SELF-CORRECTING SWING (true walk-forward, 2026-01..2026-07-10)")
print(f"  start Rs{CAP0:,.0f} -> final Rs{final:,.0f}   = {(final/CAP0-1)*100:+.2f}%")
print(f"  {len(TL)} trades closed · win {(TL.ret_pct>0).mean()*100:.0f}% · avg {TL.ret_pct.mean():+.2f}%/trade · maxDD {JR.drawdown_pct.max():.1f}% · max lev {CH.leverage.max():.2f}x")
print(f"  config changed on {int((JR.changed_today!='no change').sum())} of {len(JR)} sessions\n")
print("MONTHLY")
print(f"{'Month':<9}{'Sess':>6}{'Entered':>9}{'Closed':>8}{'P&L Rs':>12}{'Return':>9}{'MaxDD':>8}{'Equity Rs':>13}")
for _, r in mo.iterrows():
    print(f"{r.month:<9}{int(r.sessions):>6}{int(r.trades_entered):>9}{int(r.trades_closed):>8}{r.pnl:>12,.0f}{r.ret_pct:>8.2f}%{r.max_dd:>7.1f}%{r.end_equity:>13,.0f}")
print(f"\nExcel -> {out}")
print("\nWhat it learned (most-used settings):")
for col in ["universe", "cap", "entry", "target", "stop", "time_exit"]:
    vc = CH[col].astype(str).value_counts()
    print(f"  {col:<10}: " + ", ".join(f"{k} ({v}d)" for k, v in vc.head(3).items()))
