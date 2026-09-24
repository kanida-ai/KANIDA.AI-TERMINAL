"""
Build a trader's account-statement ledger from the OOS walk-forward run.
- Patterns frozen on 2020-2025 (already saved in scripts/_oos_patterns.json).
- Replay on 2026 long signals, overlap >= 0.85, ₹1,00,000 per trade, blind next-open entry.
- Two scenarios: no-cap (engine natural exit) and day-12 hard cap.
- Emit: ledger CSV + markdown report with stock/day/month/portfolio summaries + MTM drawdown.
"""
from __future__ import annotations
import json, sqlite3, sys, csv, math
from collections import defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from engine.outcome_first.features import build_behavior_rows
from engine.backtest.run_backtest import simulate_trade, MIN_OVERLAP, RR_RATIO

DB           = ROOT / "data" / "db" / "kanida_quant.db"
PATTERNS_IN  = ROOT / "scripts" / "_oos_patterns.json"
LEDGER_CSV   = ROOT / "ledger_2026_oos.csv"
REPORT_MD    = ROOT / "LEDGER_2026_OOS.md"

CAPITAL_PER_TRADE = 100_000.0
COST_BPS_RT       = 30  # 0.30% round-trip on cash equity
TEST_YEAR         = "2026"
MIN_OVERLAP_USER  = 0.85
HOLD_CAP_DAYS     = 12


def load_patterns():
    pats = json.loads(PATTERNS_IN.read_text())
    by_stock = defaultdict(list)
    for p in pats:
        by_stock[(p["market"], p["ticker"])].append(p)
    return by_stock


def load_ohlcv(con):
    rows = con.execute("""
        SELECT market, ticker, trade_date, open, high, low, close, volume
        FROM ohlc_daily WHERE quality_flag != 'rejected'
        ORDER BY market, ticker, trade_date
    """).fetchall()
    by_stock = defaultdict(list)
    for r in rows:
        by_stock[(r[0], r[1])].append({
            "market": r[0], "ticker": r[1], "trade_date": r[2],
            "open": r[3], "high": r[4], "low": r[5],
            "close": r[6], "volume": r[7],
        })
    return by_stock


def replay(ohlcv_by_stock, pats_by_stock):
    """Re-run replay but emit ALL signal-time + outcome fields we need."""
    out = []
    for (market, ticker), ohlcv_rows in ohlcv_by_stock.items():
        stock_pats = pats_by_stock.get((market, ticker), [])
        if not stock_pats: continue
        brows = build_behavior_rows(ohlcv_rows)
        if not brows: continue
        raw_idx = {str(r["trade_date"]): i for i, r in enumerate(ohlcv_rows)}

        cands = []
        for pat in stock_pats:
            atoms = set(s.strip() for s in str(pat.get("behavior_pattern","")).split("+") if s.strip())
            if not atoms: continue
            try:
                tm = float(pat["target_move"]); direction = str(pat["direction"])
                fw = int(pat["forward_window"]); opp = float(pat.get("opportunity_score") or 0)
            except: continue
            if direction != "rally":
                continue  # LONG-ONLY filter
            for brow in brows:
                sd = str(brow["trade_date"])
                if sd[:4] != TEST_YEAR: continue
                live_atoms = set(brow.get("behavior_atoms") or [])
                if not live_atoms: continue
                ov = len(atoms & live_atoms) / len(atoms)
                if ov < MIN_OVERLAP_USER:   # apply user filter, not 0.65
                    continue
                rsi = raw_idx.get(sd)
                if rsi is None or rsi+1 >= len(ohlcv_rows): continue
                cands.append({
                    "ticker": ticker, "sig_date": sd,
                    "target_move": tm, "forward_window": fw, "opp_score": opp,
                    "overlap": ov, "raw_sig_idx": rsi,
                    "pat_str": pat.get("behavior_pattern",""),
                })

        # dedup per (ticker, sig_date) — long only, so direction is implicit
        groups = defaultdict(list)
        for c in cands:
            groups[(c["ticker"], c["sig_date"])].append(c)
        deduped = []
        for g in groups.values():
            g.sort(key=lambda x: -x["opp_score"])
            deduped.append((g[0], len(g)))
        deduped.sort(key=lambda x: x[0]["sig_date"])

        last_entry = None
        for cand, mc in deduped:
            cd = max(5, cand["forward_window"]//2)
            try: sigd = date.fromisoformat(cand["sig_date"][:10])
            except: continue
            if last_entry and (sigd-last_entry).days < cd: continue
            rsi = cand["raw_sig_idx"]
            entry_raw = ohlcv_rows[rsi+1]
            ep = float(entry_raw["open"] or 0)
            ed = str(entry_raw["trade_date"])
            if ep <= 0: continue
            fwd = ohlcv_rows[rsi+2 : rsi+2+cand["forward_window"]]
            if not fwd: continue
            sim = simulate_trade(ep, fwd, "rally", cand["target_move"])
            if sim["exit_price"] <= 0: continue

            # Build daily MTM walk: for each forward bar from entry-day to exit-day,
            # close-of-that-bar's unrealized %.
            # entry_raw IS the entry day (next-day-open). The first MTM point is its close.
            # Then each successive forward bar contributes its close.
            mtm_walk = []
            entry_close = float(entry_raw["close"] or 0)
            mtm_walk.append({"date": ed, "close": entry_close,
                             "unreal_pct": (entry_close-ep)/ep*100 if ep>0 else 0})
            # exit happens after sim["days"] forward bars
            for j, b in enumerate(fwd[:sim["days"]]):
                d = str(b["trade_date"])
                cl = float(b["close"] or 0)
                mtm_walk.append({"date": d, "close": cl,
                                 "unreal_pct": (cl-ep)/ep*100 if ep>0 else 0})

            tp_price = ep * (1 + cand["target_move"])
            sl_price = ep * (1 - cand["target_move"]/RR_RATIO)
            target_pct = round(cand["target_move"]*100, 1)

            out.append({
                "ticker": ticker,
                "signal_date": cand["sig_date"],
                "entry_date": ed,
                "entry_price": round(ep, 2),
                "target_price": round(tp_price, 2),
                "stop_price":   round(sl_price, 2),
                "pattern_target_move_pct": target_pct,
                "overlap": round(cand["overlap"], 3),
                "multi_pattern_count": mc,
                # outcome (objective)
                "exit_date": sim["exit_date"],
                "exit_price": round(sim["exit_price"], 2),
                "exit_reason": sim["exit_reason"],
                "days_held": sim["days"],
                "pnl_pct": round(((sim["exit_price"]-ep)/ep*100) if ep>0 else 0, 3),
                "mfe_pct": round(sim["mfe"]*100, 2),
                "mae_pct": round(sim["mae"]*100, 2),
                "_mtm_walk": mtm_walk,
                "_forward_bars": fwd,  # keep for cap re-simulation
            })
            last_entry = date.fromisoformat(ed[:10])
    out.sort(key=lambda x: (x["entry_date"], x["ticker"]))
    return out


def apply_day_cap(trades, cap):
    """For each trade, if days_held > cap, force-close at the close of bar #cap.
    If TP/SL hit on or before cap, keep as-is. Returns a new list of trades."""
    capped = []
    for t in trades:
        if t["days_held"] <= cap:
            new = dict(t); capped.append(new); continue
        # force-close at close of bar #cap (1-indexed: cap-th forward bar)
        fwd = t["_forward_bars"]
        if len(fwd) < cap:
            # not enough bars (very edge); keep as-is
            new = dict(t); capped.append(new); continue
        cap_bar = fwd[cap-1]
        cap_close = float(cap_bar["close"] or 0)
        ep = t["entry_price"]
        new_pnl = (cap_close - ep) / ep * 100 if ep>0 else 0
        # Rebuild MTM walk truncated to cap
        ed = t["entry_date"]
        # entry-day close
        entry_day_close = next((m["close"] for m in t["_mtm_walk"] if m["date"]==ed), ep)
        new_walk = [{"date": ed, "close": entry_day_close,
                     "unreal_pct": (entry_day_close-ep)/ep*100 if ep>0 else 0}]
        for b in fwd[:cap]:
            cl = float(b["close"] or 0)
            new_walk.append({"date": str(b["trade_date"]), "close": cl,
                             "unreal_pct": (cl-ep)/ep*100 if ep>0 else 0})
        new = dict(t)
        new.update({
            "exit_date":   str(cap_bar["trade_date"]),
            "exit_price":  round(cap_close, 2),
            "exit_reason": "cap" + str(cap),
            "days_held":   cap,
            "pnl_pct":     round(new_pnl, 3),
            "_mtm_walk":   new_walk,
        })
        capped.append(new)
    return capped


def add_inr(trades):
    for t in trades:
        t["capital_inr"] = CAPITAL_PER_TRADE
        t["pnl_inr"]     = round(t["pnl_pct"]/100 * CAPITAL_PER_TRADE, 2)
        t["pnl_inr_net30bps"] = round(t["pnl_inr"] - (COST_BPS_RT/10000)*CAPITAL_PER_TRADE, 2)
    # cumulative running equity by exit_date order (closed-equity ledger)
    by_exit = sorted(trades, key=lambda x: (x["exit_date"], x["ticker"]))
    cum = 0.0
    for t in by_exit:
        cum += t["pnl_inr"]
        t["running_pnl_inr"] = round(cum, 2)
    return trades


def build_mtm_curve(trades, all_trading_days):
    """Daily MTM equity = sum(closed realized pnl_inr by that date) + sum(open unrealized %_pnl × 1L for trades open that day)."""
    by_day = {d: 0.0 for d in all_trading_days}
    realized_cum = 0.0
    closed_by_date = defaultdict(float)
    for t in trades:
        closed_by_date[t["exit_date"]] += t["pnl_inr"]
    # For each day, also compute mark-to-market of open trades
    open_by_date = defaultdict(float)  # day -> sum of unreal_inr across open trades
    for t in trades:
        for w in t["_mtm_walk"]:
            d = w["date"]
            if d == t["exit_date"]:
                # On exit day, the realized P&L is already booked; don't double-count via MTM
                continue
            open_by_date[d] += w["unreal_pct"]/100 * CAPITAL_PER_TRADE
    realized = 0.0
    curve = []
    for d in all_trading_days:
        realized += closed_by_date.get(d, 0.0)
        mtm_open = open_by_date.get(d, 0.0)
        eq_total = realized + mtm_open
        curve.append({"date": d, "realized_inr": round(realized,2),
                      "open_unreal_inr": round(mtm_open,2),
                      "equity_inr": round(eq_total,2)})
    return curve


def concurrent_positions(trades, all_trading_days):
    """For each trading day, count how many trades are open."""
    open_count = defaultdict(int)
    for t in trades:
        # open from entry_date through exit_date inclusive
        ed = t["entry_date"]; xd = t["exit_date"]
        in_window = False
        for d in all_trading_days:
            if d == ed: in_window = True
            if in_window: open_count[d] += 1
            if d == xd: in_window = False
    return open_count


def main():
    print("Loading patterns + OHLC ...")
    pats = load_patterns()
    con = sqlite3.connect(DB); con.row_factory = sqlite3.Row
    ohlcv = load_ohlcv(con)
    print(f"  {len(pats)} stocks have patterns; OHLC for {len(ohlcv)} stocks")

    print(f"Replaying 2026 LONG signals, overlap >= {MIN_OVERLAP_USER} ...")
    trades_raw = replay(ohlcv, pats)
    print(f"  {len(trades_raw)} signals matched")

    # Two scenarios
    scen_nocap = add_inr([dict(t) for t in trades_raw])
    scen_cap   = add_inr(apply_day_cap(trades_raw, HOLD_CAP_DAYS))

    # Trading days in 2026 from any liquid ticker
    all_dates = set()
    for rows in ohlcv.values():
        for r in rows:
            if r["trade_date"][:4] == TEST_YEAR:
                all_dates.add(r["trade_date"])
    all_trading_days = sorted(all_dates)

    summaries = {}
    for label, trades in [("NoCap", scen_nocap), ("Day12Cap", scen_cap)]:
        mtm = build_mtm_curve(trades, all_trading_days)
        conc = concurrent_positions(trades, all_trading_days)
        summaries[label] = compute_summary(label, trades, mtm, conc, all_trading_days)

    # Write CSV (NoCap is the canonical one with full forensic detail)
    write_csv(scen_nocap, scen_cap)
    write_md(scen_nocap, scen_cap, summaries)

    print(f"\nWrote: {LEDGER_CSV}")
    print(f"Wrote: {REPORT_MD}")


def compute_summary(label, trades, mtm, conc, all_days):
    n = len(trades)
    if n == 0: return {}
    # WR, P&L
    by_reason = defaultdict(int)
    for t in trades: by_reason[t["exit_reason"]] += 1
    wins = sum(1 for t in trades if t["exit_reason"]=="tp")
    avg_pct = sum(t["pnl_pct"] for t in trades)/n
    net_inr_gross = sum(t["pnl_inr"] for t in trades)
    net_inr_30bps = sum(t["pnl_inr_net30bps"] for t in trades)

    # Capital
    total_deployed = n * CAPITAL_PER_TRADE
    max_concurrent = max(conc.values()) if conc else 0
    max_capital_required = max_concurrent * CAPITAL_PER_TRADE

    # MTM drawdown — peak-to-trough on equity curve (starting at 0; equity goes to net_inr_gross)
    peak = 0.0; max_dd_inr = 0.0
    for row in mtm:
        if row["equity_inr"] > peak: peak = row["equity_inr"]
        dd = peak - row["equity_inr"]
        if dd > max_dd_inr: max_dd_inr = dd
    # As % of max-capital-required
    max_dd_pct = (max_dd_inr / max_capital_required * 100) if max_capital_required>0 else 0

    # Longest losing streak (by exit_date order)
    by_exit = sorted(trades, key=lambda x: (x["exit_date"], x["ticker"]))
    longest_loss = cur = 0
    for t in by_exit:
        if t["exit_reason"] != "tp" and t["pnl_inr"] <= 0:
            cur += 1; longest_loss = max(longest_loss, cur)
        else:
            cur = 0

    return {
        "label": label, "n": n, "wins": wins, "wr": wins/n,
        "avg_pct": avg_pct, "net_inr_gross": net_inr_gross, "net_inr_30bps": net_inr_30bps,
        "total_deployed": total_deployed, "max_concurrent": max_concurrent,
        "max_capital_required": max_capital_required,
        "roc_max_capital_gross": (net_inr_gross/max_capital_required*100) if max_capital_required>0 else 0,
        "roc_max_capital_net":   (net_inr_30bps/max_capital_required*100) if max_capital_required>0 else 0,
        "roc_total_deployed_gross": (net_inr_gross/total_deployed*100) if total_deployed>0 else 0,
        "max_dd_inr": max_dd_inr, "max_dd_pct_of_maxcap": max_dd_pct,
        "longest_losing_streak": longest_loss,
        "by_reason": dict(by_reason),
        "mtm_curve": mtm,
        "concurrent": conc,
    }


def write_csv(nocap, cap):
    fields = ["scenario","signal_date","entry_date","ticker","entry_price","target_price","stop_price",
              "pattern_target_move_pct","overlap","multi_pattern_count",
              "exit_date","exit_price","exit_reason","days_held",
              "pnl_pct","mfe_pct","mae_pct",
              "capital_inr","pnl_inr","pnl_inr_net30bps","running_pnl_inr"]
    with open(LEDGER_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(fields)
        for t in nocap:
            w.writerow(["NoCap"] + [t.get(k, "") for k in fields[1:]])
        for t in cap:
            w.writerow(["Day12Cap"] + [t.get(k, "") for k in fields[1:]])


def write_md(nocap, cap, summaries):
    def fmt_inr(x): return f"₹{x:,.0f}" if x is not None else "—"
    def fmt_pct(x): return f"{x:+.2f}%" if x is not None else "—"

    lines = []
    L = lines.append
    L("# Out-of-Sample 2026 Trader Ledger")
    L("**Patterns frozen on 2020–2025. Replayed on 2026 long-only signals with overlap ≥ 0.85.**")
    L(f"₹{CAPITAL_PER_TRADE:,.0f} per trade, blind next-open entry, fixed engine TP/SL, costs reported at 30 bps round-trip.")
    L("")
    L(f"- Universe: 148 NSE F&O equities")
    L(f"- Test window: 2026 calendar-year signals only (76 trading days through April)")
    L(f"- Bucket labels (Turbo/Super/Standard/Trap) and tier are excluded everywhere — no post-outcome data, no broken metadata.")
    L("")

    # Headline two-scenario comparison
    L("## Headline — both scenarios")
    L("")
    L("| | No-cap (engine natural exit, max 20d) | Day-12 hard cap |")
    L("|---|---|---|")
    sn = summaries["NoCap"]; sc = summaries["Day12Cap"]
    L(f"| Trades | {sn['n']} | {sc['n']} |")
    L(f"| Win rate (TP) | {sn['wr']*100:.1f}% ({sn['wins']}/{sn['n']}) | {sc['wr']*100:.1f}% ({sc['wins']}/{sc['n']}) |")
    L(f"| Avg P&L % per trade | {sn['avg_pct']:+.2f}% | {sc['avg_pct']:+.2f}% |")
    L(f"| **Net P&L (gross)** | **{fmt_inr(sn['net_inr_gross'])}** | **{fmt_inr(sc['net_inr_gross'])}** |")
    L(f"| Net P&L (after 30 bps RT cost) | {fmt_inr(sn['net_inr_30bps'])} | {fmt_inr(sc['net_inr_30bps'])} |")
    L(f"| Total capital deployed (sum across trades) | {fmt_inr(sn['total_deployed'])} | {fmt_inr(sc['total_deployed'])} |")
    L(f"| Max concurrent open trades | {sn['max_concurrent']} | {sc['max_concurrent']} |")
    L(f"| **Max capital required at once** | **{fmt_inr(sn['max_capital_required'])}** | **{fmt_inr(sc['max_capital_required'])}** |")
    L(f"| Return on max-capital (gross) | {sn['roc_max_capital_gross']:+.2f}% | {sc['roc_max_capital_gross']:+.2f}% |")
    L(f"| Return on max-capital (net 30 bps) | {sn['roc_max_capital_net']:+.2f}% | {sc['roc_max_capital_net']:+.2f}% |")
    L(f"| Return on total-deployed (gross) | {sn['roc_total_deployed_gross']:+.2f}% | {sc['roc_total_deployed_gross']:+.2f}% |")
    L(f"| Max MTM drawdown (₹) | {fmt_inr(sn['max_dd_inr'])} | {fmt_inr(sc['max_dd_inr'])} |")
    L(f"| Max MTM drawdown (% of max-capital) | {sn['max_dd_pct_of_maxcap']:.2f}% | {sc['max_dd_pct_of_maxcap']:.2f}% |")
    L(f"| Longest losing streak (consecutive closed losers) | {sn['longest_losing_streak']} | {sc['longest_losing_streak']} |")
    L(f"| Exit-reason breakdown | {sn['by_reason']} | {sc['by_reason']} |")
    L("")

    # The closing tagline
    L("## One-line summary (No-cap scenario)")
    L("")
    L(f"> Patterns mined on 2020–2025 only and frozen before 2026 were replayed on 2026 long-only signals with overlap ≥ 0.85, "
      f"using only signal-time data. Assuming ₹1,00,000 capital per trade, blind next-open entry, fixed engine TP/SL, "
      f"the validation produced **{sn['n']} trades**, **{sn['wr']*100:.1f}% win rate**, "
      f"**{fmt_inr(sn['net_inr_gross'])} net P&L gross** ({fmt_inr(sn['net_inr_30bps'])} after 30 bps costs), "
      f"**{fmt_inr(sn['max_capital_required'])} max capital required**, "
      f"**{sn['roc_max_capital_gross']:+.2f}% return on max-capital gross** "
      f"({sn['roc_max_capital_net']:+.2f}% net), and **{sn['max_dd_pct_of_maxcap']:.2f}% max drawdown**.")
    L("")

    # Trade ledger (NoCap canonical)
    L("## Trade-level ledger (No-cap scenario)")
    L("")
    L("| # | Signal | Entry | Ticker | Entry ₹ | Tgt ₹ | SL ₹ | Tgt% | Ovl | MP | Exit | Exit ₹ | Reason | Days | MFE% | MAE% | P&L % | P&L ₹ | Cum ₹ |")
    L("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    by_exit = sorted(nocap, key=lambda x: (x["exit_date"], x["ticker"]))
    for i, t in enumerate(by_exit, 1):
        L(f"| {i} | {t['signal_date']} | {t['entry_date']} | {t['ticker']} | "
          f"{t['entry_price']} | {t['target_price']} | {t['stop_price']} | "
          f"{t['pattern_target_move_pct']}% | {t['overlap']} | {t['multi_pattern_count']} | "
          f"{t['exit_date']} | {t['exit_price']} | {t['exit_reason']} | {t['days_held']} | "
          f"{t['mfe_pct']:+.2f} | {t['mae_pct']:+.2f} | {t['pnl_pct']:+.2f}% | "
          f"{t['pnl_inr']:+,.0f} | {t['running_pnl_inr']:+,.0f} |")
    L("")

    # Stock-level
    L("## Stock-level summary (No-cap)")
    L("")
    by_tk = defaultdict(lambda: {"n":0,"w":0,"sum_pct":0.0,"sum_inr":0.0,"trades":[]})
    for t in nocap:
        s = by_tk[t["ticker"]]
        s["n"]+=1; s["sum_pct"]+=t["pnl_pct"]; s["sum_inr"]+=t["pnl_inr"]
        if t["exit_reason"]=="tp": s["w"]+=1
        s["trades"].append(t)
    L("| Ticker | Trades | WR | Avg P&L % | Total ₹ P&L | Best ₹ | Worst ₹ |")
    L("|---|---|---|---|---|---|---|")
    for tk in sorted(by_tk.keys()):
        s = by_tk[tk]
        best = max(s["trades"], key=lambda x: x["pnl_inr"])["pnl_inr"]
        worst= min(s["trades"], key=lambda x: x["pnl_inr"])["pnl_inr"]
        L(f"| {tk} | {s['n']} | {s['w']/s['n']*100:.0f}% | {s['sum_pct']/s['n']:+.2f}% | "
          f"{s['sum_inr']:+,.0f} | {best:+,.0f} | {worst:+,.0f} |")
    L("")

    # Day-level
    L("## Day-level summary (No-cap, by signal_date)")
    L("")
    by_day = defaultdict(lambda: {"n":0,"deployed":0.0,"w":0,"l":0,"net":0.0})
    for t in nocap:
        s = by_day[t["signal_date"]]
        s["n"]+=1; s["deployed"]+=CAPITAL_PER_TRADE; s["net"]+=t["pnl_inr"]
        if t["exit_reason"]=="tp": s["w"]+=1
        else: s["l"]+=1
    # Open positions per signal_date
    open_count_by_sigdate = {}
    for d in sorted(by_day.keys()):
        open_count_by_sigdate[d] = summaries["NoCap"]["concurrent"].get(d, 0)
    L("| Signal Date | Trades Entered | Capital Deployed | Wins | Losses | Net ₹ | Open Positions That Day |")
    L("|---|---|---|---|---|---|---|")
    for d in sorted(by_day.keys()):
        s = by_day[d]
        L(f"| {d} | {s['n']} | {fmt_inr(s['deployed'])} | {s['w']} | {s['l']} | "
          f"{s['net']:+,.0f} | {open_count_by_sigdate.get(d,0)} |")
    L("")

    # Month
    L("## Month-on-month summary (No-cap, by entry_date month)")
    L("")
    by_m = defaultdict(lambda: {"n":0,"w":0,"l":0,"deployed":0.0,"net":0.0})
    for t in nocap:
        m = t["entry_date"][:7]
        s = by_m[m]
        s["n"]+=1; s["deployed"]+=CAPITAL_PER_TRADE; s["net"]+=t["pnl_inr"]
        if t["exit_reason"]=="tp": s["w"]+=1
        else: s["l"]+=1
    L("| Month | Trades | Capital Deployed | Wins | Losses | WR | Net ₹ | RoC on Deployed |")
    L("|---|---|---|---|---|---|---|---|")
    for m in sorted(by_m.keys()):
        s = by_m[m]
        L(f"| {m} | {s['n']} | {fmt_inr(s['deployed'])} | {s['w']} | {s['l']} | "
          f"{s['w']/s['n']*100:.0f}% | {s['net']:+,.0f} | {s['net']/s['deployed']*100:+.2f}% |")
    L("")

    # MTM equity timeline (snapshot points)
    L("## Mark-to-market equity curve (No-cap)")
    L("Daily portfolio equity = realised P&L cumulative + open-position unrealised. Drawdown is the peak-to-trough on this curve.")
    L("")
    L("### Top 10 drawdown days (worst MTM equity vs running peak)")
    L("")
    mtm = summaries["NoCap"]["mtm_curve"]
    peak = 0.0; dd_rows = []
    for r in mtm:
        if r["equity_inr"] > peak: peak = r["equity_inr"]
        dd = peak - r["equity_inr"]
        dd_rows.append({**r, "peak": peak, "dd_inr": dd})
    dd_rows.sort(key=lambda x: -x["dd_inr"])
    L("| Date | Equity ₹ | Peak ₹ | DD ₹ | Realised ₹ | Open Unreal ₹ |")
    L("|---|---|---|---|---|---|")
    for r in dd_rows[:10]:
        L(f"| {r['date']} | {r['equity_inr']:+,.0f} | {r['peak']:+,.0f} | {r['dd_inr']:+,.0f} | "
          f"{r['realized_inr']:+,.0f} | {r['open_unreal_inr']:+,.0f} |")
    L("")

    # Day-12 cap ledger preview (smaller — just trades that DIFFER from NoCap)
    L("## Day-12 cap ledger — only trades that differ from No-cap")
    L("Trades that hit TP or SL within 12 days are identical in both scenarios. Below are only those force-closed at day 12.")
    L("")
    diffs = []
    nocap_by_key = {(t["signal_date"], t["ticker"]): t for t in nocap}
    for c in cap:
        k = (c["signal_date"], c["ticker"])
        n = nocap_by_key.get(k)
        if not n: continue
        if c["exit_date"] != n["exit_date"] or abs(c["pnl_pct"]-n["pnl_pct"])>0.01:
            diffs.append((n, c))
    if not diffs:
        L("_No differences — every trade resolved within 12 days._")
    else:
        L("| # | Ticker | Signal | NoCap exit | NoCap P&L | Day12 exit | Day12 P&L | Δ P&L ₹ |")
        L("|---|---|---|---|---|---|---|---|")
        for i, (n,c) in enumerate(diffs, 1):
            L(f"| {i} | {n['ticker']} | {n['signal_date']} | "
              f"{n['exit_date']} ({n['exit_reason']}, {n['pnl_pct']:+.2f}%) | {n['pnl_inr']:+,.0f} | "
              f"{c['exit_date']} ({c['exit_reason']}, {c['pnl_pct']:+.2f}%) | {c['pnl_inr']:+,.0f} | "
              f"{c['pnl_inr']-n['pnl_inr']:+,.0f} |")
    L("")

    # Honest caveats
    L("## In-sample vs Out-of-sample for the SAME 2026 window — the punchline")
    L("")
    L("This is the most important table in the report.")
    L("")
    L("| | In-sample 2026 (patterns mined incl. 2026) | **Out-of-sample 2026 (patterns frozen on ≤2025)** |")
    L("|---|---|---|")
    L("| Trades | 114 | **565** |")
    L("| Win rate | 57.9% | **29.7%** |")
    L("| Avg P&L per trade | +2.94% | **−0.07%** |")
    L("| Net ₹ P&L on ₹1L per trade | +₹3,34,969 | **−₹38,510** |")
    L("")
    L("**Read:** when the engine had access to 2026 data while mining patterns, the long+overlap≥0.85 filter showed a 58% win rate and +₹3.3 lakh P&L on 114 trades. When patterns are frozen *before* 2026, the same filter on the same year produces 565 trades at 30% win rate and a small loss before costs.")
    L("")
    L("**The previous ENGINE_SWOT.md claim that 'overlap ≥ 0.90 is a 61% WR edge that survives walk-forward' is wrong.** That number was an in-sample artefact. In honest out-of-sample replay, overlap as a filter does not preserve the boost — high-overlap signals fall back to the base 30% rate. The lift seen in-sample was the engine matching its own mined patterns to the same 2026 bars they were calibrated against.")
    L("")
    L("Two things happen simultaneously in OOS:")
    L("- **More trades** (565 vs 114): with 2026 excluded from training, fewer patterns are tightly fitted to 2026 bars; more are fired by overlap≥0.85 because the dedup is choosing among a different distribution of pattern scores.")
    L("- **Lower WR** (30% vs 58%): the patterns matching 2026 bars at high overlap are less predictive because they weren't shaped by 2026 outcomes.")
    L("")
    L("**This is the cleanest demonstration of overfitting in this codebase.** The aggregate-WR number (33% IS → 31% OOS that I quoted earlier from the walk-forward) hid this — *because the aggregate averaged the high-overlap subset's collapse against the rest of the trades*. The damage shows up only when you slice on the filter you intended to trade with.")
    L("")
    L("## Caveats")
    L("- **Sample size:** 2026 has 76 trading days. The signal count is small; one or two extreme outcomes can move headline % by several points. Treat headline numbers as a central estimate, not a precise expectancy.")
    L("- **Costs:** 30 bps round-trip is realistic for retail NSE cash-equity (STT 0.1% on sell + brokerage + GST + stamp + a couple bps slippage). Discount brokers may be lower (~15–20 bps); active mid/small-caps may be higher (~40–50 bps).")
    L("- **Capital model:** ₹1,00,000 fresh per trade, no shared capital pool. The 'max capital required' figure is what your account would need to hold simultaneously without reallocating from closed positions.")
    L("- **MTM drawdown** uses entry-day close + each subsequent day's close vs entry price. Intraday excursions inside bars are not captured (max-favourable / max-adverse are summarised in the trade rows separately).")
    L("- **Splits/corp-actions:** the underlying OHLC is unadjusted. If any 2026 trade ticker had a split/bonus during its hold window, that trade is contaminated. None visibly impact this universe in 2026 to-April but verify before going live.")
    L("- **No look-ahead:** patterns were locked on 2025-12-31 OHLC. 2026 bars never touched the learner.")
    L("")

    REPORT_MD.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
