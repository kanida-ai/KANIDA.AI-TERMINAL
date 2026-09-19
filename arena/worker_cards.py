"""
WORKER PERFORMANCE CARDS — customer-facing, plain-language per-worker KPIs, and a browsable dashboard.
Capital model (stated on the page): Rs10,00,000 start, ONE position at a time, net of costs, leverage as
routed (MIS/NRML 5x, CNC 1x), fixed notional so returns are additive (no compounding illusion). Book runs
the expectancy-selected patterns over 2025+2026 (clean OOS).

Per worker: Current Value, Total/30D/90D Return, CAGR, Trades/Month, Net Expectancy, Win Rate, Profit
Factor, Max Drawdown, Avg Hold, Active Patterns, Confidence, Constitutional Status, + Order-Type and
Direction breakdowns, + an equity sparkline. Emits docs/kanida_workers.html (self-contained artifact).
Run: PYTHONIOENCODING=utf-8 python arena/worker_cards.py
"""
import sys, json, sqlite3
from pathlib import Path
import numpy as np, pandas as pd
ROOT = Path(r"C:\Users\SPS\Documents\Kanida_Falcon")
sys.path.insert(0, str(ROOT / "scripts")); sys.path.insert(0, str(ROOT / "kanida_engine"))
from mine_phase1 import apply_rule
import features as FE
import routing

SNR = str(ROOT / "db" / "KANIDA_SNR.db"); KDB = str(ROOT / "db" / "kanida.db"); REP = ROOT / "reports"
TGT = {"up_1pct_1d": ("up", 1, 1), "up_2pct_2d": ("up", 2, 2), "up_5pct_5d": ("up", 5, 5),
       "dn_1pct_1d": ("dn", 1, 1), "dn_2pct_2d": ("dn", 2, 2), "dn_5pct_5d": ("dn", 5, 5)}
CAP = 100_000.0; SELECT_MAX_YEAR = 2024; MIN_OCC_PRE = 15
POS_FRAC = 1.0           # INVESTED-CASH basis: deploy Rs1,00,000/day at 5x MIS; MIS recycles daily -> additive returns


def outcome(frame, d, pct, w, product, mode):
    """Per-day trade net ROC + hold, via the central legal router (handles daily-MIS-reshort for non-F&O)."""
    O, H, L, C = frame["_o"].values, frame["_h"].values, frame["_l"].values, frame["_c"].values
    n = len(O); net = np.full(n, np.nan); hold = np.full(n, np.nan)
    for i in range(n - 1):
        r = routing.sim_roc(O, H, L, C, i, d, pct, w, product, mode)
        if r is not None:
            net[i] = r[0]; hold[i] = r[1]
    return net, hold


def kpis(trades, active_patterns, sustained, min_netexp):
    """trades: list of dicts {date(exit), roc, order, direction, hold}. Returns the card dict."""
    if len(trades) < 8: return None
    df = pd.DataFrame(trades).sort_values("date").reset_index(drop=True)
    roc = df.roc.values
    # INVESTED-CASH, ADDITIVE (MIS recycles daily -> returns/drawdown are summed, not compounded)
    cr = np.cumsum(roc)                                  # cumulative return in % on the deployed cash
    crf = np.concatenate([[0.0], cr]); dates = pd.to_datetime(df.date.values)
    total_ret = cr[-1] / 100.0; cur = CAP * (1 + total_ret)
    eqf = CAP * (1 + crf / 100.0)                                     # continuous account equity (start CAP, +daily P&L)
    maxdd = float(((eqf - np.maximum.accumulate(eqf)) / np.maximum.accumulate(eqf)).min())  # VIEW 1 headline: account MDD
    dd_fixed = float((crf - np.maximum.accumulate(crf)).min()) / 100.0  # VIEW 2 context: absolute give-back / fixed ₹1L
    span_days = max(1, (dates[-1] - dates[0]).days); yrs = span_days / 365.25
    cagr = total_ret / yrs if yrs > 0 else 0.0           # simple annualisation (additive book, no compounding)
    crs = pd.Series(cr, index=dates)

    def ret_since(days):
        cut = dates[-1] - pd.Timedelta(days=days); before = crs[crs.index <= cut]
        base = float(before.iloc[-1]) if len(before) else 0.0
        return (cr[-1] - base) / 100.0                   # additive return over the window
    win = roc > 0; gross_win = roc[win].sum(); gross_loss = -roc[~win].sum()
    pf = float(gross_win / gross_loss) if gross_loss > 0 else 99.9
    tpm = len(df) / (span_days / 30.0)
    conf = "High" if len(df) >= 60 else ("Medium" if len(df) >= 25 else "Low")
    status = "Healthy" if (sustained and min_netexp >= 1.0) else ("Watch" if sustained else "Caution")

    base = POS_FRAC * CAP                                                # each strategy's own virtual account (Rs1.5L)

    def grp(col):
        out = []
        for k, g in df.groupby(col):
            g = g.sort_values("date"); gr = g.roc.values; gd = pd.to_datetime(g.date.values)
            gp = gr[gr > 0].sum(); gl = -gr[gr <= 0].sum()
            geq = base * (1 + np.cumsum(gr) / 100.0)                     # strategy equity (additive on Rs1.5L)

            def sret_since(days):
                cut = gd[-1] - pd.Timedelta(days=days); before = geq[gd <= cut]
                b = before[-1] if len(before) else base
                return geq[-1] / b - 1
            out.append({"k": k, "n": int(len(g)), "win": round(float((gr > 0).mean()) * 100),
                        "avg": round(float(gr.mean()), 2),                       # avg net return / trade (= net expectancy)
                        "pf": round(float(gp / gl), 2) if gl > 0 else 99.9,      # profit factor (capital-independent)
                        "hold": round(float(g.hold.mean()), 1),                  # avg hold (trading days)
                        "pnl": round(POS_FRAC * CAP * float(gr.sum()) / 100),    # ACTUAL Rs P&L (15%-sized), reconciles
                        "r30": round(sret_since(30) * 100, 1),                   # strategy 30-day return (own account)
                        "r90": round(sret_since(90) * 100, 1)})                  # strategy 90-day return (own account)
        return sorted(out, key=lambda x: -x["pnl"])
    spark = list(np.round(np.interp(np.linspace(0, len(eqf) - 1, min(40, len(eqf))),
                                    np.arange(len(eqf)), eqf) / CAP, 3))
    eqcurve = list(np.round(np.interp(np.linspace(0, len(eqf) - 1, min(90, len(eqf))),
                                      np.arange(len(eqf)), eqf) / CAP, 3))     # continuous account equity (never reset)
    dfm = df.copy(); dfm["month"] = pd.to_datetime(dfm.date).dt.strftime("%Y-%m")
    by_month = []
    for m, g in dfm.groupby("month"):
        dr = g.roc.values; mc = np.concatenate([[0.0], np.cumsum(dr)])       # intramonth cumulative (reset to 0)
        intradd = float((mc - np.maximum.accumulate(mc)).min())              # worst give-back WITHIN the month
        by_month.append({"m": m, "n": int(len(g)), "win": round(float((dr > 0).mean()) * 100),
                         "pnl": round(POS_FRAC * CAP * float(dr.sum()) / 100),
                         "ret": round(POS_FRAC * float(dr.sum()), 1), "dd": round(intradd, 1)})
    mrets = [x["ret"] for x in by_month]; byyear = {}
    for x in by_month: byyear[x["m"][:4]] = round(byyear.get(x["m"][:4], 0) + x["ret"], 1)
    consistency = {"best_month": max(mrets) if mrets else 0, "worst_month": min(mrets) if mrets else 0,
                   "pct_pos_months": round(sum(1 for r in mrets if r > 0) / len(mrets) * 100) if mrets else 0,
                   "by_year": byyear}
    dfm["yr"] = dfm["month"].str[:4]; by_year_detail = []
    for y, g in dfm.groupby("yr"):
        gm = [x for x in by_month if x["m"][:4] == y]
        by_year_detail.append({"y": y, "ret": round(float(g.roc.sum())), "n": int(len(g)),
                               "win": round(float((g.roc.values > 0).mean()) * 100),
                               "worst_month": round(min((x["ret"] for x in gm), default=0)),
                               "best_month": round(max((x["ret"] for x in gm), default=0))})
    log = [[str(pd.Timestamp(r.entry).date()), str(pd.Timestamp(r.date).date()), r.order, r.direction,
            int(r.hold), round(float(r.roc), 2), round(POS_FRAC * CAP * float(r.roc) / 100)]
           for r in df.itertuples()]
    return {
        "current_value": round(cur), "total_return": round(total_ret * 100, 1),
        "ret_30d": round(ret_since(30) * 100, 1), "ret_90d": round(ret_since(90) * 100, 1),
        "cagr": round(cagr * 100, 1), "trades": int(len(df)), "tpm": round(tpm, 1),
        "net_exp": round(float(roc.mean()), 2), "win_rate": round(float(win.mean()) * 100),
        "profit_factor": round(pf, 2), "max_dd": round(maxdd * 100, 1),
        "avg_hold": round(float(df.hold.mean()), 1), "active_patterns": active_patterns,
        "confidence": conf, "status": status, "by_order": grp("order"), "by_dir": grp("direction"),
        "by_month": by_month, "consistency": consistency, "by_year_detail": by_year_detail, "eqcurve": eqcurve,
        "dd_fixed": round(dd_fixed * 100, 1), "worst_month": round(min(mrets), 1) if mrets else 0, "log": log,
        "spark": spark, "sustained": bool(sustained), "min_netexp": round(float(min_netexp), 2) if min_netexp == min_netexp else None,
    }


def build_worker(symbol, con, sc, is_fno):
    rows = [(t, rj) for t, rj in con.execute("SELECT target,rule_json FROM unified_patterns WHERE symbol=?", (symbol,)).fetchall() if t in TGT]
    if not rows: return None
    frame = FE.load_frame(symbol, lookback_N=5)
    if frame.empty: return None
    yr = frame["year"].values; idx = frame.index
    net = {}; hold = {}; prod = {}
    for t in set(t for t, _ in rows):
        d, pct, w = TGT[t]; product, mode = routing.route(d, w, is_fno)     # LEGAL routing (F&O-aware)
        prod[t] = routing.label(product, mode)                              # 'MIS-Daily' split from true MIS
        net[t], hold[t] = outcome(frame, d, pct, w, product, mode)
    pre = yr <= SELECT_MAX_YEAR; kept = []
    for t, rj in rows:
        conds = [tuple(c) for c in json.loads(rj)]; mask = apply_rule(frame, conds).values
        r = net[t][pre & mask]; r = r[np.isfinite(r)]
        if len(r) >= MIN_OCC_PRE and r.mean() > 0:
            kept.append((r.mean(), t, TGT[t][2], mask))
    if not kept: return None
    kept.sort(reverse=True, key=lambda x: x[0])
    te = np.where((yr == 2025) | (yr == 2026))[0]; trades = []; day = 0
    while day < len(te):
        gi = te[day]; hit = None
        for pe, t, w, mask in kept:
            if mask[gi] and np.isfinite(net[t][gi]):
                d = TGT[t][0]; hit = dict(entry=idx[min(gi + 1, len(idx) - 1)],
                                          date=idx[min(gi + int(hold[t][gi]), len(idx) - 1)],
                                          roc=float(net[t][gi]), order=prod[t],
                                          direction="Long" if d == "up" else "Short", hold=int(hold[t][gi])); break
        if hit:
            trades.append(hit); day += max(1, hit["hold"])         # skip the hold window (non-overlapping)
        else:
            day += 1
    k = kpis(trades, len(kept), bool(sc.get("sustained", False)), sc.get("min_netexp", float("nan")))
    if not k: return None
    k["symbol"] = symbol
    return k


def render_html(cards):
    data = json.dumps(cards, separators=(",", ":"))
    sell = sum(1 for c in cards if c.get("ret_dd", 0) >= 10 and c["sustained"])
    med = round(float(np.median([c.get("ret_dd", 0) for c in cards])), 1) if cards else 0
    rd = lambda n: (ROOT / "agents_arena" / n).read_text(encoding="utf-8") if (ROOT / "agents_arena" / n).exists() else "[]"
    tmpl = (TEMPLATE.replace("__DATA__", data).replace("__N__", str(len(cards)))
            .replace("__SELL__", str(sell)).replace("__MED__", str(med))
            .replace("__AGENTS__", rd("_all.json")).replace("__GRID__", rd("_grid.json")).replace("__GRIDOPTS__", rd("_grid_opts.json")))
    (ROOT / "docs" / "kanida_workers.html").write_text(tmpl, encoding="utf-8")


def _newest_scorecard():
    a = REP / "expectancy_scorecard.csv"; b = REP / "expectancy_scorecard_NEW.csv"
    return b if (b.exists() and (not a.exists() or b.stat().st_mtime > a.stat().st_mtime)) else a


def main():
    sc = pd.read_csv(_newest_scorecard()).set_index("symbol")
    scd = sc.to_dict("index")
    con = sqlite3.connect(SNR); kc = sqlite3.connect(KDB)
    fno = {r[0]: r[1] for r in kc.execute("SELECT symbol,is_fno FROM instrument_labels").fetchall()}; kc.close()
    syms = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM unified_patterns").fetchall()]
    cards = []
    for n, s in enumerate(syms, 1):
        try:
            c = build_worker(s, con, scd.get(s, {}), int(fno.get(s, 0)))
            if c: cards.append(c)
        except Exception as e:
            print(f"  {s}: ERR {str(e)[:70]}")
        if n % 50 == 0: print(f"  ...{n}/{len(syms)}", flush=True)
    con.close()
    cards.sort(key=lambda c: (c["sustained"] and (c["min_netexp"] or 0) >= 1.0, c["net_exp"]), reverse=True)
    (REP / "worker_cards.json").write_text(json.dumps(cards), encoding="utf-8")
    render_html(cards)
    print(f"\n  {len(cards)} worker cards | sellable(>=+1%): "
          f"{sum(1 for c in cards if c['min_netexp'] is not None and c['sustained'] and c['min_netexp']>=1.0)}")
    print("  written: docs/kanida_workers.html + reports/worker_cards.json")


TEMPLATE = r"""<style>
:root{--bg:#f6f8f9;--card:#fff;--ink:#0f1c1a;--mut:#5b6b68;--line:#e3e9e8;--accent:#0f9d8f;--accentbg:#e6f5f3;--pos:#0a8a4f;--neg:#c8442e;--chip:#eef3f2}
:root[data-theme=dark],@media (prefers-color-scheme:dark){:root{--bg:#0d1413;--card:#131d1c;--ink:#e8f0ee;--mut:#8fa39f;--line:#243230;--accent:#3fd6c3;--accentbg:#12312d;--pos:#3fcf7f;--neg:#f2765c;--chip:#182422}}
:root[data-theme=dark]{--bg:#0d1413;--card:#131d1c;--ink:#e8f0ee;--mut:#8fa39f;--line:#243230;--accent:#3fd6c3;--accentbg:#12312d;--pos:#3fcf7f;--neg:#f2765c;--chip:#182422}
*{box-sizing:border-box}body{margin:0}
.wrap{font:15px/1.5 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:var(--ink);background:var(--bg);min-height:100vh;padding:22px}
.mono{font-family:'SF Mono',ui-monospace,'Cascadia Code',Menlo,monospace;font-variant-numeric:tabular-nums}
h1{font-size:22px;margin:0 0 2px}.sub{color:var(--mut);font-size:13px;margin:0 0 16px}
.top{display:flex;flex-wrap:wrap;gap:12px;align-items:center;margin-bottom:16px}
.stat{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:10px 14px}
.stat b{font-size:19px}.stat span{color:var(--mut);font-size:12px;display:block}
.ctrl{margin-left:auto;display:flex;gap:8px;flex-wrap:wrap}
input,select,button{font:inherit;padding:8px 12px;border:1px solid var(--line);border-radius:9px;background:var(--card);color:var(--ink)}
button{cursor:pointer}button.on{background:var(--accent);color:#fff;border-color:var(--accent)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:14px;cursor:pointer;transition:.15s;position:relative}
.card:hover{border-color:var(--accent);transform:translateY(-2px)}
.tick{font-weight:700;font-size:16px}.badge{font-size:10.5px;font-weight:700;padding:2px 7px;border-radius:20px;text-transform:uppercase;letter-spacing:.04em}
.b-elite{background:var(--accent);color:#fff}.b-strong{background:var(--accentbg);color:var(--accent)}.b-solid{background:var(--chip);color:var(--mut)}.b-watch{background:#f4ede0;color:#9a7b31}.b-port{background:#ece5fb;color:#6a4bbc}
.tabs{display:flex;gap:2px;margin:0 0 18px;border-bottom:1px solid var(--line)}
.tab{background:none;border:none;border-bottom:2px solid transparent;border-radius:0;padding:9px 4px;margin-right:16px;color:var(--mut);font-weight:600;cursor:pointer}
.tab.on{color:var(--ink);border-bottom-color:var(--accent)}
.ahero{max-width:780px;margin:0 0 22px}.ahero h2{font-size:20px;margin:0 0 6px}.ahero p{color:var(--mut);font-size:14px;line-height:1.6;margin:0}
.agent{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:20px;max-width:840px;margin-bottom:18px}
.ahead{display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap}
.aav{width:46px;height:46px;border-radius:50%;background:var(--accentbg);color:var(--accent);display:flex;align-items:center;justify-content:center;font-weight:700;font-size:19px;flex-shrink:0}
.aav.sm{width:34px;height:34px;font-size:14px}
.aname{font-weight:700;font-size:17px}.aspec{color:var(--mut);font-size:13px}
.astats{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:4px}
.astat{background:var(--bg);border:1px solid var(--line);border-radius:10px;padding:10px 12px}
.astat span{color:var(--mut);font-size:11px;display:block}.astat b{font-size:20px;color:var(--mut)}
.asec{border-top:1px solid var(--line);padding-top:14px;margin-top:14px}
.asec h4{margin:0 0 8px;font-size:14px;color:var(--accent)}
.asec ol{margin:0;padding-left:18px}.asec li{margin-bottom:6px;font-size:13.5px;line-height:1.5}
.asec p{font-size:13.5px;line-height:1.6;margin:0 0 8px}
.qs{display:flex;flex-wrap:wrap;gap:6px;margin:8px 0}
.qs span{background:var(--accentbg);color:var(--accent);border-radius:8px;padding:5px 10px;font-size:12px;font-weight:600}
.acoming{font-size:15px;margin:8px 0 10px}
.agrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:12px;max-width:840px}
.mini{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px;display:flex;align-items:center;gap:10px}
.mini b{font-size:14px}.mini span{color:var(--mut);font-size:12px;display:block}
.note{color:var(--mut);font-size:12px;font-style:italic;margin:4px 0 0}
.wgrid{overflow-x:auto;margin:14px 0 22px;max-width:900px}
.wgrid table{border-collapse:separate;border-spacing:4px;width:100%;font-size:12.5px}
.wgrid th{color:var(--mut);font-weight:600;font-size:11px;padding:4px 8px;text-transform:uppercase;letter-spacing:.03em}
.wgrid td{padding:9px 10px;text-align:center;border-radius:8px;font-variant-numeric:tabular-nums;font-weight:600}
.wgrid td.ag{text-align:left;background:var(--card);border:1px solid var(--line);font-weight:600;white-space:nowrap}
.ag small{display:block;color:var(--mut);font-weight:400;font-size:10.5px}
.rk{display:block;font-size:10px;opacity:.7;font-weight:400}
.aworld{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px 18px;max-width:900px;margin-bottom:14px}
.ahead2{display:flex;align-items:center;gap:12px;margin-bottom:6px;flex-wrap:wrap}
.wbadges{display:flex;gap:6px;flex-wrap:wrap;margin:10px 0 0}
.wbadge{border-radius:8px;padding:5px 10px;font-size:11px;font-weight:600;border:1px solid var(--line)}
.jrnl{background:var(--bg);border-left:3px solid var(--accent);border-radius:0 10px 10px 0;padding:11px 14px;font-size:13px;line-height:1.65;margin-top:12px;white-space:pre-line}
.jrnl b{color:var(--ink)}
.bform{display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px 18px;max-width:900px;margin-bottom:16px}
.bfield{display:flex;flex-direction:column;gap:5px}
.bfield label{font-size:11px;color:var(--mut);text-transform:uppercase;letter-spacing:.03em;font-weight:600}
.bfield select,.bfield input{min-width:150px}
.bkpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:10px;margin-top:12px}
.bk{background:var(--bg);border:1px solid var(--line);border-radius:10px;padding:9px 12px}
.bk span{display:block;color:var(--mut);font-size:10.5px;text-transform:uppercase;letter-spacing:.02em}
.bk b{font-size:18px;font-variant-numeric:tabular-nums}
:root[data-theme=dark] .b-watch{background:#2a2413;color:#d8b968}
.big{font-size:26px;font-weight:700;margin:8px 0 2px}.row{display:flex;justify-content:space-between;font-size:12.5px;color:var(--mut);margin-top:3px}
.pos{color:var(--pos)}.neg{color:var(--neg)}
svg.spk{width:100%;height:34px;margin-top:8px;display:block}
.ov{position:fixed;inset:0;background:rgba(0,0,0,.45);display:none;align-items:flex-start;justify-content:center;padding:24px;overflow:auto;z-index:9}
.ov.on{display:flex}
.panel{background:var(--card);border:1px solid var(--line);border-radius:16px;max-width:820px;width:100%;padding:22px}
.phead{display:flex;align-items:center;gap:10px;margin-bottom:4px}.phead h2{margin:0;font-size:22px}
.x{margin-left:auto;background:var(--chip);border:none;border-radius:8px;padding:6px 11px;cursor:pointer;color:var(--ink)}
.kgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin:14px 0}
.kv{background:var(--bg);border:1px solid var(--line);border-radius:11px;padding:10px 12px}
.kv .l{color:var(--mut);font-size:11.5px}.kv .v{font-size:19px;font-weight:700;margin-top:2px}
h3{font-size:13px;text-transform:uppercase;letter-spacing:.05em;color:var(--mut);margin:16px 0 7px}
table{width:100%;border-collapse:collapse;font-size:13.5px}th,td{text-align:right;padding:7px 9px;border-bottom:1px solid var(--line)}
th:first-child,td:first-child{text-align:left}th{color:var(--mut);font-weight:600;font-size:11.5px;text-transform:uppercase}
.note{color:var(--mut);font-size:11.5px;margin-top:14px;line-height:1.5}
.method{background:var(--card);border:1px solid var(--line);border-radius:12px;margin-bottom:16px;overflow:hidden}
.method summary{cursor:pointer;padding:12px 16px;font-weight:600;font-size:14px;list-style:none}
.method summary::-webkit-details-marker{display:none}
.method summary::before{content:"ⓘ  ";color:var(--accent)}
.method[open] summary{border-bottom:1px solid var(--line)}
.mbody{padding:6px 16px 14px;font-size:13px;color:var(--mut);line-height:1.6}
.mbody b{color:var(--ink)}.mbody p{margin:10px 0}
.scroll{overflow-x:auto}.sub2{color:var(--mut);font-size:12px;margin:2px 0 8px}
.strat{background:var(--bg);border:1px solid var(--line);border-radius:11px;padding:11px 13px;margin:8px 0;font-size:13px;line-height:1.55}
.strat .sh{font-family:var(--mono);font-weight:700;color:var(--accent);margin-bottom:4px}
.strat b{color:var(--ink)}
button.dl{background:var(--accent);color:#fff;border:none;border-radius:9px;padding:8px 14px;cursor:pointer;font-weight:600;margin-bottom:8px}
</style>
<div class="wrap">
  <h1>KANIDA — Arena</h1>
  <p class="sub">Autonomous AI traders — paper / research figures, real money human-gated. Two classes: per-stock workers, and cross-stock specialist agents.</p>
  <div class="tabs">
    <button class="tab on" id="tab-btn-workers" onclick="tab('workers')">Per-Stock Workers</button>
    <button class="tab" id="tab-btn-agents" onclick="tab('agents')">Specialist Agents</button>
    <button class="tab" id="tab-btn-worlds" onclick="tab('worlds')">Market Worlds</button>
    <button class="tab" id="tab-btn-build" onclick="tab('build')">Build an Agent</button>
  </div>
  <div id="tab-workers">
  <div class="top">
    <div class="stat"><b>__N__</b><span>workers with a live book</span></div>
    <div class="stat"><b class="pos">__SELL__</b><span>sellable (ret/DD ≥ 10, both years +)</span></div>
    <div class="stat"><b class="mono">__MED__</b><span>median return ÷ drawdown</span></div>
    <div class="ctrl">
      <input id="q" placeholder="Search ticker…" oninput="render()">
      <button id="fAll" class="on" onclick="setF('all')">All</button>
      <button id="fSell" onclick="setF('sell')">Sellable only</button>
      <select id="sort" onchange="render()">
        <option value="net_exp">Sort: Net Expectancy</option>
        <option value="total_return">Sort: Return (invested cash)</option>
        <option value="win_rate">Sort: Win Rate</option>
        <option value="max_dd">Sort: Smallest Drawdown</option>
        <option value="ord_MIS">Sort: MIS strategy P&L</option>
        <option value="ord_MIS-Daily">Sort: MIS-Daily strategy P&L</option>
        <option value="ord_NRML">Sort: NRML strategy P&L</option>
        <option value="ord_CNC">Sort: CNC strategy P&L</option>
      </select>
    </div>
  </div>
  <details class="method">
    <summary>How these numbers are calculated</summary>
    <div class="mbody">
      <p><b>One basis: invested cash/day.</b> You deploy a fixed <b>₹1,00,000 cash/day</b> at <b>5× MIS</b> (fixed — the only leverage for a non-F&amp;O intraday short). MIS squares off same-day so the cash <b>recycles daily</b>; returns are therefore <b>additive</b> (sum of daily returns), and the max active capital is one day. Every figure here is <b>Return / drawdown on that deployed cash</b> — no notional, no position-sizing factor. Scale linearly: 2× cash/day → 2× rupees.</p>
      <p><b>Net Expectancy / trade</b> = (Win% × Avg Win) − (Loss% × Avg Loss), after costs + 0.5% slippage. This is the per-trade edge and the tier badge (Elite ≥+3%, Strong ≥+2%, Solid ≥+1%).</p>
      <p><b>Risk is managed by the intraday trail, not leverage.</b> After the 09:15 entry, a per-stock <b>walk-forward-validated trail</b> (tuned on 2025, confirmed on sealed 2026) caps each day's loss — which is why drawdowns are single-digit even at fixed 5×. All numbers are clean out-of-sample 2025–2026; patterns were mined ≤2024.</p>
      <p><b>Total P&L (₹)</b> per strategy = the real rupees it contributed (each trade sized at ₹1.5L); the strategies' P&L sums to the worker's total. Win rate, avg net return/trade (= net expectancy) and profit factor are capital-independent, so they compare CNC / MIS / NRML fairly.</p>
      <p><b>Return is additive</b> — you deploy ₹1,00,000/day at 5× MIS; capital recycles daily, so daily returns are summed (not compounded). <b>Three drawdown lenses, one rupee give-back:</b> ① <b>Account MDD (headline)</b> = the drop from a peak of your growing account balance — what you'd actually feel. ② <b>Fixed-capital DD</b> = the same give-back ÷ the fixed ₹1L you committed (conservative). ③ <b>Worst month</b>. Example: a ₹58k give-back is −14.7% of a grown ₹4.3L account (①) but −57.8% of fixed ₹1L (②). <b>Return ÷ drawdown</b> uses ①. <b>Sellable / Healthy</b> = positive in BOTH 2025 and sealed 2026 AND ret÷DD ≥ 10; tiers Elite ≥25 / Strong ≥15 / Solid ≥10.</p>
    </div>
  </details>
  <div id="grid" class="grid"></div>
  </div>
  <div id="tab-agents" style="display:none">
    <div class="ahero">
      <h2>Specialist Agents</h2>
      <p>A per-stock worker masters one stock. A <b>specialist agent masters one trading style across every stock</b> — it scans the whole market live, takes only the single best setup with virtual capital, then studies its own wins and losses and adjusts <b>only itself</b>. You judge it like a fund: on its track record, never on advice.</p>
    </div>
    <div class="agent">
      <div class="ahead">
        <div class="aav">J</div>
        <div><div class="aname">Agent Jarvis</div><div class="aspec">Breakout specialist</div></div>
        <span class="badge b-watch">In development · track record pending backtest</span>
      </div>
      <div class="astats">
        <div class="astat"><span>CAGR</span><b>—</b></div>
        <div class="astat"><span>Avg monthly return</span><b>—</b></div>
        <div class="astat"><span>Rolling 12-mo</span><b>—</b></div>
        <div class="astat"><span>Max drawdown</span><b>—</b></div>
      </div>
      <p class="note">No numbers are shown until a leak-free backtest earns them — a track record we haven't proven is a track record we won't display.</p>
      <div class="asec"><h4>What Jarvis does</h4>
        <ol>
          <li>Watches the whole market <b>live, during the session</b> — the moment a stock breaks out, not at end-of-day.</li>
          <li>Analyses it in context: when did this stock last break out? what kind (range / trend / volume-driven)? was there a clean pre-move? and — critically — what did history do after breakouts that looked like this: hold, or fail?</li>
          <li>On a day with 1,000 breakouts, it <b>enters exactly one</b> — the highest-conviction setup — with virtual capital.</li>
          <li>Tracks and trails the position by its own rules; flat by close. (Paper by default; real money stays human-gated.)</li>
        </ol></div>
      <div class="asec"><h4>How Jarvis learns — and changes only itself</h4>
        <p>After every trade it runs the same self-review:</p>
        <div class="qs"><span>Why did I win?</span><span>Why did I lose?</span><span>What could improve?</span><span>Which stock behaved differently?</span><span>Change my entry?</span><span>Change my exit?</span></div>
        <p>The answers update <b>its own</b> parameters — which breakout types to trust, how it times entries, how tight it trails — <b>walk-forward and leak-free</b> (it only ever learns from trades already closed). It never touches another agent. Over time it compounds its own lessons and evolves.</p></div>
      <div class="asec"><h4>How you'd rent it</h4>
        <p>Like choosing a fund. You see its CAGR, monthly and rolling returns, and drawdown — plus a live feed of what it traded and what it changed about itself after each trade. We never tell you to buy or sell a stock. The agents compete in the arena; you pick the track record you like.</p></div>
    </div>
    <h3 class="acoming">Coming to the arena</h3>
    <div class="agrid">
      <div class="mini"><div class="aav sm">M</div><div><b>Agent Momentum</b><span>rides sustained trends</span></div></div>
      <div class="mini"><div class="aav sm">R</div><div><b>Agent Reversion</b><span>fades over-extensions</span></div></div>
      <div class="mini"><div class="aav sm">G</div><div><b>Agent Gap</b><span>trades opening gaps</span></div></div>
    </div>
  </div>
  <div id="tab-worlds" style="display:none">
    <div class="ahero">
      <h2>Market Worlds</h2>
      <p>Every agent is one honest hypothesis — <i>WHEN a trigger fires, WHAT happens next, vs the market, after costs.</i> Send it into five market environments and watch where it dominates and where it dies. #1 in a Normal market can be dead-last in a Bear market. Robustness you can <b>see</b>, not a stress-test report you skim. Every number is real, leak-free, net of 0.30% costs — the agent's journal is auto-written from its own evidence and cannot invent a trade.</p>
    </div>
    <div id="wgrid" class="wgrid"></div>
    <div id="wagents"></div>
    <p class="note" style="max-width:900px">Edge = the agent's return minus the same market world's average, per trade. Green = it beats that world; red = it loses to it. The five worlds are slices of the same 2013–2026 history: Normal (uptrend, calm), High-Vol (top-quartile volatility), Bear (index below its 200-day average), Low-Liquidity (bottom-quartile volume), and a held-out 2018–19 slice the agent never "saw."</p>
  </div>
  <div id="tab-build" style="display:none">
    <div class="ahero">
      <h2>Build an Agent</h2>
      <p>Compose your own agent the same way ours are made — pick a <b>trigger</b>, a <b>direction</b>, and a <b>horizon</b>. We instantly show its real, leak-free track record across 13 years and all five Market Worlds, and where it ranks beside our house agents. Every number is a genuine backtest, net of costs — no invented results. When you like it, deploy it into the live paper Arena.</p>
    </div>
    <div class="bform">
      <div class="bfield"><label>Agent name</label><input id="b-name" placeholder="MyAgent" maxlength="18"></div>
      <div class="bfield"><label>When this trigger fires…</label><select id="b-trg"></select></div>
      <div class="bfield"><label>…I go</label><select id="b-dir"><option value="long">Long (buy)</option><option value="short" selected>Short (sell)</option></select></div>
      <div class="bfield"><label>…and hold</label><select id="b-hz"><option value="1" selected>1 day</option><option value="2">2 days</option><option value="3">3 days</option><option value="5">5 days</option></select></div>
    </div>
    <div id="b-out"></div>
  </div>
</div>
<div class="ov" id="ov" onclick="if(event.target.id=='ov')close_()"><div class="panel" id="panel"></div></div>
<script id="agents" type="application/json">__AGENTS__</script>
<script id="grid-data" type="application/json">__GRID__</script>
<script id="grid-opts" type="application/json">__GRIDOPTS__</script>
<script id="data" type="application/json">__DATA__</script>
<script>
const DATA=JSON.parse(document.getElementById('data').textContent);
let F='all';
function tab(t){['workers','agents','worlds','build'].forEach(x=>{
  document.getElementById('tab-'+x).style.display=x==t?'':'none';
  document.getElementById('tab-btn-'+x).classList.toggle('on',x==t);});
 if(t=='worlds'&&!window._wr){window._wr=1;renderWorlds();}
 if(t=='build'&&!window._wb){window._wb=1;initBuilder();}}
const GRID=JSON.parse(document.getElementById('grid-data').textContent||'{}');
const GRIDOPTS=JSON.parse(document.getElementById('grid-opts').textContent||'[]');
function initBuilder(){
 const trg=document.getElementById('b-trg');
 trg.innerHTML=GRIDOPTS.map(o=>`<option value="${o.v}">${o.label}</option>`).join('');
 ['b-trg','b-dir','b-hz','b-name'].forEach(id=>document.getElementById(id).addEventListener('input',renderBuild));
 renderBuild();}
function renderBuild(){
 const key=document.getElementById('b-trg').value+'|'+document.getElementById('b-dir').value+'|'+document.getElementById('b-hz').value;
 const g=GRID[key]; const nm=document.getElementById('b-name').value.trim()||'MyAgent';
 const dir=document.getElementById('b-dir').value, hz=document.getElementById('b-hz').value;
 const out=document.getElementById('b-out');
 if(!g||!g.overall){out.innerHTML='<div class="aworld"><p class="note">Not enough occurrences for this combination to be statistically meaningful — pick a looser trigger or a different horizon.</p></div>';return;}
 const oe=g.overall, verb=dir=='short'?'short':'buy';
 const wins=WORLDS.map(w=>{const c=g.worlds[w];const e=c?c.edge:null;
  // rank the user's agent among the 3 house agents + itself in this world
  const field=AGENTS.map(a=>a.market_worlds[w]?a.market_worlds[w].edge:-9).concat(e==null?[]:[e]).sort((x,y)=>y-x);
  const rk=e==null?null:field.indexOf(e)+1;
  return {w,e,rk};});
 const posw=wins.filter(x=>x.e>0).length;
 const badges=wins.map(x=>`<span class="wbadge" style="background:${wbg(x.e)};color:${wtx(x.e)}">${x.w} · ${x.rk?'#'+x.rk+' ':''}${fe(x.e)}</span>`).join('');
 out.innerHTML=`<div class="aworld">
   <div class="ahead2"><div class="aav">${nm[0].toUpperCase()}</div>
    <div><div class="aname">${nm}</div><div class="aspec">${g.label} · ${dir} · T+${hz}</div></div>
    <span class="badge ${oe.edge>0?'b-strong':'b-watch'}">${oe.edge>0?'has an edge':'no edge'}</span></div>
   <div class="bkpis">
     <div class="bk"><span>Occurrences</span><b>${oe.n.toLocaleString()}</b></div>
     <div class="bk"><span>Win rate</span><b>${oe.win}%</b></div>
     <div class="bk"><span>Per trade</span><b class="${oe.expct>=0?'pos':'neg'}">${oe.expct>=0?'+':''}${oe.expct}%</b></div>
     <div class="bk"><span>Edge vs market</span><b class="${oe.edge>=0?'pos':'neg'}">${oe.edge>=0?'+':''}${oe.edge}%</b></div>
     <div class="bk"><span>Profit factor</span><b>${oe.pf==null?'∞':oe.pf}</b></div>
   </div>
   <div class="wbadges" style="margin-top:12px">${badges}</div>
   <div class="jrnl"><b>${nm}:</b> "When ${g.label.toLowerCase()} fires, I ${verb} and hold ${hz} day${hz>1?'s':''}. Over ${oe.n.toLocaleString()} times in history I won ${oe.win}% at ${oe.expct>=0?'+':''}${oe.expct}%/trade — ${oe.edge>=0?'beating':'losing to'} the market by ${Math.abs(oe.edge)}%. I beat ${posw} of 5 worlds${posw>=3?', I can hold my own in this arena.':'; I struggle when the regime turns.'}"</div>
   <div style="margin-top:14px;display:flex;gap:10px;align-items:center;flex-wrap:wrap">
     <button class="dl" onclick="alert('Deploying '+${JSON.stringify(nm)}+' into the live paper Arena — costs 12 tokens. (Token wallet + live paper-trading coming next.)')">⚔ Enter the Arena · 12 tokens</button>
     <span class="note">Deploys ${nm} to trade paper-money live and appear on the leaderboard beside the house agents.</span>
   </div></div>`;}
const AGENTS=JSON.parse(document.getElementById('agents').textContent||'[]');
const WORLDS=["Normal","High-Vol","Bear","Low-Liquidity","Unseen 18-19"];
function wbg(e){if(e==null)return'transparent';const a=Math.min(Math.abs(e)/0.8,1)*0.30+0.05;
  return e>=0?`rgba(26,150,110,${a})`:`rgba(205,68,46,${a})`;}
function wtx(e){return e==null?'var(--mut)':(e>=0?'var(--pos)':'var(--neg)');}
function fe(e){return e==null?'—':(e>=0?'+':'')+e.toFixed(2)+'%';}
function renderWorlds(){
 let h='<table><thead><tr><th>Agent</th>'+WORLDS.map(w=>`<th>${w}</th>`).join('')+'<th>Overall</th></tr></thead><tbody>';
 AGENTS.forEach(a=>{h+=`<tr><td class="ag">${a.name}<small>${a.specialty}</small></td>`;
  WORLDS.forEach(w=>{const cw=a.market_worlds[w];const e=cw?cw.edge:null;
   h+=`<td style="background:${wbg(e)};color:${wtx(e)}">${fe(e)}<span class="rk">${cw&&cw.rank?'#'+cw.rank+' in world':''}</span></td>`;});
  const oe=a.overall_evidence.edge;
  h+=`<td style="background:${wbg(oe)};color:${wtx(oe)}">${fe(oe)}</td></tr>`;});
 h+='</tbody></table>';
 document.getElementById('wgrid').innerHTML=h;
 document.getElementById('wagents').innerHTML=AGENTS.map(a=>{const oe=a.overall_evidence;
  const b=WORLDS.map(w=>{const cw=a.market_worlds[w];const e=cw?cw.edge:null;
   return `<span class="wbadge" style="background:${wbg(e)};color:${wtx(e)}">${w} · ${cw&&cw.rank?'#'+cw.rank+' ':''}${fe(e)}</span>`;}).join('');
  return `<div class="aworld"><div class="ahead2"><div class="aav">${a.name[0]}</div>
    <div><div class="aname">${a.name}</div><div class="aspec">${a.personality} · ${a.specialty} · ${a.direction} T+${a.horizon}</div></div>
    <span class="badge b-solid">${oe.n.toLocaleString()} trades · ${oe.win}% win · ${oe.expct>=0?'+':''}${oe.expct}%/trade</span></div>
   <div class="wbadges">${b}</div><div class="jrnl">${a.journal}</div></div>`;}).join('');}
const rs=n=>(n>=0?'+':'')+n.toFixed(1)+'%', rup=v=>'₹'+(v/100000).toFixed(2)+'L';
const cls=n=>n>=0?'pos':'neg';
const rup2=v=>(v<0?'−₹':'₹')+(Math.abs(v)/100000).toFixed(2)+'L';
const col=(v,t)=>`<span class="${v>=0?'pos':'neg'}">${t}</span>`;
const hd=d=>d<=1?'1 d (intraday/ON)':d+' d';
const sret=p=>p/1500;   // strategy return %: Rs P&L on the strategy's own Rs1.5L virtual account (additive)
const STRAT={
 'CNC':{u:'Own the stock outright — no leverage, no overnight limit. Rides multi-day UP-moves from high-probability setups.',
        s:"When the signal fires at the close, BUY at the next day's open as delivery (CNC). Hold up to the target window (a few days); exit when the target hits or on the time-stop. Nothing to square off."},
 'MIS':{u:'5× leverage, capital-efficient, ZERO overnight risk — always flat by 3:20 PM.',
        s:'Signal at the close → enter at the next open via MIS (long or short). Exit when the intraday target hits, else auto square-off by 3:20 PM the same day. One session, done.'},
 'MIS-Daily':{u:'Captures multi-day DOWN-moves on stocks with NO futures — legally — by re-shorting intraday every day. 5×, and no overnight-gap risk.',
        s:'While the signal persists (~a few days), SHORT at each day’s open via MIS and square off by 3:20 PM. Re-enter the next morning. You are never short overnight, so gaps can’t hurt you. (Avg-hold shown is the campaign span; each order is intraday.)'},
 'NRML':{u:'Hold a leveraged short across days/weeks using stock futures — captures the FULL multi-day down-move, including overnight drift.',
        s:'Signal at the close → SHORT the stock future (NRML) at the next open. Hold to the target or the time-stop (days). Exit when hit.'}};
function stratCard(k){const s=STRAT[k]||{u:'',s:''};
 return `<div class="strat"><div class="sh">${k}</div><div><b>USP:</b> ${s.u}</div><div><b>How you trade it daily:</b> ${s.s}</div></div>`;}
function logTable(log){return `<table><thead><tr><th>Entry</th><th>Exit</th><th>Order</th><th>Dir</th><th>Hold</th><th>Return</th><th>P&L (₹)</th></tr></thead><tbody>${
 log.map(r=>`<tr><td>${r[0]}</td><td>${r[1]}</td><td>${r[2]}</td><td>${r[3]}</td><td>${r[4]}d</td><td>${col(r[5],rs(r[5]))}</td><td>${col(r[6],rup2(r[6]))}</td></tr>`).join('')}</tbody></table>`;}
function dl(sym){const c=DATA.find(x=>x.symbol==sym);
 const csv='entry,exit,order,direction,hold_days,return_pct,pnl_rs\n'+c.log.map(r=>r.join(',')).join('\n');
 const uri='data:text/csv;charset=utf-8,'+encodeURIComponent(csv);
 const a=document.createElement('a');a.href=uri;a.download=sym+'_trades.csv';a.target='_blank';a.rel='noopener';
 document.body.appendChild(a);a.click();a.remove();
 try{if(navigator.clipboard)navigator.clipboard.writeText(csv);}catch(e){}}
function tier(c){if(!c.sustained||c.ret_dd<10)return['b-watch','Watch'];
 if(c.ret_dd>=25)return['b-elite','Elite'];if(c.ret_dd>=15)return['b-strong','Strong'];return['b-solid','Solid'];}
function spark(a){const w=200,h=34,mn=Math.min(...a),mx=Math.max(...a),r=(mx-mn)||1;
 const p=a.map((v,i)=>`${(i/(a.length-1))*w},${h-((v-mn)/r)*(h-4)-2}`).join(' ');
 const up=a[a.length-1]>=a[0];return `<svg class="spk" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none"><polyline fill="none" stroke="${up?'var(--pos)':'var(--neg)'}" stroke-width="1.6" points="${p}"/></svg>`;}
function eqchart(a){if(!a||a.length<2)return '';const w=Math.max(360,a.length*5),h=150,mn=Math.min(...a,1),mx=Math.max(...a),r=(mx-mn)||1;
 const y=v=>h-8-((v-mn)/r)*(h-26);
 const pts=a.map((v,i)=>`${(i/(a.length-1))*w},${y(v)}`).join(' ');
 const area=`0,${h-8} ${pts} ${w},${h-8}`;
 return `<div class="scroll"><svg viewBox="0 0 ${w} ${h}" style="width:100%;max-width:${w}px;height:${h}px"><polygon points="${area}" fill="var(--accentbg)" opacity="0.6"/><line x1="0" y1="${y(1)}" x2="${w}" y2="${y(1)}" stroke="var(--line)" stroke-dasharray="3 3"/><polyline fill="none" stroke="var(--accent)" stroke-width="2" points="${pts}"/></svg></div>`;}
function mbars(bm){if(!bm||!bm.length)return '';const n=bm.length,cw=Math.max(340,n*24),h=120,mid=h/2;
 const mx=Math.max(...bm.map(x=>Math.abs(x.ret)),1),bw=cw/n*0.66;
 const bars=bm.map((x,i)=>{const bh=Math.abs(x.ret)/mx*(mid-14),y=x.ret>=0?mid-bh:mid;
  return `<rect x="${i*cw/n+(cw/n-bw)/2}" y="${y}" width="${bw}" height="${bh}" rx="1" fill="${x.ret>=0?'var(--pos)':'var(--neg)'}"><title>${x.m}: ${rs(x.ret)}</title></rect>`;}).join('');
 const lab=bm.map((x,i)=>i%3==0?`<text x="${i*cw/n+cw/n/2}" y="${h-2}" font-size="8" fill="var(--mut)" text-anchor="middle">${x.m.slice(2)}</text>`:'').join('');
 return `<div class="scroll"><svg viewBox="0 0 ${cw} ${h}" style="width:100%;max-width:${cw}px;height:${h}px"><line x1="0" y1="${mid}" x2="${cw}" y2="${mid}" stroke="var(--line)"/>${bars}${lab}</svg></div>`;}
function render(){const q=document.getElementById('q').value.toUpperCase();
 const s=document.getElementById('sort').value;
 let d=DATA.filter(c=>c.symbol.includes(q));
 if(F=='sell')d=d.filter(c=>c.sustained&&c.ret_dd>=10);
 if(s.startsWith('ord_')){const o=s.slice(4);
   d=d.slice().filter(c=>c.by_order.some(x=>x.k==o)).sort((a,b)=>{
     const ga=a.by_order.find(x=>x.k==o),gb=b.by_order.find(x=>x.k==o);return gb.pnl-ga.pnl;});}
 else d=d.slice().sort((a,b)=>s=='max_dd'?b.max_dd-a.max_dd:b[s]-a[s]);
 document.getElementById('grid').innerHTML=d.map((c,i)=>{const[bc,bl]=tier(c);
  return `<div class="card" onclick="open_(${DATA.indexOf(c)})">
   <div style="display:flex;align-items:center;gap:8px"><span class="tick">${c.symbol}</span><span class="badge ${bc}">${bl}</span>${c.method&&c.method!='blend'?`<span class="badge b-port">${c.method=='rolling'?'Rolling':'Portfolio'}</span>`:''}</div>
   <div class="big ${cls(c.total_return)} mono">${rs(c.total_return)}</div>
   <div class="row"><span>Net expectancy</span><b class="mono ${cls(c.net_exp)}">${rs(c.net_exp)}/trade</b></div>
   <div class="row"><span>Win rate</span><b class="mono">${c.win_rate}%</b></div>
   <div class="row"><span>Max drawdown</span><b class="mono neg">${c.max_dd}%</b></div>
   ${spark(c.spark)}</div>`;}).join('')||'<p class="sub">No workers match.</p>';}
function tbl(rows,cols){return `<table><thead><tr>${cols.map(c=>`<th>${c[0]}</th>`).join('')}</tr></thead><tbody>${
 rows.map(r=>`<tr>${cols.map(c=>`<td>${c[1](r)}</td>`).join('')}</tr>`).join('')}</tbody></table>`;}
function open_(i){const c=DATA[i];const[bc,bl]=tier(c);
 const kv=(l,v,cl='')=>`<div class="kv"><div class="l">${l}</div><div class="v mono ${cl}">${v}</div></div>`;
 document.getElementById('panel').innerHTML=`
  <div class="phead"><h2>${c.symbol}</h2><span class="badge ${bc}">${bl}</span>
    <span class="badge b-solid">${c.status}</span>${c.method&&c.method!='blend'?`<span class="badge b-port">${c.method=='rolling'?'Rolling · adaptive monthly':('Portfolio · '+(c.n_subagents||'')+' sub-agents')}</span>`:''}<button class="x" onclick="close_()">✕ Close</button></div>
  <p class="sub">Deploy <b>₹1,00,000 cash/day at 5× MIS</b> · capital recycles daily (returns additive) · net of costs + 0.5% slippage · per-stock intraday trail, walk-forward validated · clean out-of-sample 2025–2026.</p>
  <div class="kgrid">
   ${kv('Value (₹1L/day →)',rup(c.current_value))}${kv('Return on invested cash',rs(c.total_return),cls(c.total_return))}
   ${kv('30-Day Return',rs(c.ret_30d),cls(c.ret_30d))}${kv('90-Day Return',rs(c.ret_90d),cls(c.ret_90d))}
   ${kv('Return ÷ drawdown',c.ret_dd)}${kv('Net Expectancy',rs(c.net_exp)+'/day',cls(c.net_exp))}
   ${kv('Trades / Month',c.tpm)}${kv('Avg Hold',c.avg_hold+' d')}</div>
  <h3>Drawdown — three lenses on the same rupee give-back</h3>
  <div class="kgrid">
   ${kv('① Account MDD (headline)',c.max_dd+'%','neg')}${kv('② Fixed-capital DD',c.dd_fixed+'%','neg')}${kv('③ Worst month',c.worst_month+'%','neg')}</div>
  <p class="note" style="margin:2px 0 8px">① drop from peak of your <b>growing account balance</b> (what you'd feel). ② the same give-back ÷ your <b>fixed ₹1L</b> committed (conservative). ③ worst single month. Same ₹, three denominators.</p>
  <h3>Year by year</h3>
  <div class="scroll">${tbl(c.by_year_detail,[['Year',r=>r.y],['Days',r=>r.n],['Return',r=>col(r.ret,rs(r.ret))],['Win rate',r=>r.win+'%'],['Best month',r=>col(r.best_month,rs(r.best_month))],['Worst month',r=>col(r.worst_month,rs(r.worst_month))]])}</div>
  <h3>Quality</h3>
  <div class="kgrid">
   ${kv('Win Rate',c.win_rate+'%')}${kv('Profit Factor',c.profit_factor)}${kv('Ret ÷ DD (Calmar)',c.ret_dd)}
   ${kv('Active Patterns',c.active_patterns)}${kv('Confidence',c.confidence)}${kv('Status',c.status)}</div>
  <h3>Order-type performance</h3>
  <p class="sub2">Each order type on the <b>invested-cash</b> basis (₹1L/day at 5× MIS, recycled daily).</p>
  <div class="scroll">${tbl(c.by_order,[['Order',r=>r.k],['Trades',r=>r.n],['Win rate',r=>r.win+'%'],['Avg net return / trade',r=>col(r.avg,rs(r.avg))],['Profit factor',r=>r.pf],['Avg hold',r=>hd(r.hold)],['P&L on ₹1L/day (₹)',r=>col(r.pnl,rup2(r.pnl))],['30D',r=>col(r.r30,rs(r.r30))],['90D',r=>col(r.r90,rs(r.r90))]])}</div>
  <h3>Direction performance</h3>
  <div class="scroll">${tbl(c.by_dir,[['Direction',r=>r.k],['Trades',r=>r.n],['Win rate',r=>r.win+'%'],['Avg net return / trade',r=>col(r.avg,rs(r.avg))],['Profit factor',r=>r.pf],['P&L on ₹1L/day (₹)',r=>col(r.pnl,rup2(r.pnl))],['30D',r=>col(r.r30,rs(r.r30))],['90D',r=>col(r.r90,rs(r.r90))]])}</div>
  <p class="note">Everything is on the <b>invested cash/day</b> basis (₹1L deployed at 5× MIS, recycled daily). Win rate, avg net return/trade (= net expectancy) and profit factor are capital-independent; <b>P&L (₹)</b> is the rupees on ₹1L/day (scale linearly with your cash/day). <b>MIS-Daily</b> = a non-F&O short executed as an intraday MIS short re-entered each day and squared off same-day; its "avg hold" is the campaign span in days. Research/paper figures — real-money is human-gated.</p>
  <h3>How you trade each strategy — USP &amp; daily SOP</h3>
  ${c.by_order.map(o=>stratCard(o.k)).join('')}
  <h3>Continuous account equity — ₹1,00,000 → ${rup(c.current_value)} (never reset)</h3>
  ${eqchart(c.eqcurve)}
  <p class="note" style="margin:2px 0 10px">Your actual running balance over 2025–26 (dashed line = ₹1L start). The <b>Account MDD ${c.max_dd}%</b> is the deepest drop from a peak of this curve.</p>
  <h3>Monthly normalized scorecard — each month reset to a fresh ₹1L</h3>
  <p class="sub2">Positive months <b>${c.consistency.pct_pos_months}%</b> · best <span class="pos">+${c.consistency.best_month}%</span> · worst <span class="neg">${c.consistency.worst_month}%</span></p>
  ${mbars(c.by_month)}
  <div class="scroll">${tbl(c.by_month,[['Month',r=>r.m],['Start',r=>'₹1L'],['Days',r=>r.n],['Win rate',r=>r.win+'%'],['Net P&L',r=>col(r.pnl,rup2(r.pnl))],['Monthly return',r=>col(r.ret,rs(r.ret))],['Worst intramonth DD',r=>col(r.dd,rs(r.dd))]])}</div>
  <p class="note" style="margin-top:6px">Months compare on equal footing (each starts fresh at ₹1L). <b>Worst intramonth DD</b> = deepest give-back <i>inside</i> that month — the per-month risk the equity curve smooths away.</p>
  <h3>Trade log <span style="font-weight:400;color:var(--mut);font-size:12px">(${c.trades} trades)</span></h3>
  ${c.log&&c.log.length?`<button class="dl" onclick="dl('${c.symbol}')">⬇ Download full trade log (CSV)</button>
  <div class="scroll" style="max-height:280px">${logTable(c.log)}</div>`
  :`<p class="sub2">Full trade log is provided for the sellable cohort (sustained positive expectancy). This worker is below that bar.</p>`}`;
 document.getElementById('ov').classList.add('on');}
function close_(){document.getElementById('ov').classList.remove('on');}
function setF(f){F=f;document.getElementById('fAll').classList.toggle('on',f=='all');
 document.getElementById('fSell').classList.toggle('on',f=='sell');render();}
document.addEventListener('keydown',e=>{if(e.key=='Escape')close_();});
render();
</script>"""


if __name__ == "__main__":
    main()
