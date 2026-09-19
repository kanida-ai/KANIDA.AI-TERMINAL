"""
SPS_V3 FINALIZE — turn per-stock champions into a vetted, tradeable PORTFOLIO.
Steps:
  1. Load all per-stock champions (train&val t>=2.5) from scan_results.db.
  2. Benjamini-Hochberg FDR across their validation p-values (controls the ~130x
     multiple-testing) at q=0.10.
  3. VAULT-confirm each FDR survivor on sealed 2026 (the one authorised unlock);
     keep those still net-positive out-of-sample.
  4. Correlation-prune (greedy, on vault daily returns) into a low-correlation basket.
  5. Portfolio backtest on the vault: avg %/day at 1x & 5x, % positive days, drawdown.
Writes tradeable_list.md + portfolio.json.

Run:  python finalize.py
"""
import json, sqlite3
from pathlib import Path
from datetime import datetime
import numpy as np
from scipy import stats
import lab, continuous
from lab import load_min, load_atr, day_pack, TRAIN, VAULT_START

HERE = Path(__file__).resolve().parent
VAULT = (VAULT_START, "2026-12-31")
FDR_Q = 0.10
CORR_MAX = 0.5


def sim_series(sym, days, atrmap, hyp, thr):
    """champion daily net-return series {date: net_1x} (<=1 trade/day)."""
    trig = continuous.TRIGGERS[hyp["trigger"]]; rt = continuous.COST["MIS"]; slip = continuous.ENTRY_SLIP
    out = {}
    for d, day in days.items():
        atr_pct, prevclose = atrmap.get(d, (np.nan, np.nan))
        if prevclose is None or (isinstance(prevclose, float) and np.isnan(prevclose)):
            continue
        r = trig(day)
        if r is None:
            continue
        eoff, side, level = r
        f = continuous.fact_values(day, atr_pct, prevclose, eoff)
        if not all(continuous.FILTERS[fl](f, thr, side) for fl in hyp["filters"]):
            continue
        entry = level * (1 + slip) if side == "long" else level * (1 - slip)
        tgt = entry * (1 + hyp["target"]) if side == "long" else entry * (1 - hyp["target"])
        stp = (entry * (1 - hyp["stop"]) if side == "long" else entry * (1 + hyp["stop"])) if hyp["stop"] else None
        h_, l_, cf, last = day["h"], day["l"], day["cf"], day["last"]
        exitp = None
        for t in range(eoff + 1, last + 1):
            if np.isnan(h_[t]):
                continue
            if side == "long":
                if stp is not None and l_[t] <= stp:
                    exitp = stp; break
                if h_[t] >= tgt:
                    exitp = tgt; break
            else:
                if stp is not None and h_[t] >= stp:
                    exitp = stp; break
                if l_[t] <= tgt:
                    exitp = tgt; break
        if exitp is None:
            exitp = cf[last]
        g = (exitp - entry) / entry if side == "long" else (entry - exitp) / entry
        out[str(d)] = g - rt
    return out


def main():
    con = sqlite3.connect(str(HERE / "scan_results.db"))
    champs = []
    for sym, spec, thr, va in con.execute(
            "SELECT sym,spec,thr,va_json FROM champ WHERE has_edge=1"):
        v = json.loads(va)
        champs.append({"sym": sym, "spec": json.loads(spec), "thr": thr, "val": v})
    con.close()
    if not champs:
        print("no champions"); return
    # 2. BH-FDR on validation one-sided p-values
    for c in champs:
        c["p"] = float(stats.norm.sf(c["val"]["tstat"]))
    champs.sort(key=lambda c: c["p"])
    m = len(champs); fdr_keep = []
    for i, c in enumerate(champs, 1):
        if c["p"] <= FDR_Q * i / m:
            fdr_keep = champs[:i]      # BH: largest i satisfying condition
    print(f"champions (train&val t>=2.5): {m} · BH-FDR(q={FDR_Q}) survivors: {len(fdr_keep)}")

    # 3. vault-confirm
    confirmed = []
    for c in fdr_keep:
        atrmap = load_atr(c["sym"])
        vault = day_pack(load_min(c["sym"], *VAULT, unlock=True))   # authorised seal break
        ser = sim_series(c["sym"], vault, atrmap, c["spec"], c["thr"])
        a = np.array(list(ser.values()))
        if len(a) < 15 or a.mean() <= 0:
            continue
        c["vault"] = {"trades": int(len(a)), "net_1x": float(a.mean()), "net_5x": float(5 * a.mean()),
                      "win": float((a > 0).mean()), "tstat": float(a.mean() / (a.std() + 1e-12) * np.sqrt(len(a)))}
        c["vser"] = ser
        confirmed.append(c)
    confirmed.sort(key=lambda c: -c["vault"]["net_1x"])
    print(f"vault-confirmed (net>0 OOS): {len(confirmed)}")

    # 4. correlation-prune on vault daily returns (greedy by vault net desc)
    all_dates = sorted({d for c in confirmed for d in c["vser"]})
    def vec(c): return np.array([c["vser"].get(d, np.nan) for d in all_dates])
    selected = []
    for c in confirmed:
        ok = True
        for s in selected:
            x, y = vec(c), vec(s); mask = ~np.isnan(x) & ~np.isnan(y)
            if mask.sum() > 20:
                r = np.corrcoef(x[mask], y[mask])[0, 1]
                if r > CORR_MAX:
                    ok = False; break
        if ok:
            selected.append(c)

    # 5. portfolio backtest on vault (equal-weight across stocks trading each day)
    def port_daily(members):
        rows = []
        for d in all_dates:
            day_rets = [c["vser"][d] for c in members if d in c["vser"]]
            if day_rets:
                rows.append(np.mean(day_rets))
        return np.array(rows)
    pr = port_daily(selected)
    eq = np.cumsum(pr); dd = eq - np.maximum.accumulate(eq)
    port = {"stocks": len(selected), "trading_days": int(len(pr)),
            "avg_per_day_1x": float(pr.mean()), "avg_per_day_5x": float(5 * pr.mean()),
            "pos_day_rate": float((pr > 0).mean()), "sharpe_1x": float(pr.mean() / (pr.std() + 1e-12)),
            "max_dd_1x": float(dd.min()), "max_dd_5x": float(5 * dd.min())}

    # report
    L = ["# SPS_V3 — Tradeable Portfolio (vault-confirmed)",
         f"_generated {datetime.now():%Y-%m-%d %H:%M} · universe {m} edge-candidates · "
         f"BH-FDR q={FDR_Q} → {len(fdr_keep)} · vault-confirmed {len(confirmed)} · "
         f"low-corr basket (r<{CORR_MAX}) **{len(selected)}**_", "",
         "## Portfolio on sealed 2026 vault",
         f"- stocks: **{port['stocks']}** · trading days {port['trading_days']}",
         f"- **avg/day: 1x {port['avg_per_day_1x']*100:+.3f}% · 5x {port['avg_per_day_5x']*100:+.3f}%**  "
         f"(goal +1.000%/day)",
         f"- positive-day rate {port['pos_day_rate']*100:.1f}% · Sharpe/day(1x) {port['sharpe_1x']:.3f}",
         f"- max drawdown: 1x {port['max_dd_1x']*100:.2f}% · 5x {port['max_dd_5x']*100:.2f}%",
         "",
         "## The tradeable list (low-correlation basket)",
         "| # | stock | setup | tgt/stop | vault trades | vault win | vault net 1x | vault net 5x |",
         "|--|--|--|--|--|--|--|--|"]
    for i, c in enumerate(selected, 1):
        s = c["spec"]; v = c["vault"]
        L.append(f"| {i} | {c['sym']} | {s['trigger']} {','.join(s['filters']) or '-'} | "
                 f"{s['target']*100:.1f}%/{('%.1f%%'%(s['stop']*100)) if s['stop'] else 'none'} | "
                 f"{v['trades']} | {v['win']*100:.0f}% | {v['net_1x']*100:+.3f}% | {v['net_5x']*100:+.3f}% |")
    L += ["", "## Vault-confirmed but correlation-pruned (redundant with the above)",
          ", ".join(c["sym"] for c in confirmed if c not in selected) or "(none)"]
    (HERE / "tradeable_list.md").write_text("\n".join(L), encoding="utf-8")
    json.dump({"portfolio": port, "list": [c["sym"] for c in selected]},
              open(HERE / "portfolio.json", "w"), indent=2)
    print("\n".join(L))


if __name__ == "__main__":
    main()
