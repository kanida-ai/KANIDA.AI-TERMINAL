"""FLOW WATERFALL-TO-100% analysis.

Every number is computed from the ACTUAL v1 paper trades (2026-07-08 replay + 2026-07-09
live, current config) joined with the 1-min raw order-flow. NO invented numbers.

Answers: (1) 10 traits of winning trades, (2) 5 failure causes, (3) a cumulative
waterfall from current WR toward best-achievable WR by sequentially removing the biggest
entry-knowable failure patterns, (4) per-symbol waterfall, (5) trailing / missed-potential
analysis (did early-exited trades keep moving our way?), (6) hard-filters vs ranking-boosts,
(7) stock-level vs portfolio-level trailing.

Win = a trade that closed with gross pnl_rs > 0 (matches the engine's WR reporting).
Only ENTRY-KNOWABLE features can become filters (a rule can't use the outcome).

Out: docs/ops/FLOW_WATERFALL_<stamp>.xlsx + .md
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
sys.path.insert(0, str(ROOT / "scripts"))
import flow_paper_engine as F

DAYS = ["2026-07-08", "2026-07-09"]
STAMP = "2026-07-09"
OUT_X = ROOT / "docs" / "ops" / f"FLOW_WATERFALL_{STAMP}.xlsx"
OUT_M = ROOT / "docs" / "ops" / f"FLOW_WATERFALL_{STAMP}.md"


# ---------- 1. build the trade dataset + raw bars for post-exit lookup ----------
def build():
    rows = []
    raw = {}   # (instrument_key, day) -> sorted list of (bar_time, high, low, close)
    con = F._con(ro=True)
    for d in DAYS:
        mins = F._load_day(con, d)
        for m, of_rows, _ in mins:
            for key, of in of_rows.items():
                raw.setdefault((key, d), []).append((m, of.get("high"), of.get("low"), of.get("close")))
        eng = F.Engine(d, verbose=False, consec=2, variant="v1")
        for m, of, tk in mins:
            eng.on_minute(m, of, tk)
        for t in eng.trades:
            t = dict(t); t["day"] = d; rows.append(t)
    con.close()
    df = pd.DataFrame(rows)
    df["win"] = df["pnl_rs"] > 0
    df["armed"] = df["armed"].astype(bool)
    df["hour"] = df["entry_min"].str[11:13].astype(int)
    df["abs_thrust"] = df["ret_signal"].abs()
    # entry-knowable structural features
    df["counter_atp"] = (((df["side"] == "long") & (df["entry_px"] < df["atp"])) |
                         ((df["side"] == "short") & (df["entry_px"] > df["atp"]))) & df["atp"].notna()
    df["whale_on_side"] = (((df["side"] == "long") & (df["whale_side"] == "bid")) |
                           ((df["side"] == "short") & (df["whale_side"] == "ask")))
    df["book_lean"] = np.where(df["side"] == "long", df["buy_imb"], 1 - df["buy_imb"])   # >0.5 = book agrees
    return df, raw


# ---------- 2. post-exit path (trailing / missed-potential) ----------
def post_exit(row, raw):
    bars = raw.get((row["instrument_key"], row["day"]))
    if not bars:
        return np.nan, np.nan
    after = [b for b in bars if b[0] > row["exit_min"] and b[1] is not None and b[2] is not None]
    if not after:
        return 0.0, 0.0
    hi = max(b[1] for b in after); lo = min(b[2] for b in after); ex = row["exit_px"]
    if row["side"] == "long":
        return (hi - ex) / ex * 100, (ex - lo) / ex * 100      # (favorable-after, adverse-after)
    return (ex - lo) / ex * 100, (hi - ex) / ex * 100


def wr(mask_df):
    n = len(mask_df)
    return (mask_df["win"].sum() / n * 100) if n else np.nan


# ---------- 3. trait / feature separation (winners vs losers) ----------
def feature_table(df):
    W, L = df[df.win], df[~df.win]
    n_w = len(W)
    feats = []

    def add(name, mask, why, rtype):
        sub = df[mask]
        if len(sub) < 15:
            return
        share = (W[mask.loc[W.index]].shape[0] / n_w * 100) if n_w else 0
        feats.append(dict(trait=name, wr_with=round(wr(sub), 1), wr_without=round(wr(df[~mask]), 1),
                          n=len(sub), pct_of_wins=round(share, 1), lift=round(wr(sub) - wr(df[~mask]), 1),
                          why=why, rule_type=rtype))

    med = df["abs_thrust"].median()
    add("Book strongly agrees (lean>0.55)", df["book_lean"] > 0.55, "resting book backs the push", "boost")
    add("ATP-aligned entry (not counter-ATP)", ~df["counter_atp"], "trading with fair-value location", "hard-filter")
    add("Whale on the aggressing side", df["whale_on_side"], "few big orders lifting, not a crowd", "boost")
    add("High conviction (>=2)", df["conviction"] >= 2, "book+whale/tick agree on direction", "boost")
    add("Strong thrust (>median)", df["abs_thrust"] >= med, "bigger 1-min impulse", "boost")
    add("Morning entry (<12h)", df["hour"] < 12, "trend intact, pre-chop", "boost")
    add("Long side", df["side"] == "long", "context/regime dependent", "context")
    add("Cash (not FUT)", df["segment"] == "CASH", "tighter spreads / cleaner fills", "boost")
    add("Tick buy-confirmed", df["tick_buy_pct"].notna() &
        (((df["side"] == "long") & (df["tick_buy_pct"] >= 55)) |
         ((df["side"] == "short") & (df["tick_buy_pct"] <= 45))), "real aggressor agrees", "boost")
    add("Book lean + whale together", (df["book_lean"] > 0.55) & df["whale_on_side"],
        "structure + big-player confluence", "boost")
    ft = pd.DataFrame(feats).sort_values("lift", ascending=False)
    return ft


# ---------- 4. greedy waterfall (remove biggest entry-knowable failure patterns) ----------
def waterfall(df):
    cands = {
        "Counter-ATP entry (long below / short above VWAP)": df["counter_atp"],
        "Low conviction (=1, no whale/tick confirm)": df["conviction"] <= 1,
        "Weak book lean (<0.52 agreement)": df["book_lean"] < 0.52,
        "No whale on aggressing side": ~df["whale_on_side"],
        "Weak thrust (< median impulse)": df["abs_thrust"] < df["abs_thrust"].median(),
        "Afternoon entry (>=13h, chop)": df["hour"] >= 13,
        "Futures (wider spread)": df["segment"] == "FUT",
    }
    steps = []
    surv = df.copy(); used = set()
    cur = wr(surv)
    steps.append(dict(step="0 — current (all trades)", wr_before=round(cur, 1), gap=round(100 - cur, 1),
                      root_cause="—", fix="—", wr_after=round(cur, 1), gap_after=round(100 - cur, 1),
                      removed=0, winners_lost=0, losers_removed=0, scalable="—"))
    for _ in range(5):
        best = None
        for name, pred in cands.items():
            if name in used:
                continue
            bad = surv[pred.loc[surv.index]]
            keep = surv[~pred.loc[surv.index]]
            if len(bad) < 10 or len(keep) < 20:
                continue
            gain = wr(keep) - wr(surv)
            if best is None or gain > best[1]:
                best = (name, gain, bad, keep, pred)
        if best is None or best[1] <= 0.05:
            break
        name, gain, bad, keep, pred = best
        used.add(name)
        # scalability: touches how many symbols + consistent both days?
        syms = bad["symbol"].nunique()
        d1 = df[pred & (df.day == DAYS[0])]; d2 = df[pred & (df.day == DAYS[1])]
        consistent = (wr(df[~pred & (df.day == DAYS[0])]) >= wr(df[pred & (df.day == DAYS[0])])) and \
                     (wr(df[~pred & (df.day == DAYS[1])]) >= wr(df[pred & (df.day == DAYS[1])])) \
                     if len(d1) and len(d2) else False
        scalable = f"scalable ({syms} symbols, both-day consistent)" if (syms >= 20 and consistent) \
            else f"partial ({syms} symbols)" if syms >= 10 else "symbol-specific"
        before = wr(surv)
        steps.append(dict(step=f"{len(steps)} — filter", wr_before=round(before, 1), gap=round(100 - before, 1),
                          root_cause=name, fix=f"drop trades matching: {name}",
                          wr_after=round(wr(keep), 1), gap_after=round(100 - wr(keep), 1),
                          removed=len(bad), winners_lost=int(bad["win"].sum()),
                          losers_removed=int((~bad["win"]).sum()), scalable=scalable))
        surv = keep
    return pd.DataFrame(steps), surv


# ---------- 5. per-symbol waterfall ----------
def per_symbol(df):
    def two_issues(sub):
        losers = sub[~sub.win]
        issues = []
        for name, pred in [("counter-ATP", losers["counter_atp"]),
                           ("low-conv", losers["conviction"] <= 1),
                           ("weak-lean", losers["book_lean"] < 0.52),
                           ("no-whale", ~losers["whale_on_side"]),
                           ("weak-thrust", losers["abs_thrust"] < df["abs_thrust"].median()),
                           ("afternoon", losers["hour"] >= 13)]:
            c = int(pred.sum())
            if c:
                issues.append((name, c))
        issues.sort(key=lambda x: -x[1])
        return issues

    rows = []
    for sym, sub in df.groupby("symbol"):
        n = len(sub); w = int(sub.win.sum()); l = n - w
        cur = w / n * 100
        iss = two_issues(sub)
        # WR after removing each issue's losing trades (best achievable if that pattern were filtered)
        def wr_after(issue_pred):
            drop = sub[~sub.win & issue_pred]
            keep = sub.drop(drop.index)
            return (keep.win.sum() / len(keep) * 100) if len(keep) else 100.0
        pmap = {"counter-ATP": sub["counter_atp"], "low-conv": sub["conviction"] <= 1,
                "weak-lean": sub["book_lean"] < 0.52, "no-whale": ~sub["whale_on_side"],
                "weak-thrust": sub["abs_thrust"] < df["abs_thrust"].median(), "afternoon": sub["hour"] >= 13}
        i1 = iss[0][0] if iss else "—"; i2 = iss[1][0] if len(iss) > 1 else "—"
        wr1 = round(wr_after(pmap[i1]), 0) if i1 != "—" else round(cur, 0)
        # after both
        if i1 != "—" and i2 != "—":
            drop = sub[~sub.win & (pmap[i1] | pmap[i2])]; keep = sub.drop(drop.index)
            wr2 = round((keep.win.sum() / len(keep) * 100) if len(keep) else 100.0, 0)
        else:
            wr2 = wr1
        rows.append(dict(symbol=sym, n=n, WR=round(cur, 1), wins=w, losses=l, gap=round(100 - cur, 1),
                         issue1=i1, fix1=f"filter {i1}", WR_after_fix1=wr1,
                         issue2=i2, fix2=f"filter {i2}", WR_after_fix2=wr2, best_WR=wr2))
    return pd.DataFrame(rows).sort_values(["losses", "n"], ascending=[False, False])


# ---------- 6. trailing / missed-potential ----------
def trailing(df, raw):
    fav, adv = zip(*[post_exit(r, raw) for _, r in df.iterrows()])
    df = df.copy(); df["post_fav"] = fav; df["post_adv"] = adv
    out = {}
    # winners exited by trail/EOD: how much MORE did they run after we left?
    tw = df[(df.win) & (df.exit_reason.isin(["trail_stop", "EOD"]))]
    out["trail_winner_n"] = len(tw)
    out["avg_missed_fav_pct"] = round(tw["post_fav"].mean(), 3)
    out["missed_rs"] = round((tw["post_fav"] / 100 * tw["exit_px"] * tw["qty"]).sum(), 0)
    out["pct_winners_kept_running"] = round((tw["post_fav"] > 0.3).mean() * 100, 1)
    # losers stopped out: did they recover (stop too tight) or keep going (justified)?
    sl = df[(~df.win) & (df.exit_reason.isin(["hard_stop", "gap_stop"]))]
    out["stopped_loser_n"] = len(sl)
    out["pct_stops_recovered"] = round((sl["post_fav"] > 0.5).mean() * 100, 1)   # went our way after stop
    out["pct_stops_justified"] = round((sl["post_adv"] >= sl["post_fav"]).mean() * 100, 1)
    # stock-level vs portfolio: dispersion of per-symbol mean favorable excursion (MFE)
    mfe = df.groupby("symbol")["mfe_pct"].mean()
    out["mfe_median"] = round(mfe.median(), 2)
    out["mfe_cross_symbol_std"] = round(mfe.std(), 2)
    out["mfe_p10"] = round(mfe.quantile(0.1), 2); out["mfe_p90"] = round(mfe.quantile(0.9), 2)
    return out, df


def main():
    df, raw = build()
    n = len(df); base_wr = wr(df)
    print(f"[waterfall] {n} v1 trades across {DAYS} | overall WR {base_wr:.1f}% "
          f"({int(df.win.sum())}W / {int((~df.win).sum())}L)", flush=True)

    ft = feature_table(df)
    wf, surv = waterfall(df)
    ps = per_symbol(df)
    tr, dfx = trailing(df, raw)

    # symbol WR distribution / 100% winners
    g = df.groupby("symbol").agg(n=("win", "size"), wins=("win", "sum"))
    g["WR"] = g.wins / g.n * 100
    hundo = g[g.WR == 100]
    print(f"[waterfall] symbols: {len(g)} | 100%-WR symbols: {len(hundo)} "
          f"(of which {int((hundo.n >= 3).sum())} have >=3 trades) | median trades/symbol {int(g.n.median())}")
    print("\n=== TOP WINNER TRAITS (entry-knowable) ===")
    print(ft.to_string(index=False))
    print("\n=== WATERFALL TO BEST-ACHIEVABLE WR ===")
    print(wf[["step", "wr_before", "root_cause", "wr_after", "gap_after", "removed",
              "winners_lost", "losers_removed", "scalable"]].to_string(index=False))
    print(f"\n=== TRAILING / MISSED POTENTIAL ===\n{tr}")

    # write excel
    with pd.ExcelWriter(OUT_X, engine="openpyxl") as w:
        ft.to_excel(w, "Winner_traits", index=False)
        wf.to_excel(w, "Waterfall_to_100", index=False)
        ps.to_excel(w, "Symbol_waterfall", index=False)
        g.reset_index().sort_values(["WR", "n"], ascending=[False, False]).to_excel(w, "Per_symbol_WR", index=False)
        pd.DataFrame([tr]).T.rename(columns={0: "value"}).to_excel(w, "Trailing_analysis")
        dfx.to_excel(w, "All_trades", index=False)
    print(f"\n[waterfall] -> {OUT_X}")
    write_md(df, base_wr, ft, wf, ps, g, hundo, tr, surv)
    print(f"[waterfall] -> {OUT_M}")


def write_md(df, base_wr, ft, wf, ps, g, hundo, tr, surv):
    L = []
    L.append(f"# Flow Paper-Trade — Waterfall to 100% WR ({STAMP})\n")
    L.append(f"**Dataset:** {len(df)} v1 trades (config: hard -2.5 / arm +1.3 / floor +0.9 / give 0.6), "
             f"across {DAYS[0]} (trend day) + {DAYS[1]} (chop day). Win = gross pnl>0.\n")
    L.append(f"**Overall WR: {base_wr:.1f}%** ({int(df.win.sum())}W / {int((~df.win).sum())}L). "
             f"Best-achievable after the waterfall filters: **{wf.iloc[-1]['wr_after']:.1f}%**.\n")
    L.append(f"> Honesty: {len(g)} symbols, median {int(g.n.median())} trades/symbol — per-symbol WR is thin; "
             f"the robust signal is the {len(df)}-trade pooled/pattern level. Only {int((hundo.n>=3).sum())} "
             f"of {len(hundo)} '100%' symbols have >=3 trades.\n")
    L.append("## 10 things winning trades did right (entry-knowable → the constitution)\n")
    L.append(ft.to_markdown(index=False))
    L.append("\n## Waterfall to best-achievable WR\n")
    L.append(wf.to_markdown(index=False))
    L.append("\n## Per-symbol waterfall (top by losses)\n")
    L.append(ps.head(30).to_markdown(index=False))
    L.append("\n## Trailing / missed-potential\n")
    for k, v in tr.items():
        L.append(f"- **{k}**: {v}")
    OUT_M.write_text("\n".join(L), encoding="utf-8")


if __name__ == "__main__":
    main()
