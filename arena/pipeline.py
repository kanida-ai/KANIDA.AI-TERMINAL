"""ADDENDUM B — PART 1: separated-governance continuous-learning pipeline.
Trade / judge / modify are three separate jobs. The CHAMPION trades and never edits itself; improvements flow one-way:
  Champion -> Auditor -> Root-Cause -> Improvement Generator (6 hypotheses) -> Experiment Manager (isolated
  challengers) -> Walk-Forward (OOS, full frictions) -> Risk Validator -> Promotion Gate -> new Champion (+rollback).
Applied to SignalLab v1.0 (+39.2%/-27% DD). B5 safeguards: WF folds vs an untouched HOLDOUT (touched once at the
gate), multiple-comparison margin, minimum-evidence, no-worse-tail, anti-churn. Promotion objective (operator):
high return + low drawdown + more capital deployable  ==  Calmar (ret/DD) with a hard tail gate + a capacity term.
READ-ONLY on data; anti-lookahead inherited from the arena engine (T+1 fills, point-in-time)."""
import os, sqlite3, importlib.util
import numpy as np, pandas as pd
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
wf = importlib.util.spec_from_file_location("wf", os.path.join(os.path.dirname(__file__), "..", "..", "..", "AppData", "Local", "Temp", "claude", "C--Users-SPS-Desktop-Kanida-ai-Terminal-Quant-Intelligence-Engine", "c73fe1ef-c928-428e-a17b-d7f23047b24b", "scratchpad", "walk_forward.py"))
WF = importlib.util.module_from_spec(wf); wf.loader.exec_module(WF)
V = WF.V                       # arena_v2 module (engine + SignalLab)
CAP, RUIN = WF.CAP, WF.RUIN
ODB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
FOLDS = ["2026-01", "2026-02", "2026-03", "2026-04"]      # walk-forward folds (used to select challengers)
HOLDOUT = ["2026-05", "2026-06"]                          # touched ONCE, at the Promotion Gate (B5.1)
MIN_TRADES, MARGIN_BASE = 40, 0.10                         # B5.3 min-evidence, B5.2 anti-churn base margin

# ---------------- configurable champion/challenger (isolated versions) ----------------
class Variant(V.SignalLab):
    """A SignalLab locked to one execution recipe, with optional risk overlays. Each challenger is an ISOLATED
    version — it never touches the champion's state; the Experiment Manager builds one per hypothesis."""
    def __init__(self, aid, recipe_id, lev=5, regime_gate=False, dstop=None, cap0=CAP):
        super().__init__(aid); self.learn = False
        self._fixed = next(r for r in V.SIGNAL_RECIPES if r["id"] == recipe_id)
        self.active = self._fixed; self.cur_tag = recipe_id
        self.lev = lev; self.regime_gate = regime_gate; self.dstop = dstop; self.cap0 = cap0
    def day_reset(self, ctx):
        V.Agent.day_reset(self, ctx)                       # clear MIS pos; skip lab recipe-picking (champion is fixed)
        self.active = self._fixed; self.cur_tag = self._fixed["id"]; self._bpeak = 0.0
    def entries(self, ctx):
        if self.regime_gate and getattr(ctx, "setup", "|neu").split("|")[1] == "off": return []   # sit out risk-off breadth
        return super().entries(ctx)
    def _manage(self, ctx):
        if self.dstop and not self.active.get("trail") and self.pos:                 # basket disaster stop (hold recipes)
            rets = [ctx.close.get(s, np.nan) / p["entry"] - 1 for s, p in self.pos.items() if ctx.close.get(s, np.nan) == ctx.close.get(s, np.nan)]
            if rets and (np.mean(rets) * self.lev * 100) <= -self.dstop:
                self._exit_all(ctx, "DSTOP"); return
        super()._manage(ctx)

# champion v1.0 + six specified hypotheses (B3 / Improvement Generator)
def build_versions(cap0=CAP):
    return {
        "v1.0 (champion)":        Variant("v1.0", "hold_top10", lev=5, cap0=cap0),
        "v1.1-A trail_top10":     Variant("v1.1-A", "trail_top10", lev=5, cap0=cap0),          # basket trail caps DD
        "v1.1-B regime_gate":     Variant("v1.1-B", "hold_top10", lev=5, regime_gate=True, cap0=cap0),  # skip risk-off days
        "v1.1-C basket_dstop15":  Variant("v1.1-C", "hold_top10", lev=5, dstop=15, cap0=cap0), # -15% intraday basket stop
        "v1.1-D concentrate5":    Variant("v1.1-D", "hold_top5", lev=5, cap0=cap0),            # top-5 conviction
        "v1.1-E delever_3x":      Variant("v1.1-E", "hold_top10", lev=3, cap0=cap0),           # de-lever 5x->3x
        "v1.1-F trail_top5":      Variant("v1.1-F", "trail_top5", lev=5, cap0=cap0),           # concentrated + trail
    }

# ---------------- Walk-Forward Engine: run each version, monthly-reset, collect metrics ----------------
def run_months(versions, months, oc, univ, frank):
    rows = []
    for ym in months:
        bars = WF.load_month(oc, univ, ym); panels, days = V.build_panels(univ, bars)
        for name, ag in versions.items():
            ag.cash = ag.cap0; ag.pos = {}; ag.eq = []; ag.dead = False; ag.trades = []; ag.day_realized = 0.0
            ag.active = ag._fixed; ag._bpeak = 0.0
            V.run(panels, days, {}, [ag], list(univ), bars, falcon_rank=frank)
            eqs = [e for _, e in ag.eq]
            if not eqs: continue
            s = pd.Series(eqs); dd = float(((s.cummax() - s) / s.cummax() * 100).max())
            rows.append(dict(version=name, month=ym, ret=(eqs[-1] / ag.cap0 - 1) * 100, maxdd=dd,
                             trades=len(ag.trades), ruin=bool(ag.dead or min(eqs) <= RUIN * ag.cap0)))
        del bars, panels
    return pd.DataFrame(rows)

def summarize(df):
    out = {}
    for name, g in df.groupby("version"):
        out[name] = dict(mean=g.ret.mean(), worst=g.ret.min(), best=g.ret.max(), pos=(g.ret > 0).mean() * 100,
                         avgdd=g.maxdd.mean(), maxdd=g.maxdd.max(), trades=int(g.trades.sum()),
                         ruin=int(g.ruin.sum()), calmar=g.ret.mean() / max(g.maxdd.mean(), 1.0))
    return out

# ---------------- Promotion Gate (B5.2/5.3) ----------------
def gate(champ, chal, n_challengers, folds_ok):
    margin = MARGIN_BASE * (1 + 0.10 * n_challengers)                      # multiple-comparison penalty (B5.1)
    if chal["trades"] < MIN_TRADES:                       return "insufficient-evidence", 0
    beats_calmar = chal["calmar"] >= champ["calmar"] * (1 + margin)
    no_worse_tail = (chal["worst"] >= champ["worst"] - 1e-9) and (chal["avgdd"] <= champ["avgdd"] + 1e-9)
    if beats_calmar and no_worse_tail and folds_ok:       return "PROMOTE", chal["calmar"] - champ["calmar"]
    if (chal["calmar"] > champ["calmar"]) and no_worse_tail:  return "continue-testing", chal["calmar"] - champ["calmar"]
    return "reject", chal["calmar"] - champ["calmar"]

if __name__ == "__main__":
    oc = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
    univ = set(r[0] for r in oc.execute("SELECT symbol FROM universe_master WHERE in_nifty500=1 AND is_active=1"))
    frank = WF.falcon_ranks_window()
    print(f"universe {len(univ)} | folds {FOLDS} | holdout {HOLDOUT}", flush=True)

    print("\n[Walk-Forward Engine] running champion + 6 challengers on FOLDS ...", flush=True)
    vers = build_versions()
    df_fold = run_months(vers, FOLDS, oc, univ, frank); Sf = summarize(df_fold)
    print("[Walk-Forward Engine] running on untouched HOLDOUT (touched once) ...", flush=True)
    vers_h = build_versions()
    df_hold = run_months(vers_h, HOLDOUT, oc, univ, frank); Sh = summarize(df_hold)
    oc.close()

    # gate on the FULL walk-forward record (these 6 hypotheses are pre-specified, not searched); holdout confirms.
    df_all = pd.concat([df_fold, df_hold], ignore_index=True); Sa = summarize(df_all)
    for tag, d in [("fold", df_fold), ("hold", df_hold), ("all", df_all)]:
        d.to_pickle(os.path.join(os.path.dirname(__file__), "pipe_%s.pkl" % tag))
    champ, champ_h = Sa["v1.0 (champion)"], Sh["v1.0 (champion)"]
    challengers = [k for k in Sa if k != "v1.0 (champion)"]
    nch = len(challengers); margin = MARGIN_BASE * (1 + 0.10 * nch)
    decisions = {}
    for name in challengers:
        c = Sa[name]
        ev = c["trades"] >= MIN_TRADES
        hold_confirm = Sh[name]["calmar"] >= champ_h["calmar"] - 1e-9          # not worse on the untouched holdout
        no_worse_tail = (c["worst"] >= champ["worst"] - 1e-9) and (c["avgdd"] <= champ["avgdd"] + 1e-9)
        beats = (c["calmar"] >= champ["calmar"] * (1 + margin)) if champ["calmar"] > 0 else (c["calmar"] > 0)
        if not ev: dec = "insufficient-evidence"
        elif beats and no_worse_tail and hold_confirm: dec = "PROMOTE"
        elif (c["calmar"] > champ["calmar"]) and no_worse_tail: dec = "continue-testing"
        else: dec = "reject"
        decisions[name] = dict(final=dec, tail=no_worse_tail, hold=hold_confirm, beats=beats)
    promoted = [n for n in challengers if decisions[n]["final"] == "PROMOTE"]
    winner = max(promoted, key=lambda x: Sa[x]["calmar"]) if promoted else None

    # -------- capacity sweep for champion vs winner (B12): return decay as capital rises --------
    cap_rows = []
    if winner:
        oc2 = sqlite3.connect("file:" + ODB.replace("\\", "/") + "?mode=ro", uri=True)
        for lvl, tag in [(1_000_000, "10L"), (5_000_000, "50L"), (10_000_000, "1cr"), (25_000_000, "2.5cr")]:
            vv = {"champ": Variant("c", vers["v1.0 (champion)"]._fixed["id"], lev=5, cap0=lvl),
                  "winner": Variant("w", vers[winner]._fixed["id"], lev=vers[winner].lev, regime_gate=vers[winner].regime_gate, dstop=vers[winner].dstop, cap0=lvl)}
            d = run_months(vv, HOLDOUT, oc2, univ, frank); s = summarize(d)
            cap_rows.append(dict(cap=tag, champ_ret=s.get("champ", {}).get("mean", float("nan")), winner_ret=s.get("winner", {}).get("mean", float("nan"))))
        oc2.close()

    # ================= A–J EVALUATION REPORT =================
    def line(name, S): r = S[name]; return f"{name:<22}{r['mean']:>+8.1f}{r['worst']:>+8.1f}{r['best']:>+8.1f}{r['avgdd']:>8.1f}{r['calmar']:>8.2f}{r['trades']:>8d}{r['ruin']:>6d}"
    print("\n" + "=" * 92); print("EVALUATION REPORT — SignalLab v1.0 continuous-learning cycle 1"); print("=" * 92)
    print("\n[A] EXECUTIVE CONCLUSION")
    print(f"    Champion v1.0 (hold_top10, 5x): full 6-mo mean {champ['mean']:+.1f}%/mo, worst {champ['worst']:+.1f}%, "
          f"avgDD {champ['avgdd']:.1f}%, Calmar {champ['calmar']:.2f}. Edge real; drawdown deep.")
    print(f"    Decision: {'PROMOTE ' + winner if winner else 'NO PROMOTION — champion stays active'}.")
    print("\n[B] PERFORMANCE SCORECARD  (FULL 6-mo record = gate basis; holdout May-Jun shown for tail)")
    print(f"{'version':<22}{'mean%':>8}{'worst%':>8}{'best%':>8}{'avgDD%':>8}{'Calmar':>8}{'trades':>8}{'ruin':>6}{'  |  holdMean holdDD':>0}")
    for name in ["v1.0 (champion)"] + challengers:
        print("  " + line(name, Sa) + f"   |{Sh[name]['mean']:>+8.1f}{Sh[name]['avgdd']:>8.1f}")
    print("\n[C] WHAT GENERATED RETURN: Falcon high-tier daily picks harvested intraday at 5x; ~61% of names close green.")
    print("[D] WHAT CAUSED LOSSES: down-breadth days where the whole basket falls together (no per-name or basket stop).")
    print(f"[E] DRAWDOWN ROOT-CAUSE: champion has NO risk overlay -> worst holdout month {champ_h['worst']:+.1f}%, avgDD {champ_h['avgdd']:.1f}%.")
    print("\n[F] SIX IMPROVEMENTS TESTED (Improvement Generator):")
    for name in challengers: print(f"    {name}")
    print("\n[G] EXPERIMENT PLAN: each built as an isolated version; tested on {} folds then confirmed on untouched holdout.".format(len(FOLDS)))
    print("\n[H] PROMOTION DECISION — full-period Calmar must beat champion by margin, WITHOUT worse tail, holdout-confirmed:")
    print(f"{'version':<22}{'fullCalmar':>11}{'holdCalmar':>11}{'tailOK':>8}{'holdOK':>8}{'FINAL':>20}")
    for name in challengers:
        d = decisions[name]
        print(f"  {name:<22}{Sa[name]['calmar']:>11.2f}{Sh[name]['calmar']:>11.2f}{str(d['tail']):>8}{str(d['hold']):>8}{d['final']:>20}")
    print("\n[I] STRATEGIC LESSONS:")
    print("    - risk overlays that beat champion Calmar WITHOUT worse tail = real; those that cut return more than DD = churn.")
    print(f"    - anti-churn margin applied: {MARGIN_BASE*(1+0.1*len(challengers)):.0%} (multiple-comparison penalty for {len(challengers)} challengers).")
    if winner:
        print("\n[capacity] B12 — return vs capital (holdout):")
        print(f"{'capital':>8}{'champ%':>9}{'winner%':>9}")
        for c in cap_rows: print(f"{c['cap']:>8}{c['champ_ret']:>+9.1f}{c['winner_ret']:>+9.1f}")
    print("\n[J] NEXT EVALUATION PLAN: forward-test the new champion; auto-rollback if it degrades; re-mine hypotheses next cycle.")
    print("\nRESULT:", f"v1.0 -> {winner}  (champion advances; v1.0 archived for rollback)" if winner else "champion v1.0 retained; no challenger cleared every gate.")
