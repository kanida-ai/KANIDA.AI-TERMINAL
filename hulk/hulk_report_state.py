"""
FALCON HULK V1 — STATE ENGINE report.
Reads hulk/_out/state_grid.pkl + hulk_state_manifest, emits the grid, exports the
manifest, and writes docs/ops/HULK_STATE_ENGINE.md (the plain read).
"""
import os
import pickle
import sqlite3
import numpy as np
import pandas as pd

OUT = "hulk/_out/state_grid.pkl"
HDB = "data/db/falcon_hulk.db"
MD = "docs/ops/HULK_STATE_ENGINE.md"
MAN_CSV = "hulk/_out/state_manifest.csv"


def f(x, nd=2, sign=True, pct=False):
    if x is None or (isinstance(x, float) and (np.isnan(x))):
        return "n/a"
    v = x * 100 if pct else x
    return f"{v:+.{nd}f}" if sign else f"{v:.{nd}f}"


def grid_rows(cells):
    rows = []
    for c in cells:
        b = c.get("best")
        rows.append(dict(
            stock=c["stock"], dir=c["direction"], track=c["track"], horizon=c["horizon"],
            n_states=c.get("n_states", c.get("n_states_lib")),
            n_train=c["n_train"], n_test=c["n_test"], floor=c["floor_fires"],
            base_te=round(c["test_base"], 3) if not np.isnan(c["test_base"]) else np.nan,
            raw_cand=c.get("n_raw_candidates", 0), n_cand=c["n_candidates"], n_genuine=c["n_genuine"],
            best_net_pct=(round(b["oos_net_ret"] * 100, 3) if b else np.nan),
            best_lift_pp=(round(b["oos_lift_pp"], 2) if b else np.nan),
            best_hit=(round(b["oos_hit"], 3) if b else np.nan),
            best_nfires=(b["n_fires"] if b else np.nan),
            best_rec=(round(b["recurrence"], 2) if b else np.nan),
            null_p99=round(c["null_lift_p99"], 2) if not np.isnan(c["null_lift_p99"]) else np.nan,
            null_leak=c["null_leak_flag"],
            best_rule=(b["text"] if b else ""),
        ))
    return pd.DataFrame(rows)


def main():
    d = pickle.load(open(OUT, "rb"))
    cells = d["cells"]
    df = grid_rows(cells)

    # export manifest
    con = sqlite3.connect(HDB)
    man = pd.read_sql("SELECT * FROM hulk_state_manifest", con)
    con.close()
    os.makedirs("hulk/_out", exist_ok=True)
    man.to_csv(MAN_CSV, index=False)

    tot_gen = int(df["n_genuine"].sum())
    n_leak = int(df["null_leak"].sum())
    n_cells = len(df)

    lines = []
    L = lines.append
    L("# FALCON HULK V1 — STATE ENGINE (Phase 1)")
    L("")
    L("**Open-ended state search vs hand-picked features — PREDICT not CONFIRM.**")
    L("")
    L(f"- Run: {n_cells} cells (2 stocks x 2 directions x [Track A: 2 targets] + "
      f"[Track B: 4 multiday horizons + 1 next-day-intraday]).")
    L(f"- Miner: bagged shallow trees depth {d['depths']}, B={d['B']} bootstraps, "
      f"joblib n_jobs=-1 in-memory; NULL surrogate M={d['M']} permuted reps over the "
      f"SAME generated library.")
    L(f"- State library GENERATED per stock: Track A = {d['n_states_A']} primitives, "
      f"Track B = {d['n_states_B']} primitives (union {d['n_states_A']+d['n_states_B']}/stock). "
      f"Manifest: `{MAN_CSV}` ({len(man)} rows).")
    L(f"- **CALIBRATION FIX:** min OOS fire floor = `{d['floor_rule']}` "
      f"(was a flat 150 last run — impossible for ~245-row daily OOS). "
      f"Daily cells -> floor 30; a ~15k-entry intraday cell would -> ~150.")
    L(f"- Gate (ALL required): n_fires >= floor AND recurrence >= {d['genuine_rec_min']} "
      f"AND oos_lift > null_p99 AND oos_net_ret > 0 "
      f"(cost intraday {d['cost_intraday']}, multiday {d['cost_multiday']}).")
    L(f"- Window: TRAIN 2018-2023, OOS TEST 2024 (purged + 10-day embargo). "
      f"2025 held, **2026 SEALED (asserted, never loaded)**.")
    L(f"- Compute: {d['n_cores']} cores, runtime {d['runtime_sec']/60:.1f} min.")
    L("")
    L(f"## Headline: **{tot_gen} genuine predictive rules** across all {n_cells} cells. "
      f"Null-leak flags: **{n_leak}/{n_cells}**.")
    L("")

    # ---- grid table ----
    L("## Grid")
    L("")
    hdr = ["stock", "dir", "track", "horizon", "n_states", "n_test", "floor",
           "base_te", "raw_cand", "n_cand", "n_genuine", "best_net%", "best_lift_pp",
           "best_hit", "best_nfires", "best_rec", "null_p99", "null_leak"]
    L("| " + " | ".join(hdr) + " |")
    L("|" + "|".join(["---"] * len(hdr)) + "|")
    for _, r in df.iterrows():
        L("| " + " | ".join(str(x) for x in [
            r["stock"], r["dir"], r["track"], r["horizon"], r["n_states"], r["n_test"],
            r["floor"], r["base_te"], r["raw_cand"], r["n_cand"], r["n_genuine"],
            f(r["best_net_pct"], 3), f(r["best_lift_pp"]), f(r["best_hit"], 3, sign=False),
            "" if np.isnan(r["best_nfires"]) else int(r["best_nfires"]),
            f(r["best_rec"], 2, sign=False), f(r["null_p99"]),
            "YES" if r["null_leak"] else "-"]) + " |")
    L("")

    # ---- genuine rules for independent re-apply ----
    L("## Genuine rules (for independent re-apply)")
    L("")
    any_gen = False
    for c in cells:
        gens = [r for r in c["real"] if r["genuine"]]
        if not gens:
            continue
        any_gen = True
        L(f"### {c['stock']} / {c['direction']} / {c['horizon']}  "
          f"(floor={c['floor_fires']}, null_p99={f(c['null_lift_p99'])}pp)")
        for r in gens:
            L(f"- `{r['text']}`")
            L(f"  - n_fires={r['n_fires']}  oos_hit={r['oos_hit']:.3f}  "
              f"oos_lift={r['oos_lift_pp']:+.2f}pp  net={r['oos_net_ret']*100:+.3f}%  "
              f"recurrence={r['recurrence']:.2f}  train_lift={r['train_lift_pp']:+.2f}pp")
        L("")
    if not any_gen:
        L("_None. No state rule cleared the full gauntlet on 2024 OOS._")
        L("")

    # ---- plain read ----
    L("## Plain read")
    L("")
    q1 = ("YES" if tot_gen > 0 else "NO")
    L(f"**Does the open-ended state search find genuine predictive edge the hand "
      f"features missed?** {q1}.")
    if tot_gen == 0:
        L("Across ~900 generated states per stock, mined with depth-1/2/3 bagged trees "
          "and gated on a permuted-null surrogate that already absorbs the search size, "
          "NO combination cleared all four conditions (scaled fire floor, recurrence "
          ">=0.20, OOS lift above the null p99, positive net after cost) on 2024. The "
          "bigger search did not manufacture an edge the 22 hand features lacked.")
    else:
        L("Some state rules cleared the full gauntlet (listed above). Treat each as a "
          "hypothesis to re-apply on 2025 (held) before any weight.")
    L("")
    n_leak = int(df["null_leak"].sum())
    L(f"**Does the null stay clean under the bigger search (the key overfitting test)?** "
      f"{'NO -- ' + str(n_leak) + ' cell(s) flagged' if n_leak else 'YES'}. "
      f"null_rec_max >= 0.20 in {n_leak}/{n_cells} cells. "
      + ("Where flagged, the permuted-label search itself reproduces recurrence >=0.20, "
         "meaning recurrence alone is NOT discriminative at this library size — the "
         "OOS-lift-over-null-p99 + net-positive tests are doing the real work, and any "
         "'genuine' rule in a flagged cell must clear the lift/net bar, not recurrence."
         if n_leak else
         "Permuted labels do not reproduce recurrence >=0.20, so recurrence remains a "
         "meaningful structural filter even at this library size."))
    L("")
    L("**Blunt caveats / mirages.**")
    L("- This is 2 stocks and a single 2024 OOS year. Small n; even a clean pass is thin "
      "evidence. The design's value is the *negative* control: proving the enlarged "
      "search does not fabricate signal.")
    L("- Track A intraday +0.5% long labels have a very high base rate (~0.79), so 'hit' "
      "is easy and lift is what matters; net-after-cost is the honest bar.")
    L("- Next-day-intraday uses the 09:20 bar as a proxy for the absent 09:15 (spine has "
      "no 09:15 row) — flagged, not a true open entry.")
    L("- Prediction genuinely finds little on 2 stocks (as expected). The engine is built "
      "to SCALE to more stocks next; per-stock edge should be re-mined at the wider "
      "universe, not concluded from this pair.")
    L("")
    L("## Artifacts")
    L(f"- `hulk/_out/state_grid.pkl` — full per-cell results (rules, OOS, null).")
    L(f"- `{MAN_CSV}` — every generated state primitive's definition.")
    L(f"- DB (isolated): `hulk_state_feat_A`, `hulk_state_feat_B`, `hulk_state_manifest` "
      f"in `data/db/falcon_hulk.db`.")
    L(f"- Scripts: `hulk/hulk_state_gen.py` (generator), `hulk/hulk_mine_state.py` "
      f"(miner), `hulk/hulk_report_state.py` (this report).")

    os.makedirs("docs/ops", exist_ok=True)
    with open(MD, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    # console
    pd.set_option("display.max_columns", None, "display.width", 240)
    print(df.drop(columns=["best_rule"]).to_string(index=False))
    print(f"\ngenuine total={tot_gen}  null_leak cells={n_leak}/{n_cells}  "
          f"runtime={d['runtime_sec']/60:.1f}min  cores={d['n_cores']}")
    print(f"wrote {MD}\nwrote {MAN_CSV}")


if __name__ == "__main__":
    main()
