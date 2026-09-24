"""Render the Stage-1 FULL GRID map from hulk/_out/stage1_grid.pkl.
Emits the 24-row map, the genuine-edge list (with exact rules for spot-check),
and the per-cell null-floor sanity table. Prints markdown to stdout."""
import pickle
import numpy as np

HZ_ORDER = {"intraday": 0, "T+1": 1, "T+2": 2, "T+3": 3, "T+5": 4, "T+10": 5}


def f(x, nd=2, sign=False):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "n/a"
    return (f"{x:+.{nd}f}" if sign else f"{x:.{nd}f}")


def main():
    with open("hulk/_out/stage1_grid.pkl", "rb") as fh:
        d = pickle.load(fh)
    cells = d["cells"]
    cells.sort(key=lambda c: (-c["n_genuine"], c["stock"], c["direction"],
                              HZ_ORDER.get(c["horizon"], 9)))

    print(f"Runtime: {d['runtime_sec']:.0f}s | B={d['B']} bootstraps | "
          f"M={d['M']} null reps\n")

    # ---- MAP ----
    print("## The MAP (24 cells)\n")
    hdr = ("| stock | dir | horizon | train_base | test_base | n_cand | "
           "n_genuine | best_oos_lift_pp | best_rec | best_oos_hit | "
           "best_net_ret% | best_rule |")
    print(hdr)
    print("|" + "---|" * 12)
    for c in cells:
        b = c["best"]
        if b:
            row = (f"| {c['stock']} | {c['direction']} | {c['horizon']} | "
                   f"{f(c['train_base'],3)} | {f(c['test_base'],3)} | "
                   f"{c['n_candidates']} | {c['n_genuine']} | "
                   f"{f(b['oos_lift_pp'],2,True)} | {f(b['recurrence'],2)} | "
                   f"{f(b['oos_hit'],3)} | "
                   f"{f(b['oos_net_ret']*100 if b['oos_net_ret'] is not None and not np.isnan(b['oos_net_ret']) else np.nan,2,True)} | "
                   f"`{b['text']}` |")
        else:
            row = (f"| {c['stock']} | {c['direction']} | {c['horizon']} | "
                   f"{f(c['train_base'],3)} | {f(c['test_base'],3)} | "
                   f"{c['n_candidates']} | {c['n_genuine']} | n/a | n/a | n/a | "
                   f"n/a | (no candidate >= {20} fires) |")
        print(row)

    # ---- GENUINE EDGES ----
    print("\n## Genuine edges (spot-check ready)\n")
    any_g = False
    for c in cells:
        for r in c["real"]:
            if not r["genuine"]:
                continue
            any_g = True
            print(f"- **{c['stock']} {c['direction']} {c['horizon']}** — "
                  f"n_fires(2024)={r['n_fires']}, oos_hit={f(r['oos_hit'],3)}, "
                  f"oos_lift={f(r['oos_lift_pp'],2,True)}pp "
                  f"(floor={f(c['floor_used'],2)}), rec={f(r['recurrence'],2)}, "
                  f"net={f(r['oos_net_ret']*100,2,True)}%, "
                  f"train_lift={f(r['train_lift_pp'],2,True)}pp")
            print(f"  - RULE: `{r['text']}`")
    if not any_g:
        print("_None. No cell produced a genuine discovery under the gate._")

    # ---- NULL-FLOOR SANITY ----
    print("\n## Null-floor sanity (per cell)\n")
    print("| stock | dir | horizon | null_reps_w/cand | null_p95 | null_p99 | "
          "floor_used | null_rec_max | LEAK? |")
    print("|" + "---|" * 9)
    for c in sorted(cells, key=lambda c: (c["stock"], c["direction"],
                                          HZ_ORDER.get(c["horizon"], 9))):
        print(f"| {c['stock']} | {c['direction']} | {c['horizon']} | "
              f"{c['null_n_reps_with_candidate']}/{d['M']} | "
              f"{f(c['null_lift_p95'],2,True)} | {f(c['null_lift_p99'],2,True)} | "
              f"{f(c['floor_used'],2)} | {f(c['null_rec_max'],2)} | "
              f"{'**YES**' if c['null_leak_flag'] else 'no'} |")


if __name__ == "__main__":
    main()
