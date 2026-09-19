"""
SPS_V3 VAULT TEST — the ONE authorised break of the 2026 seal.
Reads champion_<AGENT>.json, evaluates that exact spec on the sealed 2026 holdout
(never seen during research), and reports the honest verdict vs the +1%/day goal
at 1x and 5x. Writes vault_verdict_<AGENT>.md.

Run:  python vault_test.py <AGENT>
"""
import sys, json
from pathlib import Path
from datetime import datetime
import numpy as np
import lab, continuous
from lab import load_min, load_atr, day_pack, TRAIN, VAULT_START

HERE = Path(__file__).resolve().parent
VAULT = (VAULT_START, "2026-12-31")


def main(agent):
    cpath = HERE / f"champion_{agent}.json"
    if not cpath.exists():
        print(f"no champion yet for {agent}"); return
    champ = json.load(open(cpath)); hyp = champ["spec"]
    atrmap = load_atr(agent)
    # ATR threshold is fit on TRAIN only (same as research) — no vault peeking for the threshold
    tr = day_pack(load_min(agent, *TRAIN))
    atrs = [atrmap[d][0] for d in tr if atrmap.get(d) and atrmap[d][0] == atrmap[d][0]]
    atr_thr = float(np.nanmedian(atrs))
    # >>> the single authorised unlock of the sealed 2026 holdout <<<
    vault = day_pack(load_min(agent, *VAULT, unlock=True))
    r = continuous.simulate(agent, vault, atrmap, hyp, atr_thr)
    goal = 0.01
    lines = [f"# VAULT VERDICT — SELVI-{'AE' if agent=='ADANIENT' else 'CT'} ({agent})",
             f"_sealed holdout 2026 ({len(vault)} days) · evaluated {datetime.now():%Y-%m-%d %H:%M}_", "",
             f"**Champion:** {hyp['trigger']} · filters {hyp['filters'] or '-'} · "
             f"target {hyp['target']*100:.2f}% · stop {('%.2f%%'%(hyp['stop']*100)) if hyp['stop'] else 'none'}", ""]
    if not r:
        lines.append("**No trades on the vault** — champion did not fire in 2026. Inconclusive; keep searching.")
    else:
        v1, v5 = r["net_1x"], r["net_5x"]
        lines += [
            f"- vault trades: **{r['trades']}** · win **{r['win']*100:.1f}%** · target-hit **{r['tgt_hit']*100:.0f}%** · t-stat {r['tstat']:.2f}",
            f"- net **per trade**: 1x **{v1*100:+.3f}%** · 5x **{v5*100:+.3f}%** (on ₹30k capital)",
            "",
            f"### Verdict vs +1%/day goal",
            f"- 1x: {v1*100:+.3f}%/day → {'MET' if v1>=goal else 'NOT met'} (goal +1.000%)",
            f"- 5x: {v5*100:+.3f}%/day → {'MET' if v5>=goal else 'NOT met'} (goal +1.000%)",
            "",
            ("**Honest read:** " + (
                "champion holds on unseen data." if v1 > 0 else
                "champion FAILS out-of-sample on the vault — it was train/val-specific. The seal did its job; the loop keeps searching.")),
        ]
    (HERE / f"vault_verdict_{agent}.md").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "CARTRADE")
