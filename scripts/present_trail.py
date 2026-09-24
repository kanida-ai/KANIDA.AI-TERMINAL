"""Arm-sensitivity (isolated) + the 5 named configs, for the final answer."""
import sys
sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
import grid_trail as G

days = G.load(); rcs = [rc for _, rc in days]


def M(arm, init, frac, give, stop):
    return G.agg([G.day_exit(rc, arm, init, frac, give, stop) for rc in rcs])


print("ARM SENSITIVITY — everything else FIXED (init0 / give1.0 / stop2). Isolates the arm effect:")
print(f"{'arm':>5}{'%armed':>8}{'return':>8}{'@5x':>8}{'maxDD':>7}{'giveB':>7}{'win%':>6}{'prem%':>7}{'bwProt':>8}{'bwKill':>8}{'worst':>7}")
for arm in [0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5]:
    r = M(arm, 0.0, 0.0, 1.0, 2.0)
    print(f"{arm:>5}{r['armedpct']:>7.0f}%{r['total']:>8.0f}{r['total']*5:>8.0f}{r['dd']:>7.1f}{r['gb']:>7.2f}"
          f"{r['win']:>6.1f}{r['prempct']:>6.0f}%{r['bw_prot']:>8}{r['bw_kill']:>8}{r['worst']:>7.1f}")

print("\nTHE 5 NAMED CONFIGS (ALL 535 days, Rs5L, net):")
configs = [
    ("Current +2.5% arm", (2.5, 1.0, 0.0, 1.5, 3.0)),
    ("Best RETURN (ride+stop-2)", (99.0, 0.0, 0.0, 9.9, 2.0)),
    ("Best RISK-ADJ", (1.5, 0.0, 0.0, 1.0, 2.0)),
    ("Safest", (0.75, 0.0, 0.3, 1.0, 1.5)),
    ("RECOMMENDED Falcon", (2.5, 0.25, 0.0, 1.5, 2.0)),
]
print(f"{'config':<26}{'ret%':>6}{'@5x%':>7}{'maxDD':>7}{'DD@5x':>7}{'giveB':>7}{'win%':>6}{'PF':>6}"
      f"{'avgW':>6}{'avgL':>7}{'bwProt':>7}{'bwKill':>7}{'prem%':>7}{'worst':>7}")
for name, (a, i, f, g, s) in configs:
    r = M(a, i, f, g, s)
    print(f"{name:<26}{r['total']:>6.0f}{r['total']*5:>7.0f}{r['dd']:>7.1f}{r['dd']*5:>7.0f}{r['gb']:>7.2f}"
          f"{r['win']:>6.1f}{r['pf']:>6.2f}{r['avgwin']:>6.2f}{r['avgloss']:>7.2f}{r['bw_prot']:>7}{r['bw_kill']:>7}"
          f"{r['prempct']:>6.0f}%{r['worst']:>7.1f}")
print("\n(bwProt/bwKill = big winners [basket peak>=3%] protected vs killed early; prem% = premature exits among armed)")
