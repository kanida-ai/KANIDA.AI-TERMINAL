"""Codenames — SIMPLE, easy-to-pronounce quant names (Greek letters, math / market / speed terms, clean coinages
like Sigma, Alpha, Accura, Velox). ONE unique single word per agent, matched to performance character.
The strongest agents get the marquee words; the long tail is filled with simple pronounceable coinages. No suffix/index.
Writes agent_summary.codename. Re-run incubation_engine.py + build_today_signals.py after to propagate."""
import os, sqlite3
import pandas as pd

# --- simple, clean, easy-to-say words per performance character ---
VIBEWORDS = {
 # SPRINT = fast & aggressive
 "SPRINT": """Velox Bolt Dash Flash Comet Rocket Rapid Turbo Nitro Blitz Sonic Swift Racer Tracer Arrow Jet Pulse
 Zoom Bullet Rush Surge Rally Meteor Dart Streak Zephyr Volt Ampere Vroom Torque Thrust Signal Sprinta Rapida
 Momenta Turbina Velar Zippo Kineto Accela Boosta""",
 # TANK = steady & durable
 "TANK": """Anvil Bedrock Granite Basalt Titan Atlas Pillar Bastion Bulwark Fortress Monolith Keystone Ballast
 Keel Bunker Rampart Redoubt Vault Anchor Boulder Ironclad Solid Stark Terra Cobalt Tungsten Steel Forta Solida
 Stabil Fundo Basis Rocca Castellan Aegis""",
 # SNIPER = precise, high-accuracy
 "SNIPER": """Accura Precisa Exacta Vertex Apex Focus Locus Bullseye Marksman Pinpoint Laser Scope Talon Needle
 Cusp Optic Optima Sharpe Precis Detecta Metrica Aima Nova Vector Crosshair Puncto Nitida Clarita Exacto Focal
 Preciso Acuta""",
 # HEAVY = rare, big wins
 "HEAVY": """Colossus Goliath Mammoth Behemoth Juggernaut Vulcan Everest Kraken Maxima Mega Giga Summit Peak Zenith
 Magnum Grand Jumbo Titano Leviatha Prima Ultra Massif Everesta Apexus Kolossa Grandeo Maximo""",
 # WORK = high-frequency grinder
 "WORK": """Abacus Metronome Dynamo Turbine Piston Ratchet Gear Loom Forge Mill Relay Conveyor Ticker Tally Counter
 Numero Calcula Digit Tabula Ledger Beacon Rotor Spindle Lathe Bellows Cadence Tempo Serial Itero Cyclo Grinda
 Steada Volumo""",
 # BAL = all-rounder (the big general reservoir: Greek letters, math, market)
 "BAL": """Alpha Beta Gamma Delta Sigma Theta Lambda Kappa Omega Zeta Phi Chi Psi Eta Iota Rho Tau Nu Mu Xi Omicron
 Vector Scalar Tensor Matrix Radian Tangent Secant Cosine Modulus Median Radix Prime Factor Ratio Axiom Lemma
 Locus Nexus Cipher Quanta Fractal Integer Nodal Cardinal Ordinal Datum Basis Helix Orbit Quark Photon Ion Neon
 Argon Xenon Krypton Sirius Vega Rigel Altair Polaris Lyra Draco Nova Astra Cosmo Lumen Flux Quantum Radon
 Euler Gauss Fermat Pascal Bayes Markov Fourier Turing Kepler Nash Napier Boole Cantor Riemann Fibonacci Newton
 Tesla Hertz Ohm Watt Joule Ampere Kelvin Faraday Curie Bohr Planck Dirac Hilbert Galois Laplace Abacus
 Ticker Bourse Trend Yield Basis Spread Rally Surge Alphaco Quantix Deltix Metrix Vertica Optix Numera Calculo
 Algora Statix Trenda Marketa Edgeon Volata Sharpo Ratios Primex Factora Vectra Scalix Datix Nodus Axia""",
}

# --- simple pronounceable coinage generator (Sigma / Accura / Velox flavour) — fills the long tail, no numeric suffix ---
STEM = ["Acu","Velo","Opti","Maxi","Nova","Astra","Vecto","Quanta","Numer","Calcu","Algo","Trend","Rapid","Turbo",
        "Sonic","Volta","Preci","Exact","Sharp","Metri","Stata","Optim","Alpha","Delta","Sigma","Kappa","Zeta","Vega",
        "Luma","Nira","Vela","Cora","Sola","Vera","Mira","Zara","Kira","Lyra","Thea","Maia","Tara","Nara","Sera","Cara",
        "Dara","Orbi","Pola","Nexa","Apex","Vorte","Helix","Prima","Ultra","Mega","Giga","Peta","Nano","Deca","Hexa",
        "Octa","Penta","Tera","Ratio","Facto","Scala","Tenso","Matri","Radi","Tanga","Cosa","Modu","Quon","Zena","Xylo"]
END = ["a","o","us","ex","ix","on","is","yx","ra","ro","na","no","va","ta","to","x","n","or","ar"]
def _clean(w):
    out = []
    for ch in w:
        if out and out[-1] in "aeiou" and ch in "aeiou": continue     # collapse doubled vowels
        if len(out) >= 2 and out[-1] == out[-2] == ch: continue       # no triple letters
        out.append(ch)
    return "".join(out)
def coinages():
    seen = set(); res = []
    for e in END:                              # breadth-first so early names stay simple/varied
        for st in STEM:
            w = _clean(st + e)
            if 5 <= len(w) <= 8 and w not in seen and sum(c in "aeiou" for c in w) >= 2 and not w.endswith(("aa","ii")):
                seen.add(w); res.append(w.capitalize())
    return res

AP = os.path.dirname(os.path.abspath(__file__)); DB = os.path.join(AP, "arena_metrics.db")
con = sqlite3.connect(DB); S = pd.read_sql_query("SELECT * FROM agent_summary", con)

def vibe(r):
    if r.maxdd < 15 and r.cagr_comp > 20: return "TANK"
    if r.win_pct >= 63: return "SNIPER"
    if r.avg_win >= 8: return "HEAVY"
    if r.cagr_comp >= 60: return "SPRINT"
    if r.trades >= 400: return "WORK"
    return "BAL"
S["vibe"] = S.apply(vibe, axis=1)

def dd(seq):
    seen = set(); out = []
    for w in seq:
        if w not in seen: seen.add(w); out.append(w)
    return out
VW = {k: dd(v.split()) for k, v in VIBEWORDS.items()}
GENERAL = dd(VW["BAL"] + VW["SPRINT"] + VW["SNIPER"] + VW["HEAVY"] + VW["WORK"] + VW["TANK"])
COIN = [c for c in coinages() if c not in set(GENERAL)]

used = set(); name = {}
vptr = {k: 0 for k in VW}; gptr = [0]; cptr = [0]
def take(vb):
    lst = VW.get(vb, [])
    while vptr[vb] < len(lst):
        w = lst[vptr[vb]]; vptr[vb] += 1
        if w not in used: used.add(w); return w
    while gptr[0] < len(GENERAL):
        w = GENERAL[gptr[0]]; gptr[0] += 1
        if w not in used: used.add(w); return w
    while cptr[0] < len(COIN):
        w = COIN[cptr[0]]; cptr[0] += 1
        if w not in used: used.add(w); return w
    return None

order = ["verified", "profit_factor", "cagr_comp"] if "verified" in S.columns else ["profit_factor", "cagr_comp"]
for _, r in S.sort_values(order, ascending=False).iterrows():
    name[r.agent_id] = take(r.vibe)
S["codename"] = S.agent_id.map(name)
miss = int(S.codename.isna().sum())
if miss:  # last resort simple filler
    extra = [f"{s}{e}".capitalize() for e in ["yn","el","ar","is","us","on"] for s in ["Quant","Trade","Metric","Signal","Alpha","Delta","Vector","Sigma","Factor","Edge","Prime","Nova"]]
    ep = 0
    for aid in S[S.codename.isna()].agent_id:
        while ep < len(extra) and extra[ep] in used: ep += 1
        if ep < len(extra): used.add(extra[ep]); S.loc[S.agent_id == aid, "codename"] = extra[ep]; ep += 1
    miss = int(S.codename.isna().sum())

S.to_sql("agent_summary", con, if_exists="replace", index=False); con.close()
print(f"named {len(S)-miss}/{len(S)} unique codenames (unfilled {miss}) | real-word pool {len(set(GENERAL))} · coinages {len(COIN)} · coinages used {cptr[0]}")
print("\nFlagships (top by PF, verified):")
_top = S.sort_values(["verified", "profit_factor"], ascending=False) if "verified" in S.columns else S.sort_values("profit_factor", ascending=False)
for _, r in _top.head(12).iterrows():
    print(f"  {r.codename:<12}[{r.vibe:<6}] PF {r.profit_factor:>4.1f} · win {r.win_pct:.0f}% · CAGR {r.cagr_comp:+.0f}% · {int(r.trades)} tr")
print("\nSample coinages in play:")
_c = [c for c in S.codename if c in set(COIN)][:16]
print("  " + ", ".join(_c) if _c else "  (none — all real words)")
