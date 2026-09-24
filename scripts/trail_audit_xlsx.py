"""Falcon trailing-method audit workbook: empirical head-to-head (recomputed via trail_lab)
+ qualitative comparison matrix across every dimension + architecture recommendation."""
import pickle, sys
from pathlib import Path
import numpy as np
sys.path.insert(0, r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine\scripts")
import pos_sim as P
from trail_lab import run_model, metrics, catalogue, rolling_vol

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
OUT = ROOT / "docs" / "ops" / "FALCON_TRAIL_AUDIT.xlsx"


def intraday_rows():
    mats = pickle.load(open(ROOT / "docs" / "ops" / "_opt_dataset.pkl", "rb"))
    paths = []
    for d, M in mats:
        e = M["entry"].astype(float); q = np.floor((500000.0 / M["nstocks"]) / e); dep = float((e * q).sum())
        rc = (M["close"].astype(float) @ q - dep) / dep * 100.0
        paths.append((d[:7], rc, M["n"] - 1, rolling_vol(rc)))
    cat = catalogue(1.5, 3.0, 1.0, 1.5)
    cat[2] = ("CURRENT arm2.5/floor1/give1.5", dict(type="current", arm=2.5, floor=1.0, give=1.5, stop=3.0))
    res = []
    for name, model in cat:
        rows = [run_model(rc, last, model, vol) for _, rc, last, vol in paths]
        res.append((name, metrics(rows, [m for m, _, _, _ in paths])))
    return res


def positional_rows():
    ds = P.load()
    paths = []
    for m in ds:
        rc, _ = P.paths(m); last = int(m["eod"][min(3, int(m["day"].max()))])
        paths.append((m["signal_date"][:7], rc, last, rolling_vol(rc[:last + 1])))
    cat = catalogue(2.0, 6.0, 1.0, 4.0)
    cat[2] = ("CURRENT arm3/floor1/give4", dict(type="current", arm=3.0, floor=1.0, give=4.0, stop=6.0))
    cat[4] = ("STEP-lock (milestones)", dict(type="step", arm=2.0, steps=[(3.0, 1.5), (6.0, 4.0), (9.0, 6.5), (14.0, 11.0)], stop=6.0))
    res = []
    for name, model in cat:
        rows = [run_model(rc, last, model, vol) for _, rc, last, vol in paths]
        res.append((name, metrics(rows, [m for m, _, _, _ in paths])))
    return res


# ---- qualitative matrix (model -> dimensions) ----
MATRIX = [
    dict(model="CURRENT (arm/floor/giveback)",
         how="Arm at +A%, lock a floor +F%, exit if P&L falls >G% from peak or below floor; hard stop -S%.",
         cx="Medium (4 interacting knobs)", rewrite="Baseline (already live)",
         prem="Medium — small give-back whipsaws out of recoverable days",
         give="Low-Med", ease="Medium — 4 knobs interact, hard to reason about",
         best="Positional-ish; over-engineered for intraday",
         intra="POOR (costs ~0.06-0.09%/day vs ride)", posi="OK", eq="OK", fut="OK"),
    dict(model="RIDE + HARD STOP (no trail)",
         how="Hold to square-off / max-hold; single catastrophe stop -S%. No trailing at all.",
         cx="Very low (1 knob)", rewrite="Yes — disable trail, keep the stop/GTT",
         prem="Very low", give="HIGH — no profit protection if it reverses late",
         best="INTRADAY equity (proven best) & very short holds",
         ease="Very high — trivial to explain",
         intra="BEST (1.805% vs 1.744% current)", posi="Risky (worst tail -9.9)", eq="Good intraday", fut="NO — leverage makes give-back dangerous"),
    dict(model="PEAK-TRAIL (arm + give, no floor)",
         how="Arm at +A%, exit if P&L falls >G% from the running peak; hard stop -S%. No floor.",
         cx="Low (2 knobs)", rewrite="Yes — drop-in, remove the floor term",
         prem="Medium", give="Low-Med", ease="High — 2 intuitive knobs",
         best="A cleaner default trailing; ~= current with fewer knobs",
         intra="OK (1.720, still < ride)", posi="OK (4.895)", eq="OK", fut="OK"),
    dict(model="STEP-LOCK (milestones)",
         how="As peak crosses milestones, ratchet a locked floor; exit if P&L falls below the highest lock; stop -S%.",
         cx="Medium (a table, but discrete & clear)", rewrite="Small — replace threshold with a step lookup",
         prem="Low — coarse steps don't whipsaw", give="Medium — gives back within a step band",
         best="POSITIONAL & FUTURES (big multi-day swings)",
         ease="High — a table anyone reads (lock X at peak Y)",
         intra="OK (1.761)", posi="BEST (4.907, DD 13.2, 26/26 months)", eq="Good", fut="GOOD (discrete, leverage-friendly)"),
    dict(model="PERCENT-OF-PEAK (60/70/80%)",
         how="Lock a percentage of the highest profit; the % rises as peak grows; exit if P&L falls below it.",
         cx="Medium", rewrite="Small — threshold = peak x pct(peak)",
         prem="HIGH — exits too early (1.59 intraday / 4.02 positional)", give="Low",
         best="Capital-preservation mandates, NOT return-max",
         ease="Medium", intra="POOR", posi="POOR (over-protective)", eq="Weak", fut="Weak"),
    dict(model="VOLATILITY-BASED (ATR / live vol)",
         how="Give-back width scales with live basket volatility — tighter when calm, wider when volatile.",
         cx="HIGH (needs a vol feed + careful calibration)", rewrite="Medium — add rolling-vol state to the engine",
         prem="HIGH if mis-tuned (worst performer as tested)", give="Low-Med",
         best="Futures / high-vol regimes IF carefully calibrated — research track",
         ease="LOW — opaque; user can't predict the exit",
         intra="POOR as-tuned", posi="POOR as-tuned", eq="Unproven", fut="Maybe (needs work)"),
    dict(model="TIME-ADAPTIVE give-back",
         how="Give-back tightens as the session/hold ages; most aggressive near the close.",
         cx="Medium", rewrite="Small — give = f(time-of-day / bar-age)",
         prem="Medium", give="Med early, Low late", ease="Medium",
         best="INTRADAY (close-aware locking)",
         intra="Good concept (1.699, still < ride)", posi="OK (4.948)", eq="Good intraday", fut="OK"),
    dict(model="WINNER/LOSER-AWARE (contribution)",
         how="Basket decision also considers per-stock contribution — cut weak names, tighten when 1 name carries.",
         cx="HIGH (per-stock logic inside the basket decision)", rewrite="Medium-High — engine tracks per-position but the trail is aggregate",
         prem="Medium", give="Low",
         best="Concentrated baskets — but per-stock stops HURT intraday; stop+refill only MATCHED positional",
         ease="LOW — hard to reason about",
         intra="Weak (per-stock stops hurt)", posi="Marginal (refill ~= basket-only)", eq="Research", fut="Research"),
]


def write():
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    F = "Calibri"
    TITLE = Font(name=F, bold=True, size=13, color="1F6F8B"); SUB = Font(name=F, bold=True, size=11)
    NOTE = Font(name=F, size=9, italic=True, color="666666"); H = Font(name=F, bold=True, size=9, color="FFFFFF")
    HDR = PatternFill("solid", start_color="1F6F8B"); NORM = Font(name=F, size=10); BOLD = Font(name=F, bold=True, size=10)
    GRN = Font(name=F, size=10, color="1E7E34"); RED = Font(name=F, size=10, color="B00020")
    HILF = PatternFill("solid", start_color="FDF2CC"); GRNF = PatternFill("solid", start_color="E7F4EA")
    thin = Side(style="thin", color="DDDDDD"); BORD = Border(left=thin, right=thin, top=thin, bottom=thin)
    Cc = Alignment(horizontal="center", vertical="top"); Ll = Alignment(horizontal="left", vertical="top", wrap_text=True)
    wb = Workbook()

    def emp_sheet(ws, title, res, hilite, unit):
        ws.cell(1, 1, title).font = TITLE
        ws.cell(2, 1, "Same realized basket paths, all models, net of ~0.10% RT. give-back = avg peak->exit gap. "
                      "One reasonable param set per model (all tunable).").font = NOTE
        heads = ["model", "mean%", "%pos", "total%", "maxDD", "worst", "give-back", "EOD/TRAIL/STOP", "months+"]
        for c, h in enumerate(heads, 1):
            x = ws.cell(4, c, h); x.font = H; x.fill = HDR; x.border = BORD; x.alignment = Cc
        for r, (name, s) in enumerate(res, 5):
            rc3 = f"{s['eod']}/{s['trail']}/{s['stop']}"
            vals = [name, s["mean"], s["pos"], s["total"], s["maxdd"], s["worst"], s["giveback"], rc3, s["months"]]
            for c, v in enumerate(vals, 1):
                x = ws.cell(r, c, v); x.font = NORM; x.border = BORD
                x.alignment = Alignment(horizontal="left") if c == 1 else Cc
            if hilite in name:
                for c in range(1, 10): ws.cell(r, c).fill = HILF
        for i, w in enumerate([34, 8, 7, 9, 8, 8, 10, 15, 9], 1):
            ws.column_dimensions[get_column_letter(i)].width = w
        ws.cell(len(res) + 6, 1, unit).font = NOTE

    emp_sheet(wb.active, "Empirical — INTRADAY (535 days, Rs5L)", intraday_rows(), "RIDE + HARD",
              "VERDICT: ride to 15:29 + a single -3% stop beats the current trail (1.805 vs 1.744) — the arm/floor/giveback is net cost here.")
    wb.active.title = "Empirical Intraday"
    emp_sheet(wb.create_sheet("Empirical Positional"), "Empirical — POSITIONAL (528 baskets, max-hold 3)",
              positional_rows(), "STEP-lock",
              "VERDICT: step-lock matches the current trail's return (4.907 vs 4.898) with lower drawdown (13.2 vs 15.6) and 26/26 positive months.")

    # ---- matrix ----
    ws = wb.create_sheet("Comparison matrix")
    ws.cell(1, 1, "Trailing models — full comparison across every dimension").font = TITLE
    cols = [("model", "Model", 26), ("how", "How it works", 40), ("cx", "Complexity", 18),
            ("rewrite", "Add without rewriting engine?", 26), ("prem", "Premature-exit risk", 22),
            ("give", "Give-back risk", 12), ("ease", "Ease of understanding", 22), ("best", "Best use case", 30)]
    for c, (k, lab, w) in enumerate(cols, 1):
        x = ws.cell(3, c, lab); x.font = H; x.fill = HDR; x.border = BORD; x.alignment = Cc
        ws.column_dimensions[get_column_letter(c)].width = w
    for r, m in enumerate(MATRIX, 4):
        for c, (k, lab, w) in enumerate(cols, 1):
            x = ws.cell(r, c, m[k]); x.font = BOLD if c == 1 else NORM; x.border = BORD; x.alignment = Ll
        ws.row_dimensions[r].height = 58
    # suitability sub-table
    base = len(MATRIX) + 6
    ws.cell(base, 1, "Suitability by strategy / product").font = SUB
    sh = ["Model", "Intraday", "Positional", "Equity", "Futures"]
    for c, h in enumerate(sh, 1):
        x = ws.cell(base + 1, c, h); x.font = H; x.fill = HDR; x.border = BORD; x.alignment = Cc
    for r, m in enumerate(MATRIX, base + 2):
        for c, k in enumerate(["model", "intra", "posi", "eq", "fut"], 1):
            x = ws.cell(r, c, m[k]); x.font = BOLD if c == 1 else NORM; x.border = BORD; x.alignment = Ll
        ws.row_dimensions[r].height = 30

    # ---- recommendation ----
    ws = wb.create_sheet("Recommendation")
    ws.cell(1, 1, "Architecture recommendation").font = TITLE
    lines = [
        ("NOT one universal model.", SUB),
        ("The data shows intraday and positional want OPPOSITE things: intraday is best with NO trail (ride + hard stop), "
         "positional benefits from a coarse lock (step). A single universal model must compromise one of them.", NORM),
        ("", NORM),
        ("RECOMMENDED: selectable profit-locking models with STRATEGY/PRODUCT-specific DEFAULTS.", SUB),
        ("Add a `lock_model` field (+ its params) to falcon_trail_config / trail_engine.decide and dispatch on it. "
         "Backward-compatible: 'current' stays the default until each strategy is flipped. Small engineering, no rewrite.", NORM),
        ("", NORM),
        ("Recommended defaults (data-backed):", SUB),
        ("  - Intraday equity  -> RIDE + -3% hard stop (drop arm/floor/giveback). Recovers ~0.06%/day, simpler, best DD.", NORM),
        ("  - Positional equity -> STEP-LOCK (same return as current, DD 15.6->13.2, 26/26 months). PEAK-trail if you want fewer knobs.", NORM),
        ("  - Futures           -> STEP-LOCK with WIDER, leverage-aware steps + a hard stop. Never rely on give-back alone under leverage.", NORM),
        ("", NORM),
        ("PARK (do not build yet): fully ADAPTIVE 'auto-choose model by regime'.", SUB),
        ("The adaptive proxy we could test (volatility-based) was the WORST performer, and there's no evidence an auto-selector "
         "beats well-chosen fixed defaults. It adds complexity and opacity. Revisit only if a vol/regime signal proves it in backtest.", NORM),
        ("", NORM),
        ("Bottom line: Falcon should be SELECTABLE + strategy-specific, not universal and not (yet) adaptive. "
         "The single biggest, safest win is switching INTRADAY off the trail to ride+stop.", BOLD),
    ]
    r = 3
    for t, f in lines:
        c = ws.cell(r, 1, t); c.font = f; c.alignment = Alignment(wrap_text=True, vertical="top")
        ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=8)
        if len(t) > 90: ws.row_dimensions[r].height = 30
        r += 1
    for i in range(1, 9): ws.column_dimensions[get_column_letter(i)].width = 16

    out = OUT
    try:
        wb.save(out)
    except PermissionError:
        out = OUT.with_name(OUT.stem + "_v2.xlsx"); wb.save(out)
    print(f"[*] WROTE {out}  ({len(wb.sheetnames)} sheets)")


if __name__ == "__main__":
    write()
