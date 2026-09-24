"""
Walk-forward OOS validation — Falcon Top 5 @ 09:15 trailing config.

Train = 2024-05-14 .. 2025-12-31  (in-sample)
Test  = 2026-01-01 .. 2026-06-15  (OUT-OF-SAMPLE holdout)

The Falcon Top-10 selection is already walk-forward (signal-year patterns mined
strictly before that year), and the trail/stop/floor are fixed RULES (not fit to
data), so 2026 is a clean OOS test of the whole stack. The only tunable knob is
the trail giveback; we pick the best giveback on TRAIN, lock it, and report TEST.

Reuses research_trailing_top5 (cache + portfolio-trail sim).
"""
from __future__ import annotations
import sqlite3
from pathlib import Path
import numpy as np
import pandas as pd

from falcon_intraday_backtest import Config, load_signals
from research_trailing_top5 import build_top5_cache, simulate, GIVEBACKS

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs" / "Top5_WalkForward_OOS.xlsx"
DESKTOP = Path.home() / "Desktop" / "Kanida_Intraday_Backtest_Results"
SPLIT = "2026-01-01"        # test = entry_date >= SPLIT


def block(df):
    win = df[df.ret > 0]; loss = df[df.ret < 0]
    return {
        "days": len(df), "win_rate_%": round((df.ret > 0).mean() * 100, 1),
        "avg_win_%": round(win.ret.mean(), 3) if len(win) else None,
        "avg_loss_%": round(loss.ret.mean(), 3) if len(loss) else None,
        "avg_day_%": round(df.ret.mean(), 3),
        "median_%": round(float(df.ret.median()), 3),
        "worst_day_%": round(df.ret.min(), 2),
        "hit_1pct_%": round((df.ret >= 1.0).mean() * 100, 1),
        "sum_%": round(df.ret.sum(), 1),
    }


def main():
    cfg = Config()
    con = sqlite3.connect(str(cfg.db_path))
    signals = load_signals(con, cfg); con.close()
    print("[*] building Top-5 cache ...", flush=True)
    cache, days = build_top5_cache(cfg, signals)
    print(f"    days {len(days)}", flush=True)

    variants = {"Baseline +1% hard": ("hard1", None)}
    for g in GIVEBACKS:
        variants[f"Trail {g:.2f}"] = ("trail", g)

    rows = []
    series = {}
    for label, (mode, g) in variants.items():
        recs = [{"date": d, "ret": simulate(cache[d], mode, g)[0]} for d in days]
        df = pd.DataFrame(recs)
        series[label] = df
        tr = df[df.date < SPLIT]; te = df[df.date >= SPLIT]
        r = {"strategy": label}
        for k, v in block(tr).items():
            r[f"TRAIN_{k}"] = v
        for k, v in block(te).items():
            r[f"TEST_{k}"] = v
        rows.append(r)
    res = pd.DataFrame(rows)

    trail_only = res[res.strategy.str.startswith("Trail")].copy()
    best = trail_only.sort_values("TRAIN_avg_day_%", ascending=False).iloc[0]
    headline = {
        "selected_on_train": best["strategy"],
        "TRAIN_win_%": best["TRAIN_win_rate_%"], "TRAIN_avg_day_%": best["TRAIN_avg_day_%"],
        "TEST_win_%": best["TEST_win_rate_%"], "TEST_avg_day_%": best["TEST_avg_day_%"],
        "TEST_days": best["TEST_days"], "TEST_worst_%": best["TEST_worst_day_%"],
        "TEST_sum_%": best["TEST_sum_%"],
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT, engine="openpyxl") as xl:
        res.to_excel(xl, sheet_name="Train_vs_Test", index=False)
        pd.DataFrame([headline]).to_excel(xl, sheet_name="Headline_LockedOnTrain", index=False)
        sel = series[best["strategy"]].copy()
        sel["dt"] = pd.to_datetime(sel.date); sel["ym"] = sel.dt.dt.strftime("%Y-%m")
        sel_test = sel[sel.date >= SPLIT]
        monthly = sel_test.groupby("ym").apply(
            lambda x: pd.Series(block(x)), include_groups=False).reset_index()
        monthly.to_excel(xl, sheet_name="OOS_Monthly_Selected", index=False)

    pd.set_option("display.width", 240); pd.set_option("display.max_columns", 40)
    print("\n=== TRAIN (2024-05..2025-12) vs TEST/OOS (2026) ===")
    show = ["strategy", "TRAIN_days", "TRAIN_win_rate_%", "TRAIN_avg_day_%", "TRAIN_worst_day_%",
            "TEST_days", "TEST_win_rate_%", "TEST_avg_day_%", "TEST_worst_day_%"]
    print(res[show].to_string(index=False))
    print("\n=== HEADLINE: giveback chosen on TRAIN, evaluated OOS on TEST ===")
    for k, v in headline.items():
        print(f"  {k}: {v}")
    print(f"\n[*] wrote {OUT}")
    if DESKTOP.exists():
        import shutil; shutil.copy(OUT, DESKTOP / OUT.name)
        print(f"[*] copied to {DESKTOP / OUT.name}")


if __name__ == "__main__":
    main()
