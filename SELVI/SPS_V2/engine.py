"""
SPS_V2 — Conditional-Entry Engine (cumulative loss-bridging ladder)
===================================================================
Works BACKWARDS from the goal (+1% per traded day) by fixing, one at a time,
the five diagnosed causes of the SPS_V1 baseline loss. Each rung ADDS one lever
and we measure the improvement, in-sample (2022-2025) and out-of-sample (2026).

Levers (cumulative):
  L1 Direction   : trade the 15-min opening-range BREAKOUT side (long on break up,
                   short on break down). Enter at the break level. [fixes coin-flip]
  L2 Timing/selectivity : only the FIRST break inside 09:30-11:00; skip if none.
                   [fixes 9:15 open-noise / chasing late breaks]
  L3 Regime      : trade only when prior-day ATR% >= train-median (enough fuel for 1%).
  L4 Target      : bracket exit at +1%. [captures the move V1 proved exists]
  L5 Stop        : bracket exit at -STOP% (defined R:R). [caps the loser tail]

Leak-free: every decision uses only info available by the decision minute
(prev-day daily bars for ATR; the day's own bars only up to the entry/exit minute).
Intrabar target/stop: STOP checked before TARGET within a bar (conservative).
ATR-median threshold is fit on TRAIN (2022-2024) only.

Run:  PYTHONIOENCODING=utf-8 python engine.py
"""
from __future__ import annotations
import json, sqlite3
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
DB = HERE.parents[1] / "db" / "kanida.db"
OUT = HERE / "outputs"; OUT.mkdir(exist_ok=True)

STOCKS = ["ADANIENT", "CARTRADE"]
CAPITAL = 30_000.0
RT_COST = 0.0010            # round-trip charges + exit slippage
ENTRY_SLIP = 0.0003         # entry stop-order slippage (each entry)
TARGET = 0.01               # +1% goal
OR_MIN = 15                 # opening-range minutes (09:15-09:29)
WIN_START, WIN_END = 15, 105     # break window offsets: 09:30 .. 11:00
WIN_WIDE = 285              # L1 wide window cutoff (till ~14:00) before timing lever
N = 375
TRAIN_END = "2024-12-31"    # ATR threshold fit on <= this
IS = ("2022-01-01", "2025-12-31")
OOS = ("2026-01-01", "2026-12-31")
STOP = 0.006                # -0.6% stop (R:R ~ 1.67); reported alongside alt below


# ---------- data ----------
def load_min(symbol, start, end):
    con = sqlite3.connect(str(DB))
    df = pd.read_sql("SELECT bar_time,open,high,low,close FROM ohlc_1min "
                     "WHERE symbol=? AND bar_time>=? AND bar_time<=? ORDER BY bar_time",
                     con, params=[symbol, start, end + " 23:59:59"])
    con.close()
    dt = pd.to_datetime(df["bar_time"])
    df["date"] = dt.dt.date
    df["off"] = (dt.dt.hour * 60 + dt.dt.minute) - (9 * 60 + 15)
    return df[(df["off"] >= 0) & (df["off"] < N)]


def load_atr(symbol):
    con = sqlite3.connect(str(DB))
    d = pd.read_sql("SELECT bar_time,high,low,close FROM ohlc_daily WHERE symbol=? ORDER BY bar_time",
                    con, params=[symbol])
    con.close()
    d["date"] = pd.to_datetime(d["bar_time"]).dt.date
    pc = d["close"].shift(1)
    tr = pd.concat([d["high"] - d["low"], (d["high"] - pc).abs(), (d["low"] - pc).abs()], axis=1).max(axis=1)
    d["atr14"] = tr.rolling(14).mean()
    d["atr_pct"] = d["atr14"] / pc          # prior-close-normalised, known at open
    d["atr_pct_prev"] = d["atr_pct"].shift(1)   # use PRIOR day's ATR% (fully known before open)
    return dict(zip(d["date"], d["atr_pct_prev"]))


def day_arrays(df):
    dates = sorted(df["date"].unique())
    days = {}
    for d, sub in df.groupby("date"):
        o = np.full(N, np.nan); h = np.full(N, np.nan); l = np.full(N, np.nan); c = np.full(N, np.nan)
        off = sub["off"].to_numpy()
        o[off] = sub["open"].to_numpy(); h[off] = sub["high"].to_numpy()
        l[off] = sub["low"].to_numpy(); c[off] = sub["close"].to_numpy()
        if np.isnan(o[0]):
            continue
        last = np.where(~np.isnan(c))[0]
        days[d] = (o, h, l, c, int(last[-1]))
    return days


# ---------- simulation ----------
def sim_day(arr, atr_pct, cfg):
    o, h, l, c, last = arr
    entry915 = o[0]
    # baseline mode: fixed side, enter 09:15, hold EOD
    if cfg.get("baseline"):
        side = cfg["baseline"]; entry = entry915
        exitp = c[last]
        g = (exitp - entry) / entry if side == "long" else (entry - exitp) / entry
        return {"took": True, "side": side, "net": g - RT_COST, "target_hit": False}
    # regime filter
    if cfg.get("regime") and (atr_pct is None or np.isnan(atr_pct) or atr_pct < cfg["atr_thresh"]):
        return {"took": False}
    orh = np.nanmax(h[:OR_MIN]); orl = np.nanmin(l[:OR_MIN])
    if np.isnan(orh) or np.isnan(orl):
        return {"took": False}
    cutoff = cfg.get("win_end", WIN_WIDE)
    start = cfg.get("win_start", OR_MIN)
    side = entry = eoff = None
    for t in range(max(start, OR_MIN), min(cutoff, last + 1)):
        if np.isnan(h[t]):
            continue
        up = h[t] >= orh; dn = l[t] <= orl
        if up and dn:
            continue                        # both sides in one bar -> whipsaw, skip bar
        if up:
            side, entry, eoff = "long", orh, t; break
        if dn:
            side, entry, eoff = "short", orl, t; break
    if side is None:
        return {"took": False}
    entry = entry * (1 + ENTRY_SLIP) if side == "long" else entry * (1 - ENTRY_SLIP)
    tgt = entry * (1 + TARGET) if side == "long" else entry * (1 - TARGET)
    stp = None
    if cfg.get("stop"):
        stp = entry * (1 - cfg["stop"]) if side == "long" else entry * (1 + cfg["stop"])
    exitp = None; thit = False
    for t in range(eoff + 1, last + 1):     # exits from the bar AFTER the break
        if np.isnan(h[t]):
            continue
        hi, lo = h[t], l[t]
        if side == "long":
            if stp is not None and lo <= stp:
                exitp = stp; break
            if cfg.get("target") and hi >= tgt:
                exitp = tgt; thit = True; break
        else:
            if stp is not None and hi >= stp:
                exitp = stp; break
            if cfg.get("target") and lo <= tgt:
                exitp = tgt; thit = True; break
    if exitp is None:
        exitp = c[last]
    g = (exitp - entry) / entry if side == "long" else (entry - exitp) / entry
    return {"took": True, "side": side, "net": g - RT_COST, "target_hit": thit}


def run_stage(days, atrmap, cfg, all_days_count):
    nets, wins, thits, sides = [], 0, 0, {"long": 0, "short": 0}
    for d, arr in days.items():
        r = sim_day(arr, atrmap.get(d), cfg)
        if not r["took"]:
            continue
        nets.append(r["net"]); wins += r["net"] > 0; thits += r.get("target_hit", False)
        sides[r["side"]] += 1
    nets = np.array(nets)
    took = len(nets)
    return {
        "trades": took,
        "participation": took / all_days_count if all_days_count else 0,
        "win_rate": float((nets > 0).mean()) if took else 0.0,
        "avg_net_per_trade": float(nets.mean()) if took else 0.0,
        "avg_net_per_day_all": float(nets.sum() / all_days_count) if all_days_count else 0.0,
        "target_hit_rate": thits / took if took else 0.0,
        "expectancy_pct": float(nets.mean()) if took else 0.0,
        "sharpe": float(nets.mean() / (nets.std() + 1e-12)) if took else 0.0,
        "total_pnl": float(np.nansum([n * CAPITAL for n in nets])),
        "sides": sides,
    }


STAGES = [
    ("L0 baseline long (9:15→EOD)", {"baseline": "long"}),
    ("L0 baseline short (9:15→EOD)", {"baseline": "short"}),
    ("L1 +direction (OR breakout, wide window)", {"win_start": OR_MIN, "win_end": WIN_WIDE}),
    ("L2 +timing (09:30–11:00 only)", {"win_start": WIN_START, "win_end": WIN_END}),
    ("L3 +regime (ATR%≥train median)", {"win_start": WIN_START, "win_end": WIN_END, "regime": True}),
    ("L4 +target (+1%)", {"win_start": WIN_START, "win_end": WIN_END, "regime": True, "target": True}),
    ("L5 +stop (R:R)", {"win_start": WIN_START, "win_end": WIN_END, "regime": True, "target": True, "stop": STOP}),
]


def analyze(symbol):
    atrmap = load_atr(symbol)
    is_days = day_arrays(load_min(symbol, *IS))
    oos_days = day_arrays(load_min(symbol, *OOS))
    # fit ATR threshold on TRAIN only
    train_atr = [atrmap[d] for d in is_days if d.isoformat() <= TRAIN_END and atrmap.get(d) is not None and not np.isnan(atrmap.get(d, np.nan))]
    atr_thresh = float(np.nanmedian(train_atr))
    rows = []
    for name, base in STAGES:
        cfg = dict(base); cfg["atr_thresh"] = atr_thresh
        r_is = run_stage(is_days, atrmap, cfg, len(is_days))
        r_oos = run_stage(oos_days, atrmap, cfg, len(oos_days))
        rows.append((name, r_is, r_oos))
    return rows, atr_thresh, len(is_days), len(oos_days)


def fmt(r):
    return (f"part {r['participation']*100:4.0f}% | win {r['win_rate']*100:4.1f}% | "
            f"tgt-hit {r['target_hit_rate']*100:4.1f}% | net/trade {r['avg_net_per_trade']*100:+.3f}% | "
            f"net/day {r['avg_net_per_day_all']*100:+.3f}% | Sharpe {r['sharpe']:+.3f} | ₹{r['total_pnl']:+,.0f}")


def main():
    rep = ["# SPS_V2 — Conditional-Entry Ladder (bridging the baseline loss)",
           f"_generated {datetime.now():%Y-%m-%d %H:%M} · ₹{CAPITAL:,.0f} · cost {RT_COST*100:.2f}%/rt + {ENTRY_SLIP*100:.2f}% entry slip · "
           f"target {TARGET*100:.0f}% · stop {STOP*100:.1f}% · IS 2022-2025 · OOS 2026_", ""]
    payload = {}
    for symbol in STOCKS:
        rows, thr, nis, noos = analyze(symbol)
        rep.append(f"## {symbol}  (IS {nis} days, OOS {noos} days · ATR%% threshold {thr*100:.2f}%)")
        rep.append("")
        rep.append("| Stage | IS (2022-2025) | OOS (2026) |")
        rep.append("|---|---|---|")
        payload[symbol] = []
        for name, r_is, r_oos in rows:
            rep.append(f"| {name} | {fmt(r_is)} | {fmt(r_oos)} |")
            payload[symbol].append({"stage": name, "is": r_is, "oos": r_oos})
        rep.append("")
    (OUT / "report.md").write_text("\n".join(rep), encoding="utf-8")
    json.dump(payload, open(OUT / "ladder.json", "w"), indent=2)
    try:
        import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
        fig, axes = plt.subplots(1, len(STOCKS), figsize=(13, 5), sharey=True)
        short = ["L0 long", "L0 short", "L1 dir", "L2 time", "L3 regime", "L4 target", "L5 stop"]
        for ax, symbol in zip(axes, STOCKS):
            xs = range(len(payload[symbol]))
            ax.plot(xs, [s["is"]["avg_net_per_trade"] * 100 for s in payload[symbol]], "o-", label="in-sample 22-25")
            ax.plot(xs, [s["oos"]["avg_net_per_trade"] * 100 for s in payload[symbol]], "s--", label="OOS 2026")
            ax.axhline(0, color="k", lw=.7)
            ax.set_xticks(list(xs)); ax.set_xticklabels(short, rotation=45, ha="right", fontsize=8)
            ax.set_title(f"{symbol}: net %/trade by lever"); ax.set_ylabel("net % per trade"); ax.legend(fontsize=8)
        fig.tight_layout(); fig.savefig(OUT / "ladder.png", dpi=115); plt.close(fig)
    except Exception as e:
        print("plot skipped:", e)
    print("\n".join(rep))
    print(f"[written: {OUT}]")


if __name__ == "__main__":
    main()
