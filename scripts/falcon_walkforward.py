"""
TRUE point-in-time ROLLING WALK-FORWARD of the Falcon intraday-basket strategy
for test years 2024, 2025, 2026.

WHAT THIS FIXES
---------------
Production promotes patterns via universe_engine/engine/falcon_validator.py ->
validate_and_promote(), which tests each candidate on ALL years INCLUDING future
ones (a 2022 pattern is promoted partly because it worked in 2024-26). That is
look-ahead. Here, for test-year Y we re-run the IDENTICAL promotion gate but:
  (a) only candidates with mined_year in [Y-4 .. Y-1]   (mining_window_years=4)
  (b) per-year OOS validation counts ONLY years < Y.
Everything downstream (signal ranking, tiering, baskets, intraday trail) is the
frozen production/replay logic so we isolate ONLY the promotion change.

READ-ONLY on all databases. Writes one Excel + a Downloads copy. Touches no
production code.

Mirrors:
  - universe_engine/engine/falcon_validator.py   (promotion gate)
  - scripts/falcon_signal_replay.py              (rank_for_date, family drop)
  - backend/power_user/services/signal_tier.py   (signal-time tiering)
  - scripts/flow_paper_engine.py::cost           (Zerodha intraday charges)
  - scripts/falcon_full_report.py                (basket-level trail on capital)
"""
from __future__ import annotations
import json, sqlite3, time, shutil
from collections import defaultdict
from pathlib import Path
import numpy as np

ROOT = Path(r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine")
RES  = ROOT / "universe_engine" / "data" / "db" / "kanida_universe.db"   # research: panel, candidates, 1-min
PROD = ROOT / "data" / "db" / "kanida_universe.db"                        # prod: signal features, tier rulebook, ohlc_daily

# ---- promotion-gate constants (verbatim from falcon_validator.py) ------------
MIN_OOS_LIFT_PP       = 5.0
MIN_OOS_YEARS_PASS    = 2
MIN_CROSS_SECTOR_LIFT = 0.0
MIN_OOS_OBS           = 30
MINING_WINDOW_YEARS   = 4
TEST_YEARS            = [2024, 2025, 2026]

FEATURE_COLS = [
    "range_pct","close_loc","gap_pct","body_pct","upper_wick_pct","lower_wick_pct",
    "dist_sma_20","dist_sma_50","dist_sma_200","slope_sma_20","slope_sma_50",
    "rsi_14","roc_5","roc_20","roc_60",
    "vol_vs_20d","vol_5d_vs_20d","n_sub_75v_7d","n_sub_75v_20d",
    "atr_20_pct","atr_5_vs_20","n_sub_2_5_range_7d","n_sub_3_range_7d",
    "n_higher_lows_5d","n_higher_highs_5d",
    "dist_high_10","dist_high_20","dist_high_60","dist_high_120","dist_high_252",
    "weekly_close_vs_sma20","weekly_breakout_20w","weekly_range_pct","weekly_close_loc",
    "rs_sector_20d","rs_sector_60d","rs_market_20d","rs_market_60d",
]
FIDX = {c: i for i, c in enumerate(FEATURE_COLS)}
TARGETS = ["hit_10pc_20d", "hit_15pc_20d", "hit_25pc_30d", "hit_40pc_40d"]
# DERIVED hit targets (verified byte-identical to the stored columns, 827,379/827,379):
#   hit_10pc_20d=(mfe_20d>=10) hit_15pc_20d=(mfe_20d>=15) hit_25pc_30d=(mfe_30d>=25) hit_40pc_40d=(mfe_40d>=40)
TARGET_DERIVE = {
    "hit_10pc_20d": ("mfe_20d", 10.0),
    "hit_15pc_20d": ("mfe_20d", 15.0),
    "hit_25pc_30d": ("mfe_30d", 25.0),
    "hit_40pc_40d": ("mfe_40d", 40.0),
}

DROPPED_FAMILIES = {"drawdown_bounce"}
HIGH_TIER = {"ENTERPRISE-Dryup", "GOLD", "GOLD-baseline",
             "PREMIUM-Compression", "PREMIUM-Pullback"}

# ---- intraday basket trail (CAPITAL basis, 5x MIS) — configs FIXED by spec ----
CAPITAL   = 500_000.0
LEVERAGE  = 5.0
# arm / give / floor / stop  (percent, on CAPITAL basis i.e. already the 5x return)
CFG = {
    "T3":  dict(arm=3.0, give=5.0, floor=2.0, stop=3.0),
    "T15": dict(arm=5.0, give=3.0, floor=0.5, stop=3.0),
}
EOD_MIN = "15:29"

# Hindsight (look-ahead) totals from docs/ops/FOUNDERS_BASKET_TRADELOG_2024-2026.xlsx
HINDSIGHT = {
    "T3":  dict(total5x=3185.0, total1x=637.0, pos_months=25, total_months=27, worst_month=-20.6, pnl=15_926_714),
    "T15": dict(total5x=3559.0, total1x=712.0, pos_months=26, total_months=27, worst_month=-24.6, pnl=17_793_557),
}


# =============================================================================
# 1) RESEARCH panel (features INNER JOIN outcomes), hits DERIVED from mfe
# =============================================================================
def load_panel():
    t0 = time.time()
    con = sqlite3.connect(str(RES), timeout=120)
    rows = con.execute("SELECT symbol, sector FROM falcon_sectors").fetchall()
    sym_to_sec = {s: sec for s, sec in rows}
    sector_names = sorted(set(sym_to_sec.values()))
    sec_to_idx = {s: i for i, s in enumerate(sector_names)}

    feat_select = ", ".join(FEATURE_COLS)
    sql = f"""
        SELECT f.symbol, f.trade_date, {feat_select},
               o.mfe_20d, o.mfe_30d, o.mfe_40d
        FROM falcon_features f
        INNER JOIN falcon_outcomes o
          ON o.symbol = f.symbol AND o.trade_date = f.trade_date
    """
    rows = con.execute(sql).fetchall()
    con.close()

    n = len(rows); nf = len(FEATURE_COLS)
    X = np.full((n, nf), np.nan, dtype=np.float64)
    years = np.zeros(n, dtype=np.int32)
    sec_arr = np.full(n, -1, dtype=np.int32)
    mfe20 = np.full(n, np.nan); mfe30 = np.full(n, np.nan); mfe40 = np.full(n, np.nan)
    for i, r in enumerate(rows):
        sym = r[0]; date_str = r[1]
        X[i, :] = [v if v is not None else np.nan for v in r[2:2 + nf]]
        years[i] = int(date_str[:4])
        sec_arr[i] = sec_to_idx.get(sym_to_sec.get(sym, ""), -1)
        mfe20[i] = r[2 + nf] if r[2 + nf] is not None else np.nan
        mfe30[i] = r[3 + nf] if r[3 + nf] is not None else np.nan
        mfe40[i] = r[4 + nf] if r[4 + nf] is not None else np.nan
    Y = {
        "hit_10pc_20d": (mfe20 >= 10.0).astype(np.int8),
        "hit_15pc_20d": (mfe20 >= 15.0).astype(np.int8),
        "hit_25pc_30d": (mfe30 >= 25.0).astype(np.int8),
        "hit_40pc_40d": (mfe40 >= 40.0).astype(np.int8),
    }
    panel = dict(X=X, year=years, sector_idx=sec_arr, Y=Y,
                 sec_to_idx=sec_to_idx, sector_names=sector_names, n=n)
    print(f"[panel] {n:,} rows x {nf} feats loaded in {time.time()-t0:.1f}s "
          f"({X.nbytes/1e6:.0f} MB); years={sorted(set(years.tolist()))}", flush=True)
    return panel


def rule_mask(rule, X):
    m = np.ones(X.shape[0], dtype=bool)
    for f, op, th in rule:
        idx = FIDX.get(f)
        if idx is None:
            return np.zeros(X.shape[0], dtype=bool)
        col = X[:, idx]
        if op == "<=":
            m &= (col <= th) & ~np.isnan(col)
        else:
            m &= (col > th) & ~np.isnan(col)
    return m


def classify_families(rule):
    fams = set()
    for f, op, th in rule:
        if f == "weekly_close_loc" and op == ">": fams.add("weekly_close")
        if f == "atr_20_pct" and op == ">": fams.add("high_atr")
        if f in ("dist_high_252", "dist_high_120") and op == "<=" and th < -10:
            fams.add("drawdown_bounce")
        if f == "weekly_range_pct" and op == ">": fams.add("weekly_range")
    return fams


# =============================================================================
# 2) Point-in-time promotion (mirror validate_and_promote + promote_patterns)
# =============================================================================
def _promote_from(oos, sector_rows, scope):
    """Apply the exact production gate to a set of (n,lift) year-rows + sector-rows.
    Returns (promoted:bool, classification, avg_oos_year_lift, years_pass, avg_cross) or
    (False, reject_reason, ...)."""
    oos_year_lifts = [l for n, l in oos if n >= MIN_OOS_OBS]
    if not oos_year_lifts:
        return (False, "no_oos_data", 0.0, 0, 0.0)
    years_pass = sum(1 for l in oos_year_lifts if l > 0)
    avg_oos = sum(oos_year_lifts) / len(oos_year_lifts)
    if avg_oos < MIN_OOS_LIFT_PP:
        return (False, "weak_oos_year_lift", avg_oos, years_pass, 0.0)
    if years_pass < MIN_OOS_YEARS_PASS:
        return (False, "too_few_passing_years", avg_oos, years_pass, 0.0)
    cross = [l for n, l in sector_rows if n >= MIN_OOS_OBS]
    avg_cross = (sum(cross) / len(cross)) if cross else 0.0
    if scope.startswith("sector:") and avg_cross <= 0:
        cls = "sector_specific"
    elif years_pass < 3:
        cls = "regime_dependent"
    elif avg_cross >= 0:
        cls = "universal"
    else:
        cls = "regime_dependent"
    return (True, cls, avg_oos, years_pass, avg_cross)


def build_promotions(panel):
    """Promote each candidate TWO ways per test year, from the SAME candidate pool
    (mined_year in [Y-4..Y-1]):
      point-in-time (PIT): OOS validation years < Y only.
      look-ahead   (LA) : OOS validation years = all years != mined_year (incl Y and later).
    Holding the candidate pool + gate + config constant, the PIT-vs-LA delta downstream
    is PURELY the look-ahead effect."""
    X = panel["X"]; year = panel["year"]; sec = panel["sector_idx"]; Y = panel["Y"]
    panel_years = sorted(set(year.tolist()))
    year_mask = {y: (year == y) for y in panel_years}

    # Precompute per-(target,year) base rates and per-(target,year,sector) base rates.
    base_year = {t: {y: float(Y[t][year_mask[y]].mean()) * 100 if year_mask[y].any() else 0.0
                     for y in panel_years} for t in TARGETS}
    # sector base rate cache, filled lazily per (target, mined_year, sector_idx)
    sec_base_cache = {}
    def sector_base(t, my, sidx, sel_mask):
        key = (t, my, sidx)
        v = sec_base_cache.get(key)
        if v is None:
            v = float(Y[t][sel_mask].mean()) * 100 if sel_mask.any() else 0.0
            sec_base_cache[key] = v
        return v

    con = sqlite3.connect(str(RES), timeout=120)
    cands = con.execute("""
        SELECT pattern_id, mined_year, scope, outcome_target, rule_json
        FROM falcon_pattern_candidates
    """).fetchall()
    con.close()

    # arms: "PIT" (point-in-time) and "LA" (look-ahead), same candidate pool
    pit = {y: [] for y in TEST_YEARS}; la = {y: [] for y in TEST_YEARS}
    def _mkstats():
        return {y: dict(candidates=0, promoted=0, after_drop=0, by_cls=defaultdict(int),
                        max_val_year=-1) for y in TEST_YEARS}
    stats_pit = _mkstats(); stats_la = _mkstats()

    def _record(arm_results, arm_stats, Y_, pid, mined_year, target, rule, cls, avg_oos, yp, ac):
        arm_stats[Y_]["promoted"] += 1
        arm_stats[Y_]["by_cls"][cls] += 1
        if cls not in ("universal", "regime_dependent"):
            return
        if DROPPED_FAMILIES & classify_families(rule):
            return
        arm_stats[Y_]["after_drop"] += 1
        arm_results[Y_].append(dict(pattern_id=pid, mined_year=mined_year, target=target,
                                    rule=rule, oos_lift=round(avg_oos, 4), classification=cls,
                                    years_pass=yp, avg_cross=round(ac, 4)))

    t0 = time.time()
    for ci, (pid, mined_year_s, scope, target, rule_json) in enumerate(cands):
        mined_year = int(mined_year_s)
        elig_Y = [Y_ for Y_ in TEST_YEARS
                  if (Y_ - MINING_WINDOW_YEARS) <= mined_year <= (Y_ - 1)]
        if not elig_Y:
            continue
        rule = [(f, op, th) for f, op, th in json.loads(rule_json)]
        mined_sector = scope.split(":", 1)[1] if scope.startswith("sector:") else None
        rmask = rule_mask(rule, X)
        yv = Y[target]

        # per-year n_obs / n_hits over ALL panel years (compute once)
        yr_stat = {}
        for y in panel_years:
            sel = year_mask[y]
            n_obs = int((rmask & sel).sum())
            if n_obs:
                n_hits = int(yv[rmask & sel].sum())
                lift = n_hits / n_obs * 100 - base_year[target][y]
            else:
                lift = 0.0
            yr_stat[y] = (n_obs, lift)

        # per-sector tests within mined_year (identical for both arms)
        my_mask = year_mask.get(mined_year)
        sector_rows = []
        if my_mask is not None and my_mask.any():
            for sname, sidx in panel["sec_to_idx"].items():
                if sname == mined_sector:
                    continue
                sel = my_mask & (sec == sidx)
                n_obs = int((rmask & sel).sum())
                if n_obs:
                    n_hits = int(yv[rmask & sel].sum())
                    base = sector_base(target, mined_year, sidx, sel)
                    sector_rows.append((n_obs, n_hits / n_obs * 100 - base))
                else:
                    sector_rows.append((0, 0.0))

        for Y_ in elig_Y:
            stats_pit[Y_]["candidates"] += 1; stats_la[Y_]["candidates"] += 1
            # POINT-IN-TIME: only years < Y (skip mined_year, as production does)
            oos_pit = [(n, l) for y, (n, l) in yr_stat.items() if y < Y_ and y != mined_year]
            uy = [y for y in yr_stat if y < Y_ and y != mined_year and yr_stat[y][0] >= MIN_OOS_OBS]
            if uy:
                stats_pit[Y_]["max_val_year"] = max(stats_pit[Y_]["max_val_year"], max(uy))
            ok, cls, avg_oos, yp, ac = _promote_from(oos_pit, sector_rows, scope)
            if ok:
                _record(pit, stats_pit, Y_, pid, mined_year, target, rule, cls, avg_oos, yp, ac)
            # LOOK-AHEAD: all years != mined_year (includes Y and later — the production leak)
            oos_la = [(n, l) for y, (n, l) in yr_stat.items() if y != mined_year]
            uy2 = [y for y in yr_stat if y != mined_year and yr_stat[y][0] >= MIN_OOS_OBS]
            if uy2:
                stats_la[Y_]["max_val_year"] = max(stats_la[Y_]["max_val_year"], max(uy2))
            ok, cls, avg_oos, yp, ac = _promote_from(oos_la, sector_rows, scope)
            if ok:
                _record(la, stats_la, Y_, pid, mined_year, target, rule, cls, avg_oos, yp, ac)
        if (ci + 1) % 1000 == 0:
            print(f"  [promote] {ci+1}/{len(cands)} candidates ({time.time()-t0:.0f}s)", flush=True)

    for Y_ in TEST_YEARS:
        s = stats_pit[Y_]
        assert s["max_val_year"] < Y_, f"LEAK: PIT Y={Y_} used validation year {s['max_val_year']}"
        print(f"[promote] PIT Y={Y_}: cands={s['candidates']} promoted={s['promoted']} "
              f"(signal-usable={s['after_drop']}) by_cls={dict(s['by_cls'])} max_val_year={s['max_val_year']}", flush=True)
        l = stats_la[Y_]
        print(f"[promote]  LA Y={Y_}: cands={l['candidates']} promoted={l['promoted']} "
              f"(signal-usable={l['after_drop']}) by_cls={dict(l['by_cls'])} max_val_year={l['max_val_year']}", flush=True)
    return pit, la, stats_pit, stats_la, panel_years


# =============================================================================
# 3) Signal ranking (mirror falcon_signal_replay.rank_for_date), PROD features
# =============================================================================
def rank_for_date(pcon, patterns, signal_date, min_fires=10, top_n=100, keep=50):
    sel = ", ".join(FEATURE_COLS)
    rows = pcon.execute(
        f"SELECT symbol, {sel} FROM falcon_features WHERE trade_date=?",
        (signal_date,)).fetchall()
    if not rows:
        return []
    syms = [r[0] for r in rows]
    X = np.full((len(syms), len(FEATURE_COLS)), np.nan)
    for i, r in enumerate(rows):
        X[i] = [v if v is not None else np.nan for v in r[1:]]
    yr = int(signal_date[:4])
    elig = [p for p in patterns if int(p["mined_year"]) < yr]   # faithful to falcon_signal_replay
    fire = np.zeros(len(syms), dtype=np.int32)
    score = np.zeros(len(syms))
    for p in elig:
        m = rule_mask(p["rule"], X)
        if not m.any():
            continue
        fire += m.astype(np.int32)
        score += m.astype(np.float64) * p["oos_lift"]
    cands = [{"symbol": syms[i], "n_fires": int(fire[i]), "score": float(score[i])}
             for i in range(len(syms)) if fire[i] >= min_fires]
    cands.sort(key=lambda c: -c["score"])
    top = cands[:top_n]
    ranked = sorted(top, key=lambda c: -(c["score"] / max(c["n_fires"], 1)))
    out = ranked[:keep]
    for pos, c in enumerate(out, 1):
        c["rank"] = pos
        c["avg_lift"] = c["score"] / max(c["n_fires"], 1)
    return out


# =============================================================================
# 4) Signal-time tiering (mirror signal_tier.py exactly)
# =============================================================================
_OPS = {"<": lambda a, b: a < b, "<=": lambda a, b: a <= b,
        ">": lambda a, b: a > b, ">=": lambda a, b: a >= b}

def _ok(v):
    return v is not None and not (isinstance(v, float) and v != v)

def load_active_rulebook(pcon):
    rows = pcon.execute(
        "SELECT tier, conditions_json FROM falcon_tier_rules WHERE status='active'").fetchall()
    parsed = []
    for tier, cj in rows:
        spec = json.loads(cj)
        parsed.append((int(spec.get("priority", 50)), tier, spec.get("all", [])))
    parsed.sort(key=lambda r: r[0])
    return parsed

def classify_from_rulebook(feat, rules):
    for _prio, tier, conds in rules:
        match = True
        for field, op, val in conds:
            v = feat.get(field)
            if not _ok(v) or op not in _OPS or not _OPS[op](v, val):
                match = False; break
        if match:
            return tier
    return "STANDARD-weak"

def signal_day_features(bars):
    """bars: ascending list of dicts {close,high,low,volume}; last == signal day."""
    n = len(bars)
    if n < 2:
        return {}
    sd = bars[-1]
    close = sd["close"]; high = sd["high"]; low = sd["low"]; vol = sd["volume"]
    prev_close = bars[-2]["close"]
    out = {}
    if prev_close:
        out["signal_day_ret_pct"] = (close / prev_close - 1) * 100
        out["range_pct"] = (high - low) / prev_close * 100
    if n >= 3 and bars[-3]["close"]:
        out["two_day_ret_pct"] = (close / bars[-3]["close"] - 1) * 100
    vols = [b["volume"] for b in bars if b["volume"] is not None]
    if len(vols) >= 11:
        w20 = vols[-20:]; avg20 = sum(w20) / len(w20)
        if avg20 > 0:
            out["rvol20"] = vol / avg20
            avg3 = sum(vols[-3:]) / len(vols[-3:])
            out["trend3_20"] = avg3 / avg20
    turns = [(b["close"] * b["volume"]) for b in bars
             if b["close"] is not None and b["volume"] is not None]
    if len(turns) >= 60:
        window = turns[-252:]; today_turn = turns[-1]
        out["turn_pct"] = sum(1 for t in window if t <= today_turn) / len(window)
    return out

def tier_for(feats, avg_lift, rules):
    return classify_from_rulebook(
        {"sret": feats.get("signal_day_ret_pct"),
         "twoday": feats.get("two_day_ret_pct"),
         "rng": feats.get("range_pct"),
         "avg_lift": avg_lift,
         "trend3_20": feats.get("trend3_20"),
         "turn_pct": feats.get("turn_pct")}, rules)


# =============================================================================
# 5) Intraday basket trail (CAPITAL basis, 5x) + Zerodha cost
# =============================================================================
def cost(seg, entry, exit_, qty):
    """flow_paper_engine.cost — Zerodha equity-MIS round-trip charges -> Rs."""
    tb, ts = entry * qty, exit_ * qty
    brok = min(20, 0.0003 * tb) + min(20, 0.0003 * ts)
    if seg in ("FUT", "INDEX"):
        stt = 0.0002 * ts; txn = 0.0000173 * (tb + ts); stamp = 0.00002 * tb
    else:
        stt = 0.00025 * ts; txn = 0.0000297 * (tb + ts); stamp = 0.00003 * tb
    sebi = 0.000001 * (tb + ts); gst = 0.18 * (brok + txn + sebi)
    return brok + stt + txn + stamp + sebi + gst


def load_intraday(rcon, entry_date, symbols):
    """Return grid (sorted HH:MM) and per-symbol aligned open/close arrays (fwd-filled)."""
    if not symbols:
        return [], {}
    qmarks = ",".join("?" * len(symbols))
    rows = rcon.execute(
        f"SELECT symbol, substr(bar_time,12,5) m, open, high, low, close "
        f"FROM ohlc_1min WHERE bar_time>=? AND bar_time<=? AND symbol IN ({qmarks}) "
        f"ORDER BY m", (entry_date + " 09:15", entry_date + " 15:29", *symbols)).fetchall()
    by = defaultdict(dict)
    mins = set()
    for sym, m, o, h, l, c in rows:
        by[sym][m] = (o, h, l, c)
        mins.add(m)
    grid = sorted(mins)
    return grid, by


def run_basket(rcon, entry_date, picks, cfg):
    """picks: list of dicts with symbol/rank/tier. Returns (day_row, stock_rows) or None."""
    syms = [p["symbol"] for p in picks]
    grid, by = load_intraday(rcon, entry_date, syms)
    if not grid:
        return None
    # keep only symbols that actually have an entry (a bar with a valid open) this day
    traded = []
    for p in picks:
        d = by.get(p["symbol"])
        if not d:
            continue
        # entry = first available bar's open on/after 09:15 (prefer exact 09:15)
        entry_min = grid[0] if grid[0] in d else next((m for m in grid if m in d), None)
        if entry_min is None:
            continue
        eo = d[entry_min][0]
        if eo is None or eo <= 0:
            continue
        traded.append((p, entry_min, float(eo)))
    n = len(traded)
    if n == 0:
        return None

    ng = len(grid)
    alloc_notional = LEVERAGE * CAPITAL / n
    qty = np.array([np.floor(alloc_notional / e) for (_p, _m, e) in traded], dtype=np.float64)
    entry = np.array([e for (_p, _m, e) in traded], dtype=np.float64)

    # aligned close matrix (ng x n), forward-filled; pre-entry filled with entry price
    close_mat = np.empty((ng, n), dtype=np.float64)
    open_mat = np.full((ng, n), np.nan, dtype=np.float64)
    for j, (p, emin, e) in enumerate(traded):
        d = by[p["symbol"]]
        last = e
        for i, m in enumerate(grid):
            row = d.get(m)
            if row is not None:
                if row[3] is not None and row[3] > 0:
                    last = float(row[3])
                if row[0] is not None:
                    open_mat[i, j] = float(row[0])
            close_mat[i, j] = last

    dep = float((entry * qty).sum())
    gross_pnl = (close_mat * qty).sum(axis=1) - dep         # Rs, per minute
    ret_cap = gross_pnl / CAPITAL * 100.0                   # CAPITAL basis == 5x return

    a = cfg["arm"]; g = cfg["give"]; fl = cfg["floor"]; st = cfg["stop"]
    armed = False; peak = None; xb = ng - 1; reason = "EOD"
    for i in range(ng - 1):
        r = ret_cap[i]
        if r <= -st:
            xb, reason = i, "HARD_STOP"; break
        if not armed:
            if r >= a:
                armed = True; peak = r
            continue
        peak = max(peak, r)
        floor_level = max(fl, peak - g)
        if r <= floor_level:
            xb, reason = i, ("FLOOR" if floor_level == fl else "TRAIL"); break

    # exit fill: EOD -> last bar close; else -> next bar OPEN (fallback last close)
    if reason == "EOD":
        exit_px = close_mat[ng - 1].copy(); exit_time = grid[ng - 1]
    else:
        nxt = open_mat[xb + 1] if xb + 1 < ng else np.full(n, np.nan)
        cx = close_mat[xb]
        exit_px = np.where(np.isfinite(nxt) & (nxt > 0), nxt, cx)
        exit_time = grid[min(xb + 1, ng - 1)]

    gross = float((exit_px * qty).sum() - dep)
    charges = float(sum(cost("CASH", entry[j], exit_px[j], qty[j]) for j in range(n)))
    net = gross - charges
    ret5x = net / CAPITAL * 100.0
    ret1x = ret5x / LEVERAGE

    stock_rows = []
    for j, (p, emin, e) in enumerate(traded):
        xp = float(exit_px[j]); q = int(qty[j])
        stock_rows.append(dict(
            entry_date=entry_date, symbol=p["symbol"], rank=p["rank"], tier=p["tier"],
            entry_time=emin, entry_px=round(e, 2), exit_time=exit_time, exit_px=round(xp, 2),
            stock_ret_pct=round((xp / e - 1) * 100, 3), exit_reason=reason,
            basket_ret5x_pct=round(ret5x, 3), day_pnl_rs=round(net, 0)))
    day_row = dict(entry_date=entry_date, month=entry_date[:7], n_stocks=n,
                   exit_time=exit_time, exit_reason=reason,
                   deployed=round(dep, 0), gross_rs=round(gross, 0), charges_rs=round(charges, 0),
                   net_rs=round(net, 0), ret5x=round(ret5x, 4), ret1x=round(ret1x, 4))
    return day_row, stock_rows


# =============================================================================
# 6) Drive the walk-forward
# =============================================================================
def main():
    T0 = time.time()
    panel = load_panel()
    pit, la, stats_pit, stats_la, panel_years = build_promotions(panel)

    pcon = sqlite3.connect(f"file:{PROD}?mode=ro", uri=True, timeout=120)
    rcon = sqlite3.connect(f"file:{RES}?mode=ro", uri=True, timeout=120)

    import bisect
    prod_days = [r[0] for r in pcon.execute(
        "SELECT DISTINCT trade_date FROM falcon_features ORDER BY trade_date")]
    entry_dates = [r[0] for r in rcon.execute(
        "SELECT DISTINCT substr(bar_time,1,10) d FROM ohlc_1min ORDER BY d")]
    sig_of = {}
    for ed in entry_dates:
        i = bisect.bisect_left(prod_days, ed) - 1
        if i >= 0:
            sig_of[ed] = prod_days[i]

    print("[tier] loading ohlc_daily ...", flush=True)
    t0 = time.time()
    daily = defaultdict(list)
    for sym, td, c, h, l, v in pcon.execute(
            "SELECT symbol, trade_date, close, high, low, volume FROM ohlc_daily ORDER BY symbol, trade_date"):
        daily[sym].append((td, c, h, l, v))
    daily_dates = {sym: [x[0] for x in seq] for sym, seq in daily.items()}
    print(f"[tier] ohlc_daily loaded: {len(daily)} symbols in {time.time()-t0:.1f}s", flush=True)
    tier_rules = load_active_rulebook(pcon)
    print(f"[tier] active rulebook rules: {len(tier_rules)}", flush=True)

    def tier_of(sym, signal_date, avg_lift):
        seq = daily.get(sym)
        if not seq:
            return "STANDARD-weak"
        hi = bisect.bisect_right(daily_dates[sym], signal_date)
        sub = seq[max(0, hi - 260):hi]
        bars = [{"close": b[1], "high": b[2], "low": b[3], "volume": b[4]} for b in sub]
        feats = signal_day_features(bars)
        return tier_for(feats, avg_lift, tier_rules)

    def run_pipeline(promotions, tag):
        results = {b: dict(day_rows=[], stock_rows=[]) for b in ("T3", "T15")}
        n_days = 0; skipped = 0; t1 = time.time()
        for ed in entry_dates:
            sd = sig_of.get(ed)
            if sd is None:
                skipped += 1; continue
            Ysig = int(sd[:4])
            if Ysig not in promotions:
                skipped += 1; continue
            picks = rank_for_date(pcon, promotions[Ysig], sd)
            if not picks:
                skipped += 1; continue
            for p in picks[:15]:
                p["tier"] = tier_of(p["symbol"], sd, p["avg_lift"])
            for p in picks:
                p.setdefault("tier", None)
            t3 = [p for p in picks if p["rank"] <= 3]
            t15 = [p for p in picks if p["rank"] <= 15 and p["tier"] in HIGH_TIER]
            for bname, basket in (("T3", t3), ("T15", t15)):
                res = run_basket(rcon, ed, basket, CFG[bname])
                if res is None:
                    continue
                day_row, stock_rows = res
                day_row["signal_date"] = sd
                for s in stock_rows:
                    s["signal_date"] = sd
                results[bname]["day_rows"].append(day_row)
                results[bname]["stock_rows"].append(stock_rows)
            n_days += 1
        print(f"[signals:{tag}] traded {n_days} entry-days, skipped {skipped} ({time.time()-t1:.0f}s)", flush=True)
        return results

    res_pit = run_pipeline(pit, "PIT")
    res_la = run_pipeline(la, "LA")

    spot = spot_check(pit, la)
    pcon.close(); rcon.close()

    write_workbook(res_pit, res_la, stats_pit, stats_la, spot, entry_dates)
    print(f"[done] total {time.time()-T0:.0f}s", flush=True)
    print_summary(res_pit, res_la, stats_pit, stats_la, spot)


def spot_check(pit, la):
    """Same pattern, same candidate pool: look-ahead lift (all years) vs point-in-time
    lift (years < Y). Confirms the promotion weight actually differs."""
    la_by = {}
    for Y_ in TEST_YEARS:
        la_by[Y_] = {p["pattern_id"]: p for p in la[Y_]}
    out = []
    for Y_ in TEST_YEARS:
        for p in pit[Y_]:
            lp = la_by[Y_].get(p["pattern_id"])
            if lp is not None:
                out.append(dict(test_year=Y_, pattern_id=p["pattern_id"], mined_year=p["mined_year"],
                                target=p["target"], lookahead_lift=lp["oos_lift"],
                                pointintime_lift=p["oos_lift"],
                                diff=round(p["oos_lift"] - lp["oos_lift"], 4)))
                break
    return out


# =============================================================================
# 7) Workbook
# =============================================================================
def _monthly(day_rows):
    by = defaultdict(list)
    for r in day_rows:
        by[r["month"]].append(r)
    rows = []; cum = 0.0
    for ym in sorted(by):
        rs = by[ym]
        r5 = sum(r["ret5x"] for r in rs); r1 = sum(r["ret1x"] for r in rs)
        pnl = sum(r["net_rs"] for r in rs); pos = sum(1 for r in rs if r["net_rs"] > 0)
        cum += r5
        rows.append(dict(month=ym, days=len(rs), days_pos=pos,
                         ret5x=round(r5, 1), ret1x=round(r1, 1),
                         pnl_rs=round(pnl, 0), cum5x=round(cum, 1)))
    return rows

def _year_summary(day_rows):
    out = {}
    by_year = defaultdict(list)
    for r in day_rows:
        by_year[r["month"][:4]].append(r)
    for y, rs in by_year.items():
        months = _monthly(rs)
        out[y] = dict(total5x=round(sum(r["ret5x"] for r in rs), 1),
                      total1x=round(sum(r["ret1x"] for r in rs), 1),
                      pnl=round(sum(r["net_rs"] for r in rs), 0),
                      days=len(rs), days_pos=sum(1 for r in rs if r["net_rs"] > 0),
                      pos_months=sum(1 for m in months if m["ret5x"] > 0),
                      total_months=len(months),
                      worst_month=min((m["ret5x"] for m in months), default=0.0))
    # overall
    months = _monthly(day_rows)
    out["ALL"] = dict(total5x=round(sum(r["ret5x"] for r in day_rows), 1),
                      total1x=round(sum(r["ret1x"] for r in day_rows), 1),
                      pnl=round(sum(r["net_rs"] for r in day_rows), 0),
                      days=len(day_rows), days_pos=sum(1 for r in day_rows if r["net_rs"] > 0),
                      pos_months=sum(1 for m in months if m["ret5x"] > 0),
                      total_months=len(months),
                      worst_month=min((m["ret5x"] for m in months), default=0.0))
    return out


def write_workbook(res_pit, res_la, stats_pit, stats_la, spot, entry_dates):
    results = res_pit           # headline deliverable = point-in-time
    promo_stats = stats_pit
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    F = "Calibri"
    TITLE = Font(name=F, bold=True, size=13, color="1F6F8B")
    NOTE = Font(name=F, size=9, italic=True, color="666666")
    H = Font(name=F, bold=True, size=9, color="FFFFFF"); HDR = PatternFill("solid", start_color="1F6F8B")
    NORM = Font(name=F, size=10); BOLD = Font(name=F, bold=True, size=10)
    GRN = Font(name=F, size=10, color="1E7E34"); RED = Font(name=F, size=10, color="B00020")
    GRNF = PatternFill("solid", start_color="E7F4EA"); REDF = PatternFill("solid", start_color="FDECEA")
    Cc = Alignment(horizontal="center"); Rr = Alignment(horizontal="right"); Ll = Alignment(horizontal="left")
    thin = Side(style="thin", color="DDDDDD"); BORD = Border(left=thin, right=thin, top=thin, bottom=thin)
    RS = '#,##0;[Red]-#,##0'
    wb = Workbook(); first = True

    def sheet(name):
        nonlocal first
        if first:
            ws = wb.active; ws.title = name; first = False
        else:
            ws = wb.create_sheet(name)
        return ws

    # ---- Summary ----
    ws = sheet("Summary")
    ws.cell(1, 1, "Falcon Intraday-Basket — TRUE point-in-time walk-forward").font = TITLE
    ws.cell(2, 1, "Point-in-time promotion: candidates mined_year in [Y-4..Y-1]; OOS validation years < Y only. "
                  "Signals=frozen replay logic on PROD features. Tier=frozen live rulebook. "
                  "Intraday: 09:15 open, basket trail on CAPITAL basis 5x MIS, square-off 15:29, Zerodha cost. "
                  f"P&L window {entry_dates[0]}..{entry_dates[-1]} (1-min data starts 2024-05-13).").font = NOTE
    ws.cell(3, 1, "Trail configs (arm/give/floor/stop, on capital): Top-3 = 3/5/2/3 ; Top-15 high-tier = 5/3/0.5/3. "
                  "ret5x = net P&L / Rs5,00,000 ; ret1x = ret5x / 5.").font = NOTE
    r = 5
    heads = ["Basket", "Scope", "total5x %", "total1x %", "P&L Rs (Rs5L)", "days", "days+", "+months", "months", "worst month %"]
    for bname in ("T3", "T15"):
        ys = _year_summary(results[bname]["day_rows"])
        ws.cell(r, 1, f"{'Top-3' if bname=='T3' else 'Top-15 high-tier'}").font = BOLD
        r += 1
        for c, h in enumerate(heads, 1):
            cell = ws.cell(r, c, h); cell.font = H; cell.fill = HDR; cell.border = BORD; cell.alignment = Cc
        r += 1
        for scope in ("2024", "2025", "2026", "ALL"):
            s = ys.get(scope)
            if not s:
                continue
            vals = [("Top-3" if bname == "T3" else "Top-15"), scope, s["total5x"], s["total1x"],
                    s["pnl"], s["days"], s["days_pos"], s["pos_months"], s["total_months"], s["worst_month"]]
            for c, v in enumerate(vals, 1):
                cell = ws.cell(r, c, v); cell.font = BOLD if scope == "ALL" else NORM
                cell.border = BORD; cell.alignment = Ll if c <= 2 else Rr
                if c == 5: cell.number_format = RS
                if c == 3: cell.font = GRN if v >= 0 else RED
            r += 1
        r += 1
    for i, w in enumerate([10, 8, 12, 12, 16, 7, 7, 9, 9, 14], 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    # ---- Promotion (point-in-time vs look-ahead counts, SAME candidate pool) ----
    wp = wb.create_sheet("Promotion")
    wp.cell(1, 1, "Pattern promotion per test year — point-in-time vs look-ahead (same candidate pool)").font = TITLE
    wp.cell(2, 1, "Candidate pool per year = mined_year in [Y-4..Y-1] (mining_window_years=4). "
                  "PIT validates on years < Y; LA validates on all years incl Y and later (the production leak). "
                  "For reference, PROD's live look-ahead promoted set = 865 patterns; the RES full-sample = 1,943.").font = NOTE
    heads = ["Test year", "Eligible candidates",
             "PIT promoted", "PIT signal-usable", "PIT max val yr",
             "LA promoted", "LA signal-usable", "LA max val yr"]
    for c, h in enumerate(heads, 1):
        cell = wp.cell(4, c, h); cell.font = H; cell.fill = HDR; cell.border = BORD; cell.alignment = Cc
    r = 5
    for Y_ in TEST_YEARS:
        s = stats_pit[Y_]; l = stats_la[Y_]
        vals = [Y_, s["candidates"], s["promoted"], s["after_drop"], s["max_val_year"],
                l["promoted"], l["after_drop"], l["max_val_year"]]
        for c, v in enumerate(vals, 1):
            cell = wp.cell(r, c, v); cell.font = NORM; cell.border = BORD; cell.alignment = Cc
        r += 1
    r += 2
    wp.cell(r, 1, "SPOT-CHECK: same pattern & pool, look-ahead lift (all years) vs point-in-time lift (years < Y)").font = BOLD
    r += 1
    sh = ["Test year", "pattern_id", "mined_year", "target", "lookahead_lift_pp", "pointintime_lift_pp", "diff_pp"]
    for c, h in enumerate(sh, 1):
        cell = wp.cell(r, c, h); cell.font = H; cell.fill = HDR; cell.border = BORD; cell.alignment = Cc
    r += 1
    for sc in spot:
        vals = [sc["test_year"], sc["pattern_id"], sc["mined_year"], sc["target"],
                sc["lookahead_lift"], sc["pointintime_lift"], sc["diff"]]
        for c, v in enumerate(vals, 1):
            cell = wp.cell(r, c, v); cell.font = NORM; cell.border = BORD; cell.alignment = Cc
        r += 1
    for i, w in enumerate([10, 18, 12, 16, 18, 20, 12], 1):
        wp.column_dimensions[get_column_letter(i)].width = max(12, w)

    # ---- LookAhead_Isolation (config-matched: LA vs PIT through identical pipeline) ----
    wl = wb.create_sheet("LookAhead_Isolation")
    wl.cell(1, 1, "How much was the look-ahead worth? (clean isolation)").font = TITLE
    wl.cell(2, 1, "Same candidate pool, same basket/tier/trail config, same days. The ONLY difference is the "
                  "validation window used to promote patterns: LA uses future years, PIT does not. "
                  "The LA->PIT drop is PURELY the promotion look-ahead.").font = NOTE
    heads = ["Basket", "Scope", "Look-ahead total5x %", "Point-in-time total5x %", "Retained %", "Look-ahead 'worth' (pp)"]
    for c, h in enumerate(heads, 1):
        cell = wl.cell(4, c, h); cell.font = H; cell.fill = HDR; cell.border = BORD; cell.alignment = Cc
    r = 5
    for bname in ("T3", "T15"):
        label = "Top-3" if bname == "T3" else "Top-15 high-tier"
        yl = _year_summary(res_la[bname]["day_rows"])
        yp = _year_summary(res_pit[bname]["day_rows"])
        for scope in ("2024", "2025", "2026", "ALL"):
            a = yl.get(scope); b = yp.get(scope)
            if not a or not b:
                continue
            ret = (b["total5x"] / a["total5x"] * 100) if a["total5x"] else 0.0
            vals = [label, scope, a["total5x"], b["total5x"], round(ret, 1),
                    round(a["total5x"] - b["total5x"], 1)]
            for c, v in enumerate(vals, 1):
                cell = wl.cell(r, c, v); cell.font = BOLD if scope == "ALL" else NORM
                cell.border = BORD; cell.alignment = Ll if c <= 2 else Rr
            r += 1
        r += 1
    for i, w in enumerate([16, 8, 20, 22, 12, 22], 1):
        wl.column_dimensions[get_column_letter(i)].width = w

    # ---- Comparison ----
    wc = wb.create_sheet("Comparison")
    wc.cell(1, 1, "Walk-forward vs hindsight (how much was look-ahead worth?)").font = TITLE
    wc.cell(2, 1, "Hindsight = docs/ops/FOUNDERS_BASKET_TRADELOG_2024-2026.xlsx (full-sample patterns weighted by lifts "
                  "measured THROUGH 2026 + survivorship). CAVEAT: Founders also used a proportional trail "
                  "(arm6/give-back50%); this walk-forward uses the spec's absolute arm/give/floor/stop trail. "
                  "So the gap reflects look-ahead pattern promotion AND the trail-config change; the dominant, "
                  "intended isolation is the promotion look-ahead.").font = NOTE
    heads = ["Basket", "Metric", "Hindsight (look-ahead)", "Walk-forward (point-in-time)", "Retained %"]
    for c, h in enumerate(heads, 1):
        cell = wc.cell(4, c, h); cell.font = H; cell.fill = HDR; cell.border = BORD; cell.alignment = Cc
    r = 5
    for bname in ("T3", "T15"):
        ys = _year_summary(results[bname]["day_rows"])["ALL"]
        hs = HINDSIGHT[bname]
        label = "Top-3" if bname == "T3" else "Top-15 high-tier"
        pairs = [("total5x %", hs["total5x"], ys["total5x"]),
                 ("total1x %", hs["total1x"], ys["total1x"]),
                 ("P&L Rs", hs["pnl"], ys["pnl"]),
                 ("+months", hs["pos_months"], ys["pos_months"]),
                 ("worst month %", hs["worst_month"], ys["worst_month"])]
        for metric, hv, wv in pairs:
            ret = (wv / hv * 100) if hv else 0.0
            vals = [label, metric, hv, wv, round(ret, 1)]
            for c, v in enumerate(vals, 1):
                cell = wc.cell(r, c, v); cell.font = NORM; cell.border = BORD; cell.alignment = Ll if c <= 2 else Rr
                if c in (3, 4) and metric == "P&L Rs": cell.number_format = RS
            r += 1
        r += 1
    for i, w in enumerate([16, 16, 22, 28, 12], 1):
        wc.column_dimensions[get_column_letter(i)].width = w

    # ---- per-basket monthly + trades ----
    for bname in ("T3", "T15"):
        label = "Top3" if bname == "T3" else "Top15"
        wm = wb.create_sheet(f"{label}_Monthly")
        mh = ["month", "days", "days_pos", "ret5x_pct", "ret1x_pct", "pnl_rs", "cum5x_pct"]
        for c, h in enumerate(mh, 1):
            cell = wm.cell(1, c, h); cell.font = H; cell.fill = HDR; cell.border = BORD; cell.alignment = Cc
        for ri, m in enumerate(_monthly(results[bname]["day_rows"]), 2):
            vals = [m["month"], m["days"], m["days_pos"], m["ret5x"], m["ret1x"], m["pnl_rs"], m["cum5x"]]
            for c, v in enumerate(vals, 1):
                cell = wm.cell(ri, c, v); cell.font = NORM; cell.alignment = Cc
                if c == 6: cell.number_format = RS
                if c == 4: cell.font = GRN if v >= 0 else RED
        for i, w in enumerate([10, 6, 9, 11, 11, 14, 11], 1):
            wm.column_dimensions[get_column_letter(i)].width = w
        wm.freeze_panes = "A2"

        wt = wb.create_sheet(f"{label}_Trades")
        th = ["entry_date", "signal_date", "symbol", "rank", "tier", "entry_time", "entry_px",
              "exit_time", "exit_px", "stock_ret_pct", "exit_reason", "basket_ret5x_pct", "day_pnl_rs"]
        for c, h in enumerate(th, 1):
            cell = wt.cell(1, c, h); cell.font = H; cell.fill = HDR; cell.border = BORD; cell.alignment = Cc
        ri = 2
        for srows in results[bname]["stock_rows"]:
            for s in srows:
                vals = [s["entry_date"], s["signal_date"], s["symbol"], s["rank"], s["tier"],
                        s["entry_time"], s["entry_px"], s["exit_time"], s["exit_px"],
                        s["stock_ret_pct"], s["exit_reason"], s["basket_ret5x_pct"], s["day_pnl_rs"]]
                for c, v in enumerate(vals, 1):
                    cell = wt.cell(ri, c, v); cell.font = NORM
                    cell.alignment = Ll if c in (1, 2, 3, 5, 6, 8, 11) else Rr
                    if c == 13: cell.number_format = RS
                if s["day_pnl_rs"] > 0:
                    for c in range(1, 14): wt.cell(ri, c).fill = GRNF
                elif s["day_pnl_rs"] < 0:
                    for c in range(1, 14): wt.cell(ri, c).fill = REDF
                ri += 1
        for i, w in enumerate([11, 11, 12, 5, 20, 10, 10, 9, 9, 12, 11, 15, 12], 1):
            wt.column_dimensions[get_column_letter(i)].width = w
        wt.freeze_panes = "A2"; wt.auto_filter.ref = f"A1:M{ri-1}"

    out = ROOT / "docs" / "ops" / "FALCON_WALKFORWARD_2024-2026.xlsx"
    wb.save(out)
    print(f"[xlsx] wrote {out}", flush=True)
    try:
        dl = Path.home() / "Downloads" / out.name
        shutil.copy(out, dl); print(f"[xlsx] copy -> {dl}", flush=True)
    except Exception as e:
        print(f"[xlsx] Downloads copy failed: {e}", flush=True)


def print_summary(res_pit, res_la, stats_pit, stats_la, spot):
    print("\n" + "=" * 82)
    print("WALK-FORWARD SUMMARY")
    print("=" * 82)
    for Y_ in TEST_YEARS:
        s = stats_pit[Y_]; l = stats_la[Y_]
        print(f"  Y={Y_}: PIT promoted {s['promoted']} (usable {s['after_drop']}, max_val {s['max_val_year']})  |  "
              f"LA promoted {l['promoted']} (usable {l['after_drop']}, max_val {l['max_val_year']})")
    for bname in ("T3", "T15"):
        label = "Top-3" if bname == "T3" else "Top-15 high-tier"
        yp = _year_summary(res_pit[bname]["day_rows"])
        yl = _year_summary(res_la[bname]["day_rows"])
        print(f"\n  --- {label} (POINT-IN-TIME) ---")
        for scope in ("2024", "2025", "2026", "ALL"):
            s = yp.get(scope)
            if s:
                print(f"    {scope:4s}: total5x={s['total5x']:>8.1f}%  total1x={s['total1x']:>7.1f}%  "
                      f"P&L=Rs{s['pnl']:>13,.0f}  days={s['days']:>3d} +{s['days_pos']:<3d}  "
                      f"+months {s['pos_months']}/{s['total_months']}  worst {s['worst_month']:.1f}%")
        p = yp["ALL"]; a = yl["ALL"]; hs = HINDSIGHT[bname]
        print(f"    LOOK-AHEAD(config-matched) total5x={a['total5x']}%  ->  POINT-IN-TIME total5x={p['total5x']}%  "
              f"(retained {p['total5x']/a['total5x']*100:.1f}%; look-ahead worth {a['total5x']-p['total5x']:.0f}pp)")
        print(f"    FOUNDERS hindsight(ref, diff trail) total5x={hs['total5x']}%  (walk-fwd = {p['total5x']/hs['total5x']*100:.1f}% of it)")
    print("\n  SPOT-CHECK (same pattern & pool: look-ahead lift vs point-in-time lift):")
    for sc in spot:
        print(f"    Y={sc['test_year']} pid={sc['pattern_id']} mined={sc['mined_year']} {sc['target']}: "
              f"look-ahead={sc['lookahead_lift']}pp  point-in-time={sc['pointintime_lift']}pp  diff={sc['diff']}pp")


if __name__ == "__main__":
    main()
