"""Per-stock pattern engine — the fine-tuning interface over the mined artifacts.
Loaders + two headline questions answered per stock on a weekly basis:
  Q1  which pattern-stock pairs had POSITIVE next-day average behavior (after the tier filter)
  Q2  which patterns FAILED, WHEN they failed, and how many FALSE POSITIVES

Data sources (all written by miner.py):
  stock_miner.db : signals · patterns · weekly_state · pattern_weekly
  features/<SYM>.parquet : full 129-feature point-in-time matrix per stock

CLI:  python engine.py TATACHEM        -> writes ~/Downloads/STOCK_<SYM>.xlsx
      python engine.py --winners       -> ranks best pattern-stock pairs across the whole universe
"""
import os, sys, sqlite3, argparse
import pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); DB = os.path.join(HERE, "stock_miner.db")
FEATDIR = os.path.join(HERE, "features"); DL = os.path.join(os.path.expanduser("~"), "Downloads")
def _safe(s): return "".join(c if c.isalnum() or c in "-._" else "_" for c in s)
def _con(): return sqlite3.connect(DB)


def features(sym):
    """Full point-in-time feature matrix (129 features + label) for one stock."""
    p = os.path.join(FEATDIR, _safe(sym) + ".parquet")
    return pd.read_parquet(p) if os.path.exists(p) else pd.DataFrame()


def patterns(sym):
    """Every weekly leaf-rule mined for the stock (training-time rule + train_acc)."""
    return pd.read_sql_query("SELECT * FROM patterns WHERE symbol=? ORDER BY mined_week", _con(), params=[sym])


def pattern_weekly(sym):
    """Realized OUT-OF-SAMPLE outcome of each pattern, per week."""
    return pd.read_sql_query("SELECT * FROM pattern_weekly WHERE symbol=? ORDER BY mined_week", _con(), params=[sym])


def weekly_state(sym):
    return pd.read_sql_query("SELECT * FROM weekly_state WHERE symbol=? ORDER BY mined_week", _con(), params=[sym])


def signals(sym):
    return pd.read_sql_query("SELECT signal_date,trade_date,tier,direction,leaf_acc live_state,rule,ret_oc,correct_sel "
                             "FROM signals WHERE symbol=? ORDER BY signal_date", _con(), params=[sym])


import re
def _rule_feats(rule):
    """Extract the indicator names a rule splits on (stable across weeks; the fine-tune lever)."""
    if not isinstance(rule, str) or not rule: return []
    return list(dict.fromkeys(re.split(r"\s*(?:<=|>=|<|>)\s*", part)[0].strip() for part in rule.split("&")))


def feature_rollup(pw):
    """Aggregate pattern_weekly to per-FEATURE behavior (recurs across weeks, real sample sizes)."""
    if pw.empty: return pw
    rows = []
    for _, r in pw.iterrows():
        for f in _rule_feats(r.rule):
            rows.append((f, r.direction, r.n_test, r.n_win, r.n_false_pos, r.avg_next_ret))
    e = pd.DataFrame(rows, columns=["feature", "direction", "n_test", "n_win", "n_false_pos", "avg_next_ret"])
    g = e.groupby(["feature", "direction"]).apply(lambda d: pd.Series(dict(
        pattern_weeks=len(d), n_test=int(d.n_test.sum()), n_win=int(d.n_win.sum()), n_false_pos=int(d.n_false_pos.sum()),
        win_rate=round(d.n_win.sum() / max(1, d.n_test.sum()) * 100, 1),
        avg_next_ret=round((d.avg_next_ret * d.n_test).sum() / max(1, d.n_test.sum()), 3))), include_groups=False).reset_index()
    return g[g.n_test >= 30].sort_values("avg_next_ret", ascending=False)


def _rollup(pw):
    """Aggregate a pattern_weekly frame to one row per (rule,direction) across all weeks."""
    if pw.empty: return pw
    g = pw.groupby(["rule", "direction"]).apply(lambda d: pd.Series(dict(
        weeks=len(d), weeks_positive=int((d.pos_behavior == 1).sum()),
        pct_weeks_pos=round((d.pos_behavior == 1).mean() * 100, 1),
        n_test=int(d.n_test.sum()), n_win=int(d.n_win.sum()), n_false_pos=int(d.n_false_pos.sum()),
        win_rate=round(d.n_win.sum() / max(1, d.n_test.sum()) * 100, 1),
        avg_next_ret=round((d.avg_next_ret * d.n_test).sum() / max(1, d.n_test.sum()), 3),  # n-weighted
    )), include_groups=False).reset_index()
    return g.sort_values("avg_next_ret", ascending=False)


def q1_positive(sym):
    """Q1 — pattern-stock pairs with positive next-day average behavior (weekly, after filter)."""
    r = _rollup(pattern_weekly(sym))
    return r[r.avg_next_ret > 0] if not r.empty else r


def q2_failed(sym):
    """Q2 — patterns that failed + when + false positives (weekly)."""
    pw = pattern_weekly(sym)
    roll = _rollup(pw); failed = roll[roll.avg_next_ret <= 0] if not roll.empty else roll
    # WHEN each rule failed (weeks with pos_behavior=0), with that week's false positives
    when = pw[pw.pos_behavior == 0][["mined_week", "rule", "direction", "n_test", "avg_next_ret", "n_false_pos"]] \
        .sort_values(["rule", "mined_week"]) if not pw.empty else pw
    return failed, when


def inspect(sym):
    xls = os.path.join(DL, f"STOCK_{_safe(sym)}.xlsx")
    pw = pattern_weekly(sym); roll = _rollup(pw)
    failed, when = q2_failed(sym)
    with pd.ExcelWriter(xls, engine="openpyxl") as w:
        (roll[roll.avg_next_ret > 0] if not roll.empty else roll).to_excel(w, "Q1_positive_patterns", index=False)
        failed.to_excel(w, "Q2_failed_patterns", index=False)
        when.to_excel(w, "Q2_failures_when", index=False)
        feature_rollup(pw).to_excel(w, "feature_edge", index=False)
        roll.to_excel(w, "all_patterns_rollup", index=False)
        pw.to_excel(w, "pattern_weekly", index=False)
        weekly_state(sym).to_excel(w, "weekly_state", index=False)
        signals(sym).to_excel(w, "signals", index=False)
        patterns(sym).to_excel(w, "rules_by_week", index=False)
        features(sym).to_excel(w, "features_pit", index=False)
    print(f"per-stock engine workbook -> {xls}")
    if not roll.empty:
        print(f"\n{sym}: {roll.weeks.sum()} pattern-weeks · {len(roll)} distinct rules · "
              f"{int((roll.avg_next_ret>0).sum())} net-positive rules / {int((roll.avg_next_ret<=0).sum())} net-negative")
        print("\ntop positive patterns:"); print(roll.head(6).to_string(index=False))
        print("\nworst (failed) patterns:"); print(roll.tail(4).to_string(index=False))


def universe_winners(min_test=80):
    """Rank best pattern-stock pairs across the universe. Feature-grain = stable (recurs); rule-grain = raw."""
    pw = pd.read_sql_query("SELECT * FROM pattern_weekly", _con())
    # feature-grain per stock (the fine-tune target list — real sample sizes)
    rows = []
    for _, r in pw.iterrows():
        for f in _rule_feats(r.rule):
            rows.append((r.symbol, f, r.direction, r.n_test, r.n_win, r.n_false_pos, r.avg_next_ret))
    e = pd.DataFrame(rows, columns=["symbol", "feature", "direction", "n_test", "n_win", "n_false_pos", "avg_next_ret"])
    feat = e.groupby(["symbol", "feature", "direction"]).apply(lambda d: pd.Series(dict(
        pattern_weeks=len(d), n_test=int(d.n_test.sum()), n_false_pos=int(d.n_false_pos.sum()),
        win_rate=round(d.n_win.sum() / max(1, d.n_test.sum()) * 100, 1),
        avg_next_ret=round((d.avg_next_ret * d.n_test).sum() / max(1, d.n_test.sum()), 3))), include_groups=False).reset_index()
    feat = feat[feat.n_test >= min_test].sort_values("avg_next_ret", ascending=False)
    # exact-rule grain (raw, mostly single-week) for reference
    rule = pw.groupby(["symbol", "rule", "direction"]).apply(lambda d: pd.Series(dict(
        weeks=len(d), n_test=int(d.n_test.sum()), n_false_pos=int(d.n_false_pos.sum()),
        avg_next_ret=round((d.avg_next_ret * d.n_test).sum() / max(1, d.n_test.sum()), 3))), include_groups=False).reset_index()
    rule = rule[rule.n_test >= 40].sort_values("avg_next_ret", ascending=False)
    out = os.path.join(DL, "PATTERN_STOCK_WINNERS.xlsx")
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        feat.head(1000).to_excel(w, "feature_winners", index=False)
        feat.tail(1000).to_excel(w, "feature_failures", index=False)
        rule.head(500).to_excel(w, "rule_winners_raw", index=False)
    print(f"universe ranking -> {out}  ({len(feat):,} stock-feature pairs >= {min_test} signals)")
    print("\ntop stock-feature edges (short/long):"); print(feat.head(12).to_string(index=False))
    print("\nworst stock-feature edges:"); print(feat.tail(6).to_string(index=False))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("symbol", nargs="?"); ap.add_argument("--winners", action="store_true")
    a = ap.parse_args()
    if a.winners: universe_winners()
    elif a.symbol: inspect(a.symbol.upper())
    else: print("usage: python engine.py <SYMBOL>   |   python engine.py --winners")
