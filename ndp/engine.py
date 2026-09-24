# -*- coding: utf-8 -*-
"""NDP engine — discovery -> selection -> evaluation on ICICIBANK, TOUCH basis.
Signal at close of T from 129 PIT daily+weekly features; outcome = the 1-min path tensor of session T+1.
Expanding weekly walk-forward (leak-free). Four questions (Buy>=.5 / Buy>=1 / Sell>=.5 / Sell>=1), each
mined as tree-leaf conditions, entry_time E* discovered per condition, evaluated once OOS. Emits the
win-rate scorecard with validity guards, plus the account simulation. Null calibration in nullcalib.py.
"""
import os, sys, sqlite3, warnings
import numpy as np, pandas as pd
from sklearn.tree import DecisionTreeClassifier, _tree
warnings.filterwarnings("ignore")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "stock_miner"))
import indicators as IND
from ndp import core

DAILY = os.path.join(ROOT, "data", "db", "kanida_universe.db")
NDB = os.path.join(ROOT, "ndp", "ndp.db")
ENTRY_TIMES = ["09:15", "09:20", "09:30", "09:45", "10:00"]
REF_ENTRY = "09:20"
QUESTIONS = [("Q1", "LONG", 0.5), ("Q2", "LONG", 1.0), ("Q3", "SHORT", 0.5), ("Q4", "SHORT", 1.0)]
CFG = dict(wr_bar=0.70, n_min=30, f_min=12, alpha=0.05, min_train_weeks=26,
           leaf_min=40, depth=3, precision_floor=0.55)
META = {"trade_date", "wk", "ret_oc", "y", "trade_date_next"}


def load_features(sym):
    con = sqlite3.connect("file:" + DAILY.replace("\\", "/") + "?mode=ro", uri=True)
    g = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily "
                          "WHERE symbol=? AND trade_date>='2016-01-01' ORDER BY trade_date", con, params=[sym]); con.close()
    D = IND.compute_features(g).replace([np.inf, -np.inf], np.nan)
    feats = [c for c in D.columns if c not in META]
    D = D.dropna(subset=feats)
    return D, feats


def load_outcomes(sym):
    """Path-tensor -> per entry_date, all entry_times: entry, close-return, TOUCH excursions, gap."""
    con = sqlite3.connect("file:" + NDB.replace("\\", "/") + "?mode=ro", uri=True)
    t = pd.read_sql_query("SELECT signal_date entry_date, entry_time, entry_price, exit_price, "
                          "mfe_long, mfe_short, gap_open_pct, first_min_range_pct "
                          "FROM path_tensor WHERE symbol=? AND horizon='INTRADAY'", con, params=[sym]); con.close()
    t["close_ret"] = (t.exit_price / t.entry_price - 1) * 100      # signed via direction later
    return t


def build_panel(sym):
    """One row per signal_date T with features[T] joined to session T+1 outcomes (all entry times, wide)."""
    D, feats = load_features(sym)
    out = load_outcomes(sym)
    sess = set(out.entry_date.unique())
    # signal at close of T -> enter the ACTUAL next trading day; keep only if that day is in the 1-min era
    tdates = sorted(D.trade_date.unique())
    nextday = {tdates[i]: tdates[i+1] for i in range(len(tdates) - 1)}
    D = D.copy(); D["entry_date"] = D.trade_date.map(nextday)
    D = D[D.entry_date.isin(sess)]
    wide = out.pivot_table(index="entry_date", columns="entry_time",
                           values=["entry_price", "close_ret", "mfe_long", "mfe_short", "first_min_range_pct"], aggfunc="first")
    wide.columns = [f"{a}@{b}" for a, b in wide.columns]; wide = wide.reset_index()
    P = D.merge(wide, on="entry_date", how="inner")
    return P, feats


def _leaf_rule(tree, leaf_id, names):
    t = tree.tree_; found = []
    def rec(node, conds):
        if t.children_left[node] == _tree.TREE_LEAF:
            if node == leaf_id: found.extend(conds); return
            return
        f = names[t.feature[node]]; th = round(float(t.threshold[node]), 3)
        rec(t.children_left[node], conds + [f"{f}<={th}"]); rec(t.children_right[node], conds + [f"{f}>{th}"])
    rec(0, []); return " & ".join(found)


def hit_label(P, direction, thr, entry):
    col = f"mfe_long@{entry}" if direction == "LONG" else f"mfe_short@{entry}"
    return (P[col] >= thr).astype(int)


def dir_return(P, direction, entry):
    r = P[f"close_ret@{entry}"]
    return r if direction == "LONG" else -r


def walk_forward(P, feats, qid, direction, thr):
    """Expanding weekly walk-forward. Returns per-OOS-row records for this question."""
    P = P.sort_values("trade_date").reset_index(drop=True)
    P["wk"] = P.trade_date.str.slice(0, 4) + P.trade_date.map(lambda d: f"{pd.Timestamp(d).isocalendar().week:02d}")
    weeks = sorted(P.wk.unique())
    recs = []
    for wi in range(CFG["min_train_weeks"], len(weeks)):
        tw = weeks[wi]; tr = P[P.wk < tw]; te = P[P.wk == tw]
        if len(tr) < CFG["leaf_min"] * 3 or len(te) == 0: continue
        yv = hit_label(tr, direction, thr, REF_ENTRY).values
        if yv.sum() < 10 or yv.sum() > len(yv) - 10: continue
        clf = DecisionTreeClassifier(max_depth=CFG["depth"], min_samples_leaf=CFG["leaf_min"], random_state=0).fit(tr[feats].values, yv)
        leaf_tr = clf.apply(tr[feats].values)
        # precision of each leaf on training; keep leaves that predict the hit with precision >= floor
        good = {}
        for lf in np.unique(leaf_tr):
            m = leaf_tr == lf; prec = yv[m].mean()
            if prec >= CFG["precision_floor"] and m.sum() >= CFG["leaf_min"]:
                # E* : pick entry_time maximising training Delta_mu (dir return minus cost) for this leaf
                best_et, best_dm = REF_ENTRY, -1e9
                for et in ENTRY_TIMES:
                    dr = dir_return(tr[m], direction, et).values
                    fmr = tr[m][f"first_min_range_pct@{et}"].values
                    cst = np.array([core.cost_pct("MIS", et, x) for x in fmr])
                    dm = np.nanmean(dr - cst)
                    if dm > best_dm: best_dm, best_et = dm, et
                good[lf] = (_leaf_rule(clf, lf, feats), best_et, prec)
        if not good: continue
        leaf_te = clf.apply(te[feats].values)
        for j in range(len(te)):
            lf = leaf_te[j]
            if lf not in good: continue
            rule, et, prec = good[lf]
            row = te.iloc[j]
            hit_t = int(row[f"mfe_long@{et}"] >= thr) if direction == "LONG" else int(row[f"mfe_short@{et}"] >= thr)
            cret = row[f"close_ret@{et}"] * (1 if direction == "LONG" else -1)
            hit_c = int(cret >= thr)
            fmr = row[f"first_min_range_pct@{et}"]
            cst = core.cost_pct("MIS", et, fmr)
            recs.append(dict(qid=qid, direction=direction, thr=thr, rule=rule, entry=et, wk=tw,
                             trade_date=row.trade_date, entry_date=row.entry_date,
                             hit_touch=hit_t, hit_close=hit_c, dir_ret=cret, cost=cst, net=cret - cst,
                             mfe=row[f"mfe_long@{et}"] if direction == "LONG" else row[f"mfe_short@{et}"]))
    return pd.DataFrame(recs)


def base_rate(P, direction, thr, entry=REF_ENTRY):
    return hit_label(P, direction, thr, entry).mean()


def _isoweek(s):
    return s.str.slice(0, 4) + s.map(lambda d: f"{pd.Timestamp(d).isocalendar().week:02d}")


def discover_grid(P, feats, qid, direction, thr, n_cycles=3, top_k=60, min_fire=20):
    """Stable-identity discovery: indicator+threshold conditions with a FROZEN vocabulary (percentiles of
    the earliest train window), expanding walk-forward cycles, E* discovered per condition, evaluated OOS.
    Conditions persist across the whole OOS so n accumulates -> DEPLOY is reachable. Answers the 4 questions
    literally ('which indicators and parameter combinations predict...'). Returns recs like walk_forward()."""
    P = P.sort_values("trade_date").reset_index(drop=True); P["wk"] = _isoweek(P.trade_date)
    weeks = sorted(P.wk.unique()); nw = len(weeks)
    if nw < 20: return pd.DataFrame()
    start = int(0.40 * nw)
    bounds = [start] + [start + (nw - start) * k // n_cycles for k in range(1, n_cycles + 1)]
    voc_rows = P[P.wk.isin(weeks[:start])]
    vocab = {f: np.unique(np.round(np.nanpercentile(voc_rows[f].values, [20, 35, 50, 65, 80]), 3)) for f in feats}
    recs = []
    for cyc in range(n_cycles):
        tr = P[P.wk.isin(weeks[:bounds[cyc]])]; te = P[P.wk.isin(weeks[bounds[cyc]:bounds[cyc + 1]])]
        if len(tr) < 60 or len(te) == 0: continue
        base_net = dir_return(tr, direction, REF_ENTRY).values / 100.0 - \
            np.array([core.cost_pct("MIS", REF_ENTRY, x) for x in tr[f"first_min_range_pct@{REF_ENTRY}"].values]) / 100.0
        mu0 = np.nanmean(base_net)
        trret = dir_return(tr, direction, REF_ENTRY).values / 100.0
        cand = []
        for f in feats:
            fv = tr[f].values
            for v in vocab[f]:
                for op in (">", "<="):
                    m = fv > v if op == ">" else fv <= v
                    if m.sum() < min_fire: continue
                    dm = np.nanmean(trret[m]) - mu0
                    cand.append((dm, f"{f}{op}{v:.3f}", f, op, v))
        cand.sort(reverse=True); survivors = cand[:top_k]
        for _, ident, f, op, v in survivors:
            # E*: entry maximising train net Delta_mu for this condition
            trm = (tr[f].values > v) if op == ">" else (tr[f].values <= v)
            best_et, best = REF_ENTRY, -1e9
            for et in ENTRY_TIMES:
                dr = dir_return(tr[trm], direction, et).values / 100.0
                cst = np.array([core.cost_pct("MIS", et, x) for x in tr[trm][f"first_min_range_pct@{et}"].values]) / 100.0
                mm = np.nanmean(dr - cst)
                if mm > best: best, best_et = mm, et
            tem = (te[f].values > v) if op == ">" else (te[f].values <= v)
            sub = te[tem]
            for _, row in sub.iterrows():
                cret = row[f"close_ret@{best_et}"] * (1 if direction == "LONG" else -1)
                ht = int(row[f"mfe_long@{best_et}"] >= thr) if direction == "LONG" else int(row[f"mfe_short@{best_et}"] >= thr)
                cst = core.cost_pct("MIS", best_et, row[f"first_min_range_pct@{best_et}"])
                recs.append(dict(qid=qid, direction=direction, thr=thr, rule=ident, entry=best_et, wk=row.wk,
                                 trade_date=row.trade_date, entry_date=row.entry_date, hit_touch=ht,
                                 hit_close=int(cret >= thr), dir_ret=cret, cost=cst, net=cret - cst,
                                 mfe=row[f"mfe_long@{best_et}"] if direction == "LONG" else row[f"mfe_short@{best_et}"]))
    return pd.DataFrame(recs)


def evaluate_conditions(recs, P, base, null_p95_wr):
    """Aggregate per (rule, entry) across OOS weeks -> scorecard rows with guards + gate."""
    if recs.empty: return pd.DataFrame()
    rows = []
    for (rule, et), d in recs.groupby(["rule", "entry"]):
        n = len(d); wins = int(d.hit_touch.sum()); wr = d.hit_touch.mean()
        wr_close = d.hit_close.mean()
        net = d.net.values; sd = np.std(d.dir_ret.values) / 100.0
        # direction-specific baseline mu_0: unconditional net at same entry/direction
        dirn = d.direction.iloc[0]; thr = d.thr.iloc[0]
        mu_c = np.mean(net) / 100.0
        mu_c_lcb = core.block_bootstrap_lcb(net / 100.0, alpha=CFG["alpha"])
        # baseline over ALL panel days, same direction/entry
        b_ret = dir_return(P, dirn, et).values / 100.0
        b_fmr = P[f"first_min_range_pct@{et}"].values
        b_cost = np.array([core.cost_pct("MIS", et, x) for x in b_fmr]) / 100.0
        b_net = b_ret - b_cost
        mu_0 = np.nanmean(b_net)
        delta = (net / 100.0) - mu_0
        delta_lcb = core.block_bootstrap_lcb(delta, alpha=CFG["alpha"])
        # target-attainment lift Delta_p
        p_c = wr; p_0 = base
        # bootstrap LCB on delta_p via the hit vector minus base
        dp = d.hit_touch.values - p_0
        delta_p_lcb = core.block_bootstrap_lcb(dp, alpha=CFG["alpha"])
        neff = core.n_effective(n, 1.0)  # intraday, non-overlapping same-day trades
        tpy = n / max((pd.to_datetime(recs.trade_date.max()) - pd.to_datetime(recs.trade_date.min())).days / 365.25, 0.1)
        mde = core.mde_80(sd, neff)
        pch = core.wr_p_chance(wins, n, base)
        # stress 2x
        stress_net = d.dir_ret.values / 100.0 - np.array([core.cost_pct("MIS", et, f, 2.0) for f in
                                                          (d.mfe*0 + P[f"first_min_range_pct@{et}"].mean())]) / 100.0
        stress_ok = core.block_bootstrap_lcb(stress_net, alpha=CFG["alpha"]) > 0 if n >= CFG["n_min"] else False
        g = dict(wr_oos=wr, base_rate=base, n_eff=neff, mu_c_lcb=mu_c_lcb, delta_mu_lcb=delta_lcb,
                 delta_p_lcb=delta_p_lcb, trades_per_year=tpy, null_p95_wr=null_p95_wr,
                 frozen_before_eval=True, stress_2x_passed=bool(stress_ok))
        tier, result, reasons = core.gate(g, CFG)
        rows.append(dict(rule=rule, entry=et, direction=dirn, thr=thr, n=n, wr_touch=round(wr*100, 1),
                         wr_close=round(wr_close*100, 1), base=round(base*100, 1),
                         net_bps=round(np.mean(net)*100, 1), avg_win=round(d[d.net > 0].net.mean(), 3) if (d.net > 0).any() else 0,
                         avg_loss=round(d[d.net <= 0].net.mean(), 3) if (d.net <= 0).any() else 0,
                         mu_c_lcb=round(mu_c_lcb*1e4, 1), delta_mu_lcb=round(delta_lcb*1e4, 1),
                         delta_p_lcb=round(delta_p_lcb, 3), p_chance=round(pch, 4), mde_bps=round(mde, 1),
                         null_p95_wr=round(null_p95_wr*100, 1), tpy=round(tpy, 1), tier=tier, result=result,
                         reasons="|".join(reasons)))
    return pd.DataFrame(rows).sort_values("wr_touch", ascending=False)
