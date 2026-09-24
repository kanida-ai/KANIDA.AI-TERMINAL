"""Per-stock, expanding WEEK-BY-WEEK walk-forward miner. Full daily-OHLCV indicator library (indicators.py).
Decision-tree leaf-rules -> Buy/Strong Buy/Sell/Strong Sell (tier from rolling LIVE accuracy per direction) +
forced daily direction. Signal at Day-T close, judged on T+1 open->close. Continuously re-mines each week (no
freeze). PARALLEL across stocks (multiprocessing); each stock mined week-by-week, no shortcut. Own DB.
Usage: python miner.py --limit 8   (subset) | python miner.py   (full universe)
"""
import os, sys, sqlite3, argparse, warnings, multiprocessing as mp
import numpy as np, pandas as pd
from sklearn.tree import DecisionTreeClassifier, _tree
warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import indicators as IND
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
UDB = os.path.join(ROOT, "data", "db", "kanida_universe.db")
HERE = os.path.dirname(os.path.abspath(__file__)); DB = os.path.join(HERE, "stock_miner.db")
FEATDIR = os.path.join(HERE, "features")          # per-stock point-in-time feature matrices (parquet)
MIN_TRAIN = 120; STRONG, NORMAL = 0.65, 0.55; DEPTH, LEAF = 3, 40
META = {"trade_date", "wk", "ret_oc", "y", "trade_date_next"}
FEATS = None


def _feats(D): return [c for c in D.columns if c not in META]
def safe(sym): return "".join(ch if ch.isalnum() or ch in "-._" else "_" for ch in sym)


def rule_text(tree, leaf_id, names):
    t = tree.tree_; found = []
    def rec(node, conds):
        if t.children_left[node] == _tree.TREE_LEAF:
            if node == leaf_id: found.extend(conds)
            return
        f = names[t.feature[node]]; th = round(float(t.threshold[node]), 2)
        rec(t.children_left[node], conds + [f"{f}<={th}"]); rec(t.children_right[node], conds + [f"{f}>{th}"])
    rec(0, []); return " & ".join(found)


def mine_one(sym):
    try:
        con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
        g = pd.read_sql_query("SELECT symbol,trade_date,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? AND trade_date>='2018-06-01' ORDER BY trade_date", con, params=[sym]); con.close()
        if len(g) < MIN_TRAIN + 60: return sym, [], [], [], []
        D = IND.compute_features(g).replace([np.inf, -np.inf], np.nan)
        feats = _feats(D); D = D[(D.trade_date >= "2020-01-01")].dropna(subset=feats + ["y", "ret_oc"])
        if len(D) < MIN_TRAIN + 20: return sym, [], [], [], []
        # persist the exact point-in-time feature matrix the trees saw (per stock, tagged by ISO-week)
        fcols = ["trade_date", "wk"] + feats + ["ret_oc", "y"]
        D[fcols].assign(symbol=sym).to_parquet(os.path.join(FEATDIR, safe(sym) + ".parquet"), index=False)
        weeks = sorted(D.wk.unique()); sig = []; pat = []; state = []; pw = []; hist = {"up": [], "down": []}
        for wi in range(1, len(weeks)):
            tw = weeks[wi]; tr = D[D.wk < tw]; te = D[D.wk == tw]
            if len(tr) < MIN_TRAIN or len(te) == 0: continue
            ytr = tr.y.values
            if ytr.sum() < 5 or ytr.sum() > len(ytr) - 5: continue
            Xtr = tr[feats].values
            clf = DecisionTreeClassifier(max_depth=DEPTH, min_samples_leaf=LEAF, random_state=0).fit(Xtr, ytr)
            leaf_tr = clf.apply(Xtr); ls = {}
            for lf in np.unique(leaf_tr):
                yl = ytr[leaf_tr == lf]; up = yl.mean(); ls[lf] = ("up" if up >= 0.5 else "down", max(up, 1 - up), int(len(yl)))
            leaf_rule = {lf: rule_text(clf, lf, feats) for lf in ls}
            for lf, (dr, acc, ntr) in ls.items():
                if acc >= NORMAL: pat.append((sym, int(tw), leaf_rule[lf], dr, round(acc, 3), ntr))
            # weekly MODEL STATE snapshot ENTERING this week (before scoring its rows)
            ur = hist["up"][-30:]; dnr = hist["down"][-30:]
            state.append((sym, int(tw), len(tr), len(te), round(float(ytr.mean()), 3), len(ls),
                          round(float(np.mean(ur)) if ur else 0.0, 3), round(float(np.mean(dnr)) if dnr else 0.0, 3),
                          len(hist["up"]), len(hist["down"])))
            leaves = clf.apply(te[feats].values); wk_leaf = {}
            for j, (_, row) in enumerate(te.iterrows()):
                if row.trade_date_next is None: continue
                lf = leaves[j]; dr = ls[lf][0]; tacc = ls[lf][1]; rec = hist[dr][-30:]; live = (np.mean(rec) if rec else 0.0)
                fdir = "Buy" if dr == "up" else "Sell"
                if len(rec) >= 15 and live >= STRONG: tier = "StrongBuy" if dr == "up" else "StrongSell"
                elif len(rec) >= 8 and live >= NORMAL: tier = "Buy" if dr == "up" else "Sell"
                else: tier = "Neutral"
                oc = row.ret_oc; sd = "up" if "Buy" in tier else ("down" if "Sell" in tier else None)
                cs = (1 if ((sd == "up" and oc > 0) or (sd == "down" and oc < 0)) else 0) if sd else None
                cf = 1 if ((dr == "up" and oc > 0) or (dr == "down" and oc < 0)) else 0
                dret = oc if dr == "up" else -oc                     # next-day return in the pattern's direction
                wk_leaf.setdefault(lf, []).append((dret, cf, 0 if tier == "Neutral" else 1))
                hist[dr].append(cf)
                sig.append((row.trade_date, row.trade_date_next, sym, tier, sd or "", fdir, round(float(live), 3), round(float(oc), 3), cs, cf, leaf_rule.get(lf, ""), round(float(tacc), 3)))
            # per-week realized OUTCOME of each pattern-stock pair (out-of-sample, this week)
            for lf, vals in wk_leaf.items():
                dr = ls[lf][0]; drets = [v[0] for v in vals]; cfs = [v[1] for v in vals]; selm = [v[2] for v in vals]
                sret = [d for d, s in zip(drets, selm) if s]         # filtered = tiered (non-Neutral) rows only
                pw.append((sym, int(tw), leaf_rule[lf], dr, len(vals), int(sum(selm)),
                           round(float(np.mean(drets)), 3), round(float(np.mean(sret)), 3) if sret else None,
                           int(sum(cfs)), int(len(cfs) - sum(cfs)), 1 if np.mean(drets) > 0 else 0, round(float(ls[lf][1]), 3)))
        return sym, sig, pat, state, pw
    except Exception as e:
        return sym, [("ERR", str(e)[:80])], [], [], []


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--limit", type=int, default=0); ap.add_argument("--workers", type=int, default=0)
    a = ap.parse_args()
    con = sqlite3.connect("file:" + UDB.replace("\\", "/") + "?mode=ro", uri=True)
    cnt = pd.read_sql_query("SELECT symbol, COUNT(*) n FROM ohlc_daily WHERE trade_date>='2020-01-01' GROUP BY symbol HAVING n>=200 ORDER BY n DESC", con); con.close()
    syms = cnt.symbol.tolist()
    if a.limit: syms = syms[:a.limit]
    nw = a.workers or max(2, (os.cpu_count() or 4) - 2)
    os.makedirs(FEATDIR, exist_ok=True)
    print(f"mining {len(syms)} stocks · {nw} workers · features=129 · week-by-week walk-forward · persisting features+state", flush=True)
    db = sqlite3.connect(DB)
    db.executescript("""DROP TABLE IF EXISTS signals; DROP TABLE IF EXISTS patterns; DROP TABLE IF EXISTS weekly_state;
    CREATE TABLE IF NOT EXISTS signals(signal_date TEXT,trade_date TEXT,symbol TEXT,tier TEXT,direction TEXT,forced_dir TEXT,leaf_acc REAL,ret_oc REAL,correct_sel INT,correct_forced INT,rule TEXT,leaf_train_acc REAL);
    CREATE TABLE IF NOT EXISTS patterns(symbol TEXT,mined_week INT,rule TEXT,direction TEXT,train_acc REAL,n_train INT);
    CREATE TABLE IF NOT EXISTS weekly_state(symbol TEXT,mined_week INT,n_train INT,n_test INT,up_rate_train REAL,n_leaves INT,up_live_acc REAL,down_live_acc REAL,up_seen INT,down_seen INT);
    CREATE TABLE IF NOT EXISTS pattern_weekly(symbol TEXT,mined_week INT,rule TEXT,direction TEXT,n_test INT,n_filtered INT,avg_next_ret REAL,avg_next_ret_filtered REAL,n_win INT,n_false_pos INT,pos_behavior INT,train_acc REAL);
    CREATE INDEX IF NOT EXISTS ix_sig ON signals(symbol,trade_date,tier);
    CREATE INDEX IF NOT EXISTS ix_pat ON patterns(symbol,mined_week);
    CREATE INDEX IF NOT EXISTS ix_state ON weekly_state(symbol,mined_week);
    CREATE INDEX IF NOT EXISTS ix_pw ON pattern_weekly(symbol,mined_week,pos_behavior);""")
    db.commit()
    done = 0; errs = 0
    with mp.Pool(nw) as pool:
        for sym, sig, pat, state, pw in pool.imap_unordered(mine_one, syms, chunksize=1):
            if sig and sig[0][0] == "ERR": errs += 1; print(f"  ERR {sym}: {sig[0][1]}", flush=True); continue
            if sig: db.executemany("INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", sig)
            if pat: db.executemany("INSERT INTO patterns VALUES (?,?,?,?,?,?)", pat)
            if state: db.executemany("INSERT INTO weekly_state VALUES (?,?,?,?,?,?,?,?,?,?)", state)
            if pw: db.executemany("INSERT INTO pattern_weekly VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", pw)
            db.commit(); done += 1
            if done % 25 == 0: print(f"  ...{done}/{len(syms)} stocks done", flush=True)
    db.close(); print(f"DONE: {done} stocks mined, {errs} errors", flush=True)
    S = pd.read_sql_query("SELECT tier,correct_sel,correct_forced FROM signals", sqlite3.connect(DB))
    if len(S):
        print(f"\ntotal signals {len(S):,}")
        for t in ["StrongBuy", "Buy", "Sell", "StrongSell"]:
            s = S[S.tier == t]
            if len(s): print(f"  {t:<12} n={len(s):>7} acc {s.correct_sel.mean()*100:.1f}%")
        print(f"  FORCED n={len(S):>7} acc {S.correct_forced.mean()*100:.1f}%")


if __name__ == "__main__":
    mp.freeze_support(); main()
