"""
KANIDA.AI Phase-1 — stock-specific pattern mining (outcome-first, point-in-time, leak-free).
Per stock, per forward outcome target: discover COMBINATION rules (auto, via shallow random
forests) over point-in-time daily/weekly/intraday/ATP features, then keep only rules that
HOLD OUT-OF-SAMPLE. Writes to KANIDA_SNR.db.mined_patterns.

Outcome targets (touch within window): UP high>=+X% / DN low<=-X% over N days.
Train 2020-2024 (mine)  ->  Val 2025 (promote if holds)  ->  2026 stays sealed for later.
promoted=1 == held OOS on 2025 (the stock's 'Keep' set).

Run: PYTHONIOENCODING=utf-8 python mine_phase1.py [ADANIENT CARTRADE]
"""
import sys, os, json, sqlite3, hashlib
from pathlib import Path
from datetime import datetime
import numpy as np, pandas as pd
from sklearn.ensemble import RandomForestClassifier

RF_NJOBS = int(os.environ.get("KANIDA_RF_NJOBS", "-1"))   # set 1 in parallel workers to avoid oversubscription

DB = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\kanida.db")
SNR = Path(r"C:\Users\SPS\Documents\Kanida_Falcon\db\KANIDA_SNR.db")
STOCKS = sys.argv[1:] or ["ADANIENT", "CARTRADE"]
TARGETS = [("up_1pct_1d", "up", 1.0, 1), ("up_2pct_2d", "up", 2.0, 2), ("up_5pct_5d", "up", 5.0, 5),
           ("dn_1pct_1d", "dn", 1.0, 1), ("dn_2pct_2d", "dn", 2.0, 2), ("dn_5pct_5d", "dn", 5.0, 5)]
TRAIN_MAX, VAL_YEAR, MINED_YEAR = 2024, 2025, 2024
MIN_TR_OBS, MIN_TR_LIFT = 40, 5.0          # train gate (occurrence, lift in pp)
MIN_VA_OBS, MIN_VA_LIFT = 15, 2.0          # val gate to promote (Keep)


def log(m): print(f"{datetime.now():%H:%M:%S} {m}", flush=True)


def features(stock):
    con = sqlite3.connect(str(DB))
    d = pd.read_sql("SELECT bar_time,open,high,low,close,volume FROM ohlc_daily WHERE symbol=? ORDER BY bar_time",
                    con, params=[stock])
    mk = pd.read_sql("SELECT bar_time,close FROM ohlc_daily WHERE symbol='NIFTY 50' ORDER BY bar_time", con)
    # intraday-derived daily features from 1-min (incl true day-VWAP via last atp)
    mn = pd.read_sql("SELECT bar_time,open,high,low,close,volume,atp FROM ohlc_1min WHERE symbol=? ORDER BY bar_time",
                     con, params=[stock]); con.close()
    d["date"] = pd.to_datetime(d["bar_time"]); d = d.set_index("date")
    O, H, L, C, V = d["open"], d["high"], d["low"], d["close"], d["volume"]
    mk["date"] = pd.to_datetime(mk["bar_time"]); MK = mk.set_index("date")["close"].reindex(C.index).ffill()
    pc = C.shift(1); TR = np.maximum(H - L, np.maximum((H - pc).abs(), (L - pc).abs()))
    F = pd.DataFrame(index=C.index)
    F["atr_20_pct"] = TR.rolling(20).mean() / C * 100
    F["atr_5_vs_20"] = TR.rolling(5).mean() / TR.rolling(20).mean()
    F["close_loc"] = (C - L) / (H - L).replace(0, np.nan)
    F["gap_pct"] = (O / pc - 1) * 100
    F["body_pct"] = (C - O).abs() / (H - L).replace(0, np.nan)
    F["vol_vs_20d"] = V / V.rolling(20).mean()
    F["vol_5d_vs_20d"] = V.rolling(5).mean() / V.rolling(20).mean()
    for n in (2, 5, 10, 20, 60): F[f"roc_{n}"] = (C / C.shift(n) - 1) * 100
    for n in (10, 20, 60, 120, 252): F[f"dist_high_{n}"] = (C / H.rolling(n).max() - 1) * 100
    for n in (20, 50, 200): F[f"dist_sma_{n}"] = (C / C.rolling(n).mean() - 1) * 100
    for n in (20, 50): sma = C.rolling(n).mean(); F[f"slope_sma_{n}"] = (sma / sma.shift(5) - 1) * 100
    dl = C.diff(); F["rsi_14"] = 100 - 100 / (1 + dl.clip(lower=0).rolling(14).mean() / (-dl.clip(upper=0)).rolling(14).mean())
    F["rs_market_20d"] = ((C / C.shift(20) - 1) - (MK / MK.shift(20) - 1)) * 100
    F["rs_market_60d"] = ((C / C.shift(60) - 1) - (MK / MK.shift(60) - 1)) * 100
    rgp = (H - L) / pc * 100
    F["n_sub_3_range_7d"] = (rgp < 3).rolling(7).sum()
    F["n_higher_highs_5d"] = (H > H.shift(1)).rolling(5).sum()
    F["n_higher_lows_5d"] = (L > L.shift(1)).rolling(5).sum()
    lv = (V < 0.75 * V.rolling(20).mean()); F["n_sub_75v_7d"] = lv.rolling(7).sum()
    wk = C.index.to_period("W")
    F["weekly_close_loc"] = (C - L.groupby(wk).cummin()) / (H.groupby(wk).cummax() - L.groupby(wk).cummin()).replace(0, np.nan)
    F["weekly_range_pct"] = (H.groupby(wk).cummax() - L.groupby(wk).cummin()) / C * 100
    # intraday-derived (as-of close t): from 1-min
    if len(mn) > 1000:
        mn["dt"] = pd.to_datetime(mn["bar_time"]); mn["day"] = mn["dt"].dt.normalize(); mn["hm"] = mn["dt"].dt.strftime("%H:%M")
        g = mn.groupby("day")
        dvwap = g["atp"].last()                     # true day VWAP = last running-ATP of the day
        dop = g["open"].first(); dcl = g["close"].last(); dvol = g["volume"].sum()
        f30 = mn[mn["hm"] <= "09:44"].groupby("day")["close"].last()
        volfh = mn[mn["hm"] <= "10:14"].groupby("day")["volume"].sum()
        idf = pd.DataFrame(index=dop.index)
        idf["id_close_vs_vwap"] = (dcl / dvwap - 1) * 100
        idf["id_first30_ret"] = (f30 / dop - 1) * 100
        idf["id_volfh_pct"] = (volfh / dvol) * 100
        idf.index = pd.to_datetime(idf.index)
        for c in idf.columns: F[c] = idf[c].reindex(F.index)
    # all features are as-of close t (point-in-time) -> shift NOT needed; we predict t+1..t+w
    F["_close"] = C; F["_high"] = H; F["_low"] = L; F["year"] = F.index.year
    return F.dropna(how="all")


def label(F, direction, pct, w):
    C, H, L = F["_close"], F["_high"], F["_low"]
    fwd_hi = H.shift(-1).rolling(w).max().shift(-(w - 1)) if w > 1 else H.shift(-1)
    fwd_lo = L.shift(-1).rolling(w).min().shift(-(w - 1)) if w > 1 else L.shift(-1)
    if direction == "up":
        return ((fwd_hi / C - 1) * 100 >= pct).astype(float)
    return ((fwd_lo / C - 1) * 100 <= -pct).astype(float)


def leaf_rules(forest, feats):
    out = []
    for est in forest.estimators_:
        t = est.tree_
        def rec(node, conds):
            if t.feature[node] != -2:
                f = feats[t.feature[node]]; thr = float(t.threshold[node])
                rec(t.children_left[node], conds + [(f, "<=", round(thr, 4))])
                rec(t.children_right[node], conds + [(f, ">", round(thr, 4))])
            else:
                if conds: out.append(conds)
        rec(0, [])
    return out


def apply_rule(df, conds):
    m = pd.Series(True, index=df.index)
    for f, op, thr in conds:
        x = df[f]
        m &= (x <= thr) if op == "<=" else (x > thr)
    return m.fillna(False)


def stats(df, y, mask):
    n = int(mask.sum())
    if n == 0: return n, 0, 0.0
    hits = int(y[mask].sum()); return n, hits, hits / n * 100


def mine_stock(stock, con_snr):
    log(f"=== {stock} ===")
    F = features(stock)
    feats = [c for c in F.columns if not c.startswith("_") and c != "year"]
    Xall = F[feats].replace([np.inf, -np.inf], np.nan).fillna(0)
    tr = F["year"] <= TRAIN_MAX; va = F["year"] == VAL_YEAR
    total_mined = total_promoted = 0
    pid = 0
    for tname, direction, pct, w in TARGETS:
        y = label(F, direction, pct, w)
        valid = y.notna()
        ytr = y[tr & valid]; yva = y[va & valid]
        Xtr = Xall[tr & valid]
        if len(ytr) < 200 or ytr.nunique() < 2 or len(yva) < 40:
            log(f"  {tname}: insufficient data"); continue
        base_tr = ytr.mean() * 100; base_va = yva.mean() * 100
        rf = RandomForestClassifier(n_estimators=60, max_depth=3, min_samples_leaf=max(25, len(ytr) // 40),
                                    max_features=0.5, random_state=7, n_jobs=RF_NJOBS)
        rf.fit(Xtr.values, ytr.values)
        rules = leaf_rules(rf, feats)
        seen = set(); mined = promoted = 0
        for conds in rules:
            sig = hashlib.md5(json.dumps(sorted([(f, o, t) for f, o, t in conds])).encode()).hexdigest()
            if sig in seen: continue
            seen.add(sig)
            mtr = apply_rule(F[tr & valid], conds)
            n_tr, h_tr, p_tr = stats(F[tr & valid], ytr, mtr)
            lift_tr = p_tr - base_tr
            if n_tr < MIN_TR_OBS or lift_tr < MIN_TR_LIFT:
                continue
            mined += 1
            mva = apply_rule(F[va & valid], conds)
            n_va, h_va, p_va = stats(F[va & valid], yva, mva)
            lift_va = p_va - base_va
            promo = 1 if (n_va >= MIN_VA_OBS and lift_va >= MIN_VA_LIFT and p_va > base_va) else 0
            promoted += promo
            pid += 1
            rtext = " AND ".join(f"{f} {o} {t:g}" for f, o, t in conds)
            rjson = json.dumps([[f, o, t] for f, o, t in conds])
            con_snr.execute(
                "INSERT OR REPLACE INTO mined_patterns(Stock,pattern_id,mined_year,scope,outcome_target,"
                "n_obs,n_hits,precision_pct,base_rate_pct,lift_pct,depth,rule_text,rule_json,promoted) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (stock, pid, MINED_YEAR, "stock_specific", tname, n_tr, h_tr, round(p_tr, 2),
                 round(base_tr, 2), round(lift_tr, 2), len(conds), rtext, rjson, promo))
        con_snr.commit()
        total_mined += mined; total_promoted += promoted
        log(f"  {tname:12} base {base_tr:4.1f}% (val {base_va:4.1f}%) | mined {mined:>3} | promoted(held 2025) {promoted:>3}")
    log(f"  [{stock}] TOTAL mined {total_mined} | promoted {total_promoted}")
    return total_mined, total_promoted


def main():
    con = sqlite3.connect(str(SNR), timeout=60)
    for s in STOCKS:
        con.execute("DELETE FROM mined_patterns WHERE Stock=?", (s,)); con.commit()
        mine_stock(s, con)
    # report top promoted per stock
    log("\n=== TOP PROMOTED PATTERNS (held out-of-sample on 2025) ===")
    for s in STOCKS:
        rows = con.execute("SELECT outcome_target,precision_pct,base_rate_pct,lift_pct,n_obs,depth,rule_text "
                           "FROM mined_patterns WHERE Stock=? AND promoted=1 ORDER BY lift_pct DESC LIMIT 5",
                           (s,)).fetchall()
        log(f"\n{s}: {len(rows)} shown of promoted")
        for r in rows:
            log(f"  [{r[0]}] prec {r[1]:.0f}% vs base {r[2]:.0f}% (+{r[3]:.0f}pp) n={r[4]} d={r[5]}: {r[6][:90]}")
    con.close()
    log("\nPHASE-1 MINING COMPLETE")


if __name__ == "__main__":
    main()
