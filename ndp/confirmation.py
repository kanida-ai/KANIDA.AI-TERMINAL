# -*- coding: utf-8 -*-
"""NDP Addendum B — morning confirmation layer. Confirms/vetoes an already-published EOD fire using the
opening window (gap alignment). Never generates. Measures confirmation lift + veto discrimination + the
counterfactual (what vetoed signals would have returned). WR is never reported without retention%."""
import os, sqlite3
import numpy as np, pandas as pd
NDB = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ndp", "ndp.db")


def confirm(recs, sym="ICICIBANK"):
    """recs = OOS fires (from engine.discover_grid). Adds gap alignment and reports the four-model view."""
    if recs is None or recs.empty:
        return pd.DataFrame()
    con = sqlite3.connect("file:" + NDB.replace("\\", "/") + "?mode=ro", uri=True)
    g = pd.read_sql_query("SELECT signal_date entry_date, gap_open_pct FROM path_tensor "
                          "WHERE symbol=? AND entry_time='09:15'", con, params=[sym]); con.close()
    r = recs.merge(g, on="entry_date", how="left")
    r["gap_aligned"] = np.where(r.direction == "LONG", r.gap_open_pct > 0, r.gap_open_pct < 0)
    out = []
    for qid, d in r.groupby("qid"):
        base_wr = d.hit_touch.mean(); n_all = len(d)
        conf = d[d.gap_aligned]; veto = d[~d.gap_aligned]
        wr_c = conf.hit_touch.mean() if len(conf) else np.nan
        wr_v = veto.hit_touch.mean() if len(veto) else np.nan
        out.append(dict(qid=qid, model="NONE", n=n_all, retained_pct=100.0, wr=round(base_wr*100, 1),
                        veto_discrim=np.nan, lift=0.0))
        out.append(dict(qid=qid, model="C0915_gap_align", n=len(conf),
                        retained_pct=round(len(conf)/max(n_all, 1)*100, 1), wr=round(wr_c*100, 1) if len(conf) else np.nan,
                        veto_discrim=round((wr_c-wr_v)*100, 1) if (len(conf) and len(veto)) else np.nan,
                        lift=round((wr_c-base_wr)*100, 1) if len(conf) else np.nan))
    return pd.DataFrame(out)
