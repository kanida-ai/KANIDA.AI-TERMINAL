"""SHORT Falcon — mine + validate + promote bearish patterns into SEPARATE research tables
(falcon_short_candidates / falcon_short_promoted). Never touches the long engine's tables.
Mirrors falcon_validator gate exactly (OOS lift>=5pp, >=2 years, >=30 obs). READ the long tables,
WRITE only the short_* tables."""
import os, sys, json, sqlite3, time
from collections import defaultdict
import numpy as np
ROOT = r"C:\Users\SPS\Desktop\Kanida.ai Terminal Quant Intelligence Engine"
sys.path.insert(0, os.path.join(ROOT, "universe_engine", "engine"))
import falcon_miner as MI
import falcon_validator as VA
DB = os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
TARGETS = ["fell_10pc_20d", "fell_15pc_20d", "fell_25pc_30d", "fell_40pc_40d"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS falcon_short_candidates (
  pattern_id INTEGER PRIMARY KEY AUTOINCREMENT, mined_year TEXT, scope TEXT, outcome_target TEXT,
  n_obs INT, n_hits INT, precision_pct REAL, base_rate_pct REAL, lift_pct REAL, depth INT,
  rule_json TEXT, rule_text TEXT, UNIQUE(mined_year,scope,outcome_target,rule_text));
CREATE TABLE IF NOT EXISTS falcon_short_promoted (
  pattern_id INTEGER PRIMARY KEY, classification TEXT, avg_oos_year_lift_pp REAL,
  n_years_passed INT, avg_cross_sector_lift_pp REAL, mined_year TEXT, outcome_target TEXT, rule_json TEXT);
"""

def mine():
    con = sqlite3.connect(DB, timeout=120); con.execute("PRAGMA busy_timeout=120000"); con.executescript(SCHEMA)
    con.execute("DELETE FROM falcon_short_candidates"); con.commit()
    years = [str(r[0]) for r in con.execute("SELECT DISTINCT substr(trade_date,1,4) FROM falcon_features ORDER BY 1")]
    con.close()
    print(f"[short-mine] years={years} targets={TARGETS} (universe scope)", flush=True)
    all_c = []; t0 = time.time()
    for yr in years:
        for tg in TARGETS:
            c = MI._mine_slice((DB, yr, "universe", tg, 4, 50, 10.0))
            all_c.extend(c)
        print(f"  {yr}: cumulative candidates={len(all_c):,} ({time.time()-t0:.0f}s)", flush=True)
    con = sqlite3.connect(DB, timeout=120); con.execute("PRAGMA busy_timeout=120000")
    con.executemany("""INSERT OR IGNORE INTO falcon_short_candidates
        (mined_year,scope,outcome_target,n_obs,n_hits,precision_pct,base_rate_pct,lift_pct,depth,rule_json,rule_text)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        [(c["mined_year"], c["scope"], c["outcome_target"], c["n_obs"], c["n_hits"], c["precision_pct"],
          c["base_rate_pct"], c["lift_pct"], c["depth"], c["rule_json"], c["rule_text"]) for c in all_c])
    con.commit(); n = con.execute("SELECT COUNT(*) FROM falcon_short_candidates").fetchone()[0]; con.close()
    print(f"[short-mine] persisted {n} short candidates", flush=True)


class Panel:
    def __init__(self, db):
        con = sqlite3.connect(db, timeout=120)
        sym_sec = dict(con.execute("SELECT symbol,sector FROM falcon_sectors").fetchall())
        self.sec_names = sorted(set(sym_sec.values())); s2i = {s: i for i, s in enumerate(self.sec_names)}
        feats = ",".join(VA.FEATURE_COLS); tg = ",".join(f"o.{t}" for t in TARGETS)
        rows = con.execute(f"SELECT f.symbol,f.trade_date,{feats},{tg} FROM falcon_features f "
                           f"INNER JOIN falcon_outcomes o ON o.symbol=f.symbol AND o.trade_date=f.trade_date").fetchall()
        con.close()
        n = len(rows); nf = len(VA.FEATURE_COLS)
        self.X = np.full((n, nf), np.nan); self.year = np.zeros(n, np.int32); self.sector_idx = np.full(n, -1, np.int32)
        self.Y = {t: np.zeros(n, np.int8) for t in TARGETS}; self.sec_to_idx = s2i
        for i, r in enumerate(rows):
            self.X[i] = [v if v is not None else np.nan for v in r[2:2 + nf]]
            self.year[i] = int(r[1][:4]); self.sector_idx[i] = s2i.get(sym_sec.get(r[0], ""), -1)
            for j, t in enumerate(TARGETS): self.Y[t][i] = int(r[2 + nf + j] or 0)
        print(f"[short-validate] panel {n:,} rows", flush=True)


def validate_promote():
    panel = Panel(DB)
    con = sqlite3.connect(DB, timeout=120); con.execute("PRAGMA busy_timeout=120000")
    con.execute("DELETE FROM falcon_short_promoted"); con.commit()
    cands = con.execute("SELECT pattern_id,mined_year,scope,outcome_target,rule_json FROM falcon_short_candidates").fetchall()
    years = sorted(set(int(y) for y in panel.year.tolist()))
    ymask = {y: (panel.year == y) for y in years}; smask = {s: (panel.sector_idx == i) for s, i in panel.sec_to_idx.items()}
    promoted = []; t0 = time.time()
    for k, (pid, my, scope, tg, rj) in enumerate(cands):
        rule = [(f, op, th) for f, op, th in json.loads(rj)]
        yl = []
        for y in years:
            if str(y) == my: continue
            n, h, prec, base = VA.evaluate(rule, ymask[y], panel, tg)
            if n >= VA.MIN_OOS_OBS: yl.append(prec - base)
        if not yl: continue
        yp = sum(1 for l in yl if l > 0); avg = sum(yl) / len(yl)
        if avg < VA.MIN_OOS_LIFT_PP or yp < VA.MIN_OOS_YEARS_PASS: continue
        csl = []
        for s, sm in smask.items():
            n, h, prec, base = VA.evaluate(rule, ymask[int(my)] & sm, panel, tg)
            if n >= VA.MIN_OOS_OBS: csl.append(prec - base)
        acs = sum(csl) / len(csl) if csl else 0.0
        cls = "universal" if (yp >= 3 and acs >= 0) else "regime_dependent"
        promoted.append((pid, cls, round(avg, 4), yp, round(acs, 4), my, tg, rj))
        if (k + 1) % 500 == 0: print(f"  validated {k+1}/{len(cands)} ({time.time()-t0:.0f}s)", flush=True)
    con.executemany("INSERT OR REPLACE INTO falcon_short_promoted (pattern_id,classification,avg_oos_year_lift_pp,n_years_passed,avg_cross_sector_lift_pp,mined_year,outcome_target,rule_json) VALUES (?,?,?,?,?,?,?,?)", promoted)
    con.commit()
    byy = defaultdict(int)
    for p in con.execute("SELECT mined_year FROM falcon_short_promoted"): byy[p[0]] += 1
    print(f"[short-validate] PROMOTED {len(promoted)} short patterns. by mined_year: {dict(sorted(byy.items()))}", flush=True)
    con.close()


if __name__ == "__main__":
    mine(); validate_promote()
