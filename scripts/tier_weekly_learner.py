"""Phase 1 — weekly SHADOW learner for the tier rulebook.

Every week this re-grades the live (status='active') rulebook against the
latest full trade history, writes a CHALLENGER snapshot (status='challenger')
with fresh metrics, and flags DRIFT vs the active baseline. It is SHADOW-ONLY:
it never changes the 'active' rules — promotion is a future gated step (Phase 2).

What it does:
  1. load active rules from falcon_tier_rules
  2. classify every resolved trade via the live classifier (rulebook path)
  3. compute per-tier N / WR / avg-ret / stop  (full history + last-90d window)
  4. wipe old 'challenger' rows, write a fresh challenger snapshot
  5. print champion-vs-challenger + drift flags (>3pp WR drop = flag)

Read-only except the 'challenger' rows in falcon_tier_rules.
Run: POWER_DB_PATH=<prod.db> POWER_RND_DB_PATH=<rnd.db> python scripts/tier_weekly_learner.py
"""
import os, sqlite3, json, importlib.util
from datetime import datetime, timezone, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROD = os.environ.get("POWER_DB_PATH")     or os.path.join(ROOT, "data", "db", "kanida_universe.db")
RND  = os.environ.get("POWER_RND_DB_PATH") or os.path.join(ROOT, "universe_engine", "data", "db", "kanida_universe.db")
IST  = timezone(timedelta(hours=5, minutes=30))
DRIFT_PP = 3.0   # flag a tier if its WR fell >3pp vs the active baseline

# live classifier (rulebook eval + fallback)
_spec = importlib.util.spec_from_file_location(
    "signal_tier", os.path.join(ROOT, "backend", "power_user", "services", "signal_tier.py"))
st = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(st)

import numpy as np, pandas as pd


def _load_trades():
    con = sqlite3.connect(RND)
    df = pd.read_sql_query("""
        SELECT s.signal_date, s.symbol, s.avg_lift, s.net_ret_pct AS net, s.exit_reason,
               x.signal_day_ret_pct AS sret, x.two_day_ret_pct AS twoday,
               x.signal_day_range_pct AS rng
        FROM falcon_signal_day_study s
        JOIN falcon_signal_day_context x
          ON s.signal_date=x.signal_date AND s.symbol=x.symbol
        WHERE s.net_ret_pct IS NOT NULL AND x.is_extension=0""", con)
    con.close()
    oc = sqlite3.connect(OHLC := PROD)
    syms = tuple(sorted(df.symbol.unique()))
    oh = pd.read_sql_query("SELECT symbol,trade_date,close,volume FROM ohlc_daily WHERE symbol IN (%s)"
                           % ",".join("?"*len(syms)), oc, params=syms); oc.close()
    oh = oh.sort_values(["symbol","trade_date"]); g = oh.groupby("symbol", group_keys=False)
    oh["a20"]=g.volume.transform(lambda s:s.rolling(20,min_periods=10).mean())
    oh["a3"]=g.volume.transform(lambda s:s.rolling(3,min_periods=2).mean())
    oh["turn"]=oh.close*oh.volume
    oh["turn_pct"]=g.turn.transform(lambda s:s.rolling(252,min_periods=60).apply(lambda w:(w<=w[-1]).mean(),raw=True))
    oh["trend3_20"]=oh.a3/oh.a20
    df = df.merge(oh[["symbol","trade_date","trend3_20","turn_pct"]],
                  left_on=["symbol","signal_date"], right_on=["symbol","trade_date"], how="left")
    df["win"]=(df.net>0).astype(int); df["stop"]=(df.exit_reason=="INIT_STOP").astype(int)
    return df


def main():
    as_of = datetime.now(IST).strftime("%Y-%m-%d")
    con = sqlite3.connect(PROD)
    active = con.execute(
        "SELECT rule_id, tier, conditions_json, is_wr FROM falcon_tier_rules WHERE status='active'"
    ).fetchall()
    if not active:
        print("No active rulebook — run tier_rulebook_migrate.py first."); return
    rules = sorted([(json.loads(cj).get("priority",50), rid, tier, json.loads(cj).get("all",[]))
                    for rid,tier,cj,_ in active], key=lambda r:r[0])
    base_wr = {rid: wr for rid,_,_,wr in active}   # keyed by rule_id (unique)

    df = _load_trades()
    def _match_rule(r):
        feat={"sret":r.sret,"twoday":r.twoday,"rng":r.rng,"avg_lift":r.avg_lift,
              "trend3_20":r.trend3_20,"turn_pct":r.turn_pct}
        for _prio, rid, _tier, conds in rules:
            ok=True
            for f,op,val in conds:
                v=feat.get(f)
                if not st._ok(v) or op not in st._OPS or not st._OPS[op](v,val): ok=False; break
            if ok: return rid
        return "catch_all"
    df["rule_id"] = df.apply(_match_rule, axis=1)
    df["yr"] = df.signal_date.str[:4]
    cutoff = (datetime.now(IST) - timedelta(days=90)).strftime("%Y-%m-%d")

    def metrics(d):
        n=len(d); return (n, round(100*d.win.mean(),1) if n else None,
                          round(d.net.mean(),2) if n else None,
                          round(100*d.stop.mean(),1) if n else None)

    print(f"=== weekly shadow learner — as_of {as_of} ===")
    print(f"{'rule_id':<18}{'tier':<20}{'N':>6}{'WR%':>7}{'ret%':>7}{'stop%':>7}{'90d_N':>7}{'90d_WR':>8}  drift")
    con.execute("DELETE FROM falcon_tier_rules WHERE status='challenger'")
    flags = []
    for rid, tier, cj, _ in active:
        sub = df[df.rule_id==rid]
        n,wr,ret,stop = metrics(sub)
        rec = df[(df.rule_id==rid) & (df.signal_date>=cutoff)]
        rn,rwr,_,_ = metrics(rec)
        py = {y:metrics(g)[1] for y,g in sub.groupby("yr")}
        drift = ""
        if wr is not None and base_wr.get(rid) is not None and (base_wr[rid]-wr) > DRIFT_PP:
            drift = f"DRIFT -{round(base_wr[rid]-wr,1)}pp"; flags.append((rid,drift))
        print(f"{rid:<18}{tier:<20}{n:>6}{str(wr):>7}{str(ret):>7}{str(stop):>7}{rn:>7}{str(rwr):>8}  {drift}")
        con.execute(
            "INSERT INTO falcon_tier_rules (rule_id,tier,conditions_json,is_wr,is_ret,is_n,"
            "per_year_oos_json,status,reason,as_of) VALUES (?,?,?,?,?,?,?, 'challenger', ?, ?)",
            (rid, tier, cj, wr, ret, n, json.dumps(py),
             (drift or "re-graded; in line with active"), as_of))
    con.commit(); con.close()
    print(f"\nWrote challenger snapshot ({as_of}). SHADOW ONLY — active rules unchanged.")
    if flags:
        print("DRIFT flags (review before any promotion):")
        for t,d in flags: print(f"  {t}: {d}")
    else:
        print("No drift — active rulebook still tracks the data.")


if __name__ == "__main__":
    main()
